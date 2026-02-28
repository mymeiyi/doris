// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.cloud.rpc;

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.common.Config;
import org.apache.doris.metric.CloudMetrics;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.rpc.RpcException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class MetaServiceProxy {
    private static final Logger LOG = LogManager.getLogger(MetaServiceProxy.class);

    // use exclusive lock to make sure only one thread can add or remove client from
    // serviceMap.
    // use concurrent map to allow access serviceMap in multi thread.
    private ReentrantLock lock = new ReentrantLock();
    private final Map<String, MetaServiceClient> serviceMap;
    private Queue<Long> lastConnTimeMs = new LinkedList<>();

    static {
        if (Config.isCloudMode() && (Config.meta_service_endpoint == null || Config.meta_service_endpoint.isEmpty())) {
            throw new RuntimeException("in cloud mode, please configure meta_service_endpoint in fe.conf");
        }
    }

    public MetaServiceProxy() {
        this.serviceMap = Maps.newConcurrentMap();
        for (int i = 0; i < 3; ++i) {
            lastConnTimeMs.add(0L);
        }
    }

    private static class SingletonHolder {
        private static AtomicInteger count = new AtomicInteger();
        private static MetaServiceProxy[] proxies;

        static {
            if (Config.isCloudMode()) {
                int size = Config.meta_service_connection_pooled
                        ? Config.meta_service_connection_pool_size
                        : 1;
                proxies = new MetaServiceProxy[size];
                for (int i = 0; i < size; ++i) {
                    proxies[i] = new MetaServiceProxy();
                }
            }
        }

        static MetaServiceProxy get() {
            return proxies[Math.abs(count.addAndGet(1) % proxies.length)];
        }
    }

    public static MetaServiceProxy getInstance() {
        return MetaServiceProxy.SingletonHolder.get();
    }

    public boolean needReconn() {
        lock.lock();
        try {
            long now = System.currentTimeMillis();
            return (now - lastConnTimeMs.element() > Config.meta_service_rpc_reconnect_interval_ms);
        } finally {
            lock.unlock();
        }
    }

    public Cloud.GetInstanceResponse getInstance(Cloud.GetInstanceRequest request)
            throws RpcException {
        long startTime = System.currentTimeMillis();
        String methodName = "getInstance";
        int cost = MetaServiceRateLimiter.getRequestCost(methodName, request);
        if (MetricRepo.isInit && Config.isCloudMode()) {
            CloudMetrics.META_SERVICE_RPC_ALL_TOTAL.increase(1L);
            CloudMetrics.META_SERVICE_RPC_TOTAL.getOrAdd(methodName).increase(1L);
        }

        boolean acquired = false;
        try {
            acquired = MetaServiceRateLimiter.getInstance().acquire(methodName, cost);
            final MetaServiceClient client = getProxy();
            Cloud.GetInstanceResponse response = client.getInstance(request);
            if (MetricRepo.isInit && Config.isCloudMode()) {
                CloudMetrics.META_SERVICE_RPC_LATENCY.getOrAdd(methodName)
                        .update(System.currentTimeMillis() - startTime);
            }
            handleResponseStatus(response.getStatus());
            return response;
        } catch (Exception e) {
            if (MetricRepo.isInit && Config.isCloudMode()) {
                CloudMetrics.META_SERVICE_RPC_ALL_FAILED.increase(1L);
                CloudMetrics.META_SERVICE_RPC_FAILED.getOrAdd(methodName).increase(1L);
                CloudMetrics.META_SERVICE_RPC_LATENCY.getOrAdd(methodName)
                        .update(System.currentTimeMillis() - startTime);
            }
            throw new RpcException("", e.getMessage(), e);
        } finally {
            if (acquired) {
                MetaServiceRateLimiter.getInstance().release(methodName, cost);
            }
        }
    }

    public void removeProxy(String address) {
        LOG.warn("begin to remove proxy: {}", address);
        MetaServiceClient service;
        lock.lock();
        try {
            service = serviceMap.remove(address);
        } finally {
            lock.unlock();
        }

        if (service != null) {
            service.shutdown(false);
        }
    }

    private MetaServiceClient getProxy() {
        if (Config.enable_check_compatibility_mode) {
            LOG.error("Should not use RPC in check compatibility mode");
            throw new RuntimeException("use RPC in the check compatibility mode");
        }

        String address = Config.meta_service_endpoint;
        address = address.replaceAll("^[\"']|[\"']$", "");
        MetaServiceClient service = serviceMap.get(address);
        if (service != null && service.isNormalState() && !service.isConnectionAgeExpired()) {
            return service;
        }

        // not exist, create one and return.
        MetaServiceClient removedClient = null;
        lock.lock();
        try {
            service = serviceMap.get(address);
            if (service != null && !service.isNormalState()) {
                // At this point we cannot judge the progress of reconnecting the underlying
                // channel.
                // In the worst case, it may take two minutes. But we can't stand the connection
                // refused
                // for two minutes, so rebuild the channel directly.
                serviceMap.remove(address);
                removedClient = service;
                service = null;
            }
            if (service != null && service.isConnectionAgeExpired()) {
                serviceMap.remove(address);
                removedClient = service;
                service = null;
            }
            if (service == null) {
                service = new MetaServiceClient(address);
                serviceMap.put(address, service);
                lastConnTimeMs.add(System.currentTimeMillis());
                lastConnTimeMs.remove();
            }
            return service;
        } finally {
            lock.unlock();
            if (removedClient != null) {
                removedClient.shutdown(true);
            }
        }
    }

    public static class MetaServiceClientWrapper {
        private final MetaServiceProxy proxy;
        private Random random = new Random();

        public MetaServiceClientWrapper(MetaServiceProxy proxy) {
            this.proxy = proxy;
        }

        public <Response> Response executeRequest(String methodName, int cost,
                Function<MetaServiceClient, Response> function,
                Function<Response, Cloud.MetaServiceResponseStatus> statusExtractor) throws RpcException {
            long maxRetries = Config.meta_service_rpc_retry_cnt;
            for (long tried = 1; tried <= maxRetries; tried++) {
                MetaServiceClient client = null;
                boolean requestFailed = false;
                boolean acquired = false;
                try {
                    acquired = MetaServiceRateLimiter.getInstance().acquire(methodName, cost);
                    client = proxy.getProxy();
                    if (tried > 1 && MetricRepo.isInit && Config.isCloudMode()) {
                        CloudMetrics.META_SERVICE_RPC_ALL_RETRY.increase(1L);
                        CloudMetrics.META_SERVICE_RPC_RETRY.getOrAdd(methodName).increase(1L);
                    }
                    Response response = function.apply(client);
                    handleResponseStatus(statusExtractor.apply(response));
                    return response;
                } catch (StatusRuntimeException sre) {
                    requestFailed = true;
                    LOG.warn("failed to request meta service code {}, msg {}, trycnt {}", sre.getStatus().getCode(),
                            sre.getMessage(), tried);
                    boolean shouldRetry = false;
                    switch (sre.getStatus().getCode()) {
                        case UNAVAILABLE:
                        case UNKNOWN:
                            shouldRetry = true;
                            break;
                        case DEADLINE_EXCEEDED:
                            shouldRetry = tried <= Config.meta_service_rpc_timeout_retry_times;
                            break;
                        default:
                            shouldRetry = false;
                    }
                    if (!shouldRetry || tried >= maxRetries) {
                        throw new RpcException("", sre.getMessage(), sre);
                    }
                } catch (Exception e) {
                    requestFailed = true;
                    LOG.warn("failed to request meta servive trycnt {}", tried, e);
                    if (tried >= maxRetries) {
                        throw new RpcException("", e.getMessage(), e);
                    }
                } finally {
                    if (acquired) {
                        MetaServiceRateLimiter.getInstance().release(methodName, cost);
                    }
                    if (requestFailed && proxy.needReconn() && client != null) {
                        client.shutdown(true);
                    }
                }

                int delay = 20 + random.nextInt(200 - 20 + 1);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    throw new RpcException("", interruptedException.getMessage(), interruptedException);
                }
            }
            // impossible and unreachable, just make the compiler happy
            throw new RpcException("", "All retries exhausted", null);
        }
    }

    private final MetaServiceClientWrapper w = new MetaServiceClientWrapper(this);

    /**
     * Execute RPC with comprehensive metrics tracking.
     * Tracks: total calls, failures, latency
     */
    private <Response> Response executeWithMetrics(String methodName, int cost,
            Function<MetaServiceClient, Response> function,
            Function<Response, Cloud.MetaServiceResponseStatus> statusExtractor) throws RpcException {
        long startTime = System.currentTimeMillis();
        if (MetricRepo.isInit && Config.isCloudMode()) {
            CloudMetrics.META_SERVICE_RPC_ALL_TOTAL.increase(1L);
            CloudMetrics.META_SERVICE_RPC_TOTAL.getOrAdd(methodName).increase(1L);
        }

        try {
            Response response = w.executeRequest(methodName, cost, function, statusExtractor);
            if (MetricRepo.isInit && Config.isCloudMode()) {
                CloudMetrics.META_SERVICE_RPC_LATENCY.getOrAdd(methodName)
                        .update(System.currentTimeMillis() - startTime);
            }
            return response;
        } catch (RpcException e) {
            if (MetricRepo.isInit && Config.isCloudMode()) {
                CloudMetrics.META_SERVICE_RPC_ALL_FAILED.increase(1L);
                CloudMetrics.META_SERVICE_RPC_FAILED.getOrAdd(methodName).increase(1L);
                CloudMetrics.META_SERVICE_RPC_LATENCY.getOrAdd(methodName)
                        .update(System.currentTimeMillis() - startTime);
            }
            throw e;
        }
    }

    public Cloud.GetVersionResponse getVisibleVersion(Cloud.GetVersionRequest request, int timeoutMs) {
        long startTime = System.currentTimeMillis();
        String methodName = request.hasIsTableVersion() && request.getIsTableVersion() ? "getTableVersion"
                : "getPartitionVersion";
        MetaServiceClient client = null;
        int cost = MetaServiceRateLimiter.getRequestCost(methodName, request);
        boolean acquired = false;
        if (MetricRepo.isInit && Config.isCloudMode()) {
            CloudMetrics.META_SERVICE_RPC_ALL_TOTAL.increase(1L);
            CloudMetrics.META_SERVICE_RPC_TOTAL.getOrAdd(methodName).increase(1L);
        }

        long deadline = System.currentTimeMillis() + timeoutMs;
        Cloud.GetVersionResponse resp = null;
        try {
            acquired = MetaServiceRateLimiter.getInstance().acquire(methodName, cost);
            Future<Cloud.GetVersionResponse> future = getVisibleVersionAsync(request);
            while (resp == null) {
                try {
                    resp = future.get(Math.max(0, deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    LOG.warn("get version from meta service: future get interrupted exception", e);
                }
            }
        } catch (RpcException | ExecutionException | TimeoutException | RuntimeException e) {
            LOG.warn("get version from meta service failed, exception: ", e);
        } finally {
            if (acquired) {
                MetaServiceRateLimiter.getInstance().release(methodName, cost);
            }
            if (MetricRepo.isInit && Config.isCloudMode()) {
                CloudMetrics.META_SERVICE_RPC_LATENCY.getOrAdd(methodName)
                        .update(System.currentTimeMillis() - startTime);
                if (resp == null) {
                    CloudMetrics.META_SERVICE_RPC_ALL_FAILED.increase(1L);
                    CloudMetrics.META_SERVICE_RPC_FAILED.getOrAdd(methodName).increase(1L);
                }
            }
        }
        if (resp != null) {
            handleResponseStatus(resp.getStatus());
        }
        return resp;
    }

    @VisibleForTesting
    protected Future<Cloud.GetVersionResponse> getVisibleVersionAsync(Cloud.GetVersionRequest request)
            throws RpcException {
        MetaServiceClient client = null;
        try {
            client = getProxy();
            Future<Cloud.GetVersionResponse> future = client.getVisibleVersionAsync(request);
            if (future instanceof com.google.common.util.concurrent.ListenableFuture) {
                com.google.common.util.concurrent.ListenableFuture<Cloud.GetVersionResponse> listenableFuture =
                        (com.google.common.util.concurrent.ListenableFuture<Cloud.GetVersionResponse>) future;
                MetaServiceClient finalClient = client;
                com.google.common.util.concurrent.Futures.addCallback(listenableFuture,
                        new com.google.common.util.concurrent.FutureCallback<Cloud.GetVersionResponse>() {
                            @Override
                            public void onSuccess(Cloud.GetVersionResponse result) {
                            }

                            @Override
                            public void onFailure(Throwable t) {
                                if (finalClient != null) {
                                    finalClient.shutdown(true);
                                }
                            }
                        }, com.google.common.util.concurrent.MoreExecutors.directExecutor());
            }
            return future;
        } catch (Exception e) {
            if (client != null) {
                client.shutdown(true);
            }
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.GetVersionResponse getVersion(Cloud.GetVersionRequest request) throws RpcException {
        String methodName = request.hasIsTableVersion() && request.getIsTableVersion() ? "getTableVersion"
                : "getPartitionVersion";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.getVersion(request), Cloud.GetVersionResponse::getStatus);
    }

    public Cloud.CreateTabletsResponse createTablets(Cloud.CreateTabletsRequest request) throws RpcException {
        String methodName = "createTablets";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.createTablets(request), Cloud.CreateTabletsResponse::getStatus);
    }

    public Cloud.UpdateTabletResponse updateTablet(Cloud.UpdateTabletRequest request) throws RpcException {
        String methodName = "updateTablet";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.updateTablet(request), Cloud.UpdateTabletResponse::getStatus);
    }

    public Cloud.BeginTxnResponse beginTxn(Cloud.BeginTxnRequest request) throws RpcException {
        String methodName = "beginTxn";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.beginTxn(request), Cloud.BeginTxnResponse::getStatus);
    }

    public Cloud.PrecommitTxnResponse precommitTxn(Cloud.PrecommitTxnRequest request) throws RpcException {
        String methodName = "precommitTxn";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.precommitTxn(request), Cloud.PrecommitTxnResponse::getStatus);
    }

    public Cloud.CommitTxnResponse commitTxn(Cloud.CommitTxnRequest request) throws RpcException {
        String methodName = "commitTxn";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.commitTxn(request), Cloud.CommitTxnResponse::getStatus);
    }

    public Cloud.AbortTxnResponse abortTxn(Cloud.AbortTxnRequest request) throws RpcException {
        String methodName = "abortTxn";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.abortTxn(request), Cloud.AbortTxnResponse::getStatus);
    }

    public Cloud.GetTxnResponse getTxn(Cloud.GetTxnRequest request) throws RpcException {
        String methodName = "getTxn";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.getTxn(request), Cloud.GetTxnResponse::getStatus);
    }

    public Cloud.GetTxnIdResponse getTxnId(Cloud.GetTxnIdRequest request) throws RpcException {
        String methodName = "getTxnId";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.getTxnId(request), Cloud.GetTxnIdResponse::getStatus);
    }

    public Cloud.GetCurrentMaxTxnResponse getCurrentMaxTxnId(Cloud.GetCurrentMaxTxnRequest request)
            throws RpcException {
        String methodName = "getCurrentMaxTxnId";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.getCurrentMaxTxnId(request), Cloud.GetCurrentMaxTxnResponse::getStatus);
    }

    public Cloud.CreateMetaSyncPointResponse createMetaSyncPoint(Cloud.CreateMetaSyncPointRequest request)
            throws RpcException {
        String methodName = "createMetaSyncPoint";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.createMetaSyncPoint(request));
    }

    public Cloud.BeginSubTxnResponse beginSubTxn(Cloud.BeginSubTxnRequest request)
            throws RpcException {
        String methodName = "beginSubTxn";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.beginSubTxn(request), Cloud.BeginSubTxnResponse::getStatus);
    }

    public Cloud.AbortSubTxnResponse abortSubTxn(Cloud.AbortSubTxnRequest request) throws RpcException {
        String methodName = "abortSubTxn";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.abortSubTxn(request), Cloud.AbortSubTxnResponse::getStatus);
    }

    public Cloud.CheckTxnConflictResponse checkTxnConflict(Cloud.CheckTxnConflictRequest request)
            throws RpcException {
        String methodName = "checkTxnConflict";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.checkTxnConflict(request), Cloud.CheckTxnConflictResponse::getStatus);
    }

    public Cloud.CleanTxnLabelResponse cleanTxnLabel(Cloud.CleanTxnLabelRequest request) throws RpcException {
        String methodName = "cleanTxnLabel";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.cleanTxnLabel(request), Cloud.CleanTxnLabelResponse::getStatus);
    }

    public Cloud.GetClusterResponse getCluster(Cloud.GetClusterRequest request) throws RpcException {
        String methodName = "getCluster";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.getCluster(request), Cloud.GetClusterResponse::getStatus);
    }

    public Cloud.IndexResponse prepareIndex(Cloud.IndexRequest request) throws RpcException {
        String methodName = "prepareIndex";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.prepareIndex(request), Cloud.IndexResponse::getStatus);
    }

    public Cloud.IndexResponse commitIndex(Cloud.IndexRequest request) throws RpcException {
        String methodName = "commitIndex";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.commitIndex(request), Cloud.IndexResponse::getStatus);
    }

    public Cloud.CheckKVResponse checkKv(Cloud.CheckKVRequest request) throws RpcException {
        String methodName = "checkKv";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.checkKv(request), Cloud.CheckKVResponse::getStatus);
    }

    public Cloud.IndexResponse dropIndex(Cloud.IndexRequest request) throws RpcException {
        String methodName = "dropIndex";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.dropIndex(request), Cloud.IndexResponse::getStatus);
    }

    public Cloud.PartitionResponse preparePartition(Cloud.PartitionRequest request) throws RpcException {
        String methodName = "preparePartition";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.preparePartition(request), Cloud.PartitionResponse::getStatus);
    }

    public Cloud.PartitionResponse commitPartition(Cloud.PartitionRequest request) throws RpcException {
        String methodName = "commitPartition";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.commitPartition(request), Cloud.PartitionResponse::getStatus);
    }

    public Cloud.PartitionResponse dropPartition(Cloud.PartitionRequest request) throws RpcException {
        String methodName = "dropPartition";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.dropPartition(request), Cloud.PartitionResponse::getStatus);
    }

    public Cloud.GetTabletStatsResponse getTabletStats(Cloud.GetTabletStatsRequest request) throws RpcException {
        String methodName = "getTabletStats";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.getTabletStats(request), Cloud.GetTabletStatsResponse::getStatus);
    }

    public Cloud.CreateStageResponse createStage(Cloud.CreateStageRequest request) throws RpcException {
        String methodName = "createStage";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.createStage(request), Cloud.CreateStageResponse::getStatus);
    }

    public Cloud.GetStageResponse getStage(Cloud.GetStageRequest request) throws RpcException {
        String methodName = "getStage";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.getStage(request), Cloud.GetStageResponse::getStatus);
    }

    public Cloud.DropStageResponse dropStage(Cloud.DropStageRequest request) throws RpcException {
        String methodName = "dropStage";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.dropStage(request), Cloud.DropStageResponse::getStatus);
    }

    public Cloud.GetIamResponse getIam(Cloud.GetIamRequest request) throws RpcException {
        String methodName = "getIam";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.getIam(request), Cloud.GetIamResponse::getStatus);
    }

    public Cloud.BeginCopyResponse beginCopy(Cloud.BeginCopyRequest request) throws RpcException {
        String methodName = "beginCopy";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.beginCopy(request), Cloud.BeginCopyResponse::getStatus);
    }

    public Cloud.FinishCopyResponse finishCopy(Cloud.FinishCopyRequest request) throws RpcException {
        String methodName = "finishCopy";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.finishCopy(request), Cloud.FinishCopyResponse::getStatus);
    }

    public Cloud.GetCopyJobResponse getCopyJob(Cloud.GetCopyJobRequest request) throws RpcException {
        String methodName = "getCopyJob";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.getCopyJob(request), Cloud.GetCopyJobResponse::getStatus);
    }

    public Cloud.GetCopyFilesResponse getCopyFiles(Cloud.GetCopyFilesRequest request) throws RpcException {
        String methodName = "getCopyFiles";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.getCopyFiles(request), Cloud.GetCopyFilesResponse::getStatus);
    }

    public Cloud.FilterCopyFilesResponse filterCopyFiles(Cloud.FilterCopyFilesRequest request) throws RpcException {
        String methodName = "filterCopyFiles";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.filterCopyFiles(request), Cloud.FilterCopyFilesResponse::getStatus);
    }

    public Cloud.AlterClusterResponse alterCluster(Cloud.AlterClusterRequest request) throws RpcException {
        String methodName = "alterCluster";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.alterCluster(request), Cloud.AlterClusterResponse::getStatus);
    }

    public Cloud.GetDeleteBitmapUpdateLockResponse getDeleteBitmapUpdateLock(
            Cloud.GetDeleteBitmapUpdateLockRequest request) throws RpcException {
        String methodName = "getDeleteBitmapUpdateLock";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.getDeleteBitmapUpdateLock(request),
                Cloud.GetDeleteBitmapUpdateLockResponse::getStatus);
    }

    public Cloud.RemoveDeleteBitmapUpdateLockResponse removeDeleteBitmapUpdateLock(
            Cloud.RemoveDeleteBitmapUpdateLockRequest request) throws RpcException {
        String methodName = "removeDeleteBitmapUpdateLock";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.removeDeleteBitmapUpdateLock(request),
                Cloud.RemoveDeleteBitmapUpdateLockResponse::getStatus);
    }

    /**
     * This method is deprecated, there is no code to call it.
     */
    @Deprecated
    public Cloud.AlterObjStoreInfoResponse alterObjStoreInfo(Cloud.AlterObjStoreInfoRequest request)
            throws RpcException {
        String methodName = "alterObjStoreInfo";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.alterObjStoreInfo(request), Cloud.AlterObjStoreInfoResponse::getStatus);
    }

    public Cloud.AlterObjStoreInfoResponse alterStorageVault(Cloud.AlterObjStoreInfoRequest request)
            throws RpcException {
        String methodName = "alterStorageVault";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.alterStorageVault(request), Cloud.AlterObjStoreInfoResponse::getStatus);
    }

    public Cloud.FinishTabletJobResponse finishTabletJob(Cloud.FinishTabletJobRequest request)
            throws RpcException {
        String methodName = "finishTabletJob";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.finishTabletJob(request), Cloud.FinishTabletJobResponse::getStatus);
    }

    public Cloud.GetRLTaskCommitAttachResponse getRLTaskCommitAttach(Cloud.GetRLTaskCommitAttachRequest request)
            throws RpcException {
        String methodName = "getRLTaskCommitAttach";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.getRLTaskCommitAttach(request), Cloud.GetRLTaskCommitAttachResponse::getStatus);
    }

    public Cloud.ResetRLProgressResponse resetRLProgress(Cloud.ResetRLProgressRequest request)
            throws RpcException {
        String methodName = "resetRLProgress";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.resetRLProgress(request), Cloud.ResetRLProgressResponse::getStatus);
    }

    public Cloud.ResetStreamingJobOffsetResponse resetStreamingJobOffset(
            Cloud.ResetStreamingJobOffsetRequest request) throws RpcException {
        String methodName = "resetStreamingJobOffset";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.resetStreamingJobOffset(request), Cloud.ResetStreamingJobOffsetResponse::getStatus);
    }

    public Cloud.GetObjStoreInfoResponse getObjStoreInfo(Cloud.GetObjStoreInfoRequest request) throws RpcException {
        String methodName = "getObjStoreInfo";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.getObjStoreInfo(request), Cloud.GetObjStoreInfoResponse::getStatus);
    }

    public Cloud.AbortTxnWithCoordinatorResponse abortTxnWithCoordinator(
            Cloud.AbortTxnWithCoordinatorRequest request) throws RpcException {
        String methodName = "abortTxnWithCoordinator";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.abortTxnWithCoordinator(request), Cloud.AbortTxnWithCoordinatorResponse::getStatus);
    }

    public Cloud.GetPrepareTxnByCoordinatorResponse getPrepareTxnByCoordinator(
            Cloud.GetPrepareTxnByCoordinatorRequest request) throws RpcException {
        String methodName = "getPrepareTxnByCoordinator";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.getPrepareTxnByCoordinator(request),
                Cloud.GetPrepareTxnByCoordinatorResponse::getStatus);
    }

    public Cloud.CreateInstanceResponse createInstance(Cloud.CreateInstanceRequest request) throws RpcException {
        String methodName = "createInstance";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.createInstance(request), Cloud.CreateInstanceResponse::getStatus);
    }

    public Cloud.GetStreamingTaskCommitAttachResponse getStreamingTaskCommitAttach(
            Cloud.GetStreamingTaskCommitAttachRequest request) throws RpcException {
        String methodName = "getStreamingTaskCommitAttach";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.getStreamingTaskCommitAttach(request),
                Cloud.GetStreamingTaskCommitAttachResponse::getStatus);
    }

    public Cloud.DeleteStreamingJobResponse deleteStreamingJob(Cloud.DeleteStreamingJobRequest request)
            throws RpcException {
        String methodName = "deleteStreamingJob";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.deleteStreamingJob(request), Cloud.DeleteStreamingJobResponse::getStatus);
    }

    public Cloud.AlterInstanceResponse alterInstance(Cloud.AlterInstanceRequest request) throws RpcException {
        String methodName = "alterInstance";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.alterInstance(request), Cloud.AlterInstanceResponse::getStatus);
    }

    public Cloud.BeginSnapshotResponse beginSnapshot(Cloud.BeginSnapshotRequest request) throws RpcException {
        String methodName = "beginSnapshot";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.beginSnapshot(request), Cloud.BeginSnapshotResponse::getStatus);
    }

    public Cloud.UpdateSnapshotResponse updateSnapshot(Cloud.UpdateSnapshotRequest request) throws RpcException {
        String methodName = "updateSnapshot";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.updateSnapshot(request), Cloud.UpdateSnapshotResponse::getStatus);
    }

    public Cloud.CommitSnapshotResponse commitSnapshot(Cloud.CommitSnapshotRequest request) throws RpcException {
        String methodName = "commitSnapshot";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.commitSnapshot(request), Cloud.CommitSnapshotResponse::getStatus);
    }

    public Cloud.AbortSnapshotResponse abortSnapshot(Cloud.AbortSnapshotRequest request) throws RpcException {
        String methodName = "abortSnapshot";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.abortSnapshot(request), Cloud.AbortSnapshotResponse::getStatus);
    }

    public Cloud.ListSnapshotResponse listSnapshot(Cloud.ListSnapshotRequest request) throws RpcException {
        String methodName = "listSnapshot";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.listSnapshot(request), Cloud.ListSnapshotResponse::getStatus);
    }

    public Cloud.DropSnapshotResponse dropSnapshot(Cloud.DropSnapshotRequest request) throws RpcException {
        String methodName = "dropSnapshot";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.dropSnapshot(request), Cloud.DropSnapshotResponse::getStatus);
    }

    public Cloud.CloneInstanceResponse cloneInstance(Cloud.CloneInstanceRequest request) throws RpcException {
        String methodName = "cloneInstance";
        return executeWithMetrics(methodName, MetaServiceRateLimiter.getRequestCost(methodName, request),
                (client) -> client.cloneInstance(request), Cloud.CloneInstanceResponse::getStatus);
    }

    private static void handleResponseStatus(Cloud.MetaServiceResponseStatus status) {
        if (Config.meta_service_rpc_adaptive_throttle_enabled) {
            MetaServiceAdaptiveThrottle.Signal signal = isMetaServiceBusy(status)
                    ? MetaServiceAdaptiveThrottle.Signal.BACKPRESSURE
                    : MetaServiceAdaptiveThrottle.Signal.SUCCESS;
            MetaServiceAdaptiveThrottle.getInstance().recordSignal(signal);
        }
    }

    private static boolean isMetaServiceBusy(Cloud.MetaServiceResponseStatus status) {
        return status != null && status.hasCode() && status.getCode() == Cloud.MetaServiceCode.MAX_QPS_LIMIT;
    }
}
