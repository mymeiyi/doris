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

package org.apache.doris.cloud.catalog;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.cloud.transaction.CloudGlobalTransactionMgr;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.proc.PartitionsProcDir;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.thrift.TCloudVersionInfo;
import org.apache.doris.thrift.TFrontendSyncCloudVersionRequest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CloudSyncVersionDaemonTest {
    private Database db;
    private OlapTable table;
    private CloudPartition partition;
    private MetaServiceProxy proxy;
    private CloudSyncVersionDaemon daemon;
    private Field proxies;
    private MetaServiceProxy[] originalProxies;
    private MockedStatic<Env> mockedEnv;
    private MockedStatic<VariableMgr> mockedVariableMgr;
    private SessionVariable variables;
    private boolean originalEnableSyncer;
    private int originalSyncInterval;
    private int originalAttempts;
    private int originalBatchSize;

    @BeforeEach
    public void setUp() throws Exception {
        originalEnableSyncer = Config.cloud_enable_version_syncer;
        originalSyncInterval = Config.cloud_version_syncer_interval_second;
        originalAttempts = Config.cloud_version_syncer_get_version_retry_times;
        originalBatchSize = Config.cloud_get_version_task_batch_size;
        Config.cloud_enable_version_syncer = true;
        Config.cloud_version_syncer_interval_second = 0;
        Config.cloud_version_syncer_get_version_retry_times = 1;
        Config.cloud_get_version_task_batch_size = 1;

        db = new Database(1, "lazy_commit_db");
        table = new OlapTable(2, "lazy_commit_table", Collections.emptyList(), KeysType.DUP_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(1));
        Deencapsulation.setField(table, "versionLock", new ReentrantReadWriteLock(true));
        partition = CloudPartitionTest.createPartition(3, db.getId(), table.getId());
        table.addPartition(partition);
        db.registerTable(table);
        table.setCachedTableVersion(100);
        partition.setCachedVisibleVersion(12, 1000);

        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(catalog.getDbs()).thenReturn(Collections.singletonList(db));
        Mockito.when(catalog.getDbNullable(db.getFullName())).thenReturn(db);
        Mockito.when(catalog.getDbNullable(db.getId())).thenReturn(db);
        Mockito.when(catalog.getDb(db.getId())).thenReturn(Optional.of(db));
        CloudEnv env = Mockito.mock(CloudEnv.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        variables = new SessionVariable();
        variables.cloudPartitionVersionCacheTtlMs = Long.MAX_VALUE;
        variables.cloudTableVersionCacheTtlMs = Long.MAX_VALUE;
        mockedVariableMgr = Mockito.mockStatic(VariableMgr.class);
        mockedVariableMgr.when(VariableMgr::getDefaultSessionVariable).thenReturn(variables);

        // Mockito static mocks are thread-local. Replace the proxy pool so real daemon workers use this RPC stub.
        Class<?> holder = Class.forName(MetaServiceProxy.class.getName() + "$SingletonHolder");
        proxies = holder.getDeclaredField("proxies");
        proxies.setAccessible(true);
        originalProxies = (MetaServiceProxy[]) proxies.get(null);
        proxy = Mockito.mock(MetaServiceProxy.class);
        proxies.set(null, new MetaServiceProxy[] {proxy});
        daemon = new CloudSyncVersionDaemon();
    }

    @AfterEach
    public void tearDown() throws Exception {
        proxies.set(null, originalProxies);
        mockedVariableMgr.close();
        mockedEnv.close();
        Config.cloud_enable_version_syncer = originalEnableSyncer;
        Config.cloud_version_syncer_interval_second = originalSyncInterval;
        Config.cloud_version_syncer_get_version_retry_times = originalAttempts;
        Config.cloud_get_version_task_batch_size = originalBatchSize;
    }

    @Test
    public void testLazyCommitMustNotLeavePartitionCacheBehindTableVersion() throws Exception {
        // Lazy commit phase one has advanced the MS table version to 101, but partition 13 is still pending.
        AtomicBoolean pendingTxn = new AtomicBoolean(true);
        AtomicInteger tableRequests = new AtomicInteger();
        AtomicInteger partitionRequests = new AtomicInteger();
        Mockito.when(proxy.getVisibleVersionAsync(Mockito.any(Cloud.GetVersionRequest.class)))
                .thenAnswer(invocation -> {
                    Cloud.GetVersionRequest request = invocation.getArgument(0);
                    Assertions.assertTrue(request.getBatchMode());
                    Assertions.assertEquals(Collections.singletonList(db.getId()), request.getDbIdsList());
                    Assertions.assertEquals(Collections.singletonList(table.getId()), request.getTableIdsList());
                    Cloud.GetVersionResponse.Builder response = Cloud.GetVersionResponse.newBuilder()
                            .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(Cloud.MetaServiceCode.OK));
                    if (request.getIsTableVersion()) {
                        tableRequests.incrementAndGet();
                        response.addVersions(101);
                    } else {
                        partitionRequests.incrementAndGet();
                        Assertions.assertEquals(Collections.singletonList(partition.getId()),
                                request.getPartitionIdsList());
                        // A waiting read finishes lazy commit before returning the partition version.
                        if (request.getWaitForPendingTxn()) {
                            pendingTxn.set(false);
                        }
                        response.addVersions(pendingTxn.get() ? 12 : 13).addVersionUpdateTimeMs(1000);
                    }
                    return CompletableFuture.completedFuture(response.build());
                });

        daemon.runAfterCatalogReady();
        Assertions.assertEquals(2, tableRequests.get(), "Validate the table version before publishing partitions");
        Assertions.assertEquals(1, partitionRequests.get());
        Assertions.assertEquals(101, table.getCachedTableVersion());
        Assertions.assertFalse(pendingTxn.get(), "The daemon must wait before caching the new table version");
        Assertions.assertEquals(13, partition.getCachedVisibleVersion());

        // The original commit response was lost; its retry returns only VISIBLE txn_info.
        Cloud.CommitTxnResponse retryResponse = Cloud.CommitTxnResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(Cloud.MetaServiceCode.OK))
                .setTxnInfo(Cloud.TxnInfoPB.newBuilder().setDbId(db.getId()).addTableIds(table.getId())
                        .setTxnId(4).setStatus(Cloud.TxnStatusPB.TXN_STATUS_VISIBLE))
                .build();
        Deencapsulation.invoke(new CloudGlobalTransactionMgr(), "updateVersion", retryResponse);
        daemon.runAfterCatalogReady();
        Assertions.assertEquals(3, tableRequests.get());
        Assertions.assertEquals(1, partitionRequests.get(), "A successful sync does not need another partition RPC");
        Assertions.assertEquals(101, table.getCachedTableVersion());
        Assertions.assertEquals(13, partition.getCachedVisibleVersion());
    }

    @Test
    public void testForceSyncMustWaitForPendingTxnBeforePushingVersions() throws Exception {
        table.setSyncedTableVersion(100);
        AtomicBoolean pendingTxn = new AtomicBoolean(true);
        AtomicInteger partitionRequests = new AtomicInteger();
        Mockito.when(proxy.getVisibleVersionAsync(Mockito.any(Cloud.GetVersionRequest.class)))
                .thenAnswer(invocation -> {
                    Cloud.GetVersionRequest request = invocation.getArgument(0);
                    Assertions.assertEquals(Collections.singletonList(db.getId()), request.getDbIdsList());
                    Assertions.assertEquals(Collections.singletonList(table.getId()), request.getTableIdsList());
                    Cloud.GetVersionResponse.Builder response = Cloud.GetVersionResponse.newBuilder()
                            .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(Cloud.MetaServiceCode.OK));
                    if (request.getIsTableVersion()) {
                        // Lazy commit has advanced the table counter, but partition 13 is not visible yet.
                        response.addVersions(101);
                    } else {
                        partitionRequests.incrementAndGet();
                        Assertions.assertEquals(Collections.singletonList(partition.getId()),
                                request.getPartitionIdsList());
                        if (request.getWaitForPendingTxn()) {
                            pendingTxn.set(false);
                        }
                        response.addVersions(pendingTxn.get() ? 12 : 13).addVersionUpdateTimeMs(2000);
                    }
                    return CompletableFuture.completedFuture(response.build());
                });

        AtomicLong pushedTableVersion = new AtomicLong(-1);
        AtomicLong pushedPartitionVersion = new AtomicLong(-1);
        AtomicBoolean pendingAtPush = new AtomicBoolean(true);
        CloudFEVersionSynchronizer synchronizer = Mockito.mock(CloudFEVersionSynchronizer.class);
        Mockito.when(((CloudEnv) Env.getCurrentEnv()).getCloudFEVersionSynchronizer()).thenReturn(synchronizer);
        Mockito.doAnswer(invocation -> {
            List<Pair<OlapTable, Long>> tableVersions = invocation.getArgument(1);
            Map<CloudPartition, Pair<Long, Long>> partitionVersions = invocation.getArgument(2);
            Assertions.assertEquals(1, tableVersions.size());
            Assertions.assertEquals(1, partitionVersions.size());
            pushedTableVersion.set(tableVersions.get(0).second);
            pushedPartitionVersion.set(partitionVersions.get(partition).first);
            pendingAtPush.set(pendingTxn.get());
            return null;
        }).when(synchronizer).pushVersionAsync(Mockito.eq(db.getId()), Mockito.anyList(), Mockito.anyMap());

        ConnectContext previousContext = ConnectContext.get();
        ConnectContext ctx = new ConnectContext();
        ctx.setSessionVariable(variables);
        variables.cloudForceSyncVersion = true;
        ctx.setThreadLocalInfo();
        try (MockedStatic<Config> mockedConfig = Mockito.mockStatic(Config.class, Mockito.CALLS_REAL_METHODS)) {
            mockedConfig.when(Config::isNotCloudMode).thenReturn(false);
            // Exercise the actual forced SHOW PARTITIONS path; intercept only its outbound push.
            List<Long> shownVersions = Deencapsulation.invoke(new PartitionsProcDir(db, table, false),
                    "getPartitionVersions", table, Collections.singletonList(partition.getId()));
            Mockito.verify(synchronizer).pushVersionAsync(Mockito.eq(db.getId()), Mockito.anyList(), Mockito.anyMap());
            Assertions.assertEquals(1, partitionRequests.get());
            Assertions.assertEquals(101, pushedTableVersion.get());
            Assertions.assertEquals(13, pushedPartitionVersion.get(),
                    "Forced sync must not push table version 101 with partition version 12 from a pending transaction");
            Assertions.assertFalse(pendingAtPush.get(), "The pending transaction must be visible before the push");
            Assertions.assertEquals(Collections.singletonList(13L), shownVersions);
            // Cache-only observation: do not run the daemon or query getters that could repair the stale version.
            Assertions.assertEquals(13, partition.getCachedVisibleVersion());
        } finally {
            if (previousContext == null) {
                ConnectContext.remove();
            } else {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void testFollowerMustRecoverMissedPushAfterLaterPartitionPush() throws Exception {
        CloudPartition otherPartition = CloudPartitionTest.createPartition(5, db.getId(), table.getId());
        otherPartition.setCachedVisibleVersion(8, 1000);
        table.addPartition(otherPartition);
        // Start with a fully synchronized follower, not an already incomplete daemon sync.
        table.setSyncedTableVersion(100);
        Assertions.assertFalse(table.isTableVersionSyncNeeded());

        AtomicInteger tableRequests = new AtomicInteger();
        AtomicInteger partitionRequests = new AtomicInteger();
        Mockito.when(proxy.getVisibleVersionAsync(Mockito.any(Cloud.GetVersionRequest.class)))
                .thenAnswer(invocation -> {
                    Cloud.GetVersionRequest request = invocation.getArgument(0);
                    Assertions.assertEquals(Collections.singletonList(db.getId()), request.getDbIdsList());
                    Assertions.assertEquals(Collections.singletonList(table.getId()), request.getTableIdsList());
                    Cloud.GetVersionResponse.Builder response = Cloud.GetVersionResponse.newBuilder()
                            .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(Cloud.MetaServiceCode.OK));
                    if (request.getIsTableVersion()) {
                        tableRequests.incrementAndGet();
                        response.addVersions(102);
                    } else {
                        partitionRequests.incrementAndGet();
                        Assertions.assertTrue(request.getWaitForPendingTxn());
                        Assertions.assertEquals(1, request.getPartitionIdsCount());
                        if (request.getPartitionIds(0) == partition.getId()) {
                            response.addVersions(13);
                        } else {
                            Assertions.assertEquals(otherPartition.getId(), request.getPartitionIds(0));
                            response.addVersions(9);
                        }
                        response.addVersionUpdateTimeMs(2000);
                    }
                    return CompletableFuture.completedFuture(response.build());
                });

        // Master commits A (P1=13, T=101), but its push never reaches this follower.
        // Master then commits B (P2=9, T=102); only B's push is delivered.
        TFrontendSyncCloudVersionRequest push = new TFrontendSyncCloudVersionRequest()
                .setDbId(db.getId())
                .setTableVersionInfos(Collections.singletonList(new TCloudVersionInfo()
                        .setTableId(table.getId()).setVersion(102)))
                .setPartitionVersionInfos(Collections.singletonList(new TCloudVersionInfo()
                        .setTableId(table.getId()).setPartitionId(otherPartition.getId())
                        .setVersion(9).setVersionUpdateTime(2000)));
        // Run the actual follower worker synchronously so the push finishes before the daemon starts.
        Deencapsulation.invoke(new CloudFEVersionSynchronizer(), "syncVersion", db, push);
        Assertions.assertEquals(12, partition.getCachedVisibleVersion());
        Assertions.assertEquals(9, otherPartition.getCachedVisibleVersion());
        Assertions.assertEquals(102, table.getCachedTableVersion());
        Assertions.assertEquals(0, tableRequests.get(), "No daemon sync has started before the later push");

        // The fixture uses a zero polling interval: both cycles must check MS despite a fresh table cache.
        daemon.runAfterCatalogReady();
        daemon.runAfterCatalogReady();
        Assertions.assertEquals(3, tableRequests.get());
        // Inspect only the cache; a query or an MS getter could repair the missed version itself.
        Assertions.assertEquals(13, partition.getCachedVisibleVersion(),
                "A later push must not hide P1's missed update when MS and cached table versions are both 102");
        Assertions.assertEquals(9, otherPartition.getCachedVisibleVersion());
        Assertions.assertEquals(102, table.getCachedTableVersion());
        Assertions.assertEquals(2, partitionRequests.get(), "Sync both partitions once, then skip the clean table");
        Assertions.assertFalse(table.isTableVersionSyncNeeded());
    }

    @Test
    public void testQuerySnapshotMustNotMixVersionsDuringDaemonRefresh() throws Exception {
        assertQuerySnapshotDuringDaemonRefresh(2);
    }

    @Test
    public void testQuerySnapshotMustNotMixVersionsAcrossDaemonBatches() throws Exception {
        assertQuerySnapshotDuringDaemonRefresh(1);
    }

    private void assertQuerySnapshotDuringDaemonRefresh(int batchSize) throws Exception {
        CloudPartition firstPartition = Mockito.spy(partition);
        CloudPartition otherPartition = Mockito.spy(
                CloudPartitionTest.createPartition(5, db.getId(), table.getId()));
        otherPartition.setCachedVisibleVersion(12, 1000);
        table.addPartition(firstPartition);
        table.addPartition(otherPartition);
        table.setSyncedTableVersion(100);
        Config.cloud_get_version_task_batch_size = batchSize;
        List<CloudPartition> partitions = List.of(firstPartition, otherPartition);

        CountDownLatch firstVersionRead = new CountDownLatch(1);
        CountDownLatch resumeQuery = new CountDownLatch(1);
        ReentrantReadWriteLock versionLock = new ReentrantReadWriteLock(true);
        ReentrantReadWriteLock.WriteLock writeLock = Mockito.spy(versionLock.writeLock());
        ReadWriteLock observedLock = Mockito.mock(ReadWriteLock.class);
        Mockito.when(observedLock.readLock()).thenReturn(versionLock.readLock());
        Mockito.when(observedLock.writeLock()).thenReturn(writeLock);
        Deencapsulation.setField(table, "versionLock", observedLock);
        Mockito.doAnswer(invocation -> {
            // A writer respecting the query's read lock must let the query finish before applying its batch.
            resumeQuery.countDown();
            return invocation.callRealMethod();
        }).when(writeLock).lock();

        AtomicInteger cacheWrites = new AtomicInteger();
        for (CloudPartition cachedPartition : partitions) {
            Mockito.doAnswer(invocation -> {
                invocation.callRealMethod();
                if (cacheWrites.incrementAndGet() == partitions.size()) {
                    // The current daemon bypasses the write lock: resume after both caches have changed.
                    resumeQuery.countDown();
                }
                return null;
            }).when(cachedPartition).setCachedVisibleVersion(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());
        }
        Mockito.doAnswer(invocation -> {
            long version = (long) invocation.callRealMethod();
            Assertions.assertEquals(1, versionLock.getReadHoldCount(), "The query must hold the table version lock");
            firstVersionRead.countDown();
            Assertions.assertTrue(resumeQuery.await(10, TimeUnit.SECONDS),
                    "The daemon did not reach cache publication");
            return version;
        }).when(firstPartition).getCachedVisibleVersion();

        AtomicInteger partitionRequests = new AtomicInteger();
        Mockito.when(proxy.getVisibleVersionAsync(Mockito.any(Cloud.GetVersionRequest.class)))
                .thenAnswer(invocation -> {
                    Cloud.GetVersionRequest request = invocation.getArgument(0);
                    Cloud.GetVersionResponse.Builder response = Cloud.GetVersionResponse.newBuilder()
                            .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(Cloud.MetaServiceCode.OK));
                    if (request.getIsTableVersion()) {
                        response.addVersions(101);
                    } else {
                        partitionRequests.incrementAndGet();
                        Assertions.assertTrue(request.getWaitForPendingTxn());
                        Assertions.assertEquals(batchSize, request.getPartitionIdsCount());
                        // One committed transaction advanced both partitions, possibly fetched in separate batches.
                        for (long partitionId : request.getPartitionIdsList()) {
                            Assertions.assertTrue(partitionId == firstPartition.getId()
                                    || partitionId == otherPartition.getId());
                            response.addVersions(13).addVersionUpdateTimeMs(2000);
                        }
                    }
                    return CompletableFuture.completedFuture(response.build());
                });

        InternalCatalog catalog = Env.getCurrentInternalCatalog();
        ExecutorService queryExecutor = Executors.newSingleThreadExecutor();
        try {
            Future<List<Long>> querySnapshot = queryExecutor.submit(() -> {
                // Install thread-local mocks on the query thread, just as on the daemon coordinator thread.
                try (MockedStatic<Env> queryEnv = Mockito.mockStatic(Env.class);
                        MockedStatic<VariableMgr> queryVariables = Mockito.mockStatic(VariableMgr.class)) {
                    queryEnv.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
                    queryVariables.when(VariableMgr::getDefaultSessionVariable).thenReturn(variables);
                    return CloudPartition.getSnapshotVisibleVersion(partitions);
                }
            });
            Assertions.assertTrue(firstVersionRead.await(10, TimeUnit.SECONDS), "The query did not read P1");
            daemon.runAfterCatalogReady();
            List<Long> snapshot = querySnapshot.get(10, TimeUnit.SECONDS);
            Assertions.assertEquals(2 / batchSize, partitionRequests.get(),
                    "The query must not refresh either cached version");
            Assertions.assertEquals(2, cacheWrites.get());
            Assertions.assertEquals(101, table.getCachedTableVersion());
            Assertions.assertEquals(List.of(12L, 12L), snapshot,
                    "A query holding the version read lock must not see P1 before and P2 after the same transaction");
        } finally {
            resumeQuery.countDown();
            queryExecutor.shutdownNow();
            Assertions.assertTrue(queryExecutor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testRetryFailedBatchDespiteConcurrentTableCacheRefresh() throws Exception {
        assertRetryFailedBatchAfterReenabling(false);
    }

    @Test
    public void testRetryFailedBatchAfterSwitchingToFiniteTtl() throws Exception {
        assertRetryFailedBatchAfterReenabling(true);
    }

    private void assertRetryFailedBatchAfterReenabling(boolean finiteTtl) throws Exception {
        CloudPartition otherPartition = CloudPartitionTest.createPartition(5, db.getId(), table.getId());
        otherPartition.setCachedVisibleVersion(7, 1000);
        table.addPartition(otherPartition);
        AtomicBoolean timeout = new AtomicBoolean(true);
        AtomicLong msTableVersion = new AtomicLong(101);
        AtomicInteger tableRequests = new AtomicInteger();
        AtomicInteger partitionRequests = new AtomicInteger();
        Future<Cloud.GetVersionResponse> timedOut = Mockito.mock(Future.class);
        Mockito.when(timedOut.get(Mockito.anyLong(), Mockito.any(TimeUnit.class)))
                .thenThrow(new TimeoutException("pending transaction did not become visible"));
        Mockito.when(proxy.getVisibleVersionAsync(Mockito.any(Cloud.GetVersionRequest.class)))
                .thenAnswer(invocation -> {
                    Cloud.GetVersionRequest request = invocation.getArgument(0);
                    Cloud.GetVersionResponse.Builder response = Cloud.GetVersionResponse.newBuilder()
                            .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(Cloud.MetaServiceCode.OK));
                    if (request.getIsTableVersion()) {
                        tableRequests.incrementAndGet();
                        response.addVersions(msTableVersion.get());
                    } else {
                        partitionRequests.incrementAndGet();
                        Assertions.assertTrue(request.getWaitForPendingTxn());
                        Assertions.assertEquals(1, request.getPartitionIdsCount());
                        if (request.getPartitionIds(0) == partition.getId()) {
                            if (timeout.get()) {
                                return timedOut;
                            }
                            response.addVersions(13);
                        } else {
                            Assertions.assertEquals(otherPartition.getId(), request.getPartitionIds(0));
                            response.addVersions(timeout.get() ? 8 : 9);
                        }
                    }
                    return CompletableFuture.completedFuture(response.build());
                });

        daemon.runAfterCatalogReady();
        Assertions.assertEquals(2, partitionRequests.get());
        Assertions.assertEquals(12, partition.getCachedVisibleVersion());
        Assertions.assertEquals(0,
                (long) Deencapsulation.getField(partition, "lastVersionCachedTimeMs"));
        Assertions.assertTrue(partition.isCachedVersionExpired());
        Assertions.assertEquals(7, otherPartition.getCachedVisibleVersion(), "Do not publish a successful batch alone");
        Assertions.assertEquals(0,
                (long) Deencapsulation.getField(otherPartition, "lastVersionCachedTimeMs"));
        Assertions.assertEquals(100, table.getCachedTableVersion(), "A failed batch must not advance the table cache");
        Assertions.assertEquals(0,
                (long) Deencapsulation.getField(table, "lastTableVersionCachedTimeMs"));

        // Disabling the daemon must not prevent ordinary updates from restoring TTL caching.
        Config.cloud_enable_version_syncer = false;
        if (finiteTtl) {
            variables.cloudPartitionVersionCacheTtlMs = 60000;
            variables.cloudTableVersionCacheTtlMs = 60000;
        }
        // Another commit refreshes the other partition and table cache while the failed batch remains stale.
        otherPartition.setCachedVisibleVersion(9, 2000);
        msTableVersion.set(102);
        table.setCachedTableVersion(102);
        Config.cloud_version_syncer_interval_second = 3600;
        Assertions.assertTrue(table.isTableVersionSyncNeeded());
        Assertions.assertFalse(table.isCachedTableVersionExpired(variables.cloudTableVersionCacheTtlMs));
        daemon.runAfterCatalogReady();
        Assertions.assertEquals(1, tableRequests.get(), "A disabled daemon must not issue RPCs");
        Assertions.assertEquals(2, partitionRequests.get());
        Assertions.assertTrue(table.isTableVersionSyncNeeded());

        timeout.set(false);
        Config.cloud_enable_version_syncer = true;
        daemon.runAfterCatalogReady();
        Assertions.assertEquals(3, tableRequests.get(), "Retry and validate despite the refreshed table cache's TTL");
        Assertions.assertEquals(4, partitionRequests.get(),
                "Retry all batches even when MS table version equals the refreshed cache");
        Assertions.assertEquals(13, partition.getCachedVisibleVersion());
        Assertions.assertEquals(9, otherPartition.getCachedVisibleVersion());
        Assertions.assertEquals(102, table.getCachedTableVersion());
        Assertions.assertFalse(table.isTableVersionSyncNeeded());

        if (finiteTtl) {
            // Even an expired daemon polling interval must not trigger proactive sync for a clean finite-TTL table.
            Config.cloud_version_syncer_interval_second = 0;
        }
        daemon.runAfterCatalogReady();
        Assertions.assertEquals(3, tableRequests.get(), "Clear the retry marker only after all batches succeed");
        Assertions.assertEquals(4, partitionRequests.get());
    }

    @Test
    public void testDiscardBatchesWhenMsVersionChangesWithoutPush() throws Exception {
        assertRejectedPartitionSnapshot(false);
    }

    @Test
    public void testDiscardBatchesWhenTableVersionValidationFails() throws Exception {
        assertRejectedPartitionSnapshot(true);
    }

    private void assertRejectedPartitionSnapshot(boolean failValidation) throws Exception {
        CloudPartition otherPartition = CloudPartitionTest.createPartition(5, db.getId(), table.getId());
        otherPartition.setCachedVisibleVersion(12, 1000);
        table.addPartition(otherPartition);
        table.setSyncedTableVersion(100);
        AtomicLong msTableVersion = new AtomicLong(101);
        AtomicInteger tableRequests = new AtomicInteger();
        AtomicInteger partitionRequests = new AtomicInteger();
        Mockito.when(proxy.getVisibleVersionAsync(Mockito.any(Cloud.GetVersionRequest.class)))
                .thenAnswer(invocation -> {
                    Cloud.GetVersionRequest request = invocation.getArgument(0);
                    Cloud.GetVersionResponse.Builder response = Cloud.GetVersionResponse.newBuilder()
                            .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(Cloud.MetaServiceCode.OK));
                    if (request.getIsTableVersion()) {
                        if (tableRequests.incrementAndGet() == 2 && failValidation) {
                            return CompletableFuture.failedFuture(new TimeoutException("table validation timed out"));
                        }
                        response.addVersions(msTableVersion.get());
                    } else {
                        Assertions.assertTrue(request.getWaitForPendingTxn());
                        Assertions.assertEquals(1, request.getPartitionIdsCount());
                        Assertions.assertTrue(request.getPartitionIds(0) == partition.getId()
                                || request.getPartitionIds(0) == otherPartition.getId());
                        if (partitionRequests.incrementAndGet() == 1 && !failValidation) {
                            // A commit occurs between partition snapshots, without any push updating the FE cache.
                            response.addVersions(12);
                            msTableVersion.set(102);
                        } else {
                            response.addVersions(13);
                        }
                        response.addVersionUpdateTimeMs(2000).addCommitTsos(3000);
                    }
                    return CompletableFuture.completedFuture(response.build());
                });

        daemon.runAfterCatalogReady();
        Assertions.assertEquals(2, tableRequests.get());
        Assertions.assertEquals(2, partitionRequests.get());
        Assertions.assertEquals(12, partition.getCachedVisibleVersion());
        Assertions.assertEquals(12, otherPartition.getCachedVisibleVersion());
        Assertions.assertEquals(100, table.getCachedTableVersion(), "Do not confirm an unvalidated snapshot");
        Assertions.assertTrue(table.isTableVersionSyncNeeded());
        Assertions.assertEquals(0, (long) Deencapsulation.getField(partition, "lastVersionCachedTimeMs"));
        Assertions.assertEquals(0, (long) Deencapsulation.getField(otherPartition, "lastVersionCachedTimeMs"));

        daemon.runAfterCatalogReady();
        Assertions.assertEquals(4, tableRequests.get());
        Assertions.assertEquals(4, partitionRequests.get());
        Assertions.assertEquals(msTableVersion.get(), table.getCachedTableVersion());
        Assertions.assertFalse(table.isTableVersionSyncNeeded());
        for (CloudPartition cachedPartition : List.of(partition, otherPartition)) {
            Assertions.assertEquals(13, cachedPartition.getCachedVisibleVersion());
            Assertions.assertEquals(2000, cachedPartition.getVisibleVersionTime());
            Assertions.assertEquals(3000L, cachedPartition.getTso().longValue());
        }
    }

    @Test
    public void testSyncDiscardsVersionsWhenCacheAdvancesAfterValidationSnapshot() throws Exception {
        AtomicLong msTableVersion = new AtomicLong(101);
        AtomicInteger tableRequests = new AtomicInteger();
        AtomicInteger partitionRequests = new AtomicInteger();
        Mockito.when(proxy.getVisibleVersionAsync(Mockito.any(Cloud.GetVersionRequest.class)))
                .thenAnswer(invocation -> {
                    Cloud.GetVersionRequest request = invocation.getArgument(0);
                    Cloud.GetVersionResponse.Builder response = Cloud.GetVersionResponse.newBuilder()
                            .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(Cloud.MetaServiceCode.OK));
                    if (request.getIsTableVersion()) {
                        response.addVersions(msTableVersion.get());
                        if (tableRequests.incrementAndGet() == 2) {
                            // This validation response captured 101, then another path cached table version 102.
                            // Check the local version again under the publication lock before applying partition 13.
                            msTableVersion.set(102);
                            table.setCachedTableVersion(102);
                        }
                    } else {
                        Assertions.assertTrue(request.getWaitForPendingTxn());
                        if (partitionRequests.incrementAndGet() == 1) {
                            response.addVersions(13);
                        } else {
                            response.addVersions(14);
                        }
                    }
                    return CompletableFuture.completedFuture(response.build());
                });

        daemon.runAfterCatalogReady();
        Assertions.assertEquals(12, partition.getCachedVisibleVersion(), "Discard the stale MS snapshot");
        Assertions.assertEquals(102, table.getCachedTableVersion());
        Assertions.assertTrue(table.isTableVersionSyncNeeded());

        Config.cloud_version_syncer_interval_second = 3600;
        daemon.runAfterCatalogReady();
        Assertions.assertEquals(4, tableRequests.get());
        Assertions.assertEquals(2, partitionRequests.get());
        Assertions.assertEquals(14, partition.getCachedVisibleVersion());
        Assertions.assertEquals(102, table.getCachedTableVersion());
        Assertions.assertFalse(table.isTableVersionSyncNeeded());

        daemon.runAfterCatalogReady();
        Assertions.assertEquals(4, tableRequests.get());
        Assertions.assertEquals(2, partitionRequests.get());
    }
}
