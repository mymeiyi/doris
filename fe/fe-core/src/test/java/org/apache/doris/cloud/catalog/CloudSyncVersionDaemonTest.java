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
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
        Assertions.assertEquals(1, tableRequests.get());
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
        Assertions.assertEquals(2, tableRequests.get());
        Assertions.assertEquals(1, partitionRequests.get(), "A successful sync does not need another partition RPC");
        Assertions.assertEquals(101, table.getCachedTableVersion());
        Assertions.assertEquals(13, partition.getCachedVisibleVersion());
    }

    @Test
    public void testFollowerMustRecoverMissedPushAfterLaterPartitionPush() throws Exception {
        CloudPartition otherPartition = CloudPartitionTest.createPartition(5, db.getId(), table.getId());
        otherPartition.setCachedVisibleVersion(8, 1000);
        table.addPartition(otherPartition);
        // Start with a fully synchronized follower, not an already incomplete daemon sync.
        table.setSyncedTableVersion(100);
        Assertions.assertFalse(table.isTableVersionSyncNeeded());
        Deencapsulation.setField(table, "versionLock", new ReentrantReadWriteLock(true));

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
        Assertions.assertEquals(2, tableRequests.get());
        // Inspect only the cache; a query or an MS getter could repair the missed version itself.
        Assertions.assertEquals(13, partition.getCachedVisibleVersion(),
                "A later push must not hide P1's missed update when MS and cached table versions are both 102");
        Assertions.assertEquals(9, otherPartition.getCachedVisibleVersion());
        Assertions.assertEquals(102, table.getCachedTableVersion());
        Assertions.assertEquals(2, partitionRequests.get(), "Sync both partitions once, then skip the clean table");
        Assertions.assertFalse(table.isTableVersionSyncNeeded());
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
        Assertions.assertEquals(8, otherPartition.getCachedVisibleVersion(), "Keep the successful batch's version");
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
        Assertions.assertEquals(2, tableRequests.get(), "An incomplete sync must bypass the table cache's TTL");
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
        Assertions.assertEquals(2, tableRequests.get(), "Clear the retry marker only after all batches succeed");
        Assertions.assertEquals(4, partitionRequests.get());
    }

    @Test
    public void testSuccessfulSyncDoesNotClearNewerTableVersion() throws Exception {
        AtomicLong msTableVersion = new AtomicLong(101);
        AtomicInteger tableRequests = new AtomicInteger();
        AtomicInteger partitionRequests = new AtomicInteger();
        Mockito.when(proxy.getVisibleVersionAsync(Mockito.any(Cloud.GetVersionRequest.class)))
                .thenAnswer(invocation -> {
                    Cloud.GetVersionRequest request = invocation.getArgument(0);
                    Cloud.GetVersionResponse.Builder response = Cloud.GetVersionResponse.newBuilder()
                            .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(Cloud.MetaServiceCode.OK));
                    if (request.getIsTableVersion()) {
                        tableRequests.incrementAndGet();
                        response.addVersions(msTableVersion.get());
                    } else {
                        Assertions.assertTrue(request.getWaitForPendingTxn());
                        if (partitionRequests.incrementAndGet() == 1) {
                            // This response captured partition 13 before another path cached table version 102.
                            // The daemon must not certify version 102 using its version 101 sync result.
                            response.addVersions(13);
                            msTableVersion.set(102);
                            table.setCachedTableVersion(102);
                        } else {
                            response.addVersions(14);
                        }
                    }
                    return CompletableFuture.completedFuture(response.build());
                });

        daemon.runAfterCatalogReady();
        Assertions.assertEquals(13, partition.getCachedVisibleVersion());
        Assertions.assertEquals(102, table.getCachedTableVersion());
        Assertions.assertTrue(table.isTableVersionSyncNeeded());

        Config.cloud_version_syncer_interval_second = 3600;
        daemon.runAfterCatalogReady();
        Assertions.assertEquals(2, tableRequests.get());
        Assertions.assertEquals(2, partitionRequests.get());
        Assertions.assertEquals(14, partition.getCachedVisibleVersion());
        Assertions.assertEquals(102, table.getCachedTableVersion());
        Assertions.assertFalse(table.isTableVersionSyncNeeded());

        daemon.runAfterCatalogReady();
        Assertions.assertEquals(2, tableRequests.get());
        Assertions.assertEquals(2, partitionRequests.get());
    }
}
