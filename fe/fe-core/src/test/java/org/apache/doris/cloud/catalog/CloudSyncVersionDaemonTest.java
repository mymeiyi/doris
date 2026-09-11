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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class CloudSyncVersionDaemonTest {
    @Test
    public void testLazyCommitMustNotLeavePartitionCacheBehindTableVersion() throws Exception {
        Database db = new Database(1, "lazy_commit_db");
        OlapTable table = new OlapTable(2, "lazy_commit_table", Collections.emptyList(), KeysType.DUP_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(1));
        CloudPartition partition = CloudPartitionTest.createPartition(3, db.getId(), table.getId());
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

        // Lazy commit phase one has advanced the MS table version to 101, but partition 13 is still pending.
        AtomicBoolean pendingTxn = new AtomicBoolean(true);
        AtomicInteger tableRequests = new AtomicInteger();
        AtomicInteger partitionRequests = new AtomicInteger();
        MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
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
                        // A non-waiting read can return 12 while table version 101 is already observable.
                        if (request.getWaitForPendingTxn()) {
                            pendingTxn.set(false);
                        }
                        response.addVersions(pendingTxn.get() ? 12 : 13).addVersionUpdateTimeMs(1000);
                    }
                    return CompletableFuture.completedFuture(response.build());
                });

        // Mockito static mocks are thread-local. Replace the proxy pool so real daemon workers use this RPC stub.
        Class<?> holder = Class.forName(MetaServiceProxy.class.getName() + "$SingletonHolder");
        Field proxies = holder.getDeclaredField("proxies");
        proxies.setAccessible(true);
        MetaServiceProxy[] originalProxies = (MetaServiceProxy[]) proxies.get(null);
        boolean originalEnableSyncer = Config.cloud_enable_version_syncer;
        int originalSyncInterval = Config.cloud_version_syncer_interval_second;
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            proxies.set(null, new MetaServiceProxy[] {proxy});
            Config.cloud_enable_version_syncer = true;
            // Force every cycle to check MS table version, without sleeps or dependence on cache timestamps.
            Config.cloud_version_syncer_interval_second = 0;
            CloudSyncVersionDaemon daemon = new CloudSyncVersionDaemon();
            daemon.runAfterCatalogReady();
            Assertions.assertEquals(1, tableRequests.get());
            Assertions.assertEquals(1, partitionRequests.get());
            Assertions.assertEquals(101, table.getCachedTableVersion());

            // MS completes lazy commit after that sync. The original commit response was lost;
            // its retry returns only VISIBLE txn_info, with no partition versions or table_stats.
            // Inject that response into the real commit-response cache handler, without a wall-clock timeout.
            pendingTxn.set(false);
            Cloud.CommitTxnResponse retryResponse = Cloud.CommitTxnResponse.newBuilder()
                    .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(Cloud.MetaServiceCode.OK))
                    .setTxnInfo(Cloud.TxnInfoPB.newBuilder().setDbId(db.getId()).addTableIds(table.getId())
                            .setTxnId(4).setStatus(Cloud.TxnStatusPB.TXN_STATUS_VISIBLE))
                    .build();
            Deencapsulation.invoke(new CloudGlobalTransactionMgr(), "updateVersion", retryResponse);

            daemon.runAfterCatalogReady();
            Assertions.assertEquals(2, tableRequests.get(), "The next sync must actually check MS, bypassing TTL");
            Assertions.assertEquals(101, table.getCachedTableVersion());
            // Intentionally fails on the unfixed code: table 101 is cached, so the daemon skips partitions
            // and the VISIBLE retry also skips the update, leaving partition 12 cached after MS reached 13.
            Assertions.assertEquals(13, partition.getCachedVisibleVersion(),
                    "Lazy commit is visible, but table-version equality suppresses partition cache recovery");
        } finally {
            proxies.set(null, originalProxies);
            Config.cloud_enable_version_syncer = originalEnableSyncer;
            Config.cloud_version_syncer_interval_second = originalSyncInterval;
        }
    }
}
