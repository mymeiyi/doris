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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.qe.VariableMgr;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class CloudSyncVersionDaemon extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudSyncVersionDaemon.class);
    private static final ExecutorService GET_VERSION_THREAD_POOL = Executors.newFixedThreadPool(
            Config.cloud_get_version_task_threads_num,
            new ThreadFactoryBuilder().setNameFormat("get-version-%d").setDaemon(true).build());

    public CloudSyncVersionDaemon() {
        super("cloud table and partition version checker",
                Config.cloud_version_syncer_interval_second * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!Config.cloud_enable_version_syncer) {
            return;
        }
        LOG.info("begin sync cloud table and partition version");
        Map<OlapTable, Long> tableVersionMap = syncTableVersions();
        if (!tableVersionMap.isEmpty()) {
            syncPartitionVersion(tableVersionMap);
        }
    }

    private Map<OlapTable, Long> syncTableVersions() {
        Map<OlapTable, Long> tableVersionMap = new ConcurrentHashMap<>();
        List<Future<Void>> futures = new ArrayList<>();
        long start = System.currentTimeMillis();
        List<Long> dbIds = new ArrayList<>();
        List<Long> tableIds = new ArrayList<>();
        List<OlapTable> tables = new ArrayList<>();
        // Finite TTLs refresh lazily on reads, but incomplete syncs must still be retried
        // after changing TTLs or re-enabling the daemon.
        boolean syncExpiredTables = VariableMgr.getDefaultSessionVariable().cloudPartitionVersionCacheTtlMs
                == Long.MAX_VALUE
                || VariableMgr.getDefaultSessionVariable().cloudTableVersionCacheTtlMs == Long.MAX_VALUE;
        // TODO meta service support range scan all table versions
        for (Database db : Env.getCurrentInternalCatalog().getDbs()) {
            List<Table> tableList = db.getTables();
            for (Table table : tableList) {
                if (!table.isManagedTable()) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) table;
                if (!olapTable.isTableVersionSyncNeeded() && (!syncExpiredTables
                        || !olapTable.isCachedTableVersionExpired(
                                Config.cloud_version_syncer_interval_second * 1000))) {
                    continue;
                }
                dbIds.add(db.getId());
                tableIds.add(olapTable.getId());
                tables.add(olapTable);
                if (dbIds.size() >= Config.cloud_get_version_task_batch_size) {
                    Future<Void> future = submitGetTableVersionTask(tableVersionMap, ImmutableList.copyOf(dbIds),
                            ImmutableList.copyOf(tableIds), ImmutableList.copyOf(tables));
                    futures.add(future);
                    dbIds.clear();
                    tableIds.clear();
                    tables.clear();
                }
            }
        }
        if (!dbIds.isEmpty()) {
            Future<Void> future = submitGetTableVersionTask(tableVersionMap, ImmutableList.copyOf(dbIds),
                    ImmutableList.copyOf(tableIds), ImmutableList.copyOf(tables));
            futures.add(future);
            dbIds.clear();
            tableIds.clear();
            tables.clear();
        }
        try {
            for (Future<Void> future : futures) {
                future.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while waiting for get table version tasks to complete", e);
            return Collections.emptyMap();
        } catch (ExecutionException e) {
            LOG.error("Error waiting for get table version tasks to complete", e);
            return Collections.emptyMap();
        }
        LOG.info("sync table version cost {} ms, rpc size: {}, found {} tables need to sync partition version",
                System.currentTimeMillis() - start, futures.size(), tableVersionMap.size());
        return tableVersionMap;
    }

    private Future<Void> submitGetTableVersionTask(Map<OlapTable, Long> tableVersionMap, List<Long> dbIds,
            List<Long> tableIds, List<OlapTable> tables) {
        return GET_VERSION_THREAD_POOL.submit(() -> {
            try {
                List<Long> versions = OlapTable.getVisibleVersionFromMeta(
                        dbIds, tableIds, Config.cloud_version_syncer_get_version_retry_times);
                for (int i = 0; i < tables.size(); i++) {
                    OlapTable table = tables.get(i);
                    long version = versions.get(i);
                    if (table.isTableVersionSyncNeeded() || version > table.getCachedTableVersion()) {
                        tableVersionMap.compute(table, (k, v) -> {
                            if (v == null || version > v) {
                                return version;
                            } else {
                                return v;
                            }
                        });
                    } else {
                        // update lastTableVersionCachedTimeMs
                        table.setCachedTableVersion(version);
                    }
                }
            } catch (Exception e) {
                LOG.warn("get table version error", e);
            }
            return null;
        });
    }

    private void syncPartitionVersion(Map<OlapTable, Long> tableVersionMap) {
        // Keep retrying until every partition batch has completed, even if a concurrent
        // commit or table-version read refreshes the cache during this sync.
        tableVersionMap.keySet().forEach(OlapTable::invalidateCachedTableVersion);
        Set<Long> failedTables = ConcurrentHashMap.newKeySet();
        Map<Long, CloudPartition.PartitionVersion> versions = new ConcurrentHashMap<>();
        Map<Long, List<CloudPartition>> tablePartitions = new HashMap<>();
        List<Future<Void>> futures = new ArrayList<>();
        long start = System.currentTimeMillis();
        List<CloudPartition> partitions = new ArrayList<>();
        // TODO meta service support range scan partition versions
        for (Entry<OlapTable, Long> entry : tableVersionMap.entrySet()) {
            OlapTable olapTable = entry.getKey();
            LOG.info("sync partition version for db: {}, table: {}, table cache version: {}, new version: {}",
                    olapTable.getDatabase().getId(), olapTable, olapTable.getCachedTableVersion(), entry.getValue());
            List<CloudPartition> currentPartitions;
            olapTable.readLock();
            try {
                currentPartitions = olapTable.getAllPartitions().stream().map(p -> (CloudPartition) p)
                        .collect(Collectors.toList());
            } finally {
                olapTable.readUnlock();
            }
            tablePartitions.put(olapTable.getId(), currentPartitions);
            for (CloudPartition partition : currentPartitions) {
                partitions.add(partition);
                if (partitions.size() >= Config.cloud_get_version_task_batch_size) {
                    Future<Void> future = submitGetPartitionVersionTask(
                            failedTables, versions, ImmutableList.copyOf(partitions));
                    futures.add(future);
                    partitions.clear();
                }
            }
        }
        if (partitions.size() > 0) {
            Future<Void> future = submitGetPartitionVersionTask(
                    failedTables, versions, ImmutableList.copyOf(partitions));
            futures.add(future);
            partitions.clear();
        }
        try {
            for (Future<Void> future : futures) {
                future.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while waiting for get partition version tasks to complete", e);
            return;
        } catch (ExecutionException e) {
            LOG.error("Error waiting for get partition version tasks to complete", e);
            return;
        }
        // Separate RPC batches can straddle a commit. Publish only if the MS table version stayed unchanged.
        validateTableVersions(tableVersionMap, failedTables);
        for (Entry<OlapTable, Long> entry : tableVersionMap.entrySet()) {
            OlapTable olapTable = entry.getKey();
            olapTable.readLock();
            try {
                olapTable.versionWriteLock();
                try {
                    if (olapTable.getCachedTableVersion() > entry.getValue()) {
                        failedTables.add(olapTable.getId());
                    }
                    if (failedTables.contains(olapTable.getId())) {
                        // Invalidate together so a query cannot combine a failed batch with a successful one.
                        for (Partition partition : olapTable.getAllPartitions()) {
                            ((CloudPartition) partition).invalidateCachedVisibleVersion();
                        }
                        continue;
                    }
                    for (CloudPartition partition : tablePartitions.get(olapTable.getId())) {
                        versions.get(partition.getId()).updateCache(partition);
                    }
                    olapTable.setSyncedTableVersion(entry.getValue());
                } finally {
                    olapTable.versionWriteUnlock();
                }
            } finally {
                olapTable.readUnlock();
            }
        }
        LOG.info("sync partition version cost {} ms, table size: {}, rpc size: {}, failed tables: {}",
                System.currentTimeMillis() - start, tableVersionMap.size(), futures.size(), failedTables);
    }

    private void validateTableVersions(Map<OlapTable, Long> tableVersionMap, Set<Long> failedTables) {
        List<OlapTable> tables = tableVersionMap.keySet().stream()
                .filter(table -> !failedTables.contains(table.getId())).collect(Collectors.toList());
        for (List<OlapTable> batch : Lists.partition(tables, Config.cloud_get_version_task_batch_size)) {
            List<Long> dbIds = batch.stream().map(table -> table.getDatabase().getId()).collect(Collectors.toList());
            List<Long> tableIds = batch.stream().map(OlapTable::getId).collect(Collectors.toList());
            try {
                List<Long> versions = OlapTable.getVisibleVersionFromMeta(
                        dbIds, tableIds, Config.cloud_version_syncer_get_version_retry_times);
                for (int i = 0; i < batch.size(); i++) {
                    long expected = tableVersionMap.get(batch.get(i));
                    if (versions.get(i) != expected) {
                        failedTables.add(tableIds.get(i));
                        LOG.info("table version changed during partition sync, table: {}, before: {}, after: {}",
                                tableIds.get(i), expected, versions.get(i));
                    }
                }
            } catch (Exception e) {
                failedTables.addAll(tableIds);
                LOG.warn("failed to validate table versions after partition sync, tables: {}", tableIds, e);
            }
        }
    }

    private Future<Void> submitGetPartitionVersionTask(Set<Long> failedTables,
            Map<Long, CloudPartition.PartitionVersion> versions, List<CloudPartition> partitions) {
        return GET_VERSION_THREAD_POOL.submit(() -> {
            try {
                // Lazy commit advances the MS table version before partition versions become visible.
                // Do not mark that table version synchronized until its pending transactions have finished.
                List<CloudPartition.PartitionVersion> snapshots =
                        CloudPartition.getSnapshotVisibleVersionFromMsWithoutCache(
                                partitions, true, Config.cloud_version_syncer_get_version_retry_times);
                for (int i = 0; i < partitions.size(); i++) {
                    versions.put(partitions.get(i).getId(), snapshots.get(i));
                }
            } catch (Exception e) {
                LOG.warn("get partition version error", e);
                Set<Long> failedTableIds = partitions.stream().map(p -> p.getTableId())
                        .collect(Collectors.toSet());
                failedTables.addAll(failedTableIds);
            }
            return null;
        });
    }
}
