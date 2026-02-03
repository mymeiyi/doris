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
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CloudTableAndPartitionVersionChecker extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudTableAndPartitionVersionChecker.class);
    private static final ExecutorService GET_VERSION_THREAD_POOL = Executors.newFixedThreadPool(
            Config.cloud_max_get_version_task_threads_num);

    public CloudTableAndPartitionVersionChecker() {
        super("cloud table and partition version checker",
                Config.cloud_table_and_partition_version_checker_interval_second * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        LOG.info("cloud table and partition version checker begin");
        Map<OlapTable, Long> tableVersionMap = checkTableVersions();
        if (!tableVersionMap.isEmpty()) {
            updatePartitionVersion(tableVersionMap);
        }
    }

    private Map<OlapTable, Long> checkTableVersions() {
        Map<OlapTable, Long> tableVersionMap = new ConcurrentHashMap<>();
        List<Future<Void>> futures = new ArrayList<>();
        long start = System.currentTimeMillis();
        List<Long> dbIds = new ArrayList<>();
        List<Long> tableIds = new ArrayList<>();
        List<OlapTable> tables = new ArrayList<>();
        for (Database db : Env.getCurrentInternalCatalog().getDbs()) {
            List<Table> tableList = db.getTables();
            for (Table table : tableList) {
                if (!table.isManagedTable()) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) table;
                if (!olapTable.isCachedTableVersionExpired(
                        Config.cloud_table_and_partition_version_checker_interval_second * 1000)) {
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
            Future<Void> future = submitGetTableVersionTask(tableVersionMap, ImmutableList.copyOf(dbIds), ImmutableList.copyOf(tableIds), ImmutableList.copyOf(tables));
            futures.add(future);
            dbIds.clear();
            tableIds.clear();
            tables.clear();
        }
        try {
            for (Future<Void> future : futures) {
                future.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Error waiting for get table version tasks to complete", e);
        }
        LOG.info("get table versions cost {} ms, tables need to update partition version: {}, rpc size: {}", System.currentTimeMillis() - start,
                tableVersionMap.size(), futures.size());
        return tableVersionMap;
    }

    private Future<Void> submitGetTableVersionTask(Map<OlapTable, Long> tableVersionMap, List<Long> dbIds, List<Long> tableIds, List<OlapTable> tables) {
        return GET_VERSION_THREAD_POOL.submit(() -> {
            try {
                List<Long> versions = OlapTable.getVisibleVersionFromMeta(dbIds, tableIds);
                for (int i = 0; i < tables.size(); i++) {
                    OlapTable table = tables.get(i);
                    long version = versions.get(i);
                    if (version > table.getCachedTableVersion()) {
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
                LOG.warn("get tablet version error", e);
            }
            return null;
        });
    }

    private void updatePartitionVersion(Map<OlapTable, Long> tableVersionMap) {
        Set<Long> failedTables = ConcurrentHashMap.newKeySet();
        List<Future<Void>> futures = new ArrayList<>();
        long start = System.currentTimeMillis();
        List<CloudPartition> partitions = new ArrayList<>();
        // TODO meta service support range scan partition versions
        for (Entry<OlapTable, Long> entry : tableVersionMap.entrySet()) {
            OlapTable olapTable = entry.getKey();
            LOG.info("sout: check partition version for table: {}, db: {}, tableId: {}, dbId: {}, "
                            + "cache version: {}, target version: {}",
                    olapTable.getName(), olapTable.getDatabase().getFullName(), olapTable.getId(),
                    olapTable.getDatabase().getId(), olapTable.getCachedTableVersion(), entry.getValue());
            for (Partition partition : olapTable.getAllPartitions()) {
                partitions.add((CloudPartition) partition);
                if (partitions.size() >= Config.cloud_get_version_task_batch_size) {
                    Future<Void> future = submitGetPartitionVersionTask(failedTables, ImmutableList.copyOf(partitions));
                    futures.add(future);
                    partitions.clear();
                }
            }
        }
        if (partitions.size() > 0) {
            Future<Void> future = submitGetPartitionVersionTask(failedTables, ImmutableList.copyOf(partitions));
            futures.add(future);
            partitions.clear();
        }
        try {
            for (Future<Void> future : futures) {
                future.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Error waiting for get partition version tasks to complete", e);
        }
        // set table version for success tables
        for (Entry<OlapTable, Long> entry : tableVersionMap.entrySet()) {
            if (!failedTables.contains(entry.getKey().getId())) {
                OlapTable olapTable = entry.getKey();
                Long version = entry.getValue();
                olapTable.setCachedTableVersion(version);
            }
        }
        LOG.info("update partition versions cost {} ms, table size: {}, rpc size: {}, failed tables: {}", System.currentTimeMillis() - start,
                tableVersionMap.size(), futures.size(), failedTables);
        tableVersionMap.clear();
        failedTables.clear();
    }

    private Future<Void> submitGetPartitionVersionTask(Set<Long> failedTables, List<CloudPartition> partitions) {
        return GET_VERSION_THREAD_POOL.submit(() -> {
            try {
                CloudPartition.getSnapshotVisibleVersionFromMs(partitions, false);
            } catch (Exception e) {
                LOG.warn("get partition version error", e);
                Set<Long> failedTableIds = partitions.stream().map(p -> p.getTableId())
                        .collect(java.util.stream.Collectors.toSet());
                failedTables.addAll(failedTableIds);
            }
            return null;
        });
    }
}
