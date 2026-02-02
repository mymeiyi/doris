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

package org.apache.doris.catalog;

import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService.HostInfo;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TClodVersionInfo;
import org.apache.doris.thrift.TFrontendUpdateCloudVersionRequest;
import org.apache.doris.thrift.TFrontendUpdateCloudVersionResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import io.jsonwebtoken.lang.Collections;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class CloudTableAndPartitionVersionChecker extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudTableAndPartitionVersionChecker.class);

    private ConcurrentHashMap<OlapTable, Long> tableVersionMap = new ConcurrentHashMap<>();
    private Set<Long> failedTable = ConcurrentHashMap.newKeySet();
    private static final ExecutorService GET_VERSION_THREAD_POOL = Executors.newFixedThreadPool(
            Config.max_get_version_task_threads_num);
    private static final ExecutorService UPDATE_VERSION_THREAD_POOL = Executors.newFixedThreadPool(
            Config.max_get_version_task_threads_num);
    private List<Frontend> frontends = null;

    public CloudTableAndPartitionVersionChecker() {
        super("cloud table and partition version checker",
                Config.tablet_table_and_partition_checker_interval_second * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        LOG.info("cloud table and partition version checker begin");
        getTablesToCheck();
        getPartitionVersion();
    }

    private void getTablesToCheck() {
        List<Future<Void>> futures = new ArrayList<>();
        long start = System.currentTimeMillis();
        List<Long> dbIds = new ArrayList<>();
        List<Long> tableIds = new ArrayList<>();
        List<OlapTable> tables = new ArrayList<>();
        for (Long dbId : Env.getCurrentInternalCatalog().getDbIds()) {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }

            List<Table> tableList = db.getTables();
            for (Table table : tableList) {
                if (!table.isManagedTable()) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) table;
                if (olapTable.isCachedTableVersionExpired(
                        Config.tablet_table_and_partition_checker_interval_second * 1000)) {
                    /*LOG.info(
                            "sout: find one table version expired. cached version: {}, db: {}, table: {}, dbId: {}, tableId: {}",
                            olapTable.getCachedTableVersion(), olapTable.getDatabase().getFullName(),
                            olapTable.getName(), olapTable.getDatabase().getId(), olapTable.getId());*/
                    dbIds.add(dbId);
                    tableIds.add(olapTable.getId());
                    tables.add(olapTable);
                    if (dbIds.size() >= Config.get_version_task_batch_size) {
                        Future<Void> future = submitGetTableVersionTask(Collections.immutable(dbIds),
                                Collections.immutable(tableIds), Collections.immutable(tables));
                        futures.add(future);
                        dbIds.clear();
                        tableIds.clear();
                        tables.clear();
                    }
                }
            }
        }
        if (!dbIds.isEmpty()) {
            Future<Void> future = submitGetTableVersionTask(dbIds, tableIds, tables);
            futures.add(future);
        }
        try {
            for (Future<Void> future : futures) {
                future.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Error waiting for get table version tasks to complete", e);
        }
        LOG.info("get {} tables need to check table version, {} table need to check partition version, cost {} ms",
                tableIds.size(), tableVersionMap.size(), System.currentTimeMillis() - start);
    }

    private Future<Void> submitGetTableVersionTask(List<Long> dbIds, List<Long> tableIds, List<OlapTable> tables) {
        return GET_VERSION_THREAD_POOL.submit(() -> {
            try {
                List<Long> versions = OlapTable.getVisibleVersionFromMeta(dbIds, tableIds);
                for (int i = 0; i < tables.size(); i++) {
                    OlapTable table = tables.get(i);
                    Long version = versions.get(i);
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
                LOG.warn("get tablet version exception:", e);
            }
            return null;
        });
    }

    private void getPartitionVersion() {
        if (tableVersionMap.isEmpty()) {
            return;
        }
        List<Future<Void>> futures = new ArrayList<>();
        long start = System.currentTimeMillis();
        List<CloudPartition> partitions = new ArrayList<>();
        // TODO range scan table partition versions
        for (Entry<OlapTable, Long> entry : tableVersionMap.entrySet()) {
            OlapTable olapTable = entry.getKey();
            LOG.info("sout: check partition version for table: {}, db: {}, tableId: {}, dbId: {}, "
                            + "cache version: {}, target version: {}",
                    olapTable.getName(), olapTable.getDatabase().getFullName(), olapTable.getId(),
                    olapTable.getDatabase().getId(), olapTable.getCachedTableVersion(), entry.getValue());
            for (Partition partition : olapTable.getAllPartitions()) {
                partitions.add((CloudPartition) partition);
                if (partitions.size() > 2000) {
                    Future<Void> future = submitGetTableVersionTask(Collections.immutable(partitions));
                    futures.add(future);
                    partitions.clear();
                }
            }
        }
        if (partitions.size() > 0) {
            Future<Void> future = submitGetTableVersionTask(Collections.immutable(partitions));
            futures.add(future);
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
            if (!failedTable.contains(entry.getKey().getId())) {
                OlapTable olapTable = entry.getKey();
                Long version = entry.getValue();
                olapTable.setCachedTableVersion(version);
            }
        }
        LOG.info("get partition versions cost {} ms, table size: {}, rpc size: {}", System.currentTimeMillis() - start,
                tableVersionMap.size(), futures.size());
        tableVersionMap.clear();
        failedTable.clear();
    }

    private Future<Void> submitGetTableVersionTask(List<CloudPartition> partitions) {
        return GET_VERSION_THREAD_POOL.submit(() -> {
            try {
                CloudPartition.getSnapshotVisibleVersionFromMs(partitions, false);
            } catch (Exception e) {
                LOG.warn("get tablet version exception:", e);
                Set<Long> failedTableIds = partitions.stream().map(p -> p.getTableId())
                        .collect(java.util.stream.Collectors.toSet());
                failedTable.addAll(failedTableIds);
            }
            return null;
        });
    }

    private void refreshFrontends() {
        HostInfo selfNode = Env.getCurrentEnv().getSelfNode();
        frontends = Env.getCurrentEnv().getFrontends(null).stream()
                .filter(fe -> fe.isAlive() && !(fe.getHost().equals(selfNode.getHost())
                        && fe.getRpcPort() == selfNode.getPort())).collect(
                        Collectors.toList());
    }

    public void updateVersion(long dbId, List<Pair<OlapTable, Long>> tableVersions,
            Map<CloudPartition, Pair<Long, Long>> parititionVersionMap) {
        if (tableVersions.isEmpty() && parititionVersionMap.isEmpty()) {
            return;
        }
        refreshFrontends();
        if (frontends == null || frontends.isEmpty()) {
            return;
        }

        List<TClodVersionInfo> tableVersionInfos = new ArrayList<>();
        tableVersions.forEach(pair -> {
            TClodVersionInfo tableVersion = new TClodVersionInfo();
            tableVersion.setTableId(pair.first.getId());
            tableVersion.setVersion(pair.second);
            tableVersionInfos.add(tableVersion);

        });
        List<TClodVersionInfo> partitionVersionInfos = new ArrayList<>();
        parititionVersionMap.forEach((partition, versionPair) -> {
            TClodVersionInfo partitionVersion = new TClodVersionInfo();
            partitionVersion.setTableId(partition.getTableId());
            partitionVersion.setPartitionId(partition.getId());
            partitionVersion.setVersion(versionPair.first);
            partitionVersion.setVersionUpdateTime(versionPair.second);
            partitionVersionInfos.add(partitionVersion);
        });
        TFrontendUpdateCloudVersionRequest request =
                new TFrontendUpdateCloudVersionRequest();
        request.setDbId(dbId);
        request.setTableVersionInfos(tableVersionInfos);
        request.setPartitionVersionInfos(partitionVersionInfos);
        UPDATE_VERSION_THREAD_POOL.submit(() -> {
            try {
                updateCloudVersionToFes(request);
            } catch (Exception e) {
                LOG.warn("update table and partition version exception:", e);
            }
        });
    }

    private void updateCloudVersionToFes(TFrontendUpdateCloudVersionRequest request) {
        List<Frontend> frontends = Env.getCurrentEnv().getFrontends(null);
        for (Frontend fe : frontends) {
            updateCloudVersionToFe(request, fe);
        }
    }

    private void updateCloudVersionToFe(TFrontendUpdateCloudVersionRequest request, Frontend fe) {
        FrontendService.Client client = null;
        TNetworkAddress addr = new TNetworkAddress(fe.getHost(), fe.getRpcPort());
        boolean ok = false;
        try {
            client = ClientPool.frontendVersionPool.borrowObject(addr);
            TFrontendUpdateCloudVersionResult result = client.updateCloudVersion(request);
            ok = true;
            if (result.getStatus().getStatusCode() != TStatusCode.OK) {
                LOG.warn("failed to update cloud table and partition version to frontend {}:{},"
                        + " err: {}", fe.getHost(), fe.getRpcPort(), result.getStatus().getErrorMsgs());
            }
        } catch (Exception e) {
            LOG.warn("failed to update cloud table and partition version to frontend {}:{}", fe.getHost(),
                    fe.getRpcPort(), e);
        } finally {
            if (ok) {
                ClientPool.frontendVersionPool.returnObject(addr, client);
            } else {
                ClientPool.frontendVersionPool.invalidateObject(addr, client);
            }
        }
    }

    public void updateTableVersion(TFrontendUpdateCloudVersionRequest request) {
        long dbId = request.getDbId();
        Optional<Database> dbOptional = Env.getCurrentInternalCatalog().getDb(dbId);
        if (!dbOptional.isPresent()) {
            return;
        }
        Database db = dbOptional.get();

        List<Pair<OlapTable, Long>> tableVersions = new ArrayList<>(request.getTableVersionInfos().size());
        for (TClodVersionInfo tableVersionInfo : request.getTableVersionInfos()) {
            Table table = db.getTableNullable(tableVersionInfo.getTableId());
            if (table == null || !table.isManagedTable()) {
                continue;
            }
            OlapTable olapTable = (OlapTable) table;
            tableVersions.add(Pair.of(olapTable, tableVersionInfo.getVersion()));
        }
        for (Pair<OlapTable, Long> tableVersion : tableVersions) {
            tableVersion.first.versionWriteLock();
        }
        try {
            for (TClodVersionInfo partitionVersionInfo : request.getPartitionVersionInfos()) {
                Table table = db.getTableNullable(partitionVersionInfo.getTableId());
                if (table == null || !table.isManagedTable()) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) table;
                Partition partition = olapTable.getPartition(partitionVersionInfo.getPartitionId());
                if (partition == null || !(partition instanceof CloudPartition)) {
                    continue;
                }
                CloudPartition cloudPartition = (CloudPartition) partition;
                cloudPartition.setCachedVisibleVersion(partitionVersionInfo.getVersion(),
                        partitionVersionInfo.getVersionUpdateTime());
                LOG.info("Update Partition. table_id:{}, partition_id:{}, version:{}, update time:{}",
                        partitionVersionInfo.getTableId(), partition.getId(), partitionVersionInfo.getVersion(),
                        partitionVersionInfo.getVersionUpdateTime());
            }
            for (Pair<OlapTable, Long> tableVersion : tableVersions) {
                tableVersion.first.setCachedTableVersion(tableVersion.second);
                LOG.info("Update table_id:{}, visible_version:{}", tableVersion.first.getId(), tableVersion.second);
            }
        } finally {
            for (Pair<OlapTable, Long> tableVersion : tableVersions) {
                tableVersion.first.versionWriteUnlock();
            }
        }
    }
}
