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
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService.HostInfo;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TClodVersionInfo;
import org.apache.doris.thrift.TFrontendUpdateCloudVersionRequest;
import org.apache.doris.thrift.TFrontendUpdateCloudVersionResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class CloudUpdateVersionTool {
    private static final Logger LOG = LogManager.getLogger(CloudUpdateVersionTool.class);

    private static final ExecutorService UPDATE_VERSION_THREAD_POOL = Executors.newFixedThreadPool(
            Config.cloud_max_get_version_task_threads_num);
    private List<Frontend> frontends = null;

    public CloudUpdateVersionTool() {
    }

    private void refreshFrontends() {
        HostInfo selfNode = Env.getCurrentEnv().getSelfNode();
        frontends = Env.getCurrentEnv().getFrontends(null).stream()
                .filter(fe -> fe.isAlive() && !(fe.getHost().equals(selfNode.getHost())
                        && fe.getRpcPort() == selfNode.getPort())).collect(
                        Collectors.toList());
    }

    public void updateVersionAsync(long dbId, OlapTable table, long version) {
        updateVersionAsync(dbId, Collections.singletonList(Pair.of(table, version)), Collections.emptyMap());
    }

    public void updateVersionAsync(long dbId, List<Pair<OlapTable, Long>> tableVersions,
            Map<CloudPartition, Pair<Long, Long>> parititionVersionMap) {
        UPDATE_VERSION_THREAD_POOL.submit(() -> {
            try {
                updateVersion(dbId, tableVersions, parititionVersionMap);
            } catch (Exception e) {
                LOG.warn("update table and partition version exception:", e);
            }
        });
    }

    private void updateVersion(long dbId, List<Pair<OlapTable, Long>> tableVersions,
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

    public void updateVersionAsync(TFrontendUpdateCloudVersionRequest request) {
        long dbId = request.getDbId();
        Optional<Database> dbOptional = Env.getCurrentInternalCatalog().getDb(dbId);
        if (!dbOptional.isPresent()) {
            return;
        }
        Database db = dbOptional.get();
        if (request.getPartitionVersionInfos().isEmpty()) {
            request.getTableVersionInfos().forEach(tableVersionInfo -> {
                Table table = db.getTableNullable(tableVersionInfo.getTableId());
                if (table == null || !table.isManagedTable()) {
                    return;
                }
                OlapTable olapTable = (OlapTable) table;
                olapTable.setCachedTableVersion(tableVersionInfo.getVersion());
                LOG.info("Update table_id:{}, visible_version:{}", olapTable.getId(), tableVersionInfo.getVersion());
            });
            return;
        }

        List<Pair<OlapTable, Long>> tableVersions = new ArrayList<>(request.getTableVersionInfos().size());
        for (TClodVersionInfo tableVersionInfo : request.getTableVersionInfos()) {
            Table table = db.getTableNullable(tableVersionInfo.getTableId());
            if (table == null || !table.isManagedTable()) {
                continue;
            }
            OlapTable olapTable = (OlapTable) table;
            tableVersions.add(Pair.of(olapTable, tableVersionInfo.getVersion()));
        }
        Collections.sort(tableVersions, Comparator.comparingLong(o -> o.first.getId()));
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
