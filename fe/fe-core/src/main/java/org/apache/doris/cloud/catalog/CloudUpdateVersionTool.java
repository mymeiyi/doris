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
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService.HostInfo;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TCloudVersionInfo;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class CloudUpdateVersionTool {
    private static final Logger LOG = LogManager.getLogger(CloudUpdateVersionTool.class);

    private static final ExecutorService UPDATE_VERSION_THREAD_POOL = Executors.newFixedThreadPool(
            Config.cloud_update_version_task_threads_num);

    public CloudUpdateVersionTool() {
    }

    // master FE send update version rpc to other FEs
    public void updateVersionAsync(long dbId, OlapTable table, long version) {
        updateVersionAsync(dbId, Collections.singletonList(Pair.of(table, version)), Collections.emptyMap());
    }

    public void updateVersionAsync(long dbId, List<Pair<OlapTable, Long>> tableVersions,
            Map<CloudPartition, Pair<Long, Long>> parititionVersionMap) {
        UPDATE_VERSION_THREAD_POOL.submit(() -> {
            try {
                updateVersion(dbId, tableVersions, parititionVersionMap);
            } catch (Exception e) {
                LOG.warn("update table and partition version error", e);
            }
        });
    }

    private void updateVersion(long dbId, List<Pair<OlapTable, Long>> tableVersions,
            Map<CloudPartition, Pair<Long, Long>> parititionVersionMap) {
        if (tableVersions.isEmpty() && parititionVersionMap.isEmpty()) {
            return;
        }
        List<Frontend> frontends = getFrontends();
        if (frontends == null || frontends.isEmpty()) {
            return;
        }

        List<TCloudVersionInfo> tableVersionInfos = new ArrayList<>(tableVersions.size());
        tableVersions.forEach(pair -> {
            TCloudVersionInfo tableVersion = new TCloudVersionInfo();
            tableVersion.setTableId(pair.first.getId());
            tableVersion.setVersion(pair.second);
            tableVersionInfos.add(tableVersion);
        });
        List<TCloudVersionInfo> partitionVersionInfos = new ArrayList<>(parititionVersionMap.size());
        parititionVersionMap.forEach((partition, versionPair) -> {
            TCloudVersionInfo partitionVersion = new TCloudVersionInfo();
            partitionVersion.setTableId(partition.getTableId());
            partitionVersion.setPartitionId(partition.getId());
            partitionVersion.setVersion(versionPair.first);
            partitionVersion.setVersionUpdateTime(versionPair.second);
            partitionVersionInfos.add(partitionVersion);
        });
        TFrontendUpdateCloudVersionRequest request = new TFrontendUpdateCloudVersionRequest();
        request.setDbId(dbId);
        request.setTableVersionInfos(tableVersionInfos);
        request.setPartitionVersionInfos(partitionVersionInfos);
        for (Frontend fe : frontends) {
            UPDATE_VERSION_THREAD_POOL.submit(() -> {
                try {
                    updateCloudVersionToFe(request, fe);
                } catch (Exception e) {
                    LOG.warn("update table and partition version error", e);
                }
            });
        }
    }

    private List<Frontend> getFrontends() {
        HostInfo selfNode = Env.getCurrentEnv().getSelfNode();
        return Env.getCurrentEnv().getFrontends(null).stream()
                .filter(fe -> fe.isAlive() && !(fe.getHost().equals(selfNode.getHost())
                        && fe.getRpcPort() == selfNode.getPort())).collect(
                        Collectors.toList());
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

    // follower and observer FE receive update version rpc from master FE
    public void updateVersionAsync(TFrontendUpdateCloudVersionRequest request) {
        long dbId = request.getDbId();
        Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            return;
        }
        // only update table version
        if (request.getPartitionVersionInfos().isEmpty()) {
            request.getTableVersionInfos().forEach(tableVersionInfo -> {
                Table table = db.getTableNullable(tableVersionInfo.getTableId());
                if (table == null || !table.isManagedTable()) {
                    return;
                }
                OlapTable olapTable = (OlapTable) table;
                olapTable.setCachedTableVersion(tableVersionInfo.getVersion());
                LOG.info("Update tableId: {}, version: {}", olapTable.getId(), tableVersionInfo.getVersion());
            });
            return;
        }

        List<Pair<OlapTable, Long>> tableVersions = new ArrayList<>(request.getTableVersionInfos().size());
        for (TCloudVersionInfo tableVersionInfo : request.getTableVersionInfos()) {
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
            for (TCloudVersionInfo partitionVersionInfo : request.getPartitionVersionInfos()) {
                Table table = db.getTableNullable(partitionVersionInfo.getTableId());
                if (table == null || !table.isManagedTable()) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) table;
                Partition partition = olapTable.getPartition(partitionVersionInfo.getPartitionId());
                if (partition == null) {
                    continue;
                }
                CloudPartition cloudPartition = (CloudPartition) partition;
                cloudPartition.setCachedVisibleVersion(partitionVersionInfo.getVersion(),
                        partitionVersionInfo.getVersionUpdateTime());
                LOG.info("Update tableId: {}, partitionId: {}, version: {}, updateTime: {}",
                        partitionVersionInfo.getTableId(), partition.getId(), partitionVersionInfo.getVersion(),
                        partitionVersionInfo.getVersionUpdateTime());
            }
            for (Pair<OlapTable, Long> tableVersion : tableVersions) {
                tableVersion.first.setCachedTableVersion(tableVersion.second);
                LOG.info("Update tableId: {}, version: {}", tableVersion.first.getId(), tableVersion.second);
            }
        } finally {
            for (Pair<OlapTable, Long> tableVersion : tableVersions) {
                tableVersion.first.versionWriteUnlock();
            }
        }
    }
}
