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

import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.cloud.proto.Cloud.GetTabletStatsRequest;
import org.apache.doris.cloud.proto.Cloud.GetTabletStatsResponse;
import org.apache.doris.cloud.proto.Cloud.MetaServiceCode;
import org.apache.doris.cloud.proto.Cloud.TabletIndexPB;
import org.apache.doris.cloud.proto.Cloud.TabletStatsPB;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TCloudTabletStat;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TSyncCloudTabletStatsRequest;
import org.apache.doris.thrift.TSyncCloudTabletStatsResponse;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/*
 * CloudTabletStatMgr is for collecting tablet(replica) statistics from meta-service.
 * Uses incremental scheduling: tablets are fetched based on adaptive intervals.
 * Only master FE pulls from meta-service; followers receive updates via edit log sync.
 */
public class CloudTabletStatMgr extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudTabletStatMgr.class);

    private volatile List<OlapTable.Statistics> cloudTableStatsList = new ArrayList<>();

    private static final ExecutorService GET_TABLET_STATS_THREAD_POOL = Executors.newFixedThreadPool(
            Config.max_get_tablet_stat_task_threads_num);

    /**
     * Interval ladder in milliseconds: 1m, 5m, 10m, 30m, 2h, 6h, 12h, 3d, infinite.
     * Tablets with changing stats stay at lower intervals; stable tablets move to higher intervals.
     */
    private static final long[] DEFAULT_INTERVAL_LADDER_MS = {
        TimeUnit.MINUTES.toMillis(1),    // 1 minute
        TimeUnit.MINUTES.toMillis(5),    // 5 minutes
        TimeUnit.MINUTES.toMillis(10),   // 10 minutes
        TimeUnit.MINUTES.toMillis(30),   // 30 minutes
        TimeUnit.HOURS.toMillis(2),      // 2 hours
        TimeUnit.HOURS.toMillis(6),      // 6 hours
        TimeUnit.HOURS.toMillis(12),     // 12 hours
        TimeUnit.DAYS.toMillis(3),       // 3 days
        Long.MAX_VALUE                   // infinite (never auto-fetch)
    };

    /**
     * Per-tablet scheduling info: tracks last fetch time, interval tier, and cached stats.
     * Uses exactly 6 stats fields: dataSize, rowCount, rowsetCount, segmentCount, indexSize, segmentSize.
     */
    private static class TabletStatInfo {
        volatile long lastGetTabletStatsTimeMs;
        volatile int intervalTierIndex;
        // Cached stats for change detection (6 fields)
        volatile long dataSize;
        volatile long rowCount;
        volatile long rowsetCount;
        volatile long segmentCount;
        volatile long indexSize;
        volatile long segmentSize;

        TabletStatInfo() {
            this.lastGetTabletStatsTimeMs = 0;
            this.intervalTierIndex = 0;
            this.dataSize = 0;
            this.rowCount = 0;
            this.rowsetCount = 0;
            this.segmentCount = 0;
            this.indexSize = 0;
            this.segmentSize = 0;
        }

        /**
         * Checks if stats have changed from the given new values.
         */
        boolean hasStatsChanged(long newDataSize, long newRowCount, long newRowsetCount,
                                long newSegmentCount, long newIndexSize, long newSegmentSize) {
            return this.dataSize != newDataSize
                    || this.rowCount != newRowCount
                    || this.rowsetCount != newRowsetCount
                    || this.segmentCount != newSegmentCount
                    || this.indexSize != newIndexSize
                    || this.segmentSize != newSegmentSize;
        }

        /**
         * Updates cached stats with new values.
         */
        void updateStats(long newDataSize, long newRowCount, long newRowsetCount,
                         long newSegmentCount, long newIndexSize, long newSegmentSize) {
            this.dataSize = newDataSize;
            this.rowCount = newRowCount;
            this.rowsetCount = newRowsetCount;
            this.segmentCount = newSegmentCount;
            this.indexSize = newIndexSize;
            this.segmentSize = newSegmentSize;
        }
    }

    /**
     * Thread-safe map: tabletId -> TabletStatInfo for incremental scheduling.
     */
    private final ConcurrentHashMap<Long, TabletStatInfo> tabletStatInfoMap = new ConcurrentHashMap<>();

    private volatile String intervalLadderConfigCache = "";
    private volatile long[] intervalLadderMs = DEFAULT_INTERVAL_LADDER_MS;
    private volatile long lastFullRefreshTimeMs = 0;

    public CloudTabletStatMgr() {
        super("cloud tablet stat mgr", Config.tablet_stat_update_interval_second * 1000);
    }

    /**
     * Returns the interval in milliseconds for the given tier index.
     * If index is out of bounds, returns Long.MAX_VALUE (infinite).
     */
    private long getIntervalMs(int tierIndex) {
        long[] ladder = getIntervalLadderMs();
        if (tierIndex < 0 || tierIndex >= ladder.length) {
            return Long.MAX_VALUE;
        }
        return ladder[tierIndex];
    }

    private long[] getIntervalLadderMs() {
        String configValue = Config.cloud_tablet_stat_interval_ladder_ms;
        if (configValue == null) {
            return intervalLadderMs;
        }
        if (!configValue.equals(intervalLadderConfigCache)) {
            intervalLadderMs = parseIntervalLadderMs(configValue);
            intervalLadderConfigCache = configValue;
        }
        return intervalLadderMs;
    }

    private long[] parseIntervalLadderMs(String configValue) {
        String[] parts = configValue.split(",");
        List<Long> values = new ArrayList<>();
        for (String part : parts) {
            String trimmed = part.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            try {
                long value = Long.parseLong(trimmed);
                if (value < 0) {
                    values.add(Long.MAX_VALUE);
                } else {
                    values.add(value);
                }
            } catch (NumberFormatException e) {
                LOG.warn("invalid cloud_tablet_stat_interval_ladder_ms: {}, use default", configValue, e);
                return DEFAULT_INTERVAL_LADDER_MS;
            }
        }
        if (values.isEmpty()) {
            return DEFAULT_INTERVAL_LADDER_MS;
        }
        long[] ladder = new long[values.size()];
        for (int i = 0; i < values.size(); i++) {
            ladder[i] = values.get(i);
        }
        return ladder;
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!Env.getCurrentEnv().isMaster()) {
            return;
        }
        LOG.info("cloud tablet stat begin");
        long startTimeMs = System.currentTimeMillis();
        boolean needFullRefresh = Config.cloud_tablet_stat_full_refresh_interval_sec > 0
                && startTimeMs - lastFullRefreshTimeMs
                >= Config.cloud_tablet_stat_full_refresh_interval_sec * 1000L;
        List<TabletIndexPB> dueTablets = needFullRefresh
                ? selectAllTablets()
                : selectDueTablets(startTimeMs);
        if (dueTablets.isEmpty()) {
            LOG.info("no tablets due for stats update");
            return;
        }
        if (needFullRefresh) {
            lastFullRefreshTimeMs = startTimeMs;
        }
        LOG.info("selected {} tablets due for stats update", dueTablets.size());
        fetchTabletStatsBatch(dueTablets, startTimeMs);
        List<Long> dbIds = Env.getCurrentInternalCatalog().getDbIds();
        updateStatInfo(dbIds);
    }

    /**
     * Selects tablets that are due for stats fetching based on their interval tier.
     * A tablet is due if: lastGetTabletStatsTimeMs + currentIntervalMs <= nowMs
     * New tablets (not in map) are always due and initialized at tier 0.
     *
     * @param nowMs current timestamp in milliseconds
     * @return list of TabletIndexPB for tablets needing stats update
     */
    private List<TabletIndexPB> selectDueTablets(long nowMs) {
        List<TabletIndexPB> dueTablets = new ArrayList<>();
        List<Long> dbIds = Env.getCurrentInternalCatalog().getDbIds();

        for (Long dbId : dbIds) {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }

            List<Table> tableList = db.getTables();
            for (Table table : tableList) {
                if (!table.isManagedTable()) {
                    continue;
                }

                table.readLock();
                try {
                    OlapTable tbl = (OlapTable) table;
                    for (Partition partition : tbl.getAllPartitions()) {
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            for (Long tabletId : index.getTabletIdsInOrder()) {
                                if (isTabletDue(tabletId, nowMs)) {
                                    TabletIndexPB.Builder tabletBuilder = TabletIndexPB.newBuilder();
                                    tabletBuilder.setDbId(dbId);
                                    tabletBuilder.setTableId(table.getId());
                                    tabletBuilder.setIndexId(index.getId());
                                    tabletBuilder.setPartitionId(partition.getId());
                                    tabletBuilder.setTabletId(tabletId);
                                    dueTablets.add(tabletBuilder.build());
                                }
                            }
                        }
                    }
                } finally {
                    table.readUnlock();
                }
            }
        }
        return dueTablets;
    }

    private List<TabletIndexPB> selectAllTablets() {
        List<TabletIndexPB> tablets = new ArrayList<>();
        List<Long> dbIds = Env.getCurrentInternalCatalog().getDbIds();

        for (Long dbId : dbIds) {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }

            List<Table> tableList = db.getTables();
            for (Table table : tableList) {
                if (!table.isManagedTable()) {
                    continue;
                }

                table.readLock();
                try {
                    OlapTable tbl = (OlapTable) table;
                    for (Partition partition : tbl.getAllPartitions()) {
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            for (Long tabletId : index.getTabletIdsInOrder()) {
                                TabletIndexPB.Builder tabletBuilder = TabletIndexPB.newBuilder();
                                tabletBuilder.setDbId(dbId);
                                tabletBuilder.setTableId(table.getId());
                                tabletBuilder.setIndexId(index.getId());
                                tabletBuilder.setPartitionId(partition.getId());
                                tabletBuilder.setTabletId(tabletId);
                                tablets.add(tabletBuilder.build());
                            }
                        }
                    }
                } finally {
                    table.readUnlock();
                }
            }
        }
        return tablets;
    }

    /**
     * Checks if a tablet is due for stats fetching.
     * New tablets not in the map are always due.
     */
    private boolean isTabletDue(long tabletId, long nowMs) {
        TabletStatInfo info = tabletStatInfoMap.get(tabletId);
        if (info == null) {
            return true;
        }
        long intervalMs = getIntervalMs(info.intervalTierIndex);
        if (intervalMs == Long.MAX_VALUE) {
            return false;
        }
        return info.lastGetTabletStatsTimeMs + intervalMs <= nowMs;
    }

    /**
     * Fetches stats for due tablets in batches and updates replica/scheduling info.
     */
    private void fetchTabletStatsBatch(List<TabletIndexPB> dueTablets, long fetchTimeMs) {
        List<Future<Void>> futures = new ArrayList<>();
        GetTabletStatsRequest.Builder builder =
                GetTabletStatsRequest.newBuilder().setRequestIp(FrontendOptions.getLocalHostAddressCached());

        for (TabletIndexPB tabletIdx : dueTablets) {
            builder.addTabletIdx(tabletIdx);
            if (builder.getTabletIdxCount() >= Config.get_tablet_stat_batch_size) {
                futures.add(submitGetTabletStatsTask(builder.build(), fetchTimeMs));
                builder = GetTabletStatsRequest.newBuilder()
                        .setRequestIp(FrontendOptions.getLocalHostAddressCached());
            }
        }

        if (builder.getTabletIdxCount() > 0) {
            futures.add(submitGetTabletStatsTask(builder.build(), fetchTimeMs));
        }

        try {
            for (Future<Void> future : futures) {
                future.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Error waiting for get tablet stats tasks to complete", e);
        }

        LOG.info("finished to get tablet stat for {} tablets. cost: {} ms",
                dueTablets.size(), (System.currentTimeMillis() - fetchTimeMs));
    }

    private Future<Void> submitGetTabletStatsTask(GetTabletStatsRequest req, long fetchTimeMs) {
        return GET_TABLET_STATS_THREAD_POOL.submit(() -> {
            GetTabletStatsResponse resp;
            try {
                resp = getTabletStats(req);
            } catch (RpcException e) {
                LOG.warn("get tablet stats exception:", e);
                return null;
            }
            if (resp.getStatus().getCode() != MetaServiceCode.OK) {
                LOG.warn("get tablet stats return failed.");
                return null;
            }
            if (LOG.isDebugEnabled()) {
                int i = 0;
                for (TabletIndexPB idx : req.getTabletIdxList()) {
                    LOG.debug("db_id: {} table_id: {} index_id: {} tablet_id: {} size: {}",
                            idx.getDbId(), idx.getTableId(), idx.getIndexId(),
                            idx.getTabletId(), resp.getTabletStats(i++).getDataSize());
                }
            }
            List<TCloudTabletStat> syncStats = updateTabletStatWithScheduling(req, resp, fetchTimeMs);
            syncTabletStatsToFollowers(syncStats);
            return null;
        });
    }

    private void updateStatInfo(List<Long> dbIds) {
        // after update replica in all backends, update index row num
        long start = System.currentTimeMillis();
        Pair<String, Long> maxTabletSize = Pair.of(/* tablet id= */null, /* byte size= */0L);
        Pair<String, Long> maxPartitionSize = Pair.of(/* partition id= */null, /* byte size= */0L);
        Pair<String, Long> maxTableSize = Pair.of(/* table id= */null, /* byte size= */0L);
        Pair<String, Long> minTabletSize = Pair.of(/* tablet id= */null, /* byte size= */Long.MAX_VALUE);
        Pair<String, Long> minPartitionSize = Pair.of(/* partition id= */null, /* byte size= */Long.MAX_VALUE);
        Pair<String, Long> minTableSize = Pair.of(/* tablet id= */null, /* byte size= */Long.MAX_VALUE);
        long totalTableSize = 0L;
        long tabletCount = 0L;
        long partitionCount = 0L;
        long tableCount = 0L;
        List<OlapTable.Statistics> newCloudTableStatsList = new ArrayList<>();
        for (Long dbId : dbIds) {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }
            List<Table> tableList = db.getTables();
            for (Table table : tableList) {
                if (!table.isManagedTable()) {
                    continue;
                }
                tableCount++;
                OlapTable olapTable = (OlapTable) table;

                long tableDataSize = 0L;
                long tableTotalReplicaDataSize = 0L;
                long tableTotalLocalIndexSize = 0L;
                long tableTotalLocalSegmentSize = 0L;

                long tableReplicaCount = 0L;

                long tableRowCount = 0L;
                long tableRowsetCount = 0L;
                long tableSegmentCount = 0L;

                if (!table.readLockIfExist()) {
                    continue;
                }
                OlapTable.Statistics tableStats;
                try {
                    List<Partition> allPartitions = olapTable.getAllPartitions();
                    partitionCount += allPartitions.size();
                    for (Partition partition : allPartitions) {
                        long partitionDataSize = 0L;
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            long indexRowCount = 0L;
                            List<Tablet> tablets = index.getTablets();
                            tabletCount += tablets.size();
                            for (Tablet tablet : tablets) {
                                long tabletDataSize = 0L;

                                long tabletRowsetCount = 0L;
                                long tabletSegmentCount = 0L;
                                long tabletRowCount = 0L;
                                long tabletIndexSize = 0L;
                                long tabletSegmentSize = 0L;

                                for (Replica replica : tablet.getReplicas()) {
                                    if (replica.getDataSize() > tabletDataSize) {
                                        tabletDataSize = replica.getDataSize();
                                        tableTotalReplicaDataSize += replica.getDataSize();
                                    }

                                    if (replica.getRowCount() > tabletRowCount) {
                                        tabletRowCount = replica.getRowCount();
                                    }

                                    if (replica.getRowsetCount() > tabletRowsetCount) {
                                        tabletRowsetCount = replica.getRowsetCount();
                                    }

                                    if (replica.getSegmentCount() > tabletSegmentCount) {
                                        tabletSegmentCount = replica.getSegmentCount();
                                    }

                                    if (replica.getLocalInvertedIndexSize() > tabletIndexSize) {
                                        tabletIndexSize = replica.getLocalInvertedIndexSize();
                                    }
                                    if (replica.getLocalSegmentSize() > tabletSegmentSize) {
                                        tabletSegmentSize = replica.getLocalSegmentSize();
                                    }

                                    tableReplicaCount++;
                                }

                                tableDataSize += tabletDataSize;
                                partitionDataSize += tabletDataSize;
                                if (maxTabletSize.second <= tabletDataSize) {
                                    maxTabletSize = Pair.of("" + tablet.getId(), tabletDataSize);
                                }
                                if (minTabletSize.second >= tabletDataSize) {
                                    minTabletSize = Pair.of("" + tablet.getId(), tabletDataSize);
                                }

                                tableRowCount += tabletRowCount;
                                indexRowCount += tabletRowCount;

                                tableRowsetCount += tabletRowsetCount;
                                tableSegmentCount += tabletSegmentCount;
                                tableTotalLocalIndexSize += tabletIndexSize;
                                tableTotalLocalSegmentSize += tabletSegmentSize;
                            } // end for tablets
                            index.setRowCountReported(true);
                            index.setRowCount(indexRowCount);
                        } // end for indices
                        if (maxPartitionSize.second <= partitionDataSize) {
                            maxPartitionSize = Pair.of("" + partition.getId(), partitionDataSize);
                        }
                        if (minPartitionSize.second >= partitionDataSize) {
                            minPartitionSize = Pair.of("" + partition.getId(), partitionDataSize);
                        }
                    } // end for partitions
                    if (maxTableSize.second <= tableDataSize) {
                        maxTableSize = Pair.of("" + table.getId(), tableDataSize);
                    }
                    if (minTableSize.second >= tableDataSize) {
                        minTableSize = Pair.of("" + table.getId(), tableDataSize);
                    }

                    //  this is only one thread to update table statistics, readLock is enough
                    tableStats = new OlapTable.Statistics(db.getName(),
                            table.getName(), tableDataSize, tableTotalReplicaDataSize, 0L,
                            tableReplicaCount, tableRowCount, tableRowsetCount, tableSegmentCount,
                            tableTotalLocalIndexSize, tableTotalLocalSegmentSize, 0L, 0L);
                    olapTable.setStatistics(tableStats);
                    LOG.debug("finished to set row num for table: {} in database: {}",
                             table.getName(), db.getFullName());
                } finally {
                    table.readUnlock();
                }
                totalTableSize += tableDataSize;
                newCloudTableStatsList.add(tableStats);
            }
        }
        this.cloudTableStatsList = newCloudTableStatsList;

        if (MetricRepo.isInit) {
            MetricRepo.GAUGE_MAX_TABLE_SIZE_BYTES.setValue(maxTableSize.second);
            MetricRepo.GAUGE_MAX_PARTITION_SIZE_BYTES.setValue(maxPartitionSize.second);
            MetricRepo.GAUGE_MAX_TABLET_SIZE_BYTES.setValue(maxTabletSize.second);
            long minTableSizeTmp = minTableSize.second == Long.MAX_VALUE ? 0 : minTableSize.second;
            MetricRepo.GAUGE_MIN_TABLE_SIZE_BYTES.setValue(minTableSizeTmp);
            long minPartitionSizeTmp = minPartitionSize.second == Long.MAX_VALUE ? 0 : minPartitionSize.second;
            MetricRepo.GAUGE_MIN_PARTITION_SIZE_BYTES.setValue(minPartitionSizeTmp);
            long minTabletSizeTmp = minTabletSize.second == Long.MAX_VALUE ? 0 : minTabletSize.second;
            MetricRepo.GAUGE_MIN_TABLET_SIZE_BYTES.setValue(minTabletSizeTmp);
            // avoid ArithmeticException: / by zero
            long avgTableSize = totalTableSize / Math.max(1, tableCount);
            MetricRepo.GAUGE_AVG_TABLE_SIZE_BYTES.setValue(avgTableSize);
            // avoid ArithmeticException: / by zero
            long avgPartitionSize = totalTableSize / Math.max(1, partitionCount);
            MetricRepo.GAUGE_AVG_PARTITION_SIZE_BYTES.setValue(avgPartitionSize);
            // avoid ArithmeticException: / by zero
            long avgTabletSize = totalTableSize / Math.max(1, tabletCount);
            MetricRepo.GAUGE_AVG_TABLET_SIZE_BYTES.setValue(avgTabletSize);

            LOG.info("OlapTable num=" + tableCount
                    + ", partition num=" + partitionCount + ", tablet num=" + tabletCount
                    + ", max tablet byte size=" + maxTabletSize.second
                    + "(tablet_id=" + maxTabletSize.first + ")"
                    + ", min tablet byte size=" + minTabletSizeTmp
                    + "(tablet_id=" + minTabletSize.first + ")"
                    + ", avg tablet byte size=" + avgTabletSize
                    + ", max partition byte size=" + maxPartitionSize.second
                    + "(partition_id=" + maxPartitionSize.first + ")"
                    + ", min partition byte size=" + minPartitionSizeTmp
                    + "(partition_id=" + minPartitionSize.first + ")"
                    + ", avg partition byte size=" + avgPartitionSize
                    + ", max table byte size=" + maxTableSize.second + "(table_id=" + maxTableSize.first + ")"
                    + ", min table byte size=" + minTableSizeTmp + "(table_id=" + minTableSize.first + ")"
                    + ", avg table byte size=" + avgTableSize);
        }

        LOG.info("finished to update index row num of all databases. cost: {} ms",
                (System.currentTimeMillis() - start));
    }

    /**
     * Updates replica stats and adjusts scheduling interval based on stats change detection.
     * If stats changed: reset interval to tier 0 (frequent polling).
     * If stats unchanged: advance to next tier (less frequent polling).
     */
    private List<TCloudTabletStat> updateTabletStatWithScheduling(GetTabletStatsRequest request,
                                                                  GetTabletStatsResponse response,
                                                                  long fetchTimeMs) {
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        List<TabletIndexPB> tabletIdxList = request.getTabletIdxList();
        List<TabletStatsPB> tabletStatsList = response.getTabletStatsList();
        long[] ladder = getIntervalLadderMs();
        List<TCloudTabletStat> syncStats = new ArrayList<>(tabletStatsList.size());

        for (int i = 0; i < tabletStatsList.size(); i++) {
            TabletStatsPB stat = tabletStatsList.get(i);
            long tabletId = tabletIdxList.get(i).getTabletId();

            long newDataSize = stat.getDataSize();
            long newRowCount = stat.getNumRows();
            long newRowsetCount = stat.getNumRowsets();
            long newSegmentCount = stat.getNumSegments();
            long newIndexSize = stat.getIndexSize();
            long newSegmentSize = stat.getSegmentSize();

            List<Replica> replicas = invertedIndex.getReplicasByTabletId(tabletId);
            if (replicas != null && !replicas.isEmpty() && replicas.get(0) != null) {
                Replica replica = replicas.get(0);
                replica.setDataSize(newDataSize);
                replica.setRowsetCount(newRowsetCount);
                replica.setSegmentCount(newSegmentCount);
                replica.setRowCount(newRowCount);
                replica.setLocalInvertedIndexSize(newIndexSize);
                replica.setLocalSegmentSize(newSegmentSize);
            }

            TabletStatInfo info = tabletStatInfoMap.computeIfAbsent(tabletId, k -> new TabletStatInfo());

            boolean statsChanged = info.hasStatsChanged(newDataSize, newRowCount, newRowsetCount,
                    newSegmentCount, newIndexSize, newSegmentSize);

            info.updateStats(newDataSize, newRowCount, newRowsetCount, newSegmentCount, newIndexSize, newSegmentSize);
            info.lastGetTabletStatsTimeMs = fetchTimeMs;

            if (statsChanged) {
                info.intervalTierIndex = 0;
            } else {
                if (info.intervalTierIndex < ladder.length - 1) {
                    info.intervalTierIndex++;
                }
            }

            TCloudTabletStat syncStat = new TCloudTabletStat();
            syncStat.setTablet_id(tabletId);
            syncStat.setData_size(newDataSize);
            syncStat.setRow_count(newRowCount);
            syncStat.setRowset_count(newRowsetCount);
            syncStat.setSegment_count(newSegmentCount);
            syncStat.setIndex_size(newIndexSize);
            syncStat.setSegment_size(newSegmentSize);
            syncStats.add(syncStat);
        }
        return syncStats;
    }

    private void syncTabletStatsToFollowers(List<TCloudTabletStat> tabletStats) {
        if (tabletStats == null || tabletStats.isEmpty()) {
            return;
        }
        if (!Env.getCurrentEnv().isMaster()) {
            return;
        }
        SystemInfoService.HostInfo selfNode = Env.getCurrentEnv().getSelfNode();
        TSyncCloudTabletStatsRequest request = new TSyncCloudTabletStatsRequest();
        request.setTablet_stats(tabletStats);
        for (Frontend frontend : Env.getCurrentEnv().getFrontends(null)) {
            if (selfNode.getHost().equals(frontend.getHost())) {
                continue;
            }
            TNetworkAddress address = new TNetworkAddress(frontend.getHost(), frontend.getRpcPort());
            FrontendService.Client client = null;
            try {
                client = ClientPool.frontendPool.borrowObject(address);
                TSyncCloudTabletStatsResponse response = client.syncCloudTabletStats(request);
                TStatus status = response.getStatus();
                if (status == null || status.getStatusCode() != TStatusCode.OK) {
                    LOG.warn("syncCloudTabletStats to follower {} failed: {}",
                            address, status == null ? "null status" : status.getStatusCode());
                }
            } catch (Throwable t) {
                LOG.warn("syncCloudTabletStats to follower {} failed", address, t);
            } finally {
                if (client != null) {
                    ClientPool.frontendPool.returnObject(address, client);
                }
            }
        }
    }

    /**
     * Removes scheduling info for tablets that no longer exist.
     * Called during tablet deletion to prevent memory leaks.
     */
    public void removeTabletStatInfo(long tabletId) {
        tabletStatInfoMap.remove(tabletId);
    }

    /**
     * Hook for follower FE to receive stats updates from master via edit log replay.
     * Updates replica stats directly without triggering MS fetch or interval adjustment.
     */
    public void updateReplicaStatFromSync(long tabletId, long dataSize, long rowCount,
                                           long rowsetCount, long segmentCount,
                                           long indexSize, long segmentSize) {
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        List<Replica> replicas = invertedIndex.getReplicasByTabletId(tabletId);
        if (replicas == null || replicas.isEmpty() || replicas.get(0) == null) {
            return;
        }
        Replica replica = replicas.get(0);
        replica.setDataSize(dataSize);
        replica.setRowsetCount(rowsetCount);
        replica.setSegmentCount(segmentCount);
        replica.setRowCount(rowCount);
        replica.setLocalInvertedIndexSize(indexSize);
        replica.setLocalSegmentSize(segmentSize);
    }

    private GetTabletStatsResponse getTabletStats(GetTabletStatsRequest request)
            throws RpcException {
        GetTabletStatsResponse response;
        try {
            response = MetaServiceProxy.getInstance().getTabletStats(request);
        } catch (RpcException e) {
            LOG.info("get tablet stat get exception:", e);
            throw e;
        }
        return response;
    }

    public List<OlapTable.Statistics> getCloudTableStats() {
        return this.cloudTableStatsList;
    }

    /**
     * Handles BE notification that partitions have been updated (data ingested).
     * Resets the interval tier to 0 for all tablets in the specified partitions,
     * causing them to be fetched in the next scheduling round.
     *
     * @param dbId database ID
     * @param tableId table ID
     * @param partitionIds list of partition IDs that have been updated
     */
    public void handleTabletUpdateNotify(long dbId, long tableId, long partitionId, long tabletId) {
        /*if (partitionIds == null || partitionIds.isEmpty()) {
            return;
        }*/
        /*Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            LOG.warn("handlePartitionUpdateNotify: database {} not found", dbId);
            return;
        }
        Table table = db.getTableNullable(tableId);
        if (table == null || !table.isManagedTable()) {
            LOG.warn("handlePartitionUpdateNotify: table {} not found or not managed", tableId);
            return;
        }*/
        TabletStatInfo info = tabletStatInfoMap.computeIfAbsent(tabletId, k -> new TabletStatInfo());
        // Reset to tier 0 so tablet is fetched in next round
        info.intervalTierIndex = 0;
        info.lastGetTabletStatsTimeMs = 0;
        /*OlapTable olapTable = (OlapTable) table;
        olapTable.readLock();
        try {
            for (Long partitionId : partitionIds) {
                Partition partition = olapTable.getPartition(partitionId);
                if (partition == null) {
                    continue;
                }
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    for (Long tabletId : index.getTabletIdsInOrder()) {
                        TabletStatInfo info = tabletStatInfoMap.computeIfAbsent(tabletId, k -> new TabletStatInfo());
                        // Reset to tier 0 so tablet is fetched in next round
                        info.intervalTierIndex = 0;
                        info.lastGetTabletStatsTimeMs = 0;
                    }
                }
            }
        } finally {
            olapTable.readUnlock();
        }*/
        /*LOG.info("handlePartitionUpdateNotify: reset interval for partitions {} in table {}.{}",
                partitionIds, dbId, tableId);*/
    }

    /**
     * Handles tablet stats sync from master FE.
     * Updates replica stats directly without triggering meta-service fetch or interval adjustment.
     * Called on follower FEs to receive stats updates from master.
     *
     * @param tabletStats list of tablet stats from master
     */
    public void handleSyncTabletStats(List<TCloudTabletStat> tabletStats) {
        if (tabletStats == null || tabletStats.isEmpty()) {
            return;
        }
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        for (TCloudTabletStat stat : tabletStats) {
            if (!stat.isSetTabletId()) {
                continue;
            }
            List<Replica> replicas = invertedIndex.getReplicasByTabletId(stat.getTabletId());
            if (replicas == null || replicas.isEmpty() || replicas.get(0) == null) {
                continue;
            }
            Replica replica = replicas.get(0);
            if (stat.isSetDataSize()) {
                replica.setDataSize(stat.getDataSize());
            }
            if (stat.isSetRowCount()) {
                replica.setRowCount(stat.getRowCount());
            }
            if (stat.isSetRowsetCount()) {
                replica.setRowsetCount(stat.getRowsetCount());
            }
            if (stat.isSetSegmentCount()) {
                replica.setSegmentCount(stat.getSegmentCount());
            }
            if (stat.isSetIndexSize()) {
                replica.setLocalInvertedIndexSize(stat.getIndexSize());
            }
            if (stat.isSetSegmentSize()) {
                replica.setLocalSegmentSize(stat.getSegmentSize());
            }
        }
        LOG.info("handleSyncTabletStats: synced stats for {} tablets", tabletStats.size());
    }
}
