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

package org.apache.doris.alter;

import org.apache.doris.analysis.AddBackendClause;
import org.apache.doris.analysis.AddFollowerClause;
import org.apache.doris.analysis.AddObserverClause;
import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.DecommissionBackendClause;
import org.apache.doris.analysis.DropBackendClause;
import org.apache.doris.analysis.DropFollowerClause;
import org.apache.doris.analysis.DropObserverClause;
import org.apache.doris.analysis.ModifyBackendClause;
import org.apache.doris.analysis.ModifyBackendHostNameClause;
import org.apache.doris.analysis.ModifyBrokerClause;
import org.apache.doris.analysis.ModifyFrontendHostNameClause;
import org.apache.doris.catalog.CatalogRecycleBin;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MysqlCompatibleDatabase;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.AlterCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterSystemCommand;
import org.apache.doris.nereids.trees.plans.commands.CancelDecommissionBackendCommand;
import org.apache.doris.nereids.trees.plans.commands.info.AddBackendOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddBrokerOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddFollowerOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddObserverOp;
import org.apache.doris.nereids.trees.plans.commands.info.DecommissionBackendOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropAllBrokerOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropBackendOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropBrokerOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropFollowerOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropObserverOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyBackendOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyFrontendOrBackendHostNameOp;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.system.SystemInfoService.HostInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/*
 * SystemHandler is for
 * 1. add/drop/decommission backends
 * 2. add/drop frontends
 * 3. add/drop/modify brokers
 */
public class SystemHandler extends AlterHandler {
    private static final Logger LOG = LogManager.getLogger(SystemHandler.class);

    // backendId -> tabletId -> checkTime
    private Map<Long, Map<Long, Long>> backendLeakyTablets = Maps.newHashMap();

    public SystemHandler() {
        super("system");
    }

    @Override
    protected void runAfterCatalogReady() {
        super.runAfterCatalogReady();
        if (Config.isNotCloudMode()) {
            runAlterJobV2();
        }
    }

    // check all decommissioned backends, if there is no available tablet on that backend, drop it.
    private void runAlterJobV2() {
        SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        backendLeakyTablets.entrySet().removeIf(entry -> {
            long beId = entry.getKey();
            Backend backend = systemInfoService.getBackend(beId);
            return backend == null || !backend.isDecommissioned();
        });
        // check if decommission is finished
        for (Long beId : systemInfoService.getAllBackendIds(false)) {
            Backend backend = systemInfoService.getBackend(beId);
            if (backend == null || !backend.isDecommissioned()) {
                continue;
            }

            AtomicInteger totalTabletNum = new AtomicInteger(0);
            List<Long> sampleTablets = Lists.newArrayList();
            List<Long> sampleLeakyTablets = Lists.newArrayList();
            // check backend had migrated all its tablets, otherwise sample some tablets for log
            boolean migratedTablets = checkMigrateTablets(beId, 10, sampleTablets, sampleLeakyTablets, totalTabletNum);
            long walNum = Env.getCurrentEnv().getGroupCommitManager().getAllWalQueueSize(backend);
            if (Config.drop_backend_after_decommission && migratedTablets && walNum == 0) {
                try {
                    systemInfoService.dropBackend(beId);
                    LOG.info("no available tablet on decommission backend {}, drop it", beId);
                } catch (DdlException e) {
                    // does not matter, may be backend not exist
                    LOG.info("backend {} is dropped failed after decommission {}", beId, e.getMessage());
                }
                continue;
            }

            LOG.info("backend {} lefts {} replicas to decommission: normal tablets {}{}{}",
                    beId, totalTabletNum.get(), sampleTablets,
                    sampleLeakyTablets.isEmpty() ? "" : "; maybe leaky tablets " + sampleLeakyTablets,
                    walNum > 0 ? "; and has " + walNum + " unfinished WALs" : "");
        }
    }

    @Override
    public List<List<Comparable>> getAlterJobInfosByDb(Database db) {
        throw new NotImplementedException("getAlterJobInfosByDb is not supported in SystemHandler");
    }

    @Override
    // add synchronized to avoid process 2 or more stmts at same time
    public synchronized void process(String rawSql, List<AlterClause> alterClauses,
            Database dummyDb,
            OlapTable dummyTbl) throws UserException {
        Preconditions.checkArgument(alterClauses.size() == 1);
        AlterClause alterClause = alterClauses.get(0);

        if (alterClause instanceof AddBackendClause) {
            // add backend
            AddBackendClause addBackendClause = (AddBackendClause) alterClause;
            Env.getCurrentSystemInfo().addBackends(addBackendClause.getHostInfos(), addBackendClause.getTagMap());
        } else if (alterClause instanceof DropBackendClause) {
            // drop backend
            DropBackendClause dropBackendClause = (DropBackendClause) alterClause;
            if (!dropBackendClause.isForce()) {
                throw new DdlException("It is highly NOT RECOMMENDED to use DROP BACKEND stmt."
                        + "It is not safe to directly drop a backend. "
                        + "All data on this backend will be discarded permanently. "
                        + "If you insist, use DROPP instead of DROP");
            }
            if (dropBackendClause.getHostInfos().isEmpty()) {
                // drop by id
                Env.getCurrentSystemInfo().dropBackendsByIds(dropBackendClause.getIds());
            } else {
                // drop by host
                Env.getCurrentSystemInfo().dropBackends(dropBackendClause.getHostInfos());
            }
        } else if (alterClause instanceof DecommissionBackendClause) {
            // decommission
            DecommissionBackendClause decommissionBackendClause = (DecommissionBackendClause) alterClause;
            // check request
            List<Backend> decommissionBackends = checkDecommission(decommissionBackendClause);

            // set backend's state as 'decommissioned'
            // for decommission operation, here is no decommission job. the system handler will check
            // all backend in decommission state
            for (Backend backend : decommissionBackends) {
                Env.getCurrentSystemInfo().decommissionBackend(backend);
            }

        } else if (alterClause instanceof AddObserverClause) {
            AddObserverClause clause = (AddObserverClause) alterClause;
            Env.getCurrentEnv().addFrontend(FrontendNodeType.OBSERVER, clause.getHost(),
                    clause.getPort());
        } else if (alterClause instanceof DropObserverClause) {
            DropObserverClause clause = (DropObserverClause) alterClause;
            Env.getCurrentEnv().dropFrontend(FrontendNodeType.OBSERVER, clause.getHost(),
                    clause.getPort());
        } else if (alterClause instanceof AddFollowerClause) {
            AddFollowerClause clause = (AddFollowerClause) alterClause;
            Env.getCurrentEnv().addFrontend(FrontendNodeType.FOLLOWER, clause.getHost(),
                    clause.getPort());
        } else if (alterClause instanceof DropFollowerClause) {
            DropFollowerClause clause = (DropFollowerClause) alterClause;
            Env.getCurrentEnv().dropFrontend(FrontendNodeType.FOLLOWER, clause.getHost(),
                    clause.getPort());
        } else if (alterClause instanceof ModifyBrokerClause) {
            ModifyBrokerClause clause = (ModifyBrokerClause) alterClause;
            Env.getCurrentEnv().getBrokerMgr().execute(clause);
        } else if (alterClause instanceof ModifyBackendClause) {
            Env.getCurrentSystemInfo().modifyBackends(((ModifyBackendClause) alterClause));
        } else if (alterClause instanceof ModifyFrontendHostNameClause) {
            ModifyFrontendHostNameClause clause = (ModifyFrontendHostNameClause) alterClause;
            Env.getCurrentEnv().modifyFrontendHostName(clause.getHost(), clause.getPort(), clause.getNewHost());
        } else if (alterClause instanceof ModifyBackendHostNameClause) {
            Env.getCurrentSystemInfo().modifyBackendHost((ModifyBackendHostNameClause) alterClause);
        } else {
            Preconditions.checkState(false, alterClause.getClass());
        }
    }

    @Override
    // add synchronized to avoid process 2 or more stmts at same time
    public synchronized void processForNereids(String rawSql, List<AlterCommand> alterCommands,
                                     Database dummyDb,
                                     OlapTable dummyTbl) throws UserException {
        Preconditions.checkArgument(alterCommands.size() == 1);
        AlterCommand alterCommand = alterCommands.get(0);
        if (alterCommand instanceof AlterSystemCommand) {
            AlterSystemCommand alterSystemCommand = (AlterSystemCommand) alterCommand;
            if (alterSystemCommand.getType().equals(PlanType.ALTER_SYSTEM_ADD_BACKEND)) {
                // add backend
                AddBackendOp op = (AddBackendOp) alterSystemCommand.getAlterSystemOp();
                Env.getCurrentSystemInfo().addBackends(op.getHostInfos(), op.getTagMap());
            } else if (alterSystemCommand.getType().equals(PlanType.ALTER_SYSTEM_DROP_BACKEND)) {
                // drop backend
                DropBackendOp op = (DropBackendOp) alterSystemCommand.getAlterSystemOp();
                if (!op.isForce()) {
                    throw new DdlException("It is highly NOT RECOMMENDED to use DROP BACKEND stmt."
                            + "It is not safe to directly drop a backend. "
                            + "All data on this backend will be discarded permanently. "
                            + "If you insist, use DROPP instead of DROP");
                }
                if (op.getHostInfos().isEmpty()) {
                    // drop by id
                    Env.getCurrentSystemInfo().dropBackendsByIds(op.getIds());
                } else {
                    // drop by host
                    Env.getCurrentSystemInfo().dropBackends(op.getHostInfos());
                }
            } else if (alterSystemCommand.getType().equals(PlanType.ALTER_SYSTEM_DECOMMISSION_BACKEND)) {
                // decommission
                DecommissionBackendOp op = (DecommissionBackendOp) alterSystemCommand.getAlterSystemOp();
                // check request
                List<Backend> decommissionBackends = checkDecommissionForNereids(op);

                // set backend's state as 'decommissioned'
                // for decommission operation, here is no decommission job. the system handler will check
                // all backend in decommission state
                for (Backend backend : decommissionBackends) {
                    Env.getCurrentSystemInfo().decommissionBackend(backend);
                }
            } else if (alterSystemCommand.getType().equals(PlanType.ALTER_SYSTEM_ADD_OBSERVER)) {
                AddObserverOp op = (AddObserverOp) alterSystemCommand.getAlterSystemOp();
                Env.getCurrentEnv().addFrontend(FrontendNodeType.OBSERVER, op.getHost(), op.getPort());
            } else if (alterSystemCommand.getType().equals(PlanType.ALTER_SYSTEM_DROP_OBSERVER)) {
                DropObserverOp op = (DropObserverOp) alterSystemCommand.getAlterSystemOp();
                Env.getCurrentEnv().dropFrontend(FrontendNodeType.OBSERVER, op.getHost(), op.getPort());
            } else if (alterSystemCommand.getType().equals(PlanType.ALTER_SYSTEM_ADD_FOLLOWER)) {
                AddFollowerOp op = (AddFollowerOp) alterSystemCommand.getAlterSystemOp();
                Env.getCurrentEnv().addFrontend(FrontendNodeType.FOLLOWER, op.getHost(), op.getPort());
            } else if (alterSystemCommand.getType().equals(PlanType.ALTER_SYSTEM_DROP_FOLLOWER)) {
                DropFollowerOp op = (DropFollowerOp) alterSystemCommand.getAlterSystemOp();
                Env.getCurrentEnv().dropFrontend(FrontendNodeType.FOLLOWER, op.getHost(), op.getPort());
            } else if (alterSystemCommand.getType().equals(PlanType.ALTER_SYSTEM_ADD_BROKER)) {
                AddBrokerOp op = (AddBrokerOp) alterSystemCommand.getAlterSystemOp();
                Env.getCurrentEnv().getBrokerMgr().addBrokers(op.getBrokerName(), op.getHostPortPairs());
            } else if (alterSystemCommand.getType().equals(PlanType.ALTER_SYSTEM_DROP_BROKER)) {
                DropBrokerOp op = (DropBrokerOp) alterSystemCommand.getAlterSystemOp();
                Env.getCurrentEnv().getBrokerMgr().dropBrokers(op.getBrokerName(), op.getHostPortPairs());
            } else if (alterSystemCommand.getType().equals(PlanType.ALTER_SYSTEM_DROP_ALL_BROKER)) {
                DropAllBrokerOp op = (DropAllBrokerOp) alterSystemCommand.getAlterSystemOp();
                Env.getCurrentEnv().getBrokerMgr().dropAllBroker(op.getBrokerName());
            } else if (alterSystemCommand.getType().equals(PlanType.ALTER_SYSTEM_MODIFY_BACKEND)) {
                ModifyBackendOp op = (ModifyBackendOp) alterSystemCommand.getAlterSystemOp();
                Env.getCurrentSystemInfo().modifyBackends(op);
            } else if (alterSystemCommand.getType().equals(PlanType.ALTER_SYSTEM_MODIFY_FRONTEND_OR_BACKEND_HOSTNAME)) {
                ModifyFrontendOrBackendHostNameOp op =
                        (ModifyFrontendOrBackendHostNameOp) alterSystemCommand.getAlterSystemOp();
                if (op.getModifyOpType().equals(ModifyFrontendOrBackendHostNameOp.ModifyOpType.Frontend)) {
                    Env.getCurrentEnv().modifyFrontendHostName(op.getHost(), op.getPort(), op.getNewHost());
                } else {
                    Env.getCurrentSystemInfo().modifyBackendHostName(op.getHost(), op.getPort(), op.getNewHost());
                }
            } else {
                Preconditions.checkState(false, alterCommand.getClass());
            }
        } else {
            throw new UserException("Not supported alter command type " + alterCommand.getType());
        }

    }

    /*
     * check if the specified backends can be dropped
     * 1. backend does not have any tablet.
     * 2. or all tablets in backend have been recycled or been leaky for a long time.
     *
     * and return some sample tablets for log.
     *
     * sampleLimit: the max sample tablet num
     * sampleTablets: sample normal tablets
     * sampleLeakyTablets: sample leaky tablets
     *
     */
    private boolean checkMigrateTablets(long beId, int sampleLimit, List<Long> sampleTablets,
            List<Long> sampleLeakyTablets, AtomicInteger totalTabletNum) {
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        List<Long> backendTabletIds = invertedIndex.getTabletIdsByBackendId(beId);
        totalTabletNum.set(backendTabletIds.size());
        if (backendTabletIds.isEmpty()) {
            return true;
        }
        // if too many tablets, no check for efficiency
        if (backendTabletIds.size() > Config.decommission_tablet_check_threshold) {
            backendTabletIds.stream().limit(sampleLimit).forEach(sampleTablets::add);
            return false;
        }
        // dbId -> tableId -> partitionId -> tablet list
        Map<Long, Map<Long, Map<Long, List<Long>>>> tabletsMap = Maps.newHashMap();
        List<TabletMeta> tabletMetaList = invertedIndex.getTabletMetaList(backendTabletIds);
        for (int i = 0; i < backendTabletIds.size(); i++) {
            long tabletId = backendTabletIds.get(i);
            TabletMeta tabletMeta = tabletMetaList.get(i);
            if (tabletMeta == TabletInvertedIndex.NOT_EXIST_TABLET_META) {
                continue;
            }
            tabletsMap.computeIfAbsent(tabletMeta.getDbId(), k -> Maps.newHashMap())
                    .computeIfAbsent(tabletMeta.getTableId(), k -> Maps.newHashMap())
                    .computeIfAbsent(tabletMeta.getPartitionId(), k -> Lists.newArrayList())
                    .add(tabletId);
        }
        InternalCatalog catalog = Env.getCurrentInternalCatalog();
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();
        long now = System.currentTimeMillis();
        Map<Long, Long> leakyTablets = Maps.newHashMap();
        boolean searchedFirstTime = !backendLeakyTablets.containsKey(beId);
        Map<Long, Long> lastLeakyTablets = backendLeakyTablets.computeIfAbsent(beId, k -> Maps.newHashMap());
        backendLeakyTablets.put(beId, leakyTablets);
        Consumer<List<Long>> addPartitionLeakyTablets = tabletsOfPartition -> {
            tabletsOfPartition.forEach(tabletId -> {
                leakyTablets.put(tabletId, lastLeakyTablets.getOrDefault(tabletId, now));
            });
        };
        Consumer<Map<Long, List<Long>>> addTableLeakyTablets = tabletsOfTable -> {
            tabletsOfTable.values().forEach(addPartitionLeakyTablets);
        };
        Consumer<Map<Long, Map<Long, List<Long>>>> addDbLeakyTablets = tabletsOfDb -> {
            tabletsOfDb.values().forEach(addTableLeakyTablets);
        };

        // Search backend's tablets, put 10 normal tablets into sampleTablets, put leaky tablets into leakyTablets.
        // For the first time search, it will search all this backend's tablets.
        // For later search, it only search at most 10 normal tablets, in order to reduce lock table.
        boolean searchedAllTablets = true;
        OUTER:
        for (Map.Entry<Long, Map<Long, Map<Long, List<Long>>>> dbEntry : tabletsMap.entrySet()) {
            long dbId = dbEntry.getKey();
            Database db = catalog.getDbNullable(dbId);
            if (db == null) {
                // not found db, and it's not in recyle bin, then it should be leaky.
                if (!recycleBin.isRecycleDatabase(dbId)) {
                    addDbLeakyTablets.accept(dbEntry.getValue());
                }
                continue;
            }

            for (Map.Entry<Long, Map<Long, List<Long>>> tableEntry : dbEntry.getValue().entrySet()) {
                long tableId = tableEntry.getKey();
                Table tbl = db.getTableNullable(tableId);
                if (tbl == null || !tbl.isManagedTable()) {
                    if (!recycleBin.isRecycleTable(dbId, tableId)) {
                        addTableLeakyTablets.accept(tableEntry.getValue());
                    }
                    continue;
                }

                OlapTable olapTable = (OlapTable) tbl;
                olapTable.readLock();
                try {
                    for (Map.Entry<Long, List<Long>> partitionEntry : tableEntry.getValue().entrySet()) {
                        long partitionId = partitionEntry.getKey();
                        Partition partition = olapTable.getPartition(partitionId);
                        if (partition == null) {
                            if (!recycleBin.isRecyclePartition(dbId, tableId, partitionId)) {
                                addPartitionLeakyTablets.accept(partitionEntry.getValue());
                            }
                            continue;
                        }
                        // at present, the leaky tablets are belong to a not-found partition.
                        // so if a partition is in a table, no more check this partition really contains this tablet,
                        // just treat this tablet as no leaky.
                        for (long tabletId : partitionEntry.getValue()) {
                            if (sampleTablets.size() < sampleLimit) {
                                sampleTablets.add(tabletId);
                            } else if (!searchedFirstTime) {
                                // First time will search all tablets,
                                // The later search will stop searching after found 10 normal tablets
                                // in order to reduce table lock.
                                searchedAllTablets = false;
                                break OUTER;
                            }
                        }
                    }
                } finally {
                    olapTable.readUnlock();
                }
            }
        }

        if (!searchedAllTablets) {
            // due to not search all tablets, it may miss some leaky tablets.
            // so we add the leaky tablets of the last time search.
            // it can infer that leakyTablets will contains all leaky tablets of the first time search.
            // And we know that the first time it searched all tablets.
            leakyTablets.putAll(lastLeakyTablets);
        }
        leakyTablets.keySet().stream().limit(sampleLimit).forEach(sampleLeakyTablets::add);

        // If a tablet can't be found in path 'db -> table -> partition', and it's not in recyle bin,
        // we treat this tablet as leaky, but it maybe not real leaky.
        // The onflight creating new partiton/table may let its tablets seem like leaky temporarily.
        // For example, when creatting a new partition, firstly its tablets will add to TabletInvertedIndex.
        // But at this moment, the partition hadn't add to table, so search the tablet with path
        // 'db -> table -> partition' will failed. Only after finish creating, the partition will add to the table.
        //
        // So the onflight new tablet maynot be real leaky. Need to wait for a time to confirm they are real leaky.
        long skipLeakyTs = now - Config.decommission_skip_leaky_tablet_second * 1000L;

        // if a backend no normal tablets (sampleTablets size = 0), and leaky tablets had been leaky for a long time,
        // then can drop it now.
        return sampleTablets.isEmpty() && leakyTablets.values().stream().allMatch(ts -> ts < skipLeakyTs);
    }

    private List<Backend> checkDecommission(DecommissionBackendClause decommissionBackendClause)
            throws DdlException {
        if (decommissionBackendClause.getHostInfos().isEmpty()) {
            return checkDecommissionByIds(decommissionBackendClause.getIds());
        }
        return checkDecommission(decommissionBackendClause.getHostInfos());
    }

    private List<Backend> checkDecommissionForNereids(DecommissionBackendOp decommissionBackendOp)
            throws DdlException {
        if (decommissionBackendOp.getHostInfos().isEmpty()) {
            return checkDecommissionByIds(decommissionBackendOp.getIds());
        }
        return checkDecommission(decommissionBackendOp.getHostInfos());
    }

    /*
     * check if the specified backends can be decommissioned
     * 1. backend should exist.
     * 2. after decommission, the remaining backend num should meet the replication num.
     * 3. after decommission, The remaining space capacity can store data on decommissioned backends.
     */
    public static List<Backend> checkDecommission(List<HostInfo> hostInfos)
            throws DdlException {
        SystemInfoService infoService = Env.getCurrentSystemInfo();
        List<Backend> decommissionBackends = Lists.newArrayList();
        // check if exist
        for (HostInfo hostInfo : hostInfos) {
            Backend backend = infoService.getBackendWithHeartbeatPort(hostInfo.getHost(),
                    hostInfo.getPort());
            if (backend == null) {
                throw new DdlException("Backend does not exist["
                        + NetUtils.getHostPortInAccessibleFormat(hostInfo.getHost(), hostInfo.getPort()) + "]");
            }
            if (backend.isDecommissioned()) {
                // already under decommission, ignore it
                continue;
            }
            decommissionBackends.add(backend);
        }

        checkDecommissionWithReplicaAllocation(decommissionBackends);

        // TODO(cmy): check remaining space

        return decommissionBackends;
    }

    public static List<Backend> checkDecommissionByIds(List<String> ids)
            throws DdlException {
        SystemInfoService infoService = Env.getCurrentSystemInfo();
        List<Backend> decommissionBackends = Lists.newArrayList();
        // check if exist
        for (String id : ids) {
            Backend backend = infoService.getBackend(Long.parseLong(id));
            if (backend == null) {
                throw new DdlException("Backend does not exist, backend id is " + id);
            }
            if (backend.isDecommissioned()) {
                // already under decommission, ignore it
                continue;
            }
            decommissionBackends.add(backend);
        }

        checkDecommissionWithReplicaAllocation(decommissionBackends);

        // TODO(cmy): check remaining space

        return decommissionBackends;
    }

    private static void checkDecommissionWithReplicaAllocation(List<Backend> decommissionBackends)
            throws DdlException {
        if (Config.isCloudMode() || decommissionBackends.isEmpty()
                || DebugPointUtil.isEnable("SystemHandler.decommission_no_check_replica_num")) {
            return;
        }

        Set<Tag> decommissionTags = decommissionBackends.stream().map(be -> be.getLocationTag())
                .collect(Collectors.toSet());
        Map<Tag, Integer> tagAvailBackendNums = Maps.newHashMap();
        List<Backend> bes;
        try {
            bes = Env.getCurrentSystemInfo().getBackendsByCurrentCluster().values().asList();
        } catch (UserException e) {
            LOG.warn("Failed to get current cluster backend by current cluster.", e);
            return;
        }

        for (Backend backend : bes) {
            long beId = backend.getId();
            if (!backend.isScheduleAvailable()
                    || decommissionBackends.stream().anyMatch(be -> be.getId() == beId)) {
                continue;
            }

            Tag tag = backend.getLocationTag();
            if (tag != null) {
                tagAvailBackendNums.put(tag, tagAvailBackendNums.getOrDefault(tag, 0) + 1);
            }
        }

        Env env = Env.getCurrentEnv();
        List<Long> dbIds = env.getInternalCatalog().getDbIds();
        for (Long dbId : dbIds) {
            Database db = env.getInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }

            if (db instanceof MysqlCompatibleDatabase) {
                continue;
            }

            for (Table table : db.getTables()) {
                table.readLock();
                try {
                    if (!table.isManagedTable()) {
                        continue;
                    }

                    OlapTable tbl = (OlapTable) table;
                    for (Partition partition : tbl.getAllPartitions()) {
                        ReplicaAllocation replicaAlloc = tbl.getPartitionInfo().getReplicaAllocation(partition.getId());
                        for (Map.Entry<Tag, Short> entry : replicaAlloc.getAllocMap().entrySet()) {
                            Tag tag = entry.getKey();
                            if (!decommissionTags.contains(tag)) {
                                continue;
                            }
                            int replicaNum = (int) entry.getValue();
                            int backendNum = tagAvailBackendNums.getOrDefault(tag, 0);
                            if (replicaNum > backendNum) {
                                throw new DdlException("After decommission, partition " + partition.getName()
                                        + " of table " + db.getName() + "." + tbl.getName()
                                        + " 's replication allocation { " + replicaAlloc
                                        + " } > available backend num " + backendNum + " on tag " + tag
                                        + ", otherwise need to decrease the partition's replication num.");
                            }
                        }
                    }
                } finally {
                    table.readUnlock();
                }
            }
        }
    }

    public void cancel(CancelDecommissionBackendCommand command) throws DdlException {
        SystemInfoService infoService = Env.getCurrentSystemInfo();
        // check if backends is under decommission
        List<HostInfo> hostInfos = command.getHostInfos();
        if (hostInfos.isEmpty()) {
            List<String> ids = command.getIds();
            for (String id : ids) {
                Backend backend = infoService.getBackend(Long.parseLong(id));
                if (backend == null) {
                    throw new DdlException("Backend does not exist["
                        + id + "]");
                }
                if (!backend.isDecommissioned()) {
                    // it's ok. just log
                    LOG.info("backend is not decommissioned[{}]", backend.getId());
                    continue;
                }
                if (backend.setDecommissioned(false)) {
                    Env.getCurrentEnv().getEditLog().logBackendStateChange(backend);
                } else {
                    LOG.info("backend is not decommissioned[{}]", backend.getHost());
                }
            }

        } else {
            for (HostInfo hostInfo : hostInfos) {
                // check if exist
                Backend backend = infoService.getBackendWithHeartbeatPort(hostInfo.getHost(),
                        hostInfo.getPort());
                if (backend == null) {
                    throw new DdlException("Backend does not exist["
                        + NetUtils.getHostPortInAccessibleFormat(hostInfo.getHost(), hostInfo.getPort()) + "]");
                }

                if (!backend.isDecommissioned()) {
                    // it's ok. just log
                    LOG.info("backend is not decommissioned[{}]", backend.getId());
                    continue;
                }

                if (backend.setDecommissioned(false)) {
                    Env.getCurrentEnv().getEditLog().logBackendStateChange(backend);
                } else {
                    LOG.info("backend is not decommissioned[{}]", backend.getHost());
                }
            }
        }
    }
}
