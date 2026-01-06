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

import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.clone.TabletSchedCtx;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * This class represents the olap tablet related metadata.
 */
public class Tablet extends MetaObject {
    private static final Logger LOG = LogManager.getLogger(Tablet.class);
    // if current version count of replica is mor than
    // QUERYABLE_TIMES_OF_MIN_VERSION_COUNT times the minimum version count,
    // then the replica would not be considered as queryable.
    private static final int QUERYABLE_TIMES_OF_MIN_VERSION_COUNT = 3;

    public enum TabletStatus {
        HEALTHY,
        REPLICA_MISSING, // not enough alive replica num.
        VERSION_INCOMPLETE, // alive replica num is enough, but version is missing.
        REPLICA_RELOCATING, // replica is healthy, but is under relocating (eg. BE is decommission).
        REDUNDANT, // too much replicas.
        REPLICA_MISSING_FOR_TAG, // not enough healthy replicas in backend with specified tag.
        FORCE_REDUNDANT, // some replica is missing or bad, but there is no other backends for repair,
        // at least one replica has to be deleted first to make room for new replica.
        COLOCATE_MISMATCH, // replicas do not all locate in right colocate backends set.
        COLOCATE_REDUNDANT, // replicas match the colocate backends set, but redundant.
        NEED_FURTHER_REPAIR, // one of replicas need a definite repair.
        UNRECOVERABLE,   // none of replicas are healthy
        REPLICA_COMPACTION_TOO_SLOW // one replica's version count is much more than other replicas;
    }

    public static class TabletHealth {
        public TabletStatus status;
        public TabletSchedCtx.Priority priority;

        // num of alive replica with version complete
        public int aliveAndVersionCompleteNum;

        // NEED_FURTHER_REPAIR replica id
        public long needFurtherRepairReplicaId;

        // has alive replica with version incomplete, prior to repair these replica
        public boolean hasAliveAndVersionIncomplete;

        // this tablet recent write failed, then increase its sched priority
        public boolean hasRecentLoadFailed;

        // this tablet want to add new replica, but not found target backend.
        public boolean noPathForNewReplica;

        public TabletHealth() {
            status = null; // don't set for balance task
            priority = TabletSchedCtx.Priority.NORMAL;
            aliveAndVersionCompleteNum = 0;
            needFurtherRepairReplicaId = -1L;
            hasAliveAndVersionIncomplete = false;
            hasRecentLoadFailed = false;
            noPathForNewReplica = false;
        }
    }

    @SerializedName(value = "id")
    protected long id;
    @SerializedName(value = "rs", alternate = {"replicas"})
    protected List<Replica> replicas;

    public Tablet() {
        this(0L, new ArrayList<>());
    }

    public Tablet(long tabletId) {
        this(tabletId, new ArrayList<>());
    }

    private Tablet(long tabletId, List<Replica> replicas) {
        this.id = tabletId;
        this.replicas = replicas;
        if (this.replicas == null) {
            this.replicas = new ArrayList<>();
        }
    }

    public long getId() {
        return this.id;
    }

    public long getCheckedVersion() {
        return -1;
    }

    public void setCheckedVersion(long checkedVersion) {
        throw new UnsupportedOperationException("not support setCheckedVersion in Tablet");
    }

    public void setIsConsistent(boolean good) {
        throw  new UnsupportedOperationException("not support setIsConsistent in Tablet");
    }

    public boolean isConsistent() {
        return true;
    }

    public void setCooldownConf(long cooldownReplicaId, long cooldownTerm) {
        throw new UnsupportedOperationException("not support setCooldownConf in Tablet");
    }

    public long getCooldownReplicaId() {
        return -1;
    }

    public Pair<Long, Long> getCooldownConf() {
        return Pair.of(-1L, -1L);
    }

    protected boolean isLatestReplicaAndDeleteOld(Replica newReplica) {
        boolean delete = false;
        boolean hasBackend = false;
        long version = newReplica.getVersion();
        Iterator<Replica> iterator = replicas.iterator();
        while (iterator.hasNext()) {
            Replica replica = iterator.next();
            if (replica.getBackendIdWithoutException() == newReplica.getBackendIdWithoutException()) {
                hasBackend = true;
                if (replica.getVersion() <= version) {
                    iterator.remove();
                    delete = true;
                }
            }
        }

        return delete || !hasBackend;
    }

    public void addReplica(Replica replica, boolean isRestore) {
        if (isLatestReplicaAndDeleteOld(replica)) {
            replicas.add(replica);
            if (!isRestore) {
                Env.getCurrentInvertedIndex().addReplica(id, replica);
            }
        }
    }

    public void addReplica(Replica replica) {
        addReplica(replica, false);
    }

    public List<Replica> getReplicas() {
        return this.replicas;
    }

    public Set<Long> getBackendIds() {
        Set<Long> beIds = Sets.newHashSet();
        for (Replica replica : replicas) {
            beIds.add(replica.getBackendIdWithoutException());
        }
        return beIds;
    }

    public List<Long> getNormalReplicaBackendIds() {
        try {
            return Lists.newArrayList(getNormalReplicaBackendPathMap().keySet());
        } catch (Exception e) {
            LOG.warn("failed to getNormalReplicaBackendIds", e);
            return Lists.newArrayList();
        }
    }

    @FunctionalInterface
    interface BackendIdGetter {
        long get(Replica rep, String be) throws UserException;
    }

    private Multimap<Long, Long> getNormalReplicaBackendPathMapImpl(String beEndpoint, BackendIdGetter idGetter)
            throws UserException {
        Multimap<Long, Long> map = HashMultimap.create();
        SystemInfoService infoService = Env.getCurrentSystemInfo();
        for (Replica replica : replicas) {
            long backendId = idGetter.get(replica, beEndpoint);
            if (!infoService.checkBackendAlive(backendId)) {
                continue;
            }

            if (replica.isBad()) {
                continue;
            }

            ReplicaState state = replica.getState();
            if (state.canLoad()
                    || (state == ReplicaState.DECOMMISSION
                            && replica.getPostWatermarkTxnId() < 0
                            && replica.getLastFailedVersion() < 0)) {
                map.put(backendId, replica.getPathHash());
            }
        }
        return map;
    }

    // return map of (BE id -> path hash) of normal replicas
    // for load plan.
    public Multimap<Long, Long> getNormalReplicaBackendPathMap() throws UserException {
        return getNormalReplicaBackendPathMapImpl(null, (rep, be) -> rep.getBackendId());
    }

    // for cloud mode without ConnectContext. use BE IP to find replica
    protected Multimap<Long, Long> getNormalReplicaBackendPathMapCloud(String beEndpoint) throws UserException {
        return getNormalReplicaBackendPathMapImpl(beEndpoint,
                (rep, be) -> ((CloudReplica) rep).getBackendId(be));
    }

    // When a BE reports a missing version, lastFailedVersion is set. When a write fails on a replica,
    // lastFailedVersion is set.
    // for query
    public List<Replica> getQueryableReplicas(long visibleVersion, Map<Long, Set<Long>> backendAlivePathHashs,
            boolean allowMissingVersion) {
        int replicaNum = replicas.size();
        List<Replica> allQueryableReplica = Lists.newArrayListWithCapacity(replicaNum);
        List<Replica> auxiliaryReplica = Lists.newArrayListWithCapacity(replicaNum);
        List<Replica> deadPathReplica = Lists.newArrayListWithCapacity(replicaNum);
        List<Replica> mayMissingVersionReplica = Lists.newArrayListWithCapacity(replicaNum);
        List<Replica> notCatchupReplica = Lists.newArrayListWithCapacity(replicaNum);

        for (Replica replica : replicas) {
            if (replica.isBad()) {
                continue;
            }
            if (!replica.checkVersionCatchUp(visibleVersion, false)) {
                notCatchupReplica.add(replica);
                continue;
            }
            if (replica.getLastFailedVersion() > 0) {
                mayMissingVersionReplica.add(replica);
                continue;
            }

            Set<Long> thisBeAlivePaths = backendAlivePathHashs.get(replica.getBackendIdWithoutException());
            ReplicaState state = replica.getState();
            // if thisBeAlivePaths contains pathHash = 0, it mean this be hadn't report disks state.
            // should ignore this case.
            if (replica.getPathHash() != -1 && thisBeAlivePaths != null
                    && !thisBeAlivePaths.contains(replica.getPathHash())
                    && !thisBeAlivePaths.contains(0L)) {
                deadPathReplica.add(replica);
            } else if (state.canQuery()) {
                allQueryableReplica.add(replica);
            } else if (state == ReplicaState.DECOMMISSION) {
                auxiliaryReplica.add(replica);
            }
        }

        if (allQueryableReplica.isEmpty()) {
            allQueryableReplica = auxiliaryReplica;
        }
        if (allQueryableReplica.isEmpty()) {
            allQueryableReplica = deadPathReplica;
        }

        if (allQueryableReplica.isEmpty()) {
            // If be misses a version, be would report failure.
            allQueryableReplica = mayMissingVersionReplica;
        }

        if (allQueryableReplica.isEmpty() && allowMissingVersion) {
            allQueryableReplica = notCatchupReplica;
        }

        if (Config.skip_compaction_slower_replica && allQueryableReplica.size() > 1) {
            long minVersionCount = Long.MAX_VALUE;
            for (Replica replica : allQueryableReplica) {
                long visibleVersionCount = replica.getVisibleVersionCount();
                if (visibleVersionCount != 0 && visibleVersionCount < minVersionCount) {
                    minVersionCount = visibleVersionCount;
                }
            }
            long maxVersionCount = Config.min_version_count_indicate_replica_compaction_too_slow;
            if (minVersionCount != Long.MAX_VALUE) {
                maxVersionCount = Math.max(maxVersionCount, minVersionCount * QUERYABLE_TIMES_OF_MIN_VERSION_COUNT);
            }

            List<Replica> lowerVersionReplicas = Lists.newArrayListWithCapacity(allQueryableReplica.size());
            for (Replica replica : allQueryableReplica) {
                if (replica.getVisibleVersionCount() < maxVersionCount) {
                    lowerVersionReplicas.add(replica);
                }
            }
            return lowerVersionReplicas;
        }
        return allQueryableReplica;
    }

    public String getDetailsStatusForQuery(long visibleVersion) {
        StringBuilder sb = new StringBuilder("Visible Replicas:");
        sb.append("Visible version: ").append(visibleVersion);
        sb.append(", Replicas: ");
        sb.append(Joiner.on(", ").join(replicas.stream().map(replica -> replica.toStringSimple(true))
                .collect(Collectors.toList())));
        sb.append(".");

        return sb.toString();
    }

    public Replica getReplicaById(long replicaId) {
        for (Replica replica : replicas) {
            if (replica.getId() == replicaId) {
                return replica;
            }
        }
        return null;
    }

    public Replica getReplicaByBackendId(long backendId) {
        for (Replica replica : replicas) {
            if (replica.getBackendIdWithoutException() == backendId) {
                return replica;
            }
        }
        return null;
    }

    public boolean deleteReplica(Replica replica) {
        if (replicas.contains(replica)) {
            replicas.remove(replica);
            Env.getCurrentInvertedIndex().deleteReplica(id, replica.getBackendIdWithoutException());
            return true;
        }
        return false;
    }

    public boolean deleteReplicaByBackendId(long backendId) {
        Iterator<Replica> iterator = replicas.iterator();
        while (iterator.hasNext()) {
            Replica replica = iterator.next();
            if (replica.getBackendIdWithoutException() == backendId) {
                iterator.remove();
                Env.getCurrentInvertedIndex().deleteReplica(id, backendId);
                return true;
            }
        }
        return false;
    }

    @Deprecated
    public Replica deleteReplicaById(long replicaId) {
        Iterator<Replica> iterator = replicas.iterator();
        while (iterator.hasNext()) {
            Replica replica = iterator.next();
            if (replica.getId() == replicaId) {
                LOG.info("delete replica[" + replica.getId() + "]");
                iterator.remove();
                return replica;
            }
        }
        return null;
    }

    // for test,
    // and for some replay cases
    public void clearReplica() {
        this.replicas.clear();
    }

    public void setTabletId(long tabletId) {
        this.id = tabletId;
    }

    @Override
    public String toString() {
        return "tabletId=" + this.id;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Tablet)) {
            return false;
        }

        Tablet tablet = (Tablet) obj;

        if (replicas != tablet.replicas) {
            if (replicas.size() != tablet.replicas.size()) {
                return false;
            }
            int size = replicas.size();
            for (int i = 0; i < size; i++) {
                if (!tablet.replicas.contains(replicas.get(i))) {
                    return false;
                }
            }
        }
        return id == tablet.id;
    }

    // ATTN: Replica::getDataSize may zero in cloud and non-cloud
    // due to dataSize not write to image
    public long getDataSize(boolean singleReplica, boolean filterSizeZero) {
        LongStream s = replicas.stream().filter(r -> r.getState() == ReplicaState.NORMAL)
                .filter(r -> !filterSizeZero || r.getDataSize() > 0)
                .mapToLong(Replica::getDataSize);
        return singleReplica ? Double.valueOf(s.average().orElse(0)).longValue() : s.sum();
    }

    public long getRemoteDataSize() {
        return 0;
    }

    public long getRowCount(boolean singleReplica) {
        LongStream s = replicas.stream().filter(r -> r.getState() == ReplicaState.NORMAL)
                .mapToLong(Replica::getRowCount);
        return singleReplica ? Double.valueOf(s.average().orElse(0)).longValue() : s.sum();
    }

    // Get the least row count among all valid replicas.
    // The replica with the least row count is the most accurate one. Because it performs most compaction.
    public long getMinReplicaRowCount(long version) {
        long minRowCount = Long.MAX_VALUE;
        long maxReplicaVersion = 0;
        for (Replica r : replicas) {
            if (r.isAlive()
                    && r.checkVersionCatchUp(version, false)
                    && (r.getVersion() > maxReplicaVersion
                        || r.getVersion() == maxReplicaVersion && r.getRowCount() < minRowCount)) {
                minRowCount = r.getRowCount();
                maxReplicaVersion = r.getVersion();
            }
        }
        return minRowCount == Long.MAX_VALUE ? 0 : minRowCount;
    }

    public TabletHealth getHealth(SystemInfoService systemInfoService,
            long visibleVersion, ReplicaAllocation replicaAlloc, List<Long> aliveBeIds) {
        throw new UnsupportedOperationException("not support getHealth in Tablet");
    }

    public TabletHealth getColocateHealth(long visibleVersion,
            ReplicaAllocation replicaAlloc, Set<Long> backendsSet) {
        throw new UnsupportedOperationException("not support getColocateHealth in Tablet");
    }

    public boolean readyToBeRepaired(SystemInfoService infoService, TabletSchedCtx.Priority priority) {
        throw new UnsupportedOperationException("not support readyToBeRepaired in Tablet");
    }

    public void setLastStatusCheckTime(long lastStatusCheckTime) {
        throw new UnsupportedOperationException("not support setLastStatusCheckTime in Tablet");
    }

    public long getLastLoadFailedTime() {
        return -1;
    }

    public void setLastLoadFailedTime(long lastLoadFailedTime) {
        throw new UnsupportedOperationException("not support setLastLoadFailedTime in Tablet");
    }

    public void setLastTimeNoPathForNewReplica(long lastTimeNoPathForNewReplica) {
        throw new UnsupportedOperationException("not support setLastTimeNoPathForNewReplica in Tablet");
    }
}
