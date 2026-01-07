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

import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TTabletInfo;
import org.apache.doris.thrift.TUniqueId;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;

/**
 * This class represents the olap replica related metadata.
 */
public class Replica {
    private static final Logger LOG = LogManager.getLogger(Replica.class);
    public static final LastSuccessVersionComparator<Replica> LAST_SUCCESS_VERSION_COMPARATOR =
            new LastSuccessVersionComparator<Replica>();
    public static final IdComparator<Replica> ID_COMPARATOR = new IdComparator<Replica>();

    public enum ReplicaState {
        NORMAL,
        @Deprecated
        ROLLUP,
        @Deprecated
        SCHEMA_CHANGE,
        CLONE,
        ALTER, // replica is under rollup or schema change
        DECOMMISSION, // replica is ready to be deleted
        COMPACTION_TOO_SLOW; // replica version count is too large


        public boolean canLoad() {
            return this == NORMAL || this == SCHEMA_CHANGE || this == ALTER || this == COMPACTION_TOO_SLOW;
        }

        public boolean canQuery() {
            return this == NORMAL || this == SCHEMA_CHANGE;
        }
    }

    public enum ReplicaStatus {
        OK, // health
        DEAD, // backend is not available
        VERSION_ERROR, // missing version
        MISSING, // replica does not exist
        SCHEMA_ERROR, // replica's schema hash does not equal to index's schema hash
        BAD, // replica is broken.
        DROP,  // user force drop replica on this backend
    }

    public static class ReplicaContext {
        public long replicaId;
        public long backendId;
        public ReplicaState state;
        public long version;
        public int schemaHash;
        public long dbId;
        public long tableId;
        public long partitionId;
        public long indexId;
        public Replica originReplica;
    }

    @SerializedName(value = "id")
    private long id;
    // the version could be queried
    @SerializedName(value = "v", alternate = {"version"})
    private volatile long version;
    private int schemaHash = -1;
    @SerializedName(value = "ds", alternate = {"dataSize"})
    private volatile long dataSize = 0;
    @SerializedName(value = "rc", alternate = {"rowCount"})
    private volatile long rowCount = 0;
    @SerializedName(value = "st", alternate = {"state"})
    private volatile ReplicaState state;

    // the last load successful version
    @SerializedName(value = "lsv", alternate = {"lastSuccessVersion"})
    private long lastSuccessVersion = -1L;

    @Setter
    @Getter
    @SerializedName(value = "lis", alternate = {"localInvertedIndexSize"})
    private Long localInvertedIndexSize = 0L;
    @Setter
    @Getter
    @SerializedName(value = "lss", alternate = {"localSegmentSize"})
    private Long localSegmentSize = 0L;

    private volatile long totalVersionCount = -1;
    private volatile long visibleVersionCount = -1;

    // bad means this Replica is unrecoverable, and we will delete it
    private boolean bad = false;

    public Replica() {
    }

    public Replica(ReplicaContext context) {
        this(context.replicaId, context.backendId, context.state, context.version, context.schemaHash);
    }

    // for rollup
    // the new replica's version is -1 and last failed version is -1
    public Replica(long replicaId, long backendId, int schemaHash, ReplicaState state) {
        this(replicaId, backendId, -1, schemaHash, 0L, 0L, 0L, state, -1, -1);
    }

    // for create tablet and restore
    public Replica(long replicaId, long backendId, ReplicaState state, long version, int schemaHash) {
        this(replicaId, backendId, version, schemaHash, 0L, 0L, 0L, state, -1L, version);
    }

    public Replica(long replicaId, long backendId, long version, int schemaHash,
                       long dataSize, long remoteDataSize, long rowCount, ReplicaState state,
                       long lastFailedVersion,
                       long lastSuccessVersion) {
        this.id = replicaId;
        this.version = version;
        this.schemaHash = schemaHash;

        this.dataSize = dataSize;
        this.rowCount = rowCount;
        this.state = state;
        if (this.state == null) {
            this.state = ReplicaState.NORMAL;
        }
        if (lastSuccessVersion < this.version) {
            this.lastSuccessVersion = this.version;
        } else {
            this.lastSuccessVersion = lastSuccessVersion;
        }
    }

    public long getVersion() {
        return this.version;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    // for compatibility
    public void setSchemaHash(int schemaHash) {
        this.schemaHash = schemaHash;
    }

    public long getId() {
        return this.id;
    }

    public long getBackendIdWithoutException() {
        try {
            return getBackendId();
        } catch (UserException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("getBackendIdWithoutException: ", e);
            }
            return -1;
        }
    }

    public long getBackendId() throws UserException {
        return -1L;
    }

    protected long getBackendIdValue() {
        return -1L;
    }

    public void setBackendId(long backendId) {
        throw new UnsupportedOperationException("setBackendId is not supported in Replica");
    }

    public long getDataSize() {
        return dataSize;
    }

    public void setDataSize(long dataSize) {
        this.dataSize = dataSize;
    }

    public long getRemoteDataSize() {
        return 0;
    }

    public void setRemoteDataSize(long remoteDataSize) {
        if (remoteDataSize > 0) {
            throw new UnsupportedOperationException("setRemoteDataSize is not supported in Replica");
        }
    }

    public Long getRemoteInvertedIndexSize() {
        return 0L;
    }

    public void setRemoteInvertedIndexSize(long remoteInvertedIndexSize) {
        if (remoteInvertedIndexSize > 0) {
            throw new UnsupportedOperationException("setRemoteInvertedIndexSize is not supported in Replica");
        }
    }

    public Long getRemoteSegmentSize() {
        return 0L;
    }

    public void setRemoteSegmentSize(long remoteSegmentSize) {
        if (remoteSegmentSize > 0) {
            throw new UnsupportedOperationException("setRemoteSegmentSize is not supported in Replica");
        }
    }

    public long getRowCount() {
        return rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public long getSegmentCount() {
        throw new UnsupportedOperationException("getSegmentCount is not supported in Replica");
    }

    public void setSegmentCount(long segmentCount) {
        throw new UnsupportedOperationException("setSegmentCount is not supported in Replica");
    }

    public long getRowsetCount() {
        throw new UnsupportedOperationException("getRowsetCount is not supported in Replica");
    }

    public void setRowsetCount(long rowsetCount) {
        throw new UnsupportedOperationException("setRowCount is not supported in Replica");
    }

    public long getLastFailedVersion() {
        return -1;
    }

    public long getLastFailedTimestamp() {
        return 0;
    }

    public long getLastSuccessVersion() {
        return lastSuccessVersion;
    }

    public long getPathHash() {
        return -1;
    }

    public void setPathHash(long pathHash) {
        throw new UnsupportedOperationException("setPathHash is not supported in Replica");
    }

    public boolean isBad() {
        return bad;
    }

    public boolean setBad(boolean bad) {
        if (this.bad == bad) {
            return false;
        }
        this.bad = bad;
        return true;
    }

    public TUniqueId getCooldownMetaId() {
        return null;
    }

    public void setCooldownMetaId(TUniqueId cooldownMetaId) {
        throw new UnsupportedOperationException("setCooldownMetaId is not supported in Replica");
    }

    public long getCooldownTerm() {
        return -1;
    }

    public void setCooldownTerm(long cooldownTerm) {
        throw new UnsupportedOperationException("setCooldownTerm is not supported in Replica");
    }

    public boolean needFurtherRepair() {
        return false;
    }

    public void setNeedFurtherRepair(boolean needFurtherRepair) {
        if (needFurtherRepair) {
            throw new UnsupportedOperationException("setNeedFurtherRepair is not supported in Replica");
        }
    }

    public void incrFurtherRepairCount() {
        throw new UnsupportedOperationException("incrFurtherRepairCount is not supported in Replica");
    }

    public int getLeftFurtherRepairCount() {
        return 0;
    }

    public long getFurtherRepairWatermarkTxnTd() {
        return -1;
    }

    public void setFurtherRepairWatermarkTxnTd(long furtherRepairWatermarkTxnTd) {
        throw new UnsupportedOperationException("setFurtherRepairWatermarkTxnTd is not supported in Replica");
    }

    public void updateWithReport(TTabletInfo backendReplica) {
        updateVersion(backendReplica.getVersion());
        setDataSize(backendReplica.getDataSize());
        setRemoteDataSize(backendReplica.getRemoteDataSize());
        setRowCount(backendReplica.getRowCount());
        setTotalVersionCount(backendReplica.getTotalVersionCount());
        setVisibleVersionCount(
                backendReplica.isSetVisibleVersionCount() ? backendReplica.getVisibleVersionCount()
                        : backendReplica.getTotalVersionCount());
    }

    public synchronized void updateVersion(long newVersion) {
        updateReplicaVersion(newVersion, getLastFailedVersion(), this.lastSuccessVersion);
    }

    public synchronized void updateVersionWithFailed(
            long newVersion, long lastFailedVersion, long lastSuccessVersion) {
        updateReplicaVersion(newVersion, lastFailedVersion, lastSuccessVersion);
    }

    public synchronized void adminUpdateVersionInfo(Long version, Long lastFailedVersion, Long lastSuccessVersion,
            long updateTime) {
        long oldLastFailedVersion = getLastFailedVersion();
        if (version != null) {
            this.version = version;
        }
        if (lastSuccessVersion != null) {
            this.lastSuccessVersion = lastSuccessVersion;
        }
        if (lastFailedVersion != null) {
            if (getLastFailedVersion() < lastFailedVersion) {
                setLastFailedTimestamp(updateTime);
            }
            setLastFailedVersion(lastFailedVersion);
        }
        if (getLastFailedVersion() < this.version) {
            setLastFailedVersionAndTimestamp(-1, -1);
        }
        if (getLastFailedVersion() > 0
                && this.lastSuccessVersion > getLastFailedVersion()) {
            this.lastSuccessVersion = this.version;
        }
        if (this.lastSuccessVersion < this.version) {
            this.lastSuccessVersion = this.version;
        }
        if (oldLastFailedVersion < 0 && getLastFailedVersion() > 0) {
            LOG.info("change replica last failed version from '< 0' to '> 0', replica {}, old last failed version {}",
                    this, oldLastFailedVersion);
        } else if (oldLastFailedVersion > 0 && getLastFailedVersion() < 0) {
            LOG.info("change replica last failed version from '> 0' to '< 0', replica {}, old last failed version {}",
                    this, oldLastFailedVersion);
        }
    }

    protected void setLastFailedVersion(long lastFailedVersion) {
        throw new UnsupportedOperationException("setLastFailedVersion is not supported in Replica");
    }

    protected void setLastFailedTimestamp(long lastFailedTimestamp) {
        throw new UnsupportedOperationException("setLastFailedTimestamp is not supported in Replica");
    }

    /* last failed version:  LFV
     * last success version: LSV
     * version:              V
     *
     * Case 1:
     *      If LFV > LSV, set LSV back to V, which indicates that version between LSV and LFV is invalid.
     *      Clone task will clone the version between LSV and LFV
     *
     * Case 2:
     *      LFV changed, set LSV back to V. This is just same as Case 1. Cause LFV must large than LSV.
     *
     * Case 3:
     *      LFV remains unchanged, just update LSV, and then check if it falls into Case 1.
     *
     * Case 4:
     *      V is larger or equal to LFV, reset LFV. And if V is less than LSV, just set V to LSV. This may
     *      happen when a clone task finished and report version V, but the LSV is already larger than V,
     *      And we know that version between V and LSV is valid, so move V forward to LSV.
     *
     * Case 5:
     *      This is a bug case, I don't know why, may be some previous version introduce it. It looks like
     *      the V(hash) equals to LSV(hash), and V equals to LFV, but LFV hash is 0 or some unknown number.
     *      We just reset the LFV(hash) to recovery this replica.
     */
    private void updateReplicaVersion(long newVersion, long lastFailedVersion, long lastSuccessVersion) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("before update: {}", this.toString());
        }

        if (newVersion < this.version) {
            // This case means that replica meta version has been updated by ReportHandler before
            // For example, the publish version daemon has already sent some publish version tasks
            // to one be to publish version 2, 3, 4, 5, 6, and the be finish all publish version tasks,
            // the be's replica version is 6 now, but publish version daemon need to wait
            // for other be to finish most of publish version tasks to update replica version in fe.
            // At the moment, the replica version in fe is 4, when ReportHandler sync tablet,
            // it find reported replica version in be is 6 and then set version to 6 for replica in fe.
            // And then publish version daemon try to finish txn, and use visible version(5)
            // to update replica. Finally, it find the newer version(5) is lower than replica version(6) in fe.
            if (LOG.isDebugEnabled()) {
                LOG.debug("replica {} on backend {}'s new version {} is lower than meta version {},"
                        + "not to continue to update replica", id, getBackendIdValue(), newVersion, this.version);
            }
            return;
        }

        long oldLastFailedVersion = getLastFailedVersion();

        this.version = newVersion;

        // just check it
        if (lastSuccessVersion <= this.version) {
            lastSuccessVersion = this.version;
        }

        // case 1:
        if (this.lastSuccessVersion <= getLastFailedVersion()) {
            this.lastSuccessVersion = this.version;
        }

        // TODO: this case is unknown, add log to observe
        if (this.version > lastFailedVersion && lastFailedVersion > 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("current version {} is larger than last failed version {}, "
                            + "maybe a fatal error or be report version, print a stack here ",
                        this.version, lastFailedVersion, new Exception());
            }
        }

        if (lastFailedVersion != getLastFailedVersion()) {
            // Case 2:
            if (lastFailedVersion > getLastFailedVersion() || lastFailedVersion < 0) {
                setLastFailedVersionAndTimestamp(lastFailedVersion,
                        lastFailedVersion > 0 ? System.currentTimeMillis() : -1L);
            }

            this.lastSuccessVersion = this.version;
        } else {
            // Case 3:
            if (lastSuccessVersion >= this.lastSuccessVersion) {
                this.lastSuccessVersion = lastSuccessVersion;
            }
            if (lastFailedVersion >= this.lastSuccessVersion) {
                this.lastSuccessVersion = this.version;
            }
        }

        // Case 4:
        if (this.version >= getLastFailedVersion()) {
            setLastFailedVersionAndTimestamp(-1, -1);
            if (this.version < this.lastSuccessVersion) {
                this.version = this.lastSuccessVersion;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("after update {}", this.toString());
        }

        if (oldLastFailedVersion < 0 && getLastFailedVersion() > 0) {
            LOG.info("change replica last failed version from '< 0' to '> 0', replica {}, old last failed version {}",
                    this, oldLastFailedVersion);
        } else if (oldLastFailedVersion > 0 && getLastFailedVersion() < 0) {
            LOG.info("change replica last failed version from '> 0' to '< 0', replica {}, old last failed version {}",
                    this, oldLastFailedVersion);
        }
    }

    protected void setLastFailedVersionAndTimestamp(long lastFailedVersion, long lastFailedTimestamp) {
        if (lastFailedVersion == -1 || lastFailedTimestamp == -1) {
            return;
        }
        LOG.info("setLastFailedVersionAndTimestamp is not supported in Replica: {}, "
                + "lastFailedVersion: {}, lastFailedTimestamp: {}", id, lastFailedVersion, lastFailedTimestamp);
        throw new UnsupportedOperationException("setLastFailedVersionAndTimestamp is not supported in Replica");
    }

    public synchronized void updateLastFailedVersion(long lastFailedVersion) {
        updateReplicaVersion(this.version, lastFailedVersion, this.lastSuccessVersion);
    }

    /*
     * If a replica is overwritten by a restore job, we need to reset version and lastSuccessVersion to
     * the restored replica version
     */
    public void updateVersionForRestore(long version) {
        this.version = version;
        this.lastSuccessVersion = version;
    }

    /*
     * Check whether the replica's version catch up with the expected version.
     * If ignoreAlter is true, and state is ALTER, and replica's version is
     *  PARTITION_INIT_VERSION, just return true, ignore the version.
     *      This is for the case that when altering table,
     *      the newly created replica's version is PARTITION_INIT_VERSION,
     *      but we need to treat it as a "normal" replica which version is supposed to be "catch-up".
     *      But if state is ALTER but version larger than PARTITION_INIT_VERSION, which means this replica
     *      is already updated by load process, so we need to consider its version.
     */
    public boolean checkVersionCatchUp(long expectedVersion, boolean ignoreAlter) {
        if (ignoreAlter && state == ReplicaState.ALTER && version == Partition.PARTITION_INIT_VERSION) {
            return true;
        }

        if (expectedVersion == Partition.PARTITION_INIT_VERSION) {
            // no data is loaded into this replica, just return true
            return true;
        }

        if (this.version < expectedVersion) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("replica version does not catch up with version: {}. replica: {}",
                          expectedVersion, this);
            }
            return false;
        }
        return true;
    }

    public void setState(ReplicaState replicaState) {
        this.state = replicaState;
    }

    public ReplicaState getState() {
        return this.state;
    }

    public boolean tooSlow() {
        return state == ReplicaState.COMPACTION_TOO_SLOW;
    }

    public boolean tooBigVersionCount() {
        return visibleVersionCount >= Config.min_version_count_indicate_replica_compaction_too_slow;
    }

    public boolean isNormal() {
        return state == ReplicaState.NORMAL;
    }

    public long getTotalVersionCount() {
        return totalVersionCount;
    }

    public void setTotalVersionCount(long totalVersionCount) {
        this.totalVersionCount = totalVersionCount;
    }

    public long getVisibleVersionCount() {
        return visibleVersionCount;
    }

    public void setVisibleVersionCount(long visibleVersionCount) {
        this.visibleVersionCount = visibleVersionCount;
    }

    public boolean checkVersionRegressive(long newVersion) {
        throw new UnsupportedOperationException("checkVersionRegressive is not supported in Replica");
    }

    @Override
    public String toString() {
        StringBuilder strBuffer = new StringBuilder("[replicaId=");
        strBuffer.append(id);
        strBuffer.append(", BackendId=");
        strBuffer.append(getBackendIdValue());
        strBuffer.append(", version=");
        strBuffer.append(version);
        strBuffer.append(", dataSize=");
        strBuffer.append(dataSize);
        strBuffer.append(", rowCount=");
        strBuffer.append(rowCount);
        strBuffer.append(", lastFailedVersion=");
        strBuffer.append(getLastFailedVersion());
        strBuffer.append(", lastSuccessVersion=");
        strBuffer.append(lastSuccessVersion);
        strBuffer.append(", lastFailedTimestamp=");
        strBuffer.append(getLastFailedTimestamp());
        strBuffer.append(", schemaHash=");
        strBuffer.append(schemaHash);
        strBuffer.append(", state=");
        strBuffer.append(state.name());
        strBuffer.append(", isBad=");
        strBuffer.append(isBad());
        strBuffer.append("]");
        return strBuffer.toString();
    }

    public String toStringSimple(boolean checkBeAlive) {
        StringBuilder strBuffer = new StringBuilder("[replicaId=");
        strBuffer.append(id);
        strBuffer.append(", backendId=");
        strBuffer.append(getBackendIdValue());
        if (checkBeAlive) {
            Backend backend = Env.getCurrentSystemInfo().getBackend(getBackendIdValue());
            if (backend == null) {
                strBuffer.append(", backend=null");
            } else {
                strBuffer.append(", backendAlive=");
                strBuffer.append(backend.isAlive());
                if (backend.isDecommissioned()) {
                    strBuffer.append(", backendDecommission=true");
                }
            }
        }
        strBuffer.append(", version=");
        strBuffer.append(version);
        if (getLastFailedVersion() > 0) {
            strBuffer.append(", lastFailedVersion=");
            strBuffer.append(getLastFailedVersion());
            strBuffer.append(", lastSuccessVersion=");
            strBuffer.append(lastSuccessVersion);
            strBuffer.append(", lastFailedTimestamp=");
            strBuffer.append(getLastFailedTimestamp());
        }
        if (isBad()) {
            strBuffer.append(", isBad=true");
            Backend backend = Env.getCurrentSystemInfo().getBackend(getBackendIdValue());
            if (backend != null && getPathHash() != -1) {
                DiskInfo diskInfo = backend.getDisks().values().stream()
                        .filter(disk -> disk.getPathHash() == getPathHash())
                        .findFirst().orElse(null);
                if (diskInfo == null) {
                    strBuffer.append(", disk with path hash " + getPathHash() + " not exists");
                } else if (diskInfo.getState() == DiskInfo.DiskState.OFFLINE) {
                    strBuffer.append(", disk " + diskInfo.getRootPath() + " is bad");
                }
            }
        }
        strBuffer.append(", state=");
        strBuffer.append(state.name());
        strBuffer.append("]");

        return strBuffer.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Replica)) {
            return false;
        }

        Replica replica = (Replica) obj;
        return (id == replica.id)
                && (getBackendIdValue() == replica.getBackendIdValue())
                && (version == replica.version)
                && (dataSize == replica.dataSize)
                && (rowCount == replica.rowCount)
                && (state.equals(replica.state))
                && (getLastFailedVersion() == replica.getLastFailedVersion())
                && (lastSuccessVersion == replica.lastSuccessVersion);
    }

    private static class VersionComparator<T extends Replica> implements Comparator<T> {
        public VersionComparator() {
        }

        @Override
        public int compare(T replica1, T replica2) {
            if (replica1.getVersion() < replica2.getVersion()) {
                return 1;
            } else if (replica1.getVersion() == replica2.getVersion()) {
                return 0;
            } else {
                return -1;
            }
        }
    }

    private static class LastSuccessVersionComparator<T extends Replica> implements Comparator<T> {
        public LastSuccessVersionComparator() {
        }

        @Override
        public int compare(T replica1, T replica2) {
            if (replica1.getLastSuccessVersion() < replica2.getLastSuccessVersion()) {
                return 1;
            } else if (replica1.getLastSuccessVersion() == replica2.getLastSuccessVersion()) {
                return 0;
            } else {
                return -1;
            }
        }
    }

    private static class IdComparator<T extends Replica> implements Comparator<T> {
        public IdComparator() {
        }

        @Override
        public int compare(T replica1, T replica2) {
            if (replica1.getId() < replica2.getId()) {
                return -1;
            } else if (replica1.getId() == replica2.getId()) {
                return 0;
            } else {
                return 1;
            }
        }
    }

    public void setPreWatermarkTxnId(long preWatermarkTxnId) {
        throw new UnsupportedOperationException("setPreWatermarkTxnId is not supported in Replica");
    }

    public long getPreWatermarkTxnId() {
        return -1;
    }

    public void setPostWatermarkTxnId(long postWatermarkTxnId) {
        throw  new UnsupportedOperationException("setPostWatermarkTxnId is not supported in Replica");
    }

    public long getPostWatermarkTxnId() {
        return -1;
    }

    public void setUserDropTime(long userDropTime) {
        throw new UnsupportedOperationException("setUserDropTime is not supported in Replica");
    }

    public boolean isUserDrop() {
        return false;
    }

    public void setScaleInDropTimeStamp(long scaleInDropTime) {
        throw new UnsupportedOperationException("setScaleInDropTimeStamp is not supported in Replica");
    }

    public boolean isScaleInDrop() {
        return false;
    }

    public boolean isAlive() {
        return getState() != ReplicaState.CLONE
                && getState() != ReplicaState.DECOMMISSION
                && !isBad();
    }

    public boolean isScheduleAvailable() {
        return Env.getCurrentSystemInfo().checkBackendScheduleAvailable(getBackendIdValue())
            && !isUserDrop();
    }

    public void setLastReportVersion(long version) {
        throw new UnsupportedOperationException("setLastReportVersion is not supported in Replica");
    }

    public long getLastReportVersion() {
        return 0;
    }
}
