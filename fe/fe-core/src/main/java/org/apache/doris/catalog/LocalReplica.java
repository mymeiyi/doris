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
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.thrift.TUniqueId;

import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LocalReplica extends Replica {
    private static final Logger LOG = LogManager.getLogger(LocalReplica.class);

    @SerializedName(value = "rds", alternate = {"remoteDataSize"})
    private volatile long remoteDataSize = 0;
    @SerializedName(value = "ris", alternate = {"remoteInvertedIndexSize"})
    private Long remoteInvertedIndexSize = 0L;
    @SerializedName(value = "rss", alternate = {"remoteSegmentSize"})
    private Long remoteSegmentSize = 0L;

    private TUniqueId cooldownMetaId;
    private long cooldownTerm = -1;

    private long pathHash = -1;

    // A replica version should increase monotonically,
    // but backend may missing some versions due to disk failure or bugs.
    // FE should found these and mark the replica as missing versions.
    // If backend's report version < fe version, record the backend's report version as `regressiveVersion`,
    // and if time exceed 5min, fe should mark this replica as missing versions.
    private long regressiveVersion = -1;
    private long regressiveVersionTimestamp = 0;

    /*
     * This can happen when this replica is created by a balance clone task, and
     * when task finished, the version of this replica is behind the partition's visible version.
     * So this replica need a further repair.
     * If we do not do this, this replica will be treated as version stale, and will be removed,
     * so that the balance task is failed, which is unexpected.
     *
     * furtherRepairSetTime and leftFurtherRepairCount are set alone with needFurtherRepair.
     * This is an insurance, in case that further repair task always fail. If 20 min passed
     * since we set needFurtherRepair to true, the 'needFurtherRepair' will be set to false.
     */
    private long furtherRepairSetTime = -1;
    private int leftFurtherRepairCount = 0;

    // During full clone, the replica's state is CLONE, it will not load the data.
    // After full clone finished, even if the replica's version = partition's visible version,
    //
    // notice: furtherRepairWatermarkTxnTd is used to clone a replica, protected it from be removed.
    //
    private long furtherRepairWatermarkTxnTd = -1;

    private long userDropTime = -1;

    private long scaleInDropTime = -1;

    private long lastReportVersion = 0;

    public LocalReplica() {
        super();
    }

    public LocalReplica(ReplicaContext context) {
        super(context);
    }

    // for rollup
    // the new replica's version is -1 and last failed version is -1
    public LocalReplica(long replicaId, long backendId, int schemaHash, ReplicaState state) {
        super(replicaId, backendId, schemaHash, state);
    }

    // for create tablet and restore
    public LocalReplica(long replicaId, long backendId, ReplicaState state, long version, int schemaHash) {
        super(replicaId, backendId, state, version, schemaHash);
    }

    public LocalReplica(long replicaId, long backendId, long version, int schemaHash, long dataSize,
            long remoteDataSize, long rowCount, ReplicaState state, long lastFailedVersion, long lastSuccessVersion) {
        super(replicaId, backendId, version, schemaHash, dataSize, remoteDataSize, rowCount, state, lastFailedVersion,
                lastSuccessVersion);
        this.remoteDataSize = remoteDataSize;
    }

    @Override
    public long getRemoteDataSize() {
        return remoteDataSize;
    }

    @Override
    public void setRemoteDataSize(long remoteDataSize) {
        this.remoteDataSize = remoteDataSize;
    }

    @Override
    public Long getRemoteInvertedIndexSize() {
        return remoteInvertedIndexSize;
    }

    @Override
    public void setRemoteInvertedIndexSize(long remoteInvertedIndexSize) {
        this.remoteInvertedIndexSize = remoteInvertedIndexSize;
    }

    @Override
    public Long getRemoteSegmentSize() {
        return remoteSegmentSize;
    }

    @Override
    public void setRemoteSegmentSize(long remoteSegmentSize) {
        this.remoteSegmentSize = remoteSegmentSize;
    }

    @Override
    public TUniqueId getCooldownMetaId() {
        return cooldownMetaId;
    }

    @Override
    public void setCooldownMetaId(TUniqueId cooldownMetaId) {
        this.cooldownMetaId = cooldownMetaId;
    }

    @Override
    public long getCooldownTerm() {
        return cooldownTerm;
    }

    @Override
    public void setCooldownTerm(long cooldownTerm) {
        this.cooldownTerm = cooldownTerm;
    }

    @Override
    public boolean needFurtherRepair() {
        return leftFurtherRepairCount > 0
                && System.currentTimeMillis() < furtherRepairSetTime
                + Config.tablet_further_repair_timeout_second * 1000;
    }

    @Override
    public void setNeedFurtherRepair(boolean needFurtherRepair) {
        if (needFurtherRepair) {
            furtherRepairSetTime = System.currentTimeMillis();
            leftFurtherRepairCount = Config.tablet_further_repair_max_times;
        } else {
            leftFurtherRepairCount = 0;
            furtherRepairSetTime = -1;
        }
    }

    @Override
    public void incrFurtherRepairCount() {
        leftFurtherRepairCount--;
    }

    @Override
    public int getLeftFurtherRepairCount() {
        return leftFurtherRepairCount;
    }

    @Override
    public long getFurtherRepairWatermarkTxnTd() {
        return furtherRepairWatermarkTxnTd;
    }

    @Override
    public void setFurtherRepairWatermarkTxnTd(long furtherRepairWatermarkTxnTd) {
        this.furtherRepairWatermarkTxnTd = furtherRepairWatermarkTxnTd;
    }

    @Override
    public boolean checkVersionRegressive(long newVersion) {
        if (newVersion >= getVersion()) {
            regressiveVersion = -1;
            regressiveVersionTimestamp = -1;
            return false;
        }

        if (DebugPointUtil.isEnable("Replica.regressive_version_immediately")) {
            return true;
        }

        if (newVersion != regressiveVersion) {
            regressiveVersion = newVersion;
            regressiveVersionTimestamp = System.currentTimeMillis();
        }

        return System.currentTimeMillis() - regressiveVersionTimestamp >= 5 * 60 * 1000L;
    }

    @Override
    public long getPathHash() {
        return pathHash;
    }

    @Override
    public void setPathHash(long pathHash) {
        this.pathHash = pathHash;
    }

    @Override
    public void setUserDropTime(long userDropTime) {
        this.userDropTime = userDropTime;
    }

    @Override
    public boolean isUserDrop() {
        if (userDropTime > 0) {
            if (System.currentTimeMillis() - userDropTime < Config.manual_drop_replica_valid_second * 1000L) {
                return true;
            }
            userDropTime = -1;
        }

        return false;
    }

    @Override
    public void setScaleInDropTimeStamp(long scaleInDropTime) {
        this.scaleInDropTime = scaleInDropTime;
    }

    @Override
    public boolean isScaleInDrop() {
        if (this.scaleInDropTime > 0) {
            if (System.currentTimeMillis() - this.scaleInDropTime
                    < Config.manual_drop_replica_valid_second * 1000L) {
                return true;
            }
            this.scaleInDropTime = -1;
        }
        return false;
    }

    @Override
    public void setLastReportVersion(long version) {
        this.lastReportVersion = version;
    }

    @Override
    public long getLastReportVersion() {
        return lastReportVersion;
    }
}
