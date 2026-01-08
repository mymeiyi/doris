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

import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class CloudTabletInvertedIndex extends TabletInvertedIndex {
    private static final Logger LOG = LogManager.getLogger(CloudTabletInvertedIndex.class);

    // tablet id -> replica
    // for cloud mode, no need to know the replica's backend
    private Map<Long, Replica> replicaMetaTable = Maps.newHashMap();

    public CloudTabletInvertedIndex() {
        super();
    }

    @Override
    public List<Replica> getReplicas(Long tabletId) {
        long stamp = readLock();
        try {
            if (replicaMetaTable.containsKey(tabletId)) {
                return Lists.newArrayList(replicaMetaTable.get(tabletId));
            }
            return Lists.newArrayList();
        } finally {
            readUnlock(stamp);
        }
    }

    @Override
    public void addTablet(long tabletId, TabletMeta tabletMeta) {
        long stamp = writeLock();
        try {
            if (tabletMetaMap.containsKey(tabletId)) {
                return;
            }
            tabletMetaMap.put(tabletId, tabletMeta);
            if (LOG.isDebugEnabled()) {
                LOG.debug("add tablet: {}", tabletId);
            }
        } finally {
            writeUnlock(stamp);
        }
    }

    @Override
    public void deleteTablet(long tabletId) {
        long stamp = writeLock();
        try {
            replicaMetaTable.remove(tabletId);
            if (LOG.isDebugEnabled()) {
                LOG.debug("delete tablet: {}", tabletId);
            }
        } finally {
            writeUnlock(stamp);
        }
    }

    @Override
    public void addReplica(long tabletId, Replica replica) {
        long stamp = writeLock();
        try {
            Preconditions.checkState(tabletMetaMap.containsKey(tabletId),
                    "tablet " + tabletId + " not exists, replica " + replica.getId());
            replicaMetaTable.put(tabletId, replica);
            if (LOG.isDebugEnabled()) {
                LOG.debug("add replica {} of tablet {}", replica.getId(), tabletId);
            }
        } finally {
            writeUnlock(stamp);
        }
    }

    @Override
    public void deleteReplica(long tabletId, long backendId) {
        long stamp = writeLock();
        try {
            Preconditions.checkState(tabletMetaMap.containsKey(tabletId), "tablet " + tabletId + " not exists");
            if (replicaMetaTable.containsKey(tabletId)) {
                Replica replica = replicaMetaTable.remove(tabletId);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("delete replica {} of tablet {}", replica.getId(), tabletId);
                }
            } else {
                // this may happen when fe restart after tablet is empty(bug cause)
                // add log instead of assertion to observe
                LOG.error("tablet[{}] contains no replica in inverted index", tabletId);
            }
        } finally {
            writeUnlock(stamp);
        }
    }

    @Override
    public Replica getReplica(long tabletId, long backendId) {
        long stamp = readLock();
        try {
            Preconditions.checkState(tabletMetaMap.containsKey(tabletId),
                    "tablet " + tabletId + " not exists, backend " + backendId);
            return replicaMetaTable.get(tabletId);
        } finally {
            readUnlock(stamp);
        }
    }

    @Override
    public List<Replica> getReplicasByTabletId(long tabletId) {
        long stamp = readLock();
        try {
            if (replicaMetaTable.containsKey(tabletId)) {
                return Lists.newArrayList(replicaMetaTable.get(tabletId));
            }
            return Lists.newArrayList();
        } finally {
            readUnlock(stamp);
        }
    }

    @Override
    protected void innerClear() {
        replicaMetaTable.clear();
    }
}
