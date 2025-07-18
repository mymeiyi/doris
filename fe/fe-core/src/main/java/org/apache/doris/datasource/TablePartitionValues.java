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

package org.apache.doris.datasource;

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.lock.MonitoredReentrantReadWriteLock;
import org.apache.doris.planner.ColumnBound;
import org.apache.doris.planner.ListPartitionPrunerV2;
import org.apache.doris.planner.PartitionPrunerV2Base.UniqueId;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

@Data
public class TablePartitionValues {
    public static final String HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__";

    private final MonitoredReentrantReadWriteLock readWriteLock;
    private long lastUpdateTimestamp;
    private long nextPartitionId;
    private final Map<Long, PartitionItem> idToPartitionItem;
    private final Map<String, Long> partitionNameToIdMap;
    private Map<String, Long> partitionNameToLastModifiedMap;
    private final Map<Long, String> partitionIdToNameMap;

    private Map<Long, List<UniqueId>> idToUniqueIdsMap;
    private Map<Long, List<String>> partitionValuesMap;
    //multi pair
    private Map<UniqueId, Range<PartitionKey>> uidToPartitionRange;
    private Map<Range<PartitionKey>, UniqueId> rangeToId;
    //single pair
    private RangeMap<ColumnBound, UniqueId> singleColumnRangeMap;
    private Map<UniqueId, Range<ColumnBound>> singleUidToColumnRangeMap;

    public TablePartitionValues() {
        readWriteLock = new MonitoredReentrantReadWriteLock();
        lastUpdateTimestamp = 0;
        nextPartitionId = 0;
        idToPartitionItem = new HashMap<>();
        partitionNameToIdMap = new HashMap<>();
        partitionNameToLastModifiedMap = new HashMap<>();
        partitionIdToNameMap = new HashMap<>();
    }

    public void addPartitions(List<String> partitionNames, List<List<String>> partitionValues, List<Type> types,
            List<Long> partitionLastUpdateTimestamp) {
        Preconditions.checkState(partitionNames.size() == partitionValues.size());
        List<String> addPartitionNames = new ArrayList<>();
        List<PartitionItem> addPartitionItems = new ArrayList<>();
        partitionNameToIdMap.forEach((partitionName, partitionId) -> {
            addPartitionNames.add(partitionName);
            addPartitionItems.add(idToPartitionItem.get(partitionId));
        });

        for (int i = 0; i < partitionNames.size(); i++) {
            if (!partitionNameToIdMap.containsKey(partitionNames.get(i))) {
                addPartitionNames.add(partitionNames.get(i));
                addPartitionItems.add(toListPartitionItem(partitionValues.get(i), types));
            }
            partitionNameToLastModifiedMap.put(partitionNames.get(i), partitionLastUpdateTimestamp.get(i));
        }
        cleanPartitions();

        addPartitionItems(addPartitionNames, addPartitionItems, types);
    }

    private void addPartitionItems(List<String> partitionNames, List<PartitionItem> partitionItems, List<Type> types) {
        Preconditions.checkState(partitionNames.size() == partitionItems.size());
        Preconditions.checkState(nextPartitionId == 0);
        for (int i = 0; i < partitionNames.size(); i++) {
            long partitionId = nextPartitionId++;
            idToPartitionItem.put(partitionId, partitionItems.get(i));
            partitionNameToIdMap.put(partitionNames.get(i), partitionId);
            partitionIdToNameMap.put(partitionId, partitionNames.get(i));
        }

        // create a new map for partitionId <---> uniqueId
        idToUniqueIdsMap = new HashMap<>();

        if (types.size() > 1) {
            // uidToPartitionRange and rangeToId are only used for multi-column partition
            uidToPartitionRange = ListPartitionPrunerV2.genUidToPartitionRange(idToPartitionItem, idToUniqueIdsMap);
            rangeToId = ListPartitionPrunerV2.genRangeToId(uidToPartitionRange);
        } else {
            Preconditions.checkState(types.size() == 1);
            // singleColumnRangeMap is only used for single-column partition
            singleColumnRangeMap = ListPartitionPrunerV2.genSingleColumnRangeMap(idToPartitionItem, idToUniqueIdsMap);
            singleUidToColumnRangeMap = ListPartitionPrunerV2.genSingleUidToColumnRange(singleColumnRangeMap);
        }
        partitionValuesMap = ListPartitionPrunerV2.getPartitionValuesMap(idToPartitionItem);
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public Map<String, Long> getPartitionNameToLastModifiedMap() {
        return partitionNameToLastModifiedMap;
    }

    public Lock readLock() {
        return readWriteLock.readLock();
    }

    public Lock writeLock() {
        return readWriteLock.writeLock();
    }

    public void cleanPartitions() {
        nextPartitionId = 0;
        idToPartitionItem.clear();
        partitionNameToIdMap.clear();
        partitionIdToNameMap.clear();

        idToUniqueIdsMap = null;
        partitionValuesMap = null;
        uidToPartitionRange = null;
        rangeToId = null;
        singleColumnRangeMap = null;
        singleUidToColumnRangeMap = null;
    }

    private ListPartitionItem toListPartitionItem(List<String> partitionValues, List<Type> types) {
        Preconditions.checkState(partitionValues.size() == types.size());
        try {
            PartitionKey key = PartitionKey.createListPartitionKeyWithTypes(
                    partitionValues.stream().map(p -> new PartitionValue(p, HIVE_DEFAULT_PARTITION.equals(p)))
                            .collect(Collectors.toList()), types, false);
            return new ListPartitionItem(Lists.newArrayList(key));
        } catch (AnalysisException e) {
            throw new CacheException("failed to convert partition %s to list partition",
                    e, partitionValues);
        }
    }


    @Data
    public static class TablePartitionKey {
        // use local db/table name here
        // because these names are only used to compare with local names
        private final String dbName;
        private final String tblName;
        // not in key
        private List<Type> types;

        public TablePartitionKey(String dbName, String tblName, List<Type> types) {
            this.dbName = dbName;
            this.tblName = tblName;
            this.types = types;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof TablePartitionKey)) {
                return false;
            }
            return dbName.equals(((TablePartitionKey) obj).dbName)
                    && tblName.equals(((TablePartitionKey) obj).tblName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbName, tblName);
        }

        @Override
        public String toString() {
            return "TablePartitionKey{" + "dbName='" + dbName + '\'' + ", tblName='" + tblName + '\'' + '}';
        }
    }
}
