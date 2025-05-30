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

package org.apache.doris.datasource.hudi;

import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.hive.HMSSchemaCacheValue;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.internal.schema.InternalSchema;

import java.util.List;

public class HudiSchemaCacheValue extends HMSSchemaCacheValue {

    private List<String> colTypes;
    boolean enableSchemaEvolution;

    public HudiSchemaCacheValue(List<Column> schema, List<Column> partitionColumns, boolean enableSchemaEvolution) {
        super(schema, partitionColumns);
        this.enableSchemaEvolution = enableSchemaEvolution;
    }

    public List<String> getColTypes() {
        return colTypes;
    }

    public void setColTypes(List<String> colTypes) {
        this.colTypes = colTypes;
    }

    public InternalSchema getCommitInstantInternalSchema(HoodieTableMetaClient metaClient, Long commitInstantTime) {
        return InternalSchemaCache.searchSchemaAndCache(commitInstantTime, metaClient, true);
    }

    public boolean isEnableSchemaEvolution() {
        return enableSchemaEvolution;
    }

}
