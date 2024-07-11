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

package org.apache.doris.qe;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.Queriable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.UnionNode;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprList;
import org.apache.doris.thrift.TQueryOptions;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class ShortCircuitInsertContext {
    // Cached for better CPU performance, since serialize DescriptorTable and
    // outputExprs are heavy work
    public final ByteString serializedDescTable;
    public final ByteString serializedOutputExpr = null;
    public final ByteString serializedQueryOptions;

    // For prepared statement cached structure,
    // there are some pre-calculated structure in Backend TabletFetch service
    // using this ID to find for this prepared statement
    public final UUID cacheID;

    public final int schemaVersion;
    public final OlapTable tbl;

    public final OlapScanNode scanNode;
    public final Queriable analzyedQuery;
    // Serialized mysql Field, this could avoid serialize mysql field each time sendFields.
    // Since, serialize fields is too heavy when table is wide
    public Map<String, byte[]> serializedFields =  Maps.newHashMap();


    public ShortCircuitInsertContext(Planner planner, Queriable analzyedQuery) throws TException {
        this.serializedDescTable = ByteString.copyFrom(
                new TSerializer().serialize(planner.getDescTable().toThrift()));
        TQueryOptions options = planner.getQueryOptions() != null ? planner.getQueryOptions() : new TQueryOptions();
        this.serializedQueryOptions = ByteString.copyFrom(
                new TSerializer().serialize(options));
        UnionNode unionNode = (UnionNode) planner.getFragments().get(0).getPlanRoot();
        List<List<TExpr>> exprs = new ArrayList<>();
        unionNode.getMaterializedResultExprLists().forEach(
                exprList -> exprs.add(exprList.stream().map(Expr::treeToThrift).collect(Collectors.toList())));
        // serializedOutputExpr = ByteString.copyFrom(new TSerializer().serialize(exprs));
        this.cacheID = UUID.randomUUID();
        this.scanNode = ((OlapScanNode) planner.getScanNodes().get(0));
        this.tbl = this.scanNode.getOlapTable();
        this.schemaVersion = this.tbl.getBaseSchemaVersion();
        this.analzyedQuery = analzyedQuery;
    }

    public void sanitize() {
        Preconditions.checkNotNull(serializedDescTable);
        Preconditions.checkNotNull(serializedOutputExpr);
        Preconditions.checkNotNull(cacheID);
        Preconditions.checkNotNull(tbl);
    }
}
