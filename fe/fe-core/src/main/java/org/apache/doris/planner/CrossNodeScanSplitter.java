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

package org.apache.doris.planner;

import org.apache.doris.proto.InternalService.PGetTabletSplitRequest;
import org.apache.doris.proto.InternalService.PGetTabletSplitResponse;
import org.apache.doris.proto.InternalService.PTabletSplitMeta;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloScanRange;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTabletSplit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Cross-BE single-tablet parallel scan (cloud mode only), Scheme A.
 *
 * <p>For a large tablet, ask its home BE to capture the tablet at the pinned
 * version and split its read source by row id (see BE {@code get_tablet_split}).
 * Each returned {@link TTabletSplit} entity is attached onto a cloned
 * {@link TPaloScanRange} and placed (round-robin) on one of the candidate compute
 * BEs, so the splits of one tablet run concurrently across nodes.
 *
 * <p>Correctness relies entirely on the BE side: the rowset layout the split was
 * computed against is shipped as an entity and rebuilt by the execution node, so
 * the split coordinates stay valid even if compaction changes the layout
 * afterwards. See doc/cross_be_parallel_scan.md.
 */
public class CrossNodeScanSplitter {
    private static final Logger LOG = LogManager.getLogger(CrossNodeScanSplitter.class);

    private static final long RPC_TIMEOUT_SEC = 30;

    private CrossNodeScanSplitter() {}

    /**
     * Expand qualifying tablets' scan ranges into cross-BE splits. Non-qualifying
     * ranges (small tablets, no candidate BE, RPC failure) are kept unchanged, so
     * this is always safe to call when the feature flag is on.
     *
     * @param locationsList    original per-tablet scan ranges
     * @param tabletRowCounts  tabletId -> row count (for threshold)
     * @param candidateBackends alive compute BEs to spread splits onto
     * @param sv               session variables
     * @return possibly-expanded scan range list
     */
    public static List<TScanRangeLocations> split(List<TScanRangeLocations> locationsList,
            Map<Long, Long> tabletRowCounts, List<Backend> candidateBackends, SessionVariable sv) {
        if (locationsList == null || locationsList.isEmpty() || candidateBackends == null
                || candidateBackends.isEmpty()) {
            return locationsList;
        }

        long minRows = sv.getCrossNodeScanMinRows();
        int maxSplits = sv.getCrossNodeScanMaxSplits();
        if (maxSplits <= 0) {
            maxSplits = sv.getParallelScanMaxScannersCount();
        }
        if (maxSplits <= 0) {
            // Fall back to the number of candidate backends as the parallelism target.
            maxSplits = candidateBackends.size();
        }
        long minRowsPerSplit = sv.getParallelScanMinRowsPerScanner();

        List<TScanRangeLocations> result = new ArrayList<>(locationsList.size());
        int roundRobin = 0;
        for (TScanRangeLocations loc : locationsList) {
            if (loc.getScanRange() == null || loc.getScanRange().getPaloScanRange() == null) {
                result.add(loc);
                continue;
            }
            TPaloScanRange palo = loc.getScanRange().getPaloScanRange();
            long tabletId = palo.getTabletId();
            long rowCount = tabletRowCounts.getOrDefault(tabletId, 0L);
            if (rowCount < minRows) {
                result.add(loc);
                continue;
            }

            Backend homeBe = pickHomeBackend(loc, candidateBackends);
            if (homeBe == null) {
                result.add(loc);
                continue;
            }

            long version;
            try {
                version = Long.parseLong(palo.getVersion());
            } catch (NumberFormatException e) {
                result.add(loc);
                continue;
            }

            List<TTabletSplit> splits;
            try {
                splits = fetchSplits(homeBe, tabletId, version, maxSplits, minRowsPerSplit, sv);
            } catch (Exception e) {
                LOG.warn("get_tablet_split failed for tablet {} on be {}, fall back to single-BE "
                        + "scan", tabletId, homeBe.getId(), e);
                result.add(loc);
                continue;
            }
            if (splits.isEmpty()) {
                result.add(loc);
                continue;
            }

            // Place the home BE first so it keeps acting as a warm cache source.
            List<Backend> ordered = orderWithHomeFirst(candidateBackends, homeBe);
            for (TTabletSplit ts : splits) {
                Backend target = ordered.get(roundRobin % ordered.size());
                roundRobin++;
                result.add(buildSplitLocation(palo, ts, target));
            }
        }
        return result;
    }

    private static List<TTabletSplit> fetchSplits(Backend homeBe, long tabletId, long version,
            int maxSplits, long minRowsPerSplit, SessionVariable sv) throws Exception {
        PGetTabletSplitRequest request = PGetTabletSplitRequest.newBuilder()
                .setTabletId(tabletId)
                .setVersion(version)
                .setMaxSplits(maxSplits)
                .setMinRowsPerSplit(minRowsPerSplit)
                .setEnableSegmentCache(sv.enableSegmentCache)
                // mirror the query options so the split has the same semantics as a local scan
                .setSkipDeletePredicate(sv.skipDeletePredicate)
                .setSkipDeleteBitmap(sv.skipDeleteBitmap)
                .build();
        Future<PGetTabletSplitResponse> future =
                BackendServiceProxy.getInstance().getTabletSplit(homeBe.getBrpcAddress(), request);
        PGetTabletSplitResponse response = future.get(RPC_TIMEOUT_SEC, TimeUnit.SECONDS);
        if (response.getStatus().getStatusCode() != TStatusCode.OK.getValue()) {
            throw new RuntimeException("get_tablet_split error: "
                    + response.getStatus().getErrorMsgsList());
        }
        List<TTabletSplit> out = new ArrayList<>(response.getSplitsCount());
        TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
        for (PTabletSplitMeta meta : response.getSplitsList()) {
            TTabletSplit ts = new TTabletSplit();
            deserializer.deserialize(ts, meta.getTabletSplit().toByteArray());
            out.add(ts);
        }
        return out;
    }

    // Clone the original palo scan range, attach the split entity, and place it on
    // a single target backend so the scheduler assigns it there deterministically.
    private static TScanRangeLocations buildSplitLocation(TPaloScanRange original, TTabletSplit ts,
            Backend target) {
        TPaloScanRange palo = original.deepCopy();
        palo.setTabletSplit(ts);

        TScanRange scanRange = new TScanRange();
        scanRange.setPaloScanRange(palo);

        TScanRangeLocations locations = new TScanRangeLocations();
        locations.setScanRange(scanRange);
        TNetworkAddress addr = new TNetworkAddress(target.getHost(), target.getBePort());
        TScanRangeLocation location = new TScanRangeLocation(addr);
        location.setBackendId(target.getId());
        List<TScanRangeLocation> single = new ArrayList<>(1);
        single.add(location);
        locations.setLocations(single);
        return locations;
    }

    private static Backend pickHomeBackend(TScanRangeLocations loc, List<Backend> candidates) {
        if (loc.getLocations() != null) {
            for (TScanRangeLocation l : loc.getLocations()) {
                for (Backend be : candidates) {
                    if (be.getId() == l.getBackendId() && be.isQueryAvailable()) {
                        return be;
                    }
                }
            }
        }
        // No replica overlaps the candidate set; fall back to the first candidate.
        for (Backend be : candidates) {
            if (be.isQueryAvailable()) {
                return be;
            }
        }
        return null;
    }

    private static List<Backend> orderWithHomeFirst(List<Backend> candidates, Backend home) {
        List<Backend> ordered = new ArrayList<>(candidates.size());
        ordered.add(home);
        for (Backend be : candidates) {
            if (be.getId() != home.getId() && be.isQueryAvailable()) {
                ordered.add(be);
            }
        }
        return ordered;
    }
}
