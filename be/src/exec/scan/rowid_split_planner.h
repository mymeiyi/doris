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

#pragma once

#include <cstdint>
#include <utility>
#include <vector>

#include "storage/segment/row_ranges.h"

namespace doris {

// Pure (storage-engine-free) planning of how a single tablet's scan is split into scanners
// over a global rowid sub-range. Kept independent of RowsetReader/Tablet so it can be unit
// tested and reused. See `ParallelScannerBuilder::_build_scanners_by_rowid` for the caller,
// and `large_tablet_parallel_query_design.md` §10 for the model.
//
// The tablet-global rowid space is the concatenation of all rowsets (in version_path order)
// and, within each rowset, all segments (in seg_id order). Given the same snapshot `version`
// every BE sees the same layout, so the same (split_id, split_count) deterministically maps
// to the same global sub-range with no cross-BE communication.

// One rowset's contribution to a single scanner: which segments [first, second) of that rowset
// and, per segment, the row ranges to read. `segment_row_ranges` has exactly one entry per
// segment in [first, second) (the segments are contiguous within the rowset).
struct RowidRowsetSlice {
    size_t rowset_index = 0; // index into the tablet's rowset list (version_path order)
    std::pair<int64_t, int64_t> segment_offsets {0, 0};
    std::vector<segment_v2::RowRanges> segment_row_ranges;
};

// A single scanner may span multiple consecutive rowsets, so its plan is a list of slices.
using RowidScannerPlan = std::vector<RowidRowsetSlice>;

// Compute the global rowid sub-range [begin, end) for split `split_id` of `split_count` over a
// tablet with `total_rows`. `split_count <= 1` means the whole tablet, i.e. [0, total_rows).
//
// Uses integer mul-then-div so adjacent splits meet exactly: the union over split_id in
// [0, split_count) is [0, total_rows) and the splits never overlap -- coverage and
// non-overlap are algebraic properties of the formula, not of any passed-in boundary.
std::pair<int64_t, int64_t> rowid_split_range(int64_t total_rows, int32_t split_id,
                                              int32_t split_count);

// Plan the scanners that cover a tablet's global rowid sub-range [g_begin, g_end).
//   - `rowsets_segment_rows[r]` lists the row count of each segment of rowset r (seg_id order);
//     the outer vector is in version_path order (== the tablet's rowset list order). Rowsets
//     with zero total rows are allowed and contribute nothing while still occupying an index.
//   - `rows_per_scanner` is the target rows per scanner for in-BE multi-threading; each emitted
//     scanner collects roughly this many rows. Must be > 0.
//
// Every emitted scanner covers a contiguous, non-overlapping slice of [g_begin, g_end); the
// union of all scanners is exactly [g_begin, g_end).
std::vector<RowidScannerPlan> plan_rowid_scanners(
        const std::vector<std::vector<int64_t>>& rowsets_segment_rows, int64_t g_begin,
        int64_t g_end, int64_t rows_per_scanner);

} // namespace doris
