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

#include "exec/scan/rowid_split_planner.h"

#include <algorithm>

#include "common/logging.h"

namespace doris {

std::pair<int64_t, int64_t> rowid_split_range(int64_t total_rows, int32_t split_id,
                                              int32_t split_count) {
    if (split_count <= 1) {
        return {0, total_rows};
    }
    DCHECK_GE(split_id, 0);
    DCHECK_LT(split_id, split_count);
    const int64_t begin = total_rows * split_id / split_count;
    const int64_t end = total_rows * (split_id + 1) / split_count;
    return {begin, end};
}

std::vector<RowidScannerPlan> plan_rowid_scanners(
        const std::vector<std::vector<int64_t>>& rowsets_segment_rows, int64_t g_begin,
        int64_t g_end, int64_t rows_per_scanner) {
    DCHECK_GT(rows_per_scanner, 0);
    std::vector<RowidScannerPlan> plans;
    if (g_begin >= g_end) {
        return plans;
    }

    // Running global rowid offset, advanced segment by segment over the whole tablet.
    int64_t g_cursor = 0;
    // The scanner currently being accumulated. It may span multiple rowsets, so rows from a
    // partially-filled rowset are carried over via `rows_collected` until the threshold is hit.
    RowidScannerPlan partial;
    int64_t rows_collected = 0;

    for (size_t r = 0; r < rowsets_segment_rows.size(); ++r) {
        const auto& segments_rows = rowsets_segment_rows[r];

        int64_t rowset_rows = 0;
        for (int64_t rows : segments_rows) {
            rowset_rows += rows;
        }
        if (rowset_rows == 0) {
            // Empty rowset: occupies no global rows; skip but keep the index aligned.
            continue;
        }

        int64_t segment_start = 0;
        RowidRowsetSlice slice;
        slice.rowset_index = r;

        for (size_t i = 0; i < segments_rows.size(); ++i) {
            const int64_t rows_of_segment = segments_rows[i];

            // Map this segment onto the tablet-global rowid space and intersect with the
            // requested [g_begin, g_end). For the whole-tablet case the window is the whole
            // segment.
            const int64_t seg_g_begin = g_cursor;
            const int64_t seg_g_end = g_cursor + rows_of_segment;
            g_cursor = seg_g_end;

            const int64_t lo = std::max(g_begin, seg_g_begin);
            const int64_t hi = std::min(g_end, seg_g_end);
            if (lo >= hi) {
                // Segment entirely outside the range. While the current slice has not yet
                // accumulated anything, advance `segment_start` so the next slice begins at the
                // first in-range segment and segment_offsets stay consistent.
                if (slice.segment_row_ranges.empty() && rows_collected == 0) {
                    segment_start = static_cast<int64_t>(i) + 1;
                }
                continue;
            }

            // In-segment window [seg_lo, seg_hi) covered by the range.
            const int64_t seg_lo = lo - seg_g_begin;
            const int64_t seg_hi = hi - seg_g_begin;

            segment_v2::RowRanges row_ranges;
            int64_t offset_in_segment = seg_lo;

            // Try to split a large segment into multiple scanners by `rows_per_scanner`.
            while (offset_in_segment < seg_hi) {
                const int64_t remaining_rows = seg_hi - offset_in_segment;
                int64_t rows_need = rows_per_scanner - rows_collected;

                // 0.9: avoid splitting a segment into excessively small parts.
                if (rows_need >= remaining_rows * 9 / 10) {
                    rows_need = remaining_rows;
                }
                DCHECK_LE(rows_need, remaining_rows);

                // RowRange is [from, to): from inclusive, to exclusive.
                row_ranges.add({offset_in_segment, offset_in_segment + rows_need});
                rows_collected += rows_need;
                offset_in_segment += rows_need;

                if (rows_collected >= rows_per_scanner) {
                    slice.segment_offsets.first = segment_start;
                    slice.segment_offsets.second = static_cast<int64_t>(i) + 1;
                    slice.segment_row_ranges.emplace_back(std::move(row_ranges));
                    DCHECK_EQ(slice.segment_offsets.second - slice.segment_offsets.first,
                              static_cast<int64_t>(slice.segment_row_ranges.size()));

                    partial.emplace_back(std::move(slice));
                    plans.emplace_back(std::move(partial));

                    partial = RowidScannerPlan();
                    slice = RowidRowsetSlice();
                    slice.rowset_index = r;
                    row_ranges = segment_v2::RowRanges();

                    segment_start = offset_in_segment < seg_hi ? static_cast<int64_t>(i)
                                                               : static_cast<int64_t>(i) + 1;
                    rows_collected = 0;
                }
            }

            // Leftover rows in this segment not yet flushed into a scanner.
            if (!row_ranges.is_empty()) {
                DCHECK_GT(rows_collected, 0);
                DCHECK_EQ(row_ranges.to(), seg_hi);
                slice.segment_row_ranges.emplace_back(std::move(row_ranges));
            }
        }

        // Flush this rowset's leftover slice into the in-progress scanner. Use this rowset's own
        // contribution (`segment_row_ranges`) rather than the cross-rowset `rows_collected`:
        // a whole rowset may fall outside [g_begin, g_end) while `rows_collected` is still
        // carried over (>0) from an earlier rowset, in which case this rowset contributes
        // nothing and must not push an empty slice.
        if (!slice.segment_row_ranges.empty()) {
            slice.segment_offsets.first = segment_start;
            slice.segment_offsets.second =
                    segment_start + static_cast<int64_t>(slice.segment_row_ranges.size());
            DCHECK_GT(slice.segment_offsets.second, slice.segment_offsets.first);
            partial.emplace_back(std::move(slice));
        }
    }

    // Final partially-filled scanner, if any.
    if (rows_collected > 0) {
        DCHECK(!partial.empty());
        plans.emplace_back(std::move(partial));
    }

    return plans;
}

} // namespace doris
