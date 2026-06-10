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

#include <gtest/gtest.h>

#include <algorithm>
#include <utility>
#include <vector>

namespace doris {

using segment_v2::RowRanges;
using Layout = std::vector<std::vector<int64_t>>;
using Interval = std::pair<int64_t, int64_t>;

namespace {

int64_t total_rows(const Layout& layout) {
    int64_t t = 0;
    for (const auto& rs : layout) {
        for (int64_t x : rs) {
            t += x;
        }
    }
    return t;
}

// Global begin offset of segment `seg` of rowset `r` in the tablet-global rowid space.
int64_t seg_global_begin(const Layout& layout, size_t r, size_t seg) {
    int64_t off = 0;
    for (size_t rr = 0; rr < r; ++rr) {
        for (int64_t x : layout[rr]) {
            off += x;
        }
    }
    for (size_t s = 0; s < seg; ++s) {
        off += layout[r][s];
    }
    return off;
}

// Collect every global [from, to) interval covered by `plans`, checking the per-slice invariants
// (offsets consistent with the row-range count, ranges inside their segment, slices contiguous).
std::vector<Interval> collect_global_ranges(const Layout& layout,
                                            const std::vector<RowidScannerPlan>& plans) {
    std::vector<Interval> out;
    for (const auto& plan : plans) {
        EXPECT_FALSE(plan.empty());
        for (const auto& slice : plan) {
            const auto& segs = layout[slice.rowset_index];
            const int64_t first = slice.segment_offsets.first;
            const int64_t second = slice.segment_offsets.second;
            EXPECT_LT(first, second);
            EXPECT_EQ(static_cast<size_t>(second - first), slice.segment_row_ranges.size());
            for (int64_t k = 0; k < second - first; ++k) {
                const size_t seg = static_cast<size_t>(first + k);
                RowRanges rr = slice.segment_row_ranges[k]; // copy: accessors below are fine, is_empty() is non-const
                EXPECT_FALSE(rr.is_empty());
                const int64_t base = seg_global_begin(layout, slice.rowset_index, seg);
                for (size_t ri = 0; ri < rr.range_size(); ++ri) {
                    const int64_t f = rr.get_range_from(ri);
                    const int64_t t = rr.get_range_to(ri);
                    EXPECT_GE(f, 0);
                    EXPECT_LE(t, segs[seg]);
                    EXPECT_LT(f, t);
                    out.emplace_back(base + f, base + t);
                }
            }
        }
    }
    std::sort(out.begin(), out.end());
    return out;
}

// Merge sorted intervals, asserting they are pairwise non-overlapping (adjacent allowed).
std::vector<Interval> merge_disjoint(const std::vector<Interval>& v) {
    std::vector<Interval> m;
    for (const auto& iv : v) {
        EXPECT_LT(iv.first, iv.second);
        if (!m.empty()) {
            EXPECT_GE(iv.first, m.back().second) << "ranges overlap";
            if (iv.first == m.back().second) {
                m.back().second = iv.second;
                continue;
            }
        }
        m.emplace_back(iv);
    }
    return m;
}

// Plan every split of `layout` and assert: (a) each split covers exactly its [g_begin, g_end);
// (b) the union of all splits tiles [0, total) with no overlap and no gap.
void verify_full_coverage(const Layout& layout, int32_t split_count, int64_t rows_per_scanner) {
    const int64_t total = total_rows(layout);
    std::vector<Interval> all;
    for (int32_t sid = 0; sid < split_count; ++sid) {
        const auto [gb, ge] = rowid_split_range(total, sid, split_count);
        const auto plans = plan_rowid_scanners(layout, gb, ge, rows_per_scanner);
        const auto ranges = collect_global_ranges(layout, plans);
        const auto merged = merge_disjoint(ranges);
        if (gb >= ge) {
            EXPECT_TRUE(merged.empty()) << "split " << sid << "/" << split_count;
        } else {
            ASSERT_EQ(merged.size(), 1u) << "split " << sid << "/" << split_count;
            EXPECT_EQ(merged.front().first, gb);
            EXPECT_EQ(merged.front().second, ge);
        }
        all.insert(all.end(), ranges.begin(), ranges.end());
    }
    std::sort(all.begin(), all.end());
    const auto merged_all = merge_disjoint(all);
    if (total == 0) {
        EXPECT_TRUE(merged_all.empty());
    } else {
        ASSERT_EQ(merged_all.size(), 1u);
        EXPECT_EQ(merged_all.front().first, 0);
        EXPECT_EQ(merged_all.front().second, total);
    }
}

} // namespace

// The split formula tiles [0, total) exactly: pieces meet end-to-end and the last reaches total.
TEST(RowidSplitPlannerTest, SplitRangeFormula) {
    auto whole = rowid_split_range(1000, 0, 1);
    EXPECT_EQ(whole.first, 0);
    EXPECT_EQ(whole.second, 1000);
    // split_count <= 1 (including 0) means the whole tablet.
    auto whole0 = rowid_split_range(1000, 0, 0);
    EXPECT_EQ(whole0.first, 0);
    EXPECT_EQ(whole0.second, 1000);

    auto first4 = rowid_split_range(1000, 0, 4);
    EXPECT_EQ(first4.first, 0);
    EXPECT_EQ(first4.second, 250);
    auto last4 = rowid_split_range(1000, 3, 4);
    EXPECT_EQ(last4.first, 750);
    EXPECT_EQ(last4.second, 1000);

    // uneven split still tiles [0, total).
    for (int64_t total : {0, 1, 3, 1003, 999983}) {
        for (int32_t n : {1, 2, 3, 4, 7, 64}) {
            int64_t prev = 0;
            for (int32_t i = 0; i < n; ++i) {
                const auto [b, e] = rowid_split_range(total, i, n);
                EXPECT_EQ(b, prev);
                EXPECT_LE(b, e);
                prev = e;
            }
            EXPECT_EQ(prev, total);
        }
    }
}

// Whole tablet + huge rows_per_scanner => a single scanner spanning all rowsets.
TEST(RowidSplitPlannerTest, WholeTabletSingleScanner) {
    Layout layout = {{100, 200}, {300}};
    const auto plans = plan_rowid_scanners(layout, 0, 600, /*rows_per_scanner=*/100000);
    ASSERT_EQ(plans.size(), 1u);
    const auto merged = merge_disjoint(collect_global_ranges(layout, plans));
    ASSERT_EQ(merged.size(), 1u);
    EXPECT_EQ(merged.front().first, 0);
    EXPECT_EQ(merged.front().second, 600);
}

// Coverage + non-overlap across a matrix of split counts and per-scanner sizes.
TEST(RowidSplitPlannerTest, FullCoverageVariousSplits) {
    Layout layout = {{1000, 1000, 500}, {2000}, {750, 1250}};
    for (int32_t n : {1, 2, 3, 4, 7, 16}) {
        for (int64_t rps : {(int64_t)1, (int64_t)333, (int64_t)1024, (int64_t)100000}) {
            verify_full_coverage(layout, n, rps);
        }
    }
}

// Empty rowsets (no segments) occupy an index but contribute no rows.
TEST(RowidSplitPlannerTest, EmptyRowsets) {
    Layout layout = {{500}, {}, {500}};
    verify_full_coverage(layout, 4, 100000);
    verify_full_coverage(layout, 3, 100);
    verify_full_coverage(layout, 1, 1);
}

// More splits than rows: trailing splits are empty, coverage still exact.
TEST(RowidSplitPlannerTest, MoreSplitsThanRows) {
    Layout layout = {{3}};
    verify_full_coverage(layout, 5, 100000);
    verify_full_coverage(layout, 10, 1);
}

// A single large segment split into many scanners by rows_per_scanner.
TEST(RowidSplitPlannerTest, ManyScannersWithinSplit) {
    Layout layout = {{1000}};
    const auto plans = plan_rowid_scanners(layout, 0, 1000, /*rows_per_scanner=*/100);
    EXPECT_GE(plans.size(), 8u);
    const auto merged = merge_disjoint(collect_global_ranges(layout, plans));
    ASSERT_EQ(merged.size(), 1u);
    EXPECT_EQ(merged.front().first, 0);
    EXPECT_EQ(merged.front().second, 1000);
}

// Empty / degenerate ranges return no plans.
TEST(RowidSplitPlannerTest, EmptyRange) {
    Layout layout = {{1000}};
    EXPECT_TRUE(plan_rowid_scanners(layout, 500, 500, 100).empty());
    EXPECT_TRUE(plan_rowid_scanners(layout, 600, 500, 100).empty());
    EXPECT_TRUE(plan_rowid_scanners({}, 0, 0, 100).empty());
}

// A split whose range starts mid-segment of one rowset and ends mid-segment of a later rowset.
TEST(RowidSplitPlannerTest, RangeSpansRowsetsPartial) {
    Layout layout = {{100, 100}, {100, 100}}; // total 400
    // [150, 350): second half of rowset0/seg1, all of rowset1/seg0, first half of rowset1/seg1.
    const auto plans = plan_rowid_scanners(layout, 150, 350, /*rows_per_scanner=*/100000);
    const auto merged = merge_disjoint(collect_global_ranges(layout, plans));
    ASSERT_EQ(merged.size(), 1u);
    EXPECT_EQ(merged.front().first, 150);
    EXPECT_EQ(merged.front().second, 350);
}

} // namespace doris
