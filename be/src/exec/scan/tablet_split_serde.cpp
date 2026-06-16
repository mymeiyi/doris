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

#include "exec/scan/tablet_split_serde.h"

#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/olap_file.pb.h>

#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/segment/row_ranges.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet_meta.h" // DeleteBitmap

namespace doris {

using segment_v2::RowRange;
using segment_v2::RowRanges;

Status compute_rowid_splits(const TabletReadSource& entire_read_source,
                            const std::map<RowsetId, std::vector<size_t>>& all_segments_rows,
                            size_t rows_per_scanner,
                            std::vector<TabletReadSource>* out_partials) {
    // NOTE: keep this in lockstep with
    // ParallelScannerBuilder::_build_scanners_by_rowid. The only difference is the
    // emit site: instead of building an OlapScanner we collect a partial
    // TabletReadSource so it can be shipped to another BE.
    TabletReadSource partitial_read_source;
    int64_t rows_collected = 0;

    auto emit_partial = [&]() {
        partitial_read_source.delete_predicates = entire_read_source.delete_predicates;
        partitial_read_source.delete_bitmap = entire_read_source.delete_bitmap;
        out_partials->emplace_back(std::move(partitial_read_source));
        partitial_read_source = {};
    };

    for (auto& rs_split : entire_read_source.rs_splits) {
        auto reader = rs_split.rs_reader;
        auto rowset = reader->rowset();
        const auto rowset_id = rowset->rowset_id();

        auto seg_rows_it = all_segments_rows.find(rowset_id);
        DCHECK(seg_rows_it != all_segments_rows.end());
        if (seg_rows_it == all_segments_rows.end()) {
            return Status::InternalError("missing segment rows for rowset {}",
                                         rowset_id.to_string());
        }
        const auto& segments_rows = seg_rows_it->second;

        if (rowset->num_rows() == 0) {
            continue;
        }

        int64_t segment_start = 0;
        auto split = RowSetSplits(reader->clone());

        for (size_t i = 0; i != segments_rows.size(); ++i) {
            const size_t rows_of_segment = segments_rows[i];
            RowRanges row_ranges;
            int64_t offset_in_segment = 0;

            // try to split large segments into RowRanges
            while (offset_in_segment < static_cast<int64_t>(rows_of_segment)) {
                const int64_t remaining_rows =
                        static_cast<int64_t>(rows_of_segment) - offset_in_segment;
                auto rows_need = static_cast<int64_t>(rows_per_scanner) - rows_collected;

                // 0.9: try to avoid splitting the segments into excessively small parts.
                if (rows_need >= remaining_rows * 9 / 10) {
                    rows_need = remaining_rows;
                }
                DCHECK_LE(rows_need, remaining_rows);

                // RowRange stands for range: [From, To), From is inclusive, To is exclusive.
                row_ranges.add({offset_in_segment, offset_in_segment + rows_need});
                rows_collected += rows_need;
                offset_in_segment += rows_need;

                // If collected enough rows, finish a partial read source.
                if (rows_collected >= static_cast<int64_t>(rows_per_scanner)) {
                    split.segment_offsets.first = segment_start;
                    split.segment_offsets.second = i + 1;
                    split.segment_row_ranges.emplace_back(std::move(row_ranges));

                    DCHECK_EQ(split.segment_offsets.second - split.segment_offsets.first,
                              split.segment_row_ranges.size());

                    partitial_read_source.rs_splits.emplace_back(std::move(split));

                    emit_partial();

                    split = RowSetSplits(reader->clone());
                    row_ranges = RowRanges();

                    segment_start =
                            offset_in_segment < static_cast<int64_t>(rows_of_segment) ? i : i + 1;
                    rows_collected = 0;
                }
            }

            // The non-empty `row_ranges` means there are some rows left in this segment
            // not added into `split`.
            if (!row_ranges.is_empty()) {
                DCHECK_GT(rows_collected, 0);
                DCHECK_EQ(row_ranges.to(), static_cast<int64_t>(rows_of_segment));
                split.segment_row_ranges.emplace_back(std::move(row_ranges));
            }
        }

        DCHECK_LE(rows_collected, static_cast<int64_t>(rows_per_scanner));
        if (rows_collected > 0) {
            split.segment_offsets.first = segment_start;
            split.segment_offsets.second = segments_rows.size();
            DCHECK_GT(split.segment_offsets.second, split.segment_offsets.first);
            DCHECK_EQ(split.segment_row_ranges.size(),
                      split.segment_offsets.second - split.segment_offsets.first);
            partitial_read_source.rs_splits.emplace_back(std::move(split));
        }
    } // end `for (auto& rs_split : rs_splits)`

    DCHECK_LE(rows_collected, static_cast<int64_t>(rows_per_scanner));
    if (rows_collected > 0) {
        DCHECK_GT(partitial_read_source.rs_splits.size(), 0);
        emit_partial();
    }

    return Status::OK();
}

Status read_source_to_tablet_split(int64_t pinned_version, int64_t split_id,
                                   const TabletReadSource& partial, bool is_mow,
                                   TTabletSplit* out) {
    out->pinned_version = pinned_version;
    out->__set_split_id(split_id);

    out->rs_splits.clear();
    out->rs_splits.reserve(partial.rs_splits.size());
    for (const auto& rs_split : partial.rs_splits) {
        TRowSetSplit t;
        const auto& rs_meta = rs_split.rs_reader->rowset()->rowset_meta();
        RowsetMetaPB pb;
        rs_meta->to_rowset_pb(&pb);
        if (!pb.SerializeToString(&t.rowset_meta_pb)) {
            return Status::InternalError("failed to serialize rowset meta, rowset={}",
                                         rs_meta->rowset_id().to_string());
        }
        t.start_segment_id = rs_split.segment_offsets.first;
        t.end_segment_id = rs_split.segment_offsets.second;

        if (!rs_split.segment_row_ranges.empty()) {
            std::vector<TSegmentRowRanges> seg_ranges_list;
            seg_ranges_list.reserve(rs_split.segment_row_ranges.size());
            for (auto& row_ranges : rs_split.segment_row_ranges) {
                TSegmentRowRanges seg;
                for (size_t r = 0; r < row_ranges.range_size(); ++r) {
                    TRowRange range;
                    range.begin_row = row_ranges.get_range_from(r);
                    range.end_row = row_ranges.get_range_to(r);
                    seg.ranges.push_back(range);
                }
                seg_ranges_list.push_back(std::move(seg));
            }
            t.__set_segment_row_ranges(std::move(seg_ranges_list));
        }
        out->rs_splits.push_back(std::move(t));
    }

    if (!partial.delete_predicates.empty()) {
        std::vector<std::string> metas;
        metas.reserve(partial.delete_predicates.size());
        for (const auto& dp : partial.delete_predicates) {
            RowsetMetaPB pb;
            dp->to_rowset_pb(&pb);
            std::string s;
            if (!pb.SerializeToString(&s)) {
                return Status::InternalError("failed to serialize delete predicate meta, rowset={}",
                                             dp->rowset_id().to_string());
            }
            metas.push_back(std::move(s));
        }
        out->__set_delete_predicate_metas(std::move(metas));
    }

    if (is_mow && partial.delete_bitmap != nullptr) {
        DeleteBitmapPB bitmap_pb = partial.delete_bitmap->to_pb();
        std::string s;
        if (!bitmap_pb.SerializeToString(&s)) {
            return Status::InternalError("failed to serialize delete bitmap");
        }
        out->__set_delete_bitmap_pb(std::move(s));
    }

    return Status::OK();
}

Status tablet_split_to_read_source(const TTabletSplit& split, const TabletSchemaSPtr& schema,
                                   int64_t tablet_id, TabletReadSource* out) {
    out->rs_splits.reserve(split.rs_splits.size());
    for (const auto& t : split.rs_splits) {
        auto rs_meta = std::make_shared<RowsetMeta>();
        RowsetMetaPB pb;
        if (!pb.ParseFromString(t.rowset_meta_pb)) {
            return Status::InternalError("failed to parse rowset meta pb in tablet split");
        }
        if (!rs_meta->init_from_pb(pb)) {
            return Status::InternalError("failed to init rowset meta from pb in tablet split");
        }

        RowsetSharedPtr rowset;
        // tablet_path is empty: cloud rowsets resolve segment paths purely from the
        // RowsetMeta (resource_id + tablet_id + rowset_id), see cross_be_parallel_scan.md §3.2.
        RETURN_IF_ERROR(RowsetFactory::create_rowset(schema, {}, rs_meta, &rowset));
        RowsetReaderSharedPtr reader;
        RETURN_IF_ERROR(rowset->create_reader(&reader));

        RowSetSplits rs_split(reader);
        rs_split.segment_offsets = {t.start_segment_id, t.end_segment_id};
        if (t.__isset.segment_row_ranges) {
            rs_split.segment_row_ranges.reserve(t.segment_row_ranges.size());
            for (const auto& seg : t.segment_row_ranges) {
                RowRanges row_ranges;
                for (const auto& range : seg.ranges) {
                    row_ranges.add(RowRange(range.begin_row, range.end_row));
                }
                rs_split.segment_row_ranges.emplace_back(std::move(row_ranges));
            }
            DCHECK_EQ(rs_split.segment_row_ranges.size(),
                      rs_split.segment_offsets.second - rs_split.segment_offsets.first);
        }
        out->rs_splits.emplace_back(std::move(rs_split));
    }

    if (split.__isset.delete_predicate_metas) {
        out->delete_predicates.reserve(split.delete_predicate_metas.size());
        for (const auto& s : split.delete_predicate_metas) {
            auto rs_meta = std::make_shared<RowsetMeta>();
            RowsetMetaPB pb;
            if (!pb.ParseFromString(s)) {
                return Status::InternalError("failed to parse delete predicate meta pb");
            }
            if (!rs_meta->init_from_pb(pb)) {
                return Status::InternalError("failed to init delete predicate meta from pb");
            }
            out->delete_predicates.push_back(std::move(rs_meta));
        }
    }

    if (split.__isset.delete_bitmap_pb) {
        DeleteBitmapPB pb;
        if (!pb.ParseFromString(split.delete_bitmap_pb)) {
            return Status::InternalError("failed to parse delete bitmap pb in tablet split");
        }
        out->delete_bitmap =
                std::make_shared<DeleteBitmap>(DeleteBitmap::from_pb(pb, tablet_id));
    }

    return Status::OK();
}

} // namespace doris
