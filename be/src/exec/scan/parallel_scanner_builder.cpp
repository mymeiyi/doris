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

#include "exec/scan/parallel_scanner_builder.h"

#include <algorithm>
#include <cstddef>
#include <utility>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet_hotspot.h"
#include "cloud/config.h"
#include "common/consts.h"
#include "common/status.h"
#include "core/field.h"
#include "exec/operator/olap_scan_operator.h"
#include "exec/scan/olap_scanner.h"
#include "exec/scan/rowid_split_planner.h"
#include "storage/key_coder.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/segment/segment_loader.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

namespace {

// MVP first-key-column types supported by key-range tablet split. Kept in sync with the FE
// gate (OlapScanNode.isFirstKeyColumnSplittableInteger): fixed-length integers only, so the
// boundary can be decoded from the memcomparable segment key bounds and round-tripped.
bool is_supported_split_key_type(FieldType ft) {
    return ft == FieldType::OLAP_FIELD_TYPE_TINYINT ||
           ft == FieldType::OLAP_FIELD_TYPE_SMALLINT ||
           ft == FieldType::OLAP_FIELD_TYPE_INT || ft == FieldType::OLAP_FIELD_TYPE_BIGINT;
}

// Decode the first sort-key column of a memcomparable-encoded key (a segment min/max key, see
// SegmentWriter::_full_encode_keys) into int64. The key is laid out as a 1-byte per-column
// marker followed by the order-preserving value bytes. Returns false (caller skips the sample)
// for a null/minimal first column, a too-short key, or an unsupported type.
bool decode_first_int_key(const std::string& encoded, FieldType ft, int64_t* out) {
    if (encoded.size() < 2) {
        return false;
    }
    if (static_cast<uint8_t>(encoded[0]) != KeyConsts::KEY_NORMAL_MARKER) {
        return false; // null / minimal first column
    }
    Slice s(encoded.data() + 1, encoded.size() - 1);
    const KeyCoder* coder = get_key_coder(ft);
    switch (ft) {
    case FieldType::OLAP_FIELD_TYPE_TINYINT: {
        int8_t v;
        if (!coder->decode_ascending(&s, sizeof(v), reinterpret_cast<uint8_t*>(&v)).ok()) {
            return false;
        }
        *out = v;
        return true;
    }
    case FieldType::OLAP_FIELD_TYPE_SMALLINT: {
        int16_t v;
        if (!coder->decode_ascending(&s, sizeof(v), reinterpret_cast<uint8_t*>(&v)).ok()) {
            return false;
        }
        *out = v;
        return true;
    }
    case FieldType::OLAP_FIELD_TYPE_INT: {
        int32_t v;
        if (!coder->decode_ascending(&s, sizeof(v), reinterpret_cast<uint8_t*>(&v)).ok()) {
            return false;
        }
        *out = v;
        return true;
    }
    case FieldType::OLAP_FIELD_TYPE_BIGINT: {
        int64_t v;
        if (!coder->decode_ascending(&s, sizeof(v), reinterpret_cast<uint8_t*>(&v)).ok()) {
            return false;
        }
        *out = v;
        return true;
    }
    default:
        return false;
    }
}

// Build a one-column OlapTuple holding `value` typed as the first sort-key column, suitable as
// an OlapScanRange prefix bound (RowCursor::init takes the first key_size columns).
bool make_first_key_tuple(FieldType ft, int64_t value, OlapTuple* tuple) {
    switch (ft) {
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        tuple->add_field(Field::create_field<TYPE_TINYINT>(static_cast<int8_t>(value)));
        return true;
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        tuple->add_field(Field::create_field<TYPE_SMALLINT>(static_cast<int16_t>(value)));
        return true;
    case FieldType::OLAP_FIELD_TYPE_INT:
        tuple->add_field(Field::create_field<TYPE_INT>(static_cast<int32_t>(value)));
        return true;
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        tuple->add_field(Field::create_field<TYPE_BIGINT>(static_cast<int64_t>(value)));
        return true;
    default:
        return false;
    }
}

} // namespace

Status ParallelScannerBuilder::build_scanners(std::list<ScannerSPtr>& scanners) {
    RETURN_IF_ERROR(_load());
    if (_scan_parallelism_by_per_segment) {
        // Per-segment scanners scan whole segments and do not honor rowid splits. If a split was
        // requested here, every fan-out BE would whole-scan the tablet -- refuse instead.
        for (const auto& split : _tablet_splits) {
            if (split.second > 1) {
                return Status::NotSupported(
                        "tablet split is not supported with per-segment scan parallelism");
            }
        }
        return _build_scanners_by_per_segment(scanners);
    } else if (_is_dup_mow_key) {
        // No-merge tables (DUP / MoW read-as-dup / preagg): split by rowids within segments.
        return _build_scanners_by_rowid(scanners);
    } else {
        // Merge-required tables (real AGG / MoR-unique): split by key range so each key (and
        // all its versions across rowsets) stays on one BE and merges correctly.
        return _build_scanners_by_key_range(scanners);
    }
}

Status ParallelScannerBuilder::_build_scanners_by_key_range(std::list<ScannerSPtr>& scanners) {
    size_t tablet_idx = 0;
    for (auto&& [tablet, version] : _tablets) {
        DCHECK(_all_read_sources.contains(tablet->tablet_id()));
        auto& entire_read_source = _all_read_sources[tablet->tablet_id()];

        if (config::is_cloud_mode()) {
            // FIXME(plat1ko): Avoid pointer cast
            ExecEnv::GetInstance()->storage_engine().to_cloud().tablet_hotspot().count(*tablet);
        }

        // Helper: one scanner reading the WHOLE tablet (all rowsets/segments) with the given key
        // ranges. Clones each rowset reader; leaves segment_offsets at the default {0,0} so the
        // reader scans every segment (no rowid slicing). delete_predicates/bitmap are shared.
        auto build_full_scanner = [&](const std::vector<OlapScanRange*>& key_ranges) {
            TabletReadSource read_source;
            read_source.rs_splits.reserve(entire_read_source.rs_splits.size());
            for (auto& rs_split : entire_read_source.rs_splits) {
                read_source.rs_splits.emplace_back(RowSetSplits(rs_split.rs_reader->clone()));
            }
            read_source.delete_predicates = entire_read_source.delete_predicates;
            read_source.delete_bitmap = entire_read_source.delete_bitmap;
            scanners.emplace_back(
                    _build_scanner(tablet, version, key_ranges, std::move(read_source)));
        };

        // Resolve this BE's split identity. split_count <= 1 means no split was assigned, so this
        // BE owns the whole tablet (a single merging reader, honoring any predicate key ranges).
        int32_t split_id = 0;
        int32_t split_count = 1;
        if (tablet_idx < _tablet_splits.size() && _tablet_splits[tablet_idx].second > 1) {
            split_id = _tablet_splits[tablet_idx].first;
            split_count = _tablet_splits[tablet_idx].second;
        }
        if (split_count <= 1) {
            build_full_scanner(_key_ranges);
            ++tablet_idx;
            continue;
        }

        const FieldType first_key_type = tablet->tablet_schema()->column(0).type();
        if (!is_supported_split_key_type(first_key_type)) {
            // FE eligibility (isFirstKeyColumnSplittableInteger) should have prevented assigning a
            // split here; fail loudly if the two diverged rather than silently mis-scan.
            return Status::NotSupported(
                    "key-range tablet split requires an integer first key column, but tablet {} "
                    "has first key column type {}",
                    tablet->tablet_id(), static_cast<int>(first_key_type));
        }

        // Sample the key space: decode the first key column from every segment's min/max key
        // across all rowsets. These bounds are real keys carried in the rowset meta (no extra
        // IO) and are identical on every BE at this version -> deterministic boundaries.
        std::vector<int64_t> samples;
        for (auto& rs_split : entire_read_source.rs_splits) {
            auto rowset = rs_split.rs_reader->rowset();
            std::vector<KeyBoundsPB> seg_bounds;
            RETURN_IF_ERROR(rowset->get_segments_key_bounds(&seg_bounds));
            for (auto& kb : seg_bounds) {
                int64_t v = 0;
                if (decode_first_int_key(kb.min_key(), first_key_type, &v)) {
                    samples.emplace_back(v);
                }
                if (decode_first_int_key(kb.max_key(), first_key_type, &v)) {
                    samples.emplace_back(v);
                }
            }
        }

        if (samples.empty()) {
            // No key bounds available (e.g. all rowsets empty). Let split_id 0 own everything and
            // the rest own nothing -- coverage stays complete and non-overlapping.
            if (split_id == 0) {
                build_full_scanner({});
            }
            ++tablet_idx;
            continue;
        }
        std::sort(samples.begin(), samples.end());

        // Boundaries b[0..split_count] over [global_min, global_max] (both real keys, so every
        // key is covered). b[0]=min, b[split_count]=max, interior picked at even sample quantiles.
        // This BE owns the half-open range [b[split_id], b[split_id+1]); the LAST split closes the
        // upper end so the global max key is included. Equal boundaries -> empty splits (still
        // correct). Boundary precision only affects load balance, not correctness.
        auto boundary_at = [&](int32_t j) -> int64_t {
            size_t idx = (samples.size() * static_cast<size_t>(j)) / static_cast<size_t>(split_count);
            idx = std::min(idx, samples.size() - 1);
            return samples[idx];
        };
        const int64_t lo = boundary_at(split_id);
        const int64_t hi = boundary_at(split_id + 1);
        const bool is_last = (split_id == split_count - 1);

        auto range = std::make_unique<OlapScanRange>();
        range->begin_include = true;
        range->end_include = is_last;
        range->has_lower_bound = true;
        range->has_upper_bound = true;
        if (!make_first_key_tuple(first_key_type, lo, &range->begin_scan_range) ||
            !make_first_key_tuple(first_key_type, hi, &range->end_scan_range)) {
            return Status::NotSupported("key-range tablet split: unsupported first key column type");
        }
        // NOTE: any predicate-derived key ranges (_key_ranges) are intentionally replaced by the
        // split range here; the predicates remain as scan conjuncts, so correctness is preserved
        // (we only forgo predicate short-key pruning for split scans). MVP trade-off.
        // Ownership goes to the local state (outlives the scanners; this builder does not).
        OlapScanRange* range_ptr = _parent->own_split_key_range(std::move(range));
        std::vector<OlapScanRange*> scanner_key_ranges {range_ptr};

        build_full_scanner(scanner_key_ranges);
        ++tablet_idx;
    }

    return Status::OK();
}

Status ParallelScannerBuilder::_build_scanners_by_rowid(std::list<ScannerSPtr>& scanners) {
    DCHECK_GE(_rows_per_scanner, _min_rows_per_scanner);

    size_t tablet_idx = 0;
    for (auto&& [tablet, version] : _tablets) {
        DCHECK(_all_read_sources.contains(tablet->tablet_id()));
        auto& entire_read_source = _all_read_sources[tablet->tablet_id()];

        if (config::is_cloud_mode()) {
            // FIXME(plat1ko): Avoid pointer cast
            ExecEnv::GetInstance()->storage_engine().to_cloud().tablet_hotspot().count(*tablet);
        }

        // Build the per-rowset segment row layout, aligned 1:1 with `rs_splits` (version_path
        // order). Together with the snapshot `version` this defines the tablet-global rowid
        // space deterministically across BEs (see rowid_split_planner.h).
        std::vector<std::vector<int64_t>> rowsets_segment_rows;
        rowsets_segment_rows.reserve(entire_read_source.rs_splits.size());
        int64_t tablet_total_rows = 0;
        for (auto& rs_split : entire_read_source.rs_splits) {
            const auto rowset_id = rs_split.rs_reader->rowset()->rowset_id();
            const auto& segs = _all_segments_rows[rowset_id];
            auto& dst = rowsets_segment_rows.emplace_back();
            dst.reserve(segs.size());
            for (auto rows : segs) {
                dst.emplace_back(static_cast<int64_t>(rows));
                tablet_total_rows += static_cast<int64_t>(rows);
            }
        }

        // Resolve this tablet's split identity into a global rowid sub-range. split_count <= 1
        // (the default for the legacy single-BE path) means the whole tablet [0, total).
        int32_t split_id = 0;
        int32_t split_count = 1;
        if (tablet_idx < _tablet_splits.size() && _tablet_splits[tablet_idx].second > 1) {
            split_id = _tablet_splits[tablet_idx].first;
            split_count = _tablet_splits[tablet_idx].second;
        }
        const auto [g_begin, g_end] = rowid_split_range(tablet_total_rows, split_id, split_count);

        // Plan (pure) then materialize: attach a cloned reader per slice and the shared delete
        // metadata. A scanner may span several rowsets, so each plan becomes one read source.
        const auto plans = plan_rowid_scanners(rowsets_segment_rows, g_begin, g_end,
                                               static_cast<int64_t>(_rows_per_scanner));
        for (auto& plan : plans) {
            TabletReadSource read_source;
            read_source.rs_splits.reserve(plan.size());
            for (auto& slice : plan) {
                auto& reader = entire_read_source.rs_splits[slice.rowset_index].rs_reader;
                RowSetSplits split(reader->clone());
                split.segment_offsets = slice.segment_offsets;
                split.segment_row_ranges = slice.segment_row_ranges;
                DCHECK_LT(split.segment_offsets.first, split.segment_offsets.second);
                DCHECK_EQ(split.segment_row_ranges.size(),
                          split.segment_offsets.second - split.segment_offsets.first);
                read_source.rs_splits.emplace_back(std::move(split));
            }
            read_source.delete_predicates = entire_read_source.delete_predicates;
            read_source.delete_bitmap = entire_read_source.delete_bitmap;
            scanners.emplace_back(
                    _build_scanner(tablet, version, _key_ranges, std::move(read_source)));
        }
        ++tablet_idx;
    }

    return Status::OK();
}

// Build scanners so that each segment is exclusively scanned by a single scanner.
// This guarantees the number of scanners equals the number of segments across all rowsets
// for the involved tablets. It preserves delete predicates and key ranges, and clones
// RowsetReader per scanner to avoid sharing between scanners.
Status ParallelScannerBuilder::_build_scanners_by_per_segment(std::list<ScannerSPtr>& scanners) {
    DCHECK_GE(_rows_per_scanner, _min_rows_per_scanner);

    for (auto&& [tablet, version] : _tablets) {
        DCHECK(_all_read_sources.contains(tablet->tablet_id()));
        auto& entire_read_source = _all_read_sources[tablet->tablet_id()];

        if (config::is_cloud_mode()) {
            // FIXME(plat1ko): Avoid pointer cast
            ExecEnv::GetInstance()->storage_engine().to_cloud().tablet_hotspot().count(*tablet);
        }

        // For each RowSet split in the read source, split by segment id and build
        // one scanner per segment. Keep delete predicates shared.
        for (auto& rs_split : entire_read_source.rs_splits) {
            auto reader = rs_split.rs_reader;
            auto rowset = reader->rowset();
            const auto rowset_id = rowset->rowset_id();
            const auto& segments_rows = _all_segments_rows[rowset_id];
            if (segments_rows.empty() || rowset->num_rows() == 0) {
                continue;
            }

            // Build scanners for [i, i+1) segment range, without row-range slicing.
            for (int64_t i = 0; i < rowset->num_segments(); ++i) {
                RowSetSplits split(reader->clone());
                split.segment_offsets.first = i;
                split.segment_offsets.second = i + 1;
                // No row-ranges slicing; scan whole segment i.
                DCHECK_GE(split.segment_offsets.second, split.segment_offsets.first + 1);

                TabletReadSource partitial_read_source;
                partitial_read_source.rs_splits.emplace_back(std::move(split));

                scanners.emplace_back(
                        _build_scanner(tablet, version, _key_ranges,
                                       {.rs_splits = std::move(partitial_read_source.rs_splits),
                                        .delete_predicates = entire_read_source.delete_predicates,
                                        .delete_bitmap = entire_read_source.delete_bitmap}));
            }
        }
    }

    return Status::OK();
}

/**
 * Load rowsets of each tablet with specified version, segments of each rowset.
 */
Status ParallelScannerBuilder::_load() {
    _total_rows = 0;
    size_t idx = 0;
    bool enable_segment_cache = _state->query_options().__isset.enable_segment_cache
                                        ? _state->query_options().enable_segment_cache
                                        : true;
    for (auto&& [tablet, version] : _tablets) {
        const auto tablet_id = tablet->tablet_id();
        _all_read_sources[tablet_id] = _read_sources[idx];
        const auto& read_source = _all_read_sources[tablet_id];
        for (auto& rs_split : read_source.rs_splits) {
            auto rowset = rs_split.rs_reader->rowset();
            RETURN_IF_ERROR(rowset->load());
            const auto rowset_id = rowset->rowset_id();

            auto beta_rowset = std::dynamic_pointer_cast<BetaRowset>(rowset);
            std::vector<uint32_t> segment_rows;
            RETURN_IF_ERROR(beta_rowset->get_segment_num_rows(&segment_rows, enable_segment_cache,
                                                              &_builder_stats));
            auto segment_count = rowset->num_segments();
            for (int64_t i = 0; i != segment_count; i++) {
                _all_segments_rows[rowset_id].emplace_back(segment_rows[i]);
            }
            _total_rows += rowset->num_rows();
        }
        idx++;
    }

    _rows_per_scanner = _total_rows / _max_scanners_count;
    _rows_per_scanner = std::max<size_t>(_rows_per_scanner, _min_rows_per_scanner);

    return Status::OK();
}

std::shared_ptr<OlapScanner> ParallelScannerBuilder::_build_scanner(
        BaseTabletSPtr tablet, int64_t version, const std::vector<OlapScanRange*>& key_ranges,
        TabletReadSource&& read_source) {
    OlapScanner::Params params {
            _state,  _scanner_profile.get(), key_ranges,   std::move(tablet),
            version, std::move(read_source), _limit,       _is_preaggregation,
            false,   TBinlogScanType::NONE,  std::nullopt, std::nullopt,
    };
    return OlapScanner::create_shared(_parent, std::move(params));
}

} // namespace doris
