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
#include "core/decimal12.h"
#include "core/extended_types.h"
#include "core/field.h"
#include "core/string_ref.h"
#include "exec/operator/olap_scan_operator.h"
#include "exec/scan/olap_scanner.h"
#include "exec/scan/rowid_split_planner.h"
#include "storage/index/short_key_index.h"
#include "storage/key_coder.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_loader.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

namespace {

// (FieldType, PrimitiveType) pairs whose first sort-key column supports key-range tablet split:
// fixed-length, order-preserving scalar keys whose memcomparable segment key bounds can be
// decoded to a single comparable value and round-tripped back into a seek key. Kept in sync with
// the FE gate (OlapScanNode.isFirstKeyColumnSplittable). Every supported type is stored as a
// fixed-width, order-preserving value that maps monotonically into a 256-bit signed integer (see
// storage_to_key): integers (incl. LARGEINT) and DECIMAL32/64/128 are signed and sign-extend;
// DECIMAL256 is a native 256-bit integer; date/datetime (legacy v1 + v2) and IPv4/IPv6 are
// unsigned and zero-extend; legacy DECIMALV2 (decimal12_t) folds {integer, fraction} into one
// scalar. Floats, strings, multi-column prefixes and truncated keys are excluded.
#define DORIS_APPLY_FOR_SPLIT_KEY_TYPES(M)         \
    M(OLAP_FIELD_TYPE_TINYINT, TYPE_TINYINT)       \
    M(OLAP_FIELD_TYPE_SMALLINT, TYPE_SMALLINT)     \
    M(OLAP_FIELD_TYPE_INT, TYPE_INT)               \
    M(OLAP_FIELD_TYPE_BIGINT, TYPE_BIGINT)         \
    M(OLAP_FIELD_TYPE_LARGEINT, TYPE_LARGEINT)     \
    M(OLAP_FIELD_TYPE_DATE, TYPE_DATE)             \
    M(OLAP_FIELD_TYPE_DATETIME, TYPE_DATETIME)     \
    M(OLAP_FIELD_TYPE_DATEV2, TYPE_DATEV2)         \
    M(OLAP_FIELD_TYPE_DATETIMEV2, TYPE_DATETIMEV2) \
    M(OLAP_FIELD_TYPE_DECIMAL, TYPE_DECIMALV2)     \
    M(OLAP_FIELD_TYPE_DECIMAL32, TYPE_DECIMAL32)   \
    M(OLAP_FIELD_TYPE_DECIMAL64, TYPE_DECIMAL64)   \
    M(OLAP_FIELD_TYPE_DECIMAL128I, TYPE_DECIMAL128I) \
    M(OLAP_FIELD_TYPE_DECIMAL256, TYPE_DECIMAL256) \
    M(OLAP_FIELD_TYPE_IPV4, TYPE_IPV4)             \
    M(OLAP_FIELD_TYPE_IPV6, TYPE_IPV6)

// Variable-length string first key columns whose raw memcomparable bytes are already
// lexicographically ordered. These do NOT go through the fixed-width decode macro above (string
// KeyCoder::decode_ascending is unimplemented and the bytes may be truncated); they are compared
// directly as std::string. See StringKeyDomain. Kept in sync with the FE gate
// (OlapScanNode.isFirstKeyColumnSplittable). STRING is excluded -- it is never a valid key column.
bool is_string_split_key_type(FieldType ft) {
    return ft == FieldType::OLAP_FIELD_TYPE_CHAR || ft == FieldType::OLAP_FIELD_TYPE_VARCHAR;
}

bool is_supported_split_key_type(FieldType ft) {
    if (is_string_split_key_type(ft)) {
        return true;
    }
    switch (ft) {
#define M(FT, PT) case FieldType::FT:
        DORIS_APPLY_FOR_SPLIT_KEY_TYPES(M)
#undef M
        return true;
    default:
        return false;
    }
}

// Common comparable domain for split boundaries: a 256-bit signed integer, wide enough to hold
// DECIMAL256 exactly and to keep unsigned 128-bit IPv6 non-negative (which a signed __int128
// could not). DecimalV2 stores a fixed scale of 9, so fraction is always in (-1e9, 1e9) and
// shares the integer's sign; fold it to a single value below.
constexpr int64_t kDecimalV2FractionScale = 1000000000; // == decimal12_t::FRAC_RATIO

// Widen a decoded storage value to the common comparable domain. Signed integers / decimals keep
// their value (sign-extended); LARGEINT and DECIMAL128 are already 128-bit; DECIMAL256 is already
// 256-bit; date/datetime (uint24/uint32/uint64) and IPv4/IPv6 (uint32/uint128) are unsigned,
// order-preserving integers -> non-negative; DECIMALV2 (decimal12_t) folds to
// integer*FRAC_RATIO + fraction. Within one column type this mapping is monotonic with key order,
// so a plain sort/quantile over the widened samples matches key order.
template <typename S>
wide::Int256 storage_to_key(const S& v) {
    if constexpr (std::is_same_v<S, uint24_t>) {
        return wide::Int256(static_cast<uint32_t>(v));
    } else if constexpr (std::is_same_v<S, decimal12_t>) {
        return wide::Int256(v.integer) * kDecimalV2FractionScale + wide::Int256(v.fraction);
    } else {
        return wide::Int256(v);
    }
}

template <typename S>
S key_to_storage(const wide::Int256& v) {
    if constexpr (std::is_same_v<S, uint24_t>) {
        return uint24_t(static_cast<uint32_t>(v));
    } else if constexpr (std::is_same_v<S, decimal12_t>) {
        decimal12_t d;
        d.integer = static_cast<int64_t>(v / kDecimalV2FractionScale);
        d.fraction = static_cast<int32_t>(v % kDecimalV2FractionScale);
        return d;
    } else {
        return static_cast<S>(v);
    }
}

// Decode the first sort-key column of a memcomparable-encoded key (a segment min/max key or a
// short key index entry, see SegmentWriter::_full_encode_keys / RowCursor::encode_key_with_padding)
// into the common comparable domain (wide::Int256). The key is laid out as a 1-byte per-column
// marker followed by the order-preserving value bytes. Returns false (caller skips the sample)
// for a null/minimal first column, a too-short key, or an unsupported type.
bool decode_first_key(const std::string& encoded, FieldType ft, wide::Int256* out) {
    if (encoded.size() < 2) {
        return false;
    }
    if (static_cast<uint8_t>(encoded[0]) != KeyConsts::KEY_NORMAL_MARKER) {
        return false; // null / minimal first column
    }
    Slice s(encoded.data() + 1, encoded.size() - 1);
    const KeyCoder* coder = get_key_coder(ft);
    switch (ft) {
#define M(FT, PT)                                                                           \
    case FieldType::FT: {                                                                   \
        typename PrimitiveTypeTraits<PrimitiveType::PT>::StorageFieldType v {};             \
        if (!coder->decode_ascending(&s, sizeof(v), reinterpret_cast<uint8_t*>(&v)).ok()) { \
            return false;                                                                   \
        }                                                                                   \
        *out = storage_to_key(v);                                                           \
        return true;                                                                        \
    }
        DORIS_APPLY_FOR_SPLIT_KEY_TYPES(M)
#undef M
    default:
        return false;
    }
}

// Build a one-column OlapTuple holding `value` (in the wide::Int256 domain produced by
// decode_first_key) typed as the first sort-key column, suitable as an OlapScanRange prefix
// bound. The widened value is narrowed back to the storage representation and lifted to the
// compute-layer Field via the same olap-value path the storage engine uses
// (Field::create_field_from_olap_value), so re-encoding the seek key reproduces the exact bytes.
bool make_first_key_tuple(FieldType ft, const wide::Int256& value, OlapTuple* tuple) {
    switch (ft) {
#define M(FT, PT)                                                                    \
    case FieldType::FT: {                                                            \
        using S = typename PrimitiveTypeTraits<PrimitiveType::PT>::StorageFieldType; \
        tuple->add_field(Field::create_field_from_olap_value<PrimitiveType::PT>(     \
                key_to_storage<S>(value)));                                          \
        return true;                                                                 \
    }
        DORIS_APPLY_FOR_SPLIT_KEY_TYPES(M)
#undef M
    default:
        return false;
    }
}

// Extract the first sort-key column value of an OlapScanRange bound (a predicate-derived key
// range, see OlapScanLocalState::_cond_ranges) into the wide::Int256 domain used by the split
// boundaries. The compute-layer Field value is converted to the storage representation
// (PrimitiveTypeConvertor) and widened identically to decode_first_key. Returns false for an
// empty, null, or non-splittable first field -- the caller then refuses to split rather than
// risk mis-intersecting.
bool first_key_field_to_key(const OlapTuple& tuple, wide::Int256* out) {
    if (tuple.size() == 0) {
        return false;
    }
    const Field& f = tuple.get_field(0);
    if (f.is_null()) {
        return false;
    }
    switch (f.get_type()) {
#define M(FT, PT)                                                                    \
    case PrimitiveType::PT: {                                                        \
        *out = storage_to_key(                                                       \
                PrimitiveTypeConvertor<PrimitiveType::PT>::to_storage_field_type(    \
                        f.get<PrimitiveType::PT>()));                               \
        return true;                                                                 \
    }
        DORIS_APPLY_FOR_SPLIT_KEY_TYPES(M)
#undef M
    default:
        return false;
    }
}

// Comparable-domain policy for key-range tablet split. The split machinery is identical for every
// first-key type except (1) the comparable value it folds a memcomparable key into, (2) how it
// decodes that value, (3) how it re-materializes a value into a push-down OlapScanRange bound, and
// (4) the closed upper anchor of the LAST split. Every split is a finite, closed/half-open range
// [lo, hi) ([lo, hi] for the last split) so the reader always gets equal-width start/end keys (it
// rejects an empty/asymmetric bound, see TabletReader::_init_keys_param). The two policies differ
// only in how that last-split upper anchor is chosen: scalars round-trip exactly so the sampled
// global max is the real max; string samples may be truncated prefixes (< real max), so the last
// split instead closes on an all-0xFF column-width sentinel that is >= every value (the LOW side is
// truncation-safe: a truncated min sorts <= the real min and nothing is below it). Captured below;
// _build_scanners_by_key_range_impl is templated on them.

// Fixed-width, order-preserving scalars: fold the first key column into a single 256-bit signed
// integer (see storage_to_key); both outer ends anchor on the real sampled global min/max (exact
// round-trip via key_to_storage / make_first_key_tuple).
struct Int256KeyDomain {
    using Value = wide::Int256;
    static constexpr bool kStringKeys = false;

    static bool decode(const std::string& encoded, FieldType ft, int32_t /*max_len*/, Value* out) {
        return decode_first_key(encoded, ft, out);
    }
    static bool field_to_value(const OlapTuple& tuple, Value* out) {
        return first_key_field_to_key(tuple, out);
    }
    static bool make_tuple(FieldType ft, const Value& v, OlapTuple* tuple) {
        return make_first_key_tuple(ft, v, tuple);
    }
    // Never used (only consulted when kStringKeys); scalars keep the sampled max. Present so the
    // domain-generic last-split branch type-checks.
    static Value max_sentinel(int32_t /*len*/) { return Value {}; }
};

// Variable-length CHAR/VARCHAR: the raw memcomparable bytes are themselves the comparable value
// (lexicographic byte order == key order == std::string::operator<), so there is NO decode and no
// round-trip. The first byte is the per-column null/normal marker (skipped); the remaining
// order-preserving bytes are the value, capped to the column's schema length to bound the size of a
// push-down key (a composite key has no separator after the first column).
struct StringKeyDomain {
    using Value = std::string;
    static constexpr bool kStringKeys = true;

    static bool decode(const std::string& encoded, FieldType /*ft*/, int32_t max_len, Value* out) {
        if (encoded.empty() ||
            static_cast<uint8_t>(encoded[0]) != KeyConsts::KEY_NORMAL_MARKER) {
            return false; // empty / null / minimal first column
        }
        size_t len = encoded.size() - 1;
        if (max_len > 0) {
            len = std::min(len, static_cast<size_t>(max_len));
        }
        out->assign(encoded, 1, len);
        return true;
    }
    static bool field_to_value(const OlapTuple& tuple, Value* out) {
        if (tuple.size() == 0) {
            return false;
        }
        const Field& f = tuple.get_field(0);
        if (f.is_null() || !is_string_type(f.get_type())) {
            return false;
        }
        const auto sv = f.as_string_view();
        out->assign(sv.data(), sv.size());
        return true;
    }
    static bool make_tuple(FieldType ft, const Value& v, OlapTuple* tuple) {
        const StringRef ref(v.data(), v.size());
        switch (ft) {
        case FieldType::OLAP_FIELD_TYPE_CHAR:
            tuple->add_field(Field::create_field_from_olap_value<TYPE_CHAR>(ref));
            return true;
        case FieldType::OLAP_FIELD_TYPE_VARCHAR:
            tuple->add_field(Field::create_field_from_olap_value<TYPE_VARCHAR>(ref));
            return true;
        default:
            return false;
        }
    }
    // An all-0xFF string of the column's max byte width: >= every value the column can hold (a
    // VARCHAR(len)/CHAR(len) value is <= len bytes, and 0xFF is the maximal byte), so closing the
    // last split on it covers the real max even when the sampled max is a truncated prefix.
    static Value max_sentinel(int32_t len) {
        return std::string(static_cast<size_t>(len > 0 ? len : 0), '\xff');
    }
};

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
    // Pick the comparable domain from the first sort-key column type, which is uniform across this
    // scan's tablets (same table / index schema). Fixed-width scalars fold into wide::Int256;
    // CHAR/VARCHAR compare by raw memcomparable bytes (std::string), with the last split's upper
    // anchored on an all-0xFF sentinel to stay correct under key-bound truncation. See
    // Int256KeyDomain / StringKeyDomain. Per-tablet guards inside the impl re-validate the type so
    // a schema/FE drift fails loudly rather than mis-scanning.
    if (_tablets.empty()) {
        return Status::OK();
    }
    const FieldType first_key_type = _tablets.front().tablet->tablet_schema()->column(0).type();
    if (is_string_split_key_type(first_key_type)) {
        return _build_scanners_by_key_range_impl<StringKeyDomain>(scanners);
    }
    return _build_scanners_by_key_range_impl<Int256KeyDomain>(scanners);
}

template <typename Domain>
Status ParallelScannerBuilder::_build_scanners_by_key_range_impl(
        std::list<ScannerSPtr>& scanners) {
    using Value = typename Domain::Value;
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
        if (!is_supported_split_key_type(first_key_type) ||
            is_string_split_key_type(first_key_type) != Domain::kStringKeys) {
            // FE eligibility (isFirstKeyColumnSplittable) should have prevented assigning a
            // split here, and the dispatcher should have selected the matching domain; fail
            // loudly if the two diverged rather than silently mis-scan.
            return Status::NotSupported(
                    "key-range tablet split requires an integer, date/datetime, decimal, ip or "
                    "char/varchar first key column, but tablet {} has first key column type {}",
                    tablet->tablet_id(), static_cast<int>(first_key_type));
        }
        if (tablet->tablet_schema()->column(0).is_nullable()) {
            // NULL keys are encoded with a null marker that the boundary decoder skips, so they
            // would fall outside every split range and be dropped. FE
            // (isFirstKeyColumnSplittable) already excludes nullable first keys; fail
            // loudly if the two diverged rather than silently lose NULL rows.
            return Status::NotSupported(
                    "key-range tablet split requires a NOT NULL first key column, but tablet {} "
                    "has a nullable first key column",
                    tablet->tablet_id());
        }
        // For string keys, cap each sampled / pushed-down value to the column's schema length so a
        // composite key (no separator after the first column) cannot blow up the push-down key
        // size; for scalars this is ignored. <=0 means "no cap".
        const int32_t first_key_len =
                Domain::kStringKeys ? tablet->tablet_schema()->column(0).length() : 0;

        // Sample the key space: decode the first key column from every segment's min/max key
        // across all rowsets. These bounds are real keys carried in the rowset meta (no extra
        // IO) and are identical on every BE at this version -> deterministic boundaries.
        //
        // Coverage correctness:
        //   * scalar domain: the global min/max sample must equal the TRUE global min/max key, so
        //     EVERY non-empty rowset must contribute decodable bounds spanning its own first-key
        //     min/max (otherwise out-of-range keys would be dropped -> fall back).
        //   * string domain: the last split's upper closes on the all-0xFF sentinel (not the
        //     sampled max), so even truncated-prefix bounds are coverage-safe; decode still
        //     succeeds on a prefix, only balance (not coverage) is affected.
        // Two valid bound shapes either way:
        //   * aggregated: non-MoW rowsets (exactly the AGG/MoR tables handled here) collapse the
        //     per-segment bounds into a single rowset-level [min,max] by default (see
        //     BetaRowsetWriter / config segments key-bounds aggregation).
        //   * per-segment: one [min,max] entry per segment.
        // Anything else (e.g. old rowsets carrying no segment key bounds, see RowsetMeta) is
        // untrustworthy and triggers the fallback below. (Scalar truncation: the supported scalar
        // first key columns are <=33 bytes encoded -- DECIMAL256, 32 value bytes + 1 marker -- under
        // the default 36-byte segments_key_bounds_truncation_threshold, so the first column is never
        // cut; if it were, decode fails and we fall back, never mis-scan.)
        std::vector<Value> samples;
        bool all_rowsets_bounded = true;
        for (auto& rs_split : entire_read_source.rs_splits) {
            auto rowset = rs_split.rs_reader->rowset();
            if (rowset->num_rows() == 0) {
                continue; // empty rowset contributes no keys
            }
            std::vector<KeyBoundsPB> seg_bounds;
            RETURN_IF_ERROR(rowset->get_segments_key_bounds(&seg_bounds));
            const bool bounds_ok =
                    rowset->is_segments_key_bounds_aggregated()
                            ? seg_bounds.size() == 1
                            : static_cast<int64_t>(seg_bounds.size()) == rowset->num_segments();
            if (!bounds_ok) {
                all_rowsets_bounded = false; // missing / unexpected segment key bounds
                break;
            }
            for (auto& kb : seg_bounds) {
                Value v {};
                if (!Domain::decode(kb.min_key(), first_key_type, first_key_len, &v)) {
                    all_rowsets_bounded = false;
                    break;
                }
                samples.emplace_back(v);
                if (!Domain::decode(kb.max_key(), first_key_type, first_key_len, &v)) {
                    all_rowsets_bounded = false;
                    break;
                }
                samples.emplace_back(v);
            }
            if (!all_rowsets_bounded) {
                break;
            }
        }

        if (samples.empty() || !all_rowsets_bounded) {
            // Either there are no keys at all (all rowsets empty) or some non-empty rowset lacks
            // decodable bounds, so we cannot derive safe boundaries. Deterministic fallback (the
            // same decision on every BE, since they all see the same rowset metas): split 0 owns
            // the whole tablet -- honoring the original predicate _key_ranges so the SQL filter is
            // preserved -- and every other split owns nothing. Coverage stays complete; we only
            // lose parallelism for this tablet.
            if (split_id == 0) {
                build_full_scanner(_key_ranges);
            }
            ++tablet_idx;
            continue;
        }

        // Enrich interior boundary precision with the short key index. The bounds above give only
        // 2 samples per (aggregated) rowset -- just the extremes -- so quantiles over them are
        // coarse and skewed (e.g. overlapping rowsets cluster the samples). The short key index
        // stores one entry every `num_rows_per_block` (~1024) rows, so each entry is a sample
        // weighted by ~equal rows: a plain quantile over them balances scanned rows for free, with
        // no within-rowset uniformity assumption. Same memcomparable encoding as the min/max bounds
        // (RowCursor::encode_key_with_padding -> 1 marker byte + KeyCoder), so decode is identical.
        //
        // IMPORTANT: the index's last entry is the last BLOCK START, not the segment's true max
        // key, so it must NOT define global coverage. We therefore KEEP the bounds-derived true
        // global min/max (already in `samples`) as the outer anchors and only add interior points.
        // This is best-effort: the index lives in the segment and reading it is IO (remote+cached
        // in cloud), but a key-range scanner reads all these segments anyway and seeks via this
        // same index, so the cost is pulled-forward, not new. Any failure just leaves the coarser
        // bounds-only samples -- still correct, only less balanced.
        for (auto& rs_split : entire_read_source.rs_splits) {
            auto rowset = rs_split.rs_reader->rowset();
            if (rowset->num_rows() == 0) {
                continue;
            }
            auto beta = std::dynamic_pointer_cast<BetaRowset>(rowset);
            if (beta == nullptr) {
                continue;
            }
            std::vector<segment_v2::SegmentSharedPtr> segments;
            if (!beta->load_segments(&segments).ok()) {
                continue;
            }
            for (auto& seg : segments) {
                if (!seg->load_index(&_builder_stats).ok()) {
                    continue;
                }
                const auto* decoder = seg->get_short_key_index();
                if (decoder == nullptr) {
                    continue;
                }
                const uint32_t num_items = decoder->num_items();
                for (uint32_t i = 0; i < num_items; ++i) {
                    Value v {};
                    if (Domain::decode(decoder->key(i).to_string(), first_key_type, first_key_len,
                                       &v)) {
                        samples.emplace_back(std::move(v));
                    }
                }
            }
        }
        std::sort(samples.begin(), samples.end());

        // Boundaries b[0..split_count] at even sample quantiles. b[0]=sampled global min, interior
        // b[j]=samples[size*j/split_count]. This BE owns [b[split_id], b[split_id+1]); the LAST
        // split closes its upper so the max key is included. The upper outer anchor differs by
        // domain: scalars round-trip exactly so b[split_count]=sampled max is the real max; string
        // samples may be truncated prefixes (< real max), so a closed sampled max would drop
        // (sampled_max, real_max] -- the last string split instead closes on an all-0xFF
        // column-width sentinel that is >= every value. (The low side needs no sentinel: a
        // truncated min sorts <= the real min and nothing is below it.) Every split is a finite
        // range so the reader always gets equal-width start/end keys. A key's split membership is a
        // pure function of its first column vs the b[j], so the same key (and all its versions)
        // always lands on one split -> merge stays correct. Boundary precision only affects balance.
        auto boundary_at = [&](int32_t j) -> Value {
            size_t idx =
                    (samples.size() * static_cast<size_t>(j)) / static_cast<size_t>(split_count);
            idx = std::min(idx, samples.size() - 1);
            return samples[idx];
        };
        const bool is_last = (split_id == split_count - 1);
        const Value lo = boundary_at(split_id);
        const Value hi = (Domain::kStringKeys && is_last) ? Domain::max_sentinel(first_key_len)
                                                          : boundary_at(split_id + 1);

        // TabletReader stores key-range inclusivity as scalar fields, not per range. It also
        // requires every key tuple in one scanner to have the same width. Stage ranges into
        // compatible groups and build one scanner per group.
        std::vector<std::vector<OlapScanRange*>> scanner_key_range_groups;
        auto stage_key_range = [&](OlapScanRange* range) {
            for (auto& group : scanner_key_range_groups) {
                auto* first = group.front();
                if (first->begin_include == range->begin_include &&
                    first->end_include == range->end_include &&
                    first->begin_scan_range.size() == range->begin_scan_range.size() &&
                    first->end_scan_range.size() == range->end_scan_range.size()) {
                    group.push_back(range);
                    return;
                }
            }
            scanner_key_range_groups.push_back({range});
        };

        // Helper: own a single-column first-key range [r_lo, r_hi] (with the given inclusivities)
        // in the local state (outlives the scanners; this builder does not) and stage it.
        auto own_first_key_range = [&](const Value& r_lo, bool lo_incl, const Value& r_hi,
                                       bool hi_incl) -> Status {
            auto range = std::make_unique<OlapScanRange>();
            range->begin_include = lo_incl;
            range->end_include = hi_incl;
            range->has_lower_bound = true;
            range->has_upper_bound = true;
            if (!Domain::make_tuple(first_key_type, r_lo, &range->begin_scan_range) ||
                !Domain::make_tuple(first_key_type, r_hi, &range->end_scan_range)) {
                return Status::NotSupported(
                        "key-range tablet split: unsupported first key column type");
            }
            stage_key_range(_parent->own_split_key_range(std::move(range)));
            return Status::OK();
        };

        // Compose the split's first-key-column interval [lo, hi) ([lo, hi] for the last split)
        // with any predicate-derived key ranges (_key_ranges). The SQL key predicate may live
        // ONLY in these short-key ranges -- once a scan key exactly covers a predicate, the
        // matching storage predicate is erased (see OlapScanLocalState::_init_scan_keys). So we
        // must INTERSECT, not replace; replacing would scan the whole split range and e.g.
        // over-count `WHERE k = 5`. A non-empty key range always constrains the first key column
        // (scan keys are built in key order), and only carries further columns when the first
        // column is a fixed point -- so each range is either a first-column point or interval.
        bool has_predicate_range = false;
        for (auto* r : _key_ranges) {
            if (r->has_lower_bound || r->has_upper_bound) {
                has_predicate_range = true;
                break;
            }
        }
        if (!has_predicate_range) {
            // No predicate key range (full-scan, possibly with a has_lower_bound==false
            // placeholder): the split interval is the only constraint.
            RETURN_IF_ERROR(own_first_key_range(lo, true, hi, is_last));
        } else {
            for (auto* r : _key_ranges) {
                if (!r->has_lower_bound && !r->has_upper_bound) {
                    continue; // full-scan placeholder mixed in; the others carry the constraint
                }
                Value r_begin {};
                Value r_end {};
                const bool has_begin =
                        r->has_lower_bound && Domain::field_to_value(r->begin_scan_range, &r_begin);
                const bool has_end =
                        r->has_upper_bound && Domain::field_to_value(r->end_scan_range, &r_end);
                // A first-column bound we cannot decode is unsafe to intersect. Fail loudly and
                // identically on every BE (FE gating should make this unreachable) rather than
                // drop or duplicate rows across splits.
                if ((r->has_lower_bound && !has_begin) || (r->has_upper_bound && !has_end)) {
                    return Status::NotSupported(
                            "key-range tablet split: cannot intersect predicate key range with "
                            "split boundary (undecodable or null first key bound)");
                }

                // Point on the first column (begin==end, possibly with secondary columns): the
                // point is wholly inside or outside the split -> keep R verbatim or drop it. Reuse
                // the original range pointer (owned by the local state) to preserve any secondary
                // column bounds.
                const bool is_point = r->begin_scan_range.size() > 1 ||
                                      r->end_scan_range.size() > 1 ||
                                      (has_begin && has_end && r_begin == r_end);
                if (is_point) {
                    const Value& v = has_begin ? r_begin : r_end;
                    const bool in_split = v >= lo && (is_last ? v <= hi : v < hi);
                    if (in_split) {
                        stage_key_range(r);
                    }
                    continue;
                }

                // First-column interval: intersect [r_begin, r_end] with [lo, hi)/[lo, hi].
                Value new_lo;
                bool new_lo_incl;
                if (!has_begin || lo > r_begin) {
                    new_lo = lo;
                    new_lo_incl = true;
                } else if (lo < r_begin) {
                    new_lo = r_begin;
                    new_lo_incl = r->begin_include;
                } else {
                    new_lo = lo;
                    new_lo_incl = r->begin_include; // both lower bounds coincide -> AND (split incl)
                }
                Value new_hi;
                bool new_hi_incl;
                if (!has_end || hi < r_end) {
                    new_hi = hi;
                    new_hi_incl = is_last;
                } else if (hi > r_end) {
                    new_hi = r_end;
                    new_hi_incl = r->end_include;
                } else {
                    new_hi = hi;
                    new_hi_incl = r->end_include && is_last; // upper bounds coincide -> AND
                }
                if (new_lo > new_hi || (new_lo == new_hi && !(new_lo_incl && new_hi_incl))) {
                    continue; // empty intersection: this split owns none of R
                }
                RETURN_IF_ERROR(own_first_key_range(new_lo, new_lo_incl, new_hi, new_hi_incl));
            }
        }

        // Empty intersection: every predicate range fell outside this split's interval, so this
        // split owns none of the tablet. Build no scanner (an empty key-range list would mean a
        // FULL scan, not an empty one). The other splits cover the data; correctness holds.
        for (const auto& key_range_group : scanner_key_range_groups) {
            build_full_scanner(key_range_group);
        }
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
