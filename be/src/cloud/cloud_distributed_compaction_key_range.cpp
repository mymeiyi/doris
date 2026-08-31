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

#include "cloud/cloud_distributed_compaction_key_range.h"

#include <algorithm>
#include <limits>
#include <optional>
#include <set>
#include <unordered_map>
#include <utility>

#include "cloud/config.h"
#include "common/cast_set.h"
#include "common/check.h"
#include "core/data_type/data_type_factory.hpp"
#include "storage/index/primary_key_index.h"
#include "storage/index/short_key_index.h"
#include "storage/iterators.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/segment.h"
#include "storage/tablet/tablet_schema.h"
#include "util/slice.h"
#include "util/stopwatch.hpp"

namespace doris::cloud {

template <typename T>
std::vector<T> choose_key_range_boundaries(std::vector<KeySample<T>> samples, size_t range_count) {
    if (range_count < 2 || samples.size() < 2) {
        return {};
    }
    std::ranges::sort(samples, {}, &KeySample<T>::key);
    std::vector<KeySample<T>> distinct;
    distinct.reserve(samples.size());
    for (const auto& sample : samples) {
        DORIS_CHECK_GT(sample.weight, 0);
        if (!distinct.empty() && distinct.back().key == sample.key) {
            DORIS_CHECK_LE(distinct.back().weight,
                           std::numeric_limits<uint64_t>::max() - sample.weight);
            distinct.back().weight += sample.weight;
        } else {
            distinct.push_back(sample);
        }
    }
    if (distinct.size() < 2) {
        return {};
    }

    unsigned __int128 total_weight = 0;
    for (const auto& sample : distinct) {
        total_weight += sample.weight;
    }
    std::vector<T> boundaries;
    boundaries.reserve(range_count - 1);
    for (size_t split = 1; split < range_count; ++split) {
        const unsigned __int128 target = total_weight * split / range_count;
        unsigned __int128 prefix = 0;
        unsigned __int128 best_distance = total_weight;
        T best_key = distinct[1].key;
        for (size_t i = 1; i < distinct.size(); ++i) {
            prefix += distinct[i - 1].weight;
            const unsigned __int128 distance = prefix > target ? prefix - target : target - prefix;
            if (distance < best_distance) {
                best_distance = distance;
                best_key = distinct[i].key;
            }
        }
        if (boundaries.empty() || boundaries.back() != best_key) {
            boundaries.push_back(best_key);
        }
    }
    return boundaries;
}

std::vector<int128_t> choose_integer_key_range_boundaries(std::vector<IntegerKeySample> samples,
                                                          size_t range_count) {
    return choose_key_range_boundaries(std::move(samples), range_count);
}

std::vector<std::string> choose_string_key_range_boundaries(std::vector<StringKeySample> samples,
                                                            size_t range_count) {
    return choose_key_range_boundaries(std::move(samples), range_count);
}

CompositeKeyRangePlan choose_composite_key_range_boundaries(
        const std::vector<CompositeKeySample>& samples, size_t range_count) {
    CompositeKeyRangePlan plan;
    if (samples.empty()) {
        return plan;
    }
    const size_t key_columns = samples.front().key.size();
    DORIS_CHECK_GT(key_columns, 0);
    for (const auto& sample : samples) {
        DORIS_CHECK_EQ(sample.key.size(), key_columns);
    }

    for (size_t prefix_length = 1; prefix_length <= key_columns; ++prefix_length) {
        std::vector<CompositeKeySample> prefix_samples;
        prefix_samples.reserve(samples.size());
        for (const auto& sample : samples) {
            prefix_samples.push_back(
                    {.key = CompositeKey(sample.key.begin(), sample.key.begin() + prefix_length),
                     .weight = sample.weight});
        }
        auto boundaries = choose_key_range_boundaries(std::move(prefix_samples), range_count);
        if (boundaries.size() > plan.boundaries.size()) {
            plan = {.prefix_length = prefix_length, .boundaries = std::move(boundaries)};
        }
        if (plan.boundaries.size() + 1 >= range_count) {
            break;
        }
    }
    return plan;
}

std::vector<WeightedRowId> build_weighted_key_sample_rowids(uint64_t num_rows,
                                                            uint64_t rows_per_block,
                                                            size_t max_samples) {
    DORIS_CHECK_GT(rows_per_block, 0);
    if (num_rows == 0) {
        return {};
    }
    DORIS_CHECK_GT(max_samples, 0);

    const uint64_t block_count = (num_rows - 1) / rows_per_block + 1;
    const uint64_t sample_count = std::min(block_count, cast_set<uint64_t>(max_samples));
    std::vector<WeightedRowId> samples;
    samples.reserve(sample_count);
    for (uint64_t sample = 0; sample < sample_count; ++sample) {
        const uint64_t block_begin =
                cast_set<uint64_t>(static_cast<uint128_t>(sample) * block_count / sample_count);
        const uint64_t block_end =
                cast_set<uint64_t>(static_cast<uint128_t>(sample + 1) * block_count / sample_count);
        const uint64_t representative_block = block_begin + (block_end - block_begin - 1) / 2;
        const uint64_t covered_begin = block_begin * rows_per_block;
        const uint64_t covered_end =
                block_end == block_count ? num_rows : block_end * rows_per_block;
        samples.push_back({.rowid = cast_set<rowid_t>(representative_block * rows_per_block),
                           .weight = covered_end - covered_begin});
    }
    return samples;
}

bool is_integer_key(FieldType type) {
    return type == FieldType::OLAP_FIELD_TYPE_TINYINT ||
           type == FieldType::OLAP_FIELD_TYPE_SMALLINT || type == FieldType::OLAP_FIELD_TYPE_INT ||
           type == FieldType::OLAP_FIELD_TYPE_BIGINT || type == FieldType::OLAP_FIELD_TYPE_LARGEINT;
}

bool is_string_key(FieldType type) {
    return type == FieldType::OLAP_FIELD_TYPE_CHAR || type == FieldType::OLAP_FIELD_TYPE_VARCHAR;
}

bool is_date_key(FieldType type) {
    return type == FieldType::OLAP_FIELD_TYPE_DATE ||
           type == FieldType::OLAP_FIELD_TYPE_DATETIME ||
           type == FieldType::OLAP_FIELD_TYPE_DATEV2 ||
           type == FieldType::OLAP_FIELD_TYPE_DATETIMEV2;
}

bool is_decimal_key(FieldType type) {
    return field_is_decimal_type(type);
}

bool is_supported_key_range_column_type(FieldType type) {
    return is_integer_key(type) || is_string_key(type) || is_date_key(type) ||
           is_decimal_key(type);
}

namespace {

bool has_lossless_short_key_encoding(const TabletColumn& column) {
    return is_integer_key(column.type()) || is_date_key(column.type()) ||
           is_decimal_key(column.type()) ||
           (is_string_key(column.type()) && column.index_length() >= column.length());
}

KeyRangeSamplingPlan build_key_range_sampling_plan(const TabletSchema& schema, bool is_mow,
                                                   size_t key_column_count) {
    KeyRangeSamplingPlan plan;
    plan.primary_key_fallback_reason = is_mow ? "none" : "not_mow";
    if (is_mow) {
        // Cluster-key MOW is rejected before planning, so a PK-index ordinal is the segment rowid.
        plan.short_key_fallback_reason = "mow";
        if (key_column_count != schema.num_key_columns()) {
            plan.primary_key_fallback_reason = "unsupported_primary_key";
            return plan;
        }
        plan.candidate_mode = KeyRangeSamplingMode::PRIMARY_KEY;
        plan.prefix_length = key_column_count;
        if (schema.has_sequence_col()) {
            plan.encoded_key_suffix_length =
                    schema.column(schema.sequence_col_idx()).length() + 1;
        }
        return plan;
    }

    plan.prefix_length = schema.num_short_key_columns();
    if (plan.prefix_length > key_column_count) {
        plan.short_key_fallback_reason = "unsupported_short_key";
        return plan;
    }

    plan.candidate_mode = KeyRangeSamplingMode::SHORT_KEY_LOSSLESS;
    plan.short_key_fallback_reason = "none";
    for (size_t column_index = 0; column_index < plan.prefix_length; ++column_index) {
        if (has_lossless_short_key_encoding(schema.column(column_index))) {
            continue;
        }
        plan.short_key_encoding_lossless = false;
        if (column_index + 1 < plan.prefix_length) {
            plan.candidate_mode = KeyRangeSamplingMode::TYPED_KEY;
            plan.short_key_fallback_reason = "non_terminal_lossy_short_key";
        } else {
            plan.candidate_mode = KeyRangeSamplingMode::SHORT_KEY_REFINEMENT;
        }
        break;
    }
    return plan;
}
} // namespace

namespace {

struct EncodedKeyLocator {
    uint64_t weight;
    size_t segment_index;
    rowid_t rowid;
};

struct EncodedKeyGroup {
    std::string key;
    std::vector<EncodedKeyLocator> samples;
    uint64_t weight_before = 0;
    uint64_t weight = 0;
};

std::vector<EncodedKeyGroup> group_encoded_key_samples(std::vector<EncodedKeySample> samples) {
    std::ranges::sort(samples, [](const EncodedKeySample& lhs, const EncodedKeySample& rhs) {
        return Slice(lhs.key).compare(Slice(rhs.key)) < 0;
    });
    std::vector<EncodedKeyGroup> groups;
    for (auto& sample : samples) {
        DORIS_CHECK_GT(sample.weight, 0);
        if (groups.empty() || groups.back().key != sample.key) {
            const uint64_t weight_before =
                    groups.empty() ? 0 : groups.back().weight_before + groups.back().weight;
            groups.push_back({.key = std::move(sample.key),
                              .samples = {},
                              .weight_before = weight_before,
                              .weight = 0});
        }
        DORIS_CHECK_LE(groups.back().weight, std::numeric_limits<uint64_t>::max() - sample.weight);
        groups.back().weight += sample.weight;
        groups.back().samples.push_back({.weight = sample.weight,
                                         .segment_index = sample.segment_index,
                                         .rowid = sample.rowid});
    }
    return groups;
}

std::vector<CompositeKeySample> sort_and_merge_composite_key_samples(
        std::vector<CompositeKeySample> samples) {
    std::ranges::sort(samples, {}, &CompositeKeySample::key);
    std::vector<CompositeKeySample> distinct;
    distinct.reserve(samples.size());
    for (auto& sample : samples) {
        if (!distinct.empty() && distinct.back().key == sample.key) {
            DORIS_CHECK_LE(distinct.back().weight,
                           std::numeric_limits<uint64_t>::max() - sample.weight);
            distinct.back().weight += sample.weight;
        } else {
            distinct.push_back(std::move(sample));
        }
    }
    return distinct;
}

} // namespace

std::vector<EncodedKeyBoundary> choose_encoded_key_range_boundaries(
        std::vector<EncodedKeySample> samples, size_t range_count) {
    if (range_count < 2 || samples.size() < 2) {
        return {};
    }
    auto groups = group_encoded_key_samples(std::move(samples));
    if (groups.size() < 2) {
        return {};
    }
    const uint128_t total_weight =
            static_cast<uint128_t>(groups.back().weight_before) + groups.back().weight;
    std::vector<EncodedKeyBoundary> boundaries;
    boundaries.reserve(range_count - 1);
    for (size_t split = 1; split < range_count; ++split) {
        const uint128_t target = total_weight * split / range_count;
        uint128_t best_distance = total_weight;
        size_t best_index = 1;
        for (size_t index = 1; index < groups.size(); ++index) {
            const uint128_t prefix = groups[index].weight_before;
            const uint128_t distance = prefix > target ? prefix - target : target - prefix;
            if (distance < best_distance) {
                best_distance = distance;
                best_index = index;
            }
        }
        const auto& best_group = groups[best_index];
        const auto& best_locator = best_group.samples.front();
        if (boundaries.empty() || boundaries.back().key != best_group.key) {
            boundaries.push_back({.key = best_group.key,
                                  .segment_index = best_locator.segment_index,
                                  .rowid = best_locator.rowid});
        }
    }
    return boundaries;
}

namespace {

Status read_key_samples(const segment_v2::SegmentSharedPtr& segment, const TabletSchema& schema,
                        size_t key_column_count, const std::vector<rowid_t>& rowids,
                        OlapReaderStatistics* reader_stats, std::vector<CompositeKey>* keys) {
    keys->assign(rowids.size(), {});
    if (rowids.empty()) {
        return Status::OK();
    }
    StorageReadOptions read_options;
    read_options.stats = reader_stats;
    read_options.tablet_schema = segment->tablet_schema();
    auto io_ctx = read_options.io_ctx;
    io_ctx.reader_type = ReaderType::READER_BASE_COMPACTION;
    io_ctx.file_cache_stats = &reader_stats->file_cache_stats;
    segment_v2::ColumnIteratorOptions iterator_options {
            .use_page_cache = !config::disable_storage_page_cache,
            .file_reader = segment->file_reader().get(),
            .stats = reader_stats,
            .io_ctx = io_ctx,
    };
    for (size_t column_index = 0; column_index < key_column_count; ++column_index) {
        const auto& column = schema.column(column_index);
        auto values = DataTypeFactory::instance().create_data_type(column)->create_column();
        std::unique_ptr<segment_v2::ColumnIterator> iterator;
        RETURN_IF_ERROR(segment->new_column_iterator(column, &iterator, &read_options));
        RETURN_IF_ERROR(iterator->init(iterator_options));
        RETURN_IF_ERROR(iterator->read_by_rowids(rowids.data(), rowids.size(), values));
        DORIS_CHECK_EQ(values->size(), rowids.size());
        for (size_t sample_index = 0; sample_index < rowids.size(); ++sample_index) {
            (*keys)[sample_index].push_back((*values)[sample_index]);
        }
    }
    return Status::OK();
}

Status read_encoded_primary_key_samples(const segment_v2::SegmentSharedPtr& segment,
                                        const std::vector<WeightedRowId>& samples,
                                        OlapReaderStatistics* reader_stats,
                                        std::vector<std::string>* keys) {
    keys->clear();
    keys->reserve(samples.size());
    if (samples.empty()) {
        return Status::OK();
    }
    const auto* primary_key_index = segment->get_primary_key_index();
    if (primary_key_index == nullptr) {
        return Status::Corruption("primary-key index is missing, rowset_id={}, segment_id={}",
                                  segment->rowset_id().to_string(), segment->id());
    }
    DORIS_CHECK_EQ(primary_key_index->num_rows(), segment->num_rows());
    std::unique_ptr<segment_v2::IndexedColumnIterator> iterator;
    StorageReadOptions read_options;
    read_options.stats = reader_stats;
    auto io_ctx = read_options.io_ctx;
    io_ctx.reader_type = ReaderType::READER_BASE_COMPACTION;
    io_ctx.file_cache_stats = &reader_stats->file_cache_stats;
    RETURN_IF_ERROR(primary_key_index->new_iterator(&iterator, reader_stats, &io_ctx));
    auto index_type = DataTypeFactory::instance().create_data_type(primary_key_index->type(), 1, 0);
    auto index_column = index_type->create_column();
    rowid_t previous_rowid = 0;
    for (size_t sample_index = 0; sample_index < samples.size(); ++sample_index) {
        const rowid_t rowid = samples[sample_index].rowid;
        DORIS_CHECK_LT(rowid, segment->num_rows());
        DORIS_CHECK(sample_index == 0 || previous_rowid < rowid);
        previous_rowid = rowid;
        RETURN_IF_ERROR(iterator->seek_to_ordinal(rowid));
        index_column->clear();
        size_t num_read = 1;
        RETURN_IF_ERROR(iterator->next_batch(&num_read, index_column));
        DORIS_CHECK_EQ(num_read, 1);
        keys->push_back(index_column->get_data_at(0).to_string());
    }
    return Status::OK();
}
} // namespace

Status build_base_key_range_plan(const std::vector<RowsetSharedPtr>& input_rowsets,
                                 const TabletSchema& schema, bool is_mow, size_t range_count,
                                 uint64_t total_input_rows, BaseKeyRangePlanningResult* result) {
    DORIS_CHECK(result != nullptr);
    DORIS_CHECK_GT(range_count, 1);
    DORIS_CHECK_GT(total_input_rows, 0);
    DORIS_CHECK(!is_mow || schema.cluster_key_uids().empty());
    DORIS_CHECK(is_mow || schema.num_short_key_columns() > 0);
    size_t key_column_count = 0;
    while (key_column_count < schema.num_key_columns() &&
           is_supported_key_range_column_type(schema.column(key_column_count).type())) {
        ++key_column_count;
    }
    DORIS_CHECK_GT(key_column_count, 0);
    const uint64_t target_sample_count =
            cast_set<uint64_t>(range_count) *
            cast_set<uint64_t>(config::cloud_distributed_base_compaction_samples_per_range);
    auto key_sampling_plan = build_key_range_sampling_plan(schema, is_mow, key_column_count);

    struct SegmentKeySamples {
        segment_v2::SegmentSharedPtr segment;
        std::vector<WeightedRowId> weighted_rowids;
    };
    std::vector<SegmentKeySamples> segment_samples;
    std::vector<EncodedKeySample> encoded_samples;
    size_t sampled_row_count = 0;
    int64_t segment_load_time_us = 0;
    int64_t short_key_index_load_time_us = 0;
    int64_t primary_key_index_load_time_us = 0;
    int64_t primary_key_sample_read_time_us = 0;
    int64_t key_sample_read_time_us = 0;
    int64_t boundary_key_read_time_us = 0;
    io::FileCacheStatistics planning_io_stats;
    io::FileCacheStatistics primary_key_sample_io_stats;
    io::FileCacheStatistics key_sample_io_stats;
    size_t segment_count = 0;
    for (const auto& rowset : input_rowsets) {
        auto beta_rowset = std::dynamic_pointer_cast<BetaRowset>(rowset);
        if (beta_rowset == nullptr) {
            return Status::InvalidArgument("distributed base compaction requires beta rowsets");
        }
        std::vector<segment_v2::SegmentSharedPtr> segments;
        const int64_t segment_load_start_us = MonotonicMicros();
        const Status segment_load_status = beta_rowset->load_segments(&segments);
        segment_load_time_us += MonotonicMicros() - segment_load_start_us;
        RETURN_IF_ERROR(segment_load_status);
        segment_count += segments.size();
        for (const auto& segment : segments) {
            OlapReaderStatistics index_reader_stats;
            if (!is_mow || key_sampling_plan.uses_primary_key_encoding()) {
                const int64_t index_load_start_us = MonotonicMicros();
                const Status index_load_status = segment->load_index(&index_reader_stats);
                const int64_t index_load_time_us = MonotonicMicros() - index_load_start_us;
                if (is_mow) {
                    primary_key_index_load_time_us += index_load_time_us;
                } else {
                    short_key_index_load_time_us += index_load_time_us;
                }
                RETURN_IF_ERROR(index_load_status);
                planning_io_stats.merge_from(index_reader_stats.file_cache_stats);
            }
            const auto* short_key_index = is_mow ? nullptr : segment->get_short_key_index();
            if (!is_mow && short_key_index == nullptr) {
                return Status::Corruption("short-key index is missing, rowset_id={}, segment_id={}",
                                          segment->rowset_id().to_string(), segment->id());
            }
            const uint64_t rows_per_block = key_sampling_plan.uses_primary_key_encoding() ? 1
                                            : is_mow ? schema.num_rows_per_row_block()
                                                     : short_key_index->num_rows_per_block();
            DORIS_CHECK_GT(rows_per_block, 0);
            const size_t segment_sample_count = cast_set<size_t>(
                    (static_cast<uint128_t>(segment->num_rows()) * target_sample_count +
                     total_input_rows - 1) /
                    total_input_rows);
            auto weighted_rowids = build_weighted_key_sample_rowids(
                    segment->num_rows(), rows_per_block, segment_sample_count);
            sampled_row_count += weighted_rowids.size();
            const size_t segment_index = segment_samples.size();
            if (key_sampling_plan.uses_primary_key_encoding()) {
                std::vector<std::string> primary_keys;
                OlapReaderStatistics primary_key_reader_stats;
                const int64_t primary_key_read_start_us = MonotonicMicros();
                const Status primary_key_read_status = read_encoded_primary_key_samples(
                        segment, weighted_rowids, &primary_key_reader_stats, &primary_keys);
                primary_key_sample_read_time_us += MonotonicMicros() - primary_key_read_start_us;
                RETURN_IF_ERROR(primary_key_read_status);
                planning_io_stats.merge_from(primary_key_reader_stats.file_cache_stats);
                primary_key_sample_io_stats.merge_from(primary_key_reader_stats.file_cache_stats);
                DORIS_CHECK_EQ(primary_keys.size(), weighted_rowids.size());
                for (size_t sample_index = 0; sample_index < weighted_rowids.size();
                     ++sample_index) {
                    auto& primary_key = primary_keys[sample_index];
                    DORIS_CHECK_GT(primary_key.size(), key_sampling_plan.encoded_key_suffix_length);
                    primary_key.resize(primary_key.size() -
                                       key_sampling_plan.encoded_key_suffix_length);
                    encoded_samples.push_back({.key = std::move(primary_key),
                                               .weight = weighted_rowids[sample_index].weight,
                                               .segment_index = segment_index,
                                               .rowid = weighted_rowids[sample_index].rowid});
                }
            } else if (key_sampling_plan.uses_short_key_encoding()) {
                for (const auto& sample : weighted_rowids) {
                    DORIS_CHECK_EQ(sample.rowid % rows_per_block, 0);
                    const size_t block_ordinal = sample.rowid / rows_per_block;
                    DORIS_CHECK_LT(block_ordinal, short_key_index->num_items());
                    encoded_samples.push_back(
                            {.key = short_key_index->key(block_ordinal).to_string(),
                             .weight = sample.weight,
                             .segment_index = segment_index,
                             .rowid = sample.rowid});
                }
            }
            segment_samples.push_back(
                    {.segment = segment, .weighted_rowids = std::move(weighted_rowids)});
        }
    }

    CompositeKeyRangePlan key_plan;
    int64_t boundary_choose_time_us = 0;
    size_t typed_sample_count = 0;
    size_t boundary_refinement_group_count = 0;
    size_t boundary_refinement_sample_count = 0;
    const size_t encoded_sample_count = encoded_samples.size();
    if (key_sampling_plan.uses_direct_encoded_boundaries()) {
        const int64_t boundary_choose_start_us = MonotonicMicros();
        const auto encoded_boundaries =
                choose_encoded_key_range_boundaries(std::move(encoded_samples), range_count);
        boundary_choose_time_us += MonotonicMicros() - boundary_choose_start_us;
        if (key_sampling_plan.uses_primary_key_encoding() ||
            encoded_boundaries.size() + 1 >= range_count) {
            key_sampling_plan.selected_mode = key_sampling_plan.candidate_mode;
            key_plan.prefix_length = key_sampling_plan.prefix_length;
            key_plan.boundaries.reserve(encoded_boundaries.size());
            for (const auto& boundary : encoded_boundaries) {
                DORIS_CHECK_LT(boundary.segment_index, segment_samples.size());
                std::vector<rowid_t> rowids {boundary.rowid};
                std::vector<CompositeKey> keys;
                OlapReaderStatistics reader_stats;
                const int64_t sample_read_start_us = MonotonicMicros();
                const Status sample_read_status =
                        read_key_samples(segment_samples[boundary.segment_index].segment, schema,
                                         key_plan.prefix_length, rowids, &reader_stats, &keys);
                const int64_t sample_read_time_us = MonotonicMicros() - sample_read_start_us;
                key_sample_read_time_us += sample_read_time_us;
                boundary_key_read_time_us += sample_read_time_us;
                RETURN_IF_ERROR(sample_read_status);
                planning_io_stats.merge_from(reader_stats.file_cache_stats);
                key_sample_io_stats.merge_from(reader_stats.file_cache_stats);
                DORIS_CHECK_EQ(keys.size(), 1);
                key_plan.boundaries.push_back(std::move(keys.front()));
                ++typed_sample_count;
            }
            for (size_t boundary_index = 1; boundary_index < key_plan.boundaries.size();
                 ++boundary_index) {
                DORIS_CHECK(key_plan.boundaries[boundary_index - 1] <
                            key_plan.boundaries[boundary_index]);
            }
        } else {
            key_sampling_plan.short_key_fallback_reason = "insufficient_encoded_boundaries";
        }
    } else if (key_sampling_plan.candidate_mode == KeyRangeSamplingMode::SHORT_KEY_REFINEMENT) {
        key_sampling_plan.selected_mode = key_sampling_plan.candidate_mode;
        const int64_t encoded_group_start_us = MonotonicMicros();
        auto encoded_groups = group_encoded_key_samples(std::move(encoded_samples));
        std::vector<size_t> target_group_indices;
        std::set<size_t> refinement_group_indices;
        uint128_t total_weight = 0;
        if (!encoded_groups.empty()) {
            total_weight = static_cast<uint128_t>(encoded_groups.back().weight_before) +
                           encoded_groups.back().weight;
            target_group_indices.reserve(range_count - 1);
            for (size_t split = 1; split < range_count; ++split) {
                const uint128_t target = total_weight * split / range_count;
                size_t group_index = 0;
                while (group_index + 1 < encoded_groups.size() &&
                       static_cast<uint128_t>(encoded_groups[group_index].weight_before) +
                                       encoded_groups[group_index].weight <=
                               target) {
                    ++group_index;
                }
                target_group_indices.push_back(group_index);
                refinement_group_indices.insert(group_index);
                if (group_index + 1 < encoded_groups.size()) {
                    refinement_group_indices.insert(group_index + 1);
                }
            }
        }
        boundary_choose_time_us += MonotonicMicros() - encoded_group_start_us;

        std::unordered_map<size_t, std::vector<CompositeKeySample>> refined_groups;
        for (const size_t group_index : refinement_group_indices) {
            const auto& encoded_group = encoded_groups[group_index];
            std::vector<std::vector<const EncodedKeyLocator*>> samples_by_segment(
                    segment_samples.size());
            for (const auto& sample : encoded_group.samples) {
                DORIS_CHECK_LT(sample.segment_index, samples_by_segment.size());
                samples_by_segment[sample.segment_index].push_back(&sample);
            }
            std::vector<CompositeKeySample> typed_samples;
            typed_samples.reserve(encoded_group.samples.size());
            for (size_t segment_index = 0; segment_index < samples_by_segment.size();
                 ++segment_index) {
                auto& segment_group_samples = samples_by_segment[segment_index];
                if (segment_group_samples.empty()) {
                    continue;
                }
                std::ranges::sort(segment_group_samples, {},
                                  [](const EncodedKeyLocator* sample) { return sample->rowid; });
                std::vector<rowid_t> rowids;
                rowids.reserve(segment_group_samples.size());
                for (const auto* sample : segment_group_samples) {
                    rowids.push_back(sample->rowid);
                }
                std::vector<CompositeKey> keys;
                OlapReaderStatistics reader_stats;
                const int64_t sample_read_start_us = MonotonicMicros();
                const Status sample_read_status =
                        read_key_samples(segment_samples[segment_index].segment, schema,
                                         key_column_count, rowids, &reader_stats, &keys);
                const int64_t sample_read_time_us = MonotonicMicros() - sample_read_start_us;
                key_sample_read_time_us += sample_read_time_us;
                boundary_key_read_time_us += sample_read_time_us;
                RETURN_IF_ERROR(sample_read_status);
                planning_io_stats.merge_from(reader_stats.file_cache_stats);
                key_sample_io_stats.merge_from(reader_stats.file_cache_stats);
                DORIS_CHECK_EQ(keys.size(), segment_group_samples.size());
                for (size_t sample_index = 0; sample_index < keys.size(); ++sample_index) {
                    typed_samples.push_back(
                            {.key = std::move(keys[sample_index]),
                             .weight = segment_group_samples[sample_index]->weight});
                }
            }
            typed_sample_count += typed_samples.size();
            boundary_refinement_sample_count += typed_samples.size();
            refined_groups.emplace(group_index,
                                   sort_and_merge_composite_key_samples(std::move(typed_samples)));
        }
        boundary_refinement_group_count = refined_groups.size();

        const int64_t refined_boundary_start_us = MonotonicMicros();
        key_plan.prefix_length = key_column_count;
        key_plan.boundaries.reserve(range_count - 1);
        for (size_t split = 1; split < range_count; ++split) {
            if (target_group_indices.empty()) {
                break;
            }
            const uint128_t target = total_weight * split / range_count;
            const size_t group_index = target_group_indices[split - 1];
            const auto& encoded_group = encoded_groups[group_index];
            const auto& current_group = refined_groups.at(group_index);
            uint128_t prefix = encoded_group.weight_before;
            uint128_t best_distance = total_weight;
            std::optional<CompositeKey> best_key;
            for (const auto& sample : current_group) {
                if (prefix > 0) {
                    const uint128_t distance = prefix > target ? prefix - target : target - prefix;
                    if (distance < best_distance) {
                        best_distance = distance;
                        best_key = sample.key;
                    }
                }
                prefix += sample.weight;
            }
            if (group_index + 1 < encoded_groups.size()) {
                const auto& next_group = refined_groups.at(group_index + 1);
                DORIS_CHECK(!next_group.empty());
                const uint128_t next_prefix =
                        static_cast<uint128_t>(encoded_group.weight_before) + encoded_group.weight;
                const uint128_t distance =
                        next_prefix > target ? next_prefix - target : target - next_prefix;
                if (distance < best_distance) {
                    best_key = next_group.front().key;
                }
            }
            if (best_key.has_value() &&
                (key_plan.boundaries.empty() || key_plan.boundaries.back() != *best_key)) {
                DORIS_CHECK(key_plan.boundaries.empty() || key_plan.boundaries.back() < *best_key);
                key_plan.boundaries.push_back(std::move(*best_key));
            }
        }
        boundary_choose_time_us += MonotonicMicros() - refined_boundary_start_us;
    }

    std::vector<CompositeKeySample> samples;
    if (key_sampling_plan.selected_mode == KeyRangeSamplingMode::TYPED_KEY) {
        samples.reserve(sampled_row_count);
        for (const auto& segment_sample : segment_samples) {
            std::vector<rowid_t> rowids;
            rowids.reserve(segment_sample.weighted_rowids.size());
            for (const auto& sample : segment_sample.weighted_rowids) {
                rowids.push_back(sample.rowid);
            }
            std::vector<CompositeKey> keys;
            OlapReaderStatistics reader_stats;
            const int64_t sample_read_start_us = MonotonicMicros();
            const Status sample_read_status = read_key_samples(
                    segment_sample.segment, schema, key_column_count, rowids, &reader_stats, &keys);
            key_sample_read_time_us += MonotonicMicros() - sample_read_start_us;
            RETURN_IF_ERROR(sample_read_status);
            planning_io_stats.merge_from(reader_stats.file_cache_stats);
            key_sample_io_stats.merge_from(reader_stats.file_cache_stats);
            DORIS_CHECK_EQ(keys.size(), segment_sample.weighted_rowids.size());
            for (size_t sample_index = 0; sample_index < keys.size(); ++sample_index) {
                samples.push_back({.key = std::move(keys[sample_index]),
                                   .weight = segment_sample.weighted_rowids[sample_index].weight});
            }
        }
        typed_sample_count = samples.size();
        const int64_t boundary_choose_start_us = MonotonicMicros();
        key_plan = choose_composite_key_range_boundaries(samples, range_count);
        boundary_choose_time_us += MonotonicMicros() - boundary_choose_start_us;
    }
    *result = {.key_ranges = std::move(key_plan),
               .sampling = key_sampling_plan,
               .target_sample_count = target_sample_count,
               .sampled_row_count = sampled_row_count,
               .segment_load_time_us = segment_load_time_us,
               .short_key_index_load_time_us = short_key_index_load_time_us,
               .primary_key_index_load_time_us = primary_key_index_load_time_us,
               .primary_key_sample_read_time_us = primary_key_sample_read_time_us,
               .key_sample_read_time_us = key_sample_read_time_us,
               .boundary_key_read_time_us = boundary_key_read_time_us,
               .planning_io_stats = std::move(planning_io_stats),
               .primary_key_sample_io_stats = std::move(primary_key_sample_io_stats),
               .key_sample_io_stats = std::move(key_sample_io_stats),
               .segment_count = segment_count,
               .boundary_choose_time_us = boundary_choose_time_us,
               .typed_sample_count = typed_sample_count,
               .boundary_refinement_group_count = boundary_refinement_group_count,
               .boundary_refinement_sample_count = boundary_refinement_sample_count,
               .encoded_sample_count = encoded_sample_count};
    return Status::OK();
}

} // namespace doris::cloud
