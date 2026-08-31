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

#include "cloud/cloud_distributed_compaction.h"
#include "io/io_common.h"

namespace doris::cloud {

enum class KeyRangeSamplingMode {
    TYPED_KEY,
    PRIMARY_KEY,
    SHORT_KEY_LOSSLESS,
    SHORT_KEY_REFINEMENT,
};

struct KeyRangeSamplingPlan {
    bool uses_primary_key_encoding() const {
        return candidate_mode == KeyRangeSamplingMode::PRIMARY_KEY;
    }

    bool uses_short_key_encoding() const {
        return candidate_mode == KeyRangeSamplingMode::SHORT_KEY_LOSSLESS ||
               candidate_mode == KeyRangeSamplingMode::SHORT_KEY_REFINEMENT;
    }

    bool uses_direct_encoded_boundaries() const {
        return candidate_mode == KeyRangeSamplingMode::PRIMARY_KEY ||
               candidate_mode == KeyRangeSamplingMode::SHORT_KEY_LOSSLESS;
    }

    bool selected_short_key_fast_path() const {
        return selected_mode == KeyRangeSamplingMode::SHORT_KEY_LOSSLESS ||
               selected_mode == KeyRangeSamplingMode::SHORT_KEY_REFINEMENT;
    }

    bool selected_primary_key_fast_path() const {
        return selected_mode == KeyRangeSamplingMode::PRIMARY_KEY;
    }

    bool selected_short_key_refinement() const {
        return selected_mode == KeyRangeSamplingMode::SHORT_KEY_REFINEMENT;
    }

    KeyRangeSamplingMode candidate_mode = KeyRangeSamplingMode::TYPED_KEY;
    KeyRangeSamplingMode selected_mode = KeyRangeSamplingMode::TYPED_KEY;
    size_t prefix_length = 0;
    size_t encoded_key_suffix_length = 0;
    bool short_key_encoding_lossless = true;
    std::string_view short_key_fallback_reason;
    std::string_view primary_key_fallback_reason;
};

struct BaseKeyRangePlanningResult {
    CompositeKeyRangePlan key_ranges;
    KeyRangeSamplingPlan sampling;
    uint64_t target_sample_count = 0;
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
    int64_t boundary_choose_time_us = 0;
    size_t typed_sample_count = 0;
    size_t boundary_refinement_group_count = 0;
    size_t boundary_refinement_sample_count = 0;
    size_t encoded_sample_count = 0;
};

bool is_integer_key(FieldType type);
bool is_string_key(FieldType type);
bool is_date_key(FieldType type);
bool is_decimal_key(FieldType type);

Status build_base_key_range_plan(const std::vector<RowsetSharedPtr>& input_rowsets,
                                 const TabletSchema& schema, bool is_mow, size_t range_count,
                                 uint64_t total_input_rows, BaseKeyRangePlanningResult* result);

} // namespace doris::cloud
