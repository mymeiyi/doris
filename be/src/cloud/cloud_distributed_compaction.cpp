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

#include "cloud/cloud_distributed_compaction.h"

#include <brpc/controller.h>
#include <fmt/format.h>
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/internal_service.pb.h>

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstring>
#include <ctime>
#include <limits>
#include <optional>
#include <set>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "cloud/cloud_cluster_info.h"
#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "cloud/config.h"
#include "common/cast_set.h"
#include "common/check.h"
#include "common/logging.h"
#include "core/data_type/data_type_factory.hpp"
#include "cpp/sync_point.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "service/backend_options.h"
#include "storage/compaction/compaction.h"
#include "storage/index/short_key_index.h"
#include "storage/merger.h"
#include "storage/rowid_conversion.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/segment.h"
#include "storage/storage_policy.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"
#include "util/brpc_client_cache.h"
#include "util/client_cache.h"
#include "util/hash_util.hpp"
#include "util/network_util.h"
#include "util/threadpool.h"
#include "util/thrift_rpc_helper.h"
#include "util/time.h"

namespace doris::cloud {

namespace {

constexpr uint64_t WORKER_SELECTION_HASH_SEED = 0x6a09e667f3bcc909ULL;

bool is_integer_key(FieldType type) {
    return type == FieldType::OLAP_FIELD_TYPE_TINYINT ||
           type == FieldType::OLAP_FIELD_TYPE_SMALLINT || type == FieldType::OLAP_FIELD_TYPE_INT ||
           type == FieldType::OLAP_FIELD_TYPE_BIGINT || type == FieldType::OLAP_FIELD_TYPE_LARGEINT;
}

bool is_string_key(FieldType type) {
    return type == FieldType::OLAP_FIELD_TYPE_CHAR || type == FieldType::OLAP_FIELD_TYPE_VARCHAR;
}

uint64_t compaction_worker_score(uint64_t execution_seed, int64_t backend_id) {
    const std::string backend_id_string = std::to_string(backend_id);
    return HashUtil::xxHash64WithSeed(backend_id_string.data(), backend_id_string.size(),
                                      execution_seed);
}

} // namespace

bool is_supported_distributed_base_key(FieldType type) {
    return is_integer_key(type) || is_string_key(type);
}

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

std::vector<CompactionWorkerInfo> select_compaction_workers_for_groups(
        const std::vector<CompactionWorkerInfo>& candidates, int64_t coordinator_backend_id,
        size_t group_count, std::string_view execution_id) {
    DORIS_CHECK_GT(coordinator_backend_id, 0);
    DORIS_CHECK_GT(group_count, 0);
    DORIS_CHECK(!execution_id.empty());

    auto workers = candidates;
    const uint64_t execution_seed = HashUtil::xxHash64WithSeed(
            execution_id.data(), execution_id.size(), WORKER_SELECTION_HASH_SEED);
    // Keep an eligible coordinator ahead of the truncation boundary. Rank all other workers by a
    // deterministic job-specific score so repeated execution planning is stable without favoring
    // low backend IDs across compaction jobs.
    std::ranges::sort(
            workers, [coordinator_backend_id, execution_seed](const CompactionWorkerInfo& lhs,
                                                              const CompactionWorkerInfo& rhs) {
                const bool lhs_is_coordinator = lhs.backend_id == coordinator_backend_id;
                const bool rhs_is_coordinator = rhs.backend_id == coordinator_backend_id;
                if (lhs_is_coordinator != rhs_is_coordinator) {
                    return lhs_is_coordinator;
                }
                const uint64_t lhs_score = compaction_worker_score(execution_seed, lhs.backend_id);
                const uint64_t rhs_score = compaction_worker_score(execution_seed, rhs.backend_id);
                if (lhs_score != rhs_score) {
                    return lhs_score > rhs_score;
                }
                return lhs.backend_id < rhs.backend_id;
            });
    workers.resize(std::min(workers.size(), group_count));
    return workers;
}

std::vector<SegmentGroupMergeRange> build_segment_group_merge_ranges(const RowsetMeta& rowset_meta,
                                                                     int64_t segment_group_size) {
    DORIS_CHECK_GT(segment_group_size, 1);
    DORIS_CHECK_GT(rowset_meta.num_segments(), 0);

    std::vector<SegmentGroupMergeRange> ranges;
    if (rowset_meta.segments_overlap() == NONOVERLAPPING_WITHIN_GROUP) {
        const auto& input_segment_group_sizes = rowset_meta.segment_group_sizes();
        const int64_t input_group_count = cast_set<int64_t>(input_segment_group_sizes.size());
        DORIS_CHECK_GT(input_group_count, 0);
        ranges.reserve(cast_set<size_t>((input_group_count + segment_group_size - 1) /
                                        segment_group_size));

        int64_t segment_pos_end = 0;
        for (int64_t group_start = 0; group_start < input_group_count;
             group_start += segment_group_size) {
            const int64_t group_end = std::min(group_start + segment_group_size, input_group_count);
            const int64_t segment_pos_start = segment_pos_end;
            for (int64_t group_index = group_start; group_index < group_end; ++group_index) {
                const int32_t input_group_size =
                        input_segment_group_sizes.Get(cast_set<int>(group_index));
                DORIS_CHECK_GT(input_group_size, 0);
                segment_pos_end += input_group_size;
            }

            ranges.push_back({.segment_pos_start = segment_pos_start,
                              .segment_pos_end = segment_pos_end,
                              .merge_way_num = group_end - group_start});
        }
        DORIS_CHECK_EQ(segment_pos_end, rowset_meta.num_segments());
    } else {
        ranges.reserve(cast_set<size_t>((rowset_meta.num_segments() + segment_group_size - 1) /
                                        segment_group_size));
        for (int64_t segment_pos_start = 0; segment_pos_start < rowset_meta.num_segments();
             segment_pos_start += segment_group_size) {
            const int64_t segment_pos_end =
                    std::min(segment_pos_start + segment_group_size, rowset_meta.num_segments());
            ranges.push_back({.segment_pos_start = segment_pos_start,
                              .segment_pos_end = segment_pos_end,
                              .merge_way_num = segment_pos_end - segment_pos_start});
        }
    }
    return ranges;
}

Status build_output_rowset_segment_id_slots(int32_t base_segment_id, int32_t slot_capacity,
                                            size_t group_count,
                                            std::vector<OutputRowsetSegmentIdSlot>* slots) {
    if (base_segment_id < 0 || slot_capacity <= 0) {
        return Status::InvalidArgument(
                "invalid distributed compaction segment slot: base={}, capacity={}",
                base_segment_id, slot_capacity);
    }
    const int64_t end = cast_set<int64_t>(base_segment_id) +
                        cast_set<int64_t>(slot_capacity) * cast_set<int64_t>(group_count);
    if (end > std::numeric_limits<int32_t>::max()) {
        return Status::InvalidArgument(
                "distributed compaction segment slots overflow: base={}, capacity={}, groups={}",
                base_segment_id, slot_capacity, group_count);
    }
    slots->clear();
    slots->reserve(group_count);
    for (size_t group_index = 0; group_index < group_count; ++group_index) {
        const int64_t start = cast_set<int64_t>(base_segment_id) +
                              cast_set<int64_t>(slot_capacity) * cast_set<int64_t>(group_index);
        slots->push_back({.start_id = cast_set<int32_t>(start), .capacity = slot_capacity});
    }
    return Status::OK();
}

namespace {

Status fetch_compaction_workers_from_fe(std::vector<CompactionWorkerInfo>* workers) {
    const auto* cluster_info =
            static_cast<const CloudClusterInfo*>(ExecEnv::GetInstance()->cluster_info());
    if (cluster_info == nullptr || cluster_info->backend_id <= 0 ||
        cluster_info->cloud_compute_group_id().empty() ||
        cluster_info->master_fe_addr.hostname.empty() || cluster_info->master_fe_addr.port <= 0) {
        return Status::InternalError("FE master or local backend identity is not initialized");
    }

    TGetCloudCompactionBackendsRequest request;
    request.__set_backend_id(cluster_info->backend_id);
    TGetCloudCompactionBackendsResult result;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            cluster_info->master_fe_addr.hostname, cluster_info->master_fe_addr.port,
            [&request, &result](FrontendServiceConnection& client) {
                client->getCloudCompactionBackends(result, request);
            }));
    RETURN_IF_ERROR(Status::create<false>(result.status));

    workers->clear();
    workers->reserve(result.backends.size());
    for (const auto& backend : result.backends) {
        workers->push_back({.backend_id = backend.backend_id,
                            .endpoint = get_host_port(backend.host, backend.brpc_port),
                            .cloud_unique_id = backend.cloud_unique_id,
                            .compute_group_id = backend.cloud_compute_group_id});
    }
    std::ranges::sort(*workers, {}, &CompactionWorkerInfo::backend_id);
    std::unordered_set<std::string> unique_endpoints;
    for (const auto& worker : *workers) {
        if (worker.backend_id <= 0 || worker.endpoint.empty() || worker.cloud_unique_id.empty() ||
            worker.compute_group_id.empty() ||
            worker.compute_group_id != cluster_info->cloud_compute_group_id() ||
            !unique_endpoints.emplace(worker.endpoint).second) {
            return Status::InvalidArgument(
                    "FE returned invalid cloud compaction backend: id={}, "
                    "endpoint={}, cloud_unique_id={}, compute_group_id={}",
                    worker.backend_id, worker.endpoint, worker.cloud_unique_id,
                    worker.compute_group_id);
        }
    }
    return Status::OK();
}

Status parse_endpoint(std::string_view endpoint, std::string* host, int* port) {
    const size_t colon = endpoint.rfind(':');
    if (colon == std::string_view::npos || colon == 0 || colon + 1 == endpoint.size()) {
        return Status::InvalidArgument("invalid distributed compaction worker endpoint: {}",
                                       endpoint);
    }
    std::string_view host_view = endpoint.substr(0, colon);
    if (host_view.front() == '[' && host_view.back() == ']') {
        host_view.remove_prefix(1);
        host_view.remove_suffix(1);
    }
    int parsed_port = 0;
    const std::string_view port_view = endpoint.substr(colon + 1);
    const auto [ptr, error] =
            std::from_chars(port_view.data(), port_view.data() + port_view.size(), parsed_port);
    if (error != std::errc() || ptr != port_view.data() + port_view.size() || parsed_port <= 0 ||
        parsed_port > 65535) {
        return Status::InvalidArgument("invalid distributed compaction worker endpoint: {}",
                                       endpoint);
    }
    host->assign(host_view);
    *port = parsed_port;
    return Status::OK();
}

template <typename Request, typename Response, typename Method>
Status call_worker(const std::string& endpoint, const Request& request, Response* response,
                   Method method, std::string_view rpc_name, int64_t timeout_ms) {
    std::string host;
    int port = 0;
    RETURN_IF_ERROR(parse_endpoint(endpoint, &host, &port));
    auto stub = ExecEnv::GetInstance()->brpc_internal_client_cache()->get_client(host, port);
    if (stub == nullptr) {
        return Status::RpcError("failed to get brpc stub for {} ({})", endpoint, rpc_name);
    }
    brpc::Controller controller;
    controller.set_timeout_ms(timeout_ms);
    (stub.get()->*method)(&controller, &request, response, nullptr);
    if (controller.Failed()) {
        return Status::RpcError("{} rpc to {} failed: {}", rpc_name, endpoint,
                                controller.ErrorText());
    }
    return Status::create(response->status());
}

bool is_retryable_control_rpc_error(const Status& status) {
    return status.is<ErrorCode::THRIFT_RPC_ERROR>() || status.is<ErrorCode::TOO_MANY_TASKS>();
}

bool has_only_integer_value(const PValues& column) {
    return column.double_value().empty() && column.float_value().empty() &&
           column.uint32_value().empty() && column.uint64_value().empty() &&
           column.bool_value().empty() && column.string_value().empty() &&
           column.datetime_value().empty() && column.child_element().empty() &&
           column.child_offset().empty();
}

bool extract_integer_value(const PValues& column, PGenericType_TypeId* type, int128_t* value) {
    if (!column.has_type() || column.has_null() || !column.null_map().empty() ||
        !has_only_integer_value(column)) {
        return false;
    }
    *type = column.type().id();
    switch (*type) {
    case PGenericType::INT8:
        if (column.int32_value_size() != 1 || !column.int64_value().empty() ||
            !column.bytes_value().empty() ||
            column.int32_value(0) < std::numeric_limits<int8_t>::min() ||
            column.int32_value(0) > std::numeric_limits<int8_t>::max()) {
            return false;
        }
        *value = column.int32_value(0);
        return true;
    case PGenericType::INT16:
        if (column.int32_value_size() != 1 || !column.int64_value().empty() ||
            !column.bytes_value().empty() ||
            column.int32_value(0) < std::numeric_limits<int16_t>::min() ||
            column.int32_value(0) > std::numeric_limits<int16_t>::max()) {
            return false;
        }
        *value = column.int32_value(0);
        return true;
    case PGenericType::INT32:
        if (column.int32_value_size() != 1 || !column.int64_value().empty() ||
            !column.bytes_value().empty()) {
            return false;
        }
        *value = column.int32_value(0);
        return true;
    case PGenericType::INT64:
        if (!column.int32_value().empty() || column.int64_value_size() != 1 ||
            !column.bytes_value().empty()) {
            return false;
        }
        *value = column.int64_value(0);
        return true;
    case PGenericType::INT128:
        if (!column.int32_value().empty() || !column.int64_value().empty() ||
            column.bytes_value_size() != 1 || column.bytes_value(0).size() != sizeof(int128_t)) {
            return false;
        }
        memcpy(value, column.bytes_value(0).data(), sizeof(*value));
        return true;
    default:
        return false;
    }
}

bool extract_string_value(const PValues& column, std::string* value) {
    if (!column.has_type() || column.type().id() != PGenericType::STRING || column.has_null() ||
        !column.null_map().empty() || !column.double_value().empty() ||
        !column.float_value().empty() || !column.int32_value().empty() ||
        !column.int64_value().empty() || !column.uint32_value().empty() ||
        !column.uint64_value().empty() || !column.bool_value().empty() ||
        column.string_value_size() != 1 || !column.bytes_value().empty() ||
        !column.datetime_value().empty() || !column.child_element().empty() ||
        !column.child_offset().empty()) {
        return false;
    }
    *value = column.string_value(0);
    return true;
}

PGenericType_TypeId integer_key_pb_type(FieldType type) {
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        return PGenericType::INT8;
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        return PGenericType::INT16;
    case FieldType::OLAP_FIELD_TYPE_INT:
        return PGenericType::INT32;
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        return PGenericType::INT64;
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
        return PGenericType::INT128;
    default:
        DORIS_CHECK(false) << "unsupported distributed base compaction key type: " << int(type);
        __builtin_unreachable();
    }
}

FieldType integer_key_field_type(PGenericType_TypeId type) {
    switch (type) {
    case PGenericType::INT8:
        return FieldType::OLAP_FIELD_TYPE_TINYINT;
    case PGenericType::INT16:
        return FieldType::OLAP_FIELD_TYPE_SMALLINT;
    case PGenericType::INT32:
        return FieldType::OLAP_FIELD_TYPE_INT;
    case PGenericType::INT64:
        return FieldType::OLAP_FIELD_TYPE_BIGINT;
    case PGenericType::INT128:
        return FieldType::OLAP_FIELD_TYPE_LARGEINT;
    default:
        return FieldType::OLAP_FIELD_TYPE_UNKNOWN;
    }
}

Field create_integer_key_field(FieldType type, int128_t value) {
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        return Field::create_field<TYPE_TINYINT>(cast_set<int8_t>(value));
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        return Field::create_field<TYPE_SMALLINT>(cast_set<int16_t>(value));
    case FieldType::OLAP_FIELD_TYPE_INT:
        return Field::create_field<TYPE_INT>(cast_set<int32_t>(value));
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        return Field::create_field<TYPE_BIGINT>(cast_set<int64_t>(value));
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
        return Field::create_field<TYPE_LARGEINT>(value);
    default:
        DORIS_CHECK(false) << "unsupported distributed base compaction key type: " << int(type);
        __builtin_unreachable();
    }
}

Field create_string_key_field(std::string value) {
    return Field::create_field<TYPE_STRING>(std::move(value));
}

bool extract_key_fields(const PCloudDistributedCompactionKey& key,
                        std::vector<PGenericType_TypeId>* types, CompositeKey* fields) {
    if (key.columns().empty()) {
        return false;
    }
    types->clear();
    fields->clear();
    types->reserve(key.columns_size());
    fields->reserve(key.columns_size());
    for (const auto& column : key.columns()) {
        if (!column.has_type()) {
            return false;
        }
        const auto type = column.type().id();
        const FieldType field_type = integer_key_field_type(type);
        if (field_type != FieldType::OLAP_FIELD_TYPE_UNKNOWN) {
            PGenericType_TypeId actual_type = PGenericType::UNKNOWN;
            int128_t value = 0;
            if (!extract_integer_value(column, &actual_type, &value) || actual_type != type) {
                return false;
            }
            fields->push_back(create_integer_key_field(field_type, value));
        } else if (type == PGenericType::STRING) {
            std::string value;
            if (!extract_string_value(column, &value)) {
                return false;
            }
            fields->push_back(create_string_key_field(std::move(value)));
        } else {
            return false;
        }
        types->push_back(type);
    }
    return true;
}

int128_t get_integer_key_field(const Field& field, FieldType type) {
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        return field.get<TYPE_TINYINT>();
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        return field.get<TYPE_SMALLINT>();
    case FieldType::OLAP_FIELD_TYPE_INT:
        return field.get<TYPE_INT>();
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        return field.get<TYPE_BIGINT>();
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
        return field.get<TYPE_LARGEINT>();
    default:
        DORIS_CHECK(false) << "unsupported distributed base compaction key type: " << int(type);
        __builtin_unreachable();
    }
}

void set_integer_key_column(PValues* column, FieldType type, int128_t value) {
    column->mutable_type()->set_id(integer_key_pb_type(type));
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        column->add_int32_value(cast_set<int8_t>(value));
        return;
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        column->add_int32_value(cast_set<int16_t>(value));
        return;
    case FieldType::OLAP_FIELD_TYPE_INT:
        column->add_int32_value(cast_set<int32_t>(value));
        return;
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        column->add_int64_value(cast_set<int64_t>(value));
        return;
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
        column->add_bytes_value(&value, sizeof(value));
        return;
    default:
        DORIS_CHECK(false) << "unsupported distributed base compaction key type: " << int(type);
        __builtin_unreachable();
    }
}

void set_string_key_column(PValues* column, std::string_view value) {
    column->mutable_type()->set_id(PGenericType::STRING);
    column->add_string_value(value.data(), value.size());
}

void set_key_fields(PCloudDistributedCompactionKey* key, const TabletSchema& schema,
                    const CompositeKey& fields) {
    DORIS_CHECK_LE(fields.size(), schema.num_key_columns());
    for (size_t column_index = 0; column_index < fields.size(); ++column_index) {
        const auto type = schema.column(column_index).type();
        auto* column = key->add_columns();
        if (is_integer_key(type)) {
            set_integer_key_column(column, type, get_integer_key_field(fields[column_index], type));
        } else {
            DORIS_CHECK(is_string_key(type));
            set_string_key_column(column, fields[column_index].get<TYPE_STRING>());
        }
    }
}

std::pair<int128_t, int128_t> integer_key_limits(FieldType type) {
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        return {std::numeric_limits<int8_t>::min(), std::numeric_limits<int8_t>::max()};
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        return {std::numeric_limits<int16_t>::min(), std::numeric_limits<int16_t>::max()};
    case FieldType::OLAP_FIELD_TYPE_INT:
        return {std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max()};
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        return {std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max()};
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
        return {std::numeric_limits<int128_t>::min(), std::numeric_limits<int128_t>::max()};
    default:
        DORIS_CHECK(false) << "unsupported distributed base compaction key type: " << int(type);
        __builtin_unreachable();
    }
}

CompositeKey key_limit(const TabletSchema& schema, size_t prefix_length, bool upper) {
    CompositeKey key;
    key.reserve(prefix_length);
    for (size_t column_index = 0; column_index < prefix_length; ++column_index) {
        const auto& column = schema.column(column_index);
        if (is_integer_key(column.type())) {
            const auto [min_value, max_value] = integer_key_limits(column.type());
            key.push_back(create_integer_key_field(column.type(), upper ? max_value : min_value));
        } else {
            DORIS_CHECK(is_string_key(column.type()));
            key.push_back(create_string_key_field(
                    upper ? std::string(cast_set<size_t>(column.length()), '\xff')
                          : std::string()));
        }
    }
    return key;
}

bool extract_key_tuple(const PCloudDistributedCompactionKey& key, const TabletSchema& schema,
                       OlapTuple* tuple) {
    std::vector<PGenericType_TypeId> types;
    CompositeKey fields;
    if (!extract_key_fields(key, &types, &fields) || fields.size() > schema.num_key_columns()) {
        return false;
    }
    for (size_t column_index = 0; column_index < fields.size(); ++column_index) {
        const auto& column = schema.column(column_index);
        if (column.is_nullable() || !is_supported_distributed_base_key(column.type())) {
            return false;
        }
        const auto expected_type = is_integer_key(column.type())
                                           ? integer_key_pb_type(column.type())
                                           : PGenericType::STRING;
        if (types[column_index] != expected_type) {
            return false;
        }
        tuple->add_field(std::move(fields[column_index]));
    }
    return true;
}

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

Status validate_submit_request(const PCloudDistributedCompactionSubmitRequest& request) {
    const bool has_valid_input =
            request.compaction_type() == CLOUD_DISTRIBUTED_CUMULATIVE_COMPACTION
                    ? request.has_input_rowset_meta()
                    : request.compaction_type() == CLOUD_DISTRIBUTED_BASE_COMPACTION &&
                              request.input_rowset_metas_size() > 1;
    if (!has_valid_input || !request.has_output_rowset_meta() || request.execution_id().empty()) {
        return Status::InvalidArgument("invalid distributed compaction request");
    }
    const auto* cluster_info =
            static_cast<const CloudClusterInfo*>(ExecEnv::GetInstance()->cluster_info());
    DORIS_CHECK(cluster_info != nullptr);
    if (request.target_backend_id() != BackendOptions::get_backend_id() ||
        request.target_cloud_unique_id() != config::cloud_unique_id ||
        request.target_compute_group_id() != cluster_info->cloud_compute_group_id()) {
        return Status::InvalidArgument(
                "distributed compaction target mismatch: requested backend={}, "
                "cloud_unique_id={}, compute_group_id={}, local backend={}, cloud_unique_id={}, "
                "compute_group_id={}",
                request.target_backend_id(), request.target_cloud_unique_id(),
                request.target_compute_group_id(), BackendOptions::get_backend_id(),
                config::cloud_unique_id, cluster_info->cloud_compute_group_id());
    }
    return Status::OK();
}

Status validate_compaction_task(const PCloudDistributedCompactionSubmitRequest& request,
                                const PCloudDistributedCompactionTask& task) {
    const auto& key_range = task.key_range();
    std::vector<PGenericType_TypeId> lower_types;
    std::vector<PGenericType_TypeId> upper_types;
    CompositeKey lower_key;
    CompositeKey upper_key;
    const bool valid_key_range =
            task.has_key_range() && key_range.has_lower_key() && key_range.has_upper_key() &&
            extract_key_fields(key_range.lower_key(), &lower_types, &lower_key) &&
            extract_key_fields(key_range.upper_key(), &upper_types, &upper_key) &&
            lower_types == upper_types &&
            (lower_key < upper_key || (key_range.upper_inclusive() && lower_key == upper_key));
    const bool valid_range =
            request.compaction_type() == CLOUD_DISTRIBUTED_CUMULATIVE_COMPACTION
                    ? task.segment_pos_start() >= 0 &&
                              task.segment_pos_start() < task.segment_pos_end()
                    : request.compaction_type() == CLOUD_DISTRIBUTED_BASE_COMPACTION &&
                              key_range.lower_inclusive() && valid_key_range;
    if (!valid_range || task.max_segment_num() <= 0 || task.output_segment_start_id() < 0 ||
        task.group_index() < 0 ||
        cast_set<int64_t>(task.output_segment_start_id()) + task.max_segment_num() >
                std::numeric_limits<int32_t>::max()) {
        return Status::InvalidArgument("invalid distributed compaction request");
    }
    return Status::OK();
}

} // namespace

CompactionWorkerCache::CompactionWorkerCache(Fetcher fetcher) : _fetcher(std::move(fetcher)) {}

Status CompactionWorkerCache::get_workers(std::vector<CompactionWorkerInfo>* workers) {
    std::lock_guard lock(_mutex);
    const auto now = std::chrono::steady_clock::now();
    if (_initialized && now < _expires_at) {
        RETURN_IF_ERROR(_cached_status);
        *workers = _workers;
        return Status::OK();
    }

    std::vector<CompactionWorkerInfo> refreshed_workers;
    _cached_status = _fetcher(&refreshed_workers);
    _initialized = true;
    _expires_at =
            std::chrono::steady_clock::now() +
            std::chrono::milliseconds(config::cloud_distributed_compaction_worker_cache_ttl_ms);
    RETURN_IF_ERROR(_cached_status);
    _workers = std::move(refreshed_workers);
    *workers = _workers;
    return Status::OK();
}

void CompactionWorkerCache::invalidate() {
    std::lock_guard lock(_mutex);
    _initialized = false;
}

CompactionWorkerCache* compaction_worker_cache() {
    static CompactionWorkerCache cache(fetch_compaction_workers_from_fe);
    return &cache;
}

DistributedCompactionPollScheduler::DistributedCompactionPollScheduler()
        : _thread([this] { run(); }) {}

DistributedCompactionPollScheduler::~DistributedCompactionPollScheduler() {
    stop();
}

Status DistributedCompactionPollScheduler::schedule(std::chrono::milliseconds delay,
                                                    std::function<void()> callback) {
    DORIS_CHECK(callback != nullptr);
    DORIS_CHECK_GE(delay.count(), 0);
    {
        std::lock_guard lock(_mutex);
        if (_stopped) {
            return Status::Cancelled("distributed compaction poll scheduler is stopped");
        }
        _callbacks.emplace(
                std::make_pair(std::chrono::steady_clock::now() + delay, _next_sequence++),
                std::move(callback));
    }
    _cv.notify_one();
    return Status::OK();
}

void DistributedCompactionPollScheduler::stop() {
    std::vector<std::function<void()>> pending_callbacks;
    {
        std::lock_guard lock(_mutex);
        if (_stopped) {
            return;
        }
        _stopped = true;
        pending_callbacks.reserve(_callbacks.size());
        for (auto& [_, callback] : _callbacks) {
            pending_callbacks.push_back(std::move(callback));
        }
        _callbacks.clear();
    }
    _cv.notify_one();
    if (_thread.joinable()) {
        _thread.join();
    }
    // Wake suspended compactions immediately. Their callbacks observe the stopped engine and
    // drive the failure continuation before the compaction thread pools are shut down.
    for (auto& callback : pending_callbacks) {
        callback();
    }
}

void DistributedCompactionPollScheduler::run() {
    while (true) {
        std::function<void()> callback;
        {
            std::unique_lock lock(_mutex);
            while (!_stopped) {
                if (_callbacks.empty()) {
                    _cv.wait(lock);
                    continue;
                }
                const auto deadline = _callbacks.begin()->first.first;
                if (std::chrono::steady_clock::now() < deadline) {
                    _cv.wait_until(lock, deadline);
                    continue;
                }
                auto callback_iter = _callbacks.begin();
                callback = std::move(callback_iter->second);
                _callbacks.erase(callback_iter);
                break;
            }
            if (_stopped) {
                return;
            }
        }
        callback();
    }
}

Status distributed_compaction_submit_rpc(const std::string& endpoint,
                                         const PCloudDistributedCompactionSubmitRequest& request,
                                         PCloudDistributedCompactionSubmitResponse* response) {
    return call_worker(endpoint, request, response,
                       &PBackendService_Stub::cloud_distributed_compaction_submit,
                       "cloud_distributed_compaction_submit",
                       config::cloud_distributed_compaction_control_rpc_timeout_ms);
}

Status distributed_compaction_get_status_rpc(
        const std::string& endpoint, const PCloudDistributedCompactionGetStatusRequest& request,
        PCloudDistributedCompactionGetStatusResponse* response) {
    return call_worker(endpoint, request, response,
                       &PBackendService_Stub::cloud_distributed_compaction_get_status,
                       "cloud_distributed_compaction_get_status",
                       config::cloud_distributed_compaction_control_rpc_timeout_ms);
}

Status distributed_compaction_calc_incremental_delete_bitmap_rpc(
        const std::string& endpoint,
        const PCloudDistributedCompactionCalcIncrementalDeleteBitmapRequest& request,
        PCloudDistributedCompactionCalcIncrementalDeleteBitmapResponse* response) {
    const auto method =
            &PBackendService_Stub::cloud_distributed_compaction_calc_incremental_delete_bitmap;
    return call_worker(endpoint, request, response, method,
                       "cloud_distributed_compaction_calc_incremental_delete_bitmap",
                       config::cloud_distributed_compaction_incremental_bitmap_rpc_timeout_ms);
}

Status distributed_compaction_finalize_rpc(
        const std::string& endpoint, const PCloudDistributedCompactionFinalizeRequest& request,
        PCloudDistributedCompactionFinalizeResponse* response) {
    return call_worker(endpoint, request, response,
                       &PBackendService_Stub::cloud_distributed_compaction_finalize,
                       "cloud_distributed_compaction_finalize",
                       config::cloud_distributed_compaction_control_rpc_timeout_ms);
}

struct DistributedCompactionCoordinator::ValidatedPartialRowset {
    RowsetMeta meta;
    std::vector<KeyBoundsPB> key_bounds;
    std::vector<uint32_t> segment_rows;
};

struct DistributedCompactionCoordinator::ExecutionPlan {
    std::vector<CompactionWorkerInfo> workers;
    std::vector<std::vector<size_t>> groups_by_worker;
    std::vector<PCloudDistributedCompactionTaskResult> responses;
    std::vector<bool> worker_completed;
    std::vector<bool> task_completed;
    size_t remaining_workers = 0;
    int64_t expiration_time = 0;
    bool is_mow = false;
    bool check_missed_rows = false;
    bool polling_completed = false;
    CompletionCallback completion_callback;
};

struct DistributedCompactionCoordinator::PollRoundContext {
    explicit PollRoundContext(size_t worker_count)
            : rpc_statuses(worker_count, Status::OK()),
              status_responses(worker_count),
              polled_groups_by_worker(worker_count),
              status_requests(worker_count),
              scheduling_failed(worker_count, false) {}

    std::vector<Status> rpc_statuses;
    std::vector<PCloudDistributedCompactionGetStatusResponse> status_responses;
    std::vector<std::vector<size_t>> polled_groups_by_worker;
    std::vector<PCloudDistributedCompactionGetStatusRequest> status_requests;
    std::vector<bool> scheduling_failed;
    std::atomic<size_t> remaining_rpcs {0};
};

DistributedCompactionCoordinator::DistributedCompactionCoordinator(
        CloudStorageEngine& engine, std::shared_ptr<CloudTablet> tablet, std::string execution_id)
        : _engine(engine), _tablet(std::move(tablet)), _execution_id(std::move(execution_id)) {}

DistributedCompactionCoordinator::~DistributedCompactionCoordinator() = default;

Status DistributedCompactionCoordinator::submit_batches(
        const std::vector<CompactionWorkerInfo>& workers,
        const std::vector<std::vector<size_t>>& groups_by_worker,
        const std::vector<PCloudDistributedCompactionSubmitRequest>& requests) {
    DORIS_CHECK(_state != nullptr);
    DORIS_CHECK_EQ(workers.size(), groups_by_worker.size());
    DORIS_CHECK_EQ(workers.size(), requests.size());

    std::vector<PCloudDistributedCompactionSubmitResponse> submit_responses(workers.size());
    std::vector<Status> rpc_statuses(workers.size(), Status::OK());
    auto token = _engine.distributed_compaction_rpc_thread_pool().new_token(
            ThreadPool::ExecutionMode::CONCURRENT, cast_set<int>(workers.size()));

    // A successful response only means the worker accepted the tasks; compaction completion is
    // observed by asynchronous polling.
    Status submit_status = Status::OK();
    for (size_t worker_index = 0; worker_index < workers.size(); ++worker_index) {
        submit_status = token->submit_func([&, worker_index]() {
            const auto& group_indexes = groups_by_worker[worker_index];
            for (const size_t group_index : group_indexes) {
                _state->tasks[group_index].started = true;
            }
            LOG_INFO("submit distributed single-rowset compaction batch")
                    .tag("job_id", _execution_id)
                    .tag("endpoint", workers[worker_index].endpoint)
                    .tag("tasks", group_indexes.size());
            Status rpc_status;
            constexpr int MAX_SUBMIT_ATTEMPTS = 3;
            for (int attempt = 1; attempt <= MAX_SUBMIT_ATTEMPTS; ++attempt) {
                submit_responses[worker_index].Clear();
                rpc_status = distributed_compaction_submit_rpc(workers[worker_index].endpoint,
                                                               requests[worker_index],
                                                               &submit_responses[worker_index]);
                if (rpc_status.ok() || !is_retryable_control_rpc_error(rpc_status)) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100 * attempt));
            }
            rpc_statuses[worker_index] = std::move(rpc_status);
        });
        if (!submit_status.ok()) {
            break;
        }
    }
    token->wait();
    token->shutdown();
    RETURN_IF_ERROR(submit_status);

    for (const auto& status : rpc_statuses) {
        if (!status.ok()) {
            compaction_worker_cache()->invalidate();
            return status;
        }
    }
    return Status::OK();
}

Status DistributedCompactionCoordinator::start_single_rowset(
        const RowsetSharedPtr& input_rowset, RowsetWriter& output_rowset_writer,
        int64_t segment_group_size, bool allow_delete_in_cumu_compaction, bool is_vertical,
        uint32_t avg_segment_rows, CompletionCallback callback, bool* started) {
    DORIS_CHECK(callback != nullptr);
    DORIS_CHECK_GT(config::cloud_distributed_compaction_status_poll_interval_ms, 0);
    RETURN_IF_ERROR(prepare_single_rowset(input_rowset, output_rowset_writer, segment_group_size,
                                          allow_delete_in_cumu_compaction, is_vertical,
                                          avg_segment_rows, started));
    if (!*started) {
        return Status::OK();
    }
    _execution_plan->completion_callback = std::move(callback);
    return schedule_poll(std::chrono::milliseconds(
            config::cloud_distributed_compaction_status_poll_interval_ms));
}

Status DistributedCompactionCoordinator::start_base_key_ranges(
        const std::vector<RowsetSharedPtr>& input_rowsets, RowsetWriter& output_rowset_writer,
        size_t range_count, bool is_vertical, uint32_t avg_segment_rows,
        CompletionCallback callback, bool* started) {
    DORIS_CHECK(callback != nullptr);
    DORIS_CHECK_GT(config::cloud_distributed_compaction_status_poll_interval_ms, 0);
    RETURN_IF_ERROR(prepare_base_key_ranges(input_rowsets, output_rowset_writer, range_count,
                                            is_vertical, avg_segment_rows, started));
    if (!*started) {
        return Status::OK();
    }
    _execution_plan->completion_callback = std::move(callback);
    return schedule_poll(std::chrono::milliseconds(
            config::cloud_distributed_compaction_status_poll_interval_ms));
}

Status DistributedCompactionCoordinator::schedule_poll(std::chrono::milliseconds delay) {
    DORIS_CHECK(_execution_plan != nullptr);
    DORIS_CHECK(!_execution_plan->polling_completed);
    std::weak_ptr<DistributedCompactionCoordinator> weak_self = shared_from_this();
    return _engine.distributed_compaction_poll_scheduler().schedule(delay, [weak_self] {
        if (auto self = weak_self.lock()) {
            self->dispatch_poll();
        }
    });
}

void DistributedCompactionCoordinator::dispatch_poll() {
    DORIS_CHECK(_execution_plan != nullptr);
    DORIS_CHECK(!_execution_plan->polling_completed);
    if (_engine.stopped()) {
        complete_polling(Status::Cancelled("storage engine stopped during distributed compaction"));
        return;
    }
    if (_execution_plan->expiration_time <= ::time(nullptr)) {
        complete_polling(Status::TimedOut(
                "distributed single-rowset compaction expired while waiting for workers"));
        return;
    }

    auto round = std::make_shared<PollRoundContext>(_execution_plan->workers.size());
    size_t rpc_count = 0;
    for (size_t worker_index = 0; worker_index < _execution_plan->workers.size(); ++worker_index) {
        if (_execution_plan->worker_completed[worker_index]) {
            continue;
        }
        ++rpc_count;
        auto& status_request = round->status_requests[worker_index];
        status_request.set_execution_id(_execution_id);
        for (const size_t group_index : _execution_plan->groups_by_worker[worker_index]) {
            if (_execution_plan->task_completed[group_index]) {
                continue;
            }
            round->polled_groups_by_worker[worker_index].push_back(group_index);
            const auto& distributed_task = _state->tasks[group_index];
            auto* status_task = status_request.add_tasks();
            status_task->set_group_index(distributed_task.group_index);
        }
        DORIS_CHECK(!round->polled_groups_by_worker[worker_index].empty());
    }
    DORIS_CHECK_EQ(rpc_count, _execution_plan->remaining_workers);
    round->remaining_rpcs.store(rpc_count, std::memory_order_release);

    auto self = shared_from_this();
    for (size_t worker_index = 0; worker_index < _execution_plan->workers.size(); ++worker_index) {
        if (_execution_plan->worker_completed[worker_index]) {
            continue;
        }
        const Status submit_status = _engine.distributed_compaction_rpc_thread_pool().submit_func(
                [self, round, worker_index] {
                    round->rpc_statuses[worker_index] = distributed_compaction_get_status_rpc(
                            self->_execution_plan->workers[worker_index].endpoint,
                            round->status_requests[worker_index],
                            &round->status_responses[worker_index]);
                    if (round->remaining_rpcs.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                        self->finish_poll_round(round);
                    }
                });
        if (!submit_status.ok()) {
            round->scheduling_failed[worker_index] = true;
            round->rpc_statuses[worker_index] = submit_status;
            if (round->remaining_rpcs.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                finish_poll_round(round);
            }
        }
    }
}

void DistributedCompactionCoordinator::finish_poll_round(std::shared_ptr<PollRoundContext> round) {
    DORIS_CHECK(_execution_plan != nullptr);
    DORIS_CHECK(!_execution_plan->polling_completed);
    if (_engine.stopped()) {
        complete_polling(Status::Cancelled("storage engine stopped during distributed compaction"));
        return;
    }

    for (size_t worker_index = 0; worker_index < _execution_plan->workers.size(); ++worker_index) {
        if (_execution_plan->worker_completed[worker_index]) {
            continue;
        }
        const Status& rpc_status = round->rpc_statuses[worker_index];
        if (!rpc_status.ok()) {
            if (round->scheduling_failed[worker_index] ||
                is_retryable_control_rpc_error(rpc_status)) {
                LOG_WARNING("failed to query distributed compaction status, will retry")
                        .tag("job_id", _execution_id)
                        .tag("endpoint", _execution_plan->workers[worker_index].endpoint)
                        .error(rpc_status);
                continue;
            }
            complete_polling(rpc_status);
            return;
        }

        const auto& group_indexes = round->polled_groups_by_worker[worker_index];
        const auto& status_response = round->status_responses[worker_index];
        if (status_response.task_statuses_size() != cast_set<int>(group_indexes.size())) {
            complete_polling(Status::InvalidArgument(
                    "distributed single-rowset compaction status returned {} results for {} "
                    "tasks",
                    status_response.task_statuses_size(), group_indexes.size()));
            return;
        }
        bool batch_completed = true;
        for (size_t task_index = 0; task_index < group_indexes.size(); ++task_index) {
            const size_t group_index = group_indexes[task_index];
            const auto& distributed_task = _state->tasks[group_index];
            const auto& worker_status = status_response.task_statuses(cast_set<int>(task_index));
            if (!worker_status.has_state() ||
                worker_status.group_index() != distributed_task.group_index) {
                complete_polling(Status::InvalidArgument(
                        "mismatched distributed single-rowset compaction status for group {}",
                        group_index));
                return;
            }
            if (worker_status.state() == CLOUD_DISTRIBUTED_COMPACTION_TASK_PENDING ||
                worker_status.state() == CLOUD_DISTRIBUTED_COMPACTION_TASK_RUNNING) {
                batch_completed = false;
                continue;
            }
            if (worker_status.state() != CLOUD_DISTRIBUTED_COMPACTION_TASK_SUCCEEDED &&
                worker_status.state() != CLOUD_DISTRIBUTED_COMPACTION_TASK_FAILED) {
                complete_polling(Status::InvalidArgument(
                        "invalid distributed single-rowset compaction state for group {}",
                        group_index));
                return;
            }
            if (!worker_status.has_result()) {
                complete_polling(Status::InvalidArgument(
                        "terminal distributed single-rowset compaction status has no result for "
                        "group {}",
                        group_index));
                return;
            }
            const auto& task_result = worker_status.result();
            if (!task_result.has_status() ||
                task_result.group_index() != distributed_task.group_index) {
                complete_polling(Status::InvalidArgument(
                        "mismatched distributed single-rowset compaction result for group {}",
                        group_index));
                return;
            }
            const Status result_status = Status::create(task_result.status());
            if ((worker_status.state() == CLOUD_DISTRIBUTED_COMPACTION_TASK_SUCCEEDED) !=
                result_status.ok()) {
                complete_polling(Status::InvalidArgument(
                        "distributed single-rowset compaction state and result disagree for group "
                        "{}",
                        group_index));
                return;
            }
            if (!result_status.ok()) {
                complete_polling(result_status);
                return;
            }
            _execution_plan->responses[group_index] = task_result;
            _execution_plan->task_completed[group_index] = true;
        }
        if (batch_completed) {
            _execution_plan->worker_completed[worker_index] = true;
            --_execution_plan->remaining_workers;
        }
    }

    if (_execution_plan->remaining_workers == 0) {
        LOG_INFO("finish polling distributed single-rowset compaction tasks")
                .tag("job_id", _execution_id)
                .tag("workers", _execution_plan->workers.size())
                .tag("tasks", _state->tasks.size());
        complete_polling(Status::OK());
        return;
    }
    const Status schedule_status = schedule_poll(std::chrono::milliseconds(
            config::cloud_distributed_compaction_status_poll_interval_ms));
    if (!schedule_status.ok()) {
        complete_polling(schedule_status);
    }
}

void DistributedCompactionCoordinator::complete_polling(Status status) {
    DORIS_CHECK(_execution_plan != nullptr);
    DORIS_CHECK(!_execution_plan->polling_completed);
    DORIS_CHECK(_execution_plan->completion_callback != nullptr);
    _execution_plan->polling_completed = true;
    if (!status.ok()) {
        compaction_worker_cache()->invalidate();
    }
    auto callback = std::move(_execution_plan->completion_callback);
    callback(std::move(status));
}

Status DistributedCompactionCoordinator::validate_partial_rowset(
        size_t group_index, const PCloudDistributedCompactionTaskResult& response,
        const DistributedCompactionTask& task, const TabletSchema& tablet_schema,
        const RowsetId& output_rowset_id, ValidatedPartialRowset* partial_rowset) const {
    if (response.group_index() != task.group_index || !response.has_partial_rowset_meta()) {
        return Status::InvalidArgument(
                "mismatched distributed single-rowset compaction response for group {}",
                group_index);
    }
    auto& partial_meta = partial_rowset->meta;
    if (!partial_meta.init_from_pb(response.partial_rowset_meta())) {
        return Status::InvalidArgument("failed to initialize partial rowset metadata for group {}",
                                       group_index);
    }
    if (partial_meta.rowset_id() != output_rowset_id) {
        return Status::InvalidArgument("partial rowset id mismatch for group {}", group_index);
    }
    const int64_t num_segments = partial_meta.num_segments();
    if (num_segments < 0) {
        return Status::InvalidArgument("partial rowset has negative segment count for group {}",
                                       group_index);
    }
    if (num_segments > task.segment_id_slot.capacity) {
        return Status::Error<ErrorCode::TOO_MANY_SEGMENTS>(
                "group {} produced {} segments, slot capacity is {}", group_index, num_segments,
                task.segment_id_slot.capacity);
    }
    const size_t expected_num_segments = cast_set<size_t>(num_segments);
    if (partial_meta.segment_ids().size() != expected_num_segments) {
        return Status::InvalidArgument(
                "partial rowset does not contain an explicit segment id list for group {}",
                group_index);
    }
    for (const auto segment : partial_meta.segments()) {
        if (segment.id() < task.segment_id_slot.start_id ||
            segment.id() >= cast_set<int64_t>(task.segment_id_slot.start_id) +
                                    task.segment_id_slot.capacity) {
            return Status::InvalidArgument(
                    "output segment {} is outside slot [{}, {}) for group {}", segment.id(),
                    task.segment_id_slot.start_id,
                    cast_set<int64_t>(task.segment_id_slot.start_id) +
                            task.segment_id_slot.capacity,
                    group_index);
        }
    }

    partial_meta.get_segments_key_bounds(&partial_rowset->key_bounds);
    if (partial_rowset->key_bounds.size() != expected_num_segments) {
        return Status::InvalidArgument("partial key bounds are not position-aligned for group {}",
                                       group_index);
    }
    partial_meta.get_num_segment_rows(&partial_rowset->segment_rows);
    if (partial_rowset->segment_rows.size() != expected_num_segments) {
        return Status::InvalidArgument(
                "partial segment row counts are not position-aligned for group {}", group_index);
    }
    if ((tablet_schema.has_inverted_index() || tablet_schema.has_ann_index()) &&
        partial_meta.inverted_index_file_info().size() != expected_num_segments) {
        return Status::InvalidArgument(
                "partial inverted-index metadata is not position-aligned for group {}",
                group_index);
    }
    return Status::OK();
}

Status DistributedCompactionCoordinator::prepare_single_rowset(
        const RowsetSharedPtr& input_rowset, RowsetWriter& output_rowset_writer,
        int64_t segment_group_size, bool allow_delete_in_cumu_compaction, bool is_vertical,
        uint32_t avg_segment_rows, bool* started) {
    *started = false;
    if (!config::enable_cloud_single_rowset_distributed_compaction) {
        return Status::OK();
    }

    const auto segment_ranges =
            build_segment_group_merge_ranges(*input_rowset->rowset_meta(), segment_group_size);
    if (segment_ranges.size() < 2) {
        return Status::OK();
    }

    // Step 1 (prepare): discover workers, assign segment groups, and reserve disjoint output
    // segment id slots. The coordinator is also eligible to execute a group through the same
    // worker RPC path. The later MOW incremental-bitmap and finalize steps run after this method
    // assembles the output rowset.
    std::vector<CompactionWorkerInfo> workers;
    const Status discovery_status = compaction_worker_cache()->get_workers(&workers);
    if (!discovery_status.ok()) {
        LOG(WARNING) << "failed to discover distributed single-rowset compaction workers from FE, "
                     << "fallback to local compaction: " << discovery_status;
        return Status::OK();
    }
    workers = select_compaction_workers_for_groups(workers, BackendOptions::get_backend_id(),
                                                   segment_ranges.size(), _execution_id);
    // A single worker uses the original local grouped-compaction path because distribution would
    // add RPC and retained worker-state overhead without providing parallelism.
    if (workers.size() < 2) {
        return Status::OK();
    }

    std::vector<OutputRowsetSegmentIdSlot> segment_slots;
    RETURN_IF_ERROR(build_output_rowset_segment_id_slots(
            output_rowset_writer.get_allocated_segment_id(),
            config::cloud_distributed_compaction_segment_slot_capacity, segment_ranges.size(),
            &segment_slots));

    const bool is_mow = _tablet->keys_type() == KeysType::UNIQUE_KEYS &&
                        _tablet->enable_unique_key_merge_on_write();
    const bool check_missed_rows = is_mow &&
                                   (config::enable_missing_rows_correctness_check ||
                                    config::enable_mow_compaction_correctness_check_core ||
                                    config::enable_mow_compaction_correctness_check_fail) &&
                                   !allow_delete_in_cumu_compaction;
    int64_t phase1_end_version = 0;
    if (is_mow) {
        RETURN_IF_ERROR(_engine.meta_mgr().sync_tablet_rowsets(_tablet.get()));
        phase1_end_version = _tablet->max_version().second;
    }

    _state = std::make_unique<DistributedCompactionState>();
    _state->phase1_end_version = phase1_end_version;
    _state->output_delete_bitmap = std::make_shared<DeleteBitmap>(_tablet->tablet_id());
    _state->tasks.reserve(segment_ranges.size());
    std::vector<std::vector<size_t>> groups_by_worker(workers.size());
    // The coordinator is first in workers, so it receives one additional group when the groups
    // cannot be evenly distributed among workers.
    for (size_t group_index = 0; group_index < segment_ranges.size(); ++group_index) {
        const size_t worker_index = group_index % workers.size();
        const auto& worker = workers[worker_index];
        _state->tasks.push_back({.worker_endpoint = worker.endpoint,
                                 .group_index = cast_set<int32_t>(group_index),
                                 .segment_id_slot = segment_slots[group_index]});
        groups_by_worker[worker_index].push_back(group_index);
    }

    const auto input_meta_pb = input_rowset->rowset_meta()->get_rowset_pb();
    const auto output_meta_pb = output_rowset_writer.rowset_meta()->get_rowset_pb();
    // Step 2 (submit): build one request per worker so common rowset metadata is sent once while
    // worker-specific segment groups remain independent tasks in the batch.
    std::vector<PCloudDistributedCompactionSubmitRequest> requests(workers.size());
    for (size_t worker_index = 0; worker_index < workers.size(); ++worker_index) {
        const auto& worker = workers[worker_index];
        auto& request = requests[worker_index];
        request.set_tablet_id(_tablet->tablet_id());
        request.set_execution_id(_execution_id);
        *request.mutable_input_rowset_meta() = input_meta_pb;
        *request.mutable_output_rowset_meta() = output_meta_pb;
        request.set_is_vertical(is_vertical);
        request.set_avg_segment_rows(avg_segment_rows);
        request.set_delete_bitmap_start_version(0);
        request.set_delete_bitmap_end_version(phase1_end_version + 1);
        request.set_check_missed_rows(check_missed_rows);
        request.set_target_backend_id(worker.backend_id);
        request.set_target_cloud_unique_id(worker.cloud_unique_id);
        request.set_target_compute_group_id(worker.compute_group_id);
        for (const size_t group_index : groups_by_worker[worker_index]) {
            const auto& range = segment_ranges[group_index];
            const auto& distributed_task = _state->tasks[group_index];
            auto* request_task = request.add_tasks();
            request_task->set_group_index(distributed_task.group_index);
            request_task->set_segment_pos_start(range.segment_pos_start);
            request_task->set_segment_pos_end(range.segment_pos_end);
            request_task->set_output_segment_start_id(distributed_task.segment_id_slot.start_id);
            request_task->set_max_segment_num(distributed_task.segment_id_slot.capacity);
            request_task->set_merge_way_num(range.merge_way_num);
        }
    }
    RETURN_IF_ERROR(submit_batches(workers, groups_by_worker, requests));

    _execution_plan = std::make_unique<ExecutionPlan>();
    _execution_plan->workers = workers;
    _execution_plan->groups_by_worker = std::move(groups_by_worker);
    _execution_plan->responses.resize(segment_ranges.size());
    _execution_plan->worker_completed.assign(workers.size(), false);
    _execution_plan->task_completed.assign(segment_ranges.size(), false);
    _execution_plan->remaining_workers = workers.size();
    _execution_plan->expiration_time = output_meta_pb.txn_expiration();
    _execution_plan->is_mow = is_mow;
    _execution_plan->check_missed_rows = check_missed_rows;
    *started = true;
    return Status::OK();
}

Status DistributedCompactionCoordinator::prepare_base_key_ranges(
        const std::vector<RowsetSharedPtr>& input_rowsets, RowsetWriter& output_rowset_writer,
        size_t range_count, bool is_vertical, uint32_t avg_segment_rows, bool* started) {
    *started = false;
    DORIS_CHECK_GT(input_rowsets.size(), 1);
    const auto& schema = *_tablet->tablet_schema();
    const bool is_mow = _tablet->keys_type() == KeysType::UNIQUE_KEYS &&
                        _tablet->enable_unique_key_merge_on_write();
    DORIS_CHECK(!is_mow || schema.cluster_key_uids().empty());
    DORIS_CHECK(is_mow || schema.num_short_key_columns() > 0);
    size_t key_column_count = 0;
    while (key_column_count < schema.num_key_columns() &&
           is_supported_distributed_base_key(schema.column(key_column_count).type()) &&
           !schema.column(key_column_count).is_nullable()) {
        ++key_column_count;
    }
    DORIS_CHECK_GT(key_column_count, 0);
    if (!config::enable_cloud_distributed_base_compaction || range_count < 2) {
        return Status::OK();
    }

    std::vector<CompactionWorkerInfo> candidates;
    const Status discovery_status = compaction_worker_cache()->get_workers(&candidates);
    if (!discovery_status.ok()) {
        LOG(WARNING) << "failed to discover distributed base compaction workers from FE, "
                     << "fallback to local compaction: " << discovery_status;
        return Status::OK();
    }
    range_count = std::min(range_count, candidates.size());
    if (range_count < 2) {
        return Status::OK();
    }

    // ponytail: collect all index samples; switch to a streaming k-way merge if coordinator
    // memory becomes measurable for very large tablets.
    std::vector<CompositeKeySample> samples;
    for (const auto& rowset : input_rowsets) {
        auto beta_rowset = std::dynamic_pointer_cast<BetaRowset>(rowset);
        if (beta_rowset == nullptr) {
            return Status::InvalidArgument("distributed base compaction requires beta rowsets");
        }
        std::vector<segment_v2::SegmentSharedPtr> segments;
        RETURN_IF_ERROR(beta_rowset->load_segments(&segments));
        for (const auto& segment : segments) {
            OlapReaderStatistics reader_stats;
            if (!is_mow) {
                RETURN_IF_ERROR(segment->load_index(&reader_stats));
            }
            const auto* short_key_index = is_mow ? nullptr : segment->get_short_key_index();
            DORIS_CHECK(is_mow || short_key_index != nullptr);
            const uint64_t rows_per_block = is_mow ? schema.num_rows_per_row_block()
                                                   : short_key_index->num_rows_per_block();
            DORIS_CHECK_GT(rows_per_block, 0);
            const uint64_t sample_count =
                    is_mow ? (segment->num_rows() + rows_per_block - 1) / rows_per_block
                           : short_key_index->num_items();
            std::vector<rowid_t> rowids;
            std::vector<uint64_t> weights;
            rowids.reserve(sample_count);
            weights.reserve(sample_count);
            for (uint64_t sample = 0; sample < sample_count; ++sample) {
                const uint64_t row_start = sample * rows_per_block;
                DORIS_CHECK_LT(row_start, segment->num_rows());
                rowids.push_back(cast_set<rowid_t>(row_start));
                weights.push_back(std::min(rows_per_block,
                                           cast_set<uint64_t>(segment->num_rows()) - row_start));
            }
            std::vector<CompositeKey> keys;
            RETURN_IF_ERROR(read_key_samples(segment, schema, key_column_count, rowids,
                                             &reader_stats, &keys));
            DORIS_CHECK_EQ(keys.size(), weights.size());
            for (size_t sample_index = 0; sample_index < keys.size(); ++sample_index) {
                samples.push_back(
                        {.key = std::move(keys[sample_index]), .weight = weights[sample_index]});
            }
        }
    }

    const auto key_plan = choose_composite_key_range_boundaries(samples, range_count);
    const size_t group_count = key_plan.boundaries.size() + 1;
    if (group_count < 2) {
        return Status::OK();
    }
    auto workers = select_compaction_workers_for_groups(
            candidates, BackendOptions::get_backend_id(), group_count, _execution_id);
    DORIS_CHECK_EQ(workers.size(), group_count);

    std::vector<OutputRowsetSegmentIdSlot> segment_slots;
    RETURN_IF_ERROR(build_output_rowset_segment_id_slots(
            output_rowset_writer.get_allocated_segment_id(),
            config::cloud_distributed_compaction_segment_slot_capacity, group_count,
            &segment_slots));

    _state = std::make_unique<DistributedCompactionState>();
    if (is_mow) {
        RETURN_IF_ERROR(_engine.meta_mgr().sync_tablet_rowsets(_tablet.get()));
        _state->phase1_end_version = _tablet->max_version().second;
        _state->output_delete_bitmap = std::make_shared<DeleteBitmap>(_tablet->tablet_id());
    }
    _state->tasks.reserve(group_count);
    std::vector<std::vector<size_t>> groups_by_worker(group_count);
    for (size_t group_index = 0; group_index < group_count; ++group_index) {
        _state->tasks.push_back({.worker_endpoint = workers[group_index].endpoint,
                                 .group_index = cast_set<int32_t>(group_index),
                                 .segment_id_slot = segment_slots[group_index]});
        groups_by_worker[group_index].push_back(group_index);
    }

    int64_t merge_way_num = 0;
    for (const auto& rowset : input_rowsets) {
        merge_way_num += rowset->rowset_meta()->get_merge_way_num();
    }
    const auto output_meta_pb = output_rowset_writer.rowset_meta()->get_rowset_pb();
    std::vector<PCloudDistributedCompactionSubmitRequest> requests(group_count);
    for (size_t group_index = 0; group_index < group_count; ++group_index) {
        const auto& worker = workers[group_index];
        auto& request = requests[group_index];
        request.set_tablet_id(_tablet->tablet_id());
        request.set_execution_id(_execution_id);
        request.set_compaction_type(CLOUD_DISTRIBUTED_BASE_COMPACTION);
        for (const auto& rowset : input_rowsets) {
            *request.add_input_rowset_metas() = rowset->rowset_meta()->get_rowset_pb();
        }
        *request.mutable_output_rowset_meta() = output_meta_pb;
        request.set_is_vertical(is_vertical);
        request.set_avg_segment_rows(avg_segment_rows);
        request.set_delete_bitmap_start_version(0);
        request.set_delete_bitmap_end_version(_state->phase1_end_version + 1);
        request.set_check_missed_rows(false);
        request.set_target_backend_id(worker.backend_id);
        request.set_target_cloud_unique_id(worker.cloud_unique_id);
        request.set_target_compute_group_id(worker.compute_group_id);

        const auto& distributed_task = _state->tasks[group_index];
        auto* request_task = request.add_tasks();
        request_task->set_group_index(distributed_task.group_index);
        request_task->set_output_segment_start_id(distributed_task.segment_id_slot.start_id);
        request_task->set_max_segment_num(distributed_task.segment_id_slot.capacity);
        request_task->set_merge_way_num(merge_way_num);
        auto* key_range = request_task->mutable_key_range();
        const auto lower_key = group_index == 0 ? key_limit(schema, key_plan.prefix_length, false)
                                                : key_plan.boundaries[group_index - 1];
        const auto upper_key = group_index + 1 == group_count
                                       ? key_limit(schema, key_plan.prefix_length, true)
                                       : key_plan.boundaries[group_index];
        set_key_fields(key_range->mutable_lower_key(), schema, lower_key);
        set_key_fields(key_range->mutable_upper_key(), schema, upper_key);
        key_range->set_upper_inclusive(group_index + 1 == group_count);
    }
    RETURN_IF_ERROR(submit_batches(workers, groups_by_worker, requests));

    _execution_plan = std::make_unique<ExecutionPlan>();
    _execution_plan->workers = std::move(workers);
    _execution_plan->groups_by_worker = std::move(groups_by_worker);
    _execution_plan->responses.resize(group_count);
    _execution_plan->worker_completed.assign(group_count, false);
    _execution_plan->task_completed.assign(group_count, false);
    _execution_plan->remaining_workers = group_count;
    _execution_plan->expiration_time = output_meta_pb.txn_expiration();
    _execution_plan->is_mow = is_mow;
    _execution_plan->check_missed_rows = false;
    *started = true;
    return Status::OK();
}

Status DistributedCompactionCoordinator::assemble_single_rowset(
        RowsetWriter& output_rowset_writer, const TabletSchema& tablet_schema,
        std::vector<int32_t>* output_segment_group_sizes, RowsetSharedPtr* output_rowset,
        Merger::Statistics* stats) {
    DORIS_CHECK(_execution_plan != nullptr);
    DORIS_CHECK(_execution_plan->polling_completed);
    const auto& workers = _execution_plan->workers;
    const auto& responses = _execution_plan->responses;
    const bool is_mow = _execution_plan->is_mow;
    const bool check_missed_rows = _execution_plan->check_missed_rows;

    // Step 4 (assemble): validate every partial rowset and merge its metadata, statistics, and the
    // first-phase delete-bitmap shard into the coordinator's output rowset.
    int64_t output_num_rows = 0;
    int64_t output_data_size = 0;
    int64_t output_index_size = 0;
    int64_t output_total_size = 0;
    bool key_bounds_truncated = false;
    bool segment_file_sizes_available = true;
    int64_t missed_rows_count = 0;
    std::unordered_set<int64_t> output_segment_id_set;
    std::vector<int64_t> output_segment_ids;
    std::vector<KeyBoundsPB> output_key_bounds;
    std::vector<uint32_t> output_segment_rows;
    std::vector<size_t> output_segment_file_sizes;
    std::vector<InvertedIndexFileInfo> output_index_file_info;
    *stats = Merger::Statistics {};

    for (size_t group_index = 0; group_index < responses.size(); ++group_index) {
        const auto& response = responses[group_index];
        const auto& task = _state->tasks[group_index];
        ValidatedPartialRowset partial_rowset;
        RETURN_IF_ERROR(validate_partial_rowset(group_index, response, task, tablet_schema,
                                                output_rowset_writer.rowset_id(), &partial_rowset));
        const auto& partial_meta = partial_rowset.meta;
        LOG_INFO("finish distributed compaction worker task")
                .tag("job_id", _execution_id)
                .tag("tablet_id", _tablet->tablet_id())
                .tag("group_index", task.group_index)
                .tag("endpoint", task.worker_endpoint)
                .tag("output_rows", partial_meta.num_rows())
                .tag("output_segments", partial_meta.num_segments())
                .tag("output_rowset_data_size", partial_meta.data_disk_size())
                .tag("output_rowset_index_size", partial_meta.index_disk_size())
                .tag("output_rowset_total_size", partial_meta.total_disk_size())
                .tag("merged_rows", response.merged_rows())
                .tag("filtered_rows", response.filtered_rows())
                .tag("local_read_bytes", response.bytes_read_from_local())
                .tag("remote_read_bytes", response.bytes_read_from_remote())
                .tag("cached_bytes_total", response.cached_bytes_total())
                .tag("local_read_time_us", response.cloud_local_read_time())
                .tag("remote_read_time_us", response.cloud_remote_read_time());

        for (const auto segment : partial_meta.segments()) {
            if (!output_segment_id_set.emplace(segment.id()).second) {
                return Status::InvalidArgument("duplicate output segment id {}", segment.id());
            }
            output_segment_ids.push_back(segment.id());
        }

        output_key_bounds.insert(output_key_bounds.end(), partial_rowset.key_bounds.begin(),
                                 partial_rowset.key_bounds.end());
        output_segment_rows.insert(output_segment_rows.end(), partial_rowset.segment_rows.begin(),
                                   partial_rowset.segment_rows.end());

        if (partial_meta.num_segments() > 0) {
            if (partial_meta.segments_file_size().size() !=
                cast_set<size_t>(partial_meta.num_segments())) {
                segment_file_sizes_available = false;
            } else if (segment_file_sizes_available) {
                for (const auto file_size : partial_meta.segments_file_size()) {
                    output_segment_file_sizes.push_back(cast_set<size_t>(file_size));
                }
            }
            output_segment_group_sizes->push_back(cast_set<int32_t>(partial_meta.num_segments()));
        }
        for (const auto& file_info : partial_meta.inverted_index_file_info()) {
            output_index_file_info.push_back(file_info);
        }
        output_num_rows += partial_meta.num_rows();
        output_data_size += partial_meta.data_disk_size();
        output_index_size += partial_meta.index_disk_size();
        output_total_size += partial_meta.total_disk_size();
        key_bounds_truncated |= partial_meta.is_segments_key_bounds_truncated();
        missed_rows_count += response.missed_rows_count();
        stats->output_rows += response.output_rows();
        stats->merged_rows += response.merged_rows();
        stats->filtered_rows += response.filtered_rows();
        stats->bytes_read_from_local += response.bytes_read_from_local();
        stats->bytes_read_from_remote += response.bytes_read_from_remote();
        stats->cached_bytes_total += response.cached_bytes_total();
        stats->cloud_local_read_time += response.cloud_local_read_time();
        stats->cloud_remote_read_time += response.cloud_remote_read_time();
        if (is_mow && response.has_output_delete_bitmap_shard()) {
            _state->output_delete_bitmap->merge(DeleteBitmap::from_pb(
                    response.output_delete_bitmap_shard(), _tablet->tablet_id()));
        }
    }

    if (!segment_file_sizes_available) {
        output_segment_file_sizes.clear();
    }
    DORIS_CHECK_EQ(output_segment_ids.size(), output_segment_rows.size());
    DORIS_CHECK_EQ(output_segment_ids.size(), output_key_bounds.size());
    DORIS_CHECK(output_index_file_info.empty() ||
                output_index_file_info.size() == output_segment_ids.size());
    if (check_missed_rows && _tablet->tablet_state() == TABLET_RUNNING &&
        stats->merged_rows + stats->filtered_rows >= 0 &&
        stats->merged_rows + stats->filtered_rows != missed_rows_count) {
        const Status status = Status::InternalError(
                "distributed single-rowset compaction merged rows ({}) plus filtered rows ({}) "
                "does not equal missed rows ({})",
                stats->merged_rows, stats->filtered_rows, missed_rows_count);
        if (config::enable_mow_compaction_correctness_check_core) {
            CHECK(false) << status;
        }
        if (config::enable_mow_compaction_correctness_check_fail) {
            return status;
        }
        DCHECK(false) << status;
    }

    auto final_meta = std::make_shared<RowsetMeta>();
    final_meta->set_num_rows(output_num_rows);
    final_meta->set_total_disk_size(output_total_size);
    final_meta->set_data_disk_size(output_data_size);
    final_meta->set_index_disk_size(output_index_size);
    final_meta->set_empty(output_num_rows == 0);
    final_meta->set_num_segments(cast_set<int64_t>(output_segment_ids.size()));
    if (!output_segment_ids.empty()) {
        final_meta->set_segment_ids(output_segment_ids);
    }
    // Use NONOVERLAPPING as the default layout for an empty output or a single non-empty output
    // group. CloudCumulativeCompaction::update_output_rowset_after_build() changes it to
    // NONOVERLAPPING_WITHIN_GROUP and records segment_group_sizes when multiple non-empty output
    // groups are assembled.
    final_meta->set_segments_overlap(NONOVERLAPPING);
    final_meta->set_rowset_state(VISIBLE);
    final_meta->set_segments_key_bounds_truncated(key_bounds_truncated);
    final_meta->set_segments_key_bounds(output_key_bounds, false);
    final_meta->set_num_segment_rows(output_segment_rows);

    _output_rowset = output_rowset_writer.manual_build(final_meta);
    if (_output_rowset == nullptr) {
        return Status::InternalError("failed to build distributed single-rowset compaction output");
    }
    if (!output_segment_file_sizes.empty()) {
        _output_rowset->rowset_meta()->add_segments_file_size(output_segment_file_sizes);
    }
    if (!output_index_file_info.empty()) {
        std::vector<const InvertedIndexFileInfo*> output_index_file_info_ptrs;
        output_index_file_info_ptrs.reserve(output_index_file_info.size());
        for (const auto& file_info : output_index_file_info) {
            output_index_file_info_ptrs.push_back(&file_info);
        }
        _output_rowset->rowset_meta()->add_inverted_index_files_info(output_index_file_info_ptrs);
    }

    *output_rowset = _output_rowset;
    LOG_INFO("finish distributed single-rowset compaction merge, tablet_id={}",
             _tablet->tablet_id())
            .tag("job_id", _execution_id)
            .tag("groups", responses.size())
            .tag("workers", workers.size())
            .tag("output_segments", output_segment_ids.size());
    return Status::OK();
}

Status DistributedCompactionCoordinator::fetch_incremental_delete_bitmap(
        int64_t incremental_end_version) {
    DORIS_CHECK(_state != nullptr);
    DORIS_CHECK(_state->output_delete_bitmap != nullptr);
    DORIS_CHECK_GT(incremental_end_version, _state->phase1_end_version);
    LOG_INFO("fetch distributed single-rowset incremental delete bitmap")
            .tag("job_id", _execution_id)
            .tag("tablet_id", _tablet->tablet_id())
            .tag("start_version", _state->phase1_end_version)
            .tag("end_version", incremental_end_version);

    std::vector<std::string> worker_endpoints;
    std::unordered_map<std::string, size_t> worker_to_index;
    std::vector<std::vector<size_t>> groups_by_worker;
    for (size_t task_index = 0; task_index < _state->tasks.size(); ++task_index) {
        const auto& endpoint = _state->tasks[task_index].worker_endpoint;
        const auto [iter, inserted] = worker_to_index.emplace(endpoint, worker_endpoints.size());
        if (inserted) {
            worker_endpoints.push_back(endpoint);
            groups_by_worker.emplace_back();
        }
        groups_by_worker[iter->second].push_back(task_index);
    }

    std::vector<PCloudDistributedCompactionCalcIncrementalDeleteBitmapRequest> requests(
            worker_endpoints.size());
    std::vector<PCloudDistributedCompactionCalcIncrementalDeleteBitmapResponse> responses(
            worker_endpoints.size());
    for (size_t worker_index = 0; worker_index < worker_endpoints.size(); ++worker_index) {
        auto& request = requests[worker_index];
        request.set_tablet_id(_tablet->tablet_id());
        request.set_execution_id(_execution_id);
        request.set_delete_bitmap_start_version(_state->phase1_end_version);
        request.set_delete_bitmap_end_version(cast_set<uint64_t>(incremental_end_version) + 1);
        for (const size_t task_index : groups_by_worker[worker_index]) {
            const auto& task = _state->tasks[task_index];
            auto* request_task = request.add_tasks();
            request_task->set_group_index(task.group_index);
        }
    }

    std::vector<Status> worker_status(worker_endpoints.size(), Status::OK());
    auto token = _engine.distributed_compaction_rpc_thread_pool().new_token(
            ThreadPool::ExecutionMode::CONCURRENT, cast_set<int>(worker_endpoints.size()));
    Status submit_status = Status::OK();
    for (size_t worker_index = 0; worker_index < worker_endpoints.size(); ++worker_index) {
        submit_status = token->submit_func([&, worker_index]() {
            worker_status[worker_index] = distributed_compaction_calc_incremental_delete_bitmap_rpc(
                    worker_endpoints[worker_index], requests[worker_index],
                    &responses[worker_index]);
        });
        if (!submit_status.ok()) {
            break;
        }
    }
    token->wait();
    token->shutdown();
    RETURN_IF_ERROR(submit_status);
    for (const auto& status : worker_status) {
        if (!status.ok()) {
            compaction_worker_cache()->invalidate();
            return status;
        }
    }
    for (const auto& response : responses) {
        if (response.has_output_delete_bitmap_shard()) {
            _state->output_delete_bitmap->merge(DeleteBitmap::from_pb(
                    response.output_delete_bitmap_shard(), _tablet->tablet_id()));
        }
    }
    return Status::OK();
}

Status DistributedCompactionCoordinator::finish_mow_delete_bitmap(
        int64_t initiator, std::shared_ptr<DeleteBitmap>* output_delete_bitmap,
        int64_t* lock_start_time) {
    DORIS_CHECK(_state != nullptr);
    DORIS_CHECK(_state->output_delete_bitmap != nullptr);
    DORIS_CHECK(_output_rowset != nullptr);

    DBUG_EXECUTE_IF("CloudCumulativeCompaction::finish_distributed_mow_delete_bitmap.before_lock",
                    DBUG_BLOCK);
    RETURN_IF_ERROR(_engine.meta_mgr().get_delete_bitmap_update_lock(
            *_tablet, COMPACTION_DELETE_BITMAP_LOCK_ID, initiator));
    *lock_start_time = MonotonicMicros();

    RETURN_IF_ERROR(_engine.meta_mgr().sync_tablet_rowsets(_tablet.get()));
    const int64_t incremental_end_version = _tablet->max_version().second;
    DORIS_CHECK_GE(incremental_end_version, _state->phase1_end_version);
    // Old rowsets gain incremental delete bitmaps only when the tablet version advances.
    if (incremental_end_version > _state->phase1_end_version) {
        RETURN_IF_ERROR(fetch_incremental_delete_bitmap(incremental_end_version));
    } else {
        LOG_INFO("skip distributed single-rowset incremental delete bitmap")
                .tag("job_id", _execution_id)
                .tag("tablet_id", _tablet->tablet_id())
                .tag("phase1_end_version", _state->phase1_end_version);
    }

    std::shared_ptr<DeleteBitmap> delete_bitmap_v2;
    const int64_t store_version = config::delete_bitmap_store_write_version;
    if (store_version == 2 || store_version == 3) {
        delete_bitmap_v2 = std::make_shared<DeleteBitmap>(*_state->output_delete_bitmap);
        std::vector<DeleteBitmap::RowsetIdWithSegmentIds> retained_rowsets;
        {
            std::shared_lock read_lock(_tablet->get_header_lock());
            for (const auto& [rowset_version, rowset] : _tablet->rowset_map()) {
                if (rowset_version.second >= _output_rowset->start_version()) {
                    continue;
                }
                std::vector<DeleteBitmap::SegmentId> segment_ids;
                segment_ids.reserve(cast_set<size_t>(rowset->num_segments()));
                for (const auto segment : rowset->segments()) {
                    segment_ids.push_back(cast_set<DeleteBitmap::SegmentId>(segment.id()));
                }
                retained_rowsets.emplace_back(rowset->rowset_id(), std::move(segment_ids));
            }
        }
        if (config::enable_agg_delta_delete_bitmap_for_store_v2) {
            _tablet->tablet_meta()->delete_bitmap().subset_and_agg(
                    retained_rowsets, _output_rowset->start_version(),
                    _output_rowset->end_version(), delete_bitmap_v2.get());
        } else {
            _tablet->tablet_meta()->delete_bitmap().subset(
                    retained_rowsets, _output_rowset->start_version(),
                    _output_rowset->end_version(), delete_bitmap_v2.get());
        }
    }

    std::optional<StorageResource> storage_resource;
    auto output_storage_resource = _output_rowset->rowset_meta()->remote_storage_resource();
    if (output_storage_resource) {
        storage_resource = *output_storage_resource.value();
    }
    RETURN_IF_ERROR(_engine.meta_mgr().update_delete_bitmap(
            *_tablet, -1, initiator, _state->output_delete_bitmap.get(), delete_bitmap_v2.get(),
            _output_rowset->rowset_id().to_string(), storage_resource, store_version,
            _tablet->table_id()));
    *output_delete_bitmap = _state->output_delete_bitmap;
    return Status::OK();
}

void DistributedCompactionCoordinator::finalize(bool preserve_output_files) {
    if (_state == nullptr) {
        return;
    }
    std::unordered_map<std::string, std::vector<size_t>> groups_by_worker;
    for (size_t task_index = 0; task_index < _state->tasks.size(); ++task_index) {
        if (_state->tasks[task_index].started) {
            groups_by_worker[_state->tasks[task_index].worker_endpoint].push_back(task_index);
        }
    }

    if (!groups_by_worker.empty()) {
        const auto finalize_endpoint = [&](const std::string& endpoint,
                                           const std::vector<size_t>& task_indices) {
            PCloudDistributedCompactionFinalizeRequest request;
            request.set_execution_id(_execution_id);
            request.set_mode(preserve_output_files
                                     ? CLOUD_DISTRIBUTED_COMPACTION_PRESERVE_OUTPUT_FILES
                                     : CLOUD_DISTRIBUTED_COMPACTION_CANCEL_AND_RELEASE_STATE);
            for (const size_t task_index : task_indices) {
                const auto& task = _state->tasks[task_index];
                auto* request_task = request.add_tasks();
                request_task->set_group_index(task.group_index);
            }
            PCloudDistributedCompactionFinalizeResponse response;
            Status status;
            constexpr int MAX_FINALIZE_ATTEMPTS = 3;
            for (int attempt = 1; attempt <= MAX_FINALIZE_ATTEMPTS; ++attempt) {
                response.Clear();
                status = distributed_compaction_finalize_rpc(endpoint, request, &response);
                if (status.ok() || !status.is<ErrorCode::TOO_MANY_TASKS>()) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100 * attempt));
            }
            if (!status.ok()) {
                compaction_worker_cache()->invalidate();
                LOG_WARNING("failed to finalize distributed compaction worker batch")
                        .tag("job_id", _execution_id)
                        .tag("endpoint", endpoint)
                        .tag("tasks", task_indices.size())
                        .tag("preserve_output_files", preserve_output_files)
                        .error(status);
            }
        };
        auto token = _engine.distributed_compaction_rpc_thread_pool().new_token(
                ThreadPool::ExecutionMode::CONCURRENT, cast_set<int>(groups_by_worker.size()));
        for (const auto& [endpoint, task_indices] : groups_by_worker) {
            const Status submit_status = token->submit_func(
                    [&, endpoint, task_indices]() { finalize_endpoint(endpoint, task_indices); });
            if (!submit_status.ok()) {
                LOG_WARNING("failed to submit distributed compaction finalize task")
                        .tag("job_id", _execution_id)
                        .tag("endpoint", endpoint)
                        .error(submit_status);
                finalize_endpoint(endpoint, task_indices);
            }
        }
        token->wait();
        token->shutdown();
    }
    _state.reset();
}

DistributedCompactionWorker::DistributedCompactionWorker(CloudStorageEngine& engine,
                                                         std::shared_ptr<CloudTablet> tablet)
        : _engine(engine),
          _tablet(std::move(tablet)),
          _mem_tracker(MemTrackerLimiter::create_shared(
                  MemTrackerLimiter::Type::COMPACTION,
                  fmt::format("distributed-compaction-tablet-{}", _tablet->tablet_id()))),
          _runtime_state(RuntimeState::create_unique()) {}

DistributedCompactionWorker::~DistributedCompactionWorker() {
    SCOPED_INIT_THREAD_CONTEXT();
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_mem_tracker);
    _result.reset();
    reset_state();
}

Status DistributedCompactionWorker::execute_compaction(
        const PCloudDistributedCompactionSubmitRequest* request,
        const PCloudDistributedCompactionTask* task) {
    {
        std::lock_guard lock(_status_mutex);
        if (_state != State::PENDING) {
            DORIS_CHECK(_state == State::FAILED);
            return Status::Cancelled("distributed compaction task was cancelled before execution");
        }
        _state = State::RUNNING;
    }

    PCloudDistributedCompactionTaskResult result;
    const Status status = handle_compaction(request, task, &result);
    result.set_group_index(task->group_index());
    status.to_protobuf(result.mutable_status());
    {
        std::lock_guard lock(_status_mutex);
        DORIS_CHECK(_state == State::RUNNING);
        _state = status.ok() ? State::SUCCEEDED : State::FAILED;
        _result = std::make_unique<PCloudDistributedCompactionTaskResult>(std::move(result));
    }
    if (!status.ok()) {
        handle_finalize();
    }
    return status;
}

void DistributedCompactionWorker::cancel_compaction(int32_t group_index, const Status& status) {
    SCOPED_ATTACH_TASK(_mem_tracker);
    DORIS_CHECK(!status.ok());
    PCloudDistributedCompactionTaskResult result;
    result.set_group_index(group_index);
    status.to_protobuf(result.mutable_status());
    {
        std::lock_guard lock(_status_mutex);
        if (_state == State::PENDING) {
            _state = State::FAILED;
            _result = std::make_unique<PCloudDistributedCompactionTaskResult>(std::move(result));
            return;
        }
        if (_state != State::RUNNING) {
            return;
        }
    }
    if (_runtime_state->is_cancelled()) {
        return;
    }
    _runtime_state->cancel(status);
}

void DistributedCompactionWorker::get_compaction_status(
        PCloudDistributedCompactionTaskStatus* status) const {
    std::lock_guard lock(_status_mutex);
    switch (_state) {
    case State::PENDING:
        status->set_state(CLOUD_DISTRIBUTED_COMPACTION_TASK_PENDING);
        break;
    case State::RUNNING:
        status->set_state(CLOUD_DISTRIBUTED_COMPACTION_TASK_RUNNING);
        break;
    case State::SUCCEEDED:
        DORIS_CHECK(_result != nullptr);
        status->set_state(CLOUD_DISTRIBUTED_COMPACTION_TASK_SUCCEEDED);
        *status->mutable_result() = *_result;
        break;
    case State::FAILED:
        DORIS_CHECK(_result != nullptr);
        status->set_state(CLOUD_DISTRIBUTED_COMPACTION_TASK_FAILED);
        *status->mutable_result() = *_result;
        break;
    }
}

Result<std::unique_ptr<RowsetWriter>> DistributedCompactionWorker::construct_output_rowset_writer(
        const PCloudDistributedCompactionSubmitRequest& request,
        const PCloudDistributedCompactionTask& task, const RowsetMeta& output_meta,
        const StorageResource& storage_resource) {
    RowsetWriterContext context;
    context.rowset_id = output_meta.rowset_id();
    context.db_id = output_meta.db_id();
    context.table_id = output_meta.table_id();
    context.tablet_id = output_meta.tablet_id();
    context.tablet_schema_hash = output_meta.tablet_schema_hash();
    context.index_id = output_meta.index_id();
    context.partition_id = output_meta.partition_id();
    context.rowset_type = output_meta.rowset_type();
    context.tablet_schema = output_meta.tablet_schema();
    context.rowset_state = VISIBLE;
    context.version = output_meta.version();
    context.segments_overlap = NONOVERLAPPING;
    context.txn_id = output_meta.txn_id();
    context.txn_expiration = request.output_rowset_meta().txn_expiration();
    context.newest_write_timestamp = output_meta.newest_write_timestamp();
    context.enable_unique_key_merge_on_write = _tablet->enable_unique_key_merge_on_write();
    context.write_type = DataWriteType::TYPE_COMPACTION;
    context.compaction_type = request.compaction_type() == CLOUD_DISTRIBUTED_BASE_COMPACTION
                                      ? ReaderType::READER_BASE_COMPACTION
                                      : ReaderType::READER_CUMULATIVE_COMPACTION;
    context.tablet = _tablet;
    context.encrypt_algorithm = _tablet->tablet_meta()->encryption_algorithm();
    context.job_id = request.execution_id();
    context.allow_packed_file = false;
    context.is_partial_output_writer = true;
    context.storage_resource = storage_resource;

    auto writer_result =
            RowsetFactory::create_rowset_writer(_engine, context, request.is_vertical());
    if (!writer_result.has_value()) {
        return ResultError(std::move(writer_result.error()));
    }
    auto writer = std::move(writer_result).value();
    writer->set_segment_start_id(task.output_segment_start_id(), task.max_segment_num());
    return writer;
}

Status DistributedCompactionWorker::handle_compaction(
        const PCloudDistributedCompactionSubmitRequest* request,
        const PCloudDistributedCompactionTask* task,
        PCloudDistributedCompactionTaskResult* result) {
    SCOPED_ATTACH_TASK(_mem_tracker);
    std::lock_guard<std::mutex> lock(_mutex);
    if (_output_rowset != nullptr) {
        return Status::InvalidArgument(
                "duplicate distributed compaction task: execution={}, group={}",
                request->execution_id(), task->group_index());
    }

    const bool is_base = request->compaction_type() == CLOUD_DISTRIBUTED_BASE_COMPACTION;
    if (is_base) {
        const auto& schema = *_tablet->tablet_schema();
        const bool is_mow = _tablet->keys_type() == KeysType::UNIQUE_KEYS &&
                            _tablet->enable_unique_key_merge_on_write();
        const bool supported_keys_type = _tablet->keys_type() == KeysType::DUP_KEYS ||
                                         _tablet->keys_type() == KeysType::AGG_KEYS ||
                                         (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
                                          (!is_mow || schema.cluster_key_uids().empty()));
        if (!supported_keys_type || schema.num_key_columns() == 0 ||
            !is_supported_distributed_base_key(schema.column(0).type()) ||
            schema.column(0).is_nullable()) {
            return Status::InvalidArgument(
                    "distributed base compaction requires DUP_KEYS, AGG_KEYS, UNIQUE_KEYS MOR, "
                    "or non-cluster-key UNIQUE_KEYS MOW tablet with a non-null supported leading "
                    "key");
        }
    }
    std::vector<RowsetMetaPB> input_meta_pbs;
    if (is_base) {
        input_meta_pbs.assign(request->input_rowset_metas().begin(),
                              request->input_rowset_metas().end());
    } else {
        input_meta_pbs.push_back(request->input_rowset_meta());
    }
    std::vector<RowsetSharedPtr> input_rowsets;
    input_rowsets.reserve(input_meta_pbs.size());
    for (const auto& input_meta_pb : input_meta_pbs) {
        auto input_meta = std::make_shared<RowsetMeta>();
        if (!input_meta->init_from_pb(input_meta_pb)) {
            return Status::InvalidArgument("failed to initialize input rowset metadata");
        }
        if (input_meta->tablet_id() != request->tablet_id()) {
            return Status::InvalidArgument("input rowset tablet id does not match request");
        }
        RowsetSharedPtr input_rowset;
        RETURN_IF_ERROR(RowsetFactory::create_rowset(
                input_meta->tablet_schema(), _tablet->tablet_path(), input_meta, &input_rowset));
        input_rowsets.push_back(std::move(input_rowset));
    }

    std::optional<std::pair<int64_t, int64_t>> segment_range;
    std::optional<Merger::KeyRange> merge_key_range;
    if (is_base) {
        const auto& key_range = task->key_range();
        OlapTuple lower_key;
        OlapTuple upper_key;
        const auto& schema = *_tablet->tablet_schema();
        if (!extract_key_tuple(key_range.lower_key(), schema, &lower_key) ||
            !extract_key_tuple(key_range.upper_key(), schema, &upper_key)) {
            return Status::InvalidArgument(
                    "distributed base compaction key range does not match tablet key type");
        }
        merge_key_range = Merger::KeyRange {.lower_key = std::move(lower_key),
                                            .upper_key = std::move(upper_key),
                                            .lower_inclusive = key_range.lower_inclusive(),
                                            .upper_inclusive = key_range.upper_inclusive()};
    } else {
        segment_range = std::make_pair(task->segment_pos_start(), task->segment_pos_end());
        if (segment_range->second > input_rowsets.front()->num_segments()) {
            return Status::InvalidArgument(
                    "invalid input segment range: start={}, end={}, segments={}",
                    segment_range->first, segment_range->second,
                    input_rowsets.front()->num_segments());
        }
    }

    RowsetMeta output_meta;
    if (!output_meta.init_from_pb(request->output_rowset_meta())) {
        return Status::InvalidArgument("failed to initialize output rowset metadata");
    }
    if (output_meta.tablet_id() != request->tablet_id()) {
        return Status::InvalidArgument("output rowset tablet id does not match request");
    }
    auto storage_resource = output_meta.remote_storage_resource();
    if (!storage_resource) {
        return Status::InvalidArgument("output rowset has no remote storage resource: {}",
                                       storage_resource.error().to_string());
    }

    auto writer = DORIS_TRY(construct_output_rowset_writer(*request, *task, output_meta,
                                                           *storage_resource.value()));

    _is_mow = _tablet->keys_type() == KeysType::UNIQUE_KEYS &&
              _tablet->enable_unique_key_merge_on_write();
    Merger::Statistics stats;
    if (_is_mow) {
        _rowid_conversion = std::make_unique<RowIdConversion>();
        stats.rowid_conversion = _rowid_conversion.get();
    }

    RETURN_IF_CANCELLED(_runtime_state.get());
    std::vector<RowsetReaderSharedPtr> readers;
    readers.reserve(input_rowsets.size());
    for (const auto& input_rowset : input_rowsets) {
        RowsetReaderSharedPtr reader;
        RETURN_IF_ERROR(input_rowset->create_reader(&reader));
        readers.push_back(std::move(reader));
    }
    const ReaderType reader_type =
            is_base ? ReaderType::READER_BASE_COMPACTION : ReaderType::READER_CUMULATIVE_COMPACTION;
    if (request->is_vertical()) {
        RETURN_IF_ERROR(Merger::vertical_merge_rowsets(
                _tablet, reader_type, *output_meta.tablet_schema(), readers, writer.get(),
                request->avg_segment_rows(), task->merge_way_num(), &stats, nullptr, segment_range,
                _runtime_state.get(), merge_key_range));
    } else {
        RETURN_IF_ERROR(Merger::vmerge_rowsets(_tablet, reader_type, *output_meta.tablet_schema(),
                                               readers, writer.get(), &stats, segment_range,
                                               _runtime_state.get(), merge_key_range));
    }
    RETURN_IF_CANCELLED(_runtime_state.get());
    RETURN_IF_ERROR(writer->build(_output_rowset));
    RETURN_IF_CANCELLED(_runtime_state.get());

    _output_segment_ids.clear();
    _output_segment_ids.reserve(cast_set<size_t>(_output_rowset->num_segments()));
    for (const auto segment : _output_rowset->segments()) {
        const int64_t segment_id = segment.id();
        DORIS_CHECK_GE(segment_id, task->output_segment_start_id());
        DORIS_CHECK_LT(segment_id, cast_set<int64_t>(task->output_segment_start_id()) +
                                           task->max_segment_num());
        _output_segment_ids.push_back(segment_id);
    }
    DORIS_CHECK_EQ(_output_segment_ids.size(), cast_set<size_t>(_output_rowset->num_segments()));

    result->set_group_index(task->group_index());
    *result->mutable_partial_rowset_meta() = _output_rowset->rowset_meta()->get_rowset_pb();
    result->set_output_rows(stats.output_rows);
    result->set_merged_rows(stats.merged_rows);
    result->set_filtered_rows(stats.filtered_rows);
    result->set_bytes_read_from_local(stats.bytes_read_from_local);
    result->set_bytes_read_from_remote(stats.bytes_read_from_remote);
    result->set_cached_bytes_total(stats.cached_bytes_total);
    result->set_cloud_local_read_time(stats.cloud_local_read_time);
    result->set_cloud_remote_read_time(stats.cloud_remote_read_time);

    if (_is_mow) {
        RETURN_IF_CANCELLED(_runtime_state.get());
        RETURN_IF_ERROR(_engine.meta_mgr().sync_tablet_rowsets(_tablet.get()));
        DeleteBitmap shard(_tablet->tablet_id());
        std::unique_ptr<std::set<RowLocation>> missed_rows;
        if (request->check_missed_rows()) {
            missed_rows = std::make_unique<std::set<RowLocation>>();
        }
        _tablet->calc_compaction_output_rowset_delete_bitmap_by_segments(
                *_rowid_conversion, _output_rowset->rowset_id(), _output_segment_ids,
                request->delete_bitmap_start_version(), request->delete_bitmap_end_version(),
                _tablet->tablet_meta()->delete_bitmap(), &shard, missed_rows.get());
        RETURN_IF_CANCELLED(_runtime_state.get());
        *result->mutable_output_delete_bitmap_shard() = shard.to_pb();
        result->set_missed_rows_count(
                missed_rows == nullptr ? 0 : cast_set<int64_t>(missed_rows->size()));
    }
    return Status::OK();
}

Status DistributedCompactionWorker::calc_incremental_delete_bitmap(
        uint64_t start_version, uint64_t end_version, DeleteBitmap* output_delete_bitmap) {
    SCOPED_ATTACH_TASK(_mem_tracker);
    std::lock_guard<std::mutex> lock(_mutex);
    if (!_is_mow || _rowid_conversion == nullptr || _output_rowset == nullptr) {
        return Status::InvalidArgument(
                "incremental delete bitmap requested before a MoW group compaction");
    }
    _tablet->calc_compaction_output_rowset_delete_bitmap_by_segments(
            *_rowid_conversion, _output_rowset->rowset_id(), _output_segment_ids, start_version,
            end_version, _tablet->tablet_meta()->delete_bitmap(), output_delete_bitmap, nullptr);
    return Status::OK();
}

void DistributedCompactionWorker::handle_finalize() {
    SCOPED_ATTACH_TASK(_mem_tracker);
    std::lock_guard<std::mutex> lock(_mutex);
    // Remote files belong to the rowset prepared by the coordinator. Recycler reclaims them if
    // the compaction job is aborted, so the worker only releases its execution state here.
    reset_state();
}

void DistributedCompactionWorker::reset_state() {
    _rowid_conversion.reset();
    _output_rowset.reset();
    std::vector<int64_t>().swap(_output_segment_ids);
}

DistributedCompactionWorkerManager* DistributedCompactionWorkerManager::instance() {
    static DistributedCompactionWorkerManager manager;
    return &manager;
}

Status DistributedCompactionWorkerManager::submit(
        const PCloudDistributedCompactionSubmitRequest& request, CloudStorageEngine& engine) {
    struct CompactionJob {
        std::shared_ptr<DistributedCompactionWorker> worker;
        PCloudDistributedCompactionTask task;
    };
    struct CreatedWorker {
        std::string key;
        int32_t group_index;
    };

    if (request.tasks().empty()) {
        return Status::InvalidArgument("distributed compaction batch has no tasks");
    }
    auto tablet = DORIS_TRY(engine.tablet_mgr().get_tablet(request.tablet_id()));
    RETURN_IF_ERROR(validate_submit_request(request));
    std::unordered_set<std::string> request_keys;
    request_keys.reserve(cast_set<size_t>(request.tasks_size()));
    for (const auto& task : request.tasks()) {
        RETURN_IF_ERROR(validate_compaction_task(request, task));
        const std::string worker_key = key(request.execution_id(), task.group_index());
        if (!request_keys.emplace(worker_key).second) {
            return Status::InvalidArgument(
                    "duplicate distributed compaction task: execution={}, group={}",
                    request.execution_id(), task.group_index());
        }
    }

    remove_expired_workers(::time(nullptr));
    auto request_copy = std::make_shared<PCloudDistributedCompactionSubmitRequest>(request);
    std::vector<CompactionJob> jobs;
    std::vector<CreatedWorker> created_workers;
    // Keep rejected workers alive until _mutex is released because their destructors may clean
    // staging files.
    std::vector<std::shared_ptr<DistributedCompactionWorker>> workers_to_release;
    Status submit_status = Status::OK();
    {
        std::lock_guard lock(_mutex);
        for (const auto& task : request.tasks()) {
            const std::string worker_key = key(request.execution_id(), task.group_index());
            const auto iter = _workers.find(worker_key);
            if (iter != _workers.end()) {
                DORIS_CHECK_EQ(iter->second.expiration_time,
                               request.output_rowset_meta().txn_expiration());
                continue;
            }
            auto worker = std::make_shared<DistributedCompactionWorker>(engine, tablet);
            _workers.emplace(
                    worker_key,
                    WorkerEntry {.worker = worker,
                                 .expiration_time = request.output_rowset_meta().txn_expiration()});
            jobs.push_back({.worker = std::move(worker), .task = task});
            created_workers.push_back({.key = worker_key, .group_index = task.group_index()});
        }
        if (jobs.empty()) {
            return Status::OK();
        }
        submit_status = engine.distributed_compaction_worker_thread_pool().submit_func(
                [request_copy, jobs = std::move(jobs)]() {
                    bool batch_failed = false;
                    for (const auto& job : jobs) {
                        if (batch_failed) {
                            job.worker->cancel_compaction(
                                    job.task.group_index(),
                                    Status::Cancelled(
                                            "skipped after a previous task in the batch failed"));
                            continue;
                        }
                        if (request_copy->output_rowset_meta().txn_expiration() <=
                            ::time(nullptr)) {
                            job.worker->cancel_compaction(
                                    job.task.group_index(),
                                    Status::TimedOut("distributed compaction task expired before "
                                                     "execution"));
                            batch_failed = true;
                            continue;
                        }
                        batch_failed =
                                !job.worker->execute_compaction(request_copy.get(), &job.task).ok();
                    }
                });
        if (!submit_status.ok()) {
            const Status cancel_status =
                    Status::Cancelled("distributed compaction worker batch submission failed: {}",
                                      submit_status.to_string());
            for (const auto& created_worker : created_workers) {
                auto iter = _workers.find(created_worker.key);
                DORIS_CHECK(iter != _workers.end());
                iter->second.worker->cancel_compaction(created_worker.group_index, cancel_status);
                workers_to_release.push_back(std::move(iter->second.worker));
                _workers.erase(iter);
            }
        }
    }
    if (!submit_status.ok()) {
        return Status::TooManyTasks("failed to submit distributed compaction worker batch: {}",
                                    submit_status.to_string());
    }
    return Status::OK();
}

Status DistributedCompactionWorkerManager::calc_incremental_delete_bitmap(
        const PCloudDistributedCompactionCalcIncrementalDeleteBitmapRequest& request,
        PCloudDistributedCompactionCalcIncrementalDeleteBitmapResponse* response) {
    if (request.tablet_id() <= 0 || request.execution_id().empty() || request.tasks().empty() ||
        request.delete_bitmap_start_version() >= request.delete_bitmap_end_version()) {
        return Status::InvalidArgument(
                "invalid distributed compaction incremental delete bitmap request");
    }

    std::unordered_set<std::string> request_keys;
    request_keys.reserve(cast_set<size_t>(request.tasks_size()));
    std::vector<std::shared_ptr<DistributedCompactionWorker>> workers;
    workers.reserve(cast_set<size_t>(request.tasks_size()));
    std::shared_ptr<CloudTablet> tablet;
    CloudStorageEngine* engine = nullptr;
    for (const auto& task : request.tasks()) {
        if (task.group_index() < 0) {
            return Status::InvalidArgument(
                    "invalid distributed compaction incremental delete bitmap task");
        }
        const std::string worker_key = key(request.execution_id(), task.group_index());
        if (!request_keys.emplace(worker_key).second) {
            return Status::InvalidArgument(
                    "duplicate distributed compaction incremental delete bitmap task: "
                    "execution={}, group={}",
                    request.execution_id(), task.group_index());
        }
        auto worker = get(request.execution_id(), task.group_index());
        if (worker == nullptr) {
            return Status::NotFound(
                    "distributed compaction worker state not found: "
                    "execution={}, group={}",
                    request.execution_id(), task.group_index());
        }
        if (worker->_tablet->tablet_id() != request.tablet_id()) {
            return Status::InvalidArgument(
                    "distributed compaction incremental delete bitmap tablet mismatch: "
                    "execution={}, group={}, requested_tablet={}, worker_tablet={}",
                    request.execution_id(), task.group_index(), request.tablet_id(),
                    worker->_tablet->tablet_id());
        }
        if (tablet == nullptr) {
            tablet = worker->_tablet;
            engine = &worker->_engine;
        } else {
            DORIS_CHECK_EQ(tablet.get(), worker->_tablet.get());
            DORIS_CHECK_EQ(engine, &worker->_engine);
        }
        workers.push_back(std::move(worker));
    }

    DORIS_CHECK(tablet != nullptr);
    DORIS_CHECK(engine != nullptr);
    RETURN_IF_ERROR(engine->meta_mgr().sync_tablet_rowsets(tablet.get()));
    DeleteBitmap shard(request.tablet_id());
    for (const auto& worker : workers) {
        RETURN_IF_ERROR(worker->calc_incremental_delete_bitmap(
                request.delete_bitmap_start_version(), request.delete_bitmap_end_version(),
                &shard));
    }
    *response->mutable_output_delete_bitmap_shard() = shard.to_pb();
    return Status::OK();
}

Status DistributedCompactionWorkerManager::finalize(
        const PCloudDistributedCompactionFinalizeRequest& request) {
    if (request.execution_id().empty() || request.tasks().empty() || !request.has_mode()) {
        return Status::InvalidArgument("invalid distributed compaction finalize request");
    }

    struct WorkerTask {
        int32_t group_index;
        std::shared_ptr<DistributedCompactionWorker> worker;
    };
    std::vector<WorkerTask> worker_tasks;
    worker_tasks.reserve(cast_set<size_t>(request.tasks_size()));
    for (const auto& task : request.tasks()) {
        if (task.group_index() < 0) {
            return Status::InvalidArgument("invalid distributed compaction finalize task");
        }
        auto worker = get(request.execution_id(), task.group_index());
        if (worker != nullptr) {
            worker_tasks.push_back(
                    {.group_index = task.group_index(), .worker = std::move(worker)});
        }
    }
    if (worker_tasks.empty()) {
        return Status::OK();
    }

    const bool preserve_output_files =
            request.mode() == CLOUD_DISTRIBUTED_COMPACTION_PRESERVE_OUTPUT_FILES;
    if (!preserve_output_files) {
        const Status cancel_status =
                Status::Cancelled("distributed compaction batch was cancelled by coordinator");
        for (const auto& task : worker_tasks) {
            task.worker->cancel_compaction(task.group_index, cancel_status);
        }
    }

    for (const auto& task : worker_tasks) {
        task.worker->handle_finalize();
    }
    {
        std::lock_guard lock(_mutex);
        for (const auto& task : worker_tasks) {
            const auto iter = _workers.find(key(request.execution_id(), task.group_index));
            if (iter == _workers.end()) {
                continue;
            }
            DORIS_CHECK(iter->second.worker == task.worker);
            _workers.erase(iter);
        }
    }
    return Status::OK();
}

std::string DistributedCompactionWorkerManager::key(const std::string& execution_id,
                                                    int32_t group_index) {
    return execution_id + ":" + std::to_string(group_index);
}

std::shared_ptr<DistributedCompactionWorker> DistributedCompactionWorkerManager::get(
        const std::string& execution_id, int32_t group_index) {
    std::shared_ptr<DistributedCompactionWorker> worker;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        const auto iter = _workers.find(key(execution_id, group_index));
        if (iter == _workers.end()) {
            return nullptr;
        }
        if (iter->second.expiration_time <= ::time(nullptr)) {
            worker = std::move(iter->second.worker);
            _workers.erase(iter);
        } else {
            return iter->second.worker;
        }
    }
    LOG_INFO("remove expired distributed compaction worker")
            .tag("execution_id", execution_id)
            .tag("group_index", group_index);
    worker.reset();
    return nullptr;
}

void DistributedCompactionWorkerManager::remove_expired_workers(int64_t current_time) {
    std::vector<std::shared_ptr<DistributedCompactionWorker>> expired_workers;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        for (auto iter = _workers.begin(); iter != _workers.end();) {
            if (iter->second.expiration_time > current_time) {
                ++iter;
                continue;
            }
            expired_workers.push_back(std::move(iter->second.worker));
            iter = _workers.erase(iter);
        }
    }
    if (!expired_workers.empty()) {
        LOG_INFO("remove expired distributed compaction workers")
                .tag("worker_count", expired_workers.size());
    }
}

} // namespace doris::cloud
