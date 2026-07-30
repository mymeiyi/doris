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

#include "cloud/cloud_distributed_single_rowset_compaction.h"

#include <brpc/controller.h>
#include <gen_cpp/internal_service.pb.h>

#include <charconv>
#include <ctime>
#include <limits>
#include <set>
#include <system_error>
#include <unordered_set>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/config.h"
#include "common/cast_set.h"
#include "common/check.h"
#include "runtime/exec_env.h"
#include "storage/merger.h"
#include "storage/rowid_conversion.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_policy.h"
#include "storage/tablet/tablet_meta.h"
#include "util/brpc_client_cache.h"

namespace doris::cloud {

Status build_single_rowset_segment_id_slots(
        int32_t base_segment_id, int32_t slot_capacity, size_t group_count,
        std::vector<SingleRowsetSegmentIdSlot>* slots) {
    if (base_segment_id < 0 || slot_capacity <= 0) {
        return Status::InvalidArgument(
                "invalid single-rowset segment slot: base={}, capacity={}", base_segment_id,
                slot_capacity);
    }
    const int64_t end = cast_set<int64_t>(base_segment_id) +
                        cast_set<int64_t>(slot_capacity) * cast_set<int64_t>(group_count);
    if (end > std::numeric_limits<int32_t>::max()) {
        return Status::InvalidArgument(
                "single-rowset segment slots overflow: base={}, capacity={}, groups={}",
                base_segment_id, slot_capacity, group_count);
    }
    slots->clear();
    slots->reserve(group_count);
    for (size_t group_index = 0; group_index < group_count; ++group_index) {
        const int64_t start = cast_set<int64_t>(base_segment_id) +
                              cast_set<int64_t>(slot_capacity) *
                                      cast_set<int64_t>(group_index);
        slots->push_back({.start_id = cast_set<int32_t>(start), .capacity = slot_capacity});
    }
    return Status::OK();
}

Status parse_single_rowset_compaction_workers(std::string_view worker_config,
                                              std::string_view local_endpoint,
                                              std::vector<std::string>* workers) {
    workers->clear();
    std::unordered_set<std::string> unique_workers;
    size_t token_start = 0;
    while (token_start < worker_config.size()) {
        const size_t comma = worker_config.find(',', token_start);
        std::string_view token = worker_config.substr(
                token_start, comma == std::string_view::npos ? comma : comma - token_start);
        const size_t begin = token.find_first_not_of(" \t");
        if (begin != std::string_view::npos) {
            const size_t end = token.find_last_not_of(" \t");
            std::string endpoint(token.substr(begin, end - begin + 1));
            if (endpoint != local_endpoint && unique_workers.emplace(endpoint).second) {
                workers->push_back(std::move(endpoint));
            }
        }
        if (comma == std::string_view::npos) {
            break;
        }
        token_start = comma + 1;
    }
    return Status::OK();
}

namespace {

Status parse_endpoint(std::string_view endpoint, std::string* host, int* port) {
    const size_t colon = endpoint.rfind(':');
    if (colon == std::string_view::npos || colon == 0 || colon + 1 == endpoint.size()) {
        return Status::InvalidArgument("invalid single-rowset compaction worker endpoint: {}",
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
        return Status::InvalidArgument("invalid single-rowset compaction worker endpoint: {}",
                                       endpoint);
    }
    host->assign(host_view);
    *port = parsed_port;
    return Status::OK();
}

template <typename Request, typename Response, typename Method>
Status call_worker(const std::string& endpoint, const Request& request, Response* response,
                   Method method, std::string_view rpc_name) {
    std::string host;
    int port = 0;
    RETURN_IF_ERROR(parse_endpoint(endpoint, &host, &port));
    auto stub = ExecEnv::GetInstance()->brpc_internal_client_cache()->get_client(host, port);
    if (stub == nullptr) {
        return Status::RpcError("failed to get brpc stub for {} ({})", endpoint, rpc_name);
    }
    brpc::Controller controller;
    controller.set_timeout_ms(config::cloud_single_rowset_compaction_rpc_timeout_ms);
    (stub.get()->*method)(&controller, &request, response, nullptr);
    if (controller.Failed()) {
        return Status::RpcError("{} rpc to {} failed: {}", rpc_name, endpoint,
                                controller.ErrorText());
    }
    return Status::create(response->status());
}

Status validate_task_request(const PCloudSingleRowsetCompactionRequest& request) {
    if (!request.has_input_rowset_meta() || !request.has_output_rowset_meta() ||
        request.execution_id().empty() || request.output_rowset_id().empty() ||
        request.segment_pos_start() < 0 ||
        request.segment_pos_start() >= request.segment_pos_end() ||
        request.max_segment_num() <= 0 || request.output_segment_start_id() < 0 ||
        request.group_index() < 0 || request.attempt_id() < 0 ||
        request.cloud_unique_id() != config::cloud_unique_id ||
        request.compaction_type() !=
                static_cast<int32_t>(ReaderType::READER_CUMULATIVE_COMPACTION) ||
        cast_set<int64_t>(request.output_segment_start_id()) + request.max_segment_num() >
                std::numeric_limits<int32_t>::max()) {
        return Status::InvalidArgument("invalid distributed single-rowset compaction request");
    }
    return Status::OK();
}

} // namespace

Status single_rowset_compaction_rpc(const std::string& endpoint,
                                    const PCloudSingleRowsetCompactionRequest& request,
                                    PCloudSingleRowsetCompactionResponse* response) {
    return call_worker(endpoint, request, response,
                       &PBackendService_Stub::cloud_single_rowset_compaction,
                       "cloud_single_rowset_compaction");
}

Status single_rowset_compaction_incremental_rpc(
        const std::string& endpoint,
        const PCloudSingleRowsetCompactionIncrementalRequest& request,
        PCloudSingleRowsetCompactionIncrementalResponse* response) {
    return call_worker(endpoint, request, response,
                       &PBackendService_Stub::cloud_single_rowset_compaction_incremental,
                       "cloud_single_rowset_compaction_incremental");
}

Status single_rowset_compaction_finish_rpc(
        const std::string& endpoint, const PCloudSingleRowsetCompactionFinishRequest& request,
        PCloudSingleRowsetCompactionFinishResponse* response) {
    return call_worker(endpoint, request, response,
                       &PBackendService_Stub::cloud_single_rowset_compaction_finish,
                       "cloud_single_rowset_compaction_finish");
}

DistributedSingleRowsetCompactionWorker::DistributedSingleRowsetCompactionWorker(
        CloudStorageEngine& engine, std::shared_ptr<CloudTablet> tablet)
        : _engine(engine), _tablet(std::move(tablet)) {}

DistributedSingleRowsetCompactionWorker::~DistributedSingleRowsetCompactionWorker() {
    if (_partial_rowset != nullptr) {
        WARN_IF_ERROR(_partial_rowset->remove(),
                      "failed to clean distributed single-rowset local staging rowset");
    }
}

Status DistributedSingleRowsetCompactionWorker::handle_compaction(
        const PCloudSingleRowsetCompactionRequest* request,
        PCloudSingleRowsetCompactionResponse* response) {
    std::lock_guard<std::mutex> lock(_mutex);
    RETURN_IF_ERROR(validate_task_request(*request));
    if (_partial_rowset != nullptr) {
        return Status::InvalidArgument(
                "duplicate distributed single-rowset compaction task: execution={}, group={}, "
                "attempt={}",
                request->execution_id(), request->group_index(), request->attempt_id());
    }

    auto input_meta = std::make_shared<RowsetMeta>();
    if (!input_meta->init_from_pb(request->input_rowset_meta())) {
        return Status::InvalidArgument("failed to initialize input rowset metadata");
    }
    if (input_meta->tablet_id() != request->tablet_id()) {
        return Status::InvalidArgument("input rowset tablet id does not match request");
    }
    RowsetSharedPtr input_rowset;
    RETURN_IF_ERROR(RowsetFactory::create_rowset(input_meta->tablet_schema(),
                                                 _tablet->tablet_path(), input_meta,
                                                 &input_rowset));

    const int64_t segment_pos_start = request->segment_pos_start();
    const int64_t segment_pos_end = request->segment_pos_end();
    if (segment_pos_end > input_rowset->num_segments() ||
        request->input_segment_ids_size() != segment_pos_end - segment_pos_start) {
        return Status::InvalidArgument(
                "invalid input segment range: start={}, end={}, segments={}, physical_ids={}",
                segment_pos_start, segment_pos_end, input_rowset->num_segments(),
                request->input_segment_ids_size());
    }
    for (int64_t pos = segment_pos_start; pos < segment_pos_end; ++pos) {
        if (input_rowset->segment(cast_set<size_t>(pos)).id() !=
            request->input_segment_ids(cast_set<int>(pos - segment_pos_start))) {
            return Status::InvalidArgument(
                    "input physical segment id does not match position: position={}", pos);
        }
    }

    RowsetMeta output_meta;
    if (!output_meta.init_from_pb(request->output_rowset_meta())) {
        return Status::InvalidArgument("failed to initialize output rowset metadata");
    }
    if (output_meta.tablet_id() != request->tablet_id()) {
        return Status::InvalidArgument("output rowset tablet id does not match request");
    }
    const bool tablet_is_mow = _tablet->keys_type() == KeysType::UNIQUE_KEYS &&
                               _tablet->enable_unique_key_merge_on_write();
    if (request->is_mow() != tablet_is_mow) {
        return Status::InvalidArgument("request MoW mode does not match tablet");
    }
    RowsetId output_rowset_id;
    output_rowset_id.init(request->output_rowset_id());
    if (output_meta.rowset_id() != output_rowset_id) {
        return Status::InvalidArgument("output rowset id does not match prepared metadata");
    }
    auto storage_resource = output_meta.remote_storage_resource();
    if (!storage_resource) {
        return Status::InvalidArgument("output rowset has no remote storage resource: {}",
                                       storage_resource.error().to_string());
    }

    RowsetWriterContext context;
    context.rowset_id = output_rowset_id;
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
    context.txn_expiration = request->output_rowset_meta().txn_expiration();
    context.newest_write_timestamp = output_meta.newest_write_timestamp();
    context.enable_unique_key_merge_on_write = _tablet->enable_unique_key_merge_on_write();
    context.write_type = DataWriteType::TYPE_COMPACTION;
    context.compaction_type = static_cast<ReaderType>(request->compaction_type());
    context.tablet = _tablet;
    context.encrypt_algorithm = _tablet->tablet_meta()->encryption_algorithm();
    context.job_id = request->execution_id();
    context.allow_packed_file = false;
    context.is_partial_output_writer = true;

    auto writer =
            DORIS_TRY(RowsetFactory::create_rowset_writer(_engine, context, request->is_vertical()));
    writer->set_segment_start_id(request->output_segment_start_id(),
                                 request->max_segment_num());

    _is_mow = request->is_mow();
    Merger::Statistics stats;
    if (_is_mow) {
        std::vector<uint32_t> segment_rows;
        input_rowset->get_num_segment_rows(&segment_rows);
        DORIS_CHECK_EQ(segment_rows.size(), cast_set<size_t>(input_rowset->num_segments()));
        std::vector<SegmentRowIdRange> source_ranges;
        source_ranges.reserve(cast_set<size_t>(segment_pos_end - segment_pos_start));
        for (int64_t pos = segment_pos_start; pos < segment_pos_end; ++pos) {
            source_ranges.push_back(
                    {.rowset_id = input_rowset->rowset_id(),
                     .segment_id = cast_set<uint32_t>(
                             input_rowset->segment(cast_set<size_t>(pos)).id()),
                     .begin = 0,
                     .end = segment_rows[cast_set<size_t>(pos)]});
        }
        _rowid_conversion = std::make_unique<RowIdConversion>();
        RETURN_IF_ERROR(_rowid_conversion->init_segment_ranges(source_ranges));
        stats.rowid_conversion = _rowid_conversion.get();
    }

    RowsetReaderSharedPtr reader;
    RETURN_IF_ERROR(input_rowset->create_reader(&reader));
    std::vector<RowsetReaderSharedPtr> readers;
    readers.push_back(std::move(reader));
    const auto segment_range = std::make_pair(segment_pos_start, segment_pos_end);
    if (request->is_vertical()) {
        RETURN_IF_ERROR(Merger::vertical_merge_rowsets(
                _tablet, static_cast<ReaderType>(request->compaction_type()),
                *context.tablet_schema, readers, writer.get(), request->avg_segment_rows(),
                request->merge_way_num(), &stats, nullptr, segment_range));
    } else {
        RETURN_IF_ERROR(Merger::vmerge_rowsets(
                _tablet, static_cast<ReaderType>(request->compaction_type()),
                *context.tablet_schema, readers, writer.get(), &stats, segment_range));
    }
    RETURN_IF_ERROR(writer->build(_partial_rowset));

    _output_segment_ids.clear();
    _output_segment_ids.reserve(cast_set<size_t>(_partial_rowset->num_segments()));
    for (const auto segment : _partial_rowset->segments()) {
        const int64_t segment_id = segment.id();
        DORIS_CHECK_GE(segment_id, request->output_segment_start_id());
        DORIS_CHECK_LT(segment_id, cast_set<int64_t>(request->output_segment_start_id()) +
                                           request->max_segment_num());
        _output_segment_ids.push_back(segment_id);
    }
    DORIS_CHECK_EQ(_output_segment_ids.size(),
                   cast_set<size_t>(_partial_rowset->num_segments()));

    _remote_rowset_meta = std::make_shared<RowsetMeta>();
    DORIS_CHECK(
            _remote_rowset_meta->init_from_pb(_partial_rowset->rowset_meta()->get_rowset_pb()));
    _remote_rowset_meta->set_remote_storage_resource(*storage_resource.value());
    RETURN_IF_ERROR(static_cast<BetaRowset*>(_partial_rowset.get())
                            ->upload_files_to(*storage_resource.value(), output_rowset_id,
                                              _output_segment_ids));

    response->set_execution_id(request->execution_id());
    response->set_attempt_id(request->attempt_id());
    response->set_group_index(request->group_index());
    *response->mutable_partial_rowset_meta() = _remote_rowset_meta->get_rowset_pb();
    response->set_output_rows(stats.output_rows);
    response->set_merged_rows(stats.merged_rows);
    response->set_filtered_rows(stats.filtered_rows);
    response->set_bytes_read_from_local(stats.bytes_read_from_local);
    response->set_bytes_read_from_remote(stats.bytes_read_from_remote);
    response->set_cached_bytes_total(stats.cached_bytes_total);
    response->set_cloud_local_read_time(stats.cloud_local_read_time);
    response->set_cloud_remote_read_time(stats.cloud_remote_read_time);

    if (_is_mow) {
        RETURN_IF_ERROR(_engine.meta_mgr().sync_tablet_rowsets(_tablet.get()));
        DeleteBitmap shard(_tablet->tablet_id());
        std::unique_ptr<std::set<RowLocation>> missed_rows;
        if (request->check_missed_rows()) {
            missed_rows = std::make_unique<std::set<RowLocation>>();
        }
        _tablet->calc_compaction_output_rowset_delete_bitmap_by_ranges(
                *_rowid_conversion, output_rowset_id, _output_segment_ids,
                request->delete_bitmap_start_version(), request->delete_bitmap_end_version(),
                _tablet->tablet_meta()->delete_bitmap(), &shard, missed_rows.get());
        *response->mutable_output_delete_bitmap_shard() = shard.to_pb();
        response->set_missed_rows_count(
                missed_rows == nullptr ? 0 : cast_set<int64_t>(missed_rows->size()));
    }
    return Status::OK();
}

Status DistributedSingleRowsetCompactionWorker::handle_incremental(
        const PCloudSingleRowsetCompactionIncrementalRequest* request,
        PCloudSingleRowsetCompactionIncrementalResponse* response) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (!_is_mow || _rowid_conversion == nullptr || _partial_rowset == nullptr) {
        return Status::InvalidArgument(
                "incremental delete bitmap requested before a MoW group compaction");
    }
    RETURN_IF_ERROR(_engine.meta_mgr().sync_tablet_rowsets(_tablet.get()));
    DeleteBitmap shard(_tablet->tablet_id());
    _tablet->calc_compaction_output_rowset_delete_bitmap_by_ranges(
            *_rowid_conversion, _partial_rowset->rowset_id(), _output_segment_ids,
            request->delete_bitmap_start_version(), request->delete_bitmap_end_version(),
            _tablet->tablet_meta()->delete_bitmap(), &shard, nullptr);
    *response->mutable_output_delete_bitmap_shard() = shard.to_pb();
    return Status::OK();
}

Status DistributedSingleRowsetCompactionWorker::handle_finish(
        const PCloudSingleRowsetCompactionFinishRequest* request) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (!request->keep_output_files()) {
        RETURN_IF_ERROR(cleanup_output_files());
    }
    if (_partial_rowset != nullptr) {
        RETURN_IF_ERROR(_partial_rowset->remove());
        _partial_rowset.reset();
    }
    _remote_rowset_meta.reset();
    return Status::OK();
}

Status DistributedSingleRowsetCompactionWorker::cleanup_output_files() {
    if (_remote_rowset_meta == nullptr) {
        return Status::OK();
    }
    const auto& rowset_meta = _remote_rowset_meta;
    auto storage_resource = rowset_meta->remote_storage_resource();
    if (!storage_resource) {
        return storage_resource.error();
    }
    std::vector<io::Path> paths;
    const auto& schema = rowset_meta->tablet_schema();
    for (const int64_t segment_id : _output_segment_ids) {
        paths.emplace_back(
                storage_resource.value()->remote_segment_path(*rowset_meta, segment_id));
        if (schema->get_inverted_index_storage_format() == InvertedIndexStorageFormatPB::V1) {
            for (const auto& index : schema->inverted_indexes()) {
                paths.emplace_back(storage_resource.value()->remote_idx_v1_path(
                        *rowset_meta, segment_id, index->index_id(),
                        index->get_index_suffix()));
            }
        } else if (schema->has_inverted_index() || schema->has_ann_index()) {
            paths.emplace_back(
                    storage_resource.value()->remote_idx_v2_path(*rowset_meta, segment_id));
        }
    }
    if (paths.empty()) {
        _remote_rowset_meta.reset();
        return Status::OK();
    }
    RETURN_IF_ERROR(storage_resource.value()->fs->batch_delete(paths));
    _remote_rowset_meta.reset();
    return Status::OK();
}

DistributedSingleRowsetCompactionWorkerManager*
DistributedSingleRowsetCompactionWorkerManager::instance() {
    static DistributedSingleRowsetCompactionWorkerManager manager;
    return &manager;
}

std::string DistributedSingleRowsetCompactionWorkerManager::key(
        const std::string& execution_id, int32_t group_index, int32_t attempt_id) {
    return execution_id + ":" + std::to_string(group_index) + ":" + std::to_string(attempt_id);
}

std::shared_ptr<DistributedSingleRowsetCompactionWorker>
DistributedSingleRowsetCompactionWorkerManager::get_or_create(
        const std::string& execution_id, int32_t group_index, int32_t attempt_id,
        int64_t expiration_time, CloudStorageEngine& engine,
        std::shared_ptr<CloudTablet> tablet, bool* created) {
    DORIS_CHECK(created != nullptr);
    remove_expired_workers(::time(nullptr));
    std::lock_guard<std::mutex> lock(_mutex);
    const std::string worker_key = key(execution_id, group_index, attempt_id);
    const auto iter = _workers.find(worker_key);
    if (iter != _workers.end()) {
        DORIS_CHECK_EQ(iter->second.expiration_time, expiration_time);
        *created = false;
        return iter->second.worker;
    }
    auto worker =
            std::make_shared<DistributedSingleRowsetCompactionWorker>(engine, std::move(tablet));
    _workers.emplace(worker_key,
                     WorkerEntry {.worker = worker, .expiration_time = expiration_time});
    *created = true;
    return worker;
}

std::shared_ptr<DistributedSingleRowsetCompactionWorker>
DistributedSingleRowsetCompactionWorkerManager::get(const std::string& execution_id,
                                                     int32_t group_index, int32_t attempt_id) {
    std::shared_ptr<DistributedSingleRowsetCompactionWorker> worker;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        const auto iter = _workers.find(key(execution_id, group_index, attempt_id));
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
    LOG_INFO("remove expired distributed single-rowset compaction worker")
            .tag("execution_id", execution_id)
            .tag("group_index", group_index)
            .tag("attempt_id", attempt_id);
    worker.reset();
    return nullptr;
}

void DistributedSingleRowsetCompactionWorkerManager::erase(
        const std::string& execution_id, int32_t group_index, int32_t attempt_id) {
    std::shared_ptr<DistributedSingleRowsetCompactionWorker> worker;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        const auto iter = _workers.find(key(execution_id, group_index, attempt_id));
        if (iter == _workers.end()) {
            return;
        }
        worker = std::move(iter->second.worker);
        _workers.erase(iter);
    }
    worker.reset();
}

size_t DistributedSingleRowsetCompactionWorkerManager::remove_expired_workers(
        int64_t current_time) {
    std::vector<std::shared_ptr<DistributedSingleRowsetCompactionWorker>> expired_workers;
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
        LOG_INFO("remove expired distributed single-rowset compaction workers")
                .tag("worker_count", expired_workers.size());
    }
    return expired_workers.size();
}

} // namespace doris::cloud
