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

#include "load/delta_writer/delta_writer_v2.h"

#include <brpc/controller.h>
#include <butil/errno.h>
#include <fmt/format.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include <algorithm>
#include <filesystem>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "core/block/block.h"
#include "exec/sink/load_stream_stub.h"
#include "io/fs/file_writer.h" // IWYU pragma: keep
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "service/backend_options.h"
#include "storage/data_dir.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/olap_define.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/beta_rowset_writer_v2.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/schema.h"
#include "storage/schema_change/schema_change.h"
#include "storage/segment/segment.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/tablet/tablet_schema_cache.h"
#include "storage/tablet_info.h"
#include "util/brpc_client_cache.h"
#include "util/brpc_closure.h"
#include "util/debug_points.h"
#include "util/mem_info.h"
#include "util/stopwatch.hpp"
#include "util/time.h"

namespace doris {
using namespace ErrorCode;

DeltaWriterV2::DeltaWriterV2(WriteRequest* req,
                             const std::vector<std::shared_ptr<LoadStreamStub>>& streams,
                             std::shared_ptr<WorkloadGroup> workload_group)
        : _req(*req),
          _workload_group(std::move(workload_group)),
          _tablet_schema(new TabletSchema),
          _streams(streams) {}

void DeltaWriterV2::_update_profile(RuntimeProfile* profile) {
    auto child = profile->create_child(fmt::format("DeltaWriterV2 {}", _req.tablet_id), true, true);
    auto write_memtable_timer = ADD_TIMER(child, "WriteMemTableTime");
    auto wait_flush_limit_timer = ADD_TIMER(child, "WaitFlushLimitTime");
    auto close_wait_timer = ADD_TIMER(child, "CloseWaitTime");
    COUNTER_SET(write_memtable_timer, _write_memtable_time);
    COUNTER_SET(wait_flush_limit_timer, _wait_flush_limit_time);
    COUNTER_SET(close_wait_timer, _close_wait_time);
}

DeltaWriterV2::~DeltaWriterV2() {
    if (!_is_init) {
        return;
    }

    // cancel and wait all memtables in flush queue to be finished
    for (auto& sub_writer : _sub_writers) {
        static_cast<void>(sub_writer->cancel());
    }
}

Status DeltaWriterV2::init() {
    if (_is_init) {
        return Status::OK();
    }
    // build tablet schema in request level
    DBUG_EXECUTE_IF("DeltaWriterV2.init.stream_size", { _streams.clear(); });
    if (_streams.size() == 0 || _streams[0]->tablet_schema(_req.index_id) == nullptr) {
        return Status::InternalError("failed to find tablet schema for {}", _req.index_id);
    }
    RETURN_IF_ERROR(_build_current_tablet_schema(_req.index_id, _req.table_schema_param.get(),
                                                 *_streams[0]->tablet_schema(_req.index_id)));
    RowsetWriterContext context;
    context.txn_id = _req.txn_id;
    context.load_id = _req.load_id;
    context.index_id = _req.index_id;
    context.partition_id = _req.partition_id;
    context.rowset_state = PREPARED;
    context.segments_overlap = OVERLAPPING;
    context.tablet_schema = _tablet_schema;
    context.db_id = _tablet_schema->db_id();
    context.table_id = _tablet_schema->table_id();
    context.newest_write_timestamp = UnixSeconds();
    context.tablet = nullptr;
    context.write_type = DataWriteType::TYPE_DIRECT;
    context.tablet_id = _req.tablet_id;
    context.partition_id = _req.partition_id;
    context.tablet_schema_hash = _req.schema_hash;
    context.enable_unique_key_merge_on_write = _streams[0]->enable_unique_mow(_req.index_id);
    context.rowset_type = RowsetTypePB::BETA_ROWSET;
    context.rowset_id = ExecEnv::GetInstance()->storage_engine().next_rowset_id();
    context.data_dir = nullptr;
    context.partial_update_info = _partial_update_info;
    context.memtable_on_sink_support_index_v2 = true;
    context.encrypt_algorithm = EncryptionAlgorithmPB::PLAINTEXT;

    bool enable_mow = _streams[0]->enable_unique_mow(_req.index_id);

    // Phase 1: fan out into K sub-writers sharing one rowset writer. K comes from the
    // load session var (cloud-only, set in the sink). Be conservative for partial
    // update for now -> fall back to a single writer.
    // Clamp to a sane range so a misconfigured session var cannot spawn an unbounded
    // number of memtable writers / flush tokens for one tablet.
    constexpr int kMaxSubWriterCount = 64;
    _sub_writer_count = std::clamp(_req.sub_writer_count, 1, kMaxSubWriterCount);
    if (_req.table_schema_param != nullptr && _req.table_schema_param->is_partial_update()) {
        _sub_writer_count = 1;
    }
    bool shared_rowset_writer = _sub_writer_count > 1;

    _rowset_writer = std::make_shared<BetaRowsetWriterV2>(_streams);
    RETURN_IF_ERROR(_rowset_writer->init(context));

    _sub_writers.reserve(_sub_writer_count);
    for (int i = 0; i < _sub_writer_count; ++i) {
        auto sub_writer = std::make_shared<MemTableWriter>(_req);
        RETURN_IF_ERROR(sub_writer->init(_rowset_writer, _tablet_schema, _partial_update_info,
                                         _workload_group, enable_mow, shared_rowset_writer,
                                         _sub_writer_count));
        ExecEnv::GetInstance()->memtable_memory_limiter()->register_writer(sub_writer);
        _sub_writers.push_back(std::move(sub_writer));
    }

    if (_sub_writer_count > 1) {
        // Hash on key columns -> same key always lands in the same sub-writer, so MoW
        // dedup / sequence "latest-wins" stays local and cross-shard segments are
        // key-disjoint.
        auto partitioner = std::make_unique<HashPartitioner>(_sub_writer_count);
        RETURN_IF_ERROR(partitioner->init(_req.slots, _req.tuple_desc, _tablet_schema));
        _partitioner = std::move(partitioner);
    }

    _is_init = true;
    _streams.clear();
    return Status::OK();
}

Status DeltaWriterV2::write(const Block* block, const DorisVector<uint32_t>& row_idxs,
                            const std::function<Status()>& cancel_check) {
    if (UNLIKELY(row_idxs.empty())) {
        return Status::OK();
    }
    _lock_watch.start();
    std::lock_guard<std::mutex> l(_lock);
    _lock_watch.stop();
    if (!_is_init && !_is_cancelled) {
        RETURN_IF_ERROR(init());
    }
    {
        SCOPED_RAW_TIMER(&_wait_flush_limit_time);
        auto memtable_flush_running_count_limit = config::memtable_flush_running_count_limit;
        DBUG_EXECUTE_IF("DeltaWriterV2.write.back_pressure",
                        { std::this_thread::sleep_for(std::chrono::milliseconds(10 * 1000)); });
        // Back-pressure on the busiest sub-writer.
        auto max_flush_running_count = [&]() {
            uint64_t maxc = 0;
            for (auto& sub_writer : _sub_writers) {
                maxc = std::max(maxc, sub_writer->flush_running_count());
            }
            return maxc;
        };
        while (max_flush_running_count() >= memtable_flush_running_count_limit) {
            DBUG_EXECUTE_IF("DeltaWriterV2.write.flush_limit_wait", DBUG_RUN_CALLBACK());
            RETURN_IF_ERROR(cancel_check());
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    SCOPED_RAW_TIMER(&_write_memtable_time);
    if (_sub_writer_count == 1) {
        return _sub_writers[0]->write(block, row_idxs);
    }
    // Scatter the rows of this block across the K sub-writers by hash(key).
    _partitioner->partition(block, row_idxs, _shard_ids);
    std::vector<DorisVector<uint32_t>> buckets(_sub_writer_count);
    for (size_t i = 0; i < row_idxs.size(); ++i) {
        buckets[_shard_ids[i]].push_back(row_idxs[i]);
    }
    for (int s = 0; s < _sub_writer_count; ++s) {
        if (!buckets[s].empty()) {
            RETURN_IF_ERROR(_sub_writers[s]->write(block, buckets[s]));
        }
    }
    return Status::OK();
}

Status DeltaWriterV2::close() {
    _lock_watch.start();
    std::lock_guard<std::mutex> l(_lock);
    _lock_watch.stop();
    if (!_is_init && !_is_cancelled) {
        // if this delta writer is not initialized, but close() is called.
        // which means this tablet has no data loaded, but at least one tablet
        // in same partition has data loaded.
        // so we have to also init this DeltaWriterV2, so that it can create an empty rowset
        // for this tablet when being closed.
        RETURN_IF_ERROR(init());
    }
    for (auto& sub_writer : _sub_writers) {
        RETURN_IF_ERROR(sub_writer->close());
    }
    return Status::OK();
}

Status DeltaWriterV2::close_wait(int32_t& num_segments, RuntimeProfile* profile) {
    SCOPED_RAW_TIMER(&_close_wait_time);
    std::lock_guard<std::mutex> l(_lock);
    DCHECK(_is_init)
            << "delta writer is supposed be to initialized before close_wait() being called";

    if (profile != nullptr) {
        _update_profile(profile);
    }
    int64_t total_received_rows = 0;
    int64_t total_merged_rows = 0;
    for (auto& sub_writer : _sub_writers) {
        RETURN_IF_ERROR(sub_writer->close_wait(profile));
        total_received_rows += sub_writer->total_received_rows();
        total_merged_rows += sub_writer->merged_rows();
    }
    // Aggregate row-count consistency check across all sub-writers (each sub-writer
    // skips its own check because they share one rowset writer).
    if (_sub_writer_count > 1 &&
        _rowset_writer->num_rows() + total_merged_rows != total_received_rows) {
        LOG(WARNING) << "the rows number written doesn't match, rowset num rows written to file: "
                     << _rowset_writer->num_rows() << ", total merged_rows: " << total_merged_rows
                     << ", total received rows: " << total_received_rows
                     << ", tablet_id: " << _req.tablet_id;
        return Status::InternalError("rows number written by delta writer dosen't match");
    }
    num_segments = _rowset_writer->next_segment_id();

    _delta_written_success = true;
    return Status::OK();
}

Status DeltaWriterV2::cancel() {
    return cancel_with_status(Status::Cancelled("already cancelled"));
}

Status DeltaWriterV2::cancel_with_status(const Status& st) {
    std::lock_guard<std::mutex> l(_lock);
    if (_is_cancelled) {
        return Status::OK();
    }
    for (auto& sub_writer : _sub_writers) {
        RETURN_IF_ERROR(sub_writer->cancel_with_status(st));
    }
    _is_cancelled = true;
    return Status::OK();
}

Status DeltaWriterV2::_build_current_tablet_schema(int64_t index_id,
                                                   const OlapTableSchemaParam* table_schema_param,
                                                   const TabletSchema& ori_tablet_schema) {
    // find the right index id
    const OlapTableIndexSchema* index_schema = nullptr;
    for (const auto* schema : table_schema_param->indexes()) {
        if (schema->index_id == index_id) {
            index_schema = schema;
            break;
        }
    }

    auto cache_key = TabletSchemaCache::build_load_schema_cache_key(
            index_id, table_schema_param, ori_tablet_schema, index_schema);
    auto cached_schema = TabletSchemaCache::instance()->lookup_schema(cache_key);
    if (cached_schema.first != nullptr) {
        _tablet_schema = cached_schema.second;
        TabletSchemaCache::instance()->release(cached_schema.first);
    } else {
        _tablet_schema->copy_from(ori_tablet_schema);
        if (index_schema != nullptr && !index_schema->columns.empty() &&
            index_schema->columns[0]->unique_id() >= 0) {
            _tablet_schema->build_current_tablet_schema(
                    index_id, static_cast<int32_t>(table_schema_param->version()), index_schema,
                    ori_tablet_schema);
        }
        _tablet_schema->set_table_id(table_schema_param->table_id());
        _tablet_schema->set_db_id(table_schema_param->db_id());
        if (table_schema_param->is_partial_update()) {
            _tablet_schema->set_auto_increment_column(table_schema_param->auto_increment_coulumn());
        }
        auto inserted_schema = TabletSchemaCache::instance()->insert(cache_key, _tablet_schema);
        _tablet_schema = inserted_schema.second;
        TabletSchemaCache::instance()->release(inserted_schema.first);
    }

    // set partial update columns info
    _partial_update_info = std::make_shared<PartialUpdateInfo>();
    RETURN_IF_ERROR(_partial_update_info->init(
            _req.tablet_id, _req.txn_id, *_tablet_schema,
            table_schema_param->unique_key_update_mode(),
            table_schema_param->partial_update_new_key_policy(),
            table_schema_param->partial_update_input_columns(),
            table_schema_param->is_strict_mode(), table_schema_param->timestamp_ms(),
            table_schema_param->nano_seconds(), table_schema_param->timezone(),
            table_schema_param->auto_increment_coulumn()));
    return Status::OK();
}

} // namespace doris
