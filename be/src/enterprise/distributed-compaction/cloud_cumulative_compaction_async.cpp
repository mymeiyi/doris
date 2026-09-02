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

#include <utility>

#include "cloud/cloud_cumulative_compaction.h"
#include "cloud/cloud_distributed_compaction.h"
#include "cloud/cloud_meta_mgr.h"
#include "common/cast_set.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "cpp/sync_point.h"
#include "runtime/thread_context.h"
#include "storage/rowset/rowset.h"
#include "util/debug_points.h"
#include "util/defer_op.h"
#include "util/time.h"

namespace doris {

Status CloudCumulativeCompaction::execute_distributed_compact_async(
        std::function<void(Status)> remote_completion, bool* distributed_started) {
    DORIS_CHECK(remote_completion != nullptr);
    DORIS_CHECK(is_single_rowset_grouped_compaction());
    *distributed_started = false;
    TEST_SYNC_POINT_RETURN_WITH_VALUE("CloudCumulativeCompaction::execute_compact_impl",
                                      Status::OK(), this);
    TEST_INJECTION_POINT("Compaction::do_compaction");

    SCOPED_ATTACH_TASK(_mem_tracker);
    _async_profile_start_time_ms = UnixMillis();
    _async_execution_start_time = std::chrono::steady_clock::now();
    _async_execution_start_time_us = MonotonicMicros();

    Status status;
    try {
        doris::enable_thread_catch_bad_alloc++;
        Defer restore_catch_bad_alloc {[&] { doris::enable_thread_catch_bad_alloc--; }};

        status = prepare_execute_compact(get_compaction_permits());
        if (!status.ok()) {
            return handle_prepared_compaction_failure(std::move(status));
        }
        _merge_execution_context = std::make_unique<MergeInputRowsetsContext>();
        status = prepare_merge_input_rowsets_execution(_merge_execution_context.get());
        if (!status.ok()) {
            return handle_prepared_compaction_failure(std::move(status));
        }
        DORIS_CHECK(_merge_execution_context->result.is_segment_grouped);

        status = cloud::start_distributed_single_rowset_compaction(
                _engine, std::static_pointer_cast<CloudTablet>(_tablet), _uuid,
                _input_rowsets.front(), *_output_rs_writer,
                _merge_execution_context->result.segment_group_size,
                _allow_delete_in_cumu_compaction, _is_vertical,
                cast_set<uint32_t>(get_avg_segment_rows()), std::move(remote_completion),
                &_distributed_compaction);
        if (_distributed_compaction != nullptr) {
            _is_distributed = true;
            _distributed_task_count = _distributed_compaction->task_count();
            _distributed_worker_count = _distributed_compaction->worker_count();
        }
        if (!status.ok()) {
            return handle_prepared_compaction_failure(std::move(status));
        }
        if (_distributed_compaction != nullptr) {
            _merge_execution_context->input_rs_readers.clear();
            *distributed_started = true;
            LOG_INFO("suspend cumulative compaction while waiting for distributed workers")
                    .tag("job_id", _uuid)
                    .tag("tablet_id", _tablet->tablet_id());
            return Status::OK();
        }

        status = execute_merge_input_rowsets(_merge_execution_context.get());
        if (!status.ok()) {
            return handle_prepared_compaction_failure(std::move(status));
        }
        return complete_prepared_compaction(/*build_output_rowset=*/true);
    } catch (const doris::Exception& exception) {
        if (exception.code() == doris::ErrorCode::MEM_ALLOC_FAILED) {
            status = Status::MemoryLimitExceeded(exception.to_string());
        } else {
            status = exception.to_status();
        }
        return handle_prepared_compaction_failure(std::move(status));
    }
}

Status CloudCumulativeCompaction::complete_distributed_compaction(Status remote_status) {
    DORIS_CHECK(is_single_rowset_grouped_compaction());
    DORIS_CHECK(_merge_execution_context != nullptr);
    DORIS_CHECK(_distributed_compaction != nullptr);
    SCOPED_ATTACH_TASK(_mem_tracker);

    if (!remote_status.ok()) {
        return handle_prepared_compaction_failure(std::move(remote_status));
    }

    Status status;
    try {
        doris::enable_thread_catch_bad_alloc++;
        Defer restore_catch_bad_alloc {[&] { doris::enable_thread_catch_bad_alloc--; }};
        status = _distributed_compaction->assemble_output_rowset(
                *_output_rs_writer, *_cur_tablet_schema,
                &_merge_execution_context->result.output_segment_group_sizes, &_output_rowset,
                &_stats);
        if (!status.ok()) {
            return handle_prepared_compaction_failure(std::move(status));
        }
        return complete_prepared_compaction(/*build_output_rowset=*/false);
    } catch (const doris::Exception& exception) {
        if (exception.code() == doris::ErrorCode::MEM_ALLOC_FAILED) {
            status = Status::MemoryLimitExceeded(exception.to_string());
        } else {
            status = exception.to_status();
        }
        return handle_prepared_compaction_failure(std::move(status));
    }
}

Status CloudCumulativeCompaction::complete_prepared_compaction(bool build_output_rowset) {
    DORIS_CHECK(_merge_execution_context != nullptr);
    Status status = finish_merge_input_rowsets_execution(_merge_execution_context.get(),
                                                         build_output_rowset);
    if (status.ok()) {
        status = finish_execute_compact(_async_execution_start_time_us);
    }
    if (!status.ok()) {
        return handle_prepared_compaction_failure(std::move(status));
    }

    DorisMetrics::instance()->remote_compaction_read_rows_total->increment(_input_row_num);
    DorisMetrics::instance()->remote_compaction_write_rows_total->increment(
            _output_rowset->num_rows());
    DorisMetrics::instance()->remote_compaction_write_bytes_total->increment(
            _output_rowset->total_disk_size());
    _load_segment_to_cache();
    submit_profile_record(true, _async_profile_start_time_ms);
    record_compaction_success(_async_execution_start_time);
    _merge_execution_context.reset();
    return Status::OK();
}

Status CloudCumulativeCompaction::handle_prepared_compaction_failure(Status status) {
    const Status gc_status = garbage_collection();
    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write() && !gc_status.ok()) {
        _engine.meta_mgr().remove_delete_bitmap_update_lock(_tablet->table_id(),
                                                            COMPACTION_DELETE_BITMAP_LOCK_ID,
                                                            initiator(), _tablet->tablet_id());
    }
    submit_profile_record(false, _async_profile_start_time_ms, status.to_string());
    _merge_execution_context.reset();
    return record_compaction_failure(std::move(status));
}

} // namespace doris
