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

#include "cloud/cloud_cumulative_compaction.h"

#include <bvar/reducer.h>

#include <utility>

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

extern bvar::Adder<uint64_t> cumu_output_size;

Status CloudCumulativeCompaction::execute_compact_async(
        std::function<void(Status)> remote_completion, bool* suspended) {
    DORIS_CHECK(remote_completion != nullptr);
    *suspended = false;
    if (!_single_rowset_compaction_segment_group_size.has_value()) {
        return execute_compact();
    }
    TEST_SYNC_POINT_RETURN_WITH_VALUE("CloudCumulativeCompaction::execute_compact_impl",
                                      Status::OK(), this);
    TEST_INJECTION_POINT("Compaction::do_compaction");

    SCOPED_ATTACH_TASK(_mem_tracker);
    _async_profile_start_time_ms = UnixMillis();
    _async_execution_start_time_us = MonotonicMicros();

    Status status;
    try {
        doris::enable_thread_catch_bad_alloc++;
        Defer restore_catch_bad_alloc {[&] { doris::enable_thread_catch_bad_alloc--; }};

        status = prepare_execute_compact(get_compaction_permits());
        if (!status.ok()) {
            return fail_async_compaction(std::move(status));
        }
        _async_merge_context = std::make_unique<MergeInputRowsetsContext>();
        status = prepare_merge_input_rowsets_execution(_async_merge_context.get());
        if (!status.ok()) {
            return fail_async_compaction(std::move(status));
        }
        DORIS_CHECK(_async_merge_context->result.is_segment_grouped);

        _distributed_compaction = std::make_shared<cloud::DistributedCompactionCoordinator>(
                _engine, std::static_pointer_cast<CloudTablet>(_tablet), _uuid);
        bool started = false;
        status = _distributed_compaction->start_single_rowset(
                _input_rowsets.front(), *_output_rs_writer,
                _async_merge_context->result.segment_group_size,
                _allow_delete_in_cumu_compaction, _is_vertical,
                cast_set<uint32_t>(get_avg_segment_rows()), std::move(remote_completion),
                &started);
        if (!status.ok()) {
            return fail_async_compaction(std::move(status));
        }
        if (started) {
            _async_merge_context->input_rs_readers.clear();
            *suspended = true;
            LOG_INFO("suspend cumulative compaction while waiting for distributed workers")
                    .tag("job_id", _uuid)
                    .tag("tablet_id", _tablet->tablet_id());
            return Status::OK();
        }

        _distributed_compaction.reset();
        status = do_local_single_rowset_grouped_compaction(&_async_merge_context->result);
        if (!status.ok()) {
            return fail_async_compaction(std::move(status));
        }
        return finish_async_compaction();
    } catch (const doris::Exception& exception) {
        if (exception.code() == doris::ErrorCode::MEM_ALLOC_FAILED) {
            status = Status::MemoryLimitExceeded(exception.to_string());
        } else {
            status = exception.to_status();
        }
        return fail_async_compaction(std::move(status));
    }
}

Status CloudCumulativeCompaction::resume_compact(Status remote_status) {
    DORIS_CHECK(_async_merge_context != nullptr);
    DORIS_CHECK(_distributed_compaction != nullptr);
    SCOPED_ATTACH_TASK(_mem_tracker);

    if (!remote_status.ok()) {
        return fail_async_compaction(std::move(remote_status));
    }

    Status status;
    try {
        doris::enable_thread_catch_bad_alloc++;
        Defer restore_catch_bad_alloc {[&] { doris::enable_thread_catch_bad_alloc--; }};
        status = _distributed_compaction->assemble_single_rowset(
                *_output_rs_writer, *_cur_tablet_schema,
                &_async_merge_context->result.output_segment_group_sizes, &_output_rowset,
                &_stats);
        if (!status.ok()) {
            return fail_async_compaction(std::move(status));
        }
        _async_merge_context->result.output_rowset_built = true;
        return finish_async_compaction();
    } catch (const doris::Exception& exception) {
        if (exception.code() == doris::ErrorCode::MEM_ALLOC_FAILED) {
            status = Status::MemoryLimitExceeded(exception.to_string());
        } else {
            status = exception.to_status();
        }
        return fail_async_compaction(std::move(status));
    }
}

Status CloudCumulativeCompaction::finish_async_compaction() {
    DORIS_CHECK(_async_merge_context != nullptr);
    Status status = finish_merge_input_rowsets_execution(_async_merge_context.get());
    if (status.ok()) {
        status = finish_execute_compact(_async_execution_start_time_us);
    }
    if (!status.ok()) {
        return fail_async_compaction(std::move(status));
    }

    DorisMetrics::instance()->remote_compaction_read_rows_total->increment(_input_row_num);
    DorisMetrics::instance()->remote_compaction_write_rows_total->increment(
            _output_rowset->num_rows());
    DorisMetrics::instance()->remote_compaction_write_bytes_total->increment(
            _output_rowset->total_disk_size());
    _load_segment_to_cache();
    submit_profile_record(true, _async_profile_start_time_ms);

    LOG_INFO("finish CloudCumulativeCompaction, tablet_id={}, cost={}ms, range=[{}-{}]",
             _tablet->tablet_id(),
             (MonotonicMicros() - _async_execution_start_time_us) / 1000,
             _input_rowsets.front()->start_version(), _input_rowsets.back()->end_version())
            .tag("job_id", _uuid)
            .tag("input_rowsets", _input_rowsets.size())
            .tag("input_rows", _input_row_num)
            .tag("input_segments", _input_segments)
            .tag("input_rowsets_data_size", _input_rowsets_data_size)
            .tag("input_rowsets_index_size", _input_rowsets_index_size)
            .tag("input_rowsets_total_size", _input_rowsets_total_size)
            .tag("output_rows", _output_rowset->num_rows())
            .tag("output_segments", _output_rowset->num_segments())
            .tag("output_rowset_data_size", _output_rowset->data_disk_size())
            .tag("output_rowset_index_size", _output_rowset->index_disk_size())
            .tag("output_rowset_total_size", _output_rowset->total_disk_size())
            .tag("tablet_max_version", _tablet->max_version_unlocked())
            .tag("cumulative_point", cloud_tablet()->cumulative_layer_point())
            .tag("num_rowsets", cloud_tablet()->fetch_add_approximate_num_rowsets(0))
            .tag("cumu_num_rowsets", cloud_tablet()->fetch_add_approximate_cumu_num_rowsets(0))
            .tag("local_read_time_us", _stats.cloud_local_read_time)
            .tag("remote_read_time_us", _stats.cloud_remote_read_time)
            .tag("local_read_bytes", _local_read_bytes_total)
            .tag("remote_read_bytes", _remote_read_bytes_total);

    _state = CompactionState::SUCCESS;
    DorisMetrics::instance()->cumulative_compaction_deltas_total->increment(_input_rowsets.size());
    DorisMetrics::instance()->cumulative_compaction_bytes_total->increment(
            _input_rowsets_total_size);
    cumu_output_size << _output_rowset->total_disk_size();
    cloud_tablet()->set_last_cumu_compaction_status(Status::OK().to_string());
    cloud_tablet()->set_last_cumu_compaction_success_time(UnixMillis());
    _async_merge_context.reset();
    return Status::OK();
}

Status CloudCumulativeCompaction::fail_async_compaction(Status status) {
    const Status gc_status = garbage_collection();
    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write() && !gc_status.ok()) {
        _engine.meta_mgr().remove_delete_bitmap_update_lock(
                _tablet->table_id(), COMPACTION_DELETE_BITMAP_LOCK_ID, initiator(),
                _tablet->tablet_id());
    }
    submit_profile_record(false, _async_profile_start_time_ms, status.to_string());
    cloud_tablet()->set_last_cumu_compaction_status(status.to_string());
    cloud_tablet()->set_last_cumu_compaction_failure_time(UnixMillis());
    LOG_WARNING("fail to do asynchronous CloudCumulativeCompaction")
            .tag("job_id", _uuid)
            .tag("tablet_id", _tablet->tablet_id())
            .error(status);
    _async_merge_context.reset();
    return status;
}

} // namespace doris
