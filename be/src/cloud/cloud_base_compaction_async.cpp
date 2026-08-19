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

#include "cloud/cloud_base_compaction.h"
#include "cloud/cloud_distributed_compaction.h"
#include "cloud/cloud_meta_mgr.h"
#include "cloud/config.h"
#include "common/cast_set.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "cpp/sync_point.h"
#include "runtime/thread_context.h"
#include "storage/rowset/rowset.h"
#include "util/defer_op.h"
#include "util/thread.h"
#include "util/time.h"

namespace doris {

bool CloudBaseCompaction::can_use_distributed_base_compaction() const {
    if (!config::enable_cloud_distributed_base_compaction) {
        return false;
    }
    const int64_t target_size = config::cloud_distributed_base_compaction_target_input_size_bytes;
    if (target_size <= 0 || _input_rowsets_total_size <= target_size) {
        return false;
    }
    if (_tablet->is_row_binlog_tablet()) {
        return false;
    }

    const auto keys_type = _tablet->keys_type();
    const auto& schema = *_tablet->tablet_schema();
    if (schema.num_key_columns() == 0 ||
        !cloud::is_supported_distributed_base_key(schema.column(0).type()) ||
        schema.column(0).is_nullable()) {
        return false;
    }

    if (keys_type == KeysType::UNIQUE_KEYS && _tablet->enable_unique_key_merge_on_write()) {
        // ponytail: range workers rescan source delete bitmaps independently; keep diagnostic modes
        // local until row-id conversion can filter missed-row checks by key range.
        if (!schema.cluster_key_uids().empty() ||
            config::enable_missing_rows_correctness_check ||
            config::enable_mow_compaction_correctness_check_core ||
            config::enable_mow_compaction_correctness_check_fail ||
            config::enable_rowid_conversion_correctness_check) {
            return false;
        }
    }
    return true;
}

Status CloudBaseCompaction::execute_compact_async(
        std::function<void(Status)> remote_completion, bool* suspended) {
    DORIS_CHECK(remote_completion != nullptr);
    *suspended = false;
    TEST_INJECTION_POINT("Compaction::do_compaction");
#ifndef __APPLE__
    if (config::enable_base_compaction_idle_sched) {
        Thread::set_idle_sched();
    }
#endif

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
        _use_distributed_base_compaction = can_use_distributed_base_compaction();
        if (_use_distributed_base_compaction) {
            // Workers own the destination segment index writers and row-id conversion state.
            _enable_inverted_index_compaction = false;
        }
        _async_merge_context = std::make_unique<MergeInputRowsetsContext>();
        status = prepare_merge_input_rowsets_execution(_async_merge_context.get());
        if (!status.ok()) {
            return fail_async_compaction(std::move(status));
        }

        if (_use_distributed_base_compaction) {
            const int64_t target_size =
                    config::cloud_distributed_base_compaction_target_input_size_bytes;
            const size_t range_count =
                    cast_set<size_t>(1 + (_input_rowsets_total_size - 1) / target_size);
            _distributed_compaction =
                    std::make_shared<cloud::DistributedCompactionCoordinator>(
                            _engine, std::static_pointer_cast<CloudTablet>(_tablet), _uuid);
            bool started = false;
            status = _distributed_compaction->start_base_compaction(
                    _input_rowsets, *_output_rs_writer, range_count, _is_vertical,
                    cast_set<uint32_t>(get_avg_segment_rows()), std::move(remote_completion),
                    &started);
            if (!status.ok()) {
                return fail_async_compaction(std::move(status));
            }
            if (started) {
                _async_merge_context->input_rs_readers.clear();
                *suspended = true;
                LOG_INFO("suspend base compaction while waiting for distributed workers")
                        .tag("job_id", _uuid)
                        .tag("tablet_id", _tablet->tablet_id());
                return Status::OK();
            }
            _distributed_compaction.reset();
        }

        status = Compaction::do_merge_input_rowsets(_async_merge_context->input_rs_readers,
                                                    &_async_merge_context->result);
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

Status CloudBaseCompaction::resume_compact(Status remote_status) {
#ifndef __APPLE__
    if (config::enable_base_compaction_idle_sched) {
        Thread::set_idle_sched();
    }
#endif
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
        std::vector<int32_t> output_segment_group_sizes;
        status = _distributed_compaction->assemble_output_rowset(
                *_output_rs_writer, *_cur_tablet_schema, &output_segment_group_sizes,
                &_output_rowset, &_stats);
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

Status CloudBaseCompaction::finish_async_compaction() {
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
    finish_compaction_success(_async_execution_start_time_us);
    _async_merge_context.reset();
    return Status::OK();
}

Status CloudBaseCompaction::fail_async_compaction(Status status) {
    const Status gc_status = garbage_collection();
    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write() && !gc_status.ok()) {
        _engine.meta_mgr().remove_delete_bitmap_update_lock(_tablet->table_id(),
                                                            COMPACTION_DELETE_BITMAP_LOCK_ID,
                                                            initiator(), _tablet->tablet_id());
    }
    submit_profile_record(false, _async_profile_start_time_ms, status.to_string());
    _async_merge_context.reset();
    return finish_compaction_failure(std::move(status));
}

} // namespace doris
