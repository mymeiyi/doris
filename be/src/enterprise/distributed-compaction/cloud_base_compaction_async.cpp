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
#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/config.h"
#include "common/cast_set.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "common/signal_handler.h"
#include "cpp/sync_point.h"
#include "enterprise/distributed-compaction/distributed_compaction_impl.h"
#include "runtime/thread_context.h"
#include "storage/rowset/rowset.h"
#include "util/defer_op.h"
#include "util/thread.h"
#include "util/time.h"

namespace doris {

bool CloudBaseCompaction::can_use_distributed_base_compaction() const {
    return cloud::can_use_distributed_base_compaction(*cloud_tablet(), _input_rowsets_total_size);
}

Status CloudBaseCompaction::execute_distributed_compact_async(
        std::function<void(Status)> remote_completion, bool* distributed_started) {
    *distributed_started = false;
    TEST_INJECTION_POINT("Compaction::do_compaction");
#ifndef __APPLE__
    if (config::enable_base_compaction_idle_sched) {
        Thread::set_idle_sched();
    }
#endif

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
        // Workers own the destination segment index writers and row-id conversion state.
        _enable_inverted_index_compaction = false;
        _merge_execution_context = std::make_unique<MergeInputRowsetsContext>();
        status = prepare_merge_input_rowsets_execution(_merge_execution_context.get());
        if (!status.ok()) {
            return handle_prepared_compaction_failure(std::move(status));
        }

        status = cloud::start_distributed_base_compaction(
                _engine, std::static_pointer_cast<CloudTablet>(_tablet), _uuid, _input_rowsets,
                *_output_rs_writer, _is_vertical, cast_set<uint32_t>(get_avg_segment_rows()),
                std::move(remote_completion), &_distributed_compaction);
        *distributed_started = _distributed_compaction != nullptr;
        if (_distributed_compaction != nullptr) {
            _is_distributed = true;
            _distributed_task_count = _distributed_compaction->task_count();
            _distributed_worker_count = _distributed_compaction->worker_count();
        }
        if (!status.ok()) {
            return handle_prepared_compaction_failure(std::move(status));
        }
        if (_distributed_compaction == nullptr) {
            return Status::OK();
        }
        _merge_execution_context->input_rs_readers.clear();
        LOG_INFO("suspend base compaction while waiting for distributed workers")
                .tag("job_id", _uuid)
                .tag("tablet_id", _tablet->tablet_id());
        return Status::OK();
    } catch (const doris::Exception& exception) {
        if (exception.code() == doris::ErrorCode::MEM_ALLOC_FAILED) {
            status = Status::MemoryLimitExceeded(exception.to_string());
        } else {
            status = exception.to_status();
        }
        return handle_prepared_compaction_failure(std::move(status));
    }
}

Status CloudBaseCompaction::execute_local_compact_after_distributed_fallback() {
    DORIS_CHECK(_merge_execution_context != nullptr);
    DORIS_CHECK(_distributed_compaction == nullptr);

    // The distributed attempt already completed prepare_execute_compact() and
    // prepare_merge_input_rowsets_execution(); continue from the local merge step.
    Status status;
    try {
        doris::enable_thread_catch_bad_alloc++;
        Defer restore_catch_bad_alloc {[&] { doris::enable_thread_catch_bad_alloc--; }};
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

Status CloudBaseCompaction::complete_distributed_compaction(Status remote_status) {
#ifndef __APPLE__
    if (config::enable_base_compaction_idle_sched) {
        Thread::set_idle_sched();
    }
#endif
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
        std::vector<int32_t> output_segment_group_sizes;
        status = _distributed_compaction->assemble_output_rowset(
                *_output_rs_writer, *_cur_tablet_schema, &output_segment_group_sizes,
                &_output_rowset, &_stats);
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

Status CloudBaseCompaction::complete_prepared_compaction(bool build_output_rowset) {
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

Status CloudBaseCompaction::handle_prepared_compaction_failure(Status status) {
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

void CloudStorageEngine::_execute_base_compaction_task(
        const CloudTabletSPtr& tablet, const std::shared_ptr<CloudBaseCompaction>& compaction,
        std::function<void(Status)> complete_task) {
    if (!compaction->can_use_distributed_base_compaction()) {
        complete_task(compaction->execute_compact());
        return;
    }
    auto schedule_resume = std::make_shared<std::function<void(Status)>>();
    std::weak_ptr<std::function<void(Status)>> weak_schedule_resume = schedule_resume;
    *schedule_resume = [this, tablet_id = tablet->tablet_id(), compaction, complete_task,
                        weak_schedule_resume](Status remote_status) mutable {
        if (stopped()) {
            Status result = compaction->complete_distributed_compaction(std::move(remote_status));
            complete_task(std::move(result));
            return;
        }
        const Status submit_status = _base_compaction_thread_pool->submit_func(
                [tablet_id, compaction, complete_task, remote_status]() mutable {
                    signal::tablet_id = tablet_id;
                    Status result =
                            compaction->complete_distributed_compaction(std::move(remote_status));
                    complete_task(std::move(result));
                });
        if (submit_status.ok()) {
            return;
        }
        auto resume = weak_schedule_resume.lock();
        DORIS_CHECK(resume != nullptr);
        const Status retry_status = cloud::schedule_distributed_compaction(
                [resume = std::move(resume), remote_status]() mutable {
                    (*resume)(std::move(remote_status));
                });
        if (!retry_status.ok()) {
            Status result = compaction->complete_distributed_compaction(retry_status);
            complete_task(std::move(result));
        }
    };
    bool distributed_started = false;
    Status status = compaction->execute_distributed_compact_async(
            [schedule_resume](Status remote_status) mutable {
                (*schedule_resume)(std::move(remote_status));
            },
            &distributed_started);
    if (status.ok() && !distributed_started) {
        status = compaction->execute_local_compact_after_distributed_fallback();
    }
    // No remote callback will follow in either case:
    // 1. Distributed execution cannot start and the local fallback has completed.
    // 2. Execution fails before distributed work starts.
    if (!distributed_started) {
        complete_task(std::move(status));
    }
}

} // namespace doris
