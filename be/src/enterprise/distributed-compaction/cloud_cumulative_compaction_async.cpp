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

#include <bvar/reducer.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <utility>

#include "cloud/cloud_cumulative_compaction.h"
#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/config.h"
#include "common/cast_set.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "common/signal_handler.h"
#include "cpp/sync_point.h"
#include "enterprise/distributed-compaction/distributed_compaction_impl.h"
#include "runtime/thread_context.h"
#include "storage/compaction_task_tracker.h"
#include "storage/rowset/rowset.h"
#include "util/debug_points.h"
#include "util/defer_op.h"
#include "util/time.h"

namespace doris {

extern bvar::Adder<uint64_t> g_cumu_compaction_running_task_count;

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

std::optional<Status> CloudStorageEngine::_try_submit_cumulative_compaction_task(
        const CloudTabletSPtr& tablet, const std::shared_ptr<CloudCumulativeCompaction>& compaction,
        int64_t compaction_id, std::function<void()> erase_submitted_cumu_compaction,
        std::function<void()> erase_executing_cumu_compaction) {
    if (!compaction->is_single_rowset_grouped_compaction()) {
        return std::nullopt;
    }
    using namespace std::chrono;

    auto task_finished = std::make_shared<std::atomic_bool>(false);
    auto executing_registered = std::make_shared<std::atomic_bool>(false);
    auto is_large_task = std::make_shared<bool>(true);
    auto complete_task = [=, this](Status result) {
        if (task_finished->exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        DBUG_EXECUTE_IF("CloudStorageEngine._submit_cumulative_compaction_task.sleep",
                        { sleep(5); })
        if (!result.ok()) {
            tablet->set_last_cumu_compaction_failure_time(UnixMillis());
        }
        CompactionTaskTracker::instance()->remove_task(compaction_id);
        if (executing_registered->load(std::memory_order_acquire)) {
            erase_executing_cumu_compaction();
        }
        erase_submitted_cumu_compaction();
        g_cumu_compaction_running_task_count << -1;
        DorisMetrics::instance()->cumulative_compaction_task_running_total->increment(-1);
        DorisMetrics::instance()->cumulative_compaction_task_pending_total->set_value(
                _cumu_compaction_thread_pool->get_queue_size());
    };
    auto acquire_cumu_thread = [=, this] {
        std::lock_guard lock(_cumu_compaction_delay_mtx);
        _cumu_compaction_thread_pool_used_threads++;
        if (!*is_large_task) {
            _cumu_compaction_thread_pool_small_tasks_running++;
        }
    };
    auto release_cumu_thread = [=, this] {
        std::lock_guard lock(_cumu_compaction_delay_mtx);
        _cumu_compaction_thread_pool_used_threads--;
        if (!*is_large_task) {
            _cumu_compaction_thread_pool_small_tasks_running--;
        }
    };

    auto schedule_resume = std::make_shared<std::function<void(Status)>>();
    std::weak_ptr<std::function<void(Status)>> weak_schedule_resume = schedule_resume;
    *schedule_resume = [=, this](Status remote_status) mutable {
        if (stopped()) {
            Status result = compaction->complete_distributed_compaction(std::move(remote_status));
            complete_task(std::move(result));
            return;
        }
        const Status submit_status = _cumu_compaction_thread_pool->submit_func([=]() mutable {
            signal::tablet_id = tablet->tablet_id();
            acquire_cumu_thread();
            Status result = compaction->complete_distributed_compaction(std::move(remote_status));
            release_cumu_thread();
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

    Status st = _cumu_compaction_thread_pool->submit_func([=, this]() {
        DorisMetrics::instance()->cumulative_compaction_task_running_total->increment(1);
        DorisMetrics::instance()->cumulative_compaction_task_pending_total->set_value(
                _cumu_compaction_thread_pool->get_queue_size());
        DBUG_EXECUTE_IF("CloudStorageEngine._submit_cumulative_compaction_task.wait_in_line",
                        { sleep(5); })
        signal::tablet_id = tablet->tablet_id();
        g_cumu_compaction_running_task_count << 1;
        auto st = _request_tablet_global_compaction_lock(ReaderType::READER_CUMULATIVE_COMPACTION,
                                                         tablet, compaction);
        if (!st.ok()) {
            complete_task(std::move(st));
            return;
        }
        executing_registered->store(true, std::memory_order_release);
        // Update tracker to RUNNING after acquiring global lock
        {
            RunningStats rs;
            rs.start_time_ms =
                    duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
            CompactionTaskTracker::instance()->update_to_running(compaction_id, rs);
        }
        bool delayed = false;
        {
            std::lock_guard lock(_cumu_compaction_delay_mtx);
            _cumu_compaction_thread_pool_used_threads++;
            if (config::large_cumu_compaction_task_min_thread_num > 1 &&
                _cumu_compaction_thread_pool->max_threads() >=
                        config::large_cumu_compaction_task_min_thread_num) {
                // Determine if this is a small task based on configured thresholds
                *is_large_task = (compaction->get_input_rowsets_bytes() >
                                          config::large_cumu_compaction_task_bytes_threshold ||
                                  compaction->get_input_num_rows() >
                                          config::large_cumu_compaction_task_row_num_threshold);
                // Small task. No delay needed
                if (!*is_large_task) {
                    _cumu_compaction_thread_pool_small_tasks_running++;
                } else if (_should_delay_large_task()) {
                    long now = duration_cast<milliseconds>(system_clock::now().time_since_epoch())
                                       .count();
                    // sleep 5s for this tablet
                    tablet->set_last_cumu_compaction_failure_time(now);
                    LOG_WARNING(
                            "failed to do CloudCumulativeCompaction, cumu thread pool is "
                            "intensive, delay large task.")
                            .tag("tablet_id", tablet->tablet_id())
                            .tag("input_rows", compaction->get_input_num_rows())
                            .tag("input_rowsets_total_size", compaction->get_input_rowsets_bytes())
                            .tag("config::large_cumu_compaction_task_bytes_threshold",
                                 config::large_cumu_compaction_task_bytes_threshold)
                            .tag("config::large_cumu_compaction_task_row_num_threshold",
                                 config::large_cumu_compaction_task_row_num_threshold)
                            .tag("remaining threads", _cumu_compaction_thread_pool_used_threads)
                            .tag("small_tasks_running",
                                 _cumu_compaction_thread_pool_small_tasks_running);
                    _cumu_compaction_thread_pool_used_threads--;
                    delayed = true;
                }
            }
        }
        if (delayed) {
            complete_task(Status::InternalError(
                    "cumulative compaction delayed because thread pool is intensive"));
            return;
        }
        bool distributed_started = false;
        st = compaction->execute_distributed_compact_async(
                [schedule_resume](Status remote_status) mutable {
                    (*schedule_resume)(std::move(remote_status));
                },
                &distributed_started);
        release_cumu_thread();
        if (!distributed_started) {
            complete_task(std::move(st));
        }
    });
    DorisMetrics::instance()->cumulative_compaction_task_pending_total->set_value(
            _cumu_compaction_thread_pool->get_queue_size());
    if (!st.ok()) {
        CompactionTaskTracker::instance()->remove_task(compaction_id);
        erase_submitted_cumu_compaction();
        return Status::InternalError("failed to submit grouped cumu compaction, tablet_id={}",
                                     tablet->tablet_id());
    }
    return st;
}

} // namespace doris
