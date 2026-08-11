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

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "storage/merger.h"
#include "storage/rowset/rowset_fwd.h"
#include "storage/tablet/tablet_fwd.h"

namespace doris {

class CloudStorageEngine;
class CloudTablet;
class DeleteBitmap;
class MemTrackerLimiter;
class PCloudDistributedCompactionFinalizeRequest;
class PCloudDistributedCompactionFinalizeResponse;
class PCloudDistributedCompactionCalcIncrementalDeleteBitmapRequest;
class PCloudDistributedCompactionCalcIncrementalDeleteBitmapResponse;
class PCloudDistributedCompactionSubmitRequest;
class PCloudDistributedCompactionSubmitResponse;
class PCloudDistributedCompactionGetStatusRequest;
class PCloudDistributedCompactionGetStatusResponse;
class PCloudDistributedCompactionTask;
class PCloudDistributedCompactionTaskResult;
class PCloudDistributedCompactionTaskStatus;
class RowIdConversion;
struct RowsetId;
class RowsetWriter;
class RuntimeState;
struct StorageResource;
class TabletSchema;

namespace cloud {

struct OutputRowsetSegmentIdSlot {
    int32_t start_id;
    int32_t capacity;
};

struct CompactionWorkerInfo {
    int64_t backend_id;
    std::string endpoint;
    std::string cloud_unique_id;
    std::string compute_group_id;
};

std::vector<CompactionWorkerInfo> select_compaction_workers_for_groups(
        const std::vector<CompactionWorkerInfo>& candidates, int64_t coordinator_backend_id,
        size_t group_count, std::string_view execution_id);

struct DistributedCompactionTask {
    std::string worker_endpoint;
    int32_t group_index;
    OutputRowsetSegmentIdSlot segment_id_slot;
    bool started = false;
};

struct DistributedCompactionState {
    std::vector<DistributedCompactionTask> tasks;
    int64_t phase1_end_version = 0;
    std::shared_ptr<DeleteBitmap> output_delete_bitmap;
};

struct SegmentGroupMergeRange {
    int64_t segment_pos_start;
    int64_t segment_pos_end;
    int64_t merge_way_num;
};

std::vector<SegmentGroupMergeRange> build_segment_group_merge_ranges(const RowsetMeta& rowset_meta,
                                                                     int64_t segment_group_size);

Status build_output_rowset_segment_id_slots(int32_t base_segment_id, int32_t slot_capacity,
                                            size_t group_count,
                                            std::vector<OutputRowsetSegmentIdSlot>* slots);

class CompactionWorkerCache {
public:
    using Fetcher = std::function<Status(std::vector<CompactionWorkerInfo>* workers)>;

    explicit CompactionWorkerCache(Fetcher fetcher);

    Status get_workers(std::vector<CompactionWorkerInfo>* workers);
    void invalidate();

private:
    Fetcher _fetcher;
    std::mutex _mutex;
    bool _initialized = false;
    std::chrono::steady_clock::time_point _expires_at;
    Status _cached_status = Status::OK();
    std::vector<CompactionWorkerInfo> _workers;
};

CompactionWorkerCache* compaction_worker_cache();

// A single timer thread shared by every distributed compaction. It only moves due callbacks out
// of the deadline queue; status RPCs run in CloudDistributedCompactionRpcThreadPool.
class DistributedCompactionPollScheduler {
public:
    DistributedCompactionPollScheduler();
    ~DistributedCompactionPollScheduler();

    Status schedule(std::chrono::milliseconds delay, std::function<void()> callback);
    void stop();

private:
    void run();

    std::mutex _mutex;
    std::condition_variable _cv;
    bool _stopped = false;
    uint64_t _next_sequence = 0;
    std::multimap<std::pair<std::chrono::steady_clock::time_point, uint64_t>, std::function<void()>>
            _callbacks;
    std::thread _thread;
};

Status distributed_compaction_submit_rpc(const std::string& endpoint,
                                         const PCloudDistributedCompactionSubmitRequest& request,
                                         PCloudDistributedCompactionSubmitResponse* response);

Status distributed_compaction_get_status_rpc(
        const std::string& endpoint, const PCloudDistributedCompactionGetStatusRequest& request,
        PCloudDistributedCompactionGetStatusResponse* response);

Status distributed_compaction_calc_incremental_delete_bitmap_rpc(
        const std::string& endpoint,
        const PCloudDistributedCompactionCalcIncrementalDeleteBitmapRequest& request,
        PCloudDistributedCompactionCalcIncrementalDeleteBitmapResponse* response);

Status distributed_compaction_finalize_rpc(
        const std::string& endpoint, const PCloudDistributedCompactionFinalizeRequest& request,
        PCloudDistributedCompactionFinalizeResponse* response);

class DistributedCompactionCoordinator
        : public std::enable_shared_from_this<DistributedCompactionCoordinator> {
public:
    using CompletionCallback = std::function<void(Status)>;

    DistributedCompactionCoordinator(CloudStorageEngine& engine,
                                     std::shared_ptr<CloudTablet> tablet, std::string execution_id);
    ~DistributedCompactionCoordinator();

    Status start_single_rowset(const RowsetSharedPtr& input_rowset,
                               RowsetWriter& output_rowset_writer, int64_t segment_group_size,
                               bool allow_delete_in_cumu_compaction, bool is_vertical,
                               uint32_t avg_segment_rows, CompletionCallback callback,
                               bool* started);

    Status assemble_single_rowset(RowsetWriter& output_rowset_writer,
                                  const TabletSchema& tablet_schema,
                                  std::vector<int32_t>* output_segment_group_sizes,
                                  RowsetSharedPtr* output_rowset, Merger::Statistics* stats);

    Status finish_mow_delete_bitmap(int64_t initiator,
                                    std::shared_ptr<DeleteBitmap>* output_delete_bitmap,
                                    int64_t* lock_start_time);

    void finalize(bool preserve_output_files);

private:
    struct ValidatedPartialRowset;
    struct ExecutionPlan;
    struct PollRoundContext;

    Status submit_batches(const std::vector<CompactionWorkerInfo>& workers,
                          const std::vector<std::vector<size_t>>& groups_by_worker,
                          const std::vector<PCloudDistributedCompactionSubmitRequest>& requests,
                          std::vector<Status>* task_status);

    Status prepare_single_rowset(const RowsetSharedPtr& input_rowset,
                                 RowsetWriter& output_rowset_writer, int64_t segment_group_size,
                                 bool allow_delete_in_cumu_compaction, bool is_vertical,
                                 uint32_t avg_segment_rows, bool* started);

    Status schedule_poll(std::chrono::milliseconds delay);
    void dispatch_poll();
    void finish_poll_round(std::shared_ptr<PollRoundContext> round);
    void complete_polling(Status status);

    Status validate_partial_rowset(size_t group_index,
                                   const PCloudDistributedCompactionTaskResult& response,
                                   const DistributedCompactionTask& task,
                                   const TabletSchema& tablet_schema,
                                   const RowsetId& output_rowset_id,
                                   ValidatedPartialRowset* partial_rowset) const;

    Status fetch_incremental_delete_bitmap(int64_t incremental_end_version);

    CloudStorageEngine& _engine;
    std::shared_ptr<CloudTablet> _tablet;
    std::string _execution_id;
    std::unique_ptr<DistributedCompactionState> _state;
    std::unique_ptr<ExecutionPlan> _execution_plan;
    RowsetSharedPtr _output_rowset;
};

class DistributedCompactionWorker {
public:
    DistributedCompactionWorker(CloudStorageEngine& engine, std::shared_ptr<CloudTablet> tablet);
    ~DistributedCompactionWorker();

    Status execute_compaction(const PCloudDistributedCompactionSubmitRequest* request,
                              const PCloudDistributedCompactionTask* task);

    void cancel_compaction(int32_t group_index, const Status& status);

    void get_compaction_status(PCloudDistributedCompactionTaskStatus* status) const;

    void handle_finalize();

private:
    friend class DistributedCompactionWorkerManager;

    enum class State { PENDING, RUNNING, SUCCEEDED, FAILED };

    Result<std::unique_ptr<RowsetWriter>> construct_output_rowset_writer(
            const PCloudDistributedCompactionSubmitRequest& request,
            const PCloudDistributedCompactionTask& task, const RowsetMeta& output_meta,
            const StorageResource& storage_resource);
    Status handle_compaction(const PCloudDistributedCompactionSubmitRequest* request,
                             const PCloudDistributedCompactionTask* task,
                             PCloudDistributedCompactionTaskResult* result);
    Status calc_incremental_delete_bitmap(uint64_t start_version, uint64_t end_version,
                                          DeleteBitmap* output_delete_bitmap);
    void reset_state();

    CloudStorageEngine& _engine;
    std::shared_ptr<CloudTablet> _tablet;
    std::shared_ptr<MemTrackerLimiter> _mem_tracker;
    std::unique_ptr<RuntimeState> _runtime_state;
    std::mutex _mutex;
    mutable std::mutex _status_mutex;
    State _state = State::PENDING;
    std::unique_ptr<PCloudDistributedCompactionTaskResult> _result;
    bool _is_mow = false;
    RowsetSharedPtr _output_rowset;
    std::vector<int64_t> _output_segment_ids;
    std::unique_ptr<RowIdConversion> _rowid_conversion;
};

class DistributedCompactionWorkerManager {
public:
    static DistributedCompactionWorkerManager* instance();

    Status submit(const PCloudDistributedCompactionSubmitRequest& request,
                  CloudStorageEngine& engine);

    Status calc_incremental_delete_bitmap(
            const PCloudDistributedCompactionCalcIncrementalDeleteBitmapRequest& request,
            PCloudDistributedCompactionCalcIncrementalDeleteBitmapResponse* response);

    Status finalize(const PCloudDistributedCompactionFinalizeRequest& request);

    std::shared_ptr<DistributedCompactionWorker> get(const std::string& execution_id,
                                                     int32_t group_index);

    void remove_expired_workers(int64_t current_time);

private:
    struct WorkerEntry {
        std::shared_ptr<DistributedCompactionWorker> worker;
        int64_t expiration_time;
    };

    static std::string key(const std::string& execution_id, int32_t group_index);

    std::mutex _mutex;
    std::unordered_map<std::string, WorkerEntry> _workers;
};

} // namespace cloud
} // namespace doris
