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

#include <functional>
#include <memory>
#include <optional>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "storage/compaction/compaction.h"
#include "storage/compaction_task_tracker.h"

namespace doris {

namespace cloud {
class DistributedCompactionCoordinator;
}

class CloudBaseCompaction : public CloudCompactionMixin {
public:
    CloudBaseCompaction(CloudStorageEngine& engine, CloudTabletSPtr tablet);
    ~CloudBaseCompaction() override;

    Status prepare_compact() override;
    Status execute_compact() override;
    Status execute_compact_async(std::function<void(Status)> remote_completion, bool* suspended);
    Status resume_compact(Status remote_status);
    Status request_global_lock();

    std::optional<CompactionProfileType> profile_type() const override {
        return CompactionProfileType::BASE;
    }
    int64_t input_segments_num_value() const override { return _input_segments; }

    void do_lease();

private:
    Status pick_rowsets_to_compact();

    Status prepare_merge_input_rowsets(MergeInputRowsetsResult* result) override;

    Status do_merge_input_rowsets(const std::vector<RowsetReaderSharedPtr>& input_rs_readers,
                                  MergeInputRowsetsResult* result) override;

    Status start_distributed_compaction(std::function<void(Status)> remote_completion,
                                        bool* started);
    Status assemble_distributed_compaction(MergeInputRowsetsResult* result);
    Status finish_async_compaction();
    Status fail_async_compaction(Status status);
    void finish_compaction_success(int64_t execution_start_time_us);
    Status finish_compaction_failure(Status status);

    std::string_view compaction_name() const override { return "CloudBaseCompaction"; }

    Status modify_rowsets() override;

    Status garbage_collection() override;

    void _filter_input_rowset();

    void build_basic_info();

    ReaderType compaction_type() const override { return ReaderType::READER_BASE_COMPACTION; }

    int64_t _input_segments = 0;
    int64_t _base_compaction_cnt = 0;
    int64_t _cumulative_compaction_cnt = 0;
    bool _use_distributed_base_compaction = false;
    std::shared_ptr<cloud::DistributedCompactionCoordinator> _distributed_compaction;
    bool _distributed_commit_started = false;
    std::unique_ptr<MergeInputRowsetsContext> _async_merge_context;
    int64_t _async_profile_start_time_ms = 0;
    int64_t _async_execution_start_time_us = 0;
};

} // namespace doris
