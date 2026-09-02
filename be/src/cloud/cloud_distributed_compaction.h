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

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/merger.h"
#include "storage/rowset/rowset_fwd.h"

namespace doris {

class CloudStorageEngine;
class CloudTablet;
class DeleteBitmap;
class RowsetWriter;
class TabletSchema;

namespace cloud {

class DistributedCompaction {
public:
    virtual ~DistributedCompaction() = default;

    virtual Status assemble_output_rowset(RowsetWriter& output_rowset_writer,
                                          const TabletSchema& tablet_schema,
                                          std::vector<int32_t>* output_segment_group_sizes,
                                          RowsetSharedPtr* output_rowset,
                                          Merger::Statistics* stats) = 0;
    virtual Status finish_mow_delete_bitmap(int64_t initiator,
                                            std::shared_ptr<DeleteBitmap>* output_delete_bitmap,
                                            int64_t* lock_start_time) = 0;
    virtual void finalize(bool cancel_tasks) = 0;
    virtual int64_t task_count() const = 0;
    virtual int64_t worker_count() const = 0;
};

using DistributedCompactionCompletion = std::function<void(Status)>;

bool distributed_compaction_available();
Status start_distributed_compaction();
void stop_distributed_compaction();
void shutdown_distributed_compaction();
Status schedule_distributed_compaction(std::function<void()> callback);

bool can_use_distributed_base_compaction(const CloudTablet& tablet,
                                         int64_t input_rowsets_total_size);

Status start_distributed_base_compaction(
        CloudStorageEngine& engine, std::shared_ptr<CloudTablet> tablet, std::string execution_id,
        const std::vector<RowsetSharedPtr>& input_rowsets, RowsetWriter& output_rowset_writer,
        bool is_vertical, uint32_t avg_segment_rows, DistributedCompactionCompletion completion,
        std::shared_ptr<DistributedCompaction>* compaction);

Status start_distributed_single_rowset_compaction(
        CloudStorageEngine& engine, std::shared_ptr<CloudTablet> tablet, std::string execution_id,
        const RowsetSharedPtr& input_rowset, RowsetWriter& output_rowset_writer,
        int64_t segment_group_size, bool allow_delete_in_cumu_compaction, bool is_vertical,
        uint32_t avg_segment_rows, DistributedCompactionCompletion completion,
        std::shared_ptr<DistributedCompaction>* compaction);

void remove_expired_distributed_compactions(int64_t current_time);

} // namespace cloud
} // namespace doris
