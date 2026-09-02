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

#include "cloud/cloud_base_compaction.h"
#include "cloud/cloud_cumulative_compaction.h"
#include "cloud/cloud_distributed_compaction.h"

namespace doris {

bool CloudBaseCompaction::can_use_distributed_base_compaction() const {
    return false;
}

Status CloudBaseCompaction::execute_distributed_compact_async(std::function<void(Status)>,
                                                              bool* distributed_started) {
    *distributed_started = false;
    return Status::NotSupported("distributed compaction is not available");
}

Status CloudBaseCompaction::execute_local_compact_after_distributed_fallback() {
    return Status::NotSupported("distributed compaction is not available");
}

Status CloudBaseCompaction::complete_distributed_compaction(Status) {
    return Status::NotSupported("distributed compaction is not available");
}

Status CloudCumulativeCompaction::execute_distributed_compact_async(std::function<void(Status)>,
                                                                    bool* distributed_started) {
    *distributed_started = false;
    return Status::NotSupported("distributed compaction is not available");
}

Status CloudCumulativeCompaction::complete_distributed_compaction(Status) {
    return Status::NotSupported("distributed compaction is not available");
}

} // namespace doris

namespace doris::cloud {

bool distributed_compaction_available() {
    return false;
}

Status start_distributed_compaction() {
    return Status::NotSupported("distributed compaction is not available");
}

void stop_distributed_compaction() {}

void shutdown_distributed_compaction() {}

Status schedule_distributed_compaction(std::function<void()>) {
    return Status::NotSupported("distributed compaction is not available");
}

bool can_use_distributed_base_compaction(const CloudTablet&, int64_t) {
    return false;
}

Status start_distributed_base_compaction(CloudStorageEngine&, std::shared_ptr<CloudTablet>,
                                         std::string, const std::vector<RowsetSharedPtr>&,
                                         RowsetWriter&, bool, uint32_t,
                                         DistributedCompactionCompletion,
                                         std::shared_ptr<DistributedCompaction>* compaction) {
    compaction->reset();
    return Status::NotSupported("distributed compaction is not available");
}

Status start_distributed_single_rowset_compaction(
        CloudStorageEngine&, std::shared_ptr<CloudTablet>, std::string, const RowsetSharedPtr&,
        RowsetWriter&, int64_t, bool, bool, uint32_t, DistributedCompactionCompletion,
        std::shared_ptr<DistributedCompaction>* compaction) {
    compaction->reset();
    return Status::NotSupported("distributed compaction is not available");
}

void remove_expired_distributed_compactions(int64_t) {}

} // namespace doris::cloud
