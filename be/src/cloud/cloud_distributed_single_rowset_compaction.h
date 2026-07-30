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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "storage/rowset/rowset_fwd.h"
#include "storage/tablet/tablet_fwd.h"

namespace doris {

class CloudStorageEngine;
class CloudTablet;
class DeleteBitmap;
class PCloudSingleRowsetCompactionFinishRequest;
class PCloudSingleRowsetCompactionFinishResponse;
class PCloudSingleRowsetCompactionIncrementalRequest;
class PCloudSingleRowsetCompactionIncrementalResponse;
class PCloudSingleRowsetCompactionRequest;
class PCloudSingleRowsetCompactionResponse;
class RowIdConversion;

namespace cloud {

struct SingleRowsetSegmentIdSlot {
    int32_t start_id;
    int32_t capacity;
};

struct DistributedSingleRowsetTask {
    std::string worker_endpoint;
    int32_t group_index;
    int32_t attempt_id;
    SingleRowsetSegmentIdSlot segment_id_slot;
    bool started = false;
};

struct DistributedSingleRowsetCompactionState {
    std::string execution_id;
    std::vector<DistributedSingleRowsetTask> tasks;
    int64_t phase1_end_version = 0;
    std::shared_ptr<DeleteBitmap> output_delete_bitmap;
};

Status build_single_rowset_segment_id_slots(int32_t base_segment_id, int32_t slot_capacity,
                                            size_t group_count,
                                            std::vector<SingleRowsetSegmentIdSlot>* slots);

Status parse_single_rowset_compaction_workers(std::string_view worker_config,
                                              std::string_view local_endpoint,
                                              std::vector<std::string>* workers);

Status single_rowset_compaction_rpc(const std::string& endpoint,
                                    const PCloudSingleRowsetCompactionRequest& request,
                                    PCloudSingleRowsetCompactionResponse* response);

Status single_rowset_compaction_incremental_rpc(
        const std::string& endpoint,
        const PCloudSingleRowsetCompactionIncrementalRequest& request,
        PCloudSingleRowsetCompactionIncrementalResponse* response);

Status single_rowset_compaction_finish_rpc(
        const std::string& endpoint, const PCloudSingleRowsetCompactionFinishRequest& request,
        PCloudSingleRowsetCompactionFinishResponse* response);

class DistributedSingleRowsetCompactionWorker {
public:
    DistributedSingleRowsetCompactionWorker(CloudStorageEngine& engine,
                                             std::shared_ptr<CloudTablet> tablet);
    ~DistributedSingleRowsetCompactionWorker();

    Status handle_compaction(const PCloudSingleRowsetCompactionRequest* request,
                             PCloudSingleRowsetCompactionResponse* response);

    Status handle_incremental(const PCloudSingleRowsetCompactionIncrementalRequest* request,
                              PCloudSingleRowsetCompactionIncrementalResponse* response);

    Status handle_finish(const PCloudSingleRowsetCompactionFinishRequest* request);

private:
    Status cleanup_output_files();

    CloudStorageEngine& _engine;
    std::shared_ptr<CloudTablet> _tablet;
    std::mutex _mutex;
    bool _is_mow = false;
    RowsetSharedPtr _partial_rowset;
    RowsetMetaSharedPtr _remote_rowset_meta;
    std::vector<int64_t> _output_segment_ids;
    std::unique_ptr<RowIdConversion> _rowid_conversion;
};

class DistributedSingleRowsetCompactionWorkerManager {
public:
    static DistributedSingleRowsetCompactionWorkerManager* instance();

    std::shared_ptr<DistributedSingleRowsetCompactionWorker> get_or_create(
            const std::string& execution_id, int32_t group_index, int32_t attempt_id,
            int64_t expiration_time, CloudStorageEngine& engine,
            std::shared_ptr<CloudTablet> tablet,
            bool* created);

    std::shared_ptr<DistributedSingleRowsetCompactionWorker> get(
            const std::string& execution_id, int32_t group_index, int32_t attempt_id);

    void erase(const std::string& execution_id, int32_t group_index, int32_t attempt_id);

    size_t remove_expired_workers(int64_t current_time);

private:
    struct WorkerEntry {
        std::shared_ptr<DistributedSingleRowsetCompactionWorker> worker;
        int64_t expiration_time;
    };

    static std::string key(const std::string& execution_id, int32_t group_index,
                           int32_t attempt_id);

    std::mutex _mutex;
    std::unordered_map<std::string, WorkerEntry> _workers;
};

} // namespace cloud
} // namespace doris
