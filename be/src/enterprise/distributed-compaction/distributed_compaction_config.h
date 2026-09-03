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

#include "common/config.h"

namespace doris::config {

DECLARE_mBool(enable_cloud_single_rowset_distributed_compaction);
DECLARE_mBool(enable_cloud_distributed_base_compaction);
DECLARE_mBool(enable_cloud_distributed_compaction_peer_read);
DECLARE_mInt64(cloud_distributed_base_compaction_target_range_input_size_bytes);
DECLARE_mInt32(cloud_distributed_base_compaction_samples_per_range);
DECLARE_mInt32(cloud_distributed_compaction_worker_cache_ttl_ms);
DECLARE_mInt32(cloud_distributed_compaction_segment_slot_capacity);
DECLARE_mInt32(cloud_distributed_compaction_incremental_bitmap_rpc_timeout_ms);
DECLARE_mInt32(cloud_distributed_compaction_control_rpc_timeout_ms);
DECLARE_mInt32(cloud_distributed_compaction_status_poll_interval_ms);
DECLARE_Int32(cloud_distributed_compaction_rpc_thread_num);
DECLARE_Int32(cloud_distributed_compaction_rpc_queue_size);
DECLARE_Int32(cloud_distributed_compaction_worker_thread_num);
DECLARE_Int32(cloud_distributed_compaction_worker_queue_size);

} // namespace doris::config
