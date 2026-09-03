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

#include "enterprise/distributed-compaction/distributed_compaction_config.h"

namespace doris::config {

DEFINE_mBool(enable_cloud_single_rowset_distributed_compaction, "false");
DEFINE_mBool(enable_cloud_distributed_base_compaction, "false");
DEFINE_mBool(enable_cloud_distributed_compaction_peer_read, "true");
DEFINE_mInt64(cloud_distributed_base_compaction_target_range_input_size_bytes, "536870912");
DEFINE_mInt32(cloud_distributed_base_compaction_samples_per_range, "4096");
DEFINE_Validator(cloud_distributed_base_compaction_samples_per_range,
                 [](int32_t value) { return value > 0; });
DEFINE_mInt32(cloud_distributed_compaction_worker_cache_ttl_ms, "10000");
DEFINE_mInt32(cloud_distributed_compaction_segment_slot_capacity, "100");
DEFINE_mInt32(cloud_distributed_compaction_incremental_bitmap_rpc_timeout_ms, "100000");
DEFINE_mInt32(cloud_distributed_compaction_control_rpc_timeout_ms, "10000");
DEFINE_mInt32(cloud_distributed_compaction_status_poll_interval_ms, "20000");
DEFINE_Int32(cloud_distributed_compaction_rpc_thread_num, "32");
DEFINE_Int32(cloud_distributed_compaction_rpc_queue_size, "4096");
DEFINE_Int32(cloud_distributed_compaction_worker_thread_num, "32");
DEFINE_Int32(cloud_distributed_compaction_worker_queue_size, "4096");

} // namespace doris::config
