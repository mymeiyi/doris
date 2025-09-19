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

#include "exec/schema_scanner/schema_cluster_snapshot_properties_scanner.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <cstdint>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "common/status.h"
#include "exec/schema_scanner/schema_helper.h"
#include "olap/storage_engine.h"
#include "runtime/define_primitive_type.h"
#include "runtime/exec_env.h"
#include "util/url_coding.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;
namespace vectorized {
class Block;
} // namespace vectorized

std::vector<SchemaScanner::ColumnDesc> SchemaClusterSnapshotPropertiesScanner::_s_tbls_columns = {
        {"READY", TYPE_BOOLEAN, sizeof(int8_t), true},
        {"ENABLED", TYPE_BOOLEAN, sizeof(int8_t), true},
        {"MAX_RESERVED_SNAPSHOTS", TYPE_BIGINT, sizeof(int64_t), true},
        {"SNAPSHOT_INTERVAL_SECONDS", TYPE_BIGINT, sizeof(int64_t), true},
};

SchemaClusterSnapshotPropertiesScanner::SchemaClusterSnapshotPropertiesScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_CLUSTER_SNAPSHOT_PROPERTIES) {}

SchemaClusterSnapshotPropertiesScanner::~SchemaClusterSnapshotPropertiesScanner() {}

Status SchemaClusterSnapshotPropertiesScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    if (!config::is_cloud_mode()) {
        return Status::InternalError("only support cloud mode");
    }

    RETURN_IF_ERROR(
            ExecEnv::GetInstance()->storage_engine().to_cloud().meta_mgr().get_snapshot_properties(
                    _switch_status, _max_reserved_snapshots, _snapshot_interval_seconds));
}

Status SchemaClusterSnapshotPropertiesScanner::get_next_block_internal(vectorized::Block* block,
                                                                       bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (block == nullptr || eos == nullptr) {
        return Status::InternalError("invalid parameter.");
    }

    *eos = true;
    return _fill_block_impl(block);
}

Status SchemaClusterSnapshotPropertiesScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);
    std::vector<void*> datas(1);
    // ready
    {
        int8_t ready = _switch_status == SnapshotSwitchStatus::SNAPSHOT_SWITCH_DISABLED ? 0 : 1;
        datas[0] = ready;
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 0, datas));
    }
    // enabled
    {
        int8_t enabled = _switch_status == SnapshotSwitchStatus::SNAPSHOT_SWITCH_ON ? 0 : 1;
        datas[0] = enabled;
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 1, datas));
    }
    // max_reserved_snapshots
    {
        datas[0] = _max_reserved_snapshots;
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 2, datas));
    }
    // snapshot_interval_seconds
    {
        datas[0] = _max_reserved_snapshots;
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 3, datas));
    }
    /*
    // auto_snapshot
    {
        std::vector<int8_t> srcs(row_num);
        for (int i = 0; i < row_num; ++i) {
            if (_snapshots[i].has_auto_snapshot()) {
                srcs[i] = _snapshots[i].auto_snapshot() ? 1 : 0;
                datas[i] = srcs.data() + i;
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 7, datas));
    }
    */
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
