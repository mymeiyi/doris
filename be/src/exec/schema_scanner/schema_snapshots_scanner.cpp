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

#include "exec/schema_scanner/schema_snapshots_scanner.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <cstdint>
#include <string>

#include "common/status.h"
#include "exec/schema_scanner/schema_helper.h"
#include "runtime/define_primitive_type.h"
#include "runtime/runtime_state.h"
#include "util/url_coding.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;
namespace vectorized {
class Block;
} // namespace vectorized

std::vector<SchemaScanner::ColumnDesc> SchemaSnapshotsScanner::_s_tbls_columns = {
        {"ID", TYPE_STRING, sizeof(StringRef), true},                      // 0
        {"ANCESTOR", TYPE_STRING, sizeof(StringRef), true},                // 1
        {"CREATE_AT", TYPE_DATETIMEV2, sizeof(DateTimeV2ValueType), true}, // 2
        {"FINISH_AT", TYPE_DATETIMEV2, sizeof(DateTimeV2ValueType), true}, // 3
        {"IMAGE_URL", TYPE_STRING, sizeof(StringRef), true},               // 4
        {"JOURNAL_ID", TYPE_BIGINT, sizeof(int64_t), true},                // 5
        {"STATE", TYPE_STRING, sizeof(StringRef), true},                   // 6
        {"MANUAL", TYPE_BOOLEAN, sizeof(int8_t), true},                    // 7
        {"TTL", TYPE_BIGINT, sizeof(int64_t), true},                       // 8
        {"LABEL", TYPE_STRING, sizeof(StringRef), true},                   // 9
        {"MSG", TYPE_STRING, sizeof(StringRef), true},                     // 10
        {"COUNT", TYPE_INT, sizeof(int32_t), true},                        // 11
};

SchemaSnapshotsScanner::SchemaSnapshotsScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_SNAPSHOTS) {}

SchemaSnapshotsScanner::~SchemaSnapshotsScanner() {}

Status SchemaSnapshotsScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    TListSnapshotRequest request;
    TListSnapshotResult result;
    RETURN_IF_ERROR(SchemaHelper::list_snapshot(*(_param->common_param->ip),
                                                _param->common_param->port, request, &result));
    RETURN_IF_ERROR(Status::create(result.status));
    _snapshots.reserve(result.snapshots.size());
    for (const auto& snapshot : result.snapshots) {
        cloud::SnapshotInfoPB pb;
        if (!pb.ParseFromString(snapshot.snapshot_pb)) {
            return Status::InternalError("Parse snapshot info error, binary=",
                                         apache::thrift::ThriftDebugString(snapshot));
        }
        _snapshots.emplace_back(std::move(pb));
    }
    return Status::OK();
}

Status SchemaSnapshotsScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (block == nullptr || eos == nullptr) {
        return Status::InternalError("invalid parameter.");
    }

    *eos = true;
    if (_snapshots.empty()) {
        return Status::OK();
    }

    return _fill_block_impl(block);
}

Status SchemaSnapshotsScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);

    size_t row_num = _snapshots.size();
    if (row_num == 0) {
        return Status::OK();
    }

    for (size_t col_idx = 0; col_idx < _s_tbls_columns.size(); ++col_idx) {
        const auto& col_desc = _s_tbls_columns[col_idx];

        std::vector<StringRef> str_refs(row_num);
        std::vector<int32_t> int_vals(row_num);
        std::vector<int64_t> int64_vals(row_num);
        std::vector<int8_t> bool_vals(row_num);
        std::vector<void*> datas(row_num);
        std::vector<std::string> column_values(row_num);
        std::vector<DateV2Value<DateTimeV2ValueType>> date_vals(row_num);

        for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
            const auto& snapshot = _snapshots[row_idx];
            std::string& column_value = column_values[row_idx];
            std::string tmp_str;

            if (col_desc.type == TYPE_STRING) {
                switch (col_idx) {
                case 0:
                    column_value = snapshot.has_snapshot_id() ? snapshot.snapshot_id() : "";
                    break;
                case 1:
                    column_value = snapshot.has_ancestor_id() ? snapshot.ancestor_id() : "";
                    break;
                case 4:
                    column_value = snapshot.has_image_url() ? snapshot.image_url() : "";
                    break;
                case 6:
                    column_value = "";
                    if (snapshot.has_status()) {
                        if (snapshot.status() == cloud::SnapshotStatus::SNAPSHOT_PREPARE) {
                            column_value = "SNAPSHOT_PREPARE";
                        } else if (snapshot.status() == cloud::SnapshotStatus::SNAPSHOT_NORMAL) {
                            column_value = "SNAPSHOT_NORMAL";
                        } else if (snapshot.status() == cloud::SnapshotStatus::SNAPSHOT_ABORTED) {
                            column_value = "SNAPSHOT_ABORTED";
                        } else {
                            return Status::InternalError("Unknown snapshot status: ",
                                                         std::to_string(snapshot.status()));
                        }
                    }
                    LOG(INFO) << "sout: snapshot status: " << column_value << ", "
                              << std::to_string(snapshot.status());
                    break;
                case 9:
                    column_value = snapshot.has_snapshot_label() ? snapshot.snapshot_label() : "";
                    break;
                case 10:
                    column_value = snapshot.has_reason() ? snapshot.reason() : "";
                    break;
                }

                str_refs[row_idx] =
                        StringRef(column_values[row_idx].data(), column_values[row_idx].size());
                datas[row_idx] = &str_refs[row_idx];
            } else if (col_desc.type == TYPE_INT) {
                switch (col_idx) {
                case 11:
                    // TODO
                    int_vals[row_idx] = 0;
                    break;
                }
                datas[row_idx] = &int_vals[row_idx];
            } else if (col_desc.type == TYPE_BIGINT) {
                switch (col_idx) {
                case 5:
                    int64_vals[row_idx] = snapshot.has_journal_id() ? snapshot.journal_id() : 0;
                    break;
                case 8:
                    int64_vals[row_idx] = snapshot.has_ttl_seconds() ? snapshot.ttl_seconds() : 0;
                    break;
                }
                datas[row_idx] = &int64_vals[row_idx];
            } else if (col_desc.type == TYPE_DATETIMEV2) {
                switch (col_idx) {
                case 2:
                    date_vals[row_idx].from_unixtime(snapshot.create_at(), _timezone_obj);
                    break;
                case 3:
                    date_vals[row_idx].from_unixtime(snapshot.finish_at(), _timezone_obj);
                    break;
                }
                datas[row_idx] = &date_vals[row_idx];
            } else if (col_desc.type == TYPE_BOOLEAN) {
                switch (col_idx) {
                case 7:
                    bool_vals[row_idx] =
                            snapshot.has_auto_snapshot() ? snapshot.auto_snapshot() : false;
                    break;
                }
                datas[row_idx] = &bool_vals[row_idx];
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, col_idx, datas));
    }
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
