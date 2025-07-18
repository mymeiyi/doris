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

#include "jni_connector.h"

#include <glog/logging.h>

#include <sstream>
#include <variant>

#include "jni.h"
#include "runtime/decimalv2_value.h"
#include "runtime/runtime_state.h"
#include "util/jni-util.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/core/block.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_struct.h"

namespace doris {
class RuntimeProfile;
} // namespace doris

namespace doris::vectorized {

#define FOR_FIXED_LENGTH_TYPES(M)                                  \
    M(PrimitiveType::TYPE_TINYINT, ColumnInt8, Int8)               \
    M(PrimitiveType::TYPE_BOOLEAN, ColumnUInt8, UInt8)             \
    M(PrimitiveType::TYPE_SMALLINT, ColumnInt16, Int16)            \
    M(PrimitiveType::TYPE_INT, ColumnInt32, Int32)                 \
    M(PrimitiveType::TYPE_BIGINT, ColumnInt64, Int64)              \
    M(PrimitiveType::TYPE_LARGEINT, ColumnInt128, Int128)          \
    M(PrimitiveType::TYPE_FLOAT, ColumnFloat32, Float32)           \
    M(PrimitiveType::TYPE_DOUBLE, ColumnFloat64, Float64)          \
    M(PrimitiveType::TYPE_DECIMALV2, ColumnDecimal128V2, Int128)   \
    M(PrimitiveType::TYPE_DECIMAL128I, ColumnDecimal128V3, Int128) \
    M(PrimitiveType::TYPE_DECIMAL32, ColumnDecimal32, Int32)       \
    M(PrimitiveType::TYPE_DECIMAL64, ColumnDecimal64, Int64)       \
    M(PrimitiveType::TYPE_DATE, ColumnDate, Int64)                 \
    M(PrimitiveType::TYPE_DATEV2, ColumnDateV2, UInt32)            \
    M(PrimitiveType::TYPE_DATETIME, ColumnDateTime, Int64)         \
    M(PrimitiveType::TYPE_DATETIMEV2, ColumnDateTimeV2, UInt64)    \
    M(PrimitiveType::TYPE_IPV4, ColumnIPv4, IPv4)                  \
    M(PrimitiveType::TYPE_IPV6, ColumnIPv6, IPv6)

Status JniConnector::open(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;
    _profile = profile;
    ADD_TIMER(_profile, _connector_name.c_str());
    _open_scanner_time = ADD_CHILD_TIMER(_profile, "OpenScannerTime", _connector_name.c_str());
    _java_scan_time = ADD_CHILD_TIMER(_profile, "JavaScanTime", _connector_name.c_str());
    _java_append_data_time =
            ADD_CHILD_TIMER(_profile, "JavaAppendDataTime", _connector_name.c_str());
    _java_create_vector_table_time =
            ADD_CHILD_TIMER(_profile, "JavaCreateVectorTableTime", _connector_name.c_str());
    _fill_block_time = ADD_CHILD_TIMER(_profile, "FillBlockTime", _connector_name.c_str());
    _max_time_split_weight_counter = _profile->add_conditition_counter(
            "MaxTimeSplitWeight", TUnit::UNIT, [](int64_t _c, int64_t c) { return c > _c; },
            _connector_name.c_str());
    _java_scan_watcher = 0;
    // cannot put the env into fields, because frames in an env object is limited
    // to avoid limited frames in a thread, we should get local env in a method instead of in whole object.
    JNIEnv* env = nullptr;
    int batch_size = 0;
    if (!_is_table_schema) {
        batch_size = _state->batch_size();
    }
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    SCOPED_RAW_TIMER(&_jni_scanner_open_watcher);
    _scanner_params.emplace("time_zone", _state->timezone());
    RETURN_IF_ERROR(_init_jni_scanner(env, batch_size));
    // Call org.apache.doris.common.jni.JniScanner#open
    env->CallVoidMethod(_jni_scanner_obj, _jni_scanner_open);
    RETURN_ERROR_IF_EXC(env);
    _scanner_opened = true;
    return Status::OK();
}

Status JniConnector::init(
        const std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    // TODO: This logic need to be changed.
    // See the comment of "predicates" field in JniScanner.java

    // _generate_predicates(colname_to_value_range);
    // if (_predicates_length != 0 && _predicates != nullptr) {
    //     int64_t predicates_address = (int64_t)_predicates.get();
    //     // We can call org.apache.doris.common.jni.vec.ScanPredicate#parseScanPredicates to parse the
    //     // serialized predicates in java side.
    //     _scanner_params.emplace("push_down_predicates", std::to_string(predicates_address));
    // }
    return Status::OK();
}

Status JniConnector::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    // Call org.apache.doris.common.jni.JniScanner#getNextBatchMeta
    // return the address of meta information
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    long meta_address = 0;
    {
        SCOPED_RAW_TIMER(&_java_scan_watcher);
        meta_address = env->CallLongMethod(_jni_scanner_obj, _jni_scanner_get_next_batch);
    }
    RETURN_ERROR_IF_EXC(env);
    if (meta_address == 0) {
        // Address == 0 when there's no data in scanner
        *read_rows = 0;
        *eof = true;
        return Status::OK();
    }
    _set_meta(meta_address);
    long num_rows = _table_meta.next_meta_as_long();
    if (num_rows == 0) {
        *read_rows = 0;
        *eof = true;
        return Status::OK();
    }
    RETURN_IF_ERROR(_fill_block(block, num_rows));
    *read_rows = num_rows;
    *eof = false;
    env->CallVoidMethod(_jni_scanner_obj, _jni_scanner_release_table);
    RETURN_ERROR_IF_EXC(env);
    _has_read += num_rows;
    return Status::OK();
}

Status JniConnector::get_table_schema(std::string& table_schema_str) {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));

    jstring jstr = (jstring)env->CallObjectMethod(_jni_scanner_obj, _jni_scanner_get_table_schema);
    RETURN_ERROR_IF_EXC(env);

    const char* cstr = env->GetStringUTFChars(jstr, nullptr);
    RETURN_ERROR_IF_EXC(env);

    if (cstr == nullptr) {
        return Status::RuntimeError("GetStringUTFChars returned null");
    }

    table_schema_str = std::string(cstr); // copy to std::string
    env->ReleaseStringUTFChars(jstr, cstr);
    env->DeleteLocalRef(jstr);
    return Status::OK();
}

Status JniConnector::get_statistics(JNIEnv* env, std::map<std::string, std::string>* result) {
    result->clear();
    jobject metrics = env->CallObjectMethod(_jni_scanner_obj, _jni_scanner_get_statistics);
    jthrowable exc = (env)->ExceptionOccurred();
    if (exc != nullptr) {
        LOG(WARNING) << "get_statistics has error: "
                     << JniUtil::GetJniExceptionMsg(env).to_string();
        env->DeleteLocalRef(metrics);
        return Status::OK();
    }
    RETURN_IF_ERROR(JniUtil::convert_to_cpp_map(env, metrics, result));
    env->DeleteLocalRef(metrics);
    return Status::OK();
}

Status JniConnector::close() {
    if (!_closed) {
        JNIEnv* env = nullptr;
        RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
        if (_scanner_opened && _jni_scanner_obj != nullptr) {
            COUNTER_UPDATE(_open_scanner_time, _jni_scanner_open_watcher);
            COUNTER_UPDATE(_fill_block_time, _fill_block_watcher);

            RETURN_ERROR_IF_EXC(env);
            int64_t _append = (int64_t)env->CallLongMethod(_jni_scanner_obj,
                                                           _jni_scanner_get_append_data_time);
            RETURN_ERROR_IF_EXC(env);
            COUNTER_UPDATE(_java_append_data_time, _append);

            int64_t _create = (int64_t)env->CallLongMethod(
                    _jni_scanner_obj, _jni_scanner_get_create_vector_table_time);
            RETURN_ERROR_IF_EXC(env);
            COUNTER_UPDATE(_java_create_vector_table_time, _create);

            COUNTER_UPDATE(_java_scan_time, _java_scan_watcher - _append - _create);

            _max_time_split_weight_counter->conditional_update(
                    _jni_scanner_open_watcher + _fill_block_watcher + _java_scan_watcher,
                    _self_split_weight);

            // _fill_block may be failed and returned, we should release table in close.
            // org.apache.doris.common.jni.JniScanner#releaseTable is idempotent
            env->CallVoidMethod(_jni_scanner_obj, _jni_scanner_release_table);
            RETURN_ERROR_IF_EXC(env);
            env->CallVoidMethod(_jni_scanner_obj, _jni_scanner_close);
            RETURN_ERROR_IF_EXC(env);
            env->DeleteGlobalRef(_jni_scanner_obj);
            RETURN_ERROR_IF_EXC(env);
        }
        if (_jni_scanner_cls != nullptr) {
            // _jni_scanner_cls may be null if init connector failed
            env->DeleteGlobalRef(_jni_scanner_cls);
        }
        _closed = true;
        jthrowable exc = (env)->ExceptionOccurred();
        if (exc != nullptr) {
            // Ensure successful resource release
            throw Exception(Status::FatalError("Failed to release jni resource: {}",
                                               JniUtil::GetJniExceptionMsg(env).to_string()));
        }
    }
    return Status::OK();
}

Status JniConnector::_init_jni_scanner(JNIEnv* env, int batch_size) {
    RETURN_IF_ERROR(
            JniUtil::get_jni_scanner_class(env, _connector_class.c_str(), &_jni_scanner_cls));
    if (_jni_scanner_cls == nullptr) [[unlikely]] {
        if (env->ExceptionOccurred()) {
            env->ExceptionDescribe();
        }
        return Status::InternalError("Fail to get JniScanner class.");
    }
    RETURN_ERROR_IF_EXC(env);

    jmethodID scanner_constructor =
            env->GetMethodID(_jni_scanner_cls, "<init>", "(ILjava/util/Map;)V");
    RETURN_ERROR_IF_EXC(env);

    // prepare constructor parameters
    jobject hashmap_object;
    RETURN_IF_ERROR(JniUtil::convert_to_java_map(env, _scanner_params, &hashmap_object));
    jobject jni_scanner_obj =
            env->NewObject(_jni_scanner_cls, scanner_constructor, batch_size, hashmap_object);

    RETURN_ERROR_IF_EXC(env);

    // prepare constructor parameters
    env->DeleteGlobalRef(hashmap_object);
    RETURN_ERROR_IF_EXC(env);

    _jni_scanner_open = env->GetMethodID(_jni_scanner_cls, "open", "()V");
    RETURN_ERROR_IF_EXC(env);
    _jni_scanner_get_next_batch = env->GetMethodID(_jni_scanner_cls, "getNextBatchMeta", "()J");
    RETURN_ERROR_IF_EXC(env);
    _jni_scanner_get_append_data_time =
            env->GetMethodID(_jni_scanner_cls, "getAppendDataTime", "()J");
    RETURN_ERROR_IF_EXC(env);
    _jni_scanner_get_create_vector_table_time =
            env->GetMethodID(_jni_scanner_cls, "getCreateVectorTableTime", "()J");
    RETURN_ERROR_IF_EXC(env);
    _jni_scanner_get_table_schema =
            env->GetMethodID(_jni_scanner_cls, "getTableSchema", "()Ljava/lang/String;");
    RETURN_ERROR_IF_EXC(env);
    _jni_scanner_close = env->GetMethodID(_jni_scanner_cls, "close", "()V");
    _jni_scanner_release_column = env->GetMethodID(_jni_scanner_cls, "releaseColumn", "(I)V");
    _jni_scanner_release_table = env->GetMethodID(_jni_scanner_cls, "releaseTable", "()V");
    _jni_scanner_get_statistics =
            env->GetMethodID(_jni_scanner_cls, "getStatistics", "()Ljava/util/Map;");
    RETURN_ERROR_IF_EXC(env);
    RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, jni_scanner_obj, &_jni_scanner_obj));
    env->DeleteLocalRef(jni_scanner_obj);
    RETURN_ERROR_IF_EXC(env);
    return Status::OK();
}

Status JniConnector::fill_block(Block* block, const ColumnNumbers& arguments, long table_address) {
    if (table_address == 0) {
        return Status::InternalError("table_address is 0");
    }
    TableMetaAddress table_meta(table_address);
    long num_rows = table_meta.next_meta_as_long();
    for (size_t i : arguments) {
        if (block->get_by_position(i).column.get() == nullptr) {
            auto return_type = block->get_data_type(i);
            bool result_nullable = return_type->is_nullable();
            ColumnUInt8::MutablePtr null_col = nullptr;
            if (result_nullable) {
                return_type = remove_nullable(return_type);
                null_col = ColumnUInt8::create();
            }
            auto res_col = return_type->create_column();
            if (result_nullable) {
                block->replace_by_position(
                        i, ColumnNullable::create(std::move(res_col), std::move(null_col)));
            } else {
                block->replace_by_position(i, std::move(res_col));
            }
        } else if (is_column_const(*(block->get_by_position(i).column))) {
            auto doris_column = block->get_by_position(i).column->convert_to_full_column_if_const();
            bool is_nullable = block->get_by_position(i).type->is_nullable();
            block->replace_by_position(i, is_nullable ? make_nullable(doris_column) : doris_column);
        }
        auto& column_with_type_and_name = block->get_by_position(i);
        auto& column_ptr = column_with_type_and_name.column;
        auto& column_type = column_with_type_and_name.type;
        RETURN_IF_ERROR(_fill_column(table_meta, column_ptr, column_type, num_rows));
    }
    return Status::OK();
}

Status JniConnector::_fill_block(Block* block, size_t num_rows) {
    SCOPED_RAW_TIMER(&_fill_block_watcher);
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    for (int i = 0; i < _column_names.size(); ++i) {
        auto& column_with_type_and_name = block->get_by_name(_column_names[i]);
        auto& column_ptr = column_with_type_and_name.column;
        auto& column_type = column_with_type_and_name.type;
        RETURN_IF_ERROR(_fill_column(_table_meta, column_ptr, column_type, num_rows));
        // Column is not released when _fill_column failed. It will be released when releasing table.
        env->CallVoidMethod(_jni_scanner_obj, _jni_scanner_release_column, i);
        RETURN_ERROR_IF_EXC(env);
    }
    return Status::OK();
}

Status JniConnector::_fill_column(TableMetaAddress& address, ColumnPtr& doris_column,
                                  DataTypePtr& data_type, size_t num_rows) {
    auto logical_type = data_type->get_primitive_type();
    void* null_map_ptr = address.next_meta_as_ptr();
    if (null_map_ptr == nullptr) {
        // org.apache.doris.common.jni.vec.ColumnType.Type#UNSUPPORTED will set column address as 0
        return Status::InternalError("Unsupported type {} in java side", data_type->get_name());
    }
    MutableColumnPtr data_column;
    if (doris_column->is_nullable()) {
        auto* nullable_column =
                reinterpret_cast<vectorized::ColumnNullable*>(doris_column->assume_mutable().get());
        data_column = nullable_column->get_nested_column_ptr();
        NullMap& null_map = nullable_column->get_null_map_data();
        size_t origin_size = null_map.size();
        null_map.resize(origin_size + num_rows);
        memcpy(null_map.data() + origin_size, static_cast<bool*>(null_map_ptr), num_rows);
    } else {
        data_column = doris_column->assume_mutable();
    }
    // Date and DateTime are deprecated and not supported.
    switch (logical_type) {
#define DISPATCH(TYPE_INDEX, COLUMN_TYPE, CPP_TYPE)              \
    case TYPE_INDEX:                                             \
        return _fill_fixed_length_column<COLUMN_TYPE, CPP_TYPE>( \
                data_column, reinterpret_cast<CPP_TYPE*>(address.next_meta_as_ptr()), num_rows);
        FOR_FIXED_LENGTH_TYPES(DISPATCH)
#undef DISPATCH
    case PrimitiveType::TYPE_STRING:
        [[fallthrough]];
    case PrimitiveType::TYPE_CHAR:
        [[fallthrough]];
    case PrimitiveType::TYPE_VARCHAR:
        return _fill_string_column(address, data_column, num_rows);
    case PrimitiveType::TYPE_ARRAY:
        return _fill_array_column(address, data_column, data_type, num_rows);
    case PrimitiveType::TYPE_MAP:
        return _fill_map_column(address, data_column, data_type, num_rows);
    case PrimitiveType::TYPE_STRUCT:
        return _fill_struct_column(address, data_column, data_type, num_rows);
    default:
        return Status::InvalidArgument("Unsupported type {} in jni scanner", data_type->get_name());
    }
    return Status::OK();
}

Status JniConnector::_fill_string_column(TableMetaAddress& address, MutableColumnPtr& doris_column,
                                         size_t num_rows) {
    auto& string_col = static_cast<const ColumnString&>(*doris_column);
    ColumnString::Chars& string_chars = const_cast<ColumnString::Chars&>(string_col.get_chars());
    ColumnString::Offsets& string_offsets =
            const_cast<ColumnString::Offsets&>(string_col.get_offsets());
    int* offsets = reinterpret_cast<int*>(address.next_meta_as_ptr());
    char* chars = reinterpret_cast<char*>(address.next_meta_as_ptr());

    // This judgment is necessary, otherwise the following statement `offsets[num_rows - 1]` out of bounds
    // What's more, This judgment must be placed after `address.next_meta_as_ptr()`
    // because `address.next_meta_as_ptr` will make `address._meta_index` plus 1
    if (num_rows == 0) {
        return Status::OK();
    }

    size_t origin_chars_size = string_chars.size();
    string_chars.resize(origin_chars_size + offsets[num_rows - 1]);
    memcpy(string_chars.data() + origin_chars_size, chars, offsets[num_rows - 1]);

    size_t origin_offsets_size = string_offsets.size();
    size_t start_offset = string_offsets[origin_offsets_size - 1];
    string_offsets.resize(origin_offsets_size + num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        string_offsets[origin_offsets_size + i] = offsets[i] + start_offset;
    }
    return Status::OK();
}

Status JniConnector::_fill_array_column(TableMetaAddress& address, MutableColumnPtr& doris_column,
                                        DataTypePtr& data_type, size_t num_rows) {
    ColumnPtr& element_column = static_cast<ColumnArray&>(*doris_column).get_data_ptr();
    DataTypePtr& element_type = const_cast<DataTypePtr&>(
            (assert_cast<const DataTypeArray*>(remove_nullable(data_type).get()))
                    ->get_nested_type());
    ColumnArray::Offsets64& offsets_data = static_cast<ColumnArray&>(*doris_column).get_offsets();

    int64_t* offsets = reinterpret_cast<int64_t*>(address.next_meta_as_ptr());
    size_t origin_size = offsets_data.size();
    offsets_data.resize(origin_size + num_rows);
    size_t start_offset = offsets_data[origin_size - 1];
    for (size_t i = 0; i < num_rows; ++i) {
        offsets_data[origin_size + i] = offsets[i] + start_offset;
    }

    // offsets[num_rows - 1] == offsets_data[origin_size + num_rows - 1] - start_offset
    // but num_row equals 0 when there are all empty arrays
    return _fill_column(address, element_column, element_type,
                        offsets_data[origin_size + num_rows - 1] - start_offset);
}

Status JniConnector::_fill_map_column(TableMetaAddress& address, MutableColumnPtr& doris_column,
                                      DataTypePtr& data_type, size_t num_rows) {
    auto& map = static_cast<ColumnMap&>(*doris_column);
    DataTypePtr& key_type = const_cast<DataTypePtr&>(
            reinterpret_cast<const DataTypeMap*>(remove_nullable(data_type).get())->get_key_type());
    DataTypePtr& value_type = const_cast<DataTypePtr&>(
            reinterpret_cast<const DataTypeMap*>(remove_nullable(data_type).get())
                    ->get_value_type());
    ColumnPtr& key_column = map.get_keys_ptr();
    ColumnPtr& value_column = map.get_values_ptr();
    ColumnArray::Offsets64& map_offsets = map.get_offsets();

    int64_t* offsets = reinterpret_cast<int64_t*>(address.next_meta_as_ptr());
    size_t origin_size = map_offsets.size();
    map_offsets.resize(origin_size + num_rows);
    size_t start_offset = map_offsets[origin_size - 1];
    for (size_t i = 0; i < num_rows; ++i) {
        map_offsets[origin_size + i] = offsets[i] + start_offset;
    }

    RETURN_IF_ERROR(_fill_column(address, key_column, key_type,
                                 map_offsets[origin_size + num_rows - 1] - start_offset));
    return _fill_column(address, value_column, value_type,
                        map_offsets[origin_size + num_rows - 1] - start_offset);
}

Status JniConnector::_fill_struct_column(TableMetaAddress& address, MutableColumnPtr& doris_column,
                                         DataTypePtr& data_type, size_t num_rows) {
    auto& doris_struct = static_cast<ColumnStruct&>(*doris_column);
    const DataTypeStruct* doris_struct_type =
            reinterpret_cast<const DataTypeStruct*>(remove_nullable(data_type).get());
    for (int i = 0; i < doris_struct.tuple_size(); ++i) {
        ColumnPtr& struct_field = doris_struct.get_column_ptr(i);
        DataTypePtr& field_type = const_cast<DataTypePtr&>(doris_struct_type->get_element(i));
        RETURN_IF_ERROR(_fill_column(address, struct_field, field_type, num_rows));
    }
    return Status::OK();
}

void JniConnector::_generate_predicates(
        const std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    if (colname_to_value_range == nullptr) {
        return;
    }
    for (auto& kv : *colname_to_value_range) {
        const std::string& column_name = kv.first;
        const ColumnValueRangeType& col_val_range = kv.second;
        std::visit([&](auto&& range) { _parse_value_range(range, column_name); }, col_val_range);
    }
}

std::string JniConnector::get_jni_type(const DataTypePtr& data_type) {
    DataTypePtr type = remove_nullable(data_type);
    std::ostringstream buffer;
    switch (type->get_primitive_type()) {
    case TYPE_BOOLEAN:
        return "boolean";
    case TYPE_TINYINT:
        return "tinyint";
    case TYPE_SMALLINT:
        return "smallint";
    case TYPE_INT:
        return "int";
    case TYPE_BIGINT:
        return "bigint";
    case TYPE_LARGEINT:
        return "largeint";
    case TYPE_FLOAT:
        return "float";
    case TYPE_DOUBLE:
        return "double";
    case TYPE_IPV4:
        return "ipv4";
    case TYPE_IPV6:
        return "ipv6";
    case TYPE_VARCHAR:
        [[fallthrough]];
    case TYPE_CHAR:
        [[fallthrough]];
    case TYPE_STRING:
        return "string";
    case TYPE_DATE:
        return "datev1";
    case TYPE_DATEV2:
        return "datev2";
    case TYPE_DATETIME:
        return "datetimev1";
    case TYPE_DATETIMEV2:
        [[fallthrough]];
    case TYPE_TIMEV2: {
        buffer << "datetimev2(" << type->get_scale() << ")";
        return buffer.str();
    }
    case TYPE_BINARY:
        return "binary";
    case TYPE_DECIMALV2: {
        buffer << "decimalv2(" << DecimalV2Value::PRECISION << "," << DecimalV2Value::SCALE << ")";
        return buffer.str();
    }
    case TYPE_DECIMAL32: {
        buffer << "decimal32(" << type->get_precision() << "," << type->get_scale() << ")";
        return buffer.str();
    }
    case TYPE_DECIMAL64: {
        buffer << "decimal64(" << type->get_precision() << "," << type->get_scale() << ")";
        return buffer.str();
    }
    case TYPE_DECIMAL128I: {
        buffer << "decimal128(" << type->get_precision() << "," << type->get_scale() << ")";
        return buffer.str();
    }
    case TYPE_STRUCT: {
        const DataTypeStruct* struct_type = reinterpret_cast<const DataTypeStruct*>(type.get());
        buffer << "struct<";
        for (int i = 0; i < struct_type->get_elements().size(); ++i) {
            if (i != 0) {
                buffer << ",";
            }
            buffer << struct_type->get_element_names()[i] << ":"
                   << get_jni_type(struct_type->get_element(i));
        }
        buffer << ">";
        return buffer.str();
    }
    case TYPE_ARRAY: {
        const DataTypeArray* array_type = reinterpret_cast<const DataTypeArray*>(type.get());
        buffer << "array<" << get_jni_type(array_type->get_nested_type()) << ">";
        return buffer.str();
    }
    case TYPE_MAP: {
        const DataTypeMap* map_type = reinterpret_cast<const DataTypeMap*>(type.get());
        buffer << "map<" << get_jni_type(map_type->get_key_type()) << ","
               << get_jni_type(map_type->get_value_type()) << ">";
        return buffer.str();
    }
    default:
        return "unsupported";
    }
}

std::string JniConnector::get_jni_type_with_different_string(const DataTypePtr& data_type) {
    DataTypePtr type = remove_nullable(data_type);
    std::ostringstream buffer;
    switch (data_type->get_primitive_type()) {
    case TYPE_BOOLEAN:
        return "boolean";
    case TYPE_TINYINT:
        return "tinyint";
    case TYPE_SMALLINT:
        return "smallint";
    case TYPE_INT:
        return "int";
    case TYPE_BIGINT:
        return "bigint";
    case TYPE_LARGEINT:
        return "largeint";
    case TYPE_FLOAT:
        return "float";
    case TYPE_DOUBLE:
        return "double";
    case TYPE_IPV4:
        return "ipv4";
    case TYPE_IPV6:
        return "ipv6";
    case TYPE_VARCHAR: {
        buffer << "varchar("
               << assert_cast<const DataTypeString*>(remove_nullable(data_type).get())->len()
               << ")";
        return buffer.str();
    }
    case TYPE_DATE:
        return "datev1";
    case TYPE_DATEV2:
        return "datev2";
    case TYPE_DATETIME:
        return "datetimev1";
    case TYPE_DATETIMEV2:
        [[fallthrough]];
    case TYPE_TIMEV2: {
        buffer << "datetimev2(" << data_type->get_scale() << ")";
        return buffer.str();
    }
    case TYPE_BINARY:
        return "binary";
    case TYPE_CHAR: {
        buffer << "char("
               << assert_cast<const DataTypeString*>(remove_nullable(data_type).get())->len()
               << ")";
        return buffer.str();
    }
    case TYPE_STRING:
        return "string";
    case TYPE_DECIMALV2: {
        buffer << "decimalv2(" << DecimalV2Value::PRECISION << "," << DecimalV2Value::SCALE << ")";
        return buffer.str();
    }
    case TYPE_DECIMAL32: {
        buffer << "decimal32(" << data_type->get_precision() << "," << data_type->get_scale()
               << ")";
        return buffer.str();
    }
    case TYPE_DECIMAL64: {
        buffer << "decimal64(" << data_type->get_precision() << "," << data_type->get_scale()
               << ")";
        return buffer.str();
    }
    case TYPE_DECIMAL128I: {
        buffer << "decimal128(" << data_type->get_precision() << "," << data_type->get_scale()
               << ")";
        return buffer.str();
    }
    case TYPE_STRUCT: {
        const auto* type_struct =
                assert_cast<const DataTypeStruct*>(remove_nullable(data_type).get());
        buffer << "struct<";
        for (int i = 0; i < type_struct->get_elements().size(); ++i) {
            if (i != 0) {
                buffer << ",";
            }
            buffer << type_struct->get_element_name(i) << ":"
                   << get_jni_type_with_different_string(type_struct->get_element(i));
        }
        buffer << ">";
        return buffer.str();
    }
    case TYPE_ARRAY: {
        const auto* type_arr = assert_cast<const DataTypeArray*>(remove_nullable(data_type).get());
        buffer << "array<" << get_jni_type_with_different_string(type_arr->get_nested_type())
               << ">";
        return buffer.str();
    }
    case TYPE_MAP: {
        const auto* type_map = assert_cast<const DataTypeMap*>(remove_nullable(data_type).get());
        buffer << "map<" << get_jni_type_with_different_string(type_map->get_key_type()) << ","
               << get_jni_type_with_different_string(type_map->get_value_type()) << ">";
        return buffer.str();
    }
    default:
        return "unsupported";
    }
}

Status JniConnector::_fill_column_meta(const ColumnPtr& doris_column, const DataTypePtr& data_type,
                                       std::vector<long>& meta_data) {
    auto logical_type = data_type->get_primitive_type();
    const IColumn* column = nullptr;
    // insert const flag
    if (is_column_const(*doris_column)) {
        meta_data.emplace_back((long)1);
        const auto& const_column = assert_cast<const ColumnConst&>(*doris_column);
        column = &(const_column.get_data_column());
    } else {
        meta_data.emplace_back((long)0);
        column = &(*doris_column);
    }

    // insert null map address
    const IColumn* data_column = nullptr;
    if (column->is_nullable()) {
        const auto& nullable_column = assert_cast<const vectorized::ColumnNullable&>(*column);
        data_column = &(nullable_column.get_nested_column());
        const auto& null_map = nullable_column.get_null_map_data();
        meta_data.emplace_back((long)null_map.data());
    } else {
        meta_data.emplace_back(0);
        data_column = column;
    }
    switch (logical_type) {
#define DISPATCH(TYPE_INDEX, COLUMN_TYPE, CPP_TYPE)                                          \
    case TYPE_INDEX: {                                                                       \
        meta_data.emplace_back(_get_fixed_length_column_address<COLUMN_TYPE>(*data_column)); \
        break;                                                                               \
    }
        FOR_FIXED_LENGTH_TYPES(DISPATCH)
#undef DISPATCH
    case PrimitiveType::TYPE_STRING:
        [[fallthrough]];
    case PrimitiveType::TYPE_CHAR:
        [[fallthrough]];
    case PrimitiveType::TYPE_VARCHAR: {
        const auto& string_column = assert_cast<const ColumnString&>(*data_column);
        // inert offsets
        meta_data.emplace_back((long)string_column.get_offsets().data());
        meta_data.emplace_back((long)string_column.get_chars().data());
        break;
    }
    case PrimitiveType::TYPE_ARRAY: {
        const auto& element_column = assert_cast<const ColumnArray&>(*data_column).get_data_ptr();
        meta_data.emplace_back(
                (long)assert_cast<const ColumnArray&>(*data_column).get_offsets().data());
        const auto& element_type = assert_cast<const DataTypePtr&>(
                (assert_cast<const DataTypeArray*>(remove_nullable(data_type).get()))
                        ->get_nested_type());
        RETURN_IF_ERROR(_fill_column_meta(element_column, element_type, meta_data));
        break;
    }
    case PrimitiveType::TYPE_STRUCT: {
        const auto& doris_struct = assert_cast<const ColumnStruct&>(*data_column);
        const auto* doris_struct_type =
                assert_cast<const DataTypeStruct*>(remove_nullable(data_type).get());
        for (int i = 0; i < doris_struct.tuple_size(); ++i) {
            const auto& struct_field = doris_struct.get_column_ptr(i);
            const auto& field_type =
                    assert_cast<const DataTypePtr&>(doris_struct_type->get_element(i));
            RETURN_IF_ERROR(_fill_column_meta(struct_field, field_type, meta_data));
        }
        break;
    }
    case PrimitiveType::TYPE_MAP: {
        const auto& map = assert_cast<const ColumnMap&>(*data_column);
        const auto& key_type = assert_cast<const DataTypePtr&>(
                assert_cast<const DataTypeMap*>(remove_nullable(data_type).get())->get_key_type());
        const auto& value_type = assert_cast<const DataTypePtr&>(
                assert_cast<const DataTypeMap*>(remove_nullable(data_type).get())
                        ->get_value_type());
        const auto& key_column = map.get_keys_ptr();
        const auto& value_column = map.get_values_ptr();
        meta_data.emplace_back((long)map.get_offsets().data());
        RETURN_IF_ERROR(_fill_column_meta(key_column, key_type, meta_data));
        RETURN_IF_ERROR(_fill_column_meta(value_column, value_type, meta_data));
        break;
    }
    default:
        return Status::InternalError("Unsupported type: {}", data_type->get_name());
    }
    return Status::OK();
}

Status JniConnector::to_java_table(Block* block, std::unique_ptr<long[]>& meta) {
    ColumnNumbers arguments;
    for (size_t i = 0; i < block->columns(); ++i) {
        arguments.emplace_back(i);
    }
    return to_java_table(block, block->rows(), arguments, meta);
}

Status JniConnector::to_java_table(Block* block, size_t num_rows, const ColumnNumbers& arguments,
                                   std::unique_ptr<long[]>& meta) {
    std::vector<long> meta_data;
    // insert number of rows
    meta_data.emplace_back(num_rows);
    for (size_t i : arguments) {
        auto& column_with_type_and_name = block->get_by_position(i);
        RETURN_IF_ERROR(_fill_column_meta(column_with_type_and_name.column,
                                          column_with_type_and_name.type, meta_data));
    }

    meta.reset(new long[meta_data.size()]);
    memcpy(meta.get(), &meta_data[0], meta_data.size() * 8);
    return Status::OK();
}

std::pair<std::string, std::string> JniConnector::parse_table_schema(Block* block,
                                                                     const ColumnNumbers& arguments,
                                                                     bool ignore_column_name) {
    // prepare table schema
    std::ostringstream required_fields;
    std::ostringstream columns_types;
    for (int i = 0; i < arguments.size(); ++i) {
        // column name maybe empty or has special characters
        // std::string field = block->get_by_position(i).name;
        std::string type = JniConnector::get_jni_type(block->get_by_position(arguments[i]).type);
        if (i == 0) {
            if (ignore_column_name) {
                required_fields << "_col_" << arguments[i];
            } else {
                required_fields << block->get_by_position(arguments[i]).name;
            }
            columns_types << type;
        } else {
            if (ignore_column_name) {
                required_fields << ","
                                << "_col_" << arguments[i];
            } else {
                required_fields << "," << block->get_by_position(arguments[i]).name;
            }
            columns_types << "#" << type;
        }
    }
    return std::make_pair(required_fields.str(), columns_types.str());
}

std::pair<std::string, std::string> JniConnector::parse_table_schema(Block* block) {
    ColumnNumbers arguments;
    for (size_t i = 0; i < block->columns(); ++i) {
        arguments.emplace_back(i);
    }
    return parse_table_schema(block, arguments, true);
}

void JniConnector::_collect_profile_before_close() {
    if (_scanner_opened && _profile != nullptr) {
        JNIEnv* env = nullptr;
        Status st = JniUtil::GetJNIEnv(&env);
        if (!st) {
            LOG(WARNING) << "failed to get jni env when collect profile: " << st;
            return;
        }
        // update scanner metrics
        std::map<std::string, std::string> statistics_result;
        st = get_statistics(env, &statistics_result);
        if (!st) {
            LOG(WARNING) << "failed to get_statistics when collect profile: " << st;
            return;
        }

        for (const auto& metric : statistics_result) {
            std::vector<std::string> type_and_name = split(metric.first, ":");
            if (type_and_name.size() != 2) {
                LOG(WARNING) << "Name of JNI Scanner metric should be pattern like "
                             << "'metricType:metricName'";
                continue;
            }
            long metric_value = std::stol(metric.second);
            RuntimeProfile::Counter* scanner_counter;
            if (type_and_name[0] == "timer") {
                scanner_counter =
                        ADD_CHILD_TIMER(_profile, type_and_name[1], _connector_name.c_str());
            } else if (type_and_name[0] == "counter") {
                scanner_counter = ADD_CHILD_COUNTER(_profile, type_and_name[1], TUnit::UNIT,
                                                    _connector_name.c_str());
            } else if (type_and_name[0] == "bytes") {
                scanner_counter = ADD_CHILD_COUNTER(_profile, type_and_name[1], TUnit::BYTES,
                                                    _connector_name.c_str());
            } else {
                LOG(WARNING) << "Type of JNI Scanner metric should be timer, counter or bytes";
                continue;
            }
            COUNTER_UPDATE(scanner_counter, metric_value);
        }
    }
}
} // namespace doris::vectorized
