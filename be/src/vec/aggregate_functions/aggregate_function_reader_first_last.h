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

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_variant.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <typename ColVecType, bool arg_is_nullable>
struct Value {
public:
    bool is_null() const {
        if (_ptr == nullptr) {
            return true;
        }
        if constexpr (arg_is_nullable) {
            return assert_cast<const ColumnNullable*, TypeCheckOnRelease::DISABLE>(_ptr)
                    ->is_null_at(_offset);
        }
        return false;
    }

    void insert_into(IColumn& to) const {
        if constexpr (arg_is_nullable) {
            const auto* col = assert_cast<const ColumnNullable*, TypeCheckOnRelease::DISABLE>(_ptr);
            assert_cast<ColVecType&, TypeCheckOnRelease::DISABLE>(to).insert_from(
                    col->get_nested_column(), _offset);
        } else {
            assert_cast<ColVecType&, TypeCheckOnRelease::DISABLE>(to).insert_from(*_ptr, _offset);
        }
    }

    void set_value(const IColumn* column, size_t row) {
        _ptr = column;
        _offset = row;
    }

    void reset() {
        _ptr = nullptr;
        _offset = 0;
    }

protected:
    const IColumn* _ptr = nullptr;
    size_t _offset = 0;
};

template <typename ColVecType, bool arg_is_nullable>
struct CopiedValue : public Value<ColVecType, arg_is_nullable> {
public:
    void insert_into(IColumn& to) const {
        assert_cast<ColVecType&, TypeCheckOnRelease::DISABLE>(to).insert(_copied_value);
    }

    bool is_null() const { return this->_ptr == nullptr; }

    void set_value(const IColumn* column, size_t row) {
        // here _ptr, maybe null at row, so call reset to set nullptr
        // But we will use is_null() check first, others have set _ptr column to a meaningless address
        // because the address have meaningless, only need it to check is nullptr
        this->_ptr = (IColumn*)0x00000001;
        if constexpr (arg_is_nullable) {
            const auto* col =
                    assert_cast<const ColumnNullable*, TypeCheckOnRelease::DISABLE>(column);
            if (col->is_null_at(row)) {
                this->reset();
                return;
            } else {
                auto& nested_col = assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(
                        col->get_nested_column());
                nested_col.get(row, _copied_value);
            }
        } else {
            column->get(row, _copied_value);
        }
    }

private:
    Field _copied_value;
};

template <typename ColVecType, bool result_is_nullable, bool arg_is_nullable, bool is_copy>
struct ReaderFirstAndLastData {
public:
    using StoreType = std::conditional_t<is_copy, CopiedValue<ColVecType, arg_is_nullable>,
                                         Value<ColVecType, arg_is_nullable>>;
    static constexpr bool nullable = arg_is_nullable;
    static constexpr bool result_nullable = result_is_nullable;

    void reset() {
        _data_value.reset();
        _has_value = false;
    }

    void insert_result_into(IColumn& to) const {
        if constexpr (result_is_nullable) {
            if (_data_value.is_null()) { //_ptr == nullptr || null data at row
                auto& col = assert_cast<ColumnNullable&>(to);
                col.insert_default();
            } else {
                auto& col = assert_cast<ColumnNullable&>(to);
                col.get_null_map_data().push_back(0);
                _data_value.insert_into(col.get_nested_column());
            }
        } else {
            _data_value.insert_into(to);
        }
    }

    // here not check the columns[0] is null at the row,
    // but it is need to check in other
    void set_value(const IColumn** columns, size_t pos) {
        _data_value.set_value(columns[0], pos);
        _has_value = true;
    }

    bool has_set_value() { return _has_value; }

    bool is_null() { return _data_value.is_null(); }

protected:
    StoreType _data_value;
    bool _has_value = false;
};

template <typename ColVecType, bool result_is_nullable, bool arg_is_nullable, bool is_copy>
struct ReaderFunctionFirstData
        : ReaderFirstAndLastData<ColVecType, result_is_nullable, arg_is_nullable, is_copy> {
    void add(int64_t row, const IColumn** columns) {
        if (this->has_set_value()) {
            return;
        }
        this->set_value(columns, row);
    }
    static const char* name() { return "first_value"; }
};

template <typename ColVecType, bool result_is_nullable, bool arg_is_nullable, bool is_copy>
struct ReaderFunctionFirstNonNullData
        : ReaderFirstAndLastData<ColVecType, result_is_nullable, arg_is_nullable, is_copy> {
    void add(int64_t row, const IColumn** columns) {
        if (this->has_set_value()) {
            return;
        }
        if constexpr (ReaderFirstAndLastData<ColVecType, result_is_nullable, arg_is_nullable,
                                             is_copy>::nullable) {
            const auto* nullable_column =
                    assert_cast<const ColumnNullable*, TypeCheckOnRelease::DISABLE>(columns[0]);
            if (nullable_column->is_null_at(row)) {
                return;
            }
        }
        this->set_value(columns, row);
    }
    static const char* name() { return "first_non_null_value"; }
};

template <typename ColVecType, bool result_is_nullable, bool arg_is_nullable, bool is_copy>
struct ReaderFunctionLastData
        : ReaderFirstAndLastData<ColVecType, result_is_nullable, arg_is_nullable, is_copy> {
    void add(int64_t row, const IColumn** columns) { this->set_value(columns, row); }
    static const char* name() { return "last_value"; }
};

template <typename ColVecType, bool result_is_nullable, bool arg_is_nullable, bool is_copy>
struct ReaderFunctionLastNonNullData
        : ReaderFirstAndLastData<ColVecType, result_is_nullable, arg_is_nullable, is_copy> {
    void add(int64_t row, const IColumn** columns) {
        if constexpr (ReaderFirstAndLastData<ColVecType, result_is_nullable, arg_is_nullable,
                                             is_copy>::nullable) {
            const auto* nullable_column =
                    assert_cast<const ColumnNullable*, TypeCheckOnRelease::DISABLE>(columns[0]);
            if (nullable_column->is_null_at(row)) {
                return;
            }
        }
        this->set_value(columns, row);
    }

    static const char* name() { return "last_non_null_value"; }
};

template <typename Data>
class ReaderFunctionData final
        : public IAggregateFunctionDataHelper<Data, ReaderFunctionData<Data>> {
public:
    ReaderFunctionData(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, ReaderFunctionData<Data>>(argument_types_),
              _argument_type(argument_types_[0]) {}

    String get_name() const override { return Data::name(); }

    DataTypePtr get_return_type() const override {
        if constexpr (Data::result_nullable) {
            return make_nullable(_argument_type);
        } else {
            return _argument_type;
        }
    }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }

    void add(AggregateDataPtr place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        this->data(place).add(row_num, columns);
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena& arena, UInt8*, UInt8*) const override {
        throw doris::Exception(
                Status::FatalError("ReaderFunctionData do not support add_range_single_place"));
    }
    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena&) const override {
        throw doris::Exception(Status::FatalError("ReaderFunctionData do not support merge"));
    }
    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {
        throw doris::Exception(Status::FatalError("ReaderFunctionData do not support serialize"));
    }
    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena&) const override {
        throw doris::Exception(Status::FatalError("ReaderFunctionData do not support deserialize"));
    }

private:
    DataTypePtr _argument_type;
};

template <template <typename, bool, bool, bool> class FunctionData, bool result_is_nullable,
          bool arg_is_nullable, bool is_copy = false>
AggregateFunctionPtr create_function_single_value(const String& name,
                                                  const DataTypes& argument_types) {
    switch (argument_types[0]->get_primitive_type()) {
    case PrimitiveType::TYPE_BOOLEAN: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnUInt8, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_TINYINT: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnInt8, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_SMALLINT: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnInt16, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_INT: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnInt32, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_BIGINT: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnInt64, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_LARGEINT: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnInt128, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_FLOAT: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnFloat32, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_DOUBLE: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnFloat64, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_DECIMAL32: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnDecimal32, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_DECIMAL64: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnDecimal64, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_DECIMAL128I: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnDecimal128V3, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_DECIMALV2: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnDecimal128V2, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_DECIMAL256: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnDecimal256, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_STRING:
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_JSONB: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnString, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_DATE: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnDate, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_DATETIME: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnDateTime, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_DATETIMEV2: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnDateTimeV2, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_DATEV2: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnDateV2, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_IPV4: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnIPv4, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_IPV6: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnIPv6, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_ARRAY: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnArray, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_MAP: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnMap, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_STRUCT: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnStruct, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_VARIANT: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnVariant, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_BITMAP: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnBitmap, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_HLL: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnHLL, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    case PrimitiveType::TYPE_QUANTILE_STATE: {
        return std::make_shared<ReaderFunctionData<
                FunctionData<ColumnQuantileState, result_is_nullable, arg_is_nullable, is_copy>>>(
                argument_types);
    }
    default:
        LOG(WARNING) << "with unknowed type, failed in  create_aggregate_function_" << name
                     << " and type is: " << argument_types[0]->get_name();
        return nullptr;
    }
}

#define CREATE_READER_FUNCTION_WITH_NAME_AND_DATA(CREATE_FUNCTION_NAME, FUNCTION_DATA)         \
    template <bool is_copy>                                                                    \
    AggregateFunctionPtr CREATE_FUNCTION_NAME(                                                 \
            const std::string& name, const DataTypes& argument_types, bool result_is_nullable, \
            const AggregateFunctionAttr& attr) {                                               \
        const bool arg_is_nullable = argument_types[0]->is_nullable();                         \
        AggregateFunctionPtr res = nullptr;                                                    \
        std::visit(                                                                            \
                [&](auto result_is_nullable, auto arg_is_nullable) {                           \
                    res = AggregateFunctionPtr(                                                \
                            create_function_single_value<FUNCTION_DATA, result_is_nullable,    \
                                                         arg_is_nullable, is_copy>(            \
                                    name, argument_types));                                    \
                },                                                                             \
                make_bool_variant(result_is_nullable), make_bool_variant(arg_is_nullable));    \
        if (!res) {                                                                            \
            LOG(WARNING) << " failed in  create_aggregate_function_" << name                   \
                         << " and type is: " << argument_types[0]->get_name();                 \
        }                                                                                      \
        return res;                                                                            \
    }

CREATE_READER_FUNCTION_WITH_NAME_AND_DATA(create_aggregate_function_first, ReaderFunctionFirstData);
CREATE_READER_FUNCTION_WITH_NAME_AND_DATA(create_aggregate_function_first_non_null_value,
                                          ReaderFunctionFirstNonNullData);
CREATE_READER_FUNCTION_WITH_NAME_AND_DATA(create_aggregate_function_last, ReaderFunctionLastData);
CREATE_READER_FUNCTION_WITH_NAME_AND_DATA(create_aggregate_function_last_non_null_value,
                                          ReaderFunctionLastNonNullData);
#undef CREATE_READER_FUNCTION_WITH_NAME_AND_DATA

} // namespace doris::vectorized
#include "common/compile_check_end.h"