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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "cloud/cloud_base_compaction.h"
#include "cloud/cloud_cumulative_compaction.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "common/check.h"
#include "common/config.h"
#include "core/block/block.h"
#include "core/types.h"
#include "core/value/timestamptz_value.h"
#include "core/value/vdatetime_value.h"
#include "cpp/sync_point.h"
#include "enterprise/distributed-compaction/distributed_compaction_config.h"
#include "enterprise/distributed-compaction/distributed_compaction_impl.h"
#include "enterprise/distributed-compaction/distributed_compaction_key_range.h"
#include "io/fs/local_file_system.h"
#include "storage/rowset/group_rowset_writer.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_meta.h"
#include "util/defer_op.h"
#include "util/threadpool.h"
#include "util/uid_util.h"

namespace doris {

namespace {

TabletSchemaPB create_compaction_schema(KeysType keys_type = DUP_KEYS,
                                        std::string_view key_type = "INT", bool has_key = true) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(keys_type);
    auto* column = schema_pb.add_column();
    column->set_unique_id(0);
    column->set_name(has_key ? "key" : "value");
    column->set_type(key_type.data(), key_type.size());
    column->set_is_key(has_key);
    column->set_is_nullable(false);
    return schema_pb;
}

TabletMetaSharedPtr create_compaction_tablet_meta(KeysType keys_type = DUP_KEYS) {
    auto tablet_meta = std::make_shared<TabletMeta>();
    auto schema_pb = create_compaction_schema(keys_type);
    tablet_meta->mutable_tablet_schema()->init_from_pb(schema_pb);
    return tablet_meta;
}

class FailingDistributedCompaction final : public cloud::DistributedCompaction {
public:
    explicit FailingDistributedCompaction(Status assemble_status = Status::OK())
            : _assemble_status(std::move(assemble_status)) {}

    Status assemble_output_rowset(RowsetWriter&, const TabletSchema&, std::vector<int32_t>*,
                                  RowsetSharedPtr*, Merger::Statistics*) override {
        ++assemble_calls;
        return _assemble_status;
    }

    Status finish_mow_delete_bitmap(int64_t, std::shared_ptr<DeleteBitmap>*, int64_t*) override {
        return Status::OK();
    }

    void finalize(bool cancel_tasks) override {
        ++finalize_calls;
        cancelled = cancel_tasks;
    }

    int64_t task_count() const override { return 1; }
    int64_t worker_count() const override { return 1; }

    int assemble_calls = 0;
    int finalize_calls = 0;
    bool cancelled = false;

private:
    Status _assemble_status;
};

class FailingLocalMergeCloudBaseCompaction final : public CloudBaseCompaction {
public:
    using CloudBaseCompaction::CloudBaseCompaction;

protected:
    Status do_merge_input_rowsets(const std::vector<RowsetReaderSharedPtr>&,
                                  MergeInputRowsetsResult*) override {
        return Status::InternalError("injected local merge failure");
    }
};

} // namespace

// Key-range planning.

TEST(CloudDistributedCompactionTest, chooses_weighted_short_key_boundaries) {
    EXPECT_TRUE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_BOOL));
    EXPECT_TRUE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_IPV4));
    EXPECT_TRUE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_IPV6));
    EXPECT_TRUE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_TINYINT));
    EXPECT_TRUE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_SMALLINT));
    EXPECT_TRUE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_INT));
    EXPECT_TRUE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_BIGINT));
    EXPECT_TRUE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_LARGEINT));
    EXPECT_TRUE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_CHAR));
    EXPECT_TRUE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_VARCHAR));
    EXPECT_TRUE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_DATE));
    EXPECT_TRUE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_DATETIME));
    EXPECT_TRUE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_DATEV2));
    EXPECT_TRUE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_DATETIMEV2));
    EXPECT_TRUE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ));
    EXPECT_TRUE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_DECIMAL));
    EXPECT_TRUE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_DECIMAL32));
    EXPECT_TRUE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_DECIMAL64));
    EXPECT_TRUE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_DECIMAL128I));
    EXPECT_TRUE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_DECIMAL256));
    EXPECT_FALSE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_STRING));

    EXPECT_TRUE(cloud::choose_composite_key_range_boundaries({}, 4).boundaries.empty());
    const std::vector<cloud::CompositeKeySample> identical_samples = {
            {.key = {Field::create_field<TYPE_INT>(1)}, .weight = 1},
            {.key = {Field::create_field<TYPE_INT>(1)}, .weight = 1}};
    EXPECT_TRUE(
            cloud::choose_composite_key_range_boundaries(identical_samples, 4).boundaries.empty());

    const std::vector<cloud::CompositeKeySample> samples = {
            {.key = {Field::create_field<TYPE_LARGEINT>(int128_t(30))}, .weight = 10},
            {.key = {Field::create_field<TYPE_LARGEINT>(int128_t(0))}, .weight = 10},
            {.key = {Field::create_field<TYPE_LARGEINT>(int128_t(20))}, .weight = 5},
            {.key = {Field::create_field<TYPE_LARGEINT>(int128_t(10))}, .weight = 5},
            {.key = {Field::create_field<TYPE_LARGEINT>(int128_t(20))}, .weight = 5},
            {.key = {Field::create_field<TYPE_LARGEINT>(int128_t(10))}, .weight = 5}};
    const auto plan = cloud::choose_composite_key_range_boundaries(samples, 4);
    ASSERT_EQ(plan.boundaries.size(), 3);
    EXPECT_EQ(plan.boundaries[0][0].get<TYPE_LARGEINT>(), int128_t(10));
    EXPECT_EQ(plan.boundaries[1][0].get<TYPE_LARGEINT>(), int128_t(20));
    EXPECT_EQ(plan.boundaries[2][0].get<TYPE_LARGEINT>(), int128_t(30));

    const std::vector<cloud::CompositeKeySample> hot_key = {
            {.key = {Field::create_field<TYPE_LARGEINT>(int128_t(0))}, .weight = 90},
            {.key = {Field::create_field<TYPE_LARGEINT>(int128_t(10))}, .weight = 5},
            {.key = {Field::create_field<TYPE_LARGEINT>(int128_t(20))}, .weight = 5}};
    const auto hot_key_plan = cloud::choose_composite_key_range_boundaries(hot_key, 4);
    ASSERT_EQ(hot_key_plan.boundaries.size(), 1);
    EXPECT_EQ(hot_key_plan.boundaries[0][0].get<TYPE_LARGEINT>(), int128_t(10));

    const std::vector<cloud::CompositeKeySample> edge_keys = {
            {.key = {Field::create_field<TYPE_LARGEINT>(std::numeric_limits<int128_t>::min())},
             .weight = 10},
            {.key = {Field::create_field<TYPE_LARGEINT>(std::numeric_limits<int128_t>::max())},
             .weight = 10}};
    const auto edge_key_plan = cloud::choose_composite_key_range_boundaries(edge_keys, 2);
    ASSERT_EQ(edge_key_plan.boundaries.size(), 1);
    EXPECT_EQ(edge_key_plan.boundaries[0][0].get<TYPE_LARGEINT>(),
              std::numeric_limits<int128_t>::max());
}

TEST(CloudDistributedCompactionTest, chooses_nullable_boolean_key_range_boundaries) {
    const std::vector<cloud::CompositeKeySample> samples = {
            {.key = {Field::create_field<TYPE_BOOLEAN>(false)}, .weight = 10},
            {.key = {Field()}, .weight = 10},
            {.key = {Field::create_field<TYPE_BOOLEAN>(true)}, .weight = 10}};

    const auto plan = cloud::choose_composite_key_range_boundaries(samples, 3);
    ASSERT_EQ(plan.prefix_length, 1);
    ASSERT_EQ(plan.boundaries.size(), 2);
    EXPECT_EQ(plan.boundaries[0][0].get<TYPE_BOOLEAN>(), 0);
    EXPECT_EQ(plan.boundaries[1][0].get<TYPE_BOOLEAN>(), 1);
}

TEST(CloudDistributedCompactionTest, expands_key_prefix_until_ranges_are_distinct) {
    const std::vector<cloud::CompositeKeySample> samples = {
            {.key = {Field::create_field<TYPE_INT>(0), Field::create_field<TYPE_STRING>("alpha")},
             .weight = 10},
            {.key = {Field::create_field<TYPE_INT>(0), Field::create_field<TYPE_STRING>("bravo")},
             .weight = 10},
            {.key = {Field::create_field<TYPE_INT>(0), Field::create_field<TYPE_STRING>("charlie")},
             .weight = 10},
            {.key = {Field::create_field<TYPE_INT>(0), Field::create_field<TYPE_STRING>("delta")},
             .weight = 10}};

    const auto plan = cloud::choose_composite_key_range_boundaries(samples, 4);
    ASSERT_EQ(plan.prefix_length, 2);
    ASSERT_EQ(plan.boundaries.size(), 3);
    EXPECT_EQ(plan.boundaries[0][1].get<TYPE_STRING>(), "bravo");
    EXPECT_EQ(plan.boundaries[1][1].get<TYPE_STRING>(), "charlie");
    EXPECT_EQ(plan.boundaries[2][1].get<TYPE_STRING>(), "delta");

    const std::vector<cloud::CompositeKeySample> distinct_leading_keys = {
            {.key = {Field::create_field<TYPE_INT>(0), Field::create_field<TYPE_INT>(0)},
             .weight = 10},
            {.key = {Field::create_field<TYPE_INT>(1), Field::create_field<TYPE_INT>(0)},
             .weight = 10},
            {.key = {Field::create_field<TYPE_INT>(2), Field::create_field<TYPE_INT>(0)},
             .weight = 10},
            {.key = {Field::create_field<TYPE_INT>(3), Field::create_field<TYPE_INT>(0)},
             .weight = 10}};
    EXPECT_EQ(cloud::choose_composite_key_range_boundaries(distinct_leading_keys, 4).prefix_length,
              1);
}

TEST(CloudDistributedCompactionTest, expands_nullable_key_prefix) {
    const std::vector<cloud::CompositeKeySample> samples = {
            {.key = {Field(), Field::create_field<TYPE_INT>(30)}, .weight = 10},
            {.key = {Field(), Field::create_field<TYPE_INT>(10)}, .weight = 10},
            {.key = {Field(), Field::create_field<TYPE_INT>(20)}, .weight = 10}};

    const auto plan = cloud::choose_composite_key_range_boundaries(samples, 2);
    ASSERT_EQ(plan.prefix_length, 2);
    ASSERT_EQ(plan.boundaries.size(), 1);
    EXPECT_TRUE(plan.boundaries.front().front().is_null());
    EXPECT_EQ(plan.boundaries.front()[1].get<TYPE_INT>(), 20);
}

TEST(CloudDistributedCompactionTest, chooses_date_key_range_boundary) {
    auto make_date = [](uint8_t day) {
        DateV2Value<DateV2ValueType> value;
        value.unchecked_set_time(2026, 8, day, 0, 0, 0, 0);
        return Field::create_field<TYPE_DATEV2>(std::move(value));
    };
    const std::vector<cloud::CompositeKeySample> samples = {{.key = {make_date(3)}, .weight = 10},
                                                            {.key = {make_date(1)}, .weight = 10},
                                                            {.key = {make_date(2)}, .weight = 10}};

    const auto plan = cloud::choose_composite_key_range_boundaries(samples, 2);
    ASSERT_EQ(plan.prefix_length, 1);
    ASSERT_EQ(plan.boundaries.size(), 1);
    EXPECT_EQ(plan.boundaries.front().front().get<TYPE_DATEV2>().day(), 2);
}

TEST(CloudDistributedCompactionTest, chooses_ip_key_range_boundaries) {
    const std::vector<cloud::CompositeKeySample> ipv4_samples = {
            {.key = {Field::create_field<TYPE_IPV4>(IPv4(3))}, .weight = 10},
            {.key = {Field::create_field<TYPE_IPV4>(IPv4(1))}, .weight = 10},
            {.key = {Field::create_field<TYPE_IPV4>(IPv4(2))}, .weight = 10}};
    const auto ipv4_plan = cloud::choose_composite_key_range_boundaries(ipv4_samples, 2);
    ASSERT_EQ(ipv4_plan.boundaries.size(), 1);
    EXPECT_EQ(ipv4_plan.boundaries.front().front().get<TYPE_IPV4>(), IPv4(2));

    const std::vector<cloud::CompositeKeySample> ipv6_samples = {
            {.key = {Field::create_field<TYPE_IPV6>(IPv6(3))}, .weight = 10},
            {.key = {Field::create_field<TYPE_IPV6>(IPv6(1))}, .weight = 10},
            {.key = {Field::create_field<TYPE_IPV6>(IPv6(2))}, .weight = 10}};
    const auto ipv6_plan = cloud::choose_composite_key_range_boundaries(ipv6_samples, 2);
    ASSERT_EQ(ipv6_plan.boundaries.size(), 1);
    EXPECT_EQ(ipv6_plan.boundaries.front().front().get<TYPE_IPV6>(), IPv6(2));
}

TEST(CloudDistributedCompactionTest, chooses_timestamptz_key_range_boundary) {
    auto make_timestamptz = [](uint8_t day) {
        TimestampTzValue value;
        value.unchecked_set_time(2026, 9, day, 0, 0, 0, 0);
        return Field::create_field<TYPE_TIMESTAMPTZ>(std::move(value));
    };
    const std::vector<cloud::CompositeKeySample> samples = {
            {.key = {make_timestamptz(3)}, .weight = 10},
            {.key = {make_timestamptz(1)}, .weight = 10},
            {.key = {make_timestamptz(2)}, .weight = 10}};

    const auto plan = cloud::choose_composite_key_range_boundaries(samples, 2);
    ASSERT_EQ(plan.boundaries.size(), 1);
    EXPECT_EQ(plan.boundaries.front().front().get<TYPE_TIMESTAMPTZ>().day(), 2);
}

TEST(CloudDistributedCompactionTest, chooses_decimal_key_range_boundary) {
    auto make_decimal = [](int64_t value) {
        return Field::create_field<TYPE_DECIMAL64>(Decimal64(value));
    };
    const std::vector<cloud::CompositeKeySample> samples = {
            {.key = {make_decimal(5678)}, .weight = 10},
            {.key = {make_decimal(-1234)}, .weight = 10},
            {.key = {make_decimal(0)}, .weight = 10}};

    const auto plan = cloud::choose_composite_key_range_boundaries(samples, 2);
    ASSERT_EQ(plan.prefix_length, 1);
    ASSERT_EQ(plan.boundaries.size(), 1);
    EXPECT_EQ(plan.boundaries.front().front().get<TYPE_DECIMAL64>().value, 0);
}

TEST(CloudDistributedCompactionTest, downsamples_short_key_blocks_with_exact_weights) {
    EXPECT_TRUE(cloud::build_weighted_key_sample_rowids(0, 1024, 0).empty());

    const auto single_sample = cloud::build_weighted_key_sample_rowids(2 * 1024 + 17, 1024, 1);
    ASSERT_EQ(single_sample.size(), 1);
    EXPECT_EQ(single_sample[0].rowid, 1024);
    EXPECT_EQ(single_sample[0].weight, 2 * 1024 + 17);

    const auto samples = cloud::build_weighted_key_sample_rowids(10 * 1024 + 17, 1024, 3);
    ASSERT_EQ(samples.size(), 3);
    EXPECT_EQ(samples[0].rowid, 1024);
    EXPECT_EQ(samples[0].weight, 3 * 1024);
    EXPECT_EQ(samples[1].rowid, 4 * 1024);
    EXPECT_EQ(samples[1].weight, 4 * 1024);
    EXPECT_EQ(samples[2].rowid, 8 * 1024);
    EXPECT_EQ(samples[2].weight, 3 * 1024 + 17);

    uint64_t total_weight = 0;
    for (const auto& sample : samples) {
        total_weight += sample.weight;
    }
    EXPECT_EQ(total_weight, 10 * 1024 + 17);

    const auto all_blocks = cloud::build_weighted_key_sample_rowids(2 * 1024 + 17, 1024, 10);
    ASSERT_EQ(all_blocks.size(), 3);
    EXPECT_EQ(all_blocks[0].rowid, 0);
    EXPECT_EQ(all_blocks[1].rowid, 1024);
    EXPECT_EQ(all_blocks[2].rowid, 2 * 1024);
    EXPECT_EQ(all_blocks[2].weight, 17);
}

TEST(CloudDistributedCompactionTest, chooses_encoded_key_boundaries_with_locators) {
    EXPECT_TRUE(cloud::choose_encoded_key_range_boundaries({}, 4).empty());
    EXPECT_TRUE(cloud::choose_encoded_key_range_boundaries(
                        {{.key = "alpha", .weight = 1, .segment_index = 0, .rowid = 0},
                         {.key = "bravo", .weight = 1, .segment_index = 1, .rowid = 1}},
                        1)
                        .empty());

    std::vector<cloud::EncodedKeySample> samples = {
            {.key = "delta", .weight = 10, .segment_index = 3, .rowid = 300},
            {.key = "alpha", .weight = 10, .segment_index = 0, .rowid = 0},
            {.key = "charlie", .weight = 10, .segment_index = 2, .rowid = 200},
            {.key = "bravo", .weight = 10, .segment_index = 1, .rowid = 100}};
    const auto boundaries = cloud::choose_encoded_key_range_boundaries(std::move(samples), 4);
    ASSERT_EQ(boundaries.size(), 3);
    EXPECT_EQ(boundaries[0].key, "bravo");
    EXPECT_EQ(boundaries[0].segment_index, 1);
    EXPECT_EQ(boundaries[0].rowid, 100);
    EXPECT_EQ(boundaries[1].key, "charlie");
    EXPECT_EQ(boundaries[1].segment_index, 2);
    EXPECT_EQ(boundaries[1].rowid, 200);
    EXPECT_EQ(boundaries[2].key, "delta");
    EXPECT_EQ(boundaries[2].segment_index, 3);
    EXPECT_EQ(boundaries[2].rowid, 300);

    const std::vector<cloud::EncodedKeySample> hot_key = {
            {.key = "alpha", .weight = 90, .segment_index = 0, .rowid = 0},
            {.key = "bravo", .weight = 5, .segment_index = 1, .rowid = 100},
            {.key = "charlie", .weight = 5, .segment_index = 2, .rowid = 200}};
    EXPECT_EQ(cloud::choose_encoded_key_range_boundaries(hot_key, 4).size(), 1);

    const std::vector<cloud::EncodedKeySample> duplicate_primary_keys = {
            {.key = "delta", .weight = 40, .segment_index = 4, .rowid = 400},
            {.key = "alpha", .weight = 20, .segment_index = 0, .rowid = 0},
            {.key = "charlie", .weight = 10, .segment_index = 3, .rowid = 300},
            {.key = "alpha", .weight = 20, .segment_index = 1, .rowid = 100},
            {.key = "bravo", .weight = 10, .segment_index = 2, .rowid = 200}};
    const auto duplicate_boundaries =
            cloud::choose_encoded_key_range_boundaries(duplicate_primary_keys, 4);
    ASSERT_EQ(duplicate_boundaries.size(), 3);
    EXPECT_EQ(duplicate_boundaries[0].key, "bravo");
    EXPECT_EQ(duplicate_boundaries[1].key, "charlie");
    EXPECT_EQ(duplicate_boundaries[2].key, "delta");
}

namespace {

struct KeyColumnSpec {
    std::string type;
    int32_t length;
    int32_t index_length;
};

class DistributedCompactionKeyRangePlanningTest : public testing::Test {
protected:
    void SetUp() override {
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_test_dir).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_test_dir).ok());
        _engine = std::make_unique<StorageEngine>(EngineOptions {});
    }

    void TearDown() override {
        _engine.reset();
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_test_dir).ok());
    }

    TabletSchemaSPtr create_schema(KeysType keys_type,
                                   const std::vector<KeyColumnSpec>& key_columns,
                                   size_t short_key_column_count) {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(keys_type);
        schema_pb.set_num_short_key_columns(short_key_column_count);
        schema_pb.set_num_rows_per_row_block(1);
        schema_pb.set_compress_kind(COMPRESS_NONE);

        int32_t unique_id = 0;
        for (const auto& key_column : key_columns) {
            auto* column = schema_pb.add_column();
            column->set_unique_id(unique_id);
            column->set_name("k" + std::to_string(unique_id));
            column->set_type(key_column.type);
            column->set_length(key_column.length);
            column->set_index_length(key_column.index_length);
            column->set_is_key(true);
            column->set_is_nullable(false);
            column->set_aggregation("NONE");
            ++unique_id;
        }

        auto* value_column = schema_pb.add_column();
        value_column->set_unique_id(unique_id++);
        value_column->set_name("v");
        value_column->set_type("INT");
        value_column->set_length(4);
        value_column->set_index_length(4);
        value_column->set_is_key(false);
        value_column->set_is_nullable(false);
        value_column->set_aggregation("NONE");

        if (keys_type == UNIQUE_KEYS) {
            schema_pb.set_delete_sign_idx(schema_pb.column_size());
            auto* delete_sign_column = schema_pb.add_column();
            delete_sign_column->set_unique_id(unique_id++);
            delete_sign_column->set_name(DELETE_SIGN);
            delete_sign_column->set_type("TINYINT");
            delete_sign_column->set_length(1);
            delete_sign_column->set_index_length(1);
            delete_sign_column->set_is_key(false);
            delete_sign_column->set_is_nullable(false);
            delete_sign_column->set_aggregation("NONE");
        }
        schema_pb.set_next_column_unique_id(unique_id);

        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(schema_pb);
        return schema;
    }

    Status build_plan_for_segments(
            const TabletSchemaSPtr& schema,
            const std::vector<std::vector<std::vector<Field>>>& segment_key_rows, bool is_mow,
            size_t range_count, cloud::KeyRangePlanningResult* result) {
        RowsetWriterContext context;
        RowsetId rowset_id;
        rowset_id.init(_next_rowset_id++);
        context.rowset_id = rowset_id;
        context.tablet_id = 1;
        context.partition_id = 1;
        context.tablet_schema_hash = 1;
        context.rowset_type = BETA_ROWSET;
        context.rowset_state = VISIBLE;
        context.tablet_schema = schema;
        context.tablet_path = _test_dir;
        context.version = {_next_version, _next_version};
        context.segments_overlap = NONOVERLAPPING;
        context.enable_unique_key_merge_on_write = is_mow;
        context.enable_segcompaction = false;
        ++_next_version;

        auto writer_result = RowsetFactory::create_rowset_writer(*_engine, context, false);
        if (!writer_result.has_value()) {
            return writer_result.error();
        }
        auto writer = std::move(writer_result).value();
        uint64_t total_rows = 0;
        for (const auto& key_rows : segment_key_rows) {
            Block block = schema->create_storage_block();
            auto columns = std::move(block).mutate_columns();
            for (const auto& key_row : key_rows) {
                DORIS_CHECK_EQ(key_row.size(), schema->num_key_columns());
                for (size_t column_index = 0; column_index < key_row.size(); ++column_index) {
                    columns[column_index]->insert(key_row[column_index]);
                }
                for (size_t column_index = key_row.size(); column_index < columns.size();
                     ++column_index) {
                    columns[column_index]->insert_default();
                }
            }
            block.set_columns(std::move(columns));
            RETURN_IF_ERROR(writer->add_block(&block));
            RETURN_IF_ERROR(writer->flush());
            total_rows += key_rows.size();
        }
        RowsetSharedPtr rowset;
        RETURN_IF_ERROR(writer->build(rowset));
        return cloud::build_key_range_plan({rowset}, *schema, is_mow, range_count, total_rows,
                                           result);
    }

    Status build_plan(const TabletSchemaSPtr& schema,
                      const std::vector<std::vector<Field>>& key_rows, bool is_mow,
                      cloud::KeyRangePlanningResult* result) {
        return build_plan_for_segments(schema, {key_rows}, is_mow, 4, result);
    }

private:
    const std::string _test_dir = "./data_test/distributed_compaction_key_range_planning_test";
    std::unique_ptr<StorageEngine> _engine;
    int64_t _next_rowset_id = 100000;
    int64_t _next_version = 1;
};

TEST_F(DistributedCompactionKeyRangePlanningTest, selects_each_sampling_mode) {
    const auto int_schema = create_schema(DUP_KEYS, {{"INT", 4, 4}}, 1);
    cloud::KeyRangePlanningResult direct_result;
    ASSERT_TRUE(build_plan(int_schema,
                           {{Field::create_field<TYPE_INT>(0)},
                            {Field::create_field<TYPE_INT>(10)},
                            {Field::create_field<TYPE_INT>(20)},
                            {Field::create_field<TYPE_INT>(30)}},
                           false, &direct_result)
                        .ok());
    EXPECT_EQ(direct_result.sampling.mode, cloud::KeyRangeSamplingMode::SHORT_KEY_DIRECT);
    EXPECT_EQ(direct_result.key_ranges.boundaries.size(), 3);
    EXPECT_EQ(direct_result.key_column_sample_count, 3);

    cloud::KeyRangePlanningResult fallback_result;
    ASSERT_TRUE(build_plan(int_schema,
                           {{Field::create_field<TYPE_INT>(0)},
                            {Field::create_field<TYPE_INT>(0)},
                            {Field::create_field<TYPE_INT>(1)},
                            {Field::create_field<TYPE_INT>(1)}},
                           false, &fallback_result)
                        .ok());
    EXPECT_EQ(fallback_result.sampling.mode, cloud::KeyRangeSamplingMode::KEY_COLUMN_VALUES);
    EXPECT_EQ(fallback_result.sampling.short_key_skip_reason, "insufficient_encoded_boundaries");
    EXPECT_EQ(fallback_result.key_ranges.boundaries.size(), 1);
    EXPECT_EQ(fallback_result.key_column_sample_count, 4);

    const auto truncated_string_schema = create_schema(DUP_KEYS, {{"VARCHAR", 16, 4}}, 1);
    cloud::KeyRangePlanningResult refinement_result;
    ASSERT_TRUE(build_plan(truncated_string_schema,
                           {{Field::create_field<TYPE_STRING>("prefix-0001")},
                            {Field::create_field<TYPE_STRING>("prefix-0002")},
                            {Field::create_field<TYPE_STRING>("prefix-0003")},
                            {Field::create_field<TYPE_STRING>("prefix-0004")}},
                           false, &refinement_result)
                        .ok());
    EXPECT_EQ(refinement_result.sampling.mode,
              cloud::KeyRangeSamplingMode::SHORT_KEY_BOUNDARY_REFINEMENT);
    EXPECT_EQ(refinement_result.key_ranges.boundaries.size(), 3);
    EXPECT_EQ(refinement_result.boundary_refinement_group_count, 1);
    EXPECT_EQ(refinement_result.boundary_refinement_sample_count, 4);

    const auto non_terminal_truncated_schema =
            create_schema(DUP_KEYS, {{"VARCHAR", 16, 4}, {"INT", 4, 4}}, 2);
    cloud::KeyRangePlanningResult key_column_result;
    ASSERT_TRUE(build_plan(non_terminal_truncated_schema,
                           {{Field::create_field<TYPE_STRING>("prefix-0001"),
                             Field::create_field<TYPE_INT>(1)},
                            {Field::create_field<TYPE_STRING>("prefix-0002"),
                             Field::create_field<TYPE_INT>(2)},
                            {Field::create_field<TYPE_STRING>("prefix-0003"),
                             Field::create_field<TYPE_INT>(3)},
                            {Field::create_field<TYPE_STRING>("prefix-0004"),
                             Field::create_field<TYPE_INT>(4)}},
                           false, &key_column_result)
                        .ok());
    EXPECT_EQ(key_column_result.sampling.mode, cloud::KeyRangeSamplingMode::KEY_COLUMN_VALUES);
    EXPECT_EQ(key_column_result.sampling.short_key_skip_reason, "non_terminal_truncated_short_key");
    EXPECT_EQ(key_column_result.key_ranges.boundaries.size(), 3);
    EXPECT_EQ(key_column_result.key_column_sample_count, 4);

    const auto mow_schema = create_schema(UNIQUE_KEYS, {{"INT", 4, 4}}, 1);
    cloud::KeyRangePlanningResult primary_key_result;
    ASSERT_TRUE(build_plan(mow_schema,
                           {{Field::create_field<TYPE_INT>(0)}, {Field::create_field<TYPE_INT>(1)}},
                           true, &primary_key_result)
                        .ok());
    EXPECT_EQ(primary_key_result.sampling.mode, cloud::KeyRangeSamplingMode::PRIMARY_KEY);
    EXPECT_EQ(primary_key_result.key_ranges.boundaries.size(), 1);
    EXPECT_EQ(primary_key_result.key_column_sample_count, 1);
}

TEST_F(DistributedCompactionKeyRangePlanningTest,
       refines_boundaries_across_multiple_short_key_groups) {
    const auto schema = create_schema(DUP_KEYS, {{"VARCHAR", 16, 4}}, 1);
    std::vector<std::vector<std::vector<Field>>> segment_key_rows(3);
    auto append_rows = [&](size_t segment_index, std::string_view key, size_t count) {
        for (size_t i = 0; i < count; ++i) {
            segment_key_rows[segment_index].push_back(
                    {Field::create_field<TYPE_STRING>(std::string(key))});
        }
    };
    append_rows(0, "aaaa1", 6);
    append_rows(0, "aaaa2", 5);
    append_rows(1, "bbbb1", 4);
    append_rows(1, "bbbb2", 10);
    append_rows(2, "cccc1", 5);

    cloud::KeyRangePlanningResult result;
    ASSERT_TRUE(build_plan_for_segments(schema, segment_key_rows, false, 3, &result).ok());
    EXPECT_EQ(result.sampling.mode, cloud::KeyRangeSamplingMode::SHORT_KEY_BOUNDARY_REFINEMENT);
    EXPECT_EQ(result.segment_count, 3);
    ASSERT_EQ(result.key_ranges.boundaries.size(), 2);
    EXPECT_EQ(result.key_ranges.boundaries[0][0].get<TYPE_STRING>(), "bbbb1");
    EXPECT_EQ(result.key_ranges.boundaries[1][0].get<TYPE_STRING>(), "bbbb2");
    EXPECT_EQ(result.boundary_refinement_group_count, 3);
    EXPECT_EQ(result.boundary_refinement_sample_count, 30);

    cloud::KeyRangePlanningResult constant_result;
    ASSERT_TRUE(build_plan(schema,
                           {{Field::create_field<TYPE_STRING>("same-0001")},
                            {Field::create_field<TYPE_STRING>("same-0001")},
                            {Field::create_field<TYPE_STRING>("same-0001")},
                            {Field::create_field<TYPE_STRING>("same-0001")}},
                           false, &constant_result)
                        .ok());
    EXPECT_EQ(constant_result.sampling.mode,
              cloud::KeyRangeSamplingMode::SHORT_KEY_BOUNDARY_REFINEMENT);
    EXPECT_TRUE(constant_result.key_ranges.boundaries.empty());
    EXPECT_EQ(constant_result.boundary_refinement_group_count, 1);
}

TEST_F(DistributedCompactionKeyRangePlanningTest, falls_back_for_unsupported_trailing_key) {
    const std::vector<std::vector<Field>> key_rows = {
            {Field::create_field<TYPE_INT>(0), Field::create_field<TYPE_STRING>("a")},
            {Field::create_field<TYPE_INT>(10), Field::create_field<TYPE_STRING>("b")},
            {Field::create_field<TYPE_INT>(20), Field::create_field<TYPE_STRING>("c")},
            {Field::create_field<TYPE_INT>(30), Field::create_field<TYPE_STRING>("d")}};

    const auto duplicate_schema = create_schema(DUP_KEYS, {{"INT", 4, 4}, {"STRING", 16, 4}}, 2);
    cloud::KeyRangePlanningResult short_key_result;
    ASSERT_TRUE(build_plan(duplicate_schema, key_rows, false, &short_key_result).ok());
    EXPECT_EQ(short_key_result.sampling.mode, cloud::KeyRangeSamplingMode::KEY_COLUMN_VALUES);
    EXPECT_EQ(short_key_result.sampling.short_key_skip_reason, "unsupported_short_key_type");
    EXPECT_EQ(short_key_result.key_ranges.prefix_length, 1);
    EXPECT_EQ(short_key_result.key_ranges.boundaries.size(), 3);

    const auto mow_schema = create_schema(UNIQUE_KEYS, {{"INT", 4, 4}, {"STRING", 16, 4}}, 1);
    cloud::KeyRangePlanningResult primary_key_result;
    ASSERT_TRUE(build_plan(mow_schema, key_rows, true, &primary_key_result).ok());
    EXPECT_EQ(primary_key_result.sampling.mode, cloud::KeyRangeSamplingMode::KEY_COLUMN_VALUES);
    EXPECT_EQ(primary_key_result.sampling.primary_key_skip_reason, "unsupported_primary_key_type");
    EXPECT_EQ(primary_key_result.key_ranges.prefix_length, 1);
    EXPECT_EQ(primary_key_result.key_ranges.boundaries.size(), 3);
}

TEST_F(DistributedCompactionKeyRangePlanningTest, samples_uneven_segments_proportionally) {
    const int32_t old_samples_per_range =
            config::cloud_distributed_base_compaction_samples_per_range;
    Defer restore_config {[&] {
        config::cloud_distributed_base_compaction_samples_per_range = old_samples_per_range;
    }};
    config::cloud_distributed_base_compaction_samples_per_range = 1;

    const auto schema = create_schema(DUP_KEYS, {{"INT", 4, 4}}, 1);
    const std::vector<std::vector<Field>> first_segment = {{Field::create_field<TYPE_INT>(0)}};
    const std::vector<std::vector<Field>> second_segment = {
            {Field::create_field<TYPE_INT>(10)}, {Field::create_field<TYPE_INT>(11)},
            {Field::create_field<TYPE_INT>(12)}, {Field::create_field<TYPE_INT>(13)},
            {Field::create_field<TYPE_INT>(14)}, {Field::create_field<TYPE_INT>(15)},
            {Field::create_field<TYPE_INT>(16)}, {Field::create_field<TYPE_INT>(17)},
            {Field::create_field<TYPE_INT>(18)}};
    cloud::KeyRangePlanningResult result;
    ASSERT_TRUE(build_plan_for_segments(schema, {first_segment, second_segment}, false, 4, &result)
                        .ok());

    EXPECT_EQ(result.sampling.mode, cloud::KeyRangeSamplingMode::SHORT_KEY_DIRECT);
    EXPECT_EQ(result.target_sample_count, 4);
    EXPECT_EQ(result.sampled_row_count, 5);
    EXPECT_EQ(result.segment_count, 2);
    EXPECT_EQ(result.encoded_sample_count, 5);
    ASSERT_EQ(result.key_ranges.boundaries.size(), 3);
    EXPECT_EQ(result.key_ranges.boundaries[0][0].get<TYPE_INT>(), 10);
    EXPECT_EQ(result.key_ranges.boundaries[1][0].get<TYPE_INT>(), 14);
    EXPECT_EQ(result.key_ranges.boundaries[2][0].get<TYPE_INT>(), 17);
}

} // namespace

// Distributed execution planning.

TEST(CloudDistributedCompactionTest, checks_distributed_base_compaction_eligibility) {
    const bool old_enable = config::enable_cloud_distributed_base_compaction;
    const int64_t old_target =
            config::cloud_distributed_base_compaction_target_range_input_size_bytes;
    Defer restore_config {[&] {
        config::enable_cloud_distributed_base_compaction = old_enable;
        config::cloud_distributed_base_compaction_target_range_input_size_bytes = old_target;
    }};
    config::enable_cloud_distributed_base_compaction = true;
    config::cloud_distributed_base_compaction_target_range_input_size_bytes = 100;

    CloudStorageEngine engine(EngineOptions {});
    auto tablet_meta = create_compaction_tablet_meta();
    CloudTablet tablet(engine, tablet_meta);
    EXPECT_TRUE(cloud::can_use_distributed_base_compaction(tablet, 101));

    config::enable_cloud_distributed_base_compaction = false;
    EXPECT_FALSE(cloud::can_use_distributed_base_compaction(tablet, 101));
    config::enable_cloud_distributed_base_compaction = true;

    config::cloud_distributed_base_compaction_target_range_input_size_bytes = 0;
    EXPECT_FALSE(cloud::can_use_distributed_base_compaction(tablet, 101));
    config::cloud_distributed_base_compaction_target_range_input_size_bytes = 100;
    EXPECT_FALSE(cloud::can_use_distributed_base_compaction(tablet, 100));

    tablet_meta->set_tablet_role(TabletRolePB::TABLET_ROLE_ROW_BINLOG);
    EXPECT_FALSE(cloud::can_use_distributed_base_compaction(tablet, 101));
    tablet_meta->set_tablet_role(TabletRolePB::TABLET_ROLE_DATA);

    auto schema_pb = create_compaction_schema();
    schema_pb.set_sort_type(SortType::ZORDER);
    tablet_meta->mutable_tablet_schema()->init_from_pb(schema_pb);
    EXPECT_FALSE(cloud::can_use_distributed_base_compaction(tablet, 101));

    schema_pb = create_compaction_schema(DUP_KEYS, "INT", false);
    tablet_meta->mutable_tablet_schema()->init_from_pb(schema_pb);
    EXPECT_FALSE(cloud::can_use_distributed_base_compaction(tablet, 101));

    schema_pb = create_compaction_schema(DUP_KEYS, "STRING");
    tablet_meta->mutable_tablet_schema()->init_from_pb(schema_pb);
    EXPECT_FALSE(cloud::can_use_distributed_base_compaction(tablet, 101));

    schema_pb = create_compaction_schema(UNIQUE_KEYS);
    schema_pb.add_cluster_key_uids(0);
    tablet_meta->mutable_tablet_schema()->init_from_pb(schema_pb);
    tablet_meta->set_enable_unique_key_merge_on_write(true);
    EXPECT_FALSE(cloud::can_use_distributed_base_compaction(tablet, 101));
}

TEST(CloudDistributedCompactionTest, distributed_single_rowset_compaction_builds_segment_slots) {
    std::vector<cloud::OutputRowsetSegmentIdSlot> slots;
    ASSERT_TRUE(cloud::build_output_rowset_segment_id_slots(17, 100, 3, &slots).ok());
    ASSERT_EQ(slots.size(), 3);
    EXPECT_EQ(slots[0].start_id, 17);
    EXPECT_EQ(slots[1].start_id, 117);
    EXPECT_EQ(slots[2].start_id, 217);
    for (const auto& slot : slots) {
        EXPECT_EQ(slot.capacity, 100);
    }

    EXPECT_FALSE(cloud::build_output_rowset_segment_id_slots(-1, 100, 3, &slots).ok());
    EXPECT_FALSE(cloud::build_output_rowset_segment_id_slots(0, 0, 3, &slots).ok());
    EXPECT_FALSE(cloud::build_output_rowset_segment_id_slots(
                         std::numeric_limits<int32_t>::max() - 10, 100, 2, &slots)
                         .ok());
}

TEST(CloudDistributedCompactionTest,
     distributed_single_rowset_compaction_selects_workers_deterministically) {
    const std::vector<cloud::CompactionWorkerInfo> candidates = {
            {.backend_id = 3,
             .endpoint = "be-c:8060",
             .cloud_unique_id = "cloud-c",
             .compute_group_id = "compute-group-a"},
            {.backend_id = 1,
             .endpoint = "be-a:8060",
             .cloud_unique_id = "cloud-a",
             .compute_group_id = "compute-group-a"},
            {.backend_id = 2,
             .endpoint = "be-b:8060",
             .cloud_unique_id = "cloud-b",
             .compute_group_id = "compute-group-a"},
            {.backend_id = 4,
             .endpoint = "be-d:8060",
             .cloud_unique_id = "cloud-d",
             .compute_group_id = "compute-group-a"}};

    const auto selected =
            cloud::select_compaction_workers_for_groups(candidates, 3, 2, "execution-a");
    ASSERT_EQ(selected.size(), 2);
    EXPECT_EQ(selected[0].backend_id, 3);
    const auto selected_again =
            cloud::select_compaction_workers_for_groups(candidates, 3, 2, "execution-a");
    ASSERT_EQ(selected_again.size(), selected.size());
    EXPECT_EQ(selected_again[0].backend_id, selected[0].backend_id);
    EXPECT_EQ(selected_again[1].backend_id, selected[1].backend_id);
    const std::vector<cloud::CompactionWorkerInfo> reordered_candidates(candidates.rbegin(),
                                                                        candidates.rend());
    const auto selected_from_reordered =
            cloud::select_compaction_workers_for_groups(reordered_candidates, 3, 2, "execution-a");
    ASSERT_EQ(selected_from_reordered.size(), selected.size());
    EXPECT_EQ(selected_from_reordered[0].backend_id, selected[0].backend_id);
    EXPECT_EQ(selected_from_reordered[1].backend_id, selected[1].backend_id);

    const auto remote_only =
            cloud::select_compaction_workers_for_groups(candidates, 5, 2, "execution-a");
    ASSERT_EQ(remote_only.size(), 2);
    EXPECT_NE(remote_only[0].backend_id, 5);
    EXPECT_NE(remote_only[1].backend_id, 5);

    const auto single_worker =
            cloud::select_compaction_workers_for_groups(candidates, 3, 1, "execution-a");
    ASSERT_EQ(single_worker.size(), 1);
    EXPECT_EQ(single_worker[0].backend_id, 3);

    std::unordered_set<int64_t> selected_remote_backend_ids;
    for (int execution_index = 0; execution_index < 100; ++execution_index) {
        const std::string execution_id = "execution-" + std::to_string(execution_index);
        const auto selected_for_execution =
                cloud::select_compaction_workers_for_groups(candidates, 3, 2, execution_id);
        ASSERT_EQ(selected_for_execution.size(), 2);
        EXPECT_EQ(selected_for_execution[0].backend_id, 3);
        selected_remote_backend_ids.emplace(selected_for_execution[1].backend_id);
    }
    EXPECT_GT(selected_remote_backend_ids.size(), 1);
}

TEST(CloudDistributedCompactionTest, distributed_compaction_assigns_groups_round_robin) {
    const auto groups = cloud::assign_compaction_groups_round_robin(7, 3);
    ASSERT_EQ(groups.size(), 3);
    EXPECT_EQ(groups[0], (std::vector<size_t> {0, 3, 6}));
    EXPECT_EQ(groups[1], (std::vector<size_t> {1, 4}));
    EXPECT_EQ(groups[2], (std::vector<size_t> {2, 5}));
}

// Coordinator input and result validation.

TEST(CloudDistributedCompactionTest, validates_distributed_compaction_wire_requests) {
    auto make_submit_request = [](PCloudDistributedCompactionType type, size_t input_count) {
        PCloudDistributedCompactionSubmitRequest request;
        request.set_execution_id("execution");
        request.set_compaction_type(type);
        for (size_t i = 0; i < input_count; ++i) {
            request.add_input_rowset_metas();
        }
        request.mutable_output_rowset_meta();
        return request;
    };

    EXPECT_FALSE(cloud::validate_distributed_compaction_submit_request(
                         make_submit_request(CLOUD_DISTRIBUTED_CUMULATIVE_COMPACTION, 0))
                         .ok());
    EXPECT_FALSE(cloud::validate_distributed_compaction_submit_request(
                         make_submit_request(CLOUD_DISTRIBUTED_CUMULATIVE_COMPACTION, 2))
                         .ok());
    EXPECT_FALSE(cloud::validate_distributed_compaction_submit_request(
                         make_submit_request(CLOUD_DISTRIBUTED_BASE_COMPACTION, 1))
                         .ok());

    auto peer_request = make_submit_request(CLOUD_DISTRIBUTED_CUMULATIVE_COMPACTION, 1);
    peer_request.set_coordinator_host("coordinator");
    EXPECT_FALSE(cloud::validate_distributed_compaction_submit_request(peer_request).ok());
    peer_request.set_coordinator_brpc_port(65536);
    EXPECT_FALSE(cloud::validate_distributed_compaction_submit_request(peer_request).ok());

    PCloudDistributedCompactionSubmitResponse submit_response;
    EXPECT_FALSE(
            cloud::distributed_compaction_submit_rpc("missing-port", peer_request, &submit_response)
                    .ok());

    PCloudDistributedCompactionSubmitRequest cumulative_request;
    cumulative_request.set_compaction_type(CLOUD_DISTRIBUTED_CUMULATIVE_COMPACTION);
    PCloudDistributedCompactionTask cumulative_task;
    cumulative_task.set_group_index(0);
    cumulative_task.set_output_segment_start_id(0);
    cumulative_task.set_max_segment_num(1);
    cumulative_task.set_segment_pos_start(0);
    cumulative_task.set_segment_pos_end(1);
    EXPECT_TRUE(
            cloud::validate_distributed_compaction_task(cumulative_request, cumulative_task).ok());

    auto invalid_task = cumulative_task;
    invalid_task.set_segment_pos_end(0);
    EXPECT_FALSE(
            cloud::validate_distributed_compaction_task(cumulative_request, invalid_task).ok());
    invalid_task = cumulative_task;
    invalid_task.set_max_segment_num(0);
    EXPECT_FALSE(
            cloud::validate_distributed_compaction_task(cumulative_request, invalid_task).ok());
    invalid_task = cumulative_task;
    invalid_task.set_output_segment_start_id(std::numeric_limits<int32_t>::max());
    invalid_task.set_max_segment_num(1);
    EXPECT_FALSE(
            cloud::validate_distributed_compaction_task(cumulative_request, invalid_task).ok());

    auto set_integer_key = [](PCloudDistributedCompactionKey* key, PGenericType_TypeId type,
                              int64_t value) {
        auto* column = key->add_columns();
        column->mutable_type()->set_id(type);
        if (type == PGenericType::INT32) {
            column->add_int32_value(cast_set<int32_t>(value));
        } else {
            column->add_int64_value(value);
        }
    };
    PCloudDistributedCompactionSubmitRequest base_request;
    base_request.set_compaction_type(CLOUD_DISTRIBUTED_BASE_COMPACTION);
    PCloudDistributedCompactionTask base_task;
    base_task.set_group_index(0);
    base_task.set_output_segment_start_id(0);
    base_task.set_max_segment_num(1);
    base_task.mutable_key_range()->set_lower_inclusive(true);
    EXPECT_TRUE(cloud::validate_distributed_compaction_task(base_request, base_task).ok());

    set_integer_key(base_task.mutable_key_range()->mutable_lower_key(), PGenericType::INT32, 10);
    set_integer_key(base_task.mutable_key_range()->mutable_upper_key(), PGenericType::INT32, 10);
    EXPECT_FALSE(cloud::validate_distributed_compaction_task(base_request, base_task).ok());
    base_task.mutable_key_range()->set_upper_inclusive(true);
    EXPECT_TRUE(cloud::validate_distributed_compaction_task(base_request, base_task).ok());

    auto mismatched_key_types = base_task;
    mismatched_key_types.mutable_key_range()->clear_upper_key();
    set_integer_key(mismatched_key_types.mutable_key_range()->mutable_upper_key(),
                    PGenericType::INT64, 20);
    EXPECT_FALSE(
            cloud::validate_distributed_compaction_task(base_request, mismatched_key_types).ok());

    auto malformed_key = base_task;
    malformed_key.mutable_key_range()->clear_lower_key();
    malformed_key.mutable_key_range()->mutable_lower_key()->add_columns()->mutable_type()->set_id(
            PGenericType::INT32);
    EXPECT_FALSE(cloud::validate_distributed_compaction_task(base_request, malformed_key).ok());
}

TEST(CloudDistributedCompactionTest, validates_distributed_compaction_task_status) {
    bool completed = true;
    PCloudDistributedCompactionTaskResult result;
    PCloudDistributedCompactionTaskStatus worker_status;
    EXPECT_FALSE(cloud::validate_distributed_compaction_task_status(7, worker_status, &completed,
                                                                    &result)
                         .ok());
    EXPECT_FALSE(completed);

    worker_status.set_group_index(8);
    worker_status.set_state(CLOUD_DISTRIBUTED_COMPACTION_TASK_PENDING);
    EXPECT_FALSE(cloud::validate_distributed_compaction_task_status(7, worker_status, &completed,
                                                                    &result)
                         .ok());

    worker_status.set_group_index(7);
    EXPECT_TRUE(cloud::validate_distributed_compaction_task_status(7, worker_status, &completed,
                                                                   &result)
                        .ok());
    EXPECT_FALSE(completed);
    worker_status.set_state(CLOUD_DISTRIBUTED_COMPACTION_TASK_RUNNING);
    EXPECT_TRUE(cloud::validate_distributed_compaction_task_status(7, worker_status, &completed,
                                                                   &result)
                        .ok());
    EXPECT_FALSE(completed);

    worker_status.set_state(CLOUD_DISTRIBUTED_COMPACTION_TASK_SUCCEEDED);
    EXPECT_FALSE(cloud::validate_distributed_compaction_task_status(7, worker_status, &completed,
                                                                    &result)
                         .ok());
    worker_status.mutable_result();
    EXPECT_FALSE(cloud::validate_distributed_compaction_task_status(7, worker_status, &completed,
                                                                    &result)
                         .ok());

    Status::InternalError("injected worker failure")
            .to_protobuf(worker_status.mutable_result()->mutable_status());
    EXPECT_FALSE(cloud::validate_distributed_compaction_task_status(7, worker_status, &completed,
                                                                    &result)
                         .ok());
    worker_status.set_state(CLOUD_DISTRIBUTED_COMPACTION_TASK_FAILED);
    const Status worker_failure = cloud::validate_distributed_compaction_task_status(
            7, worker_status, &completed, &result);
    EXPECT_FALSE(worker_failure.ok());
    EXPECT_NE(worker_failure.to_string().find("injected worker failure"), std::string::npos);

    Status::OK().to_protobuf(worker_status.mutable_result()->mutable_status());
    EXPECT_FALSE(cloud::validate_distributed_compaction_task_status(7, worker_status, &completed,
                                                                    &result)
                         .ok());
    worker_status.set_state(CLOUD_DISTRIBUTED_COMPACTION_TASK_SUCCEEDED);
    EXPECT_TRUE(cloud::validate_distributed_compaction_task_status(7, worker_status, &completed,
                                                                   &result)
                        .ok());
    EXPECT_TRUE(completed);
    EXPECT_TRUE(Status::create(result.status()).ok());
}

TEST(CloudDistributedCompactionTest, validates_distributed_compaction_missed_rows) {
    int64_t missed_rows = 0;
    int64_t mapped_delete_rows = 0;
    PCloudDistributedCompactionTaskResult response;
    EXPECT_FALSE(cloud::accumulate_distributed_compaction_missed_rows(
                         response, false, 0, &missed_rows, &mapped_delete_rows)
                         .ok());
    response.set_missed_rows_count(-1);
    EXPECT_FALSE(cloud::accumulate_distributed_compaction_missed_rows(
                         response, false, 0, &missed_rows, &mapped_delete_rows)
                         .ok());

    response.set_missed_rows_count(3);
    EXPECT_TRUE(cloud::accumulate_distributed_compaction_missed_rows(
                        response, false, 0, &missed_rows, &mapped_delete_rows)
                        .ok());
    response.set_missed_rows_count(2);
    EXPECT_TRUE(cloud::accumulate_distributed_compaction_missed_rows(
                        response, false, 0, &missed_rows, &mapped_delete_rows)
                        .ok());
    EXPECT_EQ(missed_rows, 5);

    missed_rows = 0;
    response.set_missed_rows_count(6);
    EXPECT_TRUE(cloud::accumulate_distributed_compaction_missed_rows(
                        response, true, 10, &missed_rows, &mapped_delete_rows)
                        .ok());
    response.set_missed_rows_count(7);
    EXPECT_TRUE(cloud::accumulate_distributed_compaction_missed_rows(
                        response, true, 10, &missed_rows, &mapped_delete_rows)
                        .ok());
    EXPECT_EQ(mapped_delete_rows, 7);
    EXPECT_EQ(10 - mapped_delete_rows, 3);

    response.set_missed_rows_count(11);
    EXPECT_FALSE(cloud::accumulate_distributed_compaction_missed_rows(
                         response, true, 10, &missed_rows, &mapped_delete_rows)
                         .ok());
    mapped_delete_rows = 0;
    response.set_missed_rows_count(2);
    EXPECT_TRUE(cloud::accumulate_distributed_compaction_missed_rows(
                        response, true, 10, &missed_rows, &mapped_delete_rows)
                        .ok());
    response.set_missed_rows_count(5);
    EXPECT_FALSE(cloud::accumulate_distributed_compaction_missed_rows(
                         response, true, 10, &missed_rows, &mapped_delete_rows)
                         .ok());
}

TEST(CloudDistributedCompactionTest, validates_distributed_compaction_partial_rowsets) {
    RowsetId output_rowset_id;
    output_rowset_id.init(12345);
    const cloud::OutputRowsetSegmentIdSlot segment_id_slot {.start_id = 10, .capacity = 2};
    const TabletSchema tablet_schema;
    auto validate = [&](const PCloudDistributedCompactionTaskResult& response) {
        RowsetMeta partial_meta;
        std::vector<KeyBoundsPB> key_bounds;
        std::vector<uint32_t> segment_rows;
        return cloud::validate_distributed_compaction_partial_rowset(
                0, response, segment_id_slot, tablet_schema, output_rowset_id, &partial_meta,
                &key_bounds, &segment_rows);
    };
    auto make_valid_response = [&] {
        PCloudDistributedCompactionTaskResult response;
        auto* meta = response.mutable_partial_rowset_meta();
        meta->set_rowset_id(0);
        meta->set_rowset_id_v2(output_rowset_id.to_string());
        meta->set_num_segments(1);
        meta->add_segment_ids(10);
        meta->add_segments_key_bounds();
        meta->add_num_segment_rows(1);
        return response;
    };

    EXPECT_FALSE(validate(PCloudDistributedCompactionTaskResult {}).ok());
    EXPECT_TRUE(validate(make_valid_response()).ok());

    auto response = make_valid_response();
    RowsetId other_rowset_id;
    other_rowset_id.init(54321);
    response.mutable_partial_rowset_meta()->set_rowset_id_v2(other_rowset_id.to_string());
    EXPECT_FALSE(validate(response).ok());

    response = make_valid_response();
    response.mutable_partial_rowset_meta()->set_num_segments(-1);
    EXPECT_FALSE(validate(response).ok());
    response.mutable_partial_rowset_meta()->set_num_segments(3);
    EXPECT_TRUE(validate(response).is<ErrorCode::TOO_MANY_SEGMENTS>());

    response = make_valid_response();
    response.mutable_partial_rowset_meta()->clear_segment_ids();
    EXPECT_FALSE(validate(response).ok());
    response = make_valid_response();
    response.mutable_partial_rowset_meta()->set_segment_ids(0, 12);
    EXPECT_FALSE(validate(response).ok());
    response = make_valid_response();
    response.mutable_partial_rowset_meta()->set_num_segments(2);
    response.mutable_partial_rowset_meta()->add_segment_ids(10);
    response.mutable_partial_rowset_meta()->add_segments_key_bounds();
    response.mutable_partial_rowset_meta()->add_num_segment_rows(1);
    EXPECT_FALSE(validate(response).ok());
    response = make_valid_response();
    response.mutable_partial_rowset_meta()->clear_segments_key_bounds();
    EXPECT_FALSE(validate(response).ok());
    response = make_valid_response();
    response.mutable_partial_rowset_meta()->clear_num_segment_rows();
    EXPECT_FALSE(validate(response).ok());
}

// Shared coordinator runtime components.

TEST(CloudDistributedCompactionTest,
     distributed_single_rowset_compaction_caches_discovered_workers) {
    const int32_t old_ttl = config::cloud_distributed_compaction_worker_cache_ttl_ms;
    config::cloud_distributed_compaction_worker_cache_ttl_ms = 60000;
    Defer restore_ttl {[&] { config::cloud_distributed_compaction_worker_cache_ttl_ms = old_ttl; }};

    int fetch_count = 0;
    cloud::CompactionWorkerCache cache([&](std::vector<cloud::CompactionWorkerInfo>* workers) {
        ++fetch_count;
        workers->push_back({.backend_id = 2,
                            .endpoint = "be-a:8060",
                            .cloud_unique_id = "cloud-a",
                            .compute_group_id = "compute-group-a"});
        workers->push_back({.backend_id = 3,
                            .endpoint = "be-b:8060",
                            .cloud_unique_id = "cloud-b",
                            .compute_group_id = "compute-group-a"});
        return Status::OK();
    });

    std::vector<cloud::CompactionWorkerInfo> workers;
    ASSERT_TRUE(cache.get_workers(&workers).ok());
    ASSERT_EQ(workers.size(), 2);
    EXPECT_EQ(workers[0].endpoint, "be-a:8060");
    EXPECT_EQ(workers[1].endpoint, "be-b:8060");
    EXPECT_EQ(fetch_count, 1);

    workers.clear();
    ASSERT_TRUE(cache.get_workers(&workers).ok());
    EXPECT_EQ(workers.size(), 2);
    EXPECT_EQ(fetch_count, 1);

    cache.invalidate();
    ASSERT_TRUE(cache.get_workers(&workers).ok());
    EXPECT_EQ(fetch_count, 2);

    config::cloud_distributed_compaction_worker_cache_ttl_ms = 0;
    cache.invalidate();
    ASSERT_TRUE(cache.get_workers(&workers).ok());
    ASSERT_TRUE(cache.get_workers(&workers).ok());
    EXPECT_EQ(fetch_count, 4);
    config::cloud_distributed_compaction_worker_cache_ttl_ms = 60000;

    int failed_fetch_count = 0;
    cloud::CompactionWorkerCache failed_cache([&](std::vector<cloud::CompactionWorkerInfo>*) {
        ++failed_fetch_count;
        return Status::InternalError("injected FE discovery failure");
    });
    EXPECT_FALSE(failed_cache.get_workers(&workers).ok());
    EXPECT_FALSE(failed_cache.get_workers(&workers).ok());
    EXPECT_EQ(failed_fetch_count, 1);
}

TEST(CloudDistributedCompactionTest, distributed_compaction_poll_scheduler_runs_due_callback) {
    const int32_t old_interval = config::cloud_distributed_compaction_status_poll_interval_ms;
    config::cloud_distributed_compaction_status_poll_interval_ms = 10;
    Defer restore_interval {
            [&] { config::cloud_distributed_compaction_status_poll_interval_ms = old_interval; }};
    cloud::DistributedCompactionPollScheduler scheduler;
    std::mutex mutex;
    std::condition_variable cv;
    bool called = false;

    ASSERT_TRUE(scheduler
                        .schedule([&] {
                            {
                                std::lock_guard lock(mutex);
                                called = true;
                            }
                            cv.notify_one();
                        })
                        .ok());

    std::unique_lock lock(mutex);
    EXPECT_TRUE(cv.wait_for(lock, std::chrono::seconds(1), [&] { return called; }));
    lock.unlock();
    scheduler.stop();
    EXPECT_FALSE(scheduler.schedule([] {}).ok());

    config::cloud_distributed_compaction_status_poll_interval_ms = 60000;
    int pending_callback_count = 0;
    cloud::DistributedCompactionPollScheduler pending_scheduler;
    ASSERT_TRUE(pending_scheduler.schedule([&] { ++pending_callback_count; }).ok());
    pending_scheduler.stop();
    EXPECT_EQ(pending_callback_count, 1);
    pending_scheduler.stop();
    EXPECT_EQ(pending_callback_count, 1);
}

// Coordinator completion and failure paths.

TEST(CloudDistributedCompactionTest, distributed_base_completion_cleans_up_failures) {
    const bool old_enable_file_cache = config::enable_file_cache;
    config::enable_file_cache = false;
    Defer restore_config {[&] { config::enable_file_cache = old_enable_file_cache; }};

    auto* sync_point = SyncPoint::get_instance();
    sync_point->set_call_back("CloudMetaMgr::abort_tablet_job", [](auto&& args) {
        auto* result = try_any_cast_ret<Status>(args);
        result->first = Status::OK();
        result->second = true;
    });
    sync_point->enable_processing();
    Defer clear_sync_point {[&] {
        sync_point->clear_all_call_backs();
        sync_point->disable_processing();
    }};

    for (const bool fail_during_assemble : {false, true}) {
        SCOPED_TRACE(fail_during_assemble ? "assemble failure" : "remote failure");
        CloudStorageEngine engine(EngineOptions {});
        auto tablet = std::make_shared<CloudTablet>(engine, create_compaction_tablet_meta());
        CloudBaseCompaction compaction(engine, tablet);
        compaction._merge_execution_context =
                std::make_unique<Compaction::MergeInputRowsetsContext>();
        auto distributed_compaction = std::make_shared<FailingDistributedCompaction>(
                Status::InternalError("injected assemble failure"));
        compaction._distributed_compaction = distributed_compaction;

        if (fail_during_assemble) {
            std::unique_ptr<GroupRowsetWriter> output_writer;
            ASSERT_TRUE(RowsetFactory::create_empty_group_rowset_writer(&output_writer).ok());
            compaction._output_rs_writer = std::move(output_writer);
            compaction._cur_tablet_schema = tablet->tablet_schema();
        }

        Status status = compaction.complete_distributed_compaction(
                fail_during_assemble ? Status::OK()
                                     : Status::InternalError("injected remote failure"));
        EXPECT_FALSE(status.ok());
        EXPECT_EQ(distributed_compaction->assemble_calls, fail_during_assemble ? 1 : 0);
        EXPECT_EQ(distributed_compaction->finalize_calls, 1);
        EXPECT_TRUE(distributed_compaction->cancelled);
        EXPECT_EQ(compaction._distributed_compaction, nullptr);
        EXPECT_EQ(compaction._merge_execution_context, nullptr);
    }
}

TEST(CloudDistributedCompactionTest, distributed_base_local_fallback_cleans_up_merge_failure) {
    const bool old_enable_file_cache = config::enable_file_cache;
    config::enable_file_cache = false;
    Defer restore_config {[&] { config::enable_file_cache = old_enable_file_cache; }};

    auto* sync_point = SyncPoint::get_instance();
    sync_point->set_call_back("CloudMetaMgr::abort_tablet_job", [](auto&& args) {
        auto* result = try_any_cast_ret<Status>(args);
        result->first = Status::OK();
        result->second = true;
    });
    sync_point->enable_processing();
    Defer clear_sync_point {[&] {
        sync_point->clear_all_call_backs();
        sync_point->disable_processing();
    }};

    CloudStorageEngine engine(EngineOptions {});
    auto tablet = std::make_shared<CloudTablet>(engine, create_compaction_tablet_meta());
    FailingLocalMergeCloudBaseCompaction compaction(engine, tablet);
    compaction._merge_execution_context = std::make_unique<Compaction::MergeInputRowsetsContext>();

    Status status = compaction.execute_local_compact_after_distributed_fallback();
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("injected local merge failure"), std::string::npos);
    EXPECT_EQ(compaction._merge_execution_context, nullptr);
}

TEST(CloudDistributedCompactionTest, distributed_base_failure_releases_mow_lock_when_abort_fails) {
    const bool old_enable_file_cache = config::enable_file_cache;
    config::enable_file_cache = false;
    Defer restore_config {[&] { config::enable_file_cache = old_enable_file_cache; }};

    int remove_lock_calls = 0;
    auto* sync_point = SyncPoint::get_instance();
    sync_point->set_call_back("CloudMetaMgr::abort_tablet_job", [](auto&& args) {
        auto* result = try_any_cast_ret<Status>(args);
        result->first = Status::InternalError("injected abort failure");
        result->second = true;
    });
    sync_point->set_call_back("CloudMetaMgr::remove_delete_bitmap_update_lock", [&](auto&& args) {
        ++remove_lock_calls;
        *try_any_cast<bool*>(args.back()) = true;
    });
    sync_point->enable_processing();
    Defer clear_sync_point {[&] {
        sync_point->clear_all_call_backs();
        sync_point->disable_processing();
    }};

    CloudStorageEngine engine(EngineOptions {});
    auto tablet_meta = create_compaction_tablet_meta(UNIQUE_KEYS);
    tablet_meta->set_enable_unique_key_merge_on_write(true);
    auto tablet = std::make_shared<CloudTablet>(engine, tablet_meta);
    CloudBaseCompaction compaction(engine, tablet);
    compaction._merge_execution_context = std::make_unique<Compaction::MergeInputRowsetsContext>();

    Status status = compaction.handle_prepared_compaction_failure(
            Status::InternalError("injected compaction failure"));
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(remove_lock_calls, 1);
    EXPECT_EQ(compaction._merge_execution_context, nullptr);
}

TEST(CloudDistributedCompactionTest, distributed_base_resume_completes_when_engine_stops) {
    const bool old_enable = config::enable_cloud_distributed_base_compaction;
    const int64_t old_target =
            config::cloud_distributed_base_compaction_target_range_input_size_bytes;
    const bool old_enable_file_cache = config::enable_file_cache;
    Defer restore_config {[&] {
        config::enable_cloud_distributed_base_compaction = old_enable;
        config::cloud_distributed_base_compaction_target_range_input_size_bytes = old_target;
        config::enable_file_cache = old_enable_file_cache;
    }};
    config::enable_cloud_distributed_base_compaction = true;
    config::cloud_distributed_base_compaction_target_range_input_size_bytes = 1;
    config::enable_file_cache = false;

    std::shared_ptr<std::function<void(Status)>> resume;
    auto* sync_point = SyncPoint::get_instance();
    sync_point->set_call_back(
            "CloudStorageEngine::_execute_base_compaction_task::before_execute", [&](auto&& args) {
                resume = try_any_cast<std::shared_ptr<std::function<void(Status)>>>(args[0]);
                *try_any_cast<bool*>(args.back()) = true;
            });
    sync_point->set_call_back("CloudMetaMgr::abort_tablet_job", [](auto&& args) {
        auto* result = try_any_cast_ret<Status>(args);
        result->first = Status::OK();
        result->second = true;
    });
    sync_point->enable_processing();
    Defer clear_sync_point {[&] {
        sync_point->clear_all_call_backs();
        sync_point->disable_processing();
    }};

    CloudStorageEngine engine(EngineOptions {});
    auto tablet = std::make_shared<CloudTablet>(engine, create_compaction_tablet_meta());
    auto compaction = std::make_shared<CloudBaseCompaction>(engine, tablet);
    compaction->_input_rowsets_total_size = 2;
    compaction->_merge_execution_context = std::make_unique<Compaction::MergeInputRowsetsContext>();
    auto distributed_compaction = std::make_shared<FailingDistributedCompaction>();
    compaction->_distributed_compaction = distributed_compaction;
    int completion_count = 0;

    engine._execute_base_compaction_task(tablet, compaction, [&](Status status) {
        ++completion_count;
        EXPECT_FALSE(status.ok());
    });
    ASSERT_NE(resume, nullptr);
    engine._stopped = true;
    (*resume)(Status::InternalError("injected remote failure"));

    EXPECT_EQ(completion_count, 1);
    EXPECT_EQ(distributed_compaction->finalize_calls, 1);
    EXPECT_EQ(compaction->_merge_execution_context, nullptr);
}

TEST(CloudDistributedCompactionTest, distributed_base_resume_handles_pool_rejection) {
    const bool old_enable = config::enable_cloud_distributed_base_compaction;
    const int64_t old_target =
            config::cloud_distributed_base_compaction_target_range_input_size_bytes;
    const bool old_enable_file_cache = config::enable_file_cache;
    Defer restore_config {[&] {
        config::enable_cloud_distributed_base_compaction = old_enable;
        config::cloud_distributed_base_compaction_target_range_input_size_bytes = old_target;
        config::enable_file_cache = old_enable_file_cache;
    }};
    config::enable_cloud_distributed_base_compaction = true;
    config::cloud_distributed_base_compaction_target_range_input_size_bytes = 1;
    config::enable_file_cache = false;

    auto* sync_point = SyncPoint::get_instance();
    sync_point->enable_processing();
    Defer clear_sync_point {[&] {
        sync_point->clear_all_call_backs();
        sync_point->disable_processing();
    }};

    for (const bool retry_succeeds : {false, true}) {
        SCOPED_TRACE(retry_succeeds ? "retry succeeds" : "retry fails");
        sync_point->clear_all_call_backs();
        std::shared_ptr<std::function<void(Status)>> resume;
        sync_point->set_call_back(
                "CloudStorageEngine::_execute_base_compaction_task::before_execute",
                [&](auto&& args) {
                    resume = try_any_cast<std::shared_ptr<std::function<void(Status)>>>(args[0]);
                    *try_any_cast<bool*>(args.back()) = true;
                });
        sync_point->set_call_back("CloudMetaMgr::abort_tablet_job", [](auto&& args) {
            auto* result = try_any_cast_ret<Status>(args);
            result->first = Status::OK();
            result->second = true;
        });

        CloudStorageEngine engine(EngineOptions {});
        sync_point->set_call_back("cloud::schedule_distributed_compaction", [&](auto&& args) {
            auto* callback = try_any_cast<std::function<void()>*>(args[0]);
            auto* result = try_any_cast_ret<Status>(args);
            if (retry_succeeds) {
                engine._stopped = true;
                (*callback)();
                result->first = Status::OK();
            } else {
                result->first = Status::InternalError("injected retry scheduling failure");
            }
            result->second = true;
        });

        ASSERT_TRUE(ThreadPoolBuilder("BaseCompactionResumeTest")
                            .set_min_threads(0)
                            .set_max_threads(1)
                            .build(&engine._base_compaction_thread_pool)
                            .ok());
        engine._base_compaction_thread_pool->shutdown();
        auto tablet = std::make_shared<CloudTablet>(engine, create_compaction_tablet_meta());
        auto compaction = std::make_shared<CloudBaseCompaction>(engine, tablet);
        compaction->_input_rowsets_total_size = 2;
        compaction->_merge_execution_context =
                std::make_unique<Compaction::MergeInputRowsetsContext>();
        auto distributed_compaction = std::make_shared<FailingDistributedCompaction>();
        compaction->_distributed_compaction = distributed_compaction;
        int completion_count = 0;
        Status completion_status;

        engine._execute_base_compaction_task(tablet, compaction, [&](Status status) {
            ++completion_count;
            completion_status = std::move(status);
        });
        ASSERT_NE(resume, nullptr);
        (*resume)(Status::InternalError("injected remote failure"));

        EXPECT_EQ(completion_count, 1);
        EXPECT_FALSE(completion_status.ok());
        EXPECT_EQ(distributed_compaction->finalize_calls, 1);
        EXPECT_EQ(compaction->_merge_execution_context, nullptr);
        if (!retry_succeeds) {
            EXPECT_NE(completion_status.to_string().find("injected retry scheduling failure"),
                      std::string::npos);
        }
    }
}

TEST(CloudDistributedCompactionTest, distributed_cumulative_completion_cleans_up_failures) {
    const bool old_enable_file_cache = config::enable_file_cache;
    config::enable_file_cache = false;
    Defer restore_config {[&] { config::enable_file_cache = old_enable_file_cache; }};

    auto* sync_point = SyncPoint::get_instance();
    sync_point->set_call_back("CloudMetaMgr::abort_tablet_job", [](auto&& args) {
        auto* result = try_any_cast_ret<Status>(args);
        result->first = Status::OK();
        result->second = true;
    });
    sync_point->enable_processing();
    Defer clear_sync_point {[&] {
        sync_point->clear_all_call_backs();
        sync_point->disable_processing();
    }};

    for (const bool fail_during_assemble : {false, true}) {
        SCOPED_TRACE(fail_during_assemble ? "assemble failure" : "remote failure");
        CloudStorageEngine engine(EngineOptions {});
        auto tablet = std::make_shared<CloudTablet>(engine, create_compaction_tablet_meta());
        CloudCumulativeCompaction compaction(engine, tablet);
        compaction._single_rowset_compaction_segment_group_size = 2;
        compaction._merge_execution_context =
                std::make_unique<Compaction::MergeInputRowsetsContext>();
        auto distributed_compaction = std::make_shared<FailingDistributedCompaction>(
                Status::InternalError("injected assemble failure"));
        compaction._distributed_compaction = distributed_compaction;

        if (fail_during_assemble) {
            std::unique_ptr<GroupRowsetWriter> output_writer;
            ASSERT_TRUE(RowsetFactory::create_empty_group_rowset_writer(&output_writer).ok());
            compaction._output_rs_writer = std::move(output_writer);
            compaction._cur_tablet_schema = tablet->tablet_schema();
        }

        Status status = compaction.complete_distributed_compaction(
                fail_during_assemble ? Status::OK()
                                     : Status::InternalError("injected remote failure"));
        EXPECT_FALSE(status.ok());
        EXPECT_EQ(distributed_compaction->assemble_calls, fail_during_assemble ? 1 : 0);
        EXPECT_EQ(distributed_compaction->finalize_calls, 1);
        EXPECT_TRUE(distributed_compaction->cancelled);
        EXPECT_EQ(compaction._distributed_compaction, nullptr);
        EXPECT_EQ(compaction._merge_execution_context, nullptr);
    }
}

TEST(CloudDistributedCompactionTest,
     distributed_cumulative_failure_releases_mow_lock_when_abort_fails) {
    const bool old_enable_file_cache = config::enable_file_cache;
    config::enable_file_cache = false;
    Defer restore_config {[&] { config::enable_file_cache = old_enable_file_cache; }};

    int remove_lock_calls = 0;
    auto* sync_point = SyncPoint::get_instance();
    sync_point->set_call_back("CloudMetaMgr::abort_tablet_job", [](auto&& args) {
        auto* result = try_any_cast_ret<Status>(args);
        result->first = Status::InternalError("injected abort failure");
        result->second = true;
    });
    sync_point->set_call_back("CloudMetaMgr::remove_delete_bitmap_update_lock", [&](auto&& args) {
        ++remove_lock_calls;
        *try_any_cast<bool*>(args.back()) = true;
    });
    sync_point->enable_processing();
    Defer clear_sync_point {[&] {
        sync_point->clear_all_call_backs();
        sync_point->disable_processing();
    }};

    CloudStorageEngine engine(EngineOptions {});
    auto tablet_meta = create_compaction_tablet_meta(UNIQUE_KEYS);
    tablet_meta->set_enable_unique_key_merge_on_write(true);
    auto tablet = std::make_shared<CloudTablet>(engine, tablet_meta);
    CloudCumulativeCompaction compaction(engine, tablet);
    compaction._merge_execution_context = std::make_unique<Compaction::MergeInputRowsetsContext>();

    Status status = compaction.handle_prepared_compaction_failure(
            Status::InternalError("injected compaction failure"));
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(remove_lock_calls, 1);
    EXPECT_EQ(compaction._merge_execution_context, nullptr);
}

TEST(CloudDistributedCompactionTest, distributed_cumulative_resume_handles_pool_rejection) {
    const bool old_enable_file_cache = config::enable_file_cache;
    config::enable_file_cache = false;
    Defer restore_config {[&] { config::enable_file_cache = old_enable_file_cache; }};

    auto* sync_point = SyncPoint::get_instance();
    sync_point->enable_processing();
    Defer clear_sync_point {[&] {
        sync_point->clear_all_call_backs();
        sync_point->disable_processing();
    }};

    for (const bool retry_succeeds : {false, true}) {
        SCOPED_TRACE(retry_succeeds ? "retry succeeds" : "retry fails");
        sync_point->clear_all_call_backs();
        std::shared_ptr<std::function<void(Status)>> resume;
        sync_point->set_call_back(
                "CloudStorageEngine::_try_submit_cumulative_compaction_task::before_global_lock",
                [&](auto&& args) {
                    resume = try_any_cast<std::shared_ptr<std::function<void(Status)>>>(args[0]);
                    *try_any_cast<bool*>(args.back()) = true;
                });
        sync_point->set_call_back("CloudMetaMgr::abort_tablet_job", [](auto&& args) {
            auto* result = try_any_cast_ret<Status>(args);
            result->first = Status::OK();
            result->second = true;
        });

        CloudStorageEngine engine(EngineOptions {});
        sync_point->set_call_back("cloud::schedule_distributed_compaction", [&](auto&& args) {
            auto* callback = try_any_cast<std::function<void()>*>(args[0]);
            auto* result = try_any_cast_ret<Status>(args);
            if (retry_succeeds) {
                engine._stopped = true;
                (*callback)();
                result->first = Status::OK();
            } else {
                result->first = Status::InternalError("injected retry scheduling failure");
            }
            result->second = true;
        });

        ASSERT_TRUE(ThreadPoolBuilder("CumulativeCompactionResumeTest")
                            .set_min_threads(1)
                            .set_max_threads(1)
                            .build(&engine._cumu_compaction_thread_pool)
                            .ok());
        auto tablet = std::make_shared<CloudTablet>(engine, create_compaction_tablet_meta());
        auto compaction = std::make_shared<CloudCumulativeCompaction>(engine, tablet);
        compaction->_single_rowset_compaction_segment_group_size = 2;
        compaction->_merge_execution_context =
                std::make_unique<Compaction::MergeInputRowsetsContext>();
        auto distributed_compaction = std::make_shared<FailingDistributedCompaction>();
        compaction->_distributed_compaction = distributed_compaction;
        int erase_submitted_calls = 0;
        int erase_executing_calls = 0;

        auto status = engine._try_submit_cumulative_compaction_task(
                tablet, compaction, compaction->compaction_id(), [&] { ++erase_submitted_calls; },
                [&] { ++erase_executing_calls; });
        ASSERT_TRUE(status.has_value());
        ASSERT_TRUE(status->ok()) << status->to_string();
        ASSERT_TRUE(engine._cumu_compaction_thread_pool->wait_for(std::chrono::seconds(5)));
        ASSERT_NE(resume, nullptr);
        engine._cumu_compaction_thread_pool->shutdown();

        (*resume)(Status::InternalError("injected remote failure"));

        EXPECT_EQ(erase_submitted_calls, 1);
        EXPECT_EQ(erase_executing_calls, 0);
        EXPECT_EQ(distributed_compaction->finalize_calls, 1);
        EXPECT_TRUE(distributed_compaction->cancelled);
        EXPECT_EQ(compaction->_merge_execution_context, nullptr);
        EXPECT_EQ(engine._cumu_compaction_thread_pool_used_threads, 0);
        EXPECT_EQ(engine._cumu_compaction_thread_pool_small_tasks_running, 0);
        if (!retry_succeeds) {
            EXPECT_NE(tablet->get_last_cumu_compaction_status().find(
                              "injected retry scheduling failure"),
                      std::string::npos);
        }
    }
}

// Worker lifecycle and management.

TEST(CloudDistributedCompactionTest,
     distributed_single_rowset_compaction_tracks_async_task_status) {
    CloudStorageEngine engine(EngineOptions {});
    auto tablet_meta = std::make_shared<TabletMeta>(1, 2, 15673, 15674, 4, 5, TTabletSchema(), 6,
                                                    std::unordered_map<uint32_t, uint32_t> {{7, 8}},
                                                    UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                                    TCompressionType::LZ4F);
    auto tablet = std::make_shared<CloudTablet>(engine, tablet_meta);
    auto worker = std::make_shared<cloud::DistributedCompactionWorker>(engine, tablet, 0);

    PCloudDistributedCompactionTaskStatus task_status;
    worker->get_compaction_status(&task_status);
    EXPECT_EQ(task_status.state(), CLOUD_DISTRIBUTED_COMPACTION_TASK_PENDING);
    EXPECT_FALSE(task_status.has_result());

    PCloudDistributedCompactionTask task;
    task.set_group_index(3);
    worker->cancel_compaction(Status::Cancelled("injected cancellation"));
    PCloudDistributedCompactionSubmitRequest request;
    EXPECT_FALSE(worker->execute_compaction(&request, &task).ok());

    task_status.Clear();
    worker->get_compaction_status(&task_status);
    ASSERT_EQ(task_status.state(), CLOUD_DISTRIBUTED_COMPACTION_TASK_FAILED);
    ASSERT_TRUE(task_status.has_result());
    EXPECT_FALSE(Status::create(task_status.result().status()).ok());
}

TEST(CloudDistributedCompactionTest,
     distributed_compaction_worker_manager_validates_and_cleans_up) {
    CloudStorageEngine engine(EngineOptions {});
    cloud::DistributedCompactionWorkerManager manager;

    PCloudDistributedCompactionSubmitRequest submit_request;
    EXPECT_FALSE(manager.submit(submit_request, engine, 0).ok());
    submit_request.add_tasks();
    EXPECT_FALSE(manager.submit(submit_request, engine, 0).ok());

    PCloudDistributedCompactionCalcIncrementalDeleteBitmapRequest bitmap_request;
    PCloudDistributedCompactionCalcIncrementalDeleteBitmapResponse bitmap_response;
    EXPECT_FALSE(manager.calc_incremental_delete_bitmap(bitmap_request, &bitmap_response).ok());
    bitmap_request.set_tablet_id(15673);
    bitmap_request.set_execution_id("missing");
    bitmap_request.set_delete_bitmap_start_version(0);
    bitmap_request.set_delete_bitmap_end_version(1);
    bitmap_request.add_group_indexes(0);
    EXPECT_TRUE(manager.calc_incremental_delete_bitmap(bitmap_request, &bitmap_response)
                        .is<ErrorCode::NOT_FOUND>());

    PCloudDistributedCompactionFinalizeRequest finalize_request;
    EXPECT_FALSE(manager.finalize(finalize_request).ok());
    finalize_request.set_execution_id("invalid");
    finalize_request.add_group_indexes(-1);
    finalize_request.set_cancel_tasks(false);
    EXPECT_FALSE(manager.finalize(finalize_request).ok());

    auto tablet_meta = std::make_shared<TabletMeta>(1, 2, 15673, 15674, 4, 5, TTabletSchema(), 6,
                                                    std::unordered_map<uint32_t, uint32_t> {{7, 8}},
                                                    UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                                    TCompressionType::LZ4F);
    auto tablet = std::make_shared<CloudTablet>(engine, tablet_meta);
    auto cancelled_worker = std::make_shared<cloud::DistributedCompactionWorker>(engine, tablet, 0);
    manager._workers.emplace(cloud::DistributedCompactionWorkerManager::key("cancel", 1),
                             cloud::DistributedCompactionWorkerManager::WorkerEntry {
                                     .worker = cancelled_worker,
                                     .expiration_time = std::numeric_limits<int64_t>::max()});
    finalize_request.Clear();
    finalize_request.set_execution_id("cancel");
    finalize_request.add_group_indexes(1);
    finalize_request.set_cancel_tasks(true);
    ASSERT_TRUE(manager.finalize(finalize_request).ok());
    EXPECT_EQ(manager.get("cancel", 1), nullptr);
    ASSERT_TRUE(manager.finalize(finalize_request).ok());
    PCloudDistributedCompactionTaskStatus cancelled_status;
    cancelled_worker->get_compaction_status(&cancelled_status);
    EXPECT_EQ(cancelled_status.state(), CLOUD_DISTRIBUTED_COMPACTION_TASK_FAILED);

    auto bitmap_worker = std::make_shared<cloud::DistributedCompactionWorker>(engine, tablet, 0);
    manager._workers.emplace(cloud::DistributedCompactionWorkerManager::key("bitmap", 2),
                             cloud::DistributedCompactionWorkerManager::WorkerEntry {
                                     .worker = bitmap_worker,
                                     .expiration_time = std::numeric_limits<int64_t>::max()});
    bitmap_request.Clear();
    bitmap_request.set_tablet_id(tablet->tablet_id() + 1);
    bitmap_request.set_execution_id("bitmap");
    bitmap_request.set_delete_bitmap_start_version(0);
    bitmap_request.set_delete_bitmap_end_version(1);
    bitmap_request.add_group_indexes(2);
    EXPECT_FALSE(manager.calc_incremental_delete_bitmap(bitmap_request, &bitmap_response).ok());

    auto expired_worker = std::make_shared<cloud::DistributedCompactionWorker>(engine, tablet, 0);
    manager._workers.emplace(cloud::DistributedCompactionWorkerManager::key("expired", 3),
                             cloud::DistributedCompactionWorkerManager::WorkerEntry {
                                     .worker = expired_worker, .expiration_time = 0});
    EXPECT_EQ(manager.get("expired", 3), nullptr);

    auto swept_worker = std::make_shared<cloud::DistributedCompactionWorker>(engine, tablet, 0);
    manager._workers.emplace(cloud::DistributedCompactionWorkerManager::key("sweep", 4),
                             cloud::DistributedCompactionWorkerManager::WorkerEntry {
                                     .worker = swept_worker, .expiration_time = 10});
    manager.remove_expired_workers(10);
    EXPECT_EQ(manager.get("sweep", 4), nullptr);

    finalize_request.Clear();
    finalize_request.set_execution_id("bitmap");
    finalize_request.add_group_indexes(2);
    finalize_request.set_cancel_tasks(false);
    EXPECT_TRUE(manager.finalize(finalize_request).ok());
}

} // namespace doris
