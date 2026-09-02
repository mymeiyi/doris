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
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "core/types.h"
#include "core/value/vdatetime_value.h"
#include "enterprise/distributed-compaction/distributed_compaction_config.h"
#include "enterprise/distributed-compaction/distributed_compaction_impl.h"
#include "storage/tablet/tablet_meta.h"
#include "util/defer_op.h"
#include "util/uid_util.h"

namespace doris {

TEST(CloudDistributedCompactionTest, chooses_weighted_short_key_boundaries) {
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
    EXPECT_TRUE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_DECIMAL));
    EXPECT_TRUE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_DECIMAL32));
    EXPECT_TRUE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_DECIMAL64));
    EXPECT_TRUE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_DECIMAL128I));
    EXPECT_TRUE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_DECIMAL256));
    EXPECT_FALSE(cloud::is_supported_key_range_column_type(FieldType::OLAP_FIELD_TYPE_STRING));

    const std::vector<cloud::IntegerKeySample> samples = {
            {.key = 30, .weight = 10}, {.key = 0, .weight = 10}, {.key = 20, .weight = 5},
            {.key = 10, .weight = 5},  {.key = 20, .weight = 5}, {.key = 10, .weight = 5}};
    EXPECT_EQ(cloud::choose_integer_key_range_boundaries(samples, 4),
              (std::vector<int128_t> {10, 20, 30}));

    const std::vector<cloud::IntegerKeySample> hot_key = {
            {.key = 0, .weight = 90}, {.key = 10, .weight = 5}, {.key = 20, .weight = 5}};
    EXPECT_EQ(cloud::choose_integer_key_range_boundaries(hot_key, 4), (std::vector<int128_t> {10}));

    const std::vector<cloud::IntegerKeySample> edge_keys = {
            {.key = std::numeric_limits<int128_t>::min(), .weight = 10},
            {.key = std::numeric_limits<int128_t>::max(), .weight = 10}};
    EXPECT_EQ(cloud::choose_integer_key_range_boundaries(edge_keys, 2),
              (std::vector<int128_t> {std::numeric_limits<int128_t>::max()}));

    const std::vector<cloud::StringKeySample> string_samples = {
            {.key = "delta", .weight = 10},  {.key = "alpha", .weight = 10},
            {.key = "charlie", .weight = 5}, {.key = "bravo", .weight = 5},
            {.key = "charlie", .weight = 5}, {.key = "bravo", .weight = 5}};
    EXPECT_EQ(cloud::choose_string_key_range_boundaries(string_samples, 4),
              (std::vector<std::string> {"bravo", "charlie", "delta"}));
}

TEST(CloudDistributedCompactionTest, distributed_base_compaction_rejects_zorder) {
    const bool old_enable = config::enable_cloud_distributed_base_compaction;
    const int64_t old_target =
            config::cloud_distributed_base_compaction_target_range_input_size_bytes;
    Defer restore_config {[&] {
        config::enable_cloud_distributed_base_compaction = old_enable;
        config::cloud_distributed_base_compaction_target_range_input_size_bytes = old_target;
    }};
    config::enable_cloud_distributed_base_compaction = true;
    config::cloud_distributed_base_compaction_target_range_input_size_bytes = 1;

    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    auto* column = schema_pb.add_column();
    column->set_unique_id(0);
    column->set_name("key");
    column->set_type("INT");
    column->set_is_key(true);
    column->set_is_nullable(false);
    auto tablet_meta = std::make_shared<TabletMeta>();
    tablet_meta->mutable_tablet_schema()->init_from_pb(schema_pb);
    CloudStorageEngine engine(EngineOptions {});
    CloudTablet tablet(engine, tablet_meta);
    EXPECT_TRUE(cloud::can_use_distributed_base_compaction(tablet, 2));

    schema_pb.set_sort_type(SortType::ZORDER);
    tablet_meta->mutable_tablet_schema()->init_from_pb(schema_pb);
    EXPECT_FALSE(cloud::can_use_distributed_base_compaction(tablet, 2));
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
}

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

} // namespace doris
