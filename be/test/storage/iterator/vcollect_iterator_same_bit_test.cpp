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

#include "storage/iterator/vcollect_iterator.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "storage/iterator/block_reader.h"
#include "storage/iterators.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/schema.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

// Keep external linkage so unused gMock helpers do not trigger -Wunused-member-function.
class MockMergeRowsetReader : public RowsetReader {
public:
    MOCK_METHOD(Status, init, (RowsetReaderContext*, const RowSetSplits&), (override));
    MOCK_METHOD(Status, get_segment_iterators,
                (RowsetReaderContext*, std::vector<RowwiseIteratorUPtr>*, bool), (override));
    MOCK_METHOD(void, reset_read_options, (), (override));
    MOCK_METHOD(void, set_preferred_file_cache_peer, (const std::string&, int32_t), (override));
    MOCK_METHOD(Status, next_batch, (Block*), (override));
    MOCK_METHOD(Status, next_batch, (BlockView*), (override));
    MOCK_METHOD(Status, next_batch, (BlockWithSameBit*), (override));
    MOCK_METHOD(bool, delete_flag, (), (override));
    MOCK_METHOD(Version, version, (), (override));
    MOCK_METHOD(RowsetSharedPtr, rowset, (), (override));
    MOCK_METHOD(const ReadSchema&, read_schema, (), (const, override));
    MOCK_METHOD(int64_t, filtered_rows, (), (override));
    MOCK_METHOD(uint64_t, merged_rows, (), (override));
    MOCK_METHOD(int64_t, newest_write_timestamp, (), (override));
    MOCK_METHOD(void, update_profile, (RuntimeProfile*), (override));
    MOCK_METHOD(RowsetReaderSharedPtr, clone, (), (override));
    MOCK_METHOD(void, set_topn_limit, (size_t), (override));

    bool is_merge_iterator() const override { return true; }
    RowsetTypePB type() const override { return BETA_ROWSET; }
};

namespace {

Status fill_block(BlockWithSameBit* output, int64_t key, bool first_is_same) {
    EXPECT_TRUE(output->block->empty());
    EXPECT_TRUE(output->same_bit.empty());
    auto columns_guard = output->block->mutate_columns_scoped();
    auto& column = assert_cast<ColumnInt64&>(*columns_guard.mutable_columns()[0]);
    column.insert_value(key);
    column.insert_value(key);
    output->same_bit = {first_is_same, true};
    return Status::OK();
}

class VCollectIteratorSameBitTest : public testing::TestWithParam<bool> {};

TEST_P(VCollectIteratorSameBitTest, PreserveSameBitAcrossBlocks) {
    auto key_column = std::make_shared<TabletColumn>();
    key_column->set_name("k");
    key_column->set_type(FieldType::OLAP_FIELD_TYPE_BIGINT);
    key_column->set_unique_id(0);
    key_column->set_is_key(true);
    key_column->set_is_nullable(false);
    BlockReader reader;
    reader._read_schema =
            std::make_shared<ReadSchema>(std::vector<TabletColumnPtr> {key_column});

    const bool same_key = GetParam();
    const int64_t next_key = same_key ? 1 : 2;
    auto rowset_reader = std::make_shared<testing::StrictMock<MockMergeRowsetReader>>();
    EXPECT_CALL(*rowset_reader, next_batch(testing::An<BlockWithSameBit*>()))
            .WillOnce([](BlockWithSameBit* output) { return fill_block(output, 1, false); })
            .WillOnce([=](BlockWithSameBit* output) {
                return fill_block(output, next_key, same_key);
            })
            .WillOnce([](BlockWithSameBit* output) {
                EXPECT_TRUE(output->block->empty());
                return Status::Error<ErrorCode::END_OF_FILE>("");
            });

    VCollectIterator::Level0Iterator iterator(rowset_reader, &reader);
    ASSERT_TRUE(iterator.init(/*get_data_by_ref=*/false).ok());
    auto row = *iterator.current_row_ref();
    EXPECT_EQ(row.row_pos, 0);
    EXPECT_FALSE(row.is_same);
    ASSERT_TRUE(iterator.next(&row).ok());
    EXPECT_EQ(row.row_pos, 1);
    EXPECT_TRUE(row.is_same);

    // The first row of a refreshed block must retain the rowset reader's same-key marker.
    ASSERT_TRUE(iterator.next(&row).ok());
    EXPECT_EQ(row.row_pos, 0);
    EXPECT_EQ(assert_cast<const ColumnInt64&>(*row.block->get_by_position(0).column)
                      .get_data()[row.row_pos],
              next_key);
    EXPECT_EQ(row.is_same, same_key);
    ASSERT_TRUE(iterator.next(&row).ok());
    EXPECT_EQ(row.row_pos, 1);
    EXPECT_TRUE(row.is_same);
    EXPECT_TRUE(iterator.next(&row).is<ErrorCode::END_OF_FILE>());
}

INSTANTIATE_TEST_SUITE_P(BlockBoundary, VCollectIteratorSameBitTest, testing::Bool());

} // namespace
} // namespace doris
