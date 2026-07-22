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

#include "storage/iterator/vertical_block_reader.h"

#include <gtest/gtest.h>

#include "storage/rowset/rowset_meta.h"

namespace doris {

class VerticalBlockReaderTestAccessor {
public:
    static std::vector<bool> grouped_iterator_init_flags(
            const RowsetMeta& rowset_meta, std::pair<int64_t, int64_t> segment_offsets,
            size_t added_iterators) {
        std::vector<bool> iterator_init_flags;
        VerticalBlockReader::_append_grouped_iterator_init_flags(
                rowset_meta, segment_offsets, added_iterators, &iterator_init_flags);
        return iterator_init_flags;
    }
};

TEST(VerticalBlockReaderTest, GroupedIteratorInitFlagsForWholeRowset) {
    RowsetMeta rowset_meta;
    rowset_meta.set_num_segments(5);
    rowset_meta.set_segment_group_sizes({2, 2, 1});

    EXPECT_EQ(VerticalBlockReaderTestAccessor::grouped_iterator_init_flags(rowset_meta, {0, 0}, 5),
              std::vector<bool>({true, false, true, false, true}));
}

TEST(VerticalBlockReaderTest, GroupedIteratorInitFlagsForSegmentRange) {
    RowsetMeta rowset_meta;
    rowset_meta.set_num_segments(5);
    rowset_meta.set_segment_group_sizes({2, 2, 1});

    EXPECT_EQ(VerticalBlockReaderTestAccessor::grouped_iterator_init_flags(rowset_meta, {1, 4}, 3),
              std::vector<bool>({true, true, false}));
    EXPECT_EQ(VerticalBlockReaderTestAccessor::grouped_iterator_init_flags(rowset_meta, {2, 4}, 2),
              std::vector<bool>({true, false}));
}

} // namespace doris
