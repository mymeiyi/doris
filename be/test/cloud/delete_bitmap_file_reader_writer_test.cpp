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
#include <gtest/gtest.h>

#include <filesystem>
#include <memory>

#include "agent/be_exec_version_manager.h"
#include "common/object_pool.h"
#include "gen_cpp/internal_service.pb.h"
#include "gmock/gmock.h"
#include "io/fs/local_file_system.h"
#include "cloud/delete_bitmap_file_reader.h"
#include "cloud/delete_bitmap_file_writer.h"
#include "runtime/exec_env.h"
#include "testutil/test_util.h"
#include "util/proto_util.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

class DeleteBitmapFileReaderWriterTest : public testing::Test {
public:
    // create a mock cgroup folder
    /*virtual void SetUp() {
        static_cast<void>(io::global_local_filesystem()->create_directory(_s_test_data_path));
    }

    // delete the mock cgroup folder
    virtual void TearDown() {
        static_cast<void>(io::global_local_filesystem()->delete_directory(_s_test_data_path));
    }

    static std::string _s_test_data_path;*/
};

// std::string WalReaderWriterTest::_s_test_data_path = "./log/delete_bitmap_file_reader_writer_test";
// size_t block_rows = 1024;

/*void covert_block_to_pb(
        const vectorized::Block& block, PBlock* pblock,
        segment_v2::CompressionTypePB compression_type = segment_v2::CompressionTypePB::SNAPPY) {
    size_t uncompressed_bytes = 0;
    size_t compressed_bytes = 0;
    Status st = block.serialize(BeExecVersionManager::get_newest_version(), pblock,
                                &uncompressed_bytes, &compressed_bytes, compression_type);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(uncompressed_bytes >= compressed_bytes);
    EXPECT_EQ(compressed_bytes, pblock->column_values().size());

    const vectorized::ColumnWithTypeAndName& type_and_name =
            block.get_columns_with_type_and_name()[0];
    EXPECT_EQ(type_and_name.name, pblock->column_metas()[0].name());
}

void generate_block(PBlock& pblock, int row_index) {
    auto vec = vectorized::ColumnInt32::create();
    auto& data = vec->get_data();
    for (int i = 0; i < block_rows; ++i) {
        data.push_back(i + row_index);
    }
    vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt32>());
    vectorized::ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type, "test_int");
    vectorized::Block block({type_and_name});
    covert_block_to_pb(block, &pblock, segment_v2::CompressionTypePB::SNAPPY);
}*/

TEST_F(DeleteBitmapFileReaderWriterTest, TestWriteAndRead) {
    std::unique_ptr<ThreadPool> _pool;
    std::ignore = ThreadPoolBuilder("S3FileUploadThreadPool")
                          .set_min_threads(5)
                          .set_max_threads(10)
                          .build(&_pool);
    ExecEnv::GetInstance()->_s3_file_upload_thread_pool = std::move(_pool);

    S3Conf s3_conf {.bucket = "bucket",
                    .prefix = "prefix",
                    .client_conf = {
                            .endpoint = "endpoint",
                            .region = "region",
                            .ak = "ak",
                            .sk = "sk",
                            .token = "",
                            .bucket = "",
                            .role_arn = "",
                            .external_id = "",
                    }};
    auto res = io::S3FileSystem::create(std::move(s3_conf), io::FileSystem::TMP_FS_ID);
    ASSERT_TRUE(res.has_value()) << res.error();
    StorageResource storage_resource(res.value());
    std::optional<StorageResource> storage_resource_op = std::make_optional<StorageResource>(storage_resource);;

    int64_t tablet_id = 43231;
    std::string rowset_id = "432w1abc2";
    DeleteBitmapPB delete_bitmap_pb;
    DeleteBitmapFileWriter file_writer(tablet_id, rowset_id, storage_resource_op);
    EXPECT_TRUE(file_writer.init().ok());
    EXPECT_TRUE(file_writer.write(delete_bitmap_pb).ok());
    EXPECT_TRUE(file_writer.close().ok());
}
} // namespace doris