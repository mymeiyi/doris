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

#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "cloud/cloud_committed_rs_mgr.h"
#include "cloud/cloud_rowset_builder.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_txn_delete_bitmap_cache.h"
#include "cloud/config.h"
#include "load/channel/load_stream_writer.h"
#include "runtime/exec_env.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/tablet/tablet_meta.h"
#include "util/time.h"

namespace doris {
namespace {

class EmptyLoadRowsetBuilder : public CloudRowsetBuilder {
public:
    using CloudRowsetBuilder::CloudRowsetBuilder;

    Status init() override {
        skip_metadata_at_init = _skip_writing_rowset_metadata;
        return Status::InternalError("stop before preparing rowset metadata");
    }

    Status commit_rowset(const std::string&, int64_t) override {
        ++commit_calls;
        return Status::InternalError("injected commit failure");
    }

    bool skip_metadata_at_init = false;
    int commit_calls = 0;
};

} // namespace

class CloudLoadStreamTest : public testing::Test {
protected:
    void SetUp() override {
        _old_cloud_unique_id = config::cloud_unique_id;
        _old_skip_empty = config::skip_writing_empty_rowset_metadata;
        _old_make_visible = config::enable_cloud_make_rs_visible_on_be;
        config::cloud_unique_id = "cloud_load_stream_test";
        config::enable_cloud_make_rs_visible_on_be = true;
        _old_engine = std::move(ExecEnv::GetInstance()->_storage_engine);
        auto engine = std::make_unique<CloudStorageEngine>(EngineOptions {});
        _engine = engine.get();
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
        _engine->_committed_rs_mgr = std::make_unique<CloudCommittedRSMgr>();
        _engine->_txn_delete_bitmap_cache = std::make_unique<CloudTxnDeleteBitmapCache>(1024);
        ASSERT_TRUE(_engine->_txn_delete_bitmap_cache->init().ok());
    }

    void TearDown() override {
        ExecEnv::GetInstance()->set_storage_engine(std::move(_old_engine));
        config::cloud_unique_id = _old_cloud_unique_id;
        config::skip_writing_empty_rowset_metadata = _old_skip_empty;
        config::enable_cloud_make_rs_visible_on_be = _old_make_visible;
    }

    CloudTabletSPtr create_tablet(KeysType keys_type, bool mow) {
        TabletMetaPB meta;
        meta.set_tablet_id(10001);
        meta.set_table_id(10002);
        meta.set_enable_unique_key_merge_on_write(mow);
        meta.mutable_schema()->set_keys_type(keys_type);
        auto tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->init_from_pb(meta);
        auto tablet = std::make_shared<CloudTablet>(*_engine, tablet_meta);
        std::unique_lock lock(tablet->get_header_lock());
        tablet->reset_approximate_stats(0, 0, 0, 0);
        return tablet;
    }

    CloudStorageEngine* _engine = nullptr;
    std::unique_ptr<BaseStorageEngine> _old_engine;
    std::string _old_cloud_unique_id;
    bool _old_skip_empty = false;
    bool _old_make_visible = false;
};

TEST_F(CloudLoadStreamTest, SetEmptyPolicyBeforePreparingMetadata) {
    for (bool skip_empty : {false, true}) {
        config::skip_writing_empty_rowset_metadata = skip_empty;
        for (bool is_empty : {false, true}) {
            WriteRequest req;
            LoadStreamWriter writer(&req, nullptr);
            auto builder = std::make_unique<EmptyLoadRowsetBuilder>(*_engine, req, nullptr);
            auto* observed_builder = builder.get();
            writer._rowset_builder = std::move(builder);
            auto st = writer.init(is_empty);
            ASSERT_FALSE(st.ok());
            EXPECT_NE(st.to_string().find("stop before preparing"), std::string::npos);
            EXPECT_EQ(observed_builder->skip_metadata_at_init, skip_empty && is_empty);
        }
    }
}

TEST_F(CloudLoadStreamTest, EmptyCommitSkipsRpcAndRegistersMarkers) {
    for (auto keys_type : {DUP_KEYS, AGG_KEYS, UNIQUE_KEYS}) {
        for (bool mow : {false, true}) {
            if (mow && keys_type != UNIQUE_KEYS) {
                continue;
            }
            WriteRequest req;
            req.tablet_id = 10001;
            req.txn_id = 100 + keys_type * 2 + mow;
            req.txn_expiration = UnixSeconds() + 3600;
            EmptyLoadRowsetBuilder builder(*_engine, req, nullptr);
            auto tablet = create_tablet(keys_type, mow);
            builder._tablet = tablet;
            builder._tablet_schema = tablet->tablet_schema();
            auto meta = std::make_shared<RowsetMeta>();
            meta->set_rowset_type(BETA_ROWSET);
            meta->set_rowset_state(PREPARED);
            meta->set_num_segments(0);
            ASSERT_TRUE(
                    RowsetFactory::create_rowset(builder._tablet_schema, "", meta, &builder._rowset)
                            .ok());
            builder.set_skip_writing_rowset_metadata(true);

            auto st = builder.commit_txn();
            ASSERT_TRUE(st.ok()) << st.to_string();
            EXPECT_EQ(builder.commit_calls, 0);
            EXPECT_TRUE(builder._is_committed);
            EXPECT_EQ(tablet->fetch_add_approximate_num_rowsets(0), 1);
            if (mow) {
                EXPECT_TRUE(_engine->txn_delete_bitmap_cache().is_empty_rowset(req.txn_id,
                                                                               req.tablet_id));
            } else {
                auto marker =
                        _engine->committed_rs_mgr().get_committed_rowset(req.txn_id, req.tablet_id);
                ASSERT_TRUE(marker.has_value());
                EXPECT_EQ(marker->first, nullptr);
                EXPECT_GE(marker->second, req.txn_expiration);
            }
        }
    }
}

TEST_F(CloudLoadStreamTest, MetadataCommitFailureDoesNotRegisterMarker) {
    WriteRequest req;
    req.tablet_id = 10001;
    req.txn_id = 200;
    EmptyLoadRowsetBuilder builder(*_engine, req, nullptr);
    auto tablet = create_tablet(DUP_KEYS, false);
    builder._tablet = tablet;
    builder.set_skip_writing_rowset_metadata(false);

    auto st = builder.commit_txn();
    ASSERT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("injected commit failure"), std::string::npos);
    EXPECT_EQ(builder.commit_calls, 1);
    EXPECT_FALSE(builder._is_committed);
    EXPECT_EQ(tablet->fetch_add_approximate_num_rowsets(0), 0);
    EXPECT_FALSE(_engine->committed_rs_mgr()
                         .get_committed_rowset(req.txn_id, req.tablet_id)
                         .has_value());
}

} // namespace doris
