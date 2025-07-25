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
#include <brpc/controller.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <bthread/types.h>
#include <butil/errno.h>
#include <fmt/format.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/callback.h>
#include <parallel_hashmap/phmap.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <functional>
#include <initializer_list>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <queue>
#include <set>
#include <span>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/thread_context.h"
#include "runtime/types.h"
#include "util/countdown_latch.h"
#include "util/debug_points.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"
#include "vec/columns/column.h"
#include "vec/common/allocator.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {
#include "common/compile_check_begin.h"
class TabletSchema;
class LoadStreamStub;

struct SegmentStatistics;

using IndexToTabletSchema = phmap::parallel_flat_hash_map<
        int64_t, std::shared_ptr<TabletSchema>, std::hash<int64_t>, std::equal_to<int64_t>,
        std::allocator<phmap::Pair<const int64_t, std::shared_ptr<TabletSchema>>>, 4, std::mutex>;

using IndexToEnableMoW =
        phmap::parallel_flat_hash_map<int64_t, bool, std::hash<int64_t>, std::equal_to<int64_t>,
                                      std::allocator<phmap::Pair<const int64_t, bool>>, 4,
                                      std::mutex>;

class LoadStreamReplyHandler : public brpc::StreamInputHandler {
public:
    LoadStreamReplyHandler(PUniqueId load_id, int64_t dst_id, std::weak_ptr<LoadStreamStub> stub)
            : _load_id(load_id), _dst_id(dst_id), _stub(stub) {}

    int on_received_messages(brpc::StreamId id, butil::IOBuf* const messages[],
                             size_t size) override;

    void on_idle_timeout(brpc::StreamId id) override {}

    void on_closed(brpc::StreamId id) override;

    friend std::ostream& operator<<(std::ostream& ostr, const LoadStreamReplyHandler& handler);

private:
    PUniqueId _load_id;   // for logging
    int64_t _dst_id = -1; // for logging
    std::weak_ptr<LoadStreamStub> _stub;
};

class LoadStreamStub : public std::enable_shared_from_this<LoadStreamStub> {
    friend class LoadStreamReplyHandler;

public:
    // construct new stub
    LoadStreamStub(PUniqueId load_id, int64_t src_id,
                   std::shared_ptr<IndexToTabletSchema> schema_map,
                   std::shared_ptr<IndexToEnableMoW> mow_map, bool incremental = false);

    LoadStreamStub(UniqueId load_id, int64_t src_id,
                   std::shared_ptr<IndexToTabletSchema> schema_map,
                   std::shared_ptr<IndexToEnableMoW> mow_map, bool incremental = false)
            : LoadStreamStub(load_id.to_proto(), src_id, schema_map, mow_map, incremental) {};

// for mock this class in UT
#ifdef BE_TEST
    virtual
#endif
            ~LoadStreamStub();

    // open_load_stream
    Status open(BrpcClientCache<PBackendService_Stub>* client_cache, const NodeInfo& node_info,
                int64_t txn_id, const OlapTableSchemaParam& schema,
                const std::vector<PTabletID>& tablets_for_schema, int total_streams,
                int64_t idle_timeout_ms, bool enable_profile);

// for mock this class in UT
#ifdef BE_TEST
    virtual
#endif
            // segment_id is limited by max_segment_num_per_rowset (default value of 1000),
            // so in practice it will not exceed the range of i16.

            // APPEND_DATA
            Status
            append_data(int64_t partition_id, int64_t index_id, int64_t tablet_id,
                        int32_t segment_id, uint64_t offset, std::span<const Slice> data,
                        bool segment_eos = false, FileType file_type = FileType::SEGMENT_FILE);

    // ADD_SEGMENT
    Status add_segment(int64_t partition_id, int64_t index_id, int64_t tablet_id,
                       int32_t segment_id, const SegmentStatistics& segment_stat,
                       TabletSchemaSPtr flush_schema);

    // CLOSE_LOAD
    Status close_load(const std::vector<PTabletID>& tablets_to_commit);

    // GET_SCHEMA
    Status get_schema(const std::vector<PTabletID>& tablets);

    // wait remote to close stream,
    // remote will close stream when it receives CLOSE_LOAD
    Status close_finish_check(RuntimeState* state, bool* is_closed);

    // cancel the stream, abort close_wait, mark _is_closed and _is_cancelled
    void cancel(Status reason);

    Status wait_for_schema(int64_t partition_id, int64_t index_id, int64_t tablet_id,
                           int64_t timeout_ms = 60000);

    Status wait_for_new_schema(int64_t timeout_ms) {
        std::unique_lock<bthread::Mutex> lock(_schema_mutex);
        if (timeout_ms > 0) {
            int ret = _schema_cv.wait_for(lock, timeout_ms * 1000);
            return ret == 0 ? Status::OK() : Status::Error<true>(ret, "wait schema update timeout");
        }
        _schema_cv.wait(lock);
        return Status::OK();
    };

    std::shared_ptr<TabletSchema> tablet_schema(int64_t index_id) const {
        return (*_tablet_schema_for_index)[index_id];
    }

    bool enable_unique_mow(int64_t index_id) const {
        return _enable_unique_mow_for_index->at(index_id);
    }

    std::vector<int64_t> success_tablets() {
        std::lock_guard<bthread::Mutex> lock(_success_tablets_mutex);
        return _success_tablets;
    }

    std::unordered_map<int64_t, Status> failed_tablets() {
        std::lock_guard<bthread::Mutex> lock(_failed_tablets_mutex);
        return _failed_tablets;
    }

    brpc::StreamId stream_id() const { return _stream_id; }

    int64_t src_id() const { return _src_id; }

    int64_t dst_id() const { return _dst_id; }

    bool is_open() const { return _is_open.load(); }

    bool is_incremental() const { return _is_incremental; }

    friend std::ostream& operator<<(std::ostream& ostr, const LoadStreamStub& stub);

    std::string to_string();

    // for tests only
    void add_success_tablet(int64_t tablet_id) {
        std::lock_guard<bthread::Mutex> lock(_success_tablets_mutex);
        _success_tablets.push_back(tablet_id);
    }

    void add_failed_tablet(int64_t tablet_id, Status reason) {
        std::lock_guard<bthread::Mutex> lock(_failed_tablets_mutex);
        _failed_tablets[tablet_id] = reason;
    }

    void add_bytes_written(size_t bytes) {
        std::lock_guard<bthread::Mutex> lock(_write_mutex);
        _bytes_written += bytes;
    }

    int64_t bytes_written() {
        std::lock_guard<bthread::Mutex> lock(_write_mutex);
        return _bytes_written;
    }

    Status check_cancel() {
        DBUG_EXECUTE_IF("LoadStreamStub._check_cancel.cancelled",
                        { return Status::InternalError("stream cancelled"); });
        if (!_is_cancelled.load()) {
            return Status::OK();
        }
        std::lock_guard<bthread::Mutex> lock(_cancel_mutex);
        return Status::Cancelled("load_id={}, reason: {}", print_id(_load_id),
                                 _cancel_st.to_string_no_stack());
    }

private:
    Status _encode_and_send(PStreamHeader& header, std::span<const Slice> data = {});
    Status _send_with_buffer(butil::IOBuf& buf, bool sync = false);
    Status _send_with_retry(butil::IOBuf& buf);
    void _handle_failure(butil::IOBuf& buf, Status st);

protected:
    std::atomic<bool> _is_init;
    std::atomic<bool> _is_open;
    std::atomic<bool> _is_closing;
    std::atomic<bool> _is_closed;
    std::atomic<bool> _is_cancelled;
    std::atomic<bool> _is_eos;

    PUniqueId _load_id;
    brpc::StreamId _stream_id;
    int64_t _src_id = -1; // source backend_id
    int64_t _dst_id = -1; // destination backend_id
    Status _status = Status::InternalError<false>("Stream is not open");
    Status _cancel_st;

    bthread::Mutex _open_mutex;
    bthread::Mutex _cancel_mutex;

    std::mutex _buffer_mutex;
    std::mutex _send_mutex;
    butil::IOBuf _buffer;

    bthread::Mutex _schema_mutex;
    bthread::ConditionVariable _schema_cv;
    std::shared_ptr<IndexToTabletSchema> _tablet_schema_for_index;
    std::shared_ptr<IndexToEnableMoW> _enable_unique_mow_for_index;

    bthread::Mutex _success_tablets_mutex;
    bthread::Mutex _failed_tablets_mutex;
    std::vector<int64_t> _success_tablets;
    std::unordered_map<int64_t, Status> _failed_tablets;

    bool _is_incremental = false;

    bthread::Mutex _write_mutex;
    size_t _bytes_written = 0;
};

// a collection of LoadStreams connect to the same node
class LoadStreamStubs {
public:
    LoadStreamStubs(size_t num_streams, UniqueId load_id, int64_t src_id,
                    std::shared_ptr<IndexToTabletSchema> schema_map,
                    std::shared_ptr<IndexToEnableMoW> mow_map, bool incremental = false)
            : _is_incremental(incremental) {
        _streams.reserve(num_streams);
        for (size_t i = 0; i < num_streams; i++) {
            _streams.emplace_back(
                    new LoadStreamStub(load_id, src_id, schema_map, mow_map, incremental));
        }
    }

    Status open(BrpcClientCache<PBackendService_Stub>* client_cache, const NodeInfo& node_info,
                int64_t txn_id, const OlapTableSchemaParam& schema,
                const std::vector<PTabletID>& tablets_for_schema, int total_streams,
                int64_t idle_timeout_ms, bool enable_profile);

    bool is_incremental() const { return _is_incremental; }

    size_t size() const { return _streams.size(); }

    // for UT only
    void mark_open() { _open_success.store(true); }

    std::shared_ptr<LoadStreamStub> select_one_stream() {
        if (!_open_success.load()) {
            return nullptr;
        }
        size_t i = _select_index.fetch_add(1);
        return _streams[i % _streams.size()];
    }

    void cancel(Status reason) {
        for (auto& stream : _streams) {
            stream->cancel(reason);
        }
    }

    Status close_load(const std::vector<PTabletID>& tablets_to_commit);

    std::unordered_set<int64_t> success_tablets() {
        std::unordered_set<int64_t> s;
        for (auto& stream : _streams) {
            auto v = stream->success_tablets();
            std::copy(v.begin(), v.end(), std::inserter(s, s.end()));
        }
        return s;
    }

    std::unordered_map<int64_t, Status> failed_tablets() {
        std::unordered_map<int64_t, Status> m;
        for (auto& stream : _streams) {
            auto v = stream->failed_tablets();
            m.insert(v.begin(), v.end());
        }
        return m;
    }

    std::vector<std::shared_ptr<LoadStreamStub>> streams() { return _streams; }

private:
    std::vector<std::shared_ptr<LoadStreamStub>> _streams;
    std::atomic<bool> _open_success = false;
    std::atomic<size_t> _select_index = 0;
    const bool _is_incremental;
};

} // namespace doris

#include "common/compile_check_end.h"
