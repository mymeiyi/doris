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

#include "cloud/cloud_cumulative_compaction.h"

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/internal_service.pb.h>

#include <limits>
#include <unordered_map>
#include <unordered_set>

#include "cloud/cloud_distributed_single_rowset_compaction.h"
#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_tablet_mgr.h"
#include "cloud/config.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "service/backend_options.h"
#include "storage/compaction/compaction.h"
#include "storage/compaction/cumulative_compaction_policy.h"
#include "storage/merger.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/tablet/tablet_schema.h"
#include "util/debug_points.h"
#include "util/network_util.h"
#include "util/threadpool.h"
#include "util/trace.h"
#include "util/uuid_generator.h"

namespace doris {
using namespace ErrorCode;

namespace cloud {

bool is_single_rowset_compaction_candidate(const RowsetSharedPtr& rowset) {
    const auto& rowset_meta = rowset->rowset_meta();
    const int64_t overlap_unit_count =
            rowset_meta->segments_overlap() == NONOVERLAPPING_WITHIN_GROUP
                    ? static_cast<int64_t>(rowset_meta->segment_group_sizes().size())
                    : rowset->num_segments();
    return !rowset_meta->has_delete_predicate() && rowset_meta->is_segments_overlapping() &&
           overlap_unit_count >= config::cloud_single_rowset_compaction_min_segments;
}

bool should_use_single_rowset_grouped_compaction(const std::vector<RowsetSharedPtr>& input_rowsets,
                                                 const TabletSchema& tablet_schema,
                                                 std::string_view compaction_policy) {
    return compaction_policy == CUMULATIVE_SIZE_BASED_POLICY &&
           tablet_schema.num_key_columns() > 0 && tablet_schema.cluster_key_uids().empty() &&
           config::enable_cloud_single_rowset_compaction && input_rowsets.size() == 1 &&
           is_single_rowset_compaction_candidate(input_rowsets.front());
}

std::vector<SegmentGroupMergeRange> build_segment_group_merge_ranges(const RowsetMeta& rowset_meta,
                                                                     int64_t segment_group_size) {
    DORIS_CHECK_GT(segment_group_size, 1);
    DORIS_CHECK_GT(rowset_meta.num_segments(), 0);

    std::vector<SegmentGroupMergeRange> ranges;
    if (rowset_meta.segments_overlap() == NONOVERLAPPING_WITHIN_GROUP) {
        const auto& input_segment_group_sizes = rowset_meta.segment_group_sizes();
        const int64_t input_group_count = cast_set<int64_t>(input_segment_group_sizes.size());
        DORIS_CHECK_GT(input_group_count, 0);
        ranges.reserve(cast_set<size_t>((input_group_count + segment_group_size - 1) /
                                        segment_group_size));

        int64_t segment_pos_end = 0;
        for (int64_t group_start = 0; group_start < input_group_count;
             group_start += segment_group_size) {
            const int64_t group_end = std::min(group_start + segment_group_size, input_group_count);
            const int64_t segment_pos_start = segment_pos_end;
            for (int64_t group_index = group_start; group_index < group_end; ++group_index) {
                const int32_t input_group_size =
                        input_segment_group_sizes.Get(cast_set<int>(group_index));
                DORIS_CHECK_GT(input_group_size, 0);
                segment_pos_end += input_group_size;
            }

            ranges.push_back({.segment_pos_start = segment_pos_start,
                              .segment_pos_end = segment_pos_end,
                              .merge_way_num = group_end - group_start});
        }
        DORIS_CHECK_EQ(segment_pos_end, rowset_meta.num_segments());
    } else {
        ranges.reserve(cast_set<size_t>((rowset_meta.num_segments() + segment_group_size - 1) /
                                        segment_group_size));
        for (int64_t segment_pos_start = 0; segment_pos_start < rowset_meta.num_segments();
             segment_pos_start += segment_group_size) {
            const int64_t segment_pos_end =
                    std::min(segment_pos_start + segment_group_size, rowset_meta.num_segments());
            ranges.push_back({.segment_pos_start = segment_pos_start,
                              .segment_pos_end = segment_pos_end,
                              .merge_way_num = segment_pos_end - segment_pos_start});
        }
    }
    return ranges;
}

} // namespace cloud

bvar::Adder<uint64_t> cumu_output_size("cumu_compaction", "output_size");
bvar::LatencyRecorder g_cu_compaction_hold_delete_bitmap_lock_time_ms(
        "cu_compaction_hold_delete_bitmap_lock_time_ms");

CloudCumulativeCompaction::CloudCumulativeCompaction(CloudStorageEngine& engine,
                                                     CloudTabletSPtr tablet)
        : CloudCompactionMixin(engine, tablet,
                               "BaseCompaction:" + std::to_string(tablet->tablet_id())) {}

CloudCumulativeCompaction::~CloudCumulativeCompaction() = default;

Status CloudCumulativeCompaction::prepare_compact() {
    DBUG_EXECUTE_IF("CloudCumulativeCompaction.prepare_compact.sleep", { sleep(5); })
    Status st;
    Defer defer_set_st([&] {
        if (!st.ok()) {
            cloud_tablet()->set_last_cumu_compaction_status(st.to_string());
        }
    });
    if (_tablet->tablet_state() != TABLET_RUNNING &&
        (!config::enable_new_tablet_do_compaction ||
         static_cast<CloudTablet*>(_tablet.get())->alter_version() == -1)) {
        st = Status::InternalError("invalid tablet state. tablet_id={}", _tablet->tablet_id());
        return st;
    }

    std::vector<std::shared_ptr<CloudCumulativeCompaction>> cumu_compactions;
    _engine.get_cumu_compaction(_tablet->tablet_id(), cumu_compactions);
    if (!cumu_compactions.empty()) {
        for (auto& cumu : cumu_compactions) {
            _max_conflict_version =
                    std::max(_max_conflict_version, cumu->_input_rowsets.back()->end_version());
        }
    }

    bool need_sync_tablet = true;
    {
        std::shared_lock rlock(_tablet->get_header_lock());
        // If number of rowsets is equal to approximate_num_rowsets, it is very likely that this tablet has been
        // synchronized with meta-service.
        if (_tablet->tablet_meta()->all_rs_metas().size() >=
                    cloud_tablet()->fetch_add_approximate_num_rowsets(0) &&
            cloud_tablet()->last_sync_time_s > 0) {
            need_sync_tablet = false;
        }
    }
    if (need_sync_tablet) {
        st = cloud_tablet()->sync_rowsets();
        RETURN_IF_ERROR(st);
    }

    // pick rowsets to compact
    st = pick_rowsets_to_compact();
    if (!st.ok()) {
        if (_last_delete_version.first != -1) {
            // we meet a delete version, should increase the cumulative point to let base compaction handle the delete version.
            // plus 1 to skip the delete version.
            // NOTICE: after that, the cumulative point may be larger than max version of this tablet, but it doesn't matter.
            update_cumulative_point();
            if (!config::enable_sleep_between_delete_cumu_compaction) {
                st = Status::Error<CUMULATIVE_MEET_DELETE_VERSION>(
                        "cumulative compaction meet delete version");
            }
        }
        return st;
    }

    for (auto& rs : _input_rowsets) {
        _input_row_num += rs->num_rows();
        _input_segments += rs->num_segments();
        _input_rowsets_data_size += rs->data_disk_size();
        _input_rowsets_index_size += rs->index_disk_size();
        _input_rowsets_total_size += rs->total_disk_size();
    }
    LOG_INFO("start CloudCumulativeCompaction, tablet_id={}, range=[{}-{}]", _tablet->tablet_id(),
             _input_rowsets.front()->start_version(), _input_rowsets.back()->end_version())
            .tag("job_id", _uuid)
            .tag("input_rowsets", _input_rowsets.size())
            .tag("input_rows", _input_row_num)
            .tag("input_segments", _input_segments)
            .tag("input_rowsets_data_size", _input_rowsets_data_size)
            .tag("input_rowsets_index_size", _input_rowsets_index_size)
            .tag("input_rowsets_total_size", _input_rowsets_total_size)
            .tag("tablet_max_version", cloud_tablet()->max_version_unlocked())
            .tag("cumulative_point", cloud_tablet()->cumulative_layer_point())
            .tag("num_rowsets", cloud_tablet()->fetch_add_approximate_num_rowsets(0))
            .tag("cumu_num_rowsets", cloud_tablet()->fetch_add_approximate_cumu_num_rowsets(0));
    return st;
}

Status CloudCumulativeCompaction::request_global_lock() {
    // prepare compaction job
    cloud::TabletJobInfoPB job;
    auto idx = job.mutable_idx();
    idx->set_tablet_id(_tablet->tablet_id());
    idx->set_table_id(_tablet->table_id());
    idx->set_index_id(_tablet->index_id());
    idx->set_partition_id(_tablet->partition_id());
    auto compaction_job = job.add_compaction();
    compaction_job->set_id(_uuid);
    compaction_job->set_initiator(BackendOptions::get_localhost() + ':' +
                                  std::to_string(config::heartbeat_service_port));
    compaction_job->set_type(cloud::TabletCompactionJobPB::CUMULATIVE);
    compaction_job->set_base_compaction_cnt(_base_compaction_cnt);
    compaction_job->set_cumulative_compaction_cnt(_cumulative_compaction_cnt);
    using namespace std::chrono;
    int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    _expiration = now + config::compaction_timeout_seconds;
    compaction_job->set_expiration(_expiration);
    compaction_job->set_lease(now + config::lease_compaction_interval_seconds * 4);

    compaction_job->add_input_versions(_input_rowsets.front()->start_version());
    compaction_job->add_input_versions(_input_rowsets.back()->end_version());
    // Set input version range to let meta-service check version range conflict
    compaction_job->set_check_input_versions_range(config::enable_parallel_cumu_compaction);
    cloud::StartTabletJobResponse resp;
    Status st = _engine.meta_mgr().prepare_tablet_job(job, &resp);
    if (!st.ok()) {
        if (resp.status().code() == cloud::STALE_TABLET_CACHE) {
            // set last_sync_time to 0 to force sync tablet next time
            cloud_tablet()->last_sync_time_s = 0;
        } else if (resp.status().code() == cloud::TABLET_NOT_FOUND) {
            // tablet not found
            cloud_tablet()->clear_cache();
        } else if (resp.status().code() == cloud::JOB_TABLET_BUSY) {
            LOG_WARNING("failed to prepare cumu compaction")
                    .tag("job_id", _uuid)
                    .tag("msg", resp.status().msg());
            return Status::Error<CUMULATIVE_NO_SUITABLE_VERSION>(
                    "cumu no suitable versions: job tablet busy");
        } else if (resp.status().code() == cloud::JOB_CHECK_ALTER_VERSION) {
            (static_cast<CloudTablet*>(_tablet.get()))->set_alter_version(resp.alter_version());
            std::stringstream ss;
            ss << "failed to prepare cumu compaction. Check compaction input versions "
                  "failed in schema change. "
                  "input_version_start="
               << compaction_job->input_versions(0)
               << " input_version_end=" << compaction_job->input_versions(1)
               << " schema_change_alter_version=" << resp.alter_version();
            std::string msg = ss.str();
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }
    }
    return st;
}

Status CloudCumulativeCompaction::execute_compact() {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("CloudCumulativeCompaction::execute_compact_impl",
                                      Status::OK(), this);

    SCOPED_ATTACH_TASK(_mem_tracker);

    using namespace std::chrono;
    auto start = steady_clock::now();
    Status st;
    Defer defer_set_st([&] {
        cloud_tablet()->set_last_cumu_compaction_status(st.to_string());
        if (!st.ok()) {
            cloud_tablet()->set_last_cumu_compaction_failure_time(UnixMillis());
        } else {
            cloud_tablet()->set_last_cumu_compaction_success_time(UnixMillis());
        }
    });
    st = CloudCompactionMixin::execute_compact();
    if (!st.ok()) {
        LOG(WARNING) << "fail to do " << compaction_name() << ". res=" << st
                     << ", tablet=" << _tablet->tablet_id()
                     << ", output_version=" << _output_version;
        return st;
    }
    LOG_INFO("finish CloudCumulativeCompaction, tablet_id={}, cost={}ms, range=[{}-{}]",
             _tablet->tablet_id(), duration_cast<milliseconds>(steady_clock::now() - start).count(),
             _input_rowsets.front()->start_version(), _input_rowsets.back()->end_version())
            .tag("job_id", _uuid)
            .tag("input_rowsets", _input_rowsets.size())
            .tag("input_rows", _input_row_num)
            .tag("input_segments", _input_segments)
            .tag("input_rowsets_data_size", _input_rowsets_data_size)
            .tag("input_rowsets_index_size", _input_rowsets_index_size)
            .tag("input_rowsets_total_size", _input_rowsets_total_size)
            .tag("output_rows", _output_rowset->num_rows())
            .tag("output_segments", _output_rowset->num_segments())
            .tag("output_rowset_data_size", _output_rowset->data_disk_size())
            .tag("output_rowset_index_size", _output_rowset->index_disk_size())
            .tag("output_rowset_total_size", _output_rowset->total_disk_size())
            .tag("tablet_max_version", _tablet->max_version_unlocked())
            .tag("cumulative_point", cloud_tablet()->cumulative_layer_point())
            .tag("num_rowsets", cloud_tablet()->fetch_add_approximate_num_rowsets(0))
            .tag("cumu_num_rowsets", cloud_tablet()->fetch_add_approximate_cumu_num_rowsets(0))
            .tag("local_read_time_us", _stats.cloud_local_read_time)
            .tag("remote_read_time_us", _stats.cloud_remote_read_time)
            .tag("local_read_bytes", _local_read_bytes_total)
            .tag("remote_read_bytes", _remote_read_bytes_total);

    _state = CompactionState::SUCCESS;

    DorisMetrics::instance()->cumulative_compaction_deltas_total->increment(_input_rowsets.size());
    DorisMetrics::instance()->cumulative_compaction_bytes_total->increment(
            _input_rowsets_total_size);
    cumu_output_size << _output_rowset->total_disk_size();

    st = Status::OK();
    return st;
}

bool CloudCumulativeCompaction::should_calculate_new_cumulative_point(
        int64_t input_cumulative_point) const {
    if (!_single_rowset_compaction_segment_group_size.has_value()) {
        return true;
    }

    DORIS_CHECK_EQ(_input_rowsets.size(), 1);
    DORIS_CHECK(_output_rowset != nullptr);
    return _input_rowsets.front()->start_version() == input_cumulative_point &&
           _output_rowset->rowset_meta()->segments_overlap() == NONOVERLAPPING;
}

Status CloudCumulativeCompaction::modify_rowsets() {
    // calculate new cumulative point
    int64_t input_cumulative_point = cloud_tablet()->cumulative_layer_point();
    auto compaction_policy = cloud_tablet()->tablet_meta()->compaction_policy();
    int64_t new_cumulative_point = input_cumulative_point;
    if (should_calculate_new_cumulative_point(input_cumulative_point)) {
        new_cumulative_point =
                _engine.cumu_compaction_policy(compaction_policy)
                        ->new_cumulative_point(cloud_tablet(), _output_rowset, _last_delete_version,
                                               input_cumulative_point);
    }
    // commit compaction job
    cloud::TabletJobInfoPB job;
    auto idx = job.mutable_idx();
    idx->set_tablet_id(_tablet->tablet_id());
    idx->set_table_id(_tablet->table_id());
    idx->set_index_id(_tablet->index_id());
    idx->set_partition_id(_tablet->partition_id());
    auto compaction_job = job.add_compaction();
    compaction_job->set_id(_uuid);
    compaction_job->set_initiator(BackendOptions::get_localhost() + ':' +
                                  std::to_string(config::heartbeat_service_port));
    compaction_job->set_type(cloud::TabletCompactionJobPB::CUMULATIVE);
    compaction_job->set_input_cumulative_point(input_cumulative_point);
    compaction_job->set_output_cumulative_point(new_cumulative_point);
    compaction_job->set_num_input_rows(_input_row_num);
    compaction_job->set_num_output_rows(_output_rowset->num_rows());
    compaction_job->set_size_input_rowsets(_input_rowsets_total_size);
    compaction_job->set_size_output_rowsets(_output_rowset->total_disk_size());
    compaction_job->set_num_input_segments(_input_segments);
    compaction_job->set_num_output_segments(_output_rowset->num_segments());
    compaction_job->set_num_input_rowsets(num_input_rowsets());
    compaction_job->set_num_output_rowsets(1);
    compaction_job->add_input_versions(_input_rowsets.front()->start_version());
    compaction_job->add_input_versions(_input_rowsets.back()->end_version());
    compaction_job->add_output_versions(_output_rowset->end_version());
    compaction_job->add_txn_id(_output_rowset->txn_id());
    compaction_job->add_output_rowset_ids(_output_rowset->rowset_id().to_string());
    compaction_job->set_index_size_input_rowsets(_input_rowsets_index_size);
    compaction_job->set_segment_size_input_rowsets(_input_rowsets_data_size);
    compaction_job->set_index_size_output_rowsets(_output_rowset->index_disk_size());
    compaction_job->set_segment_size_output_rowsets(_output_rowset->data_disk_size());

    DBUG_EXECUTE_IF("CloudCumulativeCompaction::modify_rowsets.enable_spin_wait", {
        LOG(INFO) << "CloudCumulativeCompaction::modify_rowsets.enable_spin_wait, start";
        while (DebugPoints::instance()->is_enable(
                "CloudCumulativeCompaction::modify_rowsets.block")) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        LOG(INFO) << "CloudCumulativeCompaction::modify_rowsets.enable_spin_wait, exit";
    });

    // Block only NOTREADY tablets (SC new tablets) before compaction commit.
    // RUNNING tablets (system tables, base tablets) are not affected.
    DBUG_EXECUTE_IF("CloudCumulativeCompaction::modify_rowsets.block_notready", {
        if (_tablet->tablet_state() == TABLET_NOTREADY) {
            LOG(INFO) << "block NOTREADY tablet compaction before commit"
                      << ", tablet_id=" << _tablet->tablet_id() << ", output=["
                      << _input_rowsets.front()->start_version() << "-"
                      << _input_rowsets.back()->end_version() << "]";
            while (DebugPoints::instance()->is_enable(
                    "CloudCumulativeCompaction::modify_rowsets.block_notready")) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
            LOG(INFO) << "release NOTREADY tablet compaction, tablet_id=" << _tablet->tablet_id();
        }
    });

    DeleteBitmapPtr output_rowset_delete_bitmap = nullptr;
    int64_t initiator = this->initiator();
    int64_t get_delete_bitmap_lock_start_time = 0;
    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        if (_distributed_single_rowset_state != nullptr) {
            RETURN_IF_ERROR(finish_distributed_mow_delete_bitmap(
                    initiator, &output_rowset_delete_bitmap,
                    &get_delete_bitmap_lock_start_time));
        } else {
            RETURN_IF_ERROR(cloud_tablet()->calc_delete_bitmap_for_compaction(
                    _input_rowsets, _output_rowset, *_rowid_conversion, compaction_type(),
                    _stats.merged_rows, _stats.filtered_rows, initiator,
                    output_rowset_delete_bitmap, _allow_delete_in_cumu_compaction,
                    get_delete_bitmap_lock_start_time));
        }
        LOG_INFO("update delete bitmap in CloudCumulativeCompaction, tablet_id={}, range=[{}-{}]",
                 _tablet->tablet_id(), _input_rowsets.front()->start_version(),
                 _input_rowsets.back()->end_version())
                .tag("job_id", _uuid)
                .tag("initiator", initiator)
                .tag("input_rowsets", _input_rowsets.size())
                .tag("input_rows", _input_row_num)
                .tag("input_segments", _input_segments)
                .tag("number_output_delete_bitmap",
                     output_rowset_delete_bitmap->delete_bitmap.size());
        compaction_job->set_delete_bitmap_lock_initiator(initiator);
    }

    DBUG_EXECUTE_IF("CumulativeCompaction.modify_rowsets.trigger_abort_job_failed", {
        LOG(INFO) << "CumulativeCompaction.modify_rowsets.trigger_abort_job_failed for tablet_id"
                  << cloud_tablet()->tablet_id();
        return Status::InternalError(
                "CumulativeCompaction.modify_rowsets.trigger_abort_job_failed for tablet_id {}",
                cloud_tablet()->tablet_id());
    });
    cloud::FinishTabletJobResponse resp;
    _distributed_commit_started = _distributed_single_rowset_state != nullptr;
    auto st = _engine.meta_mgr().commit_tablet_job(job, &resp);
    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        int64_t hold_delete_bitmap_lock_time_ms =
                (MonotonicMicros() - get_delete_bitmap_lock_start_time) / 1000;
        g_cu_compaction_hold_delete_bitmap_lock_time_ms << hold_delete_bitmap_lock_time_ms;
    }
    if (resp.has_alter_version()) {
        (static_cast<CloudTablet*>(_tablet.get()))->set_alter_version(resp.alter_version());
    }
    if (!st.ok()) {
        if (resp.status().code() == cloud::TABLET_NOT_FOUND) {
            cloud_tablet()->clear_cache();
        } else if (resp.status().code() == cloud::JOB_CHECK_ALTER_VERSION) {
            std::stringstream ss;
            ss << "failed to prepare cumu compaction. Check compaction input versions "
                  "failed in schema change. "
                  "input_version_start="
               << compaction_job->input_versions(0)
               << " input_version_end=" << compaction_job->input_versions(1)
               << " schema_change_alter_version=" << resp.alter_version();
            std::string msg = ss.str();
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }
        return st;
    }
    finish_distributed_workers(true);

    auto& stats = resp.stats();
    LOG(INFO) << "tablet stats=" << stats.ShortDebugString();
    {
        std::unique_lock wrlock(_tablet->get_header_lock());
        // clang-format off
        cloud_tablet()->set_last_base_compaction_success_time(std::max(cloud_tablet()->last_base_compaction_success_time(), stats.last_base_compaction_time_ms()));
        cloud_tablet()->set_last_cumu_compaction_success_time(std::max(cloud_tablet()->last_cumu_compaction_success_time(), stats.last_cumu_compaction_time_ms()));
        cloud_tablet()->set_last_full_compaction_success_time(std::max(cloud_tablet()->last_full_compaction_success_time(), stats.last_full_compaction_time_ms()));
        // clang-format on
        if (cloud_tablet()->cumulative_compaction_cnt() >= stats.cumulative_compaction_cnt()) {
            // This could happen while calling `sync_tablet_rowsets` during `commit_tablet_job`, or parallel cumu compactions which are
            // committed later increase tablet.cumulative_compaction_cnt (see CloudCompactionTest.parallel_cumu_compaction)
            return Status::OK();
        }
        // Try to make output rowset visible immediately in tablet cache, instead of waiting for next synchronization from meta-service.
        if (stats.cumulative_point() > cloud_tablet()->cumulative_layer_point() &&
            stats.cumulative_compaction_cnt() != cloud_tablet()->cumulative_compaction_cnt() + 1) {
            // This could happen when there are multiple parallel cumu compaction committed, tablet cache lags several
            // cumu compactions behind meta-service (stats.cumulative_compaction_cnt > tablet.cumulative_compaction_cnt + 1).
            // If `cumu_point` of the tablet cache also falls behind, MUST ONLY synchronize tablet cache from meta-service,
            // otherwise may cause the tablet to be unable to synchronize the rowset meta changes generated by other cumu compaction.
            return Status::OK();
        }
        if (_input_rowsets.size() == 1) {
            DCHECK_EQ(_output_rowset->version(), _input_rowsets[0]->version());
            // MUST NOT move input rowset to stale path
            cloud_tablet()->add_rowsets({_output_rowset}, true, wrlock, true);
        } else {
            cloud_tablet()->delete_rowsets(_input_rowsets, wrlock);
            cloud_tablet()->add_rowsets({_output_rowset}, false, wrlock);
        }
        // ATTN: MUST NOT update `base_compaction_cnt` which are used when sync rowsets, otherwise may cause
        // the tablet to be unable to synchronize the rowset meta changes generated by base compaction.
        cloud_tablet()->set_cumulative_compaction_cnt(stats.cumulative_compaction_cnt());
        cloud_tablet()->set_cumulative_layer_point(stats.cumulative_point());
        if (output_rowset_delete_bitmap) {
            _tablet->tablet_meta()->delete_bitmap().merge(*output_rowset_delete_bitmap);
        }
        if (stats.base_compaction_cnt() >= cloud_tablet()->base_compaction_cnt()) {
            cloud_tablet()->reset_approximate_stats(stats.num_rowsets(), stats.num_segments(),
                                                    stats.num_rows(), stats.data_size());
        }
    }
    // agg delete bitmap for pre rowsets
    if (config::enable_agg_and_remove_pre_rowsets_delete_bitmap &&
        _tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write() && _input_rowsets.size() != 1) {
        OlapStopWatch watch;
        std::vector<RowsetSharedPtr> pre_rowsets {};
        {
            std::shared_lock rlock(_tablet->get_header_lock());
            for (const auto& it2 : cloud_tablet()->rowset_map()) {
                if (it2.first.second < _output_rowset->start_version()) {
                    pre_rowsets.emplace_back(it2.second);
                }
            }
        }
        std::sort(pre_rowsets.begin(), pre_rowsets.end(), Rowset::comparator);
        auto pre_rowsets_delete_bitmap = std::make_shared<DeleteBitmap>(_tablet->tablet_id());
        std::map<std::string, int64_t> pre_rowset_to_versions;
        cloud_tablet()->agg_delete_bitmap_for_compaction(
                _output_rowset->start_version(), _output_rowset->end_version(), pre_rowsets,
                pre_rowsets_delete_bitmap, pre_rowset_to_versions);
        // update delete bitmap to ms
        DBUG_EXECUTE_IF(
                "CumulativeCompaction.modify_rowsets.cloud_update_delete_bitmap_without_lock.block",
                DBUG_BLOCK);
        auto status = _engine.meta_mgr().cloud_update_delete_bitmap_without_lock(
                *cloud_tablet(), pre_rowsets_delete_bitmap.get(), pre_rowset_to_versions,
                cloud_tablet()->table_id(), _output_rowset->start_version(),
                _output_rowset->end_version());
        if (!status.ok()) {
            LOG(WARNING) << "failed to agg pre rowsets delete bitmap to ms. tablet_id="
                         << _tablet->tablet_id() << ", pre rowset num=" << pre_rowsets.size()
                         << ", output version=" << _output_rowset->version().to_string()
                         << ", status=" << status.to_string();
        } else {
            LOG(INFO) << "agg pre rowsets delete bitmap to ms. tablet_id=" << _tablet->tablet_id()
                      << ", pre rowset num=" << pre_rowsets.size()
                      << ", output version=" << _output_rowset->version().to_string()
                      << ", cost(us)=" << watch.get_elapse_time_us();
        }
    }
    DBUG_EXECUTE_IF("CumulativeCompaction.modify_rowsets.delete_expired_stale_rowset", {
        LOG(INFO) << "delete_expired_stale_rowsets for tablet=" << _tablet->tablet_id();
        _engine.tablet_mgr().vacuum_stale_rowsets(CountDownLatch(1));
    });

    _tablet->prefill_dbm_agg_cache_after_compaction(_output_rowset);
    return Status::OK();
}

Status CloudCumulativeCompaction::garbage_collection() {
    // Once commit has been sent, a transport error cannot prove that Meta Service did not commit.
    // Keep the remote files in that case to avoid deleting a visible rowset.
    finish_distributed_workers(_distributed_commit_started);
    RETURN_IF_ERROR(CloudCompactionMixin::garbage_collection());
    cloud::TabletJobInfoPB job;
    auto idx = job.mutable_idx();
    idx->set_tablet_id(_tablet->tablet_id());
    idx->set_table_id(_tablet->table_id());
    idx->set_index_id(_tablet->index_id());
    idx->set_partition_id(_tablet->partition_id());
    auto compaction_job = job.add_compaction();
    compaction_job->set_id(_uuid);
    compaction_job->set_initiator(BackendOptions::get_localhost() + ':' +
                                  std::to_string(config::heartbeat_service_port));
    compaction_job->set_type(cloud::TabletCompactionJobPB::CUMULATIVE);
    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        compaction_job->set_delete_bitmap_lock_initiator(this->initiator());
    }
    DBUG_EXECUTE_IF("CumulativeCompaction.modify_rowsets.trigger_abort_job_failed", {
        LOG(INFO) << "CumulativeCompaction.modify_rowsets.abort_job_failed for tablet_id"
                  << cloud_tablet()->tablet_id();
        return Status::InternalError(
                "CumulativeCompaction.modify_rowsets.abort_job_failed for tablet_id {}",
                cloud_tablet()->tablet_id());
    });
    auto st = _engine.meta_mgr().abort_tablet_job(job);
    if (!st.ok()) {
        LOG_WARNING("failed to abort compaction job")
                .tag("job_id", _uuid)
                .tag("tablet_id", _tablet->tablet_id())
                .error(st);
    }
    return st;
}

Status CloudCumulativeCompaction::pick_rowsets_to_compact() {
    _input_rowsets.clear();
    _single_rowset_compaction_segment_group_size.reset();

    std::vector<RowsetSharedPtr> candidate_rowsets;
    {
        std::shared_lock rlock(_tablet->get_header_lock());
        _base_compaction_cnt = cloud_tablet()->base_compaction_cnt();
        _cumulative_compaction_cnt = cloud_tablet()->cumulative_compaction_cnt();
        int64_t candidate_version = std::max(
                std::max(cloud_tablet()->cumulative_layer_point(), _max_conflict_version + 1),
                cloud_tablet()->alter_version() + 1);
        // Get all rowsets whose version >= `candidate_version` as candidate rowsets
        cloud_tablet()->traverse_rowsets_unlocked(
                [&candidate_rowsets, candidate_version](const RowsetSharedPtr& rs) {
                    if (rs->start_version() >= candidate_version) {
                        candidate_rowsets.push_back(rs);
                    }
                });
    }
    if (candidate_rowsets.empty()) {
        return Status::Error<CUMULATIVE_NO_SUITABLE_VERSION>(
                "no suitable versions: candidate rowsets empty");
    }
    std::sort(candidate_rowsets.begin(), candidate_rowsets.end(), Rowset::comparator);
    if (auto st = check_version_continuity(candidate_rowsets); !st.ok()) {
        DCHECK(false) << st;
        return st;
    }

    int64_t max_score = config::cumulative_compaction_max_deltas;
    double process_memory_usage =
            cast_set<double>(doris::GlobalMemoryArbitrator::process_memory_usage());
    bool memory_usage_high =
            process_memory_usage > cast_set<double>(MemInfo::soft_mem_limit()) * 0.8;
    if (cloud_tablet()->last_compaction_status.is<ErrorCode::MEM_LIMIT_EXCEEDED>() ||
        memory_usage_high) {
        max_score = std::max(config::cumulative_compaction_max_deltas /
                                     config::cumulative_compaction_max_deltas_factor,
                             config::cumulative_compaction_min_deltas + 1);
    }

    size_t compaction_score = 0;
    auto compaction_policy = cloud_tablet()->tablet_meta()->compaction_policy();
    _engine.cumu_compaction_policy(compaction_policy)
            ->pick_input_rowsets(cloud_tablet(), candidate_rowsets, max_score,
                                 config::cumulative_compaction_min_deltas, &_input_rowsets,
                                 &_last_delete_version, &compaction_score);

    const int64_t segment_group_size = config::cloud_single_rowset_compaction_segment_group_size;
    if (config::enable_cloud_single_rowset_compaction && segment_group_size > 1) {
        for (const auto& rowset : _input_rowsets) {
            if (cloud::should_use_single_rowset_grouped_compaction(
                        {rowset}, *cloud_tablet()->tablet_schema(), compaction_policy)) {
                auto grouped_input_rowset = rowset;
                _input_rowsets = {std::move(grouped_input_rowset)};
                _single_rowset_compaction_segment_group_size = segment_group_size;
                return Status::OK();
            }
        }
    }

    if (_input_rowsets.empty()) {
        return Status::Error<CUMULATIVE_NO_SUITABLE_VERSION>(
                "no suitable versions: input rowsets empty");
    } else if (_input_rowsets.size() == 1 &&
               !_input_rowsets.front()->rowset_meta()->is_segments_overlapping()) {
        VLOG_DEBUG << "there is only one rowset and not overlapping. tablet_id="
                   << _tablet->tablet_id() << ", version=" << _input_rowsets.front()->version();
        return Status::Error<CUMULATIVE_NO_SUITABLE_VERSION>(
                "no suitable versions: only one rowset and not overlapping");
    }

    apply_txn_size_truncation_and_log("CloudCumulativeCompaction");
    return Status::OK();
}

Status CloudCumulativeCompaction::prepare_merge_input_rowsets(MergeInputRowsetsResult* result) {
    if (!_single_rowset_compaction_segment_group_size.has_value()) {
        return Status::OK();
    }

    const int64_t segment_group_size = *_single_rowset_compaction_segment_group_size;
    DORIS_CHECK_GT(segment_group_size, 1);
    result->is_segment_grouped = true;
    result->segment_group_size = segment_group_size;
    return Status::OK();
}

Status CloudCumulativeCompaction::try_distributed_single_rowset_compaction(
        MergeInputRowsetsResult* result, bool* compacted) {
    *compacted = false;
    if (!config::enable_cloud_single_rowset_distributed_compaction) {
        return Status::OK();
    }

    DORIS_CHECK(result->is_segment_grouped);
    DORIS_CHECK_EQ(_input_rowsets.size(), 1);
    const auto& input_rowset = _input_rowsets.front();
    const auto segment_ranges = cloud::build_segment_group_merge_ranges(
            *input_rowset->rowset_meta(), result->segment_group_size);
    if (segment_ranges.size() < 2) {
        return Status::OK();
    }

    std::vector<std::string> workers;
    RETURN_IF_ERROR(cloud::parse_single_rowset_compaction_workers(
            config::cloud_single_rowset_compaction_workers,
            get_host_port(BackendOptions::get_localhost(), config::brpc_port), &workers));
    if (workers.size() < 2) {
        return Status::OK();
    }
    workers.resize(std::min(workers.size(), segment_ranges.size()));

    std::vector<cloud::SingleRowsetSegmentIdSlot> segment_slots;
    RETURN_IF_ERROR(cloud::build_single_rowset_segment_id_slots(
            _output_rs_writer->get_allocated_segment_id(),
            config::cloud_single_rowset_compaction_segment_slot_capacity, segment_ranges.size(),
            &segment_slots));

    const bool is_mow = _tablet->keys_type() == KeysType::UNIQUE_KEYS &&
                        _tablet->enable_unique_key_merge_on_write();
    const bool check_missed_rows =
            is_mow &&
            (config::enable_missing_rows_correctness_check ||
             config::enable_mow_compaction_correctness_check_core ||
             config::enable_mow_compaction_correctness_check_fail) &&
            !_allow_delete_in_cumu_compaction;
    int64_t phase1_end_version = 0;
    if (is_mow) {
        RETURN_IF_ERROR(_engine.meta_mgr().sync_tablet_rowsets(cloud_tablet()));
        phase1_end_version = cloud_tablet()->max_version().second;
    }

    _distributed_single_rowset_state =
            std::make_unique<cloud::DistributedSingleRowsetCompactionState>();
    auto& distributed_state = *_distributed_single_rowset_state;
    distributed_state.execution_id = _uuid;
    distributed_state.phase1_end_version = phase1_end_version;
    distributed_state.output_delete_bitmap =
            std::make_shared<DeleteBitmap>(_tablet->tablet_id());
    distributed_state.tasks.reserve(segment_ranges.size());
    for (size_t group_index = 0; group_index < segment_ranges.size(); ++group_index) {
        distributed_state.tasks.push_back(
                {.worker_endpoint = workers[group_index % workers.size()],
                 .group_index = cast_set<int32_t>(group_index),
                 .attempt_id = 0,
                 .segment_id_slot = segment_slots[group_index]});
    }

    const auto input_meta_pb = input_rowset->rowset_meta()->get_rowset_pb();
    const auto output_meta_pb = _output_rs_writer->rowset_meta()->get_rowset_pb();
    const std::string output_rowset_id = _output_rs_writer->rowset_id().to_string();
    std::vector<PCloudSingleRowsetCompactionRequest> requests(segment_ranges.size());
    std::vector<PCloudSingleRowsetCompactionResponse> responses(segment_ranges.size());
    for (size_t group_index = 0; group_index < segment_ranges.size(); ++group_index) {
        const auto& range = segment_ranges[group_index];
        const auto& task = distributed_state.tasks[group_index];
        auto& request = requests[group_index];
        request.set_tablet_id(_tablet->tablet_id());
        request.set_execution_id(_uuid);
        request.set_attempt_id(task.attempt_id);
        request.set_group_index(task.group_index);
        *request.mutable_input_rowset_meta() = input_meta_pb;
        request.set_segment_pos_start(range.segment_pos_start);
        request.set_segment_pos_end(range.segment_pos_end);
        for (int64_t pos = range.segment_pos_start; pos < range.segment_pos_end; ++pos) {
            request.add_input_segment_ids(
                    input_rowset->segment(cast_set<size_t>(pos)).id());
        }
        *request.mutable_output_rowset_meta() = output_meta_pb;
        request.set_output_rowset_id(output_rowset_id);
        request.set_output_segment_start_id(task.segment_id_slot.start_id);
        request.set_max_segment_num(task.segment_id_slot.capacity);
        request.set_compaction_type(static_cast<int32_t>(compaction_type()));
        request.set_is_vertical(_is_vertical);
        request.set_avg_segment_rows(cast_set<uint32_t>(get_avg_segment_rows()));
        request.set_merge_way_num(range.merge_way_num);
        request.set_is_mow(is_mow);
        request.set_delete_bitmap_start_version(0);
        request.set_delete_bitmap_end_version(phase1_end_version + 1);
        request.set_check_missed_rows(check_missed_rows);
        request.set_cloud_unique_id(config::cloud_unique_id);
    }

    std::vector<std::vector<size_t>> groups_by_worker(workers.size());
    for (size_t group_index = 0; group_index < segment_ranges.size(); ++group_index) {
        groups_by_worker[group_index % workers.size()].push_back(group_index);
    }
    std::vector<Status> task_status(segment_ranges.size(), Status::OK());
    std::unique_ptr<ThreadPool> thread_pool;
    const Status pool_status =
            ThreadPoolBuilder("DistributedSingleRowsetCompaction")
                    .set_min_threads(1)
                    .set_max_threads(cast_set<int>(workers.size()))
                    .set_max_queue_size(cast_set<int>(workers.size()))
                    .build(&thread_pool);
    if (!pool_status.ok()) {
        _distributed_single_rowset_state.reset();
        return Status::OK();
    }
    auto token = thread_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT,
                                        cast_set<int>(workers.size()));
    Status submit_status = Status::OK();
    for (size_t worker_index = 0; worker_index < workers.size(); ++worker_index) {
        submit_status = token->submit_func([&, worker_index]() {
            for (const size_t group_index : groups_by_worker[worker_index]) {
                auto& task = distributed_state.tasks[group_index];
                task.started = true;
                task_status[group_index] = cloud::single_rowset_compaction_rpc(
                        task.worker_endpoint, requests[group_index], &responses[group_index]);
                if (!task_status[group_index].ok()) {
                    break;
                }
            }
        });
        if (!submit_status.ok()) {
            break;
        }
    }
    token->wait();
    token->shutdown();
    RETURN_IF_ERROR(submit_status);
    for (const auto& status : task_status) {
        RETURN_IF_ERROR(status);
    }

    int64_t output_num_rows = 0;
    int64_t output_data_size = 0;
    int64_t output_index_size = 0;
    int64_t output_total_size = 0;
    bool key_bounds_truncated = false;
    bool segment_file_sizes_available = true;
    int64_t missed_rows_count = 0;
    std::unordered_set<int64_t> output_segment_id_set;
    std::vector<int64_t> output_segment_ids;
    std::vector<KeyBoundsPB> output_key_bounds;
    std::vector<uint32_t> output_segment_rows;
    std::vector<size_t> output_segment_file_sizes;
    std::vector<InvertedIndexFileInfo> output_index_file_info;
    _stats = Merger::Statistics {};

    for (size_t group_index = 0; group_index < responses.size(); ++group_index) {
        const auto& response = responses[group_index];
        const auto& task = distributed_state.tasks[group_index];
        if (response.execution_id() != _uuid || response.group_index() != task.group_index ||
            response.attempt_id() != task.attempt_id || !response.has_partial_rowset_meta()) {
            return Status::InvalidArgument(
                    "mismatched distributed single-rowset compaction response for group {}",
                    group_index);
        }

        RowsetMeta partial_meta;
        if (!partial_meta.init_from_pb(response.partial_rowset_meta())) {
            return Status::InvalidArgument(
                    "failed to initialize partial rowset metadata for group {}", group_index);
        }
        if (partial_meta.rowset_id() != _output_rs_writer->rowset_id()) {
            return Status::InvalidArgument("partial rowset id mismatch for group {}", group_index);
        }
        if (partial_meta.num_segments() > task.segment_id_slot.capacity) {
            return Status::Error<ErrorCode::TOO_MANY_SEGMENTS>(
                    "group {} produced {} segments, slot capacity is {}", group_index,
                    partial_meta.num_segments(), task.segment_id_slot.capacity);
        }
        if (partial_meta.num_segments() > 0 &&
            partial_meta.segment_ids().size() !=
                    cast_set<size_t>(partial_meta.num_segments())) {
            return Status::InvalidArgument(
                    "partial rowset does not contain an explicit segment id list for group {}",
                    group_index);
        }

        for (const auto segment : partial_meta.segments()) {
            if (segment.id() < task.segment_id_slot.start_id ||
                segment.id() >= cast_set<int64_t>(task.segment_id_slot.start_id) +
                                        task.segment_id_slot.capacity) {
                return Status::InvalidArgument(
                        "output segment {} is outside slot [{}, {}) for group {}", segment.id(),
                        task.segment_id_slot.start_id,
                        cast_set<int64_t>(task.segment_id_slot.start_id) +
                                task.segment_id_slot.capacity,
                        group_index);
            }
            if (!output_segment_id_set.emplace(segment.id()).second) {
                return Status::InvalidArgument("duplicate output segment id {}", segment.id());
            }
            output_segment_ids.push_back(segment.id());
        }

        std::vector<KeyBoundsPB> partial_key_bounds;
        partial_meta.get_segments_key_bounds(&partial_key_bounds);
        if (partial_meta.num_segments() > 0 &&
            partial_key_bounds.size() != cast_set<size_t>(partial_meta.num_segments())) {
            return Status::InvalidArgument(
                    "partial key bounds are not position-aligned for group {}", group_index);
        }
        output_key_bounds.insert(output_key_bounds.end(), partial_key_bounds.begin(),
                                 partial_key_bounds.end());

        std::vector<uint32_t> partial_segment_rows;
        partial_meta.get_num_segment_rows(&partial_segment_rows);
        if (partial_meta.num_segments() > 0 &&
            partial_segment_rows.size() != cast_set<size_t>(partial_meta.num_segments())) {
            return Status::InvalidArgument(
                    "partial segment row counts are not position-aligned for group {}",
                    group_index);
        }
        output_segment_rows.insert(output_segment_rows.end(), partial_segment_rows.begin(),
                                   partial_segment_rows.end());

        if (partial_meta.num_segments() > 0) {
            if (partial_meta.segments_file_size().size() !=
                cast_set<size_t>(partial_meta.num_segments())) {
                segment_file_sizes_available = false;
            } else if (segment_file_sizes_available) {
                for (const auto file_size : partial_meta.segments_file_size()) {
                    output_segment_file_sizes.push_back(cast_set<size_t>(file_size));
                }
            }
            result->output_segment_group_sizes.push_back(
                    cast_set<int32_t>(partial_meta.num_segments()));
        }
        for (const auto& file_info : partial_meta.inverted_index_file_info()) {
            output_index_file_info.push_back(file_info);
        }
        if ((_cur_tablet_schema->has_inverted_index() || _cur_tablet_schema->has_ann_index()) &&
            partial_meta.num_segments() > 0 &&
            partial_meta.inverted_index_file_info().size() !=
                    cast_set<size_t>(partial_meta.num_segments())) {
            return Status::InvalidArgument(
                    "partial inverted-index metadata is not position-aligned for group {}",
                    group_index);
        }

        output_num_rows += partial_meta.num_rows();
        output_data_size += partial_meta.data_disk_size();
        output_index_size += partial_meta.index_disk_size();
        output_total_size += partial_meta.total_disk_size();
        key_bounds_truncated |= partial_meta.is_segments_key_bounds_truncated();
        missed_rows_count += response.missed_rows_count();
        _stats.output_rows += response.output_rows();
        _stats.merged_rows += response.merged_rows();
        _stats.filtered_rows += response.filtered_rows();
        _stats.bytes_read_from_local += response.bytes_read_from_local();
        _stats.bytes_read_from_remote += response.bytes_read_from_remote();
        _stats.cached_bytes_total += response.cached_bytes_total();
        _stats.cloud_local_read_time += response.cloud_local_read_time();
        _stats.cloud_remote_read_time += response.cloud_remote_read_time();
        if (is_mow && response.has_output_delete_bitmap_shard()) {
            distributed_state.output_delete_bitmap->merge(DeleteBitmap::from_pb(
                    response.output_delete_bitmap_shard(), _tablet->tablet_id()));
        }
    }

    if (!segment_file_sizes_available) {
        output_segment_file_sizes.clear();
    }
    DORIS_CHECK_EQ(output_segment_ids.size(), output_segment_rows.size());
    DORIS_CHECK_EQ(output_segment_ids.size(), output_key_bounds.size());
    DORIS_CHECK(output_index_file_info.empty() ||
                output_index_file_info.size() == output_segment_ids.size());
    if (check_missed_rows && _tablet->tablet_state() == TABLET_RUNNING &&
        _stats.merged_rows + _stats.filtered_rows >= 0 &&
        _stats.merged_rows + _stats.filtered_rows != missed_rows_count) {
        const Status status = Status::InternalError(
                "distributed single-rowset compaction merged rows ({}) plus filtered rows ({}) "
                "does not equal missed rows ({})",
                _stats.merged_rows, _stats.filtered_rows, missed_rows_count);
        if (config::enable_mow_compaction_correctness_check_core) {
            CHECK(false) << status;
        }
        if (config::enable_mow_compaction_correctness_check_fail) {
            return status;
        }
        DCHECK(false) << status;
    }

    auto final_meta = std::make_shared<RowsetMeta>();
    final_meta->set_num_rows(output_num_rows);
    final_meta->set_total_disk_size(output_total_size);
    final_meta->set_data_disk_size(output_data_size);
    final_meta->set_index_disk_size(output_index_size);
    final_meta->set_empty(output_num_rows == 0);
    final_meta->set_num_segments(cast_set<int64_t>(output_segment_ids.size()));
    if (!output_segment_ids.empty()) {
        final_meta->set_segment_ids(output_segment_ids);
    }
    final_meta->set_segments_overlap(NONOVERLAPPING);
    final_meta->set_rowset_state(VISIBLE);
    final_meta->set_segments_key_bounds_truncated(key_bounds_truncated);
    final_meta->set_segments_key_bounds(output_key_bounds, false);
    final_meta->set_num_segment_rows(output_segment_rows);

    _output_rowset = _output_rs_writer->manual_build(final_meta);
    if (_output_rowset == nullptr) {
        return Status::InternalError(
                "failed to build distributed single-rowset compaction output");
    }
    if (!output_segment_file_sizes.empty()) {
        _output_rowset->rowset_meta()->add_segments_file_size(output_segment_file_sizes);
    }
    if (!output_index_file_info.empty()) {
        std::vector<const InvertedIndexFileInfo*> output_index_file_info_ptrs;
        output_index_file_info_ptrs.reserve(output_index_file_info.size());
        for (const auto& file_info : output_index_file_info) {
            output_index_file_info_ptrs.push_back(&file_info);
        }
        _output_rowset->rowset_meta()->add_inverted_index_files_info(
                output_index_file_info_ptrs);
    }

    result->output_rowset_built = true;
    *compacted = true;
    LOG_INFO("finish distributed single-rowset compaction merge, tablet_id={}",
             _tablet->tablet_id())
            .tag("job_id", _uuid)
            .tag("groups", segment_ranges.size())
            .tag("workers", workers.size())
            .tag("output_segments", output_segment_ids.size());
    return Status::OK();
}

Status CloudCumulativeCompaction::finish_distributed_mow_delete_bitmap(
        int64_t initiator, DeleteBitmapPtr* output_delete_bitmap, int64_t* lock_start_time) {
    DORIS_CHECK(_distributed_single_rowset_state != nullptr);
    auto& distributed_state = *_distributed_single_rowset_state;
    DORIS_CHECK(distributed_state.output_delete_bitmap != nullptr);

    RETURN_IF_ERROR(_engine.meta_mgr().get_delete_bitmap_update_lock(
            *cloud_tablet(), COMPACTION_DELETE_BITMAP_LOCK_ID, initiator));
    *lock_start_time = MonotonicMicros();

    std::vector<std::string> worker_endpoints;
    std::unordered_map<std::string, size_t> worker_to_index;
    std::vector<std::vector<size_t>> groups_by_worker;
    for (size_t task_index = 0; task_index < distributed_state.tasks.size(); ++task_index) {
        const auto& endpoint = distributed_state.tasks[task_index].worker_endpoint;
        const auto [iter, inserted] =
                worker_to_index.emplace(endpoint, worker_endpoints.size());
        if (inserted) {
            worker_endpoints.push_back(endpoint);
            groups_by_worker.emplace_back();
        }
        groups_by_worker[iter->second].push_back(task_index);
    }

    std::vector<Status> task_status(distributed_state.tasks.size(), Status::OK());
    std::vector<PCloudSingleRowsetCompactionIncrementalResponse> responses(
            distributed_state.tasks.size());
    std::unique_ptr<ThreadPool> thread_pool;
    RETURN_IF_ERROR(ThreadPoolBuilder("DistributedSingleRowsetDeleteBitmap")
                            .set_min_threads(1)
                            .set_max_threads(cast_set<int>(worker_endpoints.size()))
                            .set_max_queue_size(cast_set<int>(worker_endpoints.size()))
                            .build(&thread_pool));
    auto token = thread_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT,
                                        cast_set<int>(worker_endpoints.size()));
    Status submit_status = Status::OK();
    for (size_t worker_index = 0; worker_index < worker_endpoints.size(); ++worker_index) {
        submit_status = token->submit_func([&, worker_index]() {
            for (const size_t task_index : groups_by_worker[worker_index]) {
                const auto& task = distributed_state.tasks[task_index];
                PCloudSingleRowsetCompactionIncrementalRequest request;
                request.set_tablet_id(_tablet->tablet_id());
                request.set_execution_id(distributed_state.execution_id);
                request.set_attempt_id(task.attempt_id);
                request.set_group_index(task.group_index);
                request.set_delete_bitmap_start_version(distributed_state.phase1_end_version);
                request.set_delete_bitmap_end_version(
                        std::numeric_limits<uint64_t>::max());
                task_status[task_index] = cloud::single_rowset_compaction_incremental_rpc(
                        task.worker_endpoint, request, &responses[task_index]);
                if (!task_status[task_index].ok()) {
                    break;
                }
            }
        });
        if (!submit_status.ok()) {
            break;
        }
    }
    token->wait();
    token->shutdown();
    RETURN_IF_ERROR(submit_status);
    for (const auto& status : task_status) {
        RETURN_IF_ERROR(status);
    }
    for (const auto& response : responses) {
        if (response.has_output_delete_bitmap_shard()) {
            distributed_state.output_delete_bitmap->merge(
                    DeleteBitmap::from_pb(response.output_delete_bitmap_shard(),
                                          _tablet->tablet_id()));
        }
    }

    DeleteBitmapPtr delete_bitmap_v2;
    const int64_t store_version = config::delete_bitmap_store_write_version;
    if (store_version == 2 || store_version == 3) {
        delete_bitmap_v2 =
                std::make_shared<DeleteBitmap>(*distributed_state.output_delete_bitmap);
        std::vector<DeleteBitmap::RowsetIdWithSegmentIds> retained_rowsets;
        {
            std::shared_lock read_lock(_tablet->get_header_lock());
            for (const auto& [rowset_version, rowset] : cloud_tablet()->rowset_map()) {
                if (rowset_version.second >= _output_rowset->start_version()) {
                    continue;
                }
                std::vector<DeleteBitmap::SegmentId> segment_ids;
                segment_ids.reserve(cast_set<size_t>(rowset->num_segments()));
                for (const auto segment : rowset->segments()) {
                    segment_ids.push_back(
                            cast_set<DeleteBitmap::SegmentId>(segment.id()));
                }
                retained_rowsets.emplace_back(rowset->rowset_id(), std::move(segment_ids));
            }
        }
        if (config::enable_agg_delta_delete_bitmap_for_store_v2) {
            cloud_tablet()->tablet_meta()->delete_bitmap().subset_and_agg(
                    retained_rowsets, _output_rowset->start_version(),
                    _output_rowset->end_version(), delete_bitmap_v2.get());
        } else {
            cloud_tablet()->tablet_meta()->delete_bitmap().subset(
                    retained_rowsets, _output_rowset->start_version(),
                    _output_rowset->end_version(), delete_bitmap_v2.get());
        }
    }

    std::optional<StorageResource> storage_resource;
    auto output_storage_resource = _output_rowset->rowset_meta()->remote_storage_resource();
    if (output_storage_resource) {
        storage_resource = *output_storage_resource.value();
    }
    RETURN_IF_ERROR(_engine.meta_mgr().update_delete_bitmap(
            *cloud_tablet(), -1, initiator, distributed_state.output_delete_bitmap.get(),
            delete_bitmap_v2.get(), _output_rowset->rowset_id().to_string(), storage_resource,
            store_version, _tablet->table_id()));
    *output_delete_bitmap = distributed_state.output_delete_bitmap;
    return Status::OK();
}

void CloudCumulativeCompaction::finish_distributed_workers(bool keep_output_files) {
    if (_distributed_single_rowset_state == nullptr) {
        return;
    }
    auto& distributed_state = *_distributed_single_rowset_state;
    std::unordered_map<std::string, std::vector<size_t>> groups_by_worker;
    for (size_t task_index = 0; task_index < distributed_state.tasks.size(); ++task_index) {
        if (distributed_state.tasks[task_index].started) {
            groups_by_worker[distributed_state.tasks[task_index].worker_endpoint].push_back(
                    task_index);
        }
    }

    if (!groups_by_worker.empty()) {
        const auto finish_endpoint = [&](const std::string& endpoint,
                                         const std::vector<size_t>& task_indices) {
            for (const size_t task_index : task_indices) {
                const auto& task = distributed_state.tasks[task_index];
                PCloudSingleRowsetCompactionFinishRequest request;
                request.set_execution_id(distributed_state.execution_id);
                request.set_attempt_id(task.attempt_id);
                request.set_group_index(task.group_index);
                request.set_keep_output_files(keep_output_files);
                PCloudSingleRowsetCompactionFinishResponse response;
                const Status status =
                        cloud::single_rowset_compaction_finish_rpc(endpoint, request, &response);
                if (!status.ok()) {
                    LOG_WARNING("failed to finish distributed single-rowset worker")
                            .tag("job_id", _uuid)
                            .tag("endpoint", endpoint)
                            .tag("group_index", task.group_index)
                            .tag("keep_output_files", keep_output_files)
                            .error(status);
                }
            }
        };
        std::unique_ptr<ThreadPool> thread_pool;
        Status pool_status =
                ThreadPoolBuilder("DistributedSingleRowsetFinish")
                        .set_min_threads(1)
                        .set_max_threads(cast_set<int>(groups_by_worker.size()))
                        .set_max_queue_size(cast_set<int>(groups_by_worker.size()))
                        .build(&thread_pool);
        if (!pool_status.ok()) {
            LOG_WARNING("failed to create distributed single-rowset finish pool")
                    .tag("job_id", _uuid)
                    .error(pool_status);
            for (const auto& [endpoint, task_indices] : groups_by_worker) {
                finish_endpoint(endpoint, task_indices);
            }
        } else {
            auto token = thread_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT,
                                                cast_set<int>(groups_by_worker.size()));
            for (const auto& [endpoint, task_indices] : groups_by_worker) {
                const Status submit_status = token->submit_func([&, endpoint, task_indices]() {
                    finish_endpoint(endpoint, task_indices);
                });
                if (!submit_status.ok()) {
                    LOG_WARNING("failed to submit distributed single-rowset finish task")
                            .tag("job_id", _uuid)
                            .tag("endpoint", endpoint)
                            .error(submit_status);
                    finish_endpoint(endpoint, task_indices);
                }
            }
            token->wait();
            token->shutdown();
        }
    }
    _distributed_single_rowset_state.reset();
}

Status CloudCumulativeCompaction::do_merge_input_rowsets(
        const std::vector<RowsetReaderSharedPtr>& input_rs_readers,
        MergeInputRowsetsResult* result) {
    if (!result->is_segment_grouped) {
        return Compaction::do_merge_input_rowsets(input_rs_readers, result);
    }

    bool compacted_distributed = false;
    RETURN_IF_ERROR(try_distributed_single_rowset_compaction(result, &compacted_distributed));
    if (compacted_distributed) {
        return Status::OK();
    }

    const int64_t segment_group_size = result->segment_group_size;
    const auto& input_rowset = _input_rowsets.front();
    const auto segment_ranges = cloud::build_segment_group_merge_ranges(
            *input_rowset->rowset_meta(), segment_group_size);
    int32_t output_segment_count = 0;
    for (size_t range_index = 0; range_index < segment_ranges.size(); ++range_index) {
        const auto& range = segment_ranges[range_index];

        RowsetReaderSharedPtr rs_reader;
        RETURN_IF_ERROR(input_rowset->create_reader(&rs_reader));
        std::vector<RowsetReaderSharedPtr> group_readers;
        group_readers.push_back(std::move(rs_reader));

        Merger::Statistics group_stats;
        group_stats.rowid_conversion = _stats.rowid_conversion;
        RETURN_IF_ERROR(execute_merge(group_readers, range.merge_way_num, &group_stats,
                                      std::make_pair(range.segment_pos_start, range.segment_pos_end),
                                      {.total_ranges = cast_set<int64_t>(segment_ranges.size()),
                                       .range_index = cast_set<int64_t>(range_index)}));

        _stats.output_rows += group_stats.output_rows;
        _stats.merged_rows += group_stats.merged_rows;
        _stats.filtered_rows += group_stats.filtered_rows;
        _stats.bytes_read_from_local += group_stats.bytes_read_from_local;
        _stats.bytes_read_from_remote += group_stats.bytes_read_from_remote;
        _stats.cached_bytes_total += group_stats.cached_bytes_total;
        _stats.cloud_local_read_time += group_stats.cloud_local_read_time;
        _stats.cloud_remote_read_time += group_stats.cloud_remote_read_time;

        std::vector<uint32_t> output_segment_num_rows;
        RETURN_IF_ERROR(_output_rs_writer->get_segment_num_rows(&output_segment_num_rows));
        DORIS_CHECK_GE(output_segment_num_rows.size(),
                       cast_set<size_t>(output_segment_count));
        const int32_t new_output_segment_count =
                cast_set<int32_t>(output_segment_num_rows.size());
        const int32_t output_group_size = new_output_segment_count - output_segment_count;
        if (output_group_size > 0) {
            result->output_segment_group_sizes.push_back(output_group_size);
        }
        output_segment_count = new_output_segment_count;
    }
    return Status::OK();
}

void CloudCumulativeCompaction::update_output_rowset_after_build(
        const MergeInputRowsetsResult& result) {
    if (!result.is_segment_grouped) {
        return;
    }
    if (result.output_segment_group_sizes.size() > 1) {
        _output_rowset->rowset_meta()->set_segments_overlap(NONOVERLAPPING_WITHIN_GROUP);
        _output_rowset->rowset_meta()->set_segment_group_sizes(result.output_segment_group_sizes);
    }

    const auto& input_rowset = _input_rowsets.front();
    LOG_INFO("finish single rowset grouped compaction, tablet_id={}, version=[{}-{}]",
             _tablet->tablet_id(), input_rowset->start_version(), input_rowset->end_version())
            .tag("job_id", _uuid)
            .tag("input_segments", input_rowset->num_segments())
            .tag("segment_group_size", result.segment_group_size)
            .tag("output_segments", _output_rowset->num_segments())
            .tag("output_groups", result.output_segment_group_sizes.size())
            .tag("output_segment_group_sizes",
                 fmt::format("[{}]", fmt::join(result.output_segment_group_sizes, ", ")));
}

void CloudCumulativeCompaction::update_cumulative_point() {
    cloud::TabletJobInfoPB job;
    auto idx = job.mutable_idx();
    idx->set_tablet_id(_tablet->tablet_id());
    idx->set_table_id(_tablet->table_id());
    idx->set_index_id(_tablet->index_id());
    idx->set_partition_id(_tablet->partition_id());
    auto compaction_job = job.add_compaction();
    compaction_job->set_id(_uuid);
    compaction_job->set_initiator(BackendOptions::get_localhost() + ':' +
                                  std::to_string(config::heartbeat_service_port));
    compaction_job->set_type(cloud::TabletCompactionJobPB::EMPTY_CUMULATIVE);
    compaction_job->set_base_compaction_cnt(_base_compaction_cnt);
    compaction_job->set_cumulative_compaction_cnt(_cumulative_compaction_cnt);
    int64_t now = time(nullptr);
    compaction_job->set_lease(now + config::lease_compaction_interval_seconds);
    // No need to set expiration time, since there is no output rowset
    cloud::StartTabletJobResponse start_resp;
    auto st = _engine.meta_mgr().prepare_tablet_job(job, &start_resp);
    if (!st.ok()) {
        if (start_resp.status().code() == cloud::STALE_TABLET_CACHE) {
            // set last_sync_time to 0 to force sync tablet next time
            cloud_tablet()->last_sync_time_s = 0;
        } else if (start_resp.status().code() == cloud::TABLET_NOT_FOUND) {
            // tablet not found
            cloud_tablet()->clear_cache();
        }
        LOG_WARNING("failed to update cumulative point to meta srv")
                .tag("job_id", _uuid)
                .tag("tablet_id", _tablet->tablet_id())
                .error(st);
        return;
    }
    int64_t input_cumulative_point = cloud_tablet()->cumulative_layer_point();
    int64_t output_cumulative_point = _last_delete_version.first + 1;
    compaction_job->set_input_cumulative_point(input_cumulative_point);
    compaction_job->set_output_cumulative_point(output_cumulative_point);
    cloud::FinishTabletJobResponse finish_resp;
    st = _engine.meta_mgr().commit_tablet_job(job, &finish_resp);
    if (!st.ok()) {
        if (finish_resp.status().code() == cloud::TABLET_NOT_FOUND) {
            cloud_tablet()->clear_cache();
        }
        LOG_WARNING("failed to update cumulative point to meta srv")
                .tag("job_id", _uuid)
                .tag("tablet_id", _tablet->tablet_id())
                .error(st);
        return;
    }
    LOG_INFO("do empty cumulative compaction to update cumulative point")
            .tag("job_id", _uuid)
            .tag("tablet_id", _tablet->tablet_id())
            .tag("input_cumulative_point", input_cumulative_point)
            .tag("output_cumulative_point", output_cumulative_point);
    auto& stats = finish_resp.stats();
    LOG(INFO) << "tablet stats=" << stats.ShortDebugString();
    {
        std::lock_guard wrlock(_tablet->get_header_lock());
        // clang-format off
        cloud_tablet()->set_last_base_compaction_success_time(std::max(cloud_tablet()->last_base_compaction_success_time(), stats.last_base_compaction_time_ms()));
        cloud_tablet()->set_last_cumu_compaction_success_time(std::max(cloud_tablet()->last_cumu_compaction_success_time(), stats.last_cumu_compaction_time_ms()));
        // clang-format on
        if (cloud_tablet()->cumulative_compaction_cnt() >= stats.cumulative_compaction_cnt()) {
            // This could happen while calling `sync_tablet_rowsets` during `commit_tablet_job`
            return;
        }
        // ATTN: MUST NOT update `base_compaction_cnt` which are used when sync rowsets, otherwise may cause
        // the tablet to be unable to synchronize the rowset meta changes generated by base compaction.
        cloud_tablet()->set_cumulative_compaction_cnt(cloud_tablet()->cumulative_compaction_cnt() +
                                                      1);
        cloud_tablet()->set_cumulative_layer_point(stats.cumulative_point());
        if (stats.base_compaction_cnt() >= cloud_tablet()->base_compaction_cnt()) {
            cloud_tablet()->reset_approximate_stats(stats.num_rowsets(), stats.num_segments(),
                                                    stats.num_rows(), stats.data_size());
        }
    }
}

void CloudCumulativeCompaction::do_lease() {
    TEST_INJECTION_POINT_RETURN_WITH_VOID("CloudCumulativeCompaction::do_lease");
    if (_state == CompactionState::SUCCESS) {
        return;
    }
    cloud::TabletJobInfoPB job;
    auto idx = job.mutable_idx();
    idx->set_tablet_id(_tablet->tablet_id());
    idx->set_table_id(_tablet->table_id());
    idx->set_index_id(_tablet->index_id());
    idx->set_partition_id(_tablet->partition_id());
    auto compaction_job = job.add_compaction();
    compaction_job->set_id(_uuid);
    using namespace std::chrono;
    int64_t lease_time = duration_cast<seconds>(system_clock::now().time_since_epoch()).count() +
                         config::lease_compaction_interval_seconds * 4;
    compaction_job->set_lease(lease_time);
    auto st = _engine.meta_mgr().lease_tablet_job(job);
    if (!st.ok()) {
        LOG_WARNING("failed to lease compaction job")
                .tag("job_id", _uuid)
                .tag("tablet_id", _tablet->tablet_id())
                .error(st);
    }
}

} // namespace doris
