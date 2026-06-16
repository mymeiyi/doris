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

#include <cstddef>
#include <cstdint>
#include <map>
#include <vector>

#include "common/status.h"
#include "storage/olap_common.h" // RowsetId
#include "storage/tablet/tablet_fwd.h"

namespace doris {

class TTabletSplit;
struct TabletReadSource;

// Serialization helpers that move a single-tablet scan layout (a captured
// `TabletReadSource`) across BE nodes as a `TTabletSplit` Thrift entity, and
// rebuild it on the execution node. See doc/cross_be_parallel_scan.md.
//
// The core correctness rule: the layout (set of RowsetMeta entities) that a split
// was computed against is shipped *as an entity*, so the execution node rebuilds
// exactly that layout via `RowsetFactory::create_rowset` instead of re-capturing
// the tablet (which could pick up a post-compaction layout and invalidate the
// split coordinates).

// Splits one tablet's entire read source into several partial read sources by
// row id. This is a faithful port of
// `ParallelScannerBuilder::_build_scanners_by_rowid` so that the cross-BE producer
// path yields identical splits to the single-BE parallel scan path.
//
// `all_segments_rows` maps rowset_id -> per-segment row counts and must be
// computed identically to `ParallelScannerBuilder::_load`. Each produced partial
// read source shares `delete_predicates` / `delete_bitmap` with `entire_read_source`.
Status compute_rowid_splits(const TabletReadSource& entire_read_source,
                            const std::map<RowsetId, std::vector<size_t>>& all_segments_rows,
                            size_t rows_per_scanner,
                            std::vector<TabletReadSource>* out_partials);

// Serializes one partial read source (i.e. one cross-BE scanner's worth of work)
// into a `TTabletSplit`. The full RowsetMetaPB of each data rowset and each
// delete-predicate rowset is embedded; the MoW delete bitmap is embedded when
// `is_mow` is true.
Status read_source_to_tablet_split(int64_t pinned_version, int64_t split_id,
                                   const TabletReadSource& partial, bool is_mow,
                                   TTabletSplit* out);

// Rebuilds a `TabletReadSource` from a `TTabletSplit` on the execution node.
// `schema` is the (light) tablet schema used as a fallback by create_rowset;
// `tablet_id` is used to attach the delete bitmap.
Status tablet_split_to_read_source(const TTabletSplit& split, const TabletSchemaSPtr& schema,
                                   int64_t tablet_id, TabletReadSource* out);

} // namespace doris
