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

#include "load/delta_writer/shard_partitioner.h"

#include "core/block/block.h"
#include "runtime/descriptors.h"

namespace doris {

Status HashPartitioner::init(const std::vector<SlotDescriptor*>* slots,
                             const TupleDescriptor* tuple_desc,
                             const TabletSchemaSPtr& tablet_schema) {
    if (slots == nullptr || tuple_desc == nullptr || tablet_schema == nullptr) {
        return Status::InternalError("HashPartitioner::init got null slots/tuple_desc/schema");
    }
    // Map the (key) columns to their positions in the load block. The block is laid
    // out following `tuple_desc->slots()`; `slots` are in tablet-schema order. This is
    // the same mapping MemTable builds in _init_columns_offset_by_slot_descs().
    const auto& tuple_slots = tuple_desc->slots();
    auto block_pos_of_slot = [&](const SlotDescriptor* slot) -> int {
        for (int j = 0; j < static_cast<int>(tuple_slots.size()); ++j) {
            if (tuple_slots[j]->id() == slot->id()) {
                return j;
            }
        }
        return -1;
    };

    // Hash on key columns when present (required for MoW determinism), otherwise on
    // all loaded columns (DUP table without explicit key columns -> pure balancing).
    size_t num_hash_cols = tablet_schema->num_key_columns();
    if (num_hash_cols == 0 || num_hash_cols > slots->size()) {
        num_hash_cols = slots->size();
    }
    _hash_block_pos.clear();
    _hash_block_pos.reserve(num_hash_cols);
    for (size_t i = 0; i < num_hash_cols; ++i) {
        int pos = block_pos_of_slot((*slots)[i]);
        if (pos < 0) {
            return Status::InternalError(
                    "HashPartitioner::init cannot locate slot {} (col {}) in tuple desc",
                    (*slots)[i]->id(), i);
        }
        _hash_block_pos.push_back(static_cast<uint32_t>(pos));
    }
    if (_hash_block_pos.empty()) {
        return Status::InternalError("HashPartitioner::init found no columns to hash on");
    }
    return Status::OK();
}

void HashPartitioner::partition(const Block* block, const DorisVector<uint32_t>& rows,
                                std::vector<uint32_t>& shard_ids) {
    shard_ids.resize(rows.size());
    if (rows.empty()) {
        return;
    }
    // update_hashes_with_value() chains via the existing seed value, so applying it
    // column by column yields one combined xxHash per row across all hashed columns.
    // assign() reuses _hashes' capacity (and re-seeds every row to 0) across calls.
    size_t num_block_rows = block->rows();
    _hashes.assign(num_block_rows, 0);
    for (uint32_t pos : _hash_block_pos) {
        block->get_by_position(pos).column->update_hashes_with_value(_hashes.data(), nullptr);
    }
    for (size_t i = 0; i < rows.size(); ++i) {
        shard_ids[i] = static_cast<uint32_t>(_hashes[rows[i]] % _num_shards);
    }
}

} // namespace doris
