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

#include <cstdint>
#include <vector>

#include "common/status.h"
#include "core/custom_allocator.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

class Block;
class TupleDescriptor;
class SlotDescriptor;

// Splits the rows of an incoming load block among K sub-writers within a single
// DeltaWriter (Phase 1 of the load-parallelism design). The partition function
// must be deterministic on the key columns so that for MoW tables the same key
// always lands in the same shard (keeping dedup / sequence "latest-wins" local
// to one sub-writer and making cross-shard segments key-disjoint).
//
// NOT thread-safe: a partitioner instance is used from the single write() path
// of one DeltaWriter, which is already externally synchronized.
class ShardPartitioner {
public:
    explicit ShardPartitioner(uint32_t num_shards) : _num_shards(num_shards) {}
    virtual ~ShardPartitioner() = default;

    virtual Status init(const std::vector<SlotDescriptor*>* slots,
                        const TupleDescriptor* tuple_desc,
                        const TabletSchemaSPtr& tablet_schema) = 0;

    // Computes the shard id in [0, num_shards) for every row referenced by `rows`.
    // `shard_ids` is resized to `rows.size()`; shard_ids[i] is the shard of rows[i].
    virtual void partition(const Block* block, const DorisVector<uint32_t>& rows,
                           std::vector<uint32_t>& shard_ids) = 0;

    uint32_t num_shards() const { return _num_shards; }

protected:
    uint32_t _num_shards;
};

// Default partitioner: hash(key columns) % K. Deterministic on the key, balanced
// for free (no sampling / split-point freezing needed). Works for both DUP and
// MoW; for DUP without key columns it falls back to hashing all loaded columns.
class HashPartitioner final : public ShardPartitioner {
public:
    explicit HashPartitioner(uint32_t num_shards) : ShardPartitioner(num_shards) {}

    Status init(const std::vector<SlotDescriptor*>* slots, const TupleDescriptor* tuple_desc,
                const TabletSchemaSPtr& tablet_schema) override;

    void partition(const Block* block, const DorisVector<uint32_t>& rows,
                   std::vector<uint32_t>& shard_ids) override;

private:
    // Block column positions of the columns we hash on (key columns, in schema order).
    std::vector<uint32_t> _hash_block_pos;
};

} // namespace doris
