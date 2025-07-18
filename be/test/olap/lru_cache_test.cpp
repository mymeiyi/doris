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

#include "olap/lru_cache.h"

#include <gen_cpp/Metrics_types.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <iosfwd>
#include <vector>

#include "gtest/gtest.h"
#include "gtest/gtest_pred_impl.h"
#include "runtime/memory/lru_cache_policy.h"
#include "runtime/memory/lru_cache_value_base.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "testutil/test_util.h"

using namespace doris;
using namespace std;

namespace doris {

void PutFixed32(std::string* dst, uint32_t value) {
    char buf[sizeof(value)];
    memcpy(buf, &value, sizeof(value));
    dst->append(buf, sizeof(buf));
}

uint32_t DecodeFixed32(const char* ptr) {
    // Load the raw bytes
    uint32_t result;
    memcpy(&result, ptr, sizeof(result)); // gcc optimizes this to a plain load
    return result;
}

// Conversions between numeric keys/values and the types expected by Cache.
CacheKey EncodeKey(std::string* result, int k) {
    PutFixed32(result, k);
    return {result->c_str(), result->size()};
}

static int DecodeKey(const CacheKey& k) {
    EXPECT_EQ(k.size(), 4);
    return DecodeFixed32(k.data());
}
static void* EncodeValue(uintptr_t v) {
    return reinterpret_cast<void*>(v);
}
static int DecodeValue(void* v) {
    return reinterpret_cast<uintptr_t>(v);
}

class CacheTest : public testing::Test {
public:
    static CacheTest* _s_current;

    class CacheValueWithKey : public LRUCacheValueBase {
    public:
        CacheValueWithKey(int key, void* value) : key(key), value(value) {}
        ~CacheValueWithKey() override {
            _s_current->_deleted_keys.push_back(key);
            _s_current->_deleted_values.push_back(DecodeValue(value));
        }

        int key;
        void* value;
    };

    class CacheValue : public LRUCacheValueBase {
    public:
        CacheValue(void* value) : value(value) {}

        void* value;
    };

    class CacheTestSizePolicy : public LRUCachePolicy {
    public:
        CacheTestSizePolicy(size_t capacity)
                : LRUCachePolicy(CachePolicy::CacheType::FOR_UT_CACHE_SIZE, capacity,
                                 LRUCacheType::SIZE, -1) {}
    };

    class CacheTestNumberPolicy : public LRUCachePolicy {
    public:
        CacheTestNumberPolicy(size_t capacity, uint32_t num_shards)
                : LRUCachePolicy(CachePolicy::CacheType::FOR_UT_CACHE_NUMBER, capacity,
                                 LRUCacheType::NUMBER, -1, num_shards) {}
    };

    // there is 16 shards in ShardedLRUCache
    // And the LRUHandle size is about 100B. So the cache size should big enough
    // to run the UT.
    // kCacheSize needs to be an even number. if odd number, the cache will behave correctly,
    // but the UT Test will fail because check(capacity / 2) will fail.
    // In fact, Cache will waste an entry space.
    static const int kCacheSize = 1000 * 16;
    std::vector<int> _deleted_keys;
    std::vector<int> _deleted_values;
    LRUCachePolicy* _cache = nullptr;

    CacheTest() { _s_current = this; }

    ~CacheTest() override { delete _cache; }

    void init_size_cache(size_t capacity = kCacheSize) {
        if (_cache != nullptr) {
            delete _cache;
        }
        _cache = new CacheTestSizePolicy(capacity);
    }

    void init_number_cache(size_t capacity = kCacheSize, uint32_t num_shards = 1) {
        if (_cache != nullptr) {
            delete _cache;
        }
        _cache = new CacheTestNumberPolicy(capacity, num_shards);
    }

    LRUCachePolicy* cache() const { return _cache; }

    int Lookup(int key) const {
        std::string result;
        Cache::Handle* handle = cache()->lookup(EncodeKey(&result, key));
        const int r = (handle == nullptr)
                              ? -1
                              : DecodeValue(((CacheValueWithKey*)cache()->value(handle))->value);

        if (handle != nullptr) {
            cache()->release(handle);
        }

        return r;
    }

    void Insert(int key, int value, int charge) const {
        std::string result;
        CacheKey cache_key = EncodeKey(&result, key);
        auto* cache_value = new CacheValueWithKey(DecodeKey(cache_key), EncodeValue(value));
        cache()->release(cache()->insert(cache_key, cache_value, charge, charge));
    }

    void InsertDurable(int key, int value, int charge) const {
        std::string result;
        CacheKey cache_key = EncodeKey(&result, key);
        auto* cache_value = new CacheValueWithKey(DecodeKey(cache_key), EncodeValue(value));
        cache()->release(
                cache()->insert(cache_key, cache_value, charge, charge, CachePriority::DURABLE));
    }

    void Erase(int key) const {
        std::string result;
        cache()->erase(EncodeKey(&result, key));
    }

    void SetUp() override {}

    void TearDown() override {}
};
CacheTest* CacheTest::_s_current;

static void insert_LRUCache(LRUCache& cache, const CacheKey& key, int value,
                            CachePriority priority) {
    uint32_t hash = key.hash(key.data(), key.size(), 0);
    auto* cache_value = new CacheTest::CacheValue(EncodeValue(value));
    cache.release(cache.insert(key, hash, cache_value, value, priority));
}

static void insert_number_LRUCache(LRUCache& cache, const CacheKey& key, int value, int charge,
                                   CachePriority priority) {
    uint32_t hash = key.hash(key.data(), key.size(), 0);
    auto* cache_value = new CacheTest::CacheValue(EncodeValue(value));
    cache.release(cache.insert(key, hash, cache_value, charge, priority));
}

// https://stackoverflow.com/questions/42756443/undefined-reference-with-gtest
const int CacheTest::kCacheSize;

TEST_F(CacheTest, HitAndMiss) {
    init_size_cache();
    EXPECT_EQ(-1, Lookup(100));

    Insert(100, 101, 1);
    EXPECT_EQ(101, Lookup(100));
    EXPECT_EQ(-1, Lookup(200));
    EXPECT_EQ(-1, Lookup(300));

    Insert(200, 201, 1);
    EXPECT_EQ(101, Lookup(100));
    EXPECT_EQ(201, Lookup(200));
    EXPECT_EQ(-1, Lookup(300));

    Insert(100, 102, 1);
    EXPECT_EQ(102, Lookup(100));
    EXPECT_EQ(201, Lookup(200));
    EXPECT_EQ(-1, Lookup(300));

    EXPECT_EQ(1, _deleted_keys.size());
    EXPECT_EQ(100, _deleted_keys[0]);
    EXPECT_EQ(101, _deleted_values[0]);
}

TEST_F(CacheTest, Erase) {
    init_size_cache();
    Erase(200);
    EXPECT_EQ(0, _deleted_keys.size());

    Insert(100, 101, 1);
    Insert(200, 201, 1);
    Erase(100);
    EXPECT_EQ(-1, Lookup(100));
    EXPECT_EQ(201, Lookup(200));
    EXPECT_EQ(1, _deleted_keys.size());
    EXPECT_EQ(100, _deleted_keys[0]);
    EXPECT_EQ(101, _deleted_values[0]);

    Erase(100);
    EXPECT_EQ(-1, Lookup(100));
    EXPECT_EQ(201, Lookup(200));
    EXPECT_EQ(1, _deleted_keys.size());
}

TEST_F(CacheTest, EntriesArePinned) {
    init_size_cache();
    Insert(100, 101, 1);
    std::string result1;
    Cache::Handle* h1 = cache()->lookup(EncodeKey(&result1, 100));
    EXPECT_EQ(101, DecodeValue(((CacheValueWithKey*)cache()->value(h1))->value));

    Insert(100, 102, 1);
    std::string result2;
    Cache::Handle* h2 = cache()->lookup(EncodeKey(&result2, 100));
    EXPECT_EQ(102, DecodeValue(((CacheValueWithKey*)cache()->value(h2))->value));
    EXPECT_EQ(0, _deleted_keys.size());

    cache()->release(h1);
    EXPECT_EQ(1, _deleted_keys.size());
    EXPECT_EQ(100, _deleted_keys[0]);
    EXPECT_EQ(101, _deleted_values[0]);

    Erase(100);
    EXPECT_EQ(-1, Lookup(100));
    EXPECT_EQ(1, _deleted_keys.size());

    cache()->release(h2);
    EXPECT_EQ(2, _deleted_keys.size());
    EXPECT_EQ(100, _deleted_keys[1]);
    EXPECT_EQ(102, _deleted_values[1]);
}

TEST_F(CacheTest, EvictionPolicy) {
    init_size_cache();
    Insert(100, 101, 1);
    Insert(200, 201, 1);

    // Frequently used entry must be kept around
    for (int i = 0; i < kCacheSize + 100; i++) {
        Insert(1000 + i, 2000 + i, 1);
        EXPECT_EQ(2000 + i, Lookup(1000 + i));
        EXPECT_EQ(101, Lookup(100));
    }

    EXPECT_EQ(101, Lookup(100));
    EXPECT_EQ(-1, Lookup(200));
}

TEST_F(CacheTest, EvictionPolicyWithDurable) {
    init_size_cache();
    Insert(100, 101, 1);
    InsertDurable(200, 201, 1);
    Insert(300, 101, 1);

    // Frequently used entry must be kept around
    for (int i = 0; i < kCacheSize + 100; i++) {
        Insert(1000 + i, 2000 + i, 1);
        EXPECT_EQ(2000 + i, Lookup(1000 + i));
        EXPECT_EQ(101, Lookup(100));
    }

    EXPECT_EQ(-1, Lookup(300));
    EXPECT_EQ(101, Lookup(100));
    EXPECT_EQ(201, Lookup(200));
}

TEST_F(CacheTest, Usage) {
    LRUCache cache(LRUCacheType::SIZE);
    cache.set_capacity(1040);

    // The lru usage is handle_size + charge.
    // handle_size = sizeof(handle) - 1 + key size = 96 - 1 + 3 = 98
    CacheKey key1("100");
    insert_LRUCache(cache, key1, 100, CachePriority::NORMAL);
    ASSERT_EQ(198, cache.get_usage()); // 100 + 98

    CacheKey key2("200");
    insert_LRUCache(cache, key2, 200, CachePriority::DURABLE);
    ASSERT_EQ(496, cache.get_usage()); // 198 + 298(d), d = DURABLE

    CacheKey key3("300");
    insert_LRUCache(cache, key3, 300, CachePriority::NORMAL);
    ASSERT_EQ(894, cache.get_usage()); // 198 + 298(d) + 398

    CacheKey key4("400");
    insert_LRUCache(cache, key4, 400, CachePriority::NORMAL);
    ASSERT_EQ(796, cache.get_usage()); // 298(d) + 498, evict 198 398

    CacheKey key5("500");
    insert_LRUCache(cache, key5, 500, CachePriority::NORMAL);
    ASSERT_EQ(896, cache.get_usage()); // 298(d) + 598, evict 498

    CacheKey key6("600");
    insert_LRUCache(cache, key6, 600, CachePriority::NORMAL);
    ASSERT_EQ(996, cache.get_usage()); // 298(d) + 698, evict 598

    CacheKey key7("950");
    insert_LRUCache(cache, key7, 950, CachePriority::DURABLE);
    ASSERT_EQ(
            0,
            cache.get_usage()); // evict 298 698, because 950 + 98 > 1040, data was freed when handle release.

    CacheKey key8("900");
    insert_LRUCache(cache, key8, 900, CachePriority::NORMAL);
    ASSERT_EQ(998, cache.get_usage()); // 900 + 98 < 1050
}

TEST_F(CacheTest, UsageLRUK) {
    LRUCache cache(LRUCacheType::SIZE, true);
    cache.set_capacity(1050);

    // The lru usage is handle_size + charge.
    // handle_size = sizeof(handle) - 1 + key size = 96 - 1 + 3 = 98
    CacheKey key1("100");
    insert_LRUCache(cache, key1, 100, CachePriority::NORMAL);
    ASSERT_EQ(198, cache.get_usage()); // 100 + 98

    CacheKey key2("200");
    insert_LRUCache(cache, key2, 200, CachePriority::DURABLE);
    ASSERT_EQ(496, cache.get_usage()); // 198 + 298(d), d = DURABLE

    CacheKey key3("300");
    insert_LRUCache(cache, key3, 300, CachePriority::NORMAL);
    ASSERT_EQ(894, cache.get_usage()); // 198 + 298(d) + 398

    CacheKey key4("400");
    insert_LRUCache(cache, key4, 400, CachePriority::NORMAL);
    // Data cache is full, not insert, visits lru cache not exist key=498(400 + 98) and insert it.
    ASSERT_EQ(894, cache.get_usage());

    insert_LRUCache(cache, key4, 400, CachePriority::NORMAL);
    // Data cache 298(d) + 498, evict 198 398. visits lru cache exist key=498
    // and erase from visits lru cache, insert to Data cache.
    ASSERT_EQ(796, cache.get_usage());

    CacheKey key5("500");
    insert_LRUCache(cache, key5, 500, CachePriority::NORMAL);
    // Data cache is full, not insert, visits lru cache not exist key=598(500 + 98) and insert it.
    ASSERT_EQ(796, cache.get_usage());

    CacheKey key6("600");
    insert_LRUCache(cache, key6, 600, CachePriority::NORMAL);
    // Data cache is full, not insert, visits lru cache not exist key=698(600 + 98) and insert it,
    // visits lru cache is full, evict key=598 from visits lru cache.
    ASSERT_EQ(796, cache.get_usage());

    insert_LRUCache(cache, key5, 500, CachePriority::NORMAL);
    // Data cache is full, not insert, visits lru cache not exist key=598 and insert it.
    // visits lru cache is full, evict key=698 from visits lru cache.
    ASSERT_EQ(796, cache.get_usage());

    insert_LRUCache(cache, key5, 500, CachePriority::NORMAL);
    // Data cache 298(d) + 598, evict 498. visits lru cache exist key=598
    // and erase from visits lru cache, insert to Data cache.
    ASSERT_EQ(896, cache.get_usage());

    CacheKey key7("980");
    insert_LRUCache(cache, key7, 980, CachePriority::DURABLE);
    // Data cache is full, not insert, visits lru cache not exist key=1078(980 + 98)
    // but 1078 > capacity(1050), not insert visits lru cache.
    ASSERT_EQ(896, cache.get_usage());

    insert_LRUCache(cache, key7, 980, CachePriority::DURABLE);
    // Ssame as above, data cache is full, not insert, visits lru cache not exist key=1078(980 + 98)
    // but 1078 > capacity(1050), not insert visits lru cache.
    ASSERT_EQ(896, cache.get_usage());
}

TEST_F(CacheTest, Prune) {
    LRUCache cache(LRUCacheType::NUMBER);
    cache.set_capacity(5);

    // The lru usage is 1, add one entry
    CacheKey key1("100");
    insert_number_LRUCache(cache, key1, 100, 1, CachePriority::NORMAL);
    EXPECT_EQ(1, cache.get_usage());

    CacheKey key2("200");
    insert_number_LRUCache(cache, key2, 200, 1, CachePriority::DURABLE);
    EXPECT_EQ(2, cache.get_usage());

    CacheKey key3("300");
    insert_number_LRUCache(cache, key3, 300, 1, CachePriority::NORMAL);
    EXPECT_EQ(3, cache.get_usage());

    CacheKey key4("400");
    insert_number_LRUCache(cache, key4, 400, 1, CachePriority::NORMAL);
    EXPECT_EQ(4, cache.get_usage());

    CacheKey key5("500");
    insert_number_LRUCache(cache, key5, 500, 1, CachePriority::NORMAL);
    EXPECT_EQ(5, cache.get_usage());

    CacheKey key6("600");
    insert_number_LRUCache(cache, key6, 600, 1, CachePriority::NORMAL);
    EXPECT_EQ(5, cache.get_usage());

    CacheKey key7("700");
    insert_number_LRUCache(cache, key7, 700, 1, CachePriority::DURABLE);
    EXPECT_EQ(5, cache.get_usage());

    auto pred = [](const LRUHandle* handle) -> bool { return false; };
    cache.prune_if(pred);
    EXPECT_EQ(5, cache.get_usage());

    auto pred2 = [](const LRUHandle* handle) -> bool {
        return DecodeValue((void*)(((CacheValue*)handle->value)->value)) > 400;
    };
    cache.prune_if(pred2);
    EXPECT_EQ(2, cache.get_usage());

    cache.prune();
    EXPECT_EQ(0, cache.get_usage());

    for (int i = 1; i <= 5; ++i) {
        insert_number_LRUCache(cache, CacheKey {std::to_string(i)}, i, 1, CachePriority::NORMAL);
        EXPECT_EQ(i, cache.get_usage());
    }
    cache.prune_if([](const LRUHandle*) { return true; });
    EXPECT_EQ(0, cache.get_usage());
}

TEST_F(CacheTest, PruneIfLazyMode) {
    LRUCache cache(LRUCacheType::NUMBER);
    cache.set_capacity(10);

    // The lru usage is 1, add one entry
    CacheKey key1("100");
    insert_number_LRUCache(cache, key1, 100, 1, CachePriority::NORMAL);
    EXPECT_EQ(1, cache.get_usage());

    CacheKey key2("200");
    insert_number_LRUCache(cache, key2, 200, 1, CachePriority::DURABLE);
    EXPECT_EQ(2, cache.get_usage());

    CacheKey key3("300");
    insert_number_LRUCache(cache, key3, 300, 1, CachePriority::NORMAL);
    EXPECT_EQ(3, cache.get_usage());

    CacheKey key4("666");
    insert_number_LRUCache(cache, key4, 666, 1, CachePriority::NORMAL);
    EXPECT_EQ(4, cache.get_usage());

    CacheKey key5("500");
    insert_number_LRUCache(cache, key5, 500, 1, CachePriority::NORMAL);
    EXPECT_EQ(5, cache.get_usage());

    CacheKey key6("600");
    insert_number_LRUCache(cache, key6, 600, 1, CachePriority::NORMAL);
    EXPECT_EQ(6, cache.get_usage());

    CacheKey key7("700");
    insert_number_LRUCache(cache, key7, 700, 1, CachePriority::DURABLE);
    EXPECT_EQ(7, cache.get_usage());

    auto pred = [](const LRUHandle* handle) -> bool { return false; };
    cache.prune_if(pred, true);
    EXPECT_EQ(7, cache.get_usage());

    // in lazy mode, the first item not satisfied the pred2, `prune_if` then stopped
    // and no item's removed.
    auto pred2 = [](const LRUHandle* handle) -> bool {
        return DecodeValue((void*)(((CacheValue*)handle->value)->value)) > 400;
    };
    cache.prune_if(pred2, true);
    EXPECT_EQ(7, cache.get_usage());

    // in normal priority, 100, 300 are removed
    // in durable priority, 200 is removed
    auto pred3 = [](const LRUHandle* handle) -> bool {
        return DecodeValue((void*)(((CacheValue*)handle->value)->value)) <= 600;
    };
    PrunedInfo pruned_info = cache.prune_if(pred3, true);
    EXPECT_EQ(3, pruned_info.pruned_count);
    EXPECT_EQ(4, cache.get_usage());
}

TEST_F(CacheTest, Number) {
    LRUCache cache(LRUCacheType::NUMBER);
    cache.set_capacity(5);

    // The lru usage is 1, add one entry
    CacheKey key1("1");
    insert_number_LRUCache(cache, key1, 0, 1, CachePriority::NORMAL);
    EXPECT_EQ(1, cache.get_usage());

    CacheKey key2("2");
    insert_number_LRUCache(cache, key2, 0, 2, CachePriority::DURABLE);
    EXPECT_EQ(3, cache.get_usage());

    CacheKey key3("3");
    insert_number_LRUCache(cache, key3, 0, 3, CachePriority::NORMAL);
    EXPECT_EQ(5, cache.get_usage());

    CacheKey key4("4");
    insert_number_LRUCache(cache, key4, 0, 3, CachePriority::NORMAL);
    EXPECT_EQ(5, cache.get_usage()); // evict key3, because key3 is NORMAL, key2 is DURABLE.

    CacheKey key5("5");
    insert_number_LRUCache(cache, key5, 0, 5, CachePriority::NORMAL);
    EXPECT_EQ(5, cache.get_usage()); // evict key2, key4

    CacheKey key6("6");
    insert_number_LRUCache(cache, key6, 0, 6, CachePriority::NORMAL);
    EXPECT_EQ(0, cache.get_usage()); // evict key5, evict key6 at the same time as release.

    CacheKey key7("7");
    insert_number_LRUCache(cache, key7, 200, 1, CachePriority::NORMAL);
    EXPECT_EQ(1, cache.get_usage());

    CacheKey key8("8");
    insert_number_LRUCache(cache, key8, 100, 1, CachePriority::DURABLE);
    EXPECT_EQ(2, cache.get_usage());

    insert_number_LRUCache(cache, key8, 100, 1, CachePriority::DURABLE);
    EXPECT_EQ(2, cache.get_usage());

    auto pred = [](const LRUHandle* handle) -> bool { return false; };
    cache.prune_if(pred);
    EXPECT_EQ(2, cache.get_usage());

    auto pred2 = [](const LRUHandle* handle) -> bool {
        return DecodeValue((void*)(((CacheValue*)handle->value)->value)) > 100;
    };
    cache.prune_if(pred2);
    EXPECT_EQ(1, cache.get_usage());

    cache.prune();
    EXPECT_EQ(0, cache.get_usage());
}

TEST_F(CacheTest, HeavyEntries) {
    init_size_cache();
    // Add a bunch of light and heavy entries and then count the combined
    // size of items still in the cache, which must be approximately the
    // same as the total capacity.
    const int kLight = 1;
    const int kHeavy = 10;
    int added = 0;
    int index = 0;

    while (added < 2 * kCacheSize) {
        const int weight = (index & 1) ? kLight : kHeavy;
        Insert(index, 1000 + index, weight);
        added += weight;
        index++;
    }

    int cached_weight = 0;

    for (int i = 0; i < index; i++) {
        const int weight = (i & 1 ? kLight : kHeavy);
        int r = Lookup(i);

        if (r >= 0) {
            cached_weight += weight;
            EXPECT_EQ(1000 + i, r);
        }
    }

    EXPECT_LE(cached_weight, kCacheSize + kCacheSize / 10);
}

TEST_F(CacheTest, NewId) {
    init_size_cache();
    uint64_t a = cache()->new_id();
    uint64_t b = cache()->new_id();
    EXPECT_NE(a, b);
}

TEST_F(CacheTest, SimpleBenchmark) {
    init_size_cache();
    for (int i = 0; i < kCacheSize * LOOP_LESS_OR_MORE(10, 10000); i++) {
        Insert(1000 + i, 2000 + i, 1);
        EXPECT_EQ(2000 + i, Lookup(1000 + i));
    }
}

TEST(CacheHandleTest, HandleTableTest) {
    HandleTable ht;

    for (uint32_t i = 0; i < ht._length; ++i) {
        EXPECT_EQ(ht._list[i], nullptr);
    }

    const int count = 10;
    CacheKey keys[count] = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"};
    EXPECT_NE(keys[0], keys[1]);
    LRUHandle* hs[count];
    for (int i = 0; i < count; ++i) {
        CacheKey* key = &keys[i];
        auto* h = reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key->size()));
        h->value = nullptr;
        h->charge = 1;
        h->total_size = sizeof(LRUHandle) - 1 + key->size() + 1;
        h->key_length = key->size();
        h->hash = 1; // make them in a same hash table linked-list
        h->refs = 0;
        h->next = h->prev = nullptr;
        h->next_hash = nullptr;
        h->in_cache = false;
        h->priority = CachePriority::NORMAL;
        memcpy(h->key_data, key->data(), key->size());

        LRUHandle* old = ht.insert(h);
        EXPECT_EQ(ht._elems, i + 1);
        EXPECT_EQ(old, nullptr); // there is no entry with the same key and hash
        hs[i] = h;
    }
    EXPECT_EQ(ht._elems, count);
    LRUHandle* h = ht.lookup(keys[0], 1);
    LRUHandle** head_ptr = &(ht._list[1 & (ht._length - 1)]);
    LRUHandle* head = *head_ptr;
    ASSERT_EQ(head, h);
    int index = 0;
    while (h != nullptr) {
        EXPECT_EQ(hs[index], h) << index;
        h = h->next_hash;
        ++index;
    }

    for (int i = 0; i < count; ++i) {
        CacheKey* key = &keys[i];
        auto* h = reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key->size()));
        h->value = nullptr;
        h->charge = 1;
        h->total_size = sizeof(LRUHandle) - 1 + key->size() + 1;
        h->key_length = key->size();
        h->hash = 1; // make them in a same hash table linked-list
        h->refs = 0;
        h->next = h->prev = nullptr;
        h->next_hash = nullptr;
        h->in_cache = false;
        h->priority = CachePriority::NORMAL;
        memcpy(h->key_data, key->data(), key->size());

        EXPECT_EQ(ht.insert(h), hs[i]); // there is an entry with the same key and hash
        EXPECT_EQ(ht._elems, count);
        free(hs[i]);
        hs[i] = h;
    }
    EXPECT_EQ(ht._elems, count);

    for (int i = 0; i < count; ++i) {
        EXPECT_EQ(ht.lookup(keys[i], 1), hs[i]);
    }

    LRUHandle* old = ht.remove(CacheKey("0"), 1); // first in hash table linked-list
    ASSERT_EQ(old, hs[0]);
    ASSERT_EQ(old->next_hash, hs[1]); // hs[1] is the new first node
    ASSERT_EQ(*head_ptr, hs[1]);

    old = ht.remove(CacheKey("9"), 1); // last in hash table linked-list
    ASSERT_EQ(old, hs[9]);

    old = ht.remove(CacheKey("5"), 1); // middle in hash table linked-list
    ASSERT_EQ(old, hs[5]);
    ASSERT_EQ(old->next_hash, hs[6]);
    ASSERT_EQ(hs[4]->next_hash, hs[6]);

    ht.remove(hs[4]); // middle in hash table linked-list
    ASSERT_EQ(hs[3]->next_hash, hs[6]);

    EXPECT_EQ(ht._elems, count - 4);

    for (auto& h : hs) {
        free(h);
    }
}

TEST_F(CacheTest, SetCapacity) {
    init_number_cache();
    for (int i = 0; i < kCacheSize; i++) {
        Insert(i, 1000 + i, 1);
        EXPECT_EQ(1000 + i, Lookup(i));
    }
    ASSERT_EQ(kCacheSize, cache()->get_capacity());
    ASSERT_EQ(kCacheSize, cache()->get_usage());

    int64_t prune_num = cache()->adjust_capacity_weighted(2);
    ASSERT_EQ(prune_num, 0);
    ASSERT_EQ(kCacheSize * 2, cache()->get_capacity());
    ASSERT_EQ(kCacheSize, cache()->get_usage());

    prune_num = cache()->adjust_capacity_weighted(0.5);
    ASSERT_EQ(prune_num, kCacheSize / 2);
    ASSERT_EQ(kCacheSize / 2, cache()->get_capacity());
    ASSERT_EQ(kCacheSize / 2, cache()->get_usage());

    std::vector<Cache::Handle*> handles(kCacheSize, nullptr);
    for (int i = 0; i < kCacheSize; i++) {
        std::string result;
        CacheKey cache_key = EncodeKey(&result, kCacheSize + i);
        auto* cache_value = new CacheValueWithKey(DecodeKey(cache_key), EncodeValue(i));
        handles[i] = cache()->insert(cache_key, cache_value, 1, 1);
    }
    ASSERT_EQ(kCacheSize / 2, cache()->get_capacity());
    ASSERT_EQ(kCacheSize,
              cache()->get_usage()); // Handle not be released, so key cannot be evicted.

    for (int i = 0; i < kCacheSize; i++) {
        // The Key exists in the Cache, remove the old Entry from the Cache, and insert it again.
        Insert(i + kCacheSize, 2000 + i, 1);
        if (i < kCacheSize / 2) {
            // Insert() will replace the entry with the same key in the cache, the replaced entry will
            // not be freed because there are unreleased handles holding them.
            // The current cache capacity(kCacheSize/2) is half of the cache usage(kCacheSize),
            // Insert() method will immediately release the handle of the newly inserted entry,
            // so the newly inserted entry will be freed, until cache usage is less than or equal to capacity.
            ASSERT_GE(cache()->get_usage(), cache()->get_capacity());
            EXPECT_EQ(-1, Lookup(i + kCacheSize));
        } else if (i == kCacheSize / 2) {
            // When cache usage is equal to cache capacity, Insert() will replace the old entry
            // with the same key and will not free the entry after releasing the handle.
            ASSERT_EQ(cache()->get_usage(), cache()->get_capacity());
            EXPECT_EQ(2000 + i, Lookup(i + kCacheSize));
        } else {
            // When inserting at `i == kCacheSize / 2 + 1`, the cache usage is equal to the cache capacity,
            // so the entry in the LRU list will be evicted (usage - 1) and then inserted (usage + 1).
            // because the entry inserted is an existing key, the old entry with the same key is evicted (usage - 1),
            // so the final cache usage is equal to (capacity - 1).
            ASSERT_EQ(cache()->get_usage(), cache()->get_capacity() - 1);
            EXPECT_EQ(2000 + i, Lookup(i + kCacheSize));
        }
    }
    ASSERT_EQ(kCacheSize / 2, cache()->get_capacity());
    // Here cache usage equals cache capacity - 1, because the entry inserted in the previous step
    // at `i == kCacheSize / 2 + 1` was evicted, see the reason above.
    // Entries held by unreleased handles in `handles` will not be counted in cache usage,
    // but will still be counted in the memory tracker.
    ASSERT_EQ(kCacheSize / 2 - 1, cache()->get_usage());

    cache()->adjust_capacity_weighted(2);
    ASSERT_EQ(kCacheSize * 2, cache()->get_capacity());
    ASSERT_EQ(kCacheSize / 2 - 1, cache()->get_usage());

    for (int i = 0; i < kCacheSize; i++) {
        Insert(i, 3000 + i, 1);
        EXPECT_EQ(3000 + i, Lookup(i));
    }
    ASSERT_EQ(kCacheSize * 2, cache()->get_capacity());
    ASSERT_EQ(kCacheSize * 1.5 - 1, cache()->get_usage());

    cache()->adjust_capacity_weighted(0);
    ASSERT_EQ(0, cache()->get_capacity());
    ASSERT_EQ(0, cache()->get_usage());

    for (auto it : handles) {
        cache()->release(it);
    }
    ASSERT_EQ(0, cache()->get_capacity());
    ASSERT_EQ(0, cache()->get_usage());

    cache()->adjust_capacity_weighted(1);
    ASSERT_EQ(kCacheSize, cache()->get_capacity());
    ASSERT_EQ(0, cache()->get_usage());

    cache()->adjust_capacity_weighted(0);
    ASSERT_EQ(0, cache()->get_capacity());
    ASSERT_EQ(0, cache()->get_usage());

    for (int i = 0; i < kCacheSize; i++) {
        Insert(i, 4000 + i, 1);
        EXPECT_EQ(-1, Lookup(i));
    }
    ASSERT_EQ(0, cache()->get_capacity());
    ASSERT_EQ(0, cache()->get_usage());
}

TEST_F(CacheTest, ResetInitialCapacity) {
    init_number_cache();
    for (int i = 0; i < kCacheSize; i++) {
        Insert(i, 1000 + i, 1);
        EXPECT_EQ(1000 + i, Lookup(i));
    }
    ASSERT_EQ(kCacheSize, cache()->get_capacity());
    ASSERT_EQ(kCacheSize, cache()->get_usage());

    int64_t prune_num = cache()->adjust_capacity_weighted(0.5);
    ASSERT_EQ(prune_num, kCacheSize / 2);
    ASSERT_EQ(kCacheSize / 2, cache()->get_capacity());
    ASSERT_EQ(kCacheSize / 2, cache()->get_usage());

    prune_num = cache()->adjust_capacity_weighted(2);
    ASSERT_EQ(prune_num, 0);
    ASSERT_EQ(kCacheSize * 2, cache()->get_capacity());
    ASSERT_EQ(kCacheSize / 2, cache()->get_usage());

    prune_num = cache()->reset_initial_capacity(0.5);
    ASSERT_EQ(prune_num, 0);
    ASSERT_EQ(kCacheSize / 2, cache()->get_capacity());
    ASSERT_EQ(kCacheSize / 2, cache()->get_usage());

    prune_num = cache()->adjust_capacity_weighted(2);
    ASSERT_EQ(prune_num, 0);
    ASSERT_EQ(kCacheSize, cache()->get_capacity());
    ASSERT_EQ(kCacheSize / 2, cache()->get_usage());

    prune_num = cache()->adjust_capacity_weighted(1);
    ASSERT_EQ(prune_num, 0);
    ASSERT_EQ(kCacheSize / 2, cache()->get_capacity());
    ASSERT_EQ(kCacheSize / 2, cache()->get_usage());

    prune_num = cache()->reset_initial_capacity(4);
    ASSERT_EQ(prune_num, 0);
    ASSERT_EQ(kCacheSize * 2, cache()->get_capacity());
    ASSERT_EQ(kCacheSize / 2, cache()->get_usage());

    for (int i = kCacheSize; i < kCacheSize * 2; i++) {
        Insert(i, 1000 + i, 1);
        EXPECT_EQ(1000 + i, Lookup(i));
    }
    ASSERT_EQ(kCacheSize * 2, cache()->get_capacity());
    ASSERT_EQ(kCacheSize + kCacheSize / 2, cache()->get_usage());

    prune_num = cache()->adjust_capacity_weighted(0.5);
    ASSERT_EQ(prune_num, kCacheSize / 2);
    ASSERT_EQ(kCacheSize, cache()->get_capacity());
    ASSERT_EQ(kCacheSize, cache()->get_usage());

    prune_num = cache()->reset_initial_capacity(0.25);
    ASSERT_EQ(prune_num, kCacheSize / 2);
    ASSERT_EQ(kCacheSize / 2, cache()->get_capacity());
    ASSERT_EQ(kCacheSize / 2, cache()->get_usage());
}

} // namespace doris
