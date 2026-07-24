# Cloud Segment List Rowset Layout 审查问题

参考提交 `[feature](cloud) Support segment list rowset layout` 对当前存算分离代码进行静态审查后，仍发现多处把 rowset 内的 segment position（`0, 1, ..., num_segments - 1`）当作物理 segment id 使用的问题。

部分 compaction 输出相关问题目前尚未在生产路径触发，因为
`set_segment_id_range()` 还没有接入 Cloud compaction；一旦接入非零输出 segment id，
这些问题可能造成 delete bitmap 错误或 compaction 失败。Cloud partial update 的
transient append 只处理当前待发布的 load rowset，不会向已经 visible 的 compaction
输出 rowset 追加 segment，因此需要单独判断其触发条件。

## 1. [P1][已修复] RowIdConversion 把输出 segment position 当成物理 segment id

位置：

- `be/src/storage/rowid_conversion.h:92`
- `be/src/storage/tablet/base_tablet.cpp:1834`

`RowIdConversion::add()` 保存的 `_cur_dst_segment_id` 是输出 segment 的 position：

```cpp
_segments_rowid_map[id][item.row_id] =
        std::pair<uint32_t, uint32_t> {_cur_dst_segment_id, _cur_dst_segment_rowid++};
```

`RowIdConversion::get()` 随后直接把这个 position 填入 `RowLocation.segment_id`，而 `calc_compaction_output_rowset_delete_bitmap()` 又直接使用它生成 delete bitmap key：

```cpp
output_rowset_delete_bitmap->add({dst.rowset_id, dst.segment_id, cur_version}, dst.row_id);
```

如果 vertical compaction 输出物理 segment id 为 `[10, 11]`，这里生成的 delete bitmap key 仍然是 `0` 和 `1`。读取真实 segment `10` 和 `11` 时会漏掉对应删除记录，属于数据正确性问题。

内部 inverted index compaction 的 conversion vector 可以继续使用 position，但转换为 `RowLocation` 或 delete bitmap key 时必须映射成输出物理 segment id。

修复后，`RowIdConversion::get()` 返回显式的 destination segment position 和 row id，不再
使用 `RowLocation` 承载 position。生成 delete bitmap 和 correctness check 的
`location_map` 时，通过 output rowset 的 segment view 将 position 转换成物理 segment
id。inverted index compaction 继续使用内部 position-based conversion vector。

新增单测使用输入物理 segment id `10` 和输出物理 segment id `100`，验证 delete bitmap
key 和 `location_map` 均使用真实物理 id。

## 2. [当前生产路径不可达] Cloud partial update 尚不能合并 segment list

位置：

- `be/src/cloud/cloud_tablet.cpp:902`
- `be/src/storage/rowset/rowset_meta.cpp:375`

`CloudTablet::create_transient_rowset_writer()` 仍然使用：

```cpp
writer->set_segment_start_id(cast_set<int32_t>(rowset.num_segments()));
```

这段代码并不是只在 compaction 中设置非零起点。partial update 为 legacy rowset 追加
segment 时也会设置非零起点；由于 legacy rowset 的物理 id 是
`0, 1, ..., num_segments - 1`，从 `num_segments` 开始分配是正确的。

当前生产路径不会把带 `segment_ids` 的 rowset 传入这里：

- `set_segment_id_range()` 没有生产调用方，只在单元测试中设置；
- 当前代码只有非零起点的 vertical writer 会主动生成 `segment_ids`，因此生产流程尚不会
  生成这类 rowset；
- transient append 处理的是当前待发布的 `BEGIN_PARTIAL_UPDATE` rowset，重试时可能为
  `COMMITTED`，而不是已经 `VISIBLE` 的 compaction 输出 rowset；
- 当前 load/partial-update writer 不会为原始待发布 rowset 持久化 segment list。

因此，仅将非零 segment start 接入 Cloud compaction，不会触发本问题。

代码仍然没有兼容“待发布的 partial-update rowset 自身已经带 `segment_ids`”的情况。
如果将来 load/partial-update writer 也支持 segment list，或者滚动升级期间其他版本 BE
可能提交这种 rowset，那么对于 `segment_ids=[10, 11]`，下一个物理 id 应该是 `12`，
当前代码却会从 `2` 开始；对于 `segment_ids=[0, 2]`，还会重复分配已经存在的 segment
2。

同时，`RowsetMeta::merge_rowset_meta()` 只增加 `num_segments`，没有合并
`segment_ids`。在上述未来场景中，append 后可能得到不一致的 metadata：

```text
num_segments = 3
segment_ids = [10, 11]
```

如果扩展上述生产能力，需要从已有 segment list 的最后一个物理 id 之后开始分配，并在
合并 transient rowset metadata 时同步合并 segment list。在此之前，该项作为兼容性风险
保留，不属于当前可触发的 P1 缺陷。

## 3. [P1][已修复] Delete bitmap store v2/v3 聚合路径仍遍历连续 segment id

位置：

- `be/src/cloud/cloud_tablet.cpp:1399`
- `be/src/storage/tablet/tablet_meta.cpp:1750`

`CloudTablet::calc_delete_bitmap_for_compaction()` 只向 delete bitmap API 传递 `(rowset_id, num_segments)`：

```cpp
std::vector<std::pair<RowsetId, int64_t>> retained_rowsets_to_seg_num;
```

`DeleteBitmap::subset_and_agg()` 随后固定遍历：

```cpp
for (int64_t seg_id = 0; seg_id < segment_num; ++seg_id) {
    BitmapKey end {rowset_id, seg_id, end_version};
    auto bm = get_agg_without_cache(end, start_version);
    // ...
}
```

当 retained rowset 使用 `segment_ids=[10, 11]` 时，真实 delete bitmap 不会被聚合。

触发条件：

- `delete_bitmap_store_write_version` 为 2 或 3；
- `enable_agg_delta_delete_bitmap_for_store_v2=true`，该配置默认值为 true。

非聚合的 `DeleteBitmap::subset()` 使用整个 rowset key range 扫描，是安全的。问题只存在于 `subset_and_agg()`。

修复后，该接口接收每个 rowset 的真实 segment id 列表，Cloud 调用方通过
`rowset->segments()` 构造列表，聚合路径不再根据 segment 数量推导物理 id。新增单测使用
`segment_ids=[10, 12]` 验证只聚合列表中的真实 segment。

## 4. [P1][已修复] Inverted index compaction 按 position 查找输出 writer

位置：

- `be/src/storage/compaction/compaction.cpp:868`
- `be/src/storage/compaction/compaction.cpp:1047`

`Compaction::do_inverted_index_compaction()` 使用 `0..dest_segment_num-1` 访问输出 index writer：

```cpp
for (int dest_segment_id = 0; dest_segment_id < dest_segment_num; dest_segment_id++) {
    auto res = inverted_index_file_writers[dest_segment_id]->open(index_meta);
    // ...
}
```

`inverted_index_file_writers` 的 key 是真实物理 segment id。输出从 10 开始时，map 中的 key 是 `10`、`11`，这里却访问 `0`、`1`。`operator[]` 会插入空指针，随后解引用，可能直接崩溃。

同一函数生成 debug 文件名时也仍然使用 `0..dest_segment_num-1`，无法表示真实的输出 segment 文件名。

需要显式区分 destination position 和物理 segment id，并使用实际 writer key 访问输出 index writer。

修复后，从输出 index writer 的实际 key 构造按物理 segment id 排序的 destination
segment id 列表。`RowIdConversion`、`dest_segment_num_rows` 和 `dest_index_dirs` 仍按
destination position 组织；访问 writer 和生成 debug 文件名时，通过该列表映射到真实
物理 segment id。index compaction 单测将输出起始 segment id 设置为 10，覆盖非零
segment id 的 writer 查找和输出 rowset metadata。

## 5. [P2][已修复] Cloud Recycler 删除 V2 inverted index 时使用循环 position

位置：

- `cloud/src/recycler/recycler.cpp:3205`
- `cloud/src/recycler/recycler.cpp:3962`

两个 Recycler 路径中，data 文件和 V1 inverted index 已经使用真实 `segment_id`，但 V2 inverted index 仍然传入循环 position `i`：

```cpp
auto segment_id = rowset_segment_id(rs, i);

// Data file uses the real id.
segment_path(tablet_id, rowset_id, segment_id);

// V2 index still uses the position.
inverted_index_path_v2(tablet_id, rowset_id, i);
```

例如 `segment_ids=[10, 11]` 时，Recycler 会尝试删除 `_0.idx` 和 `_1.idx`，真实的 `_10.idx` 和 `_11.idx` 会被遗留，造成对象存储泄漏。

V2 inverted index 路径应统一使用已经解析出的 `segment_id`。

修复后，单 rowset 和批量 rowset 两条 Recycler 删除路径都使用
`rowset_segment_id()` 解析出的物理 segment id 构造 V2 inverted index 路径。新增单测
使用 `segment_ids=[10, 12]` 覆盖两条路径，验证真实 `.dat` 和 `.idx` 文件均被删除。

## 6. [P2][已修复] Rowid conversion correctness check 用物理 id 索引 position vector

位置：

- `be/src/storage/tablet/base_tablet.cpp:1867`
- `be/src/storage/tablet/base_tablet.cpp:1880`

`BaseTablet::check_rowid_conversion()` 使用：

```cpp
segments[src.segment_id]->read_key_by_rowid(...);
dst_segments[dst.segment_id]->read_key_by_rowid(...);
```

`load_segments()` 返回的是按 rowset position 排列的 vector，而 `RowLocation.segment_id` 表示物理 segment id。输入 rowset 为 `segment_ids=[10, 11]` 时，这里会发生越界访问。

该路径只在 `enable_rowid_conversion_correctness_check` 开启时触发，但 Cloud compaction 会调用它。

访问 vector 前应使用对应 rowset metadata 的 `position_of(segment_id)` 将物理 id 转换成 position。

修复后，source 和 destination `RowLocation` 都保持物理 segment id 语义；读取
`load_segments()` 返回的 vector 前，分别通过 source 和 destination rowset metadata 的
`position_of()` 转换为 position。`test_mow_compact_multi_segments` 同时开启随机非零输出
segment id 和 `enable_rowid_conversion_correctness_check`，覆盖真实 compaction 检查路径。

## 7. Production Cloud compaction 尚未设置 segment id range

位置：

- `be/src/storage/compaction/compaction.cpp:1967`
- `be/test/storage/compaction/vertical_compaction_test.cpp:418`

当前 `set_segment_id_range()` 只在单元测试中调用。实际 Cloud compaction 在创建输出 writer 后，没有设置 segment id 分配范围：

```cpp
_output_rs_writer = DORIS_TRY(_tablet->create_rowset_writer(ctx, _is_vertical));
```

因此目前非零 vertical compaction segment id 还没有作为正常配置接入生产路径。问题 1
和问题 6 已修复，并通过 debug point 在回归测试中注入随机非零起点。问题 2 不会因为
compaction 接入非零起点而单独触发。

## 已确认安全的 position 循环

以下 `0..num_segments-1` 循环表示 rowset position，不应替换成物理 segment id：

- 访问 rowset repeated metadata，例如 `segments_file_size[pos]`、`num_segment_rows[pos]` 和 `segments_key_bounds[pos]`；
- scanner 使用的 `segment_offsets`；
- `BetaRowsetReader` 先通过 position 获取 `segment(pos).ref()`；
- iterator flag、rowset id 数组等与 segment iterator vector 一一对应的数据结构。

以下路径仍使用连续物理 id，但不属于 Cloud segment-list rowset 的处理范围：

- local-only binlog；
- local snapshot/link/copy；
- local-mode `RemoteRowsetGcPB`。

## 测试覆盖缺口

当前测试主要覆盖 segment id accessor、rowset reader 和 writer metadata，没有覆盖以下非零 segment id 场景：

- inverted index compaction 输出 writer；
- Cloud Recycler 的 V2 inverted index；
- Cloud compaction 对 `set_segment_id_range()` 的生产接入。

建议补充至少一个使用非零且非连续 segment id 的 Cloud 端到端或组件级测试，避免后续再次混用 position 和物理 id。

如果将来允许 load/partial-update rowset 自身携带 segment list，还需要补充对应的
transient append 和 metadata merge 测试；当前生产能力不要求该测试。

## 审查说明

本次仅进行静态代码审查，未编译、未运行测试。
