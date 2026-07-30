# 并发 Single-Rowset Compaction 设计方案

## 1. 背景

Cloud cumulative compaction 可以选择一个 overlapping rowset，并将其中的 segment 划分为
多个 group。每个 group 独立执行 merge，最后按照原始 group 顺序拼接所有输出，构成一个
最终 output rowset。

当前 grouped single-rowset compaction 使用一个共享的 `RowsetWriter` 串行执行所有
group。本方案增加两种并发执行模式：

1. 单 BE 并发：在同一个 BE 中并发执行多个 group。
2. 多 BE 并发：由 coordinator BE 将 group 分发给多个 worker BE 并发执行。

本方案复用提交 `f4ca355e` 中的任务隔离、延迟 finalize、结果汇总和 MOW 分片思路，
并针对 grouped single-rowset compaction 以及 segment ID list rowset layout 进行适配。

## 2. 目标

- 并发执行一个 input rowset 中相互独立的 group。
- 所有 group 输出使用同一个最终 output rowset ID。
- 为每个并发任务分配互不重叠的物理 segment ID slot。
- 支持非零、不连续的物理 segment ID。
- 无论任务完成顺序如何，都保持原始 group 顺序。
- rowset commit、delete bitmap 更新和 tablet 状态变更只由 coordinator 执行。
- 单 BE 和多 BE 模式共用 merge 与 finalize 实现。
- 支持 horizontal compaction、vertical compaction、inverted index 和 MOW 表。
- 限制并发资源消耗，并提供确定的失败处理和完整清理机制。

## 3. 非目标

- 不把一个 segment 拆分给多个 worker。
- 不允许 worker 提交 partial rowset。
- 不保证不同 group 的输出在全局范围内 key 不重叠。
- 第一版不支持 coordinator 进程退出后恢复正在执行的分布式 compaction。
- 任一 group 任务开始后，不再动态改变 group 划分。

## 4. 术语和核心约束

### 4.1 Segment position 与物理 segment ID

必须严格区分 segment position 和物理 segment ID。

- Segment position 是 rowset metadata 中的数组下标，范围是
  `[0, num_segments)`。
- 物理 segment ID 用于数据文件和索引文件路径、cache key、delete bitmap key 以及
  `RowLocation`。
- 通过 `rowset->segment(position)` 将 segment position 映射为物理 segment。

以下 metadata 数组均按 segment position 索引：

- `num_segment_rows`
- `segments_key_bounds`
- `segments_file_size`
- `inverted_index_file_info`
- `segment_group_sizes`

最终的 `segment_ids` 数组负责将每个最终 segment position 映射为物理 segment ID。

例如：

```text
最终 position:      0   1   2    3    4    5
物理 segment ID:   17  18  19  117  118  217
```

代码绝不能使用物理 segment ID 作为 position-aligned metadata 数组的下标。

### 4.2 Group 顺序

每个任务都有稳定的 `group_index`。最终按照 `group_index` 汇总结果，不能按照任务
完成顺序汇总。

Input group 使用 position 范围表示：

```text
[segment_pos_start, segment_pos_end)
```

任务不能从 position 范围推导物理 ID 范围。输入文件必须通过 input rowset 的
segment ID list 解析。

### 4.3 输出 overlap 属性

每个 group 的输出内部是 nonoverlapping，但相邻 group 的 key range 仍可能重叠。

- 存在两个及以上非空输出 group 时，设置
  `NONOVERLAPPING_WITHIN_GROUP`。
- 只有零个或一个非空输出 group 时，设置
  `NONOVERLAPPING`。

当属性为 `NONOVERLAPPING_WITHIN_GROUP` 时，`segment_group_sizes` 按最终顺序记录
每个非空 group 的输出 segment 数量。

## 5. 总体架构

实现分为 planning、execution 和 finalization 三层：

```text
CloudCumulativeCompaction
        |
        v
生成 GroupCompactionPlan
        |
        v
+--------------------------------+
| GroupCompactionExecutor        |
|                                |
| LocalParallelExecutor，或      |
| DistributedParallelExecutor    |
+--------------------------------+
        |
        v
按 group_index 排列的 GroupCompactionResult[]
        |
        v
coordinator finalization
        |
        +-- 校验结果与 segment slot
        +-- 拼接 position-aligned metadata
        +-- 汇总 statistics
        +-- 组合 RowIdConversion shard
        +-- 构建最终 rowset
        +-- 计算并提交 delete bitmap
        +-- 提交 compaction
```

Group merge 算法和 finalization 逻辑不依赖任务实际执行位置。单 BE 与多 BE 模式仅替换
executor。

## 6. Task 与 Result 模型

逻辑数据结构如下：

```cpp
struct SegmentIdSlot {
    int32_t start_id;
    int32_t capacity;
};

struct GroupCompactionTask {
    int32_t group_index;

    // 输入 segment position。
    int64_t segment_pos_start;
    int64_t segment_pos_end;
    int64_t merge_way_num;

    RowsetId output_rowset_id;
    SegmentIdSlot output_segment_slot;

    std::string execution_id;
    int32_t attempt_id;
};

struct GroupCompactionResult {
    int32_t group_index;
    int32_t attempt_id;
    Status status;

    RowsetSharedPtr partial_rowset;
    std::vector<int64_t> output_segment_ids;
    Merger::Statistics stats;
    std::unique_ptr<RowIdConversion> rowid_conversion;
};
```

对于分布式执行，task RPC 还需要携带 tablet 信息、input rowset metadata、storage
resource、deadline，以及明确的输入物理 segment ID list。

- Segment position 用于索引 metadata。
- 物理 segment ID 用于定位文件、cache 和 `RowLocation`。

## 7. 共享 output rowset ID 与 segment ID slot

### 7.1 Slot 分配

Coordinator 在提交任务前只创建并 prepare 一个最终 output rowset。所有 group writer
使用相同的 rowset ID，并为每个 group 分配互不重叠的物理 segment ID slot。

例如 slot capacity 为 100：

```text
output rowset ID = X

group 0: start_seg_id = 17,  max_seg_num = 100, slot [17, 117)
group 1: start_seg_id = 117, max_seg_num = 100, slot [117, 217)
group 2: start_seg_id = 217, max_seg_num = 100, slot [217, 317)
```

假设实际输出为：

```text
group 0: [17, 18, 19]
group 1: [117, 118]
group 2: [217]
```

最终 segment ID list 为：

```text
[17, 18, 19, 117, 118, 217]
```

物理 ID 空洞是合法状态，不具有业务语义。

Compaction 只选择一次 base ID。Slot 分配必须检查整数溢出：

```text
slot.start_id = base_id + group_index * slot_capacity
slot.end_id   = slot.start_id + slot_capacity
```

Slot capacity 是可配置的硬配额。数值 100 只用于示例，不能硬编码，也不能作为正确性
假设。

### 7.2 Writer 约束

每个任务拥有独立的 writer 实例：

```cpp
writer->set_segment_start_id(slot.start_id, slot.capacity);
```

Writer 之间只共享不可变 context 和最终 rowset ID，不共享以下状态：

- segment allocator
- file writer map
- rowset metadata builder
- statistics
- RowIdConversion
- inverted index writer

`SegmentCreator::allocate_segment_id()` 在 slot 耗尽后已经可以返回
`TOO_MANY_SEGMENTS`。Slot 耗尽必须导致整个 compaction 失败，writer 不能继续分配到
其他 group 的 slot。

当前 writer 使用 `_segment_start_id != 0` 隐式判断 writer 是否为 transient writer，
并据此决定是否记录显式 segment ID。并发 group writer 需要引入明确的意图字段：

```text
is_partial_output_writer = true
```

该字段负责控制：

- 是否记录显式物理 segment ID。
- 是否允许 writer 内部执行 segment compaction。
- 是否允许 writer 独立 prepare 或 commit rowset。
- writer 的文件清理范围。

不能再使用 `start_seg_id` 推导以上行为，因为第一个 group 的物理 segment ID 可以合法
地从零开始。

### 7.3 Slot capacity

第一版为每个 group 分配固定的配置容量，并将 slot 耗尽视为确定性任务失败。任一任务
开始后，不再自动回退为串行执行。

后续可以允许一个 group 申请多个不连续 slot。Segment ID list layout 能够表达：

```text
group 0 slots: [17, 117), [317, 417)
group 0 output IDs: [17, ..., 116, 317, 318]
```

动态扩容需要 coordinator 分配 RPC 和更复杂的 retry 清理，因此不属于第一版范围。

## 8. 准备最终 rowset

`CloudCompactionMixin::construct_output_rowset_writer()` 当前会创建 output writer 并调用
`prepare_rowset`。并发 compaction 保留一个 coordinator 持有的 final writer：

1. 构造最终 `RowsetWriterContext`。
2. 创建 final output writer 并取得 rowset ID。
3. 选择物理 segment ID base。
4. 只调用一次 `prepare_rowset`。
5. 将不可变 context 复制给 partial writer。
6. 为 partial writer 设置共享 rowset ID、partial-writer 意图以及 task slot。

Partial writer 不得执行以下操作：

- 调用 `prepare_rowset`。
- 注册 visible rowset。
- 作为独立 rowset 进入 tablet rowset registry 或 cache。
- 修改 tablet 状态。
- commit transaction。

最终由 coordinator 使用 final writer 的 `manual_build()` 构建唯一可见的 output
rowset。

## 9. Group merge 执行

将 `CloudCumulativeCompaction::do_merge_input_rowsets()` 中现有的串行循环拆分为一个
worker-safe 操作：

```cpp
Status execute_group_merge(
        const GroupCompactionTask& task,
        const RowsetWriterContext& output_context,
        GroupCompactionResult* result);
```

每个 task 执行以下步骤：

1. 为 single input rowset 创建 reader。
2. 将 reader 限制在 task 的 segment position 范围。
3. 创建独立的 horizontal 或 vertical partial writer。
4. 设置分配好的 segment ID slot。
5. 创建 task-local statistics。
6. 在需要时创建 task-local RowIdConversion shard。
7. 执行 merge。
8. 关闭 writer 并构建 partial rowset metadata。
9. 返回有序的实际输出物理 segment ID 以及 position-aligned metadata。

该函数不能修改以下 coordinator 成员：

- `_output_rs_writer`
- `_output_rowset`
- `_stats`
- 共享的 `_stats.rowid_conversion`
- tablet `last_compaction_status`
- 共享 compaction progress

Executor 将每个结果写入由 `group_index` 指定的数组元素，任务完成顺序不能影响输出
顺序。

## 10. 单 BE 并发执行

### 10.1 调度

`LocalParallelExecutor` 将每个 group task 提交到 engine 级共享 compaction subtask
thread pool，并申请 concurrent token。

实际并发度为：

```text
min(
    group 数量,
    配置的最大并发度,
    当前可用 compaction task 配额,
    memory/resource permit
)
```

使用共享 pool，而不是为每次 compaction 创建一个新 pool，可以避免线程数量无限增长，
并统一资源隔离和监控。

每个任务在创建 reader 和 writer 前，需要 attach 父 compaction 的 memory tracker。

建议配置：

```text
enable_cloud_single_rowset_parallel_compaction = false
cloud_single_rowset_parallel_compaction_max_threads = 4
cloud_single_rowset_compaction_segment_slot_capacity = 100
```

构建 plan 时对配置取快照，同一个 compaction 执行期间不响应配置变化。

### 10.2 文件处理

每个本地 task 可以：

- 直接通过 cloud partial writer 写出；或者
- 先写入本地 staging rowset，merge 成功后再上传。

无论使用哪种方式，最终远端文件名都由以下两部分组成：

```text
共享 output rowset ID + task 独占的物理 segment ID
```

如果本地 staging 能避免暴露未完成的远端文件，则优先使用 staging。Staging rowset
identity 只是实现细节，不进入最终 rowset metadata。

### 10.3 失败处理

如果尚未启动任何 task，且资源申请失败，可以回退到现有串行路径。

任一 task 启动后发生失败时：

1. 停止提交新任务。
2. 取消尚未开始的任务。
3. 等待运行中的任务完成或观察到 cancellation。
4. 只清理对应 task attempt 创建的文件。
5. 返回第一个错误，整个 compaction 失败。

一旦产生 partial output，不再自动串行重试。

## 11. 多 BE 并发执行

### 11.1 角色划分

Coordinator BE 负责：

- 构建 group plan。
- 创建并 prepare 共享的 final rowset ID。
- 分配 segment ID slot。
- 选择 worker。
- 发送 merge 和 finalize 请求。
- 校验并排序结果。
- 构建最终 metadata。
- 计算或合并 delete bitmap。
- 提交 compaction。

Worker BE 负责：

- 同步所需 tablet 和 rowset metadata。
- 只读取指定的 input segment position。
- merge 一个 group。
- 使用共享 final rowset ID 和所分配 slot 上传输出文件。
- 在 MOW finalize 完成前保留 conversion 状态。
- 返回 partial metadata。
- 不提交 rowset，也不修改 tablet compaction 状态。

当前跨 BE 实现使用以下配置，且默认关闭：

```text
enable_cloud_single_rowset_distributed_compaction = false
cloud_single_rowset_compaction_workers = "be1:8060,be2:8060"
cloud_single_rowset_compaction_segment_slot_capacity = 100
cloud_single_rowset_compaction_rpc_timeout_ms = 3600000
```

`cloud_single_rowset_compaction_workers` 配置的是 worker 的 BRPC endpoint。Coordinator
会排除自身和重复 endpoint；同一个 endpoint 上的 group 串行执行，不同 endpoint 并发
执行。因此该模式不会在单个 BE 内为同一个 single-rowset compaction 并发执行多个
group。Worker 还会校验 request 中的 `cloud_unique_id` 与本机一致，避免把任务发送到
其他 cloud instance。

当前实现会在 commit 前失败时通知 worker 按已返回的物理 segment ID 精确删除远端
文件；commit RPC 一旦发出，即使 RPC 返回失败也不再主动删除远端文件，因为此时无法
排除 Meta Service 已经提交成功。Worker 析构只清理本地 staging 文件，不会以进程退出
为由删除可能已经可见的远端 rowset 文件。

### 11.2 RPC 协议

分布式实现可以扩展提交 `f4ca355e` 引入的 RPC 框架，但 request 必须明确表达 segment
ID list 语义。

Merge request 包含：

```text
execution_id
attempt_id
tablet_id
group_index
input_rowset_meta
input_segment_positions
input_physical_segment_ids
output_rowset_id
output_segment_slot.start_id
output_segment_slot.capacity
storage_resource
version
merge_way_num
is_mow
deadline
```

Merge response 包含：

```text
status
execution_id
attempt_id
group_index
有序的 output physical segment IDs
每个 output segment 的 row 数
每个 output segment 的 key bounds
data/index/total size
per-segment file sizes
inverted-index file information
statistics
需要时返回 RowIdConversion shard 引用
checksums
```

Coordinator 只接受 `execution_id`、`group_index` 和 `attempt_id` 与当前 plan 完全一致的
response。

### 11.3 工作节点两阶段执行

参考 `f4ca355e`，distributed worker 使用两个阶段：

1. Merge 到 worker 本地 staging 文件。
2. Merge 成功后，使用共享 final rowset ID 和分配的物理 segment ID 上传文件。

预先分配 slot 后，coordinator 不再需要等待输出数量，再通过物理 ID prefix sum 分配
连续 ID。但最终 segment position 和 RowIdConversion rebasing 仍需要 position prefix
sum。

### 11.4 Retry

第一版中最安全的方式是为 retry attempt 分配新 slot：

```text
group 1, attempt 0: [117, 217)
group 1, attempt 1: [317, 417)
```

最终 segment ID list 只包含被接受 attempt 的物理 ID。旧 attempt 文件进入 orphan
清理流程。

只有 coordinator 能证明上一个 attempt 已完全停止且文件已经删除时，才允许复用同一
slot。第一版不依赖此保证。

### 11.5 协调节点故障

第一版中，coordinator 进程退出会中止当前 distributed compaction：

- Worker task 在 deadline 到期后停止。
- 不提交 final rowset metadata。
- Pending 文件由基于 execution ID 的 GC 回收。

恢复执行中的 distributed compaction 不在第一版范围内。

## 12. 最终 metadata 汇总

所有 task 成功后，coordinator 按 `group_index` 遍历 result。

每个 result 必须校验：

- Result 属于当前 execution 和已接受的 attempt。
- 所有物理 ID 都位于该 attempt 分配的 slot 中。
- 不同 result 之间不存在重复物理 ID。
- 除明确的 local-only staging rowset 外，
  `partial_rowset->rowset_id()` 等于共享 final rowset ID。
- 每个 position-aligned metadata 数组均满足已有 rowset metadata 的完整或缺省约束。
- Output segment ID 数量等于 `partial_rowset->num_segments()`。

跳过空 group，并按顺序拼接每个非空 group 的 metadata：

```text
final_segment_ids
final_num_segment_rows
final_segments_key_bounds
final_segments_file_size
final_inverted_index_file_info
final_segment_group_sizes
```

同时汇总：

- output rows
- data size
- index size
- total size
- merged rows 和 filtered rows
- local bytes read 和 remote bytes read
- cache statistics 和 read time

调用 `manual_build()` 前检查：

```text
segment_ids.size() == num_segments
num_segment_rows.size() == num_segments，存在该字段时
segments_key_bounds.size() == num_segments，存在该字段时
segments_file_size.size() == num_segments，存在该字段时
inverted_index_file_info 符合对应索引格式的 position 对齐约束
sum(segment_group_sizes) == num_segments
```

Final writer 构建唯一的 rowset metadata，之后由现有 compaction commit 流程发布。

## 13. RowIdConversion 与 MOW

多个并发 task 共享一个可变 `RowIdConversion` 会产生 data race，并将目标 segment
编号与任务执行顺序耦合。因此，每个 group 必须拥有独立 shard。

Source identity 包含：

```text
input rowset ID
物理 input segment ID
source row ID
```

Group shard 记录的 destination 包含：

```text
group 内 output segment position
destination row ID
```

Destination position 不能记录为：

```text
slot.start_id + local_position
```

所有 group 完成后，coordinator 计算最终 position base：

```text
group 0 base = 0
group 1 base = group 0 的 output segment 数量
group 2 base = group 0 与 group 1 的 output segment 数量之和
```

Conversion lookup 返回：

```text
final destination position =
    group position base + local destination position
```

`BaseTablet` 再通过以下调用解析物理 ID：

```cpp
output_rowset->segment(final_destination_position)->id();
```

多 BE 执行时，如果 conversion shard 很大，应保存在 worker retained state 中，或者上传
为临时 shard 文件。RPC 只返回 shard reference、size 和 checksum，不能通过 RPC
序列化无上限的 conversion map。

MOW delete bitmap 始终由 coordinator 控制：

1. Worker 使用自己的 conversion shard 计算 historical bitmap shard。
2. Coordinator 合并 historical shard。
3. 在持有 meta-service compaction delete-bitmap lock 时，由 worker 或 coordinator
   计算 incremental version window。
4. Coordinator 合并 incremental shard，并与 final rowset 一起 commit。

Worker 不能独立提交 delete bitmap。

## 14. Inverted index

现有全局 inverted-index compaction 路径假设只有一个共享 writer 和一个共享
RowIdConversion，不能直接用于独立 group writer。

每个 group 必须选择以下一种方式：

- 在自己的 merge 过程中重建 output index；或者
- 只使用 task-local input range 和 RowIdConversion shard 执行 index compaction。

Result 返回 position-aligned index file information。Coordinator 按 group 顺序将
index metadata 与 segment metadata 一起拼接。

已经完成 partial output 汇总后，不能再次执行现有的全局 index compaction。

Data file 和所有 index format 都必须使用分配给该 task 的同一个物理 segment ID。

## 15. 进度与可观测性

现有进度计算假设 group 串行执行。并发执行需要维护 per-group progress counter。

- Horizontal compaction 中，每完成一个 group 贡献一个进度单位。
- Vertical compaction 中，每个 group 上报已经完成的 column group 数量。

Coordinator 发布的总进度为：

```text
所有 group 已完成工作量 / 总计划工作量
```

该值必须单调递增。在 final metadata 和 delete bitmap 尚未完成前，不能上报完成。

Metrics 和日志包含：

- 执行模式：serial、local parallel 或 distributed parallel。
- execution ID 和 attempt ID。
- group 数量和实际并发度。
- group input position range。
- 分配的 segment ID slot。
- 实际 output segment ID 和数量。
- 每个 group 的耗时及读写字节数。
- task queue time。
- retry 和 cancellation 数量。
- slot exhaustion 数量。
- orphan cleanup 数量。

## 16. 资源控制

并发执行会同时增加 reader、writer、buffer、index builder 和 object-store request。
Planning 阶段需要申请或估算：

- compaction thread slot
- memory permit
- object-store concurrency
- file-cache write pressure
- inverted-index build resource

Distributed coordinator 还需要限制：

- outstanding RPC 数量
- 每个 worker 的 compaction slot
- retained staging rowset 数量
- retained RowIdConversion shard 数量

对于小任务，应继续串行执行，因为调度和 finalization 开销可能超过 merge 收益。

## 17. 文件清理责任

多个 task 共享 rowset ID 后，清理操作必须基于精确的 task 文件列表。失败 task 绝不能
删除共享 rowset ID 前缀下的全部文件。

每个 attempt 记录：

```text
execution_id
group_index
attempt_id
physical segment IDs
data file paths
index file paths
staging paths
```

清理规则：

- Task 失败：只删除该 attempt 的文件。
- Retry 成功：将被替代 attempt 的文件加入清理队列。
- Compaction 失败：coordinator 汇总并删除所有 attempt 文件。
- Coordinator 失败：TTL orphan GC 根据 execution/attempt ownership 回收文件。
- Commit 成功：清理本地 staging 数据和所有未被接受的 attempt。

Attempt-level cleanup 永远不能删除 final rowset 已接受的文件。

## 18. 启用条件与回退

只有满足以下条件时才考虑 parallel grouped compaction：

- 已通过现有 single-rowset grouped-compaction eligibility。
- 至少存在两个 group。
- 配置并发度大于一。
- 当前执行模式所需的 writer、index 和 MOW 能力均已实现。

只有在尚未启动任务时允许回退：

```text
distributed 不可用 -> local parallel
local resource 不可用 -> serial
```

任一 task 启动后，失败必须中止当前 compaction，不能切换执行模式。

建议提供以下模式：

```cpp
enum class SingleRowsetCompactionMode {
    SERIAL,
    LOCAL_PARALLEL,
    DISTRIBUTED_PARALLEL,
    AUTO,
};
```

`AUTO` 根据 group 数量、input size、本地 permit 和可用 worker 选择模式。

## 19. 实施计划

### 阶段一：公共任务隔离

- 从串行 grouped loop 中抽取 `execute_group_merge()`。
- 引入 task-local writer 和 statistics。
- 增加显式 partial-output-writer 意图。
- 增加 segment slot 分配和校验。
- 使用 segment ID list 汇总 position-aligned metadata。
- 暂时将 concurrency 设为一，验证与现有串行结果一致。

主要涉及：

- `be/src/cloud/cloud_cumulative_compaction.cpp`
- `be/src/cloud/cloud_cumulative_compaction.h`
- `be/src/storage/rowset/rowset_writer_context.h`
- `be/src/storage/rowset/beta_rowset_writer.*`
- `be/src/storage/rowset/vertical_beta_rowset_writer.*`

### 阶段二：单 BE 并发

- 增加共享 subgroup execution pool 和 concurrent token。
- 为每个 group 提交独立 task。
- 增加 cancellation、进度汇总和精确文件清理。
- 同时支持 horizontal 和 vertical merge。
- Feature 默认关闭。

### 阶段三：Index 支持

- 按 group 执行 index build 或 index compaction。
- 汇总所有 position-aligned index metadata。
- 增加 V1/V2 index file 覆盖。

### 阶段四：本地 MOW 支持

- 为每个 group 增加 RowIdConversion shard。
- 增加最终 position-base composition。
- 复用 coordinator delete bitmap commit 路径。

### 阶段五：多 BE Executor

- 扩展 `f4ca355e` 中的 distributed RPC 和 worker registry。
- 同时发送显式 position list 和 physical ID list。
- 增加 local merge 与 final-name upload 两阶段流程。
- 增加 attempt slot、deadline、cancellation 和 orphan GC。

### 阶段六：Distributed MOW

- 保留或上传 conversion shard。
- 实现 historical 和 incremental bitmap shard 计算。
- 只允许 coordinator 合并 shard。

### 阶段七：灰度启用

- 增加 metrics 和 debug point。
- 在指定测试环境启用。
- 对比 serial 与 parallel 的输出、内存、延迟和 object-store request rate。
- 完成失败路径与 MOW 覆盖后，再扩大默认启用范围。

## 20. 测试计划

### 20.1 单元测试

- 使用非零 base ID 分配 segment slot。
- 校验 slot 不重叠以及整数溢出。
- Writer 达到 slot capacity 时返回 `TOO_MANY_SEGMENTS`。
- 第一个 group 从物理 ID 零开始时仍具有 partial writer 行为。
- Task 乱序完成时仍按 `group_index` 汇总。
- 空 group 不进入 `segment_group_sizes`。
- 物理 ID 不作为 metadata 数组下标。
- 使用 `[17, 18, 117, 217, 218]` 等 segment ID list 时，metadata 数组保持对齐。
- 单个和多个非空 group 的最终 overlap 属性正确。
- RowIdConversion local position 按最终 position prefix rebase。
- Delete bitmap 使用由最终 position 解析得到的物理 ID。
- Task cleanup 只删除自己创建的文件。

### 20.2 Cloud compaction 测试

- Horizontal 单 BE 并发结果与串行结果一致。
- Vertical 单 BE 并发结果与串行结果一致。
- Input rowset 使用非零、不连续物理 segment ID。
- Output group 的物理 segment ID slot 之间存在较大间隔。
- 重复 grouped compaction 能正确减少 group 数量。
- Compaction 前后 inverted-index query 结果一致。
- MOW initial 和 incremental delete bitmap 路径保持可见数据一致。
- 注入 merge、upload、index 和 finalization 失败后，不存在已提交的 partial rowset。
- Concurrency 为一时与隔离后的串行实现一致。

### 20.3 分布式测试

- Group 在不同 worker BE 上执行，并按 coordinator 顺序汇总。
- Worker response 到达时，对应 attempt 已被替代。
- Retry 使用新 slot，旧 attempt 不可见。
- Worker timeout 和 coordinator cancellation 能释放 retained state。
- Coordinator abort 后只留下可被回收的 pending object。
- Distributed MOW 能合并所有 bitmap shard。

### 20.4 压力与诊断测试

- ASAN 并发 compaction 压力测试。
- 在环境允许时执行 ThreadSanitizer 或同类 data-race 检查。
- 大量 group 下并发度保持有界。
- Memory-limit cancellation。
- Progress 单调递增。
- Slot exhaustion 和 orphan GC debug-point 覆盖。

## 21. 最终设计结论

1. 单 BE 和多 BE 模式共用同一套 task、result 和 finalization 实现。
2. 所有被接受的 output file 属于同一个 final output rowset ID。
3. 每个并发 task 拥有互不重叠的物理 segment ID slot。
4. Slot capacity 是硬配额，数值 100 只作为配置示例。
5. 最终 segment 顺序由 `segment_ids` 表达，不能通过物理 ID 算术推导。
6. RowIdConversion destination 始终保存 segment position。
7. Partial writer 不得 prepare、commit 或发布 rowset。
8. 只有 coordinator 能构建并提交 final rowset metadata 和 delete bitmap。
9. 清理范围由 execution、group、attempt 和精确文件列表共同确定。
10. 只有任务开始前允许回退；产生 partial task 后发生失败必须中止当前 compaction。
