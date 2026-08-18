# Cloud Distributed Compaction 当前设计

## 1. 目标与范围

Distributed Compaction 把一次 Cloud compaction 拆成多个独立 task，分配到同一 Compute
Group 的多个 BE。Coordinator 最终仍只提交一个 output rowset，因此不改变事务和可见性语义。

当前有两种拆分方式：

| 模式 | 入口 | task 划分 |
| --- | --- | --- |
| Single-rowset CU | 一个包含大量 overlapping segments 的 cumulative rowset | 按连续 segment position 划分 |
| Distributed Base | 输入总大小超过目标值的 Base compaction | 按主键范围划分 |

Coordinator BE 也可以作为 Worker。Worker 只生成 partial rowset，不提交 rowset，也不修改
tablet 可见状态。

## 2. 公共流程

```mermaid
flowchart LR
    A[Coordinator 生成 tasks] --> B[FE 返回同 Compute Group Workers]
    B --> C[Coordinator 优先选入<br/>tasks round-robin 分配]
    C --> D[每个 Worker 一个 batch request]
    D --> E[Worker 将 batch 内每个 task<br/>分别提交线程池并发执行]
    E --> F[Coordinator 定时批量 poll]
    F --> G[按 group index 校验并组装]
    G --> H[MOW delete bitmap]
    H --> I[Coordinator commit]
    I --> J[finalize Worker state]
```

关键不变量：

- 所有 task 共用一个 output rowset ID。
- 每个 task 使用独立 segment ID slot，避免并行写文件冲突。
- metadata 按 group index 组装，完成顺序不影响最终 segment 顺序。
- metadata 使用 segment position；文件名和 delete bitmap 使用 physical segment ID。
- `submit` 只表示 batch 已接受，最终状态由 Coordinator 主动轮询。

## 3. Task 规划

### 3.1 Single-rowset CU

候选必须使用 size-based cumulative policy，只有一个无 delete predicate 的 overlapping
rowset，存在 key column、没有 cluster key，并达到 overlap 单元阈值。

普通 overlapping rowset 按连续 segment position 分组；
`NONOVERLAPPING_WITHIN_GROUP` rowset 按已有 segment groups 再分组。无法选择至少两个 Worker
时，回退到 Coordinator 本地 grouped compaction。

### 3.2 Distributed Base

请求范围数为：

```text
requested_ranges = ceil(input_total_size / target_input_size)
```

范围数不受 BE 数限制；最终 task 数可以大于 Worker 数。低基数 key 可能产生更少的实际范围。

边界规划使用全局加权采样：样本数按 segment 行数分配，每个样本的 weight 代表被跳过的行。

- 非 MOW 优先读取 short-key index。
  - 编码无损：直接选择加权分位点，只读取最终边界的 typed key。
  - 最后一个 short-key 字符串被截断：只读取命中分位点的冲突组和相邻组，再选择 typed
    boundary。
  - 更早的 short-key 列被截断、编码边界不足或 key 不支持时，回退到 typed sample。
- MOW 且无 cluster key、全部 key 列受支持且非 nullable：按 Primary Key Index ordinal
  采样完整编码 key，移除 sequence suffix 后选择边界，只读取最终 typed key。
- 在 Distributed Base 已允许的 MOW 场景中，其他 key schema 保留 typed sample 路径。

Distributed Base 支持 DUP、AGG、MOR Unique，以及满足上述约束的 MOW Unique；row-binlog
tablet 和开启部分 MOW correctness check 的场景保持本地执行。由于 partial writer 不能复用
Coordinator 本地的 inverted index 文件，Distributed Base 会关闭 inverted index compaction。

## 4. 调度与线程模型

- FE Worker 列表按 job 稳定打散，Coordinator 优先保留，最多选择 `min(tasks, Workers)` 个
  BE。
- task 按 group index round-robin 分配；一个 request 可以携带同一 Worker 的多个 task。
- Worker 收到 batch 后，为每个 task 单独提交 `DistributedCompactionWorkerThreadPool`；同一
  Worker 的多个 task 可以并发，实际并发度受线程池大小限制。
- Coordinator 使用独立 RPC 线程池并发执行 submit、status、incremental bitmap 和 finalize。
- 所有 job 共享 Poll Scheduler timer；timer 只触发 poll，不执行 RPC。
- Single-rowset CU 和 Distributed Base 在远端执行期间都会 suspend，完成后重新进入对应的
  compaction 线程继续 assemble 和 commit。

## 5. Partial Rowset 与 MOW

每个 task 的 writer 使用：

```text
同一个 output rowset ID
独立 segment ID slot
is_partial_output_writer = true
allow_packed_file = false
```

Coordinator 校验 rowset ID、slot 边界、显式 segment ID、row count、key bounds、file size 和
index metadata，再拼成 final rowset。多个非空 task 的布局为
`NONOVERLAPPING_WITHIN_GROUP`，并记录 `segment_group_sizes`。

MOW Worker 为自己的输入范围保留 `RowIdConversion`，先返回历史 delete bitmap shard。
Coordinator assemble 后获取 delete bitmap lock；如果 tablet 版本前进，再请求各 Worker 计算
incremental shard，合并后由 Coordinator 更新 Meta Service 并提交。

## 6. File Cache 与 Peer Read

- request 显式携带 `is_coordinator`。
- 远端 Worker 强制 `disable_file_cache=true`，不缓存 partial output。
- Coordinator Worker 复用普通 Cloud CU/Base compaction 的 output cache 策略。
- 开启 `enable_cloud_distributed_compaction_peer_read` 后，远端 Worker 的 cache miss 优先从
  Coordinator BE peer read；不可用时仍可回到对象存储读取。

## 7. 失败与清理

- Worker 发现失败、task 少于 2 或可用 Worker 少于 2 时，只能在远端任务启动前回退本地。
- 任一 task 失败、超时或无法提交后，整个 batch/job 取消，不切换到本地路径。
- submit/status/finalize 等 control RPC 只做有限重试，merge task 不自动重试。
- commit 前失败会 finalize Worker state；commit 结果不确定时保留输出，避免删除可能已可见的
  rowset。
- Worker state 以 output transaction expiration 作为兜底过期时间；Coordinator 重启后暂不
  恢复未完成 job。

## 8. 观察指标

主要日志：

- `finish distributed base compaction range planning`：范围数、sample 数、short/PK fast path、
  planning local/remote/peer I/O 和各阶段耗时。
- `finish distributed compaction worker task`：每个 task 的输出行数、segment 数、大小、
  local/remote/peer read、CPU、queue、merge、output write 和总耗时。
- `finish distributed single-rowset compaction merge`：tasks、Workers、并行墙钟时间、有效并发
  度和吞吐。

比较 peer read 与对象存储性能时，重点关注 `peer_read_bytes/time_us`、
`remote_read_bytes/time_us`、`task_elapsed_time_us`、`effective_merge_parallelism` 和
`merge_throughput_mib_per_second`。

## 9. 主要配置与代码

主要配置：

- `enable_cloud_single_rowset_compaction`
- `enable_cloud_single_rowset_distributed_compaction`
- `enable_cloud_distributed_base_compaction`
- `enable_cloud_distributed_compaction_peer_read`
- `cloud_distributed_base_compaction_target_input_size_bytes`
- `cloud_distributed_base_compaction_samples_per_range`
- `cloud_distributed_compaction_worker_thread_num`
- `cloud_distributed_compaction_segment_slot_capacity`
- `cloud_distributed_compaction_status_poll_interval_ms`

主要代码：

| 模块 | 文件 |
| --- | --- |
| CU 选择、本地回退、suspend/resume 和提交 | `be/src/cloud/cloud_cumulative_compaction.cpp`、`be/src/cloud/cloud_cumulative_compaction_async.cpp` |
| Base 选择和 suspend/resume | `be/src/cloud/cloud_base_compaction.cpp` |
| Coordinator、range planner、Poll Scheduler、Worker | `be/src/cloud/cloud_distributed_compaction.cpp` |
| RPC 协议与入口 | `gensrc/proto/internal_service.proto`、`be/src/cloud/cloud_internal_service.cpp` |
| 线程池 | `be/src/cloud/cloud_storage_engine.cpp` |
