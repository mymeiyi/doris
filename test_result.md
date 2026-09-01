# Cloud 并行 Base Compaction 真实集群测试记录

## 1. 结果摘要

测试覆盖 544.44 MiB 和 10.63 GiB 两种规模的 DUP BIGINT Base Compaction，以及
11.85 GiB、带 10 个倒排索引的日志表模型。每组至少执行 3 轮。

| 输入规模 | 状态轮询 | key ranges | 串行耗时中位数 | 并行耗时中位数 | 配对加速比中位数 | 串行吞吐 | 并行端到端吞吐 | 并行扣前置吞吐 | 并行 worker 吞吐 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 544.44 MiB | 200 ms | 9 | 5,302 ms | 1,450 ms | 3.657x | 102.69 MiB/s | 375.47 MiB/s | 376.68 MiB/s | 499.73 MiB/s |
| 544.44 MiB | 10 ms | 9 | 4,795 ms | 1,323 ms | 3.732x | 113.54 MiB/s | 411.52 MiB/s | 413.13 MiB/s | 520.24 MiB/s |
| 10.63 GiB | 200 ms | 22 | 108,146 ms | 9,770 ms | 10.281x | 100.71 MiB/s | 1,114.75 MiB/s | 1,116.33 MiB/s | 1,186.40 MiB/s |
| 10.63 GiB | 10 ms | 22 | 108,146 ms¹ | 8,809 ms | 12.182x¹ | 100.71 MiB/s¹ | 1,236.36 MiB/s | 1,238.12 MiB/s | 1,304.64 MiB/s |
| 11.85 GiB（日志表） | 200 ms | 24 | 205,470 ms | 28,268 ms | 7.262x | 59.07 MiB/s | 429.35 MiB/s | 429.62 MiB/s | 440.34 MiB/s |

¹ 复用 200 ms 组的三轮串行基线；状态轮询只用于分布式 coordinator，串行路径不读取该配置。

主要结论：

- 544.44 MiB 下并行 Base Compaction 加速约 3.7 倍；10.63 GiB 下提高到 10.281 倍，
  说明小规模测试显著放大了协调和单 task 固定开销。
- 9 个 range 是 9 个并行任务，不是 9 个独立节点，因此不能预期获得 9 倍加速。
- 将状态轮询间隔从 200 ms 降至 10 ms 后，worker 完成检测延迟中位数从 145.24 ms
  降至 4.65 ms，符合预期。
- 544.44 MiB、10 ms 配置下并行端到端吞吐提高 9.60%，但以同轮串行结果归一化后，
  加速比只提高 2.05%；不能把全部差异归因于轮询。
- CPU、内存、本地盘和网络均未饱和。当前主要耗时来自 worker merge、S3 写出及
  coordinator 汇总和提交。
- 10.63 GiB 测试实际产生 22 个 range，由 3 个 worker 按 8/7/7 分配；端到端吞吐中位数
  在 200 ms 和 10 ms 下分别为 1,114.75 MiB/s 和 1,236.36 MiB/s。
- 10.63 GiB、10 ms 组的总耗时比 200 ms 少 961 ms，但 worker 关键路径本身少了约
  832 ms；轮询可解释的是完成检测延迟减少约 119 ms，不能把约 10% 的吞吐差异全算作
  轮询收益。
- 日志表模型中，索引占输入大小 65.86%；并行 Base 的中位耗时从 205.47 秒降到
  28.27 秒，加速 7.262 倍。24 个 task 在 3 个 BE 上按 8/8/8 分配，倒排索引随 range
  一起在 worker 间并行构建。
- 所有测试完成后均已恢复临时配置。

## 2. 测试环境

### 2.1 基本信息

- 开始时间：2026-08-28 15:50:53 +08:00
- 控制节点：`root@172.20.49.6`
- FE/MySQL：`172.20.49.1:9030`，用户 `root`
- 测试数据库：`parallel_base_perf_20260828`
- 测试方案：[test.md](./test.md)
- Doris BE 版本：`doris-0.0.0-d5c96b376f6`

### 2.2 BE 集群

3 个 BE 均存活，属于同一 `default_compute_group`（ID `IMnQDzlK`）。每个 BE 配置
16 CPU、61.42 GiB 内存和 3 块约 295 GiB NVMe 数据盘，文件缓存已开启。

| BE | Backend ID | HTTP/BRPC | 数据盘可用空间 | 初始负载 |
| --- | ---: | --- | --- | --- |
| 172.20.49.5 | 1784275093369 | 8040/8060 | 80～88 GiB/盘 | 0.33/0.44/0.45 |
| 172.20.49.6 | 1784275093368 | 8040/8060 | 68～70 GiB/盘 | 0.33/0.42/0.52 |
| 172.20.49.7 | 1786696642381 | 8040/8060 | 87～88 GiB/盘 | 0.12/0.30/0.39 |

测试开始时数据盘使用率为 69%～76%。本次测试不清空文件缓存，冷热状态作为环境波动处理。

### 2.3 初始 BE 配置

以下配置在 3 个 BE 上一致：

| 配置 | 初始值 |
| --- | ---: |
| `enable_cloud_distributed_base_compaction` | true |
| `cloud_distributed_base_compaction_target_range_input_size_bytes` | 536870912（512 MiB） |
| `cloud_distributed_base_compaction_samples_per_range` | 256 |
| `cloud_distributed_compaction_status_poll_interval_ms` | 200 |
| `enable_cloud_distributed_compaction_peer_read` | true |
| `cloud_distributed_compaction_worker_thread_num` | 32 |
| `cloud_distributed_compaction_worker_queue_size` | 4096 |
| `enable_vertical_compaction` | true |
| `disable_auto_compaction` | false |

## 3. 测试方法

### 3.1 测试数据

| 用例 | 行数 | Base 输入 | target | 实际并行度 |
| --- | ---: | --- | ---: | ---: |
| 冒烟 | 4,000,000 | 4 rowsets / 4 segments，570,882,922 B（544.44 MiB） | 64 MiB | 9 ranges / 3 workers |
| 大规模 | 80,000,000 | 16 rowsets / 16 segments，11,420,182,465 B（10.63 GiB） | 512 MiB | 22 ranges / 3 workers |

每轮使用尚未执行 Base Compaction 的独立串行/并行 tablet。两侧使用同构表、相同造数 SQL
和相同输入布局。触发接口只表示提交成功，最终耗时以 compaction profile 的
`cost_time_ms` 为准。两种规模的 200 ms 基线执行顺序均为 S→P、P→S、S→P。

### 3.2 吞吐口径

并行 Compaction 同时计算三种吞吐：

| 指标 | 计算公式 | 包含的阶段 |
| --- | --- | --- |
| 端到端吞吐 | 输入总大小 / `cost_time_ms` | 完整 Compaction 关键路径 |
| 扣前置吞吐 | 输入总大小 /（`cost_time_ms` - `prepare_time_us` / 1000） | worker 执行、状态轮询、结果汇总和最终提交 |
| worker 吞吐 | 输入总大小 /（最晚 `worker_finish` - 最早 `worker_arrival`） | worker 排队、读取、merge、S3 写出 |

“扣前置吞吐”只排除并行路径独有的 range 准备、worker 发现、task 构造和派发，保留串行
路径同样需要的结果汇总与最终提交。日志中的 `prepare_time_us` 已包含
`worker_discovery_time_us`、`task_build_time_us` 和 `submit_rpc_time_us`，不能重复扣除。

worker 吞吐进一步排除 coordinator 的完成轮询、结果汇总和最终提交，仅作为 worker
临界区诊断指标。它使用所有 task 的并行关键路径，不是 task 耗时之和；也不使用只覆盖
merge、未覆盖 S3 写出的 `parallel_merge_time_us`。

表中的“加速比”和“耗时下降”均先按轮次配对计算，再取三轮中位数。

### 3.3 正确性验证

544.44 MiB 的 200 ms 和 10 ms 两组共 6 轮，串行与并行摘要均完全一致：

```text
COUNT(*)          = 4000000
COUNT(k)          = 4000000
MIN(k)            = 0
MAX(k)            = 99999（字符串 MIN/MAX，仅用于配对一致性）
SUM(v)            = 7999998000000
内容 fingerprint = 8587802994972903
```

10.63 GiB 的 3 轮串行、3 轮 200 ms 并行和 3 轮 10 ms 并行摘要也完全一致：

```text
COUNT(*)          = 80000000
COUNT(k)          = 80000000
MIN(k)            = 0
MAX(k)            = 999999（字符串 MIN/MAX，仅用于配对一致性）
SUM(v)            = 3199999960000000
内容 fingerprint = 171813320618842042
```

串行输出均为 1 个 segment；544.44 MiB 和 10.63 GiB 的并行输出分别为 9 个和 22 个
`NONOVERLAPPING` segment。所有 profile 均满足：

- 串行：`is_distributed=false`
- 并行：`is_distributed=true`、`distributed_task_count` 与实际 range 数一致、
  `distributed_worker_count=3`

## 4. 数据准备

### 4.1 规模校准

使用 `BIGINT + VARCHAR(512) + BIGINT` DUP 表写入 100 万行。payload 为 8 个不同 MD5
拼接，共 256 字节/行。逻辑 payload 为 244.14 MiB，实际 rowset 为 136.08 MiB，约
142.7 B/行。因此冒烟用例使用 400 万行，实际输入 544.44 MiB；大规模用例使用 8,000 万行，
实际输入 10.63 GiB。

### 4.2 准备过程中的问题与修正

| 问题 | 原因 | 修正 |
| --- | --- | --- |
| 200 万行测试最终只剩一个 `[2-9]` rowset | 每次 cumulative 输出约 35 MiB，低于 BE 启动时生效的 100 MiB promotion threshold | 增加数据量，使每次 cumulative 输出超过 100 MiB |
| 手工 cumulative 返回 `candidate rowsets empty` | 云端自动调度器已抢先完成合并 | 造数窗口临时全局关闭自动 Compaction，只执行手工 cumulative |
| 10.63 GiB 首次触发 Base 返回 `#rowsets=0` | 4 个 cumulative rowset 各约 2.66 GiB，被 DUP 的 1 GiB 前置大文件过滤规则全部排除 | 拆为 16 个约 680 MiB 的 cumulative rowset |
| 集群无法直接 clone 母表 | 未配置 Backup/Restore repository；`CREATE TABLE LIKE` 只复制结构 | 保留一个 16-rowset 母表，目标表按 `v` 区间执行 32 次 `INSERT SELECT` 和 16 次 cumulative |

544.44 MiB 有效测试表在 Base Compaction 前包含以下 4 个输入 rowset：

```text
[2-3] 约 136.10 MiB
[4-5] 约 136.12 MiB
[6-7] 约 136.11 MiB
[8-9] 约 136.10 MiB
```

10.63 GiB 有效测试表包含 16 个约 680 MiB 的输入 rowset，版本范围从 `[2-3]` 到
`[32-33]`，每个 rowset 均为 1 个 segment。母表只复用逻辑数据内容；每个串行/并行目标
仍是独立 tablet，并生成独立的新 rowset。

失败表未计入性能结果。

### 4.3 临时配置

| 配置 | 初始值 | 造数/测试值 | 最终值 |
| --- | ---: | ---: | ---: |
| `cumulative_compaction_min_deltas` | 5 | 2 | 5 |
| `base_compaction_min_rowset_num` | 5 | 2 | 5 |
| `compaction_promotion_min_size_mbytes` | 100 | 0 | 100 |
| `disable_auto_compaction` | false | true（仅造数窗口） | false |
| `enable_cloud_distributed_base_compaction` | true | 串行 false / 并行 true | true |
| `cloud_distributed_base_compaction_target_range_input_size_bytes` | 512 MiB | 64 MiB | 512 MiB |
| `cloud_distributed_compaction_status_poll_interval_ms` | 200 ms | 200 ms / 10 ms | 200 ms |

`compaction_promotion_min_size_mbytes` 的运行时修改不会改变已经构造的 policy 参数，因此
实际仍按启动时的 100 MiB threshold 设计数据规模。

## 5. 200 ms 轮询基线

### 5.1 耗时和加速

| 轮次 | 执行顺序 | 串行耗时 | 并行耗时 | 加速比 | 耗时下降 |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | S→P | 4,922 ms | 1,696 ms | 2.902x | 65.54% |
| 2 | P→S | 5,302 ms | 1,450 ms | 3.657x | 72.65% |
| 3 | S→P | 5,412 ms | 1,443 ms | 3.751x | 73.34% |
| **中位数** | — | **5,302 ms** | **1,450 ms** | **3.657x** | **72.65%** |

### 5.2 吞吐

| 轮次 | 串行吞吐 | 并行端到端吞吐 | 并行扣前置吞吐 | 并行 worker 吞吐 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 110.61 MiB/s | 321.01 MiB/s | 324.31 MiB/s | 444.43 MiB/s |
| 2 | 102.69 MiB/s | 375.47 MiB/s | 376.68 MiB/s | 499.73 MiB/s |
| 3 | 100.60 MiB/s | 377.29 MiB/s | 378.64 MiB/s | 512.86 MiB/s |
| **中位数** | **102.69 MiB/s** | **375.47 MiB/s** | **376.68 MiB/s** | **499.73 MiB/s** |

### 5.3 为什么 9 个 range 没有获得 9 倍加速

以第 1 轮并行任务（16:01:02～16:01:04）为例：

| 指标 | 9 个 task 合计 | 单 task 最大值 |
| --- | ---: | ---: |
| CPU time | 5.741 s | 0.703 s |
| merge time | 6.887 s | 1.010 s |
| remote output write | 1.929 s | 0.313 s |
| local read | 0.094 s | — |
| peer read | 0.718 s | 0.160 s |
| task elapsed | 8.818 s | 1.220 s |

每个 BE 分配 3 个 task，但每个 task 仍以单线程 merge 为主。按最长 task 的 1.220 s
临界区估算，任务平均只使用约 4.7 个 CPU 核，占集群 48 核约 10%。并行 profile 总耗时
1.696 s，比最长 task 多约 0.48 s，差值来自 range 准备、worker 发现/派发、状态轮询、
结果汇总和提交。

因此，9 个 range 只表示可并行工作单元数。实际加速由 worker 数量、单 task 执行效率、
任务均衡程度和 coordinator 固定开销共同决定。

### 5.4 资源观测

Grafana/Prometheus 使用 15:59:30～16:02:30 窗口和 1 分钟 rate 平滑：

| 指标 | 172.20.49.5 | 172.20.49.6 | 172.20.49.7 |
| --- | ---: | ---: | ---: |
| BE CPU 峰值 | 8.24% | 8.50% | 7.25% |
| 去除 iowait 后 CPU 峰值 | 7.60% | 8.45% | 6.81% |
| BE allocated memory 峰值 | 7.89 GiB | 6.11 GiB | 5.23 GiB |
| S3 写峰值 | 59.31 MiB/s | 40.33 MiB/s | 28.23 MiB/s |
| eth0 发送峰值 | 65.49 MiB/s | 45.33 MiB/s | 54.00 MiB/s |

云存储路径的本地盘 IO time 指标为 0，peer read 峰值约 4.5 MiB/s/BE。没有证据表明
CPU、内存、本地盘或网络达到饱和；当前限制来自并行度、单 task merge/S3 写出以及
coordinator 固定开销。

## 6. 10 ms 轮询对照

除状态轮询间隔从 200 ms 调整为 10 ms 外，其余参数与基线完全一致。

### 6.1 耗时和加速

| 轮次 | 执行顺序 | 串行耗时 | 并行耗时 | 加速比 | 耗时下降 |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | S→P | 4,795 ms | 1,323 ms | 3.624x | 72.41% |
| 2 | P→S | 4,770 ms | 1,278 ms | 3.732x | 73.21% |
| 3 | S→P | 5,276 ms | 1,331 ms | 3.964x | 74.77% |
| **中位数** | — | **4,795 ms** | **1,323 ms** | **3.732x** | **73.21%** |

### 6.2 吞吐

| 轮次 | 串行吞吐 | 并行端到端吞吐 | 并行扣前置吞吐 | 并行 worker 吞吐 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 113.54 MiB/s | 411.52 MiB/s | 413.13 MiB/s | 498.35 MiB/s |
| 2 | 114.14 MiB/s | 426.01 MiB/s | 430.27 MiB/s | 520.24 MiB/s |
| 3 | 103.19 MiB/s | 409.04 MiB/s | 410.59 MiB/s | 525.54 MiB/s |
| **中位数** | **113.54 MiB/s** | **411.52 MiB/s** | **413.13 MiB/s** | **520.24 MiB/s** |

### 6.3 与 200 ms 的直接对比

| 指标（三轮中位数） | 200 ms | 10 ms | 变化 |
| --- | ---: | ---: | ---: |
| 并行 profile 耗时 | 1,450 ms | 1,323 ms | -127 ms（-8.76%） |
| 并行 worker 关键路径 | 1,089.46 ms | 1,046.52 ms | -42.94 ms（-3.94%） |
| 并行端到端吞吐 | 375.47 MiB/s | 411.52 MiB/s | +9.60% |
| 并行扣前置吞吐 | 376.68 MiB/s | 413.13 MiB/s | +9.68% |
| 并行 worker 吞吐 | 499.73 MiB/s | 520.24 MiB/s | +4.10% |
| 串行 profile 耗时 | 5,302 ms | 4,795 ms | -507 ms（环境波动） |
| 串并行配对加速比 | 3.657x | 3.732x | +2.05% |
| 最慢 worker task | 1,085.26 ms | 1,034.36 ms | -50.90 ms |
| worker 完成到 coordinator 发现 | 145.24 ms | 4.65 ms | -140.59 ms |
| coordinator 耗时减最慢 task | 386.01 ms | 243.65 ms | -142.36 ms |
| range 准备 | 5.14 ms | 5.17 ms | 基本不变 |

### 6.4 结果解读

1. **轮询优化符合预期。** 200 ms 三轮的完成检测延迟为
   182.42/116.96/145.24 ms，10 ms 三轮为 4.65/8.40/2.79 ms。检测延迟中位数减少
   140.59 ms，与并行总耗时减少 127 ms 的量级一致。
2. **worker 吞吐变化不是轮询收益。** 状态轮询发生在 coordinator，不会直接加速
   worker。worker 吞吐提高 4.10% 应视为任务执行和 S3 写出的正常波动。
3. **端到端收益需要串行基线校正。** 10 ms 组的串行耗时也下降 9.56%。虽然并行端到端
   吞吐提高 9.60%，配对加速比只从 3.657x 提高至 3.732x，增幅约 2.05%。
4. **仍有不可由轮询消除的开销。** 10 ms 下 coordinator 仍比最慢 task 多约 244 ms，
   主要用于结果汇总、元数据更新和最终提交。

因此，10 ms 消除了大部分 worker 完成检测等待，但属于约百毫秒级的尾部优化，不改变
merge 和 S3 写出占主体的判断。

补充说明：

- 测试脚本最外层手工 Compaction 状态仍按 200 ms 查询，但结果使用 BE 内部 profile
  耗时，不受脚本轮询粒度影响。
- 10 ms 测试窗口内 BE CPU 峰值为 9.00%～12.12%，allocated memory 峰值为
  6.04～6.69 GiB，仍无整机资源饱和。
- 10 ms 会提高 coordinator 状态查询频率。本轮只验证单 tablet，尚不足以确定默认值；
  修改默认配置前还需要多 tablet 并发验证。

## 7. 10.63 GiB 大规模对照

该组使用默认 200 ms 状态轮询和 512 MiB range target。实际输入为 11,420,182,465 B
（10.63 GiB），包含 16 个 rowset/segment；并行实际生成 22 个 range task，3 个 worker
分别执行 8/7/7 个 task。

### 7.1 耗时和加速

| 轮次 | 执行顺序 | 串行耗时 | 并行耗时 | 加速比 | 耗时下降 |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | S→P | 99,238 ms | 9,653 ms | 10.281x | 90.27% |
| 2 | P→S | 115,920 ms | 9,770 ms | 11.865x | 91.57% |
| 3 | S→P | 108,146 ms | 11,388 ms | 9.496x | 89.47% |
| **中位数** | — | **108,146 ms** | **9,770 ms** | **10.281x** | **90.27%** |

### 7.2 吞吐

| 轮次 | 串行吞吐 | 并行端到端吞吐 | 并行扣前置吞吐 | 并行 worker 吞吐 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 109.75 MiB/s | 1,128.26 MiB/s | 1,129.33 MiB/s | 1,199.77 MiB/s |
| 2 | 93.95 MiB/s | 1,114.75 MiB/s | 1,116.33 MiB/s | 1,186.40 MiB/s |
| 3 | 100.71 MiB/s | 956.37 MiB/s | 957.19 MiB/s | 1,018.50 MiB/s |
| **中位数** | **100.71 MiB/s** | **1,114.75 MiB/s** | **1,116.33 MiB/s** | **1,186.40 MiB/s** |

### 7.3 与 544.44 MiB 基线对比

两组均使用 200 ms 状态轮询：

| 指标（三轮中位数） | 544.44 MiB | 10.63 GiB | 变化 |
| --- | ---: | ---: | ---: |
| 输入大小 | 544.44 MiB | 10,891.13 MiB | 20.00x |
| range target | 64 MiB | 512 MiB | 8.00x |
| ranges/workers | 9/3 | 22/3 | 2.44x ranges |
| 串行耗时 | 5,302 ms | 108,146 ms | 20.40x |
| 并行耗时 | 1,450 ms | 9,770 ms | 6.74x |
| 配对加速比 | 3.657x | 10.281x | 2.81x |
| 串行吞吐 | 102.69 MiB/s | 100.71 MiB/s | -1.93% |
| 并行端到端吞吐 | 375.47 MiB/s | 1,114.75 MiB/s | 2.97x |
| 并行扣前置吞吐 | 376.68 MiB/s | 1,116.33 MiB/s | 2.96x |
| 并行 worker 吞吐 | 499.73 MiB/s | 1,186.40 MiB/s | 2.37x |
| coordinator 耗时减最慢 task | 386.01 ms | 591.90 ms | 占总耗时 26.62% → 6.06% |
| range 准备 | 5.14 ms | 9.70 ms | 均可忽略 |

### 7.4 结果解读

1. **544.44 MiB 确实偏小。** 输入放大 20 倍后，串行耗时也近似增长 20 倍，串行吞吐
   基本不变；并行耗时只增长 6.74 倍，因此加速比从 3.657x 提高到 10.281x。
2. **固定协调开销被显著摊薄。** coordinator 相对最慢 task 的额外耗时绝对值从约
   386 ms 增加到 592 ms，但占并行总耗时的比例从 26.62% 降至 6.06%。
3. **更多 range 提高了 worker 利用率。** 22 个 task 均匀分配为 8/7/7；三轮有效 merge
   并行度为 20.08/20.05/17.83。worker CPU time 合计中位数为 135.12 s，在 9.18 s 的
   worker 关键路径内平均使用约 14.7 个 CPU 核，高于小规模用例。
4. **仍未达到 22 倍加速。** 22 个 range 共享 3 个 BE 的 CPU、缓存、peer read、S3 写出
   和网络资源；任务尾部差异及 coordinator 提交仍位于关键路径。

Grafana/Prometheus 使用并行任务前后各扩展 30 秒、1 分钟 rate 平滑。三轮观测到的集群
单 BE 峰值为：CPU 40.76%、allocated memory 19.16 GiB、S3 写 139.69 MiB/s、eth0 发送
308.25 MiB/s。资源使用显著高于 544.44 MiB 用例，但仍没有整机 CPU 或内存饱和证据。

### 7.5 10 ms 状态轮询对照

按要求只重跑 3 轮并行 Compaction，没有继续其他测试。串行路径不读取 distributed
coordinator 的状态轮询配置，因此复用 7.1 中相同输入布局的三轮串行基线进行配对。

| 轮次 | 串行基线 | 并行耗时 | 加速比 | 耗时下降 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 99,238 ms | 8,809 ms | 11.266x | 91.12% |
| 2 | 115,920 ms | 9,516 ms | 12.182x | 91.79% |
| 3 | 108,146 ms | 8,803 ms | 12.285x | 91.86% |
| **中位数** | **108,146 ms** | **8,809 ms** | **12.182x** | **91.79%** |

| 轮次 | 串行吞吐 | 并行端到端吞吐 | 并行扣前置吞吐 | 并行 worker 吞吐 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 109.75 MiB/s | 1,236.36 MiB/s | 1,238.12 MiB/s | 1,327.01 MiB/s |
| 2 | 93.95 MiB/s | 1,144.51 MiB/s | 1,145.81 MiB/s | 1,206.10 MiB/s |
| 3 | 100.71 MiB/s | 1,237.21 MiB/s | 1,238.81 MiB/s | 1,304.64 MiB/s |
| **中位数** | **100.71 MiB/s** | **1,236.36 MiB/s** | **1,238.12 MiB/s** | **1,304.64 MiB/s** |

### 7.6 大规模轮询结果分析

| 指标（三轮中位数） | 200 ms | 10 ms | 变化 |
| --- | ---: | ---: | ---: |
| 并行 profile 耗时 | 9,770 ms | 8,809 ms | -961 ms（-9.84%） |
| 并行 worker 关键路径 | 9,180.00 ms | 8,348.03 ms | -831.97 ms（-9.06%） |
| 并行端到端吞吐 | 1,114.75 MiB/s | 1,236.36 MiB/s | +10.91% |
| 并行扣前置吞吐 | 1,116.33 MiB/s | 1,238.12 MiB/s | +10.91% |
| 并行 worker 吞吐 | 1,186.40 MiB/s | 1,304.64 MiB/s | +9.97% |
| worker 完成到 coordinator 发现 | 122.97 ms | 3.67 ms | -119.31 ms |
| coordinator 耗时减最慢 task | 591.90 ms | 490.20 ms | -101.70 ms |
| range 准备与派发 | 9.70 ms | 11.39 ms | +1.69 ms |

10 ms 轮询的完成检测延迟为 3.67 ms，符合配置预期；但并行总耗时减少的 961 ms 中，
worker 关键路径本身减少了约 832 ms。后者不受 coordinator 轮询控制，应视为缓存、S3
和任务执行的轮次波动。因此，不能把本组约 10% 的端到端吞吐提升全部归因于轮询调整。

“扣前置吞吐”只比端到端吞吐高 0.14%，说明 range 准备、worker 发现和派发在 10.63 GiB
输入下可以忽略。它仍保留轮询、结果汇总和最终提交；这些阶段被 worker 吞吐排除，所以
worker 吞吐只用于定位 worker 侧效率，不能代替该口径。

## 8. BIGINT 测试配置恢复

测试完成后已逐节点确认以下配置恢复：

- 状态轮询：200 ms
- 自动 Compaction：开启
- cumulative 最小 deltas：5
- Base 最小 rowset 数：5
- promotion 最小值：100 MiB
- range target：512 MiB
- 并行 Base Compaction：开启

前述测试过程和失败尝试均保留在专用数据库 `parallel_base_perf_20260828`，未修改或删除
其他数据库中的表。

## 9. 日志表模型测试

### 9.1 表模型和输入布局

测试使用用户提供的 `ali_virginia_prod_06` 模型，保留以下关键特征：

- `DUPLICATE KEY(_ctime_, app)`，其中 `_ctime_` 为 `DATETIME(6) NOT NULL`，`app` 为
  nullable `VARCHAR`
- 10 个字符串列、10 个倒排索引，其中 `custom_label` 和 `msg` 使用 unicode parser
- 小时级自动分区、random distribution、time-series compaction policy
- `time_series_compaction_goal_size_mbytes=2048`、inverted index V2

按用户要求把 bucket 数从 36 改为 1，使约 12 GiB 输入集中到一个 tablet。原 DDL 的
`enable_single_replica_compaction=true` 不被当前 Cloud 集群支持，建表时移除；其他与
Base Compaction 相关的属性保持不变。

| 项目 | 值 |
| --- | ---: |
| 测试数据库 | `parallel_base_my_table_20260831` |
| 数据范围 | 单个小时分区，20,000,000 行 |
| nullable 分布 | `app` 1,000,000 个 NULL（5%） |
| Base 输入 | 20 rowsets / 20 segments |
| 输入 data | 4,345,123,645 B（4.05 GiB） |
| 输入 index | 8,381,178,459 B（7.81 GiB，占 65.86%） |
| 输入总大小 | 12,726,302,104 B（11.85 GiB） |
| range target | 512 MiB |
| 实际并行度 | 24 ranges / 3 workers，8/8/8 分配 |
| 状态轮询 | 200 ms |

为构造不会被 DUP 1 GiB 大 rowset 规则过滤的稳定输入，造数阶段临时将该表的 time-series
goal 调为 512 MiB，每两个 500,000-row load 合成一个约 607 MiB rowset；正式 Base 前恢复
为原值 2048 MiB。该准备过程不计入 Base 耗时。

### 9.2 三轮耗时和吞吐

三轮执行顺序为 S→P、P→S、S→P：

| 轮次 | 串行耗时 | 并行耗时 | 配对加速比 | 耗时下降 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 205,470 ms | 44,058 ms | 4.664x | 78.56% |
| 2 | 236,080 ms | 27,573 ms | 8.562x | 88.32% |
| 3 | 205,293 ms | 28,268 ms | 7.262x | 86.23% |
| **中位数** | **205,470 ms** | **28,268 ms** | **7.262x** | **86.23%** |

| 轮次 | 串行吞吐 | 并行端到端吞吐 | 并行扣前置吞吐 | 并行 worker 吞吐 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 59.07 MiB/s | 275.47 MiB/s | 276.73 MiB/s | 282.70 MiB/s |
| 2 | 51.41 MiB/s | 440.17 MiB/s | 440.40 MiB/s | 453.59 MiB/s |
| 3 | 59.12 MiB/s | 429.35 MiB/s | 429.62 MiB/s | 440.34 MiB/s |
| **中位数** | **59.07 MiB/s** | **429.35 MiB/s** | **429.62 MiB/s** | **440.34 MiB/s** |

### 9.3 倒排索引并行度

每个 distributed task 生成一个 range 对应的输出 segment，并在该 task 内生成该 segment
的倒排索引。因此 24 个 range 的索引工作会随 task 分散到 3 个 BE 并行执行；这不表示
单 task 内 10 个索引列一定并发，也不能理解为 240 路并行。

| 轮次 | worker 临界区 | task CPU 合计 | task merge 合计 | 有效 merge 并行度 | remote output write 合计 | 读取耗时合计 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 42.93 s | 565.81 s | 665.67 s | 15.71 | 18.84 s | 104.49 s |
| 2 | 26.76 s | 577.24 s | 581.91 s | 22.29 | 18.98 s | 8.59 s |
| 3 | 27.56 s | 571.03 s | 574.92 s | 21.34 | 24.48 s | 7.62 s |

第 2、3 轮在约 27 秒 worker 临界区内累计使用约 571～577 CPU 秒，平均约 21 个 CPU 核；
并行输出索引合计 7.90 GiB。这说明包含倒排索引构建的 merge 工作确实跨 range 并行。

首轮 worker 较慢主要伴随读取耗时合计从后两轮的 7.62～8.59 秒升至 104.49 秒，且
worker 发现耗时为 145.79 ms，后两轮仅为 0.33/1.96 ms。首轮 4.664x 与后两轮
7.262～8.562x 的差异主要来自冷热缓存和读取状态，不是 range 数变化。

### 9.4 正确性和配置恢复

三轮串行与并行结果均满足：

```text
COUNT(*)          = 20000000
COUNT(app)        = 19000000
MIN(_ctime_)      = 2026-08-31 10:00:00.000000
MAX(_ctime_)      = 2026-08-31 10:00:19.999999
SUM(LENGTH(msg))  = 5500000000
内容 fingerprint = 42955689366318517
```

串行 profile 均为 `is_distributed=false`；并行 profile 均为 `is_distributed=true`、
`distributed_task_count=24`、`distributed_worker_count=3`。测试表和校准表保留在
`parallel_base_my_table_20260831`。结束后已逐台核验 3 个 BE 的自动 Compaction、
cumulative/Base 阈值、promotion、range target、状态轮询和并行 Base 开关均恢复为初始值。
