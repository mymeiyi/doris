# Cloud 并行 Base Compaction 真实集群性能测试方案

## 1. 测试目标

本方案用于回答以下问题：

1. 并行 Base Compaction 相比串行 Base Compaction 能缩短多少端到端耗时、提升多少处理吞吐。
2. 加速效果如何随输入数据量、range 数量和 BE 数量变化。
3. DUP、AGG、UNIQUE MOR、UNIQUE MOW 等表模型是否都能正确进入并行路径并获得收益。
4. 不同 key 类型、nullable/composite key、均匀与倾斜数据是否会导致 range 数减少或任务不均衡。
5. 并行执行增加了多少聚合 CPU、内存、网络和对象存储 IO，是否影响前台查询和导入。
6. 不满足并行条件时能否正确回退到本地 Base Compaction。

主要结论以 `cost_time_ms`、输入字节吞吐和串并行加速比为准；所有性能结果必须同时通过数据正确性检查。

## 2. 实现边界与预期行为

并行 Base Compaction 需要同时满足：

- 所有 BE 设置 `enable_cloud_distributed_base_compaction=true`。
- `cloud_distributed_base_compaction_target_range_input_size_bytes > 0`。
- 本次 Base Compaction 输入总大小大于 target range size。
- 同一 compute group 至少有 2 个可用 BE。
- 表至少有一个 key 列，且首个 key 列属于支持的类型。
- 不是 row binlog tablet。
- UNIQUE MOW 表没有 cluster key，且相关 correctness-check 配置未开启。

计划 range 数近似为：

```text
requested_ranges = ceil(input_rowsets_total_size /
                        cloud_distributed_base_compaction_target_range_input_size_bytes)
```

实际 range 数可能因为 key 边界重复而减少：

```text
actual_ranges = distinct_boundaries + 1
worker_count = min(live_BE_count_in_compute_group, actual_ranges)
```

因此不能仅根据 target size 判断是否真的并行，必须从 compaction profile 验证：

```text
is_distributed = true
distributed_task_count = actual_ranges
actual_ranges >= 2
distributed_worker_count >= 2
```

以下情况应作为“正确回退”而不是性能失败：输入不超过 target、实际只得到一个 range、compute group 只有一个 BE、MOW 带 cluster key、首 key 类型不支持或开启了会阻止 MOW 并行的 correctness check。

## 3. 测试环境

### 3.1 集群要求

- 使用非生产、独占 compute group；建议至少 4 个配置完全相同的 BE，扩展性测试建议准备 2/4/8 BE 三种规模。
- BE 使用相同 CPU、内存、磁盘/文件缓存容量、网络带宽和对象存储区域。
- FE、BE 和监控节点时钟同步，建议误差小于 100 ms。
- 测试期间禁止自动扩缩容、版本升级、节点重启和其他大规模导入/compaction。
- 记录 Doris commit、构建类型、机器规格、BE 数量、对象存储类型、文件缓存配置和所有动态配置。
- 单 tablet 微基准使用 `BUCKETS 1`；系统吞吐测试再使用接近生产的 bucket 数。

### 3.2 安全要求

- 先保存每个 BE 的原始配置，测试结束逐项恢复。
- 只对测试表设置 `"disable_auto_compaction"="true"`，不要关闭真实业务表的自动 compaction。
- legacy DATE/DECIMALV2、节点故障和冷缓存测试只允许在专用测试集群执行。
- 不直接清理 BE 文件缓存目录。冷缓存测试使用新 compute group、受控缓存淘汰接口或新对象数据。
- 每轮开始前确认所有 BE `Alive=true`，磁盘、对象存储、网络没有告警或限流。

### 3.3 关键配置

除被测变量外，其余配置在串行组和并行组中保持一致。

| 配置 | 建议值/策略 | 说明 |
| --- | --- | --- |
| `enable_cloud_distributed_base_compaction` | 串行 `false`，并行 `true` | 核心 A/B 开关，所有 BE 同时修改 |
| `cloud_distributed_base_compaction_target_range_input_size_bytes` | 默认 512 MiB；扩展性测试按目标 range 数计算 | 决定 requested ranges |
| `cloud_distributed_base_compaction_samples_per_range` | 默认 4096 | 主测试固定；敏感性测试再使用 512/4096/16384 |
| `enable_cloud_distributed_compaction_peer_read` | 主测试保持生产值，附加测试比较 true/false | 影响 worker 从 coordinator peer 读取缓存数据 |
| `cloud_distributed_compaction_status_poll_interval_ms` | 生产评估保持生产值 | 端到端耗时包含 polling 等待；算法微基准可另测 1000 ms |
| `cloud_distributed_compaction_worker_thread_num` | 主测试保持默认或生产值 | 并发 compaction 测试单独调整 |
| `cloud_distributed_compaction_segment_slot_capacity` | 保持默认 100 | 每个 range 输出 segment 上限，不作为常规调优项 |

MOW 正向并行用例需要确认以下配置均为 `false`，否则实现会回退到本地 compaction：

```text
enable_missing_rows_correctness_check
enable_mow_compaction_correctness_check_core
enable_mow_compaction_correctness_check_fail
enable_rowid_conversion_correctness_check
```

## 4. 指标和计算方法

### 4.1 主要性能指标

设：

- `T`：profile 中的 `cost_time_ms / 1000`，单位秒。
- `B`：profile 中的 `input_total_size`，单位字节。
- `N`：profile 中的 `input_row_num`。
- `W`：`distributed_worker_count`。

计算：

```text
字节吞吐 MiB/s = B / 1024 / 1024 / T
行吞吐 rows/s = N / T
加速比 Speedup = T_serial / T_parallel
扩展效率 = Speedup / W
耗时下降比例 = 1 - T_parallel / T_serial
```

同时记录外部触发时间到任务完成时间。外部 wall time 与 profile `cost_time_ms` 偏差超过 5% 时，应检查 polling、任务排队或采集脚本。

### 4.2 任务均衡和准备开销

从 coordinator/worker 日志按 `job_id` 汇总：

- range planning：`prepare_time_us`、`segment_load_time_us`、`short_key_index_load_time_us`、`primary_key_index_load_time_us`、`key_sample_read_time_us`、`boundary_key_read_time_us`。
- range 结果：`requested_ranges`、`actual_ranges`、`samples`、`typed_samples`、`encoded_samples`。
- fast path：`short_key_fast_path`、`primary_key_fast_path`、fallback reason、boundary refinement 统计。
- 每个 task：`output_rows`、`output_rowset_total_size`、`merge_time_us`、`task_elapsed_time_us`、`worker_queue_time_us`、`cpu_time_us`。
- IO：`local_read_bytes`、`remote_read_bytes`、`peer_read_bytes`、`remote_output_write_time_us`。

计算：

```text
任务耗时不均衡 = max(task_elapsed_time) / mean(task_elapsed_time)
任务字节不均衡 = max(output_total_size) / mean(output_total_size)
规划开销占比 = prepare_time / T_parallel
聚合 CPU 成本 = sum(task cpu_time)
```

### 4.3 资源与业务影响指标

以 1 秒粒度采集每个 BE，并同时保留集群汇总值：

- BE CPU 使用率、进程 CPU seconds、RSS/峰值内存、网络收发带宽。
- 文件缓存命中率、local/remote/peer read bytes。
- 对象存储 GET/PUT 请求量、吞吐、延迟、错误和限流次数。
- `compaction_used_permits`、`compaction_waitting_permits`、running/pending compaction task 数。
- 前台查询 QPS、P50/P95/P99 延迟、错误率。
- 导入 rows/s、bytes/s、P95 commit latency 和失败率。

并行方案的“更快”必须同时报告聚合资源成本。例如 wall time 减半但总 CPU 或远端读取翻倍，不能只报告加速比。

## 5. 测试矩阵

为避免不可执行的全量笛卡尔积，采用分层矩阵：先用代表性表完成规模和并行度测试，再固定规模覆盖模型、类型和数据分布。

### 5.1 P0：基础正确性与冒烟

| 用例 | 表模型/key | Tablet 输入 | 配置 | 预期 |
| --- | --- | ---: | --- | --- |
| P0-1 | DUP BIGINT，均匀 | 4～8 GiB | 并行关闭 | `is_distributed=false`，成功 |
| P0-2 | P0-1 的同构数据 | 4～8 GiB | 并行开启，目标 4 ranges | task=4，worker≥2，结果一致 |
| P0-3 | DUP BIGINT | 小于 target | 并行开启 | 正确回退本地 |
| P0-4 | 低基数首 key | 大于 target | 并行开启 | actual ranges 可能减少；无错误 |

P0 全部通过后再进行大规模测试。

### 5.2 P1：数据规模、range 数和 worker 扩展性

固定 DUP BIGINT、窄表、均匀 key，每组至少重复 3 次，推荐 5 次。

| 维度 | 取值 |
| --- | --- |
| 每 tablet 输入大小 | 8 GiB、32 GiB、128 GiB；容量不足时按 1:4:16 等比例缩小 |
| BE 数 | 2、4、8；无法改变集群规模时至少完成当前规模 |
| 目标 requested ranges | 2、W、2W、4W |
| 缓存状态 | 冷缓存、稳定热缓存分别统计，禁止混合 |

对于观测到的输入大小 `S` 和目标 range 数 `R`，设置：

```text
target_range_size = ceil(S / R)
```

重点输出：吞吐随数据量、task 数、worker 数的曲线；找到吞吐不再提升或开始下降的拐点。主结论使用 32 GiB 以上、`R >= 2W` 的结果，避免小任务被固定开销主导。

### 5.3 P1：表模型

每个模型使用相同目标输入大小、rowset 数、segment 数和 key 分布，分别执行串行/并行 A/B。

| 模型 | 数据特征 | 重点检查 |
| --- | --- | --- |
| DUP KEY | 50% 跨 rowset 重复 key | 通用 merge 吞吐、结果行数 |
| AGGREGATE KEY | BIGINT SUM、MAX 等聚合列，80% key 重叠 | merged rows、聚合结果、CPU |
| UNIQUE KEY MOR | 多轮覆盖相同 key | 最终版本值、filtered/merged rows |
| UNIQUE KEY MOW | 无 cluster key，多轮更新 | delete bitmap、输出正确性、额外 finalize 成本 |
| UNIQUE KEY MOW + sequence | 无 cluster key，显式 sequence 列 | PK ordinal fast path、sequence suffix 处理 |

额外建立一个 MOW + cluster key 表作为回退用例，预期 `is_distributed=false`，不纳入并行加速比。

### 5.4 P1：首 key 数据类型

类型矩阵固定使用 DUP KEY、8～32 GiB、4 或 8 ranges。每种类型分别与自身的串行结果比较，不跨类型直接比较绝对吞吐。

| 类别 | 用例 | 造数要点 |
| --- | --- | --- |
| 整数 | TINYINT、SMALLINT、INT、BIGINT、LARGEINT | 正负值；TINYINT 的 range 数不要超过有效 distinct key 数 |
| 字符串 | CHAR、短 VARCHAR、长 VARCHAR | 长 VARCHAR 使用超过 short-key 长度的公共前缀，触发冲突 refinement/typed boundary |
| 日期 | DATEV2、DATETIMEV2(6) | 覆盖不同日期和微秒值 |
| DecimalV3 | DECIMALV3(9,2)、(18,2)、(38,2)、(76,2) | 分别落到 Decimal32/64/128I/256；Decimal256 会话开启 `enable_decimal256=true` |
| 兼容类型 | legacy DATE、DATETIME、DECIMALV2(27,9) | 仅专用集群执行，见下方配置 |
| 复合 key | `(INT, VARCHAR)` | 第一列固定或低基数，第二列高基数，验证自动扩大 boundary prefix |
| nullable 复合 key | `(INT NULL, BIGINT NOT NULL)` | 50% NULL；再增加首列全 NULL、第二列唯一的极端用例 |

legacy 类型默认禁止新建。如确实需要覆盖对应物理类型，只在专用 FE 上临时设置并在测试后恢复：

```sql
ADMIN SET FRONTEND CONFIG ('enable_date_conversion' = 'false');
ADMIN SET FRONTEND CONFIG ('disable_datev1' = 'false');
ADMIN SET FRONTEND CONFIG ('disable_decimalv2' = 'false');
```

Decimal256 在建表和导入连接中执行：

```sql
SET enable_decimal256 = true;
```

### 5.5 P1：数据分布和 schema 宽度

| 用例 | 数据分布 | 目的 |
| --- | --- | --- |
| 均匀 | key 连续均匀，行宽固定 | 建立理想加速上限 |
| 热点 | 90% 行落入 10% key 空间 | 检查 range/task 倾斜 |
| 低基数 | 首 key 仅 2～8 个值，第二 key 唯一 | 检查 prefix 扩展和实际 range 数 |
| 长公共前缀 | VARCHAR 前缀相同，差异位于末尾 | 检查 short-key 截断和 boundary refinement 开销 |
| key 与行宽相关 | 小 key 行 32 B，大 key 行 4 KiB | 观察按行采样造成的字节不均衡 |
| 窄表 | 2 个 key/value 列 | 偏 IO/merge 基线 |
| 宽表 | 50～100 个 value 列 | 验证 vertical compaction 和内存峰值 |
| 二级索引 | 代表性倒排索引/Bloom Filter | 验证索引输出正确性和写放大 |

### 5.6 P2：业务共存和集群吞吐

1. **查询共存**：先运行 15 分钟稳定查询流量，再分别触发串行和并行 Base Compaction，比较 QPS、P95/P99 和错误率。
2. **导入共存**：保持固定持续导入速率，比较 load throughput、commit latency 和失败率。
3. **多 tablet 并发**：使用接近生产的 bucket 数，同时触发 1、W、2W 个 tablet 的 Base Compaction，计算集群总 GiB/s。
4. **多表并发**：同时运行 DUP、AGG、MOR、MOW compaction，观察 worker thread pool、permits、对象存储限流和前台负载。
5. **peer read**：在相同缓存状态下比较 `enable_cloud_distributed_compaction_peer_read=true/false`，报告对象存储流量和 wall time。

业务共存结果不能与空闲集群结果混在同一统计样本中。

### 5.7 P2：回退与稳定性

- compute group 仅 1 个 BE。
- target range size 大于等于输入大小。
- 首 key 低基数导致 actual range=1。
- MOW 带 cluster key。
- MOW 开启任一 correctness-check 配置。
- row binlog tablet。
- 可选专用集群故障测试：任务运行中停止一个非 coordinator worker，验证失败状态、无脏输出、后续 compaction 可重试。故障测试不计入性能结果。

## 6. 测试表与数据准备

### 6.1 基础表模板

性能微基准每张表只建一个 tablet，避免一次实验混入 tablet 调度差异：

```sql
CREATE TABLE perf_dup_bigint_xxx (
    k BIGINT NOT NULL,
    k2 DATETIMEV2(6) NOT NULL,
    payload VARCHAR(1024) NOT NULL,
    v BIGINT NOT NULL
)
DUPLICATE KEY(k, k2)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES (
    "replication_num" = "1",
    "disable_auto_compaction" = "true"
);
```

模型变体：

- AGG：key 列不变，使用 `v BIGINT SUM`、其他 value 列使用明确聚合函数。
- UNIQUE MOR：`UNIQUE KEY(...)`，设置 `"enable_unique_key_merge_on_write"="false"`。
- UNIQUE MOW：`UNIQUE KEY(...)`，设置 `"enable_unique_key_merge_on_write"="true"`。
- MOW + sequence：增加 `seq BIGINT NOT NULL`，设置 `"function_column.sequence_col"="seq"`。
- nullable composite：`k INT NULL, k2 BIGINT NOT NULL`，key 为 `(k, k2)`。

每个实验创建一对独立同构表：`*_serial_runN` 和 `*_parallel_runN`。Compaction 会改变输入 rowset，不能在同一个 tablet 上先串行再并行后直接比较。配对 tablet 的 coordinator 必须是同一硬件规格；如果不能固定到同一 BE，应让串行/并行样本在各 coordinator 上均衡分布。

### 6.2 key 表达式示例

设单批行号为 `number`，轮次为 `round`：

| 类型/分布 | 表达式示例 |
| --- | --- |
| BIGINT 均匀 | `CAST(round * rows_per_batch + number AS BIGINT)` |
| 更新/聚合重叠 | `CAST(number % key_cardinality AS BIGINT)` |
| DATEV2 | `DAYS_ADD(CAST('2000-01-01' AS DATEV2), CAST(number AS INT))` |
| DATETIMEV2(6) | `MICROSECONDS_ADD(CAST('2000-01-01 00:00:00.000001' AS DATETIMEV2(6)), number)` |
| Decimal | `CAST(number - rows_per_batch / 2 AS DECIMALV3(p, 2))` |
| CHAR | `CAST(LPAD(CAST(number AS STRING), width, '0') AS CHAR(width))` |
| 长 VARCHAR | `CONCAT(REPEAT('x', 120), LPAD(CAST(number AS STRING), 20, '0'))` |
| nullable composite | `IF(number % 2 = 0, NULL, CAST(number % 16 AS INT)), CAST(number AS BIGINT)` |
| 首 key 全 NULL | `CAST(NULL AS INT), CAST(number AS BIGINT)` |
| 热点 | `IF(number < rows_per_batch * 0.9, number % hot_cardinality, number)` |

### 6.3 形成可比较的 Base 输入

1. 先用 1/10 目标行数做校准，得到每行压缩后大小，据此计算每批行数。
2. 每张表形成 8～16 个大小接近的 cumulative rowset；每个 cumulative rowset 可由 2～4 次相同大小导入后手工触发 cumulative compaction 得到。
3. 数据准备阶段不得运行 Base Compaction。
4. 串行表和并行表使用同一份确定性数据，输入 rowset 数、行数、segment 数和总大小偏差均不超过 2%。
5. 正式计时前使用 `/api/compaction/show` 记录所有输入 rowset，确认至少有 4 个 Base 输入 rowset。
6. 对 AGG/MOR/MOW 使用跨 rowset 重叠 key；sequence 值随 round 单调增加，便于验证最终版本。

推荐先准备所有表，再按随机且均衡的 A/B 顺序执行。例如五轮使用 `S-P-P-S-S-P-P-S-S-P`，每次都使用未 compaction 的新表，避免固定的先后顺序偏差。

## 7. 单轮执行步骤

### 7.1 执行前记录

```sql
SHOW BACKENDS;
SHOW TABLETS FROM test_db.perf_table;
```

记录 tablet ID、coordinator BE、RemoteDataSize，并从 coordinator 获取输入状态：

```bash
curl --fail --silent --show-error -u "${BE_USER}:${BE_PASSWORD}" \
  "http://${BE_HOST}:${BE_HTTP_PORT}/api/compaction/show?tablet_id=${TABLET_ID}"
```

执行逻辑快照，至少保存：

```sql
SELECT COUNT(*), COUNT(k), MIN(k), MAX(k), SUM(v),
       BIT_XOR(CRC32(CONCAT_WS('|',
           COALESCE(CAST(k AS STRING), '__NULL__'),
           CAST(k2 AS STRING), CAST(v AS STRING)))) AS fingerprint
FROM perf_table;
```

对 nullable、AGG、MOR、MOW 再保存按 key 分桶的统计和头尾各 100 行。正确性查询不计入 compaction 时间。

### 7.2 串行基线

在所有 BE 上设置：

```bash
curl -X POST -u "${BE_USER}:${BE_PASSWORD}" \
  "http://${BE_HOST}:${BE_HTTP_PORT}/api/update_config?enable_cloud_distributed_base_compaction=false"
```

确认配置已经在所有 BE 生效后触发：

```bash
curl -X POST -u "${BE_USER}:${BE_PASSWORD}" \
  "http://${COORDINATOR_HOST}:${COORDINATOR_HTTP_PORT}/api/compaction/run?tablet_id=${TABLET_ID}&compact_type=base"
```

触发接口成功只代表任务已提交，不能作为结束时间。持续轮询：

```bash
curl -u "${BE_USER}:${BE_PASSWORD}" \
  "http://${COORDINATOR_HOST}:${COORDINATOR_HTTP_PORT}/api/compaction/run_status?tablet_id=${TABLET_ID}"
```

任务进入终态后获取 profile：

```bash
curl -u "${BE_USER}:${BE_PASSWORD}" \
  "http://${COORDINATOR_HOST}:${COORDINATOR_HTTP_PORT}/api/compaction/profile?tablet_id=${TABLET_ID}&compact_type=base&success=true&top_n=1"
```

要求 `success=true`、`is_distributed=false`。

### 7.3 并行实验

在所有 BE 上同时设置：

```bash
curl -X POST -u "${BE_USER}:${BE_PASSWORD}" \
  "http://${BE_HOST}:${BE_HTTP_PORT}/api/update_config?enable_cloud_distributed_base_compaction=true"

curl -X POST -u "${BE_USER}:${BE_PASSWORD}" \
  "http://${BE_HOST}:${BE_HTTP_PORT}/api/update_config?cloud_distributed_base_compaction_target_range_input_size_bytes=${TARGET_BYTES}"
```

若刚调整过 compute group 成员，至少等待 `cloud_distributed_compaction_worker_cache_ttl_ms` 加 5 秒。然后对并行配对表重复触发、等待、profile 和正确性检查。

要求：

- `success=true`、`is_distributed=true`。
- `distributed_task_count` 等于日志中的 `actual_ranges`。
- `distributed_worker_count = min(live BE, actual ranges)`。
- 输出 rowset 为 `NONOVERLAPPING`。
- 所有 worker task 成功，所有 BE 保持 Alive。

### 7.4 轮后正确性

1. 重跑 compaction 前逻辑快照，所有字段必须完全一致。
2. 串行表与并行表结果必须一致。
3. 对严格验证，用所有业务列分组并带 `COUNT(*)` 后双向 `EXCEPT`；差异行数必须为 0，避免普通 `EXCEPT` 忽略 DUP 表的重复次数。
4. MOW + sequence 表逐 key 验证最大 sequence 对应的 value；同时验证 NULL key 和更新期间新增行。
5. 检查 rowset 版本连续、输入 rowset 被替换、输出 segment 无重叠。

## 8. 重复、统计与无效样本

- 每个正式用例至少 3 个有效样本，推荐 5 个；额外第一次可作为 warm-up，不计入结果。
- 主结果使用中位数，同时报告最小值、最大值和变异系数 `CV=stddev/mean`；只有样本不少于 20 次时才报告 compaction P95。
- CV 超过 10% 时增加到 7 次，并排查外部噪声。
- 串行/并行必须交替或随机均衡执行，不能总是串行先跑。
- 以下样本标记为无效并说明原因后重跑：节点扩缩容、对象存储限流、其他大型 compaction、BE 重启、配置未全量生效、实际未进入预期路径。
- 不允许因为性能较差而删除样本；所有排除都必须有监控或日志证据。

## 9. 结果记录模板

### 9.1 单轮结果

| Run ID | 模型 | key 类型/分布 | 输入 GiB | rowsets/segments | BE | requested/actual ranges | workers | 串/并行 | cost s | MiB/s | rows/s | peak mem GiB | remote/peer GiB | correctness |
| --- | --- | --- | ---: | --- | ---: | --- | ---: | --- | ---: | ---: | ---: | ---: | --- | --- |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |

### 9.2 A/B 汇总

| 用例 | Serial median s | Parallel median s | Speedup | 吞吐提升 | 扩展效率 | CPU 成本变化 | 远端 IO 变化 | P99 查询变化 | 结论 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
|  |  |  |  |  |  |  |  |  |  |

### 9.3 Task 均衡

| Run ID | tasks | workers | max/mean elapsed | max/mean output bytes | max queue ms | planning ms | planning 占比 | fast path/fallback |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
|  |  |  |  |  |  |  |  |  |

最终报告至少包含以下图：

- 串行/并行耗时和吞吐柱状图。
- Speedup 与 worker 数折线图，同时画理想线性扩展线。
- 吞吐与 requested ranges 曲线。
- 各 worker/task 的 elapsed time 和 output bytes 分布。
- compaction 前、中、后的 CPU、内存、网络、对象存储吞吐时序图。
- 查询 P99 或导入 P95 在串行/并行 compaction 期间的对比。

## 10. 建议验收标准

阈值应在测试前结合生产 SLO 固化。没有既有标准时，可先采用：

### 10.1 必须满足

- 所有正向用例 compaction 成功，逻辑结果、版本和 rowset 元数据正确。
- 所有负向条件正确回退，不产生部分输出或不可重试状态。
- 32 GiB 以上、至少 4 workers、`actual_ranges >= 2W` 的代表性 DUP 用例，中位吞吐至少提升 50%。
- 任何足够大的支持表模型/类型不得出现超过 10% 的稳定性能回退；小输入只记录，不作为并行收益门槛。
- 无 BE crash、对象存储错误、任务永久挂起或持续内存增长。

### 10.2 资源与业务影响

- 均匀数据下 `max(task elapsed)/mean <= 1.5`；倾斜用例单独解释。
- range planning 时间不超过并行总耗时的 5%，或绝对值不超过 10 秒。
- 聚合远端读取和写入放大不超过串行的 1.2 倍；超过时必须解释 peer/cache 行为。
- 稳态查询 P99 或导入 P95 回退不超过 10%，错误率不增加。
- 无对象存储限流；如发生限流，该轮只用于容量分析，不用于算法加速结论。

## 11. 推荐执行顺序

1. P0 冒烟和正确性。
2. 32 GiB、当前 BE 数、`R=2W` 的 DUP BIGINT 主基线。
3. range 数和 worker 数扩展性。
4. AGG、MOR、MOW 表模型。
5. 全部 key 类型、nullable/composite 和 fast-path/fallback。
6. 数据倾斜、宽表和索引表。
7. 查询/导入共存、多 tablet 并发。
8. 可选的 peer-read、采样数和故障测试。

若第 2 步没有明显收益，先分析 CPU、对象存储带宽、任务不均衡、planning 和 polling 开销，不继续扩大完整矩阵。

## 12. 测试结束与回滚

1. 恢复所有 BE 的原始动态配置，并逐节点确认。
2. 恢复可选 legacy FE 配置。
3. 停止测试流量，确认没有 RUNNING/PENDING compaction。
4. 保留测试表和原始 profile/log 至分析完成；经确认后再删除。
5. 归档运行清单、配置快照、profile JSON、`/api/compaction/show`、相关 BE 日志和监控截图。
6. 最终结论分别说明：最佳 range 大小、可扩展到的 worker 数、各模型收益、资源成本、业务影响和不适合并行的场景。
