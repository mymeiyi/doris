# Cloud MemTable 前移：配置、导入方式与观测

本文对应当前分支（含 Cloud DUP、AGG、UNIQUE MOR/MOW 直传改动），核对日期为 2026-09-09。
默认值以本分支源码为准，运行集群可能被全局变量、配置文件或动态配置覆盖。
实现原理见 [cloud_memtable_on_sink.md](cloud_memtable_on_sink.md)。

## 1. 先区分三个独立功能

| 功能 | 做什么 | 控制位置 |
|---|---|---|
| MemTable 前移 | Sink BE 进行聚合、排序、flush 和 Segment 构建 | SQL 会话变量，或 Stream Load 请求参数 / FE 默认配置 |
| Sink BE 直传 | Sink BE 将 Segment / 索引直接上传对象存储，目标 BE 接收元数据并提交 Rowset；MOW 还传递 bitmap 结果 | SQL 会话变量，或 Stream / Routine Load 使用的 FE 配置 |
| packed file | 将符合条件的小文件写进共享 packed 对象，Rowset 保存 slice 映射 | 实际上传文件的 BE 配置 |

三条实际写入路径：

```text
前移关闭：数据 Block → 目标 BE 的 CloudDeltaWriter → 对象存储
前移开启、直传关闭：Sink 构建 Segment → LoadStream 传文件 → 目标 BE 上传
前移开启、直传开启：Sink 构建并上传 Segment → LoadStream 传元数据 → 目标 BE 提交
```

前移开关关闭时，单独打开直传开关没有效果。packed file 不要求开启前移或直传；
三种路径都可以使用符合条件的 packed 写入。Sink BE 与目标 BE 也可能恰好是同一台机器。

### 表模型与限制

| 目标表 | 前移 + 文件转发 | 前移 + Sink 直传 |
|---|---|---|
| DUPLICATE KEY | 支持 | 支持 |
| AGGREGATE KEY | 支持 | 支持 |
| UNIQUE KEY，MOR | 支持 | 支持 |
| UNIQUE KEY，MOW | 保留原 Cloud MOW 路径 | 支持全列写入 |

共同前提：目标为 Cloud OLAP 表，启用轻量 Schema Change；本次写入不是 partial update，
没有启用 row binlog，表没有 V1 倒排索引。无倒排索引或使用 V2 索引可以前移。
这些条件不满足时，BE/FE 的门控会保留原写入路径，开关为 `true` 不代表实际启用。
MOW 直传还要求获取快照时 tablet 处于 RUNNING 状态。

直传要求参与导入的 BE，以及 MetaService / Recycler 支持本分支的显式 Segment ID 格式。
目标 BE 不支持直传协议或 MOW 快照时，开启直传的导入会在上传前报错。
AGG、MOR、MOW 的业务更新顺序仍遵循各表模型和 Sequence 规则，不能以 Segment ID 大小判断。

## 2. 配置总表

### 2.1 SQL 会话变量

| 变量 | 源码默认值 | 作用 |
|---|---|---|
| `enable_memtable_on_sink_node` | `true` | 普通 INSERT、Broker Load 和创建 Routine Load 时的前移选择 |
| `enable_cloud_memtable_direct_upload` | `false` | 普通 INSERT、Broker Load 的直传选择 |
| `enable_profile` | `false` | 收集 SQL 导入 Profile |
| `profile_level` | `2` | `2` 或以上才会收集本文使用的详细 writer 标记 |
| `load_stream_per_node` | `2` | 每节点 LoadStream 数量；不是前移或直传的开关 |

```sql
SHOW VARIABLES LIKE 'enable_memtable_on_sink_node';
SHOW VARIABLES LIKE 'enable_cloud_memtable_direct_upload';
SHOW VARIABLES LIKE 'enable_profile';
SHOW VARIABLES LIKE 'profile_level';

SET enable_memtable_on_sink_node = true;
SET enable_cloud_memtable_direct_upload = true;
SET enable_profile = true;
SET profile_level = 2;
```

`SET` 只影响当前连接。需要新连接默认采用某值时使用 `SET GLOBAL`，并在新连接中核对；
不要把 SQL 全局变量当成 Stream Load 的 FE 默认配置。已创建的异步导入任务不跟随当前连接后续的 `SET`。

### 2.2 FE 配置

| 配置 | 默认值 | 用途 |
|---|---|---|
| `stream_load_default_memtable_on_sink_node` | `false` | Stream Load / HTTP Stream 未显式传前移参数时的默认值；Group Commit 内部导入也读取它 |
| `stream_load_default_cloud_memtable_direct_upload` | `true` | Stream Load、HTTP Stream、Routine Load 以及 Group Commit 内部导入的直传开关 |
| `enable_stream_load_profile` | `false` | NereidsStreamLoadPlanner 路径的 Profile 收集，包括普通 Stream Load 和 Routine Load |

前两项为动态、`masterOnly` 配置。示例：

```sql
ADMIN SET FRONTEND CONFIG ('stream_load_default_memtable_on_sink_node' = 'true');
ADMIN SET FRONTEND CONFIG ('stream_load_default_cloud_memtable_direct_upload' = 'true');
ADMIN SHOW FRONTEND CONFIG LIKE 'stream_load_default_%memtable%';
```

也可以在各 FE 的 `fe.conf` 中统一维护启动值：

```properties
stream_load_default_memtable_on_sink_node = true
stream_load_default_cloud_memtable_direct_upload = true
```

动态修改不等于已写入部署配置；需要重启后仍保持时同步维护 `fe.conf`。
配置作用于之后生成的导入计划，不会切换已经运行的 writer。
`enable_stream_load_profile` 为动态配置，但不是 `masterOnly`；观察时应检查实际生成计划的 FE。

### 2.3 BE 配置

没有需要额外打开的 BE 级“MemTable 前移开关”或“Sink 直传开关”。BE 根据 FE 下发的查询选项选择路径。

| 配置 | 默认值 | 动态修改 | 说明 |
|---|---|---|---|
| `share_delta_writers` | `true` | 否，修改 `be.conf` 后重启 | 同一 BE 上多个 Sink 共享 writer；文件转发路径需保留 `true`，直传支持 `false` |
| `enable_packed_file` | `true` | 是 | Cloud S3 packed 写入开关 |
| `small_file_threshold_bytes` | `1048576` | 是 | 小文件阈值，1 MiB；超过阈值的文件转为独立文件上传 |
| `packed_file_size_threshold_bytes` | `5242880` | 是 | packed 对象聚合大小阈值，5 MiB |
| `packed_file_time_threshold_ms` | `100` | 是 | packed 聚合时间阈值 |
| `packed_file_small_file_count_threshold` | `100` | 是 | packed 聚合小文件数量阈值 |

```sql
SHOW BACKEND CONFIG LIKE 'share_delta_writers';
SHOW BACKEND CONFIG LIKE 'enable_packed_file';
SHOW BACKEND CONFIG LIKE 'small_file_threshold_bytes';
SHOW BACKEND CONFIG LIKE 'packed_file_%';
```

每台参与导入的 BE 都应核对。动态修改示例（对指定 BE 执行，替换地址）：

```bash
curl -X POST 'http://BE_HOST:8040/api/update_config?enable_packed_file=true'
```

持久配置写入各 BE 的 `be.conf`。使用默认 writer 共享即可启用本功能；无需为启用直传而关闭共享，
也不必调整 flush / S3 上传线程数。并行度和线程池调整属于性能调优，应结合实际等待时间另行评估。

## 3. 各导入入口如何使用

下列示例假设数据库为 `demo`，目标表 `cloud_memtable_demo(k BIGINT, v BIGINT)` 已存在且满足第 1 节条件。
示例主机、用户、计算组、对象地址和凭据均需替换。Cloud 计算组仍按原有方式选择。

### 3.1 INSERT VALUES、INSERT SELECT、TVF / 外部 Catalog 导入

在执行 INSERT 的同一个 SQL 连接设置：

```sql
SET group_commit = 'off_mode';
SET enable_memtable_on_sink_node = true;
SET enable_cloud_memtable_direct_upload = true;
SET enable_profile = true;
SET profile_level = 2;

INSERT INTO demo.cloud_memtable_demo VALUES (1, 10), (2, 20);
INSERT INTO demo.cloud_memtable_demo
SELECT number, number * 10 FROM numbers('number' = '10000');
```

`INSERT INTO ... SELECT ... FROM S3(...)`、HDFS TVF 或外部 Catalog 表也按目标 OLAP Sink 使用这两个会话变量。
数据源不同不改变目标表的门控条件。这里关闭 Group Commit 是为了明确验证普通 INSERT 路径；
Group Commit 见第 3.6 节。

只启用前移、让目标 BE 上传：将 `enable_cloud_memtable_direct_upload` 设为 `false`。
这只对 DUP / AGG / MOR 保留前移；MOW 会使用原 Cloud MOW 路径。

### 3.2 Broker Load：LOAD LABEL（S3 / HDFS / Broker）

提交 `LOAD LABEL` 前，在同一连接设置两个会话变量；它们不是 `LOAD ... PROPERTIES` 的属性。

```sql
SET enable_memtable_on_sink_node = true;
SET enable_cloud_memtable_direct_upload = true;
SET enable_profile = true;
SET profile_level = 2;

LOAD LABEL demo.cloud_memtable_broker_001 (
    DATA INFILE('s3://BUCKET/input/data.csv')
    INTO TABLE cloud_memtable_demo
    COLUMNS TERMINATED BY ','
    FORMAT AS 'CSV'
    (k, v)
)
WITH S3 (
    'AWS_ENDPOINT' = 'S3_ENDPOINT',
    'AWS_REGION' = 'S3_REGION',
    'AWS_ACCESS_KEY' = 'ACCESS_KEY',
    'AWS_SECRET_KEY' = 'SECRET_KEY'
)
PROPERTIES ('timeout' = '600');

SHOW LOAD FROM demo WHERE LABEL = 'cloud_memtable_broker_001';
```

HDFS / Broker 的数据源描述使用各自原有语法，前移与直传设置相同。
任务创建后再修改会话变量不会修改已创建任务的选择。

### 3.3 普通 Stream Load：`/api/db/table/_stream_load`

**前移优先级：请求头 `memtable_on_sink_node` > FE 的 `stream_load_default_memtable_on_sink_node`。**
直传只读取 FE 的 `stream_load_default_cloud_memtable_direct_upload`，当前没有独立的单请求直传 header。
在另一个 MySQL 连接执行 `SET enable_cloud_memtable_direct_upload=true` 对该 HTTP 请求无效。

FE 默认开启直传；若此前显式关闭，可重新开启：

```sql
ADMIN SET FRONTEND CONFIG ('stream_load_default_cloud_memtable_direct_upload' = 'true');
```

再发起带前移参数的请求（CSV 文件内容例如 `1,10`、`2,20`）：

```bash
curl --location-trusted -u USER \
  -H 'label: cloud_memtable_stream_001' \
  -H 'column_separator: ,' \
  -H 'columns: k,v' \
  -H 'memtable_on_sink_node: true' \
  -H 'enable_profile: true' \
  -H 'group_commit: off_mode' \
  -T data.csv \
  'http://FE_HOST:8030/api/demo/cloud_memtable_demo/_stream_load'
```

如果 FE 已设置 `stream_load_default_memtable_on_sink_node=true`，请求可以省略前移 header。
显式 `memtable_on_sink_node:false` 可让该请求使用原写入路径，即使 FE 默认开启前移和直传。
要保留前移但关闭直传，需关闭 FE 的直传配置；目前不能只为单个 HTTP 请求覆盖直传开关。
普通 Stream Load 的 Profile 开关为 `enable_profile:true`，或 FE 的 `enable_stream_load_profile=true`。

### 3.4 HTTP Stream：`/api/_http_stream`

使用 SQL header 和 `http_stream(...)` 的入口，也接受 `memtable_on_sink_node`，直传同样读取 FE 配置。

```bash
curl --location-trusted -u USER \
  -H 'memtable_on_sink_node: true' \
  -H 'sql: INSERT INTO demo.cloud_memtable_demo SELECT c1,c2 FROM http_stream("format"="csv","column_separator"=",")' \
  -T data.csv \
  'http://FE_HOST:8030/api/_http_stream'
```

该入口使用 `FrontendServiceImpl.httpStreamPutImpl` 的 SQL 计划路径；不要假定普通 Stream Load
的 `enable_profile` header 在这里也生效，当前 `HttpStreamAction` 没有转发该 header。
需要详细 Profile 时，可临时设置 SQL 全局 `enable_profile=true`、`profile_level=2`，使新建的内部
ConnectContext 取得它们，并在验证后恢复；结合实际 Profile 与 BE 日志判断。

### 3.5 Routine Load（Kafka / Kinesis）

前移在创建 job 时读取当前 SQL 会话变量；直传在 task 规划时读取 FE 配置。
仅设置 SQL 会话的直传变量不能为 Routine Load 开启直传。

```sql
ADMIN SET FRONTEND CONFIG ('stream_load_default_cloud_memtable_direct_upload' = 'true');
SET enable_memtable_on_sink_node = true;

CREATE ROUTINE LOAD demo.cloud_memtable_kafka ON cloud_memtable_demo
COLUMNS TERMINATED BY ',',
COLUMNS(k, v)
PROPERTIES ('desired_concurrent_number' = '1')
FROM KAFKA (
    'kafka_broker_list' = 'KAFKA_HOST:9092',
    'kafka_topic' = 'CLOUD_MEMTABLE_TOPIC',
    'property.kafka_default_offsets' = 'OFFSET_BEGINNING'
);

SHOW ROUTINE LOAD FOR demo.cloud_memtable_kafka;
```

Kinesis job 使用其原有数据源属性，前移/直传配置来源相同。当前没有名为 `memtable_on_sink_node`
或 `enable_cloud_memtable_direct_upload` 的 Routine Load job property，也没有对应 ALTER 开关。
已有 job 不能靠修改另一个会话的变量切换前移；FE 的直传配置影响之后规划的 task。

观察 task Profile 时可临时开启 `enable_stream_load_profile`；`SHOW ROUTINE LOAD` 的 RUNNING 状态
只说明任务在运行，不证明前移或直传生效。

**当前恢复限制：** RoutineLoadJob 的 `memtableOnSinkNode` 没有 `@SerializedName`，不会被当前
Gson 持久化；FE 重启或元数据回放后不能假定 job 保留创建时的前移选择。BrokerLoadJob 的前移字段
也未持久化。恢复后的任务需重新观测；本文不将这些路径描述为已经覆盖故障恢复的功能。

### 3.6 Group Commit、客户端连接器及其他入口

| 入口 | 使用方式 / 当前边界 |
|---|---|
| INSERT 或 Stream Load 的 Group Commit | 接收请求与最终内部导入是两个阶段。内部 `group_commit(...)` 导入没有携带单请求前移参数，读取 FE 的两个 `stream_load_default_*` 配置；不能只靠外层 INSERT 的 `SET` 或 HTTP header 判断最终 writer |
| Flink / Spark Connector / DataX / 自编写 HTTP 客户端，以 Stream Load 写入 | 通过客户端已有的 Stream Load header 透传能力发送 `memtable_on_sink_node:true`；或配置 FE 默认前移。直传仍使用 FE 配置，不需要另加 S3 凭据到请求中 |
| MySQL `LOAD DATA` | MysqlLoadManager 转成 Stream Load，当前没有透传这两个 SQL 会话变量或前移 header；使用 FE 的两个默认配置 |
| `COPY INTO` | 当前 CopyJob 使用的 BrokerLoadJob 构造路径没有初始化前移字段，仍为 `false`；不能按普通 Broker Load 的示例宣称前移已开启 |
| Spark Load（`WITH RESOURCE` 的 ETL 导入） | 与 Spark Connector 的 Stream Load 不同；不使用本文的 BrokerLoadJob 前移链路，本文不提供其前移启用方式 |
| Stream Load 两阶段提交 | 前移不改变原有 2PC 限制；当前 Cloud MOW Stream Load / HTTP Stream 的 2PC 仍不支持 |

Group Commit 要启用最终内部导入的前移和直传，应同时设置：

```sql
ADMIN SET FRONTEND CONFIG ('stream_load_default_memtable_on_sink_node' = 'true');
ADMIN SET FRONTEND CONFIG ('stream_load_default_cloud_memtable_direct_upload' = 'true');
```

观察内部 `group_commit_...` label 对应的导入，而非只看外层请求返回或 `GROUP_COMMIT_BLOCK_SINK`。
第 3.4–3.6 节及 Routine Load 的说明来自当前代码链路核对；不能等同于下文已完成的专项回归。

## 4. 与 packed file 组合使用

### 4.1 两套常用配置

| 目标 | 前移 | 直传 | BE 配置 | 谁进行 packed / 上传 |
|---|---|---|---|---|
| 前移 + 目标 BE 上传 + packed | 开启 | 关闭 | `share_delta_writers=true`，`enable_packed_file=true` | 接收文件的目标 BE；DUP / AGG / MOR |
| 前移 + Sink BE 直传 + packed | 开启 | 开启 | `enable_packed_file=true`；共享 writer 可保持默认 | 实际构建 Segment 的 Sink BE；DUP / AGG / MOR / MOW |
| 前移 + Sink BE 直传 + 独立对象 | 开启 | 开启 | `enable_packed_file=false` | Sink BE，不打包 |

SQL 方式使用第 2.1 节的两个会话变量，Stream / Routine Load 按第 3 节选择 FE / 请求参数。
统一在参与导入的各 BE 配置：

```properties
share_delta_writers = true
enable_packed_file = true
small_file_threshold_bytes = 1048576
packed_file_size_threshold_bytes = 5242880
packed_file_time_threshold_ms = 100
packed_file_small_file_count_threshold = 100
```

直传时 packed 配置由各 Sink BE 本地读取，不会由目标 BE 通过 write context 统一下发；
只修改目标 BE 而遗漏其他 Sink BE，不能保证导入使用一致的打包策略。
Sink 使用已有 Cloud storage resource 访问对象存储，不需要增加导入级对象存储上传配置。
Broker Load 的 S3 属性用于读取输入数据，和目标 Cloud 存储资源是两回事。

### 4.2 开启 packed 不等于所有 Segment 都打包

当前 packed 仅支持 Cloud 的 S3 文件系统，不用于 HDFS 等其他远端文件系统；V1 倒排索引不参与。
沿用现有“整个 Rowset 的首 Segment”策略：首 Segment 的 `.dat` / V2 `.idx` 候选文件才尝试 packed，
超过小文件阈值仍会写成独立对象。直传只允许分配到首区间（起始 ID 为 0）的 writer 使用该策略，
**不会将每个 Sink 的首 Segment 都当成 Rowset 首 Segment，也不会将全部 Segment 合并成一个对象。**

例如 `segment_ids=[0,1000,2000]` 的 Rowset，可能只有 `_0.dat` 和 `_0.idx` 有 packed slice，
`_1000.dat`、`_2000.dat` 仍是独立对象。这是正常布局，不是 packed 失效。
packed 聚合有时间/大小/文件数阈值，小导入也可能只有一个文件进入某个 packed 对象。

## 5. 如何确认实际启用

### 5.1 首选完整 Profile

SQL INSERT / Broker Load 在提交前执行 `SET enable_profile=true; SET profile_level=2;`；
普通 Stream Load 加 `enable_profile:true`；Routine Load 使用 FE 的 `enable_stream_load_profile`。

从 FE Web UI 的 Profile 页面按 SQL 注释、label、Load ID / Query ID 找到对应导入，等待 Profile 完整上报，
展开各 BE / Fragment / Instance 下的 Sink，搜索：

| 内容 | 可以证明什么 |
|---|---|
| `DeltaWriterV2 <tablet_id>` | 此 writer 在 Sink BE 执行，实际启用了 MemTable 前移 |
| `CloudMemtableDirectUpload: true` | 此 Sink 成功执行直传并上报 partial Rowset 结果 |
| `CloudMemtableMowBitmap: true` | MOW 的存量 bitmap 比较在 Sink 执行 |
| `MemTableWriter <tablet_id>`，`MemTableSortTime`、`MemTableAggTime`、`SegmentWriterTime` | 用于观察 Sink 端 MemTable 工作及开销；单独出现通用 MemTable 计时不足以证明前移 |
| `WaitFlushLimitTime`、`MemTableWaitFlushTime`、`CloseWaitTime` | 辅助分析等待；不能直接等同于 S3 上传时间或纯排队时间 |

注意：

- `OLAP_TABLE_SINK_OPERATOR` 名称在两条实现中共用，不能单凭该名称判断前移。
- writer 共享时，只有最后关闭共享 writer 的 Sink 上报详细 writer Profile，需检查全部 Instance。
- 标记在关闭 writer 的阶段添加；空输入、失败导入、未完成的 Profile、低 Profile level 都可能没有标记。
  “未搜到标记”本身不能证明关闭。
- 当前没有 `PackedFile: true` 这样的导入级 Profile 标记；packed 请用第 5.3 节验证。

已有 Profile REST 接口也可获取（替换地址与 ID）：

```bash
curl -u USER 'http://FE_HOST:8030/rest/v1/query_profile'
curl -u USER 'http://FE_HOST:8030/rest/v1/query_profile/PROFILE_ID'
```

返回可能含 JSON 包装与 HTML 空格，需要查看完整 `data` 内容。
`SHOW QUERY PROFILE` / `SHOW LOAD PROFILE` 也可用作入口；任务状态 SUCCESS / FINISHED 只证明导入成功。

### 5.2 BE 日志辅助定位

先从导入结果或 Profile 获取 Load ID、Txn ID、Tablet ID，再查对应 BE 的 `be.INFO`。
V2 初始化日志来自 `vtablet_writer_v2.cpp`，包含：

```text
init olap tablet sink, load_id: ..., num senders: ..., stream per node: ..., total_streams ...
```

packed 写入日志来自 `packed_file_writer.cpp`：

```text
send_to_packed_manager: ... buffer size: ...
get_packed_slice_location: ... packed_path: ... offset size
```

`send_to_packed_manager` 只说明提交了候选文件；`get_packed_slice_location` 与已提交 Rowset 的映射更强。
按文件路径中的 tablet / rowset 关联本次导入，避免把其他导入的 packed 日志当作证据。
直传时应在 Sink BE 看这些日志，转发时在目标 BE 看；确认谁是 Sink 可使用 Profile 的 Instance 地址。

### 5.3 Rowset 元数据：确认 Segment ID 与 packed 映射

```sql
SHOW TABLETS FROM demo.cloud_memtable_demo;
SHOW PARTITIONS FROM demo.cloud_memtable_demo;
```

记录目标 Tablet ID 和导入对应版本。MetaService 调试接口可读取该版本的 RowsetMeta：

```bash
curl -G 'http://MS_HOST:5000/MetaService/http/get_value' \
  --data-urlencode "token=${MS_TOKEN}" \
  --data-urlencode 'unicode' \
  --data-urlencode 'key_type=MetaRowsetKey' \
  --data-urlencode "instance_id=${INSTANCE_ID}" \
  --data-urlencode "tablet_id=${TABLET_ID}" \
  --data-urlencode "version=${ROWSET_VERSION}"
```

使用实际 MS 管理 token、实例 ID 和版本；不要照抄回归环境的实例配置。
如果已发生 Compaction，当前可见版本对应的 Rowset 可能已被替换，需结合导入记录选择检查对象。

检查以下字段：

- `segment_ids`：实际物理 ID；多 writer 直传可能为 `[0,1000,2000,...]`。区间大小取决于实际配置，
  不固定为 1000。单 writer 的 ID 仍可能连续，因此稀疏 ID 不是唯一启用判据。
- `num_segments`、`num_segment_rows`、`segments_file_size`：与物理 Segment 列表对应。
- `packed_slice_locations`：逻辑 `.dat` / `.idx` 路径到 `packed_file_path`、`offset`、`size`、
  `packed_file_size` 的映射。非空映射证明这些文件采用了 packed 存储。
- 对象存储中能看到映射指向的 `data/packed_file/...` 对象；不能要求每个逻辑 Segment 路径都有独立对象。

packed 元数据不能单独证明是 Sink 直传：目标 BE 上传也会产生相同类型的映射。
要同时证明“Sink 直传 + packed”，应组合 **本次导入 Profile 的直传标记 + 对应 Rowset 的 packed 映射**。

## 6. 最小验证和排查顺序

1. 核对表模型、轻量 Schema Change、partial update、row binlog、索引格式。
2. 按入口核对配置来源：SQL 会话、HTTP header、FE 默认配置；尤其注意 Routine Load 和 Group Commit。
3. 写入非空数据，保留 label，等待成功且 Profile 上报完整；以 writer 和直传标记确认路径。
4. 验证 packed 时使用能生成小首 Segment 的小批量导入，核对实际上传 BE 的配置和 Rowset slice。
5. 如果仍不符合预期，按 Load ID / Txn ID 检查 BE 日志及旧版本协议报错。

常见误用：把 `enable_memtable_on_sink_node` 当成 HTTP header（正确名称不带 `enable_`）；
把直传变量放入 LOAD / Routine Load PROPERTIES；仅在 MySQL 会话设置直传后期待 Stream Load 生效；
将 `share_delta_writers=false` 与文件转发路径组合；以 S3 出现文件或导入成功直接判断启用。

关闭功能时，普通 SQL 新任务设 `enable_memtable_on_sink_node=false`；普通 Stream Load 新请求设
`memtable_on_sink_node:false`。关闭 FE 默认值影响之后规划的默认请求，不能覆盖显式 `true` 的 header，
也不会清除已有 Routine Load job 的创建时选择。关闭 `enable_packed_file` 只影响之后创建的写入器，
已有 packed Rowset 继续按元数据读取。

## 7. 验证范围与代码依据

已完成本分支专项回归的入口：普通 INSERT、Broker Load、普通 Stream Load；
覆盖 DUP / AGG / MOR / MOW、直传开关、packed、V2 索引和多 writer，MOW 还覆盖 Sequence、
删除/重插、cluster key、并发导入及 Compaction。其余入口在本文按代码传参链路说明，未新增专项测试。
本次仅整理文档，没有重新运行上一轮构建或回归。

| 依据 | 主要内容 |
|---|---|
| [SessionVariable.java](fe/fe-core/src/main/java/org/apache/doris/qe/SessionVariable.java) | SQL 默认值及 QueryOptions 下发 |
| [Config.java](fe/fe-common/src/main/java/org/apache/doris/common/Config.java) | FE 默认值和 mutable / masterOnly 属性 |
| [BrokerLoadJob.java](fe/fe-core/src/main/java/org/apache/doris/load/loadv2/BrokerLoadJob.java) | Broker 任务创建、COPY 构造路径及门控 |
| [RoutineLoadJob.java](fe/fe-core/src/main/java/org/apache/doris/load/routineload/RoutineLoadJob.java) | Routine Load 创建时选择和持久化字段 |
| [NereidsStreamLoadPlanner.java](fe/fe-core/src/main/java/org/apache/doris/nereids/load/NereidsStreamLoadPlanner.java) | Stream / Routine 选项和 Profile |
| [FrontendServiceImpl.java](fe/fe-core/src/main/java/org/apache/doris/service/FrontendServiceImpl.java) | HTTP Stream / Group Commit 内部 SQL 的配置 |
| [pipeline_fragment_context.cpp](be/src/exec/pipeline/pipeline_fragment_context.cpp) | BE 实际选择 V2 Sink 的门控 |
| [delta_writer_v2.cpp](be/src/load/delta_writer/delta_writer_v2.cpp) | 直传、MOW Profile 标记和首 writer packed 限制 |
| [Cloud BE config.cpp](be/src/cloud/config.cpp) | packed 配置 |
| [rowset_writer_context.h](be/src/storage/rowset/rowset_writer_context.h) | packed 的 S3 / V1 限制 |
| [test_cloud_memtable_mow.groovy](regression-test/suites/cloud_p0/test_cloud_memtable_mow.groovy) | MOW Profile 与 MS Rowset 布局验证示例 |
