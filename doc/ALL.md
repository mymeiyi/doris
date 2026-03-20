一、重点工作自评

【自评】重点工作1.1 mow 存算分离优化
请应用STARR模型介绍重点工作的背景（Situation），任务或目标（Task）、你所采取的行动（Action）、最终达成的结果（Result）对你对这项工作的复盘（Review，做得好的方面2-3点及待提升的方面2-3点）。
**Situation（背景）**：
存算分离 mow 长期遇到多个问题：
1. 导入、compaction、schema change 的锁冲突导致导入失败；
2. delete bitmap 数量多、数据量大，对 ms 造成了很大的压力；
3. delete bitmap 和 rowset 的生命周期不对齐；
4. 持续导入时查询不能命中 delete bitmap agg cache，agg 消耗 cpu 高；
5. 新的需求：Delete bitmap 适配快照功能

**Task（任务）**：
1. 解决存算分离 mow 的锁冲突问题
2. 减少 delete bitmap 的数量和数据量
3. 优化 delete bitmap 存储方式

**Action（行动）**：
1. mow 减少锁冲突专项：发现锁冲突有相当一部分原因是冲突后 sleep 重试，持有了锁被占用的时间；修改原来锁的 kv 设计，减少不必要的锁冲突；处理兼容升级，防止升级过程中产生正确性问题。
2. 减少 delete bitmap 专项：在 compaction 时合并部分 delete bitmap，减少 ms 中的 delete bitmap 数目；同时减少 be 内存中的 delete bitmap 数目，降低 be agg delete bitmap 对 cpu 的消耗。
3. delete bitmap v2 存储专项：重新设计 delete bitmap 的存储方式，大的 delete bitmap 存到对象中；由每次可能写入多个 delete bitmap 的 kv，改成只写入一个 kv；新的方式存储兼容快照工作。

**Result（结果）**：
1. 减少锁冲突效果：多 tablet 高并发的导入事务延迟 avg p99 从 1.68 min 降低到 49.4 s，max p99 从 3.13 min 降低到 1.21 mins。
2. 减少 delete bitmap 效果：delete bitmap 的数量级从版本数的平方降低到 rowset 数的平方。
3. 新的delete bitmap存储格式和路径(对象存储)：正确性和场景 case 正常跑。

**Review（复盘）**：
做得好的方面：
1. 深入分析问题根因（锁冲突后 sleep 重试持锁时间），针对性地优化 kv 设计，效果显著。
2. 方案设计时考虑到兼容升级，确保升级过程正确性。

待提升的方面：
1. 进一步降低锁粒度的空间还很大，目前方案是在之前方案上迭代，更优的方案是不要使用 table 级别的锁。
2. 可以更早开始 code review，review 后修改代码需要重新测试，比较费时间。

---

【自评】重点工作1.2 快照功能
请应用STARR模型介绍重点工作的背景（Situation），任务或目标（Task）、你所采取的行动（Action）、最终达成的结果（Result）对你对这项工作的复盘（Review，做得好的方面2-3点及待提升的方面2-3点）。

**Situation（背景）**：
快照功能是存算分离的重要特性

**Task（任务）**：
实现完整的快照功能

**Action（行动）**：
1. 实现部分 key 的双写、双读、回收机制。
2. 实现快照命令功能。
3. FE 实现做快照和 clone 快照。
4. 实现快照链压缩，减少快照链过长带来的性能问题。
5. 实现删除仓库的回收功能。
6. 实现快照计费功能。
7. 补充测试用例确保功能正确性。

**Result（结果）**：
完成了上述功能，补充了相关测试。

**Review（复盘）**：
做得好的方面：
1. 功能实现较为完整，测试编写比较充分。

待提升的方面：
1. 快照计费提测一直在等 QA 排期，这项工作长期不能 close。
---

【自评】重点工作1.3 FE 内存优化
请应用STARR模型介绍重点工作的背景（Situation），任务或目标（Task）、你所采取的行动（Action）、最终达成的结果（Result）对你对这项工作的复盘（Review，做得好的方面2-3点及待提升的方面2-3点）。

**Situation（背景）**：
多 tablet 场景下 FE 的内存占用过高

**Task（任务）**：
减少多 tablet 下 FE 的内存占用，对存算分离和存算一体场景都有优化效果。

**Action（行动）**：
1. 重构 Tablet 和 Replica 类，拆分成 Local 和 Cloud，优化数据结构，减少不必要的内存占用；并适配了升降级时元数据的兼容性。
2. 优化 TabletInvertedIndex
3. 优化 CloudGlobalTransactionMgr
4. 优化 CloudTabletStatMgr

**Result（结果）**：
1. 对存算分离，100万 tablets 大约减少 600MB 的内存（中期的测试，后续还合入了一些优化）。
2. 对存算一体也有优化效果。

**Review（复盘）**：
做得好的方面：
1. 重构后，不仅节省了内存，Tablet 和 Replica、TabletInvertedIndex等核心类的代码也更清晰了
2. 从数据结构层面修改，改动更加通用，后续如果有其他类似的内存优化需求，也可以复用这套方案

待提升的方面：
1. 这部分工作比较分散，先从一些核心的管理类入手，可能还有其他类似的内存优化点没有覆盖到，后续可以继续分析和优化
---

【自评】重点工作1.4 FE RPC 优化（存算分离 fdb/ms 减负）
请应用STARR模型介绍重点工作的背景（Situation），任务或目标（Task）、你所采取的行动（Action）、最终达成的结果（Result）对你对这项工作的复盘（Review，做得好的方面2-3点及待提升的方面2-3点）。

**Situation（背景）**：
存算分离场景下，FE 会大量重复地去 ms 拉取数据，给 fdb/ms 造成压力。特别是 get_version 和 get_tablet_stats 请求频繁，影响系统整体性能。

**Task（任务）**：
减少 FE 重复拉取 ms 数据的请求，通过主动通知机制和缓存优化，降低 fdb/ms 的负载。

**Action（行动）**：
1. 优化 get_version 请求：
    - 采用主动通知机制更新 table 和 partition 的 version，保持内存中缓存的 version 为最新。
    - 增加缓存 version 的校正机制。
2. 优化 get_tablet_stats 请求：
    - 采用主动通知机制更新 tablet stats。
    - checkpoint 持久化 tablet stats 信息，不写入 edit log，在做 checkpoint 时存储 tablet stats，尽可能保持 stats 较新，缓解了 FE 重启后统计信息不准确导致查询不准确的问题
3. FE 支持主动和被动限流：
    - 主动限流：支持根据 QPS 限流；支持根据请求的代价(kv数)限流。
    - 被动限流：根据 MS 的反压，动态调整 QPS 限制。

**Result（结果）**：
1. 基本消除了查询的 get_version 请求。
2. 对数据无变更的 tablet，不会重复的 get tablet_stats，很大程度上缓解了 stats 数据滞后导致的查询不准确问题。
3. 通过持久化 stats 信息，缓解了 FE 重启后统计信息不准确导致查询不准确的问题。

**Review（复盘）**：
待提升的方面：
1. 测试排期较晚。
2. 当前方案可能是中期方案？思考长期方案：可以考虑让 FE 订阅 MS 的变更，减少 master FE 同步给 follower 的压力，同时代码更加解耦，不需要考虑哪些操作会带来更新，只接收更新的通知。

---

三、整体自评
概要自评这一年工作中的亮点及待提升的方面（至少3项），内容可以是业绩结果、工作方法或行为表现。

**亮点**：
1. 存算分离优化成效显著: 
 - mow 锁冲突优化将导入事务延迟降低约 60%
 - delete bitmap 数量级从版本数平方降到 rowset 数平方
 - FE 内存优化在 100万 tablets 场景减少约 600MB 内存

**待提升的方面**：
1. 效率提升：
 - 可以更早开始 code review，尽早暴露问题
 - 测试排期较晚，导致一些功能的测试进展缓慢，周期较长

---

四、下一年度的重点工作
基于对自己岗位职责的理解和对团队工作目标或计划的了解，思考下一年度工作计划，包括重点工作内容、核心行动及自我提升目标。

**重点工作内容**：
1. 存算分离备份恢复相关工作

**自我提升目标**：
1. 更加高效地使用 AI 工具辅助开发和问题定位。
