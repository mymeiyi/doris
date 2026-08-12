# DORIS-27914：并行 CU Compaction 与 CU Point 一致性

## 核心约束

CU point 表示 cumulative layer 的起始版本，必须是 rowset 边界：

```text
不存在可见 rowset 满足：rowset.start < cu_point <= rowset.end
```

例如已有 `[1-10]` 时，CU point=5 是非法状态，正确值至少应为 11。

## 问题时序

实际 compaction 都发生在 BE `172.20.60.240`；BE `172.20.60.241` 是后续同步到错误状态的节点。

| 时序 | BE 240 high `[5-7]` | BE 240 low `[2-4]` | MS 状态 |
|---|---|---|---|
| T1 | 注册后延迟，执行 point=2 | - | point=2 |
| T2 | 等待 | 开始执行，按旧布局计算 proposal=6，提交前阻塞 | point=2 |
| T3 | 计算 proposal=2 并先提交 | 等待 | rowsets=`[2][3][4][5-7]`，point=2 |
| T4 | 完成 | 携带过期 proposal=6 提交 | rowsets=`[2-4][5-7]`，point=6 |
| T5 | - | - | point=6 落入 `[5-7]`，约束被破坏 |
| T6 | base 合并 `[2-7]`，输入/输出 point 都是 6 | - | rowsets=`[2-7]`，故障版本旧逻辑仍保留 point=6 |

## 暴露的根本问题

1. BE 计算的 proposal 基于本地快照，并行任务提交顺序变化后可能过期，不能直接作为最终值。
2. MS 原来无条件采纳 BE proposal；并行任务完成后，proposal 对应的 compaction counters 已经过期。
3. 发布当前 CU 输出时，仍必须保证 MS 当前 point 不落入该输出内部。
4. base 旧逻辑原样保留 point，使上游产生的非法状态继续存在。
5. sync rowsets 信任 point 是合法边界；一旦该约束被破坏，增量同步会漏 rowset。

## 修复结论

本 JIRA 的直接竞态已经解决：过期 CU proposal 不再覆盖 MS 当前 point，且每次提交都会保证 point 不落入本次输出 rowset。该功能尚未合入，不存在旧版本兼容或存量非法 point 问题。在所有 BE 都携带 compaction 输入范围的前提下，当前 MS 约束与 `calc_sync_versions` 可以保证 sync 不漏 rowset。

| 状态 | 项目 | 当前情况 | 验证状态 |
|---|---|---|---|
| 已实现 | BE 侧 CU point 计算 | 已处理 overlap/delete rowset，并保留已经前移的 point（`65b2707cfe2`） | 单测通过 |
| 已实现 | MS 拒绝过期 proposal | FINISH 时比较 BE 计算 proposal 时快照的 counters 与 MS 最新 counters；不一致则忽略 BE proposal | 单测通过 |
| 已实现 | MS point 单调及“本次 CU 输出”校验 | proposal 有效时取 `max(MS point, proposal)`；无论 proposal 是否有效，point 落入本次输出都推进到 `end+1`（`333495e83ea` 及后续修复） | 单测通过 |
| 已实现 | BE 并行提交后的本地 rowset 安装 | response counter 不连续时不直接安装单个输出，保留旧 counter，并标记下次必须 sync（`333495e83ea`） | 单测通过 |
| 已实现 | Base point 约束 | base 输出包含 point 时推进到 `end+1`（`2e48b8dcc77`） | 单测通过 |
| 已解决 | 本 JIRA 的 high 先提交、low 后提交顺序 | low FINISH 发现 proposal counters 已过期，忽略 proposal=6，point 保持为合法的 2 | 单测通过 |
| 已覆盖 | sync rowsets 增量范围 | CC/BC counters 变化后按 point 补充保守同步范围，可覆盖 compaction 重写的 rowset | 现有 `calc_sync_versions` 单测覆盖 |
| 可选增强 | sync 入口 point 不变量校验 | `get_rowset` 可从权威快照校验 point，用于尽早暴露未来新增路径破坏约束 | 未实现 |

因此，新提交不会再产生 T4 的非法 point=6；Base 修复继续保证自身提交后 point 位于 rowset 边界。

## 新增并已采用的解法

### CU FINISH：用 proposal 快照 counters 判断是否过期

BE 在 `modify_rowsets` 读取 point 和 rowsets 的同一把 tablet header lock 下快照 Base/CU
counters，并随 FINISH 请求提交。MS 使用这组 counters 判断 proposal 是否仍然有效：

```text
proposal_stale =
    proposal.base_cnt != MS 当前 base_cnt，或
    proposal.cu_cnt   != MS 当前 cu_cnt

如果 proposal_stale：
    point = MS 当前 point
否则：
    point = max(MS 当前 point, BE proposal)

无论 proposal 是否过期，如果本次输出满足 output.start < point <= output.end：
    point = output.end + 1
```

实现要点：

- 放在 `process_compaction_job` 的同一 MS 事务中，在写入 stats 前完成。
- START counters 只负责发现开始任务时的过期 BE cache；proposal counters 负责判断 FINISH
  proposal 使用的布局是否仍然有效，两者语义不能混用。
- FINISH 必须携带 proposal counters；MS 不接受缺少该快照的 CU/EMPTY_CU proposal。
- counters 一致时采纳 proposal；任一 counter 变化都说明 proposal 的 rowset 快照可能过期，只保留 MS 当前 point。
- 即使 proposal 过期，也必须检查 MS 当前 point 是否落入本次输出，因为发布输出本身会改变 rowset 边界。
- stats、point 和输出 rowset 在同一个 MS 事务中提交；point 只能前进，不能回退。
- 若发现 rowset 重叠等不可能状态，应报错，不要静默继续修正。

在本问题中，low 在 high 提交前计算 proposal=6，快照 CU counter=0；high 提交后 MS
counter=1，因此 low FINISH 忽略过期 proposal，最终保持合法但保守的 point=2。如果 low
先同步并基于 counter=1 的新布局重新计算，则 proposal 可以正常接受。

### Base FINISH：保持 point 不变量

若 base 输出满足 `base.start < point <= base.end`，将 point 推进到 `base.end + 1`。这保证 Base 发布输出后 point 仍是 rowset 边界，但不能替代 CU 根因修复，因为错误状态在 Base 完成前已经会影响同步。

## Sync 正确性

`calc_sync_versions` 依赖 MS 返回的 point 与 compaction counters，并按以下规则补齐 compaction 重写范围：

- BC counter 前进：补 `[0, MS point - 1]`。
- 仅一次 CC、point 前进且没有 Full Compaction：补 `[BE point, MS point - 1]`。
- 其他 CC 变化，包括本问题中 counter 前进但 point 保持为 2：保守补 `[BE point, MAX]`。
- 最后与普通增量 `[local max + 1, MAX]` 合并。

本问题修复后，MS 为 `point=2, cc_cnt=2`。落后的 BE 携带旧 counter 和 `point=2` 请求时，会走保守分支 `[2, MAX]`，完整拉取 `[2-4]`、`[5-7]`，不会再出现原来的 `[0,5] ∪ [8,MAX]` 漏洞。

正确性还依赖以下现有机制：

- MS 在同一事务中提交 rowset、stats、counters 和 point，并在同一事务快照中读取返回。
- BE 在同一 tablet header lock 下安装 rowsets，再更新 counters 和 point。
- 过期的 sync 响应若 counters 落后于 BE 本地状态，会重试而不会覆盖新状态。
- MS 的 CU/Base FINISH 保证 point 始终位于 rowset 边界。

结论：在当前未合入功能的统一版本代码中，可以保证本 JIRA 场景的 sync 正确性。sync 侧再校验 `start < point <= end` 可作为未来新增 compaction 路径时的 fail-fast 保护，但不是当前正确性的缺口。

### 可选增强：sync 入口校验

不能只扫描 BE 本地 rowset 或本次增量响应：原故障中的 `[2-7]` 正是被同步区间漏掉的 rowset，BE 无法据此发现 point=6 非法。校验应放在 MS `get_rowset`，使用与 stats、rowsets 相同的事务快照：

```text
cp = stats.cumulative_point
rowset = 第一个可见且 end_version >= cp 的 rowset

如果 rowset 存在且 rowset.start_version < cp：
    返回 InternalError，拒绝本次 sync
否则：
    继续执行 calc_sync_versions
```

实现要点：

- 普通 KV 从 `meta_rowset_key(tablet_id, cp)` 开始执行 `limit=1` 的 range get。
- Versioned Read 在 `MetaReader` 中提供相同语义的“当前可见 rowset successor”查询，并使用相同 snapshot version。
- `point == rowset.start` 合法；仅 `start < point <= end` 非法。
- 错误记录 tablet、point、rowset range 及 BC/CC/FC counters，不更新 BE 本地 rowsets、counters 和 point。
- 不自动 full sync：若 MS 权威快照中的 point 非法，full sync 仍会读到相同错误，不能完成修复。

## 尚未完成

- 尚无覆盖本 JIRA 完整时序的 sync rowsets 端到端测试。
- sync 入口的 MS 权威 point 校验未实现，属于可选增强。

## 测试覆盖

已验证：

1. high `[5-7]` 先提交、low `[2-4]` 携带旧快照 proposal=6 后提交，忽略 proposal，最终 point=2；low 使用新快照重新计算时接受 proposal=6。
2. low 先提交使 point=6、high 后提交时，point 落入 high 输出 `[5-7]`，最终推进到 8。
3. point 等于 rowset start 时保持不变；point 落入 `(start,end]` 时推进到 `end+1`。
4. 构造状态 `[2-7] + point=6`，经 base 提交后变为 point=8，验证 Base 自身保持不变量。

未添加：本 JIRA 完整时序的 sync rowsets 端到端测试；sync fail-fast 增强实现时需覆盖 `[2-7] + point=6` 拒绝、point=2/8 接受，以及普通 KV 和 Versioned Read 两条路径。
