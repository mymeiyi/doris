# Group Commit + Prepared Statement 数据丢失分析

## 测试用例

`regression-test/suites/insert_p0/insert_group_commit_with_prepare_stmt.groovy`

表为 UNIQUE KEY(id, name)，`group_commit_interval_ms = 40`，通过 Server-Side Prepared Statement +
Group Commit 分 3 个 `executeBatch()` 插入 6 行（id=1..6，name 各不相同，无主键冲突）。

## 现象

| 步骤 | 操作 | FE 返回 | 期望 |
|------|------|---------|------|
| Step 1 | `executeBatch()` id=1 | `[1]`，label=group_commit_28481431，txn 1693672923138 | ✅ |
| Step 2 | `executeBatch()` id=2,3,4 | `[1,1,1]`，label=group_commit_f243ad88，txn 1694013919233，`reuse=true` | ✅ |
| Step 3 | `executeBatch()` id=5,6 | `[1,1]`，label=group_commit_f243ad88，txn 1694013919233，`reuse=true` | ✅ |

三次 `executeBatch()` 在 FE 侧**全部返回成功**，但 `getRowCount(6)` 重试 30 次（120 秒）后表中始终只有
**5 行**，丢了 1 行。

> ⚠️ 注意：FE 返回给客户端的 `label`/`txnId` 是该 batch **最后一行**所落入的 block queue 的标识，
> 并不代表整个 batch 的数据都在这个 txn 里。实际在 BE 侧，一个 batch 的多行会被拆散到不同的
> group commit block queue / 内部 txn。这是后面排查的关键，也是之前误判的来源。

## 真正丢的是哪一行：用 BE 日志把 6 行全部对账

BE 的 group commit plan loadId 在 reuse 路径下是固定的 `352a1cb9026547ce-b7c7da07874a4caf`
（= step 1 的 query_id），所有 `add_block` 日志都打印这个 loadId，但 `label` 字段标明落入哪个 block queue。
把 6 次 `add_block` 全部列出来，并和每个 block queue 的最终 commit 对账：

| label | txn | add_block 时间 / 次数 | `add_load_id`(=loads) | commit rows | 结论 |
|---|---|---|---|---|---|
| 28481431 | 1693672923138 | 13.743 ×1 | 1 loads | **rows=1** | ✓ id=1 |
| **67443e4a** | **1693897897984** | **13.865 + 13.918 ×2** | **2 loads** | **rows=1** | **✗ 丢 1 行** |
| f243ad88 | 1694013919233 | 13.978 + 14.003 + 14.016 ×3 | 3 loads | **rows=3** | ✓ |

`1 + 1 + 3 = 5`，恰好等于表里剩下的 5 行。

**结论：6 行全部成功调用了 `add_block`。丢数据发生在 label `67443e4a`（txn 1693897897984）：
它 `add_load_id` 了 2 次、`add_block` 了 2 次，但 `finish group commit ... rows=1`。**

把 BE 的 `add_block` 顺序和 FE 发送的行对上：

```
FE: 13.829 发送 id=2 → BE 13.865 add_block → label 67443e4a   (B2)
FE: 13.869 发送 id=3 → BE 13.918 add_block → label 67443e4a   (B3)  ← 丢失
FE: 13.923 发送 id=4 → BE 13.978 add_block → label f243ad88
FE: 13.992 发送 id=5 → BE 14.003 add_block → label f243ad88
FE: 14.007 发送 id=6 → BE 14.016 add_block → label f243ad88
```

**丢的是 step 2 的第 2 行：id=3（`"c"`, score=NULL）。** 最终表里是 id = 1,2,4,5,6。
step 3 的 id=5,6 以及 step 2 的 id=4 全部落在 f243ad88 并正确提交（rows=3），**完好无损**。

## 决定性时序（label 67443e4a）

```
13.864683  wal_writer 创建 wal (txn 1693897897984)
13.865011  fragment_mgr 启动内部 consumer load fragment (67443e4a)
13.865404  add_block rows=1            ← id=2，block B2 入队
13.872236  open tablets channel        ← consumer 开始把 B2 写入 tablet
13.917099  VNodeChannel mark closed, left pending batch size: 1
                                        ← consumer 已判定 eos，开始关闭/提交
13.918978  add_block rows=1            ← id=3，block B3 入队，比上一行晚 1.9ms
13.919363  All node channels stopped
13.928375  close tablets channel
13.998260  quorum_success, txn 1693897897984
14.059561  delete wal
14.059665  finish group commit ... includes 2 loads ... rows=1
                                        ← B3 被孤立，从未被消费 → 丢失
```

`B3` 在 `13.918` 入队，而 consumer 在 `13.917` 就已经判定 eos 并开始提交了。`B3` 被静默塞进一个
**已经在提交的队列**里，永远不会被消费，但 `add_block` 返回了 OK，FE 因此认为 id=3 成功。

## 根因

### 1. reuse plan 下所有 insert RPC 共用同一个 load_id

server-side prepared statement 的 `reuse_group_commit_plan` 路径复用 step 1 创建的执行计划，
**所有 insert RPC 都跑在同一个 query_id / load_id `352a1cb9...` 下**（BE 全部 add_block 日志可证）。

### 2. `_load_ids_to_write_dep` 是以 load_id 为 key 的 map，eos 判据依赖它为空

`be/src/load/group_commit/group_commit_mgr.cpp`：

```cpp
// add_load_id：以 load_id 为 key，重复 key 会覆盖；计数自增
Status LoadBlockQueue::add_load_id(const UniqueId& load_id, ... put_block_dep) {
    std::unique_lock l(mutex);
    if (_need_commit) {
        return Status::InternalError(...);          // 已 need_commit 才拒绝
    }
    _load_ids_to_write_dep[load_id] = put_block_dep; // L206：同 key 覆盖
    group_commit_load_count.fetch_add(1);            // L207：loads 计数
    return Status::OK();
}

// 内部 consumer 的 eos 判据
Status LoadBlockQueue::get_block(...) {
    ...
    if (_block_queue.empty() && _need_commit && _load_ids_to_write_dep.empty()) {
        *eos = true;                                 // L163：靠 map 空判断"没有更多数据"
    }
    ...
}

// add_block：没有任何 _need_commit / 已提交 的拦截
Status LoadBlockQueue::add_block(...) {
    std::unique_lock l(mutex);
    RETURN_IF_ERROR(status);                         // 只查 status
    if (UNLIKELY(runtime_state->is_cancelled())) ... // 只查 cancel
    ...
    _block_queue.emplace_back(block);                // 直接入队，返回 OK
    return Status::OK();
}
```

而 sink 端（`be/src/exec/operator/group_commit_block_sink_operator.cpp`）每个 RPC 的顺序是：
先 `add_load_id`（在 `_initialize_load_queue` / `get_first_block_load_queue` 里）→ `_add_block`
→ `wind_up` 里 `remove_load_id`。

### 3. remove / add 之间的"空窗"导致 consumer 早提交

把 id=2、id=3 两个共用 load_id `352a1cb9` 的并发 RPC 展开：

1. id=2 `add_load_id(352a1cb9)` → map = {352a1cb9}，count=1
2. id=2 sink：`add_block(B2)` 入队
3. id=2 sink `wind_up`：`remove_load_id(352a1cb9)` → **map 变空**
4. **空窗**：此刻 consumer 跑 `get_block`：B2 已消费、queue 空；`_need_commit` 因 40ms 时间条件置真；
   `_load_ids_to_write_dep` 空 → **eos=true → 提交 txn，rows=1**
5. id=3 这边随后 `add_load_id(352a1cb9)`（count 自增到 2）、`add_block(B3)` —— 但队列已经提交完毕
6. `add_block` 不查 `_need_commit`，B3 被静默入队、返回 OK；id=3 的 `remove_load_id` 返回 NotFound 被忽略

因为 `_load_ids_to_write_dep` 以 load_id 为 key，两个 RPC 共用同一个 key，
**任何一个 RPC 先 `remove_load_id` 就会清空整张表**，让队列在还有在途 RPC 的情况下显得"无人写入"。
consumer 的 eos 判据 `_load_ids_to_write_dep.empty()` 本意是"没有更多数据要写了"，
但在"多个独立 RPC 喂同一个队列、且共用 load_id"时这个假设不成立。

f243ad88 同样复用 `352a1cb9`、同样 3 个 RPC，没丢只是因为它三次 `add_block` 的时序**碰巧**都赶在
consumer eos 之前——纯属 race 的运气，不是逻辑保证。

## 与旧版分析的对照（旧结论错误）

| 旧分析结论 | 实际情况 |
|---|---|
| step 3 的 id=5,6 丢失 | ❌ step3 两行都进 f243ad88 并正确提交（rows=3） |
| step 3 fragment 启动了但没有 add_block | ❌ 6 行全部有 add_block |
| 竞态发生在 f243ad88 / step3 的 VNodeChannel 关闭 | ❌ f243ad88 完好；竞态在 67443e4a |
| FE 收到 loaded_rows=0 | ❌ step3 返回 `[1,1]`、reuse=true |
| 丢的是最后一个 batch | ❌ 丢的是 step 2 的 id=3 |
| "add_block 在队列提交期间被静默丢弃 / 返回 OK 让 FE 误以为成功" | ⚠️ 机制方向大致正确，但 label、丢的行、触发条件均错 |

旧分析被 FE 返回的 `label=f243ad88` 误导，以为 step 2/step 3 的数据都在 f243ad88，
没有意识到 BE 把一个 batch 的多行拆进了 67443e4a 和 f243ad88 两个队列，因此没发现 67443e4a 的
`2 loads / rows=1` 这条决定性证据。

## 修复方向

1. **`add_block` 增加 `_need_commit` / 已提交拦截**：若队列已 `_need_commit`（或已进入提交流程），
   拒绝入队并返回可重试错误，让 FE 换队列/重试，而不是静默入队返回 OK。这能直接消除"返回成功但丢数据"。

2. **eos 判据不能只看 `_load_ids_to_write_dep.empty()`**：reuse plan 下需要真正的"在途 RPC 计数"
   （而不是被 load_id 去重的 map size），保证只要还有 RPC 声明要往这个 plan 写，consumer 就不能提交。

3. **根因层面：reuse plan 路径不应让多个并发 RPC 共用同一个 load_id 作为 `_load_ids_to_write_dep`
   的 key**。map 覆盖 + 单次 remove 清空，是这个 race 的放大器；为每个 RPC 用独立 key（或引用计数）
   可消除 remove/add 之间的空窗。

## 附件

### 关键日志行（be.INFO.log.20260601-232627）

```
858377  13.864683  create wal ... 1693897897984_group_commit_67443e4a...
858382  13.865404  add block rows=1, use group_commit label=group_commit_67443e4a...   (B2)
858465  13.917099  VNodeChannel ... 67443e4a ... mark closed, left pending batch size: 1
858466  13.918978  add block rows=1, use group_commit label=group_commit_67443e4a...   (B3 丢失)
858729  14.059665  finish group commit ... label=group_commit_67443e4a... includes 2 loads ... rows=1
```

### 日志路径

```
/Users/meiyi/log/prepare/63965_0eecfe63d234701b785ff94f0dea8dec3347cf25_20260602002625_doris_logs/
├── be/log/be.INFO.log.20260601-232627   # 主要证据
├── fe/log/fe.log.20260601-1
└── regression-test/log/doris-regression-test.20260601.232905.log
```

### 相关代码

```
be/src/load/group_commit/group_commit_mgr.cpp
  - LoadBlockQueue::add_block         (L50-114)  无 _need_commit 拦截
  - LoadBlockQueue::get_block         (L116-177) eos 判据 L163
  - LoadBlockQueue::add_load_id       (L199-209) map 以 load_id 为 key，L206 覆盖
  - LoadBlockQueue::remove_load_id    (L179-192)
  - GroupCommitTable::get_first_block_load_queue (L243-305) reuse(contain_load_id) + add_load_id
be/src/exec/operator/group_commit_block_sink_operator.cpp
  - sink / wind_up                    (L283-392) add_block 后 remove_load_id

### 相关测试文件
regression-test/suites/insert_p0/insert_group_commit_with_prepare_stmt.groovy
regression-test/data/insert_p0/insert_group_commit_with_prepare_stmt.out
```
