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

## 根因：为什么 prepare 的 load_id 会在 BE 侧重复写入同一个队列

### 0. 先厘清一个常见误解

每条 insert 的生命周期是这样的（`group_commit_block_sink_operator.cpp`）：

- `sink(eos)` 里 `_add_block`（L393）先把 1 行缓冲进 `_blocks`（L139）；
- `wind_up()`（eos）调 `_add_blocks(state,true)`（L316）→ `add_block`（L216）**真正入队**；
- **紧接着 L319 `remove_load_id(_load_id)`**；
- `close()`（async 模式不等 consumer 提交）→ fragment 完成回调**才发响应**。

也就是说 **`group_commit_insert` 返回 FE 时，这条 load 的 block 已入队、load_id 已 remove**，
两件事都在 `sink(eos)` 内同步做完，早于 RPC 响应。

由此可以推出一个**重要结论**：如果执行是严格串行的（语句 N 完全做完、含 `remove_load_id`，语句 N+1
才开始），那么 `_load_ids_to_write_dep` 里**永远不会同时**出现两个相同 key——所以
**丢数据并不是"同一时刻 map 被同 key 覆盖"**（这是旧分析的措辞偏差）。真正的问题在**队列路由**。

### 1. reuse plan 下所有 insert RPC 共用同一个 load_id

server-side prepared statement 的 `reuse_group_commit_plan` 复用 step 1 创建并序列化好的执行计划字节，
`query_id` / `load_id` / `fragment_instance_id` 全部**烤死在复用的字节里**，所以
**所有 insert RPC 都跑在同一个 load_id `352a1cb9...` 下**（BE 全部 add_block 日志可证）。
复用字节是减少 FE 规划代价的关键优化，不能动；问题出在 BE 把这个共享 load_id 当成了**队列路由 key**。

### 2. `_load_ids_to_write_dep` 以 load_id 为 key：两个共享 load_id 的 load 在 map 里塌缩成一个

`be/src/load/group_commit/group_commit_mgr.cpp`：

```cpp
// add_load_id：以 load_id 为 key 写入 map；计数自增
Status LoadBlockQueue::add_load_id(const UniqueId& load_id, ... put_block_dep) {
    std::unique_lock l(mutex);
    if (_need_commit) {                               // L235：已 need_commit 才拒绝
        return Status::InternalError(...);
    }
    _load_ids_to_write_dep[load_id] = put_block_dep;  // L239：同 key 覆盖，map 仍只有 1 个 entry
    group_commit_load_count.fetch_add(1);             // L240：但 loads 计数照样自增
    return Status::OK();
}

// remove_load_id：按 load_id erase 整个 entry
Status LoadBlockQueue::remove_load_id(const UniqueId& load_id) {
    std::unique_lock l(mutex);
    if (_load_ids_to_write_dep.find(load_id) != ...) {
        _load_ids_to_write_dep.erase(load_id);        // L216：一次 erase 清掉这个共享 key
        ...
    }
}

// 内部 consumer 的 eos 判据：靠 map 空判断"没有更多数据要写了"
if (_block_queue.empty() && _need_commit && _load_ids_to_write_dep.empty()) {
    *eos = true;                                      // L196
}

// add_block：不查 _need_commit，直接入队返回 OK
```

`_load_ids_to_write_dep` 是 `std::map<UniqueId, dep>`。两条共享同一个 load_id `X` 的 load 同时往同一个队列
`add_load_id(X)` 时：**map 里只有一个 entry `X`（第二次只覆盖 dep），但 `group_commit_load_count` 自增到 2**
（这正是 67443e4a 日志里 `includes 2 loads` 的来源）。于是 **两个在途 load 在 map 里塌缩成了一个 key**，
而 **任何一条 load 先 `remove_load_id(X)`，就把这个共享 entry 整个 erase 掉**，连带把另一条还在途的 load
的登记也一并清掉。consumer 的 eos 判据 `_load_ids_to_write_dep.empty()` 本意是"没有在途写入了"，
在共享 load_id 时这个前提不成立。

### 3. 把 id=2 / id=3 展开：一次 remove 清空共享 key → consumer 早提交 → B3 丢失

1. id=2 `add_load_id(X)` → map={X}，count=1
2. id=3 `add_load_id(X)`（与 id=2 **重叠**，同一队列 67443e4a）→ map 仍={X}（覆盖 dep），**count=2**
3. id=2 `add_block(B2)` 入队；consumer 开始消费 B2
4. id=2 `wind_up` `remove_load_id(X)` → **erase 共享 key → map 变空**，但 **id=3 还在途**（尚未 add_block）
5. consumer `get_block`：B2 已消费、queue 空；`_need_commit`（40ms 到）；map 空 → **eos=true → 提交 txn，rows=1**
6. id=3 `add_block(B3)`：`add_block` 不查 `_need_commit`，B3 被塞进**已经提交**的队列、返回 OK；
   B3 永远不会被消费 → **丢失**。id=3 随后的 `remove_load_id(X)` 返回 NotFound 被忽略

**所以丢数据不是"同一时刻同 key 覆盖数据"，而是"两条 load 共用一个 map key → 一次 remove 误清掉另一条的在途
登记 → consumer 误判无人写入而早提交"。**

> ⚠️ 触发的必要条件是 **id=2 / id=3 在 BE 队列层面有重叠**（id=3 的 `add_load_id` 早于 id=2 的 `remove`）。
> 前面"`group_commit_insert` 返回前已 remove"只保证了**严格串行**下不重叠——但 reuse plan + executeBatch
> 下，下一条 insert 的 BE fragment 会在上一条 `remove_load_id` 生效**之前**就到达 `add_load_id`。
> bug 的存在本身，就证明了这个重叠窗口客观存在；**共享 load_id 是把它放大成丢数据的唯一放大器**
> （独立 load_id 时两条 load 是 map 里两个 entry，一条 remove 不影响另一条）。

> 附带还有一个同源弱点：`get_first_block_load_queue` 的**第一个循环**（L284-289）仅凭 `contain_load_id`
> 复用队列，**既不 `add_load_id` 也不查 `_need_commit`**，理论上能把一个正在提交的队列原样返回给后来者。
> 本 case 的 `2 loads` 走的是 `add_load_id`（第二循环）路径、对应上面的塌缩机制；第一个循环是另一条
> 需要更强重叠才会踩中的路径。两者的共同根因都是 **load_id 被当作跨 load 的队列路由/登记 key**。

f243ad88 同样复用 `X`、同样多个 RPC，没丢只是因为它的 `add_block` / `remove` 时序**碰巧**都没踩中这个
"remove 早于另一条在途 load"的边界——纯属 race 的运气，不是逻辑保证。

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

### 采用方案：每个 sink 无条件重新生成独立 load_id（已验证）

在 `GroupCommitBlockSinkOperatorX::init` 里、`_load_id = table_sink.load_id` 之后，
**无条件 `_load_id = UniqueId::gen_uid()`**，让每个 group commit sink 拿到唯一 load_id：

```cpp
_load_id = table_sink.load_id;
UniqueId origin_load_id = _load_id;
_load_id = UniqueId::gen_uid();   // 每个 sink 独立 load_id，杜绝跨 load 复用同一队列路由 key
LOG(INFO) << "group commit sink regenerate load_id, origin=" << origin_load_id.to_string()
          << ", new=" << _load_id.to_string();
```

为什么能根治：

- 第一个循环 `contain_load_id(load_id)` **跨 load 永远命不中**（每个 load_id 唯一，只命中自己），
  那条"凭残留注册认领到正在提交的队列、且无 `_need_commit` 守卫"的危险路径被彻底关闭。
- N+1 只能走**第二个循环**（mutex 保护、有 `_need_commit` 守卫）或自建新队列。无论是否重叠，
  都不可能把一条新 load 挂进正在提交的队列。重叠窗口消失。

注意点：

- **不破坏 FE 复用序列化字节的优化**：load_id 只在 BE sink 侧重写，FE 的 `query_id` / 计划字节 / label
  / txn 都不变，规划代价照样省。
- **stream load 不受影响**：stream load 本来就用唯一 load_id，无条件 regenerate 对它无害，无需区分导入类型。
- **唯一代价**：BE 日志里 sink 的 load_id 和 FE 的 query_id 不再一致，观察时需用上面那行 origin→new 日志
  或 label/txn 对账（label/txn/query_id 均不变，可观测性损失有限）。

### 为什么没采用其它方案

- *`get_first_block_load_queue` 增加 `check_load_id_not_exist` 参数*：只在登记瞬间查一次，挡不住第一个循环
  这种"凭残留注册认领待提交队列"的路径；且 `_create_plan_deps` 用 `emplace`（同 key no-op），同 load_id
  在建计划窗口里第二个 dep 会被丢掉而 hang。不彻底。
- *`add_block` 增加 `_need_commit` 拦截 / eos 改用在途 RPC 引用计数*：能堵住症状，但治标；根因是
  **load_id 被当作队列路由 key 跨 load 复用**，从源头给每个 sink 独立 load_id 最简单、最彻底。

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
