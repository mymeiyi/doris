# `calc_sync_versions` 逻辑梳理

核心思路：在原请求版本区间之外，补充因 Compaction 被重写、需要重新同步的版本区间，最后合并重叠或相邻区间。

```mermaid
flowchart TD
    A["输入：BE 保存的 req_*、MS 当前的 *、请求区间 [start,end]"]
    A --> B{"发生过 BC？<br/>req_bc_cnt < bc_cnt"}
    B -- 是 --> B1["加入 [0, cp-1]"]
    B -- 否 --> C
    B1 --> C

    C{"发生过 CC？<br/>req_cc_cnt < cc_cnt"}
    C -- 否 --> D
    C -- 是 --> C1{"仅发生 1 次 CC<br/>且 CP 前移<br/>且没有 FC？"}
    C1 -- 是 --> C2["加入精确区间<br/>[req_cp, cp-1]"]
    C1 -- 否 --> C3["无法精确判断<br/>加入 [req_cp, MAX]"]
    C2 --> D
    C3 --> D

    D["加入原请求区间 [start,end]"]
    D --> E["合并重叠/相邻区间"]
    E --> F["按起始版本排序并返回"]
```

关键规则：

- **BC（Base Compaction）发生**：`cp` 之前的数据可能被整体重写，补 `[0, cp-1]`。
- **CC（Cumulative Compaction）发生**：
  - 仅一次 CC、`cp` 前移、且无 Full Compaction：可精确补 `[req_cp, cp-1]`。
  - 其他情况无法确定影响边界：保守补 `[req_cp, MAX]`，避免漏版本。
- 最终结果是：`压缩影响区间 ∪ 用户请求区间`。

调用方在进入该函数前已校验 `req_* <= 当前值`；返回区间随后用于读取并返回对应的 rowset meta。
