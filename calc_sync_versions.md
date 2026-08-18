# `calc_sync_versions` 当前逻辑

`calc_sync_versions` 根据 BE 请求携带的旧 Compaction 状态和 Meta Service 当前状态，补回可能已被 Compaction 重写的版本，再与请求区间合并。

缩写：BC = Base Compaction，CC = Cumulative Compaction，FC = Full Compaction，CP = Cumulative Point。

| 条件 | 补充区间 |
| --- | --- |
| `req_bc_cnt < bc_cnt` | `[0, cp - 1]` |
| 仅新增一次 CC、`req_cp < cp`，且 FC 次数未变 | `[req_cp, cp - 1]` |
| 发生 CC，但不满足上面的精确条件 | `[req_cp, INT64_MAX - 1]` |

要点：

- FC 不单独产生区间；FC 次数变化时不能使用单次 CC 的精确范围，必须保守返回到最大版本。
- 始终加入原请求区间 `[start_version, end_version]`；负的 `end_version` 会先转成 `INT64_MAX - 1`。
- 所有重叠或相邻区间会被合并，结果按起始版本排序。
- 调用方先校验请求中的 BC、CC、FC 次数和 CP 不大于当前值。
- 返回区间随后用于读取 rowset meta；普通 KV 和 versioned read 使用相同的版本计算结果。
