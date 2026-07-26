# ADR 0008: 显式排序元数据 —— 宁可报错，不做猜测

- 状态：已采纳（Accepted）
- 日期：2026-07-26（补记；决策早于本 ADR）

[English](0008-explicit-sort-metadata.md)

## 背景

序列操作（`shift`、`diff`、`rolling`、`pct_change`、累计类操作、`group_ordered`、merge join）在行序未知时没有定义。早期让顺序语义隐式化的尝试"导致了令人困惑的结果和脆弱的 API"（设计教训 §7.1）。

## 决策

在边界两侧显式跟踪排序状态——Python 侧 `_sort_keys`，Rust 侧 `sort_exprs`——并且**从不隐式推断顺序**。顺序未知时，LTSeq 宁可报错（`SortRequiredError`）也不静默产出误导性结果。如果结果依赖顺序，顺序就应该出现在代码里。

子决策：

- **`assume_sorted(*keys, desc=...)` 逃生口。** 只声明排序元数据、不做物理排序——用于已预排序的输入（如已排序的 Parquet），否则要为解锁窗口/merge join 支付一次冗余排序。这是显式的信任契约："正确性由调用方负责——错误的元数据产出错误的结果。"基准测试依赖它在计时轮之外声明已知顺序。
- **计算键会物理排序，但不计入声明顺序。** `sort_keys` 在第一个计算键处截断：`sort("a", lambda r: r.b*2, "c")` 只声明 `[a]`；单独的计算键什么都不声明（因此 `cum_sum` 仍抛 `SortRequiredError`）。文档给出的替代写法：`.derive(k=...).sort("k")`。已接受的取舍：截断之后，在声明前缀上并列的行顺序不作保证——窗口执行可能重排并列行。
- **排序元数据是语义，不只是优化。** 部分操作会保持它（许多 filter/derive/slice）；重排或结构全新的表会作废它。它还解锁 sorted-Parquet 直扫与线性/并行扫描快速路径（[ADR 0006](0006-multi-path-execution-strategy.cn.md)）。
- **排名函数是例外**：它们使用 `.over()`，不要求前置 `.sort()`（见 [ADR 0013](0013-window-over-unification.cn.md)）。

## 影响与取舍

- 结果可信、查询自我说明；merge join 可以校验其前置条件。
- 在众多操作间正确保持排序元数据是一个已具名的长期压力点。
- `assume_sorted` 按设计把正确性责任转移给调用方。

## 来源

- `docs/ARCHITECTURE.cn.md` — 排序元数据与顺序语义
- `docs/DESIGN_SUMMARY.cn.md` — §3.1–3.3、§7.1
- `docs/USER_MODEL.cn.md` — 核心心智模型 #3、为什么排序必须显式写出
- `docs/api.cn.md` — §0、`LTSeq.sort`、`assume_sorted`、常见错误表
- `README.md` — Limitations、FAQ
