# ADR 0008: 显式排序元数据（宁可报错，不做猜测）

- 状态：已采纳（Accepted）
- 决策日期：早于本记录 · 记录日期：2026-07-26

[English](0008-explicit-sort-metadata.md)

## 背景

序列操作（`shift`、`diff`、`rolling`、`pct_change`、累计类操作、`group_ordered`、merge join）在行序未知时没有定义。早期让顺序语义隐式化的尝试“导致了令人困惑的结果和脆弱的 API”（设计教训 §7.1）。

## 决策

显式跟踪排序状态，并且**从不隐式推断顺序**。声明顺序由 Rust 内核持有（`sort_specs`），Python 的 `_sort_keys` 经 FFI 读取（[ADR 0009](0009-metadata-single-source-of-truth.cn.md)）。顺序未知时，LTSeq 宁可报错（`SortRequiredError`）也不静默产出误导性结果。如果结果依赖顺序，顺序就应该出现在代码里。

子决策：

- **`assume_sorted(*keys, desc=...)` 逃生口。** 声明排序而不做物理排序，用于已预排序的输入（如已排序的 Parquet），否则要为解锁窗口/merge join 支付一次冗余排序。这是显式的信任契约：正确性由调用方负责，错误的元数据产出错误的结果。基准测试依赖它在计时轮之外声明已知顺序。实现注记：只有 Parquet 路径是纯元数据操作；非 Parquet 数据源上 `assume_sorted()` 当前会 collect 并重建带排序信息的 `MemTable`（`src/ops/sort.rs`，见 [ADR 0005](0005-no-materialization-rule.cn.md)）。
- **计算键会物理排序，但不计入声明顺序。** `sort_keys` 在第一个计算键处截断：`sort("a", lambda r: r.b*2, "c")` 只声明 `[a]`；单独的计算键什么都不声明（因此 `cum_sum` 仍抛 `SortRequiredError`）。文档给出的替代写法：`.derive(k=...).sort("k")`。已接受的取舍：截断之后，在声明前缀上并列的行顺序不作保证，窗口执行可能重排并列行。
- **排序元数据是语义，不只是优化。** 部分操作会保持它（许多 filter/derive/slice）；重排或结构全新的表会作废它。它还解锁 sorted-Parquet 直扫与线性/并行扫描快速路径（[ADR 0006](0006-multi-path-execution-strategy.cn.md)）。
- **自带顺序的窗口是例外。** 只有依赖表序退路的窗口表达式才要求声明表级排序。排名函数一律使用 `.over()`；且自 [ADR 0013](0013-window-over-unification.cn.md) 起，显式给出 `.over(order_by=...)` 的序列窗口同样自足，可以直接在未排序的表上运行。

## 影响与取舍

- 结果可信、查询自我说明；merge join 可以校验其前置条件。
- 在众多操作间正确保持排序元数据是一个已具名的长期压力点。
- `assume_sorted` 按设计把正确性责任转移给调用方。

## 来源

- `docs/ARCHITECTURE.cn.md`: 排序元数据与顺序语义
- `docs/DESIGN_SUMMARY.cn.md`: §3.1–3.3、§7.1
- `docs/USER_MODEL.cn.md`: 核心心智模型 #3、为什么排序必须显式写出
- `docs/api.cn.md`: §0、`LTSeq.sort`、`assume_sorted`、常见错误表
- `README.md`: Limitations、FAQ
