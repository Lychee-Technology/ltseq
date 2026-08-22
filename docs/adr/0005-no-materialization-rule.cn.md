# ADR 0005: 面向关系变换的 No-Materialization 规则

- 状态：已采纳（Accepted）
- 决策日期：早于本记录 · 记录日期：2026-07-26

[English](0005-no-materialization-rule.md)

## 背景

基准与设计经验表明，物化是"最主要的架构成本中心"：最重要的性能问题往往不来自底层计算内核，而来自复杂操作周边的 collect/register/re-query 模式（设计教训 §7.4）。没有硬性规则，图省事的物化（在查询 API 内部经由 pandas/Arrow/Python 行往返）会悄悄累积。

## 决策

**返回 `LTSeq`、`NestedTable`、`LinkedTable`、`PartitionedTable` 的普通关系变换 API（filter/select/derive/sort/slice/join/group/link）必须留在惰性的 Rust/DataFusion 查询路径上。** 这些 API 内部禁止 `to_pandas()`、`to_arrow()`、`from_arrow()`、`from_pandas()`、`_from_rows()`、Arrow 往返或逐行 Python。物化只保留给显式的导出/终端/构造 API（`to_pandas()`、`to_arrow()`、`to_dicts()`、`collect()`、`from_arrow()`、`from_pandas()`）。

### 已记录的 eager 边界

该规则并不（也无法）覆盖所有返回表的 API。以下操作因设计或当前实现而物化：

**正确性所需的例外**（记录于 `CLAUDE.md`；不是捷径）：

1. **物理位置操作**（`rvs`、`step`、带键 `distinct`）在分配行位置前，先把表快照为单个按序分区（collect → read_batch），因为对惰性多分区计划做无序/分区窗口不会保持输入顺序（`set_ops.rs::snapshot_single_partition`）。
2. **`fold()`** 逐行运行用户提供的 Python 回调 `fn(state, row)`；任意 Python 无法表达为 DataFusion 计划，逐行路径（`to_dicts()` → 累加 → `_from_rows()`）是固有属性。docstring 标明这是非惰性慢路径（对比 Polars `cumulative_eval`）。

**当前实现中的其他 eager 路径**（专用算法或实现现状，截至本记录时）：

- **非 Parquet 的 `assume_sorted()`** 会 collect 批次并用 `with_sort_order()` 重建 `MemTable`（`src/ops/sort.rs`）；只有 Parquet 路径是纯元数据操作。
- **`asof_join()`** 收集左右两侧输入以运行其专用匹配算法（`src/ops/asof_join.rs`）。
- **`pivot()`** 收集 distinct 透视键（并执行聚合）以构造输出 schema（`src/ops/pivot.rs`）。
- **Mutation 类 API**（`insert`/`delete`/`update`/`modify`）在调用时收集全表（`src/ops/mutation.rs`），见 [ADR 0004](0004-lazy-execution-immutable-tables.cn.md)。
- **`search_pattern`** 收集数据以运行顺序匹配器（`src/ops/pattern_match.rs`）。

这些路径都在 Rust 侧（Arrow 批次，无 Python 行往返），但它们确实终结了计划的惰性；组合流水线时若把它们当作惰性会错估成本。

## 影响与取舍

- 对被覆盖的关系变换而言，性能保持可预期，DataFusion 保有整计划优化空间。
- 该规则反过来约束其他 API 设计：`partition(by=...)` 只接受简单列表达式，因为派生表达式的可调用对象会被迫物化（见 [ADR 0010](0010-four-table-object-types.cn.md)）。
- **强制手段是部分的。** `py-ltseq/tests/test_no_materialization_rule.py` 是源码扫描式守卫：对选定的 `src/ops/` 模块 grep SQL/MemTable 标记（`session.sql`、`.sql(&`、`MemTable::try_new`），并放行已记录的例外。它**不**检测普通 `.collect()` 调用：它专门防的是 SQL 往返模式，而非一切 eager 执行。维护上面这份 eager 边界清单是文档义务，CI 并不校验。
- 一个持续存在的压力点：防止 SQL 式 fallback 路径变成意外的物化黑洞（见 [ADR 0006](0006-multi-path-execution-strategy.cn.md)）。

## 来源

- `CLAUDE.md`: No Materialization Rule（两个正确性例外）
- `docs/ARCHITECTURE.cn.md`: No-Materialization Rule、设计目标 #4
- `docs/DESIGN_SUMMARY.cn.md`: §5.4、§7.4
- `docs/USER_MODEL.cn.md`: 物化模型
- `docs/api.cn.md`: §3.2 `fold`
- 代码：`src/ops/sort.rs`、`src/ops/asof_join.rs`、`src/ops/pivot.rs`、`src/ops/mutation.rs`、`src/ops/pattern_match.rs`、`py-ltseq/tests/test_no_materialization_rule.py`
