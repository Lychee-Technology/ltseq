# ADR 0005: No-Materialization 规则（含两个已记录的例外）

- 状态：已采纳（Accepted）
- 日期：2026-07-26（补记；决策早于本 ADR）

[English](0005-no-materialization-rule.md)

## 背景

基准与设计经验表明，物化是"最主要的架构成本中心"：最重要的性能问题往往不来自底层计算内核，而来自复杂操作周边的 collect/register/re-query 模式（设计教训 §7.4）。没有硬性规则，图省事的物化（在查询 API 内部经由 pandas/Arrow/Python 行往返）会悄悄累积。

## 决策

**任何返回 `LTSeq`、`NestedTable`、`LinkedTable` 或 `PartitionedTable` 的 API 必须留在惰性的 Rust/DataFusion 查询路径上。** 返回表对象的查询 API 内部禁止 `to_pandas()`、`to_arrow()`、`from_arrow()`、`from_pandas()`、`_from_rows()`、Arrow 往返或逐行 Python。物化只保留给显式的导出/终端/构造 API（`to_pandas()`、`to_arrow()`、`to_dicts()`、`collect()`、`from_arrow()`、`from_pandas()`）。

该规则由专门测试强制执行——`py-ltseq/tests/test_no_materialization_rule.py`——"不只是约定"。

### 已记录的例外（属正确性要求，不是捷径）

1. **物理位置操作**（`rvs`、`step`、带键 `distinct`）在分配行位置之前，先把表快照为单个按序分区（collect → read_batch）——对惰性多分区计划做无序/分区窗口不会保持输入顺序，快照是正确性所必需（`set_ops.rs::snapshot_single_partition`）。
2. **`fold()`** 逐行运行用户提供的 Python 回调 `fn(state, row)` 来串联顺序状态。任意 Python 无法表达为 DataFusion 计划，因此逐行路径（`to_dicts()` → 累加 → `_from_rows()`）是该操作的固有属性。其 docstring 标明这是非惰性慢路径（对比 Polars `cumulative_eval`）。

## 影响与取舍

- 性能保持可预期，DataFusion 保有整计划优化空间。
- 该规则反过来约束其他 API 设计：`partition(by=...)` 只接受简单列表达式，因为派生表达式的可调用对象会被迫物化（见 [ADR 0010](0010-four-table-object-types.cn.md)）。
- 一个持续存在的压力点："防止 SQL fallback 路径变成意外的物化黑洞"（见 [ADR 0006](0006-multi-path-execution-strategy.cn.md)）。
- 注：截至撰写本 ADR 时，上述两个例外仅记录在 `CLAUDE.md`（及 `fold` 的 docstring/api.md）中——本 ADR 现在是它们在 `docs/` 内的正式归档处。

## 来源

- `CLAUDE.md` — No Materialization Rule（例外清单此前唯一的出处）
- `docs/ARCHITECTURE.cn.md` — No-Materialization Rule、设计目标 #4
- `docs/DESIGN_SUMMARY.cn.md` — §5.4、§7.4
- `docs/USER_MODEL.cn.md` — 物化模型
- `docs/api.cn.md` — §3.2 `fold`
- `py-ltseq/tests/test_no_materialization_rule.py`
