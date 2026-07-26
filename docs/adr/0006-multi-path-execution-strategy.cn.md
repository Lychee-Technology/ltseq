# ADR 0006: 有意为之的多路径（混合）执行策略

- 状态：已采纳（Accepted）
- 决策日期：早于本记录 · 记录日期：2026-07-26

[English](0006-multi-path-execution-strategy.md)

## 背景

LTSeq 不是 DataFusion 的纯包装。有些操作能干净地映射到原生逻辑计划；有些序列工作负载（漏斗匹配、as-of join、物理位置操作）用专用算法远胜通用规划器。设计教训 §7.3："DataFusion 很强，但并非万能。"

## 决策

维持**两条并存的执行策略**，按问题选择——"这是有意的混合设计，而非不一致"：

1. **DataFusion 原生优先（默认）** — 原生逻辑计划与表达式，含原生窗口构建（`transpiler/window_native.rs`）。
2. **专用序列路径** — 自定义算法胜过通用规划的场景用专用 Rust 实现：线性扫描（`src/ops/linear_scan.rs`）、并行扫描（`src/ops/parallel_scan.rs`）、as-of join 匹配（`src/ops/asof_join.rs`）、连续行模式/漏斗匹配（`src/ops/pattern_match.rs`）。这些路径需要 collect 输入才能运行——它们属于 [ADR 0005](0005-no-materialization-rule.cn.md) 中已记录的 eager 边界。

SQL 的使用只留下一处有意的残余：`filter_where`（`src/ops/aggregation.rs`）对**空表**调用 `session.sql()`，纯粹当 WHERE 子句解析器用，然后把解析出的原生表达式应用到惰性的 `DataFrame::filter()`——这是"把解析器当库用"的 helper，不是数据执行 fallback。

排序元数据参与策略选择：`LTSeqTable` 携带可选的源 Parquet 路径，使已排序的 Parquet 输入可以走直接扫描快速路径而非完整规划（[ADR 0008](0008-explicit-sort-metadata.cn.md)）。

### 演进：已退役的 SQL fallback 路径

早期存在第三条策略：生成 SQL + 临时表（`transpiler/sql_gen.rs`），作为对原生表达别扭的分组/窗口式变换的"兼容与实现便利层"。该路径已被移除——如今 `src/transpiler/` 只有 `mod.rs`、`window_native.rs`、`optimization.rs`——移除的原因正是 SQL 往返（`collect → MemTable → session.sql() → collect`）是物化黑洞；`test_no_materialization_rule.py` 现在专门防止该模式回归。`ARCHITECTURE.md`/`DESIGN_SUMMARY.md` 仍在描述三路径版本，在这一点上已经过时。

## 影响与取舍

- 有序/顺序算子（`search_pattern`/`search_pattern_count`、`asof_join`）获得了通用计划无法表达的算法级收益。注意 `search_first` **不在**其中：尽管旧文档声称二分搜索，它当前的实现是原生惰性的 `filter(...).limit(1)`（`src/ops/basic.rs`），无排序要求；排序数据上的二分快速路径仍是愿景，不是实现。
- 一个长期的判断题被列为压力点：何时专用执行相对 DataFusion 原生计划是合理的。
- 基准层的存在部分正是为了用实证回答这些问题（[ADR 0015](0015-tests-benchmarks-as-architecture.cn.md)）。

## 来源

- `docs/ARCHITECTURE.md` — Multi-Path Execution Strategy（已过时：仍在描述 SQL fallback 路径）、设计目标 #5
- `docs/DESIGN_SUMMARY.cn.md` — §5.1–5.3、§7.3
- `docs/MODULE_GUIDE.md` — `src/ops/*` 导览
- 代码：`src/transpiler/`、`src/ops/aggregation.rs`（`filter_where`）、`src/ops/basic.rs`（`search_first`）、`py-ltseq/tests/test_no_materialization_rule.py`
