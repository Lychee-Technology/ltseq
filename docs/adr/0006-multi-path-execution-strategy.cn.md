# ADR 0006: 有意为之的多路径（混合）执行策略

- 状态：已采纳（Accepted）
- 日期：2026-07-26（补记；决策早于本 ADR）

[English](0006-multi-path-execution-strategy.md)

## 背景

LTSeq 不是 DataFusion 的纯包装。有些操作能干净地映射到原生逻辑计划；有些分组/窗口式变换用原生表达很别扭；还有些序列工作负载（有序搜索、漏斗匹配、排序数据上的 as-of join）用专用算法远胜通用规划器。设计教训 §7.3："DataFusion 很强，但并非万能。"

## 决策

维持**三条并存的执行策略**，按问题选择——"这是有意的混合设计，而非不一致"：

1. **DataFusion 原生优先（默认）** — 原生逻辑计划与表达式。
2. **SQL fallback** — 对原生表达别扭的分组/窗口式变换，生成 SQL + 临时表；明确定位为"兼容与实现便利层"（`transpiler/sql_gen.rs`）。
3. **专用序列路径** — 自定义算法胜过通用规划的场景用专用 Rust 实现：线性扫描（`src/ops/linear_scan.rs`）、并行扫描（`src/ops/parallel_scan.rs`）、as-of join 二分搜索（`src/ops/asof_join.rs`）、连续行模式/漏斗匹配（`src/ops/pattern_match.rs`）。

排序元数据参与这一选择：`LTSeqTable` 携带可选的源 Parquet 路径，使已排序的 Parquet 输入可以走直接扫描快速路径而非完整规划（[ADR 0008](0008-explicit-sort-metadata.cn.md)）。

## 影响与取舍

- 有序搜索（可二分的 `search_first`、`search_pattern`/`search_pattern_count`）与 as-of join 获得了通用计划无法表达的算法级收益——这是序列模型价值主张的核心部分。
- 两个长期存在的判断题被列为压力点：何时专用执行相对 DataFusion 原生计划是合理的；以及防止 SQL fallback 变成意外的物化黑洞（[ADR 0005](0005-no-materialization-rule.cn.md)）。
- 基准层的存在部分正是为了用实证回答这些问题（[ADR 0015](0015-tests-benchmarks-as-architecture.cn.md)）。

## 来源

- `docs/ARCHITECTURE.md` — Multi-Path Execution Strategy、设计目标 #5、架构风险
- `docs/DESIGN_SUMMARY.cn.md` — §5.1–5.3、§7.3
- `docs/MODULE_GUIDE.md` — `src/ops/*` 导览
- `docs/api.cn.md` — §3.4（有序搜索）
