# ADR 0003: 以 DataFusion + Apache Arrow 为执行底座

- 状态：已采纳（Accepted）
- 决策日期：早于本记录 · 记录日期：2026-07-26

[English](0003-datafusion-arrow-substrate.md)

## 背景

Rust 内核需要一个查询规划/优化器和一种列式执行格式。自研引擎的工作量会远超产品本身的核心工作（序列语义）。

## 决策

基于 **Apache DataFusion 54.0**（SQL/计划引擎）与 **Apache Arrow**（列式内存格式）构建。每个 `LTSeqTable` 内部由 DataFusion 的 `SessionContext` 加惰性 `DataFrame` 持有逻辑计划。

## 理由

DataFusion 是"久经考验的 SQL 引擎"。留在其惰性计划路径上可免费获得向量化执行、零拷贝列式操作、filter/projection 下推与逻辑计划优化（这正是 [ADR 0005](0005-no-materialization-rule.cn.md) 的 No-Materialization 规则如此重要的原因）。

## 影响与取舍

- LTSeq 继承 DataFusion 的行为，包括它的 bug。已记录的案例：join 之后内存数据源（`from_pandas`/`from_arrow`）受 ProjectionPushdown bug 影响；文档给出的绕行方案是改从 CSV/Parquet 读取，或在 `collect()` 之后再选列（见 `LINKING_GUIDE.cn.md` 疑难排查）。
- DataFusion 明确*不*足以覆盖所有序列工作负载；这个缺口正是多路径执行策略（[ADR 0006](0006-multi-path-execution-strategy.cn.md)）的正当性来源，并被记录为设计教训 §7.3（"DataFusion 很强，但并非万能"）。
- 引擎升级（DataFusion/Arrow 大版本）是 `Cargo.toml` 中钉住的经常性维护成本（直接依赖的 `parquet` 必须与 DataFusion 的 Arrow 版本匹配）。

## 来源

- `docs/ARCHITECTURE.cn.md`: 概览、懒执行模型
- `docs/DESIGN_SUMMARY.cn.md`: §5.1、§7.3
- `docs/LINKING_GUIDE.cn.md`: 疑难排查（ProjectionPushdown 注意事项）
- `README.md`: Performance、Technology Stack
- `Cargo.toml`
