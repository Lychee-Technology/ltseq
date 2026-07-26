# ADR 0009: 双 Schema 模型 —— Python `_schema` + Rust Arrow Schema

- 状态：已采纳（Accepted）
- 日期：2026-07-26（补记；决策早于本 ADR）

[English](0009-dual-schema-model.md)

## 背景

表达式捕获和良好的报错需要在*Python 侧*、在任何东西跨越边界之前就掌握 schema（例如校验 `r.age` 引用的是真实存在的列）。而执行需要*Rust 侧*与当前计划绑定的 Arrow schema。

## 决策

两侧都跟踪。Python 层维护面向用户的 `_schema` dict，用于快速校验与更好的错误信息；Rust 层跟踪当前计划的 Arrow schema。两者都暴露给用户（`schema` 与 `python_schema` 属性）。

## 影响与取舍

- 表达式捕获与用户体验大幅变好——"列不存在"之类的错误在 Python 侧立即以有用的信息浮出，而不是深埋在计划执行内部。
- 文档明言的硬性要求：**在每个计划形状变化的边界都必须维护 schema 同步**——列名冲突的 join、linked table 物化、分组变换、select/derive。被记录为设计教训 §7.2（"schema 同步是关键"），并列为 #1 长期架构压力点。
- 一条贡献者启发式将其固化："保持 Python `_schema` 与 Rust schema 行为一致"（见 [ADR 0015](0015-tests-benchmarks-as-architecture.cn.md)）。
- join 冲突列"激进改名再 alias 回"的策略（[ADR 0011](0011-link-lazy-prefix-aliased-join.cn.md)）在很大程度上正是为了让这种同步可预期。

## 来源

- `docs/ARCHITECTURE.md` — Schema Management、Architectural Risks
- `docs/DESIGN_SUMMARY.cn.md` — §1.5、§7.2
- `docs/MODULE_GUIDE.md` — Contributor Heuristics
- `docs/api.cn.md` — §1（`schema` / `python_schema`）
