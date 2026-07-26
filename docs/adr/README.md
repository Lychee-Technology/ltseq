# Architecture Decision Records / 架构决策记录

This directory records the architectural decisions of LTSeq, distilled from the existing documentation in `docs/` (including the design specs under `docs/superpowers/specs/`). Each ADR exists in English (`NNNN-slug.md`) and Chinese (`NNNN-slug.cn.md`), following the repository's bilingual documentation convention.

本目录记录 LTSeq 的架构决策，内容提炼自 `docs/` 中的既有文档（含 `docs/superpowers/specs/` 下的设计规格）。每篇 ADR 有英文（`NNNN-slug.md`）与中文（`NNNN-slug.cn.md`）两个版本，沿用仓库的双语文档惯例。

## Conventions / 约定

- Template / 模板: Context → Decision → Alternatives Considered (only when the sources record them / 仅当源文档确有记载) → Consequences → Sources.
- Status lifecycle / 状态生命周期: `Proposed` → `Accepted` → `Superseded` / `Deprecated`. An ADR that replaces an earlier design carries a `Supersedes:` metadata line and records the replaced design in an Evolution section (see 0009, 0011); when a *whole ADR* is later replaced, mark it `Superseded` and add a `Superseded-by:` line pointing at its successor. / 状态取值：`Proposed` → `Accepted` → `Superseded` / `Deprecated`。取代旧设计的 ADR 带 `Supersedes:` 元数据行，并在"演进"小节记录被取代的设计（见 0009、0011）；当*整篇 ADR* 被后续决策取代时，改标 `Superseded` 并加 `Superseded-by:` 指向后继。
- Dates / 日期: `Decision date` is when the decision was made (design-spec date, issue implementation, or "predates this record" for back-filled entries); `Recorded` is when the ADR was written. / `Decision date`（决策日期）指决策做出的时间（设计规格日期、issue 实现时，或补记条目的"早于本记录"）；`Recorded`（记录日期）指 ADR 撰写时间。
- New decisions: add the next number, both languages, and a row below. / 新增决策：使用下一个编号，写两种语言，并在下表加一行。

## Index / 索引

| # | ADR | Summary / 摘要 |
|---|-----|----------------|
| 0001 | [Sequence-oriented data model](0001-sequence-oriented-data-model.md) · [中文](0001-sequence-oriented-data-model.cn.md) | Row order is a first-class part of the data model. / 行序是数据模型的一等公民。 |
| 0002 | [Rust core + Python surface](0002-rust-core-python-surface.md) · [中文](0002-rust-core-python-surface.cn.md) | Thin Python API over a Rust execution core via PyO3/maturin. / Python 薄表面 + Rust 执行内核，经 PyO3/maturin 绑定。 |
| 0003 | [DataFusion + Arrow substrate](0003-datafusion-arrow-substrate.md) · [中文](0003-datafusion-arrow-substrate.cn.md) | DataFusion 54.0 as plan engine, Arrow as columnar format. / 以 DataFusion 54.0 为计划引擎、Arrow 为列式格式。 |
| 0004 | [Lazy execution & immutable tables](0004-lazy-execution-immutable-tables.md) · [中文](0004-lazy-execution-immutable-tables.cn.md) | Lazy by default, explicit terminal boundaries, copy-on-write mutation, streaming Cursor. / 默认惰性、显式终端边界、写时复制、流式 Cursor。 |
| 0005 | [No-materialization rule](0005-no-materialization-rule.md) · [中文](0005-no-materialization-rule.cn.md) | Relational transforms stay on the lazy path; documented eager boundaries. / 关系变换必须留在惰性路径；含已记录的 eager 边界。 |
| 0006 | [Multi-path execution strategy](0006-multi-path-execution-strategy.md) · [中文](0006-multi-path-execution-strategy.cn.md) | DataFusion-first + specialized sequence paths; the SQL-fallback layer was retired. / 原生优先 + 专用序列路径；SQL fallback 层已退役。 |
| 0007 | [Lambda DSL via SchemaProxy](0007-lambda-dsl-schemaproxy.md) · [中文](0007-lambda-dsl-schemaproxy.cn.md) | Capture once, serialize to dicts, transpile in Rust; intentionally constrained. / 一次捕获、dict 序列化、Rust 转译；刻意受限。 |
| 0008 | [Explicit sort metadata](0008-explicit-sort-metadata.md) · [中文](0008-explicit-sort-metadata.cn.md) | Never infer order; fail with `SortRequiredError`; `assume_sorted` trust contract. / 从不推断顺序；宁抛 `SortRequiredError`；`assume_sorted` 信任契约。 |
| 0009 | [Metadata: Rust single source of truth](0009-metadata-single-source-of-truth.md) · [中文](0009-metadata-single-source-of-truth.cn.md) | Rust owns schema & sort metadata; Python caches/reads (supersedes the dual-schema model, #93). / Rust 持有 schema 与排序元数据，Python 只缓存/读取（取代双 schema 模型，#93）。 |
| 0010 | [Four table object types](0010-four-table-object-types.md) · [中文](0010-four-table-object-types.cn.md) | `LTSeq` / `NestedTable` / `LinkedTable` / `PartitionedTable` signal semantics by type. / 四种包装类型以类型表达语义。 |
| 0011 | [`link()` is a lazy prefix-aliased join](0011-link-lazy-prefix-aliased-join.md) · [中文](0011-link-lazy-prefix-aliased-join.cn.md) | Not a pointer/take structure; supersedes the earlier design; join surfaces & strategy matrix. / 非 pointer/take 结构；取代早期设计；含 join 表面与策略矩阵。 |
| 0012 | [Rust thin shell + Python mixins](0012-rust-thin-shell-python-mixins.md) · [中文](0012-rust-thin-shell-python-mixins.cn.md) | Single `#[pymethods]` delegating to `src/ops/*`; mixin-composed `LTSeq`; Rust coding standard. / 单 `#[pymethods]` 委托 `src/ops/*`；mixin 组合；Rust 代码标准。 |
| 0013 | [Unified `.over()` windows](0013-window-over-unification.md) · [中文](0013-window-over-unification.cn.md) | One window rule: default table order, `.over()` overrides; kwarg coexistence errors. (#117) / 窗口一句话规则：默认表序，`.over()` 覆盖；与 kwarg 并存报错。（#117） |
| 0014 | [`.pyi` stubs & typed surface](0014-pyi-stubs-typed-surface.md) · [中文](0014-pyi-stubs-typed-surface.cn.md) | Handwritten stubs (explicit + `__getattr__` fallback), flattened `LTSeq` stub, deprecated methods removed. (#8) / 手写 stubs（显式 + 兜底）、摊平的 `LTSeq` stub、移除废弃方法。（#8） |
| 0015 | [Tests & benchmarks as architecture](0015-tests-benchmarks-as-architecture.md) · [中文](0015-tests-benchmarks-as-architecture.cn.md) | Capability-organized tests, benchmark protocol, gated autoresearch. / 按能力组织的测试、基准协议、门控 autoresearch。 |
