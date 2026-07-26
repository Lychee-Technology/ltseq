# ADR 0006: Deliberate Multi-Path (Hybrid) Execution Strategy

- Status: Accepted
- Date: 2026-07-26 (recorded; decision predates the ADR)

[中文版](0006-multi-path-execution-strategy.cn.md)

## Context

LTSeq is not a pure wrapper around DataFusion. Some operations map cleanly onto native logical plans; some grouped/window-style transformations are awkward to express natively; and some sequence workloads (ordered search, funnel matching, as-of joins over sorted data) are better served by dedicated algorithms than by a generic planner. Design lesson §7.3: "DataFusion Is Strong, but Not Sufficient for Everything."

## Decision

Maintain **three coexisting execution strategies**, chosen per problem — "a deliberate hybrid design rather than an inconsistency":

1. **DataFusion-first (default)** — native logical plans and expressions.
2. **SQL fallback** — generated SQL plus temporary tables for grouped/window-style transformations that are awkward natively; explicitly "a compatibility and implementation convenience layer" (`transpiler/sql_gen.rs`).
3. **Specialized sequence paths** — dedicated Rust implementations where custom algorithms beat generic planning: linear scans (`src/ops/linear_scan.rs`), parallel scans (`src/ops/parallel_scan.rs`), as-of join binary search (`src/ops/asof_join.rs`), and consecutive-row pattern/funnel matching (`src/ops/pattern_match.rs`).

Sort metadata feeds this choice: `LTSeqTable` carries an optional source Parquet path so sorted-Parquet inputs can use direct-scan fast paths instead of full planning ([ADR 0008](0008-explicit-sort-metadata.md)).

## Consequences

- Ordered search (`search_first` with binary search, `search_pattern`/`search_pattern_count`) and as-of joins get algorithmic wins impossible to express as generic plans — a core part of the sequence-model value proposition.
- Two standing judgement calls are named as pressure points: deciding when specialized execution is justified over DataFusion-native plans, and preventing SQL fallback from becoming an accidental materialization sink ([ADR 0005](0005-no-materialization-rule.md)).
- The benchmark layer exists partly to answer exactly these questions empirically ([ADR 0015](0015-tests-benchmarks-as-architecture.md)).

## Sources

- `docs/ARCHITECTURE.md` — Multi-Path Execution Strategy, Design Goals #5, Architectural Risks
- `docs/DESIGN_SUMMARY.md` — §5.1–5.3, §7.3
- `docs/MODULE_GUIDE.md` — `src/ops/*` tour
- `docs/api.md` — §3.4 (ordered search)
