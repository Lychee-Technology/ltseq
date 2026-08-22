# ADR 0006: Deliberate Multi-Path (Hybrid) Execution Strategy

- Status: Accepted
- Decision date: predates this record · Recorded: 2026-07-26

[中文版](0006-multi-path-execution-strategy.cn.md)

## Context

LTSeq is not a pure wrapper around DataFusion. Some operations map cleanly onto native logical plans; some sequence workloads (funnel matching, as-of joins, physical-position ops) are better served by dedicated algorithms than by a generic planner. Design lesson §7.3: "DataFusion Is Strong, but Not Sufficient for Everything."

## Decision

Maintain **two coexisting execution strategies**, chosen per problem. The hybrid is deliberate, not an inconsistency.

1. **DataFusion-first (default)**: native logical plans and expressions, including native window construction (`transpiler/window_native.rs`).
2. **Specialized sequence paths**: dedicated Rust implementations where custom algorithms beat generic planning: linear scans (`src/ops/linear_scan.rs`), parallel scans (`src/ops/parallel_scan.rs`), as-of join matching (`src/ops/asof_join.rs`), and consecutive-row pattern/funnel matching (`src/ops/pattern_match.rs`). These paths collect their inputs to run, so they are among the documented eager boundaries in [ADR 0005](0005-no-materialization-rule.md).

One deliberate remnant of SQL usage exists: `filter_where` (`src/ops/aggregation.rs`) uses `session.sql()` against an **empty** table purely as a WHERE-clause parser, then applies the resulting native expression to the lazy `DataFrame::filter()`. It is a parser-as-library helper, not a data-execution fallback.

Sort metadata feeds the strategy choice: `LTSeqTable` carries an optional source Parquet path so sorted-Parquet inputs can use direct-scan fast paths instead of full planning ([ADR 0008](0008-explicit-sort-metadata.md)).

### Evolution: the retired SQL-fallback path

Earlier, a third strategy existed: generated SQL plus temporary tables (`transpiler/sql_gen.rs`) as "a compatibility and implementation convenience layer" for grouped/window-style transformations awkward to express natively. That path has been removed, precisely because SQL round-trips (`collect → MemTable → session.sql() → collect`) were a materialization sink; `src/transpiler/` today contains only `mod.rs`, `window_native.rs`, and `optimization.rs`, and `test_no_materialization_rule.py` now guards against reintroducing the pattern. `ARCHITECTURE.md`/`DESIGN_SUMMARY.md` still describe the three-path version and are stale on this point.

## Consequences

- Ordered/sequential operators (`search_pattern`/`search_pattern_count`, `asof_join`) get algorithmic wins impossible to express as generic plans. Note `search_first` is **not** one of them: despite older doc claims of binary search, it currently executes as a native lazy `filter(...).limit(1)` (`src/ops/basic.rs`) with no sort requirement; a sorted binary-search fast path remains an aspiration, not an implementation.
- A standing judgement call is named as a pressure point: deciding when specialized execution is justified over DataFusion-native plans.
- The benchmark layer exists partly to answer exactly these questions empirically ([ADR 0015](0015-tests-benchmarks-as-architecture.md)).

## Sources

- `docs/ARCHITECTURE.md`: Multi-Path Execution Strategy (stale: still describes the SQL-fallback path), Design Goals #5
- `docs/DESIGN_SUMMARY.md`: §5.1–5.3, §7.3
- `docs/MODULE_GUIDE.md`: `src/ops/*` tour
- Code: `src/transpiler/`, `src/ops/aggregation.rs` (`filter_where`), `src/ops/basic.rs` (`search_first`), `py-ltseq/tests/test_no_materialization_rule.py`
