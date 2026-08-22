# ADR 0005: The No-Materialization Rule for Relational Transforms

- Status: Accepted
- Decision date: predates this record · Recorded: 2026-07-26

[中文版](0005-no-materialization-rule.cn.md)

## Context

Benchmarking and design experience identified materialization as "the main architectural cost center": the most important performance problems come not from low-level compute kernels but from collect/register/re-query patterns around complex operations (design lesson §7.4). Without a hard rule, convenience materialization (round-tripping through pandas/Arrow/Python rows inside query APIs) accretes silently.

## Decision

**Ordinary relational-transform APIs that return `LTSeq`, `NestedTable`, `LinkedTable`, or `PartitionedTable` (filter/select/derive/sort/slice/join/group/link) must stay on the lazy Rust/DataFusion query path.** No `to_pandas()`, `to_arrow()`, `from_arrow()`, `from_pandas()`, `_from_rows()`, Arrow round-trips, or row-by-row Python inside these APIs. Materialization is reserved for explicit export/terminal/construction APIs (`to_pandas()`, `to_arrow()`, `to_dicts()`, `collect()`, `from_arrow()`, `from_pandas()`).

### Documented eager boundaries

The rule does not (and cannot) cover every table-returning API. The following operations materialize by design or by current implementation:

**Correctness-required exceptions** (documented in `CLAUDE.md`; not shortcuts):

1. **Physical-position ops** (`rvs`, `step`, keyed `distinct`) snapshot the table into a single in-order partition (collect → read_batch) before assigning row positions, because an unordered/partitioned window over a lazy multi-partition plan does not preserve input order (`set_ops.rs::snapshot_single_partition`).
2. **`fold()`** runs a user-supplied Python callback `fn(state, row)` per row; arbitrary Python cannot be a DataFusion plan, so the row-wise path (`to_dicts()` → accumulate → `_from_rows()`) is inherent. Its docstring flags it as a non-lazy slow path (compare Polars `cumulative_eval`).

**Additional eager paths in the current implementation** (specialized algorithms or implementation state, as of this record):

- **Non-Parquet `assume_sorted()`** collects batches and rebuilds a `MemTable` with `with_sort_order()` (`src/ops/sort.rs`); only the Parquet path is metadata-only.
- **`asof_join()`** collects both inputs to run its specialized matching (`src/ops/asof_join.rs`).
- **`pivot()`** collects the distinct pivot keys (and executes the aggregate) to construct the output schema (`src/ops/pivot.rs`).
- **Mutation APIs** (`insert`/`delete`/`update`/`modify`) collect the table at call time (`src/ops/mutation.rs`); see [ADR 0004](0004-lazy-execution-immutable-tables.md).
- **`search_pattern`** collects to run its sequential matcher (`src/ops/pattern_match.rs`).

These live on the Rust side (Arrow batches, no Python row round-trips), but they do end plan laziness; treating them as lazy when composing pipelines misestimates cost.

## Consequences

- For the covered relational transforms, performance stays predictable and DataFusion keeps whole-plan optimization opportunities.
- The rule constrains API design elsewhere: `partition(by=...)` only accepts simple column expressions because derived callables would force materialization (see [ADR 0010](0010-four-table-object-types.md)).
- **Enforcement is partial.** `py-ltseq/tests/test_no_materialization_rule.py` is a source-scan guard: it greps selected `src/ops/` modules for SQL/MemTable tokens (`session.sql`, `.sql(&`, `MemTable::try_new`) and allows documented exceptions. It does **not** detect plain `.collect()` calls, so it guards against the SQL-round-trip pattern specifically, not against all eager execution. Keeping the eager-boundary list above current is a documentation obligation, not something CI verifies.
- A named pressure point remains: preventing SQL-style fallback paths from becoming accidental materialization sinks (see [ADR 0006](0006-multi-path-execution-strategy.md)).

## Sources

- `CLAUDE.md`: No Materialization Rule (the two correctness exceptions)
- `docs/ARCHITECTURE.md`: No-Materialization Rule, Design Goals #4
- `docs/DESIGN_SUMMARY.md`: §5.4, §7.4
- `docs/USER_MODEL.md`: Materialization Model
- `docs/api.md`: §3.2 `fold`
- Code: `src/ops/sort.rs`, `src/ops/asof_join.rs`, `src/ops/pivot.rs`, `src/ops/mutation.rs`, `src/ops/pattern_match.rs`, `py-ltseq/tests/test_no_materialization_rule.py`
