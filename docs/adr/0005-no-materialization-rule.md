# ADR 0005: The No-Materialization Rule (with Two Documented Exceptions)

- Status: Accepted
- Date: 2026-07-26 (recorded; decision predates the ADR)

[中文版](0005-no-materialization-rule.cn.md)

## Context

Benchmarking and design experience identified materialization as "the main architectural cost center": the most important performance problems come not from low-level compute kernels but from collect/register/re-query patterns around complex operations (design lesson §7.4). Without a hard rule, convenience materialization (round-tripping through pandas/Arrow/Python rows inside query APIs) accretes silently.

## Decision

**Any API that returns `LTSeq`, `NestedTable`, `LinkedTable`, or `PartitionedTable` must stay on the lazy Rust/DataFusion query path.** No `to_pandas()`, `to_arrow()`, `from_arrow()`, `from_pandas()`, `_from_rows()`, Arrow round-trips, or row-by-row Python inside table-returning query APIs. Materialization is reserved for explicit export/terminal/construction APIs (`to_pandas()`, `to_arrow()`, `to_dicts()`, `collect()`, `from_arrow()`, `from_pandas()`).

The rule is enforced by a dedicated test — `py-ltseq/tests/test_no_materialization_rule.py` — "not just by convention."

### Documented exceptions (correctness, not shortcuts)

1. **Physical-position ops** (`rvs`, `step`, keyed `distinct`) snapshot the table into a single in-order partition (collect → read_batch) before assigning row positions — an unordered/partitioned window over a lazy multi-partition plan does not preserve input order, so the snapshot is required for correctness (`set_ops.rs::snapshot_single_partition`).
2. **`fold()`** runs a user-supplied Python callback `fn(state, row)` per row to thread sequential state. Arbitrary Python cannot be expressed as a DataFusion plan, so the row-wise path (`to_dicts()` → accumulate → `_from_rows()`) is inherent to the operation. Its docstring flags it as a non-lazy slow path (compare Polars `cumulative_eval`).

## Consequences

- Performance stays predictable and DataFusion keeps whole-plan optimization opportunities.
- The rule constrains API design elsewhere: `partition(by=...)` only accepts simple column expressions because derived callables would force materialization (see [ADR 0010](0010-four-table-object-types.md)).
- A named pressure point remains: "preventing SQL fallback paths from becoming accidental materialization sinks" (see [ADR 0006](0006-multi-path-execution-strategy.md)).
- Note: the two exceptions were, at the time of writing, documented only in `CLAUDE.md` (and `fold`'s docstring/api.md) — this ADR is now their home inside `docs/`.

## Sources

- `CLAUDE.md` — No Materialization Rule (sole prior home of the exception list)
- `docs/ARCHITECTURE.md` — No-Materialization Rule, Design Goals #4
- `docs/DESIGN_SUMMARY.md` — §5.4, §7.4
- `docs/USER_MODEL.md` — Materialization Model
- `docs/api.md` — §3.2 `fold`
- `py-ltseq/tests/test_no_materialization_rule.py`
