# ADR 0004: Lazy Evaluation, Immutable Tables, and Explicit Terminal Boundaries

- Status: Accepted
- Date: 2026-07-26 (recorded; decision predates the ADR)

[中文版](0004-lazy-execution-immutable-tables.cn.md)

## Context

Chained transformations (`filter → derive → sort → join → …`) should be cheap to compose and should let DataFusion optimize the *whole* plan, not each step in isolation. Users also need a predictable answer to "when does work actually happen?"

## Decision

1. **Lazy by default.** Most operations (`filter`, `select`, `derive`, `sort`, `slice`, `join`, `group_ordered`, …) return a new lazy query object. Execution happens only at explicit terminal boundaries: `show()`, `count()`/`len()`, `collect()`, `to_arrow()`/`to_arrow_ipc()`, `to_pandas()`, `to_dicts()`, and file writes.
2. **Tables are immutable.** Every operation returns a new `LTSeq`; the original is unchanged. APIs that *look* mutative (`insert`, `delete`, `update`, `modify`) are implemented copy-on-write in a dedicated `mutation_mixin.py`.
3. **Streaming is a separate object.** For datasets too large for memory, `LTSeq.scan()` / `scan_parquet()` return a streaming `Cursor` (iterating Arrow `RecordBatch`es, implemented in `src/cursor.rs`) rather than an `LTSeq`; `cursor.count()` counts without loading everything.

Returning a new table object means: reuse the same session, wrap a new plan, preserve/update schema metadata, and preserve or invalidate sort metadata ([ADR 0008](0008-explicit-sort-metadata.md)).

## Consequences

- DataFusion can optimize whole pipelines; chained workflows stay cheap until a terminal call.
- Users must understand that `to_pandas()`/`collect()` "changes the cost model" — materialization is the main architectural cost center (design lesson §7.4), which motivates the hard rule in [ADR 0005](0005-no-materialization-rule.md).
- The immutable style keeps semantics simple but means "mutation" APIs build new plans rather than editing data in place.

## Sources

- `docs/ARCHITECTURE.md` — Design Goals #3, Lazy Execution Model
- `docs/DESIGN_SUMMARY.md` — §1.2, §7.4
- `docs/USER_MODEL.md` — Core Mental Model #1–2, Materialization Model
- `docs/api.md` — §0 Conventions, §9 (mutation), §1/§11 (Cursor)
- `docs/MODULE_GUIDE.md` — `mutation_mixin.py`, `src/cursor.rs`
