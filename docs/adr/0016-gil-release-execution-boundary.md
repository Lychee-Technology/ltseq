# ADR 0016: Release the GIL Around Execution (Parse → Detached Execute → Convert)

- Status: Accepted
- Decision date: 2026-09-19 (issue #142) · Recorded: 2026-09-19

[中文版](0016-gil-release-execution-boundary.cn.md)

## Context

Every `#[pymethods]` entry point runs while the calling thread holds Python's GIL. Before #142 only `count()` and `to_arrow_ipc()` released it; roughly 34 other `RUNTIME.block_on(...)` sites, including full-table collects, file scans and writes, the rayon Parquet paths, and the cursor's per-batch stream pull, ran with the GIL held. During any large query every other Python thread in the process (web workers, Jupyter background threads, progress bars) stopped.

Two facts about PyO3 0.29 shape the fix:

- `Python::detach` requires the closure and its result to be `Ungil`, which on stable is `Send`. The compiler therefore rejects a captured `Bound<'_, T>`, but it does **not** reject `PyErr`, which is `Send`.
- Both `From<LtseqError> for PyErr` (via `raise_custom`) and `Display for PyErr` call `Python::attach`. Building or formatting a `PyErr` inside a detached section re-attaches the GIL from a thread that has just released it. That is correct today, but it is the classic "worker waits for the GIL while the GIL holder waits for the worker" shape the moment a future is spawned to another thread, and the `PARALLEL_FALLBACK` control flow in the sequence paths formats errors on every fallback.

## Decision

**Every entry point that performs real execution or I/O runs that work through `gil::detached`.** Plan building (`filter`, `select`, `derive`, `sort`, `join`, `union`, `slice`, `align`, ...) is synchronous and cheap; it stays under the GIL and the `block_on` wrappers that used to surround it were removed, since nothing there is awaited.

The layering is fixed per API:

1. **Parse under the GIL.** Anything that touches Python objects (`Bound<PyDict>` → `PyExpr`, dict values → `ScalarValue`) happens in the `lib.rs` stub or in the first half of the op.
2. **Execute detached.** `src/gil.rs::detached(py, || ...)` releases the GIL for a closure that sees only plain Rust values. `F: Send` keeps `Bound`/`Py` references out at compile time.
3. **Convert after.** The closure returns `Result<T, LtseqError>`; `detached` builds the `PyErr` once the GIL is held again. The execution halves of `src/ops/*` (`parallel_scan`, `linear_scan`, `pattern_match`, `asof_join`, `io`, `pivot`, the `*_exec` halves of `mutation`, the snapshot paths of `set_ops`, `grouping::group_ordered_count_impl`) return `LtseqError`, not `PyResult`, so a `PyErr` cannot be built inside them. `LTSeqTable::require_df/require_schema/from_batches*` return `LtseqError` for the same reason.

Two op shapes follow from this. When the whole impl is pure Rust (`materialize`, `rvs`, `step`, `asof_join`, `pivot`, `write_*`, `from_arrow`, `delete_rows`, the pattern-match and group-count impls after parsing), the `lib.rs` stub wraps the call. When parsing and execution are interleaved (`distinct`, `is_subset`, `insert_row`, `modify_row`, `assume_sorted`), the impl takes `py: Python<'_>` and wraps only its execution part.

**The streaming cursor takes its mutex inside the detached section.** `next_batch` and `is_exhausted` never wait on the stream mutex while holding the GIL: a thread blocked in `lock()` with the GIL would prevent the thread mid-`stream.next()` from ever re-attaching, a lock-order deadlock between the GIL and the mutex. Contending threads wait without the GIL and take the following batch.

## Alternatives Considered

- *Wrap every stub uniformly in `py.detach`.* Rejected: `search_pattern`, `search_pattern_count`, `group_ordered_count`, `insert_row`, `modify_row`, `distinct`, `is_subset` and `assume_sorted` receive `Bound<PyDict>` arguments, which cannot enter the closure; parsing has to be split out first (as the issue's review comments pointed out).
- *Keep `PyResult` inside the closure and rely on re-attach.* Works today but leaves the deadlock shape in place and re-takes the GIL on every `PARALLEL_FALLBACK` string check; the `LtseqError` return type makes the rule structural instead of a comment.

## Consequences

- A heavy call on one thread no longer stalls the interpreter for other threads. `py-ltseq/tests/test_gil_release.py` measures this with a heartbeat thread across every detached path.
- Adding an execution path means writing its pure-Rust half with `Result<_, LtseqError>` and wrapping it in `detached`; adding a plan-only transform needs no `block_on` at all.
- `format_table` is infallible and returns `String`. The Arrow boundary paths (`to_arrow_reader`, `from_arrow`, `__arrow_c_stream__`, the cursor's `next_batch`) follow the same shape: execution or stream draining runs detached, only the pyarrow hand-off happens under the GIL ([ADR 0017](0017-arrow-c-data-interface-boundary.md) replaced the IPC encoding this ADR originally listed).
- Not changed: `filter_where` awaits `session.sql()` only to parse a WHERE clause (planning, no execution) and stays under the GIL; `get_schema_dict`/`preview_join_schema` call `Python::attach` from an already-attached pymethod, which is harmless.

## Sources

- Issue #142 and its review comments (path inventory corrections, parse/execute split, cursor mutex)
- `src/gil.rs`, `src/lib.rs`, `src/cursor.rs`, `src/ops/*`
- `docs/ARCHITECTURE.md`: PyO3 Boundary Design
- [ADR 0012](0012-rust-thin-shell-python-mixins.md) (thin stubs), [ADR 0005](0005-no-materialization-rule.md) (which paths execute)
