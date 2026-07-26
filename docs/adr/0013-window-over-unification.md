# ADR 0013: Unified `.over()` Surface for All Window Expressions

- Status: Accepted
- Date: 2026-07-07 (design spec date)
- Issue: [#117](https://github.com/Lychee-Technology/ltseq/issues/117)

[中文版](0013-window-over-unification.cn.md)

## Context

LTSeq had two window paradigms users had to memorize separately:

1. **Sequence windows** (`shift`/`rolling`/`diff`/`cum_sum`/`cum_max`/`cum_min`): depend on a prior `sort()` (table order), partitioning via the `partition_by=` kwarg.
2. **Ranking windows** (`row_number`/`rank`/`dense_rank`/`ntile`): use `.over(partition_by=, order_by=, desc=)`.

Calling `.over()` on a sequence window raised `NotImplementedError`. Crucially, the Rust window planner (`src/transpiler/window_native.rs`) had **already implemented** `partition_by` for all sequence windows (`extract_partition_by`/`finalize_window_expr`, covered by `tests/test_window_partition_by.py`) — so this was a pure API unification, adding **zero new compute capability**.

## Decision

Sequence window expressions also accept an optional `.over()`, sharing one window-spec entry point with ranking functions. The whole rule collapses to one sentence:

> **Window expressions default to table order; `.over()` overrides partition/order.**

- **Coexistence rule**: `.over(partition_by=...)` together with a `partition_by=` kwarg on the same expression → **`ValueError`** (pick one). No implicit precedence — avoiding the silent surprise of "I wrote the kwarg but it didn't take effect"; the restriction is easy to relax later.
- **Supported dimensions**: sequence-window `.over()` supports `partition_by` **and** `order_by` (+`desc`); `order_by` overrides table order.
- **Wire format unchanged**: the existing `{"type":"Window", expr, partition_by, order_by, descending}` serialization already carries everything — no cross-boundary protocol change.
- Prior art: PySpark's `.over(Window.partitionBy(...).orderBy(...))` and Polars' `.over()`; this converges LTSeq's two paradigms into the same mental model.

## Alternatives Considered

- **Adopted — Plan A (inject + reuse existing converters)**: when the `PyExpr::Window` branch wraps a sequence-window Call, fold the wrapper's `partition_by` into the inner Call's kwargs, compute the effective `order_by` (wrapper's own, else table order), and re-dispatch to the existing `convert_shift/diff/cum_agg/rolling_agg`. Almost no new logic; the kwarg path and `test_window_partition_by.py` stay untouched.
- **Rejected — Plan B (refactor converter signatures)** to take an explicit `partition_by_exprs` parameter: cleaner signatures, but a much larger blast radius on green tests and call sites.

## Consequences

- One mental model for all windows; the kwarg form remains a supported equivalent spelling.
- Two accepted trade-offs: (a) when falling back to table order with a partition, a redundant sort key may remain (correctness-neutral; identical to existing ranking-window behavior); (b) `.over(order_by=)` is single-column (matching the existing ranking `.over()`); multi-key `.over()` ordering is deferred — the table-order fallback remains multi-key.
- Explicit non-goals (YAGNI): multi-column `.over(order_by=[...])`; deprecating the `partition_by=` kwarg; any change to ranking-function semantics.
- Related, pre-existing window semantics recorded in `api.md`: `rolling(n)` follows SQL `ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW` (partial frames at the start; deliberately **no `min_periods`** — passing it errors instead of NULL-padding, rejecting the pandas convention), and `shift(offset)` positive = backward, matching pandas `Series.shift()`.

## Sources

- `docs/superpowers/specs/2026-07-07-window-over-unification-design.md` — full design (decisions, Plan A/B, implementation notes, test plan, non-goals)
- `docs/api.md` — §3.1 (unified `.over()` rule, rolling/shift semantics)
- `src/transpiler/window_native.rs`, `py-ltseq/ltseq/expr/types.py`
- `py-ltseq/tests/test_window_over.py`, `test_window_partition_by.py`
