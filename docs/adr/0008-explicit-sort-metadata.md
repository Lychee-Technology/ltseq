# ADR 0008: Explicit Sort Metadata — Fail Rather Than Guess

- Status: Accepted
- Decision date: predates this record · Recorded: 2026-07-26

[中文版](0008-explicit-sort-metadata.cn.md)

## Context

Sequence operations (`shift`, `diff`, `rolling`, `pct_change`, cumulative ops, `group_ordered`, merge joins) are undefined without a known row order. An early attempt at implicit ordered semantics "led to confusing results and fragile APIs" (design lesson §7.1).

## Decision

Track sort state explicitly — the Rust kernel owns the declared order (`sort_specs`), and Python's `_sort_keys` reads it over FFI ([ADR 0009](0009-metadata-single-source-of-truth.md)) — and **never infer order implicitly**. If order is unknown, LTSeq prefers to fail (`SortRequiredError`) rather than silently produce misleading results. If a result depends on sequence, the sequence should appear in the code.

Sub-decisions:

- **`assume_sorted(*keys, desc=...)` escape hatch.** Declares sort order without physically sorting — for pre-sorted inputs (e.g. sorted Parquet) that would otherwise pay a redundant sort. This is an explicit trust-the-user contract: "the caller is responsible for correctness — wrong metadata produces wrong results." Benchmarks rely on it to declare known order outside timed rounds. Implementation note: only the Parquet path is metadata-only; for non-Parquet sources `assume_sorted()` currently collects and rebuilds a `MemTable` with the sort order attached (`src/ops/sort.rs`, see [ADR 0005](0005-no-materialization-rule.md)).
- **Computed sort keys sort physically but are not tracked as declared order.** `sort_keys` truncates at the first computed key: `sort("a", lambda r: r.b*2, "c")` declares only `[a]`; a lone computed key declares nothing (so `cum_sum` still raises `SortRequiredError`). Documented workaround: `.derive(k=...).sort("k")`. Accepted trade-off: after truncation, tie order on the declared prefix is unspecified — window execution may reorder ties.
- **Sort metadata is semantics, not just an optimization.** Some operations preserve it (many filters/derives/slices); reordering or structurally-new tables invalidate it. It also unlocks sorted-Parquet direct-scan and linear/parallel-scan fast paths ([ADR 0006](0006-multi-path-execution-strategy.md)).
- **Windows carrying their own order are the exception.** Only window expressions that depend on the table-order fallback require a declared table sort. Ranking functions always take `.over()`, and since [ADR 0013](0013-window-over-unification.md) sequence windows with an explicit `.over(order_by=...)` are equally self-sufficient — they run on unsorted tables.

## Consequences

- Results are trustworthy and queries self-documenting; merge joins can validate their precondition.
- Preserving sort metadata correctly across many operations is a named long-term pressure point.
- `assume_sorted` shifts correctness responsibility to the caller by design.

## Sources

- `docs/ARCHITECTURE.md` — Sort Metadata and Ordered Semantics
- `docs/DESIGN_SUMMARY.md` — §3.1–3.3, §7.1
- `docs/USER_MODEL.md` — Core Mental Model #3, Why Sort Is Explicit
- `docs/api.md` — §0, `LTSeq.sort`, `assume_sorted`, Common Errors table
- `README.md` — Limitations, FAQ
