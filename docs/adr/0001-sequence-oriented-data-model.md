# ADR 0001: Sequence-Oriented Data Model — Row Order Is Part of the Data Model

- Status: Accepted
- Date: 2026-07-26 (recorded; decision predates the ADR)

[中文版](0001-sequence-oriented-data-model.cn.md)

## Context

Traditional dataframe and SQL systems (pandas, relational databases) treat a table as an unordered set of rows; row order is display metadata at best. The workloads LTSeq targets — time series, event streams, streak/run detection, funnel analysis, state-machine style computations — are fundamentally about *ordered* data: "the previous row", "a run of consecutive equal values", and "the first row matching a condition after this one" are first-class questions.

## Decision

Treat row order as a semantic input to computation — a first-class part of both the data model and the query model. Data is processed as ordered sequences, not unordered sets.

This single choice drives the rest of the architecture:

- **API**: window functions that reference adjacent rows (`shift`, `diff`, `rolling`, cumulative ops), sequential grouping over consecutive runs (`group_ordered`), ordered search (`search_first` with binary search, `search_pattern` funnel matching), merge and as-of joins.
- **Metadata**: sort state must be tracked and propagated through the query pipeline (see [ADR 0008](0008-explicit-sort-metadata.md)).
- **Execution**: sorted inputs unlock specialized fast paths (see [ADR 0006](0006-multi-path-execution-strategy.md)).
- **Testing**: ordered semantics are covered as product capabilities in their own right (see [ADR 0015](0015-tests-benchmarks-as-architecture.md)).

## Alternatives Considered

Set-based semantics as in pandas/SQL were rejected as the core model. The `README.md` FAQ states this explicitly as the differentiator: LTSeq exists precisely because order-dependent computations are awkward or unreliable when order is not part of the model.

## Consequences

- Sequence operations are natural to express and cheap to validate.
- Some APIs require an explicit prior `sort()` (or `assume_sorted()`); operations whose result depends on order fail with `SortRequiredError` when order is unknown, rather than silently producing misleading results.
- Sort metadata handling becomes a pervasive, long-term maintenance obligation across every operation that transforms a table.

## Sources

- `docs/ARCHITECTURE.md` — Overview, Design Goals
- `docs/USER_MODEL.md` — "What LTSeq Is Not"
- `docs/DESIGN_SUMMARY.md` — §Overview
- `README.md` — Design Philosophy, FAQ
- `CLAUDE.md` — Architecture Overview
