# ADR 0009: Dual Schema Model — Python `_schema` + Rust Arrow Schema

- Status: Accepted
- Date: 2026-07-26 (recorded; decision predates the ADR)

[中文版](0009-dual-schema-model.cn.md)

## Context

Expression capture and good error messages need schema knowledge *in Python*, before anything crosses the boundary (e.g. validating `r.age` refers to a real column). Execution needs the Arrow schema *in Rust*, tied to the current plan.

## Decision

Track both. The Python layer keeps a user-facing `_schema` dict for fast validation and better error messages; the Rust layer tracks the Arrow schema of the current plan. Both are exposed to users (`schema` vs `python_schema` properties).

## Consequences

- Expression capture and user ergonomics get much easier — errors like "column not found" surface immediately in Python with a helpful message rather than deep inside plan execution.
- The stated hard requirement: **schema synchronization must be maintained at every boundary where plans change shape** — joins with conflicting names, linked-table materialization, grouped transformations, selects/derives. Recorded as design lesson §7.2 ("Schema synchronization is critical") and listed as the #1 long-term architectural pressure point.
- A contributor heuristic codifies it: "keep Python `_schema` and Rust schema behavior aligned" (see [ADR 0015](0015-tests-benchmarks-as-architecture.md)).
- The aggressive rename-then-alias strategy for join column conflicts ([ADR 0011](0011-link-lazy-prefix-aliased-join.md)) exists largely to keep this synchronization predictable.

## Sources

- `docs/ARCHITECTURE.md` — Schema Management, Architectural Risks
- `docs/DESIGN_SUMMARY.md` — §1.5, §7.2
- `docs/MODULE_GUIDE.md` — Contributor Heuristics
- `docs/api.md` — §1 (`schema` / `python_schema`)
