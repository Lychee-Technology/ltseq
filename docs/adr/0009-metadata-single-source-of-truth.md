# ADR 0009: Schema & Sort Metadata — Rust as the Single Source of Truth

- Status: Accepted
- Supersedes: the earlier dual-schema model (see Evolution)
- Decision date: issue #93 implementation · Recorded: 2026-07-26

[中文版](0009-metadata-single-source-of-truth.cn.md)

## Context

Expression capture and good error messages need schema knowledge in Python before anything crosses the boundary; execution needs the Arrow schema in Rust. The original answer was to maintain both independently — which proved fragile (see Evolution).

## Decision

**The Rust kernel owns the authoritative metadata**: the Arrow schema and the declared sort order (`sort_specs`) live with the plan. The Python side does not maintain parallel state:

- `LTSeq._schema` is a **lazily-fetched per-instance cache** of `_inner.get_schema_dict()` (`core.py`); a setter remains only for migration-period compatibility with external code.
- `LTSeq._sort_keys` is an **uncached FFI read** of `_inner.get_sort_keys()` — reads are rare and the call costs microseconds.
- The user-facing `schema` and `python_schema` properties are two views over the same fetched data (`python_schema` maps Arrow type names to Python type names), not two sources.

The regression guard is `py-ltseq/tests/test_schema_source_of_truth.py`, which exists specifically to prevent the old dual-tracking from coming back.

## Evolution: the superseded dual-schema model

Originally, Python kept a hand-maintained `_schema` dict alongside the Rust Arrow schema, with a stated obligation to re-synchronize "at every boundary where plans change shape." This was recorded as design lesson §7.2 and the #1 long-term pressure point — and it failed in practice: `select` kept projected-away columns, `derive`/`agg`/`pivot` invented "Unknown" placeholder types, and renaming a sort column diverged Python `_sort_keys` from Rust `sort_specs`. Issue #93 replaced the mirror with the single-source-of-truth design above. `ARCHITECTURE.md` (Schema Management) and `DESIGN_SUMMARY.md` §1.5/§7.2 still describe the dual model and are stale on this point; the lesson they record — *schema synchronization is critical* — is precisely why the duplication was removed.

## Consequences

- Python-side validation and error ergonomics are preserved (the cache makes reads cheap) without the class of drift bugs the mirror caused.
- Cache invalidation replaces synchronization as the thing to get right: any operation that changes plan shape must return a new wrapper (fresh cache) rather than mutate in place — which the immutable-table design ([ADR 0004](0004-lazy-execution-immutable-tables.md)) already guarantees.
- The join rename-then-alias strategy ([ADR 0011](0011-link-lazy-prefix-aliased-join.md)) keeps the authoritative Rust schema predictable after joins.

## Sources

- `py-ltseq/ltseq/core.py` — `_schema` cache, `_sort_keys` FFI read (issue #93 comments)
- `py-ltseq/tests/test_schema_source_of_truth.py`
- `docs/ARCHITECTURE.md` — Schema Management (stale: describes the superseded dual model)
- `docs/DESIGN_SUMMARY.md` — §1.5, §7.2 (rationale for why the dual model had to go)
