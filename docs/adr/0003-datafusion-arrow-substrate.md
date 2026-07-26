# ADR 0003: DataFusion + Apache Arrow as the Execution Substrate

- Status: Accepted
- Decision date: predates this record · Recorded: 2026-07-26

[中文版](0003-datafusion-arrow-substrate.cn.md)

## Context

The Rust core needs a query planner/optimizer and a columnar execution format. Writing a bespoke engine would dwarf the actual product work (sequence semantics).

## Decision

Build on **Apache DataFusion 54.0** as the SQL/plan engine and **Apache Arrow** as the columnar in-memory format. A DataFusion `SessionContext` plus a lazy `DataFrame` own the logical plan inside each `LTSeqTable`.

## Rationale

DataFusion is a "battle-tested SQL engine." Staying on its lazy plan path yields vectorized execution, zero-copy columnar operations, filter/projection pushdown, and logical-plan optimization for free (which is why the no-materialization rule in [ADR 0005](0005-no-materialization-rule.md) matters so much).

## Consequences

- LTSeq inherits DataFusion's behavior — including its bugs. Documented example: a ProjectionPushdown bug affecting in-memory sources (`from_pandas`/`from_arrow`) after a join; the documented workaround is to read from CSV/Parquet instead, or select columns after `collect()` (see `LINKING_GUIDE.md` Troubleshooting).
- DataFusion alone is explicitly *not* sufficient for every sequence workload; that gap is what justifies the multi-path execution strategy ([ADR 0006](0006-multi-path-execution-strategy.md)) and is recorded as design lesson §7.3 ("DataFusion Is Strong, but Not Sufficient for Everything").
- Engine upgrades (DataFusion/Arrow major versions) are a recurring maintenance cost pinned in `Cargo.toml` (the direct `parquet` dependency must match DataFusion's Arrow version).

## Sources

- `docs/ARCHITECTURE.md` — Overview, Lazy Execution Model
- `docs/DESIGN_SUMMARY.md` — §5.1, §7.3
- `docs/LINKING_GUIDE.md` — Troubleshooting (ProjectionPushdown caveat)
- `README.md` — Performance, Technology Stack
- `Cargo.toml`
