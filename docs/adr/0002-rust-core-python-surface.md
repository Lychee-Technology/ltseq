# ADR 0002: Rust Execution Core + Thin Python Surface over PyO3/maturin

- Status: Accepted
- Decision date: predates this record · Recorded: 2026-07-26

[中文版](0002-rust-core-python-surface.cn.md)

## Context

The project needs both a fluent, Pythonic API (lambdas, method chaining, good error messages) and high-performance, consistent execution over large columnar data. Doing everything in Python is too slow; doing everything in Rust sacrifices ergonomics.

## Decision

Split the system into two layers with a strict ownership boundary: Python owns the expressive syntax, Rust owns the execution semantics.

- **Python layer** (`py-ltseq/ltseq/`): the public API surface. Owns expression capture (see [ADR 0007](0007-lambda-dsl-schemaproxy.md)) and the choice of which wrapper object to return (see [ADR 0010](0010-four-table-object-types.md)); schema/sort metadata is read from the Rust kernel ([ADR 0009](0009-metadata-single-source-of-truth.md)). It is intentionally thin and must not do heavy data processing itself.
- **Rust layer** (`src/`): owns planning, expression transpilation, execution, and specialized sequence algorithms.
- **Boundary**: PyO3 0.27.2 bindings, compiled with maturin (`maturin develop` after any Rust change).

That split runs through the whole architecture.

## Consequences

- Performance-critical work runs vectorized in Rust; Python stays an orchestration shell.
- Every change to plan shape crosses the boundary; schema and sort metadata are owned by the Rust kernel and read (or cached) by Python ([ADR 0009](0009-metadata-single-source-of-truth.md), [ADR 0008](0008-explicit-sort-metadata.md)).
- Contributors need a Rust toolchain and must rebuild the extension after Rust changes; pure-Python iteration is only possible on the surface layer.
- Code organization on each side of the boundary is itself a recorded decision (see [ADR 0012](0012-rust-thin-shell-python-mixins.md)).

## Sources

- `docs/ARCHITECTURE.md`: Layered Architecture §1–3, PyO3 Boundary Design, Typical End-to-End Flow
- `docs/DESIGN_SUMMARY.md`: §1.1
- `docs/USER_MODEL.md`: one-sentence summary
- `README.md`: Technology Stack
- `Cargo.toml`: pyo3 0.27.2, datafusion 54.0.0
