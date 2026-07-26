# ADR 0002: Rust Execution Core + Thin Python Surface over PyO3/maturin

- Status: Accepted
- Date: 2026-07-26 (recorded; decision predates the ADR)

[中文版](0002-rust-core-python-surface.cn.md)

## Context

The project needs both a fluent, Pythonic API (lambdas, method chaining, good error messages) and high-performance, consistent execution over large columnar data. Doing everything in Python is too slow; doing everything in Rust sacrifices ergonomics.

## Decision

Split the system into two layers with a strict ownership boundary — "Python owns expressive syntax, Rust owns execution semantics":

- **Python layer** (`py-ltseq/ltseq/`): the public API surface. Owns expression capture (see [ADR 0007](0007-lambda-dsl-schemaproxy.md)), schema and sort metadata bookkeeping, and the choice of which wrapper object to return (see [ADR 0010](0010-four-table-object-types.md)). It is intentionally thin and must not do heavy data processing itself.
- **Rust layer** (`src/`): owns planning, expression transpilation, execution, and specialized sequence algorithms.
- **Boundary**: PyO3 0.27.2 bindings, compiled with maturin (`maturin develop` after any Rust change).

The docs call this split "the backbone of the project."

## Consequences

- Performance-critical work runs vectorized in Rust; Python stays an orchestration shell.
- Every change to plan shape crosses the boundary and requires re-synchronizing metadata on both sides — notably the dual schema model ([ADR 0009](0009-dual-schema-model.md)) and sort metadata ([ADR 0008](0008-explicit-sort-metadata.md)).
- Contributors need a Rust toolchain and must rebuild the extension after Rust changes; pure-Python iteration is only possible on the surface layer.
- Code organization on each side of the boundary is itself a recorded decision (see [ADR 0012](0012-rust-thin-shell-python-mixins.md)).

## Sources

- `docs/ARCHITECTURE.md` — Layered Architecture §1–3, PyO3 Boundary Design, Typical End-to-End Flow
- `docs/DESIGN_SUMMARY.md` — §1.1
- `docs/USER_MODEL.md` — one-sentence summary
- `README.md` — Technology Stack
- `Cargo.toml` — pyo3 0.27.2, datafusion 54.0.0
