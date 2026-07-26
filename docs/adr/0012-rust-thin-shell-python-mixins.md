# ADR 0012: Code Organization — Rust Thin Shell + `src/ops/*`, Python Mixin Composition

- Status: Accepted
- Decision date: predates this record · Recorded: 2026-07-26

[中文版](0012-rust-thin-shell-python-mixins.cn.md)

## Context

Both sides of the boundary risk monolith files: PyO3 allows only **one `#[pymethods]` block per struct**, which naturally attracts all implementation into `lib.rs`; and a broad Python API surface risks a god-class.

## Decision

**Rust side — thin shell + helpers.** Expose the extension via a single `#[pymodule]`. All `LTSeqTable` Python-facing methods live in the one `#[pymethods]` block in `lib.rs`; **most operations** are thin 1–3 line delegation stubs calling helper functions in `src/ops/` (basic, derive, window, sort, grouping, join, asof_join, aggregation, set_ops, pattern_match, linear_scan, parallel_scan, align, pivot, mutation, io, common), while constructors, IO/terminal methods, and some basics remain inline (e.g. `read_csv`, `to_arrow_ipc`, `slice`). This is "both a practical PyO3 constraint and a maintainability choice": signatures stay centralized, implementation stays modular, and `lib.rs` never becomes a multi-thousand-line execution file. Corollary for readers: *do not assume the logic lives in `lib.rs`*.

**Python side — mixin composition.** `LTSeq` is assembled in `core.py` from category mixins (`io_ops.py`, `transforms.py`, `joins.py`, `aggregation.py`, `advanced_ops.py`, `mutation_mixin.py`, `lookup.py`), keeping the public API broad while avoiding a monolithic file and preserving a single user-facing class. The Python side calls `_inner`, receives a new Rust table back, and wraps it via `_from_inner()`.

**Rust code quality standard** (`docs/rust-coding-std.md`, a *normative* standard): minimal public API; borrow over clone; `Result`/`Option` with custom error types (`thiserror`/`anyhow`), no `unwrap` in production; newtypes for domain concepts; prefer traits/enums/composition over classical OO patterns; refactor only behind a test safety net, in small compiler-checked steps; treat code smells as forward signals, not failures. Verification gap: the standard calls for Clippy + rustfmt in CI, but the current workflow (`.github/workflows/ci.yml`) runs only build, pyright, and pytest — the lint gates are not yet wired up.

## Consequences

- Finding an operation's implementation is a two-hop navigation (`lib.rs` stub → `src/ops/<category>.rs`), which `MODULE_GUIDE.md` documents as the intended reading path.
- Adding a method touches both the `#[pymethods]` block and an ops module (plus the Python mixin and stubs, [ADR 0014](0014-pyi-stubs-typed-surface.md)).
- The mixin split organizes by category without changing the user-facing type ([ADR 0010](0010-four-table-object-types.md) covers the *semantic* wrapper types).

## Sources

- `CLAUDE.md` — Key Design Patterns (PyO3 single `#[pymethods]` constraint, mixin composition)
- `docs/ARCHITECTURE.md` — PyO3 Boundary Design, Rust/Python Package Structure
- `docs/DESIGN_SUMMARY.md` — §1.3, §1.4
- `docs/MODULE_GUIDE.md` — `src/lib.rs`, module tours
- `docs/rust-coding-std.md`
