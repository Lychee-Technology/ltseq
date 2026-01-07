Plan: LTSeq Implementation Roadmap

Purpose
- Provide a concise, actionable execution plan that implements the roadmap in `docs/milestones.md` and the API surface in `docs/api.md`.
- Target an MVP walking-skeleton first (CSV IO, Rust-Python bridge, basic filter/select), then incrementally enable the Lambda DSL and sequence operators.

Scope
- Phases 1–9 from the roadmap, covering project scaffolding, bindings, DSL, core relational ops, sequence operators, ordered grouping, pointer linking, and tests/CI/docs.
- Deliver an MVP where `LTSeq.read_csv(...).show()` works and `filter`/`select` lambdas from Python compile to Rust/DataFusion logical plans and execute.

Phases & Tasks

- **Phase 1 — Project Initialization**
  - Tasks:
    - Verify `py-ltseq/ltseq` and Rust crate layout; ensure `pyproject.toml` and `Cargo.toml` exist.
    - Add Python `LTSeq` wrapper stub and minimal `__init__.py`.
    - Add Rust lib skeleton exposing pyo3 bindings.
  - Deliverable: `import ltseq` succeeds; `LTSeq` type present.
  - Success test: `python -c "import ltseq; print(ltseq)"`.

- **Phase 2 — Rust-Python Bridge & CSV IO (Walking Skeleton)**
  - Tasks:
    - Embed DataFusion `SessionContext` and hold DataFrame / logical-plan in a `RustTable` struct.
    - Implement `LTSeq.read_csv(path)` → Rust loads CSV into Arrow/DataFusion.
    - Implement `LTSeq.show()` to print RecordBatches.
    - Use `maturin` + `pyo3` for bindings.
  - Deliverable: Demo `t = LTSeq.read_csv("data/sample.csv"); t.show()`.
  - Success test: printed table matches CSV head.

- **Phase 3 — Python AST Builder (SchemaProxy & Expr)**
  - Tasks:
    - Implement `SchemaProxy` and expression classes capturing operations via magic methods.
    - Serialize expression trees into a compact structure for Rust.
  - Deliverable: `lambda r: r.age > 18` serializes to expression tree.
  - Success test: unit tests assert shape of serialized trees.

- **Phase 4 — Rust Transpiler (`PyExpr` → DataFusion Expr)**
  - Tasks:
    - Implement `PyExpr` enum and deserialization in Rust.
    - Map `PyExpr` to DataFusion `logical_expr::Expr`.
    - Implement `filter()` and `select()` accepting serialized expressions.
  - Deliverable: `t.filter(...).select(...).show()` compiles and runs.
  - Success test: example query returns expected rows.

- **Phase 5 — Core Relational Operations**
  - Tasks:
    - Implement `select`, `derive`, `sort`, `distinct`, `slice`. Ensure operations return new `LTSeq` handles (immutable).
  - Deliverable: chaining operations work from Python.
  - Success test: small integration tests verifying behavior.

- **Phase 6 — Sequence Operators (shift, rolling, diff, cum_sum)**
  - Tasks:
    - Map `r.col.shift(n)` to DataFusion lag/lead or custom operator.
    - Implement `r.col.rolling(w).agg()` mapping to WindowFrame if supported, or fallback to custom plan.
    - Implement `cum_sum()` (scan or window).
  - Deliverable: expressions using `.shift()` and `.rolling()` produce correct numeric results.
  - Success test: unit tests on fixtures validating numerical outputs.

- **Phase 7 — Ordered Grouping (`group_ordered`)**
  - Tasks:
    - Implement Mask = `col != col.shift(1)` → GroupID = `cum_sum(Mask)` and produce a sequence of sub-tables.
    - Provide group-level operations (`first`, `last`, `count`) and allow sub-table `filter(lambda g: ...)`.
  - Deliverable: DSL showcase (stock rising >3 days) works end-to-end.
  - Success test: reproduce the example from `docs/api.md`.

- **Phase 8 — Pointer-Based Linking (`link`, lookups)**
  - Tasks:
    - Implement `link(target_table, on=..., as_name=...)` building an index (key → row index).
    - Represent linked columns as virtual pointer columns; translate attribute access into `take` operations.
  - Deliverable: `orders.link(products, on=..., as_name="prod")` and `orders.select(lambda r: [r.id, r.prod.name]).show()`.
  - Success test: functional equality with standard join/lookup.

- **Phase 9 — Tests, CI, Docs, Demos**
  - Tasks:
    - Add pytest tests for Python parts and Rust unit tests.
    - Add CI workflow (GitHub Actions) with `maturin build` and tests.
    - Produce docs and demo scripts / notebooks.
  - Deliverable: CI passes; docs updated.

Implementation Choices & Recommendations
- Binding: use `pyo3` + `maturin` (consistent with roadmap). Confirm if `uv` in docs refers to a specific crate/workflow; ask if another binding is preferred.
- Expression serialization: start with structured Python dicts / lists (via pyo3) to avoid JSON overhead; adopt efficient `FromPyObject` paths in Rust.
- Use DataFusion window functions where possible; prepare to implement small custom operators only if needed.
- Keep LTSeq methods immutable and thin wrappers around logical plans, not data copies.

Testing Strategy
- Unit tests for expression serialization and mapping.
- Integration tests for `read_csv`, `filter`, `select`, `derive`.
- Numeric tests for `shift`, `diff`, `rolling`.
- Functional test for `group_ordered` showcase.
- Benchmarks for `link` vs `join` (optional after MVP).

Risks & Open Questions
- Confirm what `uv` denotes in the roadmap and whether `pyo3` + `maturin` is acceptable.
- DataFusion may not cover all window/scan primitives (rolling/cum_sum). Is implementing custom physical operators acceptable?
- Serialization performance and caching: should compiled logical plans be cached on the Python side to avoid repeated roundtrips?
- API details for `derive` (dict vs explicit column spec) and for nested pointers (`r.prod.name`) need a final ergonomic decision.

Next Steps (once edits are allowed)
- Confirm binding and serialization choices.
- Write `plan.md` with this content.
- Scaffold Phase 1 files (stubs) and run the import smoke test.
- If import works, implement Phase 2 (CSV IO) and a small example filter pipeline.
