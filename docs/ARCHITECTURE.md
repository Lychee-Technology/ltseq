# LTSeq Architecture

Related documents:

- `docs/README.md`: documentation index
- `docs/ARCHITECTURE.cn.md`: Chinese architecture document
- `docs/MODULE_GUIDE.md`: contributor-oriented codebase tour
- `docs/USER_MODEL.md`: user-facing design philosophy and working model
- `docs/DESIGN_SUMMARY.md`: design archive and decision history
- `docs/api.md`: API reference

## Overview

LTSeq is a sequence-oriented data processing library with a Python user surface and a Rust execution core. Unlike traditional dataframe systems that treat tables as unordered sets, LTSeq treats row order as part of the data model. That design choice drives the API, metadata model, execution strategy, and testing strategy.

At a high level:

- Python defines the public API and captures user expressions.
- Rust owns query planning and execution.
- DataFusion and Arrow provide the execution substrate.
- Tests and benchmarks protect both behavior and architectural invariants.

---

## Design Goals

LTSeq is designed around five goals:

1. Preserve ordered semantics as a first-class concept.
2. Keep the Python API fluent and expressive.
3. Execute lazily so DataFusion can optimize plans.
4. Avoid accidental materialization in table-returning APIs.
5. Allow specialized fast paths where generic query planning is not enough.

These goals explain most of the structure in the repository.

---

## Layered Architecture

The system is best understood as four layers.

### 1. User API Layer

Users interact with Python objects such as:

- `LTSeq`
- `NestedTable`
- `LinkedTable`
- `PartitionedTable`

This layer provides chaining, lambda-based expressions, and type-directed semantics.

### 2. Python Orchestration Layer

The Python package in `py-ltseq/ltseq/` is intentionally thin. Its responsibilities are:

- organize the public API through mixins
- track Python-side schema and sort metadata
- capture lambdas into serializable expression trees
- choose which high-level object type to return
- bridge to Rust bindings for execution

This layer should avoid doing heavy data processing itself.

### 3. Rust Execution Layer

The Rust crate in `src/` exposes `LTSeqTable` through PyO3. It is responsible for:

- constructing and transforming lazy DataFusion plans
- converting serialized expressions into DataFusion expressions or SQL
- tracking sort metadata needed for sequence semantics
- executing terminal actions such as collect, count, display, and export
- providing specialized implementations for sequence-heavy operations

### 4. Validation and Performance Layer

The repository treats tests and benchmarks as architectural components, not just project hygiene.

- `py-ltseq/tests/` validates capability behavior and invariants
- `benchmarks/` measures performance on representative ordered workloads
- `docs/BENCHMARK_AUTORESEARCH.md` documents the benchmark review workflow

---

## Major Runtime Objects

### `LTSeq`

Defined in `py-ltseq/ltseq/core.py`, `LTSeq` is the main user-visible table object. It wraps a Rust `LTSeqTable` instance in `_inner` and maintains Python-side metadata:

- `_schema`
- `_sort_keys`
- `_name`

It composes behavior through mixins instead of a single large class.

### `LTSeqTable`

Defined in `src/lib.rs`, `LTSeqTable` is the Rust kernel object exposed through PyO3. It owns:

- `SessionContext`
- an optional lazy `DataFrame`
- an optional Arrow schema
- `sort_exprs`
- an optional source Parquet path for sorted-scan optimization

It is the authoritative execution object behind most Python operations.

### `NestedTable`

Defined in `py-ltseq/ltseq/grouping/nested_table.py`, `NestedTable` represents grouped sequential context rather than a flattened aggregated result. It is used for operations where group identity and within-group order both matter.

### `LinkedTable`

Defined in `py-ltseq/ltseq/linking.py`, `LinkedTable` models a lazy relationship between two tables. It delays materializing the join until the user actually needs right-side data.

### `PartitionedTable`

Defined in `py-ltseq/ltseq/partitioning.py`, partitioned table abstractions expose grouped access patterns while preserving the project-wide lazy/query-first design where possible.

---

## Python Package Structure

The Python package uses mixin composition to organize the top-level API.

- `core.py`: main `LTSeq` class and common helpers
- `io_ops.py`: loading, exporting, constructors, Arrow/pandas bridges
- `transforms.py`: filter/select/derive/sort/distinct/slice and related transforms
- `joins.py`: joins, sorted joins, as-of join entrypoints
- `aggregation.py`: aggregations, grouping entrypoints, partitioning entrypoints
- `advanced_ops.py`: advanced operations and set operations
- `mutation_mixin.py`: copy-on-write mutation helpers
- `lookup.py`: lookup expression rewriting for derive-time joins

This organization keeps the public API broad while avoiding a monolithic implementation file.

---

## Rust Package Structure

The Rust crate follows a deliberate split:

- `src/lib.rs`: PyO3 entrypoint, `LTSeqTable`, thin Python-facing methods
- `src/engine.rs`: runtime and `SessionContext` creation
- `src/cursor.rs`: streaming cursor support
- `src/types.rs`: `PyExpr` and expression deserialization
- `src/error.rs`: internal error model and Python error conversion
- `src/transpiler/`: expression conversion and optimization
- `src/ops/`: operation implementations grouped by capability

The most important structural pattern is that `lib.rs` remains a thin shell, while `src/ops/*` holds the real execution logic.

---

## PyO3 Boundary Design

Rust exposes the Python extension module through a single `#[pymodule]` in `src/lib.rs`. `LTSeqTable` uses one `#[pymethods]` block and delegates most behavior to helpers in `src/ops/*`.

This is both a practical PyO3 constraint and a maintainability choice:

- Python-facing signatures stay centralized.
- implementation details remain modular
- operation logic can evolve without turning `lib.rs` into a multi-thousand-line execution file

The Python side generally calls methods on `_inner`, receives a new Rust table object back, then wraps it in a new `LTSeq` through `_from_inner()`.

---

## Expression Pipeline

One of the defining architectural features is the lambda capture pipeline.

### Python side

Users write expressions such as:

```python
t.filter(lambda r: r.age > 18)
```

The lambda is executed against a `SchemaProxy`, not against real row values. Proxy attribute access and operator overloads produce expression nodes. Those nodes serialize into dictionaries.

Key files:

- `py-ltseq/ltseq/expr/proxy.py`
- `py-ltseq/ltseq/expr/base.py`
- `py-ltseq/ltseq/expr/types.py`
- `py-ltseq/ltseq/expr/transforms.py`

### Rust side

The serialized dictionary is converted into `PyExpr` in `src/types.rs`, optionally optimized, then transpiled via one of three paths:

- native DataFusion `Expr`
- native window expression construction
- SQL generation fallback

Key files:

- `src/transpiler/mod.rs`
- `src/transpiler/window_native.rs`
- `src/transpiler/sql_gen.rs`
- `src/transpiler/optimization.rs`

This split is central to LTSeq's architecture: Python owns expressive syntax, Rust owns execution semantics.

---

## Lazy Execution Model

Most LTSeq operations are lazy plan transformations. A new table object usually means:

- reuse the same session
- wrap a new DataFusion plan
- preserve or update schema metadata
- preserve or invalidate sort metadata depending on the operation

Terminal methods trigger execution, including:

- `show()`
- `count()`
- `to_arrow()` / `to_arrow_ipc()`
- `to_pandas()`
- `collect()`
- file writes

The project relies on DataFusion for plan optimization and execution, but preserves the ability to bypass generic execution for targeted sequence operations.

---

## Sort Metadata and Ordered Semantics

Order is not inferred implicitly. LTSeq tracks sort state explicitly on both sides of the Python/Rust boundary.

- Python side: `_sort_keys`
- Rust side: `sort_exprs`

This metadata enables:

- validation for sequence operators
- validation for merge-style sorted joins
- sort-preserving behavior across safe transforms
- sorted Parquet optimizations and direct scan fast paths

The core rule is simple: ordered operators require known order. If order is unknown, LTSeq prefers to fail rather than silently produce misleading results.

---

## No-Materialization Rule

One of the strongest architectural constraints in the project is the no-materialization rule.

Table-returning query APIs should remain on the lazy Rust/DataFusion path and must not quietly materialize through pandas, Arrow round-trips, or row-by-row Python logic. Materialization is reserved for explicit export or terminal APIs.

This rule exists for two reasons:

- performance predictability
- semantic consistency across `LTSeq`, `NestedTable`, `LinkedTable`, and `PartitionedTable`

The repository enforces this not just by convention, but through dedicated tests.

---

## Multi-Path Execution Strategy

LTSeq is not a pure wrapper around DataFusion. It uses multiple execution strategies depending on the problem.

### DataFusion-first path

The default path is to express work as native DataFusion plans and expressions.

### SQL fallback path

Some grouped and window-style transformations are easier to express through generated SQL and temporary tables. This path exists as a compatibility and implementation convenience layer.

### Specialized sequence path

Certain sequence-heavy operations use dedicated Rust code paths, especially when sorted Parquet data allows direct scanning or when custom algorithms outperform generic planning.

Examples include:

- linear scans
- parallel scans
- as-of join binary search logic
- pattern matching/funnel-style operations

This is a deliberate hybrid design rather than an inconsistency.

---

## Grouping, Linking, and Partitioning

### Grouping

`group_ordered()` and `group_sorted()` return `NestedTable`, not an eagerly aggregated flat table. Internally, grouped workflows rely on extra columns such as `__group_id__`, `__group_count__`, and `__rn__` to preserve group identity and row position.

### Linking

`link()` returns `LinkedTable`, a lazy relationship object. Source-only operations may avoid join materialization entirely. Once linked-side columns are required, the join is materialized and the result becomes a regular `LTSeq`.

### Partitioning

`partition()` exposes logical partition access. The implementation favors capturable expressions and SQL-backed behavior over arbitrary Python execution so the system can stay on the query path.

These abstractions are a major part of LTSeq's API differentiation.

---

## Schema Management

Schema management spans both Python and Rust.

- Python tracks a user-facing `_schema` dictionary for fast validation and better error messages.
- Rust tracks Arrow schema information tied to the current query plan.

This dual model makes expression capture and user ergonomics easier, but it creates a hard requirement: schema synchronization must be maintained at every boundary where plans change shape.

This is especially important for:

- joins with conflicting column names
- linked-table materialization
- grouped transformations
- selects and derives that change visible columns

---

## Testing Strategy

Tests are organized primarily by capability, not by implementation file. That is an architectural choice.

The test suite covers:

- core I/O and constructors
- expression DSL behavior and optimization
- transform behavior
- window and ranking semantics
- grouping and nested-table behavior
- join, linking, and as-of join semantics
- partitioning and set operations
- integration flows
- explicit architectural invariants such as no accidental materialization

This makes the test suite a map of product capabilities and a guardrail for future refactors.

---

## Benchmarks and Performance Research

The `benchmarks/` directory is not incidental. It reflects an explicit product belief: LTSeq's value depends on both semantics and performance.

Benchmarks help answer:

- where DataFusion is already sufficient
- where materialization costs dominate
- where specialized Rust implementations are worthwhile
- whether architectural changes improve real workloads

The autoresearch benchmark workflow turns performance investigation into a repeatable engineering practice.

---

## Typical End-to-End Flow

A typical request moves through the system like this:

1. User calls a Python API such as `filter`, `derive`, or `group_ordered`.
2. Python validates lightweight metadata and captures any lambda expressions.
3. Python sends serialized expressions into Rust via `_inner`.
4. Rust transforms the underlying lazy DataFusion plan or specialized query state.
5. Rust returns a new `LTSeqTable` or related object state.
6. Python wraps that result in the correct high-level object and updates metadata.
7. Execution only occurs when the user calls a terminal operation.

That separation is the backbone of the project.

---

## Architectural Risks and Pressure Points

The main long-term pressure points are:

- keeping Python and Rust schema state synchronized
- preserving sort metadata correctly across many operations
- preventing SQL fallback paths from becoming accidental materialization sinks
- maintaining consistency between lazy linked/grouped abstractions and flat table APIs
- deciding when specialized execution is justified over DataFusion-native plans

Those are the areas to inspect most carefully when changing core behavior.

---

## Recommended Reading Order

For new developers:

1. `docs/USER_MODEL.md`
2. `docs/ARCHITECTURE.md`
3. `docs/MODULE_GUIDE.md`
4. `docs/DESIGN_SUMMARY.md`
5. `docs/api.md`

For contributors working directly in code:

1. `docs/ARCHITECTURE.md`
2. `docs/MODULE_GUIDE.md`
3. the relevant `py-ltseq/tests/` files for the capability being changed
