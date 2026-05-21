# LTSeq Design Summary

Related documents:

- `docs/README.md`: documentation index
- `docs/ARCHITECTURE.md`: full architecture walkthrough
- `docs/MODULE_GUIDE.md`: contributor codebase tour
- `docs/USER_MODEL.md`: user-facing design model
- `docs/api.md`: API reference
- `docs/LINKING_GUIDE.md`: focused linking guide
- `docs/DESIGN_SUMMARY.cn.md`: Chinese design summary

**Last Updated**: May 20, 2026  
**Status**: Current Architecture Summary and Design Archive

## Overview

LTSeq is a hybrid Python-Rust library for sequence-oriented data processing. It combines a Pythonic lambda-based DSL with a Rust/DataFusion execution engine and treats row order as part of the query model rather than as display-only metadata.

The core philosophy is:

- lazy evaluation
- explicit ordering guarantees
- fluent Python ergonomics
- Rust-side execution for performance and consistency

This document now serves two roles:

- a concise summary of the current architecture
- a design archive for important implementation decisions and historical lessons

For deeper reading:

- `docs/ARCHITECTURE.md`: full architecture walkthrough
- `docs/MODULE_GUIDE.md`: contributor codebase tour
- `docs/USER_MODEL.md`: user-facing design model
- `docs/api.md`: API reference
- `docs/LINKING_GUIDE.md`: focused linking guide

---

## Table of Contents

1. [Current Architecture](#1-current-architecture)
2. [Expression System](#2-expression-system)
3. [Ordered Semantics](#3-ordered-semantics)
4. [Linking, Joining, and Grouping](#4-linking-joining-and-grouping)
5. [Execution Strategy](#5-execution-strategy)
6. [Testing and Performance](#6-testing-and-performance)
7. [Design Lessons](#7-design-lessons)
8. [Historical Notes](#8-historical-notes)

---

## 1. Current Architecture

### 1.1 Hybrid Python-Rust Design

LTSeq is split into two primary layers.

- **Python layer** in `py-ltseq/ltseq/`: public API, mixin organization, schema tracking, sort metadata tracking, lambda capture, and high-level wrapper objects
- **Rust layer** in `src/`: PyO3 extension module, lazy DataFusion plan handling, expression transpilation, terminal execution, and specialized sequence implementations

In current code, the main user-facing Python object is `LTSeq`, and the main Rust execution object is `LTSeqTable`.

### 1.2 Lazy Evaluation Model

Most operations return a new lazy query object instead of executing immediately.

Common lazy operations include:

- `filter()`
- `select()`
- `derive()`
- `sort()`
- `slice()`
- `join()`
- `group_ordered()`

Execution happens at terminal boundaries such as:

- `show()`
- `count()`
- `collect()`
- `to_arrow()`
- `to_pandas()`
- file writes

This allows DataFusion to optimize logical plans and lets LTSeq preserve a consistent query-first model.

### 1.3 Python API Composition

`LTSeq` is built through mixin composition rather than a single oversized class.

Key Python modules include:

- `core.py`
- `io_ops.py`
- `transforms.py`
- `joins.py`
- `aggregation.py`
- `advanced_ops.py`
- `mutation_mixin.py`

This keeps the public surface broad while keeping responsibilities reasonably separated.

### 1.4 Rust Execution Shell + Modular Ops

Rust exposes `LTSeqTable` through a single `#[pymethods]` block in `src/lib.rs`, then delegates most real work to `src/ops/*` helper modules.

That pattern exists for two reasons:

- PyO3 favors a centralized method exposure surface
- the implementation stays maintainable only if execution logic is split by capability

Representative Rust modules:

- `src/ops/derive.rs`
- `src/ops/window.rs`
- `src/ops/grouping.rs`
- `src/ops/join.rs`
- `src/ops/asof_join.rs`
- `src/ops/parallel_scan.rs`

### 1.5 Core Metadata Model

Two metadata systems are central to the architecture:

- Python-side `_schema` / Rust-side Arrow schema
- Python-side `_sort_keys` / Rust-side `sort_exprs`

The first keeps user-visible validation and execution state aligned. The second makes ordered operations explicit and safe.

---

## 2. Expression System

### 2.1 Lambda DSL via SchemaProxy

Python lambdas are used as a declarative DSL.

Example:

```python
t.filter(lambda r: r.age > 18)
```

`r` is a `SchemaProxy`, not a real row object. Attribute access and operators build expression nodes instead of evaluating row values in Python.

### 2.2 Serialization Boundary

Captured expressions are serialized into dictionaries before crossing into Rust.

Examples:

```python
{"type": "Column", "name": "age"}
{"type": "Literal", "value": 18, "dtype": "Int64"}
```

On the Rust side, these become `PyExpr` values in `src/types.rs`.

### 2.3 Transpilation Paths

After deserialization, expressions may follow one of several paths:

- native DataFusion expression generation
- native window expression generation
- SQL generation fallback

Key files:

- `src/transpiler/mod.rs`
- `src/transpiler/window_native.rs`
- `src/transpiler/sql_gen.rs`
- `src/transpiler/optimization.rs`

### 2.4 Practical Boundaries

LTSeq's expression system is intentionally constrained. It supports Pythonic column expressions well, but it does not attempt to execute arbitrary Python logic row-by-row. That tradeoff keeps the system serializable and Rust-executable.

---

## 3. Ordered Semantics

### 3.1 Explicit Sort Requirement

One of LTSeq's strongest design decisions is that sequence-dependent operations require explicit order.

- `shift()`
- `diff()`
- `rolling()`
- many ranking/window workflows

If sort order is unknown, LTSeq prefers to raise instead of guessing.

### 3.2 Sort Metadata Preservation

Some operations preserve sort metadata because they keep row order meaningful, including many filters, derives, and slices.

Other operations may invalidate sort metadata, including operations that reorder rows or produce structurally new tables.

This propagation behavior is part of the core semantics, not just an optimization detail.

### 3.3 Sorted Fast Paths

Sort metadata also enables optimized execution paths for specific workloads, especially on sorted Parquet inputs and sequence scans.

---

## 4. Linking, Joining, and Grouping

### 4.1 Pointer-Based LinkedTable

`link()` returns a `LinkedTable`, a lazy relationship object rather than an immediately joined flat table.

This allows source-only operations to remain cheap and delays join cost until linked-side columns are actually needed.

### 4.2 Join Column Conflict Strategy

Join execution must handle duplicate or conflicting right-side column names carefully. LTSeq's join implementation aggressively renames right-side columns before joining, then aliases them back into the user-visible schema shape.

This has proven necessary for correctness and predictability.

### 4.3 Grouped Sequential Context

`group_ordered()` and `group_sorted()` return `NestedTable`, not a collapsed aggregate result. These workflows preserve grouped row context through internal columns such as:

- `__group_id__`
- `__group_count__`
- `__rn__`

This enables group-aware filtering, deriving, and first/last style analysis without immediately flattening the grouped structure.

### 4.4 Partitioning

`partition()` exposes grouped access patterns while staying as close as possible to the lazy/query path. Callable partitioning is intentionally constrained to expressions that can still be captured and executed by the engine.

---

## 5. Execution Strategy

### 5.1 DataFusion-First

The preferred path is to keep work as native DataFusion logical plans and expressions.

### 5.2 SQL Fallback

Some operations, especially grouped or window-heavy transformations, still use generated SQL and temporary tables when that is the most practical implementation route.

### 5.3 Specialized Rust Paths

LTSeq also contains specialized implementations for ordered workloads where generic planning is not the best fit.

Examples include:

- direct sequence scans
- parallel scans
- pattern matching
- as-of join binary search logic

### 5.4 No-Materialization Rule

Any API that returns `LTSeq`, `NestedTable`, `LinkedTable`, or `PartitionedTable` should stay on the lazy Rust/DataFusion path whenever possible. Materialization is reserved for explicit export or terminal APIs.

This is a defining architectural rule and is enforced by tests.

---

## 6. Testing and Performance

### 6.1 Capability-Oriented Tests

The test suite in `py-ltseq/tests/` is organized primarily by behavior and capability rather than by source file.

Major categories include:

- expressions
- transforms
- sort tracking and windows
- grouping and nested tables
- joins and as-of joins
- linking and pointer syntax
- partitioning and set operations
- architecture guardrails such as no accidental materialization

### 6.2 Benchmarks as Architecture Feedback

Benchmarks are used to validate whether architectural choices hold up under realistic ordered workloads. The project uses them to identify materialization bottlenecks and justify specialized native implementations where necessary.

---

## 7. Design Lessons

### 7.1 Order Must Be Explicit

Trying to make ordered semantics implicit leads to confusing results and fragile APIs. Requiring explicit sort state has been a net improvement.

### 7.2 Schema Synchronization Is Critical

The combination of Python-side schema tracking and Rust-side Arrow schema tracking improves ergonomics but requires strict synchronization, especially after joins and materialization boundaries.

### 7.3 DataFusion Is Strong, but Not Sufficient for Everything

DataFusion handles a large part of LTSeq's execution needs well, but sequence-specific workloads still justify dedicated logic in some paths.

### 7.4 Materialization Is the Main Architectural Cost Center

The most important performance problems tend not to be low-level compute kernels. They tend to come from collect/register/re-query patterns around complex operations.

---

## 8. Historical Notes

The previous version of this document served as a comprehensive design archive. That historical role is still useful, but the repository now separates current architecture, contributor orientation, and user mental model into dedicated companion documents.

When updating this file in the future:

- keep the high-level architecture summary current
- preserve important design lessons and rationale
- move deep implementation walkthroughs into `docs/ARCHITECTURE.md` or targeted guides where appropriate
