# LTSeq Module Guide

Related documents:

- `docs/README.md`: documentation index
- `docs/ARCHITECTURE.md`: system architecture
- `docs/MODULE_GUIDE.cn.md`: Chinese contributor guide
- `docs/DESIGN_SUMMARY.md`: design history and tradeoffs
- `docs/api.md`: public API behavior

## Purpose

This document is a contributor-oriented map of the codebase. It answers a practical question: if you need to change a feature, where should you start reading?

Use this together with:

- `docs/ARCHITECTURE.md` for system structure
- `docs/DESIGN_SUMMARY.md` for design history
- `docs/api.md` for public API behavior

---

## Repository Map

Top-level directories with architectural significance:

- `src/`: Rust execution core
- `py-ltseq/ltseq/`: Python package and public API layer
- `py-ltseq/tests/`: capability-oriented tests
- `examples/`: user workflows and product examples
- `docs/`: API, architecture, and design documents
- `benchmarks/`: performance experiments and autoresearch tooling

---

## Start Here by Role

### If you are debugging user-facing API behavior

Start with:

- `py-ltseq/ltseq/core.py`
- the relevant mixin file in `py-ltseq/ltseq/`
- the matching test file in `py-ltseq/tests/`

### If you are debugging execution behavior or Rust errors

Start with:

- `src/lib.rs`
- the relevant file in `src/ops/`
- `src/transpiler/` if expressions are involved

### If you are debugging expression capture

Start with:

- `py-ltseq/ltseq/expr/transforms.py`
- `py-ltseq/ltseq/expr/proxy.py`
- `py-ltseq/ltseq/expr/types.py`
- `src/types.rs`
- `src/transpiler/mod.rs`

### If you are debugging sort-sensitive or window behavior

Start with:

- `py-ltseq/ltseq/transforms.py`
- `py-ltseq/ltseq/joins.py`
- `src/ops/sort.rs`
- `src/ops/window.rs`
- the sort/window tests

---

## Python Package Tour

### `py-ltseq/ltseq/__init__.py`

Public package surface. Re-exports `LTSeq`, expression helpers, and high-level wrapper types.

### `py-ltseq/ltseq/core.py`

Main table class definition. This is where `LTSeq` is assembled from mixins and where common metadata lives.

Important things here:

- Rust binding import and availability check
- `_from_inner()` wrapper construction
- common representation helpers
- top-level object metadata such as `_schema` and `_sort_keys`

### `py-ltseq/ltseq/io_ops.py`

Read/write and interoperability surface.

Read this when working on:

- CSV/Parquet I/O
- Arrow or pandas conversion
- constructors such as `from_dict`, `from_rows`, `from_arrow`, `from_pandas`
- streaming entrypoints

### `py-ltseq/ltseq/transforms.py`

Core transform API.

Read this when working on:

- `filter`
- `select`
- `derive`
- `sort`
- `slice`
- sort metadata propagation
- lookup-aware derive behavior

### `py-ltseq/ltseq/joins.py`

Join-facing API entrypoints.

Read this when working on:

- normal joins
- sorted joins
- join preconditions and user-facing validation
- as-of join API behavior

### `py-ltseq/ltseq/aggregation.py`

Aggregation, grouping, and partition-related API entrypoints.

Read this when working on:

- `agg`
- `group_by`
- `group_ordered`
- `group_sorted`
- `partition`

### `py-ltseq/ltseq/advanced_ops.py`

Set operations and advanced user-facing methods that do not fit basic transforms cleanly.

### `py-ltseq/ltseq/mutation_mixin.py`

Mutation helpers with copy-on-write behavior. Read this when changing methods that look mutative from the user perspective but still need to preserve LTSeq's immutable table style.

### `py-ltseq/ltseq/lookup.py`

Lookup expression rewriting. This is the bridge that lets derive expressions trigger join-like behavior without exposing a separate user-level join step.

---

## Expression Subsystem Tour

The expression subsystem is split across several focused files.

### `py-ltseq/ltseq/expr/proxy.py`

Defines the proxy objects used to capture lambda expressions. If a lambda references a column, this file is in the path.

### `py-ltseq/ltseq/expr/base.py`

Base expression machinery and serialization behavior.

### `py-ltseq/ltseq/expr/types.py`

Concrete expression node types such as columns, calls, aliases, windows, and literals.

### `py-ltseq/ltseq/expr/accessors.py`

String and datetime accessor surface such as `.s` and `.dt`.

### `py-ltseq/ltseq/expr/transforms.py`

Core lambda-to-expression conversion. If you need to understand how `lambda r: r.a > 1` becomes a serialized tree, start here.

This file also contains the limited AST transformation logic used for `is None` and `is not None` handling.

---

## Grouping and Nested Table Tour

### `py-ltseq/ltseq/grouping/nested_table.py`

This is the main implementation of `NestedTable`.

Read it when working on:

- `group_ordered` behavior
- `group_sorted` behavior
- group-level `filter` or `derive`
- flattening grouped results
- internal grouping metadata semantics

### `py-ltseq/ltseq/grouping/proxies/`

Group-proxy logic for expressions that operate at the grouped context level.

Read this if group filters or group derives are failing to translate correctly.

---

## Linking Tour

### `py-ltseq/ltseq/linking.py`

Implements `LinkedTable`, the lazy relationship abstraction.

Read this when working on:

- `link()` behavior
- lazy vs materialized join boundaries
- linked-column access
- chained linking behavior
- alias and prefixed-column semantics

If the bug involves linked tables suddenly turning into flat tables or schema mismatches after linking, this file is likely relevant.

---

## Partitioning Tour

### `py-ltseq/ltseq/partitioning.py`

Implements partitioned views and SQL-backed partition behavior.

Read this when working on:

- `partition()` output types
- callable partition constraints
- partition lookup behavior

---

## Rust Core Tour

### `src/lib.rs`

The Rust module boundary exposed to Python.

Read this when you need to understand:

- what methods Python can call directly
- how `LTSeqTable` is constructed and returned
- where Rust execution starts

Do not assume the logic lives here. Most interesting work is delegated elsewhere.

### `src/engine.rs`

Session and runtime configuration.

Read this when investigating:

- DataFusion configuration
- Tokio runtime setup
- execution-context defaults

### `src/cursor.rs`

Streaming support for lazy scan APIs.

### `src/types.rs`

Expression deserialization into `PyExpr`.

### `src/error.rs`

Internal error types and Python exception mapping.

---

## `src/ops/` Tour

This directory is the real execution surface of the Rust core.

### `src/ops/basic.rs`

Basic optimized operations such as targeted search helpers.

### `src/ops/derive.rs`

Standard derive flow and dispatch into window-aware logic when needed.

### `src/ops/window.rs`

Window function execution, including native and fallback handling.

### `src/ops/sort.rs`

Sorting and sorted-metadata handling, including `assume_sorted`-style behavior.

### `src/ops/grouping.rs`

Ordered grouping machinery and group metadata generation.

### `src/ops/join.rs`

Join execution and schema conflict handling for right-side columns.

### `src/ops/asof_join.rs`

Specialized as-of join implementation. Read this before changing time-nearest matching behavior.

### `src/ops/aggregation.rs`

Aggregations and grouped execution logic.

### `src/ops/set_ops.rs`

Union, intersect, diff, and related set-style operations.

### `src/ops/pattern_match.rs`

Sequence matching and funnel-style logic.

### `src/ops/linear_scan.rs`

Single-pass sequence scanning logic. Important for sorted/ordered optimizations.

### `src/ops/parallel_scan.rs`

Parallel and Parquet-aware fast paths.

### `src/ops/align.rs`

Alignment-related table operations.

### `src/ops/pivot.rs`

Pivot support.

### `src/ops/mutation.rs`

Rust-side mutation helpers.

### `src/ops/io.rs`

CSV/Parquet/Arrow I/O helpers.

### `src/ops/common.rs`

Shared helpers used across multiple operations. If two execution paths seem to rely on the same schema or temp-table logic, look here.

---

## `src/transpiler/` Tour

### `src/transpiler/mod.rs`

Primary `PyExpr` to DataFusion `Expr` transpiler.

### `src/transpiler/window_native.rs`

Native DataFusion window expression generation.

### `src/transpiler/sql_gen.rs`

SQL generation fallback for expressions that are awkward or unsupported in the native path.

### `src/transpiler/optimization.rs`

Expression simplification and optimization before execution.

---

## Where to Start for Common Changes

### Add a new simple expression function

Read in this order:

1. `py-ltseq/ltseq/expr/types.py`
2. `py-ltseq/ltseq/expr/base.py`
3. `src/types.rs`
4. `src/transpiler/mod.rs`
5. relevant tests in `py-ltseq/tests/test_expr*.py`

### Change `derive()` behavior

Read in this order:

1. `py-ltseq/ltseq/transforms.py`
2. `py-ltseq/ltseq/lookup.py` if lookups may be involved
3. `src/ops/derive.rs`
4. `src/ops/window.rs` if window expressions are involved
5. `py-ltseq/tests/test_derive.py`

### Change sort or window semantics

Read in this order:

1. `py-ltseq/ltseq/transforms.py`
2. `src/ops/sort.rs`
3. `src/ops/window.rs`
4. `py-ltseq/tests/test_sort_tracking.py`
5. `py-ltseq/tests/test_window_partition_by.py`
6. `py-ltseq/tests/test_ranking.py`

### Change linking behavior

Read in this order:

1. `py-ltseq/ltseq/linking.py`
2. `src/ops/join.rs`
3. `docs/LINKING_GUIDE.md`
4. `py-ltseq/tests/test_linking_*.py`

### Change ordered grouping behavior

Read in this order:

1. `py-ltseq/ltseq/grouping/nested_table.py`
2. grouping proxies in `py-ltseq/ltseq/grouping/proxies/`
3. `src/ops/grouping.rs`
4. `py-ltseq/tests/test_group_ordered.py`
5. `py-ltseq/tests/test_group_sorted.py`

### Change I/O behavior

Read in this order:

1. `py-ltseq/ltseq/io_ops.py`
2. `src/lib.rs` read/write methods
3. `src/ops/io.rs`
4. `py-ltseq/tests/test_csv_io.py` and `test_parquet_io.py`

---

## Test Suite as a Navigation Tool

The tests are one of the best entrypoints into the codebase because they are organized by product capability.

Useful categories:

- `test_expr*.py`: expression capture and optimization
- `test_derive.py`, `test_relational_ops.py`: core transforms
- `test_sort_tracking.py`, `test_shift_default.py`, `test_window_partition_by.py`: order-sensitive logic
- `test_group_*.py`, `test_nested_*.py`: grouping model
- `test_join*.py`, `test_asof_join.py`, `test_semi_anti_joins.py`: join family
- `test_linking_*.py`, `test_lookup.py`, `test_pointer_syntax.py`: linking model
- `test_partition*.py`: partitioning
- `test_no_materialization_rule.py`: architecture guardrail

When in doubt, find the most specific relevant test first, then trace into implementation.

---

## Build and Verification Commands

Common commands from `CLAUDE.md`:

```bash
maturin develop
pytest py-ltseq/tests/ -v
cargo check
cargo test
```

For focused work, prefer running the narrowest relevant test file first.

---

## Contributor Heuristics

When making changes, keep these repository-specific rules in mind:

- preserve lazy execution unless the API is explicitly terminal
- do not break sort metadata propagation casually
- avoid introducing Python-side materialization inside table-returning APIs
- keep Python `_schema` and Rust schema behavior aligned
- prefer small, local changes in the relevant module rather than cross-cutting refactors

Those heuristics are more useful than memorizing every file.
