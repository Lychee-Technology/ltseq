# LTSeq Design Summary

**Last Updated**: January 8, 2026  
**Status**: Comprehensive Design Archive

## Overview
LTSeq is a hybrid Python-Rust library for high-performance sequential data processing. It combines a Pythonic lambda-based DSL with the raw speed of Rust's DataFusion engine. The core philosophy is **lazy evaluation with strict ordering guarantees**, enabling complex time-series and sequential operations (shift, rolling, diff, cum_sum) that are difficult or slow in standard SQL/pandas.

This document consolidates design decisions, architectural patterns, and lessons learned throughout development.

---

## Table of Contents
1. [Core Architecture](#1-core-architecture)
2. [Expression System](#2-expression-system)
3. [Relational Operations](#3-relational-operations)
4. [Sequence Operators](#4-sequence-operators)
5. [Linking & Joins](#5-linking--joins)
6. [Optimization Strategy](#6-optimization-strategy)
7. [Grouping Operations](#7-grouping-operations)
8. [API Design Patterns](#8-api-design-patterns)
9. [Lessons Learned](#9-lessons-learned)

---

## 1. Core Architecture

### 1.1 Hybrid Python-Rust Design
- **Python Layer**: Handles API surface, DSL parsing, schema tracking, and high-level logic. Uses `_schema` dict to track columns without querying the engine.
- **Rust Layer (py-ltseq)**: Exposes a `RustTable` class via PyO3. Wraps Apache DataFusion for execution.
- **Interaction**: Python builds a logical plan; Rust executes it only when `.show()`, `.to_pandas()`, or `.count()` is called.

### 1.2 Lazy Evaluation Model
- Operations like `filter()`, `select()`, `with_columns()` return a *new* `LTSeq` instance with an updated plan.
- No data is processed until terminal actions.
- **Benefit**: Allows query optimization (predicate pushdown, projection pushdown) by DataFusion.

### 1.3 Sort Order Tracking
- **Crucial Feature**: Unlike standard SQL engines which treat tables as unordered sets, LTSeq respects order.
- **Mechanism**: `RustTable` maintains a `sort_exprs` vector.
- **Invariant**: Window functions (shift, rolling) require explicit sorting. If `.sort()` hasn't been called, these operations raise an error.

---

## 2. Expression System

### 2.1 Lambda DSL via SchemaProxy
- **Problem**: Python lambdas are opaque. We need to convert `lambda r: r.age > 18` into a DataFusion logical expression.
- **Solution**: `SchemaProxy` object intercepts attribute access (`r.age`).
- **Mechanism**:
  1. `r` is a `SchemaProxy`.
  2. `r.age` returns a `CallExpr` representing column "age".
  3. `r.age > 18` returns a `CallExpr` representing "binary op >".
- **Result**: A serializable expression tree (AST) built at runtime.

### 2.2 CallExpr Pattern
- Instead of complex enum variants for every operation type, we use a generic `CallExpr`.
- **Structure**: `{ func: "gt", args: [col_expr, lit_expr] }`
- **Benefit**: Reduced boilerplate by ~40% compared to explicit expression classes.

### 2.3 Serialization
- Expressions are serialized to JSON-compatible dictionaries before being passed to Rust.
- **Format**: `{"type": "Column", "name": "age"}` or `{"type": "Literal", "value": 18, "dtype": "Int64"}`.

### 2.4 Type Inference
- Automatic inference for literals: `bool`, `int`, `float`, `string`, `null`.
- Rust side deserializes these into DataFusion `ScalarValue` types.

---

## 3. Relational Operations

### 3.1 sort()
- **Constraint**: Ascending only (in early phases), multi-column support.
- **Requirement**: Must specify at least one key.
- **State**: Updates the internal `sort_exprs` state, enabling sequence operations.

### 3.2 distinct()
- **Implementation**: Hash-based deduplication using DataFusion's `LogicalPlan::Distinct`.
- **Scope**: Global by default (deduplicates across entire dataset).

### 3.3 slice()
- **Implementation**: Zero-copy logical operation mapping to SQL `LIMIT` / `OFFSET`.
- **Performance**: Extremely fast as it modifies the plan limit, not the data.

---

## 4. Sequence Operators

### 4.1 Strict Sort Requirement
- `shift()`, `diff()`, `rolling()`, `cum_sum()` **fail hard** if `.sort()` was not called previously.
- **Rationale**: Sequential operations are undefined without order. Explicit is better than implicit.

### 4.2 Sort Metadata Preservation
- "Safe" operations (filter, derive, slice) preserve the `sort_exprs` metadata.
- "Unsafe" operations (group by, join) might reset or invalidate sort requirements.

### 4.3 NULL Handling & Boundaries
- **shift(n)**: Introduces NULLs at boundaries (first `n` rows).
- **diff()**: `val - shift(1)`. First row is NULL.
- **rolling()**: Configurable `min_periods`. Defaults to window size (result is NULL until window is full).
- **cum_sum()**: Always has value (running total).

### 4.4 Expression Architecture
- Sequence ops are implemented via pattern matching on the `CallExpr` function string in the transpiler.
- They translate to DataFusion Window Functions (e.g., `LEAD/LAG`, `SUM() OVER (...)`).

---

## 5. Linking & Joins

### 5.1 Pointer-Based LinkedTable
- **Concept**: `left.link(right, on=...)` returns a `LinkedTable`.
- **State**: Stores references to left/right tables and join keys. Does **not** execute join immediately.
- **API**: Allows chaining operations on the "virtual" joined result.

### 5.2 Transparent Materialization ("Detect & Materialize")
- **Design Decision**: When a user filters or selects a column belonging to the linked (right) table, the system automatically materializes the join.
- **User Experience**: "It just works". `linked.filter(lambda r: r.right_col > 5)` triggers the join.
- **Return Types**:
  - `LinkedTable`: No materialization happened (operations were on left table only).
  - `LTSeq`: Materialization occurred (result is a flat table).

### 5.3 Column Renaming Strategy
- **Critical Issue**: DataFusion joins fail or behave unpredictably with duplicate column names.
- **Solution**: **ALL** columns from the right table are renamed with unique temporary identifiers before joining.
- **Restoration**: After join, columns are aliased back to their expected names (or user-provided prefixes).

### 5.4 Join Types
- Supported: `inner` (default), `left`, `right`, `full`.
- Defaults to `inner` to match SQL intuition.

### 5.5 Chained Materialization
- **Bug Fix**: Fixed a critical bug where chaining multiple links caused schema mismatches.
- **Solution**: Ensure schema synchronization between Python `_schema` and Rust `ArrowSchema` after every materialization.

---

## 6. Optimization Strategy

### 6.1 Native Rust Implementations
- **join_merge**: Moved join logic to Rust. **5-10x speedup**.
- **search_first**: Implemented as `Limit(1)` + `Filter` in Rust. **10-87x speedup** (dataset dependent).

### 6.2 Deferred Optimizations
- **pivot**: Analysis showed Pandas pivot is sufficient for typical result set sizes (hundreds/thousands of rows). Kept in Python for now.
- **Window Functions**: Already optimized by DataFusion's execution planner. No custom Rust needed.

---

## 7. Grouping Operations

### 7.1 group_ordered Algorithm
- **Problem**: Standard `GROUP BY` destroys order. We need "group by X, but keep rows ordered by time within groups".
- **Solution**: Window Functions.
  - Assign `__group_id__` using dense rank or hashing.
  - Maintain sort order within partitions.

### 7.2 NestedTable Structure
- `group_ordered()` returns a `NestedTable`.
- **Metadata**: Tracks `__group_id__` and `__group_count__`.
- **Operations**: `aggregate()` reduces to 1 row per group. `filter()`/`derive()` maintain group structure.

### 7.3 Context Preservation
- `_group_assignments` dictionary preserves original group IDs even after filtering.
- Allows operations like "filter out first 2 rows of each group" while keeping group integrity.

---

## 8. API Design Patterns

### 8.1 Method Chaining
- Fluent interface: `df.sort().filter().derive()`.
- Immutability: Each call returns a new instance; original is untouched.

### 8.2 Dual Argument Forms
- **String Columns**: `df.select("a", "b")`
- **Lambda Expressions**: `df.select(lambda r: r.a + r.b)`
- **Mixed**: Supported in `with_columns` / `derive`.

### 8.3 Return Type Signals
- The type of object returned tells the user what happened:
  - `LinkedTable`: Virtual join.
  - `LTSeq`: Physical table.
  - `NestedTable`: Grouped context.

### 8.4 Error Messages
- **Philosophy**: Helpful & specific.
- **Example**: If column missing, list *available* columns.
- **Example**: If sort missing for window op, explain *why* sort is needed.

---

## 9. Lessons Learned

### 9.1 DataFusion Join Conflicts
- **Lesson**: Never trust default name handling in joins.
- **Fix**: Aggressively rename everything on the right side of a join to UUIDs/temps, then alias back.

### 9.2 AST Parsing Challenges
- **Lesson**: `inspect.getsource()` is messy. It returns the whole line/block.
- **Fix**: Use `ast` module to parse the source, then walk the tree to find the specific lambda corresponding to the argument.

### 9.3 Phased Implementation
- **Strategy**: Start narrow, expand later.
- **Example**: `cum_sum` started as "column name only". Later expanded to "arbitrary expressions". This kept momentum high.

### 9.4 Python 3.14 Compatibility
- **Lesson**: `ast.NameConstant` is deprecated.
- **Fix**: Migrated to `ast.Constant` proactively to ensure future-proofing.
