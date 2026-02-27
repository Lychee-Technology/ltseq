# LTSeq Improvement Plan: Performance, Refactoring, Dead Code, Tests

**Created**: 2026-02-26
**Status**: IN PROGRESS (Phase 3 complete, Phase 4 next)

---

## Overview

Comprehensive improvement plan covering 4 areas across 45 tasks in 5 phases.
Execution order: Dead Code → Build Config → Tests → Performance → Refactoring.

---

## Phase 1: Dead Code Cleanup

Low risk, immediate wins. Clean slate for subsequent work.

### 1.1 Rust Dead Code Removal

- [x] **T01** Remove `hello()` pyfunction and `hello` method
  - `src/lib.rs:176-186` — `fn hello(&self)` method, scaffolding/test code
  - `src/lib.rs:944-947` — standalone `fn hello()` pyfunction
  - `src/lib.rs:951` — module registration of `hello`
  - Risk: None

- [x] **T02** Remove `debug_schema()` method
  - `src/lib.rs:910-920` — debug utility, no Python-side usage
  - Risk: None

- [x] **T03** Remove unused `col_names` variable in derive
  - `src/ops/derive.rs:73` — `let mut col_names = Vec::new();`
  - `src/ops/derive.rs:92` — `col_names.push(col_name_str);`
  - Variable is populated but never read
  - Risk: None

- [x] **T04** Remove unused error variants
  - `src/error.rs:11` — `DeserializationFailed(String)` — never constructed
  - `src/error.rs:12` — `TranspilationFailed(String)` — never constructed
  - Also remove their `Display` arms at lines 21-22
  - Risk: None

- [x] **T05** Reduce visibility of internal-only exports
  - `src/lib.rs:19` — `pub use error::PyExprError` → `pub(crate) use`
  - `src/lib.rs:20` — `pub use format::{format_cell, format_table}` → `pub(crate) use`
  - `src/lib.rs:21` — `pub use types::{dict_to_py_expr, PyExpr}` → `pub(crate) use`
  - `src/engine.rs:15` — `pub const DEFAULT_BATCH_SIZE` → `pub(crate) const`
  - `src/ops/window.rs:37` — `pub fn apply_over_to_window_functions` → `pub(crate) fn`
  - Risk: Low

- [x] **T06** Eliminate `src/ops/advanced.rs` re-export layer
  - File is 39 lines of pure `pub use` re-exports
  - Update all `crate::ops::advanced::*` imports in `src/lib.rs` to point to actual submodules
  - Delete `src/ops/advanced.rs`
  - Update `src/ops/mod.rs` to remove `pub mod advanced;`
  - Risk: Low (many import path changes in lib.rs)

### 1.2 Deprecation Fixes

- [x] **T07** Replace deprecated `PyObject` with `Py<PyAny>`
  - `src/lib.rs:229` — parameter type
  - `src/lib.rs:803` — parameter type
  - `src/ops/align.rs:25` — parameter type
  - Risk: None

- [x] **T08** Replace deprecated `Python::with_gil` with `Python::attach`
  - `src/ops/align.rs:89`
  - Risk: None

### 1.3 Python / Config Cleanup

- [x] **T09** Clean up test directory — standalone scripts
  - `py-ltseq/tests/test_filter.py` — standalone script, NOT pytest
  - `py-ltseq/tests/test_filter_code.py` — standalone script
  - `py-ltseq/tests/test_filter_debug.py` — debug helper, not a test
  - `py-ltseq/tests/test_showcase.py` — demo script
  - Action: Move to `examples/` directory or convert to proper pytest
  - Risk: None

- [x] **T10** Remove stale `[tool.setuptools.package-dir]` from pyproject.toml
  - Build backend is Maturin, not setuptools — this section is dead config
  - Risk: None

---

## Phase 2: Build & Config Optimization

### 2.1 High-Impact Build Changes

- [x] **T11** Add `[profile.release]` to Cargo.toml
  ```toml
  [profile.release]
  lto = "fat"
  codegen-units = 1
  opt-level = 3
  strip = "symbols"
  ```
  - Impact: **10-20% runtime performance gain** for compute-heavy operations
  - Risk: Longer build times (~2-3x for release builds)

- [x] **T12** Remove standalone `arrow` dependency
  - `Cargo.toml` — remove `arrow = "57.1.0"`
  - Already re-exported via `datafusion::arrow` (all `use` statements already use `datafusion::arrow`)
  - Impact: Faster compilation, no version drift risk
  - Risk: Low (verify no direct `use arrow::` imports exist)

### 2.2 Dependency Cleanup

- [x] **T13** Replace `lazy_static` with `std::sync::LazyLock`
  - `Cargo.toml` — remove `lazy_static = "1.4"`
  - `src/engine.rs` — update `lazy_static!` macro to `static RUNTIME: LazyLock<...>`
  - Requires Rust 1.80+ (already required by DataFusion 52)
  - Risk: Low

- [x] **T14** Replace `num_cpus` with `std::thread::available_parallelism()`
  - `Cargo.toml` — remove `num_cpus = "1.16"`
  - `src/engine.rs` — update `num_cpus::get()` calls
  - Stable since Rust 1.59
  - Risk: Low

- [x] **T15** Replace `futures` with `futures-util`
  - `Cargo.toml` — change `futures = "0.3"` to `futures-util = "0.3"`
  - `src/cursor.rs` — update `use futures::StreamExt` to `use futures_util::StreamExt`
  - Only `StreamExt` trait is used; no need for full futures umbrella
  - Risk: Low

- [x] **T16** Remove `tokio` `"macros"` feature if unused
  - Check for `#[tokio::main]`, `#[tokio::test]`, `tokio::select!` usage
  - If none found, remove `"macros"` from tokio features in Cargo.toml
  - Risk: None

### 2.3 Build System

- [x] **T17** Evaluate removing `build.rs`
  - PyO3 `extension-module` feature handles Python linkage automatically
  - `build.rs` manually locates libpython, which can cause double-linking
  - Action: Remove `build.rs` and `build = "build.rs"` from Cargo.toml
  - Risk: **Medium** — must test on multiple platforms; some envs may need it
  - Validation: `maturin develop` must succeed without build.rs

- [x] **T18** Fix pytest `testpaths` in pyproject.toml
  - Change `testpaths = ["tests"]` → `testpaths = ["py-ltseq/tests"]`
  - Tests live in `py-ltseq/tests/`, not root `tests/`
  - Risk: None

---

## Phase 3: Additional Tests

Add test coverage BEFORE refactoring to catch regressions.

### 3.1 Completely Untested Methods (Priority 1)

- [x] **T19** Test `LTSeq.write_csv()`
  - File: `py-ltseq/tests/test_csv_io.py` (extend existing)
  - Tests: basic write, roundtrip read-write, empty table, filtered table
  - Target: `py-ltseq/ltseq/io_ops.py` `write_csv()` method

- [x] **T20** Test `LTSeq.to_arrow()`
  - File: `py-ltseq/tests/test_core_api.py` (extend existing)
  - Tests: returns `pa.Table`, schema preserved, row count correct, empty table
  - Target: `py-ltseq/ltseq/core.py` `to_arrow()` method

- [x] **T21** Test `if_else()` standalone function
  - File: `py-ltseq/tests/test_expr.py` (extend existing) or new `test_if_else.py`
  - Tests: basic if_else in derive, nested if_else, with nulls
  - Target: `py-ltseq/ltseq/expr/base.py` `if_else()` function

- [x] **T22** Test right-hand operators (`__radd__`, `__rsub__`, `__rmul__`, etc.)
  - File: `py-ltseq/tests/test_expr.py` (extend existing)
  - Tests: `2 * r.col`, `10 + r.col`, `100 - r.col`, `1.0 / r.col`, `10 // r.col`, `10 % r.col`
  - Target: `py-ltseq/ltseq/expr/base.py` reverse operators

- [x] **T23** Test `Expr.__abs__`
  - File: `py-ltseq/tests/test_expr.py` (extend existing)
  - Tests: `abs(r.col)` on negative values, mixed positive/negative
  - Target: `py-ltseq/ltseq/expr/base.py` `__abs__()` method

- [x] **T24** Test `NestedTable.__len__()` and `.to_pandas()`
  - File: `py-ltseq/tests/test_group_ordered.py` (extend existing)
  - Tests: `len(groups)` returns group count, `.to_pandas()` returns DataFrame
  - Target: `py-ltseq/ltseq/grouping/nested_table.py`

### 3.2 Serialization-Only → End-to-End Tests (Priority 2)

- [x] **T25** Test `fill_null()` end-to-end
  - File: `py-ltseq/tests/test_expr.py` (extend)
  - Test: create table with nulls, derive with `fill_null()`, verify replaced values
  - Target: `py-ltseq/ltseq/expr/base.py` `fill_null()` method

- [~] **T26** Test `cast()` end-to-end — CANCELLED (not supported by Rust transpiler)
  - File: `py-ltseq/tests/test_expr.py` (extend)
  - Tests: int→float, string→int, verify types in result
  - Target: `py-ltseq/ltseq/expr/base.py` `cast()` method

- [~] **T27** Test `between()` end-to-end — CANCELLED (not supported by Rust transpiler)
  - File: `py-ltseq/tests/test_expr.py` (extend)
  - Test: `t.filter(lambda r: r.price.between(50, 150))`, verify filtered rows
  - Target: `py-ltseq/ltseq/expr/base.py` `between()` method

- [~] **T28** Test `alias()` end-to-end — CANCELLED (not supported by Rust transpiler)
  - File: `py-ltseq/tests/test_expr.py` (extend)
  - Test: `t.select(lambda r: r.price.alias("cost"))`, verify column renamed
  - Target: `py-ltseq/ltseq/expr/base.py` `alias()` method

- [~] **T29** Test `//` (floor division) end-to-end — CANCELLED (FloorDiv not supported by Rust transpiler)
  - File: `py-ltseq/tests/test_expr.py` (extend)
  - Test: `t.derive(bucket=lambda r: r.price // 50)`, verify integer division
  - Target: `py-ltseq/ltseq/expr/base.py` `__floordiv__()` method

- [x] **T30** Test `.dt.diff()` end-to-end
  - File: `py-ltseq/tests/test_temporal.py` (extend)
  - Test: actual date subtraction between two temporal columns/values
  - Target: `py-ltseq/ltseq/expr/accessors.py` `TemporalAccessor.diff()` method

### 3.3 Edge Cases & Error Paths (Priority 3)

- [~] **T31** Test `show()` edge cases — SKIPPED (already well-tested in test_csv_io.py)
  - File: `py-ltseq/tests/test_core_api.py` (extend)
  - Tests: empty table, table with null values, wide tables
  - Target: `py-ltseq/ltseq/core.py` `show()` method

- [x] **T32** Test I/O error handling
  - File: `py-ltseq/tests/test_csv_io.py` (extend)
  - Tests: nonexistent file path, malformed CSV
  - Target: `py-ltseq/ltseq/io_ops.py` `read_csv()`, `read_parquet()`

- [x] **T33** Test `LinkedTable` extended operations
  - File: `py-ltseq/tests/test_linking_basic.py` (extend)
  - Tests: `linked.sort()`, `linked.slice()`, `linked.distinct()`
  - Target: `py-ltseq/ltseq/linking.py` LinkedTable methods

- [x] **T34** Test empty table handling
  - File: `py-ltseq/tests/test_core_api.py` (extend)
  - Tests: `collect()` on empty, `to_pandas()` on empty, `filter()` resulting in empty
  - Target: various core methods

---

## Phase 4: Performance Optimization

With tests in place, optimize confidently.

### 4.1 Reduce Unnecessary Materialization

- [ ] **T35** Eliminate `collect()` → MemTable round-trips in SQL fallback paths
  - Multiple operations materialize DataFrames and re-register as MemTables when
    falling back to SQL. These should use DataFusion's DataFrame API directly.
  - Files:
    - `src/ops/aggregation.rs` — SQL-based GROUP BY
    - `src/ops/pivot.rs` — pivot via SQL
    - `src/ops/align.rs` — align via SQL
    - `src/ops/derive_sql.rs` — window derive via SQL
    - `src/ops/grouping.rs` — group ID computation
  - Impact: Significant for large datasets (avoids full materialization)
  - Risk: Medium (need to verify DataFrame API equivalence)

### 4.2 Reduce Cloning

- [ ] **T36** Extract `LTSeqTable` construction into a builder/helper
  - The pattern `LTSeqTable { session: Arc::clone(...), ... }` is repeated 30+ times
  - Create `LTSeqTable::from_df(session, df, schema)` method
  - Also `LTSeqTable::from_batches(session, batches)` for the collect→MemTable pattern
  - Files: `src/lib.rs`, all files in `src/ops/`
  - Risk: Low (mechanical refactor)

- [ ] **T37** Audit and reduce `(**df).clone()` patterns
  - DataFrame cloning via `(**df).clone()` happens in every operation
  - Some clones are unavoidable (DataFusion requires ownership), but audit for cases
    where the same df is cloned multiple times in one function
  - Files: all files in `src/ops/`
  - Risk: Low

### 4.3 I/O Performance

- [ ] **T38** Use Arrow's CSV writer instead of manual formatting
  - `src/ops/io.rs` formats CSV by iterating cell-by-cell with `format_cell()`
  - Arrow's `arrow::csv::Writer` would be significantly faster
  - Risk: Low (Arrow CSV writer is well-tested)

### 4.4 Display Performance

- [ ] **T39** Optimize `format_cell` dispatch
  - `src/format.rs` — `format_cell` uses chained `downcast_ref` (tries each type sequentially)
  - Refactor to match on `DataType` at the column level, then use typed access
  - This avoids repeated failed downcasts for each cell
  - Risk: Low

### 4.5 As-of Join Performance

- [ ] **T40** Optimize batch handling in `asof_join`
  - `src/ops/asof_join.rs` — concatenates all batches with `concat_batches()` then `take()`
  - Could process batch-by-batch or use more efficient indexing
  - Risk: Medium (correctness-critical join logic)

---

## Phase 5: Refactoring

Clean up code structure last, tests catch any breakage.

### 5.1 Code Duplication Reduction

- [ ] **T41** Extract shared `get_df_and_schema()` helper
  - Duplicated between `src/ops/join.rs`, `src/ops/set_ops.rs`, `src/ops/grouping.rs`
  - Create shared utility in `src/ops/mod.rs` or new `src/ops/utils.rs`
  - Risk: Low

- [ ] **T42** Extract schema-extraction pattern into helper
  - `df_schema.fields().iter().map(|f| (**f).clone()).collect()` repeated ~15 times
  - Create `fn extract_schema(df: &DataFrame) -> Schema` helper
  - Risk: Low

- [ ] **T43** Deduplicate operator string-to-enum mapping
  - Duplicated between `src/transpiler/mod.rs` and `src/transpiler/window_native.rs`
  - Extract into shared function or constant mapping
  - Risk: Low

- [ ] **T44** Refactor type-dispatch patterns with macro
  - `extract_time_values` in `src/ops/asof_join.rs` and `extract_pivot_values` in `src/ops/pivot.rs`
  - Both use lengthy match arms over Arrow types for the same pattern
  - Create a macro `for_each_arrow_type!` or generic helper
  - Risk: Medium (macros can be hard to debug)

### 5.2 Architecture Improvements

- [ ] **T45** Consolidate or remove SQL fallback paths
  - Many ops have dual paths: native DataFusion API + SQL string generation
  - Where native path is complete, remove SQL fallback entirely
  - Files: `src/ops/window.rs`, `src/ops/aggregation.rs`, `src/ops/derive_sql.rs`
  - Risk: Medium (need to verify native path handles all cases)

---

## Summary

| Phase | Tasks | Risk | Impact |
|-------|-------|------|--------|
| 1. Dead Code | T01-T10 (10) | Low | Code cleanliness |
| 2. Build Config | T11-T18 (8) | Low-Med | 10-20% perf + faster builds |
| 3. Tests | T19-T34 (16) | None | Regression safety net |
| 4. Performance | T35-T40 (6) | Med | Significant for large data |
| 5. Refactoring | T41-T45 (5) | Low-Med | Maintainability |
| **Total** | **45 tasks** | | |

## Validation Commands

```bash
# After Phase 1-2: verify everything compiles and tests pass
cargo check 2>&1 | grep -E "warning|error"
maturin develop
pytest py-ltseq/tests/ -v

# After Phase 3: run new tests
pytest py-ltseq/tests/ -v --tb=short

# After Phase 4-5: benchmark comparison
# (use existing ClickBench benchmark if available)
```
