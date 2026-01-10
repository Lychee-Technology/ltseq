# LTSeq Tasks

**Last Updated**: January 9, 2026  
**Current Status**: Phase 10 Complete (704 tests), ready for future enhancements

---

## Completed: Phase 8 - Streaming & Big Data ✅

| Task | Description | Status |
|------|-------------|--------|
| 1 | `LTSeqCursor` struct in Rust | ✅ |
| 2 | `scan_csv` / `scan_parquet` methods | ✅ |
| 3 | Python `Cursor` wrapper | ✅ |
| 4 | `LTSeq.scan()` / `scan_parquet()` API | ✅ |
| 5 | Streaming tests (9 tests) | ✅ |

---

## Completed: Phase 9 - Pointer Syntax Sugar ✅

Already implemented via `SchemaProxy` and `NestedSchemaProxy` in `expr/proxy.py`.

**Syntax:**
```python
linked = orders.link(products, on=lambda o,p: o.product_id == p.product_id, as_='prod')

# Pointer syntax: r.prod.name instead of r.prod_name
result = linked.select(lambda r: r.prod.name)
result = linked.filter(lambda r: r.prod.price > 10)
```

| Tests Added | Status |
|-------------|--------|
| `test_pointer_select_single_column` | ✅ |
| `test_pointer_select_multiple_columns` | ✅ |
| `test_pointer_in_filter` | ✅ |
| `test_pointer_in_derive` | ✅ |
| `test_pointer_invalid_column_error` | ✅ |
| `test_pointer_invalid_alias_error` | ✅ |

---

## Completed: Phase 10 - Performance Optimization ✅

**Analysis**: DataFusion/Arrow already handle SIMD and multi-threading. Focus was on configuration tuning and reducing materialization overhead. See `docs/DESIGN_SUMMARY.md` Section 10 for full rationale.

| # | Task | Description | Effort | Status |
|---|------|-------------|--------|--------|
| 1 | Benchmark suite | Create 5-10 key benchmarks to measure baseline | 0.5 day | ✅ |
| 2 | DataFusion configuration | Configure SessionContext with batch_size, target_partitions | 1 day | ✅ |
| 3 | Reduce join materialization | Optimize join_impl to avoid double-collection | 1-2 days | ✅ Analyzed |
| 4 | Reduce window materialization | Optimize derive_with_window_functions_impl | 1 day | ✅ Analyzed |
| 5 | Expression optimization | Constant folding, predicate combining | 1 day | ✅ |

### Completed: Benchmark Suite
Created `benchmarks/bench_core.py` with 23 benchmarks covering:
- Filter (simple & complex predicates)
- Derive (single & multiple columns)
- Join (various sizes)
- Window functions (LAG, cumsum)
- Group aggregation
- Chained operations
- CSV I/O

### Completed: DataFusion Configuration
Updated `src/engine.rs` with optimized `SessionContext` factory:
- `target_partitions`: Set to CPU count for parallel execution
- `repartition_joins`: Enabled for parallel join execution  
- `repartition_aggregations`: Enabled for parallel aggregations
- `coalesce_batches`: Enabled for efficient batch merging

### Analyzed: Join Materialization
**Finding**: Current implementation already collects both DataFrames in a single async block (parallel collection). The MemTable registration is required for SQL-based join with column aliasing.

**Benchmark**: Join operations achieve ~300K rows/sec, which is reasonable for the complexity involved.

**Optimization opportunity** (deferred): Use DataFusion's native `join()` DataFrame API for simple equijoins where column aliasing isn't needed. This would skip the MemTable round-trip but requires detecting compatible schemas.

### Analyzed: Window Function Materialization
**Finding**: Window functions (LAG, LEAD, rolling, cumsum) require SQL execution, which necessitates the MemTable pattern. This is ~26x slower than filter/derive (~75K vs ~2M rows/sec).

**Root cause**: DataFusion's SQL engine needs registered tables to execute OVER clauses.

**Optimization opportunity** (deferred): Use DataFusion's DataFrame `window()` API directly with `WindowExpr` objects. This would avoid the MemTable round-trip but requires:
1. Building `WindowExpr` objects instead of SQL strings
2. Refactoring transpiler to emit DataFusion window expressions
3. Significant code changes to `src/ops/window.rs`

**Decision**: Defer to future phase as it requires substantial refactoring.

### Completed: Expression Optimization
Implemented compile-time expression optimization in `src/transpiler.rs`:

**Constant Folding** (arithmetic operations on literals):
- `1 + 2 + r.col` → `3 + r.col`
- `(10 + 5) * 2 + r.col` → `30 + r.col`
- Supports: `+`, `-`, `*`, `/`, `%`

**Boolean Simplification**:
- `~True` → `False`, `~False` → `True`
- Note: Due to Python's eager evaluation of pure literals, boolean simplification
  works best when at least one operand involves a column reference

**Additional Fixes**:
- Fixed boolean literal parsing to handle Python's capitalized "True"/"False"

**Tests Added**: 19 new tests in `test_expr_optimization.py`

---

## Quick Reference

```bash
uv run python -m pytest py-ltseq/tests -v
uv run maturin develop
uv run python benchmarks/bench_core.py
```

---

## Notes
- **704 tests passing**, 0 skipped
- **Pandas**: Only `partition(by=callable)` uses pandas (by design)

---

## Recent Fixes (This Session)

### 1. Fixed Join Schema Error
**Problem**: `join_impl` failed when joining tables that went through operations like `group_ordered()`, `filter()`, or `flatten()`.

**Solution**: Refactored `src/ops/advanced.rs` to use SQL-based join execution instead of DataFusion's DataFrame API.

### 2. Added `__len__` to LinkedTable
**Problem**: `len()` on `LinkedTable` raised `TypeError`.

**Solution**: Added `__len__` method to `py-ltseq/ltseq/linking.py` that triggers materialization and returns row count.

**Tests Added**: 4 new tests in `test_linking_basic.py::TestLinkLenOperation`

### 3. Expression Optimization (Phase 10 Task 5)
**Implemented**: Constant folding and boolean simplification in `src/transpiler.rs`.

**Files Modified**:
- `src/transpiler.rs` - Added `optimize_expr()` function and helper functions
- `py-ltseq/tests/test_expr_optimization.py` - 19 new tests

---

## Future Enhancement Ideas

1. **Direct DataFrame Window API**: Skip MemTable for window functions by using DataFusion's DataFrame `window()` method with `WindowExpr` objects.

2. **Join Optimization**: Use native DataFrame `join()` for simple equijoins without column aliasing needs.

3. **Lazy Evaluation**: Implement a query builder pattern that defers all operations until materialization, allowing for more aggressive optimization.

4. **Predicate Pushdown**: Push filter predicates down through joins and aggregations for early row elimination.
