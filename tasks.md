# LTSeq Tasks

**Created**: January 8, 2026  
**Last Updated**: January 8, 2026  
**Current Status**: Phase 5 Complete, Phase 6 Complete, Phase 7 In Progress

---

## Completed Phases

### Phase 5: Pandas Migration ✅ COMPLETE
Migrated pandas-dependent operations to Rust/SQL.

| Task | Description | Status |
|------|-------------|--------|
| Task 0 | Documentation cleanup | ✅ Complete |
| Task 1 | Migrate `pivot()` to Rust/SQL | ✅ Complete |
| Task 2 | Migrate `NestedTable.filter()` to SQL window functions | ✅ Complete |
| Task 3 | Migrate `NestedTable.derive()` to pure SQL | ✅ Complete |
| Task 4 | Add expression-based `partition_by()` | ✅ Complete |

**Result**: Pandas dependency reduced from 4 locations to 1 (partition with callable only)

### Phase 6: Core API Completion ✅ COMPLETE
Added set algebra operations and GROUP BY aggregation.

| Task | Description | Status |
|------|-------------|--------|
| Task 1 | Set algebra (union, intersect, diff, is_subset) | ✅ 27 tests |
| Task 2 | `agg()` GROUP BY method | ✅ 15 tests |
| Task 3 | Fix Python 3.14 AST deprecation warnings | ✅ Complete |
| Task 4 | `rolling().std()` | ✅ Verified |
| Task 5 | Documentation update | ✅ Complete |

**Test Count**: 549 tests passing (2 skipped)

---

## Current: Phase 7 - Performance & Polish

### Overview
Focus on test coverage, fixing known issues, and adding missing features.

| Task | Description | Effort | Status |
|------|-------------|--------|--------|
| 7.1 | Add tests for descending sort | 1 hour | ✅ Complete (23 tests) |
| 7.2 | Fix skipped test: derive_after_filter | 30 min | ✅ Complete |
| 7.3 | Add `abs()` expression function | 1-2 hours | ✅ Complete |
| 7.4 | Distinct with specific key columns | 1-2 days | ✅ Complete (11 tests) |
| 7.5 | Performance benchmarking | 1 day | Pending |

**Test Count**: 585 tests passing (0 skipped)

---

## Task 7.1: Add Tests for Descending Sort ✅ COMPLETE

Added 23 tests for the `desc` parameter in `sort()`:

**Unit tests (TestSortDescending)**: 14 tests
- desc=True/False single column
- desc list with mixed directions
- Error handling for invalid desc values

**Integration tests (TestSortDescendingIntegration)**: 9 tests
- Verify actual data ordering with real CSV data
- Multi-column mixed sort directions
- Sort with lambda expressions
- Sort chaining with filter

---

## Task 7.2: Fix Skipped Test - derive_after_filter ✅ COMPLETE

The test was unnecessarily skipped. The functionality works correctly.

**Fixed**: `TestNestedTableDeriveIntegration::test_derive_after_filter`
- Now tests `group_ordered() → filter() → derive()` chain
- Verifies only matching groups are retained after filter
- Verifies derived columns are added correctly

---

## Task 7.3: Add abs() Expression Function ✅ COMPLETE

**Priority**: Low  
**Effort**: 1-2 hours

### Implementation Summary
Fixed the `abs()` expression function which was already partially implemented but had two bugs:

1. **Rust deserialization** (`src/types.rs`): The `parse_call_expr` function didn't handle `on: null` (Python `None`) correctly. When Python serialized `CallExpr("abs", (self,), {}, on=None)`, the Rust side tried to cast `PyNone` to `PyDict` and failed.

2. **Window function detection** (`src/transpiler.rs`): The `contains_window_function()` function only recursed into the `on` field, not the `args` field. For `abs(r.price.diff())`, the diff expression was in `args`, not `on`.

### Files Modified
- `src/types.rs`: Added `is_none()` check before casting `on` to `PyDict`
- `src/transpiler.rs`: Added recursion into `args` field in `contains_window_function()`

### Result
- `abs(r.price.diff())` now works correctly
- Test `test_diff_chained_calculations` now passes
- 574 tests passing, 0 skipped

---

## Task 7.4: Distinct with Specific Key Columns ✅ COMPLETE

**Priority**: Medium  
**Effort**: 1-2 days (completed in ~1 hour)

### Implementation Summary
Added support for `distinct()` with specific key columns using SQL ROW_NUMBER() window function.

### Changes Made

**Rust (`src/lib.rs`):**
- Updated `distinct()` method to accept and process `key_exprs` parameter
- If no key columns: uses DataFusion's native `distinct()` (unchanged)
- If key columns provided: uses SQL with `ROW_NUMBER() OVER (PARTITION BY key_cols ORDER BY key_cols)`
- Converts key expressions to column names and validates against schema
- Creates temporary MemTable and executes SQL query

### API Examples
```python
# Current (operates on all columns)
t.distinct()

# New (operates on specific columns)
t.distinct("customer_id")               # By single column
t.distinct(lambda r: r.customer_id)     # By lambda
t.distinct("region", "customer_id")     # By multiple columns
t.distinct("id", lambda r: r.region)    # Mixed string and lambda

# Useful pattern: get max value per group
t.sort("value", desc=True).distinct("id")  # First occurrence = highest value
```

### Tests Added (11 tests)
- `test_distinct_no_args_all_columns` - Verifies default behavior
- `test_distinct_single_column` - Single key column
- `test_distinct_single_column_lambda` - Lambda expression
- `test_distinct_two_columns` - Multiple key columns
- `test_distinct_mixed_string_lambda` - Mixed arguments
- `test_distinct_preserves_all_columns` - Schema preservation
- `test_distinct_chain_with_filter` - Chaining with filter()
- `test_distinct_chain_with_sort` - Chaining with sort()
- `test_distinct_after_sort` - Sort then distinct (first occurrence pattern)
- `test_distinct_invalid_column_error` - Error handling
- `test_distinct_name_column` - Additional coverage

### Result
- 585 tests passing, 0 skipped

---

## Task 7.5: Performance Benchmarking

**Priority**: Medium  
**Effort**: 1 day

### Objective
Create benchmark suite to measure performance and prevent regressions.

---

## Future Phases

### Phase 8: Advanced Features
- search_first() improvements (binary search)
- join_merge() for sorted tables
- Streaming support for large datasets

### Phase 9: Ecosystem & Developer Experience
- Better error messages
- IDE integration hints
- Documentation site

### Phase 10: Expression Enhancements
- Additional string operations
- Additional temporal operations
- Type coercion improvements

---

## Quick Reference

### Test Commands
```bash
# Run all tests
python -m pytest py-ltseq/tests -v

# Run specific test file
python -m pytest py-ltseq/tests/test_relational_ops.py -v

# Run specific test
python -m pytest py-ltseq/tests/test_derive.py::TestNestedTableDeriveIntegration::test_derive_after_filter -v

# Rebuild after Rust changes
cargo build --release && uv pip install -e . --reinstall
```

### Key Files
| File | Purpose |
|------|---------|
| `py-ltseq/ltseq/core.py` | Main Python API |
| `py-ltseq/ltseq/grouping.py` | NestedTable for group_ordered |
| `py-ltseq/ltseq/linking.py` | Foreign key linking |
| `src/lib.rs` | PyO3 bindings |
| `src/ops/basic.rs` | Basic operations |
| `src/ops/advanced.rs` | Advanced operations (agg, pivot) |
| `src/transpiler.rs` | Expression to SQL transpilation |

---

## Notes

- **Pandas dependency**: Only `partition(by=callable)` still uses pandas (intentional - requires Python execution)
- **Skipped tests**: 0 skipped (all tests passing)
- **Python 3.14**: AST deprecation warnings fixed in Phase 6
