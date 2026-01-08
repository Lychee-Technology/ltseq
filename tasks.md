# LTSeq Pandas Migration Tasks (Phase 5)

**Created**: January 8, 2026  
**Goal**: Migrate pandas-dependent operations to Rust/SQL  
**Target**: Reduce pandas usage from 4 locations to 1 (expression-based only)

---

## Overview

| Task | Description | Effort | Status |
|------|-------------|--------|--------|
| 0 | Documentation cleanup & update api-impl-plan-2.md | 1 hour | Pending |
| 1 | Migrate `pivot()` to Rust/SQL | 2-3 days | Pending |
| 2 | Migrate `NestedTable.filter()` to SQL window functions | 2 days | Pending |
| 3 | Migrate `NestedTable.derive()` to pure SQL (audit + fix) | 1-2 days | Pending |
| 4 | Add expression-based `partition_by()` + keep callable fallback | 1-2 days | Pending |

**Total Estimated Effort**: 8-10 days

---

## Task 0: Documentation Cleanup (1 hour)

### Objective
Clean up obsolete documentation and update implementation plan status.

### Actions

1. **Delete obsolete files:**
   - `docs/README_PHASE8.md` - References deleted phase8 docs
   - `docs/phase8_quick_reference.txt` - Phase 8 fully complete

2. **Update `docs/api-impl-plan-2.md`:**
   - Update status table header: "54%" → actual current coverage
   - Update test count: 336 → 493
   - Mark Phase 8 linking as complete
   - Mark expression extensions (Phase 1-4) as complete
   - Add Phase 5 (pandas migration) section

3. **Update `docs/PANDAS_MIGRATION_ROADMAP.md`:**
   - Update test count to 493
   - Mark Phase 1-4 as complete
   - Add current task progress

### Acceptance Criteria
- [ ] No obsolete phase8 docs remain
- [ ] api-impl-plan-2.md reflects current status
- [ ] All tests still pass (493+)

---

## Task 1: Migrate `pivot()` to Rust/SQL (2-3 days)

### Objective
Replace pandas `pivot_table()` with pure SQL CASE WHEN aggregation.

### Current Implementation
**Location**: `py-ltseq/ltseq/core.py:1132-1277`

```python
# Current: Uses pandas
df = self.to_pandas()
pivoted_df = df.pivot_table(index=..., columns=..., values=..., aggfunc=...)
# Or optimized path:
grouped = df_subset.groupby(...)[values].agg(agg_fn)
pivoted_df = grouped.unstack(columns, fill_value=0)
```

### Target Implementation

**SQL Pattern:**
```sql
-- For: pivot(index="year", columns="region", values="amount", agg_fn="sum")
SELECT 
    year,
    SUM(CASE WHEN region = 'West' THEN amount ELSE NULL END) as "West",
    SUM(CASE WHEN region = 'East' THEN amount ELSE NULL END) as "East"
FROM table
GROUP BY year
```

### Implementation Steps

**Step 1: Add Rust `pivot_impl()` function**
**File**: `src/ops/advanced.rs`

```rust
pub fn pivot_impl(
    table: &RustTable,
    index_cols: Vec<String>,    // GROUP BY columns
    pivot_col: String,          // Column whose values become new columns
    value_col: String,          // Column to aggregate
    agg_fn: String,             // "sum", "mean", "count", "min", "max"
) -> PyResult<RustTable> {
    // 1. Register source table as temp table
    // 2. Query DISTINCT values from pivot_col
    // 3. Build CASE WHEN expressions for each distinct value
    // 4. Build GROUP BY query with dynamic columns
    // 5. Execute and return RustTable
}
```

**Step 2: Add PyO3 binding**
**File**: `src/lib.rs`

```rust
#[pyo3(name = "pivot")]
fn py_pivot(
    &self,
    index_cols: Vec<String>,
    pivot_col: String,
    value_col: String,
    agg_fn: String,
) -> PyResult<RustTable>
```

**Step 3: Update Python wrapper**
**File**: `py-ltseq/ltseq/core.py`

```python
def pivot(self, index, columns, values, agg_fn="sum") -> "LTSeq":
    # Keep existing validation
    # Replace pandas call with:
    result._inner = self._inner.pivot(index_cols, columns, values, agg_fn)
    # Infer schema from result
```

### Edge Cases to Handle
- [ ] Column names with special characters (spaces, quotes)
- [ ] NULL values in pivot column
- [ ] Empty result sets
- [ ] Very large number of distinct pivot values
- [ ] Numeric vs string pivot column values

### Tests
**File**: `py-ltseq/tests/test_phase3_pivoting.py`
- All 25 existing tests must pass
- Add: `test_pivot_special_char_columns()`
- Add: `test_pivot_null_in_pivot_column()`
- Add: `test_pivot_performance_large_dataset()`

### Acceptance Criteria
- [ ] All 25 existing pivot tests pass
- [ ] No pandas import in pivot code path
- [ ] Performance: ≥2x faster on 10K+ rows
- [ ] Documentation updated

---

## Task 2: Migrate `NestedTable.filter()` to SQL Window Functions (2 days)

### Objective
Replace pandas `groupby().apply()` iteration with SQL window functions.

### Current Implementation
**Location**: `py-ltseq/ltseq/grouping.py:430-543`

```python
# Current: Uses pandas iteration
df = self._ltseq.to_pandas()
for idx, row in df.iterrows():
    # Compute group_id via Python callable
grouped = df.groupby("__group_id__")
for group_id, group_df in grouped:
    if group_predicate(GroupProxy(group_df)):
        passing_group_ids.add(group_id)
filtered_df = df[df["__group_id__"].isin(passing_group_ids)]
```

### Target Implementation

**SQL Pattern:**
```sql
-- For: grouped.filter(lambda g: g.count() > 3)
SELECT * FROM (
    SELECT *,
        COUNT(*) OVER (PARTITION BY __group_id__) as __group_count__
    FROM table_with_groups
) subq
WHERE __group_count__ > 3

-- For: grouped.filter(lambda g: g.first().is_up == True)
SELECT * FROM (
    SELECT *,
        FIRST_VALUE(is_up) OVER (PARTITION BY __group_id__ ORDER BY __rn__) as __first_is_up__
    FROM table_with_groups
) subq
WHERE __first_is_up__ = TRUE
```

### Implementation Steps

**Step 1: Create `_filter_via_sql()` method**
**File**: `py-ltseq/ltseq/grouping.py`

```python
def _filter_via_sql(self, flattened: "LTSeq", filter_sql: str) -> "LTSeq":
    """
    Execute group filter via SQL window functions.
    
    1. Build SELECT with window function columns
    2. Execute via Rust
    3. Return filtered result
    """
```

**Step 2: Create `_parse_group_filter_to_sql()` method**
```python
def _parse_group_filter_to_sql(self, predicate_fn) -> Tuple[List[str], str]:
    """
    Parse group predicate lambda to SQL components.
    
    Returns:
        (window_exprs, where_clause)
        
    Example:
        lambda g: g.count() > 3
        → (["COUNT(*) OVER (PARTITION BY __group_id__) as __count__"], 
           "__count__ > 3")
    """
```

**Step 3: Extend AST parsing for filter predicates**
Similar to existing `_extract_derive_expressions()` but for boolean predicates.

### Pattern Support Matrix

| Pattern | SQL Translation |
|---------|----------------|
| `g.count() > N` | `COUNT(*) OVER (PARTITION BY __group_id__) > N` |
| `g.count() == N` | `COUNT(*) OVER (...) = N` |
| `g.first().col == val` | `FIRST_VALUE(col) OVER (...) = val` |
| `g.last().col == val` | `LAST_VALUE(col) OVER (...) = val` |
| `g.sum('col') > N` | `SUM(col) OVER (...) > N` |
| `g.avg('col') > N` | `AVG(col) OVER (...) > N` |
| `g.min('col') < N` | `MIN(col) OVER (...) < N` |
| `g.max('col') > N` | `MAX(col) OVER (...) > N` |
| `(pred1) & (pred2)` | `cond1 AND cond2` |
| `(pred1) \| (pred2)` | `cond1 OR cond2` |

### Tests
**File**: `py-ltseq/tests/test_phase7_group_ordered.py`
- Existing: `test_filter_groups_by_count()` must pass
- Existing: `test_filter_groups_by_property()` must pass
- Add: `test_filter_compound_predicate()`
- Add: `test_filter_with_aggregates()`

### Acceptance Criteria
- [ ] All existing filter tests pass
- [ ] No pandas iteration in filter code path
- [ ] All pattern support matrix items work
- [ ] Performance: ≥3x faster on datasets with 100+ groups

---

## Task 3: Migrate `NestedTable.derive()` to Pure SQL (1-2 days)

### Objective
Complete the migration of derive() - currently hybrid (parses to SQL but executes via pandas).

### Deep Audit Results

**Location**: `py-ltseq/ltseq/grouping.py:828-1165`

**Current State**: HYBRID implementation
- ✅ AST parsing to SQL expressions works (`_extract_derive_expressions()`)
- ✅ SQL generation for window functions works (`_process_derive_call()`)
- ❌ Execution still uses pandas (`_derive_via_sql()` calls `_evaluate_sql_expr_in_pandas()`)

**Key Finding** (lines 1107-1165):
```python
def _derive_via_sql(self, flattened, derive_exprs):
    # PROBLEM: Still imports pandas
    import pandas as pd
    
    # PROBLEM: Still converts to pandas
    df = self._ltseq.to_pandas()
    
    # PROBLEM: Evaluates SQL in pandas, not DataFusion
    for col_name, sql_expr in derive_exprs.items():
        df[col_name] = self._evaluate_sql_expr_in_pandas(df, sql_expr)
```

### Target Implementation

Replace pandas execution with Rust SQL execution:

```python
def _derive_via_sql(self, flattened: "LTSeq", derive_exprs: Dict[str, str]) -> "LTSeq":
    """
    Execute derive via Rust SQL with window functions.
    """
    # 1. Ensure flattened has __group_id__ and __rn__ columns
    # 2. Build SQL: SELECT *, expr1 AS col1, expr2 AS col2 FROM t
    # 3. Execute via Rust (use existing filter_where or new method)
    # 4. Remove internal columns
    # 5. Return result
```

### Implementation Steps

**Step 1: Add Rust `derive_sql()` method**
**File**: `src/lib.rs` or `src/ops/derive.rs`

```rust
fn derive_sql(&self, sql_exprs: HashMap<String, String>) -> PyResult<RustTable> {
    // Build SELECT * , expr1 AS col1, expr2 AS col2 FROM t
    // Execute via DataFusion
}
```

**Step 2: Update `_derive_via_sql()` in Python**
Remove pandas, call Rust instead.

**Step 3: Handle `__group_id__` and `__rn__` columns**
- These must exist before window functions can work
- May need to add them via Rust before derive
- Remove them after derive completes

### Patterns Already Working (via pandas emulation)
- `g.count()` → `COUNT(*) OVER (PARTITION BY __group_id__)`
- `g.first().col` → `FIRST_VALUE(col) OVER (...)`
- `g.last().col` → `LAST_VALUE(col) OVER (...)`
- `g.sum('col')` → `SUM(col) OVER (...)`
- `g.avg('col')` → `AVG(col) OVER (...)`
- `g.min('col')` → `MIN(col) OVER (...)`
- `g.max('col')` → `MAX(col) OVER (...)`
- Arithmetic: `(expr1 - expr2) / expr3`

### Tests
**File**: `py-ltseq/tests/test_phase7_group_ordered.py`
- Existing: `test_derive_group_span()` must pass
- Existing: `test_chain_filter_derive()` must pass
- Add: `test_derive_complex_arithmetic()`
- Add: `test_derive_all_aggregates()`

### Acceptance Criteria
- [ ] All existing derive tests pass
- [ ] No pandas in derive code path
- [ ] All supported patterns work via pure SQL
- [ ] Performance: ≥5x faster

---

## Task 4: Add Expression-Based `partition_by()` (1-2 days)

### Objective
Add fast SQL-based partitioning for simple column-based cases, keep callable fallback.

### Current Implementation
**Location**: `py-ltseq/ltseq/partitioning.py:172-222`

```python
# Current: Executes Python callable on each row
for row_data in rows:
    partition_key = self._partition_fn(row_proxy)  # Python callable
    partitions_dict[partition_key].append(row_data)
```

This is inherently Python-dependent for arbitrary callables. We'll add a fast path for simple cases.

### Target Implementation

**New API:**
```python
# Fast path (SQL-based) - NEW
partitions = t.partition_by("region")
partitions = t.partition_by("year", "region")

# Flexible path (Python callable) - EXISTING, unchanged
partitions = t.partition(by=lambda r: r.region)
partitions = t.partition(by=lambda r: f"{r.year}-Q{r.quarter}")
```

### Implementation Steps

**Step 1: Add `partition_by()` method to LTSeq**
**File**: `py-ltseq/ltseq/core.py`

```python
def partition_by(self, *columns: str) -> "PartitionedTable":
    """
    Partition table by column values using SQL.
    
    This is faster than partition(by=lambda r: r.col) for simple cases.
    
    Args:
        *columns: Column names to partition by
        
    Returns:
        PartitionedTable with one partition per unique key combination
        
    Example:
        >>> partitions = t.partition_by("region")
        >>> partitions = t.partition_by("year", "region")
    """
    # 1. Validate columns exist
    for col in columns:
        if col not in self._schema:
            raise AttributeError(f"Column '{col}' not found")
    
    # 2. Get distinct keys via SQL
    # 3. For each key, create filtered partition
    # 4. Return PartitionedTable
```

**Step 2: Implement SQL-based key extraction**
```python
def _get_distinct_keys(self, columns: List[str]) -> List[Tuple]:
    """Get distinct key combinations via SQL."""
    # SELECT DISTINCT col1, col2 FROM table ORDER BY col1, col2
```

**Step 3: Implement SQL-based partition creation**
```python
def _create_partition(self, columns: List[str], key_values: Tuple) -> "LTSeq":
    """Create partition by filtering to specific key values."""
    # WHERE col1 = val1 AND col2 = val2
```

### Tests
**File**: `py-ltseq/tests/test_partition_by.py` (NEW)

```python
def test_partition_by_single_column():
    """Partition by one column."""
    t = LTSeq.read_csv("sales.csv")
    partitions = t.partition_by("region")
    assert len(partitions) == 3  # West, East, Central
    
def test_partition_by_multiple_columns():
    """Partition by multiple columns."""
    partitions = t.partition_by("year", "region")
    
def test_partition_by_invalid_column():
    """Error on non-existent column."""
    with pytest.raises(AttributeError):
        t.partition_by("nonexistent")
        
def test_partition_by_vs_partition_equivalence():
    """partition_by and partition produce same results."""
    p1 = t.partition_by("region")
    p2 = t.partition(by=lambda r: r.region)
    assert p1.keys() == p2.keys()
    
def test_partition_by_performance():
    """partition_by is faster than partition for simple cases."""
    # Benchmark comparison
```

### Acceptance Criteria
- [ ] `partition_by()` method available on LTSeq
- [ ] Produces same results as `partition(by=lambda r: r.col)`
- [ ] No pandas in `partition_by()` code path
- [ ] Existing `partition()` unchanged (backward compatible)
- [ ] Performance: ≥5x faster than callable partition

---

## Implementation Order

```
Task 0: Documentation (1 hour)
    ↓
Task 1: pivot() migration (2-3 days)
    ↓  
Task 2: NestedTable.filter() migration (2 days)
    ↓
Task 3: NestedTable.derive() migration (1-2 days)
    ↓
Task 4: partition_by() addition (1-2 days)
```

Tasks 2 and 3 share infrastructure (SQL window functions) so should be done together.

---

## Files Summary

### Rust Files to Modify
| File | Changes |
|------|---------|
| `src/ops/advanced.rs` | Add `pivot_impl()` (~100 LOC) |
| `src/lib.rs` | Add `pivot()`, `derive_sql()` bindings (~30 LOC) |

### Python Files to Modify
| File | Changes |
|------|---------|
| `py-ltseq/ltseq/core.py` | Replace pandas pivot, add `partition_by()` (~80 LOC delta) |
| `py-ltseq/ltseq/grouping.py` | Replace pandas filter/derive with SQL (~150 LOC delta) |

### Documentation to Update
| File | Changes |
|------|---------|
| `docs/api-impl-plan-2.md` | Update status, add Phase 5 |
| `docs/PANDAS_MIGRATION_ROADMAP.md` | Update progress |

### Files to Delete
| File | Reason |
|------|--------|
| `docs/README_PHASE8.md` | Obsolete |
| `docs/phase8_quick_reference.txt` | Obsolete |

### New Test Files
| File | Tests |
|------|-------|
| `py-ltseq/tests/test_partition_by.py` | ~10 tests for new partition_by() |

---

## Success Criteria (Overall)

1. ✅ All 493+ existing tests pass
2. ✅ `pivot()` works without pandas
3. ✅ `NestedTable.filter()` works without pandas  
4. ✅ `NestedTable.derive()` works without pandas
5. ✅ New `partition_by()` method available
6. ✅ `partition()` with callable still works (backward compatible)
7. ✅ Performance improvements documented
8. ✅ Documentation updated

---

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| DataFusion SQL edge cases | Medium | Medium | Comprehensive test coverage |
| Window function ordering | Low | High | Verify __rn__ column handling |
| Performance regression | Low | Medium | Benchmark before/after |
| Breaking existing tests | Low | High | Run full test suite after each change |

---

## Notes

- **Pandas dependency after migration**: Only `partitioning.py` `_materialize_partitions()` will use pandas (for arbitrary Python callables). This is acceptable as it's inherently Python-dependent.
- **Expression-based partition_by()**: Provides SQL fast path for common cases.
- **Backward compatibility**: All existing APIs unchanged, new functionality is additive.
