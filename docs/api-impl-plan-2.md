# LTSeq API Implementation Plan v2

**Date**: January 2025  
**Last Updated**: January 8, 2026  
**Status**: Phase 5 - Pandas Migration In Progress
**Target Completion**: 2-3 weeks

---

## Executive Summary

This plan tracks API implementation and pandas dependency migration. As of January 8, 2026:

**Current Status (493 tests passing)**:
- ✅ **Phase 1-4**: Expression Extensions (100% complete)
  - if_else, fill_null, is_null, is_not_null, regex_match, string ops, temporal ops, lookup
- ✅ **Phase 8**: Linking/Joins (100% complete)
  - link(), join types, composite keys, transparent materialization
- ✅ **Phase 6**: Window Functions (100% complete)
  - shift, rolling, diff, cum_sum with strict sort requirements
- ✅ **Phase B**: Group-Ordered Operations (100% complete)
  - group_ordered(), filter(), derive() with group context
- **Phase 5**: Pandas Migration (In Progress)
  - pivot() - pending Rust implementation
  - NestedTable.filter() - pending SQL migration
  - NestedTable.derive() - pending SQL execution
  - partition_by() - pending implementation

| Category | Status | Notes |
|----------|--------|-------|
| Basic Relational | ✅ 100% | filter, select, derive, sort, distinct, slice |
| Window Functions | ✅ 100% | shift, rolling, diff, cum_sum |
| Ordered Computing | ✅ 100% | group_ordered, search_first |
| Linking/Joins | ✅ 100% | link, join, join_merge, lookup |
| Expression Extensions | ✅ 100% | if_else, fill_null, string ops, temporal ops |
| Pandas Dependency | 🟡 4/4 | pivot, NestedTable.filter, NestedTable.derive, partition |
| Set Algebra | ❌ 0% | union, intersect, diff, is_subset |

### Success Criteria (Phase 5)
1. ✅ All 493+ existing tests pass
2. ✅ `pivot()` works without pandas
3. ✅ `NestedTable.filter()` works without pandas  
4. ✅ `NestedTable.derive()` works without pandas
5. ✅ New `partition_by()` method available
6. ✅ Backward compatibility maintained

---

## Phase 1: Core Differentiator Features (Week 1)

These are the critical features that make LTSeq unique and are required for the showcase example to work.

### 1.1 agg() - SQL GROUP BY Aggregation

**Priority**: P0 (Critical - reorder from Phase 1)  
**Effort**: Medium (2-3 days)  
**Current State**: Not implemented

#### API Specification

```python
def agg(self, by: Callable = None, **aggregations) -> "LTSeq":
    """
    Aggregate rows into a summary table with one row per group.
    
    This is the traditional SQL GROUP BY operation, returning one row
    per unique grouping key with computed aggregates.
    
    Args:
        by: Optional grouping key lambda (e.g., lambda r: r.region).
            If None, aggregates entire table.
        **aggregations: Named aggregation expressions using group proxies.
                       Example: sum_sales=lambda g: g.sales.sum()
    
    Returns:
        New LTSeq with aggregated results (one row per group)
    
    Examples:
        >>> # Total sales by region
        >>> t.agg(by=lambda r: r.region, total=lambda g: g.sales.sum())
        
        >>> # Multiple aggregations
        >>> t.agg(
        ...     by=lambda r: [r.region, r.year],
        ...     total_sales=lambda g: g.sales.sum(),
        ...     avg_price=lambda g: g.price.avg(),
        ...     max_quantity=lambda g: g.quantity.max()
        ... )
        
        >>> # Full-table aggregation (no grouping)
        >>> t.agg(total=lambda g: g.sales.sum(), count=lambda g: g.id.count())
    """
```

#### Implementation Details

**Python Side** (`py-ltseq/ltseq/core.py`):
1. Parse the `by` lambda to extract grouping columns
2. Parse aggregation lambdas to extract aggregate functions
3. Call Rust `agg_impl()` with group keys and aggregate expressions

```python
def agg(self, by: Callable = None, **aggregations) -> "LTSeq":
    # Extract grouping columns from lambda
    if by is not None:
        group_expr = self._capture_expr(by)
        # Handle both single column and list of columns
    else:
        group_expr = None
    
    # Extract aggregation expressions
    agg_dict = {}
    for agg_name, agg_lambda in aggregations.items():
        agg_dict[agg_name] = self._capture_expr(agg_lambda)
    
    # Call Rust
    result = self.__class__()
    result._schema = {...}  # Updated schema with aggregated columns
    result._inner = self._inner.agg(group_expr, agg_dict)
    return result
```

**Rust Side** (`src/ops/advanced.rs`):
1. Extract group keys from expression
2. Build SQL GROUP BY clause
3. Execute via DataFusion's `.aggregate()` or raw SQL

```rust
pub fn agg_impl(
    table: &RustTable,
    group_by: Option<Vec<String>>,  // Column names to group by
    aggregations: HashMap<String, String>,  // "agg_name" -> "SUM(col)"
) -> PyResult<RustTable> {
    let df = table.dataframe.as_ref()?;
    
    if group_by.is_empty() {
        // Full-table aggregation: SELECT agg1, agg2, ...
        // Use single-row query
    } else {
        // GROUP BY: SELECT group_cols, agg1, agg2, ... GROUP BY group_cols
    }
    
    // Execute via SQL or DataFusion's aggregate API
    // Return new RustTable with aggregated results
}
```

**Supported Aggregation Functions**:
- `g.col.sum()` → `SUM(col)`
- `g.col.count()` → `COUNT(col)` or `COUNT(*)`
- `g.col.min()` → `MIN(col)`
- `g.col.max()` → `MAX(col)`
- `g.col.avg()` → `AVG(col)`
- `g.count()` → `COUNT(*)` (group row count)

#### Files to Modify
- `py-ltseq/ltseq/core.py`: Add `agg()` method (~60 lines)
- `src/lib.rs`: Add `agg()` PyO3 binding (~10 lines)
- `src/ops/advanced.rs`: Add `agg_impl()` (~100 lines)

#### Tests
```python
def test_agg_by_single_column(self, orders_csv):
    """Aggregate by region."""
    result = t.agg(by=lambda r: r.region, total=lambda g: g.amount.sum())
    assert len(result) == 3  # 3 regions
    assert "total" in result._schema

def test_agg_by_multiple_columns(self, orders_csv):
    """Aggregate by region and year."""
    result = t.agg(
        by=lambda r: [r.region, r.year],
        total=lambda g: g.amount.sum()
    )
    # Check results

def test_agg_full_table(self, orders_csv):
    """Full-table aggregation."""
    result = t.agg(total=lambda g: g.amount.sum())
    assert len(result) == 1
```

---

### 1.2 NestedTable.filter() - Actual Group Filtering

**Priority**: P0 (Critical)  
**Effort**: Medium (2-3 days)  
**Current State**: Returns flattened table without filtering

#### API Specification

```python
def filter(self, group_predicate: Callable) -> "LTSeq":
    """
    Filter groups based on group-level predicates.
    
    Only rows from groups that satisfy the predicate are returned.
    
    Args:
        group_predicate: Lambda taking a group proxy (g) and returning bool.
                        Can use: g.count(), g.first(), g.last(), 
                                g.max(col), g.min(col), g.sum(col), g.avg(col)
    
    Returns:
        LTSeq with only rows from matching groups
    
    Examples:
        >>> # Groups with more than 2 rows
        >>> grouped.filter(lambda g: g.count() > 2)
        
        >>> # Groups starting with is_up=True
        >>> grouped.filter(lambda g: g.first().is_up == True)
        
        >>> # Groups with high average price
        >>> grouped.filter(lambda g: g.avg('price') > 100)
        
        >>> # Complex predicates
        >>> grouped.filter(lambda g: (g.count() > 2) & (g.first().is_up == True))
    """
```

#### Current Issues
Currently, `filter()` returns `flatten()` without actually filtering. The method exists but is a placeholder.

#### Implementation Details

**Strategy**: Use SQL WHERE clause on `__group_count__` or computed group properties

**Python Side** (`py-ltseq/ltseq/grouping.py`):

1. Extend `_extract_filter_condition()` AST parser to handle more patterns:

```python
def _extract_filter_condition(self, ast_tree) -> Optional[Dict]:
    """
    Extract filter condition from AST tree.
    
    Returns: {
        'type': 'group_count' | 'group_property',
        'operator': '>' | '<' | '==' | '>=' | '<=',
        'value': numeric_value,
        'group_func': 'count' | 'max' | 'min' | 'sum' | 'avg',
        'column': column_name (for max/min/sum/avg)
    }
    """
    # Pattern 1: g.count() > N
    # Pattern 2: g.max('col') > value
    # Pattern 3: g.first().column == value
    # Pattern 4: g.last().column < value
    # Pattern 5: (g.count() > 2) & (g.first().is_up == True)
```

2. Implement `_filter_by_sql()` to build WHERE clause:

```python
def _filter_by_sql(self, flattened: "LTSeq", condition: Dict) -> "LTSeq":
    """
    Build and execute SQL WHERE clause based on condition dict.
    
    Examples:
    - {'type': 'group_count', 'operator': '>', 'value': 2}
      → WHERE __group_count__ > 2
    
    - {'type': 'group_property', 'group_func': 'max', 'column': 'price', ...}
      → WHERE MAX(price) OVER (PARTITION BY __group_id__) > 100
    """
    # Build SQL WHERE from condition
    # Call flattened._inner.filter_where(sql_condition)
    # Return filtered result
```

**Rust Side** (`src/ops/advanced.rs`):

Add `filter_where_impl()` that takes a SQL condition string:

```rust
pub fn filter_where_impl(table: &RustTable, where_clause: &str) -> PyResult<RustTable> {
    let df = table.dataframe.as_ref()?;
    let schema = table.schema.as_ref()?;
    
    // Execute: SELECT * FROM table WHERE {where_clause}
    let sql = format!("SELECT * FROM t WHERE {}", where_clause);
    let result_df = RUNTIME.block_on(async {
        // Register table as 't', execute SQL, return result
    })?;
    
    // Wrap in RustTable
    Ok(RustTable { dataframe: result_df, ... })
}
```

**Important Notes**:
- Filters are applied AFTER grouping (affects entire groups, not individual rows)
- Multiple filter conditions can be combined with `&` (and) or `|` (or)
- Filters may require materializing group properties first (e.g., max per group)

#### Files to Modify
- `py-ltseq/ltseq/grouping.py`: Expand `_extract_filter_condition()` and implement `_filter_by_sql()` (~150 lines)
- `src/lib.rs`: Add `filter_where()` PyO3 binding (~10 lines)
- `src/ops/advanced.rs`: Add `filter_where_impl()` (~60 lines)

#### Tests
```python
def test_filter_by_count(self):
    """Filter groups with more than N rows."""
    grouped = t.group_ordered(lambda r: r.is_up)
    result = grouped.filter(lambda g: g.count() > 2)
    # Verify only groups with count > 2 remain

def test_filter_by_first_property(self):
    """Filter groups by first row property."""
    result = grouped.filter(lambda g: g.first().is_up == True)

def test_filter_by_group_aggregate(self):
    """Filter groups by max/min/sum/avg."""
    result = grouped.filter(lambda g: g.max('price') > 100)

def test_filter_complex_predicate(self):
    """Filter with combined conditions."""
    result = grouped.filter(lambda g: (g.count() > 2) & (g.first().is_up == True))
```

---

### 1.3 NestedTable.derive() - Group Column Derivation

**Priority**: P0 (Critical)  
**Effort**: Medium-High (3-4 days)  
**Current State**: Returns flattened table without derived columns

#### API Specification

```python
def derive(self, group_mapper: Callable) -> "LTSeq":
    """
    Derive new columns based on group properties.
    
    Each group property (first row, last row, count, etc.) is broadcasted
    to all rows in that group.
    
    Args:
        group_mapper: Lambda taking a group proxy (g) and returning a dict
                     of new columns. Each column value will be broadcasted
                     to all rows in the group.
    
    Returns:
        LTSeq with original rows plus new derived columns
    
    Examples:
        >>> # Add group span (number of rows per group)
        >>> grouped.derive(lambda g: {"span": g.count()})
        
        >>> # Add price change from first to last row
        >>> grouped.derive(lambda g: {
        ...     "start_price": g.first().price,
        ...     "end_price": g.last().price,
        ...     "change": g.last().price - g.first().price
        ... })
        
        >>> # Add group statistics
        >>> grouped.derive(lambda g: {
        ...     "group_max": g.max('price'),
        ...     "group_avg": g.avg('price'),
        ...     "group_span": g.count()
        ... })
    """
```

#### Strategy

Use SQL Window Functions with `PARTITION BY __group_id__` to compute group properties for each row.

**SQL Pattern**:
```sql
SELECT 
    *,  -- All original columns
    FIRST_VALUE(price) OVER (PARTITION BY __group_id__ ORDER BY row_num) AS start_price,
    LAST_VALUE(price) OVER (PARTITION BY __group_id__ ORDER BY row_num ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_price,
    COUNT(*) OVER (PARTITION BY __group_id__) AS span
FROM table
```

**Python Side** (`py-ltseq/ltseq/grouping.py`):

1. Parse derive lambda AST to extract group function calls:

```python
def _extract_derive_expressions(self, group_mapper_lambda) -> Dict[str, Dict]:
    """
    Parse derive lambda to extract column expressions.
    
    Returns:
    {
        "start_price": {
            "type": "group_function",
            "group_func": "first",
            "column": "price"
        },
        "group_max": {
            "type": "group_function",
            "group_func": "max",
            "column": "price"
        },
        "change": {
            "type": "binop",
            "op": "Sub",
            "left": {...last_value...},
            "right": {...first_value...}
        }
    }
    """
```

2. Build SQL expressions using window functions

3. Call Rust `derive_group_impl()` with the expressions

**Rust Side** (`src/ops/advanced.rs`):

Add `derive_group_impl()` that translates group function patterns to window functions:

```rust
pub fn derive_group_impl(
    table: &RustTable,
    group_mappings: HashMap<String, String>,  // "col_name" -> SQL expression
) -> PyResult<RustTable> {
    // group_mappings contains SQL like:
    // "start_price" -> "FIRST_VALUE(price) OVER (PARTITION BY __group_id__ ORDER BY __rn__)"
    
    let mut select_parts = vec!["*".to_string()];
    for (col_name, expr) in group_mappings {
        select_parts.push(format!("{} AS \"{}\"", expr, col_name));
    }
    
    let sql = format!("SELECT {} FROM t", select_parts.join(", "));
    
    // Execute and return
}
```

**Window Function Mappings**:
- `g.first().col` → `FIRST_VALUE(col) OVER (PARTITION BY __group_id__ ORDER BY __rn__)`
- `g.last().col` → `LAST_VALUE(col) OVER (PARTITION BY __group_id__ ORDER BY __rn__ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)`
- `g.count()` → `COUNT(*) OVER (PARTITION BY __group_id__)`
- `g.max('col')` → `MAX(col) OVER (PARTITION BY __group_id__)`
- `g.min('col')` → `MIN(col) OVER (PARTITION BY __group_id__)`
- `g.sum('col')` → `SUM(col) OVER (PARTITION BY __group_id__)`
- `g.avg('col')` → `AVG(col) OVER (PARTITION BY __group_id__)`

#### Files to Modify
- `py-ltseq/ltseq/grouping.py`: Add `_extract_derive_expressions()` and implement derive logic (~180 lines)
- `src/lib.rs`: Add `derive_group()` PyO3 binding (~10 lines)
- `src/ops/advanced.rs`: Add `derive_group_impl()` (~100 lines)

#### Tests
```python
def test_derive_group_span(self):
    """Add group span column."""
    result = grouped.derive(lambda g: {"span": g.count()})
    # All rows in same group should have same span value

def test_derive_first_last(self):
    """Add first and last values."""
    result = grouped.derive(lambda g: {
        "start": g.first().price,
        "end": g.last().price
    })

def test_derive_aggregates(self):
    """Add group aggregates."""
    result = grouped.derive(lambda g: {
        "max_price": g.max('price'),
        "min_price": g.min('price'),
        "avg_price": g.avg('price')
    })

def test_derive_complex_expression(self):
    """Derive with computed expression."""
    result = grouped.derive(lambda g: {
        "gain": g.last().price - g.first().price,
        "gain_pct": (g.last().price - g.first().price) / g.first().price
    })
```

#### Validation: DSL Showcase Example

After implementing these three features, this example from `docs/api.md` should work:

```python
result = (
    t
    .sort(lambda r: r.date)
    .derive(lambda r: {"is_up": r.price > r.price.shift(1)})
    .group_ordered(lambda r: r.is_up)
    .filter(lambda g: g.count() > 3)
    .derive(lambda g: {
        "start": g.first().date,
        "end": g.last().date,
        "gain": (g.last().price - g.first().price) / g.first().price
    })
)
```

All rows should have `start`, `end`, and `gain` columns derived from their group properties.

---

## Phase 2: Set Operations & Expression Enhancements (Week 2)

### 2.1 Set Algebra Operations

**Priority**: P1 (High)  
**Effort**: Medium (2-3 days total)  
**Current State**: Not implemented

#### 2.1.1 union()

**API**: `t.union(other_t)` → `LTSeq`

**Specification**:
```python
def union(self, other: "LTSeq") -> "LTSeq":
    """
    Vertically concatenate two tables.
    
    Appends all rows from other_t to this table. Schemas must be compatible
    (same columns in same order).
    
    Args:
        other: Another LTSeq table
    
    Returns:
        New LTSeq with rows from both tables
    
    Example:
        >>> t1 = LTSeq.read_csv("sales_2023.csv")
        >>> t2 = LTSeq.read_csv("sales_2024.csv")
        >>> all_sales = t1.union(t2)
    """
```

**Implementation**:
- Rust: Use DataFusion's `.union()` method
- Verify schemas are compatible before combining

**Files**: `py-ltseq/ltseq/core.py`, `src/lib.rs`, `src/ops/basic.rs`

---

#### 2.1.2 intersect()

**API**: `t.intersect(other_t, on=["id"])` → `LTSeq`

**Specification**:
```python
def intersect(self, other: "LTSeq", on: List[str]) -> "LTSeq":
    """
    Set intersection: rows present in both tables.
    
    Returns rows that exist in both this table and other_t, based on
    the key columns specified in 'on'.
    
    Args:
        other: Another LTSeq table
        on: Column names to use as join key
    
    Returns:
        New LTSeq with only rows in both tables
    
    Example:
        >>> customers_2023 = t.filter(lambda r: r.year == 2023)
        >>> customers_2024 = t.filter(lambda r: r.year == 2024)
        >>> repeat_customers = customers_2023.intersect(customers_2024, on=["cust_id"])
    """
```

**Implementation**:
- SQL: `SELECT * FROM t WHERE id IN (SELECT id FROM other)`
- Or use semi-join: `t JOIN other ON t.id = other.id`

---

#### 2.1.3 diff()

**API**: `t.diff(other_t, on=["id"])` → `LTSeq`

**Specification**:
```python
def diff(self, other: "LTSeq", on: List[str]) -> "LTSeq":
    """
    Set difference: rows in this table but not in other.
    
    Returns rows from this table that don't appear in other_t, based on
    the key columns specified in 'on'.
    
    Args:
        other: Another LTSeq table
        on: Column names to use as join key
    
    Returns:
        New LTSeq with rows in self but not in other
    
    Example:
        >>> all_customers = t1
        >>> churned = all_customers.diff(t2, on=["cust_id"])  # In t1 but not t2
    """
```

**Implementation**:
- SQL: `SELECT * FROM t WHERE id NOT IN (SELECT id FROM other)`
- Or use anti-join

---

#### 2.1.4 is_subset()

**API**: `t.is_subset(other_t)` → `bool`

**Specification**:
```python
def is_subset(self, other: "LTSeq") -> bool:
    """
    Check if this table is a subset of another.
    
    Returns True if all rows in this table appear in other_t.
    
    Args:
        other: Another LTSeq table
    
    Returns:
        Boolean: True if self is a subset of other
    
    Example:
        >>> subset_rows = t.filter(lambda r: r.region == "West")
        >>> subset_rows.is_subset(t)  # True
    """
```

**Implementation**:
- Execute: `SELECT COUNT(*) FROM t` 
- Then: `SELECT COUNT(*) FROM other WHERE NOT IN (SELECT FROM t)`
- If second count == 0, return True

**Files to Modify**:
- `py-ltseq/ltseq/core.py`: Add 4 methods (~80 lines)
- `src/lib.rs`: Add 4 PyO3 bindings (~15 lines)
- `src/ops/basic.rs`: Add 4 implementations (~120 lines)

**Tests**:
```python
def test_union(self):
    """Vertical concatenation."""
    t3 = t1.union(t2)
    assert len(t3) == len(t1) + len(t2)

def test_intersect(self):
    """Set intersection."""
    result = t1.intersect(t2, on=["id"])
    # All rows in result should have matching ids in both tables

def test_diff(self):
    """Set difference."""
    result = t1.diff(t2, on=["id"])
    # No row in result should have matching id in t2

def test_is_subset(self):
    """Subset check."""
    assert subset.is_subset(full_table) == True
    assert full_table.is_subset(subset) == False
```

---

### 2.2 rolling(w).std() - Rolling Standard Deviation

**Priority**: P2 (Medium)  
**Effort**: Low (0.5 days)  
**Current State**: Partially implemented (other rolling aggregations work)

#### API Specification

```python
# Already works via method chaining:
result = t.derive(lambda r: {
    "volatility": r.close.rolling(10).std()
})
```

#### Implementation

**Rust Side** (`src/ops/window.rs`):
- Add `std` to the rolling aggregation handlers
- SQL: `STDDEV(col) OVER (ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW)`

**Files to Modify**:
- `src/ops/window.rs`: Add `std` case (~15 lines)
- `src/transpiler.rs`: Add `std` to window function detection

**Tests**:
```python
def test_rolling_std(self):
    """Rolling standard deviation."""
    result = t.derive(lambda r: {"volatility": r.price.rolling(10).std()})
    # Verify results match expected standard deviations
```

---

### 2.3 if_else() - Conditional Expression

**Priority**: P2 (Medium)  
**Effort**: Low (1 day)  
**Current State**: Not implemented

#### API Specification

```python
def if_else(condition: Expr, true_value: Any, false_value: Any) -> Expr:
    """
    Conditional expression: returns true_value if condition is True, else false_value.
    
    Similar to SQL CASE WHEN.
    
    Args:
        condition: Boolean expression
        true_value: Value if condition is True
        false_value: Value if condition is False
    
    Returns:
        Expression that evaluates to one of the two values
    
    Example:
        >>> result = t.derive(lambda r: {
        ...     "status": if_else(r.price > 100, "expensive", "cheap")
        ... })
    """
```

**Implementation**:

**Python Side** (`py-ltseq/ltseq/expr/__init__.py`):
```python
def if_else(condition: Expr, true_val, false_val) -> CallExpr:
    """Create conditional expression."""
    return CallExpr("if_else", (condition, true_val, false_val), {}, on=None)
```

**Rust Side** (`src/transpiler.rs`):
```rust
"if_else" => {
    let cond_sql = pyexpr_to_sql(&args[0], schema)?;
    let true_sql = pyexpr_to_sql(&args[1], schema)?;
    let false_sql = pyexpr_to_sql(&args[2], schema)?;
    Ok(format!("CASE WHEN {} THEN {} ELSE {} END", cond_sql, true_sql, false_sql))
}
```

**Files to Modify**:
- `py-ltseq/ltseq/expr/__init__.py`: Export `if_else` function (~5 lines)
- `py-ltseq/ltseq/expr/types.py`: Add helper if needed (~5 lines)
- `src/transpiler.rs`: Handle `if_else` in SQL generation (~10 lines)

**Tests**:
```python
def test_if_else(self):
    """Conditional expressions."""
    result = t.derive(lambda r: {
        "category": if_else(r.price > 100, "expensive", "cheap")
    })

def test_if_else_nested(self):
    """Nested conditionals."""
    result = t.derive(lambda r: {
        "category": if_else(
            r.price > 200,
            "luxury",
            if_else(r.price > 100, "expensive", "cheap")
        )
    })
```

---

### 2.4 Null Handling

**Priority**: P2 (Medium)  
**Effort**: Low (1 day)  
**Current State**: Partially implemented

#### 2.4.1 is_null()

**API**: `r.col.is_null()` → `bool`

**Current State**: Already implemented in Rust transpiler! Just needs documentation.

**Usage**:
```python
result = t.filter(lambda r: r.email.is_null())
```

**Documentation**: Add to `docs/api.md` Expression Capabilities section

---

#### 2.4.2 fill_null()

**API**: `r.col.fill_null(default_value)` → `value`

**Specification**:
```python
# Usage
result = t.derive(lambda r: {
    "email": r.email.fill_null("unknown@example.com")
})
```

**Implementation**:

**Rust Side** (`src/transpiler.rs`):
```rust
"fill_null" => {
    let on_sql = pyexpr_to_sql(on, schema)?;
    let default_sql = pyexpr_to_sql(&args[0], schema)?;
    Ok(format!("COALESCE({}, {})", on_sql, default_sql))
}
```

**Files to Modify**:
- `src/transpiler.rs`: Add `fill_null` handling (~10 lines)

**Tests**:
```python
def test_fill_null(self):
    """Fill null values."""
    result = t.derive(lambda r: {
        "email": r.email.fill_null("unknown@example.com")
    })
    # Verify nulls are replaced

def test_is_null(self):
    """Check for nulls."""
    result = t.filter(lambda r: r.email.is_null())
    # Verify only null emails remain
```

---

## Phase 3: Extended Features (Week 3)

### 3.1 partition() - Table Partitioning

**Priority**: P3 (Medium)  
**Effort**: Medium-High (2-3 days)  
**Current State**: Not implemented

#### API Specification

```python
def partition(self, by: Callable) -> "PartitionedTable":
    """
    Partition table into groups (non-consecutive).
    
    Unlike group_ordered() which groups consecutive identical values,
    partition() groups all rows with the same key value together,
    regardless of their position in the table.
    
    Args:
        by: Grouping key lambda (e.g., lambda r: r.region)
    
    Returns:
        PartitionedTable object with access to individual partitions
    
    Example:
        >>> partitioned = t.partition(by=lambda r: r.region)
        >>> west = partitioned["West"]
        >>> for region, data in partitioned.items():
        ...     print(f"{region}: {len(data)} rows")
    """
```

#### PartitionedTable Class Design (C+D Hybrid)

```python
class PartitionedTable:
    """
    Container for partitioned data with dict-like access and iteration.
    
    Internally uses NestedTable infrastructure where possible for consistency.
    """
    
    def __init__(self, ltseq_instance: "LTSeq", partition_fn: Callable):
        """Initialize from LTSeq and partitioning function."""
        self._ltseq = ltseq_instance
        self._partition_fn = partition_fn
        self._partitions_cache = None  # Lazy evaluation
    
    def __getitem__(self, key) -> "LTSeq":
        """
        Access partition by key.
        
        Example:
            >>> west_sales = partitioned["West"]
        """
        if self._partitions_cache is None:
            self._materialize_partitions()
        return self._partitions_cache.get(key)
    
    def keys(self) -> List[Any]:
        """Return all partition keys."""
        if self._partitions_cache is None:
            self._materialize_partitions()
        return list(self._partitions_cache.keys())
    
    def items(self) -> Iterator[Tuple[Any, "LTSeq"]]:
        """Iterate through (key, table) pairs."""
        if self._partitions_cache is None:
            self._materialize_partitions()
        return iter(self._partitions_cache.items())
    
    def __iter__(self) -> Iterator["LTSeq"]:
        """Iterate through partitions (values only)."""
        return iter(self.values())
    
    def values(self) -> List["LTSeq"]:
        """Return all partition tables."""
        if self._partitions_cache is None:
            self._materialize_partitions()
        return list(self._partitions_cache.values())
    
    def map(self, fn: Callable[["LTSeq"], "LTSeq"]) -> "PartitionedTable":
        """
        Apply function to each partition.
        
        Useful for parallel processing: aggregate each partition independently.
        
        Example:
            >>> # Get per-region totals
            >>> totals = partitioned.map(lambda t: t.agg(total=lambda g: g.sales.sum()))
        """
        # Apply fn to each partition, return new PartitionedTable with results
    
    def to_list(self) -> List["LTSeq"]:
        """Convert to list of LTSeq objects."""
        return self.values()
    
    def _materialize_partitions(self):
        """Actually compute the partitions (triggered on first access)."""
        # 1. Evaluate partition function on each row
        # 2. Group rows by partition key
        # 3. Create LTSeq for each partition
        # 4. Cache results
```

#### Implementation Strategy

**Python Side** (`py-ltseq/ltseq/core.py`):

```python
def partition(self, by: Callable) -> "PartitionedTable":
    """Partition table into groups."""
    from .partitioning import PartitionedTable
    return PartitionedTable(self, by)
```

**Create new file** `py-ltseq/ltseq/partitioning.py`:
- Implement `PartitionedTable` class (~250 lines)
- Lazy materialization on first access
- Support dict-like and iterator protocols

#### Files to Create/Modify
- `py-ltseq/ltseq/partitioning.py`: New file with `PartitionedTable` class (~250 lines)
- `py-ltseq/ltseq/core.py`: Add `partition()` method (~10 lines)
- `py-ltseq/ltseq/__init__.py`: Export `PartitionedTable` if needed

#### Tests
```python
def test_partition_basic(self):
    """Partition table."""
    partitioned = t.partition(by=lambda r: r.region)
    assert "West" in partitioned.keys()

def test_partition_access_by_key(self):
    """Access partition by key."""
    west = partitioned["West"]
    assert all(west._capture_expr(lambda r: r.region) == "West")

def test_partition_iterate(self):
    """Iterate through partitions."""
    for key, data in partitioned.items():
        assert len(data) > 0

def test_partition_map(self):
    """Apply function to each partition."""
    totals = partitioned.map(lambda t: t.agg(total=lambda g: g.sales.sum()))
    # Should return PartitionedTable with aggregated results
```

---

### 3.2 pivot() - Table Pivoting

**Priority**: P3 (Medium)  
**Effort**: Medium-High (2-3 days)  
**Current State**: Not implemented

#### API Specification

```python
def pivot(self, index: str, columns: str, values: str, agg_fn: str = "sum") -> "LTSeq":
    """
    Reshape data from long format to wide format (pivot table).
    
    Args:
        index: Column(s) to keep as rows
        columns: Column whose values become new column headers
        values: Column(s) to aggregate into cells
        agg_fn: Aggregation function ("sum", "mean", "count", "min", "max")
    
    Returns:
        New LTSeq in wide format
    
    Example:
        >>> # Convert from long to wide format
        >>> # Input: date | city | temperature
        >>> # Output: date | Boston | Chicago | Denver
        >>> result = t.pivot(index="date", columns="city", values="temperature")
    """
```

#### Implementation Strategy

**SQL Approach**:
```sql
SELECT 
    date,
    SUM(CASE WHEN city = 'Boston' THEN temperature ELSE 0 END) AS Boston,
    SUM(CASE WHEN city = 'Chicago' THEN temperature ELSE 0 END) AS Chicago,
    SUM(CASE WHEN city = 'Denver' THEN temperature ELSE 0 END) AS Denver
FROM t
GROUP BY date
```

**Complexity**: Need to dynamically extract unique column values

**Files to Modify**:
- `py-ltseq/ltseq/core.py`: Add `pivot()` method (~80 lines)
- `src/ops/advanced.rs`: Add `pivot_impl()` (~150 lines)

---

### 3.3 search_first() - Binary Search

**Priority**: P3 (Medium)  
**Effort**: Medium (2 days)  
**Current State**: Not implemented

#### API Specification

```python
def search_first(self, predicate: Callable) -> Optional["LTSeq"]:
    """
    Binary search for first row matching predicate in a sorted table.
    
    Requires table to be pre-sorted. Uses binary search for O(log N) performance
    instead of O(N) linear scan.
    
    Args:
        predicate: Lambda returning True for the matching condition
    
    Returns:
        LTSeq with first matching row, or empty if not found
    
    Example:
        >>> t_sorted = t.sort(lambda r: r.date)
        >>> first_high_price = t_sorted.search_first(lambda r: r.price > 100)
    """
```

**Implementation Strategy**:
- Check if sort order is tracked
- Implement binary search on Rust side
- Return single-row LTSeq or empty

---

### 3.4 join_merge() - Merge Join

**Priority**: P3 (Medium)  
**Effort**: Medium (2 days)  
**Current State**: Not implemented

#### API Specification

```python
def join_merge(self, other: "LTSeq", on: Callable, how: str = "inner") -> "LTSeq":
    """
    High-speed O(N) merge join for two sorted tables.
    
    Both tables must be pre-sorted by the join key. Performs in-order
    merge instead of hash join for better performance.
    
    Args:
        other: Another LTSeq table (must be sorted by join key)
        on: Join condition lambda
        how: Join type ("inner", "left", "right", "full")
    
    Returns:
        Merged LTSeq
    
    Example:
        >>> t1_sorted = t1.sort(lambda r: r.id)
        >>> t2_sorted = t2.sort(lambda r: r.id)
        >>> result = t1_sorted.join_merge(t2_sorted, on=lambda r, s: r.id == s.id)
    """
```

---

## Phase 4: Expression Extensions (Week 4)

### 4.1 String Operations

**Priority**: P4 (Lower)  
**Effort**: Medium (2-3 days)  
**Current State**: Not implemented

#### API Specification

| Method | SQL Equivalent | Example |
|--------|----------------|---------|
| `r.s.contains(substr)` | `s LIKE '%substr%'` | `r.email.s.contains("@example.com")` |
| `r.s.starts_with(prefix)` | `s LIKE 'prefix%'` | `r.city.s.starts_with("New")` |
| `r.s.slice(start, end)` | `SUBSTRING(s, start, length)` | `r.code.s.slice(0, 3)` |
| `r.s.regex_match(pattern)` | `REGEXP_MATCH(s, pattern)` | `r.email.s.regex_match('^[a-z]+@')` |

#### Implementation

**Python Side** (`py-ltseq/ltseq/expr/types.py`):

Create string accessor class:

```python
class StringAccessor:
    """Accessor for string operations on columns."""
    
    def __init__(self, column_expr: ColumnExpr):
        self._column = column_expr
    
    def contains(self, substr: str) -> CallExpr:
        return CallExpr("contains", (substr,), {}, on=self._column)
    
    def starts_with(self, prefix: str) -> CallExpr:
        return CallExpr("starts_with", (prefix,), {}, on=self._column)
    
    def slice(self, start: int, end: int) -> CallExpr:
        return CallExpr("slice", (start, end), {}, on=self._column)
    
    def regex_match(self, pattern: str) -> CallExpr:
        return CallExpr("regex_match", (pattern,), {}, on=self._column)
```

Then add `.s` accessor to `ColumnExpr`:

```python
class ColumnExpr(Expr):
    @property
    def s(self) -> StringAccessor:
        return StringAccessor(self)
```

**Rust Side** (`src/transpiler.rs`):

```rust
"contains" => {
    let col_sql = pyexpr_to_sql(on, schema)?;
    let pattern = match &args[0] {
        PyExpr::Literal { value, .. } => value,
        _ => return Err("contains() requires string literal".into()),
    };
    Ok(format!("{} LIKE '%{}%'", col_sql, pattern))
}

"starts_with" => {
    let col_sql = pyexpr_to_sql(on, schema)?;
    let prefix = match &args[0] {
        PyExpr::Literal { value, .. } => value,
        _ => return Err("starts_with() requires string literal".into()),
    };
    Ok(format!("{} LIKE '{}%'", col_sql, prefix))
}

// Similar for slice and regex_match
```

**Files to Modify**:
- `py-ltseq/ltseq/expr/types.py`: Add `StringAccessor` class (~60 lines)
- `src/transpiler.rs`: Add string function handlers (~60 lines)

**Tests**:
```python
def test_contains(self):
    """String contains check."""
    result = t.filter(lambda r: r.email.s.contains("@example.com"))

def test_starts_with(self):
    """String prefix check."""
    result = t.filter(lambda r: r.city.s.starts_with("New"))

def test_slice(self):
    """String slicing."""
    result = t.derive(lambda r: {"area_code": r.phone.s.slice(0, 3)})

def test_regex_match(self):
    """Regex matching."""
    result = t.filter(lambda r: r.email.s.regex_match(r"^[a-z]+@"))
```

---

### 4.2 Temporal Operations

**Priority**: P4 (Lower)  
**Effort**: Medium (2-3 days)  
**Current State**: Not implemented

#### API Specification

| Method | SQL Equivalent | Example |
|--------|----------------|---------|
| `r.dt.year()` | `EXTRACT(YEAR FROM dt)` | `r.date.dt.year()` |
| `r.dt.month()` | `EXTRACT(MONTH FROM dt)` | `r.date.dt.month()` |
| `r.dt.add_days(n)` | `dt + INTERVAL 'n' DAY` | `r.date.dt.add_days(30)` |
| `r.dt.diff(other_dt)` | `DATE_DIFF(dt, other_dt)` | `r.date.dt.diff(r.start_date)` |

#### Implementation

**Python Side** (`py-ltseq/ltseq/expr/types.py`):

Create temporal accessor class:

```python
class TemporalAccessor:
    """Accessor for temporal operations on date/datetime columns."""
    
    def __init__(self, column_expr: ColumnExpr):
        self._column = column_expr
    
    def year(self) -> CallExpr:
        return CallExpr("year", (), {}, on=self._column)
    
    def month(self) -> CallExpr:
        return CallExpr("month", (), {}, on=self._column)
    
    def day(self) -> CallExpr:
        return CallExpr("day", (), {}, on=self._column)
    
    def add_days(self, days: int) -> CallExpr:
        return CallExpr("add_days", (days,), {}, on=self._column)
    
    def diff(self, other: Expr) -> CallExpr:
        return CallExpr("date_diff", (other,), {}, on=self._column)
```

Then add `.dt` accessor to `ColumnExpr`:

```python
class ColumnExpr(Expr):
    @property
    def dt(self) -> TemporalAccessor:
        return TemporalAccessor(self)
```

**Rust Side** (`src/transpiler.rs`):

```rust
"year" => {
    let col_sql = pyexpr_to_sql(on, schema)?;
    Ok(format!("EXTRACT(YEAR FROM {})", col_sql))
}

"month" => {
    let col_sql = pyexpr_to_sql(on, schema)?;
    Ok(format!("EXTRACT(MONTH FROM {})", col_sql))
}

"add_days" => {
    let col_sql = pyexpr_to_sql(on, schema)?;
    let days = match &args[0] {
        PyExpr::Literal { value, .. } => value,
        _ => return Err("add_days() requires integer literal".into()),
    };
    Ok(format!("({} + INTERVAL '{}' DAY)", col_sql, days))
}

"date_diff" => {
    let col_sql = pyexpr_to_sql(on, schema)?;
    let other_sql = pyexpr_to_sql(&args[0], schema)?;
    Ok(format!("DATE_DIFF(MONTH, {}, {})", col_sql, other_sql))
}
```

**Files to Modify**:
- `py-ltseq/ltseq/expr/types.py`: Add `TemporalAccessor` class (~70 lines)
- `src/transpiler.rs`: Add temporal function handlers (~70 lines)

**Tests**:
```python
def test_year_extraction(self):
    """Extract year from date."""
    result = t.derive(lambda r: {"year": r.date.dt.year()})

def test_month_extraction(self):
    """Extract month."""
    result = t.derive(lambda r: {"month": r.date.dt.month()})

def test_add_days(self):
    """Add days to date."""
    result = t.derive(lambda r: {"due_date": r.date.dt.add_days(30)})

def test_date_diff(self):
    """Date difference."""
    result = t.derive(lambda r: {"days_until": r.due_date.dt.diff(r.date)})
```

---

## Implementation Schedule

### Sprint 1 (Week 1): Core Differentiators
| Task | Days | Owner | Status |
|------|------|-------|--------|
| 1.1 agg() | 2-3 | Backend | Pending |
| 1.2 NestedTable.filter() | 2-3 | Backend | Pending |
| 1.3 NestedTable.derive() | 3-4 | Backend | Pending |
| **Validation**: DSL showcase | 0.5 | QA | Pending |
| **Total** | **7-11 days** | | |

### Sprint 2 (Week 2): Sets & Expressions
| Task | Days | Owner | Status |
|------|------|-------|--------|
| 2.1 Set operations (4 methods) | 2-3 | Backend | Pending |
| 2.2 rolling().std() | 0.5 | Backend | Pending |
| 2.3 if_else() | 1 | Backend | Pending |
| 2.4 Null handling | 1 | Backend | Pending |
| **Total** | **4.5-6 days** | | |

### Sprint 3 (Week 3): Extended Features
| Task | Days | Owner | Status |
|------|------|-------|--------|
| 3.1 partition() | 2-3 | Backend | Pending |
| 3.2 pivot() | 2-3 | Backend | Pending |
| 3.3 search_first() | 2 | Backend | Pending |
| 3.4 join_merge() | 2 | Backend | Pending |
| **Total** | **8-10 days** | | |

### Sprint 4 (Week 4): Expression Extensions
| Task | Days | Owner | Status |
|------|------|-------|--------|
| 4.1 String operations | 2-3 | Backend | Pending |
| 4.2 Temporal operations | 2-3 | Backend | Pending |
| **Total** | **4-6 days** | | |

**Total Project Duration**: ~27-35 days of backend work

---

## Risks & Mitigation

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|-----------|
| Window function SQL generation complexity | Medium | High | Start with simple patterns, expand gradually |
| DataFusion compatibility issues | Low | High | Check DataFusion docs early, spike on challenging features |
| Performance regression | Medium | High | Add benchmarks to test suite, profile early |
| API usability feedback | Medium | Medium | Gather feedback after Phase 1, adjust as needed |
| Scope creep (extensions) | High | Medium | Keep P3/P4 as separate phase, revisit after Phase 2 |

---

## Success Metrics

### Phase 1 (Must Have)
- ✅ DSL showcase example executes end-to-end
- ✅ All 3 core features fully tested (>90% coverage)
- ✅ Existing 336 tests still passing

### Phase 2 (Should Have)
- ✅ Set operations fully working
- ✅ if_else() and null handling in expressions
- ✅ rolling().std() implemented

### Phase 3 (Nice to Have)
- ✅ partition() works with both access patterns
- ✅ pivot() handles common use cases
- ✅ search_first() uses binary search

### Phase 4 (Future)
- ✅ String operations cover common patterns
- ✅ Temporal operations for date handling

---

## Code Quality Standards

All implementations must meet:

1. **Test Coverage**: >90% line coverage
2. **Documentation**: 
   - Docstrings for all public methods
   - Usage examples in docstrings
   - Comments for complex logic
3. **Error Handling**:
   - Clear error messages
   - Type checking where possible
   - Graceful degradation
4. **Performance**:
   - No O(n²) or worse algorithms
   - Lazy evaluation where applicable
   - Proper indexing in SQL queries
5. **Code Style**:
   - Follow existing patterns in codebase
   - Keep functions under 100 lines (Rust) or 50 lines (Python)
   - Consistent naming conventions

---

## Dependencies & Compatibility

### No New External Dependencies Required
All features can be implemented with:
- **Rust**: DataFusion, Arrow, PyO3 (already used)
- **Python**: stdlib only (already minimal)

### Backward Compatibility
- All new methods are additive (no breaking changes)
- Existing APIs remain unchanged
- 336 existing tests should continue passing

---

## Next Steps

1. **Review & Approval**: Stakeholder review of this plan
2. **Setup**: Create feature branches, setup CI/CD for each phase
3. **Sprint 1 Kickoff**: Start with agg(), NestedTable.filter(), NestedTable.derive()
4. **Weekly Sync**: Review progress, adjust if needed
5. **Phase Gates**: Validation at end of each phase before proceeding

---

## Appendix: File Change Summary

### Files to Create
- `py-ltseq/ltseq/partitioning.py` (250 lines)

### Files to Modify (Python)
| File | Purpose | Estimated Lines |
|------|---------|-----------------|
| `py-ltseq/ltseq/core.py` | Add agg(), partition(), pivot(), search_first(), join_merge(), union(), intersect(), diff(), is_subset() | 300 |
| `py-ltseq/ltseq/grouping.py` | Implement filter() and derive() | 300 |
| `py-ltseq/ltseq/expr/__init__.py` | Export if_else, StringAccessor, TemporalAccessor | 20 |
| `py-ltseq/ltseq/expr/types.py` | Add StringAccessor, TemporalAccessor classes | 130 |
| `py-ltseq/tests/test_*.py` | New tests for all features | 800+ |

### Files to Modify (Rust)
| File | Purpose | Estimated Lines |
|------|---------|-----------------|
| `src/lib.rs` | Add PyO3 bindings for new methods | 100 |
| `src/ops/advanced.rs` | Implement agg, filter_where, derive_group, pivot, join_merge | 400 |
| `src/ops/basic.rs` | Implement union, intersect, diff, is_subset | 120 |
| `src/ops/window.rs` | Add rolling().std() | 15 |
| `src/transpiler.rs` | Add string, temporal, if_else, fill_null handling | 200 |

**Total New Code**: ~2,500 lines (Python + Rust) + 800+ lines of tests

---

## References

- Original API Spec: `docs/api.md`
- Current Implementation Status: `docs/PHASE8_ANALYSIS.txt`
- Phase B Documentation: `docs/phase_b_group_ordered.md`
- Test Suite: `py-ltseq/tests/`

