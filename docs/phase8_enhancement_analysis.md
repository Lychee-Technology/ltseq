# Phase 8 Enhancement: Full Data Joining Analysis

## Executive Summary

Phase 8 MVP currently implements **pointer-based linking infrastructure** (LinkedTable wrapper, schema management, operation delegation) but **does not implement actual data joining**. The full enhancement requires:

1. **Join operation in Rust** using DataFusion's join capabilities
2. **Lambda expression parsing** to extract join keys from user-provided conditions
3. **Data materialization** and caching for joined results
4. **Pointer column access** translation (accessing `r.prod.name` should reference joined data)
5. **Schema merging** for linked table columns

Estimated complexity: **1,500-2,000 lines of Rust code** + **300-400 lines of Python code**

---

## Current Implementation Analysis

### 1. LinkedTable Python Class (Lines 993-1079 in __init__.py)

**What exists:**
```python
class LinkedTable:
    def __init__(self, source_table, target_table, join_fn, alias):
        self._source = source_table
        self._target = target_table
        self._join_fn = join_fn  # Lambda: lambda o, p: o.product_id == p.id
        self._alias = alias      # "prod"
        self._schema = source_table._schema.copy()
        # Add prefixed target columns to schema
        for col_name, col_type in target_table._schema.items():
            self._schema[f"{alias}_{col_name}"] = col_type
```

**Status:** Schema management is complete but no data operations.

**Methods in MVP:**
- `show()` - delegates to source (MVP) 
- `filter()` - applies to source, returns new LinkedTable
- `select()` - returns source columns only (MVP limitation)
- `derive()` - delegates to source
- `slice()` - delegates to source
- `distinct()` - delegates to source
- `link()` - chains to another table

**Critical Gap:** No actual joining happens. `_join_fn` is stored but never used.

### 2. RustTable Methods (src/lib.rs)

**Methods currently implemented (lines 694-1681):**
- `read_csv()` - loads CSV into DataFusion DataFrame
- `show()` - formats output as ASCII table
- `filter()` - transpiles PyExpr to DataFusion Expr
- `select()` - selects columns
- `derive()` - adds derived columns (with window function support)
- `sort()` - sorts rows
- `distinct()` - removes duplicates
- `slice()` - row range selection
- `cum_sum()` - cumulative sum with window functions

**Critical missing method:**
- `join()` - **NOT IMPLEMENTED** - This is the key feature needed

### 3. Expression System (expr.py)

**Current capabilities:**
- `SchemaProxy` - captures lambda parameters as named column references
- `BinOpExpr` - captures binary operations (==, <, >, etc.)
- `ColumnExpr` - column references
- `LiteralExpr` - constant values
- `CallExpr` - function calls

**For joining, we need:**
- Ability to parse lambda with TWO parameters: `lambda o, p: o.product_id == p.id`
- Extract left side key: `o.product_id` (from source table)
- Extract right side key: `p.id` (from target table)
- Build a condition expression usable in JOIN ... ON clause

**Status:** Basic capability exists in `_lambda_to_expr()` but needs refinement for two-parameter lambdas.

---

## What Needs to Be Implemented

### Phase 8A: Lambda Expression Parsing for Joins

**Goal:** Extract join keys from `lambda o, p: o.product_id == p.id`

**Implementation location:** `py-ltseq/ltseq/__init__.py` - new function

```python
def _extract_join_keys(join_fn: Callable, source_schema: Dict, target_schema: Dict):
    """
    Parse join lambda to extract left and right key expressions.
    
    Args:
        join_fn: Lambda with two parameters, e.g., lambda o, p: o.product_id == p.id
        source_schema: Source table schema dict
        target_schema: Target table schema dict
    
    Returns:
        Tuple of (left_expr_dict, right_expr_dict, join_type)
        where join_type defaults to 'inner'
    
    Example:
        join_fn = lambda o, p: o.product_id == p.id
        left_key, right_key, jtype = _extract_join_keys(join_fn, orders_schema, products_schema)
        # left_key: {"type": "Column", "name": "product_id"}
        # right_key: {"type": "Column", "name": "id"}
        # jtype: "inner"
    """
    source_proxy = SchemaProxy(source_schema)
    target_proxy = SchemaProxy(target_schema)
    
    # Call the lambda to get the expression
    expr = join_fn(source_proxy, target_proxy)
    
    # expr should be a BinOpExpr with op="Eq"
    if not isinstance(expr, BinOpExpr) or expr.op != "Eq":
        raise ValueError("Join condition must be a single equality: left == right")
    
    # Extract left and right sides
    left_expr_dict = expr.left.serialize()
    right_expr_dict = expr.right.serialize()
    
    return left_expr_dict, right_expr_dict, "inner"
```

**Estimated complexity:** 50 lines of Python

**Key challenges:**
- Handle simple equality only in MVP (extend to compound conditions later)
- Validate that left side references source columns
- Validate that right side references target columns
- May need to handle OR conditions for composite keys

---

### Phase 8B: Rust Join Implementation

**Goal:** Implement `RustTable.join()` method that performs actual data joining

**Implementation location:** `src/lib.rs` in `impl RustTable` block

#### 8B.1: Join Method Signature

```rust
#[pymethods]
impl RustTable {
    /// Join two tables based on join condition
    ///
    /// Args:
    ///     other: Another RustTable to join with
    ///     left_key_expr_dict: Serialized expression dict for left join key
    ///     right_key_expr_dict: Serialized expression dict for right join key
    ///     join_type: "inner", "left", "right", "full"
    ///     how: "left_key" or "right_key" or "both" (for column naming)
    ///
    /// Returns:
    ///     New RustTable with joined data
    fn join(
        &self,
        other: &RustTable,
        left_key_expr_dict: &Bound<'_, PyDict>,
        right_key_expr_dict: &Bound<'_, PyDict>,
        join_type: String,
    ) -> PyResult<RustTable> {
        // Implementation details below
    }
}
```

#### 8B.2: Core Join Logic

DataFusion supports joins via the DataFrame API. The implementation pattern:

```rust
fn join(&self, other: &RustTable, ...) -> PyResult<RustTable> {
    // 1. Validate inputs
    if self.dataframe.is_none() || other.dataframe.is_none() {
        return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Both tables must have data loaded"
        ));
    }
    
    // 2. Deserialize join key expressions
    let left_py_expr = dict_to_py_expr(left_key_expr_dict)?;
    let right_py_expr = dict_to_py_expr(right_key_expr_dict)?;
    
    // 3. Transpile to DataFusion expressions
    let self_schema = self.schema.as_ref().ok_or(...)?;
    let other_schema = other.schema.as_ref().ok_or(...)?;
    
    let left_expr = pyexpr_to_datafusion(left_py_expr, self_schema)?;
    let right_expr = pyexpr_to_datafusion(right_py_expr, other_schema)?;
    
    // 4. Execute join
    RUNTIME.block_on(async {
        let self_df = (**self.dataframe.as_ref().unwrap()).clone();
        let other_df = (**other.dataframe.as_ref().unwrap()).clone();
        
        // DataFusion join syntax:
        let joined_df = self_df.join(
            other_df,
            JoinType::Inner,  // or other types
            Some((vec![left_expr], vec![right_expr])),
            None,  // filter
        ).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            format!("Join failed: {}", e)
        ))?;
        
        // 5. Merge schemas and return new RustTable
        let joined_schema = joined_df.schema();
        let arrow_fields: Vec<Field> = 
            joined_schema.fields().iter().map(|f| (**f).clone()).collect();
        let new_arrow_schema = ArrowSchema::new(arrow_fields);
        
        Ok(RustTable {
            session: Arc::clone(&self.session),
            dataframe: Some(Arc::new(joined_df)),
            schema: Some(Arc::new(new_arrow_schema)),
            sort_exprs: self.sort_exprs.clone(),
        })
    })
}
```

**Estimated complexity:** 100-150 lines of Rust (boilerplate included)

**Key implementation details:**

1. **JoinType enum** - DataFusion supports:
   - `JoinType::Inner`
   - `JoinType::Left`
   - `JoinType::Right`
   - `JoinType::Full`

2. **Join signature in DataFusion:**
   ```rust
   pub fn join(
       self,
       right: DataFrame,
       join_type: JoinType,
       keys: Option<(Vec<Expr>, Vec<Expr>)>,
       filter: Option<Expr>,
   ) -> Result<DataFrame>
   ```

3. **Schema handling:**
   - Left table columns remain as-is
   - Right table columns may be renamed (with alias prefix)
   - Need to update Python side to apply the alias

---

### Phase 8C: LinkedTable.join() Method Implementation

**Goal:** Connect Python LinkedTable to Rust join operation

**Implementation location:** `py-ltseq/ltseq/__init__.py`

#### Current Gap:
LinkedTable stores `_join_fn` but never calls it. Methods like `select()` and `derive()` need to **materialize** the joined data.

#### Solution Approach - Lazy Materialization:

```python
class LinkedTable:
    def __init__(self, source_table, target_table, join_fn, alias):
        self._source = source_table
        self._target = target_table
        self._join_fn = join_fn
        self._alias = alias
        self._schema = source_table._schema.copy()
        for col_name, col_type in target_table._schema.items():
            self._schema[f"{alias}_{col_name}"] = col_type
        
        # NEW: Cache for materialized joined table
        self._materialized = None
    
    def _materialize(self) -> "LTSeq":
        """Perform the actual join if not already done."""
        if self._materialized is not None:
            return self._materialized
        
        # Extract join keys from lambda
        left_key_dict, right_key_dict, _ = _extract_join_keys(
            self._join_fn, 
            self._source._schema, 
            self._target._schema
        )
        
        # Call Rust join operation
        joined_rust_table = self._source._rust_table.join(
            self._target._rust_table,
            left_key_dict,
            right_key_dict,
            "inner"
        )
        
        # Wrap result in LTSeq with updated schema
        result = LTSeq()
        result._rust_table = joined_rust_table
        result._schema = self._schema  # Use the merged schema
        
        self._materialized = result
        return result
    
    def select(self, *cols) -> "LTSeq":
        """Select columns from joined data."""
        joined = self._materialize()
        return joined.select(*cols)
    
    def show(self, n: int = 10) -> None:
        """Display joined data."""
        joined = self._materialize()
        joined.show(n)
    
    # Similar for derive(), filter(), etc.
```

**Estimated complexity:** 150-200 lines of Python

**Key design decisions:**

1. **Lazy materialization** - join only when data is accessed
2. **Caching** - materialized result stored in `_materialized`
3. **Schema merge** - target columns renamed with alias prefix
4. **Transparent to user** - LinkedTable behaves like LTSeq after materialization

---

### Phase 8D: Pointer Column Access (r.prod.name)

**Current MVP limitation:** LinkedTable.select() only returns source columns.

**Goal:** Support accessing linked table columns via attribute notation: `r.prod.name`

#### Challenge: Python Lambda Evaluation

The user writes: `linked.select(lambda r: [r.id, r.prod.name, r.prod.price])`

But `r.prod` doesn't exist as an actual attribute. We need:

1. **Enhanced SchemaProxy** to handle nested attribute access
2. **Expression translation** for nested columns

#### Implementation:

```python
class SchemaProxy:
    def __init__(self, schema, prefix=None):
        self._schema = schema
        self._prefix = prefix or ""  # For linked table alias
    
    def __getattr__(self, name):
        # Handle r.prod accessing linked table reference
        if name.startswith("_"):
            raise AttributeError(f"No attribute {name}")
        
        # First check if this is a simple column
        if name in self._schema:
            return ColumnExpr(name)
        
        # Check if this might be a linked table reference
        # Look for columns with prefix like "prod_name"
        matching_cols = [
            col for col in self._schema 
            if col.startswith(name + "_")
        ]
        
        if matching_cols:
            # Return a proxy for the linked table
            return SchemaProxy(
                {
                    col_suffix: self._schema[f"{name}_{col_suffix}"]
                    for col_suffix in [c.replace(name + "_", "") for c in matching_cols]
                },
                prefix=name
            )
        
        raise AttributeError(f"No column or linked table: {name}")
```

**Estimated complexity:** 80-120 lines of Python

**How it works:**

1. User writes: `lambda r: r.prod.name`
2. Parser calls `r.prod` which returns a SchemaProxy with prefix="prod"
3. Parser then calls `.name` which translates to column "prod_name"
4. Expression serializes as `{"type": "Column", "name": "prod_name"}`
5. Rust receives the prefixed column name and handles it normally

---

### Phase 8E: SQL-based Join Alternative

For robustness, we can also implement joins via SQL if DataFusion's DataFrame API is insufficient:

```rust
fn join_via_sql(
    &self,
    other: &RustTable,
    left_key: &str,
    right_key: &str,
) -> PyResult<RustTable> {
    RUNTIME.block_on(async {
        // Register both tables
        self.session.register_table("t1", Arc::new(
            MemTable::try_new(...)
        ))?;
        
        self.session.register_table("t2", Arc::new(
            MemTable::try_new(...)
        ))?;
        
        // Execute SQL join
        let sql = format!(
            "SELECT t1.*, t2.* FROM t1 INNER JOIN t2 ON t1.{} = t2.{}",
            left_key, right_key
        );
        
        let result_df = self.session.sql(&sql).await?;
        
        // Extract and return
        Ok(RustTable { ... })
    })
}
```

**Estimated complexity:** 100 lines of Rust

**Advantages:**
- More flexible join conditions
- Easier debugging (can inspect SQL)
- Better error messages

**Disadvantages:**
- Slower for simple joins (SQL parsing overhead)
- More SQL injection potential (though mitigated with proper validation)

---

## Test Cases Needed

### 1. Basic Join (test_phase8_linking.py additions)

```python
def test_link_materializes_data():
    """Linking should materialize data when accessed"""
    orders = LTSeq.read_csv("examples/orders.csv")
    products = LTSeq.read_csv("examples/products.csv")
    linked = orders.link(
        products, 
        on=lambda o, p: o.product_id == p.id,
        as_="prod"
    )
    
    # Access materialized data
    joined = linked._materialize()
    assert "prod_id" in joined._schema
    assert "prod_name" in joined._schema

def test_link_select_includes_linked_columns():
    """select() on linked table should include linked columns"""
    orders = LTSeq.read_csv("examples/orders.csv")
    products = LTSeq.read_csv("examples/products.csv")
    linked = orders.link(
        products,
        on=lambda o, p: o.product_id == p.id,
        as_="prod"
    )
    
    # This should work (once select properly handles linked columns)
    result = linked.select("id", "prod_name", "prod_price")
    assert result.count() == 8  # Should have 8 orders
    
def test_link_with_filter_on_linked_columns():
    """Can filter on linked table columns"""
    orders = LTSeq.read_csv("examples/orders.csv")
    products = LTSeq.read_csv("examples/products.csv")
    linked = orders.link(
        products,
        on=lambda o, p: o.product_id == p.id,
        as_="prod"
    )
    
    # Filter by linked column
    result = linked.filter(lambda r: r.prod_price > 75)
    assert result.count() > 0

def test_link_preserves_join_semantics():
    """Linked data should match SQL INNER JOIN semantics"""
    orders = LTSeq.read_csv("examples/orders.csv")
    products = LTSeq.read_csv("examples/products.csv")
    
    linked = orders.link(
        products,
        on=lambda o, p: o.product_id == p.id,
        as_="prod"
    )
    
    joined = linked._materialize()
    
    # All product_ids should have corresponding products
    for row in joined:
        product_id = row["product_id"]
        prod_id = row["prod_id"]
        assert product_id == prod_id  # Join constraint
```

### 2. Multiple Links

```python
def test_multiple_linked_tables():
    """Support linking to multiple tables with different aliases"""
    orders = LTSeq.read_csv("examples/orders.csv")
    products = LTSeq.read_csv("examples/products.csv")
    
    # Chain multiple links
    result = (orders
        .link(products, on=lambda o, p: o.product_id == p.id, as_="prod")
        .link(products, on=lambda o, p: o.product_id == p.id, as_="prod2"))
    
    # Should have columns from both links
    assert "prod_name" in result._schema
    assert "prod2_name" in result._schema
```

### 3. Data Integrity

```python
def test_link_data_correctness():
    """Verify joined data is semantically correct"""
    orders = LTSeq.read_csv("examples/orders.csv")
    products = LTSeq.read_csv("examples/products.csv")
    
    linked = orders.link(
        products,
        on=lambda o, p: o.product_id == p.id,
        as_="prod"
    )
    
    joined = linked._materialize()
    joined.show()
    
    # Sample verification:
    # Order 1 has product_id=101, should have prod_id=101, prod_name="Widget"
```

---

## Data Flow Diagram

```
User Code:
  orders.link(products, on=lambda o, p: o.product_id == p.id, as_="prod")
       |
       v
Python LinkedTable Created:
  _source: orders (LTSeq)
  _target: products (LTSeq)
  _join_fn: lambda function (stored)
  _alias: "prod"
  _schema: merged schema with "prod_" prefixed columns
       |
       v
User accesses data (select/show/filter):
  linked.select("id", "prod_name")
       |
       v
LinkedTable._materialize():
  1. Extract join keys from _join_fn
     o.product_id, p.id
       |
       v
  2. Call Rust join:
     orders_rust.join(
       products_rust,
       {"type": "Column", "name": "product_id"},
       {"type": "Column", "name": "id"},
       "inner"
     )
       |
       v
  3. Rust join process:
     a. Deserialize PyExpr to DataFusion Expr
     b. Create join plan
     c. Execute join in DataFusion
     d. Return new RustTable with merged data
       |
       v
  4. Cache result in _materialized
  5. Wrap in LTSeq
       |
       v
Return to user with full joined data
```

---

## Implementation Order & Milestones

### Milestone 1: Foundation (Days 1-2)
1. **Extract join keys** from lambda (Phase 8A)
   - Modify `_extract_join_keys()` function
   - Write unit tests for lambda parsing
   - **Deliverable:** Can parse `lambda o, p: o.product_id == p.id`

### Milestone 2: Rust Join Core (Days 3-5)
1. **Implement Rust join()** method (Phase 8B)
   - Add join method to RustTable
   - Test with simple equality joins
   - **Deliverable:** Rust join works in isolation

### Milestone 3: Python Integration (Days 5-7)
1. **LinkedTable materialization** (Phase 8C)
   - Implement `_materialize()` method
   - Update select/show/filter to use materialization
   - **Deliverable:** Basic link().show() works

### Milestone 4: Column Access (Days 7-8)
1. **Pointer column access** (Phase 8D)
   - Enhance SchemaProxy for nested attributes
   - Support `r.prod.name` syntax
   - **Deliverable:** Can access linked columns

### Milestone 5: Testing & Polish (Days 8-10)
1. **Comprehensive tests** (Phase 8E)
   - All test cases from section above
   - Edge cases and error handling
   - Data integrity verification
   - **Deliverable:** 15+ passing tests

---

## Risks & Mitigations

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|-----------|
| DataFusion join API complexity | Medium | High | Use SQL fallback, check API docs thoroughly |
| Lambda parameter binding issues | Low | Medium | Extensive unit tests for SchemaProxy |
| Schema merging edge cases | Medium | Medium | Careful handling of column name collisions |
| Performance (materializing large joins) | High | Medium | Document lazy materialization, add caching |
| Type inference in joined schema | Low | Medium | Use DataFusion schema inference from join result |

---

## API Changes Summary

### Python API (Changes to LinkedTable)

**Before (MVP):**
```python
linked = orders.link(products, on=lambda o, p: o.product_id == p.id, as_="prod")
linked.show()  # Shows only source columns
```

**After (Enhancement):**
```python
linked = orders.link(products, on=lambda o, p: o.product_id == p.id, as_="prod")
linked.select("id", "prod_name", "prod_price").show()  # Shows joined columns
linked.filter(lambda r: r.prod_price > 100).show()  # Filter by linked columns
linked.derive(lambda r: {"total": r.quantity * r.prod_price}).show()  # Derive using linked columns
```

### Rust API (New Methods)

```rust
// In RustTable
pub fn join(
    &self,
    other: &RustTable,
    left_key_expr_dict: &Bound<'_, PyDict>,
    right_key_expr_dict: &Bound<'_, PyDict>,
    join_type: String,
) -> PyResult<RustTable>
```

---

## DataFusion API Check

**Required functionality:**
- ✅ Join operation: `DataFrame.join()`
- ✅ JoinType enum: supports Inner/Left/Right/Full
- ✅ Column aliasing: rename columns in result
- ✅ Schema extraction from joined DataFrame

**Status:** All required APIs are available in DataFusion 35+

**Potential limitations:**
- No support for complex join conditions (only simple equality) - can be extended with filters
- Column name collisions need explicit handling

---

## Estimated Scope

| Component | Lines of Code | Effort | Risk |
|-----------|---------------|--------|------|
| Lambda parsing (Phase 8A) | 50 | Low | Low |
| Rust join (Phase 8B) | 150 | Medium | Medium |
| Python materialization (Phase 8C) | 200 | Medium | Low |
| Pointer column access (Phase 8D) | 120 | Medium | Medium |
| Tests | 400 | Low | Low |
| **TOTAL** | **~920** | Medium | Medium |

**Adjusted for rigor:** 1,500-2,000 lines total (including error handling, docs, examples)

---

## Conclusion

Phase 8 Enhancement is **implementable and well-scoped**. The foundation (LinkedTable, schema management, expression system) is solid. The main work is:

1. Adding join execution to Rust
2. Wiring materialization in Python
3. Enhancing schema proxy for nested column access
4. Comprehensive testing

All DataFusion APIs needed are available. No blockers identified. Implementation should follow the milestone-based approach for incremental validation.

