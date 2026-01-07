# Phase 6: Design Decisions

Comprehensive documentation of architectural decisions for Phase 6 
(Sequence Operators: shift, rolling, cum_sum, diff).

These decisions were made to balance implementation complexity, performance, 
user ergonomics, and alignment with LTSeq's design principles.

**Date**: January 2026
**Phase Reference**: `docs/phase6_sequence_operators.md`
**Test Reference**: `py-ltseq/tests/test_phase6_sequence_ops.py`

---

## Decision #1: Sort Order Requirement (Strict)

### Decision Statement
Sequence operators require explicit `.sort()` call before use.

### Chosen Approach
Mandatory sort (Option A: Strict requirement)

### Rationale
- Explicit operations align with LTSeq's functional, composable philosophy
- Window functions require ORDER BY clause (SQL standard)
- Prevents silent bugs from unexpected sort orders
- Predictable performance (no hidden sorts)
- Forces clear intent: "I want to order by this column"

### Exact Error Message

When a user attempts to use a sequence operator without sorting first:

```
RuntimeError: 
"shift() requires sorted data. Call .sort(...) first to specify the ordering.

Example:
  t.sort("date").derive(lambda r: {"prev": r.price.shift(1)})

Operators affected: shift(), rolling(), diff(), cum_sum()"
```

**Same message applies to all sequence operators**, with operator name substituted:
- `"shift() requires sorted data..."`
- `"rolling() requires sorted data..."`
- `"diff() requires sorted data..."`
- `"cum_sum() requires sorted data..."`

### Code Example - What Works

```python
# ✓ Correct - explicit sort first
t = LTSeq.read_csv("data.csv")
t.sort("date").derive(lambda r: {"prev": r.price.shift(1)}).show()

# ✓ Correct - multi-key sort (sort by date, then by id)
t.sort("date", "id").derive(lambda r: {"prev": r.price.shift(1)}).show()

# ✓ Correct - sort via lambda expression
t.sort(lambda r: r.date).derive(lambda r: {"prev": r.price.shift(1)}).show()

# ✓ Correct - chaining after sort
t.sort("date") \
  .filter(lambda r: r.price > 100) \
  .derive(lambda r: {"prev": r.price.shift(1)}) \
  .show()
```

### Code Example - What Fails

```python
# ✗ Error - no sort
t = LTSeq.read_csv("data.csv")
t.derive(lambda r: {"prev": r.price.shift(1)})
# RuntimeError: shift() requires sorted data. Call .sort(...) first...

# ✗ Error - sort not on sequence op path
t = LTSeq.read_csv("data.csv")
t_unsorted = t.filter(lambda r: r.price > 100)  # No sort here
t_unsorted.derive(lambda r: {"prev": r.price.shift(1)})
# RuntimeError: shift() requires sorted data. Call .sort(...) first...

# ✗ Error - sort exists but not passed through
def get_unsorted_table():
    return LTSeq.read_csv("data.csv")  # No sort metadata

t = get_unsorted_table()
t.derive(lambda r: {"prev": r.price.shift(1)})
# RuntimeError: shift() requires sorted data. Call .sort(...) first...
```

### Implementation Check (Rust)

Before creating any window function in `pyexpr_to_datafusion()`:

```rust
// In transpiler, before building window spec:
if self.sort_exprs.is_empty() {
    return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
        "shift() requires sorted data. Call .sort(...) first to specify the ordering.\n\n\
         Example:\n  \
         t.sort(\"date\").derive(lambda r: {\"prev\": r.price.shift(1)})\n\n\
         Operators affected: shift(), rolling(), diff(), cum_sum()"
    ));
}
```

Same check applies to all sequence operators in both expression transpilation and table methods.

### Test Coverage
- `TestSortOrderTracking::test_sort_order_preserved` - verify sort enables shifts
- `TestSortOrderTracking::test_multiple_sorts_last_wins` - verify last sort wins
- All sequence operator tests implicitly verify this (all data is pre-sorted in fixtures)

---

## Decision #2: Sort Metadata Preservation

### Decision Statement
Sort metadata persists across operations that preserve row order.

### Chosen Approach
Preserve metadata (Option A) with safeguards for future operations

### Rationale
- Operations like `derive()`, `filter()`, `slice()` don't change row order
- Avoid redundant re-sorts in long pipelines
- DataFusion optimizes away duplicate sorts anyway
- More ergonomic for users in practice
- Reduces need for `.sort()` repeated throughout pipeline

### Safe Operations (preserve sort metadata)

These operations preserve row order and should keep `sort_exprs`:

- **derive()** - adds computed columns, keeps row order
- **filter()** - removes rows, keeps remaining row order
- **slice()** - selects contiguous rows, keeps order
- **select()** - reorders columns only
- **distinct()** - removes duplicates but preserves relative order

### Unsafe Operations (will reset in future phases)

These operations destroy order and will reset `sort_exprs` when implemented:

- **group_by()** / **group_ordered()** - Phase 7+
- **join()** - Phase 8+
- Any future operation that reorders rows

### Code Example

```python
# All use the original "date" sort order through the entire pipeline
t = (LTSeq.read_csv("data.csv")
     .sort("date")
     .filter(lambda r: r.price > 100)        # Preserves sort metadata
     .derive(lambda r: {"ma": r.price.rolling(3).mean()})  # Still sorted
     .slice(0, 100)                          # Still sorted
     .derive(lambda r: {"prev": r.price.shift(1)})  # Works! Uses "date" sort
     .show())

# This is equivalent to (but more efficient):
t = (LTSeq.read_csv("data.csv")
     .sort("date")
     .filter(lambda r: r.price > 100)
     .sort("date")      # <-- Redundant (DataFusion optimizes this away)
     .derive(lambda r: {"ma": r.price.rolling(3).mean()})
     .sort("date")      # <-- Redundant (DataFusion optimizes this away)
     .slice(0, 100)
     .sort("date")      # <-- Redundant (DataFusion optimizes this away)
     .derive(lambda r: {"prev": r.price.shift(1)})
     .show())
```

### Implementation Details

**RustTable struct** includes sort tracking:

```rust
pub struct RustTable {
    session: Arc<SessionContext>,
    dataframe: Option<Arc<DataFrame>>,
    schema: Option<Arc<Vec<(String, String)>>>,
    sort_exprs: Vec<String>,  // NEW: Stores sort order like ["date"] or ["price", "id"]
}
```

**In sort() method** - capture and store sort order:

```rust
fn sort(&self, sort_exprs: Vec<Bound<'_, PyDict>>) -> PyResult<RustTable> {
    // ... existing sort logic ...
    
    // Extract sort column names for metadata tracking
    let mut captured_sort = Vec::new();
    for expr_dict in &sort_exprs {
        let py_expr = dict_to_py_expr(expr_dict)?;
        if let PyExpr::Column(name) = py_expr {
            captured_sort.push(name);
        } else {
            // For complex expressions, use a placeholder
            captured_sort.push(format!("expr_{}", captured_sort.len()));
        }
    }
    
    Ok(RustTable {
        session: Arc::clone(&self.session),
        dataframe: Some(Arc::new(sorted_df)),
        schema: self.schema.as_ref().map(|s| Arc::clone(s)),
        sort_exprs: captured_sort,  // Store the sort order
    })
}
```

**In derive(), filter(), slice(), select(), distinct()** - preserve metadata:

```rust
fn derive(&self, ...) -> PyResult<RustTable> {
    // ... existing derive logic ...
    Ok(RustTable {
        session: Arc::clone(&self.session),
        dataframe: Some(Arc::new(result_df)),
        schema: new_schema,
        sort_exprs: self.sort_exprs.clone(),  // PRESERVE sort metadata
    })
}

// Same pattern for filter(), slice(), select(), distinct()
```

### Contract for Future Operations

> **Important**: Any new operation that reorders rows MUST reset `sort_exprs` to an empty `Vec`.
> Document this in code comments. Example:
> ```rust
> // group_ordered() reorders by group boundaries, so reset sort tracking
> sort_exprs: vec![],  // Reset: data is no longer in original sort order
> ```

### Test Coverage
- `TestSortOrderTracking::test_sort_order_preserved` - long multi-step pipeline
- `TestSequenceChaining::*` - all tests verify multi-step operations work with one sort

---

## Decision #3: NULL/Partial Window Behavior (Configurable)

### Decision Statement
Graceful handling at boundaries, configurable per-operator.

### Chosen Approach
Context-dependent with configuration option for `rolling()`

### Rationale
- Different operators have different natural semantics
- Aligns with Pandas behavior (familiar to users)
- Configurable `rolling()` allows both strict and graceful modes
- Useful partial data > lost data (but users can opt for strictness)

### Per-Operator Behavior Matrix

| Operator | Boundary Behavior | Config | Rationale |
|----------|------------------|--------|-----------|
| **shift(n)** | NULL at boundaries | None | No previous row exists |
| **rolling(w)** | **Partial windows** (default) | `min_periods` param | Useful data from early rows |
| **diff(periods)** | NULL at boundaries | None | Can't subtract from NULL |
| **cum_sum()** | Full accumulation | None | Sum always starts at first row |

### shift() Behavior (Immutable)

Shift always produces NULL when there's no data to shift from:

```python
t = LTSeq.read_csv("data.csv").sort("date")
t.slice(0, 1)  # Only first row: price = 100

t.derive(lambda r: {"prev": r.price.shift(1)})
# Result:
# | price | prev  |
# |-------|-------|
# | 100   | NULL  |  # No previous row exists

# With larger dataset:
# Original: prices = [100, 102, 101, 105, 108]
# After shift(1):
# | price | prev  |
# |-------|-------|
# | 100   | NULL  |  # First row has no previous
# | 102   | 100   |
# | 101   | 102   |
# | 105   | 101   |
# | 108   | 105   |
```

### rolling() Behavior (Configurable with min_periods)

Rolling windows can be configured to gracefully handle partial data or require full windows:

**Default (Graceful - no min_periods specified):**

```python
t = LTSeq.read_csv("data.csv").sort("date")
t.slice(0, 2)  # Only 2 rows: price = [100, 102]

t.derive(lambda r: {"ma_5": r.price.rolling(5).mean()})
# Result - window grows as more data available:
# | price | ma_5  |
# |-------|-------|
# | 100   | 100.0 |  # Window: [100] (1 value, 1/5 full)
# | 102   | 101.0 |  # Window: [100, 102] (2 values, 2/5 full)
```

**Strict (min_periods=5 requires full window):**

```python
t = LTSeq.read_csv("data.csv").sort("date")
t.slice(0, 2)  # Only 2 rows: price = [100, 102]

t.derive(lambda r: {"ma_5": r.price.rolling(5, min_periods=5).mean()})
# Result - requires all 5 values or NULL:
# | price | ma_5  |
# |-------|-------|
# | 100   | NULL  |  # Window incomplete (need 5, have 1)
# | 102   | NULL  |  # Window incomplete (need 5, have 2)
```

**Mid-strict (min_periods=3 requires at least 3):**

```python
t = LTSeq.read_csv("data.csv").sort("date")
t.slice(0, 4)  # 4 rows: price = [100, 102, 101, 105]

t.derive(lambda r: {"ma_5": r.price.rolling(5, min_periods=3).mean()})
# Result:
# | price | ma_5   |
# |-------|--------|
# | 100   | NULL   |  # Need 3, have 1
# | 102   | NULL   |  # Need 3, have 2
# | 101   | 101.0  |  # Need 3, have 3 ✓ avg([100, 102, 101])
# | 105   | 102.67 |  # Need 3, have 4 ✓ avg([102, 101, 105])
```

### diff() Behavior (Immutable)

Diff produces NULL when there's no previous value to subtract from:

```python
t = LTSeq.read_csv("data.csv").sort("date")
t.slice(0, 1)  # Only first row: price = 100

t.derive(lambda r: {"change": r.price.diff()})
# Result:
# | price | change |
# |-------|--------|
# | 100   | NULL   |  # Can't compute 100 - lag(NULL)

# With larger dataset:
# Original: prices = [100, 102, 101, 105]
# After diff(1):
# | price | change |
# |-------|--------|
# | 100   | NULL   |  # First row has no previous
# | 102   | 2      |  # 102 - 100
# | 101   | -1     |  # 101 - 102
# | 105   | 4      |  # 105 - 101
```

### cum_sum() Behavior (Immutable)

Cumulative sum always has a value (starts from first row):

```python
t = LTSeq.read_csv("data.csv").sort("date")
# Rows: price = [100, 102, 101, 105, 108]

t.cum_sum("price")
# Result:
# | price | price_cumsum |
# |-------|--------------|
# | 100   | 100          |  # sum of [100]
# | 102   | 202          |  # sum of [100, 102]
# | 101   | 303          |  # sum of [100, 102, 101]
# | 105   | 408          |  # sum of [100, 102, 101, 105]
# | 108   | 516          |  # sum of [100, 102, 101, 105, 108]
```

### rolling() Configuration Signature

```python
def rolling(self, window: int, min_periods: Optional[int] = None) -> "RollingExpr":
    """
    Create rolling window with configurable strictness.
    
    Args:
        window: Window size in rows (total rows to include)
        min_periods: Minimum number of non-null observations required
                    - None (default): Use window size (graceful partials)
                    - int: Require at least this many values
                    
    Returns:
        RollingExpr: Can call .mean(), .sum(), .min(), .max(), .count()
        
    Examples:
        # Graceful: partial windows in early rows
        r.price.rolling(5).mean()
        
        # Strict: only compute when 5 values available
        r.price.rolling(5, min_periods=5).mean()
        
        # Mid-strict: require 3 of 5 values
        r.price.rolling(5, min_periods=3).mean()
    """
```

### Test Coverage
- `TestSequenceNullHandling::test_shift_introduces_nulls` - verify shift produces NULL
- `TestSequenceNullHandling::test_rolling_with_nulls` - verify rolling partial windows
- `TestSequenceNullHandling::test_diff_with_nulls` - verify diff produces NULL
- Additional tests for different `min_periods` values (in Phase 6.1)

---

## Decision #4: cum_sum() Feature Scope (Phased)

### Decision Statement
Start with column names only; add expression support in Phase 6.1.

### Chosen Approach
Option C (Phased implementation)

### Rationale
- **Column-name version**: ~1 hour to implement (straightforward)
- **Expression version**: +1 additional hour (requires intermediate column management)
- **MVP value**: 80% of use cases covered by column names
- **Iterative delivery**: Better to ship working feature, enhance later
- **Code quality**: Simpler Phase 6 = fewer bugs, cleaner code

### Phase 6 (Current): Column Names Only

What Phase 6 supports:

```python
# ✓ Phase 6: Column names
t.cum_sum("revenue")
t.cum_sum("revenue", "units", "qty")  # Multiple columns

# ✗ Phase 6: Expressions (not yet)
t.cum_sum(lambda r: r.price * r.quantity)
# ValueError: cum_sum() in Phase 6 only supports column names. 
#             Lambda expressions will be supported in Phase 6.1
```

### Phase 6.1 (Follow-up): Add Expression Support

What Phase 6.1 will add:

```python
# Phase 6.1 will support all of the above plus:
t.cum_sum(lambda r: r.price * r.quantity)
t.cum_sum("revenue", lambda r: r.discount_value)  # Mixed
```

### Python API Signature

```python
def cum_sum(self, *cols: str) -> "LTSeq":
    """
    Add cumulative sum columns for specified column names.
    
    Phase 6 only supports column names. Lambda expression support
    will be added in Phase 6.1.
    
    Args:
        *cols: Column names (str only in Phase 6)
               
    Returns:
        New LTSeq with cumulative sum columns added
        New columns named: {col_name}_cumsum
        
    Raises:
        ValueError: If any argument is not a string (Phase 6 limitation)
        RuntimeError: If data is not sorted (see Decision #1)
        
    Example:
        t.sort("date").cum_sum("revenue")
        t.sort("date").cum_sum("revenue", "units")
    """
```

### Rust Implementation (Phase 6)

```rust
fn cum_sum(&self, cum_exprs: Vec<Bound<'_, PyDict>>) -> PyResult<RustTable> {
    // Step 1: Validate sort (Decision #1)
    if self.sort_exprs.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "cum_sum() requires sorted data. Call .sort(...) first to specify the ordering.\n\n\
             Example:\n  \
             t.sort(\"date\").cum_sum(\"revenue\")\n\n\
             Operators affected: shift(), rolling(), diff(), cum_sum()"
        ));
    }
    
    let schema = self.schema.as_ref()
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Schema not available. Call read_csv() first."
        ))?;
    
    let df = self.dataframe.as_ref()
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "No data loaded. Call read_csv() first."
        ))?;
    
    let mut result_df = (**df).clone();
    
    // Step 2: For each cumulative sum expression
    for expr_dict in cum_exprs {
        // Step 3: Deserialize and extract column name
        let py_expr = dict_to_py_expr(&expr_dict)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
        
        let col_name = match py_expr {
            PyExpr::Column(name) => name,
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "cum_sum() in Phase 6 only supports column names. \
                     Lambda expressions will be supported in Phase 6.1.\n\n\
                     Example: t.cum_sum(\"revenue\")\n\
                     Not yet: t.cum_sum(lambda r: r.price * r.qty)"
                ));
            }
        };
        
        // Step 4: Create window function for cumulative sum
        let cum_col = sum(col(&col_name))
            .window(build_window_spec(&self.sort_exprs)?)
            .alias(&format!("{}_cumsum", col_name));
        
        // Step 5: Add column to result
        result_df = result_df.select(vec![
            col("*"),
            cum_col,
        ]).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            format!("Failed to add cumulative sum column: {}", e)
        ))?;
    }
    
    Ok(RustTable {
        session: Arc::clone(&self.session),
        dataframe: Some(Arc::new(result_df)),
        schema: self.schema.as_ref().map(|s| Arc::clone(s)),
        sort_exprs: self.sort_exprs.clone(),  // Preserve sort metadata
    })
}
```

### Example - Phase 6 vs Phase 6.1

**Phase 6 - What Works Now:**

```python
t = LTSeq.read_csv("sales.csv").sort("date")

# ✓ Simple column name
t.cum_sum("revenue").show()

# ✓ Multiple columns
t.cum_sum("revenue", "units", "quantity").show()

# ✓ In a pipeline
t.filter(lambda r: r.product_id == 42) \
 .cum_sum("revenue", "quantity") \
 .show()
```

**Phase 6.1 - What Gets Added Later:**

```python
# Phase 6.1 will enable:
t.cum_sum(lambda r: r.price * r.quantity)  # Cumsum of computed value
t.cum_sum("revenue", lambda r: r.margin)   # Mixed column and expression
```

### Test Coverage
- `TestCumSumBasic` (6 tests) - column names ✓ Phase 6
- `TestCumSumExpressions` (3 tests) - SKIPPED in Phase 6, enabled Phase 6.1

---

## Decision #5: Expression Serialization Architecture

### Decision Statement
Pattern match on CallExpr instead of creating new PyExpr variants.

### Chosen Approach
Option B (Pattern matching on existing CallExpr)

### Rationale
- CallExpr already serializes method calls perfectly
- Eliminates code duplication and redundant variants
- More maintainable: future methods automatically reuse pattern
- Less code overall (~100 lines vs ~250 lines)
- Aligns with existing Phase 4/5 architecture
- Simpler implementation = fewer bugs

### How Expression Serialization Works

**User writes:**
```python
r.price.shift(1)
```

**Python serializes as CallExpr:**
```python
{
    "type": "Call",
    "func": "shift",
    "args": [{"type": "Literal", "value": 1, "dtype": "Int64"}],
    "on": {"type": "Column", "name": "price"},
    "kwargs": {}
}
```

**Rust deserializes and transpiles:**
```rust
PyExpr::Call { func: "shift", args, on, .. } => {
    let col_expr = pyexpr_to_datafusion(*on, schema, sort_exprs)?;
    let n = extract_literal_int(&args[0])?;
    let sort_spec = build_window_spec(sort_exprs)?;
    Ok(lag(col_expr, n).window(sort_spec))
}
```

### Why NO New PyExpr Variants

The design doc originally suggested creating these:

```rust
// DON'T create these - they're redundant:
enum PyExpr {
    Shift { expr: Box<PyExpr>, n: i32 },        // ✗ Duplicates CallExpr
    Rolling { expr: Box<PyExpr>, window: i32 }, // ✗ Duplicates CallExpr
    Diff { expr: Box<PyExpr>, periods: i32 },   // ✗ Duplicates CallExpr
}
```

**Why this is wrong:**
- CallExpr already represents these perfectly: `Call { func: "shift", args, on }`
- Adding new variants duplicates logic
- Harder to maintain (change in one place isn't enough)
- Breaks open/closed principle (new method = new variant)

**Instead: Use CallExpr pattern matching:**

```rust
// Use the existing CallExpr and match on func string
PyExpr::Call { func, args, on, .. } => {
    match func.as_str() {
        "shift" => { /* window logic */ },
        "rolling" => { /* window logic */ },
        "diff" => { /* window logic */ },
        _ => { /* unknown method */ }
    }
}
```

### Transpilation Pattern (in pyexpr_to_datafusion)

```rust
PyExpr::Call { func, args, on, .. } => {
    let col_expr = pyexpr_to_datafusion(*on, schema, sort_exprs)?;
    
    match func.as_str() {
        "shift" => {
            // Extract n from args[0]
            let n = extract_literal_int(&args[0])?;
            let sort_spec = build_window_spec(sort_exprs)?;
            Ok(lag(col_expr, n).window(sort_spec))
        },
        
        "rolling" => {
            // rolling() is complex: r.col.rolling(3).mean()
            // Serializes as nested Calls:
            // Call("mean", args=[], on=Call("rolling", args=[3], on=Column(...)))
            // Let parent Call("mean") handle agg logic
            let window_size = extract_literal_int(&args[0])?;
            let min_periods = args.get(1)
                .and_then(|arg| extract_literal_int_opt(arg))
                .unwrap_or(window_size);
            // Return expression for next level to handle
        },
        
        "mean" | "sum" | "min" | "max" | "count" => {
            // Check if this is a rolling aggregation
            if let PyExpr::Call { func: "rolling", args: rolling_args, .. } = &**on {
                // This is r.col.rolling(w).agg()
                let window_size = extract_literal_int(&rolling_args[0])?;
                let min_periods = rolling_args.get(1)
                    .and_then(|arg| extract_literal_int_opt(arg))
                    .unwrap_or(window_size);
                let sort_spec = build_window_spec(sort_exprs)?;
                
                match func.as_str() {
                    "mean" => Ok(avg(col_expr)
                        .window_with_frame(sort_spec, window_size, min_periods)),
                    "sum" => Ok(sum(col_expr)
                        .window_with_frame(sort_spec, window_size, min_periods)),
                    "min" => Ok(min(col_expr)
                        .window_with_frame(sort_spec, window_size, min_periods)),
                    "max" => Ok(max(col_expr)
                        .window_with_frame(sort_spec, window_size, min_periods)),
                    "count" => Ok(count(col_expr)
                        .window_with_frame(sort_spec, window_size, min_periods)),
                    _ => unreachable!()
                }
            } else {
                // Regular aggregation (not rolling)
                Err("Aggregation outside rolling() not yet supported".to_string())
            }
        },
        
        "diff" => {
            let periods = extract_literal_int(&args[0]).unwrap_or(1);
            let sort_spec = build_window_spec(sort_exprs)?;
            Ok(col_expr - lag(col_expr, periods).window(sort_spec))
        },
        
        _ => {
            Err(format!("Unknown method call: {}", func))
        }
    }
}
```

### Complex Expression Example

User expression:
```python
r.price.rolling(3).mean() - r.price.shift(1)
```

Serializes to:
```python
{
    "type": "BinOp",
    "op": "Sub",
    "left": {
        "type": "Call",
        "func": "mean",
        "args": [],
        "on": {
            "type": "Call",
            "func": "rolling",
            "args": [{"type": "Literal", "value": 3, "dtype": "Int64"}],
            "on": {"type": "Column", "name": "price"}
        }
    },
    "right": {
        "type": "Call",
        "func": "shift",
        "args": [{"type": "Literal", "value": 1, "dtype": "Int64"}],
        "on": {"type": "Column", "name": "price"}
    }
}
```

Transpiles to DataFusion:
```sql
(AVG(price) OVER (ORDER BY ... ROWS BETWEEN 2 PRECEDING AND CURRENT ROW))
  -
(LAG(price, 1) OVER (ORDER BY ...))
```

### Code Complexity Comparison

| Aspect | Pattern Match | New Variants |
|--------|---------------|--------------|
| Lines to add | ~150 | ~250 |
| New enum variants | 0 | 4 |
| Deserialization changes | 0 | 4 new arms |
| Transpilation changes | 1 match arm | 4 match arms |
| Future extension (e.g., `.abs()`) | 1 line | 1 new variant + impl |
| Test impact | 0 (uses existing tests) | More edge cases |

**Pattern matching is ~40% simpler with same functionality.**

### Test Coverage
- All expression tests automatically work via CallExpr
- No new test types needed
- Existing expression pipeline tests validate the transpilation

---

## Implementation Validation Checklist

When implementing Phase 6, verify these decision points:

### Decision #1 - Sort Strictness
- [ ] Check `sort_exprs.is_empty()` before each window function
- [ ] Raise exact error message specified above
- [ ] Error applies to: shift(), rolling(), diff(), cum_sum()
- [ ] Test: sequence op without sort() errors with correct message

### Decision #2 - Sort Metadata
- [ ] RustTable struct has `sort_exprs: Vec<String>` field
- [ ] All safe operations (derive, filter, slice, select, distinct) preserve sort_exprs
- [ ] sort() method captures and stores sort column names
- [ ] Test: long pipeline works with single sort() call
- [ ] Document contract for unsafe operations

### Decision #3 - NULL Behavior
- [ ] shift(): uses `lag()` - naturally produces NULL at boundaries
- [ ] rolling(): accepts `min_periods` parameter
  - [ ] Default (None): graceful partial windows
  - [ ] Set to window size: strict full windows only
  - [ ] Set to custom value: configurable threshold
- [ ] diff(): uses subtraction with lag - naturally produces NULL
- [ ] cum_sum(): uses unbounded window - always has value
- [ ] Test: verify each boundary case behaves as documented

### Decision #4 - cum_sum Scope
- [ ] Phase 6: only column names (str type)
- [ ] Raise clear error if expression (Callable) passed
- [ ] Error message includes "Phase 6.1" for future support
- [ ] Test: column names work correctly
- [ ] Test: expressions error gracefully with helpful message

### Decision #5 - Expression Architecture
- [ ] Use pattern matching on CallExpr
- [ ] NO new PyExpr variants created
- [ ] Handle nested calls: rolling().mean() → Call within Call
- [ ] Handle complex expressions: BinOp with Call children
- [ ] Test: all expression combinations transpile correctly

---

## Open Questions Addressed

1. **min_periods naming**: Using Pandas convention `min_periods` ✓
2. **Error message location**: Generating in Rust (transpiler/method) ✓
3. **Rolling edge case**: Partial window returns float (same type as agg) ✓
4. **Future compatibility**: Phase 6.1 can change signature or overload ✓
5. **Documentation**: Keeping design decisions separate from API reference ✓

---

## Revision History

| Date | Status | Changes |
|------|--------|---------|
| Jan 2026 | Approved | All 5 decisions finalized with implementation details |

---

## References

- **Phase 6 Design Spec**: `docs/phase6_sequence_operators.md`
- **Phase 6 Tests**: `py-ltseq/tests/test_phase6_sequence_ops.py`
- **Phase 5 Reference**: `docs/phase5_relational_ops.md`
- **API Reference**: `docs/api.md`

---

## Summary

These 5 design decisions provide clear guidance for Phase 6 implementation:

| Decision | Approach | Benefit |
|----------|----------|---------|
| **Sort Strictness** | Explicit requirement (error if missing) | Prevents silent bugs, predictable |
| **Sort Preservation** | Preserve across safe ops | Efficient pipelines, less re-sorting |
| **NULL Handling** | Context-dependent + configurable rolling | Useful data, user control |
| **cum_sum Scope** | Start simple, add expressions later | Fast MVP, quality code |
| **Expression Architecture** | Pattern match on CallExpr | Simpler, maintainable, extensible |

This document is the **source of truth** for all Phase 6 implementation decisions. Reference it during:
- Code review (verify decisions are followed)
- Testing (validate each decision point)
- Future maintenance (understand the "why")

---

## Next Steps

1. ✓ Design decisions documented
2. → Implement Step 1: Sort Order Tracking (RustTable struct)
3. → Implement Step 2: Window Function Transpilation
4. → Implement Step 3: Complete cum_sum() Method
5. → Run full test suite (62 skipped tests should pass)
