# Phase 6: Sequence Operators (shift, rolling, cum_sum, diff)

## Overview

Phase 6 implements **4 critical sequence operators** that enable temporal and positional analysis on ordered data. These operators differentiate LTSeq from traditional SQL/Pandas by providing efficient, expression-based access to previous/next rows and window aggregations.

**Key distinction**: Requires data to be sorted first (Phase 5's `sort()` is a prerequisite).

## Design Principles

1. **Order dependency**: All sequence operators assume data is sorted. Users must call `sort()` before using these operators.
2. **Expression-based**: shift/rolling integrate with the lambda DSL for seamless composition.
3. **Lazy evaluation**: Operations return new LTSeq instances; computation happens during materialization (`.show()` or `.to_pandas()`).
4. **Window semantics**: Rolling uses frame-based windowing (rows before/after current row).
5. **Null handling**: shift() introduces NULLs at boundaries; rolling() handles varying frame sizes gracefully.

## API Design

### 1. shift(n) - Access Previous/Next Rows

Shift a column by n rows (positive = forward, negative = backward).

**Signature (as expression):**
```python
def shift(self, n: int = 1) -> "Expr":
    """Shift column by n rows. Returns Expr that can be used in lambdas."""
```

**Usage in Python lambda:**
```python
t.derive(lambda r: {
    "price_prev": r.price.shift(1),      # Previous row's price
    "price_next": r.price.shift(-1),     # Next row's price
    "price_change": r.price - r.price.shift(1),
}).show()
```

**Parameters:**
- `n` (int, default=1): Number of rows to shift
  - Positive: shift down (previous rows)
  - Negative: shift up (next rows)

**Returns:**
- Expr: Can be used in lambda expressions like `r.col.shift(n)`

**Schema behavior:**
- Output type matches input type
- NULLs introduced at boundaries (first n rows have NULL for positive shift)
- New column added to table

**Implementation strategy:**
- Use DataFusion `lag(col, n)` or `lead(col, -n)` window function
- Requires `OVER (ORDER BY ...)` window specification
- Need to extract sort order from previous sort() call or infer from data

### 2. rolling(window) - Sliding Window Aggregations

Apply aggregations over sliding windows of rows.

**Signature (as expression):**
```python
def rolling(self, window: int) -> "RollingExpr":
    """Create rolling window expression for aggregations."""

class RollingExpr:
    def mean(self) -> Expr
    def sum(self) -> Expr
    def min(self) -> Expr
    def max(self) -> Expr
    def count(self) -> Expr
```

**Usage in Python lambda:**
```python
t.derive(lambda r: {
    "ma_3": r.price.rolling(3).mean(),      # 3-row moving average
    "rolling_sum": r.price.rolling(5).sum(), # 5-row rolling sum
    "rolling_max": r.price.rolling(7).max(), # 7-row rolling max
}).show()
```

**Parameters:**
- `window` (int): Window size in rows
  - Frame = [current - window + 1, ..., current]
  - Example: rolling(3) on row i includes rows [i-2, i-1, i]

**Returns:**
- RollingExpr: Chainable object with aggregation methods

**Aggregation methods:**
- `.mean()` → Expr (average of window)
- `.sum()` → Expr (sum of window)
- `.min()` → Expr (minimum in window)
- `.max()` → Expr (maximum in window)
- `.count()` → Expr (non-null count in window)

**Schema behavior:**
- Output type: Depends on aggregation (sum/mean → float or same as input, count → int)
- Early rows: Partial windows handled gracefully
  - Row 0: Window contains only row 0
  - Row 1: Window contains rows 0-1
  - Row 2+: Full window of size N
- New column added to table

**Implementation strategy:**
- Use DataFusion window functions with frame specification
- Frame: `ROWS BETWEEN window-1 PRECEDING AND CURRENT ROW`
- Requires sorting (ORDER BY) specification

### 3. cum_sum() - Cumulative Sum (Scan Operator)

Calculate running sum across ordered rows.

**Signature (as table method):**
```python
def cum_sum(self, *cols: Union[str, Callable]) -> "LTSeq":
    """Add cumulative sum columns for specified columns."""
```

**Usage:**
```python
# Simple cumulative sum on "amount" column
t.cum_sum("amount").show()

# With lambda expression
t.cum_sum(lambda r: r.price * r.quantity).show()

# Multiple columns at once
t.cum_sum("amount", "quantity").show()
```

**Parameters:**
- `*cols`: Column names (str) or lambda expressions
  - Column name: `"amount"` → new column `"amount_cumsum"`
  - Lambda: `lambda r: r.price * r.qty` → new column (unnamed, or auto-name)
  - Multiple args: adds cumulative sum for each

**Returns:**
- New LTSeq instance with cumulative sum columns added

**Schema behavior:**
- Output type: Same as input column
- New columns named `{col_name}_cumsum` or auto-generated
- No rows removed; all rows preserved

**Implementation strategy:**
- Use DataFusion `sum(...) OVER (ORDER BY {sort_order} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`
- Similar to rolling() but with unbounded preceding frame
- Requires sort order from previous sort()

### 4. diff() - Row Differences (Change Detection)

Calculate differences between current and previous row.

**Signature (as expression):**
```python
def diff(self, periods: int = 1) -> "Expr":
    """Calculate difference from previous row (current - previous)."""
```

**Usage in lambda:**
```python
t.derive(lambda r: {
    "daily_change": r.price.diff(),        # price_today - price_yesterday
    "weekly_change": r.price.diff(7),      # price_today - price_7_days_ago
}).show()
```

**Parameters:**
- `periods` (int, default=1): Number of rows to look back
  - `diff(1)`: current - previous
  - `diff(7)`: current - 7 rows back

**Returns:**
- Expr: Can be used in lambda expressions

**Schema behavior:**
- Output type: Numeric (float or same as input)
- NULLs at boundaries (first `periods` rows have NULL)
- New column added to table

**Implementation strategy:**
- Use DataFusion `col - lag(col, periods)` expression
- Actually: `col - col.shift(periods)` which is `col - lag(col, periods)`
- Requires numeric column (will error on non-numeric)

## Implementation Architecture

### Phase 5 Prerequisites

Sequence operators require sorted data. We must:
1. Track the **sort order** used in the current table
   - Store sort expressions in RustTable metadata
   - Pass to window function specifications
2. **Validate sort** before sequence operations
   - If no sort applied, require explicit sort() or raise error
   - Or silently apply default sort (not recommended for explicit queries)

### Key Design Decision: Sort Tracking

**Option A (Recommended)**: Track sort order
- Store sort expressions in RustTable
- Reuse for window function OVER clauses
- Pro: Efficient, preserves intent
- Con: Adds state tracking

**Option B (Simple)**: Require explicit sort
- Sequence ops fail if not sorted
- User must call `.sort()` first
- Pro: Simple, explicit
- Con: Less ergonomic, more user errors

**Decision**: Option A - Track sort order

### Expression Integration

Shift, rolling, diff return Expr objects that integrate with Phase 4's lambda DSL:

```
Python lambda (r.price.shift(1))
    ↓
SchemaProxy chain: r.__getattr__ → price → .shift(1)
    ↓
Expression capture: _capture_expr() serializes to dict
    ↓
Rust deserialization: dict_to_py_expr → PyExpr enum
    ↓
Transpilation: pyexpr_to_datafusion → DataFusion Expr
    ↓
DataFusion window function (LAG, OVER, etc.)
    ↓
Result
```

**New PyExpr variants needed:**
```rust
enum PyExpr {
    // ... existing variants ...
    Shift { expr: Box<PyExpr>, n: i32 },
    Rolling { expr: Box<PyExpr>, window: i32, agg_fn: String }, // "mean", "sum", etc.
    CumSum { expr: Box<PyExpr> },
    Diff { expr: Box<PyExpr>, periods: i32 },
}
```

### Transpilation Rules

| Operation | DataFusion Map | Window Spec |
|-----------|-----------------|-------------|
| `col.shift(1)` | `lag(col, 1)` | `OVER (ORDER BY {sort_expr})` |
| `col.rolling(3).mean()` | `avg(col)` | `OVER (ORDER BY {sort_expr} ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)` |
| `cum_sum(col)` | `sum(col)` | `OVER (ORDER BY {sort_expr} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` |
| `col.diff(1)` | `col - lag(col, 1)` | `OVER (ORDER BY {sort_expr})` |

## Implementation Plan

### Step 1: Python API - Expression Methods (Expr class)

**File**: `py-ltseq/ltseq/__init__.py`

Add to `Expr` class:
```python
def shift(self, n: int = 1) -> "Expr":
    """Shift by n rows."""
    return Expr({
        "type": "shift",
        "expr": self._expr,
        "n": n,
    })

def rolling(self, window: int) -> "RollingExpr":
    """Create rolling window."""
    return RollingExpr(self, window)

def diff(self, periods: int = 1) -> "Expr":
    """Calculate differences."""
    return Expr({
        "type": "diff",
        "expr": self._expr,
        "periods": periods,
    })

class RollingExpr:
    def __init__(self, expr, window):
        self.expr = expr
        self.window = window
    
    def mean(self) -> Expr:
        return Expr({
            "type": "rolling",
            "expr": self.expr._expr,
            "window": self.window,
            "agg": "mean",
        })
    
    # Similar for sum, min, max, count
```

Add to `LTSeq` class:
```python
def cum_sum(self, *cols: Union[str, Callable]) -> "LTSeq":
    """Add cumulative sum columns."""
    # Serialize each col argument
    # Call Rust method with serialized expressions
    # Return new LTSeq
```

### Step 2: Rust Infrastructure - Sort Order Tracking

**File**: `src/lib.rs`

Extend `RustTable` struct:
```rust
pub struct RustTable {
    df: DataFrame,
    schema: Vec<(String, String)>,
    sort_exprs: Vec<String>,  // Store sort order for window specs
}
```

Modify `sort()` to store sort order:
```rust
fn sort(&self, sort_exprs: Vec<Bound<'_, PyDict>>) -> PyResult<RustTable> {
    // ... existing sort logic ...
    
    // Store sort expressions
    let stored_sort = /* extract sort expr strings */;
    
    Ok(RustTable {
        df: sorted_df,
        schema: self.schema.clone(),
        sort_exprs: stored_sort,
    })
}
```

### Step 3: Rust PyExpr Enum Extensions

**File**: `src/lib.rs`

Add new variants:
```rust
enum PyExpr {
    // ... existing ...
    Shift {
        expr: Box<PyExpr>,
        n: i32,
    },
    Rolling {
        expr: Box<PyExpr>,
        window: i32,
        agg_fn: String,  // "mean", "sum", "min", "max", "count"
    },
    CumSum {
        expr: Box<PyExpr>,
    },
    Diff {
        expr: Box<PyExpr>,
        periods: i32,
    },
}
```

### Step 4: Rust Transpiler Extensions

**File**: `src/lib.rs`

Extend `pyexpr_to_datafusion()`:
```rust
PyExpr::Shift { expr, n } => {
    let inner = pyexpr_to_datafusion(*expr, schema, sort_exprs)?;
    // Return lag(inner, n) with window spec
}

PyExpr::Rolling { expr, window, agg_fn } => {
    let inner = pyexpr_to_datafusion(*expr, schema, sort_exprs)?;
    // Return agg(inner) with rolling frame
}

PyExpr::CumSum { expr } => {
    let inner = pyexpr_to_datafusion(*expr, schema, sort_exprs)?;
    // Return sum(inner) with unbounded frame
}

PyExpr::Diff { expr, periods } => {
    let inner = pyexpr_to_datafusion(*expr, schema, sort_exprs)?;
    // Return inner - lag(inner, periods)
}
```

### Step 5: Python-Rust cum_sum Binding

**File**: `src/lib.rs`

Add new method to RustTable:
```rust
fn cum_sum(&self, cum_sum_exprs: Vec<Bound<'_, PyDict>>) -> PyResult<RustTable> {
    // Deserialize expressions
    // For each expression:
    //   - Create window function with ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    //   - Apply sum() aggregation
    // Return new RustTable with cumulative columns added
}
```

### Step 6: Testing

**File**: `py-ltseq/tests/test_phase6_sequence_ops.py`

Create 50+ tests covering:
- **TestShiftBasic** (8 tests): Basic shift with different n values
- **TestShiftExpressions** (6 tests): Shift in arithmetic expressions
- **TestRollingBasic** (8 tests): Rolling window aggregations
- **TestRollingMultiple** (4 tests): Multiple rolling windows in one derive
- **TestCumSumBasic** (8 tests): Cumulative sum on columns
- **TestCumSumExpressions** (5 tests): Cumulative sum of computed expressions
- **TestDiffBasic** (6 tests): Difference operators
- **TestDiffPeriods** (5 tests): Different period values
- **TestSequenceChaining** (8 tests): Combining shift/rolling/diff
- **TestSequenceWithFilter** (6 tests): Filter + sort + sequence ops
- **TestSequenceEdgeCases** (6 tests): Boundary conditions, empty tables
- **TestSequenceNullHandling** (4 tests): NULL propagation and handling
- **TestSortOrderTracking** (3 tests): Verify sort order is preserved

## Technical Challenges & Solutions

### Challenge 1: Window Function Compatibility
**Problem**: DataFusion window functions require explicit ORDER BY clause.
**Solution**: Store sort order in RustTable; extract for window specs.

### Challenge 2: Null Handling in shift()
**Problem**: Shifted rows introduce NULLs; need graceful handling.
**Solution**: Use DataFusion's null-safe lag/lead; document in API.

### Challenge 3: Frame Specification in rolling()
**Problem**: Different agg functions need different frame sizes.
**Solution**: Parameterize frame in transpiler (ROWS BETWEEN N PRECEDING AND CURRENT ROW).

### Challenge 4: Expression Serialization
**Problem**: Shift/rolling need to capture complex expressions like `r.price.shift(1)`.
**Solution**: Extend _capture_expr() to handle method chaining (shift, rolling, diff).

### Challenge 5: Type Safety
**Problem**: rolling() and cum_sum() require numeric columns; diff() also numeric.
**Solution**: Runtime validation during transpilation; return error if non-numeric.

## Testing Strategy

### Unit Tests
- Expression serialization (shift/rolling/diff in dicts)
- PyExpr deserialization from Python dicts
- Transpilation to DataFusion expressions

### Integration Tests
- Full pipeline: sort → derive(shift/rolling/diff) → show()
- Chaining: filter → sort → cum_sum → slice
- Multiple operations in single derive

### Numeric Tests
- shift(1) results match manual prev_row access
- rolling(3).mean() matches hand-calculated 3-row averages
- cum_sum() matches hand-calculated cumulative sums
- diff(1) matches manual subtraction

### Edge Case Tests
- Empty tables
- Single-row tables
- Large window sizes (window > table size)
- Negative shifts (forward shift)
- NULL propagation in results

## Success Criteria

- [ ] All 4 operators (shift, rolling, cum_sum, diff) implemented
- [ ] 50+ comprehensive tests, all passing
- [ ] 199 + 50 = 249+ total tests passing (no regressions)
- [ ] Integration with Phase 5 (sort) verified
- [ ] Documentation with examples complete
- [ ] Build succeeds with 0 errors
- [ ] End-to-end manual testing validates correctness

## Files to Create/Modify

### New Files
- `/docs/phase6_sequence_operators.md` (this file)
- `/py-ltseq/tests/test_phase6_sequence_ops.py` (tests)

### Modified Files
- `/py-ltseq/ltseq/__init__.py` (shift, rolling, diff methods on Expr; cum_sum on LTSeq)
- `/src/lib.rs` (PyExpr variants, transpilation, RustTable sort tracking, cum_sum binding)

## Example Use Cases

### Use Case 1: Financial Analysis
```python
t = LTSeq.read_csv("stock_prices.csv").sort("date")

t.derive(lambda r: {
    "daily_change": r.close - r.close.shift(1),
    "change_pct": (r.close - r.close.shift(1)) / r.close.shift(1) * 100,
    "ma_20": r.close.rolling(20).mean(),
    "volatility_20": r.close.rolling(20).std(),  # if std supported
}).filter(lambda r: r.change_pct > 5).show()
```

### Use Case 2: Cumulative Metrics
```python
t = LTSeq.read_csv("sales.csv").sort("date")

t.cum_sum("revenue", "units").show()
# Adds columns: revenue_cumsum, units_cumsum
```

### Use Case 3: Sequence Detection
```python
t = LTSeq.read_csv("events.csv").sort("timestamp")

# Detect if value increased for 3+ consecutive rows
t.derive(lambda r: {
    "increased": (r.value > r.value.shift(1)).cast_to_int(),
    "increase_streak": r.increased.cum_sum(),
}).group_ordered(lambda r: r.increase_streak).show()
```

## Next Steps (Phase 7)

After Phase 6 completion:
- Phase 7: Ordered Grouping (group_ordered)
  - Uses cum_sum() from Phase 6
  - Implements group state machines
  - Returns nested table sequences

## References

- Phase 5 Design: `/docs/phase5_relational_ops.md`
- Milestones: `/docs/milestones.md` (Milestone 3: Sequence Arithmetic)
- Overall Plan: `/plan.md` (Phase 6 specification)
- API Reference: `/docs/api.md`
