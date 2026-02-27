# Phase 4b: Review Fixes (Items 1-6)

## Changes

### Item 1: Fix `check_expr` recursion in `_has_window_functions` 
**File: `py-ltseq/ltseq/transforms.py`, lines 276-292**

Replace the `check_expr` inner function with a version that:
- Recurses into `on` field of Call expressions
- Handles `UnaryOp` expressions
- Detects `Window` expression type (ranking functions)
- Renames local variable `method` → `func` for clarity

```python
def check_expr(expr: Dict[str, Any]) -> bool:
    if not isinstance(expr, dict):
        return False
    expr_type = expr.get("type", "")
    if expr_type == "Window":
        return True
    if expr_type == "Call":
        func = expr.get("func", "")
        if func in ("shift", "diff", "rolling", "cum_sum"):
            return True
        if check_expr(expr.get("on") or {}):
            return True
        for arg in expr.get("args", []):
            if check_expr(arg):
                return True
    elif expr_type == "BinOp":
        if check_expr(expr.get("left", {})):
            return True
        if check_expr(expr.get("right", {})):
            return True
    elif expr_type == "UnaryOp":
        if check_expr(expr.get("operand", {})):
            return True
    return False
```

### Item 2: Fix SQL space bug in cumsum fallback
**File: `src/ops/window.rs`, line 442**

```rust
// Before:
"SUM({}) OVER ({}ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",

// After:
"SUM({}) OVER ({} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
```

### Item 3: Fix orphaned doc comment
**File: `src/transpiler/window_native.rs`, lines 1-45**

Merge the free-floating doc block (lines 31-44) into the module-level `//!` doc (lines 1-5),
and update to include ranking functions. Remove the orphaned `///` block entirely.

```rust
//! Native DataFusion window expression builder
//!
//! Converts PyExpr window function calls directly into DataFusion `Expr::WindowFunction`
//! via the native API, eliminating the need for SQL string generation, materialization,
//! and re-parsing.
//!
//! Handles:
//! - `shift(n)` / `shift(n, default=val)` → `lag(col, n)` or `lead(col, -n)`
//! - `diff(n)` → `col - lag(col, n)`
//! - `rolling(n).mean/sum/min/max/count/std()` → aggregate OVER (ROWS BETWEEN ...)
//! - `cum_sum` → `sum(col) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`
//! - `PyExpr::Window` (row_number, rank, dense_rank, ntile) → corresponding window UDFs
//!
//! All window functions support an optional `partition_by` kwarg:
//! - `shift(n, partition_by="col")` — single column name as string
//! - `shift(n, partition_by=r.col)` — single column as expression
//!
//! The `sort_exprs` from the table are used for ORDER BY in OVER clauses.
//! When `partition_by` is specified, any sort expression that matches a partition
//! column is filtered out of ORDER BY to avoid redundant sorting.
```

Then remove lines 31-44 (the orphaned `///` block).

### Item 4: Add `window_native` to mod.rs module doc
**File: `src/transpiler/mod.rs`, lines 9-10**

```rust
// Before:
//! - `optimization`: Constant folding and boolean simplification
//! - `sql_gen`: SQL string generation for window functions

// After:
//! - `optimization`: Constant folding and boolean simplification
//! - `sql_gen`: SQL string generation for window functions (fallback path)
//! - `window_native`: Native DataFusion window expression builder (primary path)
```

### Item 5: Clippy fix — `if let Ok(field)` 
**File: `src/ops/window.rs`, line 415**

```rust
// Before:
if let Some(field) = schema.field_with_name(col_name_ref).ok() {

// After:
if let Ok(field) = schema.field_with_name(col_name_ref) {
```

### Item 6: Clippy fix — `.map(Arc::clone)`
**File: `src/ops/window.rs`, line 468**

```rust
// Before:
schema: table.schema.as_ref().map(|s| Arc::clone(s)),

// After:
schema: table.schema.as_ref().map(Arc::clone),
```

## Build & Test

```bash
source "$HOME/.cargo/env" && source .venv/bin/activate && maturin develop --release
pytest py-ltseq/tests/ -v --ignore=py-ltseq/tests/test_filter.py --ignore=py-ltseq/tests/test_filter_code.py --ignore=py-ltseq/tests/test_filter_debug.py --ignore=py-ltseq/tests/test_showcase.py
```

## Risk Assessment

- Items 2, 4, 5, 6: **Zero risk** — trivial fixes in non-controversial areas
- Item 3: **Zero risk** — doc comment change only, no code change
- Item 1: **Low risk** — `_has_window_functions` is redundant with Rust-side detection.
  Making it more accurate only means the Python-side `derive_with_window_functions`
  path gets used more (which is the same code path anyway). The Rust function takes
  1 arg and handles everything identically to `derive`. Both paths converge in
  `ops/window.rs:derive_with_window_functions_impl`.
