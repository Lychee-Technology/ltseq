# LTSeq ClickBench Performance Plan

**Created:** 2026-02-26
**Status:** PLANNED
**Goal:** Make LTSeq outperform DuckDB on sequence-oriented workloads (sessionization, funnel analysis)

---

## Diagnosis

LTSeq is a transpilation layer over DataFusion, not a custom execution engine. The original plan (`benchmarks/click-bench-plan.md`) assumed `shift()` is a zero-copy pointer offset. In reality, `shift(n)` transpiles to DataFusion's `LAG()`/`LEAD()` window functions (`src/transpiler/window_native.rs:240-289`). For R2/R3, LTSeq does the same computation as DuckDB but through DataFusion's window executor, which is ~3x slower.

### Current Results (100M rows)

| Round | DuckDB | LTSeq | Ratio |
|-------|--------|-------|-------|
| R1: Top URLs | 2.18s | 1.99s | 1.1x LT win |
| R2: Sessionization | 1.17s | 4.44s | 3.8x DK win |
| R3: Funnel | 3.47s | 10.39s | 3.0x DK win |

### Current Results (1M row sample)

| Round | DuckDB | LTSeq | Ratio |
|-------|--------|-------|-------|
| R1: Top URLs | 0.040s | 0.046s | 1.2x DK |
| R2: Sessionization | 0.028s | 0.059s | 2.1x DK |
| R3: Funnel | 0.056s | 0.107s | 1.9x DK |

### Root Cause

- R2's primary approach (`group_ordered`) fails: "only supports simple column references"
- R2 fallback (`derive(shift) + filter`) transpiles to the same LAG window function DuckDB uses, but DataFusion's executor is ~3x slower
- R3 (`derive(LEAD) + filter`) same issue: identical computation, slower engine
- R1 already wins (1.1x) — DuckDB's hash aggregation vs DataFusion's, small gap

---

## Phase 0: Fix R3 Result Discrepancy (P0 — Correctness)

**Problem:** R3 produces 1602 (LTSeq) vs 1601 (DuckDB) on the 1M sample.

**Root Cause Hypothesis:** DuckDB's `LEAD(url, n) OVER (PARTITION BY userid ...)` returns NULL for the last n rows in each partition. LTSeq's `shift(-n, partition_by="userid")` generates `lead(col, n)` via `convert_shift` (`src/transpiler/window_native.rs:279-283`). The off-by-one likely arises from:
- Different NULL handling in `starts_with` — DataFusion may treat `NULL.starts_with(...)` differently than DuckDB
- Or: the DuckDB query has redundant `nextuid1`/`nextuid2` columns that the LTSeq benchmark omits (since `partition_by` handles user boundaries), and edge case behavior differs

**Investigation Steps:**
1. Run both queries on 1M sample, materialize full results (not just count)
2. Diff to find the extra row in LTSeq
3. Check if the extra row is at a partition boundary
4. Check `starts_with(NULL)` behavior in both engines

**Files:** `benchmarks/bench_vs.py` (add debug mode), `src/transpiler/window_native.rs` (if shift boundary handling needs fixing)

**Effort:** 2-4 hours
**Risk:** Low

---

## Phase 1: Expand `group_ordered` to Accept Complex Expressions (P1)

**Problem:** `group_ordered(lambda r: (r.userid != r.userid.shift(1)) | (r.eventtime - r.eventtime.shift(1) > 1800))` fails with "group_ordered currently only supports simple column references" at `src/ops/grouping.rs:185-191`.

**Root Cause:** `group_id_impl` pattern-matches only `PyExpr::Column` and string-interpolates the column name into SQL:

```rust
// src/ops/grouping.rs:185-191
let grouping_col_name = if let PyExpr::Column(ref col_name) = py_expr {
    col_name.clone()
} else {
    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
        "group_ordered currently only supports simple column references",
    ));
};
```

Complex expressions are rejected. The column name is then interpolated into SQL at line 207:
```sql
WHEN "{groupcol}" IS DISTINCT FROM LAG("{groupcol}") OVER (ORDER BY __row_num) THEN 1
```

**Python side:** Already works. `_capture_expr` serializes any expression. `NestedTable.flatten()` (`py-ltseq/ltseq/grouping/nested_table.py:126`) calls `_capture_expr(self._grouping_lambda)` and passes the dict to Rust's `group_id()`. No Python changes needed.

### Implementation: Native DataFusion Expr Approach

Rewrite `group_id_impl` to build group assignment logic entirely in DataFusion's native `Expr` API, similar to how `try_native_window_derive` works in `src/ops/window.rs:246-296`.

**Strategy for expressions containing `shift()`:**

The grouping boundary condition `(r.userid != r.userid.shift(1)) | (r.eventtime - r.eventtime.shift(1) > 1800)` is itself an expression containing window functions. With the native window path:

1. Check if the expression contains window functions via `contains_window_function(&py_expr)`
2. If yes: convert the full boundary expression to a DataFusion `Expr` using `pyexpr_to_window_expr()` — this produces a boolean column expression using LAG() (lazy, no materialization)
3. Derive the boundary as a column: `df.select([...existing_cols, boundary_expr.alias("__boundary__")])`
4. Build mask: `CASE WHEN __boundary__ THEN 1 ELSE 0 END` as a DataFusion `Expr`
5. Build cumulative group ID: `SUM(mask) OVER (ORDER BY sort_exprs ROWS UNBOUNDED PRECEDING TO CURRENT ROW)` using `aggregate_to_window()` from `window_native.rs:107-133`
6. Select all original columns + `__group_id__` via `df.select()`
7. The entire chain stays as a DataFusion logical plan — zero materialization until `.count()` or `.collect()`

**Strategy for expressions WITHOUT `shift()` (simple column/expression):**

1. Convert expression to DataFusion `Expr` via `pyexpr_to_datafusion()`
2. Build: `expr IS DISTINCT FROM LAG(expr, 1) OVER (ORDER BY sort_exprs)` using native lag() + finalize_window_expr()
3. Same cumulative sum pattern as above

**Pseudocode:**
```rust
pub fn group_id_impl(table: &LTSeqTable, grouping_expr: Bound<'_, PyDict>) -> PyResult<LTSeqTable> {
    let (df, schema) = table.require_df_and_schema()?;
    let py_expr = dict_to_py_expr(&grouping_expr)?;

    let has_window = contains_window_function(&py_expr);

    // Step 1: Build the boundary boolean expression
    let boundary_expr = if has_window {
        // The expression itself IS the boundary condition (e.g., userid != userid.shift(1))
        // Convert using the window-aware transpiler
        pyexpr_to_window_expr(py_expr, schema, &table.sort_exprs)?
    } else {
        // Simple expression — build: expr IS DISTINCT FROM LAG(expr, 1)
        let expr = pyexpr_to_datafusion(py_expr, schema)?;
        let lag_expr = lag(expr.clone(), Some(1), None);
        let lag_with_order = finalize_window_expr(lag_expr, vec![], &order_by, "group_id")?;
        Expr::IsDistinctFrom(Box::new(expr), Box::new(lag_with_order))
    };

    // Step 2: Build mask (1 for boundary, 0 otherwise)
    let mask_expr = case(boundary_expr)
        .when(lit(true), lit(1i64))
        .otherwise(lit(0i64))?;

    // Step 3: Cumulative sum for group IDs
    let group_id_expr = aggregate_to_window(
        sum(mask_expr),
        vec![],          // no partition_by
        order_by.clone(), // ORDER BY sort_exprs
        cumulative_frame, // ROWS UNBOUNDED PRECEDING TO CURRENT ROW
    )?;

    // Step 4: Select all columns + __group_id__ + __group_count__ + __rn__
    let mut all_exprs = existing_column_exprs(schema);
    all_exprs.push(group_id_expr.alias("__group_id__"));
    // Add __group_count__ and __rn__ as additional window functions
    // COUNT(*) OVER (PARTITION BY __group_id__)
    // ROW_NUMBER() OVER (PARTITION BY __group_id__ ORDER BY sort_exprs)

    let result_df = df.select(all_exprs)?;  // stays lazy!
    Ok(LTSeqTable::from_df(session, result_df, sort_exprs))
}
```

**Challenge:** `__group_count__` and `__rn__` partition by `__group_id__`, which is itself a window function result. DataFusion may need the group_id to be materialized before computing the partition. If so, we can split into two steps:
1. First `df.select()` adds `__group_id__` (lazy)
2. Collect to MemTable (one materialization)
3. Second `df.select()` adds `__group_count__` and `__rn__` partitioned by `__group_id__`

This is still better than the current approach (which has 2 materializations: collect + SQL execution).

**Files to modify:**
- `src/ops/grouping.rs` — rewrite `group_id_impl` (core change, ~150 lines)
- `src/transpiler/mod.rs` — ensure `contains_window_function` is `pub(crate)` (check visibility)
- `src/ops/window.rs` — may need to extract helper functions

**Files NOT changed:**
- Python side (already works)
- `src/transpiler/window_native.rs` (already handles shift->LAG)

**Effort:** ~6 hours
**Impact:** Unblocks R2 primary approach. Eliminates 2 materialization round-trips. However, the actual window function computation (LAG) is the same cost — the speedup is from removing the collect->MemTable->SQL->collect cycle (~10-20% improvement for R2).
**Dependencies:** None
**Testing:** Existing `test_group_ordered.py` tests + new test for complex expressions

---

## Phase 2: Native Arrow Shift Bypass (P1 — Highest Performance Impact)

**Problem:** `shift(n)` transpiles to DataFusion's `LAG(n)/LEAD(n)` window functions. DataFusion's window executor sorts, partitions, and evaluates per-batch, which is ~3x slower than DuckDB for this workload. Since LTSeq's data is physically pre-sorted (tracked in `sort_exprs`), we can compute shift as a direct Arrow array operation — O(n) sequential read, no hash table, no sort overhead.

**Approach:** Intercept shift operations at the `try_native_window_derive` level. When the table's `sort_exprs` satisfy the window's ORDER BY requirement, bypass DataFusion entirely and compute the shifted columns as Arrow array slices.

### Implementation

**New file: `src/ops/arrow_shift.rs`**

Core function:
```rust
/// Compute shift(n) on Arrow RecordBatches without DataFusion window functions.
///
/// Prerequisites: data is physically sorted by `sort_exprs`, and the shift's
/// ORDER BY matches `sort_exprs`.
///
/// For shift(n) without partition_by:
///   - Concatenate all batches into a single array per column
///   - For each shifted column, slice the array with offset and prepend/append nulls
///
/// For shift(n) with partition_by:
///   - Data is already sorted with partition key first in sort_exprs
///   - Find partition boundaries via binary search on the partition column
///   - Apply shift within each partition, respecting boundaries
pub fn compute_arrow_shift(
    batches: &[RecordBatch],
    column_name: &str,
    offset: i64,
    partition_by: Option<&str>,
) -> Result<ArrayRef, String> {
    // 1. Extract the column across all batches
    let arrays: Vec<&ArrayRef> = batches.iter()
        .map(|b| b.column(b.schema().index_of(column_name)?))
        .collect();

    // 2. Concatenate into one contiguous array
    let combined = arrow::compute::concat(&arrays)?;
    let total_len = combined.len();

    if partition_by.is_none() {
        // Simple case: global shift
        if offset > 0 {
            // LAG: [NULL * offset, data[0..len-offset]]
            let null_prefix = new_null_array(combined.data_type(), offset as usize);
            let sliced = combined.slice(0, total_len - offset as usize);
            concat(&[&null_prefix, &sliced])
        } else {
            // LEAD: [data[offset..len], NULL * offset]
            let abs_offset = (-offset) as usize;
            let sliced = combined.slice(abs_offset, total_len - abs_offset);
            let null_suffix = new_null_array(combined.data_type(), abs_offset);
            concat(&[&sliced, &null_suffix])
        }
    } else {
        // Partitioned shift: find partition boundaries and apply shift within each
        let partition_col = extract_column(batches, partition_by)?;
        let boundaries = find_partition_boundaries(&partition_col);
        let mut result_chunks = Vec::new();
        for window in boundaries.windows(2) {
            let (start, end) = (window[0], window[1]);
            let partition_slice = combined.slice(start, end - start);
            let shifted = shift_array(&partition_slice, offset);
            result_chunks.push(shifted);
        }
        concat_arrays(&result_chunks)
    }
}

/// Find partition boundaries in a sorted column.
/// Returns indices where the value changes (including 0 and len).
fn find_partition_boundaries(col: &ArrayRef) -> Vec<usize> {
    // Linear scan comparing adjacent values
    // For large datasets, could use binary search on sorted data
    let mut boundaries = vec![0usize];
    for i in 1..col.len() {
        if !equal_at(col, i-1, i) {
            boundaries.push(i);
        }
    }
    boundaries.push(col.len());
    boundaries
}
```

### Cost Analysis

- `concat(&arrays)` — single memcpy. For 100M rows x 8 bytes (Int64) = 800MB, ~100ms
- `slice(offset, length)` — zero-copy (adjusts pointer/offset in ArrayRef)
- `concat(&[null_prefix, sliced])` — copies, another ~800MB, ~100ms
- Total: ~200ms for a single shifted column at 100M rows
- Compare: DataFusion's LAG() takes ~4s for the same operation (R2's `derive(shift) + filter + count`)
- **Expected speedup: 10-20x** for the shift computation itself

### Integration

Modify `try_native_window_derive` in `src/ops/window.rs:246-296`:

```rust
fn try_native_window_derive(
    table: &LTSeqTable, schema: &ArrowSchema, df: &Arc<DataFrame>,
    derived_cols: &Bound<'_, PyDict>,
) -> PyResult<LTSeqTable> {
    // Check if ALL derived columns are pure shift operations and sort order matches
    if let Some(shifts) = extract_pure_shift_operations(derived_cols, schema, &table.sort_exprs)? {
        // Fast path: Arrow-level shift
        return compute_shifts_on_arrow(table, df, shifts);
    }

    // Existing path: build DataFusion Expr::WindowFunction
    // ... (current code at lines 252-295)
}
```

**Eligibility criteria for the Arrow path:**
- Table has `sort_exprs` that match the window's ORDER BY
- ALL derived columns in this `derive()` call are pure shift operations (no rolling, diff, cum_sum)
- If `partition_by` is used, the partition column appears in `sort_exprs` as the first key

**Materialization trade-off:** The Arrow path requires materializing the DataFrame to `Vec<RecordBatch>`. This loses DataFusion's lazy evaluation. However, shift + filter is so much cheaper that total cost is still lower:
- Current: lazy DataFusion plan (shift + filter + count) -> materialize at count -> ~4s
- Proposed: materialize at derive (~0.5s) -> Arrow shift (~0.2s) -> MemTable -> filter + count (~0.5s) -> total ~1.2s

**Files to modify:**
- New `src/ops/arrow_shift.rs` — core shift implementation (~200 lines)
- `src/ops/mod.rs` — add `pub mod arrow_shift;`
- `src/ops/window.rs` — add Arrow fast path in `try_native_window_derive`

**Effort:** 8-12 hours
**Impact:** Expected 5-10x speedup for R2 and R3 shift operations
**Risk:** Medium — edge cases: empty batches, partition boundaries, data types (Int64, Utf8, Timestamp), null handling, Utf8View vs Utf8
**Dependencies:** None (independent of Phase 1)
**Testing:** New `py-ltseq/tests/test_arrow_shift.py` with correctness tests against DataFusion path; benchmark comparison

---

## Phase 3: Custom Linear-Scan `group_ordered` Engine (P2)

**Problem:** Even with Phase 1 (complex expression support) and Phase 2 (Arrow shift), `group_ordered` with shift-based conditions still needs:
1. Compute shift columns via window functions or Arrow shift (~200ms per column)
2. Evaluate the grouping predicate row-by-row
3. Assign group IDs via cumulative sum (another window function)

This is 2-3 passes over the data. But for `group_ordered`, the grouping predicate is by definition sequential — `shift(1)` means "the previous row." We can evaluate this in a single pass.

### Implementation

**New file: `src/ops/linear_scan.rs`**

A mini expression evaluator that handles the subset of `PyExpr` used in grouping conditions:

```rust
/// Single-pass group ID assignment for sequential boundary detection.
///
/// For a predicate like:
///   (r.userid != r.userid.shift(1)) | (r.eventtime - r.eventtime.shift(1) > 1800)
///
/// Walk through rows linearly:
///   - Row 0: boundary = true (first row is always a new group)
///   - Row i: evaluate predicate using row[i] and row[i-1] as "shift(1)"
///   - Increment group_id when boundary = true
///
/// Replaces:
///   - DataFusion LAG() window function (O(n) with sorting overhead)
///   - CASE WHEN change detection (another pass)
///   - SUM() OVER cumulative sum (another pass)
/// With a single O(n) scan, no sorting, no hash tables.

pub fn linear_scan_group_id(
    batches: &[RecordBatch],
    predicate: &PyExpr,
    schema: &ArrowSchema,
) -> Result<(Vec<RecordBatch>, Int64Array), String> {
    // 1. Identify which columns are accessed by the predicate
    let referenced_cols = extract_referenced_columns(predicate);

    // 2. Build typed column accessors for each referenced column
    let accessors = build_accessors(batches, &referenced_cols, schema)?;

    // 3. Single-pass scan
    let total_rows = batches.iter().map(|b| b.num_rows()).sum();
    let mut group_ids = Vec::with_capacity(total_rows);
    let mut current_group_id: i64 = 0;

    for i in 0..total_rows {
        let is_boundary = if i == 0 {
            true  // First row is always a new group
        } else {
            evaluate_boundary(predicate, &accessors, i, i - 1)?
        };

        if is_boundary {
            current_group_id += 1;
        }
        group_ids.push(current_group_id);
    }

    Ok((batches.to_vec(), Int64Array::from(group_ids)))
}
```

**Supported expression subset for the mini-evaluator:**

| PyExpr | Evaluation |
|--------|-----------|
| `Column("userid")` | Read column value at current row index |
| `Call { func: "shift", on: Column("x"), args: [1] }` | Read column value at previous row index |
| `Call { func: "is_null", on: expr }` | Check if evaluated value is null |
| `BinOp { op: "Ne", left, right }` | Compare two evaluated values |
| `BinOp { op: "Or"/"And", left, right }` | Logical operations |
| `BinOp { op: "Sub", left, right }` | Arithmetic (for `eventtime - eventtime.shift(1)`) |
| `BinOp { op: "Gt"/"Lt"/"Ge"/"Le"/"Eq", left, right }` | Comparison |
| `Literal { value, dtype }` | Constant value |
| `UnaryOp { op: "Not", operand }` | Logical not |

This covers R2's sessionization predicate. More complex expressions (nested function calls, string operations) fall back to the Phase 1 approach.

**Integration into `group_id_impl`:**

```rust
// In src/ops/grouping.rs:
pub fn group_id_impl(table: &LTSeqTable, grouping_expr: Bound<'_, PyDict>) -> PyResult<LTSeqTable> {
    let py_expr = dict_to_py_expr(&grouping_expr)?;

    // Fast path: check if the expression can be evaluated by the linear scanner
    if can_linear_scan(&py_expr) {
        return linear_scan_group_id_impl(table, &py_expr);
    }

    // Phase 1 path: native DataFusion expressions (window functions)
    // ... (Phase 1 implementation)
}
```

### Performance Projection

- 100M rows, 2 columns accessed (userid: Int64, eventtime: Int64)
- Single pass: ~200ms (memory bandwidth bound: 100M x 16 bytes = 1.6GB, ~8GB/s)
- Arrow array allocation for group_id column: ~100ms
- Total: ~300ms
- Current (DataFusion LAG path): ~4s
- **Expected speedup: 10-15x**

**Files:**
- New `src/ops/linear_scan.rs` — mini-evaluator + group ID assignment (~300 lines)
- `src/ops/grouping.rs` — add fast path dispatch
- `src/ops/mod.rs` — add module

**Effort:** 12-16 hours
**Risk:** Medium — the mini-evaluator needs careful type handling (Int64, Utf8, Timestamp, Null comparisons)
**Dependencies:** Phase 1 (needs complex expression support as fallback for unsupported expressions)
**Testing:** Validate against DataFusion path on various grouping conditions

---

## Phase 4: Streaming Pattern Matcher for Funnel (P2)

**Problem:** R3 uses `derive(LEAD(url,1), LEAD(url,2)) + filter(starts_with)`. This creates 2 shifted columns for ALL 100M rows, then filters. Most rows don't match step 1 (~99.9%), so the LEAD computation is wasted on non-matching rows.

### New API

```python
count = t.search_pattern(
    lambda r: r.url.s.starts_with("http://liver.ru/saint-peterburg"),
    lambda r: r.url.s.starts_with("http://liver.ru/place_rukodel"),
    lambda r: r.url.s.starts_with("http://liver.ru/belgorod"),
    partition_by="userid"
).count()
```

### Implementation

**New file: `src/ops/pattern_match.rs`**

```rust
/// Single-pass sequential pattern matching.
///
/// For each row i:
///   1. Evaluate step1_predicate(row[i])
///   2. If no match, skip (fast path — ~99.9% of rows)
///   3. If match, check partition boundary (i+1 and i+2 must be same partition)
///   4. Evaluate step2_predicate(row[i+1])
///   5. If match, evaluate step3_predicate(row[i+2])
///   6. If all match, record index i
///
/// Finally, use arrow::compute::take() to extract matching rows.
pub fn search_pattern_impl(
    table: &LTSeqTable,
    step_predicates: Vec<PyExpr>,
    partition_by: Option<String>,
) -> PyResult<LTSeqTable> {
    let batches = materialize(df)?;
    let combined = concat_batches(&batches)?;
    let n = combined.num_rows();
    let num_steps = step_predicates.len();

    // Pre-compute partition boundaries if needed
    let boundaries = partition_by.as_ref()
        .map(|col| find_partition_boundaries(&extract_column(&combined, col)));

    // Build predicate evaluators for each step
    let evaluators: Vec<PredicateEvaluator> = step_predicates.iter()
        .map(|pred| PredicateEvaluator::new(pred, combined.schema()))
        .collect::<Result<_, _>>()?;

    let mut matching_indices = Vec::new();

    for i in 0..n.saturating_sub(num_steps - 1) {
        // Step 1: evaluate first predicate (fast rejection)
        if !evaluators[0].evaluate(&combined, i)? {
            continue;
        }

        // Check partition boundary — all steps must be in same partition
        if let Some(ref bounds) = boundaries {
            if !same_partition(bounds, i, i + num_steps - 1) {
                continue;
            }
        }

        // Steps 2+: evaluate remaining predicates
        let mut all_match = true;
        for (step, evaluator) in evaluators[1..].iter().enumerate() {
            if !evaluator.evaluate(&combined, i + step + 1)? {
                all_match = false;
                break;
            }
        }

        if all_match {
            matching_indices.push(i as u64);
        }
    }

    // Extract matching rows using arrow::compute::take()
    let indices = UInt64Array::from(matching_indices);
    let result_batch = take_record_batch(&combined, &indices)?;
    LTSeqTable::from_batches(session, vec![result_batch], sort_exprs)
}
```

**Predicate evaluation for `starts_with`:** For string predicates like `r.url.s.starts_with(prefix)`, use Arrow's byte-level comparison or the `starts_with` kernel. For the common case of literal prefix matching, this is a simple memcmp — much faster than evaluating through DataFusion's expression engine.

**Python side:**
```python
# In py-ltseq/ltseq/transforms.py:
def search_pattern(self, *step_predicates, partition_by=None):
    """Find rows where consecutive steps match in sequence.

    Each step predicate is a lambda evaluated against successive rows.
    With partition_by, lookahead cannot cross partition boundaries.
    """
    step_dicts = [self._capture_expr(p) for p in step_predicates]
    result_inner = self._inner.search_pattern(step_dicts, partition_by)
    result = LTSeq()
    result._inner = result_inner
    result._schema = self._schema.copy()
    result._sort_keys = self._sort_keys
    return result
```

**Rust method in `src/lib.rs`:**
```rust
fn search_pattern(
    &self,
    step_predicates: Vec<Bound<'_, PyDict>>,
    partition_by: Option<String>,
) -> PyResult<LTSeqTable> {
    crate::ops::pattern_match::search_pattern_impl(self, step_predicates, partition_by)
}
```

**Files:**
- New `src/ops/pattern_match.rs` (~250 lines)
- `src/lib.rs` — add `search_pattern` method
- `src/ops/mod.rs` — add module
- `py-ltseq/ltseq/transforms.py` — add `search_pattern` method
- `benchmarks/bench_vs.py` — add `search_pattern` variant for R3

**Effort:** 8-12 hours
**Impact:** Expected 5-10x speedup for R3
**Risk:** Medium — predicate evaluation on arbitrary expressions; needs type-dispatched evaluators
**Dependencies:** None (independent)
**Testing:** New `py-ltseq/tests/test_pattern_match.py`, validate against derive+filter approach

---

## Phase 5: Engine Configuration Tuning (P3 — Quick Wins)

Small changes, collectively ~5-15% improvement.

### 5A: Batch Size Tuning

**File:** `src/engine.rs:14`
**Change:** Test `DEFAULT_BATCH_SIZE` of 16384 and 32768 for window-heavy workloads. Larger batches reduce per-batch overhead in DataFusion's window executor.
**Action:** Add a benchmark comparing batch sizes on R2/R3, then set optimal default.

### 5B: GIL Release During Rust Execution

**File:** `src/lib.rs` — all methods that call `RUNTIME.block_on()`
**Change:** Wrap with `py.allow_threads(|| RUNTIME.block_on(...))` to release Python GIL during Rust execution. Won't help single-threaded benchmarks but improves multi-threaded Python usage.
**Note:** Requires changing method signatures to accept `Python<'_>` parameter.

### 5C: Eliminate Double Deserialization in derive

**File:** `src/ops/derive.rs:42-55, 74-75`
**Problem:** Expressions are deserialized once for window function detection (lines 48-49) and again for transpilation (lines 74-75).
**Change:** Deserialize once, collect into `Vec<(String, PyExpr)>`, pass to both detection and transpilation paths.

### 5D: Log Native-to-SQL Fallback Reason

**File:** `src/ops/window.rs:238`
**Change:** Currently `Err(_native_err)` silently swallows the error. Add logging:
```rust
Err(native_err) => {
    eprintln!("[ltseq] Native window derive failed, falling back to SQL: {}", native_err);
    derive_with_window_functions_sql_fallback(table, schema, df, derived_cols)
}
```

**Total Effort:** 2-4 hours
**Impact:** 5-15% collectively

---

## Execution Order & Dependencies

```
Phase 0 (Correctness)     ─┐
                            ├─→  Phase 1 (group_ordered expressions)  ─→  Phase 3 (linear scan)
Phase 2 (Arrow shift)      ─┘
Phase 4 (Pattern match)        [independent]
Phase 5 (Config tuning)        [independent]
```

### Recommended Execution Order

| Step | Phase | Effort | Cumulative |
|------|-------|--------|-----------|
| 1 | Phase 0: Fix R3 discrepancy | 2-4h | 2-4h |
| 2 | Phase 5: Quick config wins | 2-4h | 4-8h |
| 3 | Phase 2: Arrow shift bypass | 8-12h | 12-20h |
| 4 | Phase 1: group_ordered expressions | 6h | 18-26h |
| 5 | Phase 4: Pattern matcher | 8-12h | 26-38h |
| 6 | Phase 3: Linear scan group | 12-16h | 38-54h |

### Projected Results After All Phases (100M rows)

| Round | Current | After Phase 2 | After Phase 3/4 | DuckDB |
|-------|---------|---------------|-----------------|--------|
| R1: Top URLs | 1.99s | 1.99s | 1.99s | 2.18s |
| R2: Sessionization | 4.44s | ~1.5s | ~0.5s | 1.17s |
| R3: Funnel | 10.39s | ~3.0s | ~1.0s | 3.47s |

**Target ratios after all phases:**

| Round | Target |
|-------|--------|
| R1 | 1.1x LT win (unchanged) |
| R2 | 2-3x LT win |
| R3 | 2-3x LT win |

---

## Validation Commands

```bash
# Build
source .venv/bin/activate && maturin develop --release

# Run all tests
pytest py-ltseq/tests/ -v

# Run benchmark (sample)
python benchmarks/bench_vs.py --sample

# Run benchmark (full)
python benchmarks/bench_vs.py

# Run specific round
python benchmarks/bench_vs.py --sample --round 2
```
