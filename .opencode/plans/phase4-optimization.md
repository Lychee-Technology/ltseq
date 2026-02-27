# Phase 4: ORDER BY Dedup + Python Bug Fix

## Results

### 4A: Column Pruning — DISPROVED

Hypothesis was that DataFusion wasn't pruning unused columns through window functions.
Testing proved DataFusion DOES prune columns correctly — adding explicit `.select()` 
before `.derive()` had zero impact (3.985s vs 3.981s). **Reverted.**

Profiling revealed the actual bottleneck:
- `derive(shift) + count`: 0.002s — DataFusion optimizes count() to skip window computation
- `derive(shift) + filter + count`: ~4.0s — The filter forces full window materialization
- The ~4s is the genuine cost of computing LAG over 100M rows with PARTITION BY

### 4B: ORDER BY Dedup — IMPLEMENTED

Added `peek_partition_by_cols()` helper in `window_native.rs` to filter out sort 
expressions that duplicate PARTITION BY columns. This is a correctness improvement
(removes redundant work) even if the observable benchmark impact is within noise.

### 4C: Python `_has_window_functions` Bug Fix — IMPLEMENTED

- Fixed `expr.get("method", "")` → `expr.get("func", "")` in transforms.py
- Added `"cum_sum"` to the window function detection list
- Fixed `derive_with_window_functions()` call to match Rust's 1-arg signature
  (previously passed 2 args but was never reached due to the bug)

## Benchmark Numbers (After Phase 4)

| Round | DuckDB (s) | LTSeq (s) | Ratio | vs Baseline |
|-------|-----------|-----------|-------|-------------|
| R1: Top URLs | 2.18 | 1.99 | **1.1x LT** | **5.6x faster** |
| R2: Sessionization | 1.17 | 4.44 | 3.8x DK | **87.9x faster** |
| R3: Funnel | 3.47 | 10.39 | 3.0x DK | **45.5x faster** |

## Conclusion

The remaining 3-4x gap in R2/R3 is a **DataFusion window function ceiling**, not an
LTSeq overhead issue. DataFusion's window function implementation over 100M rows is
genuinely ~3x slower than DuckDB's for this workload. All LTSeq overhead has been
eliminated — the pipeline is fully lazy with a single materialization at `count()`.

### Evidence:
- Column pruning is working (`.select()` before `.derive()` has zero impact)
- No intermediate `collect()` calls in the pipeline
- The entire cost is in DataFusion's window function execution during `filter`

### Potential Future Improvements (DataFusion-level):
- DataFusion's window function parallelism across partitions
- Partition pruning (skip partitions where base filter can't match)
- Predicate pushdown through window functions
- These would require DataFusion upstream changes or custom physical plan operators
