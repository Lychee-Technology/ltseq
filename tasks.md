# LTSeq Implementation Tasks

## Gap Analysis: API Spec vs Current Implementation

### Fully Implemented

| Feature | API Method | Status |
|---------|-----------|--------|
| **I/O** | `read_csv`, `write_csv`, `scan`, `scan_parquet`, `to_cursor` | Complete |
| **Basic Ops** | `filter`, `select`, `derive`, `sort`, `distinct`, `slice` | Complete |
| **Window Funcs** | `shift`, `rolling().mean/sum/min/max/count/std`, `diff`, `cum_sum` | Complete |
| **Ordered Grouping** | `group_ordered` -> `NestedTable` | Complete |
| **NestedTable** | `first`, `last`, `count`, `flatten`, `filter`, `derive` | Complete |
| **GroupProxy** | `count()`, `first()`, `last()`, `col.sum/avg/min/max()`, `all()`, `any()`, `none()` | Complete |
| **Set Algebra** | `union`, `intersect`, `diff`, `is_subset` | Complete |
| **Joins** | `join`, `join_merge`, `link` | Complete |
| **Aggregation** | `agg(by=, **aggregations)` | Complete |
| **Partitioning** | `partition` -> `PartitionedTable` | Complete |
| **Pivot** | `pivot(index, columns, values, agg_fn)` | Complete |
| **Search** | `search_first` | Complete |
| **Expression Ops** | `+`, `-`, `*`, `/`, `//`, `%`, `==`, `!=`, `>`, `>=`, `<`, `<=`, `&`, `|`, `~` | Complete |
| **Null Handling** | `is_null`, `is_not_null`, `fill_null` | Complete |
| **String Ops** | `s.contains`, `s.starts_with`, `s.ends_with`, `s.lower`, `s.upper`, `s.strip`, `s.len`, `s.slice`, `s.regex_match` | Complete |
| **Temporal Ops** | `dt.year/month/day/hour/minute/second`, `dt.add`, `dt.diff` | Complete |
| **Conditional** | `if_else` | Complete |

### Missing or Partially Implemented

| Feature | API Method | Gap | Priority |
|---------|-----------|-----|----------|
| **read_csv has_header** | `read_csv(path, has_header=True)` | **IMPLEMENTED** - `has_header` parameter added | Medium |
| **group_sorted** | `group_sorted(key)` | **IMPLEMENTED** - one-pass grouping on pre-sorted data | High |
| **scan (stateful)** | `scan(func, init)` | **IMPLEMENTED** as `stateful_scan()` - Python-based stateful iteration | High |
| **align** | `align(ref_sequence, key)` | **IMPLEMENTED** - align to reference sequence | Medium |
| **asof_join** | `asof_join(other, on, direction)` | **IMPLEMENTED** - time-series as-of joins | High |
| **join_sorted** | `join_sorted(other, on, how)` | **IMPLEMENTED** - merge join with sort validation | Low |
| **lookup (table-level)** | `LTSeq.lookup(dim_table, on, as_)` | Partially implemented (Expr.lookup exists, not table-level) | Medium |
| **top_k** | `top_k(col, k)` | **IMPLEMENTED** - aggregate returning semicolon-delimited top K values | Low |
| **Expr.lookup** | `r.key.lookup(target_table, column, join_key)` | **IMPLEMENTED** - Expression wired to execution via Python-side join rewriting | Medium |

---

## Implementation Plan

### Phase 1: Core Missing Features (High Priority)

#### 1.1 `group_sorted(key)` - One-pass grouping
- [x] Add `group_sorted()` method to `LTSeq` in `core.py`
- [x] Validate data is sorted by the grouping key (optional warning)
- [x] Reuse `NestedTable` infrastructure from `group_ordered`
- [x] Add tests in `tests/test_group_sorted.py`

**Files**: `py-ltseq/ltseq/core.py`, `py-ltseq/tests/test_group_sorted.py`
**Status**: COMPLETED

#### 1.2 `scan(func, init)` - Stateful Scan/Iterate
- [x] Add `stateful_scan(func, init, output_col)` method to `LTSeq`
- [x] Implement Python-based row-by-row state iteration
- [x] Handle empty tables and single-row tables
- [x] Preserve sort keys after scan
- [x] Add comprehensive tests in `tests/test_stateful_scan.py`

**Implementation Notes:**
- Implemented as Python-side iteration (not Rust transpiled) for maximum flexibility
- Uses pandas DataFrame for row iteration
- State can be any Python type (int, float, bool, string)
- CSV round-trip may convert types to strings; subsequent derives should account for this
- Named `stateful_scan()` to avoid conflict with existing `scan()` (file streaming)

**Files**: `py-ltseq/ltseq/core.py`, `py-ltseq/tests/test_stateful_scan.py`
**Status**: COMPLETED

#### 1.3 `asof_join(other, on, direction)` - Time-Series As-Of Join
- [x] Add `asof_join()` method to `LTSeq`
- [x] Implement binary search logic in Rust for sorted inputs
- [x] Support `direction` parameter: "backward", "forward", "nearest"
- [x] Add tests

**Files**: `py-ltseq/ltseq/core.py`, `src/lib.rs`, `src/ops/advanced.rs`
**Status**: COMPLETED

---

### Phase 2: Secondary Features (Medium Priority)

#### 2.1 `align(ref_sequence, key)` - Sequence Alignment
- [x] Add `align()` method to `LTSeq`
- [x] Create a reference sequence from the provided list
- [x] Insert NULL rows for missing keys
- [x] Reorder to match reference sequence
- [x] Add tests in `tests/test_align.py`

**Files**: `py-ltseq/ltseq/core.py`, `src/lib.rs`, `src/ops/advanced.rs`, `py-ltseq/tests/test_align.py`
**Status**: COMPLETED

#### 2.2 `read_csv(path, has_header=True)` - Header Parameter
- [x] Add `has_header` parameter to `read_csv()`
- [x] Add `has_header` parameter to `scan()`
- [x] Pass to Rust `read_csv` and `scan_csv` implementations
- [x] Update schema inference logic for headerless CSVs
- [x] Add tests in `tests/test_has_header.py`

**Files**: `py-ltseq/ltseq/core.py`, `py-ltseq/ltseq/helpers.py`, `src/lib.rs`, `src/cursor.rs`
**Status**: COMPLETED

#### 2.3 `LTSeq.lookup(dim_table, on, as_)` - Table-Level Lookup
- [ ] Implement as optimized hash-based lookup (different from `link()`)
- [ ] Load dim table into memory for direct index access
- [ ] Store index in Rust for fast access
- [ ] Add tests

**Files**: `py-ltseq/ltseq/core.py`, `src/lib.rs`, `src/ops/advanced.rs`

#### 2.4 Wire `Expr.lookup` to Execution
- [x] Store target table reference in `LookupExpr` class
- [x] Add table registry to `LookupExpr` for retrieval after serialization
- [x] Add `_contains_lookup()` helper to detect lookup expressions in derive
- [x] Add `_extract_lookup_info()` to extract lookup metadata from expression
- [x] Add `_resolve_lookups()` to perform LEFT JOIN and rewrite expressions
- [x] Add `_perform_lookup_join()` to execute the join via Rust
- [x] Set `_name` on LTSeq tables from CSV file basename
- [x] Add integration tests for lookup execution

**Implementation Notes:**
- Implemented via Python-side join rewriting (not Rust transpiler modification)
- Lookups are detected in `derive()` before calling Rust
- LEFT JOIN brings in lookup columns; expression is rewritten to reference joined columns
- Unmatched keys return NULL values
- Table registry tracks table references by name for retrieval after serialization

**Files**: `py-ltseq/ltseq/transforms.py`, `py-ltseq/ltseq/expr/types.py`, `py-ltseq/ltseq/expr/base.py`, `py-ltseq/ltseq/io_ops.py`, `py-ltseq/ltseq/core.py`, `py-ltseq/tests/test_lookup.py`
**Status**: COMPLETED

---

### Phase 3: Enhancement Features (Low Priority)

#### 3.1 `join_sorted(other, on, how)` - Merge Join with Sort Validation
- [x] Add `join_sorted()` method that delegates to `join_merge()`
- [x] Validate inputs are sorted using `is_sorted_by()` (strict validation)
- [x] Support composite keys (multiple join columns)
- [x] Validate sort directions match (both ASC or both DESC)
- [x] Add `sort_keys` property and `is_sorted_by()` method for sort order tracking
- [x] Add tests in `tests/test_sort_tracking.py` and `tests/test_join_sorted.py`

**Files**: `py-ltseq/ltseq/core.py`, `py-ltseq/tests/test_sort_tracking.py`, `py-ltseq/tests/test_join_sorted.py`
**Status**: COMPLETED

#### 3.2 `top_k(col, k)` - Top-K Aggregate
- [x] Add `top_k` function to expression system (works automatically via CallExpr)
- [x] Implement in Rust `agg()` using ARRAY_AGG + ARRAY_SLICE + ARRAY_TO_STRING
- [x] Add `top_k()` method to GroupProxy for Python-side evaluation
- [x] Add tests in `tests/test_top_k.py`

**Implementation Notes:**
- Returns values as semicolon-delimited string (due to CSV round-trip in `to_pandas()`)
- Use `parse_top_k(value)` helper to convert string to list of floats
- Values are sorted in descending order (highest first)
- Default k=10 if not specified

**Files**: `src/ops/aggregation.rs`, `py-ltseq/ltseq/grouping/proxies.py`, `py-ltseq/tests/test_top_k.py`
**Status**: COMPLETED

#### 3.3 `GroupProxy.all/any/none` - Quantifier Functions
- [x] Add `all()`, `any()`, `none()` methods to GroupProxy in `grouping.py`
- [x] Extend SQL parsing in `_ast_call_to_sql()` for quantifier functions
- [x] Add helper method `_ast_predicate_to_sql()` for inner predicate parsing
- [x] Add tests in `tests/test_nested_quantifiers.py`

**Files**: `py-ltseq/ltseq/grouping.py`, `py-ltseq/tests/test_nested_quantifiers.py`
**Status**: COMPLETED

---

### Phase 4: Extended Features

#### 4.1 Statistical Aggregations
- [x] Add `median()` aggregate function
- [x] Add `percentile(p)` aggregate function (e.g., `percentile(0.95)`)
- [x] Add `variance()` / `var()` aggregate function
- [x] Add `std()` / `stddev()` (standard deviation) aggregate function
- [x] Add `mode()` aggregate function (most frequent value) - GroupProxy only
- [x] Add tests in `tests/test_statistical_aggs.py`

**Implementation Notes:**
- `median()`: Uses DataFusion `MEDIAN()` function
- `percentile(p)`: Uses DataFusion `APPROX_PERCENTILE_CONT(col, p)`
- `variance()` / `var()`: Uses DataFusion `VAR_SAMP()` (sample variance)
- `std()` / `stddev()`: Uses DataFusion `STDDEV_SAMP()` (sample standard deviation)
- `mode()`: Implemented in GroupProxy only (Python-side) using `statistics.mode()`
- All functions also available via GroupProxy for Python-side evaluation

**Files**: `src/ops/aggregation.rs`, `py-ltseq/ltseq/grouping/proxies.py`, `py-ltseq/tests/test_statistical_aggs.py`
**Status**: COMPLETED

#### 4.2 Row Numbering/Ranking Functions
- [ ] Add `row_number()` window function
- [ ] Add `rank()` window function (with gaps)
- [ ] Add `dense_rank()` window function (without gaps)
- [ ] Add `ntile(n)` window function (bucket assignment)
- [ ] Add tests in `tests/test_ranking.py`

**Implementation Notes:**
- All are standard SQL window functions supported by DataFusion
- Need to extend expression system to capture OVER clause with PARTITION BY and ORDER BY
- Consider adding as `derive()` expressions: `lambda r: r.col.row_number().over(partition_by="group", order_by="date")`

**Files**: `src/ops/window.rs`, `py-ltseq/ltseq/expr/types.py`, `py-ltseq/tests/test_ranking.py`
**Status**: NOT STARTED

#### 4.3 Conditional Aggregations
- [x] Add `count_if(predicate)` aggregate function
- [x] Add `sum_if(predicate, col)` aggregate function
- [x] Add `avg_if(predicate, col)` aggregate function
- [x] Add `min_if(predicate, col)` aggregate function
- [x] Add `max_if(predicate, col)` aggregate function
- [x] Add tests in `tests/test_conditional_aggs.py`

**Implementation Notes:**
- Implemented via `SUM(CASE WHEN pred THEN 1 ELSE 0 END)` for count_if
- Implemented via `SUM(CASE WHEN pred THEN col ELSE 0 END)` for sum_if
- Implemented via `AVG(CASE WHEN pred THEN col ELSE NULL END)` for avg_if
- Implemented via `MIN(CASE WHEN pred THEN col ELSE NULL END)` for min_if
- Implemented via `MAX(CASE WHEN pred THEN col ELSE NULL END)` for max_if
- Helper functions exported from `ltseq.expr` and top-level `ltseq` module
- Also available via GroupProxy methods for Python-side evaluation

**Files**: `src/ops/aggregation.rs`, `py-ltseq/ltseq/expr/base.py`, `py-ltseq/ltseq/grouping/proxies.py`, `py-ltseq/tests/test_conditional_aggs.py`
**Status**: COMPLETED

#### 4.4 Window Default Values
- [x] Extend `shift(n)` to accept `default=value` parameter
- [x] Handle NULL values at boundaries with user-specified defaults
- [x] Add tests in `tests/test_shift_default.py`

**Implementation Notes:**
- SQL: `LAG(col, n, default_value)` or `LEAD(col, n, default_value)`
- Default value is passed through kwargs in expression serialization
- Works with numeric and string defaults
- Note: String defaults like "N/A", "NA", "NULL", "None", "NaN" are interpreted as missing values by pandas during CSV round-trip
- Integer division with integer columns truncates; use float columns for decimal results

**Files**: `src/transpiler/sql_gen.rs`, `py-ltseq/tests/test_shift_default.py`
**Status**: COMPLETED

#### 4.5 Additional String Operations
- [ ] Add `s.replace(old, new)` - replace substring
- [ ] Add `s.split(delimiter)` - split string into array
- [ ] Add `s.concat(*others)` - concatenate strings
- [ ] Add `s.pad_left(width, char)` - left pad string
- [ ] Add `s.pad_right(width, char)` - right pad string
- [ ] Add tests in `tests/test_string_ops_extended.py`

**Implementation Notes:**
- DataFusion: `REPLACE(str, from, to)`, `SPLIT_PART(str, delim, n)`, `CONCAT(...)`, `LPAD(str, len, pad)`, `RPAD(str, len, pad)`
- Split may need special handling for array return (semicolon-delimited like top_k)

**Files**: `src/ops/expressions.rs`, `py-ltseq/ltseq/expr/string.py`, `py-ltseq/tests/test_string_ops_extended.py`
**Status**: NOT STARTED

#### 4.6 Additional Temporal Operations
- [ ] Add `dt.week()` - ISO week number
- [ ] Add `dt.quarter()` - quarter of year (1-4)
- [ ] Add `dt.day_of_week()` - day of week (0-6 or 1-7)
- [ ] Add `dt.day_of_year()` - day of year (1-366)
- [ ] Add tests in `tests/test_temporal_ops_extended.py`

**Implementation Notes:**
- DataFusion: `EXTRACT(WEEK FROM date)`, `EXTRACT(QUARTER FROM date)`, `EXTRACT(DOW FROM date)`, `EXTRACT(DOY FROM date)`
- Extend existing `DateTimeAccessor` in expression system

**Files**: `src/ops/expressions.rs`, `py-ltseq/ltseq/expr/temporal.py`, `py-ltseq/tests/test_temporal_ops_extended.py`
**Status**: NOT STARTED

#### 4.7 Unpivot/Melt Operation
- [ ] Add `unpivot(id_cols, value_cols, var_name, value_name)` method
- [ ] Transform wide format to long format (inverse of pivot)
- [ ] Add tests in `tests/test_unpivot.py`

**Implementation Notes:**
- Example: columns `[id, jan, feb, mar]` -> `[id, month, value]`
- Can implement via UNION ALL of multiple selects
- Or use DataFusion's UNPIVOT if available

**Files**: `py-ltseq/ltseq/core.py`, `src/lib.rs`, `py-ltseq/tests/test_unpivot.py`
**Status**: NOT STARTED

#### 4.8 Sampling Operations
- [ ] Add `sample(n)` - random sample of n rows
- [ ] Add `sample_fraction(p)` - random sample of p% of rows
- [ ] Add optional `seed` parameter for reproducibility
- [ ] Add tests in `tests/test_sampling.py`

**Implementation Notes:**
- Can implement via `ORDER BY RANDOM() LIMIT n`
- For fraction: `WHERE RANDOM() < p`
- Seed support may require custom random function

**Files**: `py-ltseq/ltseq/core.py`, `src/lib.rs`, `py-ltseq/tests/test_sampling.py`
**Status**: NOT STARTED

#### 4.9 Semi/Anti Joins
- [ ] Add `semi_join(other, on)` - rows in left that have match in right
- [ ] Add `anti_join(other, on)` - rows in left that have NO match in right
- [ ] Add tests in `tests/test_semi_anti_joins.py`

**Implementation Notes:**
- Semi-join: `WHERE EXISTS (SELECT 1 FROM right WHERE left.key = right.key)`
- Anti-join: `WHERE NOT EXISTS (SELECT 1 FROM right WHERE left.key = right.key)`
- Or implement via LEFT JOIN + filter on NULL/NOT NULL

**Files**: `py-ltseq/ltseq/core.py`, `src/lib.rs`, `src/ops/join.rs`, `py-ltseq/tests/test_semi_anti_joins.py`
**Status**: NOT STARTED

---

## Effort Estimates

| Phase | Feature | Effort | Priority |
|-------|---------|--------|----------|
| 1.1 | `group_sorted` | 2-3 days | High |
| 1.2 | `scan` (stateful) | 4-5 days | High |
| 1.3 | `asof_join` | 4-5 days | High |
| 2.1 | `align` | 2-3 days | Medium |
| 2.2 | `read_csv has_header` | 1 day | Medium |
| 2.3 | `LTSeq.lookup` (table-level) | 2-3 days | Medium |
| 2.4 | Wire `Expr.lookup` | 2 days | Medium |
| 3.1 | `join_sorted` alias | 0.5 day | Low |
| 3.2 | `top_k` | 2 days | Low |
| 3.3 | `GroupProxy.all/any/none` | 1-2 days | Low |
| 4.1 | Statistical Aggregations | 2-3 days | Medium |
| 4.2 | Row Numbering/Ranking | 2-3 days | Medium |
| 4.3 | Conditional Aggregations | 1-2 days | Medium |
| 4.4 | Window Default Values | 1 day | Low |
| 4.5 | Additional String Ops | 2-3 days | Low |
| 4.6 | Additional Temporal Ops | 1-2 days | Low |
| 4.7 | Unpivot/Melt | 1-2 days | Low |
| 4.8 | Sample/Random | 1 day | Low |
| 4.9 | Semi/Anti Joins | 1-2 days | Medium |

---

## Progress Tracking

### Phase 1-3 (Core Features)
- [x] Phase 1.1: `group_sorted` - **COMPLETED**
- [x] Phase 1.2: `scan` (stateful) - **COMPLETED** (as `stateful_scan()`)
- [x] Phase 1.3: `asof_join` - **COMPLETED**
- [x] Phase 2.1: `align` - **COMPLETED**
- [x] Phase 2.2: `read_csv has_header` - **COMPLETED**
- [ ] Phase 2.3: `LTSeq.lookup` - ON HOLD
- [x] Phase 2.4: Wire `Expr.lookup` - **COMPLETED**
- [x] Phase 3.1: `join_sorted` - **COMPLETED**
- [x] Phase 3.2: `top_k` - **COMPLETED**
- [x] Phase 3.3: `GroupProxy.all/any/none` - **COMPLETED**

### Phase 4 (Extended Features)
- [x] Phase 4.1: Statistical Aggregations (`median`, `percentile`, `variance`, `std`, `mode`) - **COMPLETED**
- [ ] Phase 4.2: Row Numbering/Ranking (`row_number`, `rank`, `dense_rank`, `ntile`)
- [x] Phase 4.3: Conditional Aggregations (`count_if`, `sum_if`, `avg_if`, `min_if`, `max_if`) - **COMPLETED**
- [x] Phase 4.4: Window Default Values (`shift(n, default=value)`) - **COMPLETED**
- [ ] Phase 4.5: Additional String Ops (`replace`, `split`, `concat`, `pad_left/right`)
- [ ] Phase 4.6: Additional Temporal Ops (`week`, `quarter`, `day_of_week`, `day_of_year`)
- [ ] Phase 4.7: Unpivot/Melt (wide-to-long transformation)
- [ ] Phase 4.8: Sample/Random (`sample(n)`, `sample_fraction(p)`)
- [ ] Phase 4.9: Semi/Anti Joins (`semi_join`, `anti_join`)
