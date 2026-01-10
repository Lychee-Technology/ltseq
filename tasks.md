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
| **read_csv has_header** | `read_csv(path, has_header=True)` | Implemented - `has_header` parameter added | Medium |
| **group_sorted** | `group_sorted(key)` | Not implemented - one-pass grouping on pre-sorted data | High |
| **scan (stateful)** | `scan(func, init)` | Not implemented - stateful scan/iterate | High |
| **align** | `align(ref_sequence, key)` | Not implemented - align to reference sequence | Medium |
| **asof_join** | `asof_join(other, on, direction)` | Implemented - time-series as-of joins | High |
| **join_sorted** | `join_sorted(other, on, how)` | Not implemented (alias for join_merge) | Low |
| **lookup (table-level)** | `LTSeq.lookup(dim_table, on, as_)` | Partially implemented (Expr.lookup exists, not table-level) | Medium |
| **top_k** | `top_k(col, k)` | Not implemented - aggregate function for Top-K | Low |
| **Expr.lookup** | `r.key.lookup(target_table, column, join_key)` | Expression defined but not fully wired to execution | Medium |

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
- [ ] Add `scan(func, init)` method to `LTSeq`
- [ ] Create expression serialization for state transition function
- [ ] Implement in Rust transpiler to generate SQL with window functions or custom UDF
- [ ] Handle state serialization/deserialization
- [ ] Add tests

**Files**: `py-ltseq/ltseq/core.py`, `src/transpiler.rs`, `src/ops/window.rs`

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
- [ ] Add `align()` method to `LTSeq`
- [ ] Create a reference sequence from the provided list
- [ ] Insert NULL rows for missing keys
- [ ] Reorder to match reference sequence
- [ ] Add tests

**Files**: `py-ltseq/ltseq/core.py`, `src/ops/advanced.rs`

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
- [ ] Implement transpilation in `src/transpiler.rs`
- [ ] Generate join SQL for the lookup expression
- [ ] Add tests

**Files**: `src/transpiler.rs`

---

### Phase 3: Enhancement Features (Low Priority)

#### 3.1 `join_sorted(other, on, how)` - Alias for Merge Join
- [ ] Add as an alias for `join_merge()` with same semantics
- [ ] Validate inputs are sorted
- [ ] Add tests

**Files**: `py-ltseq/ltseq/core.py`

#### 3.2 `top_k(col, k)` - Top-K Aggregate
- [ ] Add `top_k` function to expression system
- [ ] Implement as aggregate inside `agg()`
- [ ] Return array/list of top K values
- [ ] Add tests

**Files**: `py-ltseq/ltseq/expr/`, `src/transpiler.rs`

#### 3.3 `GroupProxy.all/any/none` - Quantifier Functions
- [x] Add `all()`, `any()`, `none()` methods to GroupProxy in `grouping.py`
- [x] Extend SQL parsing in `_ast_call_to_sql()` for quantifier functions
- [x] Add helper method `_ast_predicate_to_sql()` for inner predicate parsing
- [x] Add tests in `tests/test_nested_quantifiers.py`

**Files**: `py-ltseq/ltseq/grouping.py`, `py-ltseq/tests/test_nested_quantifiers.py`
**Status**: COMPLETED

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

---

## Progress Tracking

- [x] Phase 1.1: `group_sorted` - **COMPLETED**
- [ ] Phase 1.2: `scan` (stateful)
- [x] Phase 1.3: `asof_join` - **COMPLETED**
- [ ] Phase 2.1: `align`
- [x] Phase 2.2: `read_csv has_header` - **COMPLETED**
- [ ] Phase 2.3: `LTSeq.lookup`
- [ ] Phase 2.4: Wire `Expr.lookup`
- [ ] Phase 3.1: `join_sorted`
- [ ] Phase 3.2: `top_k`
- [x] Phase 3.3: `GroupProxy.all/any/none` - **COMPLETED**
