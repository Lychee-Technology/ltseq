# Phase B & Phase 3 Architecture Decisions

**Date:** January 2025  
**Status:** Approved for Implementation  
**Priority:** Phase B (DSL Showcase) → Phase 3 Rust Optimization

---

## Executive Summary

This document captures strategic decisions made to optimize the LTSeq API implementation:

1. **Phase Priority:** Focus on Phase B (DSL Showcase) before Phase 3 Rust optimization
2. **Partition Implementation:** Strategy A (Option A) selected for `partition()`
3. **NestedTable Operations:** Enhanced filter/derive with SQL window functions
4. **Test Coverage:** 90% minimum for all new implementations
5. **Error Handling:** Detailed error messages for AST parsing failures

---

## 1. Implementation Priority: Phase B First

### Decision
Implement Phase B (DSL Showcase) before Phase A (Rust Optimization).

### Rationale
- **Critical Path:** The DSL showcase example is the primary demonstration of LTSeq's unique capabilities
- **Blocking Feature:** Phase B operations are required for showcase to work
- **User Value:** Completing the showcase provides immediate value to users/stakeholders
- **Technical Dependency:** Phase A optimization doesn't block Phase B (pandas implementations work fine)

### Phase Order
```
Priority 1 (Week 1-2): Phase B - DSL Showcase
├── B.1: NestedTable.filter() - Complex predicates
├── B.2: NestedTable.derive() - Group property broadcasting
└── Validation: DSL showcase example end-to-end

Priority 2 (Week 3-4): Phase A - Rust Optimization
├── A.2: join_merge() via existing Rust join_impl
├── A.1: search_first() Rust implementation
├── A.4: partition() Rust optimization
└── A.3: pivot() Rust implementation
```

---

## 2. Partition Implementation: Strategy A

### Decision
**Option A:** Keep Python dict-like access, optimize with Rust filter calls.

### Alternatives Considered

#### Option B: Rust-backed partitioned structure
- **Pros:** Single Rust call, most efficient for large datasets
- **Cons:** Complex (new Rust struct), requires new protocol, higher development effort
- **Status:** Rejected (deferred to Phase A optimization if needed)

#### Option C: Keep pandas implementation unchanged
- **Pros:** Already works, no development time
- **Cons:** Slower for large datasets, data flows through Python
- **Status:** Rejected (Option A provides better performance with minimal effort)

### Option A Implementation Details

**Architecture:**
```
Python API (PartitionedTable)
    ↓
Get unique partition keys via Rust
    ↓
For each key, call Rust filter()
    ↓
Cache results in Python dict
    ↓
Return dict-like access
```

**Performance:**
- `get_distinct_keys()`: Single Rust SELECT DISTINCT (O(n))
- Per-partition access: Rust filter call (O(n)), cached
- Total: O(n × k) where k = number of partitions (vs. O(n) for Option B)

**Trade-offs:**
- Simple to implement (reuses existing `filter()`)
- Maintains exact current API
- Multiple Rust calls acceptable for typical use cases (k usually small)
- Can upgrade to Option B later if partitions > 100+

**Implementation Steps:**
1. Add `get_distinct_keys()` to Rust (2-3 lines, uses SQL SELECT DISTINCT)
2. Modify `PartitionedTable._materialize_partitions()` to:
   - Call `get_distinct_keys()` to get list of keys
   - For each key, call `ltseq.filter(lambda r: r.col == key)`
   - Cache filtered results
3. No changes to Python API needed

---

## 3. NestedTable Operations Architecture

### 3.1 NestedTable.filter() - Complex Predicates

**Current State:**
- Only supports: `g.count() > N` pattern
- Uses AST parsing to extract condition
- Falls back to returning flattened table if AST parse fails

**Target State:**
- Support: `g.count() > N` ✅
- Support: `g.first().col == value`
- Support: `g.last().col < value`
- Support: Combined predicates `(g.count() > N) & (g.first().col == value)`
- Detailed error messages for unsupported patterns

**Implementation Strategy:**

1. **AST Parser Enhancement** (`py-ltseq/ltseq/grouping.py`):
   - Enhance `_extract_filter_condition()` to detect:
     - `g.count() op N` → `__group_count__ op N`
     - `g.first().col op value` → `FIRST_VALUE(col) OVER (...) op value`
     - `g.last().col op value` → `LAST_VALUE(col) OVER (...) op value`
     - Binary operations: `(cond1) & (cond2)` → combine with AND
   - Return detailed error messages for unsupported patterns

2. **Rust Implementation** (`src/ops/advanced.rs`):
   - New function: `filter_group_predicate_impl()`
   - Build SQL with window functions:
   ```sql
   WITH group_props AS (
       SELECT *,
           COUNT(*) OVER (PARTITION BY __group_id__) as __group_count__,
           FIRST_VALUE(col) OVER (PARTITION BY __group_id__ ORDER BY __rn__) as __first_col__,
           LAST_VALUE(col) OVER (PARTITION BY __group_id__ ORDER BY __rn__ 
               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as __last_col__
       FROM t
   )
   SELECT * FROM group_props WHERE {predicate}
   ```
   - Return filtered RustTable

3. **Python Integration** (`py-ltseq/ltseq/grouping.py`):
   - Replace current fallback with call to Rust `filter_group_predicate()`
   - Pass predicate dict extracted from AST
   - Return result as LTSeq

**Unsupported Patterns (Return Error):**
- Predicates without g. prefix
- Unsupported group functions (e.g., `g.median()`)
- Complex arithmetic beyond simple comparisons
- Function calls other than g.count/first/last/max/min/sum/avg

**Error Message Example:**
```
ValueError: Unsupported filter predicate pattern: 'g.median() > 100'
Supported patterns:
  - g.count() > N
  - g.first().column == value
  - g.last().column < value
  - g.max('column') >= value
  - g.min('column') <= value
  - Combinations: (g.count() > N) & (g.first().col == value)

Got: lambda g: g.median() > 100
```

---

### 3.2 NestedTable.derive() - Group Property Broadcasting

**Current State:**
- Returns flattened table without derived columns (placeholder)
- No group properties are computed

**Target State:**
- Broadcast `g.count()` to all rows in group
- Broadcast `g.first().col` to all rows in group
- Broadcast `g.last().col` to all rows in group
- Support arithmetic: `g.last().col - g.first().col`
- Detailed error messages for unsupported patterns

**Implementation Strategy:**

1. **AST Parser Enhancement** (`py-ltseq/ltseq/grouping.py`):
   - Add `_extract_derive_expressions()` to parse derive lambda
   - Extract column mappings from dict return:
     ```python
     derive(lambda g: {
         "span": g.count(),
         "start": g.first().date,
         "end": g.last().date,
         "gain": (g.last().price - g.first().price) / g.first().price
     })
     ```
   - Build window function SQL for each derived column
   - Return detailed errors for unsupported expressions

2. **Rust Implementation** (`src/ops/advanced.rs`):
   - New function: `derive_group_columns_impl()`
   - Build SQL with window functions:
   ```sql
   SELECT *,
       COUNT(*) OVER (PARTITION BY __group_id__) AS span,
       FIRST_VALUE(date) OVER (PARTITION BY __group_id__ ORDER BY __rn__) AS start,
       LAST_VALUE(date) OVER (PARTITION BY __group_id__ ORDER BY __rn__ 
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end,
       (LAST_VALUE(price) OVER (...) - FIRST_VALUE(price) OVER (...)) / 
           FIRST_VALUE(price) OVER (...) AS gain
   FROM t
   ```
   - Return RustTable with new columns added

3. **Python Integration** (`py-ltseq/ltseq/grouping.py`):
   - Replace current placeholder with call to Rust `derive_group_columns()`
   - Pass derive expressions dict extracted from AST
   - Return result as LTSeq with new columns

**Supported Expressions:**
- `g.count()` → COUNT(*) OVER (PARTITION BY __group_id__)
- `g.first().col` → FIRST_VALUE(col) OVER (PARTITION BY __group_id__ ORDER BY __rn__)
- `g.last().col` → LAST_VALUE(col) OVER (PARTITION BY __group_id__ ORDER BY __rn__ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
- `g.max('col')` → MAX(col) OVER (PARTITION BY __group_id__)
- `g.min('col')` → MIN(col) OVER (PARTITION BY __group_id__)
- `g.sum('col')` → SUM(col) OVER (PARTITION BY __group_id__)
- `g.avg('col')` → AVG(col) OVER (PARTITION BY __group_id__)
- Arithmetic: `(expr1) - (expr2)`, `(expr1) / (expr2)`, etc.

**Unsupported Patterns (Return Error):**
- Functions not in supported list
- Row-level operations (can't use g.filter, g.select, etc.)
- Complex nested expressions
- Predicates instead of value expressions

**Error Message Example:**
```
ValueError: Unsupported derive expression: 'g.filter(lambda r: r.price > 100)'
Supported expressions:
  - g.count() - row count per group
  - g.first().column - first row value
  - g.last().column - last row value
  - g.max('column') - maximum value
  - g.min('column') - minimum value
  - g.sum('column') - sum of values
  - g.avg('column') - average value
  - Arithmetic: (expr1) - (expr2), (expr1) / (expr2), etc.

Note: Row-level operations like g.filter() are not supported in derive.
Got: {'gain': <AST for g.filter(...)>}
```

---

## 4. DSL Showcase Validation

### Target Example

This example from `docs/api.md` (lines 173-207) must work end-to-end:

```python
from ltseq import LTSeq

t = LTSeq.read_csv("stock.csv")

result = (
    t
    .sort(lambda r: r.date)
    # 1. Derive is_up flag
    .derive(lambda r: {
        "is_up": r.price > r.price.shift(1)
    })
    # 2. Group by consecutive is_up values
    .group_ordered(lambda r: r.is_up)
    # 3. ENHANCED: Filter groups with complex predicate
    .filter(lambda g: 
        (g.first().is_up == True) &   # Must be a rising group
        (g.count() > 3)               # Must last > 3 days
    )
    # 4. ENHANCED: Derive group properties to all rows
    .derive(lambda g: {
        "start": g.first().date,
        "end": g.last().date,
        "gain": (g.last().price - g.first().price) / g.first().price
    })
)

result.show()
```

### Expected Output

All rows in the result should have:
- Original columns: date, price, is_up
- Internal columns: __group_id__, __group_count__
- Derived columns from step 1: is_up
- Derived columns from step 4: start, end, gain

Each row should have the group's start/end/gain value (broadcasted from first/last).

### Validation Checklist
- ✅ `sort()` works (already implemented)
- ✅ `derive()` works (already implemented)
- ✅ `group_ordered()` works (already implemented)
- ⚠️ `filter()` with complex predicates (B.1 implementation)
- ⚠️ `derive()` with group properties (B.2 implementation)
- ✅ Result has all expected columns
- ✅ All rows in same group have same group property values

---

## 5. Test Coverage: 90% Minimum

### Test Structure

#### B.1 Tests: NestedTable.filter() (Target: 25+ tests)

**Basic Patterns:**
- `test_filter_count_greater_than` - g.count() > N
- `test_filter_count_less_than` - g.count() < N
- `test_filter_count_equal` - g.count() == N
- `test_filter_count_with_multiple_groups` - verify filtering works across groups

**First/Last Row Access:**
- `test_filter_first_column_comparison` - g.first().col == value
- `test_filter_last_column_comparison` - g.last().col < value
- `test_filter_first_numeric_comparison` - g.first().price > 100
- `test_filter_last_string_comparison` - g.last().status == "active"

**Max/Min/Sum/Avg:**
- `test_filter_group_max` - g.max('col') > value
- `test_filter_group_min` - g.min('col') < value
- `test_filter_group_sum` - g.sum('col') >= value
- `test_filter_group_avg` - g.avg('col') < value

**Combined Predicates:**
- `test_filter_count_and_first` - (g.count() > 2) & (g.first().col == val)
- `test_filter_count_or_last` - (g.count() > 5) | (g.last().col > 100)
- `test_filter_complex_nested` - ((g.count() > 2) & (g.first().col == val)) | (g.max('col') > 1000)

**Error Handling:**
- `test_filter_unsupported_function` - g.median() → detailed error
- `test_filter_invalid_syntax` - syntax error → detailed error
- `test_filter_missing_column` - g.first().nonexistent → detailed error

**Edge Cases:**
- `test_filter_empty_groups` - filter removes all groups
- `test_filter_all_pass` - filter matches all groups
- `test_filter_single_group` - only one group in result

#### B.2 Tests: NestedTable.derive() (Target: 25+ tests)

**Basic Properties:**
- `test_derive_count` - span = g.count()
- `test_derive_first_col` - start = g.first().col
- `test_derive_last_col` - end = g.last().col

**Aggregate Functions:**
- `test_derive_max_col` - max_price = g.max('price')
- `test_derive_min_col` - min_price = g.min('price')
- `test_derive_sum_col` - total = g.sum('amount')
- `test_derive_avg_col` - avg = g.avg('price')

**Arithmetic Expressions:**
- `test_derive_subtraction` - gain = g.last().price - g.first().price
- `test_derive_division` - gain_pct = (...) / g.first().price
- `test_derive_complex_expression` - (g.last().price - g.first().price) / g.first().price

**Broadcasting Verification:**
- `test_derive_all_rows_same_group_have_same_value` - verify all rows in group get same derived value
- `test_derive_different_groups_have_different_values` - different groups get different values

**Multiple Derivations:**
- `test_derive_multiple_columns` - derive multiple properties in single call
- `test_derive_with_existing_columns` - derived columns don't conflict with existing

**Error Handling:**
- `test_derive_unsupported_function` - g.filter() → detailed error
- `test_derive_invalid_arithmetic` - non-numeric operation → detailed error
- `test_derive_row_level_operation` - g.filter(...) → detailed error

**Edge Cases:**
- `test_derive_empty_groups` - derive on empty result
- `test_derive_single_group` - single group, all rows get same values
- `test_derive_null_handling` - null values in first/last/max/min

#### Integration Tests: DSL Showcase (Target: 10+ tests)

- `test_dsl_showcase_full_example` - Run the complete showcase example
- `test_dsl_showcase_filter_then_derive` - Filter → derive sequence
- `test_dsl_showcase_with_different_data` - Same pattern, different CSV
- `test_dsl_showcase_intermediate_results` - Verify state after each step

---

## 6. Error Handling Standards

### Error Categories

#### Parse Errors (AST fails to parse)
```python
ValueError: Failed to parse filter predicate
Predicate: lambda g: g.count() >  # incomplete expression
Error: Syntax error in lambda
```

#### Pattern Errors (Not a supported pattern)
```python
ValueError: Unsupported filter predicate pattern
Supported patterns: g.count() op N, g.first().col op value, ...
Got: lambda g: g.median() > 100

The following group functions are supported:
- count()
- first(), last()
- max(col), min(col), sum(col), avg(col)

The following comparison operators are supported:
- >, <, >=, <=, ==, !=
```

#### Column Errors (Column doesn't exist)
```python
AttributeError: Column 'nonexistent' not found in group
Available columns: date, price, volume
Used in: g.first().nonexistent
```

#### Type Errors (Wrong type of value)
```python
TypeError: Cannot compare with non-numeric value
Expression: g.max('price') > 'high'
Column 'price' is numeric, but compared value is string
```

### Implementation Requirements

1. **All error messages must include:**
   - What went wrong (error type)
   - What was attempted (the expression/pattern)
   - What's supported (list of valid patterns)
   - How to fix it (suggestions)

2. **Error messages must be actionable:**
   - Not just "Invalid expression"
   - Include the actual lambda received
   - List all supported patterns
   - Give examples of correct syntax

3. **Capture original AST for debugging:**
   - Store original source code
   - Include line number if available
   - Show the exact failing part

---

## 7. Implementation Timeline

### Week 1: Phase B Implementation

**Days 1-3: B.1 (NestedTable.filter())**
- Day 1: Enhance AST parser, add pattern detection
- Day 2: Implement Rust `filter_group_predicate_impl()`
- Day 3: Integration & testing

**Days 4-6: B.2 (NestedTable.derive())**
- Day 1: Add `_extract_derive_expressions()` to AST parser
- Day 2: Implement Rust `derive_group_columns_impl()`
- Day 3: Integration & testing

**Day 7: Showcase Validation**
- Run DSL showcase example end-to-end
- Fix any issues
- Document results

### Week 2: Test Coverage & Documentation

**Days 1-2: Comprehensive Testing**
- Write 50+ test cases (25+ per feature)
- Achieve 90%+ code coverage
- Test error paths thoroughly

**Days 3-4: Phase 3 Rust Optimization Planning**
- Document Rust optimization approach
- Plan A.1, A.2, A.3, A.4 implementations

---

## 8. Success Criteria

✅ **Phase B Complete When:**
1. DSL showcase example runs without errors
2. All derived columns are correct and broadcasted
3. Filtered groups have correct rows
4. 90%+ test coverage for both features
5. All error messages are detailed and helpful
6. No regression in existing tests (441 passing maintained)

✅ **Ready for Phase A When:**
1. All Phase B tests passing
2. DSL showcase validated
3. Code review completed
4. Documentation updated

---

## 9. References

- **Original API Spec:** `docs/api.md`
- **Implementation Plan:** `docs/api-impl-plan-2.md`
- **Current Status:** 441 tests passing, Phase 3 (partition/pivot/search_first/join_merge) complete
- **DSL Showcase:** `docs/api.md` lines 173-207

