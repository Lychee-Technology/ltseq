# Pandas Migration Roadmap

## Overview

This document outlines the plan for migrating remaining pandas-dependent operations to Rust/SQL equivalents. Current status: **~90% of planned migrations complete**.

## Completed Migrations (Phases 1-4)

✅ **Phase 1**: Core Expression Extensions
- `if_else()` → SQL CASE WHEN
- `fill_null()` → SQL COALESCE

✅ **Phase 2**: String Operations (8 methods)
- All via `.s` accessor
- Complete SQL mapping

✅ **Phase 3**: Temporal Operations (7 methods)
- All via `.dt` accessor
- Using SQL EXTRACT and INTERVAL

✅ **Phase 4**: Lookup Pattern
- `r.col.lookup(table, "column")`
- Serializable for future Rust join implementation

---

## Remaining Migrations (Phases 5a-5d)

### Phase 5a: Pivot Operation

**Current Status**: Uses pandas pivot_table

**Location**: `py-ltseq/ltseq/core.py:1132-1277`

**Current Implementation**:
```python
# Uses pandas pivot_table with groupby().unstack()
df = self.to_pandas()
pivoted_df = df.pivot_table(
    index=index_cols,
    columns=columns,
    values=values,
    aggfunc=agg_fn_map[agg_fn]
)
```

**Migration Strategy**:

1. **SQL-Based Approach (Preferred)**
   - Get DISTINCT values from pivot column
   - Generate dynamic CASE WHEN for each value
   - Use GROUP BY with aggregation
   
   Example SQL:
   ```sql
   SELECT 
       year,
       SUM(CASE WHEN region = 'West' THEN amount ELSE NULL END) as West,
       SUM(CASE WHEN region = 'East' THEN amount ELSE NULL END) as East
   FROM sales
   GROUP BY year
   ```

2. **Implementation Steps**:
   - Add `pivot_impl()` function to `src/ops/advanced.rs`
   - Implement dynamic CASE WHEN generation
   - Register temp table and execute aggregate query
   - Return RustTable with pivoted schema
   - Add Python `pivot()` method wrapper in RustTable

3. **Challenges**:
   - Dynamic column generation (number of distinct values unknown at compile time)
   - Column name sanitization (special characters, spaces)
   - Schema inference for pivoted result
   - Proper NULL handling with aggregation

4. **Estimated Effort**: 2-3 days
   - SQL generation: 1 day
   - Rust implementation: 1 day
   - Testing and edge cases: 1 day

---

### Phase 5b: Partition Operation

**Current Status**: Uses pandas iteration with callable partition_fn

**Location**: `py-ltseq/ltseq/partitioning.py:172-222`

**Current Implementation**:
```python
# Evaluates Python callable on each row
df = self._ltseq.to_pandas()
rows = df.to_dict("records")

for row_data in rows:
    row_proxy = _RowProxy(row_data)
    partition_key = self._partition_fn(row_proxy)  # Python callable
    partitions_dict[partition_key].append(row_data)
```

**Migration Strategy**:

This operation is **inherently Python-dependent** because:
- User provides arbitrary Python callable: `partition(lambda r: r.status)`
- Must execute Python on each row to determine partition key
- Cannot be easily translated to pure SQL

**Possible Approaches**:

1. **Keep Pandas Implementation** (Pragmatic)
   - Acceptable since operation already requires Python callable
   - Optimize by reducing intermediate copies
   - Mark as "Python integration point"

2. **Expression-Based Partitioning** (Future)
   - Allow expression-only partitioning: `partition_by(r.status)`
   - Implement using SQL GROUP BY under the hood
   - Keep callable API for backward compatibility

3. **Hybrid Approach**:
   - Detect if partition function can be converted to expression
   - Use Rust/SQL for expression-based partitioning
   - Fall back to pandas for arbitrary Python callables

**Estimated Effort**: 1-2 days (if hybrid approach)
   - Expression detection: 1/2 day
   - Implement expression-based partitioning: 1/2 day
   - Fallback logic: 1 day

---

### Phase 5c: NestedTable.filter() Rust Optimization

**Current Status**: Uses pandas groupby().apply() for group-level filtering

**Location**: `py-ltseq/ltseq/grouping.py:458-530`

**Current Implementation**:
```python
# Creates grouped DataFrames using pandas
grouped = df.groupby(__group_id__)
for group_id, group_df in grouped:
    if predicate_func(group_df):  # Apply to group
        result_list.append(group_df)
```

**Migration Strategy**:

1. **Window Function Approach**
   - Use SQL window functions: ROW_NUMBER(), RANK(), COUNT() OVER (PARTITION BY)
   - Build predicates that work on aggregate values
   - Filter groups using aggregate conditions

   Example:
   ```sql
   SELECT * FROM table
   WHERE (COUNT(*) OVER (PARTITION BY __group_id__)) > 5
   ```

2. **Implementation Steps**:
   - Extend transpiler to handle group aggregate functions
   - Support syntax like: `group.count > 5` → count(*) over clause
   - Generate SQL with window functions instead of pandas groupby
   - Maintain lazy evaluation when possible

3. **Challenges**:
   - Translating pandas group predicates to SQL window functions
   - Supporting complex group-level operations
   - Maintaining compatibility with existing API

4. **Estimated Effort**: 2 days
   - Window function support: 1 day
   - Group predicate translation: 1 day
   - Integration and testing: 1 day

---

### Phase 5d: NestedTable.derive() Rust Optimization

**Current Status**: Uses pandas transform() for group-level derivation

**Location**: `py-ltseq/ltseq/grouping.py:1107-1163`

**Current Implementation**:
```python
# Creates derived columns for each group using pandas
grouped = df.groupby(__group_id__)
for col_name, func in derived_cols.items():
    df[col_name] = grouped.transform(func)
```

**Migration Strategy**:

1. **Window Function Approach**
   - Leverage existing window function infrastructure from Phase 6
   - Support aggregations broadcasted to all rows in group
   - Maintain row-level semantics while computing group values

   Example:
   ```sql
   SELECT *,
       SUM(amount) OVER (PARTITION BY __group_id__) as group_total,
       AVG(amount) OVER (PARTITION BY __group_id__) as group_avg
   FROM table
   ```

2. **Implementation Steps**:
   - Extend existing window function transpiler
   - Support group-level aggregates: count, sum, avg, min, max
   - Generate proper PARTITION BY clauses
   - Ensure broadcasts work correctly

3. **Challenges**:
   - Distinguishing window functions from row-level operations
   - Broadcast semantics (each row gets group aggregate value)
   - Combining group aggregates with row-level transformations

4. **Estimated Effort**: 2 days
   - Window function enhancement: 1 day
   - Group broadcast implementation: 1 day
   - Integration and testing: 1 day

---

## Migration Priority Matrix

| Phase | Complexity | Impact | Effort | Priority |
|-------|-----------|--------|--------|----------|
| 5a: pivot() | High | High | 2-3d | Medium |
| 5b: partition() | Medium | Medium | 1-2d | Low* |
| 5c: filter() | High | High | 2d | High |
| 5d: derive() | High | High | 2d | High |

*Low priority because it's inherently Python-dependent

---

## Implementation Checklist

### Phase 5a: Pivot

- [ ] Add DISTINCT query for pivot column values
- [ ] Generate CASE WHEN expressions for each value
- [ ] Build GROUP BY aggregation query
- [ ] Implement column name sanitization
- [ ] Add `pivot_impl()` to advanced.rs
- [ ] Add Python method wrapper
- [ ] Test with various aggregation functions
- [ ] Handle NULL values in pivoted output
- [ ] Document SQL translation

### Phase 5b: Partition

- [ ] Design expression detection system
- [ ] Implement expression-based partitioning with GROUP BY
- [ ] Keep pandas fallback for complex callables
- [ ] Optimize row copying for pandas path
- [ ] Add hybrid routing logic
- [ ] Document detection capabilities

### Phase 5c: Filter (NestedTable)

- [ ] Extend transpiler for window functions
- [ ] Support group-level predicates
- [ ] Implement GROUP aggregate detection
- [ ] Generate PARTITION BY clauses
- [ ] Handle complex conditions
- [ ] Document supported patterns

### Phase 5d: Derive (NestedTable)

- [ ] Leverage Phase 6 window infrastructure
- [ ] Support broadcast semantics
- [ ] Combine group aggregates with row values
- [ ] Optimize multiple derived columns
- [ ] Generate efficient PARTITION BY
- [ ] Document broadcast patterns

---

## Testing Strategy

### Unit Tests
- Each operation should have comprehensive unit tests
- Test edge cases: empty groups, NULL values, special characters
- Validate SQL generation

### Integration Tests
- Test complete workflows combining multiple operations
- Verify correctness against pandas baseline
- Performance comparison tests

### Regression Tests
- Ensure existing tests continue to pass
- Validate backward compatibility

---

## Performance Expectations

| Operation | Current (Pandas) | Target (Rust) | Improvement |
|-----------|-----------------|---------------|------------|
| pivot() | Medium (full data materialize) | Fast (SQL GROUP BY) | 2-5x |
| filter() | Medium (groupby iteration) | Fast (window functions) | 3-10x |
| derive() | Slow (transform per group) | Fast (single pass) | 5-20x |
| partition() | Fast (already optimized) | N/A | (Python dependent) |

---

## Dependencies and Sequencing

```
Phase 1-4: Complete ✅
    ↓
Phase 5a: Pivot (independent)
Phase 5b: Partition (independent)
    ↓
Phase 5c: Filter (requires Phase 6 window setup)
Phase 5d: Derive (requires Phase 6 window setup)
```

Phases 5a and 5b can be done in parallel.
Phases 5c and 5d should follow Phase 6 window function work.

---

## Development Environment Setup

### For Rust Development:
```bash
cd /Users/ruoshi/code/github/ltseq
cargo build
cargo test
```

### For Python Testing:
```bash
cd /Users/ruoshi/code/github/ltseq/py-ltseq
python -m pytest tests/ -v
```

### Key Files to Modify:
- `src/ops/advanced.rs` - Add pivot_impl, optimize filter/derive
- `src/transpiler.rs` - Enhance window function support
- `py-ltseq/ltseq/core.py` - Remove pandas, use Rust for pivot
- `py-ltseq/ltseq/grouping.py` - Update filter/derive to use Rust

---

## Success Criteria

- ✅ All 481+ tests pass
- ✅ No regressions in existing functionality
- ✅ Pandas dependency reduced by 30-50%
- ✅ Performance improvements of 2-20x for migrated operations
- ✅ Documentation updated for all changes
- ✅ Backward compatibility maintained

---

## Future Considerations

1. **API Evolution**: Consider expression-based alternatives for callable-dependent operations
2. **Vectorization**: Leverage Rust SIMD for string/temporal operations
3. **Streaming**: Support streaming aggregations for very large datasets
4. **Parallelization**: Leverage Rust threading for multi-core performance

---

## References

- Phase 1-4 Documentation: `/docs/API_EXTENSIONS.md`
- Phase 6 Window Functions: Window function implementation details
- DataFusion SQL Documentation: https://datafusion.apache.org/user-guide/
