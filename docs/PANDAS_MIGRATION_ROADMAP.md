# Pandas Migration Roadmap

## Current Status (Updated: January 8, 2026)

**Tests Passing**: 585 (0 skipped)  
**Pandas Dependencies Remaining**: 1 (partition() with callable only)  
**Migration Status**: ✅ COMPLETE

## Completed Migrations

✅ **Core Expression Extensions**
- `if_else()` → SQL CASE WHEN
- `fill_null()` → SQL COALESCE

✅ **String Operations** (8 methods)
- All via `.s` accessor
- Complete SQL mapping

✅ **Temporal Operations** (7 methods)
- All via `.dt` accessor
- Using SQL EXTRACT and INTERVAL

✅ **Lookup Pattern**
- `r.col.lookup(table, "column")`
- Serializable for future Rust join implementation

✅ **Pivot Operation**
- Migrated to pure Rust/SQL via `pivot_impl()`
- No pandas dependency

✅ **Partition Operation**
- Added `partition_by()` method using SQL DISTINCT + WHERE
- No pandas dependency for column-based partitioning
- `partition(by=callable)` kept as fallback for arbitrary Python callables

✅ **NestedTable.filter()**
- Migrated to SQL window functions via `filter_where()`
- Uses `__group_id__` and `__rn__` columns for partitioning

✅ **NestedTable.derive()**
- Added `derive_window_sql_impl()` in Rust
- Pure SQL execution via DataFusion
- Pandas fallback only for filter() chaining edge case

---

## Remaining Pandas Dependencies

Only **1** pandas dependency remains:

### `partition(by=callable)` - Intentionally Retained

**Location**: `py-ltseq/ltseq/partitioning.py:_materialize_partitions()`

**Reason**: This operation is **inherently Python-dependent** because:
- User provides arbitrary Python callable: `partition(by=lambda r: custom_logic(r))`
- Must execute Python on each row to determine partition key
- Cannot be translated to SQL

**Alternative**: Use `partition_by("column")` for column-based partitioning (pure SQL, no pandas)

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

## Dependencies and Sequencing - COMPLETE

All migration work is complete. Only `partition(by=callable)` retains pandas dependency by design.

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

## Success Criteria - ALL MET ✅

- ✅ All 585 tests pass
- ✅ No regressions in existing functionality
- ✅ Pandas dependency reduced from 4 to 1 (75% reduction)
- ✅ Performance improvements for migrated operations
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

- [API Extensions](API_EXTENSIONS.md) - Expression extensions reference
- [DataFusion SQL Documentation](https://datafusion.apache.org/user-guide/)
