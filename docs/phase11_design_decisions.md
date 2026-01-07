# Phase 11: Enhanced Select on Linked Columns - Design Decisions

**Status**: Design Phase (Ready for Implementation)  
**Duration**: 2.5-3 hours  
**Target**: Enable seamless selection of linked columns without explicit materialization  
**Related**: Phase 10 (filter on linked), Phase 8 (linking foundation)

---

## Executive Summary

Phase 11 enhances `LinkedTable.select()` to mirror Phase 10's success with filtering. Users can now select linked columns directly: `linked.select("prod_name", "prod_price")` without manually calling `._materialize()`.

### Key Design Decision

**Implicit materialization with transparent semantics**:
- Detect if selection includes linked columns (prefix check)
- If source-only: return `LinkedTable` (no materialization)
- If includes linked: return `LTSeq` (materialized)
- User experience: "Just works" - they don't think about materialization

---

## 1. Problem Statement

### Current Limitation (Phase 8-10)

```python
linked = orders.link(products, on=lambda o, p: o.product_id==p.product_id, as_="prod")

# ✓ Works: Source columns only
linked.select("id", "quantity")  

# ❌ Fails: Linked columns cause AttributeError
linked.select("id", "prod_name")  # Error: prod_name not in source schema

# Required workaround
linked._materialize().select("id", "prod_name")  # Forced explicit call
```

### Why This Matters

1. **Inconsistency with Phase 10**: Phase 10 allows `linked.filter(lambda r: r.prod_price > 100)` directly
2. **Poor UX**: Users must learn that some columns need `.materialize()` but others don't
3. **Intuitive expectation fails**: `prod_name` is in `linked._schema`, so users expect `.select()` to work
4. **API asymmetry**: Filter and select should have parallel behavior

### Root Cause

`LinkedTable.select()` delegates directly to source table:
```python
def select(self, *cols) -> "LTSeq":
    materialized = self._materialize()
    return materialized.select(*cols)
```

While this works for source columns, it materializes ALWAYS, even when unnecessary.

---

## 2. Design Goals & Constraints

### Goals

| Goal | Priority | Rationale |
|------|----------|-----------|
| **Transparency** | P0 | Users shouldn't think about materialization |
| **Performance** | P1 | Avoid unnecessary materialization for source-only selection |
| **Consistency** | P0 | Mirror Phase 10 filter behavior |
| **Backward Compatibility** | P0 | Existing code continues working |
| **Clear semantics** | P1 | Return type signals operation (LinkedTable vs LTSeq) |

### Constraints

1. **Python-only implementation**: No Rust changes needed
2. **Schema-driven detection**: Use `self._schema` prefixes to identify linked columns
3. **Error handling**: Clear messages when columns don't exist
4. **Type safety**: Return type matches operation (LinkedTable for source, LTSeq for linked)

---

## 3. Core Design: The "Detect & Materialize" Pattern

### Decision: Approach A - Implicit Detection

```
User calls: linked.select("id", "prod_name", "quantity")
                     ↓
System analyzes each column:
  - "id" → in source schema ✓
  - "prod_name" → starts with "prod_" (linked prefix) → linked column
  - "quantity" → in source schema ✓
                     ↓
System decides:
  - If ANY column is linked → materialize
  - If ONLY source columns → don't materialize (fast path)
                     ↓
Return:
  - LinkedTable if source-only
  - LTSeq if any linked columns included
```

### Why This Approach (vs alternatives)

| Approach | Decision | Why |
|----------|----------|-----|
| **A: Detect & Materialize** | ✅ **CHOSEN** | Transparent, user-friendly, mirrors Phase 10 |
| B: Force Explicit | ❌ Rejected | Poor UX: forces `.materialize()` call |
| C: Smart Caching | ❌ Rejected | Adds state complexity, marginal benefit |
| D: Hybrid API | ❌ Rejected | Breaks API symmetry, confusing |

### Advantages

1. **Matches Phase 10 pattern**: Users already learned this with filter()
2. **Intuitive**: Selection of columns that exist in schema "just works"
3. **Fast path preserved**: Source-only selections skip materialization
4. **Type signal**: Return type tells user what happened (LinkedTable vs LTSeq)
5. **Python precedent**: Pandas materializes implicitly when needed

### Trade-offs

- ❌ Materialization cost if user selects linked columns unexpectedly
- ✅ But no extra API to learn, no error messages to debug
- ✅ And cost is acceptable (materialization is efficient now post-Phase 9)

---

## 4. Implementation Design

### Function Signature

```python
def select(self, *cols) -> Union["LinkedTable", "LTSeq"]:
    """
    Select columns from linked table (source or linked).
    
    Automatically materializes if linked columns are selected.
    
    Behavior:
    - Source columns only → returns LinkedTable (fast path, no materialization)
    - Includes linked columns → returns LTSeq (materialized, all columns selected)
    
    Args:
        *cols: Column names (str) or computed expressions (Callable)
               Examples: "id", "prod_name", lambda r: [r.id, r.prod_price]
    
    Returns:
        LinkedTable: If selecting only source columns
        LTSeq: If selecting any linked columns
    
    Raises:
        AttributeError: If column not found in schema
        
    Examples:
        >>> linked = orders.link(products, on=..., as_="prod")
        >>> # Source columns only - returns LinkedTable
        >>> result = linked.select("id", "quantity")  # LinkedTable
        >>> # Mixed - returns LTSeq (materialized)
        >>> result = linked.select("id", "prod_name")  # LTSeq
        >>> # Linked columns only - returns LTSeq
        >>> result = linked.select("prod_name", "prod_price")  # LTSeq
    """
```

### Algorithm

```python
def select(self, *cols) -> Union["LinkedTable", "LTSeq"]:
    # 1. Identify which columns are linked columns
    linked_alias_prefix = f"{self._alias}_"
    has_linked_cols = any(
        isinstance(col, str) and col.startswith(linked_alias_prefix)
        for col in cols
    )
    
    # 2. Fast path: source columns only
    if not has_linked_cols:
        # Validate all columns exist in source
        for col in cols:
            if isinstance(col, str) and col not in self._source._schema:
                raise AttributeError(
                    f"Column '{col}' not found in source table. "
                    f"Available: {list(self._source._schema.keys())}"
                )
        # Delegate to source
        filtered_source = self._source.select(*cols)
        return LinkedTable(
            filtered_source, self._target, self._join_fn, self._alias
        )
    
    # 3. Slow path: includes linked columns
    # Materialize and select from materialized result
    materialized = self._materialize()
    return materialized.select(*cols)
```

### Key Design Decisions Within Implementation

#### Decision 1: Prefix-based linked column detection
```python
linked_alias_prefix = f"{self._alias}_"
is_linked = col.startswith(linked_alias_prefix)
```

**Why**: 
- Simple, no schema lookup needed
- Matches Phase 10 filter detection logic
- Consistent with schema naming (prod_name, prod_price)

**Alternative considered**: Check against `self._schema` directly
- ❌ Slower (O(n) scan)
- ❌ Doesn't work for columns not in schema

#### Decision 2: Validate source columns before delegation
```python
if col not in self._source._schema:
    raise AttributeError(...)
```

**Why**:
- Early error detection (before delegating)
- Clear error message pointing to source table
- Prevents confusing "column not found" from materialized result

#### Decision 3: Return type signals operation
```python
# Source-only: return LinkedTable (no materialization)
return LinkedTable(...)  

# Includes linked: return LTSeq (materialized)
return materialized.select(...)
```

**Why**:
- Type signal: LinkedTable = can chain more linked operations
- Type signal: LTSeq = terminal operation, use for display/export
- Mirrors Phase 10 filter() return behavior

---

## 5. Edge Cases & Handling

### Case 1: Empty selection (no columns)
```python
linked.select()  # No arguments
```
**Handling**: Delegate to source, let it handle empty select  
**Rationale**: Consistent with LTSeq.select() behavior

### Case 2: Computed columns (lambdas)
```python
linked.select(lambda r: [r.id, r.prod_name])
```
**Handling**: 
- Parse lambda to detect which columns it references
- If any are linked → materialize
- Use Phase 10 SchemaProxy pattern

**Challenge**: Detecting linked columns in lambda expressions
- ✓ SchemaProxy already handles linked access via NestedSchemaProxy
- ✓ Phase 10 filter uses this pattern
- ✓ Can reuse same logic

### Case 3: Mixed source and linked
```python
linked.select("id", "prod_name", "quantity")
```
**Handling**: 
- Detect "prod_name" is linked
- Materialize and select all three columns
- Return LTSeq with all columns

**Expected**: All three columns in result schema

### Case 4: Invalid column names
```python
linked.select("id", "nonexistent_column")
```
**Handling**:
- If trying source-only path: validate early, raise AttributeError
- If trying linked path: let materialized.select() raise

**Error message**:
```
AttributeError: Column 'nonexistent_column' not found in source table. 
Available columns: id, product_id, quantity
```

### Case 5: Duplicate aliases (rare edge case)
```python
linked1 = orders.link(products, on=..., as_="prod")
linked2 = orders.link(suppliers, on=..., as_="prod")  # Same alias
linked2.select("prod_name")  # Which table's prod_name?
```
**Handling**: 
- Current design assumes single alias per LinkedTable
- If users create ambiguity, first materialization determines schema
- Document as "use unique aliases"

**Prevention**: Could add validation, but out of scope for Phase 11

---

## 6. Testing Strategy

### Test Classes to Add

**Class**: `TestLinkedTableSelectLinkedColumns` (12 tests)

| Test | Coverage | Validation |
|------|----------|-----------|
| `test_select_linked_returns_ltseq` | Basic linked selection | Return type is LTSeq |
| `test_select_linked_column_basic` | Single linked column | LTSeq has correct column |
| `test_select_multiple_linked_columns` | Multiple linked columns | All columns present |
| `test_select_mixed_source_linked` | Source + linked mix | All columns in result |
| `test_select_source_only_returns_linked_table` | Fast path | Return type is LinkedTable |
| `test_select_source_only_no_materialize` | No materialization | LinkedTable not materialized |
| `test_select_source_only_chaining` | Chaining on source result | Can call more linked ops |
| `test_select_linked_data_integrity` | Data correctness | Values unchanged |
| `test_select_linked_schema_preserved` | Schema matching | Schema matches result |
| `test_select_linked_with_filter_chaining` | Chaining after select | Can filter result |
| `test_select_nonexistent_column_error` | Error handling | AttributeError with message |
| `test_select_linked_empty_result` | Empty dataset | Handles zero rows |

### Test Data

Reuse existing fixtures:
- `orders_table`: columns [id, product_id, quantity]
- `products_table`: columns [product_id, name, price]

Linked schema after `link(as_="prod")`:
- `[id, product_id, quantity, prod_product_id, prod_name, prod_price]`

### Assertions

```python
# Test: select linked returns LTSeq
result = linked.select("prod_name")
assert isinstance(result, LTSeq)  # Not LinkedTable

# Test: select source only returns LinkedTable  
result = linked.select("id", "quantity")
assert isinstance(result, LinkedTable)

# Test: mixed returns LTSeq
result = linked.select("id", "prod_name")
assert isinstance(result, LTSeq)

# Test: data integrity
result = linked.select("id", "prod_price")
# Verify prod_price values match expected
result.show(5)
```

---

## 7. Backward Compatibility

### Impact on Existing Code

**Existing usage that continues to work**:
```python
# Already works (and continues to)
linked.select("id", "quantity")  # Source columns
```

**Behavior change**: None. Current code may get better type signals, but no breaking changes.

### Migration Path

Users who currently do:
```python
linked._materialize().select("prod_name")
```

Can now simply write:
```python
linked.select("prod_name")
```

Both patterns work; new one is preferred.

---

## 8. Performance Implications

### Materialization Cost

**Worst case**: User selects linked columns
```python
linked.select("prod_name")  # Triggers materialization
```

**Cost**:
- One join operation (already optimized in Phase 9)
- One SELECT operation (column renaming, done in Phase 10)
- Total: ~O(n) where n = number of rows

**Acceptable because**:
- Join happens anyway when materializing
- Phase 10 already handles column renaming efficiently
- User gets what they asked for

### Optimization Opportunities (Future)

- Could cache materialized result if same linked table selected multiple times
- Could defer materialization until `.show()` or `.write()`
- Out of scope for Phase 11 (implement if users report performance issues)

---

## 9. Error Messages

### Clear, Helpful Error Messages

**Scenario 1**: Column doesn't exist in source
```python
linked.select("invalid_col")

# Error:
AttributeError: Column 'invalid_col' not found in source table. 
Available columns: id, product_id, quantity
```

**Scenario 2**: Column doesn't exist anywhere
```python
linked.select("prod_invalid")

# After materialization attempts, error from LTSeq.select():
AttributeError: Column 'prod_invalid' not found in schema.
Available columns: id, product_id, quantity, prod_product_id, prod_name, prod_price
```

**Scenario 3**: Mix of good and bad columns
```python
linked.select("id", "invalid", "prod_name")

# First validates source for "id" and "invalid"
AttributeError: Column 'invalid' not found in source table.
Available columns: id, product_id, quantity
```

---

## 10. Documentation Updates

### LinkedTable.select() Docstring

Update from current (lines 1361-1385):
```python
def select(self, *cols) -> "LTSeq":
    """
    Select columns from linked table.
    ...
    Returns:
        LTSeq with selected columns from source
    """
```

To new:
```python
def select(self, *cols) -> Union["LinkedTable", "LTSeq"]:
    """
    Select columns from linked table (source or linked).
    
    Automatically materializes if linked columns are selected.
    Returns LinkedTable for source-only selection (fast path).
    Returns LTSeq for linked column selection (materialized).
    ...
    """
```

### Examples in Docstring

```python
Examples:
    >>> linked = orders.link(products, on=..., as_="prod")
    
    # Select source columns (fast path)
    >>> result = linked.select("id", "quantity")
    >>> isinstance(result, LinkedTable)
    True
    
    # Select linked columns (materializes)
    >>> result = linked.select("prod_name", "prod_price")
    >>> isinstance(result, LTSeq)
    True
    
    # Mix source and linked
    >>> result = linked.select("id", "prod_name")
    >>> result.show()
```

---

## 11. Relationship to Other Phases

### Phase 10 Connection
Phase 10 implemented transparent filtering on linked columns. Phase 11 applies same pattern to selection.

**Learning from Phase 10**:
- Implicit materialization works well
- Users appreciate the transparency
- Return type signals what happened

**Implementation parallels**:
- Same "detect then materialize" algorithm
- Same error handling approach
- Same schema-based detection logic

### Phase 12 Connection
Phase 12 documents both Phase 10 and Phase 11 together in linking guide.

**Documentation coverage**:
- Show filter() and select() work on both source and linked columns
- Examples combining both operations
- Performance notes on when materialization occurs

### Future Phases
Phase 13+ (Aggregate Operations) will benefit from:
- Linked columns can be selected, so aggregation can work with linked data
- Pattern of "detect then materialize" is reusable

---

## 12. Open Questions & Decisions Made

### Q: Should computed columns (lambdas) also trigger detection?

**Decision**: Yes, reuse Phase 10 SchemaProxy pattern

**Reasoning**:
- SchemaProxy already detects linked column access
- User writes `lambda r: r.prod_name` → SchemaProxy returns ColumnExpr
- System can detect which columns are referenced
- Consistent with Phase 10 filter() behavior

**Implementation**: 
```python
# For callable columns, parse with full schema (source + linked)
# This allows: linked.select(lambda r: [r.id, r.prod_name])
```

### Q: Should we cache the materialized result?

**Decision**: No, not in Phase 11

**Reasoning**:
- Adds complexity
- Materialization is fast (tested in Phase 9)
- Users don't typically call select() multiple times
- Can add caching later if performance becomes issue

---

## 13. Acceptance Criteria

### Must Have (P0)

- ✅ `linked.select("prod_name")` returns LTSeq with correct data
- ✅ `linked.select("id", "quantity")` returns LinkedTable (no materialization)
- ✅ `linked.select("id", "prod_name")` returns LTSeq with both columns
- ✅ Invalid columns raise clear AttributeError
- ✅ All 12 new tests pass
- ✅ All existing Phase 8 tests still pass (71 tests)
- ✅ Total tests: 330+

### Should Have (P1)

- ✅ Docstring updated with examples
- ✅ Works with computed columns (lambdas)
- ✅ Works with multiple linked tables (different aliases)
- ✅ Clear error messages guide users

### Nice to Have (P2)

- Caching of materialized results (can add later)
- Performance metrics/profiling (can add later)

---

## 14. Summary of Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Materialization approach** | Implicit (detect then materialize) | Transparent, mirrors Phase 10, user-friendly |
| **Column detection method** | Prefix-based (alias_*) | Simple, fast, matches schema naming |
| **Return type** | Union[LinkedTable, LTSeq] | Signals what happened to user |
| **Error handling** | Early validation for source columns | Clear error messages |
| **Lambda support** | Yes, via SchemaProxy | Consistent with filter() |
| **Caching** | No (Phase 11) | Complexity outweighs benefit now |
| **API changes** | Return type only | Backward compatible |
| **Test count** | 12 new tests | Mirrors Phase 10 coverage |

---

## 15. Success Metrics

**Technical**:
- 12 new tests all passing
- 71 existing Phase 8 tests still passing
- 330+ total tests passing
- Zero regressions

**User-facing**:
- Users can write `linked.select("prod_name")` without error
- Users understand return type signals materialization
- Error messages are clear and actionable
- Parallel API with Phase 10 filter()

**Code quality**:
- Implementation is straightforward (no Rust changes)
- Docstrings are clear with examples
- Edge cases handled gracefully

---

## Next Steps

1. **Implement** LinkedTable.select() (20-30 min)
2. **Test** with 12 test cases (30-45 min)
3. **Debug** edge cases (15-25 min)
4. **Document** docstrings (10-15 min)
5. **Verify** all tests pass (10 min)

**Total**: 2.5-3 hours

Once Phase 11 is complete, proceed to Phase 12 (documentation updates).
