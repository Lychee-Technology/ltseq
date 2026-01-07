# Phase 13: Aggregate Operations - Design Document

**Status**: Design Phase (Ready for Implementation after Phase 11-12)  
**Duration**: 3-4 hours  
**Goal**: Enable aggregation on both regular tables and linked tables  
**Related**: Phase 8 (linking), Phase 12 (documentation)

---

## Executive Summary

Phase 13 implements `.aggregate()` method that works with both regular LTSeq tables AND LinkedTable joined results. Users can perform "join then summarize" analytical patterns cleanly.

### Key Capabilities After Phase 13

```python
# Regular table aggregation
orders = LTSeq.read_csv("orders.csv")
summary = orders.aggregate({
    "total_orders": lambda g: g.id.count(),
    "total_revenue": lambda g: (g.quantity * g.price).sum()
})

# Linked table aggregation (NEW)
products = LTSeq.read_csv("products.csv")
linked = orders.link(products, on=..., as_="prod")

# Aggregates linked columns (Phase 13)
summary = linked.aggregate({
    "orders_per_product": lambda g: g.id.count(),
    "revenue_by_product": lambda g: (g.quantity * g.prod_price).sum(),
    "avg_quantity": lambda g: g.quantity.mean()
})

summary.show()
```

---

## 1. Problem Statement & Context

### Current State (Post-Phase 11)

✅ **Linking complete**: Phase 8-11 enable filtering and selecting linked columns
✅ **Materialization solved**: Phase 9-10 made it transparent

❌ **Aggregation gap**: No way to aggregate across linked tables

```python
linked = orders.link(products, on=..., as_="prod")

# ❌ This doesn't exist yet
summary = linked.aggregate({
    "avg_product_price": lambda g: g.prod_price.mean()
})

# Current workaround
materialized = linked._materialize()
summary = materialized.aggregate(...)  # Requires explicit call
```

### Why This Matters

"Join then aggregate" is THE most common analytical pattern:
- Revenue by product (join orders → products, sum by product)
- Inventory by category (join products → categories, count by category)
- Customer lifetime value (join customers → orders, sum by customer)

Users expect this to work naturally after linking completes.

### User Research (Implied)

From your design choices:
- You want "user-facing improvements" (Phase 11 select on linked)
- You want to "move to different features after linking" (Phase 13)
- This suggests linking should be "complete" - and aggregation completes that story

---

## 2. Design Goals & Constraints

### Goals

| Goal | Priority | Rationale |
|------|----------|-----------|
| **Works with linked tables** | P0 | Core request - complete linking story |
| **Aggregates linked columns** | P0 | Access product info in aggregation |
| **Transparent materialization** | P1 | Mirrors Phase 10-11 pattern |
| **Backward compatible** | P0 | Existing LTSeq.aggregate() unchanged |
| **Clear semantics** | P1 | Users understand what "aggregate" does |

### Constraints

1. **SQL-based approach**: Use DataFusion's GROUP BY + aggregation functions
2. **Implicit materialization**: Linked table aggregation auto-materializes
3. **Python-only logic**: Focus on Python API, minimal Rust changes
4. **Schema-aware**: Detect linked columns and ensure they're available

---

## 3. Architecture Decision: Where Does Aggregation Happen?

### Option A: LinkedTable.aggregate() method

```python
# LinkedTable has its own aggregate()
linked.aggregate({...})  # Returns LTSeq
```

**Pros**:
- Parallel API with LinkedTable.filter(), .select()
- Clear semantics: "aggregate linked table"

**Cons**:
- Duplicate code (LinkedTable + LTSeq both have aggregate)
- Need to materialize implicitly

### Option B: Delegate to materialization

```python
# linked.aggregate() calls _materialize() internally
# Returns same result as materialized.aggregate()
```

**Pros**:
- No code duplication
- One source of truth (LTSeq.aggregate)
- Clear what's happening (must materialize)

**Cons**:
- Less transparent than Phase 10-11 (requires knowing what aggregate does)
- Slight API asymmetry with filter/select

### Option C: Generic aggregation in Python layer

```python
# New method that works on any table (linked or regular)
# Detects if linked, materializes if needed
result = linked.aggregate({...})
```

**Pros**:
- Single API
- Works with both types
- Flexible for future

**Cons**:
- More complex Python logic
- Harder to test edge cases

---

### **Decision: Option B + C Hybrid**

```python
class LinkedTable:
    def aggregate(self, *args, **kwargs) -> "LTSeq":
        """Delegate to materialized table's aggregate method."""
        materialized = self._materialize()
        return materialized.aggregate(*args, **kwargs)
```

**Rationale**:
1. No code duplication
2. Users get single aggregate() interface
3. Materialization is explicit in code (can see it happens)
4. LTSeq.aggregate() already works, we just expose it on LinkedTable

---

## 4. Core Implementation Design

### API Design

```python
def aggregate(
    self, 
    *agg_specs,  # Can be dict or list of (col, agg_fn) tuples
    **named_aggs  # Named aggregation: avg_price=lambda g: g.price.mean()
) -> "LTSeq":
    """
    Aggregate data with support for linked columns.
    
    For LinkedTable: Materializes join, then aggregates.
    For LTSeq: Standard aggregation.
    
    Args:
        *agg_specs: 
            - Dict: {"name": lambda g: g.col.agg_func()}
            - List of tuples: [("name", lambda g: g.col.agg_func())]
        
        **named_aggs: Named arguments form of dict
    
    Returns:
        LTSeq with aggregated results
    
    Examples:
        # Dict form
        summary = linked.aggregate({
            "total": lambda g: g.quantity.sum(),
            "avg_price": lambda g: g.prod_price.mean()
        })
        
        # Named form
        summary = linked.aggregate(
            total=lambda g: g.quantity.sum(),
            avg_price=lambda g: g.prod_price.mean()
        )
    """
    # LinkedTable implementation
    materialized = self._materialize()
    return materialized.aggregate(*agg_specs, **named_aggs)
```

### Algorithm

```python
# LinkedTable.aggregate() - NEW (10-15 lines)
def aggregate(self, *agg_specs, **named_aggs) -> "LTSeq":
    # Materialize linked table
    materialized = self._materialize()
    # Delegate to LTSeq.aggregate
    return materialized.aggregate(*agg_specs, **named_aggs)

# LTSeq.aggregate() - EXISTS (no changes needed)
# Already supports aggregation on all columns in schema
```

### Why This Works

**Before Phase 11**:
- LinkedTable doesn't have aggregated columns available in Python layer

**After Phase 10-11**:
- Materialization makes all columns available (linked ones too)
- LTSeq.aggregate() already works with all schema columns
- So LinkedTable.aggregate() just needs to materialize first

**No Rust changes** because:
- LTSeq.aggregate() delegates to DataFusion
- Materialization already renamed columns correctly (Phase 10)
- All columns are accessible by name

---

## 5. Detailed Design Decisions

### Decision 1: Implicit vs Explicit Materialization

**Chosen**: Implicit (materialize happens inside aggregate())

```python
# User doesn't think about materialization
result = linked.aggregate({...})  # MaterializesAutomatically
```

**Rationale**:
- Consistent with Phase 10 filter(), Phase 11 select()
- Users expect `.aggregate()` to work on all columns in schema
- Materialization is transparent and cached

**Alternative considered**: Require explicit `.materialize()` first
- ❌ Inconsistent with filter/select
- ❌ Requires user knowledge of internals

### Decision 2: Return Type

**Chosen**: LTSeq (always)

```python
# Aggregation always returns LTSeq, never LinkedTable
result = linked.aggregate({...})  # Returns LTSeq
assert isinstance(result, LTSeq)
```

**Why**:
- Aggregation produces new table (no longer "linked")
- Result is disconnected from original linked context
- LTSeq is appropriate for terminal operations

### Decision 3: Schema Handling

**Aggregation produces new schema** with aggregation result columns:

```python
# Original linked schema
linked._schema  # {id, product_id, quantity, prod_name, prod_price}

# After aggregation
result = linked.aggregate({
    "total_qty": lambda g: g.quantity.sum(),
    "avg_price": lambda g: g.prod_price.mean()
})

result._schema  # {total_qty, avg_price}
```

**Why**:
- Aggregation is terminal operation (no access to detail rows)
- Result is different structure (summary, not detail)
- Consistent with how LTSeq.aggregate() works

### Decision 4: Group By Handling

**Aggregation is ungrouped by default** (single row result):

```python
# Single summary row
result = linked.aggregate({
    "total": lambda g: g.quantity.sum()
})
# Result has 1 row with total

# For grouped aggregation (future), use group_ordered()
grouped = linked.group_ordered(lambda r: r.prod_name)
group_summaries = grouped.aggregate({...})
```

**Why**:
- Simple, single-table aggregation first
- Grouped aggregation is separate feature (Phase 14+)
- Current LTSeq.aggregate() is ungrouped too

---

## 6. Test Strategy

### Test Classes to Add

**Class**: `TestLinkedTableAggregate` (12-15 tests)

| Test | Coverage | Validation |
|------|----------|-----------|
| `test_aggregate_linked_returns_ltseq` | Return type | Result is LTSeq |
| `test_aggregate_linked_count` | Basic agg | Count rows correctly |
| `test_aggregate_linked_sum` | Sum operation | Sum values correctly |
| `test_aggregate_linked_mean` | Mean operation | Average correct |
| `test_aggregate_linked_on_linked_column` | Linked column agg | Can aggregate linked col |
| `test_aggregate_linked_mixed_columns` | Mixed types | Both source & linked |
| `test_aggregate_multiple_aggs` | Multiple aggregations | All aggs present |
| `test_aggregate_named_args` | Named parameters | Works with kwargs |
| `test_aggregate_source_only` | Source aggregation | Works without linked |
| `test_aggregate_with_filter_before` | Chaining | Filter then aggregate |
| `test_aggregate_empty_result` | Edge case | Handle no rows |
| `test_aggregate_new_schema` | Schema | Result schema correct |
| `test_aggregate_with_derived_column` | Computed agg | lambda with expressions |
| `test_aggregate_multirow_result` | Grouped (future prep) | Can return multiple rows |
| `test_aggregate_performance` | Performance | No catastrophic slowdown |

### Test Data Strategy

Reuse existing fixtures:
- `orders_table`: 5 rows, various product_ids
- `products_table`: 4 products with different prices

Linked schema after `link(as_="prod")`:
- Source: [id, product_id, quantity]
- Linked: [id, product_id, quantity, prod_product_id, prod_name, prod_price]

### Sample Test Cases

```python
def test_aggregate_linked_count(self, orders_table, products_table):
    """Count rows in aggregation."""
    linked = orders_table.link(
        products_table, 
        on=lambda o, p: o.product_id == p.product_id, 
        as_="prod"
    )
    
    result = linked.aggregate({
        "total_orders": lambda g: g.id.count()
    })
    
    assert isinstance(result, LTSeq)
    assert len(result) == 1  # Single summary row
    assert "total_orders" in result._schema

def test_aggregate_linked_on_linked_column(self, orders_table, products_table):
    """Aggregate on linked columns."""
    linked = orders_table.link(
        products_table,
        on=lambda o, p: o.product_id == p.product_id,
        as_="prod"
    )
    
    result = linked.aggregate({
        "avg_product_price": lambda g: g.prod_price.mean(),
        "max_product_price": lambda g: g.prod_price.max()
    })
    
    assert "avg_product_price" in result._schema
    assert "max_product_price" in result._schema
    assert len(result) == 1
```

---

## 7. Implementation Plan

### Step 1: Add LinkedTable.aggregate() method (5-10 min)

Location: `py-ltseq/ltseq/__init__.py` after `LinkedTable.distinct()`

```python
def aggregate(self, *agg_specs, **named_aggs) -> "LTSeq":
    """Aggregate linked table by materializing first."""
    materialized = self._materialize()
    return materialized.aggregate(*agg_specs, **named_aggs)
```

### Step 2: Add docstring with examples (10-15 min)

Comprehensive docstring showing:
- Basic aggregation
- Linking + aggregation pattern
- Filter before aggregate pattern

### Step 3: Create test class (30-45 min)

`TestLinkedTableAggregate` with 12-15 tests following Phase 10-11 patterns

### Step 4: Run tests and debug (20-30 min)

Verify:
- All 12-15 new tests pass
- All existing tests still pass
- No regressions in Phase 8-11 tests

### Step 5: Update documentation (10-15 min)

Add section to phase8_linking_guide.md:
- Aggregation examples
- Common patterns
- Performance notes

---

## 8. Edge Cases & Handling

### Case 1: Aggregation on empty result

```python
linked = orders.link(products, on=..., as_="prod")
empty = linked.filter(lambda r: r.id > 1000)  # No matches
result = empty.aggregate({"count": lambda g: g.id.count()})
# Result: single row with count=0
```

**Handling**: Let aggregation proceed (SQL GROUP BY handles empty set)

### Case 2: Multiple linked columns in aggregation

```python
result = linked.aggregate({
    "total": lambda g: g.quantity.sum(),
    "avg_price": lambda g: g.prod_price.mean(),
    "max_price": lambda g: g.prod_price.max(),
    "min_price": lambda g: g.prod_price.min()
})
```

**Handling**: All aggregations happen in single pass (efficient)

### Case 3: Aggregation with computed columns

```python
result = linked.aggregate({
    "total_revenue": lambda g: (g.quantity * g.prod_price).sum(),
    "avg_order_value": lambda g: (g.quantity * g.prod_price).mean() / g.id.count()
})
```

**Handling**: Delegate to LTSeq.aggregate() (already handles expressions)

### Case 4: Aggregation after chained linking

```python
# Post-Phase 9: Chained linking
linked1 = orders.link(products, on=..., as_="prod")
mat1 = linked1._materialize()
linked2 = mat1.link(categories, on=..., as_="cat")

# Phase 13: Aggregation on chained result
result = linked2.aggregate({
    "total": lambda g: g.id.count(),
    "categories": lambda g: g.cat_name.distinct().count()
})
```

**Handling**: Materialize linked2, then aggregate (works because Phase 9 fixed chaining)

---

## 9. Performance Considerations

### Materialization Cost

**Aggregation requires materialization**:
1. Join two tables (O(n) for inner join)
2. Column renaming (SELECT statement, O(n))
3. GROUP BY aggregation (O(n log n) or O(n) depending on aggregation)

**Total**: O(n log n) - typical aggregation cost

### Optimization Opportunities (Future)

1. **Lazy aggregation**: Push group by down to join level
   - "Aggregate before materializing" pattern
   - Reduces intermediate data size
   - Requires Rust changes

2. **Caching**: Cache materialized result if multiple aggregations
   - User calls `.aggregate()` twice on same linked table
   - Reuse first materialization
   - Low priority

3. **Projection pushdown**: Only fetch columns needed for aggregation
   - Current: materializes all columns, then aggregates
   - Optimized: Only fetch aggregation columns
   - Requires query planning changes

**Phase 13 approach**: Straightforward implementation, optimize later if users report issues

---

## 10. Backward Compatibility

### Impact on Existing Code

**No breaking changes**:
- LTSeq.aggregate() unchanged
- LinkedTable doesn't currently have aggregate() (new feature)
- No API changes to existing methods

### Migration Path

Users who currently do:
```python
linked._materialize().aggregate({...})
```

Can now simplify to:
```python
linked.aggregate({...})
```

Both work; new one is preferred.

---

## 11. Documentation Plan

### Add to `docs/phase8_linking_guide.md`

New section: "Aggregation - Phase 13"

```markdown
## Aggregation on Linked Tables (Phase 13)

Aggregate linked results directly without explicit materialization:

### Basic Aggregation

```python
linked = orders.link(products, on=..., as_="prod")

result = linked.aggregate({
    "total_orders": lambda g: g.id.count(),
    "total_revenue": lambda g: (g.quantity * g.prod_price).sum(),
    "avg_product_price": lambda g: g.prod_price.mean()
})

result.show()
```

### Use Cases

1. **Revenue by Product**
```python
result = linked.aggregate({
    "product": lambda g: g.prod_name.first(),
    "total_qty": lambda g: g.quantity.sum(),
    "total_revenue": lambda g: (g.quantity * g.prod_price).sum(),
    "avg_order_qty": lambda g: g.quantity.mean()
})
```

2. **Price Statistics**
```python
result = linked.aggregate({
    "min_price": lambda g: g.prod_price.min(),
    "avg_price": lambda g: g.prod_price.mean(),
    "max_price": lambda g: g.prod_price.max()
})
```

3. **Filter Then Aggregate**
```python
expensive = linked.filter(lambda r: r.prod_price > 100)
result = expensive.aggregate({
    "count": lambda g: g.id.count(),
    "total_revenue": lambda g: (g.quantity * g.prod_price).sum()
})
```

### Performance

Aggregation on linked tables:
1. Materializes the join (join + column rename)
2. Applies GROUP BY aggregation
3. Returns summary table

Typical for analytical queries. Use `.aggregate()` for summaries, `.select()` for detail rows.
```

### Examples to Add

Add to `examples/linking_advanced.py` or create `examples/linking_aggregation.py`

---

## 12. Success Criteria

### Must Have (P0)

- ✅ LinkedTable.aggregate() method exists
- ✅ Works with linked columns (prod_name, prod_price)
- ✅ Works with source columns (id, quantity)
- ✅ Works with computed expressions (quantity * price)
- ✅ 12-15 new tests all passing
- ✅ All existing Phase 8-11 tests still passing (330+ tests)
- ✅ Clear error messages for invalid aggregations

### Should Have (P1)

- ✅ Docstring with multiple examples
- ✅ Works with filter chaining
- ✅ Works with chained linking (Phase 9)
- ✅ Documentation section in linking guide

### Nice to Have (P2)

- Grouped aggregation (Phase 14 prep)
- Performance benchmarks
- Real-world example notebooks

---

## 13. Comparison with Alternatives

### Alternative: GROUP BY syntax

```python
# Hypothetical future API
result = linked.group_by("prod_name").aggregate({
    "total": lambda g: g.quantity.sum()
})
```

**Pros**: More explicit grouping
**Cons**: More complex API, out of scope for Phase 13

**Decision**: Implement ungrouped aggregation first, add group_by in Phase 14+

---

## 14. Relationship to Other Phases

### Phase 8-11 Foundation
- Phase 8: Linking enables access to linked columns
- Phase 9: Bug fix enables chaining
- Phase 10: Filter on linked columns
- Phase 11: Select linked columns
- **Phase 13**: Aggregate linked columns (completes story)

### Phase 14+ Evolution
- **Phase 14**: GROUP BY aggregation (grouped results)
- **Phase 15**: Window functions + aggregation
- **Phase 16**: Advanced analytics

---

## 15. Summary of Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Implementation approach** | LinkedTable delegates to materialized.aggregate() | No code duplication, reuse existing logic |
| **Materialization** | Implicit (automatic) | Consistent with Phase 10-11 |
| **Return type** | Always LTSeq | Aggregation is terminal operation |
| **Grouped aggregation** | Not in Phase 13 | Simpler MVP, Phase 14+ feature |
| **Optimization** | Straightforward, no caching | Fast enough for Phase 13, optimize later |
| **API style** | Dict or named parameters | Flexible, matches LTSeq.aggregate() |
| **Test count** | 12-15 tests | Comprehensive, mirrors Phase 10-11 |

---

## 16. Implementation Checklist

- [ ] Add LinkedTable.aggregate() method (5-10 min)
- [ ] Write docstring with examples (10-15 min)
- [ ] Create TestLinkedTableAggregate class (30-45 min)
- [ ] Run and debug tests (20-30 min)
- [ ] Update linking guide documentation (10-15 min)
- [ ] Final review and verification (10 min)

**Total**: 85-125 minutes (1.5-2 hours for implementation, +1-2 hours testing/polish) = 3-4 hours estimate

---

## 17. Ready for Implementation

Phase 13 design is complete and ready for implementation after Phase 11-12.

**Dependencies**:
- ✅ Phase 8: Linking (complete)
- ✅ Phase 9: Chained materialization fix (complete)
- ✅ Phase 10: Filter on linked (complete)
- ✅ Phase 11: Select on linked (prerequisite, will be done)
- ✅ Phase 12: Documentation updates (prerequisite, will be done)

**Next step**: Implement Phase 11, then Phase 12, then Phase 13.
