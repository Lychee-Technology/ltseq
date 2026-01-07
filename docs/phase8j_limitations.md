# Phase 8J: Link Chaining - Known Limitations & Documentation

## Overview

Phase 8J implements structural support for chaining multiple `link()` calls, allowing you to join a table to another table, then join the result to a third table, etc.

**Status**: Structural support complete. Single-level links work perfectly. Multi-level chaining has known limitations.

## What Works

### Single Link (Fully Functional)
```python
# Join orders to products
linked = orders.link(
    products,
    on=lambda o, p: o.product_id == p.product_id,
    as_="prod",
    join_type="inner"  # Supported: inner, left, right, full
)

# Works perfectly
result = linked._materialize()
linked.show()
```

### Filtering and Selecting on LinkedTable
```python
# Filter on source columns
filtered = linked.filter(lambda r: r.quantity > 10)

# Select source columns
selected = linked.select("id", "product_id", "quantity")
```

## Known Limitation: Chained Materialization

### The Problem

When you try to materialize a link, then link the materialized result to another table, you get unexpected behavior:

```python
# ❌ THIS DOESN'T WORK AS EXPECTED:
linked1 = orders.link(products, on=..., as_="prod")
mat1 = linked1._materialize()  # ✓ Works: creates DataFrame with joined data
linked2 = mat1.link(categories, on=..., as_="cat")  # ✓ Works: creates LinkedTable
mat2 = linked2._materialize()  # ❌ FAILS or produces unexpected results
```

### Why It Happens

The issue is a mismatch between DataFrame column names and schema metadata:

1. When a join is materialized, the Rust code creates a DataFrame with qualified column names like `"?table?".name`
2. The Python schema metadata stores prefixed names like `prod_name`
3. The two don't match, causing issues when trying to use the materialized result as input to another join

This is a fundamental limitation of how DataFusion handles DataFrame column naming during join operations.

### Workaround: Materialize at the End

Instead of materializing between links, materialize only at the very end:

```python
# ✓ THIS WORKS:
linked1 = orders.link(products, on=..., as_="prod")
linked2 = linked1.link(categories, on=..., as_="cat")
# Materialize only at the end
result = linked2._materialize()
```

**Key Rule**: Materialize only at the end of your link chain, not in the middle.

## Tested Scenarios

### ✓ Working
- Single link with all join types (INNER, LEFT, RIGHT, FULL)
- Filtering on source columns in a linked table
- Selecting source columns from a linked table
- Creating a LinkedTable with `join_type` parameter
- Composite join keys (multi-column conditions)
- Schema includes all columns with proper prefixes
- Materialization caching (multiple calls to `_materialize()` return same object)

### ⚠ With Limitations
- Chaining `link()` calls (works structurally, but materialization issues)
- Materializing intermediate results in a chain (causes schema/DataFrame mismatch)

### ✗ Not Working
- Accessing linked columns directly through chained links
- Selecting linked columns from materialized results
- Using a materialized link result as the source for another link

## Recommended Pattern

For now, use this pattern for your link operations:

```python
# Pattern 1: Single Link (Always Works)
linked = orders.link(products, on=lambda o, p: o.product_id == p.product_id, as_="prod")
result = linked._materialize()

# Pattern 2: Link with Filter (Works)
linked = orders.link(products, on=..., as_="prod")
filtered = linked.filter(lambda r: r.quantity > 10)
# Then materialize if needed

# Pattern 3: Multiple Tables (Pre-join Approach)
# Instead of chaining links, join tables in the data pipeline before creating LTSeq instances
```

## Future Improvements (Phase 8L+)

To fix the chained materialization issue, we would need to:

1. **Column Name Consistency**: Ensure DataFrame column names match schema metadata after each join
2. **Schema Synchronization**: Rebuild schema from actual DataFrame columns after materialization
3. **Error Messages**: Provide clear errors when attempting problematic operations
4. **Alternative Approach**: Implement view-based chaining instead of materialization-based chaining

## Test Coverage

Phase 8K includes comprehensive error handling tests:
- Invalid join type rejection
- Non-existent column detection
- Schema validation
- Join type consistency
- Filter chaining
- Table preservation

See `py-ltseq/tests/test_phase8_linking.py::TestPhase8KErrorHandling` for full test suite.

## Summary

**Use Phase 8 linking for**:
- Single foreign key joins between two tables
- Filtering on source columns after joining
- All four join types (INNER, LEFT, RIGHT, FULL)

**Don't use Phase 8 linking for**:
- Complex multi-table chaining (use traditional SQL approach instead)
- Materializing intermediate join results for further joining

This is a solid MVP for the linking feature, with clear limitations documented for users.
