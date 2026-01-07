# Phase 8J & Beyond: Link Chaining - Known Limitations & Documentation

## Overview

Phase 8J implements structural support for chaining multiple `link()` calls, allowing you to join a table to another table, then join the result to a third table, etc.

Phase 9 (bug fix) resolved the chained materialization issue that was initially documented as a limitation.

**Status**: Fully functional. Single and multi-level chaining work perfectly.

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

# Filter on linked columns (Phase 10: transparent materialization)
filtered_linked = linked.filter(lambda r: r.prod_price > 100)

# Select source columns
selected = linked.select("id", "product_id", "quantity")

# Select linked columns (Phase 11: transparent materialization)
selected_linked = linked.select("prod_name", "prod_price")
```

### Chained Linking (Phase 9 Fixed)
```python
# ✅ THIS NOW WORKS:
linked1 = orders.link(products, on=..., as_="prod")
mat1 = linked1._materialize()  # ✓ Works
linked2 = mat1.link(categories, on=..., as_="cat")  # ✓ Works
mat2 = linked2._materialize()  # ✓ FIXED: Now works correctly!
```

You can materialize between link operations. Phase 9 resolved the schema/DataFrame mismatch issue.

## Tested Scenarios

### ✓ Fully Working
- Single link with all join types (INNER, LEFT, RIGHT, FULL)
- Filtering on source columns in a linked table
- Filtering on linked columns (Phase 10: automatic materialization)
- Selecting source columns from a linked table
- Selecting linked columns (Phase 11: automatic materialization)
- Creating a LinkedTable with `join_type` parameter
- Composite join keys (multi-column conditions)
- Schema includes all columns with proper prefixes
- Materialization caching (multiple calls to `_materialize()` return same object)
- Chaining `link()` calls (Phase 8J: structural support)
- Materializing intermediate results in a chain (Phase 9: schema/DataFrame mismatch fixed)
- Accessing linked columns in chained links
- Selecting linked columns from materialized results

### ✗ Never Supported
- Accessing linked columns directly without materialization (use `.select()` instead)

## Recommended Pattern

For most operations, use transparent materialization through `filter()` and `select()`:

```python
# Pattern 1: Single Link with Transparent Operations
linked = orders.link(products, on=lambda o, p: o.product_id == p.product_id, as_="prod")

# These automatically materialize if needed
filtered = linked.filter(lambda r: r.prod_price > 100)  # Linked column access
selected = linked.select("id", "prod_name", "prod_price")  # Mixed columns

# Pattern 2: Explicit Materialization When Needed
linked = orders.link(products, on=..., as_="prod")
result = linked._materialize()
result.show()

# Pattern 3: Multiple Tables with Chaining
linked1 = orders.link(products, on=..., as_="prod")
result1 = linked1._materialize()
linked2 = result1.link(categories, on=..., as_="cat")
result2 = linked2._materialize()
```

## Phases Summary

| Phase | Feature | Status |
|-------|---------|--------|
| 8 | Basic linking | ✅ Working |
| 8I | Join types (inner, left, right, full) | ✅ Working |
| 8J | Link chaining | ✅ Working |
| 9 | Chained materialization fix | ✅ Fixed |
| 10 | Filter on linked columns | ✅ Working |
| 11 | Select on linked columns | ✅ Working |
| 13 | Aggregate on linked tables | 🚧 In Progress |

## Test Coverage

- Phase 8K: Error handling tests
- Phase 9: Chained materialization tests (9/9 passing)
- Phase 10: Filter on linked columns tests (18/18 passing)
- Phase 11: Select on linked columns tests (10/10 passing)

See `py-ltseq/tests/test_phase8_linking.py` for full test suite (81 tests total).

## Summary

**Use Phase 8+ linking for**:
- Single or multiple foreign key joins between tables
- Filtering on source and linked columns
- Selecting from source and linked columns
- All four join types (INNER, LEFT, RIGHT, FULL)
- Chained linking with intermediate materialization

**Status**: Comprehensive linking support is complete and well-tested.

