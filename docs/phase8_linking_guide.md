# Phase 8: Pointer-Based Linking - User Guide

## Overview

ltseq's **linking feature** (Phase 8) provides a lightweight, pointer-based approach to foreign key relationships between tables. Instead of materializing expensive joins upfront, links create references that are evaluated only when needed.

**Key Benefits**:
- Lightweight: Links are lazy - no join execution until data access
- Intuitive: Express relationships as lambda conditions
- Flexible: All four join types supported (INNER, LEFT, RIGHT, FULL)
- Safe: Original tables unmodified; schema tracked throughout

## Quick Start

### Basic Join

```python
from ltseq import LTSeq

# Load tables
orders = LTSeq.read_csv("orders.csv")          # id, product_id, quantity
products = LTSeq.read_csv("products.csv")      # product_id, name, price

# Create a link (no join yet - just a reference)
linked = orders.link(
    products,
    on=lambda o, p: o.product_id == p.product_id,
    as_="prod"
)

# View the linked data
linked.show()

# Materialize when you need the actual joined data
result = linked._materialize()
```

**Output**:
```
| id | product_id | quantity | prod_product_id | prod_name | prod_price |
|----|------------|----------|-----------------|-----------|------------|
| 1  | 101        | 5        | 101             | Widget    | 50         |
| 2  | 102        | 3        | 102             | Gadget    | 75         |
| ... | ...        | ...      | ...             | ...       | ...        |
```

### Filtering Before Materialization

```python
# Filter on source columns (fast - no join needed)
high_qty = linked.filter(lambda r: r.quantity > 10)

# Still a linked table - join happens on materialization
high_qty.show()
```

### Using Different Join Types

```python
# INNER join (default) - only matching rows
inner = orders.link(products, on=..., as_="prod", join_type="inner")

# LEFT join - keep all orders, NULLs for unmatched products
left = orders.link(products, on=..., as_="prod", join_type="left")

# RIGHT join - keep all products, NULLs for unmatched orders
right = orders.link(products, on=..., as_="prod", join_type="right")

# FULL join - keep all rows from both tables
full = orders.link(products, on=..., as_="prod", join_type="full")
```

## Concepts

### Linking vs. Joining

**Link** (Phase 8):
- Lazy evaluation
- References stored, not data
- Lightweight metadata
- Ideal for exploration and filtering

**Join** (Traditional SQL):
- Eager evaluation
- All data materialized immediately
- Can be expensive for large tables

### Join Condition

Express the join condition as a lambda:

```python
# Simple single-column join
on=lambda o, p: o.product_id == p.product_id

# Composite key (multiple columns)
on=lambda o, p: (o.product_id == p.product_id) & (o.year == p.year)

# Complex conditions
on=lambda o, p: (o.id == p.order_id) & (o.status == "active")
```

### Column Naming

When you link with alias `"prod"`, all columns from products get prefixed:

```python
# Original orders columns: id, product_id, quantity
# Products columns: product_id, name, price

# After link with as_="prod":
# Orders columns (unchanged): id, product_id, quantity
# Products columns (prefixed): prod_product_id, prod_name, prod_price
```

Access the prefixed columns in filters/selects:
```python
linked.filter(lambda r: r.prod_price > 100)  # Filter on linked column
linked.select("id", "quantity", "prod_name")  # Select from both tables
```

### Materialization

Materialization executes the join and returns a regular `LTSeq`:

```python
linked = orders.link(products, on=..., as_="prod")

# Materialize on demand
result = linked._materialize()  # Now it's a regular LTSeq with joined data
result.show()
result.filter(lambda r: r.prod_price > 50)  # Normal LTSeq operations
```

**Note**: Materialization is cached. Multiple calls return the same object:
```python
result1 = linked._materialize()
result2 = linked._materialize()
assert result1 is result2  # Same object
```

## Use Cases

### Use Case 1: Enriching Orders with Product Details

```python
orders = LTSeq.read_csv("orders.csv")
products = LTSeq.read_csv("products.csv")

# Link orders to product information
linked = orders.link(
    products,
    on=lambda o, p: o.product_id == p.product_id,
    as_="prod"
)

# Filter for expensive items
expensive = linked.filter(lambda r: r.prod_price > 500)

# Materialize and display
expensive.show()
```

### Use Case 2: Multiple Joins with Filtering

```python
orders = LTSeq.read_csv("orders.csv")
products = LTSeq.read_csv("products.csv")
customers = LTSeq.read_csv("customers.csv")

# First join: orders to products
linked1 = orders.link(
    products,
    on=lambda o, p: o.product_id == p.product_id,
    as_="prod"
)

# Second join: linked result to customers
linked2 = linked1.link(
    customers,
    on=lambda l, c: l.customer_id == c.id,
    as_="cust"
)

# Filter and view
high_value = linked2.filter(lambda r: r.prod_price > 100)
high_value.show()
```

**Important**: See [Known Limitations](#known-limitations) for chaining restrictions.

### Use Case 3: LEFT Join for Missing Data

```python
orders = LTSeq.read_csv("orders.csv")
products = LTSeq.read_csv("products.csv")

# LEFT join keeps all orders, even if product not found
linked = orders.link(
    products,
    on=lambda o, p: o.product_id == p.product_id,
    as_="prod",
    join_type="left"  # Keep all orders
)

# Find orders with missing product info
missing = linked.filter(lambda r: r.prod_name is None)
missing.show()
```

### Use Case 4: Composite Key Join

```python
sales = LTSeq.read_csv("sales.csv")      # year, month, product_id, qty
pricing = LTSeq.read_csv("pricing.csv")  # year, month, product_id, price

# Join on multiple columns (price valid for specific year/month)
linked = sales.link(
    pricing,
    on=lambda s, p: (s.year == p.year) & (s.month == p.month) & (s.product_id == p.product_id),
    as_="price"
)

linked.show()
```

## API Reference

### `LTSeq.link()` Method

```python
linked = table.link(
    target,              # Target table to link to (LTSeq)
    on,                  # Join condition (lambda)
    as_,                 # Alias for linked columns (str)
    join_type="inner"    # Type of join: "inner", "left", "right", "full"
)
```

**Parameters**:
- `target` (LTSeq): Table to join with
- `on` (callable): Lambda expressing join condition
  - Example: `lambda l, r: l.id == r.user_id`
  - Can use `&` (AND) for composite keys
  - Cannot use `|` (OR) in conditions
- `as_` (str): Prefix for target columns in result
  - Example: `as_="prod"` creates `prod_name`, `prod_price`, etc.
- `join_type` (str): Type of join (default: "inner")
  - `"inner"`: Only matching rows
  - `"left"`: All left rows, NULLs from right
  - `"right"`: All right rows, NULLs from left
  - `"full"`: All rows from both tables

**Returns**: LinkedTable object

### LinkedTable Methods

#### `_materialize()`
Executes the join and returns an LTSeq with joined data.

```python
result = linked._materialize()
# result is now an LTSeq with all columns (source + prefixed target)
```

#### `show(n=10)`
Display joined data without materializing to a variable.

```python
linked.show(5)  # Show first 5 rows of joined result
```

#### `filter(predicate)`
Filter rows on source columns or materialized linked columns.

```python
# Filter on source columns (fast - no join)
filtered = linked.filter(lambda r: r.quantity > 10)

# Still a LinkedTable
assert isinstance(filtered, LinkedTable)
```

#### `select(*cols)`
Select specific columns.

```python
result = linked.select("id", "quantity", "prod_name")
# Returns LTSeq with only specified columns
```

#### `link(target, on, as_, join_type="inner")`
Chain another link (with limitations - see Known Limitations).

```python
linked1 = orders.link(products, on=..., as_="prod")
linked2 = linked1.link(categories, on=..., as_="cat")
# See Known Limitations for caveats
```

## Known Limitations

### Chained Materialization Issue

You cannot materialize a link, then link the result:

```python
# ❌ DON'T DO THIS:
linked1 = orders.link(products, on=..., as_="prod")
mat1 = linked1._materialize()      # Works
linked2 = mat1.link(categories, on=..., as_="cat")  # Works
mat2 = linked2._materialize()      # ❌ Problem!
```

**Workaround**: Materialize only at the end:

```python
# ✅ DO THIS INSTEAD:
linked1 = orders.link(products, on=..., as_="prod")
linked2 = linked1.link(categories, on=..., as_="cat")
result = linked2._materialize()  # Materialize at the very end
```

See [Phase 8J Limitations](phase8j_limitations.md) for technical details.

### Accessing Linked Columns in Lambdas

After materialization, you cannot easily access linked columns by object notation:

```python
linked = orders.link(products, on=..., as_="prod")
result = linked._materialize()

# ❌ This might not work:
filtered = result.filter(lambda r: r.prod_name == "Widget")

# ✓ This works better:
result.show()  # Manual inspection instead
```

As a workaround, filter on linked table before materialization:

```python
# Filter before materializing (works on source columns)
filtered = linked.filter(lambda r: r.quantity > 10)
filtered.show()
```

## Troubleshooting

### Issue: "Column not found" Error

```
TypeError: Invalid join condition: Column 'id' not found in schema
```

**Cause**: Join condition references wrong column name

**Fix**: Check column names in both tables:
```python
orders.show()    # See actual column names
products.show()

# Use correct names in lambda:
linked = orders.link(
    products,
    on=lambda o, p: o.product_id == p.product_id,  # ✓ Correct
    as_="prod"
)
```

### Issue: "Invalid join_type" Error

```
ValueError: Invalid join_type: 'outer'
```

**Cause**: Used unsupported join type

**Fix**: Use only: `"inner"`, `"left"`, `"right"`, `"full"`

```python
# ❌ Wrong
linked = orders.link(products, on=..., as_="prod", join_type="outer")

# ✓ Correct
linked = orders.link(products, on=..., as_="prod", join_type="full")
```

### Issue: Schema Shows Unexpected Column Names

After materialization, column names might not be what you expect:

```python
linked = orders.link(products, on=..., as_="prod")
result = linked._materialize()

# Check what columns actually exist:
print(result._schema.keys())

# Use actual column names
result.show()
```

## Best Practices

1. **Filter Early**: Filter on source columns before materialization for better performance
   ```python
   linked = orders.link(products, on=..., as_="prod")
   filtered = linked.filter(lambda r: r.quantity > 10)  # Fast
   result = filtered._materialize()  # Join only on filtered rows
   ```

2. **Materialize Late**: Don't materialize until you need actual joined data
   ```python
   # Good: Lazy evaluation
   linked = orders.link(products, on=..., as_="prod")
   filtered = linked.filter(lambda r: r.quantity > 10)
   
   # Bad: Eager evaluation
   result = orders.link(products, on=..., as_="prod")._materialize()
   filtered = result.filter(lambda r: r.quantity > 10)
   ```

3. **Use Appropriate Join Type**: Choose the right join for your use case
   ```python
   # INNER if you only want matching rows
   inner = orders.link(products, on=..., as_="prod", join_type="inner")
   
   # LEFT if you want all orders (even with missing products)
   left = orders.link(products, on=..., as_="prod", join_type="left")
   ```

4. **Single-Level Links**: For now, use single links. Multi-level chaining has limitations
   ```python
   # ✓ Good
   linked = orders.link(products, on=..., as_="prod")
   
   # ⚠ Use with caution - see Known Limitations
   linked = orders.link(products, on=..., as_="prod").link(categories, on=..., as_="cat")
   ```

## Examples

Complete, runnable examples are available in the `examples/` directory:

- `linking_basic.py` - Basic order-product join
- `linking_composite_keys.py` - Multi-column join keys
- `linking_join_types.py` - All four join type examples
- `linking_filtering.py` - Filtering patterns

See [Examples](#examples) section below for code snippets.

## What's Next?

- **Phase 8L** (Current): Documentation and user guides ✓
- **Phase 8M**: Performance optimization and benchmarking
- **Future**: Multi-level chaining improvements, better error messages

## Summary

ltseq's linking feature provides:
- ✅ Lightweight, lazy foreign key relationships
- ✅ All four join types (INNER, LEFT, RIGHT, FULL)
- ✅ Composite join keys
- ✅ Intuitive lambda-based conditions
- ✅ Safe operations (original tables unmodified)
- ⚠️ Single-level joins (multi-level chaining has limitations)

Use linking when you need to:
- Join tables on specific conditions
- Explore relationships without materializing everything
- Filter before joining for better performance
- Work with foreign key references

See [Known Limitations](#known-limitations) and [Troubleshooting](#troubleshooting) for edge cases.
