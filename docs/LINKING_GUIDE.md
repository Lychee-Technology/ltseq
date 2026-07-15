# Linking Guide (LTSeq.link)

Related documents:

- `docs/README.md`: documentation index
- `docs/USER_MODEL.md`: user mental model and usage guidance
- `docs/ARCHITECTURE.md`: system architecture and execution model
- `docs/api.md`: API reference
- `docs/LINKING_GUIDE.cn.md`: Chinese linking guide

## Overview

`LTSeq.link()` expresses a foreign-key relationship as a **deferred,
prefix-aliased equi-join**. `link()` records the join condition and the
target's alias and computes the joined schema up front, but executes
nothing until you materialize the result. It is a lazy join — not a
pointer/take structure and not a cheap per-row navigation.

**Key points**:
- Lazy: no join execution until data access (collect / to_pandas / len / ...)
- Prefix-aliased: target columns become `{alias}_{col}`; source columns keep their names
- All four join types (INNER, LEFT, RIGHT, FULL)
- Transforms return a plain `LTSeq`, and their rows follow the join

## Quick Start

```python
from ltseq import LTSeq

orders = LTSeq.read_csv("orders.csv")          # id, product_id, quantity
products = LTSeq.read_csv("products.csv")      # product_id, name, price

# Create a link (no join yet — just the condition + schema)
linked = orders.link(
    products,
    on=lambda o, p: o.product_id == p.product_id,
    as_="prod",
)

# View the joined data (executes the join for display)
linked.show()

# Get the lazy joined table as a plain LTSeq
joined = linked.to_ltseq()

# Or execute and cache the result in memory
result = linked.collect()
```

**Output**:
```
| id | product_id | quantity | prod_product_id | prod_name | prod_price |
|----|------------|----------|-----------------|-----------|------------|
| 1  | 101        | 5        | 101             | Widget    | 50         |
| 2  | 102        | 3        | 102             | Gadget    | 75         |
```

### Transforms return an LTSeq

Every transform on a `LinkedTable` builds the lazy join plan and runs on top
of it, returning a plain `LTSeq`:

```python
# Filter can reference source columns (original names) and linked
# columns ({alias}_col) — both live in the joined schema.
cheap = linked.filter(lambda r: r.prod_price < 10)   # -> LTSeq
picked = linked.select("id", "quantity", "prod_name")  # -> LTSeq
```

Because a transform applies **after** the join, its rows follow the join:
an inner/right/full join drops or adds unmatched rows, and a one-to-many
match fans a source row out to several result rows — a subsequent
`slice`/`filter` sees the joined rows, not the original source rows.

### Using Different Join Types

```python
# INNER (default) — only matching rows
inner = orders.link(products, on=..., as_="prod", join_type="inner")

# LEFT — keep all orders, NULLs for unmatched products
left = orders.link(products, on=..., as_="prod", join_type="left")

# RIGHT — keep all products, NULLs for unmatched orders
right = orders.link(products, on=..., as_="prod", join_type="right")

# FULL — keep all rows from both tables
full = orders.link(products, on=..., as_="prod", join_type="full")
```

## Concepts

### link() vs. join()

`link()` and `join()` build the same kind of lazy DataFusion join. They
differ in the column-naming convention and ergonomics:

- **`link()`** namespaces the *whole* target table as `{alias}_col`, which
  keeps chained multi-hop joins unambiguous, and exposes `LinkedTable`
  chaining sugar. Use it for fact-to-dimension enrichment.
- **`join()`** uses Polars-style conflict-only suffixes and returns an
  `LTSeq` directly. Use it for a one-off relational join.

Neither materializes until the result is consumed.

### Join Condition

Express the join condition as a two-parameter lambda (equality only; use
`&` for composite keys, `|` is not supported):

```python
on=lambda o, p: o.product_id == p.product_id
on=lambda o, p: (o.product_id == p.product_id) & (o.year == p.year)
```

### Column Naming

Linking with alias `"prod"` prefixes every target column:

```python
# orders:   id, product_id, quantity          (kept as-is)
# products: product_id, name, price           (prefixed)
# joined:   id, product_id, quantity,
#           prod_product_id, prod_name, prod_price
```

Reference the prefixed columns in transforms:

```python
linked.filter(lambda r: r.prod_price > 100)
linked.select("id", "quantity", "prod_name")
```

### to_ltseq() and collect()

- `to_ltseq()` returns the **lazy** joined table as a plain `LTSeq` (no
  execution) — use it to keep composing with the full `LTSeq` API.
- `collect()` **executes** the join and returns an in-memory `LTSeq`
  (same semantics as `LTSeq.collect`).

The lazy plan is built once and cached, so repeated transforms reuse one
join node.

## Use Cases

### Enriching Orders with Product Details

```python
linked = orders.link(
    products, on=lambda o, p: o.product_id == p.product_id, as_="prod"
)
expensive = linked.filter(lambda r: r.prod_price > 500)  # -> LTSeq
expensive.show()
```

### Multi-Hop Chaining

A chained `link()` builds on the previous join's real plan, so the next
condition may reference the previous alias's columns:

```python
chained = (
    orders.link(products, on=lambda o, p: o.product_id == p.product_id, as_="prod")
          .link(customers, on=lambda r, c: r.prod_customer_id == c.id, as_="cust")
)
high_value = chained.filter(lambda r: r.prod_price > 100)  # -> LTSeq
high_value.show()
```

### LEFT Join for Missing Data

```python
linked = orders.link(
    products,
    on=lambda o, p: o.product_id == p.product_id,
    as_="prod",
    join_type="left",  # keep all orders
)
missing = linked.filter(lambda r: r.prod_name.is_null())
missing.show()
```

### Composite Key Join

```python
linked = sales.link(
    pricing,
    on=lambda s, p: (s.year == p.year) & (s.month == p.month) & (s.product_id == p.product_id),
    as_="price",
)
linked.show()
```

## API Reference

### `LTSeq.link()`

```python
linked = table.link(
    target,              # target table (LTSeq)
    on,                  # join condition (two-param lambda, equality only)
    as_,                 # alias prefixing every target column (str)
    join_type="inner",   # "inner" | "left" | "right" | "full"
)
```

**Returns**: `LinkedTable`.

### LinkedTable methods

- `to_ltseq() -> LTSeq` — the lazy joined table (no execution).
- `collect() -> LTSeq` — execute the join, return an in-memory LTSeq.
- `show(n=10)` — display up to `n` joined rows.
- `filter(predicate) -> LTSeq` — filter over the joined rows.
- `select(*cols) -> LTSeq` — project columns from the joined table.
- `derive(*args, **kwargs) -> LTSeq` — add columns (may reference linked columns).
- `sort(*keys, **kwargs) -> LTSeq` — sort the joined rows.
- `slice(offset, length) -> LTSeq` — slice the joined rows.
- `distinct(key_fn=None) -> LTSeq` — deduplicate the joined rows.
- `link(target, on, as_, join_type="inner") -> LinkedTable` — chain another link.

## Troubleshooting

### "Column not found" in the join condition

```
TypeError: Invalid join condition: Column 'id' not found in schema
```

The condition references a column absent from the respective table. Check
`orders.columns` / `products.columns` and fix the lambda.

### "Invalid join_type"

Use only `"inner"`, `"left"`, `"right"`, `"full"` (not `"outer"`).

### Unexpected joined column names

Target columns are prefixed with the alias. Inspect the joined schema:

```python
print(linked.to_ltseq().columns)   # id, ..., prod_name, prod_price, ...
```

### In-memory (`from_pandas`) source + column projection

Selecting a subset of columns after a join over an in-memory
(`from_pandas` / `from_arrow`) source can hit a DataFusion
ProjectionPushdown bug (a plain `join().select(...)` triggers it too). Read
the source from CSV/Parquet, or select after `collect()`, if you encounter
it.

## Best Practices

1. **Let the optimizer push down** — predicate and projection pushdown are
   the query optimizer's job; write the transform you mean and it runs over
   the joined plan. There is no "filter the source first for speed"
   shortcut anymore (it produced wrong rows for unmatched/fan-out joins).
2. **Materialize late** — keep composing on the lazy `LTSeq` from
   `to_ltseq()`; call `collect()` only when you want the result cached in
   memory.
3. **Choose the right join type** — INNER for matches only, LEFT to keep
   all source rows, etc. The join type is baked into the plan, so it
   survives every downstream transform.
4. **Reference linked columns by their prefix** — `r.prod_price`, not a
   nested `r.prod.price`.

## Summary

`LTSeq.link()` provides:
- Lazy, prefix-aliased equi-joins
- All four join types (INNER, LEFT, RIGHT, FULL) and composite keys
- Transforms that return a plain `LTSeq`, with rows following the join
- Multi-hop chaining, where each hop builds on the previous join's real plan
- `to_ltseq()` (lazy) and `collect()` (execute) to leave the LinkedTable surface
