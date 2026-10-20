<!-- Generated from ../../en/01_hello_sequence.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 01 · Hello LTSeq: Think in Sequences

> **Level** ★☆☆ · **Domain** General · **Prereqs** None
>
> **APIs used**: `from_dict` / `filter` / `derive` / `select` / `sort` / `sort_keys` / `shift` / `cum_sum` / `show` / `to_dicts` / `to_pandas`

**Learning goals**

- Build the core mental model: **a table is an ordered sequence, and order is a computation input**
- Master the everyday chain `filter → derive → sort`
- Meet your first window functions (`shift`, `cum_sum`) and understand why windows require a prior sort

> 💡 **About outputs**: code cells ship without outputs. Each expected-output block is hand-written and checked against a real run on `main`; random data comes from fixed seeds, so reruns match.

## The problem

A 6-row order table (order id, user, amount). Four small tasks:

1. Keep orders above 100 and add a tax-inclusive amount;
2. Sort by amount, descending;
3. For each order, see the **previous order's amount** (window thinking 101);
4. Compute the **running total** of amounts (ordered aggregation 101).

Tiny questions — but each one exercises a core LTSeq capability.

```python
from ltseq import LTSeq

orders = LTSeq.from_dict({
    "order_id": ["ORD-001", "ORD-002", "ORD-003", "ORD-004", "ORD-005", "ORD-006"],
    "user":     ["u1", "u2", "u1", "u3", "u2", "u1"],
    "amount":   [120.0, 85.5, 240.0, 60.0, 310.0, 45.0],
})

orders.show();
```

> **Expected output**
>
> ```text
> +----------+------+--------+
> | order_id | user | amount |
> +----------+------+--------+
> | ORD-001  | u1   | 120    |
> | ORD-002  | u2   | 85.5   |
> | ORD-003  | u1   | 240    |
> | ORD-004  | u3   | 60     |
> | ORD-005  | u2   | 310    |
> | ORD-006  | u1   | 45     |
> +----------+------+--------+
> ```
>
> In Jupyter you can also just evaluate `orders` — LTSeq implements
> `_repr_html_` and renders a table preview.
>
> `show()` prints the table and returns it so you can keep chaining; the
> trailing `;` stops Jupyter from rendering that returned table a second time.

## Step 1 · Relational ops: filter / derive / select

```python
big = (
    orders
    .filter(lambda r: r.amount > 100)
    .derive(amount_with_tax=lambda r: r.amount * 1.1)
    .select("order_id", "user", "amount", "amount_with_tax")
)
big.show();
```

> **Expected output**
>
> ```text
> +----------+------+--------+-----------------+
> | order_id | user | amount | amount_with_tax |
> +----------+------+--------+-----------------+
> | ORD-001  | u1   | 120    | 132             |
> | ORD-003  | u1   | 240    | 264             |
> | ORD-005  | u2   | 310    | 341             |
> +----------+------+--------+-----------------+
> ```

**Notes**

- The `filter` lambda returns an **expression**, not a Python bool. LTSeq
  transpiles it into a DataFusion plan and pushes it down to the Rust engine —
  a big part of why LTSeq stays fast on large data.
- `derive` comes in two styles: keyword style `derive(new=lambda r: ...)` or
  dict style `derive(lambda r: {"new": ...})` (handy for several columns at once).
- `select` accepts plain column names or an expression list `lambda r: [r.a, r.b]`.
- **Nothing has executed yet**: every call just builds a lazy plan. Terminal
  operations (`show()` / `to_pandas()` / `count()`) trigger execution.

## Step 2 · Sorting — and sort *metadata*

Here is the biggest difference from pandas / SQL: `sort()` does more than
reorder rows. It records the **declared sort order** in table metadata, where
later window functions and ordered grouping read it.

```python
t = orders.sort("amount", desc=True)

print(t.sort_keys)        # declared order: [(column, descending?)]
```

> **Expected output**
>
> ```text
> [('amount', True)]
> ```

**Notes**

- `sort_keys` returns `[(column, descending)]`, or `None` when order is unknown.
- Calling a window function without a prior `sort()` (or `assume_sorted()`)
  raises `SortRequiredError`. That is a **feature, not a bug**: order is an
  input to computation, and LTSeq refuses to produce unreproducible results
  when order is unknown.
- Data already sorted on disk (e.g. pre-sorted Parquet)? Declare it with
  `assume_sorted("col")` and skip the physical sort.

## Step 3 · First window functions: shift & cum_sum

```python
result = (
    t
    .derive(prev_amount=lambda r: r.amount.shift(1))
    .cum_sum("amount")
    .select("order_id", "amount", "prev_amount", "amount_cumsum")
)
result.show();
```

> **Expected output**
>
> ```text
> +----------+--------+-------------+---------------+
> | order_id | amount | prev_amount | amount_cumsum |
> +----------+--------+-------------+---------------+
> | ORD-005  | 310    | None        | 310           |
> | ORD-003  | 240    | 310         | 550           |
> | ORD-001  | 120    | 240         | 670           |
> | ORD-002  | 85.5   | 120         | 755.5         |
> | ORD-004  | 60     | 85.5        | 815.5         |
> | ORD-006  | 45     | 60          | 860.5         |
> +----------+--------+-------------+---------------+
> ```

**Notes**

- `shift(1)` reads the previous row: positive offsets look backward, negative
  forward; boundaries are NULL (or use `shift(1, default=0)`). The table is
  sorted **descending**, so the running total also accumulates downward —
  **order defines semantics**. That is sequence thinking.
- `cum_sum("amount")` is the table-level shortcut and adds an `amount_cumsum`
  column. For a custom name use the expression form:
  `derive(x=lambda r: r.amount.cum_sum())`.
- Siblings: `cum_max()` / `cum_min()` (running extremes) and `diff()` (row
  differences) — case 06 builds its drawdown on `cum_max()`.

## Step 4 · Back to the Python ecosystem

```python
rows = result.to_dicts()
print(rows[0])

df = result.to_pandas()      # needs pandas; converted from Arrow
print(type(df).__name__)
```

> **Expected output**
>
> ```text
> {'order_id': 'ORD-005', 'amount': 310.0, 'prev_amount': None, 'amount_cumsum': 310.0}
> DataFrame
> ```

**Notes**

- `to_dicts()` returns rows as dicts (NULL → `None`) — great for small results.
- `to_arrow()` crosses the boundary over the Arrow C Data Interface with
  **shared, not copied, buffers**; `to_pandas()` then converts that Arrow
  table with pyarrow, which copies. Inbound works too: `from_arrow(pa_table)`
  shares buffers the same way, and any object implementing the Arrow
  PyCapsule interface (polars, duckdb, ...) plugs right in; `from_pandas(df)`
  converts to Arrow first.

## Key takeaways

1. **A table is an ordered sequence.** pandas / SQL treat tables as unordered
   sets where order is a display concern; LTSeq treats order as computation
   input. Windows, run detection, sessionization all stand on it.
2. **Lazy + pushdown.** Chains build a plan; terminal ops execute. Filters are
   pushed to storage, selects prune projections; below it all is a Rust +
   DataFusion + Arrow vectorized engine.
3. **Two kinds of lambdas.** Row lambdas (`filter` / `derive` / `sort`) get a
   row proxy `r`; grouping lambdas get a group proxy `g` (later cases).
4. **Sort before windows.** Sequence windows (`shift` / `rolling` / `cum_*` /
   `diff`) follow the declared order; without one you get `SortRequiredError`.
5. **Zero-friction interop.** Arrow in and out is zero-copy, pandas converts
   through Arrow; CSV and Parquet read out of the box.

## Exercises

- Generate an integer sequence with `seq(1, 10)` and verify 1+2+…+n with
  `cum_sum` (`from ltseq import seq`; like `range` it yields 1…9, and it has
  no declared order, so `sort("value")` first).
- Swap `LTSeq.from_dict(...)` for `LTSeq.read_csv("your.csv")` and rerun.
- Drop the `.sort(...)` on purpose and meet `SortRequiredError`.
- Further reading: the "Core mental model" section in `docs/USER_MODEL.md`.

---
**Previous**: [Table of contents](../../README.md) · **Next**: [02 · Stock indicators](02_stock_indicators.md)
