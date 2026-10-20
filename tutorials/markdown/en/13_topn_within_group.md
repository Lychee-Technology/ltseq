<!-- Generated from ../../en/13_topn_within_group.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 13 · Top-N Within Groups: Three Ways to Take the First N

> **Level** ★★☆ · **Domain** Finance · **Prereqs** Cases 01, 02
>
> **APIs used**: `sort` + `slice` / `rank().over(partition_by=, order_by=, descending=)` / `filter` / `drop` / `group_by().agg()` + `top_k`

**Learning goals**

- Master three ways to "take the top N": sort + slice, ranking window, and Top-N
  aggregation — and when each one fits
- Understand why `rank().over()` does **not** require a prior `sort()` — the sort
  intent travels inside `.over(order_by=)`
- See which spelling skips the full sort: `sort + slice` runs as a k-row
  **TopK**, while the per-group spellings still sort

## The problem

A trade table `trades(symbol, amount)` with 11 rows across three symbols. Goals:

1. The **global** top 3 by amount (as a baseline);
2. The top-3 **detail rows per symbol** (ties all kept at the same rank);
3. The three largest **amounts per symbol** (values only, no full rows).

```python
from ltseq import LTSeq

trades = LTSeq.from_rows([
    {"symbol": "T1", "amount": 310.0},
    {"symbol": "T1", "amount": 120.0},
    {"symbol": "T1", "amount": 240.0},
    {"symbol": "T1", "amount": 90.0},
    {"symbol": "T2", "amount": 480.0},
    {"symbol": "T2", "amount": 55.0},
    {"symbol": "T2", "amount": 365.0},
    {"symbol": "T2", "amount": 200.0},
    {"symbol": "T2", "amount": 150.0},
    {"symbol": "T3", "amount": 75.0},
    {"symbol": "T3", "amount": 95.0},
])

# show() prints 10 rows by default
trades.show(11);
```

> **Expected output**
>
> ```text
> +--------+--------+
> | symbol | amount |
> +--------+--------+
> | T1     | 310    |
> | T1     | 120    |
> | T1     | 240    |
> | T1     | 90     |
> | T2     | 480    |
> | T2     | 55     |
> | T2     | 365    |
> | T2     | 200    |
> | T2     | 150    |
> | T3     | 75     |
> | T3     | 95     |
> +--------+--------+
> ```

## Step 1 · Global Top-N: sort + slice

SQL's `ORDER BY amount DESC LIMIT 3` reads as "sort everything, take three
rows". In LTSeq that is `sort(desc=True)` + `slice(offset, length)` — the most
direct spelling.

```python
global_top3 = trades.sort("amount", desc=True).slice(0, 3)

global_top3.show();
```

> **Expected output**
>
> ```text
> +--------+--------+
> | symbol | amount |
> +--------+--------+
> | T2     | 480    |
> | T2     | 365    |
> | T1     | 310    |
> +--------+--------+
> ```

**Notes**

- `sort` + `slice(0, 3)` equals LIMIT 3. Note `slice` takes `slice(offset,
  length)`, not start/end indices.
- It does not sort the whole table: the engine fuses `sort` and `slice` into a
  **TopK** operator that keeps only the best 3 rows while it scans
  (`explain_plan()` shows `SortExec: TopK(fetch=3)`). The real pain is
  **per-group** Top-N: SQL needs a window function wrapped in a subquery to
  rank inside each group. That is what Step 2 fixes.

## Step 2 · Per-group Top-N: ranking window + filter

`rank().over(...)` carries its own sort semantics and does **not** require a
prior `sort()`. `partition_by=r.symbol` ranks independently inside each symbol;
`descending=True` orders by amount largest-first.

```python
from ltseq import rank

ranked = trades.derive(
    rk=lambda r: rank().over(
        partition_by=r.symbol, order_by=r.amount, descending=True
    ),
)
top3 = (
    ranked
    .filter(lambda r: r.rk <= 3)
    .drop("rk")
    .sort("symbol", "amount", desc=[False, True])
)

top3.show();
```

> **Expected output**
>
> ```text
> +--------+--------+
> | symbol | amount |
> +--------+--------+
> | T1     | 310    |
> | T1     | 240    |
> | T1     | 120    |
> | T2     | 480    |
> | T2     | 365    |
> | T2     | 200    |
> | T3     | 95     |
> | T3     | 75     |
> +--------+--------+
> ```

**Notes**

- T3 has only two rows and both survive `rk <= 3` — **when N exceeds the group
  size you get the whole group**, no special-casing needed.
- `rank()` gives tied values the same rank and skips after (1, 2, 2, 4). Use
  `row_number()` to break ties arbitrarily (be careful — ties make it
  nondeterministic), or `dense_rank()` to keep ranks gapless.
- This step does sort: the plan sorts all rows by `(symbol, amount)` before the
  window ranks them. The ranking window saves you the subquery, not the sort.
- The final `sort` orders the amounts inside each symbol too: sorting on
  `symbol` alone would leave each symbol's rows in no defined order.

## Step 3 · Values only, no rows: the top_k aggregation

Many reports only need "the three largest amounts per symbol", not the rows.
LTSeq provides `g.col.top_k(k)` in aggregation context — it returns the k
largest values joined into a string.

```python
by_symbol = (
    trades
    .group_by("symbol")
    .agg(top3_amounts=lambda g: g.amount.top_k(3))
    .sort("symbol")
)

by_symbol.show();
```

> **Expected output**
>
> ```text
> +--------+--------------+
> | symbol | top3_amounts |
> +--------+--------------+
> | T1     | 310;240;120  |
> | T2     | 480;365;200  |
> | T3     | 95;75        |
> +--------+--------------+
> ```

**Notes**

- `top_k(k)` is an **aggregation**: one row per group, no row explosion, the
  values largest-first as a `;`-joined string. It is not a k-sized heap,
  though: each group's values are collected in descending order and the first
  k kept, so memory grows with the group, not with k.
- Need whole rows (with timestamps and other columns) → use the ranking window
  from Step 2. Need only a value summary → `top_k` gives it in one row per group.
- `top_k`'s `k` must be an integer literal (default 10); it takes the k
  **largest**. For the smallest values per group, rank in ascending order
  (Step 2 without `descending=True`).

## Key takeaways

- **One table, three spellings**: global rows → `sort+slice`; per-group rows →
  `rank().over(partition_by=) + filter`; per-group values → `group_by().agg(top_k)`.
  Ask first: rows or values? grouped or not?
- **Ranking functions skip the sort declaration**: `rank/row_number/dense_rank/ntile`
  declare order via `.over(order_by=)`, while sequence windows (`shift/rolling`)
  consume the table's declared sort — two different rules worth keeping apart.
- **What each spelling costs**: only the global `sort + slice` runs as a
  bounded TopK; the ranking window and `top_k` both sort the rows first.
  `explain_plan()` shows which plan you got.

## Exercises

1. Swap `rank()` for `row_number()` and watch what happens to ties (add a second
   240.0 row to T1 first).
2. Use `ntile(4).over(partition_by=r.symbol, order_by=r.amount)` to bucket each
   symbol into quartiles and keep the priciest bucket.
3. Reverse requirement: the two **cheapest** trades per symbol. Hint:
   `row_number().over(partition_by=r.symbol, order_by=r.amount)` ascending.
4. Print `explain_plan()` for each of the three spellings (it returns the
   logical and the physical plan) and find the `SortExec` nodes: which one is a
   `TopK`, and what does each of the others sort by?

---
**Previous**: [12 · Streaming big files](12_streaming_cursor.md) · **Next**: [14 · YoY & MoM](14_yoy_mom.md)
