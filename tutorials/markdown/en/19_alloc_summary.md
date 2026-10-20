<!-- Generated from ../../en/19_alloc_summary.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 19 · Allocating Group Totals to Detail Rows

> **Level** ★★☆ · **Domain** E-commerce · **Prereqs** Case 01
>
> **APIs used**: `join(on=, how="left")` / `group_by().agg()` / `group_sorted` + `NestedTable.derive` (group broadcast) / `cast("float64")` / `sort`

**Learning goals**

- Learn two routes for "spread a group-level total back onto detail rows":
  **join back-fill** and **group broadcast** (SQL window semantics)
- Understand `NestedTable.derive` vs `NestedTable.agg`: broadcast **keeps every
  row**, agg collapses to one row per group
- Rehearse the integer-division trap once more — cast to `float64` before any
  ratio

## The problem

Monthly store sales `months(year, month, sales)` plus a small **annual fees
table** `fees(year, fee)` (say, yearly marketing spend). Goal: allocate each
year's fee onto its monthly rows in proportion to **each month's share of the
year's sales**, producing a `share` column.

This is a group-share problem at heart: `share = fee / sum(sales of year) *
sales` — the denominator is **group-level**, the numerator is **row-level**;
two levels must meet in one row.

```python
from ltseq import LTSeq

months = LTSeq.from_rows([
    {"year": 2023, "month": "2023-01", "sales": 30},
    {"year": 2023, "month": "2023-02", "sales": 40},
    {"year": 2023, "month": "2023-03", "sales": 50},
    {"year": 2024, "month": "2024-01", "sales": 20},
    {"year": 2024, "month": "2024-02", "sales": 30},
    {"year": 2024, "month": "2024-03", "sales": 30},
    {"year": 2024, "month": "2024-04", "sales": 40},
])

fees = LTSeq.from_rows([
    {"year": 2023, "fee": 12},
    {"year": 2024, "fee": 24},
])

months.show()
fees.show();
```

> **Expected output**
>
> ```text
> +------+---------+-------+
> | year | month   | sales |
> +------+---------+-------+
> | 2023 | 2023-01 | 30    |
> | 2023 | 2023-02 | 40    |
> | 2023 | 2023-03 | 50    |
> | 2024 | 2024-01 | 20    |
> | 2024 | 2024-02 | 30    |
> | 2024 | 2024-03 | 30    |
> | 2024 | 2024-04 | 40    |
> +------+---------+-------+
>
> +------+-----+
> | year | fee |
> +------+-----+
> | 2023 | 12  |
> | 2024 | 24  |
> +------+-----+
> ```

Predicted shares: 2023 totals 120, fee 12 → 3 / 4 / 5; 2024 totals 120, fee
24 → 4 / 6 / 6 / 8. Verify after running.

## Step 1 · Route one: join the group value back

The plainest idea: compute the group total, join it onto the detail table —
the two levels meet in the same row after the join.

```python
fees_joined = months.join(fees, on="year", how="left")

fees_joined.sort("month").show();
```

> **Expected output**
>
> ```text
> +------+---------+-------+-----+
> | year | month   | sales | fee |
> +------+---------+-------+-----+
> | 2023 | 2023-01 | 30    | 12  |
> | 2023 | 2023-02 | 40    | 12  |
> | 2023 | 2023-03 | 50    | 12  |
> | 2024 | 2024-01 | 20    | 24  |
> | 2024 | 2024-02 | 30    | 24  |
> | 2024 | 2024-03 | 30    | 24  |
> | 2024 | 2024-04 | 40    | 24  |
> +------+---------+-------+-----+
> ```

**Notes**

- `join(on="year", how="left")`: `year` is an integer column on both sides, so it
  is the key; all 7 left rows survive and the one-to-one match adds no rows.
- The two group-level inputs (fee, yearly sales total) come from **different
  places** — fee from an external table, the total from this table's own
  aggregate — so route one joins twice. Route two (broadcast) needs only the
  fee join.

## Step 2 · Add the yearly total and compute the share

The yearly total comes from grouping `months` itself; the share division
casts first — case 14's integer-division trap again.

```python
totals = months.group_by("year").agg(total=lambda g: g.sales.sum())

enriched = (
    fees_joined.join(totals, on="year", how="left")
    .derive(
        share=lambda r: r.fee.cast("float64") / r.total * r.sales,
    )
    .sort("month")
)

enriched.show();
```

> **Expected output**
>
> ```text
> +------+---------+-------+-----+-------+-------+
> | year | month   | sales | fee | total | share |
> +------+---------+-------+-----+-------+-------+
> | 2023 | 2023-01 | 30    | 12  | 120   | 3     |
> | 2023 | 2023-02 | 40    | 12  | 120   | 4     |
> | 2023 | 2023-03 | 50    | 12  | 120   | 5     |
> | 2024 | 2024-01 | 20    | 24  | 120   | 4     |
> | 2024 | 2024-02 | 30    | 24  | 120   | 6     |
> | 2024 | 2024-03 | 30    | 24  | 120   | 6     |
> | 2024 | 2024-04 | 40    | 24  | 120   | 8     |
> +------+---------+-------+-----+-------+-------+
> ```

**Notes**

- `12 / 120 * 30`: without the cast, integer `/` integer truncates to `0` and
  every share in the table becomes 0 — case 14's trap does far more damage on
  business data; build the reflex.
- Sanity check: within each group the shares must sum to the group's fee
  (3+4+5=12 ✔; 4+6+6+8=24 ✔). Allocation results always deserve a
  conservation check — that is what separates them from ordinary derived
  columns.
- After two joins the table carries redundant columns like `total`; `drop`
  them downstream if unneeded.

## Step 3 · Route two: group broadcast (one join)

`group_sorted` + `NestedTable.derive` **broadcasts** a group-level value to
every row of the group — the LTSeq form of SQL's
`sum() OVER (PARTITION BY year)`. The external fee is joined in first; the
yearly total is produced by the broadcast itself.

```python
nested = (
    fees_joined
    .sort("year", "month")
    .group_sorted(lambda r: r.year)
)

shared = nested.derive(lambda g: {"year_total": g.sum("sales")}).derive(
    share2=lambda r: r.fee.cast("float64") / r.year_total * r.sales,
)

shared.sort("month").show();
```

> **Expected output**
>
> ```text
> +------+---------+-------+-----+------------+--------+
> | year | month   | sales | fee | year_total | share2 |
> +------+---------+-------+-----+------------+--------+
> | 2023 | 2023-01 | 30    | 12  | 120        | 3      |
> | 2023 | 2023-02 | 40    | 12  | 120        | 4      |
> | 2023 | 2023-03 | 50    | 12  | 120        | 5      |
> | 2024 | 2024-01 | 20    | 24  | 120        | 4      |
> | 2024 | 2024-02 | 30    | 24  | 120        | 6      |
> | 2024 | 2024-03 | 30    | 24  | 120        | 6      |
> | 2024 | 2024-04 | 40    | 24  | 120        | 8      |
> +------+---------+-------+-----+------------+--------+
> ```

Identical to route one, one join fewer. ✔

**Notes**

- `group_sorted(lambda r: r.year)`: the grouping key must **lead the declared
  sort** — hence `sort("year", "month")` first; otherwise `group_sorted`
  raises `SortRequiredError`. Groups are runs of equal keys in that order, as
  with `group_ordered`.
- The lambda of `nested.derive(...)` returns a **dict**: one `year_total` per
  group, then broadcast to every row of that group — all detail rows survive,
  which is exactly the divide between `derive` and `agg` (collapse to one row).
- **Choosing between routes**: group value lives in another table → join; it
  is computed from this table → broadcast; both (as here) → join the external
  value first, then broadcast the local aggregate — one join overall.

## Key takeaways

- **Three weapons for meeting group and row levels**: join back-fill,
  `NestedTable.derive` broadcast, `NestedTable.agg` collapse. Allocation/share
  needs the first two; summaries need the third.
- **Broadcast = window semantics**: `nested.derive(lambda g: {...})` keeps all
  rows and attaches the group value — readers from SQL can treat it as the
  counterpart of a `PARTITION BY` window.
- **Conservation check is the ID card of allocations**: per-group share sum ==
  group fee, one `group_by().agg()` line away — make it a habit.

## Exercises

1. Add the conservation check to `shared`: `group_by("year").agg(sum_share=...,
  fee=...)` then `filter` for mismatched groups (should be empty).
2. Change the allocation rule to "proportional to sales × store count" (add a
  stores column) — notice only one expression changes.
3. Keep the months whose sales exceed their year's mean: broadcast
  `g.mean("sales")` with `nested.derive` first, then `filter` at table level.
  `NestedTable.filter(lambda g: ...)` keeps or drops whole groups, so it
  cannot pick single months.
4. Try deriving the fee from `months` itself (10% of yearly sales) so no join
  is needed anywhere — the limit form of the broadcast route.

---
**Previous**: [18 · Composite session rules](18_session_composite.md) · **Next**: [20 · Sparse reports](20_sparse_report.md)
