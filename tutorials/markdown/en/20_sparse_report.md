<!-- Generated from ../../en/20_sparse_report.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 20 · Sparse Reports: Nulling Repeats Within Groups

> **Level** ★☆☆ · **Domain** E-commerce · **Prereqs** Case 01
>
> **APIs used**: `sort` / `shift(1)` / `if_else` / `&` combos / `fold` (forward fill)

**Learning goals**

- Build the Excel-style sparse report where repeated values show only on their
  first row: equal-to-previous becomes NULL
- Cascade the rule for multi-level headers: the lower level nulls only when
  the upper level also equals the previous row
- Do the reverse with `fold` — fill NULLs back with previous values (ffill),
  and see why it reads naturally as a fold

## The problem

Store sales `shops(region, city, sales)` with heavily repeated regions and
cities. For the exported report: **the region column shows only on the first
row of each region block; the city column shows only on the first row of each
(region, city) combination** — a sparse layout, extra repeats become NULL.

This is an adjacent-row comparison problem: each value relates only to its
**previous row**.

```python
from ltseq import LTSeq

shops = LTSeq.from_rows([
    {"region": "east", "city": "shanghai", "sales": 120},
    {"region": "east", "city": "nanjing", "sales": 80},
    {"region": "east", "city": "suzhou", "sales": 60},
    {"region": "north", "city": "beijing", "sales": 200},
    {"region": "north", "city": "tianjin", "sales": 90},
    {"region": "south", "city": "guangzhou", "sales": 150},
    {"region": "south", "city": "guangzhou", "sales": 110},
    {"region": "south", "city": "shenzhen", "sales": 95},
]).sort("region", "city", "sales")

shops.show();
```

> **Expected output**
>
> ```text
> +--------+-----------+-------+
> | region | city      | sales |
> +--------+-----------+-------+
> | east   | nanjing   | 80    |
> | east   | shanghai  | 120   |
> | east   | suzhou    | 60    |
> | north  | beijing   | 200   |
> | north  | tianjin   | 90    |
> | south  | guangzhou | 110   |
> | south  | guangzhou | 150   |
> | south  | shenzhen  | 95    |
> +--------+-----------+-------+
> ```

Strings sort lexicographically: within east, nanjing < shanghai < suzhou.
South's guangzhou appears twice — exactly what the combination-level nulling
must catch. `sales` is a third sort key only to order those two rows; without
it their order is not defined.

## Step 1 · Nulling the region column: compare with the previous row

`shift(1)` reads the previous row; equal means NULL. The first row has no
previous, its comparison is NULL, the equality fails, the original value
survives — NULL semantics land exactly right here.

```python
from ltseq import if_else

sparse1 = shops.derive(
    region_show=lambda r: if_else(
        r.region == r.region.shift(1), None, r.region
    ),
)

sparse1.show();
```

> **Expected output**
>
> ```text
> +--------+-----------+-------+-------------+
> | region | city      | sales | region_show |
> +--------+-----------+-------+-------------+
> | east   | nanjing   | 80    | east        |
> | east   | shanghai  | 120   | None        |
> | east   | suzhou    | 60    | None        |
> | north  | beijing   | 200   | north       |
> | north  | tianjin   | 90    | None        |
> | south  | guangzhou | 110   | south       |
> | south  | guangzhou | 150   | None        |
> | south  | shenzhen  | 95    | None        |
> +--------+-----------+-------+-------------+
> ```

**Notes**

- `r.region == r.region.shift(1)`: NULL on the first row →
  `if_else(NULL, ...)` takes the false branch → keeps the original. **No**
  `is_null()` guard needed; SQL three-valued logic has already arranged the
  first row for you.
- Passing the `None` literal in `if_else`'s true branch produces NULL; NULL
  coexists with any string column without type trouble.
- `region_show` coexists with `region`: the sparse column is a **new display
  column** — keeping the original is what supports later computation and
  restoration.

## Step 2 · Nulling the city column: cascading conditions

City nulls only when **both** region and city equal the previous row. The
second guangzhou row satisfies both → nulled.

```python
sparse2 = sparse1.derive(
    city_show=lambda r: if_else(
        (r.region == r.region.shift(1)) & (r.city == r.city.shift(1)),
        None,
        r.city,
    ),
)

sparse2.show();
```

> **Expected output**
>
> ```text
> +--------+-----------+-------+-------------+-----------+
> | region | city      | sales | region_show | city_show |
> +--------+-----------+-------+-------------+-----------+
> | east   | nanjing   | 80    | east        | nanjing   |
> | east   | shanghai  | 120   | None        | shanghai  |
> | east   | suzhou    | 60    | None        | suzhou    |
> | north  | beijing   | 200   | north       | beijing   |
> | north  | tianjin   | 90    | None        | tianjin   |
> | south  | guangzhou | 110   | south       | guangzhou |
> | south  | guangzhou | 150   | None        | None      |
> | south  | shenzhen  | 95    | None        | shenzhen  |
> +--------+-----------+-------+-------------+-----------+
> ```

**Notes**

- The cascading rule = several adjacent comparisons joined by `&`. Even if a
  city repeats across two regions (possible in other datasets), a different
  region forbids nulling — that is why the region comparison must be in the
  condition.
- The city rule references the original `region`/`city`, not `region_show` —
  sparse columns are display-only; judgments always hang off original
  columns. Keep the two responsibilities apart.
- Want the general "change detection" form? `r.x != r.x.shift(1)` is "changed
  on this row", from which changed-count and streak-length report metrics
  follow.

## Step 3 · The reverse: filling NULLs back (ffill)

To compute on the sparse table, NULLs are in the way. **Forward fill** is
fold's natural stage: state = the last non-empty value seen.

```python
filled = sparse2.fold(
    lambda s, r: r["region_show"] if r["region_show"] is not None else s,
    init=None,
    into="region_fill",
)

filled.select("region_show", "city_show", "sales", "region_fill").show();
```

> **Expected output**
>
> ```text
> +-------------+-----------+-------+-------------+
> | region_show | city_show | sales | region_fill |
> +-------------+-----------+-------+-------------+
> | east        | nanjing   | 80    | east        |
> | None        | shanghai  | 120   | east        |
> | None        | suzhou    | 60    | east        |
> | north       | beijing   | 200   | north       |
> | None        | tianjin   | 90    | north       |
> | south       | guangzhou | 110   | south       |
> | None        | None      | 150   | south       |
> | None        | shenzhen  | 95    | south       |
> +-------------+-----------+-------+-------------+
> ```

**Notes**

- `fold` advances state row by row: non-empty current value → state updates;
  empty → state passes through unchanged; either way the state lands in
  `region_fill`. This is pandas `ffill()` in one fold line.
- `fold` needs a declared order, so it runs on `sparse2` itself. Selecting
  the display columns first would drop the sort-key columns, and the declared
  order with them: `fold` would raise `SortRequiredError`. The `select` comes
  after, for display only.
- fold's lambda operates on **Python values** (the row is a read-only view:
  `r["region_show"]` or `r.region_show`), so `is not None` is an ordinary
  Python check — distinct from `.is_null()` in expression lambdas (case 25
  contrasts the two worlds again).
- **Why fold for ffill**: NULL propagation is stateful (it must remember the
  last non-empty value) while shift only sees a fixed offset; fold carries
  that state directly. It is not the only route: number the blocks with
  `cum_sum` over a "has a value" flag, then broadcast each block's
  `g.first().region_show` with `group_ordered(...).derive(...)` (case 10) —
  that stays lazy. fold is shorter to write, and case 06's performance caveat
  applies: fold materializes the table.

## Key takeaways

- **Adjacent comparison is all that sparse layout needs**: one `x ==
  x.shift(1)` yields "keep first row"; multi-level headers are `&` cascades of
  such comparisons.
- **Display columns and original columns separate**: `*_show` handles layout;
  judgments always read originals — the most common decay of report code is
  feeding display columns downstream.
- **ffill is a natural fold**: anything that must "remember the last time" is
  state, and fold carries it in one line. On a large table, the lazy route
  (`cum_sum` block numbers plus a `group_ordered` broadcast) avoids fold's
  materialization.

## Exercises

1. ffill the city column too (`city_fill`), then verify `region_fill +
  city_fill` reproduces the original combination exactly.
2. The reverse report — keep only rows that **differ** from the previous row.
  `filter` cannot hold `shift` itself, so derive the flag first:
  `derive(changed=lambda r: r.region != r.region.shift(1)).filter(lambda r: r.changed)`
  loses the first row (NULL comparison fails) — think how to win it back
  (`r.region.shift(1).is_null() |` in the condition).
3. Add a `run_len` column with fold: how many consecutive rows the current
  combination has been running (state is a counter: +1 when equal to
  previous, reset to 1 otherwise).
4. Do the conclusions survive unsorted input? Build interleaved-region data
  and rerun — "adjacent" always means adjacent **in declared order**.

---
**Previous**: [19 · Allocating group totals](19_alloc_summary.md) · **Next**: [21 · Basket co-occurrence pairs](21_basket_pairs.md)
