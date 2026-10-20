<!-- Generated from ../../en/17_rank_migration.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 17 · Rank Migration: Who Is Climbing Since Yesterday

> **Level** ★★★ · **Domain** Finance · **Prereqs** Case 13
>
> **APIs used**: `rank().over(partition_by=, order_by=, descending=)` / `cast("int64")` / `filter` / `select` / `rename` / `join(on=, how="left")` / `when` / `is_null`

**Learning goals**

- Answer the classic reporting question "how did the leaderboard move" with
  **partitioned ranking + a self join**: who rose, who fell, who is new
- Learn `join`'s column-naming rule: conflicting right-table columns get the
  `_right` suffix — renaming up front is the cleanest defense
- See how a left join's NULL naturally carries the "newcomer" meaning — NULL
  as information, again

## The problem

Two days of GMV leaderboards in `sales(day, product, gmv)`. Goals:

1. Rank each day independently (per-group, descending);
2. Using today's board as the base, label each product's **rank movement**:
   up / down / flat / new;
3. Think ahead: how would you find products that dropped **off** the board?

```python
from ltseq import LTSeq

sales = LTSeq.from_rows([
    {"day": "d1", "product": "A", "gmv": 900},
    {"day": "d1", "product": "B", "gmv": 700},
    {"day": "d1", "product": "C", "gmv": 500},
    {"day": "d1", "product": "D", "gmv": 300},
    {"day": "d1", "product": "E", "gmv": 100},
    {"day": "d1", "product": "F", "gmv": 400},
    {"day": "d2", "product": "A", "gmv": 850},
    {"day": "d2", "product": "B", "gmv": 950},
    {"day": "d2", "product": "C", "gmv": 520},
    {"day": "d2", "product": "D", "gmv": 610},
    {"day": "d2", "product": "E", "gmv": 90},
    {"day": "d2", "product": "G", "gmv": 700},
])

sales.show(12);
```

> **Expected output**
>
> ```text
> +-----+---------+-----+
> | day | product | gmv |
> +-----+---------+-----+
> | d1  | A       | 900 |
> | d1  | B       | 700 |
> | d1  | C       | 500 |
> | d1  | D       | 300 |
> | d1  | E       | 100 |
> | d1  | F       | 400 |
> | d2  | A       | 850 |
> | d2  | B       | 950 |
> | d2  | C       | 520 |
> | d2  | D       | 610 |
> | d2  | E       | 90  |
> | d2  | G       | 700 |
> +-----+---------+-----+
> ```

Note the two product **sets differ**: d2 lacks F and adds G, so the two boards
must be matched per product (a join here, a `pivot` in exercise 4), not by row
position. `show(12)` prints all 12 rows; the default is 10.

## Step 1 · Partitioned ranking: each board independent

`partition_by=r.day` keeps the two boards from bleeding into each other;
`descending=True` orders by GMV largest-first.

```python
from ltseq import rank

ranked = sales.derive(
    rk=lambda r: rank().over(
        partition_by=r.day, order_by=r.gmv, descending=True
    ),
).derive(rk=lambda r: r.rk.cast("int64"))   # rank() returns an unsigned integer; see the notes

ranked.sort("day", "rk").show(12);
```

> **Expected output**
>
> ```text
> +-----+---------+-----+----+
> | day | product | gmv | rk |
> +-----+---------+-----+----+
> | d1  | A       | 900 | 1  |
> | d1  | B       | 700 | 2  |
> | d1  | C       | 500 | 3  |
> | d1  | F       | 400 | 4  |
> | d1  | D       | 300 | 5  |
> | d1  | E       | 100 | 6  |
> | d2  | B       | 950 | 1  |
> | d2  | A       | 850 | 2  |
> | d2  | G       | 700 | 3  |
> | d2  | D       | 610 | 4  |
> | d2  | C       | 520 | 5  |
> | d2  | E       | 90  | 6  |
> +-----+---------+-----+----+
> ```

**Notes**

- Ranking functions need no prior `sort()` — `.over(order_by=...)` carries the
  order (case 13).
- `rank()` shares ranks on ties; switch to `row_number()` (force an order) or
  `dense_rank()` (ties without gaps) depending on the business contract.
- `rank()` currently returns an unsigned integer (`uint64`), although
  `ranked.schema` reports `int64`. Unsigned subtraction wraps around instead of
  going negative: without the cast, step 2's `prev_rank - rk` turns A's 1 − 2
  into 18446744073709551615. The second `derive` casts `rk` to `int64`; it is a
  separate step because `cast` cannot wrap a window expression yet.
- The output is still a long table (day on rows); rank migration wants both
  boards **aligned per product** — the next step moves d1's ranks onto d2's
  rows.

## Step 2 · Self join alignment: yesterday's rank onto today

Extract each day's rank column, rename yesterday's `rk` to `prev_rank`, then
left-join yesterday onto today — set differences become NULLs.

```python
today = (
    ranked
    .filter(lambda r: r.day == "d2")
    .select("product", "rk", "gmv")
)
yest = (
    ranked
    .filter(lambda r: r.day == "d1")
    .select("product", "rk")
    .rename(rk="prev_rank")
)

mig = (
    today.join(yest, on="product", how="left")
    .derive(delta=lambda r: r.prev_rank - r.rk)
    .sort("rk")
)

mig.show();
```

> **Expected output**
>
> ```text
> +---------+----+-----+-----------+-------+
> | product | rk | gmv | prev_rank | delta |
> +---------+----+-----+-----------+-------+
> | B       | 1  | 950 | 2         | 1     |
> | A       | 2  | 850 | 1         | -1    |
> | G       | 3  | 700 | None      | None  |
> | D       | 4  | 610 | 5         | 1     |
> | C       | 5  | 520 | 3         | -2    |
> | E       | 6  | 90  | 6         | 0     |
> +---------+----+-----+-----------+-------+
> ```

**Notes**

- `rename(rk="prev_rank")`: the keyword is the **old** column name, the value is
  the new one. Renaming up front means no `_right` suffix to scrub later —
  without it the conflicting right column would arrive as `rk_right`.
- `join(on="product", how="left")`: the same-named `product` column merges as the
  key; the left table dominates and unmatched rows (G) get NULL.
- `delta = prev_rank - rk`: **positive = climbed** (yesterday 5, today 4 → +1),
  negative = fell. G's `prev_rank` is NULL so `delta` is NULL too — three-valued
  logic propagates as usual, ready for the labeling step.
- Dropped-off F is absent: a left join keeps today's rows only. Finding
  drop-offs needs `anti_join` (exercise 2).

## Step 3 · Movement labels: the when chain closes it out

Check NULL first (newcomer), then classify by delta sign — same `when`-chain
discipline as case 14.

```python
from ltseq import when

labeled = mig.derive(
    move=lambda r: when(r.prev_rank.is_null(), "new")
    .when(r.delta > 0, "up")
    .when(r.delta == 0, "flat")
    .otherwise("down"),
)

labeled.show();
```

> **Expected output**
>
> ```text
> +---------+----+-----+-----------+-------+------+
> | product | rk | gmv | prev_rank | delta | move |
> +---------+----+-----+-----------+-------+------+
> | B       | 1  | 950 | 2         | 1     | up   |
> | A       | 2  | 850 | 1         | -1    | down |
> | G       | 3  | 700 | None      | None  | new  |
> | D       | 4  | 610 | 5         | 1     | up   |
> | C       | 5  | 520 | 3         | -2    | down |
> | E       | 6  | 90  | 6         | 0     | flat |
> +---------+----+-----+-----------+-------+------+
> ```

**Notes**

- `when(r.prev_rank.is_null(), ...)` leads the chain — NULL matches no
  comparison and would otherwise fall into `otherwise` and be mislabeled "down".
- `delta > 0` is also false when delta is NULL (NULL comparisons are NULL);
  with the leading guard the rest of the chain is safe.
- B climbed 2→1, C sank 3→5 — six rows tell the story better than flipping
  two boards. That is the value of turning order information into columns.

## Key takeaways

- **A general skeleton for migration reports**: partitioned rank → pick columns
  → rename → left join → delta → when labels. Swap in user retention boards,
  streamer gift boards, trending searches — the skeleton does not move.
- **Join naming rules** (Polars semantics): conflicting right columns get the
  `_right` suffix, the join key merges; an up-front `rename` is the most
  readable collision guard.
- **NULL carries "newcomer"**: left-join unmatched NULL, delta NULL, and the
  when-chain's is_null branch interlock — an extension of case 14's "NULL is
  information".

## Exercises

1. Three-day migration: add a d3 board to `sales`, use d3 as base and join
   both d2 (yesterday) and d1 (day before). Watch the column collisions:
   without renames the second join raises `ValueError` because `rk_right`
   already exists, so rename both rank columns.
2. Find **dropped-off** products: `yest.anti_join(today, on="product")` — F
   should surface.
3. Add `rk_change_abs` (absolute movement) to `labeled` and sort by it for the
   TOP 2 movers (hint: `r.delta.abs()`, and filter NULL deltas first; A, B
   and D tie at 1, so pick a tie-breaker).
4. Cross-check with a wide view:
   `ranked.pivot(index="product", columns="day", values="rk")` — F's d2 cell
   and G's d1 cell come back NULL, corroborating the join result.

---
**Previous**: [16 · Longest rising span](16_longest_rising_span.md) · **Next**: [18 · Composite session rules](18_session_composite.md)
