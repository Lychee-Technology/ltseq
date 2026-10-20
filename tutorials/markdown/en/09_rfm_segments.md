<!-- Generated from ../../en/09_rfm_segments.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 09 · RFM Segmentation: Aggregate, Rank, Bucket

> **Level** ★★★ · **Domain** E-commerce user behavior · **Prereqs** Cases 02, 08
>
> **APIs used**: `group_by().agg()` / `dt.diff` / `cast` / `ntile().over()` / `when` / constant-column `derive` (`LiteralExpr`)

**Learning goals**

- Compute R / F / M raw metrics in one aggregation
- Score quartiles with `ntile().over()` — ranking windows carry their own
  order, **no table-level sort needed**
- Emit classic segment labels with a `when` chain

## The problem

Order table `orders(user, order_id, date, amount)`, snapshot date
`2026-10-01`:

- **R** (Recency): days since the last purchase (smaller is better);
- **F** (Frequency): number of orders;
- **M** (Monetary): total spend.

Score R / F / M into quartiles (1-4), then label the classic segments:
Champion / Potential / Loyal / At-risk / General.

```python
from ltseq import LTSeq

orders = (
    LTSeq.from_rows([
        {"user": "u1", "order_id": "o1",  "date": "2026-09-28", "amount": 1200.0},
        {"user": "u1", "order_id": "o2",  "date": "2026-09-30", "amount": 380.0},
        {"user": "u2", "order_id": "o3",  "date": "2026-06-15", "amount": 90.0},
        {"user": "u3", "order_id": "o4",  "date": "2026-09-25", "amount": 260.0},
        {"user": "u3", "order_id": "o5",  "date": "2026-09-29", "amount": 640.0},
        {"user": "u3", "order_id": "o6",  "date": "2026-09-30", "amount": 150.0},
        {"user": "u4", "order_id": "o7",  "date": "2026-03-02", "amount": 45.0},
        {"user": "u5", "order_id": "o8",  "date": "2026-09-20", "amount": 800.0},
        {"user": "u6", "order_id": "o9",  "date": "2026-08-01", "amount": 300.0},
        {"user": "u7", "order_id": "o10", "date": "2026-09-30", "amount": 120.0},
    ])
    .sort("user", "date")
)
print(len(orders), "orders /", orders.distinct("user").count(), "users")
```

> **Expected output**
>
> ```text
> 10 orders / 7 users
> ```

## Step 1 · Aggregate RFM raw values

One `group_by().agg()` for all three metrics; `max` on ISO date strings gives
the latest purchase date (lexicographic = chronological).

```python
rfm = orders.group_by("user").agg(
    last_date=lambda g: g.date.max(),          # latest purchase (ISO string max)
    freq=lambda g: g.order_id.count(),         # frequency F
    monetary=lambda g: g.amount.sum(),         # monetary M
).sort("user")                                 # agg rows have no defined order
rfm.show();
```

> **Expected output**
>
> ```text
> +------+------------+------+----------+
> | user | last_date  | freq | monetary |
> +------+------------+------+----------+
> | u1   | 2026-09-30 | 2    | 1580     |
> | u2   | 2026-06-15 | 1    | 90       |
> | u3   | 2026-09-30 | 3    | 1050     |
> | u4   | 2026-03-02 | 1    | 45       |
> | u5   | 2026-09-20 | 1    | 800      |
> | u6   | 2026-08-01 | 1    | 300      |
> | u7   | 2026-09-30 | 1    | 120      |
> +------+------------+------+----------+
> ```

## Step 2 · Compute Recency

Derive the snapshot date as a **constant column**, then take the day
difference with `dt.diff`. The order dates here are strings, so cast them to
real dates first with `cast("date32")` (`read_csv` already reads ISO dates as
`date32`).

```python
from datetime import date
from ltseq.expr import LiteralExpr

rfm = (
    rfm
    .derive(snap=lambda r: LiteralExpr(date(2026, 10, 1)))   # constant column: snapshot
    .derive(
        recency=lambda r: r.snap.dt.diff(r.last_date.cast("date32")),
    )
)
rfm.select("user", "last_date", "recency", "freq", "monetary").show();
```

> **Expected output**
>
> ```text
> +------+------------+---------+------+----------+
> | user | last_date  | recency | freq | monetary |
> +------+------------+---------+------+----------+
> | u1   | 2026-09-30 | 1       | 2    | 1580     |
> | u2   | 2026-06-15 | 108     | 1    | 90       |
> | u3   | 2026-09-30 | 1       | 3    | 1050     |
> | u4   | 2026-03-02 | 213     | 1    | 45       |
> | u5   | 2026-09-20 | 11      | 1    | 800      |
> | u6   | 2026-08-01 | 61      | 1    | 300      |
> | u7   | 2026-09-30 | 1       | 1    | 120      |
> +------+------------+---------+------+----------+
> ```

**Notes**

- `derive(snap=lambda r: LiteralExpr(date(2026, 10, 1)))`: a `derive` lambda
  must return an expression, so a bare `date` raises `TypeError`; `LiteralExpr`
  (from `ltseq.expr`) turns the value into a constant column — parameterize
  the snapshot in one place.
- `dt.diff(other)` = `self - other`; `unit` defaults to `"day"` and date-minus-
  date returns whole days (float). Also supports
  `month / year / hour / minute / second`.
- Castable types: `int32/int64/float32/float64/utf8/string/bool/date32`.

## Step 3 · Quartile scores: ntile

`ntile(4).over(order_by=...)` splits rows into 4 near-equal buckets (1-4).
Ranking windows declare their own order — **no table sort required**. Unify
the direction to "higher is better" with `5 - bucket`.

```python
from ltseq import ntile

# ntile cuts by row position, so each key breaks ties by spend (see the notes)
rfm = rfm.derive(lambda r: {
    "r_score": 5 - ntile(4).over(order_by=r.recency * 1e6 - r.monetary),                # closer is better
    "f_score": 5 - ntile(4).over(order_by=r.freq * 1e6 + r.monetary, descending=True),  # more is better
    "m_score": 5 - ntile(4).over(order_by=r.monetary, descending=True),                 # higher is better
}).derive(lambda r: {     # `5 - ntile(...)` comes back as a decimal; make it an integer
    "r_score": r.r_score.cast("int64"),
    "f_score": r.f_score.cast("int64"),
    "m_score": r.m_score.cast("int64"),
})
rfm.select("user", "recency", "freq", "monetary", "r_score", "f_score", "m_score").sort("user").show();
```

> **Expected output**
>
> ```text
> +------+---------+------+----------+---------+---------+---------+
> | user | recency | freq | monetary | r_score | f_score | m_score |
> +------+---------+------+----------+---------+---------+---------+
> | u1   | 1       | 2    | 1580     | 4       | 4       | 4       |
> | u2   | 108     | 1    | 90       | 2       | 2       | 2       |
> | u3   | 1       | 3    | 1050     | 4       | 4       | 4       |
> | u4   | 213     | 1    | 45       | 1       | 1       | 1       |
> | u5   | 11      | 1    | 800      | 3       | 3       | 3       |
> | u6   | 61      | 1    | 300      | 2       | 3       | 3       |
> | u7   | 1       | 1    | 120      | 3       | 2       | 2       |
> +------+---------+------+----------+---------+---------+---------+
> ```

**Notes**

- Bucket numbers follow the `order_by` direction (`descending=True` flips it):
  bucket 1 is always the leading quarter. Ascending `recency` therefore maps
  directly; `freq` / `monetary` need `descending=True`.
- `5 - bucket` maps 1-4 to 4-1: bucket 1 (the best quarter) becomes 4 points.
- `ntile` cuts by row **position**, not by value: rows with equal keys land in
  buckets in whatever order the engine meets them, which can change between
  runs. `.over()` takes a single `order_by` expression, so the keys fold in
  spend as a tie-breaker: `r.recency * 1e6 - r.monetary` orders by recency,
  then bigger spender first (spend stays far below 1e6 here).
- `5 - ntile(...)` comes back as a `decimal` (an integer literal minus
  `ntile`'s unsigned integer), and `show()` cannot print decimals yet, so the
  second `derive` casts the scores to `int64`.
- 7 rows over 4 buckets: sizes 2/2/2/1 — `ntile` keeps buckets "roughly
  equal", earlier buckets absorbing the remainder.
- The ranking family: `row_number` (unique) / `rank` (ties skip) /
  `dense_rank` (ties don't skip) / `ntile` (buckets), all with
  `.over(partition_by=, order_by=)`.

## Step 4 · Segment labels: a when chain

Classic rules (R/F/M view):

- R high, F high, M high → **Champion**; R high, F low → **Potential**;
  R low, F high, M high → **Loyal**; R low, F low, M high → **At-risk**;
- everything else → General.

```python
from ltseq import when

rfm = rfm.derive(
    segment=lambda r: when(
        (r.r_score >= 3) & (r.f_score >= 3) & (r.m_score >= 3), "Champion",
    )
    .when((r.r_score >= 3) & (r.f_score <= 2), "Potential")
    .when((r.r_score <= 2) & (r.f_score >= 3) & (r.m_score >= 3), "Loyal")
    .when((r.r_score <= 2) & (r.m_score >= 3), "At-risk")
    .otherwise("General"),
).select(
    "user", "recency", "freq", "monetary",
    "r_score", "f_score", "m_score", "segment",
).sort("user")

rfm.show();
```

> **Expected output**
>
> ```text
> +------+---------+------+----------+---------+---------+---------+-----------+
> | user | recency | freq | monetary | r_score | f_score | m_score | segment   |
> +------+---------+------+----------+---------+---------+---------+-----------+
> | u1   | 1       | 2    | 1580     | 4       | 4       | 4       | Champion  |
> | u2   | 108     | 1    | 90       | 2       | 2       | 2       | General   |
> | u3   | 1       | 3    | 1050     | 4       | 4       | 4       | Champion  |
> | u4   | 213     | 1    | 45       | 1       | 1       | 1       | General   |
> | u5   | 11      | 1    | 800      | 3       | 3       | 3       | Champion  |
> | u6   | 61      | 1    | 300      | 2       | 3       | 3       | Loyal     |
> | u7   | 1       | 1    | 120      | 3       | 2       | 2       | Potential |
> +------+---------+------+----------+---------+---------+---------+-----------+
> ```

(u5 and u6 placed one order each yet score F = 3: `ntile` cuts by position,
so the five one-order users, ordered by spend, fill three F buckets. On data
this small and this tied, quartile scores reflect the tie-breaker as much as
the customer.)

**Notes**

- `when(cond, val).when(...).otherwise(val)` short-circuits **in order** —
  put the strictest rules first when they overlap.
- Combine booleans with `&` / `|` / `~` (SQL three-valued logic) — **never**
  Python's `and / or / not`.

## Key takeaways

1. **Three operator families in relay**: aggregation (`group_by().agg`) →
   ranking (`ntile().over`) → conditioning (`when` chain) — RFM is the
   smallest complete loop through all three.
2. **`.over()` carries its own order**: ranking windows need no table sort;
   add `partition_by` to score per channel/region independently.
3. **The constant-column pattern**: snapshot dates, thresholds, and other
   business parameters become columns; change them in one place.
4. **String dates are fine — with boundaries**: sorting and `max` work; the
   moment you do **date arithmetic**, `cast("date32")` or store native dates
   in Parquet.

## Exercises

- Deciles: `ntile(10)` for 1-10 scores; segment on the R+F+M sum.
- Add a `channel` column and `.over(partition_by=r.channel, ...)`: independent
  quartiles per channel.
- Replace the constant snapshot with `today()` (`from ltseq import today`)
  to auto-align with the run date (the UTC date).

---
**Previous**: [08 · User retention](08_user_retention.md) · **Next**: [10 · Sessionization](10_sessionization.md)
