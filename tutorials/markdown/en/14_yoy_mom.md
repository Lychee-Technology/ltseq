<!-- Generated from ../../en/14_yoy_mom.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 14 · YoY & MoM: Ordered Reference with shift(12)

> **Level** ★★☆ · **Domain** Finance · **Prereqs** Case 01
>
> **APIs used**: `sort` / `shift(n)` / `shift(n, partition_by=)` / `cast("float64")` / `when` / `slice`

**Learning goals**

- Implement MoM (vs last month) and YoY (vs same month last year) with `shift(1)`
  and `shift(12)` — the most classic business use of window functions
- Learn LTSeq's **integer-division pitfall**: on `main`, `/` follows SQL
  semantics and truncates on integers — cast to `float64` before computing rates
- See the semantic value of NULL: the NULLs produced by `shift(12)` on the first
  12 rows say exactly "no prior-year base"

## The problem

A monthly GMV table `monthly(shop, month, gmv)` covering 2023-01 through
2024-12 (24 months). Goals:

1. Compute the MoM and YoY growth rates;
2. Label the MoM trend (jump / grow / drift / slide);
3. Extend to multiple shops: each shop's MoM computed inside its own partition.

```python
from ltseq import LTSeq

gmv_2023 = [820, 750, 910, 880, 960, 1020, 990, 1050, 1100, 1080, 1150, 1600]
gmv_2024 = [900, 830, 1000, 950, 1050, 1120, 1080, 1160, 1210, 1190, 1270, 1760]

rows = [
    {"shop": "S1", "month": f"2023-{m:02d}", "gmv": v}
    for m, v in enumerate(gmv_2023, start=1)
] + [
    {"shop": "S1", "month": f"2024-{m:02d}", "gmv": v}
    for m, v in enumerate(gmv_2024, start=1)
]

monthly = LTSeq.from_rows(rows).sort("month")

monthly.head(3).show();
```

> **Expected output**
>
> ```text
> +------+---------+-----+
> | shop | month   | gmv |
> +------+---------+-----+
> | S1   | 2023-01 | 820 |
> | S1   | 2023-02 | 750 |
> | S1   | 2023-03 | 910 |
> +------+---------+-----+
> ```

`YYYY-MM` strings sort chronologically under lexicographic order — a nice
property of the format. If months come from CSV you can sort without casting;
convert to a date only when you need `dt.*`
(`r.month.s.concat("-01").cast("date")`). This case only shifts, so no
conversion is needed.

## Step 1 · Shifting: shift(1) and shift(12)

Window functions need a declared order — `sort("month")` already registered it.
`shift(1)` reads the previous row; `shift(12)` reads twelve rows back, which is
exactly "same month, last year".

```python
shifted = monthly.derive(
    prev_month=lambda r: r.gmv.shift(1),
    prev_year=lambda r: r.gmv.shift(12),
)

# 2023-12 to 2024-02, across the year boundary
shifted.slice(11, 3).show();
```

> **Expected output**
>
> ```text
> +------+---------+------+------------+-----------+
> | shop | month   | gmv  | prev_month | prev_year |
> +------+---------+------+------------+-----------+
> | S1   | 2023-12 | 1600 | 1150       | None      |
> | S1   | 2024-01 | 900  | 1600       | 820       |
> | S1   | 2024-02 | 830  | 900        | 750       |
> +------+---------+------+------------+-----------+
> ```

**Notes**

- `slice(11, 3)` takes rows 12-14 (offset is 0-based): the last month of 2023
  and the first two months of 2024.
- `prev_year` for 2023-12 is NULL: only 11 rows precede it, so `shift(12)` runs
  past the start of the table, as it does for every month of 2023. That NULL is
  **information, not a defect** — "no prior-year base" should be NULL. From
  2024-01 on, `shift(12)` lands on the same month of 2023 (820 for 2024-01).
- `shift(1)` and `shift(12)` share one `derive` fine: the two new columns do not
  reference each other, each computes independently.

## Step 2 · Rates and trend labels

Growth rate = (current - base) / base. There is a must-know LTSeq trap here —
read the code first, the explanation after.

```python
from ltseq import when

rates = shifted.derive(
    mom=lambda r: (r.gmv.cast("float64") - r.prev_month) / r.prev_month,
    yoy=lambda r: (r.gmv.cast("float64") - r.prev_year) / r.prev_year,
).derive(
    mom_trend=lambda r: when(r.mom.is_null(), "no base")
    .when(r.mom > 0.05, "jump")
    .when(r.mom > 0, "grow")
    .when(r.mom > -0.05, "drift")
    .otherwise("slide"),
)

rates.slice(11, 3).show();
```

> **Expected output**
>
> ```text
> +------+---------+------+------------+-----------+----------------------+---------------------+-----------+
> | shop | month   | gmv  | prev_month | prev_year | mom                  | yoy                 | mom_trend |
> +------+---------+------+------------+-----------+----------------------+---------------------+-----------+
> | S1   | 2023-12 | 1600 | 1150       | None      | 0.391304347826087    | None                | jump      |
> | S1   | 2024-01 | 900  | 1600       | 820       | -0.4375              | 0.0975609756097561  | slide     |
> | S1   | 2024-02 | 830  | 900        | 750       | -0.07777777777777778 | 0.10666666666666667 | slide     |
> +------+---------+------+------------+-----------+----------------------+---------------------+-----------+
> ```

**Notes**

- **Integer-division trap**: `gmv` is Int64, and on `main` LTSeq's `/` follows
  SQL semantics — integer / integer **truncates** (issue #218). Without the cast,
  `(830 - 900) / 900` would yield `0`, not `-0.0778`! Cast the numerator with
  `cast("float64")` so float - integer → float and the rate survives.
- **NULL propagation**: when `prev_year` is NULL, `(x - NULL) / NULL` is NULL —
  SQL three-valued logic carries "no base" into the final metric with zero
  special-casing.
- **First branch of the `when` chain wins**: a NULL condition matches no
  comparison branch and falls straight to `otherwise` — the "no base" branch
  must come **first**, or 2023-01 gets mislabeled as "slide".
- yoy 0.1067 = (830 - 750) / 750: 2024-02 against 2023-02, exactly as intended.
- 2024-01 is a 44% "slide" month on month only because December is the
  seasonal peak; against 2023-01 it grew 9.8%. That is why reports carry both
  rates.

## Step 3 · Multiple shops: partition_by resets at group boundaries

With two shops mixed in one table, plain `shift(1)` would map S2's first month
onto S1's last. The window must be **partitioned per shop**.

```python
rows2 = rows + [
    {"shop": "S2", "month": "2024-11", "gmv": 500},
    {"shop": "S2", "month": "2024-12", "gmv": 640},
]

multi = (
    LTSeq.from_rows(rows2)
    .sort("shop", "month")
    .derive(prev_gmv=lambda r: r.gmv.shift(1, partition_by="shop"))
    .derive(
        mom_by_shop=lambda r: (r.gmv.cast("float64") - r.prev_gmv) / r.prev_gmv,
    )
)

multi.filter(lambda r: r.shop == "S2").show();
```

> **Expected output**
>
> ```text
> +------+---------+-----+----------+-------------+
> | shop | month   | gmv | prev_gmv | mom_by_shop |
> +------+---------+-----+----------+-------------+
> | S2   | 2024-11 | 500 | None     | None        |
> | S2   | 2024-12 | 640 | 500      | 0.28        |
> +------+---------+-----+----------+-------------+
> ```

**Notes**

- `shift(1, partition_by="shop")` returns NULL on **each shop's first row** — the
  window resets at group boundaries. This is LTSeq's spelling of SQL's
  `LAG() OVER (PARTITION BY shop ORDER BY month)`.
- Note `sort("shop", "month")`: the partitioned window orders each shop's rows
  by the declared sort, so `month` must be in it. Leading with `shop` lists each
  shop's rows together, because rows come back in the declared order.
- Deriving `prev_gmv` first and referencing it in the next step avoids writing
  `shift` twice — new columns inside one `derive` cannot reference each other
  (a hard LTSeq rule, see case 02).

## Key takeaways

- **One shift, one sentence**: `shift(1)` is MoM, `shift(12)` is YoY, and
  `shift(12, partition_by=)` is grouped YoY — three parameter tweaks of one
  function, where SQL writes `LAG(gmv, 12) OVER (PARTITION BY shop ORDER BY month)`.
- **Cast before rates**: on `main`, integer `/` integer truncates as in SQL
  (issue #218); write `cast("float64")` first for every rate, share, or mean
  difference.
- **NULL is part of the semantics**: out-of-bound shift NULLs, all-NULL group
  aggregates, and `otherwise` fallbacks — understand them or the output will
  quietly mislead you.

## Exercises

1. Add a `gmv_cum` column: year-to-date cumulative GMV (hint: plain
   `r.gmv.cum_sum()` keeps accumulating across years — derive a year column and
   use `cum_sum(partition_by=...)`).
2. Complete the YoY on the multi-shop table with
   `shift(12, partition_by="shop")` — S2 has only two months, so its yoy is
   always NULL; why is that the right answer?
3. Rewrite MoM with `pct_change()` and compare with the hand-written form.
   `pct_change()` divides like `/`, so cast first
   (`r.gmv.cast("float64").pct_change()`); it also has no `partition_by`
   parameter — how do you work around that for multi-shop data?
4. Change the trend labels to four bands: jump (>10%) / mild grow (0-10%) /
   mild drift (-10%-0) / slide (<-10%), and verify how branch order in the
   `when` chain matters.

---
**Previous**: [13 · Top-N within groups](13_topn_within_group.md) · **Next**: [15 · Payment-cycle grouping](15_payment_cycle.md)
