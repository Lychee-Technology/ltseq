<!-- Generated from ../../en/02_stock_indicators.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 02 · Stock Indicators: Moving Averages & Returns

> **Level** ★★☆ · **Domain** Financial time series · **Prereqs** Case 01
>
> **APIs used**: `rolling().mean()/std()` / `shift` / `pct_change` / `when` / `fill_null` / `round` / `write_csv` / `read_csv`

**Learning goals**

- Compute moving averages and rolling volatility with `rolling(n)`, and
  understand **partial-frame semantics**
- Compute daily returns with `pct_change()` and label states with
  `when().otherwise()`
- Round-trip CSV I/O and learn how to plug in real market data

## The problem

Given 60 trading days of closes, compute:

- `ma_5` / `ma_20`: 5-day and 20-day moving averages;
- `ret`: daily return vs the previous close;
- `vol_20`: 20-day rolling volatility (std of returns);
- `trend`: label vs the 5-day line (`above_ma5` / `below_ma5` / `at_ma5`).

Then surface the high-volatility days.

## Sample data

A seeded random walk for closes; write it to CSV and read it back to
demonstrate the I/O round-trip.

```python
import random

from ltseq import LTSeq

rng = random.Random(42)
price, closes = 100.0, []
for _ in range(60):
    price = max(5.0, price * (1 + rng.gauss(0.0005, 0.02)))
    closes.append(round(price, 2))

dates = [f"2026-{1 + i // 28:02d}-{1 + i % 28:02d}" for i in range(60)]

LTSeq.from_dict({"date": dates, "close": closes}).write_csv("stock.csv")

stock = LTSeq.read_csv("stock.csv")
stock.head(3).show();
```

> **Expected output**
>
> ```text
> +------------+-------+
> | date       | close |
> +------------+-------+
> | 2026-01-01 | 99.76 |
> | 2026-01-02 | 99.47 |
> | 2026-01-03 | 99.3  |
> +------------+-------+
> ```

**Notes**

- `write_csv()` / `read_csv()` just work. `read_csv()` infers the ISO
  `YYYY-MM-DD` column as a native `date32`, so `sort("date")` is
  chronological and `r.date.dt.*` works on it directly.
- Parquet stores column types instead of re-inferring them on every read:
  `write_parquet("s.parquet", compression="zstd")`.

## Step 1 · Indicators in two derives

Dict-style `derive` adds several columns at once. If columns depend on each
other (one needs another *new* column), split into two `derive`s (key point 2
below explains why). Here `vol_20` is the rolling std of the new `ret`
column, so it goes in a second `derive`.

```python
indicators = (
    stock
    .sort("date")
    .derive(lambda r: {
        "prev_close": r.close.shift(1),
        "ma_5":   r.close.rolling(5).mean().round(2),
        "ma_20":  r.close.rolling(20).mean().round(2),
        "ret":    r.close.pct_change(),
    })
    .derive(vol_20=lambda r: r.ret.rolling(20).std().round(4))   # reads the new `ret`
)
indicators.select("date", "close", "ma_5", "ma_20", "ret", "vol_20").head(4).show();
```

> **Expected output**
>
> ```text
> +------------+--------+-------+-------+-----------------------+--------+
> | date       | close  | ma_5  | ma_20 | ret                   | vol_20 |
> +------------+--------+-------+-------+-----------------------+--------+
> | 2026-01-01 | 99.76  | 99.76 | 99.76 | None                  | None   |
> | 2026-01-02 | 99.47  | 99.62 | 99.62 | -0.002906976744186109 | None   |
> | 2026-01-03 | 99.3   | 99.51 | 99.51 | -0.001709058007439446 | 0.0008 |
> | 2026-01-04 | 100.74 | 99.82 | 99.82 | 0.014501510574018104  | 0.0097 |
> +------------+--------+-------+-------+-----------------------+--------+
> ```

**Notes**

- `rolling(5).mean()` means SQL `ROWS BETWEEN 4 PRECEDING AND CURRENT ROW`:
  **partial frames** — near the head of the series it aggregates over however
  many rows exist, instead of emitting NULL like pandas `min_periods=5`. This
  is a deliberate LTSeq-vs-pandas behavioral difference to keep in mind when
  aligning indicators.
- Supported rolling aggregations: `mean / sum / min / max / count / std`.
- `pct_change()` = `(x - x.shift(1)) / x.shift(1)`; the first row is NULL.
  `vol_20` therefore starts on row 3: the sample std needs two non-NULL
  returns.
- Numeric modifiers like `.round(2)` chain directly onto window expressions.

## Step 2 · State labels and filtering

```python
from ltseq import when

labeled = indicators.derive(
    ret_pct=lambda r: (r.ret * 100).round(2).fill_null(0.0),
    trend=lambda r: when(r.close > r.ma_5, "above_ma5")
                    .when(r.close < r.ma_5, "below_ma5")
                    .otherwise("at_ma5"),
)

labeled.filter(lambda r: r.vol_20 > 0.018).select(
    "date", "close", "ma_5", "vol_20", "trend"
).head(5).show();
```

> **Expected output**
>
> ```text
> +------------+-------+--------+--------+-----------+
> | date       | close | ma_5   | vol_20 | trend     |
> +------------+-------+--------+--------+-----------+
> | 2026-02-05 | 95.55 | 101.56 | 0.0194 | below_ma5 |
> | 2026-02-06 | 94.43 | 99.46  | 0.0194 | below_ma5 |
> | 2026-02-07 | 92.75 | 97.48  | 0.0196 | below_ma5 |
> | 2026-02-08 | 94.42 | 95.59  | 0.0198 | below_ma5 |
> | 2026-02-09 | 95.72 | 94.57  | 0.02   | above_ma5 |
> +------------+-------+--------+--------+-----------+
> ```

**Notes**

- `when(cond, val).when(cond2, val2).otherwise(default)` is SQL `CASE WHEN` as
  a chain — far more readable than nested `if_else` (which also exists for the
  two-branch case).
- `fill_null(0.0)` fills day-one's NULL return; the inverse is filtering with
  `is_null()` / `is_not_null()`.
- `vol_20` is the std of daily returns, and the sample walk is drawn with a
  2% daily std, so `vol_20 > 0.018` flags the stretch where realized
  volatility climbs toward that level: it starts with the −5.2% day on
  2026-02-05.
- `select` prunes the plan: unused columns such as `ma_20` and `ret_pct` are
  never computed. The `vol_20` filter cannot move below the window that
  produces it, so it runs after that window.

## Plugging in real market data

Swap the data source and align the column names to `date` / `close`. The
other lines stay the same:

```python
# Three common ways (pick one):
#
# 1) Local CSV exported from your broker / akshare / yfinance, columns renamed
#    stock = LTSeq.read_csv("my_stock.csv")           # columns: date, close
#
# 2) Parquet (bigger data + native date types):
#    stock = LTSeq.read_parquet("my_stock.parquet")
#
# 3) Bridge from an existing pandas / polars / duckdb pipeline
#    (Arrow sources are zero-copy; pandas converts to Arrow first):
#    stock = LTSeq.from_pandas(df)
#    stock = LTSeq.from_arrow(pa_table)
#    stock = LTSeq.from_arrow(duckdb.sql("SELECT date, close FROM quotes"))
#
# Use one of them in place of the sample-data cell, then rerun Steps 1 and 2.
```

## Key takeaways

1. **Rolling uses partial frames**: near the head it aggregates over the rows
   that exist (SQL semantics), not pandas `min_periods` NULL padding. If you
   need "full window only", filter out the first n-1 rows with
   `row_number().over(order_by=r.date)`.
2. **Within one `derive`, new columns cannot reference each other**: expressions
   read the *original* columns. Split into two `derive`s (that is why `vol_20`
   and `ret_pct`, which read `ret`, come in later calls).
3. **Sort before windows**: `shift` / `rolling` / `pct_change` all rely on the
   declared order — drop `.sort("date")` and you get `SortRequiredError`.
4. **`read_csv` reads ISO dates as native `date32`**: they sort
   chronologically, and `r.date.dt.*` (case 04) works on them directly.

## Exercises

- Compute the 5-day MA slope: `r.ma_5 - r.ma_5.shift(3)`.
- Bucket volatility with `ntile(4).over(order_by=r.vol_20)`.
- Turn the ma5/ma20 relationship into a signal column — groundwork for case 03.

---
**Previous**: [01 · Hello LTSeq](01_hello_sequence.md) · **Next**: [03 · Golden cross](03_golden_cross.md)
