<!-- Generated from ../../en/05_asof_join_quotes.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 05 · Trades Meet Quotes: asof_join

> **Level** ★★☆ · **Domain** Financial time series · **Prereqs** Case 01
>
> **APIs used**: `from_rows` / `sort` / `asof_join(on=, by=, strategy=)` / `derive` / `group_by().agg()`

**Learning goals**

- Understand as-of semantics: **find the latest row not after mine on another timeline**
- Keep `on` (time column) and `by` (group key) responsibilities straight
- Know the three `strategy` directions and why the right time column survives

## The problem

`trades` (time, symbol, price, qty) and `quotes` (time, symbol, bid, ask) are
**two independent timelines**: quotes every few seconds, trades at random.
Tasks:

1. For each trade, find **the latest same-symbol quote not after the trade
   time** (backward alignment);
2. Compute the spread at trade time: `spread = ask - bid`;
3. Summarize average spread and trade count per symbol.

```python
from ltseq import LTSeq

trades = (
    LTSeq.from_rows([
        {"time": 10, "symbol": "A", "px": 101.5, "qty": 200},
        {"time": 15, "symbol": "B", "px": 200.0, "qty": 100},
        {"time": 22, "symbol": "A", "px": 101.8, "qty": 300},
        {"time": 31, "symbol": "B", "px": 199.5, "qty": 150},
    ])
    .sort("time")                     # left table sorted by the time column
)

quotes = (
    LTSeq.from_rows([
        {"time": 5,  "symbol": "A", "bid": 100.9, "ask": 101.2},
        {"time": 5,  "symbol": "B", "bid": 199.8, "ask": 200.3},
        {"time": 20, "symbol": "A", "bid": 101.4, "ask": 101.9},
        {"time": 20, "symbol": "B", "bid": 199.6, "ask": 200.1},
    ])
    .sort("time")
)
print("trades:", trades.count(), "rows / quotes:", quotes.count(), "rows")
```

> **Expected output**
>
> ```text
> trades: 4 rows / quotes: 4 rows
> ```

## Step 1 · Backward alignment

One `asof_join` does it: for each trade, among same-symbol quotes with
`quote.time <= trade.time`, take the one with the greatest time.

```python
aligned = trades.asof_join(
    quotes,
    on="time",             # same-named time column on both sides
    by="symbol",           # each symbol is an independent timeline
    strategy="backward",   # direction: nearest right row not after the left
)
aligned.show();
```

> **Expected output**
>
> ```text
> +------+--------+-------+-----+------------+--------------+-------+-------+
> | time | symbol | px    | qty | time_right | symbol_right | bid   | ask   |
> +------+--------+-------+-----+------------+--------------+-------+-------+
> | 10   | A      | 101.5 | 200 | 5          | A            | 100.9 | 101.2 |
> | 15   | B      | 200   | 100 | 5          | B            | 199.8 | 200.3 |
> | 22   | A      | 101.8 | 300 | 20         | A            | 101.4 | 101.9 |
> | 31   | B      | 199.5 | 150 | 20         | B            | 199.6 | 200.1 |
> +------+--------+-------+-----+------------+--------------+-------+-------+
> ```

**Notes**

- **`on` is the time column, `by` is the group key** — clean separation:
  `by="symbol"` guarantees A's trades never match B's quotes. Differently
  named time columns? Use `left_on=` / `right_on=`.
- Three strategies: `backward` (latest before), `forward` (earliest after),
  `nearest`.
- **The right time column is kept** (suffixed on collision, e.g. `time_right`;
  the right `by` column survives too, as `symbol_right`): unlike an equi-join,
  as-of is approximate, so the matched quote time is real information (quote
  staleness, latency).
- The lambda form also works: `on=lambda t, q: t.time >= q.time` — the
  **comparison operator decides the direction** (`>=` backward, `<=` forward).

## Step 2 · Spread and summary

The join result is a plain `LTSeq`; everything after is case-01 material:
derive → group_by → agg.

```python
enriched = aligned.derive(
    spread=lambda r: (r.ask - r.bid).round(3),
    slippage=lambda r: (r.px - r.bid).round(3),
)

summary = enriched.group_by("symbol").agg(
    avg_spread=lambda g: g.spread.mean(),
    avg_slippage=lambda g: g.slippage.mean(),
    trades=lambda g: g.px.count(),
).sort("symbol")
summary.show();
```

> **Expected output**
>
> ```text
> +--------+------------+--------------+--------+
> | symbol | avg_spread | avg_slippage | trades |
> +--------+------------+--------------+--------+
> | A      | 0.4        | 0.5          | 2      |
> | B      | 0.5        | 0.05         | 2      |
> +--------+------------+--------------+--------+
> ```

**Notes**

- `group_by().agg()` returns groups in no particular order (it changes from run
  to run here), so sort before displaying.
- Inside `group_by("col").agg(...)`, the aggregation lambda receives a
  **column proxy**: `g.spread.mean()` / `g.px.count()` — dot the column, chain
  the method.
- Full aggregation menu: `sum / avg(=mean) / count / min / max / median / var /
  std / percentile(p) / top_k(k)`; conditional aggregations
  (`count_if / sum_if / ...`) and statistical ones (`corr / covar / skew /
  concat_agg`) live in the same context (`from ltseq import ...`).

## Key takeaways

1. **As-of ≠ equi-join**: exact-time joins miss almost everything; as-of
   matches by "nearest" semantics. In SQL this is a mouthful of
   `LATERAL JOIN`; here it is one operator.
2. **Order by time**: each side must be ordered by its time column, and
   `asof_join` sorts a side whose declared order does not start with it.
   Declare the order yourself with `sort("time")`, or with
   `assume_sorted("time")` on pre-sorted Parquet to skip the physical sort.
3. **Always pass `by` with multiple instruments**, or timelines cross-match.
4. **The right timestamp is information, not redundancy**: `time - time_right`
   is quote staleness — free data-quality monitoring.

## Exercises

- Align with `strategy="forward"` and compare fills to the *next* quote.
- Different column names? Use `left_on="time", right_on="quote_ts"`.
- Chain with case 02: rolling averages on the enriched table for a full
  slippage monitoring pipeline.

---
**Previous**: [04 · Ticks to candles](04_kline_ohlc.md) · **Next**: [06 · Max drawdown](06_max_drawdown.md)
