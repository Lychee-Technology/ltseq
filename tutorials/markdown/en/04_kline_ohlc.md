<!-- Generated from ../../en/04_kline_ohlc.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 04 · From Ticks to Candles: Ordered Grouping

> **Level** ★★★ · **Domain** Financial time series · **Prereqs** Cases 01-02
>
> **APIs used**: `dt.hour()/dt.minute()` / floor division `//` / `partition` / `group_sorted` / `g.first()/g.max()/g.sum()` / `concat` / `pivot`

**Learning goals**

- Time-bucket with component extraction + integer division (the bread and
  butter of candle aggregation)
- Choose between `group_by` (hash grouping) and `group_sorted` (ordered grouping)
- Split with `partition`, build per-symbol candles, `concat` them, then `pivot` wide

## The problem

A tick table (timestamp, symbol, price, volume) is too fine to read. Goals:

1. Aggregate into 5-minute candles: `open / high / low / close / vol / ticks`;
2. Produce them per symbol and stack into one long table;
3. `pivot` volume into a "time × symbol" matrix to compare activity.

**Why open/close matter**: within a bucket, row order must be preserved — the
first tick is the open, the last is the close. That is exactly what **ordered
grouping** is for.

```python
import random
from datetime import datetime, timedelta

from ltseq import LTSeq

rng = random.Random(11)
start = datetime(2026, 9, 1, 9, 30)
rows, px = [], {"600519": 1800.0, "000858": 160.0}
sec = 0
while sec < 3600:                                   # one hour from 09:30:00
    for sym in ("600519", "000858"):
        for k in range(rng.randint(1, 3)):          # 1-3 ticks, one second apart
            px[sym] = max(1.0, px[sym] * (1 + rng.gauss(0, 0.0015)))
            rows.append({
                "ts":     start + timedelta(seconds=sec + k),
                "symbol": sym,
                "price":  round(px[sym], 2),
                "volume": rng.randint(100, 900),
            })
    sec += rng.randint(8, 25)

ticks = LTSeq.from_rows(rows)
print(len(ticks), "ticks")
```

> **Expected output**
>
> ```text
> 863 ticks
> ```

**Notes**

- `ts` holds Python `datetime` values, which `from_rows` stores as a timestamp
  column; the `dt` accessor in step 1 needs a temporal column and rejects strings.
- Each burst of 1-3 ticks is spaced one second apart, so every tick of a symbol
  has its own timestamp. Otherwise open and close (the first and last tick of a
  bucket) would depend on how ties in `ts` happen to be ordered.
- `from_rows` builds a table from row dicts: column names come from the first
  row, and Arrow infers each column's type from the values.

## Step 1 · Time bucketing

Convert `HH:MM` into "minute of day", then integer-divide by 5 for the bucket
id; derive an `HH:MM` label back from the bucket id (used as the pivot row key).

```python
from ltseq.expr import concat_ws

staged = (
    ticks
    .derive(lambda r: {
        "bucket": (r.ts.dt.hour() * 60 + r.ts.dt.minute()) // 5,
    })
    .derive(lambda r: {
        "hh": r.bucket * 5 // 60,
        "mm": (r.bucket * 5) % 60,
    })
    .derive(lambda r: {
        "kline": concat_ws(":", r.hh.cast("string").s.pad_left(2, "0"),
                                r.mm.cast("string").s.pad_left(2, "0")),
    })
)
staged.select("ts", "symbol", "bucket", "kline").head(3).show();
```

> **Expected output**
>
> ```text
> +---------------------+--------+--------+-------+
> | ts                  | symbol | bucket | kline |
> +---------------------+--------+--------+-------+
> | 2026-09-01 09:30:00 | 600519 | 114    | 09:30 |
> | 2026-09-01 09:30:01 | 600519 | 114    | 09:30 |
> | 2026-09-01 09:30:00 | 000858 | 114    | 09:30 |
> +---------------------+--------+--------+-------+
> ```

**Notes**

- `r.ts.dt.hour()` / `dt.minute()` extract components; `//` is floor division
  (exact for integers) and `%` is the remainder — bucketing is one expression.
- `.s.pad_left(2, "0")` zero-pads to a fixed width; it works on strings only,
  so `cast("string")` the integer first. `concat_ws(":", a, b)` concatenates
  (from `ltseq.expr`, SQL `CONCAT_WS` equivalent).

## Step 2 · Per-symbol candles: partition + group_sorted

`partition("symbol")` splits the table into a dict-like container; inside each
partition we **sort by `("bucket", "ts")`** and use `group_sorted` — the
grouping key leads the declared sort, so each bucket forms exactly one group and
the in-bucket time order is preserved (open = first tick, close = last tick).

```python
def build_kline(sub):
    return (
        sub.sort("bucket", "ts")
        .group_sorted(lambda r: r.bucket)
        .agg(lambda g: {
            "kline":  g.first().kline,
            "symbol": g.first().symbol,
            "open":   g.first().price,
            "high":   g.max("price"),
            "low":    g.min("price"),
            "close":  g.last().price,
            "vol":    g.sum("volume"),
            "ticks":  g.count(),
        })
    )

kline_long = None
for sym, sub in staged.partition("symbol").items():
    part = build_kline(sub)
    kline_long = part if kline_long is None else kline_long.concat(part)

kline_long = kline_long.sort("kline", "symbol")
kline_long.head(6).show();
```

> **Expected output**
>
> ```text
> +-------+--------+---------+---------+---------+---------+-------+-------+
> | kline | symbol | open    | high    | low     | close   | vol   | ticks |
> +-------+--------+---------+---------+---------+---------+-------+-------+
> | 09:30 | 000858 | 159.68  | 162.3   | 158.38  | 162.08  | 22194 | 43    |
> | 09:30 | 600519 | 1803.54 | 1822.55 | 1797.11 | 1800.71 | 16135 | 36    |
> | 09:35 | 000858 | 162.22  | 163.26  | 161.14  | 161.75  | 17944 | 36    |
> | 09:35 | 600519 | 1801.46 | 1804.1  | 1781.34 | 1784.69 | 14294 | 30    |
> | 09:40 | 000858 | 161.39  | 161.39  | 158.71  | 160.16  | 15739 | 32    |
> | 09:40 | 600519 | 1786.83 | 1805.01 | 1776.38 | 1801.66 | 18078 | 40    |
> +-------+--------+---------+---------+---------+---------+-------+-------+
> ```

**Notes**

- **`group_by` vs `group_sorted`**: `group_by("col").agg(...)` is hash grouping
  with no order guarantee; `group_sorted` requires the grouping key to lead the
  declared sort (otherwise it raises `SortRequiredError`) and buys you groups in
  sequence order plus the in-group order that `g.first()` / `g.last()` read.
- `agg` returns only the columns the lambda maps, so `symbol` is carried along
  with `g.first().symbol`; without it the stacked table could not be sorted or
  read by symbol.
- open/close correctness rests entirely on the `ts` order inside
  `sort("bucket", "ts")` — once again, **order defines semantics**.
- `partition` returns a `PartitionedTable`: `parts[key]`, `items()`, `map(fn)`,
  `to_list()` — the general container for per-category modeling. `items()`
  yields the partitions in no fixed order, hence the sort after stacking.
- `concat` is UNION ALL semantics (keeps duplicates), perfect for stacking
  same-schema candle tables.

## Step 3 · Widen with pivot

Comparing two symbols is most natural as a "time × symbol" matrix:

```python
wide = kline_long.pivot(
    index="kline",
    columns="symbol",
    values="vol",
    agg_fn="sum",
).sort("kline")
wide.show();
```

> **Expected output**
>
> ```text
> +-------+--------+--------+
> | kline | 000858 | 600519 |
> +-------+--------+--------+
> | 09:30 | 22194  | 16135  |
> | 09:35 | 17944  | 14294  |
> | 09:40 | 15739  | 18078  |
> | 09:45 | 18476  | 17965  |
> | 09:50 | 19011  | 15301  |
> | 09:55 | 16980  | 18077  |
> | 10:00 | 17582  | 15761  |
> | 10:05 | 19874  | 15870  |
> | 10:10 | 18225  | 20471  |
> | 10:15 | 15149  | 16019  |
> +-------+--------+--------+
> ```

**Notes**

- `pivot(index=, columns=, values=, agg_fn=)` goes long → wide; `agg_fn`
  supports `sum` (the default) `/ mean / min / max / count`.
- Pivot rows come out in no particular order, hence the `.sort("kline")`;
  `show()` prints the first 10 of the 12 buckets.
- Missing combinations arrive as NULL (`count` gives 0). For zeros, overwrite
  the column in a `derive` with `.fill_null(0)`; these column names start with a
  digit, so reach them with `getattr(r, "600519")`.

## Key takeaways

1. **Time bucketing, three moves**: extract components (`dt.hour()/minute()`) →
   align arithmetically (`// 5 * 5`) → (optional) format a label. No magic
   resample function; everything composes.
2. **Two questions for ordered grouping**: does the grouping key lead the
   declared sort? do you need in-group order? Both "yes" → `group_sorted`;
   only stats matter → `group_by`.
3. **Split, model, stack**: `partition → per-group computation → concat` is the
   universal recipe for per-category modeling — more readable than a giant
   SQL and closer to LTSeq's sequence semantics.
4. **A candle is ordered aggregation**: `g.first()/g.last()` are row proxies
   (attribute access); `g.max()/g.min()/g.sum()/g.count()` are aggregate
   proxies (string column names) — do not mix the styles.

## Exercises

- Add `vwap`: derive `pv = price * volume` per tick first, then aggregate
  `g.sum("pv")` and `g.sum("volume")` as two columns and divide in a `derive`
  afterwards (agg arithmetic limitation, again).
- Fetch each bucket's three biggest prints with
  `sub.group_by("bucket").agg(top3=lambda g: g.volume.top_k(3))`. `top_k` is a
  `group_by` aggregate (the `group_sorted` group proxy has no `top_k`) and
  returns the values as one `;`-joined string.
- Parameterize the bucket width (1m/5m/15m) into a reusable `to_kline(ticks, minutes)`.

---
**Previous**: [03 · Golden cross](03_golden_cross.md) · **Next**: [05 · Trades meet quotes](05_asof_join_quotes.md)
