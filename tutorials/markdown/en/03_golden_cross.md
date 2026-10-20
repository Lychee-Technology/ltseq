<!-- Generated from ../../en/03_golden_cross.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 03 · Golden Cross: Detecting State Runs

> **Level** ★★★ · **Domain** Financial time series · **Prereqs** Case 02
>
> **APIs used**: `group_ordered` / `NestedTable.agg` / `g.first()/g.last()/g.count()` / `cum_sum` / `search_first` / `shift` / `is_null` / `if_else`

**Learning goals**

- Learn LTSeq's signature trio: **flip flag → cum_sum numbering → group_ordered**
- Upgrade "point signals" into "interval views": turn any per-row test into runs
- Use `search_first` for find-first-then-stop lookups

## The problem

When the short MA crosses above the long MA it is a **golden cross**; the
reverse is a death cross. Traders care less about the crossing instant and more
about **bull runs**: complete stretches where `ma_5 > ma_20` — start, end,
length, and gain. Tasks:

1. Find all bull runs (state segments);
2. Locate the first day above the line;
3. Report run gains and pick the best one.

## Approach: from points to runs

```
above:    F F T T T F F T T T T
changed:  1 0 1 0 0 1 0 1 0 0 0     ← 1 where the state flips
seg_id:   1 1 2 2 2 3 3 4 4 4 4     ← cum_sum of flips = segment id
```

- `group_ordered` groups **consecutive equal** keys — exactly the definition
  of a run;
- `agg` collapses each run into one row: `g.first()` head, `g.last()` tail,
  `g.count()` length.

```python
import random

from ltseq import LTSeq, if_else

rng = random.Random(11)
price, closes = 50.0, []
for _ in range(40):
    price = max(5.0, price * (1 + rng.gauss(0.003, 0.025)))
    closes.append(round(price, 2))

t = (
    LTSeq.from_dict({"date": [f"D{i:02d}" for i in range(1, 41)], "close": closes})
    .sort("date")
    .derive(lambda r: {
        "ma_5":  r.close.rolling(5).mean().round(2),
        "ma_20": r.close.rolling(20).mean().round(2),
    })
    .derive(lambda r: {
        "above":   r.ma_5 > r.ma_20,
        "changed": if_else(
            r.ma_5.shift(1).is_null() | r.ma_20.shift(1).is_null(),
            1,
            if_else((r.ma_5 > r.ma_20) != (r.ma_5.shift(1) > r.ma_20.shift(1)), 1, 0),
        ),
    })
    .cum_sum("changed")                       # → changed_cumsum segment id
)
t.select("date", "close", "ma_5", "ma_20", "above", "changed_cumsum").head(5).show();
```

> **Expected output**
>
> ```text
> +------+-------+-------+-------+-------+----------------+
> | date | close | ma_5  | ma_20 | above | changed_cumsum |
> +------+-------+-------+-------+-------+----------------+
> | D01  | 48.62 | 48.62 | 48.62 | false | 1              |
> | D02  | 49.22 | 48.92 | 48.92 | false | 1              |
> | D03  | 50.6  | 49.48 | 49.48 | false | 1              |
> | D04  | 50.1  | 49.64 | 49.64 | false | 1              |
> | D05  | 48.59 | 49.43 | 49.43 | false | 1              |
> +------+-------+-------+-------+-------+----------------+
> ```

**Notes**

- The first-row special case uses `shift(1).is_null()`: row one has no
  previous MAs, so the flip test would be NULL and `if_else` would return 0.
  The guard makes row one always a segment start (case 10's sessionization
  guards its first row the same way).
- In the first five rows `ma_5` and `ma_20` average the same rows (partial
  frames), so they are equal and `above` is false.
- `cum_sum("changed")` adds `changed_cumsum`: +1 at every flip, so **segment
  ids are monotonic and constant within a segment**.

## Step 1 · Collapse into runs: group_ordered + agg

```python
runs = (
    t.group_ordered(lambda r: r.changed_cumsum)
    .agg(lambda g: {
        "state":   g.first().above,
        "start":   g.first().date,
        "end":     g.last().date,
        "days":    g.count(),
        "close_0": g.first().close,
        "close_1": g.last().close,
    })
)

bull_runs = (
    runs
    .filter(lambda r: r.state == True)          # bull runs only
    .derive(gain_pct=lambda r: ((r.close_1 / r.close_0 - 1) * 100).round(2))
    .select("start", "end", "days", "gain_pct")
)
bull_runs.show();
```

> **Expected output**
>
> ```text
> +-------+-----+------+----------+
> | start | end | days | gain_pct |
> +-------+-----+------+----------+
> | D06   | D08 | 3    | 4.58     |
> | D11   | D33 | 23   | 8.57     |
> | D39   | D40 | 2    | -3       |
> +-------+-----+------+----------+
> ```

**Notes**

- `group_ordered` returns a `NestedTable`; `.agg()` collapses each run into
  **one row** (SQL GROUP BY semantics) with groups in appearance order. To
  **broadcast** group stats back onto every row use `.derive()` (SQL window
  semantics) — the two methods are a matched pair.
- `g.first()` / `g.last()` return **row proxies** (attribute access);
  `g.count()` / `g.sum("col")` take **string** column names.
- **Current limitation**: `agg` does not support arithmetic over aggregates
  (e.g. `g.last().close - g.first().close`). Extract `close_0` / `close_1`
  first, then compute the gain in a table-level `derive`.

## Step 2 · First signal: search_first

Finding the first matching row does not need a full filter — `search_first` is
a lazy scan with `LIMIT 1` early exit.

```python
first_bull = t.search_first(lambda r: r.above == True)
first_bull.show()

best = (
    bull_runs
    .sort("gain_pct", desc=True)
    .head(1)
)
best.show();
```

> **Expected output**
>
> ```text
> +------+-------+-------+-------+-------+---------+----------------+
> | date | close | ma_5  | ma_20 | above | changed | changed_cumsum |
> +------+-------+-------+-------+-------+---------+----------------+
> | D06  | 48.65 | 49.43 | 49.3  | true  | 1       | 2              |
> +------+-------+-------+-------+-------+---------+----------------+
>
> +-------+-----+------+----------+
> | start | end | days | gain_pct |
> +-------+-----+------+----------+
> | D11   | D33 | 23   | 8.57     |
> +-------+-----+------+----------+
> ```

**Notes**

- `search_first(predicate)` returns a single-row `LTSeq` (zero rows if no
  match) — perfect for "first cross above the line / first big order / first
  alert" lookups.
- Calibration reminder: `ma_20` uses partial frames for the first 19 days.
  For a strict definition, filter out rows before
  `row_number().over(order_by=r.date) > 19` first.

## Key takeaways

1. **The run trio is LTSeq's soul**: `flip flag → cum_sum numbering →
   group_ordered`. Consecutive rises, sessions, alert episodes, badge pairing —
   all the same skeleton with different skin.
2. **group_ordered ≠ group_by**: the former groups **consecutive equal** keys
   without reordering, one O(n) pass; the latter is hash grouping with no
   order guarantee. For "group globally, keep order" use `group_sorted`
   (grouping key must lead the declared sort — case 04).
3. **NestedTable has four verbs**: `agg()` collapse / `derive()` broadcast /
   `filter()` group-level predicate (with `g.all()/any()/none()` quantifiers —
   case 11) / `flatten()` expand with `__group_id__` (case 10).
4. **`== True` on boolean columns** mirrors the repo's official examples —
   explicit is better than implicit.

## Exercises

- Find the longest **consecutive rising** run (`close > close.shift(1)` as the
  state).
- Emit a cross **event table**: rows where the state flips, with direction.
- Wrap the trio into `find_runs(t, flag_col)` and attach it with `pipe()`.

---
**Previous**: [02 · Stock indicators](02_stock_indicators.md) · **Next**: [04 · Ticks to candles](04_kline_ohlc.md)
