<!-- Generated from ../../en/06_max_drawdown.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 06 · Max Drawdown & Compounding: cum_max & fold

> **Level** ★★★ · **Domain** Financial time series · **Prereqs** Case 02
>
> **APIs used**: `cum_max()` / whole-table `agg` / `pct_change` / `fold(fn, init=, into=)` / `tail`

**Learning goals**

- One line for the drawdown series via the running peak (`cum_max()`)
- Meet LTSeq's **state machine operator** `fold`: a stateful loop as a declaration
- Extract scalar conclusions with whole-table `agg` (no `by`)

## The problem

Given 90 days of fund NAV:

1. **Drawdown series**: daily decline from the historical high
   `dd = nav / cummax(nav) - 1`;
2. **Max drawdown**: the minimum of `dd` (deeper = more negative);
3. **Compounded return**: `(1+r1)(1+r2)...` — a stateful loop by nature.

```python
import random

from ltseq import LTSeq

rng = random.Random(5)
nav, navs = 1.0, []
for _ in range(90):
    nav = max(0.2, nav * (1 + rng.gauss(0.001, 0.02)))
    navs.append(round(nav, 4))

t = LTSeq.from_dict({"day": list(range(1, 91)), "nav": navs}).sort("day")
t.head(3).show();
```

> **Expected output**
>
> ```text
> +-----+--------+
> | day | nav    |
> +-----+--------+
> | 1   | 0.9774 |
> | 2   | 0.956  |
> | 3   | 0.9697 |
> +-----+--------+
> ```

## Step 1 · Drawdown series: cum_max in one line

`cum_max()` is the running maximum — the highest NAV seen so far, i.e. the
high-water mark.

```python
nav_t = (
    t
    .derive(lambda r: {
        "peak": r.nav.cum_max(),
        "dd":   (r.nav / r.nav.cum_max() - 1).round(4),
    })
)

nav_mdd = nav_t.agg(mdd=lambda g: g.dd.min()).to_dicts()[0]["mdd"]
print("max drawdown =", round(nav_mdd * 100, 2), "%")

nav_t.select("day", "nav", "peak", "dd").tail(3).show();
```

> **Expected output**
>
> ```text
> max drawdown = -9.59 %
> +-----+--------+--------+---------+
> | day | nav    | peak   | dd      |
> +-----+--------+--------+---------+
> | 88  | 1.2898 | 1.3723 | -0.0601 |
> | 89  | 1.2597 | 1.3723 | -0.0821 |
> | 90  | 1.3093 | 1.3723 | -0.0459 |
> +-----+--------+--------+---------+
> ```

**Notes**

- The expression form `r.nav.cum_max()` lives inside `derive` so you name the
  column; the table-level shortcut family only covers `cum_sum`.
- `agg(mdd=...)` **without `by`** is whole-table aggregation — a scalar
  conclusion wrapped in a one-row table.
- `peak` and `dd` each compute independently (both read the original `nav`
  column): within one `derive` you cannot reference a column added in the same
  call — split into two derives when needed (case 02, key point 2).

## Step 2 · Compounding: fold as a state machine

Cumulative return cannot be expressed by "look at neighbors" windows — it
needs **state threaded row by row**: `s ← s × (1 + ret)`. Enter `fold` with
its three ingredients: `fn(state, row)`, `init`, `into` (output column).

```python
with_ret = nav_t.derive(ret=lambda r: r.nav.pct_change())

compounded = with_ret.fold(
    lambda s, r: s * (1 + (r["ret"] or 0.0)),   # row 1 has ret = NULL → treat as 0
    init=1.0,
    into="cum_return",
)

compounded.select("day", "nav", "ret", "cum_return").tail(3).show();
```

> **Expected output**
>
> ```text
> +-----+--------+-----------------------+--------------------+
> | day | nav    | ret                   | cum_return         |
> +-----+--------+-----------------------+--------------------+
> | 88  | 1.2898 | -0.027740087441579936 | 1.3196234908942095 |
> | 89  | 1.2597 | -0.023336951465343474 | 1.2888275015346842 |
> | 90  | 1.3093 | 0.03937445423513524   | 1.3395743810108454 |
> +-----+--------+-----------------------+--------------------+
> ```

**Notes**

- In `fold`, the row argument `r` is a **read-only view of the row**
  (`r["ret"]` or `r.ret`, NULL → `None`) and the callback is plain Python:
  `or`, `if`, helper functions — anything windows cannot express.
- The price: **a Python callback per row, non-lazy, whole-table
  materialization** — clearly slower than windows on big tables. Official
  guidance: **prefer windows whenever they can express the computation**;
  reach for fold only when you truly need arbitrary state.
- `partition_by="fund"` resets state to `init` at each partition start —
  multi-fund compounding in one call (see exercises).

## Key takeaways

1. **Windows vs state machines**: `cum_max` / `cum_sum` / `shift` cover
   "look around" needs; `fold` covers "carry state" needs. Think windows
   first, fold second.
2. **Whole-table `agg`**: omit `by` for a scalar conclusion; pair with
   `to_dicts()[0]` to inject into titles/alerts.
3. **fold's three ingredients**: `fn(s, r) -> new state`, `init`, `into`;
   the value written to `into` is also carried forward.
4. **Honesty note**: fold breaks laziness and materializes the table —
   reconsider on 100k+ row inputs.

## Exercises

- Multi-fund: add a `fund` column and `fold(..., partition_by="fund")`.
- Locate the max-drawdown **interval**: combine with case 03's run trio around
  the `dd` trough.
- Implement a "consecutive ERROR counter" with `fold` and compare with case
  11's window implementation.

---
**Previous**: [05 · Trades meet quotes](05_asof_join_quotes.md) · **Next**: [07 · Conversion funnel](07_funnel_analysis.md)
