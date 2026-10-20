<!-- Generated from ../../en/16_longest_rising_span.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 16 · Longest Rising Span: Start/End Dates and Ties

> **Level** ★★☆ · **Domain** Finance · **Prereqs** Case 03
>
> **APIs used**: `sort` / `shift(1)` / `fill_null` / `group_ordered` / `NestedTable.agg` / `filter` / `rank().over(order_by=, descending=)` / `drop`

**Learning goals**

- Graduate from "longest rising **streak length**" to "longest rising **span**" —
  the business usually wants the start and end dates
- Fold each run into a (start, end, days) summary row with `NestedTable.agg`
- Handle ties correctly: `rank()` keeps all tied spans while `sort+slice` silently
  drops them — pick the right tool

## The problem

Daily closes `px(date, close)` for 10 trading days. Goals:

1. Find all rising spans (consecutive days where the close went up);
2. Report the longest span's **start and end dates** and its length;
3. If several spans tie for longest, **keep them all**.

```python
from ltseq import LTSeq

px = LTSeq.from_rows([
    {"date": "d01", "close": 10.0},
    {"date": "d02", "close": 10.5},
    {"date": "d03", "close": 11.0},
    {"date": "d04", "close": 10.8},
    {"date": "d05", "close": 11.5},
    {"date": "d06", "close": 12.0},
    {"date": "d07", "close": 11.9},
    {"date": "d08", "close": 12.5},
    {"date": "d09", "close": 12.4},
    {"date": "d10", "close": 13.0},
]).sort("date")

px.show();
```

> **Expected output**
>
> ```text
> +------+-------+
> | date | close |
> +------+-------+
> | d01  | 10    |
> | d02  | 10.5  |
> | d03  | 11    |
> | d04  | 10.8  |
> | d05  | 11.5  |
> | d06  | 12    |
> | d07  | 11.9  |
> | d08  | 12.5  |
> | d09  | 12.4  |
> | d10  | 13    |
> +------+-------+
> ```

Predicted rising spans: d02-d03, d05-d06, d08, d10 — **two spans tie for
longest (2 days each)**; step 2 must keep both.

## Step 1 · Marking and run grouping

Case 03 numbered its runs with a flip flag and `cum_sum`; here the `is_up` flag
itself can be the `group_ordered` key, because `group_ordered` already groups
consecutive equal values. The first row has no previous value, so the
comparison is NULL — `fill_null(False)` lands it explicitly on "not rising"
instead of leaving a NULL-only group.

```python
flagged = px.derive(
    is_up=lambda r: (r.close > r.close.shift(1)).fill_null(False),
)

runs = flagged.group_ordered(lambda r: r.is_up)

spans = runs.agg(lambda g: {
    "rising": g.first().is_up,
    "start": g.first().date,
    "end": g.last().date,
    "days": g.count(),
})

spans.show();
```

> **Expected output**
>
> ```text
> +--------+-------+-----+------+
> | rising | start | end | days |
> +--------+-------+-----+------+
> | false  | d01   | d01 | 1    |
> | true   | d02   | d03 | 2    |
> | false  | d04   | d04 | 1    |
> | true   | d05   | d06 | 2    |
> | false  | d07   | d07 | 1    |
> | true   | d08   | d08 | 1    |
> | false  | d09   | d09 | 1    |
> | true   | d10   | d10 | 1    |
> +--------+-------+-----+------+
> ```

**Notes**

- `(r.close > r.close.shift(1))` is NULL on the first row; `fill_null(False)`
  pins it to False. Without it the NULL forms its own key group — the result
  happens to stay correct (the NULL group is not a True group), but writing the
  fill makes the intent readable.
- `group_ordered` collects **consecutive equal** `is_up` rows into one run in
  declared order; `agg` folds each run into one summary: start `g.first().date`,
  end `g.last().date`, length `g.count()` — all plain group aggregates the docs
  allow.
- `spans` is an ordinary LTSeq again and chains filter/sort/derive freely —
  "reduce to a span table, then analyze as a plain table" is the standard
  closing move of ordered grouping.

## Step 2 · Tied longest spans: rank, not slice

Filter to rising spans and rank by length. `rank()` assigns the same rank to
ties, so `rk == 1` keeps **both** spans.

```python
from ltseq import rank

longest = (
    spans
    .filter(lambda r: r.rising)
    .derive(rk=lambda r: rank().over(order_by=r.days, descending=True))
    .filter(lambda r: r.rk == 1)
    .drop("rk")
    .sort("start")
)

longest.show();
```

> **Expected output**
>
> ```text
> +--------+-------+-----+------+
> | rising | start | end | days |
> +--------+-------+-----+------+
> | true   | d02   | d03 | 2    |
> | true   | d05   | d06 | 2    |
> +--------+-------+-----+------+
> ```

**Notes**

- Two spans tie at 2 days; `rank()` gives both rank 1 and `filter(rk == 1)`
  keeps them. `sort("days", desc=True).slice(0, 1)` would **silently drop one**:
  ranking semantics and slicing semantics diverge exactly at ties, and "find
  the longest" in business reporting usually means ties included.
- `rank().over(order_by=r.days, descending=True)` needs no prior `sort()`
  (ranking functions carry their own order, see case 13).
- The final `sort("start")` puts the tied spans in date order: the ranking
  window returns rows in window order (issue #202), and rows tied on `days`
  have no defined order among themselves.
- Want the "top two lengths" rather than "all tied longest"? Use `dense_rank()`
  then `filter(rk <= 2)` — settle the business definition first, then pick the
  function.

## Key takeaways

- **Length → span**: a span table answers "from when to when", not only "how
  many days". `g.first()/g.last()` inside `NestedTable.agg` are where start/end
  come from, as in case 03's bull runs — once the span table exists, everything
  after is plain table-level work.
- **Land boundary NULLs explicitly**: window-boundary NULLs must be handled
  sooner or later; one `fill_null(False)` beats relying on "NULL happens not to
  equal True".
- **The right tie behavior**: `rank() + filter(==1)` keeps ties; `slice` keeps
  one. For reporting, ties-included is usually the default contract.

## Exercises

1. Find the longest **falling** span (hint: `rising == False` gets close, but
   it also holds the first day, which `fill_null(False)` marked "not rising",
   and it would hold flat days — how do you leave them out?).
2. Add a `gain` column to `spans` (in-span return = last close / first
   close - 1) and find the span with the **largest gain** — longest and
   strongest are different questions. `agg` cannot do arithmetic on group
   values (case 03), so collect `g.first().close` and `g.last().close` as
   columns and divide in a `derive`.
3. Report only the **most recent** of the tied longest spans: number the rows
   of `longest` with `row_number().over(order_by=r.end, descending=True)` and
   keep row 1 — stacking the "longest" and "most recent" dimensions.
4. If closes can be flat, `is_up` splits "up + flat" into two runs — change the
   condition to `r.close >= r.close.shift(1)` and discuss which definition the
   business wants.

---
**Previous**: [15 · Payment-cycle grouping](15_payment_cycle.md) · **Next**: [17 · Rank migration](17_rank_migration.md)
