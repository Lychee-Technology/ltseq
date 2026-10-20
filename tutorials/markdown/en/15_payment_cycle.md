<!-- Generated from ../../en/15_payment_cycle.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 15 · Payment-Cycle Grouping: group_ordered in the Wild

> **Level** ★★★ · **Domain** Finance · **Prereqs** Case 03
>
> **APIs used**: `sort` / `shift(1, partition_by=)` / `if_else` / `is_null` / `cum_sum(partition_by=)` / `s.concat` / `cast("string")` / `group_ordered` / `NestedTable.agg`

**Learning goals**

- Apply the "run detection trio" from case 03 to a real business rule where the
  **group boundary is defined by a business event** (an invoice closing the cycle)
- Learn the standard move when the grouping key is not an existing column:
  derive a group number first, then build a composite key
- Compare SQL's roundabout spelling with LTSeq's direct one — and see why
  ordered grouping is worth its own primitive

## The problem

A billing table `bills(project, month, invoiced, amount)`: regular months carry
only the month's amount and no invoice (`invoiced=0`); closing months carry both
(`invoiced=1`). Rule: **a new payment cycle starts when the previous month has
invoiced=1**. Goal: for each cycle, report its closing month and the total
amount of the whole cycle.

```
project  month     invoiced  amount
AAA      2023-01   0         10      ┐
AAA      2023-02   0         15      ├ cycle 1 → closes 2023-03, total 40
AAA      2023-03   1         15      ┘
AAA      2023-04   0         10      ┐
AAA      2023-05   0         10      ├ cycle 2 → closes 2023-06, total 30
AAA      2023-06   1         10      ┘
```

Note: the grouping depends on **order** — this is not equivalence grouping
(`group_by` cannot see it), it is event-driven consecutive grouping.

```python
from ltseq import LTSeq

bills = LTSeq.from_rows([
    {"project": "AAA", "month": "2023-01", "invoiced": 0, "amount": 10},
    {"project": "AAA", "month": "2023-02", "invoiced": 0, "amount": 15},
    {"project": "AAA", "month": "2023-03", "invoiced": 1, "amount": 15},
    {"project": "AAA", "month": "2023-04", "invoiced": 0, "amount": 10},
    {"project": "AAA", "month": "2023-05", "invoiced": 0, "amount": 10},
    {"project": "AAA", "month": "2023-06", "invoiced": 1, "amount": 10},
    {"project": "BBB", "month": "2022-05", "invoiced": 0, "amount": 40},
    {"project": "BBB", "month": "2022-06", "invoiced": 1, "amount": 20},
    {"project": "BBB", "month": "2022-07", "invoiced": 0, "amount": 30},
    {"project": "BBB", "month": "2022-08", "invoiced": 1, "amount": 30},
])

bills = bills.sort("project", "month")
bills.show();
```

> **Expected output**
>
> ```text
> +---------+---------+----------+--------+
> | project | month   | invoiced | amount |
> +---------+---------+----------+--------+
> | AAA     | 2023-01 | 0        | 10     |
> | AAA     | 2023-02 | 0        | 15     |
> | AAA     | 2023-03 | 1        | 15     |
> | AAA     | 2023-04 | 0        | 10     |
> | AAA     | 2023-05 | 0        | 10     |
> | AAA     | 2023-06 | 1        | 10     |
> | BBB     | 2022-05 | 0        | 40     |
> | BBB     | 2022-06 | 1        | 20     |
> | BBB     | 2022-07 | 0        | 30     |
> | BBB     | 2022-08 | 1        | 30     |
> +---------+---------+----------+--------+
> ```

Expected answer: AAA closes 2023-03 / 2023-06 with totals 40 / 30; BBB closes
2022-06 / 2022-08 with totals 60 / 60. Verify after running.

## Step 1 · Marking cycle starts: in-group shift + three-valued logic

A "new cycle" appears on exactly two kinds of rows: each project's **first row**
(prev is NULL), and rows whose **previous month had invoiced=1**.
`shift(1, partition_by="project")` returns NULL at group boundaries, so one
expression covers both.

```python
from ltseq import if_else

marked = bills.derive(
    prev_invoiced=lambda r: r.invoiced.shift(1, partition_by="project"),
).derive(
    new_cycle=lambda r: if_else(
        r.prev_invoiced.is_null() | (r.prev_invoiced == 1), 1, 0
    ),
)

marked.show();
```

> **Expected output**
>
> ```text
> +---------+---------+----------+--------+---------------+-----------+
> | project | month   | invoiced | amount | prev_invoiced | new_cycle |
> +---------+---------+----------+--------+---------------+-----------+
> | AAA     | 2023-01 | 0        | 10     | None          | 1         |
> | AAA     | 2023-02 | 0        | 15     | 0             | 0         |
> | AAA     | 2023-03 | 1        | 15     | 0             | 0         |
> | AAA     | 2023-04 | 0        | 10     | 1             | 1         |
> | AAA     | 2023-05 | 0        | 10     | 0             | 0         |
> | AAA     | 2023-06 | 1        | 10     | 0             | 0         |
> | BBB     | 2022-05 | 0        | 40     | None          | 1         |
> | BBB     | 2022-06 | 1        | 20     | 0             | 0         |
> | BBB     | 2022-07 | 0        | 30     | 1             | 1         |
> | BBB     | 2022-08 | 1        | 30     | 0             | 0         |
> +---------+---------+----------+--------+---------------+-----------+
> ```

**Notes**

- The same skeleton as cases 03/10: **event/gap marker → running number →
  ordered grouping**. Only the marker condition changes — that is what makes it
  a pattern.
- `r.prev_invoiced.is_null() | (r.prev_invoiced == 1)`: combine booleans with
  `|`, never Python `or` (the lambda builds a plan, it does not evaluate
  eagerly).
- Marking the **start** of each cycle directly keeps the code close to the rule
  itself: "a new cycle starts when the previous month was invoiced".

## Step 2 · Numbering + composite key: turning runs into groups

`new_cycle` is a 0/1 marker; an in-group `cum_sum` turns it into a **cycle
number** (1, 1, 1, 2, 2, 2 ...). Grouping needs two keys (project, cycle_no),
since `cycle_no` restarts in every project, while `group_ordered` takes one key
expression — so build a composite key string.

```python
numbered = marked.derive(
    cycle_no=lambda r: r.new_cycle.cum_sum(partition_by="project"),
).derive(
    cycle_key=lambda r: r.project.s.concat("#", r.cycle_no.cast("string")),
)

numbered.show();
```

> **Expected output**
>
> ```text
> +---------+---------+----------+--------+---------------+-----------+----------+-----------+
> | project | month   | invoiced | amount | prev_invoiced | new_cycle | cycle_no | cycle_key |
> +---------+---------+----------+--------+---------------+-----------+----------+-----------+
> | AAA     | 2023-01 | 0        | 10     | None          | 1         | 1        | AAA#1     |
> | AAA     | 2023-02 | 0        | 15     | 0             | 0         | 1        | AAA#1     |
> | AAA     | 2023-03 | 1        | 15     | 0             | 0         | 1        | AAA#1     |
> | AAA     | 2023-04 | 0        | 10     | 1             | 1         | 2        | AAA#2     |
> | AAA     | 2023-05 | 0        | 10     | 0             | 0         | 2        | AAA#2     |
> | AAA     | 2023-06 | 1        | 10     | 0             | 0         | 2        | AAA#2     |
> | BBB     | 2022-05 | 0        | 40     | None          | 1         | 1        | BBB#1     |
> | BBB     | 2022-06 | 1        | 20     | 0             | 0         | 1        | BBB#1     |
> | BBB     | 2022-07 | 0        | 30     | 1             | 1         | 2        | BBB#2     |
> | BBB     | 2022-08 | 1        | 30     | 0             | 0         | 2        | BBB#2     |
> +---------+---------+----------+--------+---------------+-----------+----------+-----------+
> ```

**Notes**

- `r.new_cycle.cum_sum(partition_by="project")` is the **expression form** of
  cumulative sum: you control the output name and it accepts `partition_by`.
  The table-level `t.cum_sum("new_cycle")` adds a `*_cumsum` column that keeps
  counting across projects (BBB's cycles would be 3 and 4); the expression form
  restarts the count in each project.
- The composite key `project#cycle_no` is LTSeq's standard workaround for
  multi-key grouping (a grouping key is one expression; a list or tuple of keys
  raises `TypeError`): concat, group, split later if needed. Three lines of
  code buy real key flexibility.
- The declared sort is `(project, month)` and the running `cum_sum` advances in
  that same direction — that is the precondition for ordered accumulation.

## Step 3 · Collapsing cycles: closing month = group's last row

A cycle **ends** on its `invoiced=1` row, so the closing month is the group's
**last** row and the total is the sum over the whole group. `group_ordered`
turns each run of equal `cycle_key` values into one group, keeping its rows in
month order, and `agg` collapses each group into one row.

```python
summary = (
    numbered
    .group_ordered(lambda r: r.cycle_key)
    .agg(lambda g: {
        "project": g.first().project,
        "close_month": g.last().month,
        "total": g.sum("amount"),
    })
)

summary.show();
```

> **Expected output**
>
> ```text
> +---------+-------------+-------+
> | project | close_month | total |
> +---------+-------------+-------+
> | AAA     | 2023-03     | 40    |
> | AAA     | 2023-06     | 30    |
> | BBB     | 2022-06     | 60    |
> | BBB     | 2022-08     | 60    |
> +---------+-------------+-------+
> ```

Matches the expected answer: AAA 40/30, BBB 60/60. ✔

**Notes**

- `g.first()` / `g.last()` return row proxies with attribute access — the
  closing month is simply `g.last().month`; no extra `filter(invoiced==1)`
  needed (every cycle here closes on its last row; a trailing cycle that is not
  invoiced yet would report its latest month instead).
- `group_ordered(...).agg()` returns one row per cycle in sequence order, so no
  trailing `sort` is needed. `group_by().agg()` is the other option, but it is
  hash grouping with no defined output order, and its aggregates work on
  columns: there is no first or last row (`g.first()` raises
  `ColumnNotFoundError`).
- A common SQL spelling numbers the cycles with
  `sum(invoiced) OVER (PARTITION BY project ORDER BY month DESC)`: counting
  invoices backwards gives every row of a cycle the same number, because the
  invoice row closes its cycle. It works, but the reader has to run the
  timeline in reverse to check it. LTSeq's mark → number → group pipeline reads
  forward, and every step can be inspected with `show()` on its own.

## Key takeaways

- **The trio's third outing**: mark (shift + is_null) → number (cum_sum) →
  group (`group_ordered` on a composite key). Case 03 tracked bull runs, 10
  tracked sessions, 15 tracks billing cycles — same skeleton, different marker
  condition.
- **Multi-key grouping workaround**: LTSeq's `group_by` and `group_ordered` keys
  are one column or one expression; concatenating a composite key string is
  the standard move (or derive the composite column and use `group_sorted` —
  provided it leads the declared sort).
- **Expression cum_sum ≠ table cum_sum**: when you need a custom name, a
  `partition_by`, or a place inside a bigger expression, always use
  `r.col.cum_sum(partition_by=...)`.

## Exercises

1. Re-solve with `fold`: state = "amount accumulated in the current cycle",
   emit and reset when `invoiced==1`. Compare readability (fold is a per-row
   Python callback — remember the performance semantics from case 06).
2. Add `months` (cycle length) and `avg_monthly` to the summary — remember that
   `agg` does not support arithmetic over aggregates: produce `g.count()` first,
   then divide in a table-level derive (cast first, as in case 14).
3. If months can skip (gaps in the calendar), does the shift marker stay
   correct? Build a counterexample and decide whether month continuity should
   participate in the group definition.
4. Change the rule to "the cycle closes **two** months after invoicing" — how
   does the marker condition change?

---
**Previous**: [14 · YoY & MoM](14_yoy_mom.md) · **Next**: [16 · Longest rising span](16_longest_rising_span.md)
