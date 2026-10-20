<!-- Generated from ../../en/10_sessionization.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 10 · Sessionization: Gap Detection & Ordered Grouping

> **Level** ★★★ · **Domain** Logs / monitoring · **Prereqs** Case 03
>
> **APIs used**: `shift(1, partition_by=)` / `if_else` / `cum_sum` / `group_ordered` / `agg` / `flatten()` / `__group_id__`

**Learning goals**

- Implement ClickBench-style **sessionization**: stamp session ids onto raw
  event streams that lack them
- Meet the in-group window mode: `shift(1, partition_by=)`
- Backfill group ids onto details with `flatten()`

**Where you will meet it**: LTSeq's own benchmark (`benchmarks/bench_vs.py`,
round 2) sessionizes the ClickBench `hits` dataset with the same gap flag — a new
session when the user changes or more than 30 minutes pass — and counts the
sessions with `group_ordered`.

## The problem

Event stream `events(user, ts, page)` with `ts` = seconds since day start.
Rule: **a gap over 30 minutes between consecutive rows of the same user opens
a new session**. Tasks:

1. Stamp every row with a session id;
2. Summarize each session: user, start, end, page count, duration (minutes);
3. Backfill: every detail row carrying its `__group_id__`.

```python
from ltseq import LTSeq

plans = {
    "u1": [(0, "home"), (120, "list"), (300, "detail"), (5400, "home"), (5700, "cart")],
    "u2": [(10, "home"), (60, "search")],
    "u3": [(5, "home"), (100, "list"), (2600, "detail"), (3000, "order")],
}
rows = []
for u, evs in plans.items():
    for ts, page in evs:
        rows.append({"user": u, "ts": ts, "page": page})

events = LTSeq.from_rows(rows).sort("user", "ts")
events.show();
```

> **Expected output**
>
> ```text
> +------+------+--------+
> | user | ts   | page   |
> +------+------+--------+
> | u1   | 0    | home   |
> | u1   | 120  | list   |
> | u1   | 300  | detail |
> | u1   | 5400 | home   |
> | u1   | 5700 | cart   |
> | u2   | 10   | home   |
> | u2   | 60   | search |
> | u3   | 5    | home   |
> | u3   | 100  | list   |
> | u3   | 2600 | detail |
> +------+------+--------+
> ```

`show()` prints the first 10 rows by default (`show(n)` prints more), so
u3's last event is cut off here.

Predict first, then verify: u1 has 2 sessions (gap 5100 > 1800), u2 has 1,
u3 has 2 (gap 2500) — 5 sessions in total.

## Step 1 · Gap flag: in-group shift + three-valued logic

`shift(1, partition_by="user")` reads the previous row **within the group**
and NULL at group boundaries — the universal opener for session-style
problems.

```python
from ltseq import if_else

flagged = (
    events
    .derive(prev_ts=lambda r: r.ts.shift(1, partition_by="user"))
    .derive(
        new_session=lambda r: if_else(
            r.prev_ts.is_null() | ((r.ts - r.prev_ts) > 1800), 1, 0
        ),
    )
    .cum_sum("new_session")           # → new_session_cumsum: session id
)
flagged.show();
```

> **Expected output**
>
> ```text
> +------+------+--------+---------+-------------+--------------------+
> | user | ts   | page   | prev_ts | new_session | new_session_cumsum |
> +------+------+--------+---------+-------------+--------------------+
> | u1   | 0    | home   | None    | 1           | 1                  |
> | u1   | 120  | list   | 0       | 0           | 1                  |
> | u1   | 300  | detail | 120     | 0           | 1                  |
> | u1   | 5400 | home   | 300     | 1           | 2                  |
> | u1   | 5700 | cart   | 5400    | 0           | 2                  |
> | u2   | 10   | home   | None    | 1           | 3                  |
> | u2   | 60   | search | 10      | 0           | 3                  |
> | u3   | 5    | home   | None    | 1           | 4                  |
> | u3   | 100  | list   | 5       | 0           | 4                  |
> | u3   | 2600 | detail | 100     | 1           | 5                  |
> +------+------+--------+---------+-------------+--------------------+
> ```

**Notes**

- Row one has `prev_ts` NULL. Under three-valued logic `TRUE | NULL = TRUE`,
  so `is_null() | (gap > 1800)` is **always true on the first row** — no
  special-casing needed; one condition covers both situations (same pattern as
  case 03's `changed`).
- `cum_sum("new_session")` adds `new_session_cumsum`: session ids are
  **globally monotonic**, and with users sorted, each session's rows are
  consecutive — exactly what `group_ordered` wants.

## Step 2 · Session summary: consecutive grouping collapse

`group_ordered` groups consecutive equal session ids; `agg` folds each into
one row.

```python
sessions = (
    flagged
    .group_ordered(lambda r: r.new_session_cumsum)
    .agg(lambda g: {
        "user":  g.first().user,
        "start": g.first().ts,
        "end":   g.last().ts,
        "pages": g.count(),
    })
    .derive(dur_min=lambda r: (r.end - r.start) / 60.0)
)
sessions.show();
```

> **Expected output**
>
> ```text
> +------+-------+------+-------+--------------------+
> | user | start | end  | pages | dur_min            |
> +------+-------+------+-------+--------------------+
> | u1   | 0     | 300  | 3     | 5                  |
> | u1   | 5400  | 5700 | 2     | 5                  |
> | u2   | 10    | 60   | 2     | 0.8333333333333334 |
> | u3   | 5     | 100  | 2     | 1.5833333333333333 |
> | u3   | 2600  | 3000 | 2     | 6.666666666666667  |
> +------+-------+------+-------+--------------------+
> ```

**Notes**

- **Why is duration computed after `agg`?** `agg` does not yet support
  arithmetic over aggregates (`(g.last().ts - g.first().ts)` errors) — fold
  `start` / `end` into columns first, then subtract in a table-level `derive`.
  This is the documented current limitation and its standard workaround.
- `group_ordered`'s key is a lambda (expressions allowed); groups emerge in
  appearance order — no hashing, no reordering.

## Step 3 · Backfill details: flatten

For a "detail rows + session id" table, `flatten()` expands with the group id
in `__group_id__`:

```python
labeled = (
    flagged
    .group_ordered(lambda r: r.new_session_cumsum)
    .flatten()
    .select("user", "ts", "page", "__group_id__")
)
labeled.show();
```

> **Expected output**
>
> ```text
> +------+------+--------+--------------+
> | user | ts   | page   | __group_id__ |
> +------+------+--------+--------------+
> | u1   | 0    | home   | 1            |
> | u1   | 120  | list   | 1            |
> | u1   | 300  | detail | 1            |
> | u1   | 5400 | home   | 2            |
> | u1   | 5700 | cart   | 2            |
> | u2   | 10   | home   | 3            |
> | u2   | 60   | search | 3            |
> | u3   | 5    | home   | 4            |
> | u3   | 100  | list   | 4            |
> | u3   | 2600 | detail | 5            |
> +------+------+--------+--------------+
> ```

**Notes**

- `flatten()` also carries internal helpers `__group_count__` / `__rn__`
  (in-group row number) — "Nth step within session" analyses come free.
- All four NestedTable verbs are now on the table: `agg` collapse / `derive`
  broadcast / `filter` segment predicate / `flatten` expand — pick by output
  shape.

## Key takeaways

1. **The sessionization trio** (also case 03's run trio): `in-group shift gap
   flag → cum_sum numbering → group_ordered collapse`. Change the threshold
   and the key, and it becomes card-swipe pairing, parking billing, chunked
   upload segmentation...
2. **`partition_by` localizes windows**: `shift` / `rolling` / `cum_*` all
   accept `partition_by=`, equivalent to SQL `OVER (PARTITION BY ...)`. For
   `cum_*` that is the expression form, `r.x.cum_sum(partition_by="user")`;
   the table-level `cum_sum("col")` used above takes no `partition_by`.
3. **Session ids = monotonic numbering**: guaranteed by "sort by user first +
   cum_sum never decreases"; a table with no declared order fails fast with
   `SortRequiredError` instead of silently scrambling. A declared order that
   does not start with `user` (say `sort("ts")`) is not caught: users'
   sessions interleave and `group_ordered` cuts them into pieces.
4. **Limitations worth memorizing**: no arithmetic over aggregates inside
   `agg`; the two-step dance (agg columns → derive combination) is standard.

## Exercises

- In-session step numbers: take `__rn__` from `flatten()` and pair with
  `shift(1)` for "previous page → current page" transition stats.
- Composite session keys: concatenate `user` and `device` into a key column
  first, e.g. `r.user.s.concat("|", r.device)`.
- Big-file edition: write the events to Parquet sorted by `user, ts`, read
  them back with `LTSeq.read_parquet(...).assume_sorted("user", "ts")`
  (case 12) and run this trio unchanged; `benchmarks/bench_vs.py` loads
  ClickBench `hits` the same way. A `scan` cursor will not do here: it yields
  raw Arrow batches, not a table.

---
**Previous**: [09 · RFM segmentation](09_rfm_segments.md) · **Next**: [11 · Error bursts](11_error_burst.md)
