<!-- Generated from ../../en/28_offline_segments.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 28 · Offline Segment Detection: Gap Thresholds and Duration Ranking

> **Level** ★★☆ · **Domain** IoT · **Prereqs** Cases 13, 18
>
> **APIs used**: `sort` / `shift(1, partition_by=)` / `filter` / `derive` / `rank().over(partition_by=, order_by=, descending=)` / `group_by().agg()` + `count_if` / `max_if`

**Learning goals**

- Translate "heartbeat timeout" into ordered computing: **adjacent gap >
  threshold ⇒ one offline event** — the interval's start and end hide inside
  the heartbeat rows themselves
- Rank offline events by duration per device with `rank().over()` and take
  each device's **worst one**
- Handle the NULL aggregate for devices with no offline — `max_if` returns
  NULL when nothing qualifies

## The problem

Device heartbeats `hb(device, ts)`, ts in minutes (one expected per minute).
Rule: **adjacent gap over 5 minutes ⇒ one offline event**, the interval
being (previous heartbeat, this heartbeat). Goals:

1. Find all offline intervals (start, end, duration);
2. Rank each device's outages by duration and report the **worst one**;
3. Summarize outage counts and the longest duration per device.

Prediction: device A heartbeats 0, 3, 6, 15, 18, 60, 63 → offline (6,15) 9
minutes and (18,60) 42 minutes; device B 0, 2, 4, 7 → none.

```python
from ltseq import LTSeq

hb = LTSeq.from_rows([
    {"device": "A", "ts": 0},
    {"device": "A", "ts": 3},
    {"device": "A", "ts": 6},
    {"device": "A", "ts": 15},
    {"device": "A", "ts": 18},
    {"device": "A", "ts": 60},
    {"device": "A", "ts": 63},
    {"device": "B", "ts": 0},
    {"device": "B", "ts": 2},
    {"device": "B", "ts": 4},
    {"device": "B", "ts": 7},
]).sort("device", "ts")

hb.show(11);
```

> **Expected output**
>
> ```text
> +--------+----+
> | device | ts |
> +--------+----+
> | A      | 0  |
> | A      | 3  |
> | A      | 6  |
> | A      | 15 |
> | A      | 18 |
> | A      | 60 |
> | A      | 63 |
> | B      | 0  |
> | B      | 2  |
> | B      | 4  |
> | B      | 7  |
> +--------+----+
> ```

## Step 1 · Gaps: in-group shift and differences

The usual trio: in-group `shift(1)` for the previous heartbeat, the
difference is the gap. The first row's prev is NULL, so gap follows as NULL
— first rows are naturally not offline events, and NULL semantics get it
right again.

```python
gapped = hb.derive(
    prev=lambda r: r.ts.shift(1, partition_by="device"),
).derive(
    gap=lambda r: r.ts - r.prev,
)

gapped.show(11);
```

> **Expected output**
>
> ```text
> +--------+----+------+------+
> | device | ts | prev | gap  |
> +--------+----+------+------+
> | A      | 0  | None | None |
> | A      | 3  | 0    | 3    |
> | A      | 6  | 3    | 3    |
> | A      | 15 | 6    | 9    |
> | A      | 18 | 15   | 3    |
> | A      | 60 | 18   | 42   |
> | A      | 63 | 60   | 3    |
> | B      | 0  | None | None |
> | B      | 2  | 0    | 2    |
> | B      | 4  | 2    | 2    |
> | B      | 7  | 4    | 3    |
> +--------+----+------+------+
> ```

**Notes**

- No interval construction needed: each offline interval's start is the
  previous row's ts and its end is this row's ts — **the gap row itself
  carries the whole interval**. An "event is the interval" perspective that
  skips pairing or expansion.
- NULL-gap rows (each device's first) fail `gap > 5` in the next step (NULL
  comparison), so they are never misflagged — zero special-casing.
- With a timestamp column, diff by unit:
  `r.ts.dt.diff(r.prev, unit="minute")` — same idea, explicit units.

## Step 2 · Offline intervals and severity ranking

Filter gap > 5, add the interval's start and end (`gap` is already its
length in minutes), then rank by duration descending inside each device —
`rank().over(partition_by=, order_by=, descending=)` in one shot.

```python
from ltseq import rank

offline = gapped.filter(lambda r: r.gap > 5).derive(
    start=lambda r: r.prev,
    end=lambda r: r.ts,
    severity=lambda r: rank().over(
        partition_by=r.device, order_by=r.gap, descending=True
    ),
)

offline.sort("device", "ts").show();
```

> **Expected output**
>
> ```text
> +--------+----+------+-----+-------+-----+----------+
> | device | ts | prev | gap | start | end | severity |
> +--------+----+------+-----+-------+-----+----------+
> | A      | 15 | 6    | 9   | 6     | 15  | 2        |
> | A      | 60 | 18   | 42  | 18    | 60  | 1        |
> +--------+----+------+-----+-------+-----+----------+
> ```

**Notes**

- `.over(order_by=...)` currently returns its rows ordered by the window
  key (gap, descending), so the display sorts them back to time order.
- Device B is absent: its largest gap is 3, so `gap > 5` eliminates
  everything — an empty filter result is the correct expression of "no
  offline" (B is this case's control group).
- **One derive, three new columns**: start/end reference existing columns and
  severity is an independent window expression — no cross-references, so
  sharing one derive does not violate the rule.
- `rank()` shares ranks on ties: two 42-minute outages would both carry
  severity 1 — the same "ties included" contract as case 16.
- severity means "worst within the device"; drop partition_by for the
  **global** worst — one parameter apart, two different report semantics.

## Step 3 · Worst events and per-device summary

`severity == 1` rows are each device's longest outage; device-level summary
uses conditional aggregates `count_if` / `max_if` in one pass.

```python
worst = offline.filter(lambda r: r.severity == 1)

worst.show();
```

> **Expected output**
>
> ```text
> +--------+----+------+-----+-------+-----+----------+
> | device | ts | prev | gap | start | end | severity |
> +--------+----+------+-----+-------+-----+----------+
> | A      | 60 | 18   | 42  | 18    | 60  | 1        |
> +--------+----+------+-----+-------+-----+----------+
> ```

**Notes**

- Rank first, filter after: `filter(severity == 1)` is the universal closing
  move for "top 1 within group" — shorter than aggregating a max and joining
  it back.
- Want the second-longest too? `severity <= 2` — a rank column supports any
  Top-K natively.

```python
from ltseq import count_if, max_if

per_device = (
    gapped
    .group_by("device")
    .agg(
        outages=lambda g: count_if(g.gap > 5),
        worst_gap=lambda g: max_if(g.gap > 5, g.gap),
    )
    .sort("device")
)

per_device.show();
```

> **Expected output**
>
> ```text
> +--------+---------+-----------+
> | device | outages | worst_gap |
> +--------+---------+-----------+
> | A      | 2       | 42        |
> | B      | 0       | None      |
> +--------+---------+-----------+
> ```

**Notes**

- `max_if(g.gap > 5, g.gap)`: max over qualifying rows only — B has none, so
  the result is **NULL**. NULL precisely means "no outage"; when a report
  must show 0, one line of `derive(worst=lambda r: nvl(r.worst_gap, 0))`
  covers it.
- The predicates of `count_if` and `max_if` take column expressions and sit
  alongside plain aggregates — the conditional-aggregation family
  (`count_if/sum_if/avg_if/min_if/max_if`) is the alert-report workhorse.
- Conservation check: per_device's outages sum (2+0) must equal offline's
  row count (2) ✔ — two routes corroborating each other is the standard
  self-check for data pipelines.

## Key takeaways

- **The event is the interval**: offline intervals need no expansion — the
  gap-over-threshold row carries its own start and end. Same perspective:
  timed-out requests (send↔reply), stockouts (sell↔restock).
- **A triple strike of NULL**: shift boundary NULL → gap NULL → filter skips
  — "no previous value" is handled layer by layer by three-valued logic,
  without a single special case.
- **rank's partition_by is the report's scope**: within-device (this case) or
  global (drop partition_by) — always ask "compared within whose scope"
  before writing a window expression.

## Exercises

1. The device-restart trap: if a device's clock resets on reboot, sorting by
   ts interleaves the heartbeats from before and after the reboot, and every
   gap looks normal. Keep arrival order instead (a `seq` column and
   `sort("device", "seq")`) so the reset shows up as a negative gap, add a
   `gap < 0` flag column, and decide whether it counts as offline, dropped
   data, or an anomaly.
2. Context for each outage: if heartbeats carried more fields (firmware,
   battery), `join` offline back to the heartbeat table with
   `on=lambda a, b: (a.device == b.device) & (a.start == b.ts)` to attach
   the last heartbeat's fields — handy for log lookup.
3. Offline duration per hour: splitting (start, end) into hour buckets is
   the reverse of case 23's merge (hint: give offline and an hour-slot table
   the same constant key column and `join` on it, so every outage meets
   every hour; keep the overlapping pairs and clip each to its hour with
   `if_else` — feel how much harder interval splitting is).
4. Parameterize the threshold: lambdas capture outer variables — write
   `def offline_report(hb, threshold): ...` returning the offline table, and
   verify closure variables work inside expression lambdas.

---
**Previous**: [27 · Resampling & calendar alignment](27_resample_align.md) · **Back to [contents](../../README.md)**
