<!-- Generated from ../../en/11_error_burst.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 11 · Error Burst Detection: Rolling Stats & Episodes

> **Level** ★★★ · **Domain** Logs / monitoring · **Prereqs** Cases 03, 10
>
> **APIs used**: `rolling(10, partition_by=)` / `when` / `group_ordered.filter` / `g.all/any/none` quantifiers

**Learning goals**

- Per-service rolling stats: `rolling(n, partition_by="svc")` in one line
- Collapse consecutive same-severity logs into **episodes** — on-call sees
  storms, not isolated alerts
- Use group quantifiers `g.all()/any()/none()` in a group `filter` for
  "entirely / at least one / no" predicates

## The problem

Service log `logs(ts, svc, level, latency_ms)`. Tasks:

1. Maintain a **rolling error rate** and average latency over the last 10
   records per service;
2. Grade by `err_rate`: `≥0.5` critical, `≥0.3` warning, else normal;
3. Collapse consecutive same-severity logs into **episodes**; report critical
   episodes lasting ≥5 records and check whether every row inside is an ERROR
   (`g.all`).

```python
import random

from ltseq import LTSeq, if_else

rng = random.Random(21)
levels = (
    ["INFO"] * 12
    + ["ERROR"] * 10            # injected storm: records 13-22
    + ["ERROR", "INFO", "ERROR", "INFO", "INFO"]
    + ["INFO"] * 3
)
rows = []
for i, lv in enumerate(levels):
    rows.append({
        "ts":         i * 30,
        "svc":        rng.choice(["api"]),           # single service; see exercises
        "level":      lv,
        "latency_ms": 80 + (i % 7) * 15 + (900 if lv == "ERROR" else 0),
    })

logs = LTSeq.from_rows(rows).sort("ts")
print(len(logs), "log rows")
```

> **Expected output**
>
> ```text
> 30 log rows
> ```

## Step 1 · Rolling stats and grading

Two chained derives: first the `is_err` 0/1 column (the next derive needs it),
then the rolling means.

```python
from ltseq import when

staged = (
    logs
    .sort("ts")
    .derive(is_err=lambda r: if_else(r.level == "ERROR", 1, 0))
    .derive(lambda r: {
        "err_rate": r.is_err.rolling(10, partition_by="svc").mean().round(2),
        "lat_avg":  r.latency_ms.rolling(10, partition_by="svc").mean().round(0),
    })
    .derive(
        severity=lambda r: when(r.err_rate >= 0.5, "critical")
                            .when(r.err_rate >= 0.3, "warning")
                            .otherwise("normal"),
    )
)
staged.select("ts", "level", "err_rate", "lat_avg", "severity").tail(8).show();
```

> **Expected output**
>
> ```text
> +-----+-------+----------+---------+----------+
> | ts  | level | err_rate | lat_avg | severity |
> +-----+-------+----------+---------+----------+
> | 660 | ERROR | 1        | 1022    | critical |
> | 690 | INFO  | 0.9      | 926     | critical |
> | 720 | ERROR | 0.9      | 931     | critical |
> | 750 | INFO  | 0.8      | 845     | critical |
> | 780 | INFO  | 0.7      | 760     | critical |
> | 810 | INFO  | 0.6      | 674     | critical |
> | 840 | INFO  | 0.5      | 578     | critical |
> | 870 | INFO  | 0.4      | 482     | warning  |
> +-----+-------+----------+---------+----------+
> ```

**Notes**

- `rolling(10, partition_by="svc")`: the grouping parameter rides on the
  window itself — multi-service in one line (kept here to show the general
  form); semantics stay partial-frame near the head.
- Rolling stats make storms surface naturally: `err_rate` climbs through the
  dense ERROR block and decays afterward.
- The `when` chain short-circuits in order; thresholds go high → low.

## Step 2 · Collapse into episodes

The familiar trio once more: shift flags severity flips → cum_sum
numbers → group_ordered folds.

```python
flagged = (
    staged
    .derive(
        changed=lambda r: if_else(
            r.severity.shift(1).is_null() | (r.severity != r.severity.shift(1)),
            1, 0,
        ),
    )
    .cum_sum("changed")
)

episodes = (
    flagged
    .group_ordered(lambda r: r.changed_cumsum)
    .agg(lambda g: {
        "svc":      g.first().svc,
        "severity": g.first().severity,
        "start_ts": g.first().ts,
        "end_ts":   g.last().ts,
        "n":        g.count(),
    })
)
episodes.show();
```

> **Expected output**
>
> ```text
> +-----+----------+----------+--------+----+
> | svc | severity | start_ts | end_ts | n  |
> +-----+----------+----------+--------+----+
> | api | normal   | 0        | 390    | 14 |
> | api | warning  | 420      | 450    | 2  |
> | api | critical | 480      | 840    | 13 |
> | api | warning  | 870      | 870    | 1  |
> +-----+----------+----------+--------+----+
> ```

## Step 3 · Filter storms: group-level filter + quantifiers

`NestedTable.filter`'s predicate receives the **group proxy g**: compare
group-level aggregates, or quantify over rows with
`g.all()/any()/none()`.

```python
runs = flagged.group_ordered(lambda r: r.changed_cumsum)

def summary(g):
    return {
        "severity": g.first().severity,
        "start_ts": g.first().ts,
        "end_ts":   g.last().ts,
        "n":        g.count(),
    }

# group aggregates: critical episodes lasting >= 5 records
storms = runs.filter(lambda g: (g.first().severity == "critical") & (g.count() >= 5))
storms.agg(summary).show()

# quantifiers: a row predicate checked against every row of the group
all_error = storms.filter(lambda g: g.all(lambda r: r.level == "ERROR"))
any_slow  = storms.filter(lambda g: g.any(lambda r: r.latency_ms > 900))
no_error  = runs.filter(lambda g: g.none(lambda r: r.level == "ERROR"))
print("storms with only ERROR rows:  ", len(all_error.agg(summary)))
print("storms with a row over 900 ms:", len(any_slow.agg(summary)))
print("episodes with no ERROR row:")
no_error.agg(summary).show();
```

> **Expected output**
>
> ```text
> +----------+----------+--------+----+
> | severity | start_ts | end_ts | n  |
> +----------+----------+--------+----+
> | critical | 480      | 840    | 13 |
> +----------+----------+--------+----+
>
> storms with only ERROR rows:   0
> storms with a row over 900 ms: 1
> episodes with no ERROR row:
> +----------+----------+--------+---+
> | severity | start_ts | end_ts | n |
> +----------+----------+--------+---+
> | warning  | 870      | 870    | 1 |
> +----------+----------+--------+---+
> ```

**Notes**

- Group predicates combine with `&` (**never** Python `and` — it is rejected
  with a hint).
- `g.all(row predicate)` / `g.any(...)` / `g.none(...)`: what SQL writes as a
  `COUNT(CASE WHEN...) = n` detour becomes one declaration.
- Quantifiers are group predicates, so they work only inside `filter`; the
  group proxy in `agg` and `derive` has no `all/any/none`. To report the
  check as a column instead, aggregate `g.sum("is_err")` next to
  `g.count()` and compare the two in a table-level `derive`.
- No storm is all ERROR because `err_rate` is a trailing average, so the
  grade lags the raw levels: the burst's first ERRORs fall in the `normal`
  and `warning` episodes, the critical episode runs on into the INFO rows
  after the burst, and the `warning` episode at ts 870 has no ERROR at all.
- `filter` then `agg` on the same NestedTable: only surviving groups flow on.

## Key takeaways

1. **Two levels of abstraction for observability**: row-level windows answer
   "what's the level right now"; group-level runs answer "how long did it
   last" — different operators, clean division.
2. **rolling's grouping parameter**: `partition_by=` rides directly on the
   window — more direct than group-then-window two-step flows.
3. **Quantifiers make intent visible**: `all/any/none` read like the original
   requirement — audit-friendly.
4. **Single → multi service for free**: `partition_by="svc"` already sits in
   place; just swap in multi-service data (sort by `svc, ts` and add a
   service-change check to `changed`, see exercises).

## Exercises

- Multi-service: sort by `("svc", "ts")` and add `r.svc != r.svc.shift(1)`
  to the `changed` condition so storms stay per-service.
- Latency storms: a separate `rolling(20).std()` on `latency_ms`; alert on
  sudden volatility.
- Combine with case 07: `search_pattern_count` for `ERROR → ERROR → FATAL`
  preludes.

---
**Previous**: [10 · Sessionization](10_sessionization.md) · **Next**: [12 · Streaming big files](12_streaming_cursor.md)
