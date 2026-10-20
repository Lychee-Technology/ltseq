<!-- Generated from ../../en/27_resample_align.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 27 · Resampling & Calendar Alignment: Filling Missing Time Points

> **Level** ★★★ · **Domain** IoT · **Prereqs** Cases 06, 14
>
> **APIs used**: `from_rows` (`datetime` values) / `dt.diff(other, unit=)` / `//` floor bucketing / `group_by().agg()` / `align(ref, key=)` / `join(how="left")` / `fold` (forward fill)

**Learning goals**

- **Resample** irregular timestamps onto a fixed grid: `dt.diff(anchor, unit)`
  + `//` floor = time bucketing
- **Align** the aggregated result to a full calendar — missing ticks appear
  as explicit NULL rows: `align()` inserts them, a left join from an axis
  table inserts them and keeps their keys
- **Forward-fill** the gaps with `fold` so downstream computation gets a
  continuous series

## The problem

Device telemetry `tel(device, ts, temp)`: ideally one row per minute, but the
00:02 reading is **missing**. Goals:

1. **Bucket** the timestamps to minute granularity and aggregate
   (resampling);
2. **Align** the result to the full 0-4 minute axis so the missing minute
   appears explicitly;
3. **Forward-fill** temperatures so downstream gets a continuous series.

This is IoT/monitoring daily life: collection is never perfect; analysis must
"build the axis" before computing.

```python
from datetime import datetime

from ltseq import LTSeq

tel = LTSeq.from_rows([
    {"device": "T1", "ts": datetime(2024, 1, 1, 0, 0), "temp": 20.5},
    {"device": "T1", "ts": datetime(2024, 1, 1, 0, 1), "temp": 21.0},
    {"device": "T1", "ts": datetime(2024, 1, 1, 0, 3), "temp": 19.5},
    {"device": "T1", "ts": datetime(2024, 1, 1, 0, 4), "temp": 20.0},
]).sort("device", "ts")

print(tel.schema)   # ts is a native timestamp
tel.show();
```

> **Expected output**
>
> ```text
> {'device': 'string', 'ts': 'timestamp', 'temp': 'float64'}
> +--------+---------------------+------+
> | device | ts                  | temp |
> +--------+---------------------+------+
> | T1     | 2024-01-01 00:00:00 | 20.5 |
> | T1     | 2024-01-01 00:01:00 | 21   |
> | T1     | 2024-01-01 00:03:00 | 19.5 |
> | T1     | 2024-01-01 00:04:00 | 20   |
> +--------+---------------------+------+
> ```

**Notes**

- `ts` holds Python `datetime` values, so `from_rows` builds a native
  `timestamp` column and every later `dt.*` call works on it directly.
  `read_csv` likewise reads ISO `YYYY-MM-DD HH:MM:SS` text as `timestamp`.
- Timestamps that arrive as **strings** have to be converted at the source
  for now: `cast("timestamp")` raises `ValueError` on `main` (case 09's
  `cast("date32")` works but keeps only the date). ISO strings do sort
  correctly (lexicographic = chronological), but time **arithmetic** such
  as `dt.diff` needs the real type and raises on a string column.

## Step 1 · Bucketing: dt.diff against an anchor, then floor

Diff against a fixed anchor (an epoch) in minutes and `// 1` floor it — the
integer is the **minute bucket id**. Far shorter than splitting
year/month/day/hour and re-concatenating keys.

```python
EPOCH = datetime(2024, 1, 1)

bucketed = tel.derive(
    minute=lambda r: (r.ts.dt.diff(EPOCH, unit="minute") // 1).cast("int64"),
)

per_min = (
    bucketed
    .group_by("minute")
    .agg(
        avg_temp=lambda g: g.temp.mean(),
        max_temp=lambda g: g.temp.max(),
        n=lambda g: g.temp.count(),
    )
    .sort("minute")
)

per_min.show();
```

> **Expected output**
>
> ```text
> +--------+----------+----------+---+
> | minute | avg_temp | max_temp | n |
> +--------+----------+----------+---+
> | 0      | 20.5     | 20.5     | 1 |
> | 1      | 21       | 21       | 1 |
> | 3      | 19.5     | 19.5     | 1 |
> | 4      | 20       | 20       | 1 |
> +--------+----------+----------+---+
> ```

**Notes**

- `dt.diff(EPOCH, unit="minute")` returns **float** minutes (0.0, 1.0, 3.0,
  4.0); `// 1` floors — second-level data lands correctly too (59.5 seconds →
  0.99 → bucket 0).
- Changing granularity: hourly via `unit="hour"`, 5-minute via `// 5` — one
  parameter each.
- After `group_by`, **bucket 2 is absent**: hash grouping only sees keys the
  data has. The gap was not dropped — it never existed; building the axis is
  the next step's dedicated job.
- `n=lambda g: g.temp.count()` counts rows through a column; a bare
  `g.count()` would look for a column named `count`.
- In real pipelines the anchor and range should be derived from the data
  (`to_dicts()` for min/max); this case hardcodes constants to keep the
  method in focus.

## Step 2 · Calendar alignment: align() materializes missing rows

`align(ref_sequence, key=)` reorders the table along a reference sequence,
auto-inserting NULL rows for keys **present in the reference but not the
table** — on the 0-4 axis, the missing minute gets a row of its own.

```python
aligned = per_min.align([0, 1, 2, 3, 4], key=lambda r: r.minute)

aligned.show();
```

> **Expected output**
>
> ```text
> +--------+----------+----------+------+
> | minute | avg_temp | max_temp | n    |
> +--------+----------+----------+------+
> | 0      | 20.5     | 20.5     | 1    |
> | 1      | 21       | 21       | 1    |
> | None   | None     | None     | None |
> | 3      | 19.5     | 19.5     | 1    |
> | 4      | 20       | 20       | 1    |
> +--------+----------+----------+------+
> ```

**Notes**

- The missing minute is now a row, third in reference order — but **every**
  column of it is NULL, the key `minute` included: `align` fills the row
  from the table, and the table has nothing for minute 2. Only the row's
  position says which minute it is.
- `align` is one of LTSeq's few "**sequence-first**" operators: row order
  obeys the reference, and reference keys missing from the table become NULL
  rows.
- The keyless row gets in the way downstream: `align` drops the declared
  sort order, and `sort("minute")` would move the NULL key to the end. To
  keep computing on the series, build the axis as a table and left-join the
  data onto it.

```python
axis = LTSeq.from_rows([{"minute": m} for m in range(5)])

grid = axis.join(per_min, on="minute", how="left").sort("minute")

grid.show();
```

> **Expected output**
>
> ```text
> +--------+----------+----------+------+
> | minute | avg_temp | max_temp | n    |
> +--------+----------+----------+------+
> | 0      | 20.5     | 20.5     | 1    |
> | 1      | 21       | 21       | 1    |
> | 2      | None     | None     | None |
> | 3      | 19.5     | 19.5     | 1    |
> | 4      | 20       | 20       | 1    |
> +--------+----------+----------+------+
> ```

**Notes**

- Same five rows, but minute 2 keeps its key: the axis table supplies it,
  and `how="left"` keeps every axis row whether or not `per_min` has a
  match. The left join drops `per_min`'s copy of the key column.
- A join returns rows in no particular order, so `sort("minute")` restores
  time order and declares it — Step 3's `fold` requires a declared order.
- The full grid plots directly (line-chart holes are visible), diffs safely
  (a gap yields a NULL diff, no fake trend), and feeds packet-loss statistics
  by counting NULLs — **the axis pays off at every downstream step**.

## Step 3 · Forward fill: fold closes it

Many downstream computations hate holes (say, cumulative energy). Use fold
to carry the last valid value forward — the exact ffill posture from case
20.

```python
filled = grid.fold(
    lambda s, r: r["avg_temp"] if r["avg_temp"] is not None else s,
    init=None,
    into="avg_ffill",
)

filled.show();
```

> **Expected output**
>
> ```text
> +--------+----------+----------+------+-----------+
> | minute | avg_temp | max_temp | n    | avg_ffill |
> +--------+----------+----------+------+-----------+
> | 0      | 20.5     | 20.5     | 1    | 20.5      |
> | 1      | 21       | 21       | 1    | 21        |
> | 2      | None     | None     | None | 21        |
> | 3      | 19.5     | 19.5     | 1    | 19.5      |
> | 4      | 20       | 20       | 1    | 20        |
> +--------+----------+----------+------+-----------+
> ```

**Notes**

- fold state = the last non-null avg_temp seen; NULL rows keep the state and
  write it into `avg_ffill` — the minimal one-dimensional forward fill.
- **Keep both columns**: `avg_temp` (holes visible) and `avg_ffill`
  (continuous, computable) coexist, each serving its own downstream — the
  same display/compute column discipline as case 20.
- Backward fill or interpolation? Backward fill is the same fold over the
  reverse order: `grid.sort("minute", desc=True).fold(...)`, then sort back.
  Linear interpolation is best handed to `to_pandas()` and a proper library
  — LTSeq need not do everything.

## Key takeaways

- **The resampling trio**: bucket (diff + floor) → aggregate (group_by) →
  build the axis (left join onto an axis table). Three independently
  verifiable steps — more transparent than pandas `resample()` magic.
- **The axis is the ordered world's coordinate system**: declaring what the
  full axis looks like turns missing from "nonexistent" into "explicit
  NULL" — packet-loss monitoring, calendar reports, experiment alignment all
  lean on it. `align()` declares it with a Python list but loses the key on
  the rows it inserts; an axis table keeps it.
- **ffill belongs to fold, second proof**: stateful carry ("remember the
  last valid value") lives outside window expressions; one fold line does
  it.

## Exercises

1. Multi-device alignment: `group_by` takes a single key, so
   `partition("device")` into sub-tables, bucket each one and left-join it
   onto the same axis table, add the device back as a constant column
   (`LiteralExpr`, case 09), then stitch them together with `union` and fold
   with `partition_by="device"` — the shared axis table is what guarantees
   both devices line up.
2. A packet-loss report:
   `grid.agg(missing=lambda g: count_if(g.avg_temp.is_null()))` — count only
   after building the axis, or the missing never show up.
3. Re-bucket at 5 minutes (`// 5`) with 30 minutes of hand-made data —
   verify the three linked touchpoints: bucket id, axis table, ffill.
4. Toward smoothed series: bucket-aggregation is a non-overlapping window;
   run `rolling(3).mean()` on `grid` and compare the two "smoothings"'
   semantics.

---
**Previous**: [26 · Volatility-band anomaly detection](26_sensor_anomaly.md) · **Next**: [28 · Offline segment detection](28_offline_segments.md)
