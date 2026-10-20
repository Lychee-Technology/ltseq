<!-- Generated from ../../en/26_sensor_anomaly.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 26 · Volatility-Band Anomaly Detection: rolling mean ± 3σ

> **Level** ★★☆ · **Domain** IoT · **Prereqs** Case 02
>
> **APIs used**: `sort` / `rolling(n, partition_by=).mean()/.std()` / `round` / `derive` / `shift(1, partition_by=)` / `|` combos / `group_by().agg()` + `count_if`

**Learning goals**

- Build a **dynamic volatility band** (the 3σ rule) with rolling mean and
  standard deviation instead of a hand-picked fixed threshold
- Understand and fix a real trap: **an anomaly pollutes its own baseline
  window** — shift the whole band so the comparison only sees history
- Face rolling's **partial-frame semantics**: the first rows have short
  windows, σ is NULL, anomaly verdicts are NULL — the warmup is "no
  judgment", not a missed report

## The problem

Temperature readings `temps(device, ts, temp)` — two devices, 8 ticks each;
D1 has a 41-degree spike at ts=5 (the normal band is 19-22). Goals:

1. Build a rolling 4-tick band per device: `mean ± 3σ`;
2. Flag readings **outside the previous tick's band** (anomalies);
3. Summarize anomaly counts per device.

Why not a fixed threshold of 30? Normal variance drifts with the season —
**the band should move with the data**.

```python
from ltseq import LTSeq

temps = LTSeq.from_rows([
    {"device": "D1", "ts": 0, "temp": 20.0},
    {"device": "D1", "ts": 1, "temp": 21.0},
    {"device": "D1", "ts": 2, "temp": 19.0},
    {"device": "D1", "ts": 3, "temp": 22.0},
    {"device": "D1", "ts": 4, "temp": 20.0},
    {"device": "D1", "ts": 5, "temp": 41.0},
    {"device": "D1", "ts": 6, "temp": 21.0},
    {"device": "D1", "ts": 7, "temp": 20.0},
    {"device": "D2", "ts": 0, "temp": 21.0},
    {"device": "D2", "ts": 1, "temp": 20.0},
    {"device": "D2", "ts": 2, "temp": 22.0},
    {"device": "D2", "ts": 3, "temp": 21.0},
    {"device": "D2", "ts": 4, "temp": 20.0},
    {"device": "D2", "ts": 5, "temp": 22.0},
    {"device": "D2", "ts": 6, "temp": 21.0},
    {"device": "D2", "ts": 7, "temp": 20.0},
]).sort("device", "ts")
```

## Step 1 · The rolling band: mean ± 3σ

`rolling(4, partition_by="device")` slides a 4-tick window inside each device;
note LTSeq's **partial-frame semantics** — short windows compute on what they
have, and row 0's std is NULL (one sample has no standard deviation).

```python
banded = temps.derive(
    ma=lambda r: r.temp.rolling(4, partition_by="device").mean().round(2),
    sd=lambda r: r.temp.rolling(4, partition_by="device").std().round(3),
).derive(
    upper=lambda r: (r.ma + 3 * r.sd).round(2),
    lower=lambda r: (r.ma - 3 * r.sd).round(2),
)

banded.filter(lambda r: r.device == "D1").show();
```

> **Expected output**
>
> ```text
> +--------+----+------+------+--------+-------+-------+
> | device | ts | temp | ma   | sd     | upper | lower |
> +--------+----+------+------+--------+-------+-------+
> | D1     | 0  | 20   | 20   | None   | None  | None  |
> | D1     | 1  | 21   | 20.5 | 0.707  | 22.62 | 18.38 |
> | D1     | 2  | 19   | 20   | 1      | 23    | 17    |
> | D1     | 3  | 22   | 20.5 | 1.291  | 24.37 | 16.63 |
> | D1     | 4  | 20   | 20.5 | 1.291  | 24.37 | 16.63 |
> | D1     | 5  | 41   | 25.5 | 10.408 | 56.72 | -5.72 |
> | D1     | 6  | 21   | 26   | 10.033 | 56.1  | -4.1  |
> | D1     | 7  | 20   | 25.5 | 10.344 | 56.53 | -5.53 |
> +--------+----+------+------+--------+-------+-------+
> ```

**Notes**

- Row ts5 leaks the trap: with 41 in the window, ma jumps to 25.5 and σ to
  10.4, so **upper becomes 56.72** — judging a row by its own band would call
  41 "normal"! The anomaly pollutes its own baseline.
- A negative lower bound is fine: the spike widened the band; a negative
  edge is a mathematical artifact, not a physical temperature.
- ts0's sd is NULL: one sample has no standard deviation, so
  `mean + 3*σ` is NULL — the warmup is honestly "no judgment", and SQL
  three-valued logic propagates it for free.
- `.round()` keeps the printout narrow. σ is rounded to 3 places before it
  scales the band; to 2 decimals the bands match those from the unrounded σ.

## Step 2 · The key move: shift the band, compare only against history

The fix: take the **previous row's** band as the reference. Whether the
current reading is anomalous is decided by history up to the previous row —
an anomaly can no longer bribe its own judge.

```python
guard = banded.derive(
    prev_upper=lambda r: r.upper.shift(1, partition_by="device"),
    prev_lower=lambda r: r.lower.shift(1, partition_by="device"),
).derive(
    is_anomaly=lambda r: (r.temp > r.prev_upper) | (r.temp < r.prev_lower),
)

guard.filter(lambda r: r.device == "D1").select(
    "device", "ts", "temp", "prev_upper", "prev_lower", "is_anomaly"
).show();
```

> **Expected output**
>
> ```text
> +--------+----+------+------------+------------+------------+
> | device | ts | temp | prev_upper | prev_lower | is_anomaly |
> +--------+----+------+------------+------------+------------+
> | D1     | 0  | 20   | None       | None       | None       |
> | D1     | 1  | 21   | None       | None       | None       |
> | D1     | 2  | 19   | 22.62      | 18.38      | false      |
> | D1     | 3  | 22   | 23         | 17         | false      |
> | D1     | 4  | 20   | 24.37      | 16.63      | false      |
> | D1     | 5  | 41   | 24.37      | 16.63      | true       |
> | D1     | 6  | 21   | 56.72      | -5.72      | false      |
> | D1     | 7  | 20   | 56.1       | -4.1       | false      |
> +--------+----+------+------------+------------+------------+
> ```

**Notes**

- **ts5 is caught**: its judge is ts4's band (24.37 / 16.63), and 41 flies
  past the upper edge.
- **The lagging window at ts6/ts7 is the price**: the spike stays in the
  4-tick window for four ticks, and until it leaves, the band is about ±31
  wide. 21 and 20 pass either way (they sit inside the pre-spike band too),
  but so would a second fault: 30 at ts6 judges False (exercise 1). To
  tighten faster, shorten the window or replace the anomalous value with an
  in-band estimate and recompute.
- is_anomaly is NULL at ts0/ts1: the judge itself is not ready yet (shift
  boundary plus σ warmup). **NULL ≠ False**: filters skip them and counts
  skip them — the warmup shows up as itself, not disguised as "normal".
- What happens without the shift, judging `r.temp > r.upper` directly? ts5's
  upper is 56.72 and 41 judges False — a **missed detection**. Run both
  versions once and remember the contrast.

## Step 3 · Device summary and the counting contract

`count_if` tallies anomalies; NULL rows (warmup) do not count — exactly the
contract you want.

```python
from ltseq import count_if

summary = (
    guard
    .group_by("device")
    .agg(
        total=lambda g: g.ts.count(),
        n_anom=lambda g: count_if(g.is_anomaly == True),
    )
    .sort("device")
)

summary.show();
```

> **Expected output**
>
> ```text
> +--------+-------+--------+
> | device | total | n_anom |
> +--------+-------+--------+
> | D1     | 8     | 1      |
> | D2     | 8     | 0      |
> +--------+-------+--------+
> ```

**Notes**

- D1 catches 1 (the 41-degree spike), D2 stays green — matching eyeball
  inspection.
- `count_if(g.is_anomaly == True)`: NULL == True is NULL, not counted — the
  two warmup rows never pollute the numerator (whether `total` should also
  exclude them is a business call; see exercise 3).
- `total` counts rows through a column (`g.ts.count()`), as in case 25.
- At this point "rolling baseline + historical comparison + quantifier
  summary" is a deployable lightweight anomaly detector — on single-machine
  datasets it beats standing up a streaming monitoring stack.

## Key takeaways

- **Partial-frame semantics of rolling**: no min_periods; short windows
  compute short — early σ values can be noisy or NULL. Either accept "no
  judgment" during warmup (this case) or exclude it explicitly (exercise 3).
- **Separate the judge from the player**: one `shift(1)` cuts the baseline
  from the reading. Every "judge the present by the past" problem (anomaly
  detection, drawdown alerts, incremental validation) passes this gate.
- **σ is the sample statistic**: rolling `std` divides by n-1, which on small
  windows gives a noticeably larger σ than the population formula, widening
  the band: fewer false alarms, slower detection. Window size and k are a
  pair of linked knobs.

## Exercises

1. How long does the spike blind the detector? Change ts6/ts7's temps to 30
   and 25 — both far above the pre-spike band (24.37) — and check
   `is_anomaly`; then append ticks to D1 and find the first one judged
   against a band without the spike.
2. Symmetry of two-sided detection: craft a **sudden drop** to -10 and verify
   the lower edge fires.
3. Warmup handling: number rows with
   `row_number().over(partition_by=r.device, order_by=r.ts)`, `filter` out
   numbers ≤ 4, and re-summarize — compare the `total` semantics.
4. Add `z_score=lambda r: (r.temp - r.prev_ma) / r.prev_sd` (shift ma/sd to
   the previous tick first, as Step 2 does for the band), drop the warmup
   NULLs, and rank the top anomalies with
   `rank().over(order_by=r.z_score.abs(), descending=True)` — more
   explainable than a boolean flag.

---
**Previous**: [25 · Alert cooldown](25_alert_suppress.md) · **Next**: [27 · Resampling & calendar alignment](27_resample_align.md)
