<!-- Generated from ../../en/25_alert_suppress.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 25 · Alert Cooldown: fold the State Machine to the Rescue

> **Level** ★★★ · **Domain** Logs / monitoring · **Prereqs** Cases 06, 18
>
> **APIs used**: `sort` / `shift(1, partition_by=)` / `if_else` / `fold(init=, into=, partition_by=)` / `derive` / `group_by().agg()` + `count_if`

**Learning goals**

- Implement "30-minute cooldown per rule": compare against **the most recent
  alert that actually fired**, not the previous record — the two diverge on
  alert chains
- Deliberately write the **wrong** `shift`-based solution first, watch it fail
  on a chain, then bring in fold
- Learn fold state design: state = the moment of the last fired alert, kept
  untouched on suppressed rows

## The problem

An alert stream `alerts(ts, rule)`, ts in minutes. Cooldown rule: **same rule
within 30 minutes of its most recent fired alert → suppress**. Goals:

1. Mark every alert as **fired** or suppressed;
2. Count fired/suppressed per rule;
3. See clearly why the naive "compare with the previous record" solution is
   wrong on alert chains.

```python
from ltseq import LTSeq

alerts = LTSeq.from_rows([
    {"rule": "cpu", "ts": 0},
    {"rule": "cpu", "ts": 10},
    {"rule": "cpu", "ts": 35},
    {"rule": "cpu", "ts": 80},
    {"rule": "cpu", "ts": 95},
    {"rule": "disk", "ts": 5},
    {"rule": "disk", "ts": 20},
    {"rule": "mem", "ts": 15},
]).sort("rule", "ts")

alerts.show();
```

> **Expected output**
>
> ```text
> +------+----+
> | rule | ts |
> +------+----+
> | cpu  | 0  |
> | cpu  | 10 |
> | cpu  | 35 |
> | cpu  | 80 |
> | cpu  | 95 |
> | disk | 5  |
> | disk | 20 |
> | mem  | 15 |
> +------+----+
> ```

Prediction (fire only if > 30 since the last fire): cpu fires 0, 35, 80 and
suppresses 10, 95; disk fires 5, suppresses 20; mem fires 15. **The crux is
cpu@35**: only 25 since the previous record (10), but 35 since the last
*fired* alert (0) — it should fire.

## Step 1 · The naive solution: compare with the previous record (on purpose)

`shift(1)` grabs the previous ts; fire when the gap exceeds 30 — looks airtight
until you run the cpu chain.

```python
from ltseq import if_else

naive = (
    alerts
    .derive(prev_ts=lambda r: r.ts.shift(1, partition_by="rule"))
    .derive(
        naive_fire=lambda r: if_else(
            r.prev_ts.is_null() | ((r.ts - r.prev_ts) > 30), True, False
        ),
    )
)

naive.filter(lambda r: r.rule == "cpu").show();
```

> **Expected output**
>
> ```text
> +------+----+---------+------------+
> | rule | ts | prev_ts | naive_fire |
> +------+----+---------+------------+
> | cpu  | 0  | None    | true       |
> | cpu  | 10 | 0       | false      |
> | cpu  | 35 | 10      | false      |
> | cpu  | 80 | 35      | true       |
> | cpu  | 95 | 80      | false      |
> +------+----+---------+------------+
> ```

**Notes**

- cpu@35 uses the wrong reference: it compares with the **previous record**
  (10, itself suppressed) instead of the **most recent fired alert** (0).
  25 ≤ 30 → wrongly suppressed.
- The error **propagates**: the reference moves with every record, fired or
  not. 80 is right only by coincidence: its previous record, 35, is also the
  last alert that should have fired. On a chain of alerts 20 minutes apart
  (0, 20, 40, 60, ...) the naive rule fires at 0 and then stays silent for the
  rest of the chain, while the cooldown rule fires again at 40 and 80.
- Conclusion: the reference must be **a quantity filtered by the rule
  itself**; shift only reaches fixed-offset neighbors and cannot — this is
  exactly fold's stage.

## Step 2 · The fold state machine: state = last fired moment

State `s` is the cooldown reference: on fire (s is None, or gap > 30) update
it to this row's ts; on suppression pass it through untouched. fold's return
value both lands in the new column and travels to the next row — one channel
doing "store + relay".

```python
suppressed = alerts.fold(
    lambda s, r: (
        r["ts"]
        if (s is None) or (r["ts"] - s > 30)
        else s
    ),
    init=None,
    into="last_alert",
    partition_by="rule",
)

fired = suppressed.derive(
    is_fired=lambda r: r.last_alert == r.ts,
)

fired.filter(lambda r: r.rule == "cpu").show();
```

> **Expected output**
>
> ```text
> +------+----+------------+----------+
> | rule | ts | last_alert | is_fired |
> +------+----+------------+----------+
> | cpu  | 0  | 0          | true     |
> | cpu  | 10 | 0          | false    |
> | cpu  | 35 | 35         | true     |
> | cpu  | 80 | 80         | true     |
> | cpu  | 95 | 80         | false    |
> +------+----+------------+----------+
> ```

**Notes**

- cpu@35 fires correctly: after 10 was suppressed the state **stayed 0**, and
  35 - 0 > 30 → fire, state becomes 35. The state relay fixes exactly where
  the naive solution broke.
- `is_fired = (last_alert == ts)`: fired rows have last_alert equal to their
  own ts; suppressed rows keep an earlier moment — one equality turns the
  stored value back into an event flag. Safe because ts values are unique
  within a rule here: a suppressed duplicate of a fired ts would pass the
  equality too. With duplicates, flag the rows where the state changed
  instead: `last_alert` differs from `r.last_alert.shift(1, partition_by="rule")`,
  or that shift is NULL.
- fold's lambda lives in the **Python world**: `s is None`, `or`, ternaries —
  cleanly separated from expression lambdas' `& |` and `.is_null()` (the
  contrast from cases 20/24 closes here).
- `partition_by="rule"`: cooldowns count per rule, reset at rule boundaries.

## Step 3 · Summary: fired/suppressed per rule

`fired` is an ordinary LTSeq; `count_if` conditional counting closes it.

```python
from ltseq import count_if

summary = (
    fired
    .group_by("rule")
    .agg(
        total=lambda g: g.ts.count(),
        n_fired=lambda g: count_if(g.is_fired == True),
    )
    .sort("rule")
)

summary.show();
```

> **Expected output**
>
> ```text
> +------+-------+---------+
> | rule | total | n_fired |
> +------+-------+---------+
> | cpu  | 5     | 3       |
> | disk | 2     | 1       |
> | mem  | 1     | 1       |
> +------+-------+---------+
> ```

**Notes**

- 8 alerts converge into 5 notifications (37.5% suppressed) — in a storm this
  is how quiet your pager stays.
- `total=lambda g: g.ts.count()` counts the rows through a column: inside
  `group_by().agg()` the lambda gets a column proxy (case 05), so a bare
  `g.count()` would look for a column named `count`.
- `count_if(g.is_fired == True)`: neither NULL nor False counts — is_fired is
  never NULL here (an equality), but conditional counting is the right habit
  regardless.
- A free by-product: `last_alert` is "the most recent alert as seen from each
  row" — useful for root-cause analysis (when I was suppressed, who
  suppressed me?).

## Key takeaways

- **"Compare with the previous row" ≠ "compare with the most recent
  qualifier"**: the former is one shift away; the latter needs state. Chain
  suppression, sticky sessions, weighted accumulation — anywhere the reference
  updates by rule is fold territory.
- **Three questions of state design**: initial value (None = no alert yet);
  when to update (on fire); when to keep (on suppress). Answer them and the
  fold lambda fits in ten lines.
- **fold is not lazy**: whole-table materialization, per-row Python callback —
  alert-scale tables (tens of rows per rule per day) are the sweet spot; for
  billion-row streams pre-aggregate first.

## Exercises

1. Change the rule to "at most 2 per sliding window" (suppress if 2 already
   fired within 60 minutes): the state becomes a list of recent fired ts —
   maintain a Python list in fold. `show()` prints a list column as
   `[unsupported type]`, so read the result back with `to_dicts()`.
2. Add a severity column with the rule "critical never suppressed, warning
   cooled down" — how does the state update condition change?
3. On `naive`, find the alerts at most 30 minutes after the previous record
   with `search_pattern(lambda r: r.ts - r.prev_ts <= 30, partition_by="rule")`
   (a `search_pattern` predicate cannot call `shift`, hence `prev_ts`) and
   compare the count with summary's suppressed count — why don't they match?
4. Switch to timestamp columns (build `ts` from Python `datetime` values in
   `from_rows`) and compare against `timedelta(minutes=30)` inside fold —
   note how the state machine is transparent to gap units.

---
**Previous**: [24 · Pairing in/out events](24_pair_events.md) · **Next**: [26 · Volatility-band anomaly detection](26_sensor_anomaly.md)
