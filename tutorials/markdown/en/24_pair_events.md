<!-- Generated from ../../en/24_pair_events.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 24 · Pairing In/Out Events: Adjacent Matching and State Relay

> **Level** ★★★ · **Domain** Logs / monitoring · **Prereqs** Cases 06, 18
>
> **APIs used**: `sort` / `shift(-1, partition_by=)` (look forward) / `filter` / `group_by().agg()` + `count_if` / `fold(partition_by=)` / `is_not_null`

**Learning goals**

- Pair "adjacent in→out" with `shift(-1)` **looking one row ahead** — the
  simplest pairing solution
- Recognize adjacent matching's blind spot: one interleaved noise row breaks
  it — repair with fold's state relay
- Audit in/out counts per person with `count_if` to expose unbalanced records

## The problem

Access-control swipes `swipes(person, ts, direction)`, ts in minutes,
direction ∈ {in, out, deny}. Goals:

1. Pair each `in` with its `out` and compute time-on-premises;
2. **Audit** in/out counts per person and flag imbalances;
3. Handle two kinds of dirty data: **double-in** (in followed by in) and
   **noise insertion** (a deny between in and out).

```python
from ltseq import LTSeq

swipes = LTSeq.from_rows([
    {"person": "p1", "ts": 0, "direction": "in"},
    {"person": "p1", "ts": 25, "direction": "out"},
    {"person": "p1", "ts": 120, "direction": "in"},
    {"person": "p1", "ts": 150, "direction": "out"},
    {"person": "p2", "ts": 5, "direction": "in"},
    {"person": "p2", "ts": 40, "direction": "in"},
    {"person": "p2", "ts": 60, "direction": "out"},
    {"person": "p3", "ts": 10, "direction": "in"},
    {"person": "p3", "ts": 35, "direction": "out"},
    {"person": "p3", "ts": 50, "direction": "out"},
    {"person": "p4", "ts": 0, "direction": "in"},
    {"person": "p4", "ts": 10, "direction": "deny"},
    {"person": "p4", "ts": 20, "direction": "out"},
]).sort("person", "ts")

swipes.show(13);
```

> **Expected output**
>
> ```text
> +--------+-----+-----------+
> | person | ts  | direction |
> +--------+-----+-----------+
> | p1     | 0   | in        |
> | p1     | 25  | out       |
> | p1     | 120 | in        |
> | p1     | 150 | out       |
> | p2     | 5   | in        |
> | p2     | 40  | in        |
> | p2     | 60  | out       |
> | p3     | 10  | in        |
> | p3     | 35  | out       |
> | p3     | 50  | out       |
> | p4     | 0   | in        |
> | p4     | 10  | deny      |
> | p4     | 20  | out       |
> +--------+-----+-----------+
> ```

Predictions: p1 two pairs (25, 30 minutes); p2 one pair (40→60, 20 minutes —
the in at 5 is displaced by the in at 40); p3 one pair (10→35, 25 minutes —
50 is an orphan out); p4 one pair but broken by deny — adjacent matching
misses it, fold rescues it.

## Step 1 · Adjacent pairing: shift(-1) looks forward

`shift(1)` reads the previous row; `shift(-1)` reads the **next** — the sign
is the direction. An in-row pairs when its next row is an out; duration =
next_ts - ts.

```python
stamped = swipes.derive(
    next_dir=lambda r: r.direction.shift(-1, partition_by="person"),
    next_ts=lambda r: r.ts.shift(-1, partition_by="person"),
)

pairs = stamped.filter(
    lambda r: (r.direction == "in") & (r.next_dir == "out"),
).derive(
    minutes=lambda r: r.next_ts - r.ts,
)

pairs.show();
```

> **Expected output**
>
> ```text
> +--------+-----+-----------+----------+---------+---------+
> | person | ts  | direction | next_dir | next_ts | minutes |
> +--------+-----+-----------+----------+---------+---------+
> | p1     | 0   | in        | out      | 25      | 25      |
> | p1     | 120 | in        | out      | 150     | 30      |
> | p2     | 40  | in        | out      | 60      | 20      |
> | p3     | 10  | in        | out      | 35      | 25      |
> +--------+-----+-----------+----------+---------+---------+
> ```

**Notes**

- p2 only the 40 row pairs: the 5 row's next is an in, not an out — adjacent
  matching handles **double-in by latest-wins**, matching real turnstile
  behavior.
- **p4 is entirely missing**: the 0 row's next is deny and the 20 row is an
  out with no in — the interleaved noise blinds adjacent matching. A
  structural blind spot, not a bug.
- 4 pairs vs the predicted 5: the missing one is p4. The count audit (next
  step) exposes such gaps systematically.

## Step 2 · The in/out audit: conditional counting

Any pairing solution needs independent verification. Aggregate in/out counts
per person — unequal counts mean something is off.

```python
from ltseq import count_if

audit = (
    swipes
    .group_by("person")
    .agg(
        n_in=lambda g: count_if(g.direction == "in"),
        n_out=lambda g: count_if(g.direction == "out"),
    )
    .sort("person")
)

audit.show();
```

> **Expected output**
>
> ```text
> +--------+------+-------+
> | person | n_in | n_out |
> +--------+------+-------+
> | p1     | 2    | 2     |
> | p2     | 2    | 1     |
> | p3     | 1    | 2     |
> | p4     | 1    | 1     |
> +--------+------+-------+
> ```

**Notes**

- `count_if(g.direction == "in")`: counts only rows matching the predicate —
  imported from `ltseq`'s top level, aggregation context only.
- Reading the audit: p1 balanced; p2 one extra in (double-in); p3 one extra
  out (orphan out); p4 counts balance **but pairing failed** — the audit is
  necessary, not sufficient; run both checks.
- p4's "balanced yet unmatched" is the documentary proof of adjacent
  matching's blind spot: the in and out both exist, they are just **not
  adjacent**.

## Step 3 · fold state relay: pairing across noise

fold carries one piece of state: **the moment of the latest unpaired in**.
in updates the state; out consumes it; deny and friends leave it untouched —
noise is crossed over naturally.

```python
paired = swipes.fold(
    lambda s, r: (
        r["ts"]
        if r["direction"] == "in"
        else s
    ),
    init=None,
    into="open_in",
    partition_by="person",
)

matched = paired.filter(
    lambda r: (r.direction == "out") & r.open_in.is_not_null(),
).derive(
    minutes=lambda r: r.ts - r.open_in,
).sort("person", "ts")

matched.show();
```

> **Expected output**
>
> ```text
> +--------+-----+-----------+---------+---------+
> | person | ts  | direction | open_in | minutes |
> +--------+-----+-----------+---------+---------+
> | p1     | 25  | out       | 0       | 25      |
> | p1     | 150 | out       | 120     | 30      |
> | p2     | 60  | out       | 40      | 20      |
> | p3     | 35  | out       | 10      | 25      |
> | p3     | 50  | out       | 10      | 40      |
> | p4     | 20  | out       | 0       | 20      |
> +--------+-----+-----------+---------+---------+
> ```

**Notes**

- **p4 is rescued**: in@0 opens the state, deny leaves it untouched, out@20
  consumes open_in=0 → 20 minutes — the state relay crosses the noise row.
- **p3 produces one suspicious record**: out@50 still sees open_in=10 — the
  first out (35) consumed the match but the state was **not cleared** (fold's
  return value is both stored and carried: a single channel), so the second
  out "pairs" 10→50 again. The audit table (p3 has one more out than in)
  flags exactly this person; strict handling needs a closed-flag or a stack
  in the state — see the exercises.
- fold's lambda works on **Python values**: `r["direction"] == "in"` is an
  ordinary comparison and ternaries are welcome — a different expressiveness
  boundary from expression lambdas (cases 20/25 contrast them again).
- `partition_by="person"` resets the state to None at each person boundary —
  the standing anti-cross-contamination rule.

## Key takeaways

- **Negative offset = look forward**: `shift(-1)` reads the next row,
  symmetric with `shift(1)`. "Pair adjacent records" problems (in/out,
  request/response, open/close events) all start here.
- **Adjacent matching vs state relay**: the former is one expression and
  fails on noise and gaps; the latter is one fold and fails on complex
  nesting — choose by dirt shape, and keep an audit table as the safety net.
- **fold's single channel has a price**: the return is both stored and
  carried, so "consume the state" and "keep the state" cannot both happen —
  encode the state or run two folds when you need stack semantics.

## Exercises

1. Strict fold: encode "(open_in, is_open)" in the state (e.g. open_in + 0.5
  means closed) so p3's second out is correctly judged an orphan — experience
   the single-channel workaround.
2. Orphan-out report: run `filter(lambda r: (r.direction == "out") &
   r.open_in.is_null())` on the strict fold from exercise 1 — p3's 50 belongs
   there. On the simplified `paired` the same filter returns no rows, because
   that version mispaired it.
3. Count "adjacent in→out" occurrences with `search_pattern_count` and
   cross-check against pairs' count.
4. Advanced: multiple in/out per day plus a lunch rule (outs between
   12:00-13:00 don't count) — should the rule live in fold's state machine or
   in a pre-filter? Write down your trade-off.

---
**Previous**: [23 · Merging overlapping intervals](23_interval_merge.md) · **Next**: [25 · Alert cooldown](25_alert_suppress.md)
