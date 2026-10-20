<!-- Generated from ../../en/18_session_composite.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 18 · Composite Session Rules: Beyond the Time Gap

> **Level** ★★★ · **Domain** E-commerce · **Prereqs** Case 10
>
> **APIs used**: `sort` / `shift(1, partition_by=)` / `if_else` / `is_null` / `& |` combos / `cum_sum(partition_by=)` / `group_by().agg()` / `s.concat` / `cast("string")`

**Learning goals**

- Extend case 10's gap-only sessions with a rule that **depends on the previous
  event's type**
- Practice compound boolean conditions:
  `(gap > 600) | (previous action is exit AND gap > 300)`
- Produce two levels of summary from one marked table: sessions per user (one
  row) and per-session details (many rows)

## The problem

An event stream `events(user, ts, action)` with `ts` in seconds from midnight.
Session termination rule (composite):

- Adjacent events **more than 600 seconds apart** (10 idle minutes) → new
  session;
- **Previous action is `exit`** and the gap exceeds 300 seconds (gone 5 minutes
  after exiting) → new session.

Goals: assign a session number to every event; report sessions per user and a
per-session summary.

Two deliberate traps: u2's 760→900 gap is only 140 seconds — the previous
action IS `exit`, but 140 ≤ 300 so it must **not** break; u3's 10→3800 gap of
3790 seconds **breaks on the time rule alone**.

```python
from ltseq import LTSeq

events = LTSeq.from_rows([
    {"user": "u1", "ts": 0, "action": "login"},
    {"user": "u1", "ts": 60, "action": "view"},
    {"user": "u1", "ts": 300, "action": "add_cart"},
    {"user": "u1", "ts": 340, "action": "exit"},
    {"user": "u1", "ts": 1200, "action": "login"},
    {"user": "u1", "ts": 1250, "action": "view"},
    {"user": "u1", "ts": 1500, "action": "exit"},
    {"user": "u2", "ts": 5, "action": "login"},
    {"user": "u2", "ts": 400, "action": "view"},
    {"user": "u2", "ts": 700, "action": "add_cart"},
    {"user": "u2", "ts": 760, "action": "exit"},
    {"user": "u2", "ts": 900, "action": "view"},
    {"user": "u2", "ts": 2000, "action": "exit"},
    {"user": "u3", "ts": 10, "action": "login"},
    {"user": "u3", "ts": 3800, "action": "view"},
]).sort("user", "ts")
```

## Step 1 · In-group shifts: previous time + previous action

Two shifts that do not reference each other share one `derive`. Group
boundaries return NULL — a NULL `prev_ts` marks each user's first row.

```python
stamped = events.derive(
    prev_ts=lambda r: r.ts.shift(1, partition_by="user"),
    prev_action=lambda r: r.action.shift(1, partition_by="user"),
)
```

## Step 2 · The composite break condition: business rule as a boolean

The rule's AND/OR map to `&` / `|`; **parentheses are mandatory** — in Python
`&` and `|` bind tighter than `>` and `==`, so every comparison needs its own
pair, or Python pairs the wrong operands (`600 | (...)`) and raises `TypeError`.
`&` also binds tighter than `|`; parenthesizing that grouping too keeps the
rule readable.

```python
from ltseq import if_else

flagged = stamped.derive(
    brk=lambda r: if_else(
        r.prev_ts.is_null(),
        1,
        if_else(
            ((r.ts - r.prev_ts) > 600)
            | ((r.prev_action == "exit") & ((r.ts - r.prev_ts) > 300)),
            1,
            0,
        ),
    ),
).derive(
    sid=lambda r: r.brk.cum_sum(partition_by="user"),
)

flagged.show(15);
```

> **Expected output**
>
> ```text
> +------+------+----------+---------+-------------+-----+-----+
> | user | ts   | action   | prev_ts | prev_action | brk | sid |
> +------+------+----------+---------+-------------+-----+-----+
> | u1   | 0    | login    | None    | None        | 1   | 1   |
> | u1   | 60   | view     | 0       | login       | 0   | 1   |
> | u1   | 300  | add_cart | 60      | view        | 0   | 1   |
> | u1   | 340  | exit     | 300     | add_cart    | 0   | 1   |
> | u1   | 1200 | login    | 340     | exit        | 1   | 2   |
> | u1   | 1250 | view     | 1200    | login       | 0   | 2   |
> | u1   | 1500 | exit     | 1250    | view        | 0   | 2   |
> | u2   | 5    | login    | None    | None        | 1   | 1   |
> | u2   | 400  | view     | 5       | login       | 0   | 1   |
> | u2   | 700  | add_cart | 400     | view        | 0   | 1   |
> | u2   | 760  | exit     | 700     | add_cart    | 0   | 1   |
> | u2   | 900  | view     | 760     | exit        | 0   | 1   |
> | u2   | 2000 | exit     | 900     | view        | 1   | 2   |
> | u3   | 10   | login    | None    | None        | 1   | 1   |
> | u3   | 3800 | view     | 10      | login       | 1   | 2   |
> +------+------+----------+---------+-------------+-----+-----+
> ```

**Notes**

- Key check, u2's row at 900: gap 140 — `(140 > 600)` false,
  `(prev==exit AND 140 > 300)` false → `brk=0`, **no break**. Drop the time
  bound on the exit rule and this row wrongly starts a new session.
- u3's row at 3800: gap 3790 > 600 → `brk=1` — the time rule works alone, no
  exit required.
- In this data the exit clause never breaks a session on its own: u1's 1200
  row follows an `exit`, but its 860-second gap already breaks on the time
  rule. Exercise 4 moves that login so only the exit clause fires.
- `brk` is 0/1; an in-group `cum_sum` turns it into the session number — the
  numbering trick from cases 10/15 reused as-is.
- Nested `if_else`: the outer handles NULL (first row), the inner holds the
  composite condition. A `when(...).when(...).otherwise(...)` chain works too.

## Step 3 · User-level summary: one row per user

Session numbers increase within a user — so **the max session number is the
session count**. One aggregation line, no session detail needed.

```python
counts = (
    flagged
    .group_by("user")
    .agg(n_sessions=lambda g: g.sid.max())
    .sort("user")
)

counts.show();
```

> **Expected output**
>
> ```text
> +------+------------+
> | user | n_sessions |
> +------+------------+
> | u1   | 2          |
> | u2   | 2          |
> | u3   | 2          |
> +------+------------+
> ```

**Notes**

- `g.sid.max()` aggregates a **derived** column exactly like an original one —
  derive output is a normal column.
- `max(sid) = session count` relies on how sid was generated (consecutive from
  1). Ordered numbering hands you free summaries: many ordered-world
  statistics are min/max/count over a sequence number.

## Step 4 · Session-level detail: composite-key grouping

For per-session start/end and size you need a (user, sid) second-level group —
reusing case 15's composite-key move.

```python
keyed = flagged.derive(
    session_key=lambda r: r.user.s.concat("#", r.sid.cast("string")),
)

sessions = (
    keyed
    .group_by("session_key")
    .agg(
        user=lambda g: g.user.min(),
        start=lambda g: g.ts.min(),
        end=lambda g: g.ts.max(),
        actions=lambda g: g.ts.count(),
    )
    .sort("user", "start")
)

sessions.show();
```

> **Expected output**
>
> ```text
> +-------------+------+-------+------+---------+
> | session_key | user | start | end  | actions |
> +-------------+------+-------+------+---------+
> | u1#1        | u1   | 0     | 340  | 4       |
> | u1#2        | u1   | 1200  | 1500 | 3       |
> | u2#1        | u2   | 5     | 900  | 5       |
> | u2#2        | u2   | 2000  | 2000 | 1       |
> | u3#1        | u3   | 10    | 10   | 1       |
> | u3#2        | u3   | 3800  | 3800 | 1       |
> +-------------+------+-------+------+---------+
> ```

**Notes**

- u2#1 ends at 900: the 760 `exit` is **not** a session end — the rule breaks
  only if the user stays away 300 seconds after exiting; they returned at 900,
  and the new session starts at 2000.
- `group_by().agg()` groups by hash, so a group has no row order and no
  `first()` / `last()`. `ts` rises within a session, so `min` / `max` give
  its start and end; `user` is constant within a session, so `min` just picks
  it. When you need a group's real first or last row, `group_ordered(...).agg()`
  (case 10) has `g.first()` / `g.last()`.
- Single-action sessions (u2#2) have start == end — a normal shape, no
  special-casing.
- Both summary levels (user counts, session details) grow from the same
  `flagged` table — the marked breakpoint table is the shared foundation of
  every session requirement.

## Key takeaways

- **Composite rule = compound boolean**: fully parenthesized `& |` is the most
  error-prone yet most valuable expression skill here. Write the business rule
  in a comment first, then translate word by word.
- **"Previous row state" is the key to session problems**: `prev_action` and
  `prev_ts` share one partition_by and are shifted together, so they stay
  aligned.
- **Two summaries, one base table**: max(sid) for counts, composite-key
  grouping for details — mark the breakpoints first, the summaries come
  cheap.

## Exercises

1. Add a third rule: force a new session after **3 consecutive `view`s**
   (hint: a sliding sum of a view flag,
   `if_else(r.action == "view", 1, 0).rolling(3, partition_by="user").sum()`,
   or a counter column — which reads better?).
2. Find the two longest sessions: derive `duration = r.end - r.start` on the
   sessions table, sort descending, `slice(0, 2)` — where does u2#2's
   0-duration land?
3. Switch to real timestamps: build `ts` from Python `datetime` values and
   compute the gap with `r.ts.dt.diff(r.prev_ts, unit="second")` — compare it
   with the plain numeric difference.
4. Make the exit clause decide: move u1's second login from 1200 to 800 (460
   seconds after the exit). `brk` is still 1 there, now only because of the
   exit clause; drop that clause and u1 collapses into one session.

---
**Previous**: [17 · Rank migration](17_rank_migration.md) · **Next**: [19 · Allocating group totals](19_alloc_summary.md)
