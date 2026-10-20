<!-- Generated from ../../en/23_interval_merge.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 23 · Merging Overlapping Intervals: cum_max and the Baton Pass

> **Level** ★★★ · **Domain** Logs / monitoring · **Prereqs** Case 15
>
> **APIs used**: `sort` / `cum_max(partition_by=)` / `shift(1, partition_by=)` / `if_else` / `is_null` / `cum_sum(partition_by=)` / `s.concat` / `cast("string")` / `group_by().agg()` + `min` / `max`

**Learning goals**

- Decompose "merge overlapping intervals" into four ordered steps:
  **running max → look back → block marker → block aggregation**
- Meet `cum_max`'s real role: not "find the max" but "carry the baton so far"
- See why no loop or recursion is needed: once rows are sorted by start, the
  running max lets each row decide on its own whether it opens a new block

## The problem

Room bookings `booking(room, start, end)` in minutes, intervals closed-open
[start, end). Bookings of the same room that **overlap or touch** merge into
one occupancy block. Goal: output each room's merged occupancy intervals.

```
R1:  [9,11) [10,12) [13,15)   →  [9,12) [13,15)
R2:  [8,10) [9,11)  [12,13)   →  [8,11) [12,13)
R3:  [14,16)                  →  [14,16)
R4:  [8,12) [9,10)  [11,13)   →  [8,13)
```

Note R2's third span [12,13): it does **not** intersect the merged [8,11)
(11 < 12) and must split. R4 is the opposite trap: [11,13) does not touch the
span before it, [9,10), yet it overlaps [8,12), so all three spans merge. The
merge criterion is "does the new start fall inside the **current baton
span**", not "is it adjacent to the previous row".

```python
from ltseq import LTSeq

rooms = LTSeq.from_rows([
    {"room": "R1", "start": 9, "end": 11},
    {"room": "R1", "start": 10, "end": 12},
    {"room": "R1", "start": 13, "end": 15},
    {"room": "R2", "start": 8, "end": 10},
    {"room": "R2", "start": 9, "end": 11},
    {"room": "R2", "start": 12, "end": 13},
    {"room": "R3", "start": 14, "end": 16},
    {"room": "R4", "start": 8, "end": 12},
    {"room": "R4", "start": 9, "end": 10},
    {"room": "R4", "start": 11, "end": 13},
]).sort("room", "start")
```

## Step 1 · The baton: in-group running maximum

`cum_max(partition_by="room")` carries "the largest end seen so far" — the
right edge of the current merged block. A second column `shift(1)` reads the
**previous row's** baton to test whether the current interval joins it.

```python
step1 = rooms.derive(
    running_end=lambda r: r.end.cum_max(partition_by="room"),
)
step2 = step1.derive(
    prev_running=lambda r: r.running_end.shift(1, partition_by="room"),
)

step2.show();
```

> **Expected output**
>
> ```text
> +------+-------+-----+-------------+--------------+
> | room | start | end | running_end | prev_running |
> +------+-------+-----+-------------+--------------+
> | R1   | 9     | 11  | 11          | None         |
> | R1   | 10    | 12  | 12          | 11           |
> | R1   | 13    | 15  | 15          | 12           |
> | R2   | 8     | 10  | 10          | None         |
> | R2   | 9     | 11  | 11          | 10           |
> | R2   | 12    | 13  | 13          | 11           |
> | R3   | 14    | 16  | 16          | None         |
> | R4   | 8     | 12  | 12          | None         |
> | R4   | 9     | 10  | 12          | 12           |
> | R4   | 11    | 13  | 13          | 12           |
> +------+-------+-----+-------------+--------------+
> ```

**Notes**

- **Two columns, two derives**: `prev_running` references `running_end`, and
  new columns inside one derive cannot reference each other (hard rule) —
  splitting changes nothing semantically.
- R4's row at 9: running_end=12 while **this row's end=10** — the baton still
  carries [8,12)'s right edge, so the next row sees prev_running=12 rather
  than the 10 its previous row ended at. In R1–R3 the ends only grow, so
  there the baton happens to equal each row's own end.
- Each room's first row has prev_running NULL — the partition boundary NULL,
  treated as "new block" as usual.

## Step 2 · Block markers and numbering

New block when **the previous baton ends before the current start**
(disjoint), or when this is the group's first row. Mark 0/1, then in-group
`cum_sum` numbers the blocks — the mark → number → group trio of cases 03 and 15
once more.

```python
from ltseq import if_else

step3 = step2.derive(
    new_block=lambda r: if_else(
        r.prev_running.is_null() | (r.prev_running < r.start), 1, 0
    ),
).derive(
    block=lambda r: r.new_block.cum_sum(partition_by="room"),
)

step3.show();
```

> **Expected output**
>
> ```text
> +------+-------+-----+-------------+--------------+-----------+-------+
> | room | start | end | running_end | prev_running | new_block | block |
> +------+-------+-----+-------------+--------------+-----------+-------+
> | R1   | 9     | 11  | 11          | None         | 1         | 1     |
> | R1   | 10    | 12  | 12          | 11           | 0         | 1     |
> | R1   | 13    | 15  | 15          | 12           | 1         | 2     |
> | R2   | 8     | 10  | 10          | None         | 1         | 1     |
> | R2   | 9     | 11  | 11          | 10           | 0         | 1     |
> | R2   | 12    | 13  | 13          | 11           | 1         | 2     |
> | R3   | 14    | 16  | 16          | None         | 1         | 1     |
> | R4   | 8     | 12  | 12          | None         | 1         | 1     |
> | R4   | 9     | 10  | 12          | 12           | 0         | 1     |
> | R4   | 11    | 13  | 13          | 12           | 0         | 1     |
> +------+-------+-----+-------------+--------------+-----------+-------+
> ```

**Notes**

- `r.prev_running < r.start`: R1's row at 13 has prev=12 < 13 → split; R2's
  row at 9 has prev=10 >= 9 → join. **Touching counts as merging**: a span
  that starts exactly where the baton ends (prev_running == start) fails `<`
  and joins the block; if touching should split, use `<=`.
- The comparison targets **prev_running (the previous baton)**, not the
  previous row's end: R4's row at 11 has prev_running=12 >= 11 → join, while
  its previous row ended at 10 and would have split [11,13) off a block it
  overlaps.
- `block` numbers blocks within a room; with `room` it forms a globally
  unique pair — the next step builds the composite key.

## Step 3 · Block aggregation: start from the first row, end from max

Each block's start = its first row's start, which is the smallest start
since rows are sorted by start; end = the largest end inside (the baton's
final value). Composite-key grouping closes it — same as cases
15/18.

```python
keyed = step3.derive(
    block_key=lambda r: r.room.s.concat("#", r.block.cast("string")),
)

merged = (
    keyed
    .group_by("block_key")
    .agg(
        room=lambda g: g.room.min(),     # one room per block: min() just carries it
        start=lambda g: g.start.min(),   # sorted by start: the first row's start
        end=lambda g: g.end.max(),
    )
    .sort("room", "start")
)

merged.show();
```

> **Expected output**
>
> ```text
> +-----------+------+-------+-----+
> | block_key | room | start | end |
> +-----------+------+-------+-----+
> | R1#1      | R1   | 9     | 12  |
> | R1#2      | R1   | 13    | 15  |
> | R2#1      | R2   | 8     | 11  |
> | R2#2      | R2   | 12    | 13  |
> | R3#1      | R3   | 14    | 16  |
> | R4#1      | R4   | 8     | 13  |
> +-----------+------+-------+-----+
> ```

Exactly the prediction. ✔

**Notes**

- `g.end.max()` equals the baton's final value (the block's last row carries
  the max end) — aggregating the original column is more direct and drops the
  dependency on an intermediate column. In `group_by().agg()`, `g.<column>`
  names a column and the aggregate hangs off it; the row accessors
  `g.first()`/`g.last()` belong to `group_ordered`/`group_sorted` groups.
- Ten rows in, six out: R1's first two spans merge, R2's first two merge,
  R3 passes through, R4's three spans become one — the merged result is
  naturally non-overlapping, no extra dedup needed.
- **Why not fold**: this problem's state (current block edge) is expressible
  by pushdown-friendly window operators like cum_max; fold's per-row Python
  callback would be overkill — look for window expressions first, fold is the
  fallback (consistent with cases 06/25).

## Key takeaways

- **The baton pattern**: `cum_max`/`cum_min` + `shift(1)` is the general shape
  of "compare against the best so far" — interval merging is one
  application; inventory high-water marks and peak detection share the DNA.
- **Mark-number-group again**: case 15's marker watched the previous
  row's business event; 23's marker watches the previous row's accumulated
  state — the condition varies, the skeleton never does.
- **Two derives is a rule, not a style**: new columns referencing each other
  must split; since every step can be `show()`-checked, splitting helps
  debugging anyway.
- **No recursion needed**: once rows are sorted by start, the running max
  makes each row's merge decision depend only on the rows before it, so
  ordinary window functions and one aggregation do the whole job — no loop,
  no recursive query.

## Exercises

1. The "touching splits" version: change the condition to
   `r.prev_running <= r.start` — do R1's [9,11) [10,12) still merge? Build a
   touching-interval dataset to check the boundary.
2. Rewrite with `fold`: state = current block edge (new block sets it to this
   row's end, inside a block takes the max); compare the two versions.
3. Total occupancy per room: one more `group_by("room").agg()` over merged —
   remember `end - start` is row-level arithmetic: derive first, aggregate
   after.
4. Coverage check: given a query window [a, b), decide whether each room's
   merged blocks **fully cover** it (hint: semi_join or group quantifiers —
   which reads better?).

---
**Previous**: [22 · Balance snapshot](22_balance_snapshot.md) · **Next**: [24 · Pairing in/out events](24_pair_events.md)
