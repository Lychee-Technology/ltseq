<!-- Generated from ../../en/22_balance_snapshot.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 22 · Balance Snapshot: Grouped Running Totals and the Last Row

> **Level** ★★☆ · **Domain** E-commerce · **Prereqs** Cases 01, 14
>
> **APIs used**: `sort` / `cum_sum(partition_by=)` (expression form) / `group_sorted` / `NestedTable.last` / `NestedTable.filter` + `g.any` quantifier

**Learning goals**

- Maintain a running balance per account with **partitioned accumulation** —
  `cum_sum`, the sibling of `shift`, in its expression form
- Take "the last row of each group" in one shot with
  `group_sorted(...).last()` — the standard move for latest snapshots
- Use the `g.any()` group quantifier for **existence judgments** like "has
  ever been overdrawn"

## The problem

A wallet ledger `ledger(account, seq, delta)` where delta is the
top-up/spend amount. Goals:

1. Compute the **balance after each transaction** (in-group running total);
2. Take each account's **latest snapshot** (last row) — the account balance
   table;
3. Find the current snapshot of accounts that have **ever been overdrawn**
   (balance went negative at some point).

```python
from ltseq import LTSeq

ledger = LTSeq.from_rows([
    {"account": "A1", "seq": 1, "delta": 100},
    {"account": "A1", "seq": 2, "delta": -30},
    {"account": "A1", "seq": 3, "delta": 20},
    {"account": "A1", "seq": 4, "delta": -200},
    {"account": "A2", "seq": 1, "delta": 50},
    {"account": "A2", "seq": 2, "delta": 50},
    {"account": "A2", "seq": 3, "delta": -20},
    {"account": "A3", "seq": 1, "delta": 10},
    {"account": "A3", "seq": 2, "delta": -5},
]).sort("account", "seq")

ledger.show();
```

> **Expected output**
>
> ```text
> +---------+-----+-------+
> | account | seq | delta |
> +---------+-----+-------+
> | A1      | 1   | 100   |
> | A1      | 2   | -30   |
> | A1      | 3   | 20    |
> | A1      | 4   | -200  |
> | A2      | 1   | 50    |
> | A2      | 2   | 50    |
> | A2      | 3   | -20   |
> | A3      | 1   | 10    |
> | A3      | 2   | -5    |
> +---------+-----+-------+
> ```

Predicted balance tracks: A1 100 → 70 → 90 → **-110** (overdrawn); A2 → 80;
A3 → 5.

## Step 1 · Partitioned accumulation: one balance track per account

`r.delta.cum_sum(partition_by="account")` — the expression form of cumulative
sum, resetting at account boundaries. Same operator as case 15, this time
accumulating signed amounts.

```python
balances = ledger.derive(
    balance=lambda r: r.delta.cum_sum(partition_by="account"),
)

balances.show();
```

> **Expected output**
>
> ```text
> +---------+-----+-------+---------+
> | account | seq | delta | balance |
> +---------+-----+-------+---------+
> | A1      | 1   | 100   | 100     |
> | A1      | 2   | -30   | 70      |
> | A1      | 3   | 20    | 90      |
> | A1      | 4   | -200  | -110    |
> | A2      | 1   | 50    | 50      |
> | A2      | 2   | 50    | 100     |
> | A2      | 3   | -20   | 80      |
> | A3      | 1   | 10    | 10      |
> | A3      | 2   | -5    | 5       |
> +---------+-----+-------+---------+
> ```

**Notes**

- `partition_by` starts the accumulation **from zero on each account's first
  row** — A2's first 50 accumulates to 50 instead of continuing A1's -110.
  Contrast: a whole-table `cum_sum` without partition_by would thread all
  accounts into one line, ruining the balance table.
- The sort keys `(account, seq)` give each account its transactions in
  sequence order — what the accumulation needs — and keep each account's
  rows adjacent, which Step 2's `group_sorted` needs (cases 14/15/18 sort
  the same way).
- The -110 is written out as-is: LTSeq does not clip business anomalies for
  you; detecting them is the next step's job.

## Step 2 · Latest snapshot: group_sorted().last()

The last row per group is the account's current balance. `group_sorted` makes
one group per account and `last()` takes each group's tail row — richer than
`group_by` aggregation because **entire rows survive**.

```python
snapshot = (
    balances
    .sort("account", "seq")
    .group_sorted(lambda r: r.account)
    .last()
)

snapshot.show();
```

> **Expected output**
>
> ```text
> +---------+-----+-------+---------+
> | account | seq | delta | balance |
> +---------+-----+-------+---------+
> | A1      | 4   | -200  | -110    |
> | A2      | 3   | -20   | 80      |
> | A3      | 2   | -5    | 5       |
> +---------+-----+-------+---------+
> ```

**Notes**

- `group_sorted(key)` requires the key to **lead the declared sort**:
  `sort("account", "seq")` leads with account, satisfying the constraint.
  Groups are runs of equal keys in that order, as with `group_ordered`.
- `last()` returns an ordinary LTSeq — the tail rows' **full columns,
  derived ones included** (balance). An aggregation keeps only values:
  `agg(balance=lambda g: g.balance.max())` returns one number per account
  and loses seq and delta — and that number is the peak balance (A1: 100),
  not the latest (-110). "Take rows" and "take values" part ways here.
- Case 16 read the edge rows' values inside `NestedTable.agg`
  (`g.first().date`, `g.last().date`); `NestedTable.first()`/`last()` return
  those rows whole, turning "newest/oldest per group" into one line.

## Step 3 · Has ever been overdrawn: the group quantifier g.any

"Has ever been overdrawn" is a **group-level existence check**: any row in
the group with balance < 0. `NestedTable.filter` with `g.any(row predicate)`
exists precisely for this.

```python
risk = (
    balances
    .sort("account", "seq")
    .group_sorted(lambda r: r.account)
    .filter(lambda g: g.any(lambda r: r.balance < 0))
    .last()
)

risk.show();
```

> **Expected output**
>
> ```text
> +---------+-----+-------+---------+
> | account | seq | delta | balance |
> +---------+-----+-------+---------+
> | A1      | 4   | -200  | -110    |
> +---------+-----+-------+---------+
> ```

**Notes**

- `g.any(lambda r: r.balance < 0)`: a row-level predicate handed to a
  group-level quantifier — the trio `g.all() / g.any() / g.none()` mirrors
  SQL's EVERY / ANY / NOT ANY.
- After `filter` the object is still a NestedTable, so `.last()` chains on —
  "filter groups" and "take rows" compose with independent semantics.
- Note the judgment is **ever** overdrawn: A1 is also currently negative, so
  both definitions coincide here. If A1 later topped up into positive, `g.any`
  would still catch it — the risk-control "historical stain" view; for the
  current-state view use `snapshot.filter(lambda r: r.balance < 0)`.

## Key takeaways

- **Choosing cum_sum forms**: custom name or partition → expression form
  `r.col.cum_sum(partition_by=)`; a quick extra column → table-level
  `t.cum_sum("col")`. Anything grouped always takes the expression form.
- **Rows vs values**: `NestedTable.last()` keeps whole rows (context
  included); `agg` keeps aggregates only. Snapshot needs (latest/earliest
  row) belong to the former.
- **Group quantifiers are the switch for existence questions**: "has an
  overdraft row" (any), "all succeeded" (all), "no failures" (none) — an SQL
  EXISTS subquery translated into one predicate.

## Exercises

1. Add `is_overdraft` (balance < 0) to `balances`, then find accounts
   overdrawn **throughout** with `g.all(lambda r: r.is_overdraft == True)`
   (none here — craft a ledger where one appears).
2. A drawdown-style metric per account: lowest-ever balance
   (`g.balance.min()`) joined with the latest balance (hint:
   `group_by("account").agg()` the min, then join with snapshot on account).
3. Replace seq with a timestamp column, keep the order, and verify nothing
   changes — snapshot logic depends on **declared order**, not on whether it
   is a sequence number or a time.
4. Rebuild the snapshot without `group_sorted`:
   `group_by("account").agg(last_seq=lambda g: g.seq.max())`, then join it
   back to `balances` with `left_on=["account", "seq"],
   right_on=["account", "last_seq"]`. Compare the two routes: which one needs
   the extra join, and why?

---
**Previous**: [21 · Basket co-occurrence pairs](21_basket_pairs.md) · **Next**: [23 · Merging overlapping intervals](23_interval_merge.md)
