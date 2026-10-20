<!-- Generated from ../../en/07_funnel_analysis.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 07 · Conversion Funnel: search_pattern

> **Level** ★★★ · **Domain** E-commerce user behavior · **Prereqs** Case 01
>
> **APIs used**: `search_pattern` / `search_pattern_count` / `partition_by` / `sort` / `filter` / `distinct`

**Learning goals**

- Express a funnel natively: `search_pattern` matches "view → cart → order" as
  a **consecutive row pattern**
- Internalize the adjacent-rows semantics and `partition_by` isolation
- Go from match details to a conversion rate in two steps

## The problem

Event table `events(user, ts, event)` with `event ∈ {view, cart, order, pay}`.
Marketing wants the "view → cart → order" three-step funnel: how many users
convert, and at what rate.

**Key semantics**: `search_pattern` matches **adjacent rows** — step 1 at row
i, step 2 at row i+1, step 3 at row i+2; `partition_by` keeps the pattern from
crossing users. The sample data below deliberately includes typical shapes so
you can verify your understanding:

```python
from ltseq import LTSeq

plans = {
    "u1": ["view", "cart", "order"],                    # full hit
    "u2": ["view", "pay", "cart", "order"],             # pay in between → broken
    "u3": ["cart", "order"],                            # missing leading view
    "u4": ["view", "cart", "view", "cart", "order"],    # hits on round two
    "u5": ["view", "order"],                            # skips cart
    "u6": ["order", "cart", "view"],                    # reversed
    "u7": ["view", "view", "cart", "cart", "order"],    # no adjacent triple
    "u8": ["view", "cart", "order"],                    # full hit
}
rows, ts = [], 1000
for u, seq_ in plans.items():
    uts = ts
    for ev in seq_:
        uts += 10
        rows.append({"user": u, "ts": uts, "event": ev})
    ts += 1000

events = LTSeq.from_rows(rows).sort("user", "ts")     # sort by (user, ts) first
events.select("user", "ts", "event").head(6).show();
```

> **Expected output**
>
> ```text
> +------+------+-------+
> | user | ts   | event |
> +------+------+-------+
> | u1   | 1010 | view  |
> | u1   | 1020 | cart  |
> | u1   | 1030 | order |
> | u2   | 2010 | view  |
> | u2   | 2020 | pay   |
> | u2   | 2030 | cart  |
> +------+------+-------+
> ```

**Notes**

- Hand-built data makes every semantic visible: predict which users hit
  (`u1 / u4 / u8`), then verify against the output.

## Step 1 · Match the three-step funnel

```python
hits = events.search_pattern(
    lambda r: r.event == "view",
    lambda r: r.event == "cart",
    lambda r: r.event == "order",
    partition_by="user",
)
hits.select("user", "ts", "event").show()

n_hits = events.search_pattern_count(
    lambda r: r.event == "view",
    lambda r: r.event == "cart",
    lambda r: r.event == "order",
    partition_by="user",
)
print("funnel hits:", n_hits)
```

> **Expected output**
>
> ```text
> +------+------+-------+
> | user | ts   | event |
> +------+------+-------+
> | u1   | 1010 | view  |
> | u4   | 4030 | view  |
> | u8   | 8010 | view  |
> +------+------+-------+
>
> funnel hits: 3
> ```

**Notes**

- `search_pattern` returns the **step-1 rows** (one row per match);
  `search_pattern_count` returns just the number and reads only the columns
  the steps use.
- Each step is a row-level predicate lambda — arbitrary conditions allowed
  (e.g. `r.url.s.starts_with("/pay")`).
- `partition_by="user"`: the pattern **cannot cross user boundaries**; omit it
  and users cross-match.
- `u4` matches on its **second round** — `search_pattern` reports every start,
  not one per user.

## Step 2 · From matches to conversion

```python
viewed    = events.filter(lambda r: r.event == "view").distinct("user").count()
converted = hits.distinct("user").count()

print(f"users with a view: {viewed}")
print(f"users completing the funnel: {converted}")
print(f"conversion: {converted / viewed:.0%}")
```

> **Expected output**
>
> ```text
> users with a view: 7
> users completing the funnel: 3
> conversion: 43%
> ```

**Notes**

- Two-step summary: `distinct("user")` then `count()`; `distinct` without keys
  dedupes on all columns.
- The denominator definition (viewed users vs all users) is a business
  decision; the code just expresses it faithfully.

## Key takeaways

1. **Funnels are first-class**: no triple self-join, no flag gymnastics — one
   operator states the intent.
2. **Adjacent-rows semantics are binding**: any unmatched row in between breaks
   the chain. Real event streams with interleaved noise need the loose funnel
   (see exercises).
3. **Sort first**: patterns follow the **current table order**;
   `sort("user", "ts")` is a correctness prerequisite.
4. **Two exits**: details via `search_pattern`, numbers via
   `search_pattern_count` — the latter is friendlier for dashboards.

## Exercises

- **Loose funnel** (gaps allowed): build per-user flags
  `has_view / has_cart_after_view / has_order_after_cart`: running flags with
  `cum_max().over(partition_by=r.user)`, then `group_by + max`.
- Add `pay` as step four and compare drop-off.
- Reuse `search_pattern_count` on logs: detect `ERROR → ERROR → FATAL`
  preludes (case 11 shows the window-based alternative).

---
**Previous**: [06 · Max drawdown](06_max_drawdown.md) · **Next**: [08 · User retention](08_user_retention.md)
