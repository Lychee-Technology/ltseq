<!-- Generated from ../../en/08_user_retention.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 08 · User Retention: Set Operations at Work

> **Level** ★★☆ · **Domain** E-commerce user behavior · **Prereqs** Case 01
>
> **APIs used**: `group_by().agg()` / `filter` / `intersect(on=)` / `semi_join` / `anti_join` / `from_rows`

**Learning goals**

- Express retention as a **set intersection**: day-D actives ∩ day-D+1 actives
- Divide labor among `intersect / semi_join / anti_join`
- Loop over day pairs to produce a full retention table

## The problem

A login table `logins(user, date)` over 7 days. Tasks:

1. Daily active users (DAU);
2. **Next-day retention**: of day-D actives, the share still active on D+1 —
   as a per-day table;
3. For any chosen day, list retained and **churned** users.

```python
from ltseq import LTSeq

daily = {
    "D1": ["u1", "u2", "u3", "u4"],
    "D2": ["u1", "u2", "u5"],
    "D3": ["u2", "u3", "u5", "u6"],
    "D4": ["u1", "u2", "u3"],
    "D5": ["u2", "u4"],
    "D6": ["u1", "u2", "u3", "u5", "u6"],
    "D7": ["u2", "u3", "u7"],
}
rows = [{"user": u, "date": d} for d, users in daily.items() for u in users]
logins = LTSeq.from_rows(rows).sort("date", "user")
print(len(logins), "login rows")
```

> **Expected output**
>
> ```text
> 24 login rows
> ```

## Step 1 · DAU: grouped counting

```python
dau = logins.group_by("date").agg(active=lambda g: g.user.count())
# agg rows have no defined order; sort before display
dau.sort("date").show();
```

> **Expected output**
>
> ```text
> +------+--------+
> | date | active |
> +------+--------+
> | D1   | 4      |
> | D2   | 3      |
> | D3   | 4      |
> | D4   | 3      |
> | D5   | 2      |
> | D6   | 5      |
> | D7   | 3      |
> +------+--------+
> ```

## Step 2 · Next-day retention: intersect per day pair

Retention = the **intersection** of two daily active sets. LTSeq's
`intersect(other, on=key)` says it directly:

```python
days = sorted(daily.keys())

retention = []
for d_prev, d_next in zip(days, days[1:]):
    prev_u = logins.filter(lambda r: r.date == d_prev).select("user")
    next_u = logins.filter(lambda r: r.date == d_next).select("user")
    kept   = prev_u.intersect(next_u, on=lambda r: r.user)

    retention.append({
        "date":     d_next,
        "active":   len(prev_u),
        "retained": len(kept),
        "rate":     round(len(kept) / len(prev_u), 3),
    })

LTSeq.from_rows(retention).show();
```

> **Expected output**
>
> ```text
> +------+--------+----------+-------+
> | date | active | retained | rate  |
> +------+--------+----------+-------+
> | D2   | 4      | 2        | 0.5   |
> | D3   | 3      | 2        | 0.667 |
> | D4   | 4      | 2        | 0.5   |
> | D5   | 3      | 1        | 0.333 |
> | D6   | 2      | 1        | 0.5   |
> | D7   | 5      | 2        | 0.4   |
> +------+--------+----------+-------+
> ```

**Notes**

- `intersect(other, on=lambda r: r.user)` intersects on the key and returns
  left-shaped rows; without `on` it compares all columns.
- Each loop iteration builds **independent lazy plans** — repeated full scans,
  fine at analysis scale; for very large tables rewrite as one self-join plus
  conditional aggregation (see exercises).
- `len(kept)` is `kept.count()` — LTSeq implements `__len__`.
- The `date` column is the **retention observation day**; `active` is the
  previous day's size.

## Step 3 · Names: semi_join / anti_join

Intersections are set operations (duplicate rows collapse); rosters often want
every left row as it is. `semi_join` keeps left rows whose key exists on the
right (no fan-out); `anti_join` keeps those that don't — the "retained" and
"churned" lists.

```python
d6 = logins.filter(lambda r: r.date == "D6").select("user")
d7 = logins.filter(lambda r: r.date == "D7").select("user")

print("== D6 → D7 retained ==")
d6.semi_join(d7, on=lambda a, b: a.user == b.user).sort("user").show()

print("== D6 → D7 churned ==")
d6.anti_join(d7, on=lambda a, b: a.user == b.user).sort("user").show();
```

> **Expected output**
>
> ```text
> == D6 → D7 retained ==
> +------+
> | user |
> +------+
> | u2   |
> | u3   |
> +------+
>
> == D6 → D7 churned ==
> +------+
> | user |
> +------+
> | u1   |
> | u5   |
> | u6   |
> +------+
> ```

**Notes**

- `semi_join` / `anti_join` keep **only left columns**, and one-to-many
  matches never duplicate left rows — semantically SQL's
  `WHERE EXISTS / NOT EXISTS`.
- Versus `intersect`: both return **left-table rows**, but `intersect` is a
  set operation, so identical rows collapse into one, while semi/anti keep
  **every left row**, duplicates included. On this one-column roster they
  agree. Rates → intersect; rosters → semi/anti.

## Key takeaways

1. **Retention = set intersection**: translate the business question into set
   language and let the engine do the rest.
2. **The trio's division of labor**: `intersect` for rates, `semi_join` for
   retained rosters, `anti_join` for churned rosters — remember "set or
   filter".
3. **Loop + lazy**: building a fresh plan per day in a Python loop is clear;
   performance-critical versions collapse into one join + aggregation — both
   styles are first-class in LTSeq.
4. **Where order matters**: set operations ignore order; windows don't.
   Knowing which operators consume order is part of sequence thinking.
   Grouping and joins don't produce a defined order either, which is why the
   DAU table and the rosters are sorted before display.

## Exercises

- **7-day retention**: replace "next day" with "7 days later" — extend the
  sample past seven days, then try `r.date.dt.add(days=7)` on a `date32`
  column, or string date offsets.
- New users per day: `today.anti_join(history, on=...)`.
- Full-period retention in one plan: self-join `logins` on user, filter a
  one-day gap, group by the earlier day.

---
**Previous**: [07 · Conversion funnel](07_funnel_analysis.md) · **Next**: [09 · RFM segmentation](09_rfm_segments.md)
