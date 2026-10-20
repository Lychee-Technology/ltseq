<!-- Generated from ../../en/21_basket_pairs.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 21 · Basket Co-occurrence Pairs: Self Join and Dedup Pairing

> **Level** ★★★ · **Domain** E-commerce · **Prereqs** Case 17
>
> **APIs used**: `join(on=, suffix=)` (self join) / `filter` / `s.concat` / `group_by().agg()` / `sort` / `count`

**Learning goals**

- Expand "products within an order" into co-occurrence pairs with a **self
  join** — the first step of market-basket analysis
- Pick up the dedup pairing trick: `product < product_b` makes every pair
  appear exactly once
- Face the row explosion of self joins (N products → N² rows) and understand
  why it is acceptable on single-machine datasets

## The problem

Order details `orders(order_id, product)` — 5 orders, 4 products (products
unique within an order). Goals:

1. Expand every order's **product pairs** (unordered: (A,B) and (B,A) are the
   same pair);
2. Count in how many orders each pair **co-occurs**;
3. Output descending by co-occurrence — the higher, the better the bundle
   placement.

```python
from ltseq import LTSeq

orders = LTSeq.from_rows([
    {"order_id": "O1", "product": "beer"},
    {"order_id": "O1", "product": "diaper"},
    {"order_id": "O2", "product": "beer"},
    {"order_id": "O2", "product": "chips"},
    {"order_id": "O3", "product": "beer"},
    {"order_id": "O3", "product": "diaper"},
    {"order_id": "O3", "product": "chips"},
    {"order_id": "O4", "product": "diaper"},
    {"order_id": "O4", "product": "coke"},
    {"order_id": "O5", "product": "chips"},
    {"order_id": "O5", "product": "coke"},
])

orders.show(11);
```

> **Expected output**
>
> ```text
> +----------+---------+
> | order_id | product |
> +----------+---------+
> | O1       | beer    |
> | O1       | diaper  |
> | O2       | beer    |
> | O2       | chips   |
> | O3       | beer    |
> | O3       | diaper  |
> | O3       | chips   |
> | O4       | diaper  |
> | O4       | coke    |
> | O5       | chips   |
> | O5       | coke    |
> +----------+---------+
> ```

## Step 1 · Self join: the in-order Cartesian product

Join `orders` to itself on `order_id` — every pair of products in an order
meets. Right-column name conflicts get the custom `suffix="_b"`, producing
`product_b`.

```python
cartesian = orders.join(orders, on="order_id", suffix="_b")

cartesian.count()
```

> **Expected output**
>
> ```text
> 25
> ```

**Notes**

- Row-count check: N products per order → N² rows: O1/O2/O4/O5 give 4 each,
  O3 has 3 products → 9, total 25 ✔. This is the **row explosion** of self
  joins: combinations grow with the square of basket size.
- 18 of the 25 rows are noise: 11 (A,A) self-pairs, one per order line, and
  7 mirrors — every real pair appears twice, as (A,B) and (B,A). The next
  step eliminates both kinds with one filter.
- LTSeq's join handles same-table joins, and the default hash join needs no
  ordering. `strategy="merge"` only checks that both inputs are already
  sorted by the key and then runs the same join, so it adds a safety check,
  not speed.

## Step 2 · Dedup pairing: one filter kills self-pairs and mirrors

Strings compare lexicographically. `product < product_b` in one stroke:
(A,A) fails and disappears; every mirror pair keeps only its lexicographically
smaller orientation.

```python
pairs = cartesian.filter(lambda r: r.product < r.product_b)

pairs.sort("order_id", "product", "product_b").show();
```

> **Expected output**
>
> ```text
> +----------+---------+-----------+
> | order_id | product | product_b |
> +----------+---------+-----------+
> | O1       | beer    | diaper    |
> | O2       | beer    | chips     |
> | O3       | beer    | chips     |
> | O3       | beer    | diaper    |
> | O3       | chips   | diaper    |
> | O4       | coke    | diaper    |
> | O5       | chips   | coke      |
> +----------+---------+-----------+
> ```

7 rows — the true unordered pairs of 5 orders: O1 one, O2 one, O3 three, O4
one, O5 one.

**Notes**

- 25 → 7: eleven (A,A) rows and seven mirrors filtered away, leaving 7 real
  combinations — one filter expression compresses the combinatorics.
- The lexicographic comparison only decides **orientation** (which side is A,
  which is B); it carries no business meaning.
- The display sorts by `product_b` as well: O3 has two rows whose `product`
  is beer, and without a third key their order is not defined.
- If products could repeat within an order, `distinct("order_id",
  "product")` before joining — otherwise the same pair counts twice.

## Step 3 · Co-occurrence counts: aggregate per pair

Give each pair a readable key, count via `group_by`, sort descending — the
candidate set for association rules appears.

```python
counted = (
    pairs
    .derive(pair=lambda r: r.product.s.concat(" + ", r.product_b))
    .group_by("pair")
    .agg(n_orders=lambda g: g.order_id.count())
    .sort("n_orders", "pair", desc=[True, False])
)

counted.show();
```

> **Expected output**
>
> ```text
> +----------------+----------+
> | pair           | n_orders |
> +----------------+----------+
> | beer + chips   | 2        |
> | beer + diaper  | 2        |
> | chips + coke   | 1        |
> | chips + diaper | 1        |
> | coke + diaper  | 1        |
> +----------------+----------+
> ```

**Notes**

- `beer + diaper` co-occurs twice (O1, O3) — the miniature of the classic
  beer-and-diapers effect.
- `beer + chips` ties with it at 2. `group_by().agg()` returns groups in no
  defined order, so a sort on `n_orders` alone could print the tied pairs
  either way; the second key `pair` (ascending, hence `desc=[True, False]`)
  pins them.
- `g.order_id.count()` counts the group's rows = orders containing the pair
  (given uniqueness within orders). In `group_by().agg()`, `g.<column>` names
  a column and the aggregate hangs off it; the bare `g.count()` belongs to
  `group_ordered`/`group_sorted` groups. For fancier output,
  `concat_agg(g.order_id)` (imported from `ltseq`) packs the order ids into
  one cell.

## Key takeaways

- **Self join = the universal escape hatch for combination expansion**: LTSeq
  has no dedicated combination operator, but join + an inequality filter
  expresses any "pairwise within a set" semantics — at the price of quadratic
  row counts.
- **The suffix parameter**: `join(..., suffix="_b")` customizes the conflict
  suffix — more business-flavored than the default `_right`; the join key
  itself never carries the suffix.
- **Mind the row explosion**: the self join builds all N² in-order pairs
  before the filter drops most of them, so on very large order tables watch
  memory, or pre-aggregate per order first.
- **Toward association rules**: support = n_orders / total orders,
  confidence = P(B|A) = co-occurrence / occurrences of A — two more
  `group_by` steps complete them (see exercises).

## Exercises

1. Count orders per **single** product (one `group_by("product")`), then
   derive support and confidence columns for a full beer→diaper rule table.
2. Keep pairs with n_orders >= 2 (minimum support) — one filter line; decide
   whether it belongs before or after counting.
3. A histogram of products per order: `group_by("order_id").agg(n=lambda g:
   g.product.count())`, then group the `n` column again — two-level
   aggregation for basket shapes.
4. Use `semi_join` to find orders containing beer, then look at what else
   those baskets hold — the other route to association analysis (filter
   baskets first, combine after).

---
**Previous**: [20 · Sparse reports](20_sparse_report.md) · **Next**: [22 · Balance snapshot](22_balance_snapshot.md)
