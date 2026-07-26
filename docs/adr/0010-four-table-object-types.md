# ADR 0010: Four High-Level Table Object Types Signal Semantic Context

- Status: Accepted
- Date: 2026-07-26 (recorded; decision predates the ADR)

[中文版](0010-four-table-object-types.cn.md)

## Context

Grouping, linking, and partitioning all attach extra semantics to a table. Collapsing everything into a single class would hide those semantics; SQL-style grouping that immediately collapses rows would lose the sequential questions LTSeq cares about ("what are the rows inside each consecutive run?", "first/last row of each run?").

## Decision

The API deliberately returns **different wrapper types to signal semantics**, and choosing which one to return is an explicit responsibility of the Python orchestration layer:

- **`LTSeq`** — an ordinary (ordered) table.
- **`NestedTable`** — "this table now has grouped semantics attached." `group_ordered()` (alias `group_consecutive`) and `group_sorted()` return grouped *sequential context* instead of eagerly collapsing. Internal columns `__group_id__`, `__group_count__`, `__rn__` preserve group identity and row position. `NestedTable.derive` **broadcasts** group values to every row (SQL window semantics); `NestedTable.agg` **collapses** to one row per group (SQL GROUP BY semantics) and explicitly replaces the old `derive(...) + distinct(...)` idiom. Groups appear in original sequence order; `group_ordered` groups only *consecutive* equal values and never reorders. `len(nested)` returns row count, not group count; `to_pandas()` drops `__group_id__` (use `flatten()`). A deliberate asymmetry: group aggregations take column names as strings (`g.sum("amount")`) while `g.first()`/`g.last()` return row proxies with attribute access.
- **`LinkedTable`** — "this table can see another table if and when I actually need it" (see [ADR 0011](0011-link-lazy-prefix-aliased-join.md)).
- **`PartitionedTable`** — dict-like grouped access by key. `partition(*cols)` / `partition(by=callable)`; a callable key **must be a simple column expression** (`lambda r: r.region`) — derived expressions (`lambda r: r.price + 1`) raise `ValueError` because they would force internal materialization, violating [ADR 0005](0005-no-materialization-rule.md).

All four stay lazy; deferred grouping/joining is materialized only when required ([ADR 0004](0004-lazy-execution-immutable-tables.md)).

## Consequences

- The type itself documents what semantics are in play; users pick the object for the question they are asking (flat transforms vs grouped context vs cross-table navigation vs keyed access).
- Maintaining consistency between the lazy linked/grouped abstractions and the flat table APIs is a named pressure point.

## Sources

- `docs/USER_MODEL.md` — Choosing the Right Object Type, Grouping Model
- `docs/ARCHITECTURE.md` — Major Runtime Objects; Grouping/Linking/Partitioning; Architectural Risks
- `docs/DESIGN_SUMMARY.md` — §4.3, §4.4
- `docs/api.md` — §0, §4, `LTSeq.partition`, `PartitionedTable`
- `README.md` — Limitations (`group_ordered` semantics)
