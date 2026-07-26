# ADR 0011: `link()` Is a Lazy Prefix-Aliased Equi-Join — Not a Pointer/Take Structure

- Status: Accepted (supersedes the earlier pointer/take design)
- Date: 2026-07-26 (recorded; decision predates the ADR)

[中文版](0011-link-lazy-prefix-aliased-join.cn.md)

## Context

Foreign-key navigation and fact-to-dimension enrichment need cross-table access; multi-hop chains need unambiguous column names. The question was what `link()` actually *is* under the hood, and how it relates to plain `join()`.

## Decision

`link(target, on, as_/alias, join_type)` records the join condition and alias and computes the joined schema up front, but **executes nothing**. It builds one lazy DataFusion join plan, cached so repeated transforms reuse the same join node.

- **Naming**: target columns are exposed as `{alias}_{col}`; source columns keep their names. Reference linked columns by prefix (`r.prod_price`, not `r.prod.price`).
- **Join semantics**: all four join types (inner/left/right/full); equality-only conditions; composite keys via `&` (`|` unsupported).
- **Transforms run on the joined plan.** Every `select`/`filter`/`derive`/`sort`/`slice`/`distinct` on a `LinkedTable` runs against the joined plan and returns a plain `LTSeq`, so rows follow the join (unmatched rows dropped/added; one-to-many fan-out visible). Chained `link()` layers on the previous join's real plan, so later conditions may reference the previous alias's columns.
- **`to_ltseq()`** returns the lazy joined `LTSeq`; **`collect()`** executes.

### Evolution: what this supersedes

- An earlier design treated linking as a **pointer/take structure** (cheap per-row navigation). The current docs are explicit: "It is a lazy join — not a pointer/take structure and not a cheap per-row navigation."
- A **source-only shortcut** ("filter the source first for speed") existed and was removed: "There is no 'filter the source first for speed' shortcut anymore (it produced wrong rows for unmatched/fan-out joins)." Correctness beat hand-rolled speed hacks; predicate/projection pushdown is delegated to the optimizer.
- Residue of the old design still exists in prose: `CLAUDE.md` describes `linking.py` as "LinkedTable for pointer-based joins" (and tests as "Pointer-based join tests"), and the `README.md` FAQ contrasts linking with "full data materialization" joins. Those descriptions are outdated relative to `LINKING_GUIDE.md`/`api.md` and should be updated separately.

### Related join-surface decisions

- **Two coexisting surfaces**: `link()` namespaces the *whole* target as `{alias}_col` (keeps multi-hop chains unambiguous) and offers `LinkedTable` chaining sugar — use for enrichment. `join()` uses Polars-style **conflict-only** suffixes (`suffix="_right"`) and returns `LTSeq` directly — use for one-off relational joins. Neither materializes until consumed.
- **Conflict strategy**: the join implementation renames right-side columns aggressively before joining, then aliases them back into the user-visible shape (`src/ops/join.rs`) — "proven necessary for correctness and predictability." Inner/left joins coalesce the duplicate right key column; right/full joins keep both keys.
- **Strategy matrix**: `join` (hash, default); `join(strategy="merge")` for pre-sorted inputs — *validates* sort order and raises `SortRequiredError` otherwise; `semi_join`/`anti_join` (`WHERE EXISTS` / `NOT EXISTS`); `asof_join` (ordered/binary search, `src/ops/asof_join.rs`, API aligned with Polars `join_asof`; the right time column is deliberately kept because "an asof match is approximate, so the matched timestamp is real information"); expression-level `r.col.lookup(target, column, join_key)` resolves a single-column left join during `derive()` (`lookup.py`) — join-like enrichment without a user-level join step.

## Consequences

- One consistent mental model: everything is a lazy DataFusion join; the differences are naming conventions and ergonomics.
- Join type is baked into the plan and survives downstream transforms; materialize late is the stated best practice.
- The prefix scheme keeps multi-hop chains unambiguous at the cost of longer column names.

## Sources

- `docs/LINKING_GUIDE.md` — entire document (incl. "link() vs join()", the removed shortcut, ProjectionPushdown caveat)
- `docs/DESIGN_SUMMARY.md` — §4.1, §4.2
- `docs/USER_MODEL.md` — Linking vs Joining
- `docs/api.md` — `LTSeq.link`, `LTSeq.join`, `asof_join`, Join Strategy Summary
- `docs/MODULE_GUIDE.md` — `src/ops/join.rs`, `lookup.py`
- `CLAUDE.md`, `README.md` — outdated "pointer-based" wording (see Evolution)
