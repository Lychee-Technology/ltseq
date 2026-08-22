# LTSeq User Model

Related documents:

- `docs/README.md`: documentation index
- `docs/USER_MODEL.cn.md`: Chinese user guide
- `docs/ARCHITECTURE.md`: system architecture and execution model
- `docs/api.md`: API reference
- `docs/LINKING_GUIDE.md`: focused linking guide

## What LTSeq Is

LTSeq is a Python data processing library for ordered data. It is built for workflows where row order carries meaning, rather than being a presentation detail.

Examples:

- time-series analysis
- event streams
- streak and run detection
- sequence grouping
- nearest-in-time joins
- lazy relationship navigation across tables

LTSeq uses a Python API, but most execution happens in a Rust/DataFusion engine.

---

## What LTSeq Is Not

LTSeq is not a drop-in clone of pandas or of generic SQL tables.

The difference: most dataframe systems assume tables are unordered unless you sort right before display. LTSeq treats order as an input to the computation.

That is why some APIs ask you to sort first, why sort metadata is tracked, and why certain operations fail when order is unknown.

---

## Core Mental Model

Four ideas cover most of it.

### 1. Tables are immutable query objects

Most operations return a new object instead of modifying the original one.

```python
t1 = LTSeq.read_csv("orders.csv")
t2 = t1.filter(lambda r: r.amount > 100)
```

`t1` is still the original table. `t2` is a new query derived from it.

### 2. Work is lazy until you ask for results

Operations like `filter`, `select`, `derive`, and `sort` usually build a plan instead of immediately computing data.

Execution typically happens when you call something like:

- `show()`
- `count()`
- `collect()`
- `to_arrow()`
- `to_pandas()`
- `write_csv()`
- `write_parquet()`

### 3. Order matters

If an operation depends on row sequence, LTSeq expects you to establish that sequence explicitly.

```python
prices = LTSeq.read_csv("prices.csv").sort("timestamp")
result = prices.derive(prev=lambda r: r.price.shift(1))
```

The explicit `sort()` is part of the meaning of the query.

### 4. Lambda expressions describe work; they do not execute row-by-row in Python

When you write:

```python
t.filter(lambda r: r.age > 18)
```

the lambda is captured as an expression and executed by the Rust engine. `r` is not a real Python row object.

---

## The Typical LTSeq Workflow

Most LTSeq workflows follow a small number of recurring steps.

### Step 1: Load data

```python
t = LTSeq.read_csv("events.csv")
```

### Step 2: Establish order if the workflow is sequence-sensitive

```python
t = t.sort("user_id", "event_time")
```

### Step 3: Transform lazily

```python
t = t.filter(lambda r: r.event_type == "click")
t = t.derive(next_gap=lambda r: r.event_time.diff(1))
```

### Step 4: Use higher-level semantics when needed

Examples:

- `group_ordered()` for consecutive-group logic
- `link()` for lazy relationship navigation
- `partition()` for partitioned access

### Step 5: Materialize only when you need output

```python
t.show()
rows = t.to_dicts()
```

---

## Why Sort Is Explicit

Many LTSeq users first encounter the library through an error like:

```text
window function used without sort
```

That error is deliberate.

Sequence operations such as `shift`, `diff`, `rolling`, and many ranking patterns are undefined without a known order. Rather than guessing, LTSeq makes the query state its ordering assumption. The results are then trustworthy, and the next person to read the query can see what it assumed.

If a result depends on sequence, the sequence should appear in the code.

---

## Choosing the Right Object Type

Most work starts with `LTSeq`, but other objects appear when the query enters a richer semantic context.

### `LTSeq`

Use `LTSeq` for ordinary table transforms, joins, sequence expressions, and exports.

### `NestedTable`

Returned by `group_ordered()` or `group_sorted()`.

Use it when you want to operate on groups while keeping the rows inside each group, instead of collapsing immediately to one row per group.

### `LinkedTable`

Returned by `link()`.

Use it when you want to enrich a table from another table with prefix-aliased columns (`{alias}_{col}`), staying lazy until the result is consumed. The join plan is built lazily and every transform runs on the joined plan. Read it as "this table, enriched by another table, still lazy".

### `PartitionedTable`

Returned by `partition()`.

Use it when you want grouped access by partition key rather than one global flat table workflow.

---

## Linking vs Joining

Both build the same kind of lazy DataFusion join, and neither materializes until the result is consumed. The difference is naming convention and ergonomics.

### Join

`join()` returns a plain `LTSeq` with Polars-style naming: only conflicting right-side columns get a suffix (`suffix="_right"`); non-conflicting columns keep their names.

Use it when:

- you want a one-off relational join
- downstream steps treat the result like one flat table

### Link

`link()` returns a `LinkedTable` that namespaces the *whole* target table as `{alias}_{col}`.

Use it when:

- you are enriching a fact table from dimension tables
- you chain multiple hops and need unambiguous column names
- you want a more relationship-oriented workflow

Every transform on a `LinkedTable` runs on the joined plan and returns a plain `LTSeq`, so rows follow the join (unmatched rows and one-to-many fan-out are reflected). See the Linking Guide for details.

---

## Grouping Model

Traditional SQL grouping collapses rows. LTSeq has that style too, and it also keeps grouped sequential context.

With `group_ordered()`, the question is often not "what is the aggregate for this key?" but something about the rows inside each consecutive group: what they are, which one comes first or last in the run, or whether the group's internal sequence satisfies some condition.

That is why these workflows return `NestedTable` rather than an immediate flat aggregation.

---

## Expression Model

Expressions are built with Python lambdas, but the lambdas are declarative, not row-iterative.

Good examples:

```python
t.filter(lambda r: r.amount > 100)
t.derive(total=lambda r: r.price * r.quantity)
t.select(lambda r: [r.id, r.total])
```

The lambda should describe a computation in terms of columns and expressions. It should not contain Python control flow that expects real row values.

---

## Materialization Model

LTSeq avoids materializing data during table-to-table workflows. For a user, that means chaining table operations stays efficient longer, and exporting to pandas or Arrow is an explicit boundary. `show()` is cheap enough for inspection, but it is still a terminal action.

Calling `to_pandas()` or `collect()` asks LTSeq to leave the lazy query world and produce concrete data. That is often what you want, but it changes the cost model.

---

## Recommended Usage Patterns

### For time-series or event streams

Use:

1. `read_*`
2. `sort(...)`
3. window or sequence operations
4. optional `group_ordered()`

### For relationship-heavy analytics

Use:

1. `read_*`
2. left-side filtering first
3. `link()` for dimension enrichment and multi-hop chains (whole-table `{alias}_{col}` prefixes)
4. `join()` for one-off relational joins (conflict-only suffixes)

### For large pipelines

Prefer staying in LTSeq until the end of the workflow. Delay `to_pandas()` and `collect()` until you truly need interoperability or final output.

---

## Common Mistakes

### Forgetting to sort before sequence operations

If the logic depends on previous/next rows, sort first.

### Thinking lambdas run over Python rows

They do not. They build expressions.

### Materializing too early

Calling `to_pandas()` too soon gives up the benefits of lazy planning and Rust-side execution.

### Assuming `link()` and `join()` differ in cost

They build the same kind of lazy DataFusion join, and neither materializes until consumed. Choose by naming and ergonomics (prefix-aliased enrichment for `link`, a one-off flat join for `join`), not by expected cost.

### Assuming all grouping means immediate aggregation

In LTSeq, some grouping workflows preserve row context on purpose.

---

## Best Practices

Use these as default habits:

1. Sort explicitly before any order-dependent logic.
2. Keep workflows lazy as long as possible.
3. Use `show()` and focused tests to validate assumptions incrementally.
4. Prefer `link()` for enrichment and multi-hop naming, and `join()` for one-off relational joins.
5. Treat `NestedTable` as a grouped context object, not just another dataframe.

---

## One-Sentence Summary

LTSeq is a lazy, immutable, sequence-aware query system: Python describes the work, Rust executes it.
