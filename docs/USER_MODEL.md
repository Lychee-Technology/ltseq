# LTSeq User Model

Related documents:

- `docs/README.md`: documentation index
- `docs/USER_MODEL.cn.md`: Chinese user guide
- `docs/ARCHITECTURE.md`: system architecture and execution model
- `docs/api.md`: API reference
- `docs/LINKING_GUIDE.md`: focused linking guide

## What LTSeq Is

LTSeq is a Python data processing library for ordered data. It is built for workflows where row order is part of the meaning of the dataset, not just a presentation detail.

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

LTSeq is not trying to be a drop-in clone of pandas or generic SQL tables.

The biggest difference is this:

- many dataframe systems assume tables are unordered unless you sort right before display
- LTSeq treats order as a semantic input to computation

That is why some APIs ask you to sort first, why sort metadata is tracked, and why certain operations fail when order is unknown.

---

## Core Mental Model

The easiest way to use LTSeq correctly is to think in four ideas.

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
rows = t.collect()
```

This pattern is the intended use model for the library.

---

## Why Sort Is Explicit

Many LTSeq users first encounter the library through an error like:

```text
window function used without sort
```

That error is deliberate.

In LTSeq, sequence operations such as `shift`, `diff`, `rolling`, and many ranking patterns are undefined without a known order. Rather than guessing, LTSeq forces the query to state the ordering assumption.

This has two benefits:

- results are more trustworthy
- the query is easier to read and review later

If a result depends on sequence, the sequence should appear in the code.

---

## Choosing the Right Object Type

Most work starts with `LTSeq`, but other objects appear when the query enters a richer semantic context.

### `LTSeq`

Use `LTSeq` for ordinary table transforms, joins, sequence expressions, and exports.

### `NestedTable`

Returned by `group_ordered()` or `group_sorted()`.

Use it when you want to operate on groups while preserving grouped row context instead of collapsing immediately to one row per group.

Think of it as: "this table now has grouped semantics attached to it."

### `LinkedTable`

Returned by `link()`.

Use it when you want a lazy relationship to another table without paying the cost of a join up front.

Think of it as: "this table can see another table if and when I actually need it."

### `PartitionedTable`

Returned by `partition()`.

Use it when you want grouped access by partition key rather than one global flat table workflow.

---

## Linking vs Joining

This is one of the most important user-facing distinctions in LTSeq.

### Join

`join()` creates a physical combined table result.

Use it when:

- you know you need both sides immediately
- downstream steps treat the result like one flat table

### Link

`link()` creates a lazy relationship object.

Use it when:

- you want to defer join cost
- many operations touch only the left table
- you want a more relationship-oriented workflow

The key idea is that linking lets you stay lazy longer.

---

## Grouping Model

Traditional SQL grouping usually collapses rows. LTSeq has that style too, but it also supports grouped sequential context.

With `group_ordered()`, the important question is often not just:

- "what is the aggregate for this key?"

but also:

- "what are the rows inside each consecutive group?"
- "what is the first/last row in each run?"
- "which groups satisfy a condition based on their internal sequence?"

That is why LTSeq returns `NestedTable` for these workflows instead of forcing an immediate flat aggregation result.

---

## Expression Model

Expressions are built with Python lambdas, but the lambdas are declarative, not row-iterative.

Good examples:

```python
t.filter(lambda r: r.amount > 100)
t.derive(total=lambda r: r.price * r.quantity)
t.select(lambda r: [r.id, r.total])
```

The main thing to remember is that the lambda should describe a computation in terms of columns and expressions. It should not contain arbitrary Python control flow that expects real row values.

---

## Materialization Model

LTSeq tries hard not to materialize data accidentally during table-to-table workflows.

From a user perspective, that means:

- chaining table operations stays efficient longer
- exporting to pandas or Arrow is an explicit boundary
- `show()` is cheap enough for inspection but still a terminal action

If you call `to_pandas()` or `collect()`, you are asking LTSeq to leave the lazy query world and produce concrete data.

That is useful, but it changes the cost model.

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
3. `link()` when right-side access is conditional or infrequent
4. `join()` when flat physical results are definitely needed

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

### Using `join()` when `link()` would be cheaper

If you only sometimes need right-side data, linking may be the better fit.

### Assuming all grouping means immediate aggregation

In LTSeq, some grouping workflows preserve row context on purpose.

---

## Best Practices

Use these as default habits:

1. Sort explicitly before any order-dependent logic.
2. Keep workflows lazy as long as possible.
3. Use `show()` and focused tests to validate assumptions incrementally.
4. Prefer `link()` for lazy relationship exploration and `join()` for definite flat-table work.
5. Treat `NestedTable` as a grouped context object, not just another dataframe.

---

## One-Sentence Summary

The best mental model for LTSeq is: a lazy, immutable, sequence-aware query system where Python describes the work and Rust executes it.
