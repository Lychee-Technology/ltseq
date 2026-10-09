<!-- v0.5-modular:header -->

# v0.5 review: Executive verdict and the contract (A, C)

[Index](../README.md) › Review · Next: [Audit of the current API](audit.md)

**Non-normative.** Part of the [review](../README.md#ltseq-v05-api-review) behind the contract. It records evidence, reasoning and decisions; the behavior itself is specified by the contract modules.

**Scope.** The assessment of the current API, the design principles behind v0.5, the direction for v0.5 and its positioning ([Deliverable A]); then the pointer to the contract and the three cross-cutting rules that govern how it is read ([Deliverable C]).

**Most cited from here.** [§9] Ordering contract · [§21] Performance contract · [§20] Errors · [§22] Complete canonical API reference

<!-- /v0.5-modular:header -->

## A. Executive verdict

### A.1 The current API

LTSeq's premise is right and has no direct peer. It offers ordered tables with first-class sequence operations: table-order windows, consecutive-run grouping, pattern search, as-of joins and sequential folds.

- None of pandas, Polars or Ibis has a consecutive-run grouping API. Polars builds one from [`Expr.rle_id`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.rle_id.html).
- No Python dataframe library has row-pattern search.

The Rust kernel keeps most transforms on a lazy DataFusion plan and releases the GIL while it executes (ADR 0016). ADR 0018 gives LTSeq a stricter literal policy than any peer reviewed ([Deliverable D], #248).

The current API does not keep the promise its name makes. The audit (below) found five debts, in order of harm:

1. **Order is not reliable.**
   - DataFusion repartitions memory and file sources to one partition per core (`src/engine.rs:43`). Terminals then merge partitions in completion order. On multi-batch or multi-file input, `head`, `tail`, `slice`, writers and `fold` therefore see a different row order from run to run (audit D1).
   - Windows reorder rows while `sort_keys` still reports the earlier order (#202).
   - `assume_sorted` on non-Parquet input declares an order the emitted rows do not have.
   - Most multi-input operations drop order without saying so.

   Each of these returns plausible rows with no error.
2. **Numbers can be silently wrong.**
   - Integer arithmetic and `SUM` wrap (#221).
   - Integer `/` truncates and `%` takes the dividend's sign (#218).
   - `decimal32`/`decimal64` values are rounded to integers next to an integer (#241).
   - `concat` promotes `int64` to `float64` when the other input is empty (#222).
   - All-literal branches round `2**53 + 1` (#248).
   - `asof_join` reads a NULL time as the smallest time, and `pivot` counts `"01"` and `"1"` as the same column.
3. **Semantics depend on the execution path.** The DataFusion path, the `search_pattern` evaluator and the linear-scan fast path disagree on overflow, mixed-type comparison, zero divisors and NULL (audit I3). The linear-scan path's errors are swallowed by a bare `except Exception` (`py-ltseq/ltseq/grouping/nested_table.py:390`), which falls back to another path.
4. **The error model does not hold.**
   - `except LTSeqError` catches almost nothing, because most kernel errors surface as plain `ValueError` or `RuntimeError` (`src/error.rs:88-90`).
   - An unknown column raises five different exception types depending on the method.
   - Several arguments are silently ignored: `shift(offset=k)` uses lag 1, and a missing path reads as an empty table.
5. **The surface is wider than the concepts.**
   - There are five table types: `LTSeq`, `NestedTable`, `LinkedTable`, `PartitionedTable` (with a sixth, `SQLPartitionedTable`, returned for string keys at `py-ltseq/ltseq/core.py:703`) and `Cursor`. Besides `LTSeq`, only `NestedTable` has a role no other type covers.
   - The function namespace has `nvl`, `ifa`, `char` and `str_char` alongside `coalesce` and `if_else`.
   - Table methods have aliases: `union` and `concat`, `subtract` and `except_`, `group_consecutive`, `with_columns`.
   - The same concept takes different keywords: `how=` and `join_type=`, `other` and `target_table`, `as_=` and `alias=`.
   - `Expr.__getattr__` accepts any name, so a typo fails late.

### A.2 Design principles

The contract states these ten principles normatively in [§1.3]. Earlier principles win when two conflict. Each one answers a finding above.

1. **No silent wrong results.** A result is correct under the contract, or the call raises. This answers debts 1 and 2. It is why v0.5 checks arithmetic, compares mixed types exactly and refuses inexact literals.
2. **One meaning per spelling.** A name, keyword or operator means the same thing on every execution path. Fast paths are optimizations and never semantics ([§21.2]). This answers debt 3.
3. **Order is explicit.** A table knows whether its order is defined and which keys declare it. An operation that needs order says which kind it needs and refuses at plan time when it is missing ([§9]). This answers debt 1.
4. **Errors are typed and staged.** Every documented failure has one class and one stage: Plan, Call or Execution ([§20]). This answers debt 4.
5. **Python spelling, Arrow types.** Operators mean what they mean in Python, names follow the dominant Python libraries, and every result type is an Arrow type the contract states.
6. **Minimal and orthogonal.** There is one way to do each thing. A method that is a composition of others is removed. This answers debt 5.
7. **Lazy until a terminal.** Plan-building calls read no data beyond schema discovery. The eager calls are listed ([§5.3]).
8. **Composable.** Every table-returning method returns an `LTSeq`, or a builder (`NestedTable`, `GroupBy`) whose methods return one.
9. **Deterministic.** The same plan over the same data gives the same rows in the same order. The contract names six exceptions: the row sequence of an unordered table (and so which rows `head()` and `show()` return from it); the order of unspecified ties, and every value computed from a row's position among them; which row `distinct(keep="any")` keeps, and which of several equal rows `intersect` and `difference` keep over an undefined left order; which error is raised when several demanded values fail; and the clock functions `now()` and `today()`.
10. **Streaming is the default terminal path.** Terminals produce batches incrementally. Whole-table materialization happens only where [§21.1] lists it.

### A.3 Direction for v0.5

Five changes carry most of the design.

- **Order becomes a state with two tiers of requirement.**
  - Every table has an order state: defined or undefined, plus optional `sort_keys`. Every operation's effect on that state is tabulated ([§9.2]).
  - *Positional* operations (`tail`, `slice`, `reverse`, …) need only a defined order. *Sequence* operations (windows, `group_ordered`, `search_pattern`, `fold`) need declared keys. Both are checked at plan time ([§9.3]).
  - Readers give a defined file-then-row order (#148).
  - Windows never move rows (#202).
- **Every value is exact or the call raises.**
  - Integer and decimal arithmetic is checked everywhere, aggregates included (#221).
  - Mixed numeric comparisons compare exact values (#228).
  - Literals must be exact in their context (#247, #248).
  - Operators follow Python (#218).
- **One semantics for every evaluator.** Specialized evaluators must match the general path in values, types, order and `sort_keys`, and raise only errors the general path may raise; the test matrix runs every case with each one forced on and off ([§21.2], [§24.1]).
- **A smaller surface.**
  - Three table types: `LTSeq`, `NestedTable` and `GroupBy`.
  - `link` and `lookup` become `join(..., alias=)`, and `partition` returns a `dict`.
  - `Cursor`, `scan` and `scan_parquet` give way to `to_batches()`, which returns a `pyarrow.RecordBatchReader`.
  - One keyword per concept and no aliases ([Deliverable B]).
- **Python-first naming.**
  - `.str` instead of `.s`, with Python's `str` method names (`startswith`, `rjust`); `descending=` instead of `desc=`, `reverse` instead of `rvs`, `difference` instead of `except_`.
  - A closed `Expr` method set, so a typo raises `AttributeError` when the lambda runs ([§8.7]).

### A.4 Positioning

LTSeq v0.5 is a sequence engine with a dataframe surface. It is not a general dataframe library that also does windows. It competes on guarantees, not on breadth.

**Order.**

- pandas order is positional and always defined.
- Polars 2.0 made streaming the default engine for lazy queries. Its [upgrade guide](https://docs.pola.rs/releases/upgrade/2/) says "The streaming engine does not guarantee row order for operations that don't require it (`unpivot`, `group_by`, joins, ...)".
- DuckDB documents a fixed list of [order-preserving operators](https://duckdb.org/docs/current/sql/dialect/order_preservation.html). Joins, `GROUP BY` and `UNION` are not on it.
- Ibis states that "SQL (and therefore Ibis) makes no guarantees about row order" ([Ibis for pandas users](https://ibis-project.org/tutorials/ibis-for-pandas-users)).

LTSeq takes DuckDB's model: an explicit list of what preserves order ([§9.2]). It adds an order state that is part of every table's plan-time description (`is_ordered`, `sort_keys`) and is checked before any data is read. Unlike pandas, a table may be unordered, as after `group_by`. Unlike the lazy engines, an operation that needs order refuses an unordered table instead of returning arbitrary rows.

**Numbers.**

- Vectorized engines wrap on integer overflow by default. Arrow's compute documentation says "The default variant of these functions does not detect overflow (the result then typically wraps around)" ([Arrow compute](https://arrow.apache.org/docs/cpp/compute.html)). NumPy documents the same for fixed-size integers ([NumPy data types](https://numpy.org/doc/stable/user/basics.types.html)). pandas' NumPy-backed `int64` wraps on add and multiply (checked with pandas 3.0.5).
- DuckDB raises instead: "Attempts to store values outside of the allowed range will result in an error" ([DuckDB numeric types](https://duckdb.org/docs/current/sql/data_types/numeric.html)).

LTSeq sides with DuckDB, and goes further on comparisons and literals, which are exact at every type pair.

**Expressions.**

- Polars, pandas 3 and Ibis use deferred column objects.
- LTSeq keeps row lambdas over a proxy (`lambda r: r.price > 10`). The proxy gives attribute completion over the schema, and a lambda reads the same as plain Python. v0.5 makes it safe for static tools: `Row` and `Group` are protocols in `ltseq.typing`, the `Expr` method set is closed, and a lambda is called exactly once, at plan time ([§3.4]).

**Interchange.**

- Like pyarrow, Polars, pandas 3 and DuckDB, LTSeq exports through the Arrow PyCapsule stream and returns `pyarrow.RecordBatchReader` for streaming.
- Pickle is a same-version Arrow IPC snapshot, never a plan, because plans are not stable across versions. Polars says the same of its own plans: "Serialization is not stable across Polars versions" ([`LazyFrame.serialize`](https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.serialize.html)).

**Where it departs from its engine or a peer.**

- Integer `/` gives `float64` as in Python and DuckDB, where DataFusion truncates. `//` floors and `%` takes the divisor's sign, as in Python, where DataFusion and DuckDB truncate ([DuckDB numeric functions](https://duckdb.org/docs/current/sql/functions/numeric.html); DuckDB's truncation is read from its source, which that page does not state).
- `NaN == NaN` holds in comparisons, grouping and joins, as in Polars and DuckDB, rather than IEEE: "NaN compares equal to NaN and greater than any other floating point number" ([DuckDB numeric types](https://duckdb.org/docs/current/sql/data_types/numeric.html)). Polars documents the same total order ([data types](https://docs.pola.rs/user-guide/concepts/data-types-and-structures/)).
- Aware timestamps subtract as instants even within one zone, which follows pandas rather than Python ([Deliverable D], #246).
- `concat` has no relaxed mode, where Polars has `vertical_relaxed`.
- NULLs sort last in both directions, as in pandas and DuckDB, where DataFusion puts them first when descending.

Each of these is chosen for correctness or for predictability. None is chosen to look like a particular library.

## C. The contract

The contract is [`docs/proposals/v0.5/contract/`](../README.md#contract-modules). It is normative and uses the BCP 14 key words. Its 24 sections follow the brief's outline one for one. [§15] is titled "Streaming" rather than "Streaming and Cursor" because v0.5 has no `Cursor` ([Deliverable D], #148).

Each public method's signature is in [§22], and its prose lives in the section that owns the concept: behavior, effect on order and schema, NULL and type handling, and the exception raised at each stage. Every method in the inventory below with a v0.5 name appears in [§22]. Every name the inventory removes is required by [§24.1] to raise `AttributeError` or `ImportError`.

Three cross-cutting rules govern how the rest of the contract is read:

- **Order state ([§9]).** Every table has a defined or undefined order, plus optional `sort_keys`. The order requirements are checked at plan time, so missing order raises before any data is read.
- **Stages ([§20.2]).** Every documented failure names its class and its stage: Plan, Call or Execution. A failure at a different stage is a contract violation.
- **Path independence ([§21.2]).** A specialized evaluator is an optimization. It matches the general path in values, types, order and `sort_keys` and raises only errors the general path may raise ([§20.2]), or it declines before executing.

<!-- v0.5-modular:footer -->

---

[Index](../README.md) · Next: [Audit of the current API](audit.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§1.3]: ../contract/overview.md#13-design-principles
[§3.4]: ../contract/public-surface.md#34-lambdas-and-proxies
[§5.3]: ../contract/loading-and-laziness.md#53-eager-calls
[§8.7]: ../contract/expressions.md#87-the-closed-method-set
[§9]: ../contract/ordering.md#9-ordering-contract
[§9.2]: ../contract/ordering.md#92-sources-and-propagation
[§9.3]: ../contract/ordering.md#93-order-requirements
[§15]: ../contract/streaming-and-output.md#15-streaming
[§20]: ../contract/errors-and-performance.md#20-errors
[§20.2]: ../contract/errors-and-performance.md#202-stages
[§21]: ../contract/errors-and-performance.md#21-performance-contract
[§21.1]: ../contract/errors-and-performance.md#211-materialization
[§21.2]: ../contract/errors-and-performance.md#212-fast-paths
[§22]: ../contract/api-reference.md#22-complete-canonical-api-reference
[§24.1]: ../contract/acceptance.md#241-acceptance-criteria
[Deliverable A]: #a-executive-verdict
[Deliverable B]: inventory.md#b-api-inventory-and-review
[Deliverable C]: #c-the-contract
[Deliverable D]: decisions.md#d-decisions-on-the-eleven-open-semantic-issues

<!-- /v0.5-modular:links -->
