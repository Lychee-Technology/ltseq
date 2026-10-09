<!-- v0.5-modular:header -->

# v0.5 contract: Errors and performance (§20–§21)

[Index](../README.md) › Contract · Previous: [Numeric, NULL and temporal semantics (§17–§19)](numeric-null-temporal.md) · Next: [Canonical API reference (§22)](api-reference.md)

**Normative.** One module of the LTSeq v0.5 public API contract. It uses the BCP 14 key words as the contract's [status block](../README.md#ltseq-v05-public-api-contract) declares, and [§1.6] names the section that owns each cross-cutting rule.

**Scope.** The exception classes and the stage at which each documented failure is raised, then the performance contract: what must stream, what may hold its input, and fast paths.

**Most cited from here.** [§16] Output and interchange · [§10] Windows and ordered computation · [§5] Lazy evaluation and materialization · [§3] Object model

<!-- /v0.5-modular:header -->

## 20. Errors

### 20.1 Exception classes

Every exception LTSeq raises for a documented failure is an instance of one of these classes. Each also derives from the built-in exception a Python programmer would expect, so `except ValueError` keeps working.

| Class | Bases | Raised for |
|---|---|---|
| `LTSeqError` | `Exception` | Base of all LTSeq errors; never raised directly |
| `LTSeqTypeError` | `LTSeqError`, `TypeError` | Operand or argument types that no rule accepts |
| `LTSeqValueError` | `LTSeqError`, `ValueError` | Invalid argument values, inexact literals |
| `ColumnNotFoundError` | `LTSeqValueError`, `AttributeError` | A column name that does not exist |
| `SchemaMismatchError` | `LTSeqValueError` | Schemas that MUST match and do not |
| `SortRequiredError` | `LTSeqValueError` | An order requirement ([§9.3]) not met |
| `OrderViolationError` | `LTSeqValueError` | Data that violates `assume_sorted` |
| `DuplicateKeyError` | `LTSeqValueError` | A `validate=` violation |
| `CastError` | `LTSeqValueError` | A value that fails to convert: a supplied value that an implicit conversion cannot hold exactly ([§17.6]), a value the [§17.6] table says fails in an explicit cast, or a value that a conversion defined elsewhere cannot hold (`date64` to `date32` in [§6.2], a writer's in [§16.5]) |
| `LTSeqIndexError` | `LTSeqError`, `IndexError` | A row position outside the table |
| `ArithmeticOverflowError` | `LTSeqError`, `OverflowError` | Integer, decimal or temporal overflow, including an operand's change of unit inside temporal arithmetic ([§19.2]) |
| `DivisionByZeroError` | `LTSeqError`, `ZeroDivisionError` | Integer, decimal or duration division by zero |
| `LTSeqIOError` | `LTSeqError`, `OSError` | I/O failures |
| `SourceNotFoundError` | `LTSeqIOError`, `FileNotFoundError` | A source path that resolves to nothing |
| `ExecutionError` | `LTSeqError`, `RuntimeError` | An engine failure not covered above (an internal error; its message says so) |

`TypeError` (not `LTSeqTypeError`) is raised only by the Python protocol refusals of [§3] and [§3.4], and `AttributeError` by unknown `Expr` methods ([§8.7]). An error that leaves LTSeq through the Arrow C stream interface keeps its class only as the start of its message, and the consumer raises its own exception ([§15.1], [§16.4]).

Messages name the method, the column or literal involved and, where one exists, the fix. Message wording is not part of this contract; classes and stages are.

### 20.2 Stages

Each documented error belongs to exactly one stage:

| Stage | When | Examples |
|---|---|---|
| Plan | When a plan-building method or expression function is called | Unknown column, incompatible types, inexact literal, missing order, schema mismatch, invalid argument |
| Call | When a reader or eager method opens its source or discovers values, or a writer or `to_pandas` checks the input's schema | Missing file, CSV inference, Parquet footers, `partition` NaN key or key that `to_dicts` cannot convert, `pivot` column value that `to_dicts` cannot convert, a column type a writer or `to_pandas` cannot hold, a table without columns given to a writer, a declared `from_dict` or `from_rows` type, or a requested type, of another kind than its values ([§4.6], [§16.4]) |
| Execution | While a terminal (or an eager call) computes values | Overflow, division by zero, failed `cast`, CSV parse error, `assume_sorted` violation, `validate=` violation, out-of-range position, a value that `to_dicts`, iteration, `fold` or `to_pandas` cannot convert to Python ([§16.2], [§16.3]), a `fold` state that its `dtype` cannot take ([§10.7]) |

**Demand.** An execution error is raised if and only if a value that the terminal demands fails. A terminal demands every value of every output column of every output row, and every value that decides which rows exist and in what order (filter predicates, join keys, sort keys, grouping keys, window inputs those depend on). `count()` demands only the latter. `collect()` and `pickle.dumps` demand what a terminal does, every value of their input, including values a later operation on the snapshot would drop ([§5.2]). A row is *read* when any of its values is demanded. `count()` reads the rows its input's plan reads: every row, unless an operation that keeps a prefix (`head(n)`, `slice`, `search_first`) stops reading earlier. Where the output follows input order, no row after the last one the output needs is read: `head(n)` over `filter(p)` demands `p` up to the *n*-th passing row and no further, while a sort, grouping or aggregate demands its inputs on every row, and so does a join on its right side (on both sides for `right` and `full`). `validate=` ([§12.1]) demands its keys on every row. The discovery of `partition` and of `pivot` without `column_values` ([§5.3]) demands what `count()` over its input demands and its key columns (or `columns`) on every row it reads. A value is not demanded on these rows:

- either operand of `a & b` on rows where the other operand is FALSE, and either operand of `a | b` on rows where the other is TRUE. The rule is symmetric, so which operand is written first is not observable: `(r.d != 0) & (r.n // r.d > 1)` and `(r.n // r.d > 1) & (r.d != 0)` both give FALSE for `d == 0`. An operand that fails on a row is not FALSE (or TRUE) there, so where both operands of one `&` fail, both are demanded and one of their errors is raised;
- `when`/`if_else` evaluate a branch only for rows that take it;
- `coalesce` evaluates an argument only for rows where every earlier one is NULL;
- `search_pattern` evaluates step *k* only for starts where every earlier step is TRUE;
- an aggregate evaluates its arguments only on rows where its `where=` is TRUE ([§14.2]), so `(g.a // g.b).sum(where=g.b != 0)` never divides by zero. The `where=` expression itself is demanded on every row of the group;
- an operation evaluates its expressions only on the rows of its input, so a row an earlier `filter`, `head` or join removed is never evaluated after it: `t.filter(lambda r: r.x != 0).filter(lambda r: 10 // r.x > 1)` never divides by zero. A later operation never guards an earlier one, so unlike the operands of `&`, the order of chained filters is observable: `t.filter(lambda r: 10 // r.x > 1).filter(lambda r: r.x != 0)` raises where `x` is 0.

This is a property of rows, not of batches: whether a guarded row's error is raised MUST NOT depend on what other rows share its batch.

**Several failures.** When more than one demanded value fails, the execution raises the error of one of them, and which one is not specified (principle 9): it may depend on partitioning, batch boundaries and the order in which columns or operands are evaluated. Whatever is raised is an error that one of the failing demanded values raises on its own, at its stage, and no result is returned. Tests compare against that set of errors ([§24.1]). Fixing the choice (the earliest failing row, then the leftmost column) would serialize error reporting across partitions for no gain in correctness.

**Evaluation strategy.** These rules fix what is observed, not how it is computed. An execution MAY evaluate any expression on any row speculatively, in any order, batch or partition, including values the rules above do not demand, provided that every error from a value that is not demanded is suppressed and the results and errors are those the rules require. Evaluating under a selection mask, or carrying errors as values and raising only at demanded positions, are two ways to do this. So a rewrite may merge filters, reorder conjuncts, push a predicate into a scan or below a join, evaluate a projection before a filter, hoist a subexpression out of a `when` branch, or run a limit over parallel partitions, but where the moved expression can fail and its errors cannot be suppressed, the rewrite MUST NOT be applied.

**Parity.** For the same plan and input, every execution path ([§21.2]) returns the same result, or raises an error from the same set (one failing demanded value's class and stage, above). An error MUST NOT appear, disappear or leave that set because a fast path was or was not taken.

---

## 21. Performance contract

### 21.1 Materialization

- Plan-building calls do work proportional to the plan, not to the data.
- These pipelines MUST stream in memory bounded independently of the number of rows (for fixed batch size, fixed row width and a fixed number of groups, partitions or distinct keys): readers, `filter`, `select`, `derive` and `search_first`, each only when its expressions have no window method, ranking function or aggregate over a window ([§10.1]–[§10.3]), `rename`, `drop`, `head`, `slice`, `step`, `concat`, `assume_sorted`, `semi_join`/`anti_join`/`join` with the right side fitting in memory, `with_row_index`, `update` and `delete` with a predicate, `group_by().agg` and `LTSeq.agg` without `quantile`, `median`, `mode`, `n_unique` or `string_agg`, and every terminal over them. The bound is on the execution, not on a result the caller keeps. `to_arrow`, `to_pandas` and `to_dicts` return every output row, and `collect` and `pickle.dumps` store it, as does a consumer that builds a table from the stream of [§16.4] (`pa.table(t)`, `pl.from_dataframe(t)`): the result, and the Arrow data `to_pandas` and `to_dicts` convert it from, grow with the output and are outside the bound. Reading `to_batches` or iterating while dropping what was read, the writers, `count` and `show` hold no more than the bound, and so does LTSeq's side of the stream of [§16.4]; what a stream consumer keeps is its own.
- The operations below SHOULD hold only what their definition needs. Rows leave in input order, so while a row waits for a later row of its partition, the output rows after it wait too, and with `partition_by` these include other partitions' rows.
  - Window methods in table order ([§10.1]): in each partition, `|n|` rows for `shift`, `diff` and `pct_change`, `window` rows for `rolling` and a running value for `cum_*`; for a negative `n`, also the rows between a row and the row it reads, or, where there is none, the end of the input.
  - `row_number`, `rank` and `dense_rank` in table order ([§10.2]): in each partition, a count and the last row's `sort_keys` values and rank.
  - `group_ordered` and its `NestedTable`, with the aggregates and windows that run within a group: one group, and what the window methods in `starts_when` need.
  - `search_pattern`: in each partition, the rows one match spans, one per step, and what the steps' window methods need; with `partition_by`, also the matches found while an earlier start waits for its last step.
  - `asof_join` whose inputs' `sort_keys` both begin with the as-of key, ascending, or both begin with the `by` keys, in the order `by` (or `left_by` and `right_by`) lists them and in the same directions on both sides, and then the as-of key, ascending: for `backward`, one right row per distinct `by` value; for `forward` and `nearest`, also the right rows read ahead to find a left row's match, which, when the as-of key comes first, include rows of other `by` values.
  - `distinct`: each distinct key, and for `keep="last"` the latest row with it.
- `sort`, `tail`, `reverse`, `pivot`, `intersect`, `difference`, `quantile`, `median`, `mode`, `n_unique` and `string_agg` MAY hold the whole input, as MAY window methods and ranking functions with `.over(order_by=...)`, `ntile`, and aggregates over windows ([§10.3]), `insert`, and `update` and `delete` at a position ([§7.10]). `join`, `semi_join`, `anti_join` and any other `asof_join` MAY hold their right input.
- The work an eager call does at the call ([§5.3]) MAY hold its whole input; `fold` holds it. A reader's call does only the work [§5.3] lists, and the tables eager calls return are bound by the bullets above like any other.
- Restoring the defined order after parallel execution MUST NOT materialize: memory stays as bounded as the pipeline's own (#148). How the order is restored (a sort-preserving merge, an order-preserving repartition, or another mechanism) is the implementation's choice.

### 21.2 Fast paths

An implementation MAY run any operation through specialized evaluators (linear scans, pre-sorted Parquet readers, hand-written pattern matchers). Every such path MUST produce the same values, types, row order and `sort_keys` as the general path for every input, and raise only errors the general path may raise ([§20.2]). A fast path that cannot guarantee this for an input MUST NOT be taken for it. Acceptance tests ([§24]) run the parity cases on every path.

<!-- v0.5-modular:footer -->

---

Previous: [Numeric, NULL and temporal semantics (§17–§19)](numeric-null-temporal.md) · [Index](../README.md) · Next: [Canonical API reference (§22)](api-reference.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§1.6]: overview.md#16-rule-ownership
[§3]: public-surface.md#3-object-model
[§3.4]: public-surface.md#34-lambdas-and-proxies
[§4.6]: loading-and-laziness.md#46-ltseqfrom_dict-and-ltseqfrom_rows
[§5]: loading-and-laziness.md#5-lazy-evaluation-and-materialization
[§5.2]: loading-and-laziness.md#52-ltseqcollect
[§5.3]: loading-and-laziness.md#53-eager-calls
[§6.2]: schema-and-table-operations.md#62-supported-types
[§7.10]: schema-and-table-operations.md#710-value-level-edits-insert-delete-update
[§8.7]: expressions.md#87-the-closed-method-set
[§9.3]: ordering.md#93-order-requirements
[§10]: windows-and-grouping.md#10-windows-and-ordered-computation
[§10.1]: windows-and-grouping.md#101-window-methods
[§10.2]: windows-and-grouping.md#102-ranking-functions
[§10.3]: windows-and-grouping.md#103-aggregates-over-windows
[§10.7]: windows-and-grouping.md#107-fold
[§12.1]: joins-and-sets.md#121-join
[§14.2]: aggregation.md#142-aggregate-expressions
[§15.1]: streaming-and-output.md#151-to_batches
[§16]: streaming-and-output.md#16-output-and-interchange
[§16.2]: streaming-and-output.md#162-to_pandas
[§16.3]: streaming-and-output.md#163-to_dicts
[§16.4]: streaming-and-output.md#164-arrow-pycapsule-stream
[§16.5]: streaming-and-output.md#165-writers
[§17.6]: numeric-null-temporal.md#176-explicit-casts
[§19.2]: numeric-null-temporal.md#192-arithmetic-and-comparison
[§20.2]: #202-stages
[§21.2]: #212-fast-paths
[§24]: acceptance.md#24-acceptance-criteria-and-contract-test-matrix
[§24.1]: acceptance.md#241-acceptance-criteria

<!-- /v0.5-modular:links -->
