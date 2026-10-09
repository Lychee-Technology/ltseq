<!-- v0.5-modular:header -->

# v0.5 contract: Windows and ordered grouping (§10–§11)

[Index](../README.md) › Contract · Previous: [Ordering contract (§9)](ordering.md) · Next: [Joins and set operations (§12–§13)](joins-and-sets.md)

**Normative.** One module of the LTSeq v0.5 public API contract. It uses the BCP 14 key words as the contract's [status block](../README.md#ltseq-v05-public-api-contract) declares, and [§1.6] names the section that owns each cross-cutting rule.

**Scope.** Window methods, ranking functions and aggregates over windows, `over`, ordered search (`search_first`, `search_pattern`), sequential state with `fold`, and consecutive grouping with `group_ordered` and `NestedTable`.

**Most cited from here.** [§14] Aggregation, partitioning and pivot · [§17] Numeric semantics and literals · [§4] Loading · [§5] Lazy evaluation and materialization

<!-- /v0.5-modular:header -->

## 10. Windows and ordered computation

### 10.1 Window methods

Window methods are `Expr` methods used in `Row` contexts (`select`, `derive`, `filter`, `update`, `search_*`, `starts_when`) and in `NestedTable.derive`.

```python
def shift(self, n: int = 1, *, fill_value: Literal = None) -> Expr
def diff(self, n: int = 1) -> Expr
def pct_change(self, n: int = 1) -> Expr
def cum_sum(self) -> Expr
def cum_min(self) -> Expr
def cum_max(self) -> Expr
def rolling(self, window: int, *, min_periods: int | None = None) -> Rolling
```

**The window order.** A window method reads other rows in *window order*:

- with `.over(order_by=...)` ([§10.4]), the `order_by` columns, ties broken by table order (unspecified ties if the table's order is undefined);
- otherwise table order, which requires the sequence tier (`sort_keys` not `None`, else `SortRequiredError`);
- within `.over(partition_by=...)`, within a group in `NestedTable.derive`, or within the steps of `search_pattern(partition_by=...)` ([§10.6]), only rows of the same partition or group take part.

Window methods never reorder rows: the result has the input's rows in the input's order (#202).

A window method takes any `Row` expression as input, window expressions included (`r.x.diff().shift(1)`, `(r.a * r.b).cum_sum()`), and its result is an `Expr` that composes with every operator and function of [§8] (`sqrt(r.x.diff())`, `r.s.shift(1).str.len()`). The one exception is an aggregate, which MUST NOT contain a window method ([§14.2]).

| Method | Value at row *i* | Type |
|---|---|---|
| `shift(n, fill_value=None)` | The value `n` rows earlier in window order (later when `n < 0`; the value itself when `n == 0`). `fill_value` where no such row exists. | Input type |
| `diff(n)` | `x - x.shift(n)`, computed in the result type | Integers: `int64`, or `decimal128(20, 0)` for `uint64` ([§17.3]); otherwise the type of that subtraction ([§17], [§19]) |
| `pct_change(n)` | `x / x.shift(n) - 1` in `float64`. For every numeric input type, decimals and `float32` included, the quotient is the exact one rounded once, as integer `/` rounds it ([§17.3]), and a zero, NaN or infinite operand gives what IEEE division gives, integer input included: unlike integer `/`, a zero base never raises (`0` to a positive value is `inf`) | `float64` |
| `cum_sum()` | Sum of the non-NULL values from the partition start to row *i*; NULL where `x` is NULL | As `sum` ([§14.2]), checked |
| `cum_min()`, `cum_max()` | Minimum or maximum of the non-NULL values so far, under the order of [§9.4] (so `cum_max` is NaN from the first NaN on); NULL where `x` is NULL | Input type |

- `n` MUST be an `int` literal; anything else raises `LTSeqTypeError`.
- `fill_value` MUST be exact in the input type ([§17.5]); it never widens the result type (ADR 0018 D-c).
- `diff` and `pct_change` take numeric input (`diff` also temporal); `cum_*` take numeric input (`cum_min`/`cum_max` also temporal and string). Other types raise `LTSeqTypeError`.

**`rolling(window, min_periods=None)`** returns a `Rolling` builder whose methods produce the expression: `sum()`, `mean()`, `min()`, `max()`, `count()`, `std()`, `var()`. The frame of row *i* is row *i* and the `window - 1` rows before it in window order, within the partition. NULL values in the frame are skipped. A frame with fewer than `min_periods` non-NULL values (default `window`) gives NULL, except that `count()` always returns the number of non-NULL values. Values, types and NaN handling are those of the same-named aggregate over the frame ([§14.2]), so a rolling `mean`, `std` or `var` is the exact value rounded once. `window` MUST be at least 1 and `min_periods` between 1 and `window` (`LTSeqValueError`).

```python
t.sort("ts").derive(ma20=lambda r: r.close.rolling(20).mean(),
                    prev=lambda r: r.close.shift(1, fill_value=0.0))
```

The table method `cum_sum(*columns)` and the `partition_by=` keyword on the window methods that took it (`shift`, `diff`, `rolling`, `cum_sum`, `cum_min`, `cum_max`) are removed; use the expression methods and `.over(partition_by=...)`.

### 10.2 Ranking functions

```python
def row_number() -> Expr
def rank() -> Expr
def dense_rank() -> Expr
def ntile(n: int, /) -> Expr
```

- `row_number()` is the 1-based position in window order. `rank()` gives tied rows the same rank and skips (1, 1, 3); `dense_rank()` does not skip (1, 1, 2). `ntile(n)` splits each partition into `n` buckets numbered from 1 whose sizes differ by at most one, larger buckets first.
- Ties are rows equal on every `order_by` column, or, without `order_by`, on every `sort_keys` column.
- All return `int64`. `ntile(n)` requires `n >= 1` (`LTSeqValueError`).
- Without `.over(order_by=...)` they use table order: sequence tier.

```python
t.derive(rk=lambda r: rank().over(partition_by="dept", order_by="salary", descending=True))
```

### 10.3 Aggregates over windows

An aggregate method ([§14.2]) followed by `.over(partition_by=...)` computes the aggregate over the whole partition (the whole table when `partition_by` is `None`) and repeats it on every row of the partition. Row order is unchanged.

```python
t.derive(share=lambda r: r.qty / r.qty.sum().over(partition_by="day"))
```

`order_by` is not accepted on an aggregate (`LTSeqValueError`); running totals are `cum_*` and moving windows are `rolling`. The `first`, `last` and `string_agg` aggregates use table order: positional tier.

### 10.4 `over`

```python
def over(self, partition_by: str | Sequence[str] | None = None,
         order_by: str | Sequence[str] | None = None, *,
         descending: bool | Sequence[bool] = False,
         nulls_last: bool | Sequence[bool] = True) -> Expr
```

- Applies to window methods, `Rolling` results, ranking functions and aggregates. On any other expression it raises `LTSeqTypeError`. Calling it twice on one expression raises `LTSeqValueError`.
- `partition_by` and `order_by` are column names. Partitions use the equality of [§18](numeric-null-temporal.md#18-null-nan-and-boolean-logic): NULL is one partition, NaN is one partition, and `-0.0` and `0.0` are one partition. `descending` and `nulls_last` apply to `order_by` as in `sort`.
- `.over()` with no arguments is the same as no `.over()` for window methods and ranking functions, and is required for an aggregate in a `Row` context.

### 10.5 `search_first`

```python
def search_first(self, predicate: Expr | ExprFn, /) -> LTSeq
```

- **Behavior.** The first row in table order where `predicate` is TRUE, as a table of 0 or 1 rows. Plan-building. The predicate is demanded up to the first match and no further ([§20.2]), so a value that fails after the match does not raise.
- **Order.** Positional tier. Keeps `sort_keys`. A predicate with window methods also needs the sequence tier.
- **Example.** `t.sort("ts").search_first(lambda r: r.price > 100).to_dicts()`

`search_first` always returns an `LTSeq`, never `None`.

### 10.6 `search_pattern`

```python
def search_pattern(self, *steps: Expr | ExprFn, partition_by: str | Sequence[str] | None = None) -> LTSeq
```

- **Behavior.** Returns every row *i* such that, for each step *k* (0-based), `steps[k]` is TRUE when evaluated on row *i + k*, where rows are counted in table order within the row's partition. Matches may overlap: rows *i* and *i + 1* can both be returned.
- **Steps.** Each step is a `Row` predicate evaluated on its own row; window methods inside a step are relative to that row and, with `partition_by`, read only rows of its partition ([§10.1]), so `shift(1)` on a partition's first row is NULL. Steps are demanded left to right: step *k* is evaluated for start *i* only if steps 0 … *k − 1* were TRUE ([§20.2]).
- **Result.** The matched start rows with all columns, in input order. Keeps `sort_keys`. `t.search_pattern(...).count()` counts matches; `search_pattern_count` is removed.
- **Order.** Sequence tier.
- **Errors.** No steps raise `LTSeqValueError`.
- **Example.** Three rising closes in a row per symbol:

```python
t.sort("symbol", "ts").search_pattern(
    lambda r: r.close > r.close.shift(1),
    lambda r: r.close > r.close.shift(1),
    lambda r: r.close > r.close.shift(1),
    partition_by="symbol")
```

### 10.7 `fold`

```python
def fold(self, fn: Callable[[S, dict[str, Any]], S], /, *, init: S, into: str,
         partition_by: str | Sequence[str] | None = None,
         dtype: DTypeLike | None = None) -> LTSeq
```

- **Behavior.** Threads a Python state through the rows in table order, restarting at `init` in each partition: `state = fn(state, row)` for each row, where `row` is the row as `to_dicts()` would give it. The new column `into` holds the state after each row.
- **Types.** With `dtype`, each state converts as a value of a declared `from_dict` type does ([§4.6]): exactly, else `CastError`, and a state of another kind raises `LTSeqTypeError`, both during execution of the `fold` call ([§5.3]). Without, the column type is inferred from all states as `from_dict` infers it ([§4.6]). Each state converts when `fn` returns it, so mutating a returned object later does not change earlier outputs.
- **Order.** Sequence tier. Keeps order and `sort_keys`.
- **Execution.** Eager: the whole table is executed and `fn` runs on every row when `fold` is called ([§5.3]). An exception raised by `fn` propagates unchanged. This is the one operation whose computation runs in Python, and it is the slow path by design: anything expressible with window methods or `group_ordered` SHOULD use them instead.
- **Errors.** An `into` that is already a column raises `LTSeqValueError`. A value of the input that [§16.3] cannot convert raises its `CastError` when `fold` is called, as `to_dicts` would, since `fn` sees every row.
- **Example.** A drawdown tracker:

```python
t.sort("ts").fold(lambda peak, row: max(peak, row["equity"]), init=float("-inf"), into="peak")
```

`stateful_scan` is removed.

---

## 11. Ordered grouping

### 11.1 `group_ordered`

```python
def group_ordered(self, *keys: str, starts_when: Expr | ExprFn | None = None) -> NestedTable
```

- **Behavior.** Splits the table, in table order, into consecutive groups. A new group starts at the first row and at every row where any key differs from the previous row's, or where `starts_when` is TRUE. A NULL `starts_when` counts as FALSE. Key comparison uses the equality of [§18](numeric-null-temporal.md#18-null-nan-and-boolean-logic): NULL equals NULL, NaN equals NaN.
- **`starts_when`** is a `Row` predicate and usually compares with the previous row: `starts_when=lambda r: r.ts - r.ts.shift(1) > timedelta(minutes=30)`.
- **Order.** Sequence tier.
- **Errors.** No `keys` and no `starts_when` raise `LTSeqValueError`. An unknown key raises `ColumnNotFoundError`; a key of a pass-through type raises `LTSeqTypeError`.

`group_sorted` and `group_consecutive` are removed: on sorted data every run is a whole group, so `group_ordered` covers both. The v0.4 form `group_ordered(lambda r: ...)`, which took a key expression, is replaced by `derive` + `keys`, or by `starts_when` when the lambda was a boundary condition.

### 11.2 `NestedTable`

```python
class NestedTable:
    def filter(self, predicate: Expr | AggFn, /) -> NestedTable
    def agg(self, /, **named: AggExpr) -> LTSeq
    def derive(self, /, **named: AggExpr) -> LTSeq
    def first(self) -> LTSeq
    def last(self) -> LTSeq
    def flatten(self, *, group_id: str | None = None) -> LTSeq
    def count(self) -> int
```

- **`filter(predicate)`** keeps the groups where the aggregate predicate is TRUE. Groups keep their identity: two surviving groups with equal keys that were separated by a removed group stay two groups.
- **`agg(**named)`** returns one row per group: the key columns in `keys` order, then the named aggregates in keyword order. Rows are in group order. Order defined; `sort_keys` is `None`. A name that collides with a key raises `LTSeqValueError`; no names raise `LTSeqValueError`.
- **`derive(**named)`** returns every row of the surviving groups and adds or replaces columns as `LTSeq.derive` does ([§7.3]): a replaced column keeps its position, and replacing a column named in `sort_keys` truncates `sort_keys` before that key. In the expressions, bare column references are the current row's values, aggregates are computed over the row's group, and window methods run within the group in table order.
- **`first()`**, **`last()`** return the first or last row of each group, with the input schema, in group order.
- **`flatten(group_id=None)`** returns every row of the surviving groups in input order. With `group_id`, it appends an `int64` column numbering the surviving groups 0, 1, 2, … in order; a name that is already a column raises `LTSeqValueError`.
- **`count()`** executes and returns the number of surviving groups.
- `derive`, `first`, `last` and `flatten` keep the input's order. `first`, `last` and `flatten` replace no column (`flatten` may append one) and keep `sort_keys`; `derive` keeps the keys its replacements leave ([§9.2]).

### 11.3 Example

Sessions of one user's events, split at gaps of more than 30 minutes, keeping sessions with at least three events:

```python
sessions = (events.sort("ts")
    .group_ordered(starts_when=lambda r: r.ts - r.ts.shift(1) > timedelta(minutes=30))
    .filter(lambda g: g.count() >= 3)
    .agg(start=lambda g: g.ts.min(), end=lambda g: g.ts.max(), n=lambda g: g.count()))
```

<!-- v0.5-modular:footer -->

---

Previous: [Ordering contract (§9)](ordering.md) · [Index](../README.md) · Next: [Joins and set operations (§12–§13)](joins-and-sets.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§1.6]: overview.md#16-rule-ownership
[§4]: loading-and-laziness.md#4-loading
[§4.6]: loading-and-laziness.md#46-ltseqfrom_dict-and-ltseqfrom_rows
[§5]: loading-and-laziness.md#5-lazy-evaluation-and-materialization
[§5.3]: loading-and-laziness.md#53-eager-calls
[§7.3]: schema-and-table-operations.md#73-derive
[§8]: expressions.md#8-expression-dsl
[§9.2]: ordering.md#92-sources-and-propagation
[§9.4]: ordering.md#94-sort
[§10.1]: #101-window-methods
[§10.4]: #104-over
[§10.6]: #106-search_pattern
[§14]: aggregation.md#14-aggregation-partitioning-and-pivot
[§14.2]: aggregation.md#142-aggregate-expressions
[§16.3]: streaming-and-output.md#163-to_dicts
[§17]: numeric-null-temporal.md#17-numeric-semantics-and-literals
[§17.3]: numeric-null-temporal.md#173-arithmetic-operators
[§17.5]: numeric-null-temporal.md#175-shared-values
[§19]: numeric-null-temporal.md#19-temporal-semantics
[§20.2]: errors-and-performance.md#202-stages

<!-- /v0.5-modular:links -->
