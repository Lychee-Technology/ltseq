<!-- v0.5-modular:header -->

# v0.5 contract: Aggregation, partitioning and pivot (§14)

[Index](../README.md) › Contract · Previous: [Joins and set operations (§12–§13)](joins-and-sets.md) · Next: [Streaming, output and interchange (§15–§16)](streaming-and-output.md)

**Normative.** One module of the LTSeq v0.5 public API contract. It uses the BCP 14 key words as the contract's [status block](../README.md#ltseq-v05-public-api-contract) declares, and [§1.6] names the section that owns each cross-cutting rule.

**Scope.** `group_by` and `GroupBy.agg`, the aggregate expressions and their result types, whole-table and `NestedTable` aggregation, `partition` and `pivot`.

**Most cited from here.** [§9] Ordering contract · [§16] Output and interchange · [§18] NULL, NaN and Boolean logic · [§5] Lazy evaluation and materialization

<!-- /v0.5-modular:header -->

## 14. Aggregation, partitioning and pivot

### 14.1 `group_by` and `GroupBy.agg`

```python
def group_by(self, *keys: str) -> GroupBy
class GroupBy:
    def agg(self, /, **named: AggExpr) -> LTSeq
```

- **Behavior.** One output row per distinct combination of key values among the input rows. Keys are compared with the equality of [§18](numeric-null-temporal.md#18-null-nan-and-boolean-logic): NULL is one group, NaN is one group, `-0.0` and `0.0` are one group.
- **Output.** The key columns in `keys` order, then the named aggregates in keyword order. No names raise `LTSeqValueError`, as for `NestedTable.agg` and `LTSeq.agg`; the distinct key combinations are `select(*keys).distinct(keep="any")`.
- **Order.** Undefined; `sort` the result for a defined order. `first`, `last` and `string_agg` read each group's rows in input order (positional tier on the input).
- **Errors.** No keys raise `LTSeqValueError` (use `LTSeq.agg`). An aggregate name that equals a key raises `LTSeqValueError`. Unknown key `ColumnNotFoundError`; a pass-through-type key `LTSeqTypeError`.

### 14.2 Aggregate expressions

In a `Group` context, a column reference MUST appear inside an aggregate (`LTSeqTypeError` otherwise), except in `NestedTable.derive`. Expressions over aggregates are allowed (`g.x.sum() / g.count()`); an aggregate inside an aggregate and a window method inside an aggregate raise `LTSeqTypeError`.

Every aggregate method takes `*, where: IntoExpr | None = None`: only rows where `where` is TRUE take part, and the aggregate's arguments are demanded only on those rows ([§20.2]). `where` replaces the `*_if` functions.

| Aggregate | Value | Type | No non-NULL value taking part |
|---|---|---|---|
| `g.count(where=None)` | Number of rows | `int64` | `0` |
| `x.count()` | Number of non-NULL values (NaN counts) | `int64` | `0` |
| `x.n_unique()` | Number of distinct non-NULL values under [§18] (NaN is one value; `-0.0` and `0.0` are one value) | `int64` | `0` |
| `x.sum()` | Sum of non-NULL values, checked (#221) | `int64` for signed integers, `uint64` for unsigned, `float64` for floats, `decimal(min(p + 10, P), s)` for decimals | NULL |
| `x.mean()` | Arithmetic mean of non-NULL values | `float64` for integers and floats; `decimal(min(p + 4, P), min(s + 4, P − (p − s)))` for decimals, truncated toward zero as decimal `/` | NULL |
| `x.min()`, `x.max()` | Smallest, largest non-NULL value under the order of [§9.4](ordering.md#94-sort): `max` is NaN when a NaN is present, `min` is NaN only when every value is | Input type | NULL |
| `x.median()` | `x.quantile(0.5)` | `float64` | NULL |
| `x.quantile(q)` | Quantile with linear interpolation between the two nearest ranks (NumPy's default `linear` method), as an exact value rounded once (below); `q` is a literal in [0, 1] | `float64`; numeric input only | NULL |
| `x.std()`, `x.var()` | Sample standard deviation and variance (`ddof = 1`) | `float64` | NULL (also with one non-NULL value) |
| `x.first()`, `x.last()` | Value of the first or last row taking part in table order, NULL included | Input type | NULL when no row takes part |
| `x.mode()` | Most frequent non-NULL value; the smallest under [§9.4] on a tie | Input type | NULL |
| `x.string_agg(delimiter=",")` | Non-NULL values joined in table order; string input only | `string` | NULL |
| `x.all()`, `x.any()` | Whether all, any, non-NULL values are TRUE; `bool` input only | `bool` | `True` for `all`, `False` for `any` |

The last column applies when no row takes part or every value taking part is NULL, so `x.sum()` and `x.sum(where=x.is_not_null())` agree. `g.count()` counts rows taking part, NULL or not. In decimal types, *p* and *s* are the input's precision and scale, and *P* is the input width's maximum precision: 38 for `decimal128` (and for `decimal32`/`decimal64`, widened per [§17.4]), 76 for `decimal256`. The result keeps the input's width, and the mean type never has fewer integer digits than the input.

**Statistics are exact values rounded once.** The `sum` and `mean` of floats, the `mean` of integers, and every `median`, `quantile`, `var`, `std`, `cov` and `corr` return the exact mathematical value over the values taking part, rounded once to `float64` (round half to even; `±inf` only when the exact value rounds beyond the `float64` range). Inputs are not converted first: an integer, decimal, `float32` or `float64` value takes part with its exact value, so integer inputs above `2**53` lose nothing before the final rounding. A zero result is `+0.0`, since exact arithmetic has a single zero. With *n* the number of values taking part and Σ an exact sum over them:

- `sum` is Σ*x*; for finite floats this is `float(sum(map(Fraction, values)))`, which `math.fsum` also returns when it does not raise and the result is not zero.
- `mean` is Σ*x* / *n*, so it overflows only when the mean itself is beyond the range: `mean([1e308, 1e308])` is `1e308`.
- `quantile(q)` is *x*ⱼ + *t*(*x*ⱼ₊₁ − *x*ⱼ), with the values sorted ascending as *x*₀ … *x*ₙ₋₁, *h* = *q*(*n* − 1) for the exact value of the `float` *q*, *j* = ⌊*h*⌋ and *t* = *h* − *j*; it is *x*ⱼ where *t* = 0. So `median([1, 2**53 + 1])` is `4503599627370497.0`, where NumPy and pyarrow, which convert the inputs first, give `4503599627370496.0`.
- `var` is (*n*Σ*x*² − (Σ*x*)²) / (*n*(*n* − 1)), never negative. `std` is its exact square root rounded once, so `std([1e308, -1e308])` is `1.4142135623730951e+308` although that `var` rounds to `inf`.
- `cov(a, b)` is (*n*Σ*ab* − Σ*a*Σ*b*) / (*n*(*n* − 1)).
- `corr(a, b)` is (*n*Σ*ab* − Σ*a*Σ*b*) / √((*n*Σ*a*² − (Σ*a*)²)(*n*Σ*b*² − (Σ*b*)²)) rounded once, so it lies in [−1, 1] and `corr(a, a)` is `1.0`; a zero variance in either argument gives NaN.

Non-finite inputs: a float `sum` or `mean` is NaN if a value taking part is NaN or both `+inf` and `-inf` take part, and that infinity if only one sign of infinity does; `var`, `std`, `cov` and `corr` are NaN when a value taking part is NaN or infinite. A `median` or `quantile` with *t* > 0 and *x*ⱼ or *x*ⱼ₊₁ infinite is the value of (1 − *t*)·*x*ⱼ + *t*·*x*ⱼ₊₁: that infinity if only one sign of infinity is among the two, NaN if both are. NumPy differs here: it interpolates in floats and gives NaN for `quantile([1.0, inf], 0.5)`. `cum_sum` and `rolling` apply the same rules to the running or window values, once per row. Each result is a function of the multiset of values taking part, so every evaluation order, batch size and partitioning gives the same bits (P6, P10). Exact accumulation (Σ*x*, Σ*x*² and Σ*ab* held without rounding) also supports sliding frames by exact subtraction; its cost is measured before the aggregates are built (#251).

```python
def corr(a: IntoExpr, b: IntoExpr, /, *, where: IntoExpr | None = None) -> Expr
def cov(a: IntoExpr, b: IntoExpr, /, *, where: IntoExpr | None = None) -> Expr
```

`corr` (Pearson) and `cov` (`ddof = 1`) use the rows where both values are non-NULL; they return `float64`, NULL for fewer than two such rows, and are otherwise the exact values above, rounded once.

A NaN taking part in `sum`, `mean`, `std`, `var`, `quantile`, `corr` or `cov` makes the result NaN. A decimal `sum` or `mean` whose value does not fit its result type raises `ArithmeticOverflowError`.

Removed, with their replacements: `avg` (`mean`), `variance` (`var`), `stddev` (`std`), `percentile(p)` (`quantile(q)`, now exact), `top_k`, `count_if`/`sum_if`/`avg_if`/`min_if`/`max_if` (`where=`), `skew`, `concat_agg` (`string_agg`, now ordered), `covar` (`cov`).

### 14.3 `LTSeq.agg`

```python
def agg(self, /, **named: AggExpr) -> LTSeq
```

One row holding the named aggregates over the whole table, in keyword order, even when the table is empty. Order defined; `sort_keys` is `None`. No names raise `LTSeqValueError`. The `by=` argument is removed (use `group_by`).

### 14.4 `NestedTable` aggregation

`NestedTable.agg`, `filter` and `derive` ([§11.2]) use the same aggregates; `first`, `last` and `string_agg` read each group in table order.

### 14.5 `partition`

```python
def partition(self, *keys: str) -> dict[Any, LTSeq]
```

- **Behavior.** Finds the distinct key values **when called** (eager, [§5.3]) and returns a `dict` from each to the table of its rows. Key values are distinct under the equality of [§18], and two dict keys compare equal, with equal hashes, exactly when their key values are equal under [§18], so every partition has its own dict key. The dict key is the key column's value as `to_dicts` returns it ([§16.3]), with two exceptions: an aware timestamp is its instant as a `datetime` with `tzinfo=datetime.timezone.utc`, and a float zero is `0.0`. For several key columns it is a tuple of these.
- **Aware keys.** `to_dicts` returns an aware value in its own zone, and Python compares two `datetime`s that share a `tzinfo` by wall clock and ignores `fold` (PEP 495). The two occurrences of a repeated hour, 01:30 EDT and 01:30 EST on 2024-11-03 in New York, are one hour apart but equal as `ZoneInfo` values with equal hashes, so a `dict` keyed by them would hold one of the two partitions. As UTC values they differ. Python compares values of different zones by instant, except that PEP 495 makes a value in a repeated hour unequal to every value of another zone, so `parts[v]` with `v` taken from `to_dicts` finds its key except in a repeated hour, where it raises `KeyError`; it never returns another key's table. `parts[v.astimezone(timezone.utc)]` finds the key at every instant, and `k.astimezone(zone)` displays a key in its zone.
- **Dict order.** Keys are in the order `sort(*keys)` gives them with its defaults ([§9.4]): ascending, the first key column most significant, NULL last in each column. Tuple keys are ordered by their first component, then the next, with `None` last within each: `("E", "x")`, `("E", None)`, `("W", "x")`, `(None, "x")`.
- **Values.** Each value is plan-building: the rows of `self` whose keys equal the dict key (NULL equal to `None`), with `self`'s order and `sort_keys`.
- **Consistency.** The keys come from one read of the source at the call, and each value reads the source again when it executes ([§5.1]), so executing every value reads the source N + 1 times for N keys, counting the read at the call, and two values executed at different times can see different versions of the source: the dict is not an atomic snapshot of it. If the source changes in between, rows with a key added since the call appear in no value, and a value whose rows were removed is empty. To split a source that may change, or to scan it once, partition a snapshot: `t.collect().partition("region")`.
- **Errors.** No keys raise `LTSeqValueError`. A NaN key value raises `LTSeqValueError` at the call, because NaN cannot be looked up in a `dict`. A key value that `to_dicts` cannot convert ([§16.3]) raises that `CastError` at the call, naming the column, even though the key itself is in UTC. No key is rounded or truncated to fit; to split on such a column, partition on a derived key that converts, such as `r.ts.dt.truncate("second")`. A pass-through-type key raises `LTSeqTypeError`.

`PartitionedTable`, `SQLPartitionedTable` and `partition(by=lambda ...)` are removed. To apply a function per partition: `{k: f(v) for k, v in t.partition("region").items()}`.

### 14.6 `pivot`

```python
def pivot(self, *, index: str | Sequence[str], columns: str, values: str,
          agg: Literal["sum", "mean", "min", "max", "count", "first", "last"] = "sum",
          column_values: Sequence[Literal] | None = None) -> LTSeq
```

- **Behavior.** One row per distinct `index` combination (grouped as `group_by`). For each value *v* of the `columns` column, one output column whose cell is `values.<agg>(where=columns == v)` over the row's group, with NULL *v* matching NULL. So a combination with no rows is NULL, or `0` for `count`.
- **Output columns.** The index columns, then one column per *v*, named `str(v)` with *v* as `to_dicts` returns it ([§16.3]), or `"null"` for NULL; a *v* that [§16.3] cannot convert raises its `CastError`, at plan time for `column_values` and at the call otherwise. With `column_values`, in that order; otherwise in ascending order under [§9.4], NULL last. A generated name that equals an index column or another generated name raises `LTSeqValueError`.
- **Laziness.** With `column_values`, plan-building; a `columns` value not in `column_values` raises `LTSeqValueError` during execution. Without it, the distinct values are found when called ([§5.3]) and then act as `column_values`, so a value that a later execution meets and the call did not find raises `LTSeqValueError` during execution.
- **Order.** Undefined; `sort_keys` is `None`. `first` and `last` read rows in input order (positional tier).
- **Types.** As the aggregate of [§14.2].
- **Example.** `sales.pivot(index="region", columns="quarter", values="amount", column_values=["Q1", "Q2", "Q3", "Q4"])`

<!-- v0.5-modular:footer -->

---

Previous: [Joins and set operations (§12–§13)](joins-and-sets.md) · [Index](../README.md) · Next: [Streaming, output and interchange (§15–§16)](streaming-and-output.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§1.6]: overview.md#16-rule-ownership
[§5]: loading-and-laziness.md#5-lazy-evaluation-and-materialization
[§5.1]: loading-and-laziness.md#51-plan-building-and-execution
[§5.3]: loading-and-laziness.md#53-eager-calls
[§9]: ordering.md#9-ordering-contract
[§9.4]: ordering.md#94-sort
[§11.2]: windows-and-grouping.md#112-nestedtable
[§14.2]: #142-aggregate-expressions
[§16]: streaming-and-output.md#16-output-and-interchange
[§16.3]: streaming-and-output.md#163-to_dicts
[§17.4]: numeric-null-temporal.md#174-types-of-mixed-operands
[§18]: numeric-null-temporal.md#18-null-nan-and-boolean-logic
[§20.2]: errors-and-performance.md#202-stages

<!-- /v0.5-modular:links -->
