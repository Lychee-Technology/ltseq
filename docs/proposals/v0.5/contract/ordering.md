<!-- v0.5-modular:header -->

# v0.5 contract: Ordering contract (§9)

[Index](../README.md) › Contract · Previous: [Expression DSL (§8)](expressions.md) · Next: [Windows and ordered grouping (§10–§11)](windows-and-grouping.md)

**Normative.** One module of the LTSeq v0.5 public API contract. It uses the BCP 14 key words as the contract's [status block](../README.md#ltseq-v05-public-api-contract) declares, and [§1.6] names the section that owns each cross-cutting rule.

**Scope.** The order model: order state and how each operation propagates it, the order requirements, `sort`, `assume_sorted`, order inspection, positional selection and `with_row_index`.

**Most cited from here.** [§10] Windows and ordered computation · [§4] Loading · [§20] Errors · [§6] Schema and types

<!-- /v0.5-modular:header -->

## 9. Ordering contract

### 9.1 Order state

Every `LTSeq` carries an order state that is known at plan time:

- **`is_ordered`**: whether the table has a *defined order*. A defined order is a sequence of the rows that every terminal delivers (`to_arrow`, `to_batches`, iteration, writers, the PyCapsule stream, `pickle`) and that every positional operation ([§9.3]) uses. It is reproducible: the same plan over the same input data yields the same sequence, except for unspecified ties.
- **`sort_keys`**: either `None` or a non-empty tuple of `SortKey(column, descending, nulls_last)`. When it is not `None`, the table has a defined order and that order satisfies the keys: for adjacent rows, the key tuple of the first does not sort after that of the second under [§9.4]. `sort_keys` never names a column the table does not have.

*Unspecified ties* arise in exactly two places: rows that compare equal on every key of a `sort` (or `.over(order_by=)`, [§10.4]) whose input order was undefined, and the matching right rows of one left row in a `join` whose right input order was undefined ([§12.1]). Their relative order is not specified and MAY differ between executions. So is every value computed from a row's position within a run of ties: `row_number`, `cum_sum`, `shift` and the other window methods, `with_row_index`, `slice`, `first` and `last`, `distinct(keep="first")`, and the right row `asof_join` takes among equal times. Each such result equals the operation applied to some order of each run, and that order MAY differ between executions. Everywhere else a defined order is fully determined.

An *undefined* order means rows may arrive in any sequence, which MAY differ between executions. A table with undefined order is a bag: every terminal still works, and positional operations refuse to run.

### 9.2 Sources and propagation

| Operation | Result order | Result `sort_keys` |
|---|---|---|
| `read_csv`, `read_parquet`, `from_arrow`, `from_pandas`, `from_dict`, `from_rows` | Defined ([§4]) | `None` |
| `LTSeq.range` | Defined | `(SortKey(name, step < 0, True),)` ([§4.7]) |
| `filter`, `delete`, `distinct`, `head`, `tail`, `slice`, `step`, `search_first`, `search_pattern`, `fold`, `collect`, `partition` values | Input's | Input's |
| `select`, `derive`, `NestedTable.derive`, `update`, `rename`, `drop`, window expressions | Input's (windows never reorder rows, #202) | Input's, renamed or truncated as [§7] states |
| `insert` | Input's (MUST be defined) | `None` |
| `sort` | Defined | The sort keys |
| `assume_sorted` | Input's (MUST be defined) | The declared keys |
| `reverse` | Reversed input order (MUST be defined) | Each key with `descending` and `nulls_last` both flipped |
| `with_row_index` | Input's (MUST be defined) | Input's if not `None`, else `(index ascending)` |
| `NestedTable.first`, `.last`, `.flatten` | Input's | Input's |
| `NestedTable.agg` | Group order (defined) | `None` |
| `LTSeq.agg` | Defined (one row) | `None` |
| `GroupBy.agg`, `pivot` | Undefined | `None` |
| `join` with `how` in `inner`, `left`, `cross`; `semi_join`, `anti_join`, `asof_join` | Left input's | Left input's |
| `join` with `how` in `right`, `full` | Undefined | `None` |
| `concat` | Defined if every input's is, else undefined | `None` |
| `intersect`, `difference` | Left input's | Left input's |

"Input's" means: defined if and only if the input's order is defined, and in that case the same relative order of the surviving rows.

Truncation: when an operation removes or replaces a column named in `sort_keys`, the result keeps the keys before it and drops it and every later key. If no key is left, `sort_keys` is `None` and the order stays defined.

### 9.3 Order requirements

Operations that depend on order belong to one of two tiers. Both checks happen at plan time.

| Tier | Requires | Operations | Error otherwise |
|---|---|---|---|
| Positional | A defined order | `tail`, `slice`, `step`, `reverse`, `with_row_index`, `insert`, `delete(int)`, `update(int, ...)`, `search_first`, `distinct` with `keep="first"` or `"last"`, the `first`, `last` and `string_agg` aggregates, `assume_sorted`, `is_sorted_by`, the right input of `asof_join` | `SortRequiredError` |
| Sequence | `sort_keys` is not `None` | Window methods and ranking functions without `.over(order_by=)` ([§10]), `group_ordered` ([§11]), `search_pattern` ([§10.6]), `fold` ([§10.7]) | `SortRequiredError` |

The sequence tier asks for declared keys, not just a defined order, so that a computation over "the previous row" always names what "previous" means. To use the order a source was read in, write `t.with_row_index()` (which declares `(index ascending)`) or `t.assume_sorted(...)` with the key the file is sorted by.

`head` and `show` are not in either tier: on a table with undefined order they return some `n` rows, so that any table can be inspected.

### 9.4 `sort`

```python
def sort(self, *keys: str, descending: bool | Sequence[bool] = False,
         nulls_last: bool | Sequence[bool] = True) -> LTSeq
```

- **Behavior.** Orders rows by the key columns, first key most significant. The sort is **stable**: rows equal on every key keep their input order when it is defined (otherwise they are unspecified ties, [§9.1]).
- **Arguments.** `descending` and `nulls_last` are one `bool` for all keys or one per key; a sequence of the wrong length raises `LTSeqValueError`. No keys raise `LTSeqValueError`. Keys MUST be column names; to sort by a computed value, `derive` it first.
- **Value order.** NULL is placed last when `nulls_last` is `True` and first otherwise, in either direction. Among non-NULL values:
  - Numbers by value; for floats, every NaN is greater than `+inf`, whatever its sign bit and payload (on x86-64, `inf - inf` gives a NaN with the sign bit set), and `-0.0` and `0.0` are equal.
  - Strings by Unicode code point (equal to UTF-8 byte order); binary by byte.
  - `False < True`.
  - Dates and naive timestamps chronologically; time-zone-aware timestamps by instant; durations by length.
- **Order and schema.** Same schema. Defined order; `sort_keys` is the given keys.
- **Errors.** An unknown key raises `ColumnNotFoundError`. A key of a pass-through type ([§6.2]) raises `LTSeqTypeError`.
- **Example.** `t.sort("symbol", "ts", descending=[False, True])`

NULL placement is a change from v0.4, which sorted NULL as the largest value (first when descending). `nulls_last=True` in both directions matches pandas `sort_values` and DuckDB's default.

### 9.5 `assume_sorted`

```python
def assume_sorted(self, *keys: str, descending: bool | Sequence[bool] = False,
                  nulls_last: bool | Sequence[bool] = True) -> LTSeq
```

- **Behavior.** Declares that the table's defined order already satisfies the keys, without sorting. Plan-building for every source.
- **Validation.** Adjacent rows *k* and *k + 1* that violate the keys are a failure of row *k + 1*, checked whenever row *k + 1* is read ([§20.2]), including pairs that straddle batch, file and partition boundaries. It raises `OrderViolationError` naming the position and key. A terminal that reads the whole table, `count()` among them, checks every pair; `head(n)`, `show(n)`, `search_first`, `count()` over `head(n)` or `search_first`, and an iteration stopped early check only the rows they read. An execution MUST NOT skip reading a row on the strength of the declaration (for example by ending a filter once the key passes a bound), so a violation can make a result raise but never makes it wrong. A filter above the declaration demands its predicate on every row it reaches ([§20.2]), so the declared table is read without gaps up to where the terminal stops, and every pair read is checked: Parquet statistics pruning and predicate pushdown into the scan are not applied below an `assume_sorted`, even though they could not make a result wrong, because they would leave pairs unchecked. With row groups `x = [1, 5]` and `x = [3, 7]`, `.assume_sorted("x").filter(lambda r: r.x >= 6)` raises on every path. To keep pruning, filter first: `.filter(p).assume_sorted(k)` declares and checks the order of the rows that pass.
- **Order requirement.** Positional tier.
- **Errors.** Arguments as `sort`. `OrderViolationError` during execution.
- **Example.** `LTSeq.read_parquet("ticks/").assume_sorted("ts")`

### 9.6 `is_sorted_by`

```python
def is_sorted_by(self, *keys: str, descending: bool | Sequence[bool] = False,
                 nulls_last: bool | Sequence[bool] = True) -> bool
```

Executes and returns whether the defined order satisfies the keys (adjacent rows non-decreasing under [§9.4]). `True` for 0 or 1 rows. Positional tier. Arguments as `sort`.

### 9.7 `sort_keys` and `is_ordered`

```python
@property
def sort_keys(self) -> tuple[SortKey, ...] | None
@property
def is_ordered(self) -> bool
```

Neither executes. `t.sort("a", descending=True).sort_keys == (SortKey("a", True, True),)`.

### 9.8 Positional selection

```python
def head(self, n: int = 10) -> LTSeq
def tail(self, n: int = 10) -> LTSeq
def slice(self, offset: int = 0, length: int | None = None) -> LTSeq
def step(self, n: int, offset: int = 0) -> LTSeq
def reverse(self) -> LTSeq
```

| Method | Rows | Requires |
|---|---|---|
| `head(n)` | The first `n` rows; any `n` rows if the order is undefined | Nothing |
| `tail(n)` | The last `n` rows | Positional tier |
| `slice(offset, length)` | Rows `offset` … `offset + length - 1`; to the end when `length` is `None` | Positional tier |
| `step(n, offset)` | Rows `offset`, `offset + n`, `offset + 2n`, …, as `rows[offset::n]` | Positional tier |
| `reverse()` | All rows, last first | Positional tier |

- Fewer rows than requested is not an error.
- `n`, `offset` and `length` MUST be non-negative and `step`'s `n` at least 1; otherwise `LTSeqValueError`. Negative values are refused instead of being given pandas' "all but the last n" meaning, because that meaning needs the row count and has no counterpart in `slice` or `step`.
- `rvs` is renamed `reverse`.

### 9.9 `with_row_index`

```python
def with_row_index(self, name: str = "index", *, offset: int = 0) -> LTSeq
```

- **Behavior.** Adds an `int64` column holding each row's 0-based position plus `offset`, as the first column.
- **Order.** Positional tier. Keeps order. Keeps `sort_keys` if not `None`; otherwise declares `(SortKey(name, False, True),)`.
- **Errors.** A `name` that is already a column raises `LTSeqValueError`.

<!-- v0.5-modular:footer -->

---

Previous: [Expression DSL (§8)](expressions.md) · [Index](../README.md) · Next: [Windows and ordered grouping (§10–§11)](windows-and-grouping.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§1.6]: overview.md#16-rule-ownership
[§4]: loading-and-laziness.md#4-loading
[§4.7]: loading-and-laziness.md#47-ltseqrange
[§6]: schema-and-table-operations.md#6-schema-and-types
[§6.2]: schema-and-table-operations.md#62-supported-types
[§7]: schema-and-table-operations.md#7-basic-table-operations
[§9.1]: #91-order-state
[§9.3]: #93-order-requirements
[§9.4]: #94-sort
[§10]: windows-and-grouping.md#10-windows-and-ordered-computation
[§10.4]: windows-and-grouping.md#104-over
[§10.6]: windows-and-grouping.md#106-search_pattern
[§10.7]: windows-and-grouping.md#107-fold
[§11]: windows-and-grouping.md#11-ordered-grouping
[§12.1]: joins-and-sets.md#121-join
[§20]: errors-and-performance.md#20-errors
[§20.2]: errors-and-performance.md#202-stages

<!-- /v0.5-modular:links -->
