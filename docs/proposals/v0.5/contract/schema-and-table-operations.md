<!-- v0.5-modular:header -->

# v0.5 contract: Schema, types and basic table operations (§6–§7)

[Index](../README.md) › Contract · Previous: [Loading and lazy evaluation (§4–§5)](loading-and-laziness.md) · Next: [Expression DSL (§8)](expressions.md)

**Normative.** One module of the LTSeq v0.5 public API contract. It uses the BCP 14 key words as the contract's [status block](../README.md#ltseq-v05-public-api-contract) declares, and [§1.6] names the section that owns each cross-cutting rule.

**Scope.** Schemas, the supported Arrow types, dtype arguments and column names; then the basic table operations from `filter` to the value-level edits `insert`, `delete` and `update`.

**Most cited from here.** [§16] Output and interchange · [§17] Numeric semantics and literals · [§4] Loading · [§9] Ordering contract

<!-- /v0.5-modular:header -->

## 6. Schema and types

### 6.1 `schema` and `columns`

```python
@property
def schema(self) -> pa.Schema
@property
def columns(self) -> list[str]
```

- `schema` is the exact Arrow schema of the rows the table produces: names, types and nullability. It has no field or schema metadata. For every table `t`, `t.to_arrow().schema.equals(t.schema)` MUST hold. Nullability is the planned nullability and is conservative: a field marked non-nullable never holds NULL, and a field MAY be marked nullable when it never holds NULL. Beyond these two rules nullability is not part of the contract, so two conforming implementations may mark the same field differently.
- `columns` is `[f.name for f in t.schema]`.
- A table has zero or more columns and zero or more rows, and the two counts are independent. A table without columns is a valid relation with a row count, which `count()` returns; it is not a table without rows. It comes from `select()` and from a `drop` of every column, which keep the input's rows ([§7.2], [§7.5]), and from a constructor input that carries rows but no columns: an Arrow schema without fields ([§4.4]), a pandas frame without columns ([§4.5]) and `from_rows` ([§4.6]). An input that gives no row count is not one: `from_dict({})` ([§4.6]), and a 0-byte CSV without a `pa.Schema`, whose column count is unknown ([§4.2]), raise `LTSeqValueError` at the call, and no row count is assumed. The other rules apply to an empty column list unchanged:
  - Every row equals every other ([§18]), so `distinct()` keeps one row of a nonempty table ([§7.6]), and `intersect` and `difference` treat all rows as one value ([§13.2]).
  - Operations that read no column keep or derive the row count as they would with columns: `filter` with a predicate that reads no column, `head`, `tail`, `slice`, `step`, `reverse`, `concat`, a cross join, `with_row_index`, `derive`, `LTSeq.agg`, `collect` and pickling. `sort_keys` is `None`, and the order stays as defined as the input's ([§9.2]).
  - A reference to any column raises `ColumnNotFoundError`, as for any unknown name.
  - `to_arrow` returns a `pa.Table` with an empty schema and `num_rows == t.count()`. `to_batches` and the C stream ([§15.1], [§16.4]) carry the rows in the lengths of batches without fields. `to_dicts` and iteration give one `{}` per row. `to_pandas` gives a frame without columns whose `RangeIndex` has one entry per row.
  - `write_csv` and `write_parquet` refuse it ([§16.5]): CSV cannot write a row without fields, and today's Parquet writers record such a file as 0 rows.
- Neither executes. Both are also available on the tables `NestedTable.flatten()` and the aggregations return; `NestedTable` itself exposes neither.

### 6.2 Supported types

| Arrow types | Support |
|---|---|
| `null` | The type of an all-`None` column and of an untyped `lit(None)`. Pass-through, except that `cast` and `try_cast` convert it to any type ([§17.6]), `is_null`, `is_not_null`, `fill_null` and `coalesce` accept it, and it shares a column with values of any type ([§17.5]) |
| `bool` | Full |
| `int8`–`int64`, `uint8`–`uint64` | Full |
| `float32`, `float64` | Full. `float16` is widened to `float32` when read (exact; only that column is copied). |
| `decimal32`, `decimal64`, `decimal128`, `decimal256` | Full ([§17.4] for coercions) |
| `string`, `large_string` | Full |
| `binary`, `large_binary`, `fixed_size_binary` | Comparison, sort, keys, `is_in`, NULL helpers |
| `date32` | Full. `date64` is converted to `date32` when read (exact: `date64` values are whole days; only that column is copied). A `date64` value that `date32` cannot hold (2**31 days or more after 1970, or more than 2**31 days before) raises `CastError` during execution. |
| `time32`, `time64` | Comparison, sort, keys, `.dt` time fields |
| `timestamp[s\|ms\|us\|ns]`, with or without time zone | Full ([§19]) |
| `duration[s\|ms\|us\|ns]` | Full ([§19]) |
| `interval`, `list`, `large_list`, `fixed_size_list`, `struct`, `map`, `union` | Pass-through only: they can be selected, renamed, dropped, carried through filters, sorts, joins and set operations (not as keys), and written to Parquet unless they are or contain a type [§16.5] refuses there. Any expression on them raises `LTSeqTypeError` at plan time. |

Normalization at every boundary where a schema is visible (`schema`, `to_arrow`, `to_batches`, the PyCapsule stream, written files):

- Engine-internal layouts MUST NOT appear: `string_view` is reported as `string`, `binary_view` as `binary`.
- Dictionary-encoded columns are reported as their value type.
- Extension types are reported as their storage type.
- Field and schema metadata are dropped. The Arrow schema that `write_parquet` stores in the file is not metadata in this sense ([§16.5]).

A type that a consumer asks for through `requested_schema` ([§16.4]), and that [§16.4] accepts, is delivered as asked, even where this normalization would report another: a consumer that asks for `string_view` receives `string_view`.

### 6.3 Data type arguments (`DTypeLike`)

Wherever a type is an argument (`cast`, `try_cast`, `schema=`, `lit(...).cast`), it is a `pyarrow.DataType` or a string. A string MUST be an alias that `pyarrow.type_for_alias` accepts, with these exceptions, which raise `LTSeqValueError`:

- `"float"` and `"halffloat"`: `pyarrow` reads `"float"` as `float32` while pandas and NumPy read it as `float64`. Write `"float32"` or `"float64"`.
- `"float16"`: not a supported computation type.

Parameterized types are passed as objects: `pa.decimal128(12, 2)`, `pa.timestamp("ms", tz="Europe/Paris")`. An unknown string raises `LTSeqValueError` at plan time.

### 6.4 Column names

- A column name is any non-empty `str`. Names are case-sensitive and unique within a table.
- No name is reserved. LTSeq MUST NOT add a column to a user-visible schema unless the method's contract names it, and MUST NOT fail or change results because a user column has a name the implementation uses internally.
- In a lambda, `r.name` and `r["name"]` both reference a column; `r["name"]` works for any name, including names that are not Python identifiers. The row proxy has no methods, so no column name is shadowed. The group proxy ([§8.1]) reserves exactly one attribute, `count`; `g["count"]` reaches a column named `count`.

---

## 7. Basic table operations

Unless a section says otherwise, the methods in this section are plan-building, keep the order state of their input ([§9.1]), and raise their documented errors when they are called. "Keeps `sort_keys`" means the result's `sort_keys` equal the input's; where a method can invalidate a key, the section says how `sort_keys` is truncated ([§9.2]).

Value arguments (`RowExpr`) take an `Expr`, a literal or a lambda; condition arguments take an `Expr` or a lambda ([§2.3]). Column references inside an `Expr` are resolved by name against the table the method is called on.

### 7.1 `filter`

```python
def filter(self, predicate: Expr | ExprFn, /) -> LTSeq
```

- **Behavior.** Keeps the rows where `predicate` is TRUE. Rows where it is FALSE or NULL are dropped.
- **Order and schema.** Same schema; keeps order and `sort_keys`.
- **Types.** The predicate MUST be a `bool` expression. Any other type, including the `null` type of a lambda that returns `None` (typically a missing `return`), and a Python `bool` ([§3.4]), raises `LTSeqTypeError`.
- **Example.** `t.filter(lambda r: (r.qty > 0) & r.symbol.is_in(["AAPL", "MSFT"]))`

### 7.2 `select`

```python
def select(self, /, *columns: str, **named: RowExpr) -> LTSeq
```

- **Behavior.** Returns the positional columns in the given order, followed by one column per keyword argument, in keyword order, computed from the input row. `select()` with no arguments returns a table without columns and the input's rows ([§6.1]).
- **Order and schema.** Keeps order. `sort_keys` keeps the longest prefix whose columns appear in the result unchanged; a keyword argument whose expression is a bare column reference (`y=lambda r: r.x`) carries `x`'s key under the name `y`.
- **Errors.** A name produced twice (positionally, by keyword, or both) raises `LTSeqValueError`. A positional argument that is not a `str` raises `LTSeqTypeError` with a hint to pass it as a keyword. An unknown positional name raises `ColumnNotFoundError`.
- **Example.** `t.select("date", "symbol", notional=lambda r: r.price * r.qty)`

### 7.3 `derive`

```python
def derive(self, /, **named: RowExpr) -> LTSeq
```

- **Behavior.** Adds or replaces columns. Every expression is evaluated against the **input** row: an expression in the same call that references a name being derived sees the input column of that name, or raises `ColumnNotFoundError` if there is none. To build on a derived column, chain a second `derive`.
- **Order and schema.** A replaced column keeps its position and takes the new expression's type; new columns are appended in keyword order. Keeps order. Replacing a column that appears in `sort_keys` truncates `sort_keys` before that key.
- **Values.** A literal value derives a constant column of the literal's type ([§17.2]): `derive(flag=False)` is `bool` (a lambda returning `False` raises, [§3.4]), `derive(x=None)` is the `null` type. A typed NULL column is `lit(None).cast("int64")`.
- **Errors.** No keyword arguments raise `LTSeqValueError`.
- **Example.** `t.derive(ret=lambda r: r.close / r.close.shift(1) - 1, close=lambda r: r.close.round(2))`

`with_columns`, and the positional form that takes a lambda returning a `dict`, are removed.

### 7.4 `rename`

```python
def rename(self, mapping: Mapping[str, str] | None = None, /, **renames: str) -> LTSeq
```

- **Behavior.** Renames columns from old name to new name. `mapping` and `renames` are combined; renames apply simultaneously, so `{"a": "b", "b": "a"}` swaps two columns.
- **Order and schema.** Positions and types unchanged. Keeps order; `sort_keys` follow their columns' new names.
- **Errors.** An old name that is not a column raises `ColumnNotFoundError`. An old name given in both arguments, or a result with duplicate names, raises `LTSeqValueError`.
- **Example.** `t.rename({"px": "price"})`

### 7.5 `drop`

```python
def drop(self, *columns: str) -> LTSeq
```

- **Behavior.** Removes the named columns. A name listed twice is removed once. `drop()` with no names returns an equivalent table. Dropping every column returns a table without columns and the input's rows ([§6.1]).
- **Order and schema.** Remaining columns keep their relative order. Keeps order. `sort_keys` is truncated before the first dropped key.
- **Errors.** An unknown name raises `ColumnNotFoundError`.
- **Example.** `t.drop("tmp1", "tmp2")`

### 7.6 `distinct`

```python
def distinct(self, *keys: str, keep: Literal["first", "last", "any"] = "first") -> LTSeq
```

- **Behavior.** With `keys`, keeps one row per distinct combination of the key columns, with all of its columns: the first or last such row in table order, or an unspecified one for `keep="any"`. Without `keys`, it is `distinct(*all columns, keep=keep)`; which duplicate survives matters because rows equal under [§18] can differ in bits (`-0.0` and `0.0`). On a table without columns every row is equal, so `distinct()` keeps one row of a nonempty table, and none of an empty one ([§6.1]).
- **Equality.** NULL equals NULL and NaN equals NaN; `-0.0` equals `0.0` ([§18]).
- **Order and schema.** Same schema. The kept rows appear in input order. Keeps `sort_keys`.
- **Order requirement.** Positional tier ([§9.3]) when `keep` is `"first"` or `"last"`, with or without `keys`: an undefined order raises `SortRequiredError`, whose message suggests `keep="any"`. With `keep="any"`, it is legal on any table.
- **Errors.** An unknown key raises `ColumnNotFoundError`.
- **Example.** `quotes.distinct("symbol", keep="last")` keeps the latest quote per symbol.

### 7.7 `pipe`

```python
def pipe(self, func: Callable[Concatenate[LTSeq, P], T], /, *args: P.args, **kwargs: P.kwargs) -> T
```

Returns `func(self, *args, **kwargs)`.

### 7.8 `explain`

```python
def explain(self, *, physical: bool = False) -> str
```

Returns a text rendering of the logical plan, followed by the physical plan when `physical=True`. It does not read rows. The text is for people; its format is not part of this contract. `explain_plan` is removed.

### 7.9 `count` and `show`

```python
def count(self) -> int
def show(self, n: int = 10) -> None
```

- `count()` executes and returns the number of rows. It demands what decides which rows exist and in what order (filter predicates, join, sort and grouping keys, and their inputs) and nothing else ([§20.2]): an error in a column that only supplies output values does not make `count()` fail. It reads the rows its input reads ([§20.2]), so over an input that no `head(n)`, `slice` or `search_first` bounds it checks every `assume_sorted` pair ([§9.5]).
- `show(n)` prints the schema and the rows of `head(n)` to standard output and returns `None`. It executes `head(n)`, not the whole table. The layout is not part of this contract. `n < 0` raises `LTSeqValueError`.

### 7.10 Value-level edits: `insert`, `delete`, `update`

These are ordinary plan-building methods that return a new table. `modify` is removed; `update` covers it.

```python
def insert(self, pos: int, rows: Mapping[str, Any] | Sequence[Mapping[str, Any]], /) -> LTSeq
def delete(self, where: Expr | ExprFn | int | numpy.integer, /) -> LTSeq
def update(self, where: Expr | ExprFn | int | numpy.integer, /, **values: RowExpr) -> LTSeq
```

**`insert`** places the given rows before row `pos`, with `list.insert` semantics: a negative `pos` counts from the end, and an out-of-range `pos` is clamped to the start or the end. It never raises `IndexError`.

- Positional tier: an undefined order raises `SortRequiredError`.
- A key that is not a column raises `ColumnNotFoundError`; a missing key is NULL. Values MUST be exact in the column type ([§17.5]), else `CastError`.
- Result `sort_keys` is `None`; order stays defined.

**`delete`** removes the rows where `where` is TRUE. A row where it is NULL is kept, so `delete(p)` is not `filter(~p)`. An `int` or a NumPy integer is a position; a `bool` raises `LTSeqTypeError` ([§3.4]). With a position, removes the row at that position, counting from the end when negative; positional tier. A position outside the table raises `LTSeqIndexError` during execution. Keeps order and `sort_keys`.

**`update`** sets the named columns in the rows where `where` is TRUE (or in the row at a position, given as for `delete`; positional tier) and leaves all other rows unchanged, including rows where `where` is NULL. Each value is an expression evaluated against the input row, or a literal.

- A column's type never changes. A literal MUST be exact in the column type (`CastError`). An expression's type MUST be one whose every value the column type holds exactly ([§17.5]), else `LTSeqTypeError`; cast it explicitly.
- An unknown column raises `ColumnNotFoundError`; no values raise `LTSeqValueError`. A position outside the table raises `LTSeqIndexError` during execution.
- Updating a column in `sort_keys` truncates `sort_keys` before that key.

```python
t.update(lambda r: r.price < 0, price=None, flag=lambda r: ~r.flag)
```

<!-- v0.5-modular:footer -->

---

Previous: [Loading and lazy evaluation (§4–§5)](loading-and-laziness.md) · [Index](../README.md) · Next: [Expression DSL (§8)](expressions.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§1.6]: overview.md#16-rule-ownership
[§2.3]: public-surface.md#23-type-aliases-used-in-signatures
[§3.4]: public-surface.md#34-lambdas-and-proxies
[§4]: loading-and-laziness.md#4-loading
[§4.2]: loading-and-laziness.md#42-ltseqread_csv
[§4.4]: loading-and-laziness.md#44-ltseqfrom_arrow
[§4.5]: loading-and-laziness.md#45-ltseqfrom_pandas
[§4.6]: loading-and-laziness.md#46-ltseqfrom_dict-and-ltseqfrom_rows
[§6.1]: #61-schema-and-columns
[§7.2]: #72-select
[§7.5]: #75-drop
[§7.6]: #76-distinct
[§8.1]: expressions.md#81-contexts-and-proxies
[§9]: ordering.md#9-ordering-contract
[§9.1]: ordering.md#91-order-state
[§9.2]: ordering.md#92-sources-and-propagation
[§9.3]: ordering.md#93-order-requirements
[§9.5]: ordering.md#95-assume_sorted
[§13.2]: joins-and-sets.md#132-intersect-and-difference
[§15.1]: streaming-and-output.md#151-to_batches
[§16]: streaming-and-output.md#16-output-and-interchange
[§16.4]: streaming-and-output.md#164-arrow-pycapsule-stream
[§16.5]: streaming-and-output.md#165-writers
[§17]: numeric-null-temporal.md#17-numeric-semantics-and-literals
[§17.2]: numeric-null-temporal.md#172-literals
[§17.4]: numeric-null-temporal.md#174-types-of-mixed-operands
[§17.5]: numeric-null-temporal.md#175-shared-values
[§17.6]: numeric-null-temporal.md#176-explicit-casts
[§18]: numeric-null-temporal.md#18-null-nan-and-boolean-logic
[§19]: numeric-null-temporal.md#19-temporal-semantics
[§20.2]: errors-and-performance.md#202-stages

<!-- /v0.5-modular:links -->
