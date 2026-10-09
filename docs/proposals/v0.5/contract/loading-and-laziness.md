<!-- v0.5-modular:header -->

# v0.5 contract: Loading and lazy evaluation (§4–§5)

[Index](../README.md) › Contract · Previous: [Exports and object model (§2–§3)](public-surface.md) · Next: [Schema, types and basic table operations (§6–§7)](schema-and-table-operations.md)

**Normative.** One module of the LTSeq v0.5 public API contract. It uses the BCP 14 key words as the contract's [status block](../README.md#ltseq-v05-public-api-contract) declares, and [§1.6] names the section that owns each cross-cutting rule.

**Scope.** How sources become tables (`read_csv`, `read_parquet`, `from_arrow`, `from_pandas`, `from_dict`, `from_rows`, `range`), and when work happens: plan-building against execution, `collect`, and the complete list of eager calls.

**Most cited from here.** [§6] Schema and types · [§17] Numeric semantics and literals · [§16] Output and interchange · [§9] Ordering contract

<!-- /v0.5-modular:header -->

## 4. Loading

Every constructor is a `classmethod` on `LTSeq` and returns an `LTSeq` with a **defined order** ([§9.1]) and `sort_keys is None`, except `LTSeq.range`, which also declares its sort key. Constructors open their sources when called: listing files, reading Parquet footers and inferring CSV types happen at the call, so a missing file or an unreadable header fails there and not at the first terminal.

### 4.1 Source paths

`read_csv` and `read_parquet` resolve `path` the same way:

- A path that names an existing regular file reads that file, even when its name contains glob characters.
- A path that names an existing directory reads the regular files directly inside it whose names end in `.csv` (for `read_csv`) or `.parquet` (for `read_parquet`). Files whose names start with `.` or `_` are skipped. Subdirectories are not read.
- Any other path containing `*`, `?` or `[` is a glob, expanded with the semantics of `glob.glob(path, recursive=True)`. An existing file or directory is never reinterpreted as a glob, so `report[2024].csv` reads that file and never `report2.csv`; `glob.escape` makes a pattern that matches such a name literally.
- A sequence of paths resolves each element as above and concatenates the results in sequence order.
- Within one directory or glob, files are read in lexicographic order of their full path, compared as strings by code point.
- A path, directory or glob that resolves to no file raises `SourceNotFoundError` **at the call**. Permission and I/O failures raise `LTSeqIOError` at the call.

The row order of the result is the file order above, then the order of rows within each file. The set of files is fixed at the call: a file that the directory or glob matches only later is not read, and a listed file that disappears fails execution ([§5.1]).

### 4.2 `LTSeq.read_csv`

```python
@classmethod
def read_csv(cls, path: PathLike | Sequence[PathLike], *, has_header: bool = True,
             schema: Mapping[str, DTypeLike] | pa.Schema | None = None,
             delimiter: str = ",") -> LTSeq
```

- **Behavior.** Reads UTF-8 CSV with RFC 4180 quoting (`"` quotes, `""` escapes a quote), `\n` or `\r\n` line endings. `delimiter` is one ASCII character.
- **Column names.** With `has_header=True`, from the header of the first file; every other file MUST have the same header (`SchemaMismatchError` at the call). With `has_header=False`, the names are `column_1`, `column_2`, … unless `schema` is a `pa.Schema`, whose names are used. Duplicate or empty header names raise `LTSeqValueError` at the call.
- **Types.** A `pa.Schema` gives every column's name and type; its field count MUST equal the column count. A mapping gives the type of the columns it names and the rest are inferred; a name that is not a column raises `ColumnNotFoundError` at the call. Inference reads the first 1,000 records of the first file at the call and assigns each column the first type in this list that parses every non-empty value seen, using the syntax of [§16.5](streaming-and-output.md#165-writers):
  1. `bool`: `true`/`false`, case-insensitive;
  2. `int64`: integer syntax, every value in range;
  3. `uint64`: integer syntax, every value in [0, `2**64`);
  4. `float64`: float syntax, where every value written in integer syntax has magnitude at most `2**53`;
  5. `date32`: `YYYY-MM-DD`;
  6. `timestamp[us]`: the naive timestamp text form ([§16.5]) with at most six fractional digits;
  7. `timestamp[us, tz=UTC]`: the same followed by `Z` or a numeric offset, converted to the UTC instant;
  8. `string`.

  So inference never rounds an integer: each integer-syntax value equals `int(text)` in the inferred type, or the call raises. Float syntax is rounded to the nearest `float64` as [§16.5] parses it, as Python's `float(text)` does. Where every sampled value is integer or float syntax but items 2–4 all fail, the call raises rather than fall through to `string`, naming the column and the first value that fails: `LTSeqValueError` when every value is integer syntax and no integer type holds them all (`2**64`, or `-1` with `2**63`), otherwise `LTSeqTypeError`, for an integer of magnitude above `2**53` next to a fraction or exponent (`9007199254740993` with `0.5`). Declare such a column, for example `schema={"h": pa.decimal128(38, 0)}` or `"string"`. Leading zeros count as integer syntax, so a column of ZIP codes such as `02134` is `int64` unless declared `string`. A column with no non-empty value in the sample is `string`. A declared type parses the text forms of [§16.5] for that type as `cast` from `string` does, so `read_csv(path, schema=t.schema)` reads back what `t.write_csv(path)` wrote, nanosecond timestamps included.
- **NULL.** An unquoted empty field is NULL in every column type. A quoted empty field (`""`) is the empty string in a string column and NULL in any other type.
- **Order and schema.** Defined order (file order, then line order). `sort_keys` is `None`.
- **Errors.** Path errors per [§4.1] at the call. A 0-byte file raises `LTSeqValueError` at the call, because it has no header with `has_header=True`, and otherwise no record to give the column count (a CSV file has no way to write a row without fields, so this is an unknown column count, not a table without columns, [§6.1]), unless `has_header=False` and `schema` is a `pa.Schema`, which gives a 0-row table with the schema's columns; a file with only a header gives a 0-row table whose columns are `string` unless `schema` says otherwise. A delimiter that is not one ASCII character raises `LTSeqValueError` at the call. During execution, a value that does not parse as its column's type, a row with the wrong number of fields, or invalid UTF-8 raises `CastError` naming the file, line and column. For an inferred column, "parses" means the inference rule above: an integer-syntax value of magnitude above `2**53` after the sample of a column inferred as `float64` raises. Inference never makes a later value NULL or truncated, and never rounds a later integer.
- **Example.** `trades = LTSeq.read_csv("data/trades-*.csv", schema={"qty": "int32"})`

### 4.3 `LTSeq.read_parquet`

```python
@classmethod
def read_parquet(cls, path: PathLike | Sequence[PathLike]) -> LTSeq
```

- **Behavior.** Reads Parquet files. The schema is read from the footers at the call and MUST equal what `pyarrow.parquet.read_schema` reports for the first file, after the normalization of [§6.2]. Every file MUST have the same column names and types (`SchemaMismatchError` at the call).
- **Order and schema.** Defined order: files in [§4.1] order, row groups in file order, rows in row-group order. `sort_keys` is `None` even when the file records `sorting_columns`; declare order with `assume_sorted` ([§9.5]), which validates it.
- **NULL and types.** Parquet nulls are NULL. Floating-point NaN stays NaN.
- **Errors.** Path errors per [§4.1] at the call. A file that is not valid Parquet raises `LTSeqIOError` at the call (footer) or at execution (data pages).
- **Example.** `quotes = LTSeq.read_parquet("data/quotes/")`

### 4.4 `LTSeq.from_arrow`

```python
@classmethod
def from_arrow(cls, data: object, /) -> LTSeq
```

- **Behavior.** Accepts tabular Arrow data: a `pyarrow.Table`, `pyarrow.RecordBatch` or `pyarrow.RecordBatchReader`, or any object implementing the Arrow PyCapsule interface (`__arrow_c_stream__`, else `__arrow_c_array__`) whose schema is a struct. The struct's fields are the columns, with their names, types and nullability; this is how the interface represents a record batch, so a `pyarrow.StructArray` or a struct-typed `ChunkedArray` is read as the table of its fields. A `Table` or `RecordBatch` is referenced without copying at the call. The conversions of [§6.2] (`float16`, `date64`, view and dictionary types) happen when the data is read and copy only the converted columns. A stream (a reader, or a PyCapsule stream from any producer, including another `LTSeq`) is read to the end **at the call**, because a stream can be consumed only once.
- **No fields.** A schema without fields gives a table without columns ([§6.1]) whose row count is the sum of the batches' lengths, which Arrow carries without any field: `pa.table({})` gives no columns and no rows, and `pa.table({"a": [1, 2, 3]}).select([])` no columns and 3 rows.
- **Order and schema.** Defined order: batch order, then row order. Schema normalized per [§6.2].
- **Errors.** At the call, except the last:
  - An object that implements neither protocol, or whose schema is not a struct, raises `LTSeqTypeError`. That includes a bare array (`pa.array([1, 2])`) and a `ChunkedArray` of a non-struct type; name the column instead: `LTSeq.from_arrow(pa.table({"x": arr}))`.
  - A struct array with a NULL at the top level raises `LTSeqValueError`, because a row cannot be NULL as a whole. pyarrow refuses the same import (`pa.table`, `pa.record_batch`).
  - An exception raised by a `pyarrow.RecordBatchReader` while it is read, including the reader of `t.to_batches()` ([§15.1]), propagates unchanged. A PyCapsule stream reports a failure only as an error code and a message ([§15.1]), so a stream that fails raises `LTSeqIOError` carrying the producer's message. To keep the class of another table's error, read `t.to_batches()` rather than `t`, or use `t.collect()`.
  - A `date64` value that `date32` cannot hold raises `CastError` during execution ([§6.2]).
- **Example.** `t = LTSeq.from_arrow(pa.table({"id": [1, 2], "v": [0.5, None]}))`

### 4.5 `LTSeq.from_pandas`

```python
@classmethod
def from_pandas(cls, df: pandas.DataFrame, /, *, preserve_index: bool = False) -> LTSeq
```

- **Behavior.** Converts with the semantics of `pyarrow.Table.from_pandas(df, preserve_index=preserve_index)`, except that an `object` column takes the type `from_dict` infers for its values ([§4.6]), after the values `isna()` reports become NULL. pyarrow's own inference loses data there: it reads a `date` with a `datetime` as `date32`, and a `timedelta` with an `int` as `duration[us]`, where the `int` counts microseconds. A frame without columns keeps the length of its index as its row count: `pd.DataFrame(index=range(3))` gives a table without columns and 3 rows ([§6.1]), where `pyarrow.Table.from_pandas` gives 0 rows (pyarrow 25.0.1), and `pd.DataFrame()` gives no columns and no rows. pandas is not a dependency of LTSeq; it is imported only when this method is called.
- **NULL.** Values pandas reports as missing through `isna()` become NULL. In particular, NaN in a NumPy-backed float column becomes NULL, because NaN is pandas' missing-value marker there.
- **Order and schema.** Defined order: row position. With `preserve_index=True` the index becomes leading columns, named as pyarrow names them.
- **Errors.** Column labels that are not `str`, or a `MultiIndex` on columns, raise `LTSeqValueError` at the call. An `object` column whose values [§4.6] gives no type raises the error [§4.6] gives, at the call.
- **Example.** `t = LTSeq.from_pandas(df)`

### 4.6 `LTSeq.from_dict` and `LTSeq.from_rows`

```python
@classmethod
def from_dict(cls, data: Mapping[str, Sequence[Any]], /, *,
              schema: Mapping[str, DTypeLike] | pa.Schema | None = None) -> LTSeq
@classmethod
def from_rows(cls, rows: Sequence[Mapping[str, Any]], /, *,
              schema: Mapping[str, DTypeLike] | pa.Schema | None = None) -> LTSeq
```

- **Behavior.** `from_dict` takes columns in mapping order; every sequence MUST have the same length. `from_rows` takes one mapping per row; the columns are the schema's columns if a `pa.Schema` is given, otherwise every key that appears in any row, in order of first appearance. A key missing from a row is NULL in that row.
- **Types.** A column without a declared type is inferred from its values that are not `None`, each of which has the literal type of [§17.2]. The inference is LTSeq's own and does not depend on `pyarrow.array`:
  - Values of one literal type give that type: a naive `datetime` column is `timestamp[us]`, a column of nanosecond `pandas.Timestamp`s is `timestamp[ns]`. A column of only `None` is `null`.
  - Python `int` values follow the integer rules of `read_csv` inference ([§4.2] items 2–4), so no inferred type rounds: integers are `int64`, else `uint64` when every value is in [0, `2**64`), and with a `float` they are `float64` only when every integer has magnitude at most `2**53` (`[1, 2.5]` is `float64`). A column of integers that no integer type holds together (`[2**64]`, `[-1, 2**63]`) raises `LTSeqValueError` at the call, and so does an `int` that neither `int64` nor `uint64` holds whatever its neighbours, because it has no literal type ([§17.2]): `[2**64, 0.5]`. An `int64` or `uint64` integer of magnitude above `2**53` next to a `float` (`[2**53 + 1, 0.5]`) raises `LTSeqTypeError`.
  - `Decimal` values of different sizes give the smallest `decimal128` that holds each exactly, with the most integer digits plus the largest scale: `[Decimal("1.5"), Decimal("10.25")]` is `decimal128(4, 2)`. More than 38 digits raise `LTSeqValueError`.
  - Every other mix raises `LTSeqTypeError` at the call: `[1, "a"]`, `[True, 1]`, `[1, Decimal("1.5")]`, a `date` with a `datetime`, naive with aware `datetime`s, aware values in different zones, `pandas.Timestamp`s of different units, `[timedelta(1), 1]`.
  - A `list`, `tuple` or `dict` value has no literal type, so a column that holds one needs a declared type (`LTSeqTypeError` otherwise).

  A declared type takes each value by exact compatible conversion ([§17.6]): within the value's kind, integers, decimals and floats counting as one kind, and only when the type holds the value **exactly**. A value's kind is its literal type's ([§17.2]), with two additions that have no literal type: a Python `int` of any size is a number, so `2**64` goes into `decimal128(20, 0)`, and a `list`, `tuple` or `dict` goes into a nested type, each element converting by this rule ([§17.6], step 2). A value of the type's kind that it cannot hold exactly (for example `1.5` into `int64`, which `pyarrow.array` would truncate to `1`, or `2**31` into `int32`) raises `CastError` at the call. A value of another kind raises `LTSeqTypeError` at the call, even where `cast` would convert it: `"2024-01-01"` into `date32`, `1` into `string`, `"1"` into `int64`, `True` into `int64` and `1` into `bool` all raise, because a declared type is not a parser or formatter. Give the value in the type's kind (`date(2024, 1, 1)`), or build the column and `cast` it. `None` is NULL whatever the declared type.
- **NULL.** `None` is NULL; `float("nan")` is NaN.
- **Order and schema.** Defined order: sequence order. A mapping `schema` may name a subset of columns; a name that is not a column raises `ColumnNotFoundError`. In `from_rows`, with a `pa.Schema`, a row key not in the schema raises `ColumnNotFoundError`.
- **No columns.** `from_rows` gives one row per mapping whatever its keys, so `from_rows([{}, {}])` is a table without columns and 2 rows ([§6.1]), and `from_rows([])` without a `pa.Schema` one without columns or rows; `from_rows([], schema=s)` is a 0-row table with `s`'s columns. `from_dict` gives a row count only through its columns' common length, so a mapping without columns does not say how many rows it has, and `from_dict({})` raises `LTSeqValueError` at the call rather than assume one; `from_rows` or `from_arrow` builds a table without columns and a given row count.
- **Errors.** Unequal column lengths, and `from_dict({})`, raise `LTSeqValueError`.
- **Example.** `t = LTSeq.from_rows([{"id": 1, "v": 2.0}, {"id": 2}])` has `v = [2.0, None]`.

### 4.7 `LTSeq.range`

```python
@classmethod
def range(cls, start: int, stop: int | None = None, step: int = 1, /, *, name: str = "value") -> LTSeq
```

- **Behavior.** The values of Python `range(start, stop, step)` (with `range(stop)` semantics when `stop` is omitted) in one `int64` column called `name`. Lazy: no values are produced before execution.
- **Order and schema.** Defined order, and `sort_keys == (SortKey(name, step < 0, True),)`.
- **Errors.** `step == 0` or a bound outside `int64` raises `LTSeqValueError` at the call.
- **Example.** `LTSeq.range(1, 4).to_dicts() == [{"value": 1}, {"value": 2}, {"value": 3}]`

---

## 5. Lazy evaluation and materialization

### 5.1 Plan-building and execution

A **plan-building** call returns an `LTSeq`, `NestedTable` or `GroupBy` and performs every check decidable from the schema and order state ([§20.2]). It reads no data. A **terminal** executes the plan and returns Python or Arrow data. An **eager** table-returning call executes some or all of the plan before it returns ([§5.3]).

Execution reads sources as they are at that moment: each execution reads the files the constructor listed ([§4.1]) afresh, Parquet footers and the row counts they record included, so `count()` never answers from an earlier execution's metadata. A source whose schema no longer matches the planned one raises `SchemaMismatchError` during execution; a listed file that disappeared raises `SourceNotFoundError`. Two terminals on one table are two executions and MAY observe different source contents.

### 5.2 `LTSeq.collect`

```python
def collect(self) -> LTSeq
```

- **Behavior.** Executes the plan and returns an `LTSeq` backed by the resulting in-memory batches. Later operations on the result do not re-read the original sources.
- **Order and schema.** Same schema, order state and `sort_keys` as `self`; the batches hold the rows in the defined order.
- **Errors.** Any execution error of the plan. `collect` demands what a terminal over `self` demands ([§20.2]), every value of every column, so it raises for a failing value that a later operation would have dropped: where `d` is 0, `t.derive(q=lambda r: r.n // r.d).drop("q").to_arrow()` succeeds and `t.derive(q=lambda r: r.n // r.d).collect().drop("q")` raises `DivisionByZeroError`. To leave such a value unevaluated, drop it before collecting.
- **Example.** `snapshot = t.filter(lambda r: r.ok).collect()`

### 5.3 Eager calls

This list is complete. Every other method that returns an `LTSeq`, `NestedTable` or `GroupBy` is plan-building.

| Call | What runs at the call | Why |
|---|---|---|
| `read_csv` | File listing; type inference over the first 1,000 records | The schema must be known at plan time ([§4.2]) |
| `read_parquet` | File listing; footers | Same |
| `from_arrow` of a stream | Reads the whole stream | Streams are single-use |
| `from_pandas`, `from_dict`, `from_rows` | Converts the Python data to Arrow | The input is already in memory |
| `partition` | Finds the distinct key values | The result is a `dict` keyed by them ([§14.5]) |
| `pivot` without `column_values` | Finds the distinct pivot values | They become output columns, which must be known at plan time ([§14.6]) |
| `fold` | Runs the Python callback over every row | Arbitrary Python cannot run inside the engine ([§10.7]) |
| `collect` | Executes the whole plan | It is the snapshot API ([§5.2]) |

Terminals: `count`, `len`, `is_sorted_by`, `to_arrow`, `to_pandas`, `to_dicts`, `to_batches`, `iter`, `__arrow_c_stream__`, `write_csv`, `write_parquet`, `show`, `pickle.dumps`, and `NestedTable.count`.

<!-- v0.5-modular:footer -->

---

Previous: [Exports and object model (§2–§3)](public-surface.md) · [Index](../README.md) · Next: [Schema, types and basic table operations (§6–§7)](schema-and-table-operations.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§1.6]: overview.md#16-rule-ownership
[§4.1]: #41-source-paths
[§4.2]: #42-ltseqread_csv
[§4.6]: #46-ltseqfrom_dict-and-ltseqfrom_rows
[§5.1]: #51-plan-building-and-execution
[§5.2]: #52-ltseqcollect
[§5.3]: #53-eager-calls
[§6]: schema-and-table-operations.md#6-schema-and-types
[§6.1]: schema-and-table-operations.md#61-schema-and-columns
[§6.2]: schema-and-table-operations.md#62-supported-types
[§9]: ordering.md#9-ordering-contract
[§9.1]: ordering.md#91-order-state
[§9.5]: ordering.md#95-assume_sorted
[§10.7]: windows-and-grouping.md#107-fold
[§14.5]: aggregation.md#145-partition
[§14.6]: aggregation.md#146-pivot
[§15.1]: streaming-and-output.md#151-to_batches
[§16]: streaming-and-output.md#16-output-and-interchange
[§16.5]: streaming-and-output.md#165-writers
[§17]: numeric-null-temporal.md#17-numeric-semantics-and-literals
[§17.2]: numeric-null-temporal.md#172-literals
[§17.6]: numeric-null-temporal.md#176-explicit-casts
[§20.2]: errors-and-performance.md#202-stages

<!-- /v0.5-modular:links -->
