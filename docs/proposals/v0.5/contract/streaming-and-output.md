<!-- v0.5-modular:header -->

# v0.5 contract: Streaming, output and interchange (§15–§16)

[Index](../README.md) › Contract · Previous: [Aggregation, partitioning and pivot (§14)](aggregation.md) · Next: [Numeric, NULL and temporal semantics (§17–§19)](numeric-null-temporal.md)

**Normative.** One module of the LTSeq v0.5 public API contract. It uses the BCP 14 key words as the contract's [status block](../README.md#ltseq-v05-public-api-contract) declares, and [§1.6] names the section that owns each cross-cutting rule.

**Scope.** Streaming with `to_batches` and iteration, then every way out of a table: `to_arrow`, `to_pandas`, `to_dicts`, the Arrow PyCapsule stream, the writers and pickle.

**Most cited from here.** [§4] Loading · [§14] Aggregation, partitioning and pivot · [§6] Schema and types · [§20] Errors

<!-- /v0.5-modular:header -->

## 15. Streaming

### 15.1 `to_batches`

```python
def to_batches(self, batch_size: int | None = None) -> pa.RecordBatchReader
```

- **Behavior.** Starts an execution and returns a reader over its result. Batches arrive in the defined order. Each call is a new execution. A reader can be read once.
- **Batches.** With `batch_size`, no batch has more than `batch_size` rows; batches MAY be smaller. No batch is empty unless every batch is, in which case the reader yields none. `batch_size` MUST be at least 1 (`LTSeqValueError`).
- **Schema.** `reader.schema.equals(t.schema)`.
- **Errors.** An execution error raises from the Python read that hits it (`read_next_batch`, iteration, `read_all`) as an LTSeq exception from the set `to_arrow` could raise ([§20.2]). The Arrow C stream interface carries an error code and a message but no exception class, so a library that imports the reader through it (pyarrow's `RecordBatchReader.from_stream(reader)`, DuckDB) raises its own exception, with a message that pyarrow composes and this contract does not fix. A consumer that takes a PyCapsule object gets LTSeq's own code and message by reading the table itself ([§16.4]).
- **Resources.** Closing the reader, or dropping the last reference to it, stops the execution and releases its resources.
- **Memory.** Over the pipelines [§21.1] requires to stream, a consumer that drops each batch after reading it holds memory bounded independently of the input size; one that keeps every batch holds the result too ([§21.1]).

`Cursor`, `scan` and `scan_parquet` are removed: readers are lazy ([§4]), so `read_parquet(p).to_batches()` is the streaming scan.

### 15.2 Iteration

`iter(t)` yields one `dict[str, Any]` per row in the defined order, converting values as `to_dicts` does ([§16.3]), and streams through `to_batches()`. Its errors are those of `to_dicts`, including the `CastError` of a value Python cannot hold, raised from the `next()` that reaches them.

---

## 16. Output and interchange

### 16.1 `to_arrow`

```python
def to_arrow(self) -> pa.Table
```

Executes and returns all rows in the defined order. `result.schema.equals(t.schema)`. The chunk layout is not specified.

### 16.2 `to_pandas`

```python
def to_pandas(self, *, dtype_backend: Literal["pyarrow", "numpy_nullable"] = "pyarrow") -> pandas.DataFrame
```

- **Behavior.** Executes and returns a DataFrame with a `RangeIndex` and the rows in the defined order. The column types are those `pandas.read_feather(path, dtype_backend=dtype_backend)` produces for a Feather file (Arrow IPC) holding `t.to_arrow()`, a format that keeps every Arrow type, so `timestamp[s]` stays in seconds. A column type that pandas cannot convert under the chosen backend raises `LTSeqTypeError` at the call. With `"numpy_nullable"`, dates, times and decimals become Python objects, so a value [§16.3] cannot convert in such a column (a date in year 10000, a time that is not a whole number of microseconds) raises its `CastError` during execution; the `"pyarrow"` backend holds every value.
- **Default.** `"pyarrow"` keeps every value: NULL is `pd.NA` and NaN stays NaN. With `"numpy_nullable"`, pandas turns NaN in float columns into `pd.NA`; the caller opts into that loss.
- pandas is imported only when this method is called; if it is missing, `ImportError`.

### 16.3 `to_dicts`

```python
def to_dicts(self) -> list[dict[str, Any]]
```

One `dict` per row, in the defined order, column order as the schema. Values follow `pyarrow`'s `to_pylist`, with these differences, so that the result never depends on whether pandas is installed and every value Python cannot hold raises the same error:

- NULL is `None`; NaN is `float("nan")`; decimals are `decimal.Decimal`; dates `datetime.date`; times `datetime.time`; durations `datetime.timedelta`; naive timestamps naive `datetime.datetime`; aware timestamps `datetime.datetime` with `tzinfo` set (`zoneinfo.ZoneInfo(name)` for IANA names).
- A nanosecond timestamp, time or duration whose value is not a whole number of microseconds raises `CastError` naming the column, because the Python type cannot hold it and pyarrow would return a pandas type instead. Use `to_arrow` for nanosecond values.
- A duration outside `datetime.timedelta`'s range, `timedelta.min` (−999,999,999 days) to `timedelta.max` (999,999,999 days, 23:59:59.999999), raises `CastError` naming the column, because `timedelta` cannot hold it (pyarrow raises `OverflowError`). Only `duration[s]` and `duration[ms]` reach that range.
- A date or timestamp outside the years 1 to 9999 raises `CastError` naming the column, because `datetime` cannot hold it (pyarrow raises `OverflowError`). An aware timestamp is outside when its instant in UTC or its wall clock in its zone is, so every aware value `to_dicts` returns also has a UTC form ([§14.5]).

These `CastError`s belong to the conversion to Python, so only the operations that make that conversion raise them: `to_dicts` and iteration ([§15.2]), `fold` for the rows it passes to `fn` ([§10.7]), `partition` for its keys ([§14.5]), `pivot` for its column names ([§14.6]), and `to_pandas` for the columns its backend holds as Python objects ([§16.2]). `to_arrow`, `to_batches` and the C stream return the same values without error. Each value that cannot be converted is one more failing demanded value ([§20.2]). Where `to_arrow()` raises, `to_dicts()` raises an error from the same set ([§20.2]) or one of these `CastError`s, and where `to_arrow()` succeeds, `to_dicts()` may still raise a `CastError`.

An aware value carries `fold=1` for the second occurrence of a repeated hour, so it denotes the instant LTSeq holds. Python compares two `datetime`s that share a `tzinfo` by wall clock, though, so the two occurrences compare equal and hash equally: compare or collect aware values from `to_dicts` after `.astimezone(timezone.utc)`. `partition` keys are in that form ([§14.5]).

### 16.4 Arrow PyCapsule stream

```python
def __arrow_c_stream__(self, requested_schema: object | None = None) -> object
```

- Exports the result as an Arrow C stream (the PyCapsule interface), with the semantics of `to_batches()` except for errors.
- **Errors in the stream.** The interface reports a failure as an error code and a message, so no exception class reaches the consumer. An execution error raised while the stream is read, or a `requested_schema` value that fails, ends the stream with the code `EIO` for `LTSeqIOError` and its subclasses and `EINVAL` for every other class, and with a message that starts with the class name: `"ArithmeticOverflowError: ..."`. The consumer raises its own exception: pyarrow raises `OSError` for `EIO` and `pyarrow.ArrowInvalid`, a `ValueError`, for `EINVAL`, with that message.
- `requested_schema`, when given, MUST have the same field names in the same order (`SchemaMismatchError` at the call). Fields whose type differs are converted by exact compatible conversion ([§17.6]): within a kind, nested types included, among integers, decimals and floats, and from `null`, whose values become NULLs of the requested type, keeping every value exactly, so a value that a rule would round or truncate fails. A requested type of another kind, such as `string` for an `int64` column, `date32` for a `string` column, `int8` for a `bool` column or `timestamp` for a `date32` column, raises `LTSeqTypeError` at the call, as does a pair with no rule: `requested_schema` does not parse or format, and a consumer that wants another kind reads a table converted with `cast`. A value that fails ends the stream with a `CastError` message. The stream's schema has `requested_schema`'s names and types, delivered as asked ([§6.2]), with the table's nullability ([§6.1]) and no metadata: the nullability and metadata that `requested_schema` declares are not checked or carried. A consumer may check them itself: `pa.table(t, schema=s)` casts what it reads to `s` and raises `ValueError` for a NULL in a field `s` marks non-nullable, while `RecordBatchReader.from_stream(t, schema=s)` returns the stream's schema.
- Any consumer of the PyCapsule interface reads an `LTSeq` through it without converting values in Python: `pyarrow.table(t)`, `pyarrow.RecordBatchReader.from_stream(t)`, `polars.from_dataframe(t)`. Consumers that take a `pyarrow.RecordBatchReader`, such as DuckDB's Python client, take `t.to_batches()` ([§23.10]).

### 16.5 Writers

```python
def write_csv(self, path: PathLike, *, include_header: bool = True, delimiter: str = ",") -> None
def write_parquet(self, path: PathLike, *,
                  compression: Literal["zstd", "snappy", "gzip", "lz4", "none"] = "zstd") -> None
```

Both execute, write one file at `path`, and are atomic: the data goes to a temporary file in the same directory, renamed over `path` only after a successful write. On any failure, `path` is left as it was. A missing parent directory, or a `path` that is a directory, raises `LTSeqIOError`. A table with 0 rows writes a valid file: a CSV with only the header (no bytes when `include_header=False`), or a Parquet file with the schema and no rows. A table without columns ([§6.1]) raises `LTSeqValueError` at the call, decided from the schema whatever the row count. CSV has no way to write a row without fields, so its rows could not be read back. The Parquet format does record a row count, but the writers available today write such a table as a file of 0 rows (parquet 59.2.0 counts a row group's rows from its columns; pyarrow 25.0.1 does the same), so writing it would lose its rows silently. Lifting the Parquet refusal once a writer records the count would turn an error into a result.

**Text forms.** Each type with a text form has exactly one canonical form, used by `write_csv`, by `cast` to `string`, and (with the variations listed) by `cast` from `string` and by `read_csv` with a declared type:

| Type | Canonical form | Also parsed |
|---|---|---|
| `bool` | `true`, `false` | Any letter case |
| Integers | Optional `-`, then decimal digits without leading zeros | A leading `+` or leading zeros |
| Floats | The shortest decimal form that reads back to the same value of the column's type (Python's `repr` for `float64`): `1.0`, `1e+16`, `nan`, `inf`, `-inf` | Float syntax, rounded to the nearest value of the type |
| Decimals | Optional `-`, digits, and exactly *s* digits after a `.` when the scale *s* is positive; never an exponent: `0.0000000`, `-1.50` | Fewer fractional digits; more than *s* fail |
| `string` | The value | |
| `date32` | `YYYY-MM-DD` | |
| `time32`, `time64` | `HH:MM:SS`, then `.` and exactly 3, 6 or 9 digits for `ms`, `us`, `ns` | Fewer fractional digits; more than the unit's fail |
| Naive `timestamp` | `YYYY-MM-DDTHH:MM:SS`, then the unit's fractional digits as for time | A space instead of `T`; fewer fractional digits |
| Aware `timestamp` | The UTC instant in the naive form, then `Z` | A numeric offset instead of `Z`; a form without either fails |
| `duration` | Optional `-`, then the integer count of the unit: 1.5 seconds in `duration[ms]` is `1500` | A leading `+` |

*Integer syntax* is an optional `+` or `-` followed by one or more ASCII digits, leading zeros allowed. *Float syntax* is an optional sign, then digits containing at most one `.` (at least one digit in all), then an optional exponent (`e` or `E`, an optional sign, digits); or `nan`, `inf` or `infinity` with an optional sign, in any letter case. Neither allows the surrounding whitespace or the `_` separators that Python's `int()` and `float()` accept, so ` 5` and `1_000` are strings to inference ([§4.2]) and fail as numbers. A date or timestamp outside the years 1 to 9999 has no text form and fails. `binary` and the pass-through types have none either: `cast` between them and `string` raises `LTSeqTypeError` at plan time.

**CSV format.** RFC 4180, UTF-8, `\n` line endings. A field is quoted when it contains the delimiter, a quote, `\r` or `\n`, and the empty string is written as `""`, so that NULL (an empty unquoted field) and `""` read back differently under [§4.2]. Values are written in their canonical text form.

A `binary` or pass-through column raises `LTSeqTypeError` at the call; a date or timestamp outside the years 1 to 9999 raises `CastError` during execution. For every other column, `LTSeq.read_csv(path, schema=t.schema)` MUST return values equal to `t`'s, with aware timestamps equal as instants.

**Parquet.** One file. Rows are written in the defined order. When `sort_keys` is not `None`, the row groups record them as `sorting_columns`. `read_parquet` does not trust that record ([§4.3]). A column whose type is or contains a `union`, an `interval`, a decimal with a negative scale, a `struct` with no fields or a `fixed_size_binary` of width 0, at any depth of nesting, raises `LTSeqTypeError` at the call, also when `t` has 0 rows: Parquet has no type that holds all their values, and pyarrow writes no file with a field-less group or a zero-length fixed binary. The file stores the Arrow schema, as pyarrow does by default, so that a reader recovers the types Parquet has no logical type for, such as `duration`, `large_string` and a zone name. Reading the file back gives `t`'s values in order and `t.schema`, except that a `timestamp` or `time32` in seconds, also nested, reads back in milliseconds: Parquet has no second unit, and `read_parquet` reports the schema pyarrow reads ([§4.3]). The writer converts these values as `cast` to milliseconds does ([§17.6]), so a `timestamp` value beyond ±9223372036854775 seconds, about 292 million years from 1970, raises `CastError` during execution. A `time32` value is less than a day and always converts.

### 16.6 Pickle

`pickle.dumps(t)` executes `t` as `t.collect()` does, demanding and raising what `collect` does ([§5.2]), and stores its rows (as an Arrow IPC stream), its order state and its `sort_keys`. `pickle.loads` returns an in-memory `LTSeq` equal to `t.collect()`, so a pickled table can cross process boundaries (for example to `multiprocessing` workers) and never re-reads `t`'s sources. Payloads are readable by the same LTSeq minor version; another version raises `LTSeqValueError` when loading. `NestedTable`, `GroupBy` and `Expr` raise `TypeError` when pickled.

<!-- v0.5-modular:footer -->

---

Previous: [Aggregation, partitioning and pivot (§14)](aggregation.md) · [Index](../README.md) · Next: [Numeric, NULL and temporal semantics (§17–§19)](numeric-null-temporal.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§1.6]: overview.md#16-rule-ownership
[§4]: loading-and-laziness.md#4-loading
[§4.2]: loading-and-laziness.md#42-ltseqread_csv
[§4.3]: loading-and-laziness.md#43-ltseqread_parquet
[§5.2]: loading-and-laziness.md#52-ltseqcollect
[§6]: schema-and-table-operations.md#6-schema-and-types
[§6.1]: schema-and-table-operations.md#61-schema-and-columns
[§6.2]: schema-and-table-operations.md#62-supported-types
[§10.7]: windows-and-grouping.md#107-fold
[§14]: aggregation.md#14-aggregation-partitioning-and-pivot
[§14.5]: aggregation.md#145-partition
[§14.6]: aggregation.md#146-pivot
[§15.2]: #152-iteration
[§16.2]: #162-to_pandas
[§16.3]: #163-to_dicts
[§16.4]: #164-arrow-pycapsule-stream
[§17.6]: numeric-null-temporal.md#176-explicit-casts
[§20]: errors-and-performance.md#20-errors
[§20.2]: errors-and-performance.md#202-stages
[§21.1]: errors-and-performance.md#211-materialization
[§23.10]: examples-semantics.md#2310-streaming-and-interchange

<!-- /v0.5-modular:links -->
