# LTSeq API Design Document

Related documents:

- `docs/README.md`: documentation index
- `docs/USER_MODEL.md`: user mental model and usage guidance
- `docs/ARCHITECTURE.md`: system architecture and execution model
- `docs/MODULE_GUIDE.md`: contributor-oriented codebase tour
- `docs/DESIGN_SUMMARY.md`: current architecture summary and design archive
- `docs/LINKING_GUIDE.md`: focused guide for pointer-based linking

LTSeq is an ordered-sequence data processing library for Python backed by Rust/DataFusion. Unlike traditional DataFrames, LTSeq emphasizes order semantics and provides SPL-style capabilities such as window functions, ordered grouping, and cursor-based streaming.

This document describes the API **as currently implemented**. Every signature below is verified against the source in `py-ltseq/ltseq/`.

## 0. Conventions and Terms

- t: current LTSeq instance (immutable; all operations return a new instance)
- r: row proxy used inside lambdas to build expressions (not executed in Python)
- g: group proxy used in NestedTable filter/derive; group aggregations use string column names (`g.sum("amount")`), while `g.first()`/`g.last()` return row proxies with attribute access (`g.first().date`)
- Most operations return a new LTSeq; `LTSeq.scan()`/`scan_parquet()` return a streaming `Cursor`; `is_subset`/`contain` return a bool
- Window/ordered operations require a prior `sort` (or `assume_sorted`); otherwise runtime errors or incorrect results may occur. `shift`/`rolling`/`diff` also accept a `partition_by=` kwarg for per-group windows
- Expressions are captured into AST on the Python side and executed in Rust/DataFusion

## Common Errors and Solutions

| Error | Cause | Solution |
|-------|-------|----------|
| `RuntimeError: window function used without sort` | Called `shift`/`rolling`/`diff` without prior `.sort()` | Add `.sort(order_column)` (or `.assume_sorted(...)`) before window operations |
| `AttributeError: column 'xxx' not found` | Typo in column name or column doesn't exist | Check `t.columns` for available column names |
| `ValueError: schema mismatch` | Union/intersect with incompatible tables | Ensure both tables have same column names and types |
| `ValueError: merge strategy requires sorted tables` | `join(..., strategy="merge")` called on unsorted tables | Call `.sort(join_key)` on both tables first |
| `TypeError: predicate not boolean Expr` | Filter lambda returns non-boolean | Ensure predicate uses comparison operators (`>`, `==`, etc.) |
| `ValueError: desc length mismatch` | `desc` list length doesn't match number of sort keys | Provide one bool per sort key, or use single bool for all |
| `ValueError: Schema not initialized` | Operation called on an empty `LTSeq()` | Load data first (`read_csv`, `from_pandas`, ...) |

## Quick Reference

### Data Loading & Output
| Operation | Method | Example |
|-----------|--------|---------|
| Load CSV | `LTSeq.read_csv()` | `t = LTSeq.read_csv("data.csv")` |
| Load Parquet | `LTSeq.read_parquet()` | `t = LTSeq.read_parquet("data.parquet")` |
| Stream large file | `LTSeq.scan()` | `for batch in LTSeq.scan("big.csv"): ...` |
| From Python data | `LTSeq.from_dict()` | `t = LTSeq.from_dict({"id": [1, 2]})` |
| View schema | `.columns` / `.schema` | `print(t.columns)` |
| Rows as dicts | `.to_dicts()` | `rows = t.to_dicts()` |
| Materialize plan | `.collect()` | `result = t.collect()` |
| To pandas | `.to_pandas()` | `df = t.to_pandas()` |
| Write file | `.write_csv()` / `.write_parquet()` | `t.write_csv("out.csv")` |
| Row count | `.count()` / `len(t)` | `n = t.count()` |
| Pretty print | `.show()` | `t.show(20)` |

### Basic Operations
| Operation | Method | Example |
|-----------|--------|---------|
| Filter rows | `.filter()` | `t.filter(lambda r: r.age > 18)` |
| Select columns | `.select()` | `t.select("id", "name")` |
| Add columns | `.derive()` (alias `.with_columns()`) | `t.derive(total=lambda r: r.a + r.b)` |
| Rename columns | `.rename()` | `t.rename(old="new")` |
| Drop columns | `.drop()` | `t.drop("tmp")` |
| Sort | `.sort()` | `t.sort("date", desc=True)` |
| Deduplicate | `.distinct()` | `t.distinct("id")` |
| Slice rows | `.slice()` | `t.slice(offset=10, length=5)` |
| First N rows | `.head()` | `t.head(10)` |
| Last N rows | `.tail()` | `t.tail(10)` |

### Window Operations (require `.sort()` first; `partition_by=` for per-group)
| Operation | Method | Example |
|-----------|--------|---------|
| Previous row | `.shift(n)` | `r.price.shift(1)` |
| Rolling agg | `.rolling(n).agg()` | `r.price.rolling(5).mean()` |
| Row difference | `.diff(n)` | `r.price.diff(1)` |
| Percent change | `.pct_change()` | `r.close.pct_change()` |
| Cumulative sum | `.cum_sum()` | `t.cum_sum("volume")` or `r.volume.cum_sum()` |

### Ranking (use `.over()`)
| Operation | Method | Example |
|-----------|--------|---------|
| Row number | `row_number()` | `row_number().over(order_by=r.date)` |
| Rank with gaps | `rank()` | `rank().over(order_by=r.score)` |
| Dense rank | `dense_rank()` | `dense_rank().over(order_by=r.score)` |
| Buckets | `ntile(n)` | `ntile(4).over(order_by=r.value)` |

### Aggregation
| Operation | Method | Example |
|-----------|--------|---------|
| Group aggregate (chain) | `.group_by().agg()` | `t.group_by("region").agg(total=lambda g: g.sales.sum())` |
| Group aggregate | `.agg()` | `t.agg(by=lambda r: r.region, total=lambda g: g.sales.sum())` |
| Partition | `.partition()` | `parts = t.partition("region")` |
| Pivot | `.pivot()` | `t.pivot(index="date", columns="region", values="amount")` |

### Joins
| Operation | Method | Example |
|-----------|--------|---------|
| Hash join | `.join()` | `a.join(b, on=lambda a, b: a.id == b.id)` |
| Merge join | `.join(..., strategy="merge")` | `a.sort("id").join(b.sort("id"), on=lambda a, b: a.id == b.id, strategy="merge")` |
| As-of join | `.asof_join()` | `trades.asof_join(quotes, on=lambda t, q: t.time >= q.time)` |
| Semi join | `.semi_join()` | `a.semi_join(b, on=lambda a, b: a.id == b.id)` |
| Anti join | `.anti_join()` | `a.anti_join(b, on=lambda a, b: a.id == b.id)` |
| Pointer link | `.link()` | `orders.link(products, on=lambda o, p: o.product_id == p.id, alias="prod")` |

### Set Operations
| Operation | Method | Example |
|-----------|--------|---------|
| Concat (union all) | `.concat()` / `.union()` | `t1.concat(t2)` |
| Intersect | `.intersect()` | `t1.intersect(t2)` |
| Diff | `.subtract()` / `.except_()` | `t1.subtract(t2)` |
| Symmetric diff | `.xunion()` | `t1.xunion(t2)` |

### Row Mutation (copy-on-write)
| Operation | Method | Example |
|-----------|--------|---------|
| Insert row | `.insert()` | `t.insert(0, {"id": 99})` |
| Delete rows | `.delete()` | `t.delete(lambda r: r.expired)` |
| Conditional update | `.update()` | `t.update(lambda r: r.age > 65, discount=0.2)` |
| Modify one row | `.modify()` | `t.modify(0, status="active")` |

## 1. Input / Output

### `LTSeq.read_csv`
- **Signature**: `LTSeq.read_csv(path: str, has_header: bool = True) -> LTSeq`
- **Behavior**: Load a CSV file and infer schema; returns a chainable LTSeq. When `has_header=False`, columns are named `column_0`, `column_1`, ...
- **Parameters**: `path` CSV path; `has_header` whether the first row is a header
- **Returns**: `LTSeq` with loaded data and schema
- **Exceptions**: `FileNotFoundError` (invalid path), `ValueError` (parse or schema inference failure)
- **Example**:
```python
from ltseq import LTSeq

t = LTSeq.read_csv("data.csv")
t2 = LTSeq.read_csv("raw.csv", has_header=False)
```

### `LTSeq.read_parquet`
- **Signature**: `LTSeq.read_parquet(path: str) -> LTSeq`
- **Behavior**: Load a Parquet file and return an LTSeq table
- **Parameters**: `path` Parquet path
- **Returns**: `LTSeq` with loaded data and schema
- **Exceptions**: `FileNotFoundError` (invalid path), `RuntimeError` (parse failure)
- **Example**:
```python
t = LTSeq.read_parquet("data.parquet")
```

### `LTSeq.scan` / `LTSeq.scan_parquet` (streaming)
- **Signature**: `LTSeq.scan(path: str, has_header: bool = True) -> Cursor`; `LTSeq.scan_parquet(path: str) -> Cursor`
- **Behavior**: Create a streaming cursor over a CSV/Parquet file for lazy batch iteration without loading the whole file into memory
- **Parameters**: `path` file path; `has_header` (CSV only) whether the first row is a header
- **Returns**: `Cursor` (iterable of record batches)
- **Exceptions**: `FileNotFoundError` (invalid path), `RuntimeError` (scan failure)
- **Example**:
```python
for batch in LTSeq.scan("large.csv"):
    process(batch)

cursor = LTSeq.scan_parquet("large.parquet")
```

### `Cursor` (streaming handle)
- **Signature**: iterable object returned by `scan`/`scan_parquet`
- **Behavior**: Lazily yields record batches; also supports whole-stream materialization
- **Members**:
  - `__iter__()`: iterate over batches
  - `schema -> dict[str, str]` (property), `columns -> list[str]` (property)
  - `source -> str` (property): source file path; `exhausted -> bool` (property)
  - `to_pandas()`, `to_arrow()`: materialize the remaining stream
  - `count() -> int`: consume the stream and count rows
- **Example**:
```python
cursor = LTSeq.scan("large.csv")
print(cursor.columns)
for batch in cursor:
    ...
```

### `LTSeq.from_rows`
- **Signature**: `LTSeq.from_rows(rows: list[dict[str, Any]], schema: dict[str, str] | None = None) -> LTSeq`
- **Behavior**: Build a table from row dictionaries. Types are inferred from the first row unless `schema` is given; an explicit schema is required when `rows` is empty
- **Parameters**: `rows` list of dicts (same keys per row); `schema` optional `{column: arrow_type}` mapping
- **Returns**: new `LTSeq`
- **Exceptions**: `ValueError` (empty rows without schema), `TypeError` (not a list of dicts)
- **Example**:
```python
t = LTSeq.from_rows([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
empty = LTSeq.from_rows([], schema={"id": "Int64", "name": "Utf8"})
```

### `LTSeq.from_dict`
- **Signature**: `LTSeq.from_dict(data: dict[str, list[Any]]) -> LTSeq`
- **Behavior**: Build a table from a column-oriented dictionary
- **Parameters**: `data` mapping of column name to list of values (equal lengths)
- **Returns**: new `LTSeq`
- **Exceptions**: `ValueError` (unequal column lengths), `TypeError` (not a dict)
- **Example**:
```python
t = LTSeq.from_dict({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
```

### `LTSeq.from_pandas` / `LTSeq.from_arrow`
- **Signature**: `LTSeq.from_pandas(df) -> LTSeq`; `LTSeq.from_arrow(arrow_table) -> LTSeq`
- **Behavior**: Build a table from a pandas DataFrame / PyArrow Table
- **Returns**: new `LTSeq`
- **Exceptions**: `ImportError` (pandas/pyarrow not installed), `TypeError` (wrong input type)
- **Example**:
```python
t = LTSeq.from_pandas(df)
t = LTSeq.from_arrow(arrow_table)
```

### `seq` (integer sequence constructor)
- **Signature**: `seq(start_or_stop: int, stop: int | None = None, step: int = 1) -> LTSeq`
- **Behavior**: Generate an integer sequence as a single-column LTSeq (column named `value`, Int64). Mirrors Python's built-in `range()`
- **Import**: `from ltseq import seq`
- **Example**:
```python
from ltseq import seq

seq(5)         # 0, 1, 2, 3, 4
seq(2, 7)      # 2, 3, 4, 5, 6
seq(0, 10, 2)  # 0, 2, 4, 6, 8
```

### `LTSeq.write_csv` / `LTSeq.write_parquet`
- **Signature**: `LTSeq.write_csv(path: str) -> None`; `LTSeq.write_parquet(path: str, compression: str | None = None) -> None`
- **Behavior**: Write the table to a CSV/Parquet file. Parquet `compression` is one of `"snappy" | "zstd" | "gzip" | "lz4" | "none"` (default uncompressed)
- **Exceptions**: `RuntimeError` (write failure), `ValueError` (unknown compression)
- **Example**:
```python
t.write_csv("output.csv")
t.write_parquet("output.parquet", compression="zstd")
```

### `LTSeq.schema` (property)
- **Signature**: `LTSeq.schema -> dict[str, str]`
- **Behavior**: Return the table schema as a dictionary mapping column names to Arrow type strings
- **Example**:
```python
print(t.schema)   # {"id": "Int64", "name": "Utf8", ...}

for name, dtype in t.schema.items():
    print(f"{name}: {dtype}")
```

### `LTSeq.python_schema` (property)
- **Signature**: `LTSeq.python_schema -> dict[str, str]`
- **Behavior**: Return the schema with Python-friendly type names (`Int64` → `int`, `Utf8` → `str`, `Float64` → `float`, `Boolean` → `bool`, ...). Unknown types are returned as-is
- **Example**:
```python
print(t.python_schema)  # {"id": "int", "name": "str", "score": "float"}
```

### `LTSeq.columns` (property)
- **Signature**: `LTSeq.columns -> list[str]`
- **Behavior**: Return list of column names (shortcut for `list(schema.keys())`)
- **Example**:
```python
print(t.columns)  # ["id", "name", "age"]
```

### `LTSeq.dtypes` (property)
- **Signature**: `LTSeq.dtypes -> list[tuple[str, str]]`
- **Behavior**: Return list of `(column_name, data_type)` tuples
- **Example**:
```python
print(t.dtypes)  # [("id", "Int64"), ("name", "Utf8"), ...]
```

### `LTSeq.collect`
- **Signature**: `LTSeq.collect() -> LTSeq`
- **Behavior**: Materialize the lazy plan and return a new in-memory `LTSeq` (same semantics as Polars/PySpark `collect()`). The pending pipeline executes once; downstream operations reuse the computed data instead of re-executing the plan. Row order, schema, and sort metadata are preserved, and the result remains chainable. To get rows as dictionaries, use `to_dicts()`.
- **Parameters**: none
- **Returns**: new `LTSeq` backed by the materialized in-memory data
- **Exceptions**: `MemoryError` (dataset too large), `RuntimeError` (execution failure)
- **Example**:
```python
result = t.filter(lambda r: r.age > 18).collect()  # runs the plan once
result.count()
rows = result.to_dicts()
```

### `LTSeq.to_dicts`
- **Signature**: `LTSeq.to_dicts() -> list[dict[str, Any]]`
- **Behavior**: Materialize all rows as a list of dictionaries (same name and semantics as Polars `to_dicts()`)
- **Parameters**: none
- **Returns**: list of row dictionaries
- **Exceptions**: `MemoryError` (dataset too large), `RuntimeError` (execution failure)
- **Example**:
```python
rows = t.filter(lambda r: r.age > 18).to_dicts()
for row in rows:
    print(row["name"])
```

### `LTSeq.to_pandas` / `LTSeq.to_arrow`
- **Signature**: `LTSeq.to_pandas() -> pandas.DataFrame`; `LTSeq.to_arrow() -> pyarrow.Table`
- **Behavior**: Materialize the table as a pandas DataFrame / PyArrow Table
- **Exceptions**: `ImportError` (pandas/pyarrow not installed), `MemoryError` (dataset too large)
- **Example**:
```python
df = t.to_pandas()
df.plot(x="date", y="price")

arrow_table = t.to_arrow()
```

### `LTSeq.count` / `len(t)`
- **Signature**: `LTSeq.count() -> int`; `LTSeq.__len__() -> int`
- **Behavior**: Return the number of rows; `count()` is equivalent to `len(t)`
- **Exceptions**: `RuntimeError` (execution failure)
- **Example**:
```python
n = t.filter(lambda r: r.status == "active").count()
n = len(t)
```

### `LTSeq.show`
- **Signature**: `LTSeq.show(n: int = 10) -> LTSeq`
- **Behavior**: Pretty-print up to `n` rows as an ASCII table; returns `self` for chaining. The REPL `repr` of an LTSeq also shows dimensions, schema, and a 5-row preview
- **Example**:
```python
t.show()
t.filter(lambda r: r.active).show().derive(upper=lambda r: r.name.s.upper())
```

### `LTSeq.head`
- **Signature**: `LTSeq.head(n: int = 10) -> LTSeq`
- **Behavior**: Return the first n rows
- **Exceptions**: `ValueError` (n < 0)
- **Example**:
```python
top_10 = t.sort("score", desc=True).head(10)
```

### `LTSeq.tail`
- **Signature**: `LTSeq.tail(n: int = 10) -> LTSeq`
- **Behavior**: Return the last n rows
- **Exceptions**: `ValueError` (n < 0)
- **Example**:
```python
recent = t.sort("date").tail(5)
```

### `LTSeq.pipe`
- **Signature**: `LTSeq.pipe(func: Callable[..., LTSeq], *args, **kwargs) -> LTSeq`
- **Behavior**: Apply a user function to the table inside a method chain (same as pandas `DataFrame.pipe`)
- **Example**:
```python
def add_tax(t, rate):
    return t.derive(tax=lambda r: r.price * rate)

result = t.filter(lambda r: r.qty > 0).pipe(add_tax, 0.1).sort("tax")
```

### `LTSeq.explain_plan`
- **Signature**: `LTSeq.explain_plan() -> tuple[str, str]`
- **Behavior**: Return the optimized logical and physical DataFusion plans for debugging
- **Example**:
```python
logical, physical = t.filter(lambda r: r.age > 18).explain_plan()
print(logical)
```

## 2. Basic Relational Operations

### `LTSeq.filter`
- **Signature**: `LTSeq.filter(predicate: Callable[[Row], Expr]) -> LTSeq`
- **Behavior**: Filter rows matching the predicate; pushed down to the Rust engine
- **Parameters**: `predicate` row predicate expression (returns boolean Expr)
- **Returns**: filtered `LTSeq`
- **Exceptions**: `ValueError` (schema not initialized), `TypeError` (predicate not boolean Expr), `AttributeError` (column not found)
- **Example**:
```python
filtered = t.filter(lambda r: r.amount > 100)
```

### `LTSeq.select`
- **Signature**: `LTSeq.select(*cols: str | Callable) -> LTSeq`
- **Behavior**: Project specified columns or expressions; supports column pruning
- **Parameters**: `cols` column names or lambdas (single expr or list)
- **Returns**: projected `LTSeq`
- **Exceptions**: `ValueError` (schema not initialized), `TypeError` (invalid return type), `AttributeError` (column not found)
- **Example**:
```python
t.select("id", "name")
# or
t.select(lambda r: [r.id, r.name])
```

### `LTSeq.derive` (alias: `with_columns`)
- **Signature**: `LTSeq.derive(**new_cols: Callable) -> LTSeq` or `LTSeq.derive(func: Callable[[Row], dict[str, Expr]]) -> LTSeq`
- **Behavior**: Add or overwrite columns; keeps existing columns. `with_columns` is an alias for Polars users
- **Parameters**: `new_cols` mapping of column name to lambda; or a lambda that returns a dict
- **Returns**: new `LTSeq` with derived columns
- **Exceptions**: `ValueError` (schema not initialized), `TypeError` (invalid return type), `AttributeError` (column not found)
- **Example**:
```python
# Style 1
with_tax = t.derive(tax=lambda r: r.price * 0.1)
# Style 2
with_tax = t.derive(lambda r: {"tax": r.price * 0.1})
# Polars-style alias
with_tax = t.with_columns(tax=lambda r: r.price * 0.1)
```

### `LTSeq.rename`
- **Signature**: `LTSeq.rename(mapping: dict[str, str] | None = None, **kwargs: str) -> LTSeq`
- **Behavior**: Rename columns via a dict or keyword arguments (`old_name=new_name`)
- **Returns**: new `LTSeq` with renamed columns
- **Exceptions**: `AttributeError` (column not found), `ValueError` (schema not initialized)
- **Example**:
```python
t.rename({"old_name": "new_name"})
t.rename(price="unit_price", qty="quantity")
```

### `LTSeq.drop`
- **Signature**: `LTSeq.drop(*cols: str) -> LTSeq`
- **Behavior**: Remove the given columns, keeping the rest
- **Exceptions**: `AttributeError` (column not found), `ValueError` (schema not initialized)
- **Example**:
```python
t.drop("tmp", "debug_flag")
```

### `LTSeq.sort`
- **Signature**: `LTSeq.sort(*keys: str | Callable, desc: bool | list[bool] = False, descending: bool | list[bool] | None = None) -> LTSeq`
- **Behavior**: Sort by one or more keys; required for window/ordered computing. Also populates `sort_keys` for sort order tracking. `descending` is an alias for `desc` (Polars naming) and takes precedence when both are given
- **Parameters**: `keys` column names or expressions; `desc`/`descending` global or per-key descending flags
- **Returns**: sorted `LTSeq` with tracked sort keys
- **Exceptions**: `ValueError` (schema not initialized or desc length mismatch), `TypeError` (invalid key type), `AttributeError` (column not found)
- **Example**:
```python
t_sorted = t.sort("date", "id", desc=[False, True])
print(t_sorted.sort_keys)  # [("date", False), ("id", True)]

t.sort("price", descending=True)  # Polars-style alias
```

### `LTSeq.sort_keys` (property)
- **Signature**: `LTSeq.sort_keys -> list[tuple[str, bool]] | None`
- **Behavior**: Returns the current sort keys as a list of (column_name, is_descending) tuples, or None if sort order is unknown
- **Example**:
```python
t = LTSeq.read_csv("data.csv")
print(t.sort_keys)  # None (unknown sort order)

t_sorted = t.sort("date", "id")
print(t_sorted.sort_keys)  # [("date", False), ("id", False)]

t_desc = t.sort("price", desc=True)
print(t_desc.sort_keys)  # [("price", True)]
```

### `LTSeq.is_sorted_by`
- **Signature**: `LTSeq.is_sorted_by(*keys: str, desc: bool | list[bool] = False) -> bool`
- **Behavior**: Check if the table is sorted by the given keys (uses prefix matching)
- **Parameters**: `keys` column names to check; `desc` expected descending flags (single bool or per-key list)
- **Returns**: `bool` - True if sorted by the given keys (as a prefix), False otherwise
- **Exceptions**: `ValueError` (no keys provided or desc length mismatch)
- **Example**:
```python
t_sorted = t.sort("a", "b", "c")
t_sorted.is_sorted_by("a")          # True (prefix match)
t_sorted.is_sorted_by("a", "b")     # True (prefix match)
t_sorted.is_sorted_by("a", "b", "c") # True (exact match)
t_sorted.is_sorted_by("b")          # False (not a prefix)
t_sorted.is_sorted_by("a", desc=True)  # False (direction mismatch)
```

### `LTSeq.assume_sorted`
- **Signature**: `LTSeq.assume_sorted(*keys: str, desc: bool | list[bool] = False) -> LTSeq`
- **Behavior**: Declare that data is already sorted by the given keys **without physically sorting**. Enables window functions and merge joins on pre-sorted data (e.g., pre-sorted Parquet) while skipping the sort. The caller is responsible for correctness — wrong metadata produces wrong results
- **Parameters**: `keys` column names declaring the sort order; `desc` descending flag(s)
- **Returns**: `LTSeq` with sort metadata set (same underlying data)
- **Exceptions**: `ValueError` (schema not initialized or desc length mismatch), `TypeError` (invalid desc type)
- **Example**:
```python
t = LTSeq.read_parquet("presorted.parquet").assume_sorted("userid", "eventtime")
t.is_sorted_by("userid")  # True
```

### `LTSeq.distinct`
- **Signature**: `LTSeq.distinct(*keys: str | Callable) -> LTSeq`
- **Behavior**: Deduplicate; if no keys are provided, deduplicate by all columns
- **Parameters**: `keys` key columns or expressions
- **Returns**: deduplicated `LTSeq`
- **Exceptions**: `ValueError` (schema not initialized), `TypeError` (invalid key type), `AttributeError` (column not found)
- **Example**:
```python
unique = t.distinct("customer_id")
```

### `LTSeq.slice`
- **Signature**: `LTSeq.slice(offset: int = 0, length: int | None = None) -> LTSeq`
- **Behavior**: Select a contiguous row range with logical zero-copy semantics
- **Parameters**: `offset` starting row (0-based); `length` number of rows (None means to the end)
- **Returns**: sliced `LTSeq`
- **Exceptions**: `ValueError` (negative offset/length or schema not initialized)
- **Example**:
```python
t.slice(offset=10, length=5)
```

## 3. Ordered and Window Functions

### 3.1 Row-Level Window Operations

> These operations require a prior `.sort()` (or `.assume_sorted()`) to establish order. `shift` additionally accepts a `partition_by=` kwarg (column name string or expression) to restart the window at group boundaries.

#### `r.col.shift`
- **Signature**: `r.col.shift(offset: int, default: Any = None, partition_by: str | Expr | None = None) -> Expr`
- **Behavior**: Access relative rows. Positive offset looks backward (previous rows), negative offset looks forward (future rows). Consistent with pandas `Series.shift()`. With `partition_by`, the window resets at group boundaries (LAG/LEAD OVER PARTITION BY)
- **Parameters**: `offset` row offset (positive = backward, negative = forward); `default` fill value at boundaries (default NULL); `partition_by` optional partition column
- **Returns**: expression (NULL — or `default` — at boundaries where offset exceeds available rows)
- **Exceptions**: `TypeError` (offset not int), `RuntimeError` (used without sort)
- **Example**:
```python
with_prev = t.sort("date").derive(prev=lambda r: r.close.shift(1))

# Per-group shift: NULL at each group boundary
t.sort("grp", "date").derive(
    prev=lambda r: r.value.shift(1, partition_by="grp")
)

# Fill boundary with a default instead of NULL
t.sort("date").derive(prev=lambda r: r.value.shift(1, default=0))
```

#### `r.col.rolling`
- **Signature**: `r.col.rolling(window_size: int).agg_func() -> Expr`
- **Behavior**: Sliding window aggregation; supported aggs: `mean/sum/min/max/count/std`
- **Parameters**: `window_size` window size
- **Returns**: window aggregation expression
- **Exceptions**: `ValueError` (window_size <= 0), `RuntimeError` (used without sort)
- **Example**:
```python
ma5 = t.sort("date").derive(ma_5=lambda r: r.close.rolling(5).mean())
```

#### `r.col.diff`
- **Signature**: `r.col.diff(offset: int = 1) -> Expr`
- **Behavior**: Row difference, equivalent to `r.col - r.col.shift(offset)`
- **Parameters**: `offset` row offset
- **Returns**: difference expression
- **Exceptions**: `TypeError` (non-numeric or offset not int), `RuntimeError` (used without sort)
- **Example**:
```python
changes = t.sort("date").derive(daily=lambda r: r.close.diff())
```

#### `r.col.pct_change`
- **Signature**: `r.col.pct_change() -> Expr`
- **Behavior**: Percentage change from the previous row: `(x - x.shift(1)) / x.shift(1)`. Requires sort
- **Returns**: numeric expression
- **Example**:
```python
returns = t.sort("date").derive(daily_return=lambda r: r.close.pct_change())
```

#### `r.col.cum_sum` (expression form)
- **Signature**: `r.col.cum_sum() -> Expr`
- **Behavior**: Cumulative sum over the current order, usable inside `derive()` so the output column name is under your control
- **Exceptions**: `TypeError` (non-numeric), `RuntimeError` (used without sort)
- **Example**:
```python
t.sort("date").derive(cum_vol=lambda r: r.volume.cum_sum())
```

### 3.2 Table-Level Cumulative Operations

#### `LTSeq.cum_sum`
- **Signature**: `LTSeq.cum_sum(*cols: str | Callable) -> LTSeq`
- **Behavior**: Add cumulative sum columns with `*_cumsum` suffix. For custom output names use the expression form `r.col.cum_sum()` inside `derive()`
- **Parameters**: `cols` column names or expressions
- **Returns**: new `LTSeq` with cumulative columns
- **Exceptions**: `ValueError` (no columns or schema not initialized), `TypeError` (non-numeric)
- **Example**:
```python
with_cum = t.sort("date").cum_sum("volume", "amount")
# → adds volume_cumsum, amount_cumsum
```

### 3.3 Ranking Functions

> Ranking functions use `.over()` to specify ordering; they do NOT require a prior `.sort()`.

Ranking functions assign positions or ranks to rows within partitions. They must be used with `.over()` to specify ordering.

#### `row_number`
- **Signature**: `row_number().over(partition_by=None, order_by=Expr, descending=False) -> Expr`
- **Behavior**: Assign a unique sequential number to each row (1, 2, 3, ...). Unlike `rank()`, always produces distinct values even for ties
- **Parameters**: Use `.over()` to specify `partition_by` (optional), `order_by` (required), `descending` (optional)
- **Returns**: integer expression
- **Exceptions**: `RuntimeError` (no order_by specified)
- **Example**:
```python
from ltseq import row_number

# Simple row numbering
t.derive(rn=lambda r: row_number().over(order_by=r.date))

# Row number within partitions
t.derive(rn=lambda r: row_number().over(partition_by=r.group, order_by=r.date))
```

#### `rank`
- **Signature**: `rank().over(partition_by=None, order_by=Expr, descending=False) -> Expr`
- **Behavior**: Assign rank with gaps for ties. Rows with equal values get the same rank; next rank is skipped (1, 2, 2, 4, 5)
- **Parameters**: Use `.over()` to specify `partition_by` (optional), `order_by` (required), `descending` (optional)
- **Returns**: integer expression
- **Exceptions**: `RuntimeError` (no order_by specified)
- **Example**:
```python
from ltseq import rank

# Rank by score (ties get same rank, gaps after)
t.derive(rnk=lambda r: rank().over(order_by=r.score))

# Rank within departments
t.derive(rnk=lambda r: rank().over(partition_by=r.dept, order_by=r.salary, descending=True))
```

#### `dense_rank`
- **Signature**: `dense_rank().over(partition_by=None, order_by=Expr, descending=False) -> Expr`
- **Behavior**: Assign rank without gaps for ties. Rows with equal values get the same rank; next rank is NOT skipped (1, 2, 2, 3, 4)
- **Parameters**: Use `.over()` to specify `partition_by` (optional), `order_by` (required), `descending` (optional)
- **Returns**: integer expression
- **Exceptions**: `RuntimeError` (no order_by specified)
- **Example**:
```python
from ltseq import dense_rank

# Dense rank by score (no gaps after ties)
t.derive(drnk=lambda r: dense_rank().over(order_by=r.score))

# Dense rank within departments
t.derive(drnk=lambda r: dense_rank().over(partition_by=r.dept, order_by=r.salary))
```

#### `ntile`
- **Signature**: `ntile(n: int).over(partition_by=None, order_by=Expr, descending=False) -> Expr`
- **Behavior**: Divide rows into `n` roughly equal buckets (1 to n). Useful for percentiles and quantiles
- **Parameters**: `n` number of buckets; use `.over()` for partitioning/ordering
- **Returns**: integer expression (bucket number 1 to n)
- **Exceptions**: `ValueError` (n <= 0), `RuntimeError` (no order_by specified)
- **Example**:
```python
from ltseq import ntile

# Divide into quartiles
t.derive(quartile=lambda r: ntile(4).over(order_by=r.score))

# Deciles within groups
t.derive(decile=lambda r: ntile(10).over(partition_by=r.group, order_by=r.value))
```

#### `CallExpr.over` (Window Specification)
- **Signature**: `expr.over(partition_by: Expr | None = None, order_by: Expr | None = None, descending: bool | None = None, desc: bool | None = None) -> WindowExpr`
- **Behavior**: Apply a window specification to a ranking function. `partition_by` and `order_by` each take a **single column expression**; `descending` is a single bool applied to `order_by`
- **Parameters**:
  - `partition_by` column to partition by (optional)
  - `order_by` column to order by (required for ranking functions)
  - `descending` sort direction for order_by (default False)
  - `desc` alias for `descending`; when both are given, `descending` takes precedence, matching `sort()`'s resolution
- **Returns**: `WindowExpr` ready for use in derive()
- **Exceptions**: `TypeError` (invalid partition_by/order_by types)
- **Example**:
```python
t.derive(rn=lambda r: row_number().over(
    partition_by=r.region,
    order_by=r.date,
    descending=True,
))
```

### 3.4 Ordered Search

#### `LTSeq.search_first`
- **Signature**: `LTSeq.search_first(predicate: Callable[[Row], Expr]) -> LTSeq`
- **Behavior**: Return the first matching row (single-row LTSeq); can do binary search on sorted data
- **Parameters**: `predicate` row predicate
- **Returns**: single-row `LTSeq` (empty if not found)
- **Exceptions**: `ValueError` (schema not initialized), `TypeError` (invalid predicate), `RuntimeError` (predicate cannot be transpiled)
- **Example**:
```python
first_big = t.sort("price").search_first(lambda r: r.price > 100)
```

#### `LTSeq.search_pattern`
- **Signature**: `LTSeq.search_pattern(*step_predicates: Callable, partition_by: str | None = None) -> LTSeq`
- **Behavior**: Find rows where **consecutive rows** match a sequence of predicates (funnel/sequence matching). Returns the rows where step 1 matched, i.e. row `i` such that `step1(i), step2(i+1), ..., stepN(i+N-1)` all hold. With `partition_by`, the pattern cannot cross partition boundaries
- **Parameters**: `step_predicates` one lambda per step; `partition_by` optional partition column
- **Returns**: `LTSeq` of step-1 rows
- **Exceptions**: `ValueError` (no predicates or schema not initialized)
- **Example**:
```python
# Find a 3-step URL funnel per user
matches = t.search_pattern(
    lambda r: r.url.s.starts_with("/landing"),
    lambda r: r.url.s.starts_with("/product"),
    lambda r: r.url.s.starts_with("/checkout"),
    partition_by="userid",
)
```

#### `LTSeq.search_pattern_count`
- **Signature**: `LTSeq.search_pattern_count(*step_predicates: Callable, partition_by: str | None = None) -> int`
- **Behavior**: Same as `search_pattern` but returns only the match count
- **Returns**: `int`
- **Example**:
```python
n = t.search_pattern_count(
    lambda r: r.event == "view",
    lambda r: r.event == "buy",
    partition_by="userid",
)
```

#### `LTSeq.align`
- **Signature**: `LTSeq.align(ref_sequence: list[Any], key: Callable[[Row], Expr]) -> LTSeq`
- **Behavior**: Align to `ref_sequence` order and insert NULLs for missing keys
- **Parameters**: `ref_sequence` reference key sequence; `key` key extractor
- **Returns**: aligned `LTSeq`
- **Exceptions**: `TypeError` (invalid key), `ValueError` (empty ref_sequence)
- **Example**:
```python
aligned = t.align(["2024-01-01", "2024-01-02"], key=lambda r: r.date)
```

### 3.5 Physical Row-Order Operations

#### `LTSeq.rvs`
- **Signature**: `LTSeq.rvs() -> LTSeq`
- **Behavior**: Reverse the row order of the table
- **Example**:
```python
reversed_t = t.sort("date").rvs()
```

#### `LTSeq.step`
- **Signature**: `LTSeq.step(n: int) -> LTSeq`
- **Behavior**: Take every nth row (0-based: rows 0, n, 2n, ...)
- **Parameters**: `n` step size (must be >= 1)
- **Exceptions**: `ValueError` (n < 1)
- **Example**:
```python
every_other = t.step(2)   # rows 0, 2, 4, ...
every_tenth = t.step(10)
```

## 4. Ordered Grouping and Procedural Computing

### `LTSeq.group_ordered` (alias: `group_consecutive`)
- **Signature**: `LTSeq.group_ordered(key: Callable[[Row], Expr]) -> NestedTable`
- **Behavior**: Group only consecutive equal values; does not reorder. `group_consecutive` is a more descriptive alias for the same method
- **Parameters**: `key` grouping key expression
- **Returns**: `NestedTable` (group-level operations)
- **Exceptions**: `ValueError` (schema not initialized), `TypeError` (invalid key), `AttributeError` (column not found)
- **Example**:
```python
groups = t.sort("date").group_ordered(lambda r: r.is_up)
# equivalent:
groups = t.sort("date").group_consecutive(lambda r: r.is_up)
```

### `LTSeq.group_sorted`
- **Signature**: `LTSeq.group_sorted(key: Callable[[Row], Expr]) -> NestedTable`
- **Behavior**: Assumes global sort by key; one-pass grouping without hashing
- **Parameters**: `key` grouping key expression
- **Returns**: `NestedTable`
- **Exceptions**: `ValueError` (unsorted or schema not initialized), `TypeError` (invalid key)
- **Example**:
```python
groups = t.sort("user_id").group_sorted(lambda r: r.user_id)
```

### `NestedTable` (from `group_ordered` / `group_sorted`)

#### `NestedTable.first`
- **Signature**: `nested.first() -> LTSeq`
- **Behavior**: First row of each group
- **Returns**: `LTSeq` of first rows (one row per group)
- **Example**:
```python
first_rows = groups.first()
```

#### `NestedTable.last`
- **Signature**: `nested.last() -> LTSeq`
- **Behavior**: Last row of each group
- **Returns**: `LTSeq` of last rows (one row per group)
- **Example**:
```python
last_rows = groups.last()
```

#### `NestedTable.flatten`
- **Signature**: `nested.flatten() -> LTSeq`
- **Behavior**: Return the underlying rows with a `__group_id__` column identifying each consecutive group (plus internal `__group_count__` and `__rn__` helper columns)
- **Returns**: flattened `LTSeq`
- **Example**:
```python
flat = groups.flatten()
```

#### `NestedTable.filter`
- **Signature**: `nested.filter(predicate: Callable[[GroupProxy], Expr]) -> NestedTable`
- **Behavior**: Keep only groups satisfying a group-level predicate; rows of surviving groups are preserved
- **Parameters**: `predicate` group predicate (`g` is a group proxy, see below)
- **Returns**: filtered `NestedTable`
- **Exceptions**: `TypeError` (invalid predicate), `RuntimeError` (execution failure)
- **Example**:
```python
big_groups = groups.filter(lambda g: g.count() > 3)
```

#### `NestedTable.derive`
- **Signature**: `nested.derive(func: Callable[[GroupProxy], dict[str, Expr]]) -> LTSeq`
- **Behavior**: Compute group-level values and **broadcast them to every row of the group** (SQL window semantics). The result keeps all original rows and columns, plus the new columns. To collapse to one row per group instead, use `NestedTable.agg()`
- **Parameters**: `func` returns a dict of group expressions
- **Returns**: `LTSeq` with original rows plus broadcast group-level columns
- **Exceptions**: `ValueError` (lambda doesn't return a dict), `RuntimeError` (execution failure)
- **Example**:
```python
# Every row gets its group's size and span
enriched = groups.derive(lambda g: {
    "group_size": g.count(),
    "start": g.first().date,
    "end": g.last().date,
})
```

#### `NestedTable.agg`
- **Signature**: `nested.agg(func: Callable[[GroupProxy], dict[str, Expr]]) -> LTSeq`
- **Behavior**: Collapse each group to **one summary row** (SQL GROUP BY semantics) — the counterpart of `derive`'s broadcast. Groups appear in their original sequence order; the output contains only the mapped columns. Replaces the old `derive(...) + distinct(...)` idiom
- **Parameters**: `func` returns a dict of plain group aggregates (`g.count()`, `g.first().col`, `g.last().col`, `g.sum('col')`, ...). Arithmetic combinations of aggregates are not supported yet — compute them after agg()
- **Returns**: `LTSeq` with one row per group
- **Exceptions**: `ValueError` (non-dict return or unsupported expression), `RuntimeError` (execution failure)
- **Example**:
```python
# One row per consecutive run
spans = t.group_ordered(lambda r: r.is_up).agg(lambda g: {
    "start": g.first().date,
    "end": g.last().date,
    "n": g.count(),
})
```

#### `len(nested)` / `NestedTable.to_pandas`
- **Signature**: `NestedTable.__len__() -> int`; `NestedTable.to_pandas()`
- **Behavior**: `len()` returns the number of **rows** in the underlying table (not the number of groups); `to_pandas()` materializes the underlying rows as-is, without the `__group_id__` column (use `flatten().to_pandas()` for that)
- **Example**:
```python
n_rows = len(groups)                  # row count, not group count
df = groups.flatten().to_pandas()     # rows with __group_id__
```

### Group Proxy (`g` inside NestedTable filter/derive)

Group aggregations take the **column name as a string**; `first()`/`last()` return row proxies with attribute access.

#### Aggregations: `g.count()`, `g.sum()`, `g.avg()`/`g.mean()`, `g.min()`, `g.max()`, `g.median()`, `g.std()`, `g.var()`, `g.percentile()`
- **Signature**: `g.count() -> Expr`; `g.sum(column: str) -> Expr` (same for `avg`/`mean`/`min`/`max`/`median`/`std`/`var`); `g.percentile(column: str, p: float) -> Expr`
- **Behavior**: Group size / aggregation over one column within each group. `mean` is an alias for `avg` (Pandas/Polars verb); `std`/`var` are sample statistics; `percentile` is approximate with `p` in [0, 1]
- **Example**:
```python
groups.derive(lambda g: {"avg_price": g.mean("price"), "med": g.median("price")})
groups.filter(lambda g: g.std("amount") > 5)
```

#### Row access: `g.first()`, `g.last()`
- **Signature**: `g.first() -> RowProxy`; `g.last() -> RowProxy`
- **Behavior**: First/last row of the group; access columns as attributes
- **Example**:
```python
groups.derive(lambda g: {"start": g.first().date, "end": g.last().date})
```

#### Quantifiers (filter only): `g.all()`, `g.any()`, `g.none()`
- **Signature**: `g.all(predicate: Callable[[Row], Expr]) -> Expr` (same for `any`/`none`)
- **Behavior**: True if the row-level predicate holds for ALL / AT LEAST ONE / NO rows of the group. Available in `NestedTable.filter`
- **Parameters**: `predicate` row-level predicate (full row expression surface)
- **Example**:
```python
# Groups where all amounts are positive
groups.filter(lambda g: g.all(lambda r: r.amount > 0))

# Groups containing at least one error
groups.filter(lambda g: g.any(lambda r: r.status == "error"))

# Groups with no deleted rows
groups.filter(lambda g: g.none(lambda r: r.is_deleted == True))
```

## 5. Set Algebra

### `LTSeq.concat` / `LTSeq.union`
- **Signature**: `LTSeq.concat(other: LTSeq) -> LTSeq`
- **Behavior**: Vertical concatenation, **keeping duplicates** (SQL UNION ALL semantics). `union` is a compatibility alias — note it does NOT deduplicate like SQL UNION; prefer `concat`, the Pandas/Polars verb that carries no dedup expectation
- **Parameters**: `other` another LTSeq with same schema
- **Returns**: combined `LTSeq`
- **Exceptions**: `TypeError` (other is not LTSeq), `ValueError` (schema mismatch)
- **Example**:
```python
combined = t1.concat(t2)
```

### `LTSeq.intersect`
- **Signature**: `LTSeq.intersect(other: LTSeq, on: Callable | None = None) -> LTSeq`
- **Behavior**: Intersection of two tables
- **Parameters**: `other` another table; `on` key selector (None means all columns)
- **Returns**: intersection `LTSeq`
- **Exceptions**: `TypeError` (other not LTSeq or invalid on), `ValueError` (schema not initialized)
- **Example**:
```python
common = t1.intersect(t2, on=lambda r: r.id)
```

### `LTSeq.subtract` / `LTSeq.except_` (set difference)
- **Signature**: `LTSeq.subtract(other: LTSeq, on: Callable | None = None) -> LTSeq`
- **Behavior**: Rows in left table but not in right table (SQL EXCEPT semantics). `except_` is a compatibility alias (the trailing underscore dodges the Python keyword); `subtract` matches PySpark
- **Parameters**: `other` another table; `on` key selector
- **Returns**: difference `LTSeq`
- **SQL Equivalent**: `EXCEPT` / `MINUS`
- **Exceptions**: `TypeError` (other not LTSeq or invalid on), `ValueError` (schema not initialized)
- **Example**:
```python
only_left = t1.subtract(t2, on=lambda r: r.id)
```

> Note: `Expr.diff()` (row-level differences, Section 3) is unrelated to table set difference. Use `except_()` for table set difference.

### `LTSeq.xunion` (symmetric difference)
- **Signature**: `LTSeq.xunion(other: LTSeq, on: Callable | None = None) -> LTSeq`
- **Behavior**: Rows in either table but not in both; equivalent to `(a except b) union (b except a)`
- **Parameters**: `other` another table; `on` key selector (None means all columns)
- **Returns**: symmetric difference `LTSeq`
- **Example**:
```python
unique_to_either = t1.xunion(t2, on=lambda r: r.id)
```

### `LTSeq.is_subset`
- **Signature**: `LTSeq.is_subset(other: LTSeq, on: Callable | None = None) -> bool`
- **Behavior**: Check if this table is a subset of another
- **Parameters**: `other` another table; `on` key selector
- **Returns**: `bool`
- **Exceptions**: `TypeError` (other not LTSeq or invalid on), `ValueError` (schema not initialized)
- **Example**:
```python
flag = t_small.is_subset(t_big, on=lambda r: r.id)
```

### `LTSeq.contain`
- **Signature**: `LTSeq.contain(key_col: str, *values) -> bool`
- **Behavior**: Check if **all** given values are present in the column
- **Parameters**: `key_col` column name; `values` values to look for
- **Returns**: `bool` (True if all values found; True for empty values)
- **Exceptions**: `AttributeError` (column not found)
- **Example**:
```python
t.contain("status", "active", "pending")
t.contain("id", 1, 2, 3)
```

## 6. Association and Joins

### `LTSeq.join`
- **Signature**: `LTSeq.join(other: LTSeq, on: Callable, how: str = "inner", strategy: str | None = None) -> LTSeq`
- **Behavior**: Join two tables. Default is a hash join (no sorting required); `strategy="merge"` performs a merge join on pre-sorted inputs and validates sort order. Conflicting right-table column names are prefixed with `_other_` (e.g. `product` → `_other_product`)
- **Parameters**: `other` other table; `on` join condition lambda (e.g. `lambda a, b: a.id == b.id`); `how` in {inner,left,right,full}; `strategy` in {None,"hash","merge"}
- **Returns**: joined `LTSeq`
- **Exceptions**: `TypeError` (invalid other/on), `ValueError` (invalid how/strategy, or unsorted inputs for merge)
- **Example**:
```python
joined = users.join(orders, on=lambda u, o: u.id == o.user_id, how="left")

# Merge join on pre-sorted tables
result = t1.sort("id").join(
    t2.sort("id"),
    on=lambda a, b: a.id == b.id,
    strategy="merge",
)
```

### `LTSeq.asof_join`
- **Signature**: `LTSeq.asof_join(other: LTSeq, on: Callable, direction: str = "backward", is_sorted: bool = False) -> LTSeq`
- **Behavior**: As-of join for nearest time match. `is_sorted=True` skips sort verification when both inputs are known to be sorted (faster)
- **Parameters**: `other` other table; `on` join condition (e.g. `lambda t, q: t.time >= q.time`); `direction` in {"backward","forward","nearest"}; `is_sorted` skip sort check
- **Returns**: as-of joined `LTSeq`
- **Exceptions**: `TypeError` (invalid other/on), `ValueError` (invalid direction)
- **Example**:
```python
result = trades.asof_join(quotes, on=lambda t, q: t.time >= q.time, direction="backward")
```

### `LTSeq.semi_join`
- **Signature**: `LTSeq.semi_join(other: LTSeq, on: Callable) -> LTSeq`
- **Behavior**: Return rows from left table where keys exist in right table. Returns only left table columns with no duplicates from multiple matches
- **Parameters**: `other` right table to match against; `on` join condition lambda
- **Returns**: `LTSeq` with matching rows from left table
- **Exceptions**: `TypeError` (invalid other/on), `ValueError` (schema not initialized)
- **Example**:
```python
# Users who have placed at least one order
active_users = users.semi_join(orders, on=lambda u, o: u.id == o.user_id)
```

### `LTSeq.anti_join`
- **Signature**: `LTSeq.anti_join(other: LTSeq, on: Callable) -> LTSeq`
- **Behavior**: Return rows from left table where keys do NOT exist in right table. Returns only left table columns
- **Parameters**: `other` right table to match against; `on` join condition lambda
- **Returns**: `LTSeq` with non-matching rows from left table
- **Exceptions**: `TypeError` (invalid other/on), `ValueError` (schema not initialized)
- **Example**:
```python
# Users who have never placed an order
inactive_users = users.anti_join(orders, on=lambda u, o: u.id == o.user_id)
```

### `LTSeq.link`
- **Signature**: `LTSeq.link(target_table: LTSeq, on: Callable, as_: str | None = None, join_type: str = "inner", *, alias: str | None = None) -> LinkedTable`
- **Behavior**: Pointer-style association; not materialized until needed; access target columns via the alias
- **Parameters**: `target_table` target table; `on` join condition; `alias` alias for the linked reference (`as_` is a compatibility alias for it — pass exactly one); `join_type` in {inner,left,right,full}
- **Returns**: `LinkedTable`
- **Exceptions**: `TypeError` (invalid on), `ValueError` (invalid join_type, both/neither of as_ and alias, or schema not initialized)
- **Example**:
```python
linked = orders.link(products, on=lambda o, p: o.product_id == p.id, alias="prod")
result = linked.select(lambda r: [r.id, r.prod.name, r.prod.price])
```

### `LinkedTable`
- **Behavior**: Chainable view over a linked pair of tables. Supports `select`, `filter`, `derive`, `sort`, `slice`, `distinct`, `show`, and further `link` calls (multi-hop chains). Materializes via the underlying join only when required
- **Example**:
```python
linked = orders.link(products, on=lambda o, p: o.product_id == p.id, alias="prod")
cheap = linked.filter(lambda r: r.prod.price < 10)
chained = linked.link(categories, on=lambda o, c: o.category_id == c.id, alias="cat")
```

See `docs/LINKING_GUIDE.md` for the full linking guide.

### `r.col.lookup` (expression-level lookup)
- **Signature**: `r.col.lookup(target_table: LTSeq, column: str, join_key: str | None = None) -> Expr`
- **Behavior**: Lightweight lookup inside expressions, returning a single column value from a related table. Works on any expression (column or transformed value). Resolved via a join during `derive()`
- **Parameters**: `target_table` target table; `column` output column; `join_key` join key in target table (optional)
- **Returns**: lookup expression
- **Exceptions**: `TypeError` (invalid params), `RuntimeError` (execution failure)
- **Example**:
```python
enriched = orders.derive(product_name=lambda r: r.product_id.lookup(products, "name"))

# Lookup on a transformed key
enriched = orders.derive(
    product_name=lambda r: r.product_id.lower().lookup(products, "name")
)
```

### Join Strategy Summary

| Method | Best Use Case | Algorithm | SQL Equivalent |
| --- | --- | --- | --- |
| `join` | Unsorted general data | Hash Join | `JOIN` |
| `join(..., strategy="merge")` | Pre-sorted large tables | Merge Join | `JOIN` (optimized) |
| `semi_join` | Filter by key existence | Hash Semi-Join | `WHERE EXISTS` |
| `anti_join` | Filter by key non-existence | Hash Anti-Join | `WHERE NOT EXISTS` |
| `link` | Fact-to-dimension pointer access | Pointer | `LEFT JOIN` (lazy) |
| `r.col.lookup(...)` | Fetch one column from a dimension table | Join during derive | `LEFT JOIN` (single column) |
| `asof_join` | Financial time series | Ordered Search | `LATERAL JOIN` |

## 7. Aggregation, Partitioning, Pivot

### `LTSeq.group_by` (chain style)
- **Signature**: `LTSeq.group_by(key: str | Callable) -> GroupBy`; `GroupBy.agg(**aggregations: Callable) -> LTSeq`
- **Behavior**: Polars/pandas-style two-step grouped aggregation. `key` may be a column name string or a lambda; `agg` produces one row per group
- **Parameters**: `key` grouping key; `aggregations` named aggregation lambdas
- **Returns**: `GroupBy` intermediate, then aggregated `LTSeq`
- **Exceptions**: `TypeError` (invalid key), `AttributeError` (column not found), `ValueError` (no aggregations)
- **Example**:
```python
summary = t.group_by("region").agg(
    total=lambda g: g.sales.sum(),
    avg_price=lambda g: g.price.avg(),
)

# Expression grouping key
summary = t.group_by(lambda r: r.date.dt.year()).agg(n=lambda g: g.id.count())
```

### `LTSeq.agg`
- **Signature**: `LTSeq.agg(by: Callable | None = None, **aggs: Callable) -> LTSeq`
- **Behavior**: Grouped aggregation (or full-table aggregation when `by=None`); one row per group. `by` must be a lambda (for string column names use `group_by`)
- **Parameters**: `by` grouping key lambda; `aggs` aggregation expressions
- **Returns**: aggregated `LTSeq`
- **Exceptions**: `ValueError` (schema not initialized or no aggregations), `TypeError` (by/aggregation not callable)
- **Example**:
```python
summary = t.agg(by=lambda r: r.region, total=lambda g: g.sales.sum())

# Full-table aggregation
total = t.agg(total=lambda g: g.sales.sum())
```

### Aggregate column methods (inside `agg` / `group_by().agg()` lambdas)
- **Signature**: `g.col.sum() / .avg() / .mean() / .count() / .min() / .max() / .median() / .var() / .variance() / .std() / .stddev() / .percentile(p)`
- **Behavior**: Column aggregations available in aggregation context. `mean` is an alias for `avg` (Pandas/Polars verb, same name as the rolling aggregate); `var`/`variance` is sample variance; `std`/`stddev` is sample standard deviation; `percentile(p)` takes `p` in 0–1 (approximate percentile)
- **Example**:
```python
stats = t.group_by("region").agg(
    total=lambda g: g.sales.sum(),
    med=lambda g: g.sales.median(),
    sd=lambda g: g.sales.std(),
    p95=lambda g: g.latency.percentile(0.95),
    n=lambda g: g.id.count(),
)
```

### Conditional aggregation functions
- **Signature**: `count_if(predicate: Expr)`, `sum_if(predicate: Expr, column: Expr)`, `avg_if(...)`, `min_if(...)`, `max_if(...)`
- **Behavior**: Aggregate only rows where the predicate holds. Used inside `agg`/`group_by().agg()` lambdas
- **Import**: `from ltseq import count_if, sum_if, avg_if, min_if, max_if`
- **Example**:
```python
from ltseq import count_if, sum_if

result = t.group_by("region").agg(
    high_count=lambda g: count_if(g.price > 100),
    high_sales=lambda g: sum_if(g.price > 100, g.quantity),
)
```

### Statistical aggregation functions
- **Signature**: `skew(col: Expr)`, `corr(a: Expr, b: Expr)`, `covar(a: Expr, b: Expr)`, `concat_agg(col: Expr, delimiter: str = ",")`
- **Behavior**: Skewness, Pearson correlation, sample covariance, and string concatenation aggregation. Used inside `agg`/`group_by().agg()` lambdas
- **Import**: `from ltseq import skew, corr, covar, concat_agg`
- **Example**:
```python
from ltseq import corr, concat_agg

result = t.group_by("region").agg(
    price_qty_corr=lambda g: corr(g.price, g.quantity),
    names=lambda g: concat_agg(g.name, delimiter="|"),
)
```

### `LTSeq.partition`
- **Signature**: `LTSeq.partition(*cols: str) -> PartitionedTable` or `LTSeq.partition(by: Callable) -> PartitionedTable`
- **Behavior**: Split into sub-tables by key (no aggregation). A callable key must be a **simple column expression** (e.g. `lambda r: r.region`); derived expressions like `lambda r: r.price + 1` raise `ValueError` (they would force internal materialization)
- **Parameters**: column name(s), a single callable, or `by=` callable
- **Returns**: `PartitionedTable` (dict-like: key → LTSeq)
- **Exceptions**: `TypeError` (invalid params), `AttributeError` (column not found), `ValueError` (schema not initialized, or callable is not a simple column expression)
- **Example**:
```python
parts = t.partition("region")
west = parts["West"]

parts = t.partition("year", "region")       # multiple columns
parts = t.partition(by=lambda r: r.region)  # lambda key
```

### `PartitionedTable`
- **Behavior**: Dict-like container of partitions. Supports `parts[key]`, iteration, `keys()`, `values()`, `items()`, `map(fn)` (apply a function to every partition), and `to_list()`
- **Example**:
```python
for key, sub in parts.items():
    print(key, len(sub))

filtered = parts.map(lambda sub: sub.filter(lambda r: r.amount > 0))
```

### `LTSeq.pivot`
- **Signature**: `LTSeq.pivot(index: str | list[str], columns: str, values: str, agg_fn: str = "sum") -> LTSeq`
- **Behavior**: Pivot from long to wide
- **Parameters**:
  - `index` row index column(s)
  - `columns` column to pivot on (values become column names)
  - `values` column containing values to aggregate
  - `agg_fn` aggregation function: `"sum"` | `"mean"` | `"min"` | `"max"` | `"count"` | `"first"` | `"last"` (default: `"sum"`)
- **Returns**: pivoted `LTSeq`
- **Exceptions**: `ValueError` (invalid agg_fn or schema not initialized), `AttributeError` (column not found)
- **Example**:
```python
pivoted = t.pivot(index="date", columns="region", values="amount", agg_fn="sum")
```

## 8. Expression API (inside lambdas)

### Expression Operators
- **Signature**: `+ - * / // %`, `== != > >= < <=`, `& | ~`
- **Behavior**: Build expression trees, not executed in Python
- **Parameters**: left/right operands (Expr or literals)
- **Returns**: expression object
- **Exceptions**: `TypeError` (type mismatch)
- **Example**:
```python
expr = (r.price * r.qty) > 100
```

### `if_else`
- **Signature**: `if_else(condition: Expr, true_value: Any, false_value: Any) -> Expr`
- **Behavior**: Conditional expression (SQL CASE WHEN)
- **Parameters**: `condition`, `true_value`, `false_value`
- **Returns**: expression
- **Import**: `from ltseq import if_else`
- **Exceptions**: `TypeError` (condition not boolean)
- **Example**:
```python
from ltseq import if_else
status = if_else(r.amount > 100, "VIP", "Normal")
```

### `ifa` / `nvl` / `coalesce`
- **Signature**: `ifa(cond: Expr, value: Expr) -> Expr`; `nvl(x: Expr, default: Expr) -> Expr`; `coalesce(*args: Expr) -> Expr`
- **Behavior**: `ifa(cond, v)` = `if_else(cond, v, NULL)`; `nvl(x, d)` = `coalesce(x, d)`; `coalesce` returns the first non-NULL argument
- **Import**: `from ltseq import ifa, nvl, coalesce`
- **Example**:
```python
from ltseq import nvl, coalesce
t.derive(price2=lambda r: nvl(r.price, 0))
t.derive(contact=lambda r: coalesce(r.mobile, r.phone, r.email))
```

### Null / membership / type helpers (`Expr` methods)

#### `r.col.fill_null`
- **Signature**: `r.col.fill_null(default: Any) -> Expr`
- **Behavior**: NULL fill (SQL COALESCE)
- **Example**:
```python
safe_price = r.price.fill_null(0)
```

#### `r.col.is_null` / `r.col.is_not_null`
- **Signature**: `r.col.is_null() -> Expr`; `r.col.is_not_null() -> Expr`
- **Behavior**: NULL / NOT NULL check
- **Example**:
```python
missing = t.filter(lambda r: r.email.is_null())
valid = t.filter(lambda r: r.email.is_not_null())
```

#### `r.col.is_in`
- **Signature**: `r.col.is_in(values: list[Any]) -> Expr`
- **Behavior**: Membership check (SQL `IN`)
- **Example**:
```python
t.filter(lambda r: r.status.is_in(["active", "pending", "review"]))
```

#### `r.col.between`
- **Signature**: `r.col.between(low: Any, high: Any) -> Expr`
- **Behavior**: Inclusive range check, equivalent to `(x >= low) & (x <= high)`
- **Example**:
```python
t.filter(lambda r: r.price.between(10, 100))
```

#### `r.col.cast`
- **Signature**: `r.col.cast(dtype: str) -> Expr`
- **Behavior**: Cast to another type. Supported: `"int32"`, `"int64"`, `"float32"`, `"float64"`, `"utf8"`/`"string"`, `"bool"`, `"date32"`, `"timestamp"`
- **Example**:
```python
t.derive(amount=lambda r: r.amount_str.cast("float64"))
```

#### `r.col.abs` / `r.col.round` / `r.col.floor` / `r.col.ceil`
- **Signature**: `r.col.abs() -> Expr`; `r.col.round(decimals: int = 0) -> Expr`; `r.col.floor() -> Expr`; `r.col.ceil() -> Expr`
- **Behavior**: Numeric rounding helpers
- **Example**:
```python
t.derive(
    abs_change=lambda r: (r.price - r.prev).abs(),
    rounded=lambda r: r.score.round(2),
)
```

### String Operations (`r.col.s.*`)

> `.str` is an alias for `.s` (Pandas/Polars convention): `r.email.str.contains("@")` ≡ `r.email.s.contains("@")`.

#### `contains`
- **Signature**: `r.col.s.contains(pattern: str) -> Expr`
- **Behavior**: substring containment
- **Example**:
```python
gmail = t.filter(lambda r: r.email.s.contains("gmail"))
```

#### `starts_with` / `ends_with`
- **Signature**: `r.col.s.starts_with(prefix: str) -> Expr`; `r.col.s.ends_with(suffix: str) -> Expr`
- **Behavior**: prefix / suffix match
- **Example**:
```python
orders = t.filter(lambda r: r.code.s.starts_with("ORD"))
pdfs = t.filter(lambda r: r.filename.s.ends_with(".pdf"))
```

#### `lower` / `upper`
- **Signature**: `r.col.s.lower() -> Expr`; `r.col.s.upper() -> Expr`
- **Behavior**: case conversion
- **Example**:
```python
normalized = t.derive(email_lower=lambda r: r.email.s.lower())
```

#### `strip` / `lstrip` / `rstrip`
- **Signature**: `r.col.s.strip() -> Expr`; `r.col.s.lstrip() -> Expr`; `r.col.s.rstrip() -> Expr`
- **Behavior**: trim whitespace (both sides / left only / right only)
- **SQL Equivalent**: `TRIM` / `LTRIM` / `RTRIM`
- **Example**:
```python
clean = t.derive(name_clean=lambda r: r.name.s.strip())
```

#### `len`
- **Signature**: `r.col.s.len() -> Expr`
- **Behavior**: string length
- **Example**:
```python
long_names = t.filter(lambda r: r.name.s.len() > 50)
```

#### `slice`
- **Signature**: `r.col.s.slice(start: int, length: int) -> Expr`
- **Behavior**: substring slicing
- **Parameters**: `start` start index (0-based); `length` length
- **Example**:
```python
year = t.derive(year=lambda r: r.date.s.slice(0, 4))
```

#### `regex_match`
- **Signature**: `r.col.s.regex_match(pattern: str) -> Expr`
- **Behavior**: regex match (boolean)
- **Exceptions**: `ValueError` (invalid regex)
- **Example**:
```python
valid = t.filter(lambda r: r.email.s.regex_match(r"^[a-z]+@"))
```

#### `like`
- **Signature**: `r.col.s.like(pattern: str) -> Expr`
- **Behavior**: SQL LIKE pattern match (`%` any chars, `_` one char)
- **Example**:
```python
t.filter(lambda r: r.code.s.like("ORD-%"))
```

#### `replace`
- **Signature**: `r.col.s.replace(old: str, new: str) -> Expr`
- **Behavior**: Replace all occurrences of a substring with another string
- **Example**:
```python
clean = t.derive(clean_name=lambda r: r.name.s.replace("-", "_"))
```

#### `concat`
- **Signature**: `r.col.s.concat(*others) -> Expr`
- **Behavior**: Concatenate this string with other strings or column expressions
- **Example**:
```python
greeting = t.derive(msg=lambda r: r.name.s.concat(" says hello"))
full = t.derive(full_name=lambda r: r.first.s.concat(" ", r.last))
```

#### `pad_left` / `pad_right`
- **Signature**: `r.col.s.pad_left(width: int, char: str = " ") -> Expr`; `r.col.s.pad_right(width: int, char: str = " ") -> Expr`
- **Behavior**: Pad to the given width. Note: truncates if the string is longer than width (SQL LPAD/RPAD behavior)
- **Example**:
```python
padded = t.derive(padded_id=lambda r: r.id.s.pad_left(5, "0"))
```

#### `split_part` / `split`
- **Signature**: `r.col.s.split_part(delimiter: str, index: int) -> Expr`
- **Behavior**: Split string by delimiter and return the part at the specified index. Index is **1-based** (1 = first part) to match SQL SPLIT_PART. Returns empty string if index is out of range. `split` is a compatibility alias — prefer `split_part`, which does not collide with Python's list-returning `str.split`
- **Exceptions**: `ValueError` (index <= 0)
- **Example**:
```python
# Get domain from email "user@example.com"
domain = t.derive(domain=lambda r: r.email.s.split_part("@", 2))
```

#### `find`
- **Signature**: `r.col.s.find(sub: str) -> Expr`
- **Behavior**: Returns the **0-based** position of the first occurrence of `sub`; returns -1 if not found (Python `str.find` semantics)
- **Example**:
```python
at_idx = t.derive(idx=lambda r: r.email.s.find("@"))  # "user@example" → 4, absent → -1
```

#### `pos`
- **Signature**: `r.col.s.pos(sub: str) -> Expr`
- **Behavior**: Returns the **1-based** position of the first occurrence of `sub`; returns 0 if not found. SQL semantics — for Python semantics use `find`
- **SQL Equivalent**: `STRPOS(col, sub)`
- **Example**:
```python
at_pos = t.derive(pos=lambda r: r.email.s.pos("@"))  # "user@example" → 5
```

#### `left` / `right`
- **Signature**: `r.col.s.left(n: int) -> Expr`; `r.col.s.right(n: int) -> Expr`
- **Behavior**: Leftmost / rightmost `n` characters; returns the full string if `n` exceeds its length
- **SQL Equivalent**: `LEFT(col, n)` / `RIGHT(col, n)`
- **Example**:
```python
prefix = t.derive(pfx=lambda r: r.code.s.left(3))    # "ABC123" → "ABC"
suffix = t.derive(sfx=lambda r: r.code.s.right(3))   # "ABC123" → "123"
```

#### `ord` / `asc`
- **Signature**: `r.col.s.ord() -> Expr`
- **Behavior**: Returns the ASCII/Unicode code point of the first character of the string (like Python `ord()`). `asc` is a compatibility alias — prefer `ord`, since `asc` reads as "ascending" in a sorting-heavy library
- **SQL Equivalent**: `ASCII(col)`
- **Example**:
```python
code = t.derive(code=lambda r: r.ch.s.ord())  # "A" → 65, "a" → 97
```

#### `isalpha` / `isdigit` / `islower` / `isupper`
- **Signature**: `r.col.s.isalpha() -> Expr` (same for the others)
- **Behavior**: Character-class checks, mirroring Python `str` methods
- **Example**:
```python
numeric_codes = t.filter(lambda r: r.code.s.isdigit())
```

### String Global Functions

#### `char` / `str_char`
- **Signature**: `char(n: Expr) -> Expr`
- **Behavior**: Converts a Unicode code point integer to its single-character string representation (like Python `chr()`). `str_char` is a compatibility alias
- **SQL Equivalent**: `CHR(n)`
- **Import**: `from ltseq.expr import char`
- **Example**:
```python
from ltseq.expr import char
ch = t.derive(ch=lambda r: char(r.code))  # 65 → "A", 97 → "a"
```

#### `concat_ws`
- **Signature**: `concat_ws(delimiter: str, *cols: Expr) -> Expr`
- **Behavior**: Concatenate two or more column expressions with a separator between each
- **SQL Equivalent**: `CONCAT_WS(delimiter, col1, col2, ...)`
- **Import**: `from ltseq.expr import concat_ws`
- **Example**:
```python
from ltseq.expr import concat_ws
full = t.derive(full_name=lambda r: concat_ws(" ", r.first, r.last))
```

### Temporal Operations (`r.col.dt.*`)

#### `year` / `month` / `day`
- **Signature**: `r.col.dt.year() -> Expr` (same for month/day)
- **Behavior**: extract date components
- **Example**:
```python
by_date = t.derive(year=lambda r: r.date.dt.year())
```

#### `hour` / `minute` / `second` / `millisecond`
- **Signature**: `r.col.dt.hour() -> Expr` (same for minute/second/millisecond)
- **Behavior**: extract time components (`millisecond` returns 0–999)
- **Example**:
```python
with_time = t.derive(hour=lambda r: r.ts.dt.hour())
```

#### `weekday`
- **Signature**: `r.col.dt.weekday() -> Expr`
- **Behavior**: Extract weekday as 0-based integer (Monday=0 ... Sunday=6)
- **Example**:
```python
t.derive(wd=lambda r: r.date.dt.weekday())
```

#### `add`
- **Signature**: `r.col.dt.add(days: int = 0, months: int = 0, years: int = 0, hours: int = 0, minutes: int = 0, seconds: int = 0, weeks: int = 0) -> Expr`
- **Behavior**: Date/time arithmetic. `weeks` is converted to `days × 7`; `hours/minutes/seconds` use nanosecond precision internally
- **Parameters**: integer offsets (all default to 0; negative to subtract)
- **Returns**: date/timestamp expression
- **SPL Equivalent**: `elapse(t, k, unit)`
- **Example**:
```python
delivery   = t.derive(delivery=lambda r: r.order_date.dt.add(days=5))
after_2h   = t.derive(ts2=lambda r: r.ts.dt.add(hours=2, minutes=30))
next_week  = t.derive(d2=lambda r: r.date.dt.add(weeks=1))
```

#### `diff`
- **Signature**: `r.col.dt.diff(other: Expr, unit: str = "day") -> Expr`
- **Behavior**: Returns the integer difference between `self` and `other` in the specified unit. `unit` can be `"day"` (default), `"month"`, `"year"`, `"hour"`, `"minute"`, or `"second"`
- **SPL Equivalent**: `interval(t1, t2, unit)`
- **Example**:
```python
days   = t.derive(d=lambda r: r.end.dt.diff(r.start))
months = t.derive(m=lambda r: r.end.dt.diff(r.start, unit="month"))
```

#### `age`
- **Signature**: `r.col.dt.age() -> Expr`
- **Behavior**: Returns the number of complete years between the column's date and today. Result is negative for future dates
- **SPL Equivalent**: `age(dt)`
- **Example**:
```python
age = t.derive(age=lambda r: r.birth_date.dt.age())
```

#### `now` / `today` (global functions)
- **Signature**: `now() -> Expr`; `today() -> Expr`
- **Behavior**: Current timestamp / current date
- **Import**: `from ltseq import now, today`
- **Example**:
```python
from ltseq import now
t.derive(fetched_at=lambda r: now())
```

### Math Global Functions

#### `gcd` / `lcm` / `factorial`
- **Signature**: `gcd(a: Expr, b: Expr) -> Expr`; `lcm(a: Expr, b: Expr) -> Expr`; `factorial(n: Expr) -> Expr`
- **Behavior**: Greatest common divisor, least common multiple, factorial (DataFusion `GCD`/`LCM`/`FACTORIAL`)
- **Import**: `from ltseq.expr import gcd, lcm, factorial`
- **Example**:
```python
from ltseq.expr import gcd, lcm, factorial
t.derive(g=lambda r: gcd(r.a, r.b))       # gcd(12, 8) → 4
t.derive(l=lambda r: lcm(r.a, r.b))       # lcm(4, 6) → 12
t.derive(f=lambda r: factorial(r.n))      # factorial(5) → 120
```

#### General math: `sqrt`, `power`, `sign`, `log`, `ln`, `exp`, trig, `rand`
- **Signature**: `sqrt(x)`, `power(x, n)`, `sign(x)`, `log(x, base=None)`, `ln(x)`, `exp(x)`, `sin/cos/tan/asin/acos/atan(x)`, `atan2(y, x)`, `rand()`
- **Behavior**: Standard math functions mapped to DataFusion equivalents
- **Import**: `from ltseq import sqrt, power, log, ...`
- **Example**:
```python
from ltseq import sqrt, power, log

t.derive(
    dist=lambda r: sqrt(power(r.x, 2) + power(r.y, 2)),
    log_amt=lambda r: log(r.amount, 10),
)
```

## 9. Row Mutation Operations (copy-on-write)

All mutation operations return a **new** LTSeq; the original is unchanged.

### `LTSeq.insert`
- **Signature**: `LTSeq.insert(pos: int, row_dict: dict[str, Any]) -> LTSeq`
- **Behavior**: Insert a row at the given 0-based position. `pos` beyond the end appends; negative `pos` is clamped to 0
- **Example**:
```python
t2 = t.insert(0, {"id": 99, "name": "Alice"})     # prepend
t2 = t.insert(len(t), {"id": 100, "name": "Bob"}) # append
```

### `LTSeq.delete`
- **Signature**: `LTSeq.delete(predicate_or_pos: Callable | int) -> LTSeq`
- **Behavior**: Delete rows matching a predicate, or the single row at a 0-based index
- **Example**:
```python
t2 = t.delete(lambda r: r.status == "expired")
t2 = t.delete(0)   # delete first row
```

### `LTSeq.update`
- **Signature**: `LTSeq.update(predicate: Callable, **updates: Any) -> LTSeq`
- **Behavior**: Conditionally update column values where predicate is True. Each column becomes `if_else(predicate, new_value, old_value)`
- **Example**:
```python
t2 = t.update(lambda r: r.age > 65, discount=0.2)
t2 = t.update(lambda r: r.status == "old", status="archived")
```

### `LTSeq.modify`
- **Signature**: `LTSeq.modify(pos: int, **updates: Any) -> LTSeq`
- **Behavior**: Modify specific columns in the single row at 0-based position `pos`. Silently ignored if `pos` is out of range
- **Example**:
```python
t2 = t.modify(0, status="active", score=100)
```

## 10. End-to-End Examples

### Ordered Computing + Consecutive Grouping
```python
from ltseq import LTSeq

# Task: find intervals where a stock rose for more than 3 consecutive days
runs = (
    LTSeq.read_csv("stock.csv")
    .sort("date")
    .derive(is_up=lambda r: r.price > r.price.shift(1))
    .group_ordered(lambda r: r.is_up)
    .filter(lambda g: g.count() > 3)
    .filter(lambda g: g.all(lambda r: r.is_up == True))
    .derive(lambda g: {          # broadcast: every row gets its group's span
        "start": g.first().date,
        "end": g.last().date,
        "gain": g.last().price - g.first().price,
    })
)
intervals = runs.distinct("start", "end")   # one row per interval
```

### String + Temporal + Conditional + Lookup
```python
from ltseq import LTSeq, if_else

orders = LTSeq.read_csv("orders.csv")
products = LTSeq.read_csv("products.csv")

result = orders.derive(
    order_id_clean=lambda r: r.order_id.s.strip(),
    product_name=lambda r: r.product_id.lookup(products, "name"),
    order_year=lambda r: r.order_date.dt.year(),
    status=lambda r: if_else(r.quantity > 10, "Bulk", "Standard"),
)
```

### Streaming a Large File
```python
from ltseq import LTSeq

total = 0
for batch in LTSeq.scan("huge.csv"):
    total += process(batch)
```

## 11. Execution and Performance Notes

- All expressions are serialized and pushed down to Rust/DataFusion, not evaluated row-by-row in Python
- String/temporal/NULL extensions map to SQL/DataFusion functions
- `LTSeq.scan()`/`scan_parquet()` enable streaming for very large datasets
- `assume_sorted()` skips physical sorting for pre-sorted inputs while enabling window functions and merge joins

### Expression SQL Translation Reference

All expressions are transpiled to the Rust/DataFusion layer before execution. No Python-level evaluation happens inside lambdas.

| Expression | SQL / DataFusion Equivalent |
|-----------|----------------------------|
| `if_else(cond, a, b)` | `CASE WHEN cond THEN a ELSE b END` |
| `r.col.fill_null(v)` | `COALESCE(col, v)` |
| `r.col.is_null()` | `col IS NULL` |
| `r.col.is_not_null()` | `col IS NOT NULL` |
| `r.col.is_in([a, b])` | `col IN (a, b)` |
| `r.col.between(lo, hi)` | `col >= lo AND col <= hi` |
| `r.col.cast("int64")` | `CAST(col AS BIGINT)` |
| `r.col.abs()` | `ABS(col)` |
| `r.col.round(n)` | `ROUND(col, n)` |
| `r.col.floor()` / `r.col.ceil()` | `FLOOR(col)` / `CEIL(col)` |
| `r.col.s.contains(p)` | `POSITION(p IN col) > 0` |
| `r.col.s.starts_with(p)` | `STARTS_WITH(col, p)` |
| `r.col.s.ends_with(p)` | `ENDS_WITH(col, p)` |
| `r.col.s.lower()` / `.upper()` | `LOWER(col)` / `UPPER(col)` |
| `r.col.s.strip()` / `.lstrip()` / `.rstrip()` | `TRIM` / `LTRIM` / `RTRIM` |
| `r.col.s.len()` | `CHARACTER_LENGTH(col)` |
| `r.col.s.slice(s, n)` | `SUBSTRING(col, s+1, n)` |
| `r.col.s.pos(sub)` | `STRPOS(col, sub)` |
| `r.col.s.left(n)` / `.right(n)` | `LEFT(col, n)` / `RIGHT(col, n)` |
| `r.col.s.ord()` | `ASCII(col)` |
| `r.col.s.like(p)` | `col LIKE p` |
| `r.col.s.replace(old, new)` | `REPLACE(col, old, new)` |
| `r.col.s.pad_left(n, c)` / `.pad_right(n, c)` | `LPAD(col, n, c)` / `RPAD(col, n, c)` |
| `r.col.s.split_part(d, i)` | `SPLIT_PART(col, d, i)` |
| `char(n)` | `CHR(n)` |
| `concat_ws(d, ...)` | `CONCAT_WS(d, ...)` |
| `r.col.dt.year()` etc. | `EXTRACT(YEAR FROM col)` etc. |
| `r.col.dt.add(days=n)` | `col + INTERVAL 'n' DAY` |
| `r.col.dt.diff(other)` | `DATEDIFF('day', other, col)` |
| `r.col.dt.age()` | year diff from `CURRENT_DATE` with day-of-year correction |
| `gcd(a, b)` / `lcm(a, b)` / `factorial(n)` | `GCD` / `LCM` / `FACTORIAL` |
| `count_if(cond)` | `SUM(CASE WHEN cond THEN 1 ELSE 0 END)` |
| `sum_if(cond, col)` | `SUM(CASE WHEN cond THEN col END)` |
| `g.col.percentile(p)` | `APPROX_PERCENTILE_CONT(col, p)` |

## Appendix A: Migrating from Pandas

| Pandas | LTSeq | Notes |
|--------|-------|-------|
| `df[df.age > 18]` | `t.filter(lambda r: r.age > 18)` | Lambda captures expression |
| `df[['id', 'name']]` | `t.select("id", "name")` | |
| `df.assign(total=df.a + df.b)` | `t.derive(total=lambda r: r.a + r.b)` | `with_columns` is an alias |
| `df.rename(columns={'a': 'b'})` | `t.rename(a="b")` | |
| `df.drop(columns=['tmp'])` | `t.drop("tmp")` | |
| `df.sort_values('date')` | `t.sort("date")` | |
| `df.sort_values('date', ascending=False)` | `t.sort("date", desc=True)` | `descending=` also accepted |
| `df.drop_duplicates('id')` | `t.distinct("id")` | |
| `df.groupby('region').agg({'sales': 'sum'})` | `t.group_by("region").agg(sales=lambda g: g.sales.sum())` | |
| `df.merge(df2, on='id')` | `t.join(t2, on=lambda a, b: a.id == b.id)` | |
| `df.merge(df2, on='id', how='left')` | `t.join(t2, on=lambda a, b: a.id == b.id, how="left")` | |
| `pd.merge_asof(t, q, on='time')` | `t.asof_join(q, on=lambda t, q: t.time >= q.time)` | |
| `df['col'].shift(1)` | `t.sort(...).derive(prev=lambda r: r.col.shift(1))` | Requires sort |
| `df['col'].rolling(5).mean()` | `t.sort(...).derive(ma=lambda r: r.col.rolling(5).mean())` | Requires sort |
| `df['col'].diff()` | `t.sort(...).derive(d=lambda r: r.col.diff())` | Requires sort |
| `df['col'].pct_change()` | `t.sort(...).derive(p=lambda r: r.col.pct_change())` | Requires sort |
| `df['col'].cumsum()` | `t.sort(...).cum_sum("col")` or `r.col.cum_sum()` | Requires sort |
| `df['col'].isin([1, 2])` | `t.filter(lambda r: r.col.is_in([1, 2]))` | |
| `df['col'].astype('float')` | `t.derive(col=lambda r: r.col.cast("float64"))` | |
| `df.head(10)` / `df.tail(10)` | `t.head(10)` / `t.tail(10)` | |
| `df.to_dict('records')` | `t.to_dicts()` | |
| `len(df)` | `len(t)` or `t.count()` | |
| `df.columns.tolist()` | `t.columns` | |
| `df.isna()` | `t.filter(lambda r: r.col.is_null())` | Per-column |
| `df.fillna(0)` | `t.derive(col=lambda r: r.col.fill_null(0))` | Per-column |
| `df.pipe(fn, arg)` | `t.pipe(fn, arg)` | |
