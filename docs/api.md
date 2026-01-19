# LTSeq API Design Document

LTSeq is an ordered-sequence data processing library for Python backed by Rust/DataFusion. Unlike traditional DataFrames, LTSeq emphasizes order semantics and provides SPL-style capabilities such as window functions, ordered grouping, and cursor-based streaming.

## 0. Conventions and Terms

- t: current LTSeq instance (immutable; all operations return a new instance)
- r: row proxy used inside lambdas to build expressions (not executed in Python)
- g: group proxy used in NestedTable filter/derive
- Most operations return a new LTSeq; `to_cursor` returns an iterator; `is_subset` returns a bool
- Window/ordered operations require a prior `sort`; otherwise runtime errors or incorrect results may occur
- Expressions are captured into AST on the Python side and executed in Rust/DataFusion

## Common Errors and Solutions

| Error | Cause | Solution |
|-------|-------|----------|
| `RuntimeError: window function used without sort` | Called `shift`/`rolling`/`diff` without prior `.sort()` | Add `.sort(order_column)` before window operations |
| `AttributeError: column 'xxx' not found` | Typo in column name or column doesn't exist | Check `t.columns` for available column names |
| `ValueError: schema mismatch` | Union/intersect with incompatible tables | Ensure both tables have same column names and types |
| `ValueError: tables not sorted by join keys` | `join_sorted` called on unsorted tables | Call `.sort(join_key)` on both tables first |
| `TypeError: predicate not boolean Expr` | Filter lambda returns non-boolean | Ensure predicate uses comparison operators (`>`, `==`, etc.) |
| `ValueError: desc length mismatch` | `desc` list length doesn't match number of sort keys | Provide one bool per sort key, or use single bool for all |

## Quick Reference

### Data Loading & Output
| Operation | Method | Example |
|-----------|--------|---------|
| Load CSV | `LTSeq.read_csv()` | `t = LTSeq.read_csv("data.csv")` |
| View schema | `.columns` | `print(t.columns)` |
| Stream results | `.to_cursor()` | `for batch in t.to_cursor(): ...` |
| Collect all | `.collect()` | `rows = t.collect()` |
| To pandas | `.to_pandas()` | `df = t.to_pandas()` |
| Row count | `.count()` | `n = t.count()` |

### Basic Operations
| Operation | Method | Example |
|-----------|--------|---------|
| Filter rows | `.filter()` | `t.filter(lambda r: r.age > 18)` |
| Select columns | `.select()` | `t.select("id", "name")` |
| Add columns | `.derive()` | `t.derive(total=lambda r: r.a + r.b)` |
| Sort | `.sort()` | `t.sort("date", desc=True)` |
| Deduplicate | `.distinct()` | `t.distinct("id")` |
| Slice rows | `.slice()` | `t.slice(offset=10, length=5)` |
| First N rows | `.head()` | `t.head(10)` |
| Last N rows | `.tail()` | `t.tail(10)` |

### Window Operations (require `.sort()` first)
| Operation | Method | Example |
|-----------|--------|---------|
| Previous row | `.shift(n)` | `r.price.shift(1)` |
| Rolling agg | `.rolling(n).agg()` | `r.price.rolling(5).mean()` |
| Row difference | `.diff(n)` | `r.price.diff(1)` |
| Cumulative sum | `.cum_sum()` | `t.cum_sum("volume")` |

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
| Group aggregate | `.agg()` | `t.agg(by=lambda r: r.region, total=lambda g: g.sales.sum())` |
| Partition | `.partition()` | `parts = t.partition("region")` |
| Pivot | `.pivot()` | `t.pivot(index="date", columns="region", values="amount")` |

### Joins
| Operation | Method | Example |
|-----------|--------|---------|
| Hash join | `.join()` | `a.join(b, on=lambda a, b: a.id == b.id)` |
| Merge join | `.join_sorted()` | `a.sort("id").join_sorted(b.sort("id"), on="id")` |
| Semi join | `.semi_join()` | `a.semi_join(b, on=lambda a, b: a.id == b.id)` |
| Anti join | `.anti_join()` | `a.anti_join(b, on=lambda a, b: a.id == b.id)` |

### Set Operations
| Operation | Method | Example |
|-----------|--------|---------|
| Union | `.union()` | `t1.union(t2)` |
| Intersect | `.intersect()` | `t1.intersect(t2)` |
| Diff | `.diff()` | `t1.diff(t2)` |

## 1. Input / Output

### `LTSeq.read_csv`
- **Signature**: `LTSeq.read_csv(path: str, has_header: bool = True) -> LTSeq`
- **Behavior**: Load a CSV file and infer schema; returns a chainable LTSeq
- **Parameters**: `path` CSV path; `has_header` whether the first row is a header
- **Returns**: `LTSeq` with loaded data and schema
- **Exceptions**: `FileNotFoundError`/`IOError` (invalid path or unreadable), `ValueError` (parse or schema inference failure)
- **Example**:
```python
from ltseq import LTSeq

t = LTSeq.read_csv("data.csv")
```

### `LTSeq.schema` (property)
- **Signature**: `LTSeq.schema -> Schema`
- **Behavior**: Return the table schema with column names and types
- **Parameters**: none (property)
- **Returns**: `Schema` object with `.names` and `.types` attributes
- **Exceptions**: none
- **Example**:
```python
t = LTSeq.read_csv("data.csv")
print(t.schema.names)   # ["id", "name", "age", "created_at"]
print(t.schema.types)   # [Int64, Utf8, Int64, Timestamp]

# Iterate columns
for name, dtype in zip(t.schema.names, t.schema.types):
    print(f"{name}: {dtype}")
```

### `LTSeq.columns` (property)
- **Signature**: `LTSeq.columns -> List[str]`
- **Behavior**: Return list of column names (shortcut for `schema.names`)
- **Parameters**: none (property)
- **Returns**: list of column name strings
- **Exceptions**: none
- **Example**:
```python
print(t.columns)  # ["id", "name", "age"]
```

### `LTSeq.to_cursor`
- **Signature**: `LTSeq.to_cursor(chunk_size: int = 10000) -> Iterator[Record]`
- **Behavior**: Stream results via cursor to avoid full materialization
- **Parameters**: `chunk_size` rows per batch
- **Returns**: record iterator (e.g., Record/RecordBatch)
- **Exceptions**: `ValueError` (invalid chunk_size), `RuntimeError` (streaming not supported or execution failure)
- **Example**:
```python
for batch in t.to_cursor(chunk_size=5000):
    print(batch)
```

### `LTSeq.collect`
- **Signature**: `LTSeq.collect() -> List[Dict[str, Any]]`
- **Behavior**: Materialize all rows as a list of dictionaries
- **Parameters**: none
- **Returns**: list of row dictionaries
- **Exceptions**: `MemoryError` (dataset too large), `RuntimeError` (execution failure)
- **Example**:
```python
rows = t.filter(lambda r: r.age > 18).collect()
for row in rows:
    print(row["name"])
```

### `LTSeq.to_pandas`
- **Signature**: `LTSeq.to_pandas() -> pandas.DataFrame`
- **Behavior**: Convert to pandas DataFrame for interoperability
- **Parameters**: none
- **Returns**: `pandas.DataFrame`
- **Exceptions**: `ImportError` (pandas not installed), `MemoryError` (dataset too large)
- **Example**:
```python
df = t.to_pandas()
df.plot(x="date", y="price")
```

### `LTSeq.count`
- **Signature**: `LTSeq.count() -> int`
- **Behavior**: Return the number of rows
- **Parameters**: none
- **Returns**: integer row count
- **Exceptions**: `RuntimeError` (execution failure)
- **Example**:
```python
n = t.filter(lambda r: r.status == "active").count()
```

### `LTSeq.head`
- **Signature**: `LTSeq.head(n: int = 10) -> LTSeq`
- **Behavior**: Return the first n rows
- **Parameters**: `n` number of rows (default 10)
- **Returns**: `LTSeq` with first n rows
- **Exceptions**: `ValueError` (n < 0)
- **Example**:
```python
top_10 = t.sort("score", desc=True).head(10)
```

### `LTSeq.tail`
- **Signature**: `LTSeq.tail(n: int = 10) -> LTSeq`
- **Behavior**: Return the last n rows
- **Parameters**: `n` number of rows (default 10)
- **Returns**: `LTSeq` with last n rows
- **Exceptions**: `ValueError` (n < 0)
- **Example**:
```python
recent = t.sort("date").tail(5)
```

## 2. Basic Relational Operations

### `LTSeq.filter`
- **Signature**: `LTSeq.filter(predicate: Callable[[Row], Expr]) -> LTSeq`
- **Behavior**: Filter rows matching the predicate; pushed down to the Rust engine when possible
- **Parameters**: `predicate` row predicate expression (returns boolean Expr)
- **Returns**: filtered `LTSeq`
- **Exceptions**: `ValueError` (schema not initialized), `TypeError` (predicate not boolean Expr), `AttributeError` (column not found)
- **Example**:
```python
filtered = t.filter(lambda r: r.amount > 100)
```

### `LTSeq.select`
- **Signature**: `LTSeq.select(*cols: Union[str, Callable]) -> LTSeq`
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

### `LTSeq.derive`
- **Signature**: `LTSeq.derive(**new_cols: Callable) -> LTSeq` or `LTSeq.derive(func: Callable[[Row], Dict[str, Expr]]) -> LTSeq`
- **Behavior**: Add or overwrite columns; keeps existing columns
- **Parameters**: `new_cols` mapping of column name to lambda; or a lambda that returns a dict
- **Returns**: new `LTSeq` with derived columns
- **Exceptions**: `ValueError` (schema not initialized), `TypeError` (invalid return type), `AttributeError` (column not found)
- **Example**:
```python
# Style 1
with_tax = t.derive(tax=lambda r: r.price * 0.1)
# Style 2
with_tax = t.derive(lambda r: {"tax": r.price * 0.1})
```

### `LTSeq.sort`
- **Signature**: `LTSeq.sort(*keys: Union[str, Callable], desc: Union[bool, list] = False) -> LTSeq`
- **Behavior**: Sort by one or more keys; required for window/ordered computing. Also populates `sort_keys` for sort order tracking.
- **Parameters**: `keys` column names or expressions; `desc` (or `descending`) global or per-key descending flags
- **Returns**: sorted `LTSeq` with tracked sort keys
- **Exceptions**: `ValueError` (schema not initialized or desc length mismatch), `TypeError` (invalid key type), `AttributeError` (column not found)
- **Example**:
```python
t_sorted = t.sort("date", "id", desc=[False, True])
print(t_sorted.sort_keys)  # [("date", False), ("id", True)]
```

### `LTSeq.sort_keys` (property)
- **Signature**: `LTSeq.sort_keys -> Optional[List[Tuple[str, bool]]]`
- **Behavior**: Returns the current sort keys as a list of (column_name, is_descending) tuples, or None if sort order is unknown
- **Parameters**: none (property)
- **Returns**: `Optional[List[Tuple[str, bool]]]` - list of (column, descending) tuples or None
- **Exceptions**: none
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
- **Signature**: `LTSeq.is_sorted_by(*keys: str, desc: Union[bool, List[bool]] = False) -> bool`
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

### `LTSeq.distinct`
- **Signature**: `LTSeq.distinct(*keys: Union[str, Callable]) -> LTSeq`
- **Behavior**: Deduplicate; if no keys are provided, deduplicate by all columns
- **Parameters**: `keys` key columns or expressions
- **Returns**: deduplicated `LTSeq`
- **Exceptions**: `ValueError` (schema not initialized), `TypeError` (invalid key type), `AttributeError` (column not found)
- **Example**:
```python
unique = t.distinct("customer_id")
```

### `LTSeq.slice`
- **Signature**: `LTSeq.slice(offset: int = 0, length: Optional[int] = None) -> LTSeq`
- **Behavior**: Select a contiguous row range with logical zero-copy semantics
- **Parameters**: `offset` starting row (0-based); `length` number of rows (None means to the end)
- **Returns**: sliced `LTSeq`
- **Exceptions**: `ValueError` (negative offset/length), `ValueError` (schema not initialized)
- **Example**:
```python
t.slice(offset=10, length=5)
```

## 3. Ordered and Window Functions

### 3.1 Row-Level Window Operations

> These operations require a prior `.sort()` to establish order.

#### `r.col.shift`
- **Signature**: `r.col.shift(offset: int) -> Expr`
- **Behavior**: Access relative rows. Positive offset looks backward (previous rows), negative offset looks forward (future rows). Consistent with pandas `Series.shift()`.
- **Parameters**: `offset` row offset (positive = backward, negative = forward)
- **Returns**: expression (NULL at boundaries where offset exceeds available rows)
- **Exceptions**: `TypeError` (offset not int), `RuntimeError` (used without sort)
- **Example**:
```python
with_prev = t.sort("date").derive(prev=lambda r: r.close.shift(1))
```

#### `r.col.rolling`
- **Signature**: `r.col.rolling(window_size: int).agg_func() -> Expr`
- **Behavior**: Sliding window aggregation; common aggs: `mean/sum/min/max/std`
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

### 3.2 Table-Level Cumulative Operations

#### `LTSeq.cum_sum`
- **Signature**: `LTSeq.cum_sum(*cols: Union[str, Callable]) -> LTSeq`
- **Behavior**: Add cumulative sum columns with `*_cumsum` suffix
- **Parameters**: `cols` column names or expressions
- **Returns**: new `LTSeq` with cumulative columns
- **Exceptions**: `ValueError` (no columns or schema not initialized), `TypeError` (non-numeric)
- **Example**:
```python
with_cum = t.sort("date").cum_sum("volume", "amount")
```

### 3.3 Ranking Functions

> Ranking functions use `.over()` to specify ordering; they do NOT require a prior `.sort()`.

Ranking functions assign positions or ranks to rows within partitions. They must be used with `.over()` to specify ordering.

#### `row_number`
- **Signature**: `row_number().over(partition_by=None, order_by=Expr, descending=False) -> Expr`
- **Behavior**: Assign a unique sequential number to each row (1, 2, 3, ...). Unlike `rank()`, always produces distinct values even for ties.
- **Parameters**: Use `.over()` to specify `partition_by` (optional), `order_by` (required), `descending` (optional)
- **Returns**: integer expression
- **Exceptions**: `RuntimeError` (no order_by specified)
- **Example**:
```python
from ltseq.expr import row_number

# Simple row numbering
t.derive(rn=lambda r: row_number().over(order_by=r.date))

# Row number within partitions
t.derive(rn=lambda r: row_number().over(partition_by=r.group, order_by=r.date))
```

#### `rank`
- **Signature**: `rank().over(partition_by=None, order_by=Expr, descending=False) -> Expr`
- **Behavior**: Assign rank with gaps for ties. Rows with equal values get the same rank; next rank is skipped (1, 2, 2, 4, 5).
- **Parameters**: Use `.over()` to specify `partition_by` (optional), `order_by` (required), `descending` (optional)
- **Returns**: integer expression
- **Exceptions**: `RuntimeError` (no order_by specified)
- **Example**:
```python
from ltseq.expr import rank

# Rank by score (ties get same rank, gaps after)
t.derive(rnk=lambda r: rank().over(order_by=r.score))

# Rank within departments
t.derive(rnk=lambda r: rank().over(partition_by=r.dept, order_by=r.salary, descending=True))
```

#### `dense_rank`
- **Signature**: `dense_rank().over(partition_by=None, order_by=Expr, descending=False) -> Expr`
- **Behavior**: Assign rank without gaps for ties. Rows with equal values get the same rank; next rank is NOT skipped (1, 2, 2, 3, 4).
- **Parameters**: Use `.over()` to specify `partition_by` (optional), `order_by` (required), `descending` (optional)
- **Returns**: integer expression
- **Exceptions**: `RuntimeError` (no order_by specified)
- **Example**:
```python
from ltseq.expr import dense_rank

# Dense rank by score (no gaps after ties)
t.derive(drnk=lambda r: dense_rank().over(order_by=r.score))

# Dense rank within departments
t.derive(drnk=lambda r: dense_rank().over(partition_by=r.dept, order_by=r.salary))
```

#### `ntile`
- **Signature**: `ntile(n: int).over(partition_by=None, order_by=Expr, descending=False) -> Expr`
- **Behavior**: Divide rows into `n` roughly equal buckets (1 to n). Useful for percentiles and quantiles.
- **Parameters**: `n` number of buckets; use `.over()` for partitioning/ordering
- **Returns**: integer expression (bucket number 1 to n)
- **Exceptions**: `ValueError` (n <= 0), `RuntimeError` (no order_by specified)
- **Example**:
```python
from ltseq.expr import ntile

# Divide into quartiles
t.derive(quartile=lambda r: ntile(4).over(order_by=r.score))

# Deciles within groups
t.derive(decile=lambda r: ntile(10).over(partition_by=r.group, order_by=r.value))
```

#### `CallExpr.over` (Window Specification)
- **Signature**: `expr.over(partition_by: Optional[Union[Expr, List[Expr]]] = None, order_by: Optional[Union[Expr, List[Expr]]] = None, descending: Union[bool, List[bool]] = False) -> WindowExpr`
- **Behavior**: Apply window specification to a ranking function
- **Parameters**:
  - `partition_by` column(s) to partition by (single Expr or list of Exprs)
  - `order_by` column(s) to order by (single Expr or list of Exprs)
  - `descending` sort direction (single bool or per-column list of bools)
- **Returns**: `WindowExpr` ready for use in derive()
- **Exceptions**: `TypeError` (invalid partition_by/order_by types)
- **Example**:
```python
# Single column partition and order
t.derive(rn=lambda r: row_number().over(
    partition_by=r.region,
    order_by=r.date,
    descending=True
))

# Multiple columns partition and order
t.derive(rn=lambda r: row_number().over(
    partition_by=[r.region, r.category],
    order_by=[r.date, r.id],
    descending=[True, False]
))
```

### 3.4 Ordered Search

#### `LTSeq.search_first`
- **Signature**: `LTSeq.search_first(predicate: Callable[[Row], Expr]) -> LTSeq`
- **Behavior**: Return the first matching row (single-row LTSeq); can do binary search on sorted data
- **Parameters**: `predicate` row predicate
- **Returns**: single-row `LTSeq` (empty if not found)
- **Exceptions**: `ValueError` (schema not initialized), `TypeError` (invalid predicate), `RuntimeError` (execution failure)
- **Example**:
```python
first_big = t.sort("price").search_first(lambda r: r.price > 100)
```

#### `LTSeq.align`
- **Signature**: `LTSeq.align(ref_sequence: List[Any], key: Callable[[Row], Expr]) -> LTSeq`
- **Behavior**: Align to `ref_sequence` order and insert NULLs for missing keys
- **Parameters**: `ref_sequence` reference key sequence; `key` key extractor
- **Returns**: aligned `LTSeq`
- **Exceptions**: `TypeError` (invalid key), `ValueError` (empty ref_sequence)
- **Example**:
```python
aligned = t.align(["2024-01-01", "2024-01-02"], key=lambda r: r.date)
```

## 4. Ordered Grouping and Procedural Computing

### `LTSeq.group_ordered`
- **Signature**: `LTSeq.group_ordered(key: Callable[[Row], Expr]) -> NestedTable`
- **Behavior**: Group only consecutive equal values; does not reorder
- **Parameters**: `key` grouping key expression
- **Returns**: `NestedTable` (group-level operations)
- **Exceptions**: `ValueError` (schema not initialized), `TypeError` (invalid key), `AttributeError` (column not found)
- **Example**:
```python
groups = t.sort("date").group_ordered(lambda r: r.is_up)
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

### `LTSeq.scan`
- **Signature**: `LTSeq.scan(func: Callable[[State, Row], State], init: Any) -> LTSeq`
- **Behavior**: Stateful scan over current order; outputs per-row accumulated state
- **Parameters**: `func` state transition; `init` initial state
- **Returns**: `LTSeq` (usually a state sequence or appended state column)
- **Exceptions**: `TypeError` (invalid func), `RuntimeError` (execution failure)
- **Example**:
```python
# Compounding
rates = t.sort("date").scan(lambda s, r: s * (1 + r.rate), init=1.0)
```

### `NestedTable` (from `group_ordered` / `group_sorted`)

#### `NestedTable.first`
- **Signature**: `nested.first() -> LTSeq`
- **Behavior**: First row of each group
- **Parameters**: none
- **Returns**: `LTSeq` of first rows
- **Exceptions**: `RuntimeError` (execution failure)
- **Example**:
```python
first_rows = groups.first()
```

#### `NestedTable.last`
- **Signature**: `nested.last() -> LTSeq`
- **Behavior**: Last row of each group
- **Parameters**: none
- **Returns**: `LTSeq` of last rows
- **Exceptions**: `RuntimeError` (execution failure)
- **Example**:
```python
last_rows = groups.last()
```

#### `NestedTable.count`
- **Signature**: `nested.count() -> LTSeq`
- **Behavior**: Group size for each group
- **Parameters**: none
- **Returns**: `LTSeq` with counts
- **Exceptions**: `RuntimeError` (execution failure)
- **Example**:
```python
sizes = groups.count()
```

#### `NestedTable.flatten`
- **Signature**: `nested.flatten() -> LTSeq`
- **Behavior**: Flatten into a table with `__group_id__`
- **Parameters**: none
- **Returns**: flattened `LTSeq`
- **Exceptions**: `RuntimeError` (execution failure)
- **Example**:
```python
flat = groups.flatten()
```

#### `NestedTable.filter`
- **Signature**: `nested.filter(predicate: Callable[[GroupProxy], Expr]) -> NestedTable`
- **Behavior**: Filter groups by group-level predicate
- **Parameters**: `predicate` group predicate (`g` is GroupProxy)
- **Returns**: filtered `NestedTable`
- **Exceptions**: `TypeError` (invalid predicate), `RuntimeError` (execution failure)
- **Example**:
```python
a = groups.filter(lambda g: g.count() > 3)
```

#### `NestedTable.derive`
- **Signature**: `nested.derive(func: Callable[[GroupProxy], Dict[str, Expr]]) -> LTSeq`
- **Behavior**: Derive group-level columns
- **Parameters**: `func` returns a dict of group expressions
- **Returns**: group-level `LTSeq`
- **Exceptions**: `TypeError` (invalid func), `RuntimeError` (execution failure)
- **Example**:
```python
spans = groups.derive(lambda g: {"start": g.first().date, "end": g.last().date})
```

#### `GroupProxy` Common Aggregations
- **Signature**: `g.count()`, `g.first()`, `g.last()`, `g.col.sum()`, `g.col.avg()`, `g.col.min()`, `g.col.max()`
- **Behavior**: Aggregations or first/last within each group
- **Parameters**: none (or column selection)
- **Returns**: group-level expressions
- **Exceptions**: `TypeError` (unsupported column type), `RuntimeError` (execution failure)
- **Example**:
```python
groups.derive(lambda g: {"avg": g.price.avg(), "hi": g.price.max()})
```

#### `GroupProxy.median`
- **Signature**: `g.median(column: str) -> Any`
- **Behavior**: Get the median value of a column in the group
- **Parameters**: `column` column name
- **Returns**: median value (or None if empty)
- **Exceptions**: `TypeError` (non-numeric column)
- **Example**:
```python
groups.derive(lambda g: {"med_price": g.median("price")})
```

#### `GroupProxy.percentile`
- **Signature**: `g.percentile(column: str, p: float) -> Any`
- **Behavior**: Get the p-th percentile value of a column in the group
- **Parameters**: `column` column name; `p` percentile (0-1, e.g., 0.95 for 95th percentile)
- **Returns**: percentile value (uses linear interpolation)
- **Exceptions**: `ValueError` (p not in 0-1), `TypeError` (non-numeric column)
- **Example**:
```python
groups.derive(lambda g: {"p95": g.percentile("latency", 0.95)})
```

#### `GroupProxy.variance` / `GroupProxy.var`
- **Signature**: `g.variance(column: str) -> Any` or `g.var(column: str) -> Any`
- **Behavior**: Get the sample variance of a column in the group
- **Parameters**: `column` column name
- **Returns**: sample variance (None if fewer than 2 values)
- **Exceptions**: `TypeError` (non-numeric column)
- **Example**:
```python
groups.derive(lambda g: {"price_var": g.variance("price")})
```

#### `GroupProxy.std` / `GroupProxy.stddev`
- **Signature**: `g.std(column: str) -> Any` or `g.stddev(column: str) -> Any`
- **Behavior**: Get the sample standard deviation of a column in the group
- **Parameters**: `column` column name
- **Returns**: sample standard deviation (None if fewer than 2 values)
- **Exceptions**: `TypeError` (non-numeric column)
- **Example**:
```python
groups.derive(lambda g: {"price_std": g.std("price")})
```

#### `GroupProxy.mode`
- **Signature**: `g.mode(column: str) -> Any`
- **Behavior**: Get the most frequent value (mode) of a column in the group
- **Parameters**: `column` column name
- **Returns**: most frequent value (one of the modes if multiple)
- **Exceptions**: none
- **Example**:
```python
groups.derive(lambda g: {"common_category": g.mode("category")})
```

#### `GroupProxy.top_k`
- **Signature**: `g.top_k(column: str, k: int) -> List[Any]`
- **Behavior**: Get the top K values from a column in the group (sorted descending)
- **Parameters**: `column` column name; `k` number of values
- **Returns**: list of top K values
- **Exceptions**: `ValueError` (k <= 0)
- **Example**:
```python
groups.derive(lambda g: {"top_3_prices": g.top_k("price", 3)})
```

#### `GroupProxy.all`
- **Signature**: `g.all(predicate: Callable[[Row], Expr]) -> Expr`
- **Behavior**: Returns True if predicate holds for ALL rows in the group
- **Parameters**: `predicate` row-level predicate function
- **Returns**: boolean expression
- **Exceptions**: `TypeError` (invalid predicate), `RuntimeError` (execution failure)
- **Example**:
```python
# Filter groups where ALL rows have positive amounts
groups.filter(lambda g: g.all(lambda r: r.amount > 0))

# Derive a flag for groups where all items are in stock
groups.derive(lambda g: {"all_in_stock": g.all(lambda r: r.quantity > 0)})
```

#### `GroupProxy.any`
- **Signature**: `g.any(predicate: Callable[[Row], Expr]) -> Expr`
- **Behavior**: Returns True if predicate holds for AT LEAST ONE row in the group
- **Parameters**: `predicate` row-level predicate function
- **Returns**: boolean expression
- **Exceptions**: `TypeError` (invalid predicate), `RuntimeError` (execution failure)
- **Example**:
```python
# Filter groups where ANY row has an error
groups.filter(lambda g: g.any(lambda r: r.status == "error"))

# Derive a flag for groups containing VIP customers
groups.derive(lambda g: {"has_vip": g.any(lambda r: r.is_vip == True)})
```

#### `GroupProxy.none`
- **Signature**: `g.none(predicate: Callable[[Row], Expr]) -> Expr`
- **Behavior**: Returns True if predicate holds for NO rows in the group (equivalent to `not any`)
- **Parameters**: `predicate` row-level predicate function
- **Returns**: boolean expression
- **Exceptions**: `TypeError` (invalid predicate), `RuntimeError` (execution failure)
- **Example**:
```python
# Filter groups where NO rows are marked as deleted
groups.filter(lambda g: g.none(lambda r: r.is_deleted == True))

# Derive a flag for groups with no null values
groups.derive(lambda g: {"complete": g.none(lambda r: r.value.is_null())})
```

#### `GroupProxy.count_if`
- **Signature**: `g.count_if(predicate: Callable[[Row], bool]) -> int`
- **Behavior**: Count rows in the group where predicate is True
- **Parameters**: `predicate` row-level predicate function
- **Returns**: integer count
- **Exceptions**: `TypeError` (invalid predicate)
- **Example**:
```python
groups.derive(lambda g: {"high_value_count": g.count_if(lambda r: r.price > 100)})
```

#### `GroupProxy.sum_if`
- **Signature**: `g.sum_if(predicate: Callable[[Row], bool], column: str) -> Any`
- **Behavior**: Sum column values in the group where predicate is True
- **Parameters**: `predicate` row-level predicate function; `column` column name to sum
- **Returns**: sum of matching values
- **Exceptions**: `TypeError` (invalid predicate or non-numeric column)
- **Example**:
```python
groups.derive(lambda g: {"vip_sales": g.sum_if(lambda r: r.is_vip, "amount")})
```

#### `GroupProxy.avg_if`
- **Signature**: `g.avg_if(predicate: Callable[[Row], bool], column: str) -> Any`
- **Behavior**: Average column values in the group where predicate is True
- **Parameters**: `predicate` row-level predicate function; `column` column name to average
- **Returns**: average of matching values (None if no matches)
- **Exceptions**: `TypeError` (invalid predicate or non-numeric column)
- **Example**:
```python
groups.derive(lambda g: {"active_avg_score": g.avg_if(lambda r: r.status == "active", "score")})
```

#### `GroupProxy.min_if`
- **Signature**: `g.min_if(predicate: Callable[[Row], bool], column: str) -> Any`
- **Behavior**: Minimum column value in the group where predicate is True
- **Parameters**: `predicate` row-level predicate function; `column` column name
- **Returns**: minimum of matching values (None if no matches)
- **Exceptions**: `TypeError` (invalid predicate)
- **Example**:
```python
groups.derive(lambda g: {"min_valid_price": g.min_if(lambda r: r.is_valid, "price")})
```

#### `GroupProxy.max_if`
- **Signature**: `g.max_if(predicate: Callable[[Row], bool], column: str) -> Any`
- **Behavior**: Maximum column value in the group where predicate is True
- **Parameters**: `predicate` row-level predicate function; `column` column name
- **Returns**: maximum of matching values (None if no matches)
- **Exceptions**: `TypeError` (invalid predicate)
- **Example**:
```python
groups.derive(lambda g: {"max_valid_price": g.max_if(lambda r: r.is_valid, "price")})
```

## 5. Set Algebra

### `LTSeq.union`
- **Signature**: `LTSeq.union(other: LTSeq) -> LTSeq`
- **Behavior**: Vertical concatenation (similar to SQL UNION ALL)
- **Parameters**: `other` another LTSeq with same schema
- **Returns**: combined `LTSeq`
- **Exceptions**: `TypeError` (other is not LTSeq), `ValueError` (schema mismatch)
- **Example**:
```python
combined = t1.union(t2)
```

### `LTSeq.intersect`
- **Signature**: `LTSeq.intersect(other: LTSeq, on: Optional[Callable] = None) -> LTSeq`
- **Behavior**: Intersection of two tables
- **Parameters**: `other` another table; `on` key selector (None means all columns)
- **Returns**: intersection `LTSeq`
- **Exceptions**: `TypeError` (other not LTSeq or invalid on), `ValueError` (schema not initialized)
- **Example**:
```python
common = t1.intersect(t2, on=lambda r: r.id)
```

### `LTSeq.diff` (set difference)
- **Signature**: `LTSeq.diff(other: LTSeq, on: Optional[Callable] = None) -> LTSeq`
- **Behavior**: Rows in left table but not in right table
- **Parameters**: `other` another table; `on` key selector
- **Returns**: difference `LTSeq`
- **SQL Equivalent**: `EXCEPT` / `MINUS`
- **Exceptions**: `TypeError` (other not LTSeq or invalid on), `ValueError` (schema not initialized)
- **Example**:
```python
only_left = t1.diff(t2, on=lambda r: r.id)
```

### `LTSeq.is_subset`
- **Signature**: `LTSeq.is_subset(other: LTSeq, on: Optional[Callable] = None) -> bool`
- **Behavior**: Check if this table is a subset of another
- **Parameters**: `other` another table; `on` key selector
- **Returns**: `bool`
- **Exceptions**: `TypeError` (other not LTSeq or invalid on), `ValueError` (schema not initialized)
- **Example**:
```python
flag = t_small.is_subset(t_big, on=lambda r: r.id)
```

## 6. Association and Joins

### `LTSeq.join`
- **Signature**: `LTSeq.join(other: LTSeq, on: Callable, how: str = "inner") -> LTSeq`
- **Behavior**: Standard hash join; no sorting required
- **Parameters**: `other` other table; `on` join condition; `how` in {inner,left,right,full}
- **Returns**: joined `LTSeq` (conflicting columns get a suffix)
- **Exceptions**: `TypeError` (invalid other/on), `ValueError` (invalid how or schema not initialized)
- **Example**:
```python
joined = users.join(orders, on=lambda u, o: u.id == o.user_id, how="left")
```

### `LTSeq.join_merge`
- **Signature**: `LTSeq.join_merge(other: LTSeq, on: Callable, join_type: str = "inner") -> LTSeq`
- **Behavior**: Merge join; requires both sides sorted by join key; O(N+M)
- **Parameters**: `other` other table; `on` join condition; `join_type` in {inner,left,right,full}
- **Returns**: joined `LTSeq`
- **Exceptions**: `TypeError` (invalid other/on), `ValueError` (invalid join_type or unsorted)
- **Example**:
```python
result = t1.sort("id").join_merge(t2.sort("id"), on=lambda a, b: a.id == b.id)
```

### `LTSeq.join_sorted`
- **Signature**: `LTSeq.join_sorted(other: LTSeq, on: Union[str, List[str]], how: str = "inner") -> LTSeq`
- **Behavior**: Merge join with strict validation that both tables are sorted by join keys. Validates sort order using `is_sorted_by()` before executing.
- **Parameters**: `other` other table (must be sorted by join key); `on` join column name(s); `how` in {inner,left,right,full}
- **Returns**: joined `LTSeq`
- **Exceptions**: `TypeError` (invalid other/on), `ValueError` (tables not sorted by join keys, or sort directions don't match)
- **Example**:
```python
# Both tables must be sorted by their join keys
t1_sorted = t1.sort("id")
t2_sorted = t2.sort("id")
result = t1_sorted.join_sorted(t2_sorted, on="id")

# Composite keys: both tables must be sorted by all join keys
t1_sorted = t1.sort("region", "date")
t2_sorted = t2.sort("region", "date")
result = t1_sorted.join_sorted(t2_sorted, on=["region", "date"])

# Descending sort also supported (must match)
t1_desc = t1.sort("id", desc=True)
t2_desc = t2.sort("id", desc=True)
result = t1_desc.join_sorted(t2_desc, on="id")

# Raises ValueError if not sorted correctly
t_unsorted = LTSeq.read_csv("data.csv")
t_unsorted.join_sorted(t2_sorted, on="id")  # ValueError!
```

### `LTSeq.asof_join`
- **Signature**: `LTSeq.asof_join(other: LTSeq, on: Callable, direction: str = "backward") -> LTSeq`
- **Behavior**: As-of join for nearest time match
- **Parameters**: `other` other table; `on` join condition; `direction` in {"backward","forward","nearest"}
- **Returns**: as-of joined `LTSeq`
- **Exceptions**: `TypeError` (invalid other/on), `ValueError` (invalid direction or unsorted)
- **Example**:
```python
quotes = trades.asof_join(quotes, on=lambda t, q: t.time >= q.time, direction="backward")
```

### `LTSeq.semi_join`
- **Signature**: `LTSeq.semi_join(other: LTSeq, on: Callable) -> LTSeq`
- **Behavior**: Return rows from left table where keys exist in right table. Returns only left table columns with no duplicates from multiple matches.
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
- **Behavior**: Return rows from left table where keys do NOT exist in right table. Returns only left table columns.
- **Parameters**: `other` right table to match against; `on` join condition lambda
- **Returns**: `LTSeq` with non-matching rows from left table
- **Exceptions**: `TypeError` (invalid other/on), `ValueError` (schema not initialized)
- **Example**:
```python
# Users who have never placed an order
inactive_users = users.anti_join(orders, on=lambda u, o: u.id == o.user_id)
```

### `LTSeq.link`
- **Signature**: `LTSeq.link(target_table: LTSeq, on: Callable, as_: str, join_type: str = "inner") -> LinkedTable`
- **Behavior**: Pointer-style association; not materialized; access via alias
- **Parameters**: `target_table` target table; `on` join condition; `as_` alias; `join_type` join type
- **Returns**: `LinkedTable` (can be used like LTSeq)
- **Exceptions**: `TypeError` (invalid on), `ValueError` (invalid join_type or schema not initialized)
- **Example**:
```python
linked = orders.link(products, on=lambda o, p: o.product_id == p.id, as_="prod")
result = linked.select(lambda r: [r.id, r.prod.name, r.prod.price])
```

### `LTSeq.lookup` (table-level addressization)
- **Signature**: `LTSeq.lookup(dim_table: LTSeq, on: Callable, as_: str) -> LTSeq`
- **Behavior**: Load dim table into memory and build a direct index for fast lookups
- **Parameters**: `dim_table` dimension table; `on` join condition; `as_` alias
- **Returns**: `LTSeq` with internal pointers/indexes
- **Exceptions**: `MemoryError`/`RuntimeError` (dim table too large or build failure), `TypeError` (invalid params)
- **Example**:
```python
fact = orders.lookup(products, on=lambda o, p: o.product_id == p.id, as_="prod")
```

### Join Strategy Summary

| Method | Best Use Case | Algorithm | SQL Equivalent |
| --- | --- | --- | --- |
| `join` | Unsorted general data | Hash Join | `JOIN` |
| `join_sorted` / `join_merge` | Pre-sorted large tables | Merge Join | `JOIN` (optimized) |
| `semi_join` | Filter by key existence | Hash Semi-Join | `WHERE EXISTS` |
| `anti_join` | Filter by key non-existence | Hash Anti-Join | `WHERE NOT EXISTS` |
| `link` | Fact-to-dimension pointer access | Pointer | `LEFT JOIN` (lazy) |
| `lookup` | Fact + small dimension in memory | Direct Address | `LEFT JOIN` (indexed) |
| `asof_join` | Financial time series | Ordered Search | `LATERAL JOIN` |

## 7. Aggregation, Partitioning, Pivot

### `LTSeq.agg`
- **Signature**: `LTSeq.agg(by: Optional[Callable] = None, **aggs: Callable[[GroupProxy], Expr]) -> LTSeq`
- **Behavior**: Grouped aggregation (or full-table aggregation); one row per group
- **Parameters**: `by` grouping key (single column or list); `aggs` aggregation expressions
- **Returns**: aggregated `LTSeq`
- **Exceptions**: `ValueError` (schema not initialized), `TypeError` (invalid expressions)
- **Example**:
```python
summary = t.agg(by=lambda r: r.region, total=lambda g: g.sales.sum())
```

### `top_k` (aggregate function)
- **Signature**: `top_k(col: Union[Expr, Callable], k: int) -> List[Value]`
- **Behavior**: Return Top-K of a column (aggregate semantics)
- **Parameters**: `col` target column (expression or callable); `k` Top count
- **Returns**: Top-K list
- **Exceptions**: `ValueError` (k <= 0), `TypeError` (invalid col)
- **Example**:
```python
# Used inside agg
result = t.agg(top_prices=lambda g: top_k(g.price, 5))
```

### `LTSeq.partition`
- **Signature**: `LTSeq.partition(*cols: str) -> PartitionedTable` or `LTSeq.partition(by: Callable) -> PartitionedTable`
- **Behavior**: Split into sub-tables by key (no aggregation)
- **Parameters**: column names or lambda
- **Returns**: `PartitionedTable` (key -> LTSeq)
- **Exceptions**: `TypeError` (invalid params), `AttributeError` (column not found), `ValueError` (schema not initialized)
- **Example**:
```python
parts = t.partition("region")
west = parts["West"]
```

### `LTSeq.pivot`
- **Signature**: `LTSeq.pivot(index: Union[str, List[str]], columns: str, values: str, agg_fn: str = "sum") -> LTSeq`
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
- **Exceptions**: `TypeError` (condition not boolean)
- **Example**:
```python
from ltseq.expr import if_else
status = if_else(r.amount > 100, "VIP", "Normal")
```

### `r.col.fill_null`
- **Signature**: `r.col.fill_null(default: Any) -> Expr`
- **Behavior**: NULL fill (SQL COALESCE)
- **Parameters**: `default` fallback value
- **Returns**: expression
- **Exceptions**: `TypeError` (incompatible default type)
- **Example**:
```python
safe_price = r.price.fill_null(0)
```

### `r.col.is_null`
- **Signature**: `r.col.is_null() -> Expr`
- **Behavior**: NULL check
- **Parameters**: none
- **Returns**: boolean expression
- **Exceptions**: none (may fail at execution for unsupported types)
- **Example**:
```python
missing = t.filter(lambda r: r.email.is_null())
```

### `r.col.is_not_null`
- **Signature**: `r.col.is_not_null() -> Expr`
- **Behavior**: NOT NULL check
- **Parameters**: none
- **Returns**: boolean expression
- **Exceptions**: none (may fail at execution for unsupported types)
- **Example**:
```python
valid = t.filter(lambda r: r.email.is_not_null())
```

### String Operations (`r.col.s.*`)

#### `contains`
- **Signature**: `r.col.s.contains(pattern: str) -> Expr`
- **Behavior**: substring containment
- **Parameters**: `pattern` substring
- **Returns**: boolean expression
- **Exceptions**: `TypeError` (non-string column)
- **Example**:
```python
gmail = t.filter(lambda r: r.email.s.contains("gmail"))
```

#### `starts_with`
- **Signature**: `r.col.s.starts_with(prefix: str) -> Expr`
- **Behavior**: prefix match
- **Parameters**: `prefix` prefix string
- **Returns**: boolean expression
- **Exceptions**: `TypeError` (non-string column)
- **Example**:
```python
orders = t.filter(lambda r: r.code.s.starts_with("ORD"))
```

#### `ends_with`
- **Signature**: `r.col.s.ends_with(suffix: str) -> Expr`
- **Behavior**: suffix match
- **Parameters**: `suffix` suffix string
- **Returns**: boolean expression
- **Exceptions**: `TypeError` (non-string column)
- **Example**:
```python
pdfs = t.filter(lambda r: r.filename.s.ends_with(".pdf"))
```

#### `lower`
- **Signature**: `r.col.s.lower() -> Expr`
- **Behavior**: lowercase
- **Parameters**: none
- **Returns**: string expression
- **Exceptions**: `TypeError` (non-string column)
- **Example**:
```python
normalized = t.derive(email_lower=lambda r: r.email.s.lower())
```

#### `upper`
- **Signature**: `r.col.s.upper() -> Expr`
- **Behavior**: uppercase
- **Parameters**: none
- **Returns**: string expression
- **Exceptions**: `TypeError` (non-string column)
- **Example**:
```python
normalized = t.derive(email_upper=lambda r: r.email.s.upper())
```

#### `strip`
- **Signature**: `r.col.s.strip() -> Expr`
- **Behavior**: trim whitespace
- **Parameters**: none
- **Returns**: string expression
- **Exceptions**: `TypeError` (non-string column)
- **Example**:
```python
clean = t.derive(name_clean=lambda r: r.name.s.strip())
```

#### `len`
- **Signature**: `r.col.s.len() -> Expr`
- **Behavior**: string length
- **Parameters**: none
- **Returns**: integer expression
- **Exceptions**: `TypeError` (non-string column)
- **Example**:
```python
long_names = t.filter(lambda r: r.name.s.len() > 50)
```

#### `slice`
- **Signature**: `r.col.s.slice(start: int, length: int) -> Expr`
- **Behavior**: substring slicing
- **Parameters**: `start` start index (0-based); `length` length
- **Returns**: string expression
- **Exceptions**: `ValueError` (invalid start/length), `TypeError` (non-string column)
- **Example**:
```python
year = t.derive(year=lambda r: r.date.s.slice(0, 4))
```

#### `regex_match`
- **Signature**: `r.col.s.regex_match(pattern: str) -> Expr`
- **Behavior**: regex match (boolean)
- **Parameters**: `pattern` regex
- **Returns**: boolean expression
- **Exceptions**: `ValueError` (invalid regex), `TypeError` (non-string column)
- **Example**:
```python
valid = t.filter(lambda r: r.email.s.regex_match(r"^[a-z]+@"))
```

#### `replace`
- **Signature**: `r.col.s.replace(old: str, new: str) -> Expr`
- **Behavior**: Replace all occurrences of a substring with another string
- **Parameters**: `old` substring to find; `new` replacement string
- **Returns**: string expression
- **Exceptions**: `TypeError` (non-string column)
- **Example**:
```python
clean = t.derive(clean_name=lambda r: r.name.s.replace("-", "_"))
```

#### `concat`
- **Signature**: `r.col.s.concat(*others) -> Expr`
- **Behavior**: Concatenate this string with other strings or column expressions
- **Parameters**: `others` strings or column expressions to concatenate
- **Returns**: string expression
- **Exceptions**: `TypeError` (non-string arguments)
- **Example**:
```python
# Concatenate with literal string
greeting = t.derive(msg=lambda r: r.name.s.concat(" says hello"))

# Concatenate multiple columns
full = t.derive(full_name=lambda r: r.first.s.concat(" ", r.last))
```

#### `pad_left`
- **Signature**: `r.col.s.pad_left(width: int, char: str = " ") -> Expr`
- **Behavior**: Pad string on the left to reach specified width. Note: truncates if string is longer than width (SQL LPAD behavior).
- **Parameters**: `width` target width; `char` padding character (default: space)
- **Returns**: string expression
- **Exceptions**: `TypeError` (non-string column), `ValueError` (invalid width)
- **Example**:
```python
# Zero-pad IDs to 5 characters
padded = t.derive(padded_id=lambda r: r.id.s.pad_left(5, "0"))
```

#### `pad_right`
- **Signature**: `r.col.s.pad_right(width: int, char: str = " ") -> Expr`
- **Behavior**: Pad string on the right to reach specified width. Note: truncates if string is longer than width (SQL RPAD behavior).
- **Parameters**: `width` target width; `char` padding character (default: space)
- **Returns**: string expression
- **Exceptions**: `TypeError` (non-string column), `ValueError` (invalid width)
- **Example**:
```python
# Pad names with dots to 20 characters
padded = t.derive(padded_name=lambda r: r.name.s.pad_right(20, "."))
```

#### `split`
- **Signature**: `r.col.s.split(delimiter: str, index: int) -> Expr`
- **Behavior**: Split string by delimiter and return the part at specified index. Index is 1-based (1 = first part) to match SQL SPLIT_PART convention.
- **Parameters**: `delimiter` string to split on; `index` 1-based index of part to return
- **Returns**: string expression (empty string if index out of range)
- **Exceptions**: `TypeError` (non-string column), `ValueError` (index <= 0)
- **Example**:
```python
# Get domain from email "user@example.com"
domain = t.derive(domain=lambda r: r.email.s.split("@", 2))

# Get first name from "first.last" format
first = t.derive(first_name=lambda r: r.username.s.split(".", 1))
```

### Temporal Operations (`r.col.dt.*`)

#### `year` / `month` / `day`
- **Signature**: `r.col.dt.year() -> Expr` (same for month/day)
- **Behavior**: extract date components
- **Parameters**: none
- **Returns**: integer expression
- **Exceptions**: `TypeError` (non-date column)
- **Example**:
```python
by_date = t.derive(year=lambda r: r.date.dt.year())
```

#### `hour` / `minute` / `second`
- **Signature**: `r.col.dt.hour() -> Expr` (same for minute/second)
- **Behavior**: extract time components
- **Parameters**: none
- **Returns**: integer expression
- **Exceptions**: `TypeError` (non-time column)
- **Example**:
```python
with_time = t.derive(hour=lambda r: r.ts.dt.hour())
```

#### `add`
- **Signature**: `r.col.dt.add(days: int = 0, months: int = 0, years: int = 0) -> Expr`
- **Behavior**: date arithmetic
- **Parameters**: `days/months/years` offsets
- **Returns**: date expression
- **Exceptions**: `TypeError` (non-date column)
- **Example**:
```python
delivery = t.derive(delivery=lambda r: r.order_date.dt.add(days=5))
```

#### `diff`
- **Signature**: `r.col.dt.diff(other: Expr) -> Expr`
- **Behavior**: date difference (days)
- **Parameters**: `other` other date expression
- **Returns**: integer expression
- **Exceptions**: `TypeError` (non-date column)
- **Example**:
```python
age_days = t.derive(age=lambda r: r.end_date.dt.diff(r.start_date))
```

### `r.col.lookup` (expression-level lookup)
- **Signature**: `r.col.lookup(target_table: LTSeq, column: str, join_key: Optional[str] = None) -> Expr`
- **Behavior**: Lightweight lookup inside expressions, returning a single column value
- **Parameters**: `target_table` target table; `column` output column; `join_key` join key in target table
- **Returns**: lookup expression
- **Exceptions**: `TypeError` (invalid params), `RuntimeError` (execution failure)
- **Example**:
```python
enriched = orders.derive(product_name=lambda r: r.product_id.lookup(products, "name"))
```

### Conditional Aggregation Functions

Module-level functions for conditional aggregation in `.agg()` contexts. These take expression predicates (not lambdas) and are designed for use with the GroupProxy `g` parameter.

#### `count_if`
- **Signature**: `count_if(predicate: Expr) -> Expr`
- **Behavior**: Count rows where predicate expression is True
- **Parameters**: `predicate` boolean expression
- **Returns**: integer expression
- **Exceptions**: `TypeError` (non-boolean predicate)
- **Example**:
```python
from ltseq.expr import count_if

result = t.agg(by=lambda r: r.region, high_count=lambda g: count_if(g.price > 100))
```

#### `sum_if`
- **Signature**: `sum_if(predicate: Expr, column: Expr) -> Expr`
- **Behavior**: Sum column values where predicate expression is True
- **Parameters**: `predicate` boolean expression; `column` column expression to sum
- **Returns**: numeric expression
- **Exceptions**: `TypeError` (non-boolean predicate or non-numeric column)
- **Example**:
```python
from ltseq.expr import sum_if

result = t.agg(by=lambda r: r.region, high_sales=lambda g: sum_if(g.price > 100, g.quantity))
```

#### `avg_if`
- **Signature**: `avg_if(predicate: Expr, column: Expr) -> Expr`
- **Behavior**: Average column values where predicate expression is True
- **Parameters**: `predicate` boolean expression; `column` column expression to average
- **Returns**: numeric expression
- **Exceptions**: `TypeError` (non-boolean predicate or non-numeric column)
- **Example**:
```python
from ltseq.expr import avg_if

result = t.agg(by=lambda r: r.region, avg_high=lambda g: avg_if(g.price > 100, g.sales))
```

#### `min_if`
- **Signature**: `min_if(predicate: Expr, column: Expr) -> Expr`
- **Behavior**: Minimum column value where predicate expression is True
- **Parameters**: `predicate` boolean expression; `column` column expression
- **Returns**: expression of column type
- **Exceptions**: `TypeError` (non-boolean predicate)
- **Example**:
```python
from ltseq.expr import min_if

result = t.agg(by=lambda r: r.region, min_active=lambda g: min_if(g.is_active, g.score))
```

#### `max_if`
- **Signature**: `max_if(predicate: Expr, column: Expr) -> Expr`
- **Behavior**: Maximum column value where predicate expression is True
- **Parameters**: `predicate` boolean expression; `column` column expression
- **Returns**: expression of column type
- **Exceptions**: `TypeError` (non-boolean predicate)
- **Example**:
```python
from ltseq.expr import max_if

result = t.agg(by=lambda r: r.region, max_active=lambda g: max_if(g.is_active, g.score))
```

## 9. End-to-End Examples

### Ordered Computing + Consecutive Grouping
```python
from ltseq import LTSeq

# Task: find intervals where a stock rose for more than 3 consecutive days
result = (
    LTSeq.read_csv("stock.csv")
    .sort(lambda r: r.date)
    .derive(lambda r: {"is_up": r.price > r.price.shift(1)})
    .group_ordered(lambda r: r.is_up)
    .filter(lambda g: (g.first().is_up == True) & (g.count() > 3))
    .derive(lambda g: {
        "start": g.first().date,
        "end": g.last().date,
        "gain": (g.last().price - g.first().price) / g.first().price,
    })
)
```

### String + Temporal + Conditional + Lookup
```python
from ltseq import LTSeq
from ltseq.expr import if_else

orders = LTSeq.read_csv("orders.csv")
products = LTSeq.read_csv("products.csv")

result = orders.derive(
    order_id_clean=lambda r: r.order_id.s.strip(),
    product_name=lambda r: r.product_id.lookup(products, "name"),
    order_year=lambda r: r.order_date.dt.year(),
    status=lambda r: if_else(r.quantity > 10, "Bulk", "Standard"),
)
```

## 10. Execution and Performance Notes

- All expressions are serialized and pushed down to Rust/DataFusion, not evaluated row-by-row in Python
- String/temporal/NULL extensions map to SQL/DataFusion functions
- `to_cursor` enables streaming for very large datasets

## Appendix A: Migrating from Pandas

| Pandas | LTSeq | Notes |
|--------|-------|-------|
| `df[df.age > 18]` | `t.filter(lambda r: r.age > 18)` | Lambda captures expression |
| `df[['id', 'name']]` | `t.select("id", "name")` | |
| `df.assign(total=df.a + df.b)` | `t.derive(total=lambda r: r.a + r.b)` | |
| `df.sort_values('date')` | `t.sort("date")` | |
| `df.sort_values('date', ascending=False)` | `t.sort("date", desc=True)` | |
| `df.drop_duplicates('id')` | `t.distinct("id")` | |
| `df.groupby('region').agg({'sales': 'sum'})` | `t.agg(by=lambda r: r.region, sales=lambda g: g.sales.sum())` | |
| `df.merge(df2, on='id')` | `t.join(t2, on=lambda a, b: a.id == b.id)` | |
| `df.merge(df2, on='id', how='left')` | `t.join(t2, on=lambda a, b: a.id == b.id, how="left")` | |
| `df['col'].shift(1)` | `t.sort(...).derive(prev=lambda r: r.col.shift(1))` | Requires sort |
| `df['col'].rolling(5).mean()` | `t.sort(...).derive(ma=lambda r: r.col.rolling(5).mean())` | Requires sort |
| `df['col'].diff()` | `t.sort(...).derive(d=lambda r: r.col.diff())` | Requires sort |
| `df['col'].cumsum()` | `t.sort(...).cum_sum("col")` | Requires sort |
| `df.head(10)` | `t.head(10)` | |
| `df.tail(10)` | `t.tail(10)` | |
| `df.to_dict('records')` | `t.collect()` | |
| `len(df)` | `t.count()` | |
| `df.columns.tolist()` | `t.columns` | |
| `df.isna()` | `t.filter(lambda r: r.col.is_null())` | Per-column |
| `df.fillna(0)` | `t.derive(col=lambda r: r.col.fill_null(0))` | Per-column |
