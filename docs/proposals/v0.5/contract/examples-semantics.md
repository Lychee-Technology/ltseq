<!-- v0.5-modular:header -->

# v0.5 contract: Examples: streaming, values, errors and interchange (§23.10–§23.18)

[Index](../README.md) › Contract · Previous: [Examples: ordered computation, joins and state (§23–§23.9)](examples-sequences.md) · Next: [Acceptance criteria and test matrix (§24)](acceptance.md)

**Normative.** One module of the LTSeq v0.5 public API contract. It uses the BCP 14 key words as the contract's [status block](../README.md#ltseq-v05-public-api-contract) declares, and [§1.6] names the section that owns each cross-cutting rule.

**Scope.** Examples 23.10–23.18: streaming and interchange, NULL and NaN, checked and exact arithmetic, time zones and DST, demand and error stages, order state, set and bag operations, a lazy CSV pipeline, and the Arrow and pandas round trip. The imports and stage terms they use are defined at the start of [§23].

**Most cited from here.** [§16] Output and interchange · [§7] Basic table operations · [§9] Ordering contract · [§14] Aggregation, partitioning and pivot

## 23. End-to-end examples (continued)

<!-- /v0.5-modular:header -->

### 23.10 Streaming and interchange

```python
ticks = LTSeq.read_parquet("ticks/").assume_sorted("ts")
live = ticks.filter(lambda r: r.qty > 0).derive(notional=lambda r: r.price * r.qty)

with live.to_batches(batch_size=65_536) as reader:   # pa.RecordBatchReader; one new execution
    assert reader.schema.equals(live.schema)
    for batch in reader:
        assert 0 < batch.num_rows <= 65_536          # batches in ts order, none empty
        ...

tbl = pa.table(live)                                 # PyCapsule stream (§16.4), same rows as live.to_arrow(); holds them all

import polars as pl
df = pl.from_dataframe(live)                         # Polars accepts PyCapsule-interface objects

import duckdb
reader = live.to_batches()
duckdb.sql("SELECT sym, sum(notional) AS n FROM reader GROUP BY sym").fetchall()   # DuckDB scans a RecordBatchReader
```

The pipeline is in the MUST-stream list of [§21.1], so the `to_batches` loop, which drops each batch, holds memory bounded however large `ticks/` is, and so does LTSeq's side of the `pa.table`, Polars and DuckDB reads. What each library keeps is its own: `pa.table` and Polars build the whole result, whose size grows with `ticks/`. If two adjacent rows violate `ts` order, or `price * qty` overflows, the read that reaches that row raises `OrderViolationError` or `ArithmeticOverflowError`. The `pa.table`, Polars and DuckDB lines read through the Arrow C stream interface, which carries no exception class, so each library raises its own exception; pyarrow's is `ArrowInvalid`, whose message starts with that class name when it reads `live` itself ([§16.4], [§15.1]). A consumer that reads to the end reads every row of `ticks/`, because the filter sits above `assume_sorted` ([§9.5]) and no row group is pruned; a selective filter meant to prune row groups goes first: `LTSeq.read_parquet("ticks/").filter(...).assume_sorted("ts")`. Leaving the `with` block early stops the execution. The Polars and DuckDB lines rely on their documented support (https://docs.pola.rs/api/python/stable/reference/api/polars.from_dataframe.html, https://duckdb.org/docs/current/guides/python/sql_on_arrow.html), not on anything LTSeq-specific.

### 23.11 NULL and NaN

```python
nan = float("nan")
t = LTSeq.from_dict({"k": ["a", "b", "c", "d", "e", "f"],
                     "x": [1.0, nan, None, nan, -0.0, 0.0]})
key = lambda u: [d["k"] for d in u.to_dicts()]

key(t.filter(lambda r: r.x == None))           == ["c"]
key(t.filter(lambda r: r.x > 0))               == ["a", "b", "d"]     # NaN is greater than every number
key(t.filter(lambda r: ~(r.x > 0)))            == ["e", "f"]          # NULL is in neither result
key(t.filter(lambda r: r.x == nan))            == ["b", "d"]
key(t.filter(lambda r: r.x == 0.0))            == ["e", "f"]          # -0.0 equals 0.0
[d["m"] for d in t.select(m=lambda r: r.x.is_in([None, 0.0])).to_dicts()] == [
    False, False, True, False, True, True]                           # never NULL
key(t.sort("x"))                               == ["e", "f", "a", "b", "d", "c"]
key(t.sort("x", descending=True))              == ["b", "d", "a", "e", "f", "c"]   # NULL still last
t.group_by("x").agg(n=lambda g: g.count()).sort("x").to_dicts() == [
    {"x": 0.0, "n": 2}, {"x": 1.0, "n": 1}, {"x": nan, "n": 2}, {"x": None, "n": 1}]  # compare NaN as NaN
t.agg(s=lambda g: g.x.sum(), mx=lambda g: g.x.max(), n=lambda g: g.x.count()).to_dicts()
                                               # [{"s": nan, "mx": nan, "n": 5}]
t.select(y=lambda r: r.x < None)               # LTSeqTypeError at plan time
```

The merged zero group reports `0.0` ([§18], Representatives). `t.sort("x")` keeps `e` (−0.0) before `f` (0.0) because the sort is stable and they are equal.

### 23.12 Checked and exact arithmetic

```python
nan = float("nan")
t = LTSeq.from_dict({"i": [2**62, 2**62], "j": [2**53 + 1, 3], "f": [2.0**53, 3.0]})

t.agg(s=lambda g: g.i.sum()).to_dicts()        # ArithmeticOverflowError during execution
t.agg(s=lambda g: g.i.cast(pa.decimal128(38, 0)).sum()).to_dicts() == [{"s": Decimal("9223372036854775808")}]
t.select(p=lambda r: r.j * 2**62).to_dicts()   # ArithmeticOverflowError during execution

[d["j"] for d in t.filter(lambda r: r.j == r.f).to_dicts()] == [3]    # 2**53 + 1 != 2.0**53
t.filter(lambda r: r.j == 1.5).count() == 0                            # exact: no int equals 1.5
t.filter(lambda r: r.j > nan)                  # LTSeqValueError at plan time: NaN literal next to an integer
t.derive(j=lambda r: r.j.fill_null(1.5))       # CastError at plan time
t.derive(j=lambda r: r.j.fill_null(0.0)).schema.field("j").type == pa.int64()
t.select(a=lambda r: if_else(r.j > 3, 2**53 + 1, 1.5))   # CastError at plan time (#248)
t.select(y=lambda r: (2**53 + 1) - r.f).to_dicts()[0] == {"y": 0.0}   # not 1: the literal rounds to 2.0**53, as a column would (§17.4)

t.select(q=lambda r: r.j / 0).to_dicts()       # DivisionByZeroError during execution, as Python's 1 / 0 raises
[d["q"] for d in t.select(q=lambda r: r.j / 0.0).to_dicts()] == [float("inf"), float("inf")]   # a float operand: IEEE
LTSeq.from_dict({"n": [1]}).select(q=lambda r: r.n / 2).to_dicts() == [{"q": 0.5}]
t.select(q=lambda r: r.j // 0).to_dicts()      # DivisionByZeroError during execution
LTSeq.from_dict({"n": [-7]}).select(a=lambda r: r.n // 2, b=lambda r: r.n % 2).to_dicts() == [{"a": -4, "b": 1}]
```

Today the first line returns `-9223372036854775808` because DataFusion's `SUM` wraps (#221).

### 23.13 Time zones and DST

```python
paris = ZoneInfo("Europe/Paris")
t = LTSeq.from_dict({"local": [datetime(2024, 3, 30, 2, 30), datetime(2024, 10, 27, 2, 30)]})

t.derive(ts=lambda r: r.local.dt.replace_time_zone("Europe/Paris")).to_dicts()
                       # LTSeqValueError during execution: 2024-10-27 02:30 is ambiguous in Europe/Paris
p = t.head(1).derive(ts=lambda r: r.local.dt.replace_time_zone("Europe/Paris"))
p.schema.field("ts").type == pa.timestamp("us", tz="Europe/Paris")
p.select(n=lambda r: r.ts.dt.add(days=1)).to_dicts()
                       # LTSeqValueError during execution: 2024-03-31 02:30 does not exist in Europe/Paris
p.select(n=lambda r: r.ts + timedelta(days=1)).to_dicts() == [
    {"n": datetime(2024, 3, 31, 3, 30, tzinfo=paris)}]          # 24 hours later as an instant
p.filter(lambda r: r.local < r.ts)             # LTSeqTypeError at plan time: naive vs aware

u = LTSeq.from_dict({"ny":    [datetime(2024, 1, 1, 9, 0, tzinfo=ZoneInfo("America/New_York"))],
                     "paris": [datetime(2024, 1, 1, 15, 0, tzinfo=paris)]})
u.select(d=lambda r: r.ny - r.paris, h=lambda r: r.paris.dt.hour()).to_dicts() == [
    {"d": timedelta(0), "h": 15}]               # same instant (#246); fields are local
LTSeq.from_dict({"d": [date(2024, 3, 1)]}).select(n=lambda r: r.d - date(2024, 2, 1)).to_dicts() == [
    {"n": timedelta(days=29)}]                  # duration[s]

w = LTSeq.from_dict({"d": [timedelta(seconds=-5)]}, schema={"d": "duration[s]"})
w.select(p=lambda r: r.d * 2, q=lambda r: r.d // 2).to_dicts() == [
    {"p": timedelta(seconds=-10), "q": timedelta(seconds=-3)}]   # floored in seconds, the column's unit
w.select(q=lambda r: r.d.cast("duration[ms]") // 2).to_dicts() == [
    {"q": timedelta(seconds=-2.5)}]             # a finer unit, asked for explicitly, keeps a finer quotient
w.select(q=lambda r: r.d // 0).to_dicts()       # DivisionByZeroError during execution
```

### 23.14 Demand and error stages

```python
t = LTSeq.from_dict({"n": [10, 7, 3], "d": [2, 0, -2]})

q = t.select(q=lambda r: r.n // r.d)           # plan-building: succeeds
q.count() == 3                                 # q is not demanded by count()
q.to_dicts()                                   # DivisionByZeroError during execution
q.collect()                                    # DivisionByZeroError: collect is an eager call (§5.3)
t.derive(q=lambda r: r.n // r.d).drop("q").to_dicts() == t.to_dicts()   # q is never demanded
t.derive(q=lambda r: r.n // r.d).collect().drop("q")   # DivisionByZeroError: collect demands q (§5.2)

t.select(q=lambda r: if_else(r.d == 0, None, r.n // r.d)).to_dicts() == [
    {"q": 5}, {"q": None}, {"q": -2}]          # the division never runs for d == 0
t.filter(lambda r: (r.d != 0) & (r.n // r.d > 1)).to_dicts() == [{"n": 10, "d": 2}]
t.filter(lambda r: (r.n // r.d > 1) & (r.d != 0)).to_dicts() == [{"n": 10, "d": 2}]   # either side guards
t.filter(lambda r: r.d != 0).filter(lambda r: r.n // r.d > 1).to_dicts() == [{"n": 10, "d": 2}]
t.filter(lambda r: r.n // r.d > 1).filter(lambda r: r.d != 0).to_dicts()   # DivisionByZeroError: chained filters run in order
t.filter(lambda r: r.n // r.d > 1).count()     # DivisionByZeroError: the predicate decides row existence
t.filter(lambda r: r.nn > 0)                   # ColumnNotFoundError at plan time, suggesting "n"
LTSeq.from_dict({"n": [1], "d": [0]}).head(0).select(q=lambda r: r.n // r.d).to_dicts() == []
```

The guarded forms give the same result whatever the batch size, including when the zero divisor shares a batch with rows that divide.

### 23.15 Order state

```python
t = LTSeq.from_dict({"k": ["x", "y", "x"], "a": [2, None, 1]})
t.is_ordered, t.sort_keys                      # (True, None): defined order, no declared keys
[d["a"] for d in t.sort("a", descending=True).to_dicts()] == [2, 1, None]   # v0.4 gave [None, 2, 1]
s = t.sort("k", "a")
s.sort_keys == (SortKey("k", False, True), SortKey("a", False, True))
s.derive(a=lambda r: r.a * 10).sort_keys == (SortKey("k", False, True),)    # truncated before "a"
s.reverse().sort_keys == (SortKey("k", True, False), SortKey("a", True, False))
s.select(key=lambda r: r.k).sort_keys == (SortKey("key", False, True),)

g = t.group_by("k").agg(n=lambda g: g.count())
g.is_ordered                                   # False
g.head(1).count() == 1                         # head works on any table
g.tail(1)                                      # SortRequiredError at plan time
g.with_row_index()                             # SortRequiredError at plan time
g.sort("k").tail(1).to_dicts() == [{"k": "y", "n": 1}]
t.with_row_index().sort_keys == (SortKey("index", False, True),)
```

### 23.16 Set and bag operations

```python
a = LTSeq.from_dict({"x": [1, 1, 2, 3]})
b = LTSeq.from_dict({"x": [1, 3, 3]})
[d["x"] for d in a.difference(b).to_dicts()] == [2]
[d["x"] for d in a.difference(b, distinct=False).to_dicts()] == [1, 2]
[d["x"] for d in a.intersect(b, distinct=False).to_dicts()] == [1, 3]
[d["x"] for d in a.concat(b).to_dicts()] == [1, 1, 2, 3, 1, 3, 3]           # was union() or concat()
[d["x"] for d in a.concat(b).distinct().to_dicts()] == [1, 2, 3]          # SQL UNION
a.concat(LTSeq.from_dict({"x": [1.5]}))        # SchemaMismatchError at plan time: int64 vs float64 (#222)
a.concat(LTSeq.from_dict({"x": []}, schema={"x": "int32"}))   # SchemaMismatchError, though empty
```

### 23.17 Lazy CSV pipeline with composed expressions

Two files, `orders/2024-01.csv`:

```text
order_id,ts,region,sku,price,qty,status
1,2024-01-03T10:00:00,E,A-1,12.50,10,paid
2,2024-01-09T12:30:00,W,A-2,450.00,3,paid
3,2024-01-20T08:15:00,E,B-7,99.99,1,paid
4,2024-01-28T16:45:00,E,A-1,12.50,4,void
```

and `orders/2024-02.csv`, with the same header and the rows `5,2024-02-02T09:00:00,E,A-3,80.00,2,paid` and `6,2024-02-14T11:00:00,W,A-2,450.00,1,paid`.

```python
orders = LTSeq.read_csv("orders/", schema={"price": pa.decimal128(12, 2)})
# At the call: lists both files and infers the other columns from the first:
# order_id int64, ts timestamp[us], region/sku/status string, qty int64.

def notional(r):                                  # a plain function is a reusable expression
    return r.price * r.qty

def size(r):
    return when(notional(r) >= 1000, "large").when(notional(r) >= 100, "medium").otherwise("small")

clean = (orders
         .filter(lambda r: (r.status == "paid") & r.sku.str.startswith("A-"))
         .derive(notional=notional, size=size, month=lambda r: r.ts.dt.truncate("month"))
         .select("order_id", "region", "month", "size", "notional"))
clean.schema.field("notional").type == pa.decimal128(33, 2)   # known without reading a row
clean.head(2).to_dicts() == [
    {"order_id": 1, "region": "E", "month": datetime(2024, 1, 1), "size": "medium", "notional": Decimal("125.00")},
    {"order_id": 2, "region": "W", "month": datetime(2024, 1, 1), "size": "large",  "notional": Decimal("1350.00")},
]

summary = clean.group_by("region").agg(
    orders=lambda g: g.count(),
    revenue=lambda g: g.notional.sum(),
    large=lambda g: g.count(where=g.size == "large"))
summary.sort("region").write_parquet("summary.parquet")      # the one execution of the pipeline
LTSeq.read_parquet("summary.parquet").to_dicts() == [
    {"region": "E", "orders": 2, "revenue": Decimal("285.00"),  "large": 0},
    {"region": "W", "orders": 2, "revenue": Decimal("1800.00"), "large": 1},
]
```

`revenue` is `decimal128(38, 2)` ([§14.2]), so the sums are exact. `size` calls `notional(r)` again instead of reading the derived column, because every expression in one `derive` sees the input row ([§7.3]). If a third file had `qty` value `x` on line 5,000, beyond the inference sample, `write_parquet` would raise `CastError` naming that file, line and column, and leave any existing `summary.parquet` unchanged ([§16.5]).

### 23.18 Arrow and pandas round trip

```python
import pandas as pd

ny = ZoneInfo("America/New_York")
src = pa.table({
    "id":  pa.array([1, 2, 3], pa.int32()),
    "px":  [1.5, float("nan"), None],
    "amt": pa.array([Decimal("1.10"), None, Decimal("-2.00")], pa.decimal128(9, 2)),
    "ts":  [datetime(2024, 3, 10, 1, 30, tzinfo=ny), None, datetime(2024, 3, 10, 3, 30, tzinfo=ny)],
})
t = LTSeq.from_arrow(src)                       # references src's buffers, no copy
t.schema == src.schema                          # px double, ts timestamp[us, tz=America/New_York]

df = t.to_pandas()                              # dtype_backend="pyarrow"
df.dtypes["px"] == pd.ArrowDtype(pa.float64())
df["px"].isna().tolist() == [False, False, True]          # NaN is a value; only NULL is missing
u = LTSeq.from_pandas(df)
u.schema == src.schema                                    # every type comes back
[d["px"] for d in u.to_dicts()]                           # [1.5, nan, None]

t.to_pandas(dtype_backend="numpy_nullable")["px"].isna().tolist() == [False, True, True]
                                                # pandas folds NaN into NA: the caller opted into that loss
LTSeq.from_pandas(pd.DataFrame({"x": [1.0, float("nan")]})).to_dicts() == [{"x": 1.0}, {"x": None}]
                                                # in a NumPy float column NaN is pandas' missing marker (§4.5)

z = t.select()                                  # no columns, t's 3 rows (§6.1)
z.columns == [] and z.count() == 3
z.to_dicts() == [{}, {}, {}] and pa.table(z).num_rows == 3 and z.to_pandas().shape == (3, 0)
t.drop("id", "px", "amt", "ts").count() == 3
LTSeq.from_arrow(src.select([])).count() == 3   # the batch lengths carry the rows
LTSeq.from_pandas(pd.DataFrame(index=range(3))).count() == 3
z.distinct(keep="any").count() == 1             # every row of a table without columns is equal
z.write_parquet("z.parquet")                    # LTSeqValueError at the call: it would be written as 0 rows (§16.5)
LTSeq.from_dict({})                             # LTSeqValueError at the call: no row count (§4.6)

LTSeq.from_dict({"d": [date(2024, 1, 1)]}, schema={"d": "date32"})   # a date into a date type
LTSeq.from_dict({"d": ["2024-01-01"]}, schema={"d": "date32"})     # LTSeqTypeError at the call: no implicit parsing (§17.6)
LTSeq.from_dict({"s": [1]}, schema={"s": "string"})                # LTSeqTypeError at the call: no implicit formatting
LTSeq.from_dict({"b": [True]}, schema={"b": "int64"})              # LTSeqTypeError at the call: a bool is not a number
LTSeq.from_dict({"n": [1, 2.0]}, schema={"n": "int16"}).to_dicts() == [{"n": 1}, {"n": 2}]   # numbers convert exactly
LTSeq.from_dict({"n": [1.5]}, schema={"n": "int64"})               # CastError at the call: not exact
LTSeq.from_dict({"s": ["2024-01-01"]}).select(d=lambda r: r.s.cast("date32")).to_dicts() == [
    {"d": date(2024, 1, 1)}]                   # an explicit cast parses
pa.table(t, schema=src.schema.set(0, pa.field("id", pa.int64()))).column("id").type == pa.int64()
                                                # requested_schema widens int32 exactly (§16.4)
pa.table(t, schema=src.schema.set(0, pa.field("id", pa.string())))   # LTSeqTypeError at the call: no implicit formatting
```

The two `ts` values are one hour apart as instants although their wall clocks differ by two: 2024-03-10 is the day New York moves to daylight time. Subtracting them gives `timedelta(hours=1)`.

<!-- v0.5-modular:footer -->

---

Previous: [Examples: ordered computation, joins and state (§23–§23.9)](examples-sequences.md) · [Index](../README.md) · Next: [Acceptance criteria and test matrix (§24)](acceptance.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§1.6]: overview.md#16-rule-ownership
[§7]: schema-and-table-operations.md#7-basic-table-operations
[§7.3]: schema-and-table-operations.md#73-derive
[§9]: ordering.md#9-ordering-contract
[§9.5]: ordering.md#95-assume_sorted
[§14]: aggregation.md#14-aggregation-partitioning-and-pivot
[§14.2]: aggregation.md#142-aggregate-expressions
[§15.1]: streaming-and-output.md#151-to_batches
[§16]: streaming-and-output.md#16-output-and-interchange
[§16.4]: streaming-and-output.md#164-arrow-pycapsule-stream
[§16.5]: streaming-and-output.md#165-writers
[§18]: numeric-null-temporal.md#18-null-nan-and-boolean-logic
[§21.1]: errors-and-performance.md#211-materialization
[§23]: examples-sequences.md#23-end-to-end-examples

<!-- /v0.5-modular:links -->
