<!-- v0.5-modular:header -->

# v0.5 contract: Examples: ordered computation, joins and state (§23–§23.9)

[Index](../README.md) › Contract · Previous: [Canonical API reference (§22)](api-reference.md) · Next: [Examples: streaming, values, errors and interchange (§23.10–§23.18)](examples-semantics.md)

**Normative.** One module of the LTSeq v0.5 public API contract. It uses the BCP 14 key words as the contract's [status block](../README.md#ltseq-v05-public-api-contract) declares, and [§1.6] names the section that owns each cross-cutting rule.

**Scope.** The conventions every [§23] example shares (imports and the meaning of each stage), then examples 23.1–23.9: sessions with `group_ordered`, table-order windows, ranking and window aggregates, pattern search, as-of and aliased joins, pivot, partitions across processes, and `fold`.

**Most cited from here.** [§5] Lazy evaluation and materialization · [§20] Errors · [§24] Acceptance criteria and contract test matrix

<!-- /v0.5-modular:header -->

## 23. End-to-end examples

Each example is normative: an implementation MUST produce the shown results, types and exceptions, and [§24] includes every example as a test. All examples share these imports:

```python
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo
import pyarrow as pa
from ltseq import (LTSeq, SortKey, lit, if_else, when, rank, row_number,
                   ArithmeticOverflowError, CastError, ColumnNotFoundError, DivisionByZeroError,
                   DuplicateKeyError, LTSeqTypeError, LTSeqValueError, SchemaMismatchError,
                   SortRequiredError)
```

The stages are those of [§20.2](errors-and-performance.md#202-stages): "at plan time" means the plan-building call raises; "at the call" means an eager call ([§5.3]) raises while it reads; "during execution" means every call so far succeeds and the terminal that demands the value raises.

### 23.1 Sessions with `group_ordered`

```python
events = LTSeq.from_rows([
    {"user": "a", "ts": datetime(2024, 5, 1, 9, 0)},
    {"user": "b", "ts": datetime(2024, 5, 1, 9, 5)},
    {"user": "a", "ts": datetime(2024, 5, 1, 9, 10)},
    {"user": "a", "ts": datetime(2024, 5, 1, 9, 20)},
    {"user": "b", "ts": datetime(2024, 5, 1, 9, 40)},
    {"user": "b", "ts": datetime(2024, 5, 1, 9, 50)},
    {"user": "b", "ts": datetime(2024, 5, 1, 10, 0)},
    {"user": "a", "ts": datetime(2024, 5, 1, 10, 30)},
    {"user": "a", "ts": datetime(2024, 5, 1, 10, 35)},
])
gap = lambda r: r.ts - r.ts.shift(1) > timedelta(minutes=30)

events.group_ordered("user", starts_when=gap)        # SortRequiredError at plan time: sort_keys is None

groups = events.sort("user", "ts").group_ordered("user", starts_when=gap)
groups.count()                                        # 4: a 9:00–9:20, a 10:30–10:35, b 9:05, b 9:40–10:00

sessions = (groups.filter(lambda g: g.count() >= 3)
                  .agg(start=lambda g: g.ts.min(), end=lambda g: g.ts.max(), n=lambda g: g.count()))
sessions.to_dicts() == [
    {"user": "a", "start": datetime(2024, 5, 1, 9, 0),  "end": datetime(2024, 5, 1, 9, 20), "n": 3},
    {"user": "b", "start": datetime(2024, 5, 1, 9, 40), "end": datetime(2024, 5, 1, 10, 0), "n": 3},
]
sessions.schema == pa.schema([("user", pa.string()), ("start", pa.timestamp("us")),
                              ("end", pa.timestamp("us")), ("n", pa.int64())])  # nullability aside
sessions.is_ordered, sessions.sort_keys                # (True, None)
```

The first row of user `b` compares with user `a`'s last row in `gap`; that does not matter, because the key change starts a group anyway. `groups.flatten(group_id="session")` returns all nine rows in `(user, ts)` order with `session` = `0, 0, 0, 1, 1, 2, 3, 3, 3`.

### 23.2 Table-order windows

```python
raw = LTSeq.from_rows([
    {"date": date(2024, 1, 3), "close": 99.0},
    {"date": date(2024, 1, 1), "close": 100.0},
    {"date": date(2024, 1, 2), "close": 102.0},
    {"date": date(2024, 1, 5), "close": 104.0},
    {"date": date(2024, 1, 4), "close": 99.0},
])
raw.derive(prev=lambda r: r.close.shift(1))      # SortRequiredError at plan time

px = raw.sort("date").derive(
    prev=lambda r: r.close.shift(1, fill_value=0.0),
    ret=lambda r: r.close.pct_change(),
    ma3=lambda r: r.close.rolling(3).mean(),
    hi=lambda r: r.close.cum_max())
[(d["close"], d["prev"], d["ret"], d["ma3"], d["hi"]) for d in px.to_dicts()] == [
    (100.0,   0.0, None,                  None,               100.0),
    (102.0, 100.0, 0.020000000000000018,  None,               102.0),
    ( 99.0, 102.0, -0.02941176470588236,  100.33333333333333, 102.0),
    ( 99.0,  99.0, 0.0,                   100.0,              102.0),
    (104.0,  99.0, 0.05050505050505061,   100.66666666666667, 104.0),
]
px.sort_keys == (SortKey("date", False, True),)
```

`ret` is the binary64 result of `102.0 / 100.0 - 1`, not a rounded `0.02`. A window with its own order works on the unsorted table and leaves the rows where they were (#202):

```python
raw.derive(prev=lambda r: r.close.shift(1).over(order_by="date")).to_dicts() == [
    {"date": date(2024, 1, 3), "close": 99.0,  "prev": 102.0},
    {"date": date(2024, 1, 1), "close": 100.0, "prev": None},
    {"date": date(2024, 1, 2), "close": 102.0, "prev": 100.0},
    {"date": date(2024, 1, 5), "close": 104.0, "prev": 99.0},
    {"date": date(2024, 1, 4), "close": 99.0,  "prev": 99.0},
]
```

### 23.3 Ranking and window aggregates

```python
emp = LTSeq.from_rows([
    {"name": "ann", "dept": "eng", "salary": 120},
    {"name": "bob", "dept": "ops", "salary": 90},
    {"name": "cid", "dept": "eng", "salary": 150},
    {"name": "dee", "dept": "eng", "salary": 120},
    {"name": "eve", "dept": "ops", "salary": 95},
])
out = emp.derive(
    rk=lambda r: rank().over(partition_by="dept", order_by="salary", descending=True),
    rn=lambda r: row_number().over(partition_by="dept", order_by="salary", descending=True),
    share=lambda r: r.salary / r.salary.sum().over(partition_by="dept"))
[(d["name"], d["rk"], d["rn"], d["share"]) for d in out.to_dicts()] == [
    ("ann", 2, 2, 0.3076923076923077),
    ("bob", 2, 2, 0.4864864864864865),
    ("cid", 1, 1, 0.38461538461538464),
    ("dee", 2, 3, 0.3076923076923077),
    ("eve", 1, 1, 0.5135135135135135),
]
out.schema.field("rk").type == pa.int64() and out.schema.field("share").type == pa.float64()
emp.derive(x=lambda r: r.salary.sum())                       # LTSeqTypeError at plan time: aggregate needs .over()
emp.derive(x=lambda r: r.salary.sum().over(order_by="name")) # LTSeqValueError at plan time
```

Rows keep their input order. `rn` breaks the ann/dee tie by table order, which is defined, so the result is deterministic.

### 23.4 Pattern search and `search_first`

```python
ticks = LTSeq.from_dict({"ts": list(range(1, 10)),
                         "close": [10, 11, 12, 13, 12, 13, 14, 15, 16]}).assume_sorted("ts")
up = lambda r: r.close > r.close.shift(1)

ticks.search_pattern(up, up, up).to_dicts() == [
    {"ts": 2, "close": 11}, {"ts": 6, "close": 13}, {"ts": 7, "close": 14}]
ticks.search_pattern(up, up, up).count() == 3
ticks.search_first(lambda r: r.close >= 14).to_dicts() == [{"ts": 7, "close": 14}]
ticks.search_first(lambda r: r.close > 100).count() == 0    # a 0-row LTSeq, never None
ticks.search_pattern()                                       # LTSeqValueError at plan time
```

The matches at `ts` 6 and 7 overlap. Row 1 (`ts` 1) has no previous row, so `up` is NULL there and no match starts at it.

### 23.5 As-of join

```python
T = lambda s: datetime(2024, 1, 2, 10, 0, s)
trades = LTSeq.from_rows([
    {"ts": T(1),  "sym": "A", "qty": 100},
    {"ts": T(3),  "sym": "B", "qty": 50},
    {"ts": T(7),  "sym": "A", "qty": 20},
    {"ts": T(20), "sym": "A", "qty": 5},
])
quotes = LTSeq.from_rows([
    {"ts": T(0), "sym": "A", "bid": 9.9},
    {"ts": T(2), "sym": "B", "bid": 20.0},
    {"ts": T(2), "sym": "A", "bid": 10.0},
    {"ts": T(5), "sym": "A", "bid": 10.1},
    {"ts": T(5), "sym": "A", "bid": 10.2},
])
j = trades.asof_join(quotes, on="ts", by="sym", tolerance=timedelta(seconds=5))
j.columns == ["ts", "sym", "qty", "bid"]
j.to_dicts() == [
    {"ts": T(1),  "sym": "A", "qty": 100, "bid": 9.9},
    {"ts": T(3),  "sym": "B", "qty": 50,  "bid": 20.0},
    {"ts": T(7),  "sym": "A", "qty": 20,  "bid": 10.2},   # two quotes at T(5): the last in right order
    {"ts": T(20), "sym": "A", "qty": 5,   "bid": None},   # nearest earlier quote is 15 s away
]
trades.asof_join(quotes.group_by("sym").agg(ts=lambda g: g.ts.max()), on="ts", by="sym")
                                     # SortRequiredError at plan time: the right input's order is undefined
trades.asof_join(quotes, on="ts", tolerance=timedelta(seconds=-1))   # LTSeqValueError at plan time
```

Neither input declares `sort_keys`: the as-of join needs only a defined order on the right, to resolve ties. The result is exactly one row per trade, in trade order.

### 23.6 Join with `alias` and `validate`

```python
trades = LTSeq.from_rows([
    {"id": 1, "account": "a1", "amount": 100}, {"id": 2, "account": "a2", "amount": 50},
    {"id": 3, "account": "a9", "amount": 70},  {"id": 4, "account": "a1", "amount": 30},
])
accounts = LTSeq.from_rows([
    {"id": "a1", "name": "Ada", "tier": "gold"}, {"id": "a2", "name": "Bo", "tier": "basic"},
])
j = trades.join(accounts, left_on="account", right_on="id", how="left", alias="acct", validate="m:1")
j.columns == ["id", "account", "amount", "acct_id", "acct_name", "acct_tier"]
[(d["id"], d["acct_name"]) for d in j.to_dicts()] == [(1, "Ada"), (2, "Bo"), (3, None), (4, "Ada")]

trades.join(accounts, left_on="account", right_on="id", how="left").columns == [
    "id", "account", "amount", "id_right", "name", "tier"]

dup = accounts.concat(LTSeq.from_rows([{"id": "a1", "name": "Ann", "tier": "gold"}]))
bad = trades.join(dup, left_on="account", right_on="id", how="left", validate="m:1")  # plan-building succeeds
bad.to_dicts()                       # DuplicateKeyError during execution, naming the right side and "a1"
trades.join(accounts, on="id")                        # LTSeqTypeError at plan time: int64 key vs string key
```

### 23.7 Pivot

```python
sales = LTSeq.from_rows([
    {"region": "E", "quarter": "Q1", "amount": 10}, {"region": "E", "quarter": "Q1", "amount": 5},
    {"region": "W", "quarter": "Q2", "amount": 7},  {"region": "E", "quarter": "Q2", "amount": 3},
])
p = sales.pivot(index="region", columns="quarter", values="amount", column_values=["Q1", "Q2", "Q3"])
p.columns == ["region", "Q1", "Q2", "Q3"]           # known at plan time; nothing executed yet
p.sort("region").to_dicts() == [
    {"region": "E", "Q1": 15,   "Q2": 3, "Q3": None},
    {"region": "W", "Q1": None, "Q2": 7, "Q3": None},
]
sales.pivot(index="region", columns="quarter", values="amount", agg="count",
            column_values=["Q1", "Q2", "Q3"]).sort("region").to_dicts() == [
    {"region": "E", "Q1": 2, "Q2": 1, "Q3": 0},
    {"region": "W", "Q1": 0, "Q2": 1, "Q3": 0},
]
sales.pivot(index="region", columns="quarter", values="amount", column_values=["Q1"]).to_dicts()
                                     # LTSeqValueError during execution: "Q2" is not in column_values
sales.pivot(index="region", columns="quarter", values="amount").columns == ["region", "Q1", "Q2"]
                                     # without column_values: eager discovery, ascending
```

`p.is_ordered` is `False`, hence the `sort` before `to_dicts`.

### 23.8 Partitions across processes

```python
from concurrent.futures import ProcessPoolExecutor

orders = LTSeq.from_rows([
    {"region": "W", "amt": 5}, {"region": "E", "amt": 7}, {"region": None, "amt": 1}, {"region": "E", "amt": 2},
])
parts = orders.partition("region")                    # eager: finds the distinct regions now
list(parts) == ["E", "W", None]
{k: v.count() for k, v in parts.items()} == {"E": 2, "W": 1, None: 1}
parts["E"].to_dicts() == [{"region": "E", "amt": 7}, {"region": "E", "amt": 2}]   # input order

def total(t: LTSeq) -> int:
    return t.agg(s=lambda g: g.amt.sum()).to_dicts()[0]["s"]

with ProcessPoolExecutor() as ex:                     # each value is pickled as a snapshot (§16.6)
    list(ex.map(total, parts.values())) == [9, 5, 1]

LTSeq.from_dict({"x": [1.0, float("nan")]}).partition("x")   # LTSeqValueError at the call: NaN key

ny = ZoneInfo("America/New_York")                     # 01:30 happens twice on 2024-11-03
ev = LTSeq.from_dict({"ts": [datetime(2024, 11, 3, 1, 30, tzinfo=ny), datetime(2024, 11, 3, 1, 30, tzinfo=ny, fold=1)]})
hours = ev.partition("ts")
list(hours) == [datetime(2024, 11, 3, 5, 30, tzinfo=timezone.utc),
                datetime(2024, 11, 3, 6, 30, tzinfo=timezone.utc)]   # two instants, keyed in UTC
v = ev.to_dicts()[1]["ts"]                            # 01:30 EST, fold=1
hours[v.astimezone(timezone.utc)].count() == 1        # hours[v] raises KeyError: v is in the repeated hour

snap = LTSeq.read_csv("orders.csv").collect().partition("region")   # one read: keys and values agree even if the file changes
```

### 23.9 Sequential state with `fold`

A stock level that cannot go below zero, `s_i = max(0, s_{i-1} + delta_i)`, is not a window function: each value depends on the previous result through `max`. `fold` computes it.

```python
moves = LTSeq.from_rows([
    {"sku": "x", "t": 1, "delta": 5},  {"sku": "x", "t": 2, "delta": -3},
    {"sku": "x", "t": 3, "delta": -4}, {"sku": "x", "t": 4, "delta": 6},
    {"sku": "x", "t": 5, "delta": -2},
]).sort("t")
stock = moves.fold(lambda s, row: max(0, s + row["delta"]), init=0, into="stock")
[d["stock"] for d in stock.to_dicts()] == [5, 2, 0, 6, 4]
stock.schema.field("stock").type == pa.int64()
stock.sort_keys == (SortKey("t", False, True),)

moves.fold(lambda s, row: s + row["delta"] / 2, init=0, into="half", dtype="int64")
                                     # CastError from the fold call itself (eager, §5.3): 2.5 is not an int64
LTSeq.from_rows([{"t": 1, "delta": 5}]).fold(lambda s, r: s, init=0, into="s")
                                     # SortRequiredError at plan time
```

<!-- v0.5-modular:footer -->

---

Previous: [Canonical API reference (§22)](api-reference.md) · [Index](../README.md) · Next: [Examples: streaming, values, errors and interchange (§23.10–§23.18)](examples-semantics.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§1.6]: overview.md#16-rule-ownership
[§5]: loading-and-laziness.md#5-lazy-evaluation-and-materialization
[§5.3]: loading-and-laziness.md#53-eager-calls
[§20]: errors-and-performance.md#20-errors
[§23]: #23-end-to-end-examples
[§24]: acceptance.md#24-acceptance-criteria-and-contract-test-matrix

<!-- /v0.5-modular:links -->
