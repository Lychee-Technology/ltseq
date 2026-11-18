# B1 as filtered rows, and §8.5's membership contract (`witnesses.py`)

## B1: run-end-encoded Int64 `x = [1, 42, NULL]`, IN list `[NULL, 1, 2, 3]`

Expected is SQL's three-valued logic, which LTSeq ships today; a filter keeps the rows (by position `k`) whose value is TRUE. The plain column is the control.

| Encoding | Question | Expected | 55.0.0 | 56.0.0-rc1 |
|---|---|---|---|---|
| run-end | `x.is_in([None, 1, 2, 3])` | [TRUE, NULL, NULL] | [TRUE, NULL, NULL] | **[TRUE, FALSE, NULL]** (wrong) |
| run-end | `~x.is_in([None, 1, 2, 3])` | [FALSE, NULL, NULL] | [FALSE, NULL, NULL] | **[FALSE, TRUE, NULL]** (wrong) |
| run-end | `filter ~x.is_in([None, 1, 2, 3])` | kept [] | kept [] | kept [] |
| run-end | `filter if_else(~x.is_in([None, 1, 2, 3]), 1, 2) == 1` | kept [] | kept [] | **kept [1]** (wrong) |
| run-end | `filter (~x.is_in([None, 1, 2, 3])).is_null()` | kept [1, 2] | kept [1, 2] | **kept [2]** (wrong) |
| plain | `x.is_in([None, 1, 2, 3])` | [TRUE, NULL, NULL] | [TRUE, NULL, NULL] | [TRUE, NULL, NULL] |
| plain | `~x.is_in([None, 1, 2, 3])` | [FALSE, NULL, NULL] | [FALSE, NULL, NULL] | [FALSE, NULL, NULL] |
| plain | `filter ~x.is_in([None, 1, 2, 3])` | kept [] | kept [] | kept [] |
| plain | `filter if_else(~x.is_in([None, 1, 2, 3]), 1, 2) == 1` | kept [] | kept [] | kept [] |
| plain | `filter (~x.is_in([None, 1, 2, 3])).is_null()` | kept [1, 2] | kept [1, 2] | kept [1, 2] |

Filter plans, run-end-encoded:

`filter ~x.is_in([None, 1, 2, 3])`, 55.0.0:

```text
Filter: CAST(?table?.x AS Int64) NOT IN ([Int64(NULL), Int64(1), Int64(2), Int64(3)])
  TableScan: ?table? projection=[k, x]
```

`filter ~x.is_in([None, 1, 2, 3])`, 56.0.0-rc1:

```text
EmptyRelation: rows=0
```

`filter if_else(~x.is_in([None, 1, 2, 3]), 1, 2) == 1`, 55.0.0:

```text
Filter: CASE CAST(?table?.x AS Int64) NOT IN ([Int64(NULL), Int64(1), Int64(2), Int64(3)]) WHEN Boolean(true) THEN Int64(1) ELSE Int64(2) END = Int64(1)
  TableScan: ?table? projection=[k, x]
```

`filter if_else(~x.is_in([None, 1, 2, 3]), 1, 2) == 1`, 56.0.0-rc1:

```text
Filter: CASE ?table?.x NOT IN ([RunEndEncoded(Field { name: "run_ends", data_type: Int32 }, Field { name: "values", data_type: Int64, nullable: true }, Int64(NULL)), RunEndEncoded(Field { name: "run_ends", data_type: Int32 }, Field { name: "values", data_type: Int64, nullable: true }, Int64(1)), RunEndEncoded(Field { name: "run_ends", data_type: Int32 }, Field { name: "values", data_type: Int64, nullable: true }, Int64(2)), RunEndEncoded(Field { name: "run_ends", data_type: Int32 }, Field { name: "values", data_type: Int64, nullable: true }, Int64(3))]) WHEN Boolean(true) THEN Int64(1) ELSE Int64(2) END = Int64(1)
  TableScan: ?table? projection=[k, x]
```

`filter (~x.is_in([None, 1, 2, 3])).is_null()`, 55.0.0:

```text
Filter: CAST(?table?.x AS Int64) NOT IN ([Int64(NULL), Int64(1), Int64(2), Int64(3)]) IS NULL
  TableScan: ?table? projection=[k, x]
```

`filter (~x.is_in([None, 1, 2, 3])).is_null()`, 56.0.0-rc1:

```text
Filter: ?table?.x NOT IN ([RunEndEncoded(Field { name: "run_ends", data_type: Int32 }, Field { name: "values", data_type: Int64, nullable: true }, Int64(NULL)), RunEndEncoded(Field { name: "run_ends", data_type: Int32 }, Field { name: "values", data_type: Int64, nullable: true }, Int64(1)), RunEndEncoded(Field { name: "run_ends", data_type: Int32 }, Field { name: "values", data_type: Int64, nullable: true }, Int64(2)), RunEndEncoded(Field { name: "run_ends", data_type: Int32 }, Field { name: "values", data_type: Int64, nullable: true }, Int64(3))]) IS NULL
  TableScan: ?table? projection=[k, x]
```

## v0.5 §8.5: `is_in` never returns NULL (not shipped)

Today's shipped answer is SQL's `IN` (with NaN equal to NaN, as LTSeq compares); §8.5's is the later contract. Both engines are compared with both. Rows where an engine differs from §8.5 are existing contract gaps on 55.0.0 already, not 56 regressions; this spike changes no shipped semantics.

| Question (Int64 `x = [NULL, 1, 7]` unless noted) | Today's SQL answer / kept | §8.5 answer / kept | 55.0.0 | 56.0.0-rc1 | 56 against 55 |
|---|---|---|---|---|---|
| `NULL receiver, None among the items: is_in([None, 1])` | [NULL, TRUE, NULL] / kept [1] | [TRUE, TRUE, FALSE] / kept [0, 1] | [NULL, TRUE, NULL] / kept [1] (= SQL, ≠ §8.5) | [NULL, TRUE, NULL] / kept [1] (= SQL, ≠ §8.5) | same |
| `NULL receiver, no None: is_in([1, 2])` | [NULL, TRUE, FALSE] / kept [1] | [FALSE, TRUE, FALSE] / kept [1] | [NULL, TRUE, FALSE] / kept [1] (= SQL, ≠ §8.5) | [NULL, TRUE, FALSE] / kept [1] (= SQL, ≠ §8.5) | same |
| `nonmatching receiver, None among the items: is_in([None, 2])` | [NULL, NULL, NULL] / kept [] | [TRUE, FALSE, FALSE] / kept [0] | [NULL, NULL, NULL] / kept [] (= SQL, ≠ §8.5) | [NULL, NULL, NULL] / kept [] (= SQL, ≠ §8.5) | same |
| `negated: ~is_in([None, 1])` | [NULL, FALSE, NULL] / kept [] | [FALSE, FALSE, TRUE] / kept [2] | [NULL, FALSE, NULL] / kept [] (= SQL, ≠ §8.5) | [NULL, FALSE, NULL] / kept [] (= SQL, ≠ §8.5) | same |
| `empty list: is_in([])` | error | [FALSE, FALSE, FALSE] / kept [] | error: `ValueError: is_in requires at least one value` | error: `ValueError: is_in requires at least one value` | same |
| `NaN among the items: is_in([nan, 1.0])`, float64 `x = [nan, NULL, 1.0, 2.0]` | [TRUE, NULL, TRUE, FALSE] / kept [0, 2] | [TRUE, FALSE, TRUE, FALSE] / kept [0, 2] | [TRUE, NULL, TRUE, FALSE] / kept [0, 2] (= SQL, ≠ §8.5) | [TRUE, NULL, TRUE, FALSE] / kept [0, 2] (= SQL, ≠ §8.5) | same |
