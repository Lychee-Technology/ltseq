## Matrix: 55.0.0 against 56.0.0-rc1

| Layout and type | Cases | Projection wrong (55.0.0) | Filter wrong (55.0.0) | Projection wrong (56.0.0-rc1) | Filter wrong (56.0.0-rc1) |
|---|---|---|---|---|---|
| sorted float16 | 1352 | 0 | 4 | 0 | 0 |
| sorted float32 | 1352 | 0 | 4 | 0 | 0 |
| sorted float64 | 1352 | 2 | 8 | 0 | 0 |
| sorted float64 dictionary | 1352 | 0 | 0 | 0 | 0 |
| sorted float64 run-end | 1352 | 0 | 0 | 102 | 0 |
| sorted int64 | 1352 | 43 | 24 | 0 | 0 |
| sorted int64 dictionary | 1352 | 43 | 24 | 0 | 0 |
| sorted int64 run-end | 1352 | 3 | 3 | 180 | 0 |
| sorted string | 1352 | 43 | 24 | 0 | 0 |
| unsorted float16 | 1352 | 0 | 4 | 0 | 0 |
| unsorted float32 | 1352 | 0 | 4 | 0 | 0 |
| unsorted float64 | 1352 | 2 | 8 | 0 | 0 |
| unsorted float64 dictionary | 1352 | 0 | 0 | 0 | 0 |
| unsorted float64 run-end | 1352 | 0 | 0 | 102 | 0 |
| unsorted int64 | 1352 | 43 | 24 | 0 | 0 |
| unsorted int64 dictionary | 1352 | 43 | 24 | 0 | 0 |
| unsorted int64 run-end | 1352 | 3 | 3 | 180 | 0 |
| unsorted string | 1352 | 43 | 24 | 0 | 0 |

- Wrong under 55.0.0, right under 56.0.0-rc1: 302 cases. 176 of them had a wrong filter; the filter's optimized plan differs under 56.0.0-rc1 for 164, and is the same for 12. The plan recorded is the filter's only, and over a sorted table the scan plans a filter pushed into it in a plan of its own (`TableOrder::scan`), which `explain_plan` does not show.
- Right under 55.0.0, wrong under 56.0.0-rc1: 558 cases.
- Wrong under both: 6 cases.

### Right under 55.0.0, wrong under 56.0.0-rc1: by type {'float64 run-end': 204, 'int64 run-end': 354}; the first 12

- `sorted float64 run-end (eq|['c', 1.0, 2.5, 7.0] & is_in[None, 1.0, 2.5, 7.0])`: projection [False, False, True, None, False, True, False, False, False, False], kept None, want [None, None, True, None, None, True, None, False, None, False]
- `sorted float64 run-end (eq|['c', 1.0, 2.5, 7.0] & ~is_in[None, 1.0, 2.5, 7.0])`: projection [True, True, False, None, True, False, True, False, True, False], kept None, want [None, None, False, None, None, False, None, False, None, False]
- `sorted float64 run-end (eq|['c', 1.0, 2.5, 7.0] | is_in[None, 1.0, 2.5, 7.0])`: projection [True, True, True, None, True, True, True, False, True, False], kept None, want [True, True, True, None, True, True, True, None, True, None]
- `sorted float64 run-end (eq|['c', 1.0, 2.5, 7.0] | ~is_in[None, 1.0, 2.5, 7.0])`: projection [True, True, True, None, True, True, True, True, True, True], kept None, want [True, True, True, None, True, True, True, None, True, None]
- `sorted float64 run-end (eq|['z(-0.0)', 1.0, 2.5, 7.0] & is_in[None, 1.0, 2.5, 7.0])`: projection [False, False, True, None, False, True, False, False, False, False], kept None, want [None, None, True, None, False, True, None, None, False, False]
- `sorted float64 run-end (eq|['z(-0.0)', 1.0, 2.5, 7.0] & ~is_in[None, 1.0, 2.5, 7.0])`: projection [True, True, False, None, False, False, True, True, False, False], kept None, want [None, None, False, None, False, False, None, None, False, False]
- `sorted float64 run-end (eq|['z(-0.0)', 1.0, 2.5, 7.0] | is_in[None, 1.0, 2.5, 7.0])`: projection [True, True, True, None, False, True, True, True, False, False], kept None, want [True, True, True, None, None, True, True, True, None, None]
- `sorted float64 run-end (eq|['z(-0.0)', 1.0, 2.5, 7.0] | ~is_in[None, 1.0, 2.5, 7.0])`: projection [True, True, True, None, True, True, True, True, True, True], kept None, want [True, True, True, None, None, True, True, True, None, None]
- `sorted float64 run-end (eq|[-0.0, 1.0, 2.5] & is_in[None, 1.0, 2.5, 7.0])`: projection [False, False, True, None, False, True, False, False, False, False], kept None, want [None, None, True, None, False, True, None, None, False, False]
- `sorted float64 run-end (eq|[-0.0, 1.0, 2.5] & ~is_in[None, 1.0, 2.5, 7.0])`: projection [True, True, False, None, False, False, True, True, False, False], kept None, want [None, None, False, None, False, False, None, None, False, False]
- `sorted float64 run-end (eq|[-0.0, 1.0, 2.5] | is_in[None, 1.0, 2.5, 7.0])`: projection [True, True, True, None, False, True, True, True, False, False], kept None, want [True, True, True, None, None, True, True, True, None, None]
- `sorted float64 run-end (eq|[-0.0, 1.0, 2.5] | ~is_in[None, 1.0, 2.5, 7.0])`: projection [True, True, True, None, True, True, True, True, True, True], kept None, want [True, True, True, None, None, True, True, True, None, None]

### Wrong under both: by type {'int64 run-end': 6}; the first 12

- `sorted int64 run-end (is_in[None, 1, 2, 5] & ~is_in[None, 3, 4, 6])`: projection [True, True, None, True, False, False, False, False], kept None, want [None, None, None, None, False, None, None, None]
- `sorted int64 run-end ~(is_in[None, 1, 2, 5] & ~is_in[None, 3, 4, 6])`: projection [False, False, None, False, True, True, True, True], kept None, want [None, None, None, None, True, None, None, None]
- `sorted int64 run-end ~(is_in[None, 1, 2, 5] | ~is_in[None, 3, 4, 6])`: projection [False, False, None, False, True, False, False, False], kept None, want [False, False, None, False, None, None, None, None]
- `unsorted int64 run-end (is_in[None, 1, 2, 5] & ~is_in[None, 3, 4, 6])`: projection [True, True, None, True, False, False, False, False], kept None, want [None, None, None, None, False, None, None, None]
- `unsorted int64 run-end ~(is_in[None, 1, 2, 5] & ~is_in[None, 3, 4, 6])`: projection [False, False, None, False, True, True, True, True], kept None, want [None, None, None, None, True, None, None, None]
- `unsorted int64 run-end ~(is_in[None, 1, 2, 5] | ~is_in[None, 3, 4, 6])`: projection [False, False, None, False, True, False, False, False], kept None, want [False, False, None, False, None, None, None, None]

### Plans of the cases #296 names

`unsorted int64 (is_in[1, 2, 5] & ~is_in[None, 3, 4, 6])`

55.0.0, wrong (projection [True, True, None, True, False, False, False, False], kept [0, 1, 3], want [None, None, None, None, False, False, False, False]):

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Int64(1) AS z1
  Filter: ?table?.x = Int64(1) OR ?table?.x = Int64(2) OR ?table?.x = Int64(5)
    TableScan: ?table? projection=[k, x, c]
```

56.0.0-rc1, right:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Int64(1) AS z1
  Filter: ?table?.x IN ([Int64(1), Int64(2), Int64(5)]) AND Boolean(NULL)
    TableScan: ?table? projection=[k, x, c]
```

`unsorted int64 (is_in[1, 2, 5, 9] & is_in[1, 3, 4, 6])`

55.0.0, right:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Int64(1) AS z1
  Filter: ?table?.x = Int64(1)
    TableScan: ?table? projection=[k, x, c]
```

56.0.0-rc1, right:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Int64(1) AS z1
  Filter: ?table?.x = Int64(1)
    TableScan: ?table? projection=[k, x, c]
```

`unsorted int64 (eq|['c', 2, 10, 11] & eq|['c', 5, 12, 13])`

55.0.0, right:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Int64(1) AS z1
  Filter: (__common_expr_846 OR ?table?.x = Int64(2) OR ?table?.x = Int64(10) OR ?table?.x = Int64(11)) AND (__common_expr_846 OR ?table?.x = Int64(5) OR ?table?.x = Int64(12) OR ?table?.x = Int64(13))
    Projection: ?table?.x = ?table?.c AS __common_expr_846, ?table?.k, ?table?.x, ?table?.c
      TableScan: ?table? projection=[k, x, c]
```

56.0.0-rc1, right:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Int64(1) AS z1
  Filter: (__common_expr_640 OR ?table?.x = Int64(2) OR ?table?.x = Int64(10) OR ?table?.x = Int64(11)) AND (__common_expr_640 OR ?table?.x = Int64(5) OR ?table?.x = Int64(12) OR ?table?.x = Int64(13))
    Projection: ?table?.x = ?table?.c AS __common_expr_640, ?table?.k, ?table?.x, ?table?.c
      TableScan: ?table? projection=[k, x, c]
```

`unsorted int64 (eq|['z(1)', 2, 10, 11] & eq|['z(1)', 5, 12, 13])`

55.0.0, right:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Int64(1) AS z1
  Filter: (__common_expr_888 OR ?table?.x = Int64(2) OR ?table?.x = Int64(10) OR ?table?.x = Int64(11)) AND (__common_expr_888 OR ?table?.x = Int64(5) OR ?table?.x = Int64(12) OR ?table?.x = Int64(13))
    Projection: ?table?.x = Int64(1) AS __common_expr_888, ?table?.k, ?table?.x, ?table?.c
      TableScan: ?table? projection=[k, x, c]
```

56.0.0-rc1, right:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Int64(1) AS z1
  Filter: (__common_expr_670 OR ?table?.x = Int64(2) OR ?table?.x = Int64(10) OR ?table?.x = Int64(11)) AND (__common_expr_670 OR ?table?.x = Int64(5) OR ?table?.x = Int64(12) OR ?table?.x = Int64(13))
    Projection: ?table?.x = Int64(1) AS __common_expr_670, ?table?.k, ?table?.x, ?table?.c
      TableScan: ?table? projection=[k, x, c]
```

`unsorted string (is_in['s1', 's2', 's5'] & ~is_in[None, 's3', 's4', 's6'])`

55.0.0, wrong (projection [True, True, None, True, False, False, False, False], kept [0, 1, 3], want [None, None, None, None, False, False, False, False]):

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Utf8("s1") AS z_s1_
  Filter: ?table?.x = Utf8("s1") OR ?table?.x = Utf8("s2") OR ?table?.x = Utf8("s5")
    TableScan: ?table? projection=[k, x, c]
```

56.0.0-rc1, right:

```text
EmptyRelation: rows=0
```

`unsorted float64 (eq|['z(-0.0)', 1.0, 2.5, 7.0] & eq|['z(0.0)', 3.0, 4.0, 5.0])`

55.0.0, wrong (projection None, kept [], want [True, True, False, None, False, False, True, True, False, False]):

```text
EmptyRelation: rows=0
```

56.0.0-rc1, right:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Float64(-0) AS z_0_0, Float64(0) AS z0_0
  Filter: (?table?.x IN ([Float64(1), Float64(2.5), Float64(7)]) OR ?table?.x >= Float64(0) AND ?table?.x <= Float64(0)) AND (?table?.x IN ([Float64(3), Float64(4), Float64(5)]) OR ?table?.x >= Float64(0) AND ?table?.x <= Float64(0))
    TableScan: ?table? projection=[k, x, c]
```

`unsorted float64 (is_in[None, 0.0, 1.0, 2.5] & ~is_in[None, 1.0, 2.5, 7.0])`

55.0.0, right:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Float64(-0) AS z_0_0, Float64(0) AS z0_0
  Filter: (Boolean(NULL) OR ?table?.x = Float64(1) OR ?table?.x = Float64(2.5) OR ?table?.x >= Float64(-0) AND ?table?.x <= Float64(0)) AND ?table?.x NOT IN ([Float64(NULL), Float64(1), Float64(2.5), Float64(7)])
    TableScan: ?table? projection=[k, x, c]
```

56.0.0-rc1, right:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Float64(-0) AS z_0_0, Float64(0) AS z0_0
  Filter: (?table?.x IN ([Float64(1), Float64(2.5)]) OR ?table?.x >= Float64(0) AND ?table?.x <= Float64(0)) AND Boolean(NULL)
    TableScan: ?table? projection=[k, x, c]
```

`unsorted int64 run-end (is_in[1, 2, 5, 9] | is_in[None, 3, 4, 6])`

55.0.0, right:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Int64(1) AS z1
  Filter: ?table?.x IN ([RunEndEncoded(Field { name: "run_ends", data_type: Int32 }, Field { name: "values", data_type: Int64, nullable: true }, Int64(1)), RunEndEncoded(Field { name: "run_ends", data_type: Int32 }, Field { name: "values", data_type: Int64, nullable: true }, Int64(2)), RunEndEncoded(Field { name: "run_ends", data_type: Int32 }, Field { name: "values", data_type: Int64, nullable: true }, Int64(5)), RunEndEncoded(Field { name: "run_ends", data_type: Int32 }, Field { name: "values", data_type: Int64, nullable: true }, Int64(9))]) OR CAST(?table?.x AS Int64) IN ([Int64(NULL), Int64(3), Int64(4), Int64(6)])
    TableScan: ?table? projection=[k, x, c]
```

56.0.0-rc1, wrong (projection [True, True, None, True, True, False, False, True], kept None, want [True, True, None, True, True, None, None, True]):

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Int64(1) AS z1
  Filter: ?table?.x IN ([RunEndEncoded(Field { name: "run_ends", data_type: Int32 }, Field { name: "values", data_type: Int64, nullable: true }, Int64(1)), RunEndEncoded(Field { name: "run_ends", data_type: Int32 }, Field { name: "values", data_type: Int64, nullable: true }, Int64(2)), RunEndEncoded(Field { name: "run_ends", data_type: Int32 }, Field { name: "values", data_type: Int64, nullable: true }, Int64(5)), RunEndEncoded(Field { name: "run_ends", data_type: Int32 }, Field { name: "values", data_type: Int64, nullable: true }, Int64(9)), RunEndEncoded(Field { name: "run_ends", data_type: Int32 }, Field { name: "values", data_type: Int64, nullable: true }, Int64(3)), RunEndEncoded(Field { name: "run_ends", data_type: Int32 }, Field { name: "values", data_type: Int64, nullable: true }, Int64(4)), RunEndEncoded(Field { name: "run_ends", data_type: Int32 }, Field { name: "values", data_type: Int64, nullable: true }, Int64(6))])
    TableScan: ?table? projection=[k, x, c]
```


## Volatile values

Share of TRUE rows of 2000, projection / filter. Inline, each test draws once and the two draws are independent; staged, both tests read one draw. A tolerance of 0.06 separates the two (about six standard deviations).

| Test | Two draws | One draw | 55.0.0 inline | 55.0.0 staged | 56.0.0-rc1 inline | 56.0.0-rc1 staged |
|---|---|---|---|---|---|---|
| `float in[0,5,6,7] & in[1,5,6,7]` | 0.25 | 0.0 | 0.26 / 0.24 ok | 0.00 / 0.00 ok | 0.23 / 0.25 ok | 0.00 / 0.00 ok |
| `float in[0,5,6,7] | in[1,5,6,7]` | 0.75 | 1.0 | 0.75 / 0.75 ok | 1.00 / 1.00 ok | 0.76 / 0.76 ok | 1.00 / 1.00 ok |
| `int in[0,5,6,7] & in[1,5,6,7]` | 0.25 | 0.0 | 0.25 / 0.25 ok | 0.00 / 0.00 ok | 0.25 / 0.24 ok | 0.00 / 0.00 ok |
| `int in[0,5,6,7] & ~in[0,8,9,10]` | 0.25 | 0.0 | 0.24 / 0.24 ok | 0.00 / 0.00 ok | 0.26 / 0.26 ok | 0.00 / 0.00 ok |
| `int in[0,5,6,7] | in[1,5,6,7]` | 0.75 | 1.0 | 0.75 / 0.75 ok | 1.00 / 1.00 ok | 0.75 / 0.75 ok | 1.00 / 1.00 ok |
| `zero in[0.0,5,6,7] & in[-0.0,5,6,7]` | 1.0 | 1.0 | 1.00 / 1.00 ok | 1.00 / 1.00 ok | 1.00 / 1.00 ok | 1.00 / 1.00 ok |

## `test_two_columns_holding_zeros_of_either_sign` (strict xfail, #296; PR #402's unsorted input)

| Layout | 55.0.0 | 56.0.0-rc1 |
|---|---|---|
| unsorted | wrong: kept [] | right |
| sorted | right | right |

unsorted, 55.0.0:

```text
EmptyRelation: rows=0
```

unsorted, 56.0.0-rc1:

```text
Projection: ?table?.k, ?table?.x, Float64(-0) AS a, Float64(0) AS b
  Filter: (?table?.x IN ([Float64(1), Float64(2.5), Float64(7)]) OR ?table?.x >= Float64(0) AND ?table?.x <= Float64(0)) AND (?table?.x IN ([Float64(3), Float64(4), Float64(5)]) OR ?table?.x >= Float64(0) AND ?table?.x <= Float64(0))
    TableScan: ?table? projection=[k, x]
```

sorted, 55.0.0:

```text
Projection: ?table?.k, ?table?.x, a, b
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, Float64(-0) AS a, Float64(0) AS b, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, __ltseq_position_0], full_filters=[?table?.x = Float64(1) OR ?table?.x = Float64(2.5) OR ?table?.x = Float64(7) OR ?table?.x >= Float64(-0) AND ?table?.x <= Float64(0), ?table?.x = Float64(3) OR ?table?.x = Float64(4) OR ?table?.x = Float64(5) OR ?table?.x >= Float64(-0) AND ?table?.x <= Float64(0)]
```

sorted, 56.0.0-rc1:

```text
Projection: ?table?.k, ?table?.x, a, b
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, Float64(-0) AS a, Float64(0) AS b, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, __ltseq_position_0], full_filters=[?table?.x IN ([Float64(1), Float64(2.5), Float64(7)]) OR ?table?.x >= Float64(-0) AND ?table?.x <= Float64(0), ?table?.x IN ([Float64(3), Float64(4), Float64(5)]) OR ?table?.x >= Float64(-0) AND ?table?.x <= Float64(0)]
```
