# Plans of representative cases (`repro_296.py --plans`)

For one case of each kind the matrix found wrong on the first engine and right on the second, on both layouts: the answers, and the projection's and the filter's optimized logical and physical plans. Over a sorted table the filter is pushed into the scan, which plans it apart when it executes; that inner plan is recorded through the spike's `LTSEQ_SPIKE_TRACE_INNER` switch. `REPORT.md` reads each case as a demonstrated rewrite fix, a valid fold, or inconclusive plan evidence.

| Kind | Layout | Expected projection / kept | 55.0.0 | 56.0.0-rc1 |
|---|---|---|---|---|
| NULL item, Int64, IN minus NOT IN | unsorted | [NULL, NULL, NULL, NULL, FALSE, FALSE, FALSE, FALSE] / kept [] | projection **wrong** [TRUE, TRUE, NULL, TRUE, FALSE, FALSE, FALSE, FALSE]; filter **wrong**, kept [0, 1, 3] | projection right; filter right |
| NULL item, Int64, IN minus NOT IN | sorted | [NULL, NULL, NULL, NULL, FALSE, FALSE, FALSE, FALSE] / kept [] | projection **wrong** [TRUE, TRUE, NULL, TRUE, FALSE, FALSE, FALSE, FALSE]; filter **wrong**, kept [0, 1, 3] | projection right; filter right |
| NULL item, string, IN minus NOT IN | unsorted | [NULL, NULL, NULL, NULL, FALSE, FALSE, FALSE, FALSE] / kept [] | projection **wrong** [TRUE, TRUE, NULL, TRUE, FALSE, FALSE, FALSE, FALSE]; filter **wrong**, kept [0, 1, 3] | projection right; filter right |
| NULL item, string, IN minus NOT IN | sorted | [NULL, NULL, NULL, NULL, FALSE, FALSE, FALSE, FALSE] / kept [] | projection **wrong** [TRUE, TRUE, NULL, TRUE, FALSE, FALSE, FALSE, FALSE]; filter **wrong**, kept [0, 1, 3] | projection right; filter right |
| NULL item, dictionary Int64, IN minus NOT IN | unsorted | [NULL, NULL, NULL, NULL, FALSE, FALSE, FALSE, FALSE] / kept [] | projection **wrong** [TRUE, TRUE, NULL, TRUE, FALSE, FALSE, FALSE, FALSE]; filter **wrong**, kept [0, 1, 3] | projection right; filter right |
| NULL item, dictionary Int64, IN minus NOT IN | sorted | [NULL, NULL, NULL, NULL, FALSE, FALSE, FALSE, FALSE] / kept [] | projection **wrong** [TRUE, TRUE, NULL, TRUE, FALSE, FALSE, FALSE, FALSE]; filter **wrong**, kept [0, 1, 3] | projection right; filter right |
| NULL item, Int64, intersection (projection only) | unsorted | [NULL, NULL, NULL, NULL, FALSE, FALSE, FALSE, NULL] / kept [] | projection **wrong** [FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE]; filter right | projection right; filter right |
| NULL item, Int64, intersection (projection only) | sorted | [NULL, NULL, NULL, NULL, FALSE, FALSE, FALSE, NULL] / kept [] | projection **wrong** [FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE]; filter right | projection right; filter right |
| NULL item and signed zero, float64, negated intersection | unsorted | [FALSE, FALSE, TRUE, NULL, TRUE, TRUE, FALSE, FALSE, TRUE, NULL] / kept [2, 4, 5, 8] | projection **wrong** [FALSE, FALSE, TRUE, NULL, TRUE, TRUE, FALSE, FALSE, TRUE, TRUE]; filter **wrong**, kept [2, 4, 5, 8, 9] | projection right; filter right |
| NULL item and signed zero, float64, negated intersection | sorted | [FALSE, FALSE, TRUE, NULL, TRUE, TRUE, FALSE, FALSE, TRUE, NULL] / kept [2, 4, 5, 8] | projection **wrong** [FALSE, FALSE, TRUE, NULL, TRUE, TRUE, FALSE, FALSE, TRUE, TRUE]; filter **wrong**, kept [2, 4, 5, 8, 9] | projection right; filter right |
| Staged signed zeros, float64 (filter only) | unsorted | [TRUE, TRUE, FALSE, NULL, FALSE, FALSE, TRUE, TRUE, FALSE, FALSE] / kept [0, 1, 6, 7] | projection right; filter **wrong**, kept [] | projection right; filter right |
| Staged signed zeros, float64 (filter only) | sorted | [TRUE, TRUE, FALSE, NULL, FALSE, FALSE, TRUE, TRUE, FALSE, FALSE] / kept [0, 1, 6, 7] | projection right; filter **wrong**, kept [] | projection right; filter right |
| Staged signed zeros, float32 (filter only) | unsorted | [TRUE, TRUE, FALSE, NULL, FALSE, FALSE, TRUE, TRUE, FALSE, FALSE] / kept [0, 1, 6, 7] | projection right; filter **wrong**, kept [] | projection right; filter right |
| Staged signed zeros, float32 (filter only) | sorted | [TRUE, TRUE, FALSE, NULL, FALSE, FALSE, TRUE, TRUE, FALSE, FALSE] / kept [0, 1, 6, 7] | projection right; filter **wrong**, kept [] | projection right; filter right |
| Staged Int64 item and NULL item (filter only) | unsorted | [NULL, NULL, NULL, FALSE, FALSE, FALSE, FALSE, FALSE] / kept [] | projection right; filter **wrong**, kept [0, 1] | projection right; filter right |
| Staged Int64 item and NULL item (filter only) | sorted | [NULL, NULL, NULL, FALSE, FALSE, FALSE, FALSE, FALSE] / kept [] | projection right; filter **wrong**, kept [0, 1] | projection right; filter right |

## NULL item, Int64, IN minus NOT IN

`int64 (is_in[1, 2, 5] & ~is_in[None, 3, 4, 6])`

unsorted, 55.0.0, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Int64(1) AS z1, ?table?.x = Int64(1) OR ?table?.x = Int64(2) OR ?table?.x = Int64(5) AS p
  TableScan: ?table? projection=[k, x, c]
```

unsorted, 56.0.0-rc1, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Int64(1) AS z1, ?table?.x IN ([Int64(1), Int64(2), Int64(5)]) AND ?table?.x NOT IN ([Int64(NULL), Int64(3), Int64(4), Int64(6)]) AS p
  TableScan: ?table? projection=[k, x, c]
```

unsorted, 55.0.0, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, 1 as z1, x@1 = 1 OR x@1 = 2 OR x@1 = 5 as p]
  DataSourceExec: partitions=1, partition_sizes=[1]
```

unsorted, 56.0.0-rc1, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, 1 as z1, x@1 IN (SET) ([1, 2, 5]) AND x@1 NOT IN (SET) ([NULL, 3, 4, 6]) as p]
  DataSourceExec: partitions=1, partition_sizes=[1]
```

unsorted, 55.0.0, filter logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Int64(1) AS z1
  Filter: ?table?.x = Int64(1) OR ?table?.x = Int64(2) OR ?table?.x = Int64(5)
    TableScan: ?table? projection=[k, x, c]
```

unsorted, 56.0.0-rc1, filter logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Int64(1) AS z1
  Filter: ?table?.x IN ([Int64(1), Int64(2), Int64(5)]) AND Boolean(NULL)
    TableScan: ?table? projection=[k, x, c]
```

unsorted, 55.0.0, filter physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, 1 as z1]
  FilterExec: x@1 = 1 OR x@1 = 2 OR x@1 = 5
    DataSourceExec: partitions=1, partition_sizes=[1]
```

unsorted, 56.0.0-rc1, filter physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, 1 as z1]
  FilterExec: x@1 IN (SET) ([1, 2, 5]) AND NULL
    DataSourceExec: partitions=1, partition_sizes=[1]
```

sorted, 55.0.0, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1, p
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1, ?table?.x = Int64(1) OR ?table?.x = Int64(2) OR ?table?.x = Int64(5) AS p, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z1, __ltseq_position_0]
```

sorted, 56.0.0-rc1, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1, p
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1, ?table?.x IN ([Int64(1), Int64(2), Int64(5)]) AND ?table?.x NOT IN ([Int64(NULL), Int64(3), Int64(4), Int64(6)]) AS p, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z1, __ltseq_position_0]
```

sorted, 55.0.0, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1, p@4 as p]
  SortPreservingMergeExec: [__ltseq_hoisted_0@5 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST]
    SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST], preserve_partitioning=[true]
      ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1, x@1 = 1 OR x@1 = 2 OR x@1 = 5 as p, k@0 as __ltseq_hoisted_0, __ltseq_position_0@4 as __ltseq_hoisted_1]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z1, __ltseq_position_0]
```

sorted, 56.0.0-rc1, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1, p@4 as p]
  SortPreservingMergeExec: [__ltseq_hoisted_0@5 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST]
    SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST], preserve_partitioning=[true]
      ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1, x@1 IN (SET) ([1, 2, 5]) AND x@1 NOT IN (SET) ([NULL, 3, 4, 6]) as p, k@0 as __ltseq_hoisted_0, __ltseq_position_0@4 as __ltseq_hoisted_1]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z1, __ltseq_position_0]
```

sorted, 55.0.0, filter logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z1, __ltseq_position_0], full_filters=[?table?.x = Int64(1) OR ?table?.x = Int64(2) OR ?table?.x = Int64(5)]
```

sorted, 56.0.0-rc1, filter logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z1, __ltseq_position_0], full_filters=[?table?.x IN ([Int64(1), Int64(2), Int64(5)]), Boolean(NULL)]
```

sorted, 55.0.0, filter physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1]
  SortPreservingMergeExec: [__ltseq_hoisted_0@4 ASC NULLS LAST, __ltseq_hoisted_1@5 ASC NULLS LAST]
    ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1, k@0 as __ltseq_hoisted_0, __ltseq_position_0@4 as __ltseq_hoisted_1]
      SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_position_0@4 ASC NULLS LAST], preserve_partitioning=[true]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z1, __ltseq_position_0]
```

sorted, 56.0.0-rc1, filter physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1]
  SortPreservingMergeExec: [__ltseq_hoisted_0@4 ASC NULLS LAST, __ltseq_hoisted_1@5 ASC NULLS LAST]
    ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1, k@0 as __ltseq_hoisted_0, __ltseq_position_0@4 as __ltseq_hoisted_1]
      SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_position_0@4 ASC NULLS LAST], preserve_partitioning=[true]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z1, __ltseq_position_0]
```

sorted, 55.0.0, filter inner:

```text
=== inner logical
Projection: ?table?.k, ?table?.x, ?table?.c, Int64(1) AS z1
  Filter: ?table?.x = Int64(1) OR ?table?.x = Int64(2) OR ?table?.x = Int64(5)
    TableScan: ?table? projection=[k, x, c]
=== inner physical
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, 1 as z1]
  FilterExec: x@1 = 1 OR x@1 = 2 OR x@1 = 5
    DataSourceExec: partitions=1, partition_sizes=[1]
```

sorted, 56.0.0-rc1, filter inner:

```text
=== inner logical
EmptyRelation: rows=0
=== inner physical
EmptyExec
```


## NULL item, string, IN minus NOT IN

`string (is_in['s1', 's2', 's5'] & ~is_in[None, 's3', 's4', 's6'])`

unsorted, 55.0.0, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Utf8("s1") AS z_s1_, ?table?.x = Utf8("s1") OR ?table?.x = Utf8("s2") OR ?table?.x = Utf8("s5") AS p
  TableScan: ?table? projection=[k, x, c]
```

unsorted, 56.0.0-rc1, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Utf8("s1") AS z_s1_, (?table?.x = Utf8("s1") OR ?table?.x = Utf8("s2") OR ?table?.x = Utf8("s5")) AND ?table?.x NOT IN ([Utf8(NULL), Utf8("s3"), Utf8("s4"), Utf8("s6")]) AS p
  TableScan: ?table? projection=[k, x, c]
```

unsorted, 55.0.0, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, s1 as z_s1_, x@1 = s1 OR x@1 = s2 OR x@1 = s5 as p]
  DataSourceExec: partitions=1, partition_sizes=[1]
```

unsorted, 56.0.0-rc1, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, s1 as z_s1_, (x@1 = s1 OR x@1 = s2 OR x@1 = s5) AND x@1 NOT IN (SET) ([NULL, s3, s4, s6]) as p]
  DataSourceExec: partitions=1, partition_sizes=[1]
```

unsorted, 55.0.0, filter logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Utf8("s1") AS z_s1_
  Filter: ?table?.x = Utf8("s1") OR ?table?.x = Utf8("s2") OR ?table?.x = Utf8("s5")
    TableScan: ?table? projection=[k, x, c]
```

unsorted, 56.0.0-rc1, filter logical:

```text
EmptyRelation: rows=0
```

unsorted, 55.0.0, filter physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, s1 as z_s1_]
  FilterExec: x@1 = s1 OR x@1 = s2 OR x@1 = s5
    DataSourceExec: partitions=1, partition_sizes=[1]
```

unsorted, 56.0.0-rc1, filter physical:

```text
EmptyExec
```

sorted, 55.0.0, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_s1_, p
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_s1_, ?table?.x = Utf8("s1") OR ?table?.x = Utf8("s2") OR ?table?.x = Utf8("s5") AS p, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z_s1_, __ltseq_position_0]
```

sorted, 56.0.0-rc1, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_s1_, p
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_s1_, (?table?.x = Utf8("s1") OR ?table?.x = Utf8("s2") OR ?table?.x = Utf8("s5")) AND ?table?.x NOT IN ([Utf8(NULL), Utf8("s3"), Utf8("s4"), Utf8("s6")]) AS p, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z_s1_, __ltseq_position_0]
```

sorted, 55.0.0, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_s1_@3 as z_s1_, p@4 as p]
  SortPreservingMergeExec: [__ltseq_hoisted_0@5 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST]
    SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST], preserve_partitioning=[true]
      ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_s1_@3 as z_s1_, x@1 = s1 OR x@1 = s2 OR x@1 = s5 as p, k@0 as __ltseq_hoisted_0, __ltseq_position_0@4 as __ltseq_hoisted_1]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z_s1_, __ltseq_position_0]
```

sorted, 56.0.0-rc1, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_s1_@3 as z_s1_, p@4 as p]
  SortPreservingMergeExec: [__ltseq_hoisted_0@5 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST]
    SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST], preserve_partitioning=[true]
      ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_s1_@3 as z_s1_, (x@1 = s1 OR x@1 = s2 OR x@1 = s5) AND x@1 NOT IN (SET) ([NULL, s3, s4, s6]) as p, k@0 as __ltseq_hoisted_0, __ltseq_position_0@4 as __ltseq_hoisted_1]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z_s1_, __ltseq_position_0]
```

sorted, 55.0.0, filter logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_s1_
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_s1_, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z_s1_, __ltseq_position_0], full_filters=[?table?.x = Utf8("s1") OR ?table?.x = Utf8("s2") OR ?table?.x = Utf8("s5")]
```

sorted, 56.0.0-rc1, filter logical:

```text
EmptyRelation: rows=0
```

sorted, 55.0.0, filter physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_s1_@3 as z_s1_]
  SortPreservingMergeExec: [__ltseq_hoisted_0@4 ASC NULLS LAST, __ltseq_hoisted_1@5 ASC NULLS LAST]
    ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_s1_@3 as z_s1_, k@0 as __ltseq_hoisted_0, __ltseq_position_0@4 as __ltseq_hoisted_1]
      SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_position_0@4 ASC NULLS LAST], preserve_partitioning=[true]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z_s1_, __ltseq_position_0]
```

sorted, 56.0.0-rc1, filter physical:

```text
EmptyExec
```

sorted, 55.0.0, filter inner:

```text
=== inner logical
Projection: ?table?.k, ?table?.x, ?table?.c, Utf8("s1") AS z_s1_
  Filter: ?table?.x = Utf8("s1") OR ?table?.x = Utf8("s2") OR ?table?.x = Utf8("s5")
    TableScan: ?table? projection=[k, x, c]
=== inner physical
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, s1 as z_s1_]
  FilterExec: x@1 = s1 OR x@1 = s2 OR x@1 = s5
    DataSourceExec: partitions=1, partition_sizes=[1]
```


## NULL item, dictionary Int64, IN minus NOT IN

`int64 dictionary (is_in[1, 2, 5] & ~is_in[None, 3, 4, 6])`

unsorted, 55.0.0, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Int64(1) AS z1, ?table?.x = Dictionary(Int32, Int64(1)) OR ?table?.x = Dictionary(Int32, Int64(2)) OR ?table?.x = Dictionary(Int32, Int64(5)) AS p
  TableScan: ?table? projection=[k, x, c]
```

unsorted, 56.0.0-rc1, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Int64(1) AS z1, (?table?.x = Dictionary(Int32, Int64(1)) OR ?table?.x = Dictionary(Int32, Int64(2)) OR ?table?.x = Dictionary(Int32, Int64(5))) AND ?table?.x NOT IN ([Dictionary(Int32, Int64(NULL)), Dictionary(Int32, Int64(3)), Dictionary(Int32, Int64(4)), Dictionary(Int32, Int64(6))]) AS p
  TableScan: ?table? projection=[k, x, c]
```

unsorted, 55.0.0, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, 1 as z1, x@1 = 1 OR x@1 = 2 OR x@1 = 5 as p]
  DataSourceExec: partitions=1, partition_sizes=[1]
```

unsorted, 56.0.0-rc1, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, 1 as z1, (x@1 = 1 OR x@1 = 2 OR x@1 = 5) AND x@1 NOT IN (SET) ([NULL, 3, 4, 6]) as p]
  DataSourceExec: partitions=1, partition_sizes=[1]
```

unsorted, 55.0.0, filter logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Int64(1) AS z1
  Filter: ?table?.x = Dictionary(Int32, Int64(1)) OR ?table?.x = Dictionary(Int32, Int64(2)) OR ?table?.x = Dictionary(Int32, Int64(5))
    TableScan: ?table? projection=[k, x, c]
```

unsorted, 56.0.0-rc1, filter logical:

```text
EmptyRelation: rows=0
```

unsorted, 55.0.0, filter physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, 1 as z1]
  FilterExec: x@1 = 1 OR x@1 = 2 OR x@1 = 5
    DataSourceExec: partitions=1, partition_sizes=[1]
```

unsorted, 56.0.0-rc1, filter physical:

```text
EmptyExec
```

sorted, 55.0.0, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1, p
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1, ?table?.x = Dictionary(Int32, Int64(1)) OR ?table?.x = Dictionary(Int32, Int64(2)) OR ?table?.x = Dictionary(Int32, Int64(5)) AS p, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z1, __ltseq_position_0]
```

sorted, 56.0.0-rc1, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1, p
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1, (?table?.x = Dictionary(Int32, Int64(1)) OR ?table?.x = Dictionary(Int32, Int64(2)) OR ?table?.x = Dictionary(Int32, Int64(5))) AND ?table?.x NOT IN ([Dictionary(Int32, Int64(NULL)), Dictionary(Int32, Int64(3)), Dictionary(Int32, Int64(4)), Dictionary(Int32, Int64(6))]) AS p, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z1, __ltseq_position_0]
```

sorted, 55.0.0, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1, p@4 as p]
  SortPreservingMergeExec: [__ltseq_hoisted_0@5 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST]
    SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST], preserve_partitioning=[true]
      ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1, x@1 = 1 OR x@1 = 2 OR x@1 = 5 as p, k@0 as __ltseq_hoisted_0, __ltseq_position_0@4 as __ltseq_hoisted_1]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z1, __ltseq_position_0]
```

sorted, 56.0.0-rc1, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1, p@4 as p]
  SortPreservingMergeExec: [__ltseq_hoisted_0@5 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST]
    SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST], preserve_partitioning=[true]
      ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1, (x@1 = 1 OR x@1 = 2 OR x@1 = 5) AND x@1 NOT IN (SET) ([NULL, 3, 4, 6]) as p, k@0 as __ltseq_hoisted_0, __ltseq_position_0@4 as __ltseq_hoisted_1]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z1, __ltseq_position_0]
```

sorted, 55.0.0, filter logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z1, __ltseq_position_0], full_filters=[?table?.x = Dictionary(Int32, Int64(1)) OR ?table?.x = Dictionary(Int32, Int64(2)) OR ?table?.x = Dictionary(Int32, Int64(5))]
```

sorted, 56.0.0-rc1, filter logical:

```text
EmptyRelation: rows=0
```

sorted, 55.0.0, filter physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1]
  SortPreservingMergeExec: [__ltseq_hoisted_0@4 ASC NULLS LAST, __ltseq_hoisted_1@5 ASC NULLS LAST]
    ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1, k@0 as __ltseq_hoisted_0, __ltseq_position_0@4 as __ltseq_hoisted_1]
      SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_position_0@4 ASC NULLS LAST], preserve_partitioning=[true]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z1, __ltseq_position_0]
```

sorted, 56.0.0-rc1, filter physical:

```text
EmptyExec
```

sorted, 55.0.0, filter inner:

```text
=== inner logical
Projection: ?table?.k, ?table?.x, ?table?.c, Int64(1) AS z1
  Filter: ?table?.x = Dictionary(Int32, Int64(1)) OR ?table?.x = Dictionary(Int32, Int64(2)) OR ?table?.x = Dictionary(Int32, Int64(5))
    TableScan: ?table? projection=[k, x, c]
=== inner physical
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, 1 as z1]
  FilterExec: x@1 = 1 OR x@1 = 2 OR x@1 = 5
    DataSourceExec: partitions=1, partition_sizes=[1]
```


## NULL item, Int64, intersection (projection only)

`int64 (eq|[1, 2, 5, 9] & is_in[None, 3, 4, 6])`

unsorted, 55.0.0, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Int64(1) AS z1, Boolean(false) AS p
  TableScan: ?table? projection=[k, x, c]
```

unsorted, 56.0.0-rc1, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Int64(1) AS z1, ?table?.x IN ([Int64(1), Int64(2), Int64(5), Int64(9)]) AND ?table?.x IN ([Int64(NULL), Int64(3), Int64(4), Int64(6)]) AS p
  TableScan: ?table? projection=[k, x, c]
```

unsorted, 55.0.0, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, 1 as z1, false as p]
  DataSourceExec: partitions=1, partition_sizes=[1]
```

unsorted, 56.0.0-rc1, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, 1 as z1, x@1 IN (SET) ([1, 2, 5, 9]) AND x@1 IN (SET) ([NULL, 3, 4, 6]) as p]
  DataSourceExec: partitions=1, partition_sizes=[1]
```

unsorted, 55.0.0, filter logical:

```text
EmptyRelation: rows=0
```

unsorted, 56.0.0-rc1, filter logical:

```text
EmptyRelation: rows=0
```

unsorted, 55.0.0, filter physical:

```text
EmptyExec
```

unsorted, 56.0.0-rc1, filter physical:

```text
EmptyExec
```

sorted, 55.0.0, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1, p
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1, Boolean(false) AS p, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z1, __ltseq_position_0]
```

sorted, 56.0.0-rc1, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1, p
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1, ?table?.x IN ([Int64(1), Int64(2), Int64(5), Int64(9)]) AND ?table?.x IN ([Int64(NULL), Int64(3), Int64(4), Int64(6)]) AS p, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z1, __ltseq_position_0]
```

sorted, 55.0.0, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1, p@4 as p]
  SortPreservingMergeExec: [__ltseq_hoisted_0@5 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST]
    SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST], preserve_partitioning=[true]
      ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1, false as p, k@0 as __ltseq_hoisted_0, __ltseq_position_0@4 as __ltseq_hoisted_1]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z1, __ltseq_position_0]
```

sorted, 56.0.0-rc1, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1, p@4 as p]
  SortPreservingMergeExec: [__ltseq_hoisted_0@5 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST]
    SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST], preserve_partitioning=[true]
      ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1, x@1 IN (SET) ([1, 2, 5, 9]) AND x@1 IN (SET) ([NULL, 3, 4, 6]) as p, k@0 as __ltseq_hoisted_0, __ltseq_position_0@4 as __ltseq_hoisted_1]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z1, __ltseq_position_0]
```

sorted, 55.0.0, filter logical:

```text
EmptyRelation: rows=0
```

sorted, 56.0.0-rc1, filter logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z1, __ltseq_position_0], full_filters=[?table?.x IN ([Int64(1), Int64(2), Int64(5), Int64(9)]), ?table?.x IN ([Int64(3), Int64(4), Int64(6)])]
```

sorted, 55.0.0, filter physical:

```text
EmptyExec
```

sorted, 56.0.0-rc1, filter physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1]
  SortPreservingMergeExec: [__ltseq_hoisted_0@4 ASC NULLS LAST, __ltseq_hoisted_1@5 ASC NULLS LAST]
    ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1, k@0 as __ltseq_hoisted_0, __ltseq_position_0@4 as __ltseq_hoisted_1]
      SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_position_0@4 ASC NULLS LAST], preserve_partitioning=[true]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z1, __ltseq_position_0]
```

sorted, 56.0.0-rc1, filter inner:

```text
=== inner logical
EmptyRelation: rows=0
=== inner physical
EmptyExec
```


## NULL item and signed zero, float64, negated intersection

`float64 ~(is_in[None, 0.0, 1.0, 2.5] & is_in[-0.0, 3.0, 4.0, 5.0])`

unsorted, 55.0.0, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Float64(-0) AS z_0_0, Float64(0) AS z0_0, ?table?.x < Float64(-0) OR ?table?.x > Float64(0) AS p
  TableScan: ?table? projection=[k, x, c]
```

unsorted, 56.0.0-rc1, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Float64(-0) AS z_0_0, Float64(0) AS z0_0, (?table?.x < Float64(-0) OR ?table?.x > Float64(0)) AND (?table?.x NOT IN ([Float64(NULL), Float64(1), Float64(2.5)]) OR ?table?.x NOT IN ([Float64(3), Float64(4), Float64(5)])) AS p
  TableScan: ?table? projection=[k, x, c]
```

unsorted, 55.0.0, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, -0 as z_0_0, 0 as z0_0, x@1 < -0 OR x@1 > 0 as p]
  DataSourceExec: partitions=1, partition_sizes=[1]
```

unsorted, 56.0.0-rc1, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, -0 as z_0_0, 0 as z0_0, (x@1 < -0 OR x@1 > 0) AND (x@1 NOT IN (SET) ([NULL, 1, 2.5]) OR x@1 NOT IN (SET) ([3, 4, 5])) as p]
  DataSourceExec: partitions=1, partition_sizes=[1]
```

unsorted, 55.0.0, filter logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Float64(-0) AS z_0_0, Float64(0) AS z0_0
  Filter: ?table?.x < Float64(-0) OR ?table?.x > Float64(0)
    TableScan: ?table? projection=[k, x, c]
```

unsorted, 56.0.0-rc1, filter logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Float64(-0) AS z_0_0, Float64(0) AS z0_0
  Filter: (?table?.x < Float64(-0) OR ?table?.x > Float64(0)) AND ?table?.x NOT IN ([Float64(3), Float64(4), Float64(5)])
    TableScan: ?table? projection=[k, x, c]
```

unsorted, 55.0.0, filter physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, -0 as z_0_0, 0 as z0_0]
  FilterExec: x@1 < -0 OR x@1 > 0
    DataSourceExec: partitions=1, partition_sizes=[1]
```

unsorted, 56.0.0-rc1, filter physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, -0 as z_0_0, 0 as z0_0]
  FilterExec: (x@1 < -0 OR x@1 > 0) AND x@1 NOT IN (SET) ([3, 4, 5])
    DataSourceExec: partitions=1, partition_sizes=[1]
```

sorted, 55.0.0, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0, p
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0, ?table?.x < Float64(-0) OR ?table?.x > Float64(0) AS p, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z_0_0, z0_0, __ltseq_position_0]
```

sorted, 56.0.0-rc1, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0, p
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0, (?table?.x < Float64(-0) OR ?table?.x > Float64(0)) AND (?table?.x NOT IN ([Float64(NULL), Float64(1), Float64(2.5)]) OR ?table?.x NOT IN ([Float64(3), Float64(4), Float64(5)])) AS p, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z_0_0, z0_0, __ltseq_position_0]
```

sorted, 55.0.0, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0, p@5 as p]
  SortPreservingMergeExec: [__ltseq_hoisted_0@6 ASC NULLS LAST, __ltseq_hoisted_1@7 ASC NULLS LAST]
    SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_hoisted_1@7 ASC NULLS LAST], preserve_partitioning=[true]
      ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0, x@1 < -0 OR x@1 > 0 as p, k@0 as __ltseq_hoisted_0, __ltseq_position_0@5 as __ltseq_hoisted_1]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z_0_0, z0_0, __ltseq_position_0]
```

sorted, 56.0.0-rc1, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0, p@5 as p]
  SortPreservingMergeExec: [__ltseq_hoisted_0@6 ASC NULLS LAST, __ltseq_hoisted_1@7 ASC NULLS LAST]
    SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_hoisted_1@7 ASC NULLS LAST], preserve_partitioning=[true]
      ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0, (x@1 < -0 OR x@1 > 0) AND (x@1 NOT IN (SET) ([NULL, 1, 2.5]) OR x@1 NOT IN (SET) ([3, 4, 5])) as p, k@0 as __ltseq_hoisted_0, __ltseq_position_0@5 as __ltseq_hoisted_1]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z_0_0, z0_0, __ltseq_position_0]
```

sorted, 55.0.0, filter logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z_0_0, z0_0, __ltseq_position_0], full_filters=[?table?.x < Float64(-0) OR ?table?.x > Float64(0)]
```

sorted, 56.0.0-rc1, filter logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z_0_0, z0_0, __ltseq_position_0], full_filters=[?table?.x < Float64(-0) OR ?table?.x > Float64(0), ?table?.x NOT IN ([Float64(3), Float64(4), Float64(5)])]
```

sorted, 55.0.0, filter physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0]
  SortPreservingMergeExec: [__ltseq_hoisted_0@5 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST]
    ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0, k@0 as __ltseq_hoisted_0, __ltseq_position_0@5 as __ltseq_hoisted_1]
      SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_position_0@5 ASC NULLS LAST], preserve_partitioning=[true]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z_0_0, z0_0, __ltseq_position_0]
```

sorted, 56.0.0-rc1, filter physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0]
  SortPreservingMergeExec: [__ltseq_hoisted_0@5 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST]
    ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0, k@0 as __ltseq_hoisted_0, __ltseq_position_0@5 as __ltseq_hoisted_1]
      SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_position_0@5 ASC NULLS LAST], preserve_partitioning=[true]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z_0_0, z0_0, __ltseq_position_0]
```

sorted, 55.0.0, filter inner:

```text
=== inner logical
Projection: ?table?.k, ?table?.x, ?table?.c, Float64(-0) AS z_0_0, Float64(0) AS z0_0
  Filter: ?table?.x < Float64(-0) OR ?table?.x > Float64(0)
    TableScan: ?table? projection=[k, x, c]
=== inner physical
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, -0 as z_0_0, 0 as z0_0]
  FilterExec: x@1 < -0 OR x@1 > 0
    DataSourceExec: partitions=1, partition_sizes=[1]
```

sorted, 56.0.0-rc1, filter inner:

```text
=== inner logical
Projection: ?table?.k, ?table?.x, ?table?.c, Float64(-0) AS z_0_0, Float64(0) AS z0_0
  Filter: (?table?.x < Float64(-0) OR ?table?.x > Float64(0)) AND ?table?.x NOT IN ([Float64(3), Float64(4), Float64(5)])
    TableScan: ?table? projection=[k, x, c]
=== inner physical
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, -0 as z_0_0, 0 as z0_0]
  FilterExec: (x@1 < -0 OR x@1 > 0) AND x@1 NOT IN (SET) ([3, 4, 5])
    DataSourceExec: partitions=1, partition_sizes=[1]
```


## Staged signed zeros, float64 (filter only)

`float64 (eq|['z(-0.0)', 1.0, 2.5, 7.0] & eq|['z(0.0)', 3.0, 4.0, 5.0])`

unsorted, 55.0.0, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, z_0_0, z0_0, (?table?.x = z_0_0 OR ?table?.x = Float64(1) OR ?table?.x = Float64(2.5) OR ?table?.x = Float64(7)) AND (?table?.x = z0_0 OR ?table?.x = Float64(3) OR ?table?.x = Float64(4) OR ?table?.x = Float64(5)) AS p
  Projection: ?table?.k, ?table?.x, ?table?.c, Float64(-0) AS z_0_0, Float64(0) AS z0_0
    TableScan: ?table? projection=[k, x, c]
```

unsorted, 56.0.0-rc1, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, z_0_0, z0_0, (?table?.x = z_0_0 OR ?table?.x = Float64(1) OR ?table?.x = Float64(2.5) OR ?table?.x = Float64(7)) AND (?table?.x = z0_0 OR ?table?.x = Float64(3) OR ?table?.x = Float64(4) OR ?table?.x = Float64(5)) AS p
  Projection: ?table?.k, ?table?.x, ?table?.c, Float64(-0) AS z_0_0, Float64(0) AS z0_0
    TableScan: ?table? projection=[k, x, c]
```

unsorted, 55.0.0, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0, (x@1 = z_0_0@3 OR x@1 = 1 OR x@1 = 2.5 OR x@1 = 7) AND (x@1 = z0_0@4 OR x@1 = 3 OR x@1 = 4 OR x@1 = 5) as p]
  ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, -0 as z_0_0, 0 as z0_0]
    DataSourceExec: partitions=1, partition_sizes=[1]
```

unsorted, 56.0.0-rc1, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0, (x@1 = z_0_0@3 OR x@1 = 1 OR x@1 = 2.5 OR x@1 = 7) AND (x@1 = z0_0@4 OR x@1 = 3 OR x@1 = 4 OR x@1 = 5) as p]
  ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, -0 as z_0_0, 0 as z0_0]
    DataSourceExec: partitions=1, partition_sizes=[1]
```

unsorted, 55.0.0, filter logical:

```text
EmptyRelation: rows=0
```

unsorted, 56.0.0-rc1, filter logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Float64(-0) AS z_0_0, Float64(0) AS z0_0
  Filter: (?table?.x IN ([Float64(1), Float64(2.5), Float64(7)]) OR ?table?.x >= Float64(0) AND ?table?.x <= Float64(0)) AND (?table?.x IN ([Float64(3), Float64(4), Float64(5)]) OR ?table?.x >= Float64(0) AND ?table?.x <= Float64(0))
    TableScan: ?table? projection=[k, x, c]
```

unsorted, 55.0.0, filter physical:

```text
EmptyExec
```

unsorted, 56.0.0-rc1, filter physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, -0 as z_0_0, 0 as z0_0]
  FilterExec: (x@1 IN (SET) ([1, 2.5, 7]) OR x@1 >= 0 AND x@1 <= 0) AND (x@1 IN (SET) ([3, 4, 5]) OR x@1 >= 0 AND x@1 <= 0)
    DataSourceExec: partitions=1, partition_sizes=[1]
```

sorted, 55.0.0, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0, p
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0, (?table?.z_0_0 = ?table?.x OR ?table?.x = Float64(1) OR ?table?.x = Float64(2.5) OR ?table?.x = Float64(7)) AND (?table?.z0_0 = ?table?.x OR ?table?.x = Float64(3) OR ?table?.x = Float64(4) OR ?table?.x = Float64(5)) AS p, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z_0_0, z0_0, __ltseq_position_0]
```

sorted, 56.0.0-rc1, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0, p
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0, (?table?.z_0_0 = ?table?.x OR ?table?.x = Float64(1) OR ?table?.x = Float64(2.5) OR ?table?.x = Float64(7)) AND (?table?.z0_0 = ?table?.x OR ?table?.x = Float64(3) OR ?table?.x = Float64(4) OR ?table?.x = Float64(5)) AS p, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z_0_0, z0_0, __ltseq_position_0]
```

sorted, 55.0.0, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0, p@5 as p]
  SortPreservingMergeExec: [__ltseq_hoisted_0@6 ASC NULLS LAST, __ltseq_hoisted_1@7 ASC NULLS LAST]
    SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_hoisted_1@7 ASC NULLS LAST], preserve_partitioning=[true]
      ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0, (z_0_0@3 = x@1 OR x@1 = 1 OR x@1 = 2.5 OR x@1 = 7) AND (z0_0@4 = x@1 OR x@1 = 3 OR x@1 = 4 OR x@1 = 5) as p, k@0 as __ltseq_hoisted_0, __ltseq_position_0@5 as __ltseq_hoisted_1]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z_0_0, z0_0, __ltseq_position_0]
```

sorted, 56.0.0-rc1, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0, p@5 as p]
  SortPreservingMergeExec: [__ltseq_hoisted_0@6 ASC NULLS LAST, __ltseq_hoisted_1@7 ASC NULLS LAST]
    SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_hoisted_1@7 ASC NULLS LAST], preserve_partitioning=[true]
      ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0, (z_0_0@3 = x@1 OR x@1 = 1 OR x@1 = 2.5 OR x@1 = 7) AND (z0_0@4 = x@1 OR x@1 = 3 OR x@1 = 4 OR x@1 = 5) as p, k@0 as __ltseq_hoisted_0, __ltseq_position_0@5 as __ltseq_hoisted_1]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z_0_0, z0_0, __ltseq_position_0]
```

sorted, 55.0.0, filter logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z_0_0, z0_0, __ltseq_position_0], full_filters=[?table?.z_0_0 = ?table?.x OR ?table?.x = Float64(1) OR ?table?.x = Float64(2.5) OR ?table?.x = Float64(7), ?table?.z0_0 = ?table?.x OR ?table?.x = Float64(3) OR ?table?.x = Float64(4) OR ?table?.x = Float64(5)]
```

sorted, 56.0.0-rc1, filter logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z_0_0, z0_0, __ltseq_position_0], full_filters=[?table?.z_0_0 = ?table?.x OR ?table?.x = Float64(1) OR ?table?.x = Float64(2.5) OR ?table?.x = Float64(7), ?table?.z0_0 = ?table?.x OR ?table?.x = Float64(3) OR ?table?.x = Float64(4) OR ?table?.x = Float64(5)]
```

sorted, 55.0.0, filter physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0]
  SortPreservingMergeExec: [__ltseq_hoisted_0@5 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST]
    ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0, k@0 as __ltseq_hoisted_0, __ltseq_position_0@5 as __ltseq_hoisted_1]
      SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_position_0@5 ASC NULLS LAST], preserve_partitioning=[true]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z_0_0, z0_0, __ltseq_position_0]
```

sorted, 56.0.0-rc1, filter physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0]
  SortPreservingMergeExec: [__ltseq_hoisted_0@5 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST]
    ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0, k@0 as __ltseq_hoisted_0, __ltseq_position_0@5 as __ltseq_hoisted_1]
      SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_position_0@5 ASC NULLS LAST], preserve_partitioning=[true]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z_0_0, z0_0, __ltseq_position_0]
```

sorted, 55.0.0, filter inner:

```text
=== inner logical
EmptyRelation: rows=0
=== inner physical
EmptyExec
```

sorted, 56.0.0-rc1, filter inner:

```text
=== inner logical
Projection: ?table?.k, ?table?.x, ?table?.c, Float64(-0) AS z_0_0, Float64(0) AS z0_0
  Filter: (?table?.x IN ([Float64(1), Float64(2.5), Float64(7)]) OR ?table?.x >= Float64(0) AND ?table?.x <= Float64(0)) AND (?table?.x IN ([Float64(3), Float64(4), Float64(5)]) OR ?table?.x >= Float64(0) AND ?table?.x <= Float64(0))
    TableScan: ?table? projection=[k, x, c]
=== inner physical
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, -0 as z_0_0, 0 as z0_0]
  FilterExec: (x@1 IN (SET) ([1, 2.5, 7]) OR x@1 >= 0 AND x@1 <= 0) AND (x@1 IN (SET) ([3, 4, 5]) OR x@1 >= 0 AND x@1 <= 0)
    DataSourceExec: partitions=1, partition_sizes=[1]
```


## Staged signed zeros, float32 (filter only)

`float32 (eq|['z(-0.0)', 1.0, 2.5, 7.0] & eq|['z(0.0)', 3.0, 4.0, 5.0])`

unsorted, 55.0.0, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, z_0_0, z0_0, (z_0_0 = __common_expr_1 OR __common_expr_1 = Float64(1) OR __common_expr_1 = Float64(2.5) OR __common_expr_1 = Float64(7)) AND (z0_0 = __common_expr_1 OR __common_expr_1 = Float64(3) OR __common_expr_1 = Float64(4) OR __common_expr_1 = Float64(5)) AS p
  Projection: CAST(?table?.x AS Float64) AS __common_expr_1, ?table?.k, ?table?.x, ?table?.c, Float64(-0) AS z_0_0, Float64(0) AS z0_0
    TableScan: ?table? projection=[k, x, c]
```

unsorted, 56.0.0-rc1, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, z_0_0, z0_0, (z_0_0 = __common_expr_1 OR __common_expr_1 = Float64(1) OR __common_expr_1 = Float64(2.5) OR __common_expr_1 = Float64(7)) AND (z0_0 = __common_expr_1 OR __common_expr_1 = Float64(3) OR __common_expr_1 = Float64(4) OR __common_expr_1 = Float64(5)) AS p
  Projection: CAST(?table?.x AS Float64) AS __common_expr_1, ?table?.k, ?table?.x, ?table?.c, Float64(-0) AS z_0_0, Float64(0) AS z0_0
    TableScan: ?table? projection=[k, x, c]
```

unsorted, 55.0.0, projection physical:

```text
ProjectionExec: expr=[k@1 as k, x@2 as x, c@3 as c, z_0_0@4 as z_0_0, z0_0@5 as z0_0, (z_0_0@4 = __common_expr_2@0 OR __common_expr_2@0 = 1 OR __common_expr_2@0 = 2.5 OR __common_expr_2@0 = 7) AND (z0_0@5 = __common_expr_2@0 OR __common_expr_2@0 = 3 OR __common_expr_2@0 = 4 OR __common_expr_2@0 = 5) as p]
  ProjectionExec: expr=[CAST(x@1 AS Float64) as __common_expr_2, k@0 as k, x@1 as x, c@2 as c, -0 as z_0_0, 0 as z0_0]
    DataSourceExec: partitions=1, partition_sizes=[1]
```

unsorted, 56.0.0-rc1, projection physical:

```text
ProjectionExec: expr=[k@1 as k, x@2 as x, c@3 as c, z_0_0@4 as z_0_0, z0_0@5 as z0_0, (z_0_0@4 = __common_expr_2@0 OR __common_expr_2@0 = 1 OR __common_expr_2@0 = 2.5 OR __common_expr_2@0 = 7) AND (z0_0@5 = __common_expr_2@0 OR __common_expr_2@0 = 3 OR __common_expr_2@0 = 4 OR __common_expr_2@0 = 5) as p]
  ProjectionExec: expr=[CAST(x@1 AS Float64) as __common_expr_2, k@0 as k, x@1 as x, c@2 as c, -0 as z_0_0, 0 as z0_0]
    DataSourceExec: partitions=1, partition_sizes=[1]
```

unsorted, 55.0.0, filter logical:

```text
EmptyRelation: rows=0
```

unsorted, 56.0.0-rc1, filter logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Float64(-0) AS z_0_0, Float64(0) AS z0_0
  Filter: (__common_expr_5 IN ([Float64(1), Float64(2.5), Float64(7)]) OR __common_expr_5 >= Float64(0) AND __common_expr_5 <= Float64(0)) AND (__common_expr_5 IN ([Float64(3), Float64(4), Float64(5)]) OR __common_expr_5 >= Float64(0) AND __common_expr_5 <= Float64(0))
    Projection: CAST(?table?.x AS Float64) AS __common_expr_5, ?table?.k, ?table?.x, ?table?.c
      TableScan: ?table? projection=[k, x, c]
```

unsorted, 55.0.0, filter physical:

```text
EmptyExec
```

unsorted, 56.0.0-rc1, filter physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, -0 as z_0_0, 0 as z0_0]
  FilterExec: (__common_expr_8@0 IN (SET) ([1, 2.5, 7]) OR __common_expr_8@0 >= 0 AND __common_expr_8@0 <= 0) AND (__common_expr_8@0 IN (SET) ([3, 4, 5]) OR __common_expr_8@0 >= 0 AND __common_expr_8@0 <= 0), projection=[k@1, x@2, c@3]
    ProjectionExec: expr=[CAST(x@1 AS Float64) as __common_expr_8, k@0 as k, x@1 as x, c@2 as c]
      DataSourceExec: partitions=1, partition_sizes=[1]
```

sorted, 55.0.0, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0, p
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0, (?table?.z_0_0 = __common_expr_1 OR __common_expr_1 = Float64(1) OR __common_expr_1 = Float64(2.5) OR __common_expr_1 = Float64(7)) AND (?table?.z0_0 = __common_expr_1 OR __common_expr_1 = Float64(3) OR __common_expr_1 = Float64(4) OR __common_expr_1 = Float64(5)) AS p, __ltseq_hoisted_0 AS __ltseq_hoisted_0, __ltseq_hoisted_1 AS __ltseq_hoisted_1
      Projection: CAST(?table?.x AS Float64) AS __common_expr_1, ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
        TableScan: ?table? projection=[k, x, c, z_0_0, z0_0, __ltseq_position_0]
```

sorted, 56.0.0-rc1, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0, p
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0, (?table?.z_0_0 = __common_expr_1 OR __common_expr_1 = Float64(1) OR __common_expr_1 = Float64(2.5) OR __common_expr_1 = Float64(7)) AND (?table?.z0_0 = __common_expr_1 OR __common_expr_1 = Float64(3) OR __common_expr_1 = Float64(4) OR __common_expr_1 = Float64(5)) AS p, __ltseq_hoisted_0 AS __ltseq_hoisted_0, __ltseq_hoisted_1 AS __ltseq_hoisted_1
      Projection: CAST(?table?.x AS Float64) AS __common_expr_1, ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
        TableScan: ?table? projection=[k, x, c, z_0_0, z0_0, __ltseq_position_0]
```

sorted, 55.0.0, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0, p@5 as p]
  SortPreservingMergeExec: [__ltseq_hoisted_0@6 ASC NULLS LAST, __ltseq_hoisted_1@7 ASC NULLS LAST]
    SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_hoisted_1@7 ASC NULLS LAST], preserve_partitioning=[true]
      ProjectionExec: expr=[k@1 as k, x@2 as x, c@3 as c, z_0_0@4 as z_0_0, z0_0@5 as z0_0, (z_0_0@4 = __common_expr_2@0 OR __common_expr_2@0 = 1 OR __common_expr_2@0 = 2.5 OR __common_expr_2@0 = 7) AND (z0_0@5 = __common_expr_2@0 OR __common_expr_2@0 = 3 OR __common_expr_2@0 = 4 OR __common_expr_2@0 = 5) as p, __ltseq_hoisted_0@6 as __ltseq_hoisted_0, __ltseq_hoisted_1@7 as __ltseq_hoisted_1]
        ProjectionExec: expr=[CAST(x@1 AS Float64) as __common_expr_2, k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0, k@0 as __ltseq_hoisted_0, __ltseq_position_0@5 as __ltseq_hoisted_1]
          StreamingTableExec: partition_sizes=8, projection=[k, x, c, z_0_0, z0_0, __ltseq_position_0]
```

sorted, 56.0.0-rc1, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0, p@5 as p]
  SortPreservingMergeExec: [__ltseq_hoisted_0@6 ASC NULLS LAST, __ltseq_hoisted_1@7 ASC NULLS LAST]
    SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_hoisted_1@7 ASC NULLS LAST], preserve_partitioning=[true]
      ProjectionExec: expr=[k@1 as k, x@2 as x, c@3 as c, z_0_0@4 as z_0_0, z0_0@5 as z0_0, (z_0_0@4 = __common_expr_2@0 OR __common_expr_2@0 = 1 OR __common_expr_2@0 = 2.5 OR __common_expr_2@0 = 7) AND (z0_0@5 = __common_expr_2@0 OR __common_expr_2@0 = 3 OR __common_expr_2@0 = 4 OR __common_expr_2@0 = 5) as p, __ltseq_hoisted_0@6 as __ltseq_hoisted_0, __ltseq_hoisted_1@7 as __ltseq_hoisted_1]
        ProjectionExec: expr=[CAST(x@1 AS Float64) as __common_expr_2, k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0, k@0 as __ltseq_hoisted_0, __ltseq_position_0@5 as __ltseq_hoisted_1]
          StreamingTableExec: partition_sizes=8, projection=[k, x, c, z_0_0, z0_0, __ltseq_position_0]
```

sorted, 55.0.0, filter logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z_0_0, z0_0, __ltseq_position_0], full_filters=[CAST(?table?.x AS Float64) = ?table?.z_0_0 OR CAST(?table?.x AS Float64) = Float64(1) OR CAST(?table?.x AS Float64) = Float64(2.5) OR CAST(?table?.x AS Float64) = Float64(7), CAST(?table?.x AS Float64) = ?table?.z0_0 OR CAST(?table?.x AS Float64) = Float64(3) OR CAST(?table?.x AS Float64) = Float64(4) OR CAST(?table?.x AS Float64) = Float64(5)]
```

sorted, 56.0.0-rc1, filter logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z_0_0, ?table?.z0_0, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z_0_0, z0_0, __ltseq_position_0], full_filters=[CAST(?table?.x AS Float64) = ?table?.z_0_0 OR CAST(?table?.x AS Float64) = Float64(1) OR CAST(?table?.x AS Float64) = Float64(2.5) OR CAST(?table?.x AS Float64) = Float64(7), CAST(?table?.x AS Float64) = ?table?.z0_0 OR CAST(?table?.x AS Float64) = Float64(3) OR CAST(?table?.x AS Float64) = Float64(4) OR CAST(?table?.x AS Float64) = Float64(5)]
```

sorted, 55.0.0, filter physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0]
  SortPreservingMergeExec: [__ltseq_hoisted_0@5 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST]
    ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0, k@0 as __ltseq_hoisted_0, __ltseq_position_0@5 as __ltseq_hoisted_1]
      SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_position_0@5 ASC NULLS LAST], preserve_partitioning=[true]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z_0_0, z0_0, __ltseq_position_0]
```

sorted, 56.0.0-rc1, filter physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0]
  SortPreservingMergeExec: [__ltseq_hoisted_0@5 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST]
    ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z_0_0@3 as z_0_0, z0_0@4 as z0_0, k@0 as __ltseq_hoisted_0, __ltseq_position_0@5 as __ltseq_hoisted_1]
      SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_position_0@5 ASC NULLS LAST], preserve_partitioning=[true]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z_0_0, z0_0, __ltseq_position_0]
```

sorted, 55.0.0, filter inner:

```text
=== inner logical
EmptyRelation: rows=0
=== inner physical
EmptyExec
```

sorted, 56.0.0-rc1, filter inner:

```text
=== inner logical
Projection: ?table?.k, ?table?.x, ?table?.c, Float64(-0) AS z_0_0, Float64(0) AS z0_0
  Filter: (__common_expr_9 IN ([Float64(1), Float64(2.5), Float64(7)]) OR __common_expr_9 >= Float64(0) AND __common_expr_9 <= Float64(0)) AND (__common_expr_9 IN ([Float64(3), Float64(4), Float64(5)]) OR __common_expr_9 >= Float64(0) AND __common_expr_9 <= Float64(0))
    Projection: CAST(?table?.x AS Float64) AS __common_expr_9, ?table?.k, ?table?.x, ?table?.c
      TableScan: ?table? projection=[k, x, c]
=== inner physical
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, -0 as z_0_0, 0 as z0_0]
  FilterExec: (__common_expr_6@0 IN (SET) ([1, 2.5, 7]) OR __common_expr_6@0 >= 0 AND __common_expr_6@0 <= 0) AND (__common_expr_6@0 IN (SET) ([3, 4, 5]) OR __common_expr_6@0 >= 0 AND __common_expr_6@0 <= 0), projection=[k@1, x@2, c@3]
    ProjectionExec: expr=[CAST(x@1 AS Float64) as __common_expr_6, k@0 as k, x@1 as x, c@2 as c]
      DataSourceExec: partitions=1, partition_sizes=[1]
```


## Staged Int64 item and NULL item (filter only)

`int64 (eq|['z(1)', 2, 10, 11] & ~is_in[None, 3, 4, 6])`

unsorted, 55.0.0, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, z1, (?table?.x = z1 OR ?table?.x = Int64(2) OR ?table?.x = Int64(10) OR ?table?.x = Int64(11)) AND ?table?.x NOT IN ([Int64(NULL), Int64(3), Int64(4), Int64(6)]) AS p
  Projection: ?table?.k, ?table?.x, ?table?.c, Int64(1) AS z1
    TableScan: ?table? projection=[k, x, c]
```

unsorted, 56.0.0-rc1, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, z1, (?table?.x = z1 OR ?table?.x = Int64(2) OR ?table?.x = Int64(10) OR ?table?.x = Int64(11)) AND ?table?.x NOT IN ([Int64(NULL), Int64(3), Int64(4), Int64(6)]) AS p
  Projection: ?table?.k, ?table?.x, ?table?.c, Int64(1) AS z1
    TableScan: ?table? projection=[k, x, c]
```

unsorted, 55.0.0, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1, (x@1 = z1@3 OR x@1 = 2 OR x@1 = 10 OR x@1 = 11) AND x@1 NOT IN (SET) ([NULL, 3, 4, 6]) as p]
  ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, 1 as z1]
    DataSourceExec: partitions=1, partition_sizes=[1]
```

unsorted, 56.0.0-rc1, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1, (x@1 = z1@3 OR x@1 = 2 OR x@1 = 10 OR x@1 = 11) AND x@1 NOT IN (SET) ([NULL, 3, 4, 6]) as p]
  ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, 1 as z1]
    DataSourceExec: partitions=1, partition_sizes=[1]
```

unsorted, 55.0.0, filter logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, Int64(1) AS z1
  Filter: ?table?.x IN ([Int64(1), Int64(2), Int64(10), Int64(11)])
    TableScan: ?table? projection=[k, x, c]
```

unsorted, 56.0.0-rc1, filter logical:

```text
EmptyRelation: rows=0
```

unsorted, 55.0.0, filter physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, 1 as z1]
  FilterExec: x@1 IN (SET) ([1, 2, 10, 11])
    DataSourceExec: partitions=1, partition_sizes=[1]
```

unsorted, 56.0.0-rc1, filter physical:

```text
EmptyExec
```

sorted, 55.0.0, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1, p
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1, (?table?.z1 = ?table?.x OR ?table?.x = Int64(2) OR ?table?.x = Int64(10) OR ?table?.x = Int64(11)) AND ?table?.x NOT IN ([Int64(NULL), Int64(3), Int64(4), Int64(6)]) AS p, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z1, __ltseq_position_0]
```

sorted, 56.0.0-rc1, projection logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1, p
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1, (?table?.z1 = ?table?.x OR ?table?.x = Int64(2) OR ?table?.x = Int64(10) OR ?table?.x = Int64(11)) AND ?table?.x NOT IN ([Int64(NULL), Int64(3), Int64(4), Int64(6)]) AS p, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z1, __ltseq_position_0]
```

sorted, 55.0.0, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1, p@4 as p]
  SortPreservingMergeExec: [__ltseq_hoisted_0@5 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST]
    SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST], preserve_partitioning=[true]
      ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1, (z1@3 = x@1 OR x@1 = 2 OR x@1 = 10 OR x@1 = 11) AND x@1 NOT IN (SET) ([NULL, 3, 4, 6]) as p, k@0 as __ltseq_hoisted_0, __ltseq_position_0@4 as __ltseq_hoisted_1]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z1, __ltseq_position_0]
```

sorted, 56.0.0-rc1, projection physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1, p@4 as p]
  SortPreservingMergeExec: [__ltseq_hoisted_0@5 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST]
    SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_hoisted_1@6 ASC NULLS LAST], preserve_partitioning=[true]
      ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1, (z1@3 = x@1 OR x@1 = 2 OR x@1 = 10 OR x@1 = 11) AND x@1 NOT IN (SET) ([NULL, 3, 4, 6]) as p, k@0 as __ltseq_hoisted_0, __ltseq_position_0@4 as __ltseq_hoisted_1]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z1, __ltseq_position_0]
```

sorted, 55.0.0, filter logical:

```text
Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1
  Sort: __ltseq_hoisted_0 ASC NULLS LAST, __ltseq_hoisted_1 ASC NULLS LAST
    Projection: ?table?.k, ?table?.x, ?table?.c, ?table?.z1, ?table?.k AS __ltseq_hoisted_0, ?table?.__ltseq_position_0 AS __ltseq_hoisted_1
      TableScan: ?table? projection=[k, x, c, z1, __ltseq_position_0], full_filters=[?table?.z1 = ?table?.x OR ?table?.x = Int64(2) OR ?table?.x = Int64(10) OR ?table?.x = Int64(11), ?table?.x NOT IN ([Int64(NULL), Int64(3), Int64(4), Int64(6)])]
```

sorted, 56.0.0-rc1, filter logical:

```text
EmptyRelation: rows=0
```

sorted, 55.0.0, filter physical:

```text
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1]
  SortPreservingMergeExec: [__ltseq_hoisted_0@4 ASC NULLS LAST, __ltseq_hoisted_1@5 ASC NULLS LAST]
    ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, z1@3 as z1, k@0 as __ltseq_hoisted_0, __ltseq_position_0@4 as __ltseq_hoisted_1]
      SortExec: expr=[k@0 ASC NULLS LAST, __ltseq_position_0@4 ASC NULLS LAST], preserve_partitioning=[true]
        StreamingTableExec: partition_sizes=8, projection=[k, x, c, z1, __ltseq_position_0]
```

sorted, 56.0.0-rc1, filter physical:

```text
EmptyExec
```

sorted, 55.0.0, filter inner:

```text
=== inner logical
Projection: ?table?.k, ?table?.x, ?table?.c, Int64(1) AS z1
  Filter: ?table?.x IN ([Int64(1), Int64(2), Int64(10), Int64(11)])
    TableScan: ?table? projection=[k, x, c]
=== inner physical
ProjectionExec: expr=[k@0 as k, x@1 as x, c@2 as c, 1 as z1]
  FilterExec: x@1 IN (SET) ([1, 2, 10, 11])
    DataSourceExec: partitions=1, partition_sizes=[1]
```

