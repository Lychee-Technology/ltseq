<!-- Generated from ../../en/12_streaming_cursor.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 12 · Streaming Big Files: Cursor

> **Level** ★★☆ · **Domain** General / logs · **Prereqs** Case 01
>
> **APIs used**: `LTSeq.scan` / `Cursor` (iteration / `count` / `to_pandas` / `schema` / `columns`) / `read_csv` / `write_parquet` / `scan_parquet`

**Learning goals**

- The right posture when data does not fit in memory: **pull batches through a
  cursor**, release as you go
- Master the cursor's three consumption modes: batch iteration, pure counting,
  materialize-the-rest
- Understand why Parquet + zstd is the default for large files

## The problem

A 200k-row access log CSV. It fits in memory — but pretend it doesn't (the
conclusion is identical at 200 million rows). With a cursor:

1. Inspect columns and types without loading the file;
2. Iterate batch by batch — and exit early after 3 batches;
3. Count rows without materializing any into Python;
4. After consuming part of the stream, materialize the **remaining** part into
   pandas;
5. Convert to Parquet (zstd) and stream it back.

```python
import random

rng = random.Random(9)
with open("access_log.csv", "w") as f:
    f.write("ts,user_id,status,latency_ms\n")
    ts = 1_700_000_000
    for i in range(200_000):
        ts += rng.randint(1, 5)
        status = rng.choice([200, 200, 200, 404, 500])
        f.write(f"{ts},u{rng.randint(1, 5000)},{status},{rng.randint(5, 900)}\n")

print("access_log.csv ready")
```

> **Expected output**
>
> ```text
> access_log.csv ready
> ```

## Step 1 · Cursor: schema, batch iteration, early exit

`LTSeq.scan(path)` returns a `Cursor` **without loading the file**; iterating
yields `pyarrow.RecordBatch` objects.

```python
from ltseq import LTSeq

cursor = LTSeq.scan("access_log.csv")
print("columns:", cursor.columns)
print("schema: ", cursor.schema)

batch_sizes = []
for batch in cursor:                    # batch: pyarrow.RecordBatch
    batch_sizes.append(batch.num_rows)
    if len(batch_sizes) == 3:
        break                           # early exit: stop pulling batches

print("first 3 batch sizes:", batch_sizes)
```

> **Expected output**
>
> ```text
> columns: ['ts', 'user_id', 'status', 'latency_ms']
> schema:  {'ts': 'Int64', 'user_id': 'Utf8', 'status': 'Int64', 'latency_ms': 'Int64'}
> first 3 batch sizes: [16384, 16384, 16384]
> ```

The batch sizes depend on the machine: the scan is split into one partition
per CPU core by default, each partition cuts its share of the file into
batches of at most 16,384 rows, and the partitions race, so even on one
machine a partition's short last batch can occasionally appear among the
first three.

**Notes**

- `cursor.columns` / `cursor.schema` return the schema `scan()` settled when
  it opened the file (a CSV's is inferred from its first rows; Parquet stores
  its own); reading them loads no data.
- `break` only stops pulling: the engine reads a little ahead and then
  waits, and the cursor keeps its place, so another loop or `next(cursor)`
  resumes with the next batch. Execution is **cancelled** and its resources
  released when the cursor itself is dropped (`del cursor`, or rebinding the
  name as Step 2 does).
- Batches do not arrive in file order: the partitions' batches interleave,
  in an order that can change between runs (issue #148). Per-batch code must
  not assume a batch continues the previous one.
- Batch size is the engine's choice; business code always processes per batch
  and never depends on the concrete value.

## Step 2 · Pure counting and materialize-the-rest

```python
# 2) pure counting: consume the whole stream, materialize nothing
total = LTSeq.scan("access_log.csv").count()
print("total rows:", total)

# 3) partial iteration + materialize the rest
cursor = LTSeq.scan("access_log.csv")
first = next(iter(cursor))              # pull only the first batch
rest = cursor.to_pandas()               # materialize from the current position
print("first batch:", first.num_rows, "/ remaining:", len(rest))
```

> **Expected output**
>
> ```text
> total rows: 200000
> first batch: 16384 / remaining: 183616
> ```

Both numbers depend on the machine, for the same reason as the batch sizes
above.

**Notes**

- `count()` is the cheapest full consumption: one streaming pass, a running
  row count, memory decoupled from file size.
- `to_pandas()` materializes the **remaining** stream — wherever iteration
  stopped, it continues from there; `to_arrow()` behaves the same.
- Materializing 200k rows is fine; at 200 million, think about the downstream
  first (batched writes / aggregate before materializing).

## Step 3 · Parquet: smaller, faster, typed

```python
t = LTSeq.read_csv("access_log.csv")
t.write_parquet("access_log.parquet", compression="zstd")

cursor = LTSeq.scan_parquet("access_log.parquet")
print("parquet rows:", cursor.count())
```

> **Expected output**
>
> ```text
> parquet rows: 200000
> ```

**Notes**

- `write_parquet(path, compression=)` supports `snappy / zstd / gzip / lz4 /
  none`; zstd balances ratio and speed for log-like data.
- Parquet is columnar and stores its schema, so types come back exactly as
  written and downstream `dt.*` needs no casts. CSV types are re-inferred
  from text: ISO dates and timestamps survive, but a decimal comes back as
  float64 and a date like `2024/01/02` as a string.
- **Pre-sorted Parquet + windows** are a golden combo:
  `LTSeq.read_parquet(...)` then `assume_sorted("ts")` skips the physical
  sort and goes straight into window computation (a cursor has no
  `assume_sorted`). The declaration must be true, so write such a file from
  a sorted table, `t.sort("ts").write_parquet(...)`: until issue #148 is
  fixed, a table with no declared order, like `t` above, is not written in
  its CSV row order.

## Key takeaways

1. **Three ways to consume a cursor**: batch iteration (custom processing),
   `count()` (pure counting), `to_pandas()/to_arrow()` (materialize the rest) —
   memory profiles differ enormously; choose by downstream need.
2. **Early exit is a feature**: after `break` the engine stops reading, and
   dropping the cursor cancels execution and releases resources — on a big
   file, "just the first N batches" costs a few batches, not the whole file.
3. **CSV in, Parquet stored**: one conversion, long-term payoff (size, speed,
   types).
4. **LTSeq is also an Arrow producer**: every `LTSeq` implements
   `__arrow_c_stream__` — `pa.table(t)`, `duckdb.sql("... FROM t")`,
   `pl.from_arrow(t)` consume it directly, so every stage of a streaming
   pipeline can mix Arrow-ecosystem parts.

## Exercises

- Big-file sessionization: write the log sorted by `user_id, ts`, read it
  back with `LTSeq.read_parquet` + `assume_sorted("user_id", "ts")`, then
  run case 10's trio = a complete big-file pipeline. (A cursor only yields
  raw batches, so the trio runs on the table.)
- Batched writes: iterate the cursor and hand
  `pa.Table.from_batches([batch])` to your writer.
- Open the repo's `benchmarks/README.md` and ClickBench harness to see how
  LTSeq stacks up against DuckDB on the ClickBench `hits` dataset.

---
**Previous**: [11 · Error bursts](11_error_burst.md) · **Next**: [13 · Top-N within groups](13_topn_within_group.md)
