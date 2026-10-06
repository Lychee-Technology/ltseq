# Benchmark Guide

This document describes how to run LTSeq's local operation benchmarks and the
ClickBench comparison benchmark.

## Prerequisites

The ClickBench comparison and data-preparation scripts import `duckdb` + `psutil`,
which live in the optional `bench` dependency group. Activate the group on **each**
run with `uv run --group bench` so those packages are synced into the venv. Do
**not** run `uv sync --group bench` once and then use a plain `uv run`: the plain
run re-syncs only the default groups and removes `duckdb`/`psutil` again, which is
exactly the failure this guide is written to avoid. The core operation benchmark
needs no external deps and can use a plain `uv run`.

Build the Rust extension in release mode before running benchmarks:

```bash
maturin develop --release
```

Rebuild the extension after changing Rust code or Rust dependencies. The
benchmark commands import the extension from the current Python environment.

## Run the Whole Suite

For a one-shot run, `benchmarks/run_all.py` orchestrates the rebuild, the core
operation benchmark, and the ClickBench comparison, then prints a consolidated
PASS/SKIP/FAIL summary:

```bash
# Fast smoke test: rebuild + core benchmark + ClickBench on the 1M-row sample
uv run --group bench python benchmarks/run_all.py --sample

# Full ClickBench comparison on the sorted dataset
uv run --group bench python benchmarks/run_all.py --full

# Skip the rebuild, or run only one part
uv run --group bench python benchmarks/run_all.py --skip-build
uv run python benchmarks/run_all.py --only core        # core only: no bench group needed
uv run --group bench python benchmarks/run_all.py --only vs
```

`run_all.py` does not download the ~14 GB ClickBench dataset on its own. If the
comparison data is missing, the `bench_vs` step is skipped with guidance; pass
`--prepare` to run `prepare_data.py` first (this downloads the data if absent).
The sections below document each underlying script directly.

## Prepare ClickBench Data

The ClickBench dataset is large. The full download is approximately 14 GB and
the sorted dataset requires additional disk space.

Prepare the full dataset and a 1M-row sample:

```bash
uv run --group bench python benchmarks/prepare_data.py
```

For a sample-only setup, skip the full sort:

```bash
uv run --group bench python benchmarks/prepare_data.py --sample-only
```

The preparation script creates these files when the required input data is
available:

- `benchmarks/data/hits.parquet`: downloaded source dataset
- `benchmarks/data/hits_sorted.parquet`: sorted by `userid`, `eventtime`, and `watchid`
- `benchmarks/data/hits_sample.parquet`: 1M-row validation dataset

Verify the physical ordering before running ordered workloads:

```bash
uv run --group bench python benchmarks/verify_parquet_order.py \
  benchmarks/data/hits_sorted.parquet userid eventtime watchid
```

## Core Operation Benchmark

Run the complete synthetic operation suite:

```bash
uv run python benchmarks/bench_core.py
```

This benchmark generates temporary CSV inputs and covers filtering,
derivation, joins, windows, grouping, sorting, search, mutation, and I/O at
10K, 100K, and 1M row scales. Each case performs one warmup and three timed
iterations and reports the mean.

Every case belongs to one group, and the group fixes what its timed region
contains:

| Group | Prepared before timing | Timed region | Names |
|---|---|---|---|
| `operator` | CSV parsed and held in memory (`collect()`); inputs of order-dependent operators sorted and declared with `assume_sorted()` | building the operator's plan and executing it | `filter_*`, `derive_*`, `join_*`, `window_*`, `group_*`, `chain_*`, `sort_*`, `search_first_*`, `mutation_*` |
| `io` | data generated; writes start from the in-memory table | `read_csv` / `read_parquet` decoding every column, or `write_parquet` | `read_*`, `write_parquet_*` |
| `end_to_end` | CSV file generated | CSV scan and parse plus the operator | `e2e_csv_*` |

An operator case and its `e2e_csv_*` twin run the same query, so their
difference is what reading the CSV adds to that query. The scan parses only
the columns the query uses, so it can cost less than `read_csv_*`.

The harness executes whatever a timed call returns with `collect()`, which
runs the whole plan and produces every output column. Two cheaper ways to
"finish" a call do not measure the operator:

- Returning a lazy table without executing it times plan construction only.
  `search_first`, for example, returns a lazy `filter(...).limit(1)` plan.
- `len()` runs `count(*)`, and DataFusion drops whatever cannot change the row
  count: derived columns and sorts are never computed, and a Parquet row count
  is read from file metadata without decoding any data.

The order-dependent cases (windows, `group_ordered`) run at 10K and 100K rows,
where `assume_sorted()` keeps the input sort out of their plans. At 1M rows
DataFusion sorts an in-memory table again even when its order is declared
(#212), so a 1M-row case would time that sort as well.

Results are printed per group and written to:

```text
benchmarks/results.json
```

Each result records its `group`. Results without a `group` field come from
the suite before #151 and are not comparable with current ones: operator
timings included CSV parsing, `len()` let the `derive`, `window`, `sort`, and
`read_*` cases skip some or all of the measured work, and `search_first` timed
plan construction only.

## ClickBench Comparison

Run all three ClickBench rounds against the full sorted dataset:

```bash
uv run --group bench python benchmarks/bench_vs.py
```

Run a quick smoke test using the 1M-row sample:

```bash
uv run --group bench python benchmarks/bench_vs.py --sample
```

Run one round only:

```bash
uv run --group bench python benchmarks/bench_vs.py --sample --round 2
```

Available rounds:

- Round 1: top URLs aggregation
- Round 2: user sessionization
- Round 3: sequential URL funnel matching

Useful options:

```bash
# Use a specific Parquet file
uv run --group bench python benchmarks/bench_vs.py --data path/to/data.parquet

# Change measurement counts
uv run --group bench python benchmarks/bench_vs.py --sample --warmup 1 --iterations 5
```

The default configuration uses one warmup and three timed iterations. Each
engine is measured with `time.perf_counter()`, and the median duration is
reported. RSS memory change is also recorded. LTSeq opens the Parquet file and
declares its known sort order before the timed query rounds; those setup times
are reported separately. Opening is lazy, so every timed LTSeq round reads and
decodes the columns it needs from the file, as every DuckDB query does.

Each round validates its result against DuckDB. The JSON report is written to:

```text
benchmarks/clickbench_results.json
```

The report includes timing samples, medians, memory deltas, validation status,
dataset path, host information, and the configured warmup/iteration counts.

### Fast paths behind Rounds 2 and 3

The Round 2 and Round 3 LTSeq timings come from dedicated Rust kernels that
LTSeq selects by the shape of the query. They read the sorted Parquet file
directly, in parallel, without going through DataFusion. A query that differs
from the benchmark's shape gets the same answer from a general path that
collects the needed columns into memory first. On a synthetic 5M-row sorted
file the general path was 13 to 24 times slower for both rounds; merely
writing the Round 2 predicate as `r.userid.shift(1) != r.userid` was enough to
leave the fast path. Read the Round 2 and Round 3 speedups as results for
these shapes, not for sequence queries in general. Round 1 has no fast path;
it is an ordinary DataFusion aggregation.

**Round 2, `group_ordered(cond).first().count()`.** The parallel kernel counts
group boundaries without building per-row group arrays. It runs only when all
of these hold:

1. The table comes straight from `LTSeq.read_parquet(...)` on a single
   Parquet file, followed by `assume_sorted(...)`. Any other source (CSV,
   `from_arrow`, `collect()`) or any transform in between, `sort()` included,
   drops the link to the file.
2. The groups are consumed as `first().count()` (or `len()` of `first()`).
   Any other use of `first()` materializes the grouped table through
   DataFusion window functions.
3. `cond` combines leaves with `|` and `&`, and every leaf is exactly
   `r.c != r.c.shift(1)`, where `c` is an Int32, Int64, UInt32, UInt64, or
   timestamp column, or `(r.c - r.c.shift(1)) > N`, where `c` is an Int64
   column and `N` is an integer literal, or a float literal that is finite,
   integral, and smaller than `2**53` in magnitude. The
   benchmark's `(r.userid != r.userid.shift(1)) | (r.eventtime - r.eventtime.shift(1) > 1800)`
   has this shape (both columns are Int64). Swapped operands
   (`r.c.shift(1) != r.c`), other comparisons (`>=`, `<`, `==`), `shift(n)`
   with `n != 1`, string columns, and `is_null()` all fall outside it.

When condition 1 or 3 fails, the count takes the general linear-scan path: it
collects the predicate's columns plus every sort key in the declared order
(sorting them unless the source already reports that order), evaluates the
predicate with intermediate arrays, builds three per-row arrays, and counts
through DataFusion. That path still requires a
predicate built from columns, literals, `shift(1)`, `is_null()`, comparisons,
arithmetic, `&`, `|`, and `~` that contains at least one `shift(1)`, and one
it computes the way DataFusion does on the columns' types: arithmetic only
where DataFusion computes it in Int64 (or Float64, for `-`), so not on Int32,
UInt32, UInt64, or timestamp columns, whose differences DataFusion computes in
32 bits, past the kernel's `i64` range, or as durations it does not compare
with a number. Anything else, including a float `N` that condition 3 rejects
on an integer column, materializes the grouped table and counts its first
rows; the linear-scan path declines it before collecting. The linear-scan count and the
DataFusion path currently disagree on NULLs (#189), so a directory passed to
`read_parquet`, which the kernel cannot read, does not take the linear-scan
path: its grouped table is materialized.

**Round 3, `search_pattern_count(*steps, partition_by=col)`.** The parallel
kernel matches each Parquet row group independently and stitches matches that
cross row-group boundaries. It runs only when all of these hold:

1. The table comes straight from `read_parquet(...)` on a single file +
   `assume_sorted(...)`, as in Round 2. A directory currently raises instead
   of falling back (#217).
2. `partition_by` is given, and the partition column is Int32, Int64, UInt32,
   or UInt64. A string partition column currently raises instead of falling
   back (#211).

Inside the kernel, steps that are all `r.c.s.starts_with("literal")` on one
string column scan the raw strings with a prefix loop. This depends on the
shape, not on the column name; Round 3's three prefixes on `url` are one
instance. Other step predicates are evaluated vectorized per row group, and a
predicate the kernel cannot evaluate sends the whole count to the general path.

Without condition 1 or a `partition_by`, or when the kernel declines, the
general path collects the referenced columns in the declared order into one
batch and evaluates step 1 over the whole batch. Later
`starts_with` steps on one column are checked with the prefix loop at the
rows where step 1 matched; any other steps are each evaluated over the whole
batch. Both paths evaluate step predicates with LTSeq's own evaluator rather
than DataFusion, and that evaluator has no implicit type coercion (#188).

## Benchmark-Gated Experiments

For baseline/candidate comparisons, use the benchmark autoresearch pilot:

```bash
uv run python benchmarks/autoresearch/pilot/scripts/benchmark_baseline.py \
  clickbench_funnel --sample

uv run python benchmarks/autoresearch/pilot/scripts/benchmark_candidate.py \
  clickbench_funnel --sample

uv run python benchmarks/autoresearch/pilot/scripts/benchmark_gate.py \
  clickbench_funnel
```

Use `--sample` only for smoke tests. Use the full sorted dataset for decisions
about performance changes. The pilot writes baseline, candidate, diff, and
keep/discard evaluation artifacts under
`benchmarks/autoresearch/pilot/reports/`.

See [BENCHMARK_AUTORESEARCH.md](BENCHMARK_AUTORESEARCH.md) for the supervised
autoloop, profiling, artifact retention, and review rules.

## Reproducible Runs

For comparable measurements:

1. Run on the same machine and keep CPU load low.
2. Use the same dataset, warmup count, and iteration count.
3. Rebuild with `maturin develop --release` after source or dependency changes.
4. Record the current Git commit and Rust/Python versions with the benchmark output.
5. Treat sample results as smoke-test evidence, not as full-dataset performance decisions.
