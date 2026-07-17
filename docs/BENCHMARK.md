# Benchmark Guide

This document describes how to run LTSeq's local operation benchmarks and the
ClickBench comparison benchmark.

## Prerequisites

The ClickBench comparison and data-preparation scripts import `duckdb` + `psutil`,
which live in the optional `bench` dependency group. Activate the group on **each**
run with `uv run --group bench` so those packages are synced into the venv. Do
**not** run `uv sync --group bench` once and then use a plain `uv run` — the plain
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

This benchmark generates temporary CSV/Parquet inputs and covers filtering,
derivation, joins, windows, grouping, sorting, mutation, and I/O at 10K, 100K,
and 1M row scales. Each case performs one warmup and three timed iterations.

Results are printed as a table and written to:

```text
benchmarks/results.json
```

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
reported. RSS memory change is also recorded. LTSeq loads the input and declares
the known sort order before the timed query rounds; those setup times are
reported separately.

Each round validates its result against DuckDB. The JSON report is written to:

```text
benchmarks/clickbench_results.json
```

The report includes timing samples, medians, memory deltas, validation status,
dataset path, host information, and the configured warmup/iteration counts.

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
