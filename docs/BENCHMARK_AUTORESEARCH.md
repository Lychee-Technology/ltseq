# Benchmark Autoresearch

LTSeq includes a benchmark-gated autoresearch pilot under `benchmarks/autoresearch/pilot`.

## Setup

Install benchmark dependencies:

```bash
uv sync --group bench
```

Optional profiling support:

```bash
uv sync --group autoresearch
```

Build the Rust extension before collecting benchmark evidence:

```bash
maturin develop --release
```

## Data Preparation

Prepare the ClickBench parquet files under `benchmarks/data/`.

Common helpers:

- `python benchmarks/prepare_data.py`
- `python benchmarks/verify_parquet_order.py benchmarks/data/hits_sorted.parquet userid eventtime watchid`

## Pilot Targets

- `clickbench_funnel`
- `clickbench_sessionization`

## Workflow

Capture a baseline:

```bash
python benchmarks/autoresearch/pilot/scripts/benchmark_baseline.py clickbench_funnel
```

Capture a candidate after code changes:

```bash
python benchmarks/autoresearch/pilot/scripts/benchmark_candidate.py clickbench_funnel
```

Compare baseline and candidate:

```bash
python benchmarks/autoresearch/pilot/scripts/benchmark_gate.py clickbench_funnel
```

Use `--sample` for smoke tests only. Review baseline decisions on the full sorted dataset.
