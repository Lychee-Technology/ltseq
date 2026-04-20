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

## Machine-readable Benchmark Summary

`benchmarks/bench_vs.py` writes `benchmarks/clickbench_results.json` for downstream gating.

Top-level fields used by the pilot:

- `passed`
- `correctness_failures`
- `infra_failures`
- `completed_rounds`
- `total_rounds`
- `rounds`

Each round also carries machine-readable status fields:

- `round_id`
- `round_name`
- `benchmark_status`
- `validation.status`
- `validation.detail`

Infrastructure failures are persisted in JSON with `benchmark_status=infra_failure` and `validation.status=infra_failure`, so the pilot evaluator does not need to parse stdout to decide whether a candidate should be kept or discarded.

## Supervised Controller

The pilot also includes a supervised controller that uses isolated git worktrees and one OpenCode candidate per iteration.

Start with a dry-run:

```bash
bash benchmarks/autoresearch/pilot/scripts/autoloop.sh \
  --target clickbench_funnel \
  --baseline \
  --iterations 1 \
  --sample \
  --dry-run
```

In non-dry-run mode, the controller performs preflight checks before running the loop.

Required checks:

- `opencode` in `PATH`
- `maturin` in `PATH` unless `--skip-build` is used
- Python benchmark modules `duckdb` and `psutil`
- benchmark data file exists at `benchmarks/data/hits_sorted.parquet`, `benchmarks/data/hits_sample.parquet`, or the `--data` override

If preflight fails, the controller exits early with actionable setup guidance instead of creating partial benchmark artifacts.
