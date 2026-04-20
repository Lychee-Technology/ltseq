# LTSeq Benchmark Autoresearch Pilot

This directory holds the first benchmark-gated autoresearch pilot for LTSeq.

The pilot reuses `benchmarks/autoresearch/runner.py` and `benchmarks/bench_vs.py`, then adds a stable baseline/candidate/diff workflow for reviewable performance experiments.

## Scope

- benchmark-driven performance evidence only
- no auto-commit or auto-merge
- one target at a time
- manual review remains the final decision step

## Current Targets

- `clickbench_funnel`
- target workload: `R3: Funnel`
- protected workloads: `R1: Top URLs`, `R2: Sessionization`
- `clickbench_sessionization`
- target workload: `R2: Sessionization`
- protected workloads: `R1: Top URLs`, `R3: Funnel`

## Layout

- `common.py`: shared target config and artifact helpers
- `program-perf.md`: benchmark-specific working rules
- `targets/`: target briefs
- `scripts/benchmark_baseline.py`: capture baseline artifacts
- `scripts/benchmark_candidate.py`: capture candidate artifacts
- `scripts/benchmark_gate.py`: compare baseline and candidate artifacts
- `scripts/evaluate_benchmark_candidate.py`: keep/discard evaluator
- `reports/`: generated artifacts

## Recommended Flow

1. Capture a baseline:

```bash
python benchmarks/autoresearch/pilot/scripts/benchmark_baseline.py clickbench_funnel
python benchmarks/autoresearch/pilot/scripts/benchmark_baseline.py clickbench_sessionization
```

2. Apply a candidate performance change.

3. Capture candidate evidence:

```bash
python benchmarks/autoresearch/pilot/scripts/benchmark_candidate.py clickbench_funnel
python benchmarks/autoresearch/pilot/scripts/benchmark_candidate.py clickbench_sessionization
```

4. Compare baseline and candidate:

```bash
python benchmarks/autoresearch/pilot/scripts/benchmark_gate.py clickbench_funnel
python benchmarks/autoresearch/pilot/scripts/benchmark_gate.py clickbench_sessionization
```

## Notes

- use `--sample` for smoke tests only; baseline decisions should use the full sorted ClickBench dataset
- baseline/candidate scripts rebuild the extension with `maturin develop --release` by default
- use `--skip-build` only when you know the native extension is already up to date
- benchmark dependencies currently include `duckdb` and `psutil`; optional profiling uses `py-spy` or `perf`
