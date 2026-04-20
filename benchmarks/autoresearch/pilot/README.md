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
- `common.sh`: shared controller helpers for shell entrypoints
- `program-perf.md`: benchmark-specific working rules
- `prompts/`: OpenCode prompt templates for supervised runs
- `targets/`: target briefs
- `scripts/benchmark_baseline.py`: capture baseline artifacts
- `scripts/benchmark_candidate.py`: capture candidate artifacts
- `scripts/benchmark_gate.py`: compare baseline and candidate artifacts
- `scripts/evaluate_benchmark_candidate.py`: keep/discard evaluator
- `scripts/opencode_autoresearch.sh`: single-candidate OpenCode launcher
- `scripts/autoloop.sh`: supervised benchmark autoresearch controller
- `results.tsv`: controller-written run ledger
- `issues.tsv`: controller-written issue ledger
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

For each target, the gate writes machine-readable evidence under `reports/diff/<target>/`:

- `benchmark-diff.json`: per-workload LTSeq median/p95 deltas for the target workload plus protected workloads
- `evaluation.json`: keep/discard recommendation, target-win details, protected-regression details, and threshold metadata

Examples:

- `reports/diff/clickbench_funnel/`
- `reports/diff/clickbench_sessionization/`

## Notes

- use `--sample` for smoke tests only; baseline decisions should use the full sorted ClickBench dataset
- baseline/candidate scripts rebuild the extension with `maturin develop --release` by default
- use `--skip-build` only when you know the native extension is already up to date
- benchmark dependencies currently include `duckdb` and `psutil`; optional profiling uses `py-spy` or `perf`

## Supervised Autoloop

The pilot now includes a supervised OpenCode-driven loop for one candidate per iteration.

Dry-run first:

```bash
./benchmarks/autoresearch/pilot/scripts/autoloop.sh \
  --target clickbench_funnel \
  --baseline \
  --iterations 2 \
  --sample \
  --dry-run
```

Controller behavior:

- creates a dedicated research worktree and branch
- asks OpenCode for exactly one benchmark candidate per iteration
- runs candidate benchmark capture and gate evaluation after each candidate
- archives the patch, benchmark artifacts, and recommendation under `reports/runs/`
- records outcomes in `results.tsv`
- records harness, environment, and perf-opportunity issues in `issues.tsv`
- discards worktree changes after archiving so the loop stays supervised and reviewable

The controller does not auto-commit or auto-merge candidate changes.

Non-dry-run controller runs also perform preflight checks before creating benchmark evidence:

- `opencode` must be available in `PATH`
- `maturin` must be available unless `--skip-build` is used
- Python benchmark dependencies must include `duckdb` and `psutil`
- the benchmark dataset must exist at the default path or the `--data` override
