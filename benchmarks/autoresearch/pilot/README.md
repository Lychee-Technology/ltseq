# Baseline / candidate benchmark comparison

Capture a baseline, apply a change, capture a candidate, and get a keep or
discard decision from the ClickBench rounds. The workflow, the evaluation
contract, the thresholds' limits and how to calibrate them are in
[`docs/BENCHMARK_AUTORESEARCH.md`](../../../docs/BENCHMARK_AUTORESEARCH.md).

```bash
python benchmarks/autoresearch/pilot/scripts/benchmark_baseline.py clickbench_funnel
# apply the change
python benchmarks/autoresearch/pilot/scripts/benchmark_candidate.py clickbench_funnel
python benchmarks/autoresearch/pilot/scripts/benchmark_gate.py clickbench_funnel
echo $?   # 0 keep, 1 discard, 2 inputs cannot be evaluated
```

- `common.py`: targets (`clickbench_funnel`, `clickbench_sessionization`,
  `clickbench_top_urls`), their thresholds, and the capture, summary and diff
  code.
- `scripts/benchmark_baseline.py`, `scripts/benchmark_candidate.py`: capture.
- `scripts/benchmark_gate.py`: compare a target's captures and write
  `reports/diff/<target>/benchmark-diff.json` and `evaluation.json`.
- `scripts/evaluate_benchmark_candidate.py`: evaluate any two
  `benchmark-summary.json` files.
- `reports/`: local output, gitignored; created on the first run.

The capture scripts run `maturin develop --release` first unless given
`--skip-build`. Use `--sample` only as a smoke test.
