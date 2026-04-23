# LTSeq Benchmark Autoresearch Rules

## Goal

Improve one benchmark target at a time using benchmark-backed evidence.

## Working Style

- prefer the smallest correct performance change
- keep each patch attributable to one target hypothesis
- preserve correctness first
- treat protected workloads as regression guards
- prefer production changes that materially reduce target workload cost over broad refactors

## Keep Criteria

- `correctness_failures = 0`
- `infra_failures = 0`
- at least one target workload improves by 3% or more in LTSeq median **or** p95 latency (either metric suffices)
- no protected workload regresses by 5% or more in LTSeq median **or** p95 latency (either metric triggers regression)

## Discard Criteria

- any correctness regression
- any infrastructure failure
- no target workload meets the improvement threshold in either median or p95
- any protected workload regresses by 5%+ in median or p95
- broad or hard-to-explain code churn

## Threshold Rationale

- **Target improvement (≥3%, either metric)**: Observed same-SHA variance on the full ClickBench dataset is 0.8–1.6% for R1/R3 workloads. A 3% bar sits above typical noise for those workloads, so genuine improvements should pass reliably while noise-driven "improvements" are unlikely to qualify.
- **Protected regression (≥5%, either metric)**: The 5% bar is well above observed noise for R1 (≈0.8%) and R3 (≈1.6%), catching real regressions while tolerating measurement jitter. R2 (≈9% variance) is always a protected workload in existing targets, so a conservative regression bar avoids false rejections driven by R2 noise alone.
- Both targets share one policy because thresholds are evaluated per-workload and the chosen values accommodate the widest observed variance range. Per-target overrides are supported via `TargetSpec` constructor arguments.
- Re-calibrate once `clickbench_funnel` and `clickbench_sessionization` baseline data is available.
