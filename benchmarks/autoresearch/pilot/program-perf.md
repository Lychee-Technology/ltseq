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
- at least one target workload improves by 3% or more in LTSeq median or p95 latency
- protected workloads do not regress by 5% or more in LTSeq median or p95 latency

## Discard Criteria

- any correctness regression
- any infrastructure failure
- no meaningful target workload improvement
- clear protected workload regression
- broad or hard-to-explain code churn
