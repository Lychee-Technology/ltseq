# Performance Target: `clickbench_top_urls`

## Objective

Use the ClickBench benchmark to judge whether candidate changes improve LTSeq's top-URLs aggregation performance without introducing correctness regressions.

## Why This Target

`R1: Top URLs` is LTSeq's current aggregation-heavy benchmark hotspot. It exercises grouped counting followed by ordered top-k selection, making it a focused target for improving lazy aggregation and downstream sort/limit behavior.

## Benchmark Subset

Target workload:

- `R1: Top URLs`

Protected workloads:

- `R2: Sessionization`
- `R3: Funnel`

## Success Criteria

- `correctness_failures = 0`
- `infra_failures = 0`
- `R1: Top URLs` improves by at least 3% in LTSeq median or p95 latency (either metric suffices)
- protected workloads do not regress by 5% or more in LTSeq median or p95 latency (either metric triggers regression)

## In Scope

- `src/ops/aggregation.rs`
- nearby aggregation and lazy-plan execution paths that directly affect grouped top-k workloads
- narrow Python/Rust glue changes required by the target optimization

## Out Of Scope

- benchmark methodology changes
- ClickBench workload definition changes
- broad engine-wide refactors without clear top-URLs evidence
- controller automation and git workflow changes
