# Performance Target: `clickbench_sessionization`

## Objective

Use the ClickBench benchmark to judge whether candidate changes improve LTSeq's sessionization performance without introducing correctness regressions.

## Why This Target

`R2: Sessionization` is a core ordered-computing workload for LTSeq. It exercises sorted, per-user state transitions and provides a narrow target for improving shift, partition, and grouped sequence logic.

## Benchmark Subset

Target workload:

- `R2: Sessionization`

Protected workloads:

- `R1: Top URLs`
- `R3: Funnel`

## Success Criteria

- `correctness_failures = 0`
- `infra_failures = 0`
- `R2: Sessionization` improves by at least 3% in LTSeq median or p95 latency (either metric suffices)
- protected workloads do not regress by 5% or more in LTSeq median or p95 latency (either metric triggers regression)

## In Scope

- `src/ops/window.rs`
- `src/ops/derive.rs`
- nearby sorted-window execution paths that directly affect session boundary detection
- narrow Python/Rust glue changes required by the target optimization

## Out Of Scope

- benchmark methodology changes
- ClickBench workload definition changes
- broad engine-wide refactors without clear sessionization evidence
- controller automation and git workflow changes
