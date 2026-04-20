# Performance Target: `clickbench_funnel`

## Objective

Use the ClickBench benchmark to judge whether candidate changes improve LTSeq's sequential funnel performance without introducing correctness regressions.

## Why This Target

`R3: Funnel` is LTSeq's most sequence-native benchmark in the current ClickBench suite.
It exercises ordered pattern matching and is a better first benchmark autoresearch target than broad mixed workloads.

## Benchmark Subset

Target workload:

- `R3: Funnel`

Protected workloads:

- `R1: Top URLs`
- `R2: Sessionization`

## Success Criteria

- `correctness_failures = 0`
- `infra_failures = 0`
- `R3: Funnel` improves by at least 3% in LTSeq median or p95 latency
- protected workloads do not regress by 5% or more in LTSeq median or p95 latency

## In Scope

- `src/ops/pattern_match.rs`
- nearby ordered-search execution paths directly supporting funnel matching
- narrow Python/Rust glue changes required by the target optimization

## Out Of Scope

- benchmark methodology changes
- ClickBench workload definition changes
- broad engine-wide refactors without clear funnel evidence
- controller automation and git workflow changes
