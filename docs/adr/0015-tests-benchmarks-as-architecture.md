# ADR 0015: Tests and Benchmarks Are an Architectural Layer

- Status: Accepted
- Decision date: predates this record · Recorded: 2026-07-26 · Amended: 2026-11-11 (supervised autoresearch controller retired; the gate keeps its JSON contract and gains exit statuses and input checks, #174)

[中文版](0015-tests-benchmarks-as-architecture.cn.md)

## Context

The hardest recurring questions in this codebase are empirical: where does DataFusion suffice, where does materialization dominate, when is specialized Rust worthwhile, and did an architectural change help real workloads ([ADR 0006](0006-multi-path-execution-strategy.md)). Answering them requires treating validation and performance measurement as part of the system, "not just project hygiene."

## Decision

Model the system with a fourth layer alongside API, orchestration, and execution: **validation & performance** (`py-ltseq/tests/`, `benchmarks/`, the baseline/candidate evaluation workflow).

### Test organization

- Tests are organized **by product capability/behavior, not by source file**, and that is an architectural choice. The suite doubles as a map of product capabilities and a guardrail for refactors; `MODULE_GUIDE.md` promotes it as the primary navigation tool ("find the most specific relevant test first, then trace into implementation").
- Architectural invariants get their own tests, e.g. `test_no_materialization_rule.py` ([ADR 0005](0005-no-materialization-rule.md)).
- Five contributor heuristics are codified as binding constraints: preserve lazy execution unless the API is explicitly terminal; don't break sort-metadata propagation casually; no Python-side materialization inside table-returning APIs; keep Python `_schema` and Rust schema aligned (post-#93 this reads: don't bypass the Rust-owned metadata, [ADR 0009](0009-metadata-single-source-of-truth.md)); prefer small local changes over cross-cutting refactors.

### Benchmark protocol

- Shared principles: warmup before timed iterations (default 1 + 3); LTSeq's data load and `assume_sorted` declaration happen **before** timed rounds and are reported separately; results go to machine-readable JSON; reproducibility rules apply (same machine/low load, rebuild `maturin develop --release`, "treat sample results as smoke-test evidence, not as full-dataset performance decisions").
- There are currently **two protocols**, not one: the **ClickBench comparison** (`bench_vs.py`) reports the **median**, records RSS memory delta, and **validates every round against DuckDB**; the **core suite** (`bench_core.py`) reports the **mean** over iterations and records no per-sample data, RSS, or validation. Commit and toolchain versions are captured in the core suite's host info but not in the ClickBench system info. Operational parameters (iteration counts, thresholds, result schemas) are specified in `BENCHMARK.md`, which is authoritative for them.
- Workloads exercise the sequence thesis: top-URL aggregation, user sessionization, sequential URL funnel matching, plus the core suite over 10K/100K/1M rows.

### Benchmark-gated evaluation

A performance change is judged by a baseline → candidate → gate comparison on the ClickBench rounds (`benchmarks/autoresearch/pilot/`). The gate decides on machine-readable JSON (`benchmark-diff.json`, `evaluation.json`, `keep`/`discard` with a reason code; infrastructure failures are persisted as `infra_failure` rather than parsed from stdout) and its exit status says the same: 0 keep, 1 discard, 2 when the inputs cannot be evaluated. It refuses comparisons it cannot trust: a different dataset, iteration count or warmup, an invalid baseline, a missing workload or metric. A target workload must improve by the target's bar in median and p95 (default 3%); a protected workload must not regress by its bar in median or p95 (default 5%). The defaults are not statistically validated, and `docs/BENCHMARK_AUTORESEARCH.md` gives the A/A calibration a target needs before its decisions are relied on.

Who proposes the change is outside the system: a person or a general-purpose coding agent. The project first ran its own supervised LLM-agent loop (OpenCode in an isolated git worktree, one candidate per iteration, a narrow editable scope, run ledgers, and graduation criteria before any auto-commit). That controller was retired on 2026-11-11: it required Bash 4.3+, which kept its tests from passing on macOS (#174, a prerequisite of macOS CI, #329), and maintaining an agent-orchestration layer is not this project's work. The gate, capture and profiling tools it called are what this decision keeps.

## Consequences

- Performance investigation becomes "a repeatable engineering practice" rather than ad-hoc tuning.
- Capability-organized tests survive refactors that reorganize source files.
- A keep/discard decision is only as good as the thresholds behind it; until a target is calibrated, a reviewer reads the deltas, not just the recommendation.

## Sources

- `docs/ARCHITECTURE.md`: Layered Architecture §4, Testing Strategy, Benchmarks and Performance Research
- `docs/DESIGN_SUMMARY.md`: §6.1, §6.2
- `docs/MODULE_GUIDE.md`: Test Suite as a Navigation Tool, Contributor Heuristics
- `docs/BENCHMARK.md`, `docs/BENCHMARK_AUTORESEARCH.md`
