# ADR 0015: Tests and Benchmarks Are an Architectural Layer

- Status: Accepted
- Decision date: predates this record · Recorded: 2026-07-26

[中文版](0015-tests-benchmarks-as-architecture.cn.md)

## Context

The hardest recurring questions in this codebase are empirical: where does DataFusion suffice, where does materialization dominate, when is specialized Rust worthwhile, and did an architectural change help real workloads ([ADR 0006](0006-multi-path-execution-strategy.md)). Answering them requires treating validation and performance measurement as part of the system, "not just project hygiene."

## Decision

Model the system with a fourth layer alongside API, orchestration, and execution: **validation & performance** (`py-ltseq/tests/`, `benchmarks/`, the autoresearch workflow).

### Test organization

- Tests are organized **by product capability/behavior, not by source file**, and that is an architectural choice. The suite doubles as a map of product capabilities and a guardrail for refactors; `MODULE_GUIDE.md` promotes it as the primary navigation tool ("find the most specific relevant test first, then trace into implementation").
- Architectural invariants get their own tests, e.g. `test_no_materialization_rule.py` ([ADR 0005](0005-no-materialization-rule.md)).
- Five contributor heuristics are codified as binding constraints: preserve lazy execution unless the API is explicitly terminal; don't break sort-metadata propagation casually; no Python-side materialization inside table-returning APIs; keep Python `_schema` and Rust schema aligned (post-#93 this reads: don't bypass the Rust-owned metadata, [ADR 0009](0009-metadata-single-source-of-truth.md)); prefer small local changes over cross-cutting refactors.

### Benchmark protocol

- Shared principles: warmup before timed iterations (default 1 + 3); LTSeq's data load and `assume_sorted` declaration happen **before** timed rounds and are reported separately; results go to machine-readable JSON; reproducibility rules apply (same machine/low load, rebuild `maturin develop --release`, "treat sample results as smoke-test evidence, not as full-dataset performance decisions").
- There are currently **two protocols**, not one: the **ClickBench comparison** (`bench_vs.py`) reports the **median**, records RSS memory delta, and **validates every round against DuckDB**; the **core suite** (`bench_core.py`) reports the **mean** over iterations and records no per-sample data, RSS, or validation. Commit and toolchain versions are captured in the core suite's host info but not in the ClickBench system info. Operational parameters (iteration counts, thresholds, result schemas) are specified in `BENCHMARK.md`, which is authoritative for them.
- Workloads exercise the sequence thesis: top-URL aggregation, user sessionization, sequential URL funnel matching, plus the core suite over 10K/100K/1M rows.

### Benchmark-gated autoresearch (supervised)

An LLM-agent experiment loop (baseline → candidate → gate) with guardrails so it cannot game or destabilize the repo: gate on machine-readable JSON (`benchmark-diff.json`, `evaluation.json`, `keep`/`discard`; infra failures persisted as `infra_failure` rather than parsed from stdout); one isolated git worktree and one candidate per iteration, discarded afterward; preflight validation before any artifacts; a **narrow editable scope** (only a target's allowed source files; tests, benchmark scripts, and infra are off-limits); thresholds (target improvement -3.0%, protected-workload regression tolerance +5.0%); a defined artifact-retention policy; and an explicitly **supervised, review-first** stance with written graduation criteria (including a required zero false-negative rate for `keep` recommendations) before any auto-commit/auto-merge, plus incremental scope-widening rules.

## Consequences

- Performance investigation becomes "a repeatable engineering practice" rather than ad-hoc tuning.
- Capability-organized tests survive refactors that reorganize source files.
- The autoresearch guardrails trade automation speed for trust; widening them is gated on explicit criteria.

## Sources

- `docs/ARCHITECTURE.md`: Layered Architecture §4, Testing Strategy, Benchmarks and Performance Research
- `docs/DESIGN_SUMMARY.md`: §6.1, §6.2
- `docs/MODULE_GUIDE.md`: Test Suite as a Navigation Tool, Contributor Heuristics
- `docs/BENCHMARK.md`, `docs/BENCHMARK_AUTORESEARCH.md`
