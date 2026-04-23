# Benchmark Autoresearch

LTSeq includes a benchmark-gated autoresearch pilot under `benchmarks/autoresearch/pilot`.

## Setup

Install benchmark dependencies:

```bash
uv sync --group bench
```

Optional profiling support:

```bash
uv sync --group autoresearch
```

Build the Rust extension before collecting benchmark evidence:

```bash
maturin develop --release
```

## Data Preparation

Prepare the ClickBench parquet files under `benchmarks/data/`.

Common helpers:

- `python benchmarks/prepare_data.py`
- `python benchmarks/verify_parquet_order.py benchmarks/data/hits_sorted.parquet userid eventtime watchid`

## Pilot Targets

- `clickbench_funnel`
- `clickbench_sessionization`

## Workflow

Capture a baseline:

```bash
python benchmarks/autoresearch/pilot/scripts/benchmark_baseline.py clickbench_funnel
```

Capture a candidate after code changes:

```bash
python benchmarks/autoresearch/pilot/scripts/benchmark_candidate.py clickbench_funnel
```

Compare baseline and candidate:

```bash
python benchmarks/autoresearch/pilot/scripts/benchmark_gate.py clickbench_funnel
```

Use `--sample` for smoke tests only. Review baseline decisions on the full sorted dataset.

## Machine-readable Benchmark Summary

`benchmarks/bench_vs.py` writes `benchmarks/clickbench_results.json` for downstream gating.

Top-level fields used by the pilot:

- `passed`
- `correctness_failures`
- `infra_failures`
- `completed_rounds`
- `total_rounds`
- `rounds`

Each round also carries machine-readable status fields:

- `round_id`
- `round_name`
- `benchmark_status`
- `validation.status`
- `validation.detail`

Infrastructure failures are persisted in JSON with `benchmark_status=infra_failure` and `validation.status=infra_failure`, so the pilot evaluator does not need to parse stdout to decide whether a candidate should be kept or discarded.

## Supervised Controller

The pilot also includes a supervised controller that uses isolated git worktrees and one OpenCode candidate per iteration.

Start with a dry-run:

```bash
bash benchmarks/autoresearch/pilot/scripts/autoloop.sh \
  --target clickbench_funnel \
  --baseline \
  --iterations 1 \
  --sample \
  --dry-run
```

In non-dry-run mode, the controller performs preflight checks before running the loop.

Required checks:

- `opencode` in `PATH`
- `maturin` in `PATH` unless `--skip-build` is used
- Python benchmark modules `duckdb` and `psutil`
- benchmark data file exists at `benchmarks/data/hits_sorted.parquet`, `benchmarks/data/hits_sample.parquet`, or the `--data` override

If preflight fails, the controller exits early with actionable setup guidance instead of creating partial benchmark artifacts.

---

## End-to-End Acceptance Checklist

This checklist defines what a valid pilot run must produce. Use it to validate both dry-run
verification and non-dry-run controller executions.

### Dry-run verification

- [ ] `autoloop.sh --dry-run` prints commands without executing `opencode` or touching the worktree
- [ ] No benchmark artifacts are created under `reports/`
- [ ] No worktree directory is created under `.worktrees/`
- [ ] Preflight checks are skipped (no requirement for `opencode`, `maturin`, or data files)

### Non-dry-run validation

#### Preflight

- [ ] `opencode` is available in `PATH`
- [ ] `maturin` is available in `PATH` (unless `--skip-build` is used)
- [ ] Python modules `duckdb` and `psutil` are importable
- [ ] Benchmark dataset exists at the default path or the `--data` override

#### Baseline

- [ ] `reports/baseline/<target>/benchmark-summary.json` exists
- [ ] The summary contains a valid `data_file` field matching the requested dataset
- [ ] `reports/baseline/<target>/benchmark-result.md` exists (human-readable evidence)

#### Candidate and diff (for each iteration)

- [ ] `reports/candidates/<target>/benchmark-summary.json` exists
- [ ] `reports/candidates/<target>/benchmark-result.md` exists
- [ ] `reports/diff/<target>/benchmark-diff.json` exists
- [ ] `reports/diff/<target>/evaluation.json` exists with a `recommendation` field (`keep` or `discard`)

#### Run archive

- [ ] A new directory `reports/runs/<target>/run-NNN/` is created
- [ ] `run-NNN/decision.txt` exists (structured decision from OpenCode)
- [ ] `run-NNN/stdout.log` exists (full OpenCode stdout)
- [ ] `run-NNN/evaluation.txt` exists (controller evaluation output)
- [ ] `run-NNN/patch.diff` exists (git diff of candidate changes)
- [ ] For successful benchmarked runs: `run-NNN/candidate/` and `run-NNN/diff/` contain benchmark artifacts
- [ ] For discarded runs: decision, stdout, evaluation, and patch are still archived

#### Ledgers

- [ ] `results.tsv` has a header row and one data row per completed iteration
- [ ] Each `results.tsv` row contains: `base_ref`, `target`, `model_status`, `recommendation`, `hypothesis`, `target_win`, `protected_status`, `evidence`, `run_dir`, `patch_path`
- [ ] The `run_dir` and `patch_path` fields in `results.tsv` point to existing paths
- [ ] `issues.tsv` exists if any issues were recorded (may not exist if no issues)
- [ ] Each `issues.tsv` row contains: `id`, `category`, `target`, `file`, `title`, `evidence`, `suggested_fix`, `status`, `run_date`

#### Cleanup

- [ ] Worktree changes are discarded after each iteration (no uncommitted changes remain)
- [ ] Stale root candidate/diff artifacts are cleared on discard or gate failure paths

---

## Reviewer Rubric

This rubric defines how reviewers should judge archived autoresearch pilot runs.
Apply the same criteria to every run to ensure consistent evaluation.

### 1. Scope compliance

- [ ] The patch only modifies allowed source files (e.g., `src/ops/*.rs`, `py-ltseq/ltseq/*.py`)
- [ ] No changes to test files, benchmark scripts, or autoresearch infrastructure
- [ ] No changes to files outside the target's allowed scope (see `is_target_allowed_perf_file` in `autoloop.sh`)

### 2. Target improvement

- [ ] `evaluation.json` has `recommendation: keep`
- [ ] Target workload shows improvement in median or p95 (or both)
- [ ] The improvement magnitude is meaningful (not within noise)

### 3. Protected-workload safety

- [ ] No protected workload shows regression in median or p95
- [ ] If a protected workload shows any regression, it is documented and justified

### 4. Patch quality

- [ ] The diff is focused and minimal (no unrelated changes)
- [ ] The code change is understandable from the diff alone
- [ ] No obvious correctness issues (e.g., removing necessary logic, changing semantics unintentionally)

### 5. Artifact completeness

- [ ] All required run archive files are present (see acceptance checklist above)
- [ ] `decision.txt` contains a valid structured decision
- [ ] `patch.diff` is non-empty and applies cleanly to the base commit

### 6. Failure classification

When a run does **not** produce a `keep` recommendation, distinguish:

| Outcome | Meaning | Action |
|---------|---------|--------|
| `discard` (benchmark) | Candidate produced but did not pass gate | Archive for reference; no action needed |
| `discard` (scope) | Candidate modified disallowed files | Review prompt compliance |
| `discard` (infra-failure) | Benchmark or gate command failed | Investigate harness issue; may need rerun |
| No decision | OpenCode produced no usable decision artifact | Tighten prompt or fallback parsing |
| Harness error | Controller itself errored | Fix controller bug before rerunning |

---

## Standard Run Summary Format

Each archived run should be reviewed using a compact summary that points to the
relevant evidence without manually opening every artifact.

### Fields

| Field | Source | Description |
|-------|--------|-------------|
| `run_id` | Archive directory name | e.g., `run-001` |
| `base_ref` | `results.tsv` | Short git commit hash of the base |
| `target` | `results.tsv` | Benchmark target key |
| `recommendation` | `results.tsv` / `evaluation.json` | `keep` or `discard` |
| `hypothesis` | `results.tsv` | Scenario or hypothesis from decision |
| `target_win` | `evaluation.json` | Target workload improvement summary |
| `protected_status` | `evaluation.json` | Protected workload regression status |
| `evidence` | `evaluation.json` | Reason for the recommendation |
| `patch_path` | `results.tsv` | Path to `patch.diff` |
| `artifacts` | Archive directory | List of archived files |

### Markdown template

```markdown
## Run: <run_id>

| Field | Value |
|-------|-------|
| Base ref | `<base_ref>` |
| Target | `<target>` |
| Recommendation | `<keep\|discard>` |
| Hypothesis | `<hypothesis>` |
| Target win | `<target_win>` |
| Protected status | `<protected_status>` |
| Evidence | `<evidence>` |

### Artifacts

- Decision: `run-NNN/decision.txt`
- Evaluation: `run-NNN/evaluation.txt`
- Patch: `run-NNN/patch.diff`
- Candidate: `run-NNN/candidate/` (if present)
- Diff: `run-NNN/diff/` (if present)

### Reviewer Notes

<free-form notes from the reviewer>
```

---

## Artifact Retention and Ignore Policy

This section defines which autoresearch-generated artifacts belong in git history
and which should remain local scratch data.

### Committed artifacts

These artifacts are contractual, reviewable evidence and **should be committed**
when a run is validated:

- `results.tsv` — run ledger with one row per iteration
- `issues.tsv` — recorded harness, environment, and performance issues
- `reports/baseline/<target>/` — latest validated baseline for each target
- `reports/runs/<target>/run-NNN/` — archived run directories for reviewed runs

### Local-only artifacts

These artifacts are disposable local state and **should not be committed**:

- `.worktrees/` — git worktrees used by the controller; always local
- `reports/candidates/<target>/` — overwritten each iteration; only the archived copy in `reports/runs/` matters
- `reports/diff/<target>/` — overwritten each iteration; only the archived copy in `reports/runs/` matters
- `reports/logs/` — controller loop logs; useful for debugging but not reviewable evidence

### Gitignore rules

The following patterns are in `.gitignore` to prevent accidental commits of
large or disposable artifacts:

- `.worktrees/` — worktree directories
- `benchmarks/autoresearch/results/` — flamegraphs and run summaries from the older autoresearch runner
- `benchmarks/data/*.parquet` — large benchmark datasets

### Placeholder files

`.gitkeep` files in `reports/baseline/`, `reports/candidates/`, and `reports/diff/`
ensure the directory structure exists in git even when the directories are empty.
These placeholders should be kept.

### Cleanup expectations

- After a validated run, the `reports/candidates/` and `reports/diff/` directories
  may be cleared; the evidence lives in `reports/runs/`.
- Local worktrees under `.worktrees/` can be pruned with `git worktree prune`.
- Old `reports/runs/` directories may be removed if they are no longer needed for
  review, but `results.tsv` and `issues.tsv` should be preserved as the authoritative ledger.

---

## Benchmark Environment Metadata

Reproducibility requires capturing the environment behind each run.
The following metadata should be available for every archived run.

### Required metadata

| Field | Source | Example |
|-------|--------|---------|
| `base_ref` | `results.tsv` | `abc1234` |
| `target` | `results.tsv` | `clickbench_funnel` |
| `dataset` | `--data` flag or default path | `benchmarks/data/hits_sorted.parquet` |
| `dataset_label` | Derived from dataset name | `full dataset` |
| `model` | `--model` flag | `github-copilot/gpt-4.1` |
| `iterations` | `--iterations` flag | `3` |
| `warmup` | Target spec default | `1` |
| `build_mode` | `--skip-build` flag | `release` or `skipped` |
| `platform` | `uname -s` / `uname -m` | `Linux x86_64` |
| `cpu_count` | `nproc` | `32` |
| `rust_version` | `rustc --version` | `rustc 1.85.0` |
| `run_date` | Controller execution date | `2026-04-22` |

### Where to find it

- `results.tsv` captures `base_ref`, `target`, `recommendation`, and outcome fields
- The loop log (`benchmark-autoloop-<target>.log`) records dataset label, model, and iteration count
- The `benchmark-summary.json` files capture the `data_file` path
- System details (`platform`, `cpu_count`, `rust_version`) can be obtained from the machine that ran the benchmark

### Reproduction checklist

To reproduce a specific archived run:

1. Check out the `base_ref` commit
2. Install the same Rust and Python dependency versions
3. Use the same dataset (sample, full, or custom)
4. Run the baseline and candidate scripts with the same `--data` and `--skip-build` flags
5. Compare the resulting `benchmark-diff.json` against the archived evidence

---

## Target Onboarding Checklist

Before adding a new benchmark autoresearch target to the supervised controller,
verify all items below.

### Required inputs

- [ ] **Target key**: A unique identifier (e.g., `clickbench_funnel`)
- [ ] **Title**: Human-readable name (e.g., `ClickBench Funnel`)
- [ ] **Target brief**: A `targets/<key>_perf.md` file describing the optimization goal,
  target workloads, and protected workloads
- [ ] **Editable source files**: A list of source files the agent is allowed to modify
  (e.g., `("src/ops/pattern_match.rs",)`)
- [ ] **Target workloads**: The round(s) whose improvement is the goal
  (e.g., `("r3_funnel",)`)
- [ ] **Protected workloads**: The round(s) that must not regress
  (e.g., `("r1_top_urls", "r2_sessionization")`)

### Benchmark readiness

- [ ] The target's rounds are supported by `bench_vs.py`
- [ ] Baseline capture works: `benchmark_baseline.py <key> --sample --dry-run` succeeds
- [ ] Candidate capture works: `benchmark_candidate.py <key> --sample --dry-run` succeeds
- [ ] Gate evaluation works: `benchmark_gate.py <key>` produces `benchmark-diff.json` and `evaluation.json`
- [ ] The target is registered in `TARGETS` dict in `common.py`
- [ ] The target is registered in `is_target_allowed_perf_file()` in `autoloop.sh`

### Scope validation

- [ ] `is_target_allowed_perf_file()` returns `0` only for the target's editable source files
- [ ] Test files, benchmark scripts, and autoresearch infrastructure are excluded from allowed edits
- [ ] The target brief explicitly states what the agent should and should not optimize

### Review readiness

- [ ] Threshold values are defined (default: target improvement `-3.0%`, protected regression `+5.0%`)
- [ ] The target has at least one validated dry-run through the full autoloop
- [ ] A human reviewer can interpret the target's `benchmark-diff.json` output

### Wiring into the controller

- [ ] Add the target to `TARGETS` in `benchmarks/autoresearch/pilot/common.py`
- [ ] Add the target to `is_target_allowed_perf_file()` in `autoloop.sh`
- [ ] Add the target brief to `targets/<key>_perf.md`
- [ ] Add `.gitkeep` placeholders under `reports/baseline/<key>/`, `reports/candidates/<key>/`, `reports/diff/<key>/`

---

## Graduation Criteria: From Supervised Pilot to Broader Automation

The current pilot is explicitly supervised and review-first.
Before transitioning to broader automation (e.g., auto-commit, auto-merge),
the following criteria must be met.

### Run reliability

- [ ] The controller runs without harness errors for at least N consecutive iterations
  across all registered targets
- [ ] Decision artifact parsing recovers from malformed or missing decision files
  without losing diagnostic information
- [ ] Preflight checks catch all common setup failures before creating artifacts

### Artifact quality

- [ ] Every archived run contains all required artifacts (see acceptance checklist)
- [ ] `results.tsv` and `issues.tsv` pass integrity checks (no broken references,
  no empty required fields)
- [ ] Archived patches apply cleanly to the base commit

### Reviewer trust

- [ ] Multiple reviewers have applied the reviewer rubric consistently to the same runs
- [ ] The false-positive rate (discard recommendations that would have been valid) is acceptably low
- [ ] The false-negative rate (keep recommendations that introduced regressions) is zero

### Benchmark confidence

- [ ] Threshold values are calibrated against observed run data for each target
- [ ] Sample-versus-full dataset variance is characterized and documented
- [ ] Median and p95 gate behavior is reviewed against observed pilot runs

### Automation readiness

- [ ] A clear rollback path exists for auto-committed changes
- [ ] Protected-workload regressions are caught before any auto-merge
- [ ] The project has explicit criteria for when to revert to supervised mode
  (e.g., after a regression slips through)

---

## Readiness Criteria: Widening Editable Scope

The current controller restricts candidate edits to a narrow set of target files.
Before widening the editable scope, the following readiness gates must be met.

### Evidence gates

- [ ] At least N successful `keep` runs have been archived across all current targets
- [ ] No protected-workload regression has been introduced by a `keep` candidate
- [ ] The reviewer rubric has been applied consistently to at least N runs

### Review capacity

- [ ] The project has enough reviewers to handle the increased review load
- [ ] Review turnaround time is acceptable (e.g., within 1-2 days)
- [ ] The false-positive and false-negative rates are characterized

### Risk assessment

- [ ] The expanded file list has been reviewed for blast radius
  (e.g., adding `src/ops/derive.rs` affects more code paths than `src/ops/pattern_match.rs`)
- [ ] Protected workloads cover the expanded scope
- [ ] The controller's scope validation (`validate_candidate_scope`) correctly rejects
  files outside the expanded allowed set

### Gradual expansion

Scope expansion should be incremental:

1. Add one file or file pattern at a time
2. Run at least N iterations with the expanded scope
3. Review all resulting runs before further expansion
4. Document the rationale for each expansion in the target brief

---

## Run Archive Completeness Validation

This section documents the validation of run archive completeness across
keep and discard paths.

### Validated paths

The following archive paths have been validated against the acceptance checklist:

#### Discard paths

| Path | Artifact | Source |
|------|----------|--------|
| `run-NNN/decision.txt` | Structured decision | `archive_run_artifacts()` line 888 |
| `run-NNN/stdout.log` | Full OpenCode stdout | `archive_run_artifacts()` line 889 |
| `run-NNN/evaluation.txt` | Controller evaluation | `archive_run_artifacts()` line 890 |
| `run-NNN/patch.diff` | Git diff of changes | `archive_run_artifacts()` lines 891-898 |

All discard paths (status != keep, scope failure, benchmark failure, gate failure)
archive these four artifacts before calling `discard_candidate_state()`.

#### Successful benchmarked paths

In addition to the four discard artifacts, successful runs also archive:

| Path | Artifact | Source |
|------|----------|--------|
| `run-NNN/candidate/` | Candidate benchmark outputs | `archive_run_artifacts()` lines 900-903 |
| `run-NNN/diff/` | Diff and evaluation artifacts | `archive_run_artifacts()` lines 904-907 |

These are copied from the worktree report root only when artifacts exist
(`phase_dir_has_artifacts` check).

### Validation status

**All acceptance criteria are met:**

- [x] Discard runs archive `decision.txt`, `stdout.log`, `evaluation.txt`, and `patch.diff`
- [x] Successful benchmarked runs also archive candidate and diff artifacts
- [x] Every archived run directory is self-contained enough for later review
- [x] Missing archive content would be a bug (the controller always creates these files)

### Related PRs

- PR #63: Stabilized controller contracts, added archive test coverage
- PR #67: Added archive artifact assertions and ledger preservation
- PR #85: Relaxed archive artifact assertions to verify directories rather than hard-coded paths
