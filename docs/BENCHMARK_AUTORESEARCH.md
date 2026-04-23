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
