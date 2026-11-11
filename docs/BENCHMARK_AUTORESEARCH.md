# Benchmark Comparison and Evaluation

`benchmarks/autoresearch/` holds the tools for judging a performance change:
run a baseline, apply the change, run the candidate, and compare the two with a
keep/discard decision that a reviewer can check. Anyone can propose the change,
a person or a general-purpose coding agent; the tools neither write code nor
manage git.

Until November 2026 this directory also held a supervised controller,
`pilot/scripts/autoloop.sh`, that ran OpenCode in a dedicated worktree to
produce one candidate per iteration, archived each run, and kept two ledgers
(`results.tsv`, `issues.tsv`). It was retired rather than ported: it needed
Bash 4.3+, which is why its tests failed on macOS (#174), and maintaining an
agent-orchestration layer is not this project's work. The capture, comparison
and evaluation scripts it called are what remain. Its documentation is in git
history before the retirement (`65c5924`).

## Tools

| Path | Does |
|---|---|
| `runner.py` | Runs `bench_vs.py` (ClickBench rounds, LTSeq against DuckDB, each round validated against DuckDB) and optionally `bench_core.py`; with `--profile`, writes flamegraphs and a hotspot report. Run directories go to `results/` (gitignored) or `--output-dir`. |
| `profiler.py`, `analyzer.py`, `hypothesis.py`, `report.py`, `tracker.py` | Profiling with py-spy or perf + inferno, hotspot extraction from the SVGs, a static list of known optimization patterns matched against hotspots, the Markdown report, and run history. Used by `runner.py`. |
| `pilot/common.py` | Target definitions (`TARGETS`), the shared capture step, summary and diff builders. |
| `pilot/scripts/benchmark_baseline.py`, `benchmark_candidate.py` | Build the release extension, run the target's rounds through `runner.py`, and write `benchmark-summary.json` plus a Markdown table. |
| `pilot/scripts/benchmark_gate.py` | Compares a target's baseline and candidate, writes the diff and the evaluation, and exits with the decision. |
| `pilot/scripts/evaluate_benchmark_candidate.py` | The same evaluation on any two summary files, printed as JSON; the gate calls it. |

Targets (`pilot/common.py`):

| Target | Target workload | Protected workloads |
|---|---|---|
| `clickbench_funnel` | R3: Funnel | R1: Top URLs, R2: Sessionization |
| `clickbench_sessionization` | R2: Sessionization | R1: Top URLs, R3: Funnel |
| `clickbench_top_urls` | R1: Top URLs | R2: Sessionization, R3: Funnel |

The scripts are plain Python and run on Linux and macOS with the `bench`
dependency group. Profiling needs py-spy (the `autoresearch` group) or Linux
perf with inferno.

## Workflow

Run the scripts with `uv run --group bench` (`--group autoresearch` for
profiling) so `duckdb` and `psutil` are in the environment, as
`benchmarks/README.md` explains. The ClickBench data goes under
`benchmarks/data/`, from `uv run --group bench python benchmarks/prepare_data.py`. Decisions need the full sorted dataset;
`--sample` is a smoke test only, and the sample currently has order breaks
that make Round 2 wrong (#336).

The capture scripts rebuild the extension with `maturin develop --release`
into the active environment, so run all three steps from one checkout:

```bash
# 1. Baseline, on the commit the change is based on
uv run --group bench python benchmarks/autoresearch/pilot/scripts/benchmark_baseline.py clickbench_funnel

# 2. Apply the candidate change (edit, or check out the branch)

# 3. Candidate
uv run --group bench python benchmarks/autoresearch/pilot/scripts/benchmark_candidate.py clickbench_funnel

# 4. Compare: exit 0 keep, 1 discard, 2 inputs cannot be evaluated
uv run python benchmarks/autoresearch/pilot/scripts/benchmark_gate.py clickbench_funnel
```

5. Review: put `reports/diff/<target>/evaluation.json` (or `evaluation.txt`
   and `benchmark-diff.txt`) in the PR description, with the dataset and
   machine. The reports directory is local output and gitignored.

Options shared by the capture scripts: `--sample`, `--data PATH`,
`--iterations N`, `--warmup N` (baseline and candidate must use the same),
`--skip-build` when the installed extension is already the code being
measured, and `--dry-run` to print the commands without running or deleting
anything.

To compare summaries captured elsewhere, for example in two checkouts, pass
the files to the evaluator:

```bash
uv run python benchmarks/autoresearch/pilot/scripts/evaluate_benchmark_candidate.py \
  base/benchmark-summary.json cand/benchmark-summary.json clickbench_funnel
```

For profiling rather than a decision:

```bash
uv run --group autoresearch python benchmarks/autoresearch/runner.py --profile --full --round 3
# Linux perf + inferno, with Rust symbols from: maturin develop --profile flamegraph
uv run --group autoresearch python benchmarks/autoresearch/runner.py --profile --profiler perf
```

Until this change, a capture without `--sample` or `--data` ran on the sample,
not the full dataset: `runner.py` defaults to the sample and the capture step
passed no data flag. Each summary's `data_file` recorded the file actually
used, so check it before trusting an older "full dataset" result.

## Evaluation contract

The evaluator checks, in this order, and the first failed check decides:

| Check | `reason_code` on failure |
|---|---|
| Both summaries record the target, dataset, `iterations` and `warmup`, and they match: the evaluated target, the same dataset (file name and size, or the same path when a size is unknown), the same counts. A setting missing from both does not count as a match | `incompatible_inputs` |
| The baseline has no correctness or infrastructure failure, every target and protected workload, and a positive median and p95 for each | `invalid_baseline` |
| The candidate has no correctness failure (count or a failed workload validation) | `correctness_failure` |
| ... no infrastructure failure | `infra_failure` |
| ... every target and protected workload | `missing_workloads` |
| ... each of them completed and validated `pass` | `unvalidated_workload` |
| ... a positive, finite median and p95 for each | `missing_metrics` |
| No protected workload slower by the regression bar or more in median **or** p95 | `protected_regression` |
| At least one target workload faster by the improvement bar or more in median **and** p95 | `no_target_improvement` |

Passing every check gives `keep` with `reason_code: target_improvement`;
anything else is `discard`. `evaluation.json` also lists `problems` (every
failed check, not only the deciding one), `missing_workloads`, the per-workload
deltas behind a win or a regression, both git SHAs, the `thresholds` used and
where they came from, and `warnings`. A difference in platform, processor, CPU
count or Python version between the two runs is a warning rather than a
rejection, because an OS or library update changes these on the same machine;
read it before trusting the numbers.

Exit status, for both the gate and the evaluator: 0 keep, 1 discard, 2 when no
evaluation can be made (a summary is missing, unreadable or not a benchmark
summary, the target is unknown, or an option is invalid). On status 2 the gate
leaves the diff directory empty, so an earlier evaluation cannot be mistaken
for this one.

A target win needs median **and** p95. Issue #51 set this rule, and the code
and tests have required it since April 2026. The rule documents said "median
or p95" until this change. The rule is conservative: a change that only trims
the tail, or only the typical run, is not a win.

The gate is a tool for one person or agent judging one change. It is not a CI
check, and nothing runs it on PRs; a regression gate in CI is #158's open
question.

## Thresholds and their limits

The defaults are a 3% improvement bar and a 5% regression bar, for every
target. They have not been validated statistically, and they should not be
read as noise limits.

- An earlier version of this document and `pilot/common.py` cited same-commit
  variation of about 0.8% for R1, 1.6% for R3 and 9% for R2 on the full
  dataset. The runs behind those figures are not in the repository, and
  because of the capture bug above they may have been taken on the sample.
  Treat them as unverified.
- If R2 does vary by around 9% between identical runs, both bars are inside
  its noise. A `clickbench_sessionization` keep can then be noise, and R2 as a
  protected workload can reject a change of another target that did not touch
  it.
- The default is 3 timed iterations. p95 over three samples is 0.9 of the way
  from the second-slowest to the slowest, so it is close to a single sample.
  The median is the middle of three.
- `bench_vs.py` runs LTSeq and DuckDB in one process and records its memory
  figure as an RSS difference, not a peak (#331). The gate does not use memory.

Calibrating a target, before relying on its decisions:

1. On one commit and an idle machine, capture a baseline and then a candidate
   with no code change (an A/A comparison). Repeat this several times, with the
   same `--iterations` you intend to use; more iterations narrow the spread.
2. Record each workload's median and p95 deltas from `benchmark-diff.json`.
   The largest absolute delta seen is a floor for that workload's bars, not a
   bound.
3. Try candidate bars with `--min-improvement-pct` and `--max-regression-pct`
   on the gate or the evaluator; `evaluation.json` records them with
   `source: override`.
4. Make a bar the default by setting `target_improvement_threshold_pct` and
   `protected_regression_threshold_pct` on that target in `pilot/common.py`,
   in a PR that includes the A/A deltas.

Bars apply per target, so a workload that is noisier as a protected workload
than as a target needs its own target definitions, or a wider bar on the
targets that protect it.

## Artifacts

Under `benchmarks/autoresearch/pilot/reports/`:

- `baseline/<target>/` and `candidates/<target>/`: `runner.py`'s output
  (`summary.json`, `bench_vs.json`, `report.md`), `benchmark-result.json` (a
  copy of the runner summary), `benchmark-summary.json` (normalized: target, git SHA, dataset
  path and size, warmup, iterations, system, failure counts, per-workload
  median, p95, samples and validation), and `benchmark-result.md`.
- `diff/<target>/`: `benchmark-diff.json` and `.txt` (median and p95 deltas
  for the target and protected workloads) and `evaluation.json` and `.txt`.

Each capture and gate run clears its own directory first, so files never mix
two runs.

`bench_vs.py` writes `benchmarks/clickbench_results.json`; the fields the
capture reads are `passed`, `correctness_failures`, `infra_failures`,
`completed_rounds`, `total_rounds`, `data_file`, `warmup`, `iterations`,
`system` and `rounds`. Each round carries `round_id`, `round_name`,
`benchmark_status` and `validation.status` / `validation.detail`. A round that
raises is recorded as `benchmark_status: infra_failure` rather than aborting
the run, so the evaluator never parses stdout.

## Known limitations

- The sample is not sorted although the benchmark declares it sorted, and the
  Parquet order verifier skips row-group seams (#336).
- There is no published comparative claim; the numbers in
  `CLICKBENCH_MAPPING.md` predate several changes (#331).
- Only the three ClickBench rounds are gated. `bench_core.py` runs through
  `runner.py` but has no evaluation, and many operators have no benchmark
  (#158).
