# DataFusion 56 compatibility spike (#411)

#411 asks whether LTSeq can move from DataFusion 55.0.0 to 56, and whether
56 fixes #296 (DataFusion's simplifier combining two membership tests of one
value wrongly), before anyone opens an upgrade PR. The findings and the
Go/No-Go recommendation are in [REPORT.md](REPORT.md). Nothing here changes
the shipped build: `Cargo.toml` stays on 55.0.0, and the 56 build exists only
as a patch to apply in a throwaway checkout.

| File | What |
|---|---|
| `REPORT.md` | The feasibility report |
| `repro_296.py` | #296's counterexamples as a matrix over today's API, with a three-valued reference, a comparison of two runs, and (`--plans`) the full plans of representative cases |
| `probes.py`, `df_probe/` | #296's runtime-item and volatile rewrites asked of DataFusion directly in SQL, which LTSeq's literal-only `is_in` cannot reach: the case file and reference (`probes.py`), and a probe (`df_probe/probe.rs`) built against each engine from its own locked graph (`df_probe/df55`, `df_probe/df56`) |
| `witnesses.py` | B1 as wrongly filtered rows, and v0.5 §8.5's never-NULL membership contract against both engines |
| `results/repro-55.json.gz`, `results/repro-56.json.gz` | One matrix run on each engine |
| `results/matrix.md` | `repro_296.py --compare` of the two runs |
| `results/plans-55.json`, `results/plans-56.json`, `results/plans.md` | `repro_296.py --plans` on each engine, and their comparison |
| `results/probe-cases.txt`, `results/probe-55.jsonl`, `results/probe-56.jsonl`, `results/probes.md` | The probe's case file, its output on each engine, and `probes.py --report` |
| `results/witnesses-55.json`, `results/witnesses-56.json`, `results/witnesses.md` | `witnesses.py` on each engine, and their comparison |
| `results/suite.md` | The Rust and Python suites, clippy and the guard ablations on each engine, with the build each ran on |
| `bench_in_lists.py`, `results/bench.md` | Compound membership filters on 10 million rows, and `bench_core.py`, on release builds of each engine |
| `df56-migration.patch` | The source changes that build LTSeq against 56.0.0-rc1, including the spike-only switches and probes |
| `df56-Cargo.lock` | The candidate's resolved dependency graph (299 packages), the `Cargo.lock` every 56 build here used |
| `df55-spike-switches.patch` | The same spike-only switches for a 55.0.0 build (`LTSEQ_SPIKE_SKIP`, `LTSEQ_SPIKE_TRACE_INNER`), for the ablations and the inner plans |

## The candidate

DataFusion `56.0.0-rc1`, the tag on commit
`cce6374e6256b3b8892ec60674578cf32bc3a32c` (the head of `branch-56` on
2026-11-17), with arrow and parquet 60.1.0 from crates.io. 56.0.0 is not
published; the release tracker apache/datafusion#24461 is open. The patch
pins the commit, not the branch, and `df56-Cargo.lock` pins every other
crate: rebuild against it with `--locked`, not with a fresh `cargo update`.
(`cargo update -p datafusion -p arrow -p parquet` on the patched tree gave
this lock on 2026-11-17, and an offline rerun on 2026-11-18 gave it byte for
byte, but a networked rerun later may resolve newer versions.)

## The builds

Debug builds (`maturin develop`, as CI builds them), CPython 3.13, rustc
1.97.1, each in its own virtualenv from `uv.lock`:

Run the block from a checkout of this branch: `REPO` records its root, and
every `git archive` runs against it with `git -C "$REPO"`, because the
extracted trees below hold no Git metadata. Each `cd` then moves into a
patched tree, and the commands after it run from that tree's root.

```bash
REPO=$(git rev-parse --show-toplevel)   # the checkout of this branch

# Baseline, 55.0.0: this tree with the 55 switches only (no-ops unless set).
mkdir /tmp/ltseq-df55 && git -C "$REPO" archive HEAD | tar -x -C /tmp/ltseq-df55
cd /tmp/ltseq-df55   # the 55 tree's root, for the next three commands
git apply benchmarks/datafusion56/df55-spike-switches.patch
UV_PROJECT_ENVIRONMENT=/tmp/venv55 uv sync --no-install-project --group dev --group bench
VIRTUAL_ENV=/tmp/venv55 /tmp/venv55/bin/maturin develop --locked

# Candidate, 56.0.0-rc1: the patch and the candidate's lockfile.
mkdir /tmp/ltseq-df56 && git -C "$REPO" archive HEAD | tar -x -C /tmp/ltseq-df56
cd /tmp/ltseq-df56   # the 56 tree's root, for the next four commands
git apply benchmarks/datafusion56/df56-migration.patch
cp benchmarks/datafusion56/df56-Cargo.lock Cargo.lock
UV_PROJECT_ENVIRONMENT=/tmp/venv56 uv sync --no-install-project --group dev --group bench
VIRTUAL_ENV=/tmp/venv56 /tmp/venv56/bin/maturin develop --locked
```

The Rust tests need libpython on the loader path; run them from the root of
the patched tree under test (`/tmp/ltseq-df55` or `/tmp/ltseq-df56`):
`PYO3_PYTHON=<python> LD_LIBRARY_PATH=<python prefix>/lib cargo test --locked --lib`.

### Patch states

The patch grew during the spike. Behaviorally, every state is the published
patch with one switch set or unset, so each recorded run can be repeated on
the builds above:

| State | What it held | Repeat it with | Runs recorded in it |
|---|---|---|---|
| S0 | The migration only (Cargo, the API changes, `#[allow(deprecated)]`) | the published patch with `LTSEQ_SPIKE_NO_PDF_GUARD=1` (the other switches are no-ops unless set; the trace test and the `resolve.rs` control are test-only) | the full Python suite on 56 (17 Nov), the matrix run `results/repro-56.json.gz`; the first `cargo test --lib` on 56 (235 tests, 7 failed, also with `resolve.rs`'s original control) |
| S1 | S0, the `LTSEQ_SPIKE_SKIP` / `LTSEQ_SPIKE_NO_PDF_GUARD` switches, the `push_down_filter` guard extension (on unless `LTSEQ_SPIKE_NO_PDF_GUARD=1`), the trace test `spike411_probe_eliminate_filter`, and `resolve.rs`'s control replaced by a print | the published patch | the guard ablations, `bench_core.py` and `bench_in_lists.py` (release builds) |
| S2 = published | S1 and the `LTSEQ_SPIKE_TRACE_INNER` switch | the published patch | `cargo test --lib` and clippy on 56 (18 Nov), the full Python suite rerun, the matrix rerun, `--plans`, `witnesses.py` |

On 55 the matrix ran on this tree unchanged; the ablations, `--plans` and
`witnesses.py` on this tree with `df55-spike-switches.patch`, whose switches
change nothing unless set.

## Reproducing

Every path below is relative to a tree's root: run the block from the root
of either patched tree (both hold the same `benchmarks/datafusion56/`) or of
the checkout itself. Each virtualenv's Python imports the `ltseq` its own
tree built, whatever the working directory.

```bash
# The matrix (about nine minutes each on a debug build), then the comparison.
/tmp/venv55/bin/python benchmarks/datafusion56/repro_296.py --label 55.0.0 --out /tmp/repro-55.json.gz
/tmp/venv56/bin/python benchmarks/datafusion56/repro_296.py --label 56.0.0-rc1 --out /tmp/repro-56.json.gz
python benchmarks/datafusion56/repro_296.py --compare /tmp/repro-55.json.gz /tmp/repro-56.json.gz

# Full plans of the representative cases, with the sorted scan's inner plan.
/tmp/venv55/bin/python benchmarks/datafusion56/repro_296.py --plans --label 55.0.0 --out /tmp/plans-55.json
/tmp/venv56/bin/python benchmarks/datafusion56/repro_296.py --plans --label 56.0.0-rc1 --out /tmp/plans-56.json
python benchmarks/datafusion56/repro_296.py --compare-plans /tmp/plans-55.json /tmp/plans-56.json

# The direct DataFusion probe: runtime items and volatile values.
python benchmarks/datafusion56/probes.py --emit /tmp/cases.txt
(cd benchmarks/datafusion56/df_probe/df55 && cargo run --locked -q -- /tmp/cases.txt) > /tmp/probe-55.jsonl
(cd benchmarks/datafusion56/df_probe/df56 && cargo run --locked -q -- /tmp/cases.txt) > /tmp/probe-56.jsonl
python benchmarks/datafusion56/probes.py --report /tmp/probe-55.jsonl /tmp/probe-56.jsonl

# B1's filter witness and §8.5's contract.
/tmp/venv55/bin/python benchmarks/datafusion56/witnesses.py --label 55.0.0 --out /tmp/witnesses-55.json
/tmp/venv56/bin/python benchmarks/datafusion56/witnesses.py --label 56.0.0-rc1 --out /tmp/witnesses-56.json
python benchmarks/datafusion56/witnesses.py --compare /tmp/witnesses-55.json /tmp/witnesses-56.json
```

`repro_296.py` and `witnesses.py` import `ltseq` and `pyarrow` even to
compare, so run their comparisons with either virtualenv's Python.

### Switches (spike-only; each is a no-op unless set)

| Variable | Builds | Effect |
|---|---|---|
| `LTSEQ_SPIKE_SKIP=<rule>[,<rule>]` | 55 and 56 | leaves the named LTSeq optimizer rules out of every session (the guard ablations in `results/suite.md`) |
| `LTSEQ_SPIKE_NO_PDF_GUARD=1` | 56 | turns off the patch's `push_down_filter` guard extension (finding B2) |
| `LTSEQ_SPIKE_TRACE_INNER=<file>` | 55 and 56 | appends to `<file>` the optimized logical and physical plans a sorted table runs for the filters pushed into its scan, which `explain_plan` does not show |

The ablation commands, per rule and engine, are in `results/suite.md`.
