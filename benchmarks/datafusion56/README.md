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
| `repro_296.py` | #296's counterexamples as a matrix over today's API, with a three-valued reference, and a comparison of two runs |
| `results/repro-55.json.gz`, `results/repro-56.json.gz` | One run of the matrix on each engine |
| `results/matrix.md` | `repro_296.py --compare` of the two runs |
| `results/suite.md` | The Rust and Python suites, clippy and the guard ablations on each engine |
| `bench_in_lists.py` | Compound membership filters on 10 million rows, where #296's fix changes plans |
| `results/bench.md` | `bench_core.py` and `bench_in_lists.py` on release builds of each engine |
| `df56-migration.patch` | The source changes that build LTSeq against 56.0.0-rc1, including the spike-only probes |

## The candidate

DataFusion `56.0.0-rc1`, the tag on commit
`cce6374e6256b3b8892ec60674578cf32bc3a32c` (the head of `branch-56` on
2026-11-17), with arrow and parquet 60.1.0 from crates.io. 56.0.0 is not
published; the release tracker apache/datafusion#24461 is open. The patch
pins the commit, not the branch.

## Reproducing

```bash
# Baseline: this checkout, DataFusion 55.0.0.
maturin develop
python benchmarks/datafusion56/repro_296.py --label 55.0.0 --out /tmp/repro-55.json.gz

# Candidate: a copy with the patch applied, in its own virtualenv.
mkdir /tmp/ltseq-df56 && git archive HEAD | tar -x -C /tmp/ltseq-df56
cd /tmp/ltseq-df56 && git apply benchmarks/datafusion56/df56-migration.patch
cargo update -p datafusion -p arrow -p parquet
maturin develop
python benchmarks/datafusion56/repro_296.py --label 56.0.0-rc1 --out /tmp/repro-56.json.gz

python benchmarks/datafusion56/repro_296.py --compare /tmp/repro-55.json.gz /tmp/repro-56.json.gz
```

A matrix run takes about nine minutes on a debug build. On the patched
build, `LTSEQ_SPIKE_SKIP=<rule>[,<rule>]` leaves the named LTSeq optimizer
rules out of every session, and `LTSEQ_SPIKE_NO_PDF_GUARD=1` turns off the
spike's extra `push_down_filter` guard; `results/suite.md` lists the
ablations run with them. The Rust tests need libpython on the loader path
(`LD_LIBRARY_PATH=<python prefix>/lib cargo test`).
