# Stable D6 evidence: what is recorded (#296, PR #423)

Baseline: `291aa14` (DataFusion 55.0.0). Measured candidate: PR #423 at
`f3c9404` (DataFusion 56.0.0 stable; its Rust sources are the reviewed head
`5f93a2e`'s, and it differs from that head only in docstrings). **These are
not the final Rust sources**: `5a6fdfa` later changed
`src/transpiler/literal_policy.rs` and `src/transpiler/resolve.rs`, so the
negative-scale string-cast check now reads types through `ask`, which also
catches a panicking type lookup. The items that run LTSeq's transpiler
(matrix, plans, witnesses, deep `distinct` timings, memory harness) were
rerun on a release wheel of `5a6fdfa`. Every outcome is the same;
[`final-head/README.md`](final-head/README.md) has the comparison and why
the other items were not rerun. Each file below names the build it ran on.

| Item (owner's decision on R1-F4 / R1-F5) | Files | Build | Outcome |
|---|---|---|---|
| Release-build deep `distinct` timings, 55 against stable 56, sorted and unsorted, depths 25–200 | `deep-distinct-55.json`, `deep-distinct-56.json`, `deep-distinct.md` | fat-LTO release wheels | sorted 200 levels: 0.106 s against 28.6 s; 25 levels: 0.005 s against 0.023 s |
| Plain-DataFusion reproduction and upstream report | `../deep_sort_repro/`, `deep-sort-repro-55.tsv`, `deep-sort-repro-56.tsv`, `../UPSTREAM_DRAFT.md` | `cargo --release` | `create_physical_plan`, `ORDER BY` over 200 levels: 0.035 s against 6.5–7.1 s. The operator reports filing it as apache/datafusion#27763 |
| 55-red / 56-green matrix, unsorted and sorted, 9 types and encodings | `repro-55.json.gz`, `repro-56.json.gz`, `matrix.md` | fat-LTO release wheels | 308 cases wrong on 55 are right on 56; none right on 55 is wrong on 56; none wrong on both |
| Optimized logical and physical plans of representative cases | `plans-55.json`, `plans-56.json`, `plans.md` (release wheels, no inner plans); `plans-traced-55.json`, `plans-traced-56.json`, `plans-traced.md` (switch builds, with the sorted scan's inner plans) | as stated | each kind wrong on 55 and right on 56, unsorted and sorted, with both plans |
| Direct SQL probe: column items and volatile values | `probe-cases.txt`, `probe-55.jsonl`, `probe-56.jsonl`, `probes.md`; `../df_probe56/` | `cargo --release` against 55.0.0 and 56.0.0 | every row-varying case wrong on 55 is right on 56; volatile values are drawn twice on 56, once (merged) on 55 |
| B1 witnesses and v0.5 §8.5's membership contract | `witnesses-55.json`, `witnesses-56.json`, `witnesses.md` | fat-LTO release wheels | B1: the expected rows on both, run-end and plain, with the filter plans of each; §8.5 (not shipped): both engines give today's SQL answer, the same on 56 as on 55 |
| Guard ablations on stable | `suite.md`, `../df56-stable-switches.patch` | debug builds with evidence-only switches | every rule the PR keeps is still needed except `ltseq_keep_sorts_above_offsets` (redundant for its tests) and `ltseq_float_list_zeros` (one plan-shape assertion only) |
| `bench_core.py` and `bench_in_lists.py` | `bench.md`, `bench_core/` | fat-LTO release wheels | geometric mean 56 / 55 = 0.94 over 47 benchmarks; slower: `read_csv_10000` (1.26×) and the float intersection (1.65×, expected from apache/datafusion#24952) |
| Fat-LTO release build | `packaging.md` | the shipped release profile | both build without warnings; 56's `.so` is 8.4 % larger |
| Memory harness | `memory.md`, `memory/harness-55.txt`, `memory/harness-56.txt` | fat-LTO release `.so` | 16 passed, 2 xfailed on both, same per-test outcomes |
| Rerun on the final Rust sources (`5a6fdfa`) | `final-head/` | fat-LTO release wheel of `5a6fdfa` | matrix, plans, witnesses and memory verdicts the same as `f3c9404`'s; deep `distinct` medians within 2 % |
| macOS | (CI) | | from CI's `macos` job on the PR, per the owner's decision; not run here |

Reported on #296 by the operator after the local runs (their comment of
2026-11-26), and not checked by this file:

- **Upstream filing**: `../UPSTREAM_DRAFT.md` was filed at
  https://github.com/apache/datafusion/issues/27763.
- **The PR description**: PR #423's description was updated to separate
  `benchmarks/datafusion56-stable/` from the frozen RC1 archive
  (`benchmarks/datafusion56/`), state that the initial stable runs were made
  at `f3c9404`, and cite `results/final-head/` for the reruns on the final
  Rust sources (`5a6fdfa`) and the committed memory-harness logs.

Not done here, and not claimed:

- **macOS and Linux CI on the final head**: CI's, on the pushed commit.
