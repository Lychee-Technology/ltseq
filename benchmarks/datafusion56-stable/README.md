# DataFusion 56.0.0 stable: D6 evidence for PR #423 (#296)

The owner's decision on PR #423's review round 1 (R1-F5) puts the evidence
for the **stable** DataFusion 56.0.0 upgrade here. `benchmarks/datafusion56/`
stays frozen as the #411 spike's **56.0.0-rc1** archive; nothing in this
directory was produced on RC1, and nothing there is restated as stable.

## What was compared

| | Baseline | Candidate |
|---|---|---|
| Source | `291aa14cf8af5c108ba35a06b960ed9fc9ed5cee` (`main`, PR #423's merge base) | PR #423's head at the time of the run (DataFusion 56.0.0 from commit `27379478506a7ccacc04d7c24920cd5dd4b41e17` on; the run's head is named in each result) |
| DataFusion | 55.0.0 | 56.0.0 (crates.io, checksum `0d7fbede7d2bf609055871b98ad99151a2cdd95ba333f5a8874ad03e8dc07779`, the stable tag's commit `7e7ae72a820e2bf87da1205b96639fba334ed2f7`) |
| arrow / parquet | 59.2.0 | 60.2.0 |
| Lockfile | `Cargo.lock` at `291aa14` | the PR's `Cargo.lock` |

Both are **release** wheels built the way the project ships them: the
`[profile.release]` of each tree (fat LTO, one codegen unit, `opt-level = 3`,
stripped), `maturin build --release --locked`, offline, rustc 1.97.1,
maturin 1.14.1, CPython 3.13 (`uv`), on one machine: Intel Core i7-8700K,
8 CPUs visible, 23 GiB, Linux 7.0.14 under Proxmox. Each wheel was unpacked
into its own directory and put first on `PYTHONPATH` for the project's
virtualenv, so each run imports only its own `ltseq`; every result file
records the `ltseq_core` it loaded.

```bash
REPO=$(git rev-parse --show-toplevel)
# Candidate: the checkout itself.
maturin build --release --locked -o /var/tmp/wheels56
# Baseline: 291aa14 exported, with its own target directory.
mkdir /var/tmp/base && git -C "$REPO" archive 291aa14 | tar -x -C /var/tmp/base
(cd /var/tmp/base && CARGO_TARGET_DIR=/var/tmp/base/target maturin build --release --locked -o /var/tmp/wheels55)
for v in 55 56; do mkdir /var/tmp/site$v && (cd /var/tmp/site$v && python -m zipfile -e /var/tmp/wheels$v/*.whl .); done
# Then: PYTHONPATH=/var/tmp/site55 python ...   and   PYTHONPATH=/var/tmp/site56 python ...
```

## R1-F4: deep computed sort and group keys

DataFusion 56 plans a sort or grouping over a column computed by a deep
expression in time that grows about as the cube of the depth. The owner
accepted this for the upgrade on three conditions; their evidence:

1. **Release-build `distinct` timings, 55 against stable 56, sorted and
   unsorted, depths 25, 50, 100 and 200, same machine and layout**
   (8 partitions, 8,192-row batches, `pinned_engine`; each case in a fresh
   subprocess on a 1 GiB thread, median of 3):
   [`bench_deep_distinct.py`](bench_deep_distinct.py),
   [`results/deep-distinct-55.json`](results/deep-distinct-55.json),
   [`results/deep-distinct-56.json`](results/deep-distinct-56.json),
   [`results/deep-distinct.md`](results/deep-distinct.md).

   | `t.derive(s=a + 1 + … + 1).distinct().count()` | depth | 55.0.0 (s) | 56.0.0 (s) | 56 / 55 |
   |---|---:|---:|---:|---:|
   | sorted (`t.sort("a")`) | 25 | 0.0054 | 0.0226 | 4.2× |
   | sorted | 50 | 0.0105 | 0.188 | 18× |
   | sorted | 100 | 0.0293 | 2.17 | 74× |
   | sorted | 200 | 0.106 | 28.6 | 270× |
   | unsorted | 25 | 0.0046 | 0.0056 | 1.2× |
   | unsorted | 50 | 0.0083 | 0.0284 | 3.4× |
   | unsorted | 100 | 0.0249 | 0.263 | 11× |
   | unsorted | 200 | 0.0921 | 3.21 | 35× |

   At 25 levels the cost stays within hundredths of a second; it is the
   deep end that grows, by 5 to 13 times per doubling on 56 (2 to 4 on 55).

   ```bash
   PYTHONPATH=/var/tmp/site55 python bench_deep_distinct.py --label 55 --out results/deep-distinct-55.json
   PYTHONPATH=/var/tmp/site56 python bench_deep_distinct.py --label 56 --out results/deep-distinct-56.json
   python bench_deep_distinct.py --compare results/deep-distinct-55.json results/deep-distinct-56.json
   ```

2. **A minimal plain-DataFusion reproduction and a draft upstream report**:
   [`deep_sort_repro/`](deep_sort_repro/) (one source, `repro.rs`, built
   against `df55/` and `df56/`, each with its own `Cargo.lock`; three runs
   each in [`results/deep-sort-repro-55.tsv`](results/deep-sort-repro-55.tsv)
   and [`results/deep-sort-repro-56.tsv`](results/deep-sort-repro-56.tsv))
   and [`UPSTREAM_DRAFT.md`](UPSTREAM_DRAFT.md). With no LTSeq code,
   `create_physical_plan` for `ORDER BY s` over a 200-level `s` takes
   6.5–7.1 s on 56.0.0 against 0.035 s on 55.0.0, and `GROUP BY a, s`
   4.3–5.2 s against 0.034 s. **The operator files the draft with
   apache/datafusion before the PR merges**; that issue is the tracked
   mitigation, and its URL goes in `UPSTREAM_DRAFT.md` and the PR.

   ```bash
   (cd deep_sort_repro/df55 && cargo run --release --locked -q -- 25 50 100 200)
   (cd deep_sort_repro/df56 && cargo run --release --locked -q -- 25 50 100 200)
   ```

3. **The deep-plan stack check** keeps `distinct` at 100 levels
   (`DISTINCT_DEEP`) and every other operation at 500 (`DEEP`) in
   `test_operations_on_a_deep_plan_on_a_small_thread`
   (`py-ltseq/tests/test_expression_depth.py`). When the upstream issue is
   fixed, `DISTINCT_DEEP` goes back to 500.

## R1-F5: the D6 evidence on stable

The rest of this directory is the stable rerun of the #411 archive's
evidence, with that archive's scripts unchanged (they live in
`benchmarks/datafusion56/` and are run from here against each wheel). See
[`results/STATUS.md`](results/STATUS.md) for which items are recorded and
which are still to run.
