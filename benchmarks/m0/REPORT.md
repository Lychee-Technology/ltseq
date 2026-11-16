# M0 feasibility report (#251, #306)

PR #250's API review (finding V9) found that the v0.5 contract's costliest
commitments would be priced only after being built. #251 prices them first,
with prototypes, so the owner can record a budget for each gated item before
work starts. This page gives the findings and the decisions they inform; every
cell is in [results/](results/), and [README.md](README.md) says how to rerun
them.

Nothing here is a budget. Recording budgets is the owner's step (#251's third
acceptance criterion), and the questions for it are at the end.

#306 added two kernels that the reassessment found unpriced ([#251
comment](https://github.com/Lychee-Technology/ltseq/issues/251#issuecomment-6072922309)),
with the same harness: m7, decimal `/` and `%` through a 256-bit
intermediate (gates M6), and m8, exact `quantile` and `median` (gates M13).
They ran later, on another machine, so their ratios are not comparable
cell by cell with m1 to m6 ([Setup](#setup)).

## Setup

| | |
|---|---|
| Machine | AMD Ryzen 5 5600G (6 cores, 12 threads), 27 GiB, Linux 7.2.8, NVMe |
| Build | `maturin develop --release --features m0` (fat LTO), rustc 1.96.1, Python 3.13.15, pyarrow 25.0.1 |
| Commit | `bfa963a` for m1, m3, m5 and m6, each with a clean worktree. m4b ran again at `15209f4`, after its `operator` placement over a sort was fixed, which changed only that placement's three plans over the sort. m4 ran again at `e168139`, after its `fallible_and_guard` was made to run in its written order, and m2 at `1668833`, after its grouped state stopped walking every group to report its size and its cells stopped skipping partial aggregation. Of the branch's other later commits, `aeea468` turns off work stealing in m5's `prune_off` and `validate`, which changes no m5 plan. m4b's `scan_filter_off` control turned its scan options off through `validate=prune_off`, as its cells record, so from `aeea468` its partitions stopped sharing work while `pushdown`'s still did; `c5753f8` gives the control its own flag, `pruning=off`, which sets what `prune_off` set at `15209f4` and nothing more, so the control runs as measured again. A later commit makes `validate` refuse a scan of several files, which no cell has; the rest add this report, the results, tests, a stricter self-check, a CI step, README notes, comments and a `--batch-size` option for `run.py`, and drop a blank line in `report.py` |
| Command | `python benchmarks/m0/run.py --only m6,m4b,m3,m5,m1,m2,m4`, one run started 2026-10-17 17:22 EDT; m4b again with `--only m4b`, started 2026-10-18 21:09 EDT; m4 again with `--only m2,m4`, started 2026-10-19 11:09 EDT (its m2 results were replaced); m2 again with `--only m2`, started 2026-10-19 12:19 EDT |
| Partitions | 1, 2, 4 and 12 (the CPU count) through #274's `pinned_engine`; m3 adds 24 for the baseline comment |
| Data | bench_core's synthetic table at 10K, 100K and 1M rows (m1 and m2 add 10M); ClickBench's 1M-row sample and its 100M-row file sorted by `(userid, eventtime, watchid)` (16 GiB, 814 row groups); the R3 harness's 640 MiB input |

**#306's measurements (m7, m8).**

| | |
|---|---|
| Machine | An LXC container limited to 8 of the 12 threads (CPUs 0 to 7) of an Intel Core i7-8700K (6 cores), 23 GiB, Linux 7.0.14 (Proxmox) |
| Build | `maturin develop --release --features m0` (fat LTO), rustc 1.97.1, Python 3.13.18, pyarrow 25.0.1 |
| Commit | m7 at `fa38940`, m8 at `0ec3c3b`, each with a clean worktree. `0ec3c3b` changes only how m8's global accumulator hands on its state (m8 section) |
| Command | `python benchmarks/m0/run.py --only m7,m8`, started 2026-11-16 19:24 UTC (its m8 results were replaced); m8 again with `--only m8`, started 2026-11-16 20:08 UTC |
| Partitions | 1, 2, 4 and 8 (the CPU count the container reports) |
| Data | m7: generated `decimal128` columns at 1M and 10M rows (`inputs.decimals`); m8: bench_core at 1M and 10M rows |
| Contention | None of the 444 cells ran while other processes kept more than half a core busy |

With `decimal` and `quantile` off, `session` adds neither rule, so a
measurement build's baseline is still today's engine. The test suite was
not rerun on this machine.

**Baseline.** Each measurement compares a prototype with the same build and
every flag off. With every flag off a measurement build is today's engine:
the test suite passes on it (15,702 tests), except five tests in
`test_gil_release.py` whose guard refuses a call that finishes in under
30 ms, which these calls do in a release build. On a default debug build,
as CI tests it, all 15,706 pass.

**Method.** Each cell (one pipeline, terminal, input, partition count and
flag set) runs in a fresh process: it builds the input, runs the pipeline
once over a 10K-row warm-up input, then times 5 runs (3 over the sorted
ClickBench file and the R3 input). A run's time covers building the plan and
draining the terminal; the tables give the median. Every run of a cell
queries one table, built before the first, so later runs find the
Parquet footer that DataFusion cached during the first. Over the sorted
ClickBench file that read takes about 30 ms, ten times today's lookup,
so m4b and m5 give lookup ratios two ways: by medians, which describe a
repeated query, and by first runs, which describe the first query on a
table. Peak memory is the resident growth over the first measured run,
as in the R3 harness. TTFR is the time to the first non-empty batch of
the Arrow C stream, which is what `to_batches` will return.

**Noise.** m1, m3, m4b, m5 and m6 also ran at an earlier commit with the
same prototypes. Between the two runs a cell's ratio to today moved by
0.03 to 0.07 at the median and 0.11 to 0.19 at the 90th percentile; the
large m4b and m5 ratios moved by under 10%; m4b's third run is compared
in its section. m2 and m4 ran again after their fixes; their cells
whose plans did not change moved by 0.04 at the median and by 0.15 (m4)
and 0.23 (m2) at the 90th percentile, m2's most in global aggregates
that take 1 to 15 ms. Cells that take a few milliseconds move most.
Read a ratio within 0.1 of 1 as no difference, and within 0.2 for a
cell under 10 ms.

**Contention.** Other workloads share the machine. Before each cell the
harness waited until the machine was at most 8% busy. When other
processes kept more than half a core busy during a cell, it ran the cell
again, up to three times, and kept the least contended attempt. 26 of
1,894 cells ran more than once. 3 stayed contended (2 in m5 and 1 in
m4b, with at most 0.65 of another core busy) and are flagged in
`results/*.md`. That load is averaged over the cell's whole process,
which also builds the input and runs the warm-up, so a burst during the
timed runs alone can stay under the threshold. The run-to-run noise
above was measured with the same harness, so it includes what such
bursts did in those runs.

## Summary

Each row gives what the contract's version costs against today on this
machine, from the sections below. Ratios are median times; "today" is
the same build with every prototype off.

| | Prices | Gates | Cost against today | Decision it informs |
|---|---|---|---|---|
| m1 | Checked integer arithmetic | M5, #221 | Within noise for `+ - *` computed as wrapping arithmetic plus an overflow mask; up to 1.3× for a grouped `sum` over 100,000 groups at 10M rows, 1.43× at 1M | The budget only |
| m2 | Exact float statistics | M13, M4, OD-W | 0.9 to 2.0× for global `var`, `std`, `cov` and `corr`, up to 4.1× (about 12 ms per 10M rows) for `sum` and `mean`; 1.7 to 1.9× grouped over a few keys, 3.1 to 4.0× (4.3 to 4.9× at 1M rows) and 2.9× the memory over 100,000 groups; windows 1.3 to 3.8× (compensated 1.1 to 1.6×) | OD-W: exact or compensated windows; a budget exception or a leaner state for many groups |
| m3 | Order-preserving merge | M1, #148 | Row-group merge on R3 with whole row groups buffered: 0.97 to 1.09× at 12 and 24 partitions, 1.18× at 4, 7 to 15 MiB more; with a buffer that binds, 1.4 to 2.1×; up to 1.66× on a file of 11 row groups. Contiguous ranges, the only schedule measured on CSV: 1.4 to 4.3× | How the buffer is bounded; a budget per reader |
| m4 | Masked `&` and `\|` | M16, #252 | One `&` or `\|`, nesting not priced. Errors kept as masks: within noise without failing rows, up to 1.6× with them. Today's checked plan raises in 22 of 24 cells with failing rows; the two that pass run the guard first at a 1% share | Which strategy M16 builds |
| m4b | Pushdown lost to D10a | M16 | If every checked operation counts as fallible: filter kept above the keys' computation, no cost where statistics cannot prune, 59 to 265× on a repeated lookup by a sorted key and 7.5 to 22× on a first one; kept above the operator, 1.2 to 2.3× and up to 1,440× (117× first). Every measured key is infallible by its operand types | Where M16 places the filter; how it decides a key cannot fail |
| m5 | `assume_sorted` checked, no pruning | M3 | The check: 5 to 7% on top of D7's read, 1.2 to 1.8× on full scans; the lost pruning: 156 to 604× on a repeated sort-key lookup, 70 to 245× on a first one | D7 as written, or a weaker check that keeps pruning |
| m6 | `string_view` normalized | M18 | Plain at the scan: up to 1.3× on filters and grouping; cast at the root: 1.1 to 1.45× on string-heavy results; both hold 25 to 35% less memory on export at one partition, and between 14% less and 32% more at 12 | Where M18 normalizes |
| m7 | Decimal `/` and `%` through 256 bits (#306) | M6 | Guarded fallback: `/` at today's time (0.87 to 1.10×) with none or 1% of rows needing 256 bits, `%` 1.1 to 1.3× from the floored remainder and checks, not the guard; every row at 256 bits: 2.0 to 2.6× (`/`) and 2.2 to 4.1× (`%`) today's time on rows that fit, where today raises. 256 bits on every row: 2× (`/`), 1.6 to 2.8× (`%`). Memory unchanged | Guarded or always-wide; whether `%` shares the intermediate |
| m8 | Exact `quantile` and `median` (#306) | M13 | 0.70 to 0.95× `percentile_cont` globally, within noise of it grouped; exact interpolation not measurable (about 0.1 µs a group); same memory. 1.06 to 1.10× today's `median`; 8 to 24× faster than today's t-digest `percentile` over 100,000 groups | The budget only; windowed quantiles still unpriced |

The two large costs are both lost row-group pruning (m4b and m5), and
both come from rules that forbid a filter or a skip below a point the
contract fixes. Apart from pruning, the worst measured costs are exact
statistics: 3.8 times on a rolling `std`, 4.1 times on a global `mean`
at one partition (12 ms more), and 3.1 to 4.9 times grouped over 100,000
groups. Other costs above 1.5 times, with the cheapest prototype
measured: m5's check at 1.7 to 1.8 times on full scans; m3's row-group
merge once its buffer binds (1.4 to 2.1 times) and on a file with fewer
row groups than partitions (1.66); contiguous ranges, the only CSV
schedule measured (1.85 to 3.1); and m4's `optimistic` at 1.75 to 1.9
times when a batch raises and is evaluated again.

## m1: checked integer arithmetic (gates M5, #221)

**What it prices.** Contract §17.1 makes integer overflow raise
`ArithmeticOverflowError`. DataFusion 55 wraps on Int64 `+ - *` and in
`sum`; `/`, `%`, `abs` and LTSeq's `//` already raise, and `//` and `%`
are timed as controls (the `abs` pipeline also subtracts, so it times a
checked `-`). v0.4 has no integer `**`: its `power` computes in float64, which
the prototype replaces with a checked integer power cast back to float64.

**Prototypes** (`checked.rs`). `arrow` uses arrow's checked kernels, which
stop at the first overflowing row. `fast` computes the wrapped values and
one overflow word per batch in a single branch-free loop, and looks for the
row only when the word is set. Both make `sum` add in i128 and check only
the values it returns, because the contract judges a sum by its value: a
check per addition would raise for some row orders and not others.
Nothing in the inputs overflows (`value` is in [1, 1000]), so the
measurement prices the check, not the error path.

**Results.** In-memory bench_core at 10M rows, streamed. Today's median at
one partition, then each prototype's time as a multiple of today's at 1,
2, 4 and 12 partitions. Peak memory differs between modes only for the
grouped sum over 100,000 groups: 57 MiB today and 74 to 75 MiB checked,
at 12 partitions. At 10K to 1M rows the element-wise pattern is the same
with more noise, since such a pipeline takes about a millisecond at 1M
rows ([results/m1.md](results/m1.md)). The grouped sum over 100,000
groups costs more at 1M rows: 1.06 to 1.43 times, on cells of 10 to
18 ms, above this measurement's noise for cells of that size.

| pipeline | terminal | off s (p=1) | arrow × at p=1/2/4/12 | fast × at p=1/2/4/12 |
|---|---|---|---|---|
| add | stream | 0.009 | 1.37 / 1.20 / 0.98 / 1.03 | 1.03 / 1.05 / 1.04 / 0.96 |
| sub | stream | 0.009 | 1.36 / 1.13 / 0.98 / 1.03 | 1.07 / 1.01 / 0.90 / 0.97 |
| mul | stream | 0.010 | 1.29 / 1.11 / 1.01 / 1.14 | 1.06 / 0.99 / 1.07 / 1.04 |
| floordiv | stream | 0.020 | 1.01 / 0.84 / 0.82 / 1.03 | 1.01 / 0.92 / 0.85 / 1.01 |
| mod | stream | 0.019 | 0.99 / 0.99 / 1.00 / 1.03 | 0.99 / 1.01 / 0.92 / 0.99 |
| abs | stream | 0.014 | 1.37 / 1.27 / 1.34 / 1.01 | 1.01 / 0.98 / 1.14 / 1.01 |
| pow | stream | 0.106 | 0.19 / 0.19 / 0.21 / 0.34 | 0.31 / 0.33 / 0.31 / 0.38 |
| expr | stream | 0.035 | 1.54 / 1.32 / 1.23 / 1.28 | 1.04 / 0.92 / 1.05 / 1.01 |
| sum | stream | 0.004 | 1.15 / 1.11 / 1.07 / 1.01 | 1.09 / 1.09 / 1.01 / 1.02 |
| sum_by_category | stream | 0.062 | 1.10 / 1.10 / 1.10 / 1.07 | 1.13 / 1.11 / 1.12 / 1.06 |
| sum_by_id_mod | stream | 0.095 | 1.07 / 1.03 / 1.27 / 1.20 | 1.08 / 1.21 / 1.32 / 1.18 |
| cum_sum | stream | 1.103 | 0.68 / 0.72 / 0.74 / 0.75 | 0.69 / 0.73 / 0.75 / 0.75 |

**Reading.** Checked arithmetic is affordable if it is computed the way
`fast` computes it.

- `fast` is within noise of today on `+ - *` (0.90 to 1.07) and on the
  compound expression (0.92 to 1.05) at every partition count. The
  controls `//` and `%`, which neither prototype changes, moved between
  0.82 and 1.03, which is this measurement's noise floor.
- `arrow` costs 1.3 to 1.55 times at one partition and fades toward 1.0 as
  partitions are added. Across two runs a ratio moved by 0.06 at the
  median and 0.19 at the 90th percentile, so the fade is not resolved
  more finely than that.
- A checked `sum` costs 1.0 to 1.15 times globally or over a few groups,
  and up to 1.3 times over 100,000 groups (1.43 at 1M rows), where each
  group keeps an i128 instead of an i64.
- Integer `**` takes 0.19 to 0.38 of today's time, and `cum_sum` 0.68 to
  0.75. Neither is a cost: today's `power` computes in float64, and the
  checked window sum happens to be faster than DataFusion's.

For M5 the measured cost is nothing on element-wise arithmetic and up to
1.3 times on grouped sums over many groups at 10M rows, 1.43 at 1M; a
budget should be set against the larger figure.

## m2: exact float accumulation (gates M13, M4; OD-W, #276)

**What it prices.** Contract §14.2 rounds `sum`, `mean`, `var`, `std`,
`cov` and `corr` once from exact sums, so a result does not depend on the
order rows arrive in. DataFusion accumulates in f64 (Welford's updates for
the moments), and a parallel aggregate does not fix the order.

**Prototypes** (`exact.rs`, `exact_acc.rs`). `exact` keeps Σx, Σx² and Σab
in superaccumulators and rounds once; windows retract by exact
subtraction. `compensated` keeps them as double-doubles: deterministic for
one row order and far more accurate than f64, but neither exact nor
order-independent. It prices a weaker answer to OD-W (#276), not the
contract. Partial aggregates exchange their sums serialized. A grouped
accumulator reports its memory from a running total of what its sums
allocate, because DataFusion asks for it after every batch.

DataFusion 55 skips partial aggregation in a partition whose first
100,000 rows hold more than 0.8 groups per row. Over 100,000 groups,
whether that happens depends on how rows reached the partition, and on
the skip path each row travels as a serialized one-row accumulator. m2's
cells turn the skip off (`partial_skip=never`), so every run takes the
same path; the skip path is not priced.

**Results.** In-memory bench_core at 10M rows (`amount` is uniform in
[0, 10000] at two decimals), streamed. Today's median at one partition,
each prototype's time as a multiple of today's at 1, 2, 4 and 12
partitions, and peak memory at 12 partitions. The smaller sizes are in
[results/m2.md](results/m2.md).

| pipeline | terminal | off s (p=1) | exact × at p=1/2/4/12 | compensated × at p=1/2/4/12 | peak MiB p=12: off/exact/compensated |
|---|---|---|---|---|---|
| sum | stream | 0.004 | 3.92 / 2.89 / 1.85 / 1.18 | 11.85 / 8.16 / 4.83 / 1.88 | 0 / 1 / 1 |
| mean | stream | 0.004 | 4.08 / 2.91 / 1.87 / 1.21 | 12.40 / 8.24 / 4.91 / 1.82 | 0 / 1 / 1 |
| var | stream | 0.047 | 0.88 / 0.87 / 0.88 / 1.41 | 2.27 / 2.20 / 2.17 / 2.18 | 0 / 1 / 1 |
| std | stream | 0.047 | 0.88 / 0.90 / 0.87 / 1.43 | 2.27 / 2.26 / 2.16 / 2.20 | 0 / 1 / 1 |
| cov | stream | 0.050 | 1.44 / 1.41 / 1.42 / 2.03 | 3.22 / 3.13 / 3.05 / 3.09 | 1 / 1 / 1 |
| corr | stream | 0.141 | 0.86 / 0.86 / 0.87 / 1.38 | 1.93 / 1.91 / 1.90 / 1.96 | 1 / 1 / 1 |
| stats_by_category | stream | 0.143 | 1.73 / 1.74 / 1.76 / 1.86 | 1.34 / 1.33 / 1.37 / 1.33 | 0 / 0 / 0 |
| stats_by_id_mod | stream | 0.171 | 3.13 / 4.01 / 3.94 / 3.99 | 1.32 / 1.51 / 1.54 / 1.65 | 192 / 553 / 364 |
| sum_by_id_mod | stream | 0.093 | 1.40 / 1.89 / 2.48 / 2.08 | 1.18 / 1.22 / 1.40 / 1.38 | 51 / 122 / 85 |
| rolling_sum | stream | 1.708 | 1.32 / 1.29 / 1.28 / 1.29 | 1.11 / 1.16 / 1.12 / 1.11 | 383 / 382 / 390 |
| rolling_mean | stream | 1.275 | 2.30 / 2.03 / 2.02 / 2.04 | 1.51 / 1.41 / 1.41 / 1.39 | 392 / 384 / 395 |
| rolling_std | stream | 1.384 | 3.74 / 3.38 / 3.27 / 3.28 | 1.59 / 1.53 / 1.47 / 1.50 | 395 / 390 / 384 |
| rolling_mean_1000 | stream | 1.292 | 2.21 / 2.01 / 2.01 / 1.98 | 1.48 / 1.43 / 1.41 / 1.40 | 386 / 391 / 383 |
| cum_sum | stream | 1.177 | 1.01 / 1.02 / 1.00 / 1.00 | 0.92 / 0.93 / 0.92 / 0.93 | 393 / 383 / 389 |

**Reading.** Exact accumulation costs little where today's kernel already
does real work per row, and most where it keeps a small state per group
or per window.

- Global statistics: up to 4 partitions, exact `var`, `std` and `corr`
  take 0.86 to 0.90 of today's time, since Welford's update divides on
  every row, and `cov` 1.4 times. Exact `sum` and `mean` cost 3.9 to 4.1
  times at one partition, but today's takes 4 ms per 10M rows, so that
  is about 12 ms more. At 12 partitions, where today's queries take 3
  to 14 ms, exact costs 1.2 to 2.0 times; between runs these ratios
  moved by up to 0.5.
- Grouped statistics: 1.7 to 1.9 times over 5 categories. Over 100,000
  groups, 3.1 to 4.0 times at 10M rows and 4.3 to 4.9 at 1M, where the
  exact state per group also takes 2.9 times the memory (553 MiB against
  192 at 12 partitions). The prototype keeps a full superaccumulator per
  sum and group. A grouped sum alone costs 1.4 to 2.5 times. The first
  run of this measurement gave 6.2 to 6.5 times over 100,000 groups: its
  accumulator walked every group's heap after every batch to report its
  size, which took a third to a half of the time.
- Rolling windows: 1.3 times for `sum`, 2.0 to 2.3 for `mean` and 3.3
  to 3.8 for `std`. A window of 1,000 rows costs the same as one of 20, so
  retraction does not grow with the window. `cum_sum` costs nothing: the
  window operator, about 120 ns per row today, dominates it. Exact
  `rolling_sum` cost 1.14 to 1.20 times in the two earlier runs, at
  `bfa963a` and `e168139`, and 1.28 to 1.43 in this one and in a second
  run of its cells on the same build. The cause was not found. The
  change between those builds makes the accumulator's allocating paths
  return the bytes they allocate, which the window path ignores;
  `rolling_mean`, which shares the window's update and retraction, did
  not move; and a standalone benchmark of the accumulator, finish
  included, times the old and new code the same.
- `compensated` costs 1.1 to 1.6 times on windows and 1.2 to 1.7 on
  grouped statistics (1.0 to 2.1 at 1M rows). On global aggregates it
  costs more than `exact` (1.8 to 12.4 times today's), probably because a
  double-double addition is a longer dependency chain than a
  superaccumulator update; this was not profiled. The first run's 5.2
  times over 100,000 groups at 2 partitions most likely took the skip
  path; with the skip off it is 1.5.

For OD-W (#276) the trade is exact rolling `mean` and `std` at 2.0 to 3.8
times today's time against compensated ones at 1.4 to 1.6, which are
deterministic for a fixed row order but not exact. The prototype
finishes each exact `mean` and `std` row through a BigInt division or
square root, and this measurement does not separate that cost from
accumulation, so a cheaper exact finish could narrow the gap. Grouped
statistics over many groups, at 3.1 to 4.9 times and nearly 3 times the
memory, are the item most likely to need a budget exception or a leaner
accumulator for M13.

## m3: order-preserving merge (gates M1, #148)

**What it prices.** The contract defines a file-then-row order for every
reader and terminal (§9) and bounds memory independently of the row count
(§21.1). A parallel Parquet scan today splits each file into contiguous
byte ranges, one per partition, and interleaves their batches as they
arrive. Restoring order with bounded memory needs a merge that holds only a
few batches ahead of the consumer.

**Prototypes** (`ordered.rs`). `concat` keeps today's contiguous ranges and
emits them in order, each allowed two batches ahead. `interleaved` splits
the scan into one unit per row group, runs as many consecutive units at a
time as there are partitions (the window) and emits them in unit order,
each allowed a few batches ahead (the buffer, two unless stated). Both
turn off DataFusion's file-stream work stealing and round-robin
repartitioning, which reorder rows.

**Results.** The R3 input (filter, select, derive over 640 MiB in 8
Parquet files), streamed. Each cell gives the median time as a multiple
of today's, then peak memory in MiB. The first column is today's peak in
#251's baseline comment, measured by the R3 harness from before the
reader's call; this harness starts after the warm-up, so its peaks can be
a few MiB lower.

| Partitions | Baseline comment | Today | `concat` | `interleaved` | buffer 4 | buffer 8 |
|---|---|---|---|---|---|---|
| 1 | 3.5 | 0.226 s, 3.1 | 1.00, 2.8 | 1.01, 4.7 | | |
| 2 | 9.9 | 0.117 s, 9.1 | 1.95, 6.7 | 1.06, 12.1 | 1.06, 12.9 | 1.06, 9.2 |
| 4 | 14.9 | 0.062 s, 15.3 | 3.59, 11.5 | 1.31, 20.6 | 1.17, 21.4 | 1.18, 19.7 |
| 12 | 37.9 | 0.053 s, 44.3 | 4.06, 34.1 | 1.75, 63.2 | 1.13, 67.4 | 1.09, 51.0 |
| 24 | 67.0 | 0.061 s, 67.6 | 3.48, 66.0 | 1.59, 106.1 | 1.01, 110.8 | 0.97, 82.3 |

Twice as many units in flight: at 12 partitions 1.84 (115 MiB) with a
buffer of 2 and 1.15 (106 MiB) with 4; at 24, 1.75 (181 MiB) and 1.09
(179 MiB). Today's peak at 12 partitions was 39.5 MiB in the previous
run, against 44.3 here.

At the default batch size an R3 row group filtered to 75% leaves six
batches, so a buffer of 8 holds a whole unit and never stops one. A
check with 4,096-row batches, where a unit is 24 batches and every
buffer binds ([results/checks/](results/checks/)), left today's time
unchanged (0.071, 0.054 and 0.056 s at 4, 12 and 24 partitions). The
row-group merge cost 1.96, 2.83 and 2.85 times there with a buffer of 2,
and 1.42, 2.11 and 2.11 with a buffer of 8. Contiguous ranges cost 3.5 to
4.3 times.

Other readers were measured without the buffer sweep. On ClickBench's
sample, 11 row groups for up to 12 partitions, the row-group merge costs
0.77 to 1.20 times on a filter and up to 1.66 on a full scan
(`hits_scan` at 12 partitions). bench_core's CSV has no row groups, so
only contiguous ranges were measured there: 1.85, 3.09 and 2.71 times at
2, 4 and 12 partitions.

Writing Parquet, every mode took 0.94 to 1.10 times today's time;
`concat` held 65 and 96 MiB at 12 and 24 partitions against today's 104
and 171. `count` runs only today's plan: 1.0, 4.3, 7.9, 22.5 and 24.2 MiB
at 1, 2, 4, 12 and 24 partitions (baseline comment: 3.6, 4.7, 9.0, 23.3,
24.1). With the first twelfth or quarter of the data delayed 20 ms per
batch, today's first row arrived after 3 to 4 ms and both merges' after
24 to 26 ms. The ClickBench sample (11 row groups) and bench_core's CSV
are in [results/m3.md](results/m3.md).

**Reading.** Order can be restored with bounded memory at a small cost,
but not by concatenating today's ranges.

- `concat` runs the scan at one partition's speed: while the first range
  drains, every later range stops after its buffer fills. It costs 1.95
  times at 2 partitions and 3.5 to 4.1 times above (1.85 to 3.1 on CSV).
- The row-group merge with two batches per unit costs 1.06 times at 2
  partitions, 1.31 at 4 and 1.6 to 1.75 at 12 and 24. A buffer of 8
  batches brings it to 1.09 at 12 and 0.97 at 24, for 7 to 15 MiB more
  than today, but 1.18 remains at 4 partitions, where wider windows were
  not tried. Those figures hold only while the buffer holds a whole unit.
  Holding a third of one, it costs 1.6 to 1.75 times at 12 and 24
  partitions (a buffer of 2 at the default batch size) and 2.1 (a buffer
  of 8 at 4,096 rows). So the merge stays near today's time by buffering
  whole row groups' output, and its memory then grows with the row
  group's output, not with a count of batches. The check does not
  separate the binding from the four times as many batches the merge
  hands on at 4,096 rows. More units in flight do not help, and cost
  memory: about 110 MiB with 24 units and 180 with 48, at buffers of 2
  and 4. At every window tried the buffer of 8 had the lowest peak of the
  three, as it did at 12 and 24 partitions in the previous run; this
  measurement does not explain why.
- An ordered stream cannot start before the first unit does, so a slow
  start delays the first row (24 to 26 ms against 3 to 4 above). Today's
  early row comes from another partition, out of order. At 12 partitions
  the row-group merge finished the delayed scan in 0.60 of today's time,
  because it reads the slow region's row groups in parallel where today
  one partition reads them all. That gain comes from the split, not the
  order.
- Writing Parquet is bound by the writer, so ordering costs nothing there,
  and `concat` holds about 40% less memory.

For M1 the open choices are how the buffer is bounded (by a row group's
output, which keeps the cost near today's on R3, or by a count of
batches, which costs 1.4 to 2.1 times once it binds), whether about 1.2
times at 4 partitions fits the budget, and what the other inputs cost:
up to 1.66 times on a Parquet file with fewer row groups than
partitions, where no buffer was swept, and 1.85 to 3.1 on CSV, where
only contiguous ranges were measured.

## m4: masked evaluation of `&` and `|` (gates M16, #252)

**What it prices.** Decision D10 makes `a & b` FALSE wherever either
operand is FALSE, and `a | b` TRUE wherever either is TRUE, even where the
other operand would raise. DataFusion's `BinaryExpr` evaluates the left
operand on every row and pre-selects rows for the right one only when the
left has no NULL and leaves at most 20% of rows open. Today, whether
`fallible & guard` raises depends on the operand order and on the data
(#252).

**Prototypes** (`masked.rs`; checked arithmetic is on in every mode, so
products can overflow). `select` evaluates the infallible operand first and
the fallible one only on the rows it leaves open; with two fallible
operands it keeps the first one's errors as a mask. `errors` evaluates
every operand on every row, keeps errors as masks and raises only for a
row the other operand did not decide. `optimistic` evaluates as today and
redoes a batch as `errors` only when it raises.

Each prototype handles one `&` or `|`, which is all an m4 pipeline has.
Nested ones are neither supported nor priced. In
`(g > 0) & (a1 * a2 > 0) & (b1 * b2 > 0)`, on a row where `g > 0` is
TRUE, `a1 * a2` overflows and `b1 * b2 > 0` is FALSE, the inner `&`
raises in every mode, though the outer FALSE should make the row FALSE.

The input puts each row in one of three zones: one where every operand is
TRUE, holding the guard's share of rows, and two where one product is
FALSE and, with failing rows on, the other overflows. With failing rows
on, D10 decides whether the query raises.

The operand order in a pipeline's name is the order its plan runs.
DataFusion's optimizer reorders the operands of `&`: it moves a conjunct
without function calls ahead of one with them, and checked arithmetic is
a function call. Written as `(a1 * a2 > 0) & (g > 0)`, the product would
run first in today's unchecked plan and second in every checked one. So
`fallible_and_guard` writes its guard as `abs(g) > 0`, also a function
call and the same predicate on this input (`g` is 0 or 1), which keeps
the written order in every mode. The self-check asserts the executed
order of both `&` forms in every mode. `|` is not reordered.

**Results.** 1M rows at 12 partitions. Each cell lists the four shares of
rows on which the fallible operand is demanded (1%, 20%, 50% and 99%):
today's median in milliseconds, then each mode's time as a multiple of
today's unchecked plan, first without failing rows and then with them.
Partition counts 1, 2 and 4 and the smaller sizes are in
[results/m4.md](results/m4.md).

| pipeline | today ms | `select` × | `errors` × | `optimistic` × | today's checked plan, failing rows | `select` ×, failing | `errors` ×, failing | `optimistic` ×, failing |
|---|---|---|---|---|---|---|---|---|
| guard_and_fallible | 1.3 / 3.4 / 4.3 / 5.3 | 1.00 / 1.04 / 1.25 / 1.58 | 1.27 / 0.91 / 0.99 / 0.97 | 0.93 / 0.98 / 1.04 / 0.96 | raises at 20%, 50%, 99% | 0.94 / 1.08 / 1.27 / 1.50 | 1.49 / 0.95 / 1.05 / 1.00 | 1.10 / 1.08 / 1.16 / 1.02 |
| fallible_and_guard | 1.6 / 3.5 / 4.4 / 5.1 | 0.96 / 1.06 / 1.20 / 1.59 | 1.08 / 0.91 / 0.99 / 1.03 | 0.99 / 1.01 / 1.00 / 1.04 | raises at 1%, 20%, 50%, 99% | 1.06 / 1.05 / 1.25 / 1.59 | 1.25 / 0.95 / 1.03 / 1.00 | 1.75 / 1.01 / 1.07 / 1.11 |
| fallible_and_fallible | 1.6 / 3.7 / 4.6 / 5.3 | 1.03 / 1.03 / 1.21 / 1.47 | 1.18 / 0.92 / 1.04 / 1.04 | 0.95 / 0.99 / 0.98 / 1.02 | raises at 1%, 20%, 50%, 99% | 2.25 / 1.51 / 1.49 / 1.55 | 1.60 / 0.93 / 1.05 / 1.01 | 1.92 / 1.04 / 1.10 / 1.05 |
| guard_or_fallible | 4.8 / 5.1 / 4.2 / 1.7 | 1.01 / 1.08 / 1.37 / 2.74 | 1.07 / 0.94 / 1.05 / 0.99 | 1.06 / 1.01 / 1.06 / 0.97 | raises at 20%, 50%, 99% | 1.03 / 1.09 / 1.33 / 2.79 | 1.08 / 0.93 / 1.07 / 1.09 | 1.00 / 0.99 / 1.08 / 1.45 |
| fallible_or_guard | 5.0 / 5.1 / 4.4 / 1.6 | 1.00 / 1.17 / 1.32 / 2.99 | 1.06 / 0.90 / 0.98 / 1.06 | 1.02 / 1.04 / 1.01 / 1.15 | raises at 1%, 20%, 50%, 99% | 0.99 / 1.10 / 1.32 / 3.12 | 1.06 / 0.93 / 1.01 / 1.17 | 1.08 / 0.99 / 1.06 / 1.50 |
| fallible_or_fallible | 5.3 / 5.4 / 4.6 / 1.9 | 0.98 / 1.05 / 1.25 / 2.58 | 1.04 / 0.91 / 1.01 / 0.98 | 0.95 / 0.95 / 0.99 / 1.04 | raises at 1%, 20%, 50%, 99% | 1.23 / 1.21 / 1.48 / 2.64 | 1.04 / 0.90 / 1.05 / 1.36 | 1.06 / 0.99 / 1.14 / 1.61 |

**Reading.** D10 is affordable, and the strategy decides how affordable.

- Today's behaviour is what #252 describes. With failing rows, today's
  checked plan raised in 22 of 24 cells, and whether it raised depended on
  the operand order and on the share. It did not raise in two, both at 1%
  with the guard first (`guard_and_fallible` and `guard_or_fallible`),
  where DataFusion evaluates the product only on the rows the guard
  leaves open. With the fallible operand first it raised at every share,
  for `&` as for `|`. All three prototypes returned the rows today's
  unchecked plan returns, in every cell.
- `errors` costs nothing measurable without failing rows (0.90 to 1.18,
  and 1.27 in one cell that takes 1.3 ms) and at most 1.6 times with them.
  Every cell above 1.1 is one where today's plan takes under 2 ms.
- `optimistic` also costs nothing without failing rows (0.93 to 1.15),
  and up to 1.9 times when a batch raises and is evaluated again.
- `select` costs more as more rows are left open, because it filters,
  evaluates and scatters back: 0.94 to 1.23 at 1% and 20% (2.25 and 1.51
  for `fallible_and_fallible` with failing rows), 1.2 to 1.5 at 50%, 1.5
  to 1.6 for `&` and 2.6 to 3.1 for `|` at 99%.

These queries take 1 to 6 ms, so a single ratio can move by about 0.2.
m4 ran at `bfa963a` before; the cells whose plan did not change moved by
0.04 at the median and 0.27 at most between the two runs. At 1, 2 and 4
partitions with a 50% share, `select` is again the costliest (1.01 to
1.46) and the other two are within noise, apart from one `optimistic`
cell at 0.72. At 10K and 100K rows every mode is within about 0.2 of
today, except `select` and `errors` on `fallible_or_fallible` at 100K
rows (1.37 and 1.24, in a cell that takes 1.2 ms).

## m4b: filter pushdown lost past sort, aggregate and distinct (gates M16)

**What it prices.** Decision D10a makes a sort, grouping, distinct or
aggregate demand its keys and arguments on every input row (contract
§20.2), and a rewrite may not move a filter to where a fallible key would go
unevaluated. DataFusion moves such a filter below the operator and into the
Parquet scan, where a predicate on a sorted column skips row groups by
their statistics. The reassessment (hazard X4) asked for this to be priced
before M16.

**Prototype** (`pushdown.rs`). A no-op barrier node that filter pushdown
does not cross, logically or physically, in two placements:

- `operator`: above the operator, so the filter never crosses it. An upper
  bound. Above a sort the filter reads the sort's merged output in one
  partition: DataFusion would otherwise scatter that output round-robin
  for the filter's sake, and the rows would leave unsorted (a #148 case).
  Filtering each sorted partition before the merge would cost less; it
  was not measured.
- `keys`: between the operator and its input, so the filter crosses the
  operator but stays above the projection that computes the keys. This is
  what D10a requires when a key or argument can fail.

The barrier goes in whatever the keys compute, so these timings price an
implementation that treats every checked operation as fallible. None of
the measured keys can fail. Each adds an `int` literal, which is `int64`
(§17.2), to an `int32` column (`counterid` or `regionid`), and no `int32`
value plus one overflows `int64`. A rule that reads only the operand
types would keep today's pushdown in all four pipelines. Checked
arithmetic (m1) is on in every mode, so only the pushdown differs.
A control, `scan_filter_off`, keeps today's pushdown but turns off the
scan's row filter and pruning, the four Parquet options m5's `prune_off`
turns off (`pruning=off`). Unlike `prune_off` it leaves DataFusion's
file-stream work stealing as today's plan has it, so only the scan's
filtering differs from `pushdown`. On the sample file no
row group's statistics exclude the filter's value, so the control
separates the cost of evaluating the filter inside the scan from the
barrier's placement.

**Results.** The first three pipelines read ClickBench's sample and keep
the 20% of rows with `regionid = 229` (2,127 regions in all), which
statistics cannot prune. `user_totals` reads hits_sorted and filters on
its first sort key, which leaves one of 814 row groups today. Today's
median at one partition, each mode's time as a multiple of today's at 1,
2, 4 and 12 partitions, and peak memory at one partition:

| pipeline | terminal | pushdown s (p=1) | operator × at p=1/2/4/12 | keys × at p=1/2/4/12 | scan_filter_off × at p=1/2/4/12 | peak MiB p=1: pushdown/operator/keys/scan_filter_off |
|---|---|---|---|---|---|---|
| sort_then_filter | stream | 0.127 | 1.79 / 1.58 / 1.82 / 2.26 | 0.79 / 0.79 / 0.89 / 0.96 | 0.78 / 0.80 / 0.87 / 0.96 | 111 / 170 / 53 / 53 |
| group_then_filter | stream | 0.015 | 0.98 / 1.05 / 0.88 / 0.87 | 0.80 / 0.80 / 0.76 / 0.79 | 0.72 / 0.81 / 0.83 / 0.81 | 5 / 4 / 4 / 4 |
| distinct_then_filter | stream | 0.020 | 1.16 / 1.32 / 1.17 / 1.41 | 0.73 / 0.81 / 0.85 / 0.90 | 0.73 / 0.78 / 0.87 / 0.85 | 2 / 5 / 1 / 1 |
| user_totals | stream | 0.003 | 1441.28 / 832.53 / 508.14 / 307.87 | 265.20 / 97.08 / 59.41 / 74.17 | – / – / – / – | 38 / 1328 / 38 / – |

`user_totals` in seconds: today 0.003 to 0.004 at every partition count;
`keys` 0.73, 0.38, 0.24 and 0.31; `operator` 4.0, 3.3, 2.0 and 1.3, with
1.3 to 2.0 GiB peak. Today's first run takes 0.034 s at every partition
count, ten times its median, because it reads the footer. By first runs,
`keys` costs 22, 12, 7.5 and 9.5 times today's time and `operator` 117,
94, 61 and 39 times. Against the run at `bfa963a`, the ratios of cells
whose plan did not change moved by 0.04 at the median and 0.22 at most
on the sample file, and by under 11% on `user_totals`, except its `keys`
cell at 12 partitions (16%), which other processes contended.

At one partition the sort's ratios depend on the allocator as much as on
the placement. Today's plan took 0.126 and 0.127 s in the two full runs,
but in a 15-run check it dropped from 0.13 to 0.09 s after its fourth
run, and with glibc's mmap threshold fixed every mode stayed at the
slower level ([results/checks/](results/checks/)). Across these runs
`operator` costs 1.6 to 2.2 times at one partition, and `keys` and the
control 0.77 to 1.13 (1.72, 0.96 and 0.99 with the threshold fixed).
This was not traced further. At 2, 4 and 12 partitions the 15-run check
agrees with the table within 0.16.

**Reading.** The placement decides the price, so what this informs is how
M16 places the filter, not whether D10a is affordable.

- With the barrier above the operator, the sort costs 1.6 to 2.3 times
  today's time and 60 MiB more: it sorts all 1M rows instead of the 20%
  the filter keeps, and merges all of them into the one partition the
  filter reads. That merge and the filter do not run in parallel, so its
  time stops falling after 4 partitions (0.098 and 0.097 s at 4 and 12)
  and the ratio is highest at 12. Distinct costs 1.16 to 1.41 times.
  Grouping costs nothing, because the aggregate reduces the rows to one
  per region before the filter. `user_totals` aggregates 100M rows instead
  of one row group: 310 to 1,440 times today's time, and gigabytes of
  memory.
- With the barrier between the operator and its input, which is what
  D10a requires when a key is fallible, the sample-file pipelines take
  0.73 to 0.96 times today's time (the one-partition sort aside, above).
  The control matches `keys` within 0.08 in every cell of the table, so
  the gain is not the barrier's: on this file, a filter that keeps 20% of
  rows costs more inside the Parquet scan than after it. The real cost is
  the lost row-group pruning: `user_totals` reads every row group, 59 to
  265 times today's time on a repeated lookup (0.24 to 0.73 s against 3
  to 4 ms) and 7.5 to 22 times on a first one, with the same memory
  except at 12 partitions (118 MiB against 40).

The lost pruning is the same order as m5's (D7), but D10a requires it
only where a key or argument can fail, and none of these can. Their
operand types show that without reading any data. Statistics would
extend the proof to keys whose type allows overflow but whose values do
not (hits_sorted bounds `counterid` at 262,115), but only with the bounds
of every row group the filter would skip. M16 has neither analysis, and
which to build is part of deciding the placement. A key that can fail,
such as the sum of two `int64` columns, gets the same barrier over the
same scan, since the barrier does not look at the keys, so its cost
should be close to these figures; that was not measured.

## m5: `assume_sorted` validated without pruning (gates M3)

**What it prices.** Decision D7 (contract §9.5) checks each adjacent pair
of an `assume_sorted` declaration when its later row is read, across batch,
file and partition boundaries. It applies no statistics pruning or
pushdown below the declaration, so no pair goes unchecked. v0.4 trusts the
declaration.

**Prototypes** (`validate.rs`). `prune_off` turns off four Parquet
options, row-group statistics pruning, the scan's row filter, the page
index and bloom filters, without checking: D7's read on its own.
`validate` adds the check, which
compares adjacent rows' keys in arrow's row format; the last partition to
finish checks the pairs between partitions.

m5 reads one file, and `validate` refuses a scan of several, so these
cells say nothing about checking across files. Across files a partition
need not follow the one before it in file order, and none of the four
options stops DataFusion's file opener from skipping a
whole file by its statistics. On one file that skips every row or none,
and each pipeline below matches rows. M3 needs both: partitions in
file-then-row order, and that file pruning off.

**Results.** hits_sorted (100M rows, declared sorted by `(userid,
eventtime, watchid)`), streamed. `user_events` filters on one user, whose
659 rows lie in one row group; `counter_events` filters on `counterid`,
which statistics cannot prune (738,172 rows); `keys` reads the three key
columns whole. Today's median at one partition, each mode's time as a
multiple of today's at 1, 2, 4 and 12 partitions, and peak memory at 12
partitions:

| pipeline | terminal | off s (p=1) | prune_off × at p=1/2/4/12 | validate × at p=1/2/4/12 | peak MiB p=12: off/prune_off/validate |
|---|---|---|---|---|---|
| user_events | stream | 0.013 | 566.35 / 282.38 / 161.22 / 155.95 | 604.18 / 302.26 / 169.34 / 164.51 | 22 / 360 / 363 |
| counter_events | stream | 1.076 | 1.18 / 1.17 / 1.17 / 1.24 | 1.74 / 1.76 / 1.67 / 1.42 | 149 / 120 / 125 |
| keys | stream | 1.031 | 1.00 / 0.98 / 1.00 / 1.00 | 1.60 / 1.58 / 1.51 / 1.18 | 142 / 146 / 159 |

`user_events` in seconds: today 0.013 to 0.016; `validate` 8.01, 4.39,
2.61 and 2.55. Across the last two runs the ratios moved by 0.05 at the
median, and `user_events`' by under 6%.

**Reading.** D7's cost is almost all lost pruning, and it falls on the
queries a sort declaration exists for.

- A lookup on the first sort key reads one row group today and every row
  group under D7: 156 to 566 times today's time (165 to 604 with the
  check) when repeated, and 70 to 232 (73 to 245) as the first query on
  the table, which also reads the footer. It holds 360 MiB instead of 22
  at 12 partitions. The check adds 5 to 7% on top of that read.
- Where statistics prune nothing, D7's read costs 1.17 to 1.24 times on
  `counter_events` and nothing on `keys`, which has no filter. Of the
  four options `prune_off` turns off, only the row filter has anything to
  act on in `counter_events`: the file has no page index, and every row
  group holds `counterid = 62`, so neither statistics nor bloom filters
  can skip one. That reading comes from the file's metadata, not from a
  control, since no cell turned the row filter off alone: the 17 to 24% is
  what the row filter saves, net of the statistics and bloom-filter
  checks that today's plan runs and that skip nothing. The check then
  costs 1.2 to 1.8 times today's time. The ratio falls with the partition
  count because
  each partition checks its own range; on one core the check takes about
  0.6 s per 100M rows of three keys.

Two weaker checks would keep most of the pruning: checking only the row
groups the scan reads, or also checking each skipped row group's
statistics against its neighbours. Neither was prototyped, and neither
checks every adjacent pair, so whether either is acceptable is a
contract question for D7, not a measurement.

## m6: `string_view` normalized to `string` (gates M18)

**What it prices.** DataFusion 55's Parquet scan decodes strings as
`string_view`; the contract shows plain types wherever a schema is visible.

**Prototypes** (`views.rs`). `plain` turns `schema_force_view_types` off, so
the scan decodes plain strings. `cast` keeps views through the plan and casts
at its root, as a terminal would.

**Results.** ClickBench sample (1M rows). Today's median at one partition,
each prototype's time as a multiple of today's at 1, 2, 4 and 12
partitions, and peak memory at one partition (the most repeatable):

| pipeline | terminal | off s (p=1) | plain × at p=1/2/4/12 | cast × at p=1/2/4/12 | peak MiB p=1: off/plain/cast |
|---|---|---|---|---|---|
| strings | stream | 0.411 | 1.16 / 1.00 / 1.06 / 1.13 | 1.27 / 1.24 / 1.37 / 1.45 | 95 / 72 / 67 |
| strings | to_arrow | 0.528 | 0.81 / 0.81 / 0.94 / 0.95 | 1.03 / 1.03 / 1.23 / 1.38 | 727 / 478 / 461 |
| url_title | stream | 0.267 | 1.00 / 0.98 / 1.05 / 1.19 | 1.12 / 1.00 / 1.14 / 1.14 | 57 / 41 / 40 |
| url_title | to_arrow | 0.326 | 0.91 / 0.98 / 0.99 / 1.23 | 0.94 / 1.01 / 1.17 / 1.30 | 215 / 212 / 200 |
| all_columns | stream | 0.572 | 1.13 / 1.07 / 0.98 / 1.09 | 1.32 / 1.24 / 1.26 / 1.32 | 120 / 98 / 93 |
| all_columns | to_arrow | 0.589 | 1.02 / 1.10 / 1.12 / 1.06 | 1.19 / 1.14 / 1.19 / 1.26 | 986 / 743 / 736 |
| filter_contains | stream | 0.086 | 0.93 / 1.16 / 1.02 / 1.29 | 0.85 / 0.83 / 0.88 / 1.04 | 37 / 29 / 25 |
| filter_contains | to_arrow | 0.115 | 0.79 / 0.69 / 0.85 / 1.26 | 0.68 / 0.61 / 0.72 / 1.01 | 97 / 30 / 25 |
| group_by_url | stream | 0.172 | 0.98 / 1.20 / 1.07 / 1.18 | 1.02 / 0.99 / 0.99 / 1.11 | 89 / 87 / 88 |
| group_by_url | to_arrow | 0.172 | 0.97 / 1.20 / 1.08 / 1.10 | 1.09 / 0.92 / 0.96 / 1.00 | 89 / 87 / 108 |

**Reading.** Neither placement is free, and neither is cheaper everywhere.
The pattern held in each of three runs. Across them a cell's ratio moved
by 0.08 at the median and 0.17 at the 90th percentile; the largest move
was `filter_contains` streamed at one partition with plain strings (1.38,
1.38, then 0.93).

- Casting at the root costs 1.1 to 1.45 times today's time wherever many
  strings reach the terminal (`strings`, `all_columns`; 1.03 in two
  cells): every string is copied once more after the scan decoded it as a
  view. After a selective filter it costs today's time or less (0.6 to
  1.04), because it copies only the surviving strings.
- Decoding plain strings at the scan costs up to 1.3 times where the plan
  discards or hashes most strings (`filter_contains`, `group_by_url`),
  most consistently at 12 partitions (1.1 to 1.3): decoding a view does
  not copy the string's bytes, and the plain decode copies every one.
  Exporting a string-heavy result costs 0.8 to 1.12 times today's time,
  except `url_title` at 12 partitions (1.2 to 1.3 in every run).
- Both use less memory than today when a result is exported from one
  partition. A view keeps the decoded page buffer it points into alive,
  so `to_arrow` of the string columns holds 727 MiB today against 461 to
  478 MiB with either prototype, and the filtered export holds 97 MiB
  against 25 to 30. The saving shrinks as partitions are added: at 12
  partitions the string columns' export holds 0.86 to 0.97 of today's
  peak, the filtered export 0.88 to 1.08, and `url_title`'s 1.27 to 1.32,
  above today's from 2 partitions on.

For M18 the choice is which pipelines pay about 1.3 times. A placement
that decodes plain strings when the plan keeps most of them and casts at
the root after a selective filter would avoid both costs. It was not
prototyped, and it needs an estimate of selectivity before execution.

## m7: decimal `/` and `%` through a 256-bit intermediate (gates M6, #306)

**What it prices.** Contract §17.3 gives decimal `/` the type
`decimal(min(p1 − s1 + s2 + s, 38), s)` with `s = min(s1 + 4, 38)` and
truncates the quotient at that scale; `%` takes the larger scale and the
divisor's sign. Both raise `ArithmeticOverflowError` only when the result
does not fit. arrow-rs rescales an operand in `i128` before it divides, so
it raises where the result fits (#276's N5): `decimal128(38, 10)` 1e15 `/`
`decimal128(38, 10)` 1 multiplies 1e25 by 10¹⁴, and `decimal128(38, 0)`
10³⁷ `%` `decimal128(38, 10)` 7 multiplies 10³⁷ by 10¹⁰. M6 needs a wider
intermediate, either on every row or only on the rows whose rescaling
overflows.

**Prototypes** (`decimal.rs`). Both rewrite `decimal128` `/` and `%` into a
function that computes the contract's value: the quotient truncated, the
remainder floored, a zero divisor raising `DivisionByZeroError` and a
result beyond the precision raising. `wide` computes every row in arrow's
`i256`. `guarded` computes a row in `i128` with checked multiplications
and recomputes it in `i256` when one overflows. `i256` always suffices for
`decimal128` operands: a dividend that overflows it has a quotient of more
than 38 digits, which raises. The prototypes keep DataFusion's result type
for `%`, which is narrower than the contract's. This does not change the
work done per row.

**Inputs** (`inputs.decimals`). No benchmark file has a decimal column, so
the cells generate them: `a` and divisor `b` as `decimal128(38, 10)`,
`c` as `decimal128(38, 0)`. On a set share of the rows (the wide rows),
`a` lies in [10¹⁵, 10¹⁶) and `c` in [10²⁹, 10³⁰), so `a / b` and `c % b`
overflow arrow's rescaling exactly there. Elsewhere `a` is below 10⁶ and
`c` below 10¹². `b` is in [1, 1000) (`small`: a scaled value of one 64-bit
word, where `i256` division takes a short path) or in [10⁹, 10¹²)
(`large`: two words). `a % b` has equal scales and rescales nothing. It
is the control.

**Results.** 10M rows held in memory, streamed. The columns give today's
median time at one partition, then each prototype's time as a multiple of
today's at 1, 2, 4 and 8 partitions. Today's kernel raises on any wide row.
Rows with 1% or all wide rows are therefore compared with today's time on
the input of the same divisor with no wide rows, which is the work the
operation does when it fits. Peak memory at 10M rows was 0.1 to 0.2 MiB
at one partition and 6 to 11 MiB at 8, in every mode. The 1M-row cells, at 1 and 8 partitions,
show the same pattern ([results/m7.md](results/m7.md)).

| pipeline | divisor | wide rows | off s (p=1) | wide × at p=1/2/4/8 | guarded × at p=1/2/4/8 |
|---|---|---|---|---|---|
| `a / b` | small | none | 0.345 | 1.98 / 2.02 / 1.65 / 2.14 | 0.98 / 1.00 / 0.87 / 1.07 |
| `a / b` | small | 1% | raises | 1.99 / 2.02 / 1.74 / 1.96 | 0.99 / 1.04 / 0.91 / 1.10 |
| `a / b` | small | all | raises | 2.39 / 2.40 / 2.00 / 2.43 | 2.39 / 2.40 / 2.03 / 2.46 |
| `a / b` | large | none | 0.385 | 1.96 / 2.00 / 1.88 / 2.00 | 1.04 / 1.02 / 1.02 / 1.03 |
| `a / b` | large | 1% | raises | 1.96 / 2.00 / 1.91 / 1.98 | 1.04 / 1.04 / 1.00 / 1.03 |
| `a / b` | large | all | raises | 2.52 / 2.57 / 2.35 / 2.53 | 2.54 / 2.60 / 2.35 / 2.53 |
| `c % b` | small | none | 0.211 | 2.74 / 2.62 / 1.61 / 2.79 | 1.12 / 1.13 / 0.69 / 1.21 |
| `c % b` | small | 1% | raises | 2.74 / 2.63 / 1.66 / 2.84 | 1.17 / 1.15 / 0.71 / 1.28 |
| `c % b` | small | all | raises | 3.93 / 3.78 / 2.16 / 3.42 | 3.97 / 3.73 / 2.26 / 3.51 |
| `c % b` | large | none | 0.246 | 1.75 / 1.72 / 1.66 / 1.83 | 1.13 / 1.12 / 1.42 / 1.20 |
| `c % b` | large | 1% | raises | 1.84 / 1.76 / 1.70 / 1.96 | 1.20 / 1.20 / 1.22 / 1.31 |
| `c % b` | large | all | raises | 4.10 / 3.89 / 3.75 / 3.91 | 4.14 / 3.97 / 3.81 / 3.46 |
| `a % b` (control) | small | none | 0.182 | 2.64 / 2.45 / 2.17 / 2.56 | 1.13 / 1.06 / 1.37 / 1.29 |
| `a % b` (control) | large | none | 0.217 | 1.64 / 1.68 / 1.26 / 1.78 | 1.16 / 1.18 / 0.97 / 1.18 |

At 4 partitions a cell's runs were bimodal. Today's `c % b` took 0.07 s in
two runs and 0.11 s in three, so its ratios at 4 partitions (0.69, 1.42)
are noise. The cells at 1, 2 and 8 partitions are steady. At one partition
today's `/` takes 34 to 38 ns a row and `%` 21 to 25. `wide` takes 68 to 75
and 43 to 58. With every row wide, both prototypes take 82 to 102 ns.

**Reading.** The guarded fallback dominates: it costs today's time when no
row needs 256 bits and `wide`'s time when every row does.

- With no wide rows, `guarded` `/` takes 0.98 to 1.07 of today's time,
  leaving out the bimodal 4-partition cells. `wide` costs about 2 times for
  `/`, whether the divisor fills one word or two, and 1.6 to 2.8 times for
  `%`.
- A 1% share of wide rows costs the same as none, in both modes.
- With every row wide, the two modes take the same time: the failed `i128`
  attempt costs nothing measurable. That is 2.0 to 2.6 times today's time
  on rows that fit for `/`, and 2.2 to 4.1 times for `%`, where today's
  `i128` remainder is cheaper. Today raises on these rows.
- `guarded` `%` costs 1.1 to 1.3 times today's with no wide rows, and so
  does the control, which never rescales and never falls back. That cost
  comes from what the prototype computes on every row, which today's
  kernel does not: the floored remainder's sign fix-up, the precision
  check and an error check per row. The guard adds nothing measurable to
  it. The measurement does not separate those three.
- `wide` costs least against today's `%` with a two-word divisor (1.6 to
  1.8 times): today's `i128` remainder by such a divisor is itself slower.

For M6: the guarded fallback is within 10% of today for `/`. `%` costs
1.1 to 1.3 times, from the contract's floored remainder and checks, not
from the wider intermediate. A row that needs 256 bits costs 2 to 4 times
a row that does not, against today's error. #306 asked whether `%` should
share `/`'s intermediate or get a remainder kernel of its own. Sharing it
costs nothing measurable, because the control without rescaling costs the
same, so a separate kernel would have little to gain.

## m8: exact `quantile` and `median` (gates M13, #306)

**What it prices.** Contract §14.2 makes `quantile(q)` the exact value of
*x*ⱼ + *t*(*x*ⱼ₊₁ − *x*ⱼ), with *h* = *q̂*(*n* − 1) for the decimal *q̂* that
*q* was written as, rounded once. Every input takes part at its exact
value: `median([1, 2**53 + 1])` is `4503599627370497.0`. DataFusion 55's
`percentile_cont` coerces an integer input to `float64`, computes *h* in
`float64` and interpolates there with *t* cut to six decimals. Its
`median` returns the input type. LTSeq's `percentile` today is the
t-digest `approx_percentile_cont`. §21.1 lets `quantile` hold its input,
so the cost to price is time: selecting the two ranks, and the exact
interpolation, which runs once per group.

**Prototypes** (`quantile.rs`). A rule replaces `median` over `float64`
and `approx_percentile_cont` (LTSeq's `percentile`) with one of:

- `cont`: DataFusion's `percentile_cont`, the baseline #306 names, given
  no `WITHIN GROUP` ordering, which would make the aggregate demand sorted
  input. The ratios below are against it.
- `select`: the prototype's accumulators, which hold every value as
  `percentile_cont`'s do. It selects *x*ⱼ with `select_nth_unstable` and
  takes *x*ⱼ₊₁ as the least value above it, where DataFusion selects a
  second time. It interpolates in `float64`. This is a control, not the
  contract: it separates the selection's cost from the interpolation's.
- `exact`: the same selection, *j* and *t* from *q̂* in integer
  arithmetic, and the interpolation in `BigInt`, rounded once with m2's
  rounding. An `int64` input stays `int64`. NaN and infinities follow
  §14.2.

`off`, today's `median` and t-digest, is given for reference. Not
prototyped: `median` over an integer type (DataFusion's returns the input
type, which a rule may not change), decimal inputs, `DISTINCT` and windows.
The first measurement is global and grouped only, one of the two options
#306 left open.

**Results.** bench_core in memory at 10M rows, streamed, with partial
aggregation never skipped (as in m2). `percentile_cont`'s median time at
1, 2, 4 and 8 partitions, then each mode's time as a multiple of it, and
peak memory at one and at 8 partitions. Over 100,000 groups each group
holds 100 values. 1M rows show the same pattern with more noise
([results/m8.md](results/m8.md)).

| pipeline | cont s at p=1/2/4/8 | off × at p=1/2/4/8 | select × | exact × | peak MiB p=1: off/cont/select/exact | p=8 |
|---|---|---|---|---|---|---|
| `median` (`float64`) | 0.087 / 0.110 / 0.102 / 0.110 | 0.75 / 0.81 / 0.81 / 0.89 | 0.81 / 0.87 / 0.86 / 0.89 | 0.81 / 0.89 / 0.86 / 0.95 | 76 / 76 / 76 / 76 | 89 / 90 / 93 / 90 |
| `percentile(0.9)` (`float64`) | 0.076 / 0.103 / 0.094 / 0.105 | 3.49 / 1.31 / 0.76 / 0.57 | 0.86 / 0.90 / 0.89 / 0.95 | 0.89 / 0.88 / 0.92 / 0.95 | 0 / 76 / 76 / 76 | 0 / 92 / 87 / 86 |
| `percentile(0.9)` (`int64`) | 0.083 / 0.106 / 0.095 / 0.101 | 2.37 / 0.97 / 0.66 / 0.49 | 0.70 / 0.79 / 0.80 / 0.86 | 0.70 / 0.78 / 0.79 / 0.87 | 0 / 76 / 76 / 76 | 0 / 92 / 90 / 89 |
| `median` by 5 categories | 0.176 / 0.211 / 0.132 / 0.124 | 0.96 / 0.87 / 0.98 / 0.94 | 0.94 / 0.95 / 1.05 / 1.04 | 0.97 / 0.83 / 1.10 / 1.04 | 77 / 81 / 77 / 77 | 233 / 208 / 207 / 277 |
| `median` by 100,000 groups | 0.362 / 0.381 / 0.307 / 0.319 | 0.94 / 0.90 / 1.01 / 1.01 | 0.97 / 0.99 / 0.95 / 0.96 | 0.98 / 0.97 / 0.98 / 1.01 | 103 / 104 / 104 / 104 | 246 / 250 / 248 / 243 |
| `percentile(0.9)` (`int64`) by 100,000 groups | 0.390 / 0.356 / 0.334 / 0.330 | 20.86 / 11.95 / 9.26 / 7.49 | 0.84 / 0.99 / 0.87 / 0.87 | 0.86 / 0.97 / 0.94 / 0.94 | 161 / 104 / 104 / 104 | 919 / 280 / 263 / 233 |

m8 ran twice, before and after a fix to how the prototype's global
accumulator hands on its state. Before the fix it copied every value and
held about 50 MiB more than `percentile_cont` at 8 partitions. The tables
are from the second run. Between the two runs, the cells the fix did not
touch (`off` and every grouped cell) moved by 0.03 to 0.12 in their
ratio at one and two partitions, and by up to 0.28 at 4 and 8, most in
the grouped cells. Read a ratio within 0.15 of 1 as no difference, and
within 0.3 for a grouped cell at 4 or 8 partitions.

**Reading.** An exact quantile costs no more than DataFusion's
`percentile_cont`, and the exact interpolation costs nothing measurable.

- Selection: the prototype (`select`) takes 0.70 to 0.95 of
  `percentile_cont`'s time globally, and is within noise of it grouped
  (0.84 to 1.05). It selects once where DataFusion selects twice, and an
  `int64` input is not converted to `float64` first (0.70 to 0.86).
- Interpolation: `exact` and `select` differ by at most 8%, either way,
  in every 10M-row cell but one (0.87). Over 100,000 groups at one
  partition, where every group's interpolation runs on one thread,
  `exact` took 3.6 to 9.4 ms
  more than `select`, about 0.1 µs a group, inside the noise. #306's
  assumption holds: the interpolation's cost is small next to the
  selection's. Every 100-value group interpolates (*t* > 0), so this is the
  case that exercises the `BigInt` path most.
- Against today's `median` the exact one costs 1.06 to 1.10 times
  globally and 0.96 to 1.12 grouped. Against today's `percentile`, the
  t-digest, it is 3 to 4 times faster at one partition and 1.2 to 1.8 times
  slower at 4 and 8. Over 100,000 groups it is 8 to 24 times faster, and
  the t-digest holds 919 MiB at 8 partitions against the exact one's 233.
- Memory matches `percentile_cont`'s: every value is held, 8 bytes each,
  about 76 MiB for 10M values at one partition. The one gap, 277 MiB
  against 208 over 5 categories at 8 partitions, lies within that cell's
  spread across modes and runs (207 to 277 MiB).
- Partition scaling: neither DataFusion's quantile nor the prototype
  gains from partitions. One partition is fastest for the global
  quantiles (0.06 to 0.09 s against 0.09 to 0.11 at 8), most likely
  because every value moves to one final accumulator, which selects on
  one thread; this was not profiled.
  Grouped quantiles gain at most 1.4 times from 1 to 8 partitions.

For M13 the exact quantile needs no budget beyond `percentile_cont`'s
time and memory. One cost is common to both: a quantile holds its input
and does not scale with partitions. Windowed quantiles were not measured.
DataFusion's `percentile_cont` retracts by scanning every value it holds
and selects again for every frame, so a windowed quantile needs its own
measurement before M13 commits to one.

## Questions for the owner

Recording a budget for each gated item (D9) is the owner's step; the
cost ranges above are its inputs. The measurements also leave these
decisions open:

1. **D7 (M3).** A lookup on a sort key costs 156 to 604 times today's
   time under D7 as written when repeated, and 70 to 245 times as the
   first query on a table. Should the contract keep checking every
   adjacent pair, or accept a weaker check that keeps pruning (only the
   row groups read, or the skipped ones by their statistics)? Neither
   alternative was prototyped.
2. **D10a (M16).** Placing the filter between the operator and its input
   costs nothing here except lost pruning on sorted keys: 59 to 265 times
   today's time on a repeated lookup, 7.5 to 22 on a first one, if M16
   treats every checked operation as fallible. None of the measured keys
   can fail, and their operand types alone show it. Should M16 classify
   keys by operand type, so that such keys keep today's pushdown, and
   should it also use statistics where the type allows overflow but the
   values do not?
3. **D10 (M16).** Keeping errors as masks costs nothing measurable without
   failing rows and up to 1.6 times with them. Evaluating the fallible
   operand only on open rows costs up to 3.1 times. Is the mask strategy
   the one M16 should build? These figures are for one `&` or `|`;
   carrying errors through nested ones, which M16 needs, was neither
   built nor measured.
4. **OD-W (#276).** Exact rolling `mean` and `std` cost 2.0 to 3.8 times
   today's time with the prototype's BigInt finish, compensated ones 1.4
   to 1.6. Which does the contract require? And do exact grouped
   statistics over many groups (3.1 to 4.9 times, nearly 3 times the
   memory) get a budget exception, or a leaner accumulator first?
5. **M1.** M1 covers every reader. On R3 the row-group merge is within
   10% of today at 12 and 24 partitions, and 1.18 times at 4, only while
   its buffer holds a row group's whole output; held to 8 batches of
   4,096 rows it costs 1.4 to 2.1 times. On the 11-row-group ClickBench
   sample it costs up to 1.66 times, and on CSV only contiguous ranges
   were measured (1.85 to 3.1 times). Should the budget be set per
   reader, and should a merge whose buffer is bounded in rows or bytes,
   and a CSV schedule, be prototyped before M1 starts?
6. **M18.** Normalize at the scan, at the root, or by estimated
   selectivity (not prototyped)?
7. **M6 (#306).** The guarded fallback costs today's time on `/` when no
   row needs 256 bits, and the same as 256 bits on every row when all do.
   Always-wide arithmetic costs about 2 times on every row. Should M6
   build the guarded fallback, with `%` going through the same
   intermediate as `/`? The 1.1 to 1.3 times on `%` comes from the
   floored remainder and the checks, which either design pays.
8. **M13 (#306).** The exact quantile is priced at `percentile_cont`'s time
   and memory, with the exact interpolation not measurable. Should its
   budget be `percentile_cont`'s, and should windowed quantiles, which were
   not measured, be priced before M13 builds them?

One observation concerns today's engine rather than the contract. LTSeq
turns on the Parquet scan's row filter (`pushdown_filters`). In m4b it
cost 1.04 to 1.4 times on a filter that keeps 20% of rows. In m5, turning
it off together with pruning, the page index and bloom filters cost 17 to
24% on a filter that keeps 0.7%, where the row filter is the only one of
those with anything to act on. #307 asks for it to be measured more
widely.

## Caveats

- One machine, with 6 cores and 12 threads: at 12 partitions two threads
  share each core, and another machine may scale differently. The machine
  was shared; the harness waited for quiet and reran contended cells, and
  cells that stayed contended are flagged in `results/*.md`.
- Warm page cache: every cell read its input from memory. On cold storage
  the plans that lose pruning (m4b `keys` and `operator`, m5) would also
  pay for the I/O today's plan skips, so their ratios understate the cost
  there.
- Prototypes, not implementations. Each module's header says where it
  stops short. The ones that matter most here: m1 checks Int64 only; m4
  keeps errors as masks only for the shapes it measures, none of which
  nests an `&` or `|`; m5 checks one file and needs the keys among the
  scan's columns; m3's row-group merge covers Parquet only; m2's
  per-group state is unoptimized, and m2 does not price the path where
  DataFusion skips partial aggregation.
- Window pipelines (`cum_sum`, `rolling`) run over the declared order on
  one partition whatever the partition count. At two or more partitions
  today's plan materializes the input first (about 385 MiB at 10M rows)
  in every mode, so their peak memory says nothing about the prototypes.
- For a pipeline that returns one row (the aggregates, m4b's grouped
  filters), time to first row equals the total time.
- Peak memory is the resident growth over the first measured run; values
  under about 5 MiB are at its resolution.
- m7 and m8 ran on a container whose 8 CPUs are 6 cores, two of them with
  both threads. At 4 partitions a cell's runs were often bimodal (for
  example 0.07 s and 0.11 s), plausibly as threads did or did not land on
  sibling threads; this was not verified. Their 4-partition ratios move
  more than the others. m7's inputs are generated, and m8 prices `int64`
  and `float64` inputs only.
