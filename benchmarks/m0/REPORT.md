# M0 feasibility report (#251)

PR #250's API review (finding V9) found that the v0.5 contract's costliest
commitments would be priced only after being built. #251 prices them first,
with prototypes, so the owner can record a budget for each gated item before
work starts. This page gives the findings and the decisions they inform; every
cell is in [results/](results/), and [README.md](README.md) says how to rerun
them.

Nothing here is a budget. Recording budgets is the owner's step (#251's third
acceptance criterion), and the questions for it are at the end.

## Setup

| | |
|---|---|
| Machine | AMD Ryzen 5 5600G (6 cores, 12 threads), 27 GiB, Linux 7.2.8, NVMe |
| Build | `maturin develop --release --features m0` (fat LTO), rustc 1.96.1, Python 3.13.15, pyarrow 25.0.1 |
| Commit | `bfa963a` for every measurement except m4b, with a clean worktree. m4b ran again at `15209f4` after its `operator` placement over a sort was fixed, which changed only that placement's three plans over the sort. The branch's other later commits add this report, the results, a test, a stricter self-check, a CI step, README notes and comments, rewrite one loop in m2's state decoding for a newer clippy lint without changing what it does, and drop a blank line in `report.py` |
| Command | `python benchmarks/m0/run.py --only m6,m4b,m3,m5,m1,m2,m4`, one run started 2026-10-17 17:22 EDT; m4b again with `--only m4b`, started 2026-10-18 21:09 EDT |
| Partitions | 1, 2, 4 and 12 (the CPU count) through #274's `pinned_engine`; m3 adds 24 for the baseline comment |
| Data | bench_core's synthetic table at 10K, 100K and 1M rows (m1 and m2 add 10M); ClickBench's 1M-row sample and its 100M-row file sorted by `(userid, eventtime, watchid)` (16 GiB, 814 row groups); the R3 harness's 640 MiB input |

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
draining the terminal; the tables give the median. Peak memory is the
resident growth over the first measured run, as in the R3 harness. TTFR is
the time to the first non-empty batch of the Arrow C stream, which is what
`to_batches` will return.

**Noise.** m1, m3, m4b, m5 and m6 also ran at an earlier commit with the
same prototypes. Between the two runs a cell's ratio to today moved by
0.03 to 0.07 at the median and 0.11 to 0.19 at the 90th percentile; the
large m4b and m5 ratios moved by under 10%; m4b's third run is compared
in its section. Cells that take a few
milliseconds move most. Read a ratio within 0.1 of 1 as no difference,
and within 0.2 for a cell under 10 ms.

**Contention.** Other workloads share the machine. Before each cell the
harness waited until the machine was at most 8% busy. When other
processes kept more than half a core busy during a cell, it ran the cell
again, up to three times, and kept the least contended attempt. 66 of
1,894 cells ran more than once. 9 stayed contended (6 in m2, 2 in m5 and
1 in m4b, with at most one other core busy) and are flagged in
`results/*.md`.

## Summary

Each row gives what the contract's version costs against today on this
machine, from the sections below. Ratios are median times; "today" is
the same build with every prototype off.

| | Prices | Gates | Cost against today | Decision it informs |
|---|---|---|---|---|
| m1 | Checked integer arithmetic | M5, #221 | Within noise for `+ - *` computed as wrapping arithmetic plus an overflow mask; up to 1.3× for a grouped `sum` over 100,000 groups at 10M rows, 1.43× at 1M | The budget only |
| m2 | Exact float statistics | M13, M4, OD-W | 0.9 to 1.9× for global `var`, `std`, `cov` and `corr`, up to 3.9× (about 12 ms per 10M rows) for `sum` and `mean`; 1.7 to 1.8× grouped over a few keys, 6.2 to 6.5× (4.4 to 6.75× at 1M rows) and 2.8× the memory over 100,000 groups; windows 1.2 to 3.9× (compensated 1.1 to 1.6×) | OD-W: exact or compensated windows; a budget exception or a leaner state for many groups |
| m3 | Order-preserving merge | M1, #148 | Row-group merge with 8 batches per unit: 0.97 to 1.09× at 12 and 24 partitions, 1.18× at 4, 7 to 15 MiB more; contiguous ranges 2 to 4× | Buffer size and schedule |
| m4 | Masked `&` and `\|` | M16, #252 | Errors kept as masks: within noise without failing rows, up to 1.5× with them. Today's checked plan raises in 21 of 24 cells with failing rows, depending on operand order | Which strategy M16 builds |
| m4b | Pushdown lost to D10a | M16 | Filter kept above the keys' computation: no cost where statistics cannot prune, 59 to 265× on a lookup by a sorted key; kept above the operator, 1.2 to 2.3× and up to 1,440× | Where M16 places the filter; whether it proves keys infallible |
| m5 | `assume_sorted` checked, no pruning | M3 | The check: 5 to 7% on top of D7's read, 1.2 to 1.8× on full scans; the lost pruning: 156 to 604× on a sort-key lookup | D7 as written, or a weaker check that keeps pruning |
| m6 | `string_view` normalized | M18 | Plain at the scan: up to 1.3× on filters and grouping; cast at the root: 1.1 to 1.45× on string-heavy results; both hold 25 to 35% less memory on export | Where M18 normalizes |

The two large costs are both lost row-group pruning (m4b and m5), and
both come from rules that forbid a filter or a skip below a point the
contract fixes. Apart from pruning, the worst measured costs are exact
statistics: 3.9 times on a rolling `std` and on a global `sum` at one
partition, and 6.5 times grouped over 100,000 groups (6.75 at 1M rows). Everything else
costs at most about 1.5 times with the cheapest prototype measured.

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
contract. Partial aggregates exchange their sums serialized.

**Results.** In-memory bench_core at 10M rows (`amount` is uniform in
[0, 10000] at two decimals), streamed. Today's median at one partition,
each prototype's time as a multiple of today's at 1, 2, 4 and 12
partitions, and peak memory at 12 partitions. The smaller sizes are in
[results/m2.md](results/m2.md).

| pipeline | terminal | off s (p=1) | exact × at p=1/2/4/12 | compensated × at p=1/2/4/12 | peak MiB p=12: off/exact/compensated |
|---|---|---|---|---|---|
| sum | stream | 0.004 | 3.88 / 2.74 / 1.99 / 1.23 | 11.74 / 7.81 / 4.80 / 1.92 | 1 / 1 / 1 |
| mean | stream | 0.004 | 3.85 / 2.81 / 1.87 / 1.24 | 11.67 / 7.92 / 4.88 / 1.90 | 1 / 1 / 1 |
| var | stream | 0.047 | 0.88 / 0.90 / 0.90 / 0.95 | 2.27 / 2.25 / 2.17 / 1.64 | 1 / 1 / 1 |
| std | stream | 0.047 | 0.88 / 0.92 / 0.91 / 1.48 | 2.27 / 2.22 / 2.21 / 2.30 | 1 / 1 / 1 |
| cov | stream | 0.050 | 1.46 / 1.44 / 1.42 / 1.94 | 3.22 / 3.16 / 3.04 / 2.92 | 1 / 1 / 1 |
| corr | stream | 0.141 | 0.86 / 0.87 / 0.89 / 1.37 | 1.93 / 1.91 / 1.89 / 2.01 | 1 / 1 / 1 |
| stats_by_category | stream | 0.145 | 1.76 / 1.75 / 1.73 / 1.84 | 1.34 / 1.33 / 1.28 / 1.33 | 0 / 0 / 0 |
| stats_by_id_mod | stream | 0.174 | 6.25 / 6.53 / 6.23 / 6.34 | 1.31 / 5.17 / 1.54 / 2.28 | 196 / 546 / 332 |
| sum_by_id_mod | stream | 0.097 | 1.58 / 2.59 / 3.37 / 2.72 | 1.13 / 1.24 / 1.46 / 1.37 | 61 / 118 / 86 |
| rolling_sum | stream | 1.691 | 1.19 / 1.18 / 1.18 / 1.16 | 1.12 / 1.10 / 1.11 / 1.10 | 382 / 399 / 393 |
| rolling_mean | stream | 1.290 | 2.25 / 2.04 / 2.10 / 2.00 | 1.47 / 1.39 / 1.42 / 1.39 | 390 / 391 / 390 |
| rolling_std | stream | 1.342 | 3.90 / 3.33 / 3.28 / 3.20 | 1.63 / 1.53 / 1.52 / 1.51 | 385 / 393 / 394 |
| rolling_mean_1000 | stream | 1.297 | 2.22 / 2.03 / 2.03 / 2.01 | 1.47 / 1.41 / 1.40 / 1.41 | 391 / 384 / 389 |
| cum_sum | stream | 1.187 | 1.00 / 0.99 / 1.00 / 1.00 | 0.92 / 0.93 / 0.92 / 0.93 | 389 / 383 / 383 |

**Reading.** Exact accumulation costs little where today's kernel already
does real work per row, and most where it keeps a small state per group
or per window.

- Global statistics: exact `var`, `std` and `corr` take 0.86 to 0.92 of
  today's time up to 4 partitions, since Welford's update divides on every
  row. At 12 partitions a whole query takes 5 to 15 ms and the ratios
  (0.95 to 1.48) are mostly noise. Exact `sum` and `mean` cost 3.9 times
  at one partition and 1.2 at 12, but today's takes 4 ms per 10M rows, so
  that is about 12 ms more. Exact `cov` costs 1.4 to 1.9 times.
- Grouped statistics: 1.7 to 1.8 times over 5 categories; 6.2 to 6.5
  times over 100,000 groups, where the exact state per group also takes
  2.8 times the memory (546 MiB against 196 at 12 partitions). At 1M rows
  the same pipeline costs 4.4 to 6.75 times (6.75 at one partition). The
  prototype keeps a full superaccumulator per group. A grouped sum alone
  costs 1.6 to 3.4 times.
- Rolling windows: 1.2 times for `sum`, 2.0 to 2.25 for `mean` and 3.2
  to 3.9 for `std`. A window of 1,000 rows costs the same as one of 20, so
  retraction does not grow with the window. `cum_sum` costs nothing: the
  window operator, about 120 ns per row today, dominates it.
- `compensated` costs 1.1 to 1.6 times on windows and 1.1 to 2.3 on
  grouped statistics, except 5.2 over 100,000 groups at 2 partitions (in
  each of five runs in that cell; not explained). On global aggregates it
  costs more than `exact` (1.6 to 11.7 times today's), probably because a
  double-double addition is a longer dependency chain than a
  superaccumulator update; this was not profiled.

For OD-W (#276) the trade is exact rolling `mean` and `std` at 2 to 3.9
times today's time against compensated ones at 1.4 to 1.6, which are
deterministic for a fixed row order but not exact. Grouped statistics
over many groups, at 6 to 6.75 times and nearly 3 times the memory, are
the item most likely to need a budget exception or a leaner accumulator
for M13.

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
  not tried. More units in flight do not help, and cost memory: about
  110 MiB with 24 units and 180 with 48, at buffers of 2 and 4. At every
  window tried the buffer of 8 had the lowest peak of the three, as it did
  at 12 and 24 partitions in the previous run; this measurement does not
  explain why.
- An ordered stream cannot start before the first unit does, so a slow
  start delays the first row (24 to 26 ms against 3 to 4 above). Today's
  early row comes from another partition, out of order. At 12 partitions
  the row-group merge finished the delayed scan in 0.60 of today's time,
  because it reads the slow region's row groups in parallel where today
  one partition reads them all. That gain comes from the split, not the
  order.
- Writing Parquet is bound by the writer, so ordering costs nothing there,
  and `concat` holds about 40% less memory.

For M1 the open choices are the buffer per unit, and whether about 1.2
times at 4 partitions fits the budget or needs a different schedule.

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

The input puts each row in one of three zones: one where every operand is
TRUE, holding the guard's share of rows, and two where one product is
FALSE and, with failing rows on, the other overflows. With failing rows
on, D10 decides whether the query raises.

**Results.** 1M rows at 12 partitions. Each cell lists the four shares of
rows on which the fallible operand is demanded (1%, 20%, 50% and 99%):
today's median in milliseconds, then each mode's time as a multiple of
today's unchecked plan, first without failing rows and then with them.
Partition counts 1, 2 and 4 and the smaller sizes are in
[results/m4.md](results/m4.md).

| pipeline | today ms | `select` × | `errors` × | `optimistic` × | today's checked plan, failing rows | `select` ×, failing | `errors` ×, failing | `optimistic` ×, failing |
|---|---|---|---|---|---|---|---|---|
| guard_and_fallible | 1.3 / 3.4 / 4.3 / 5.0 | 1.02 / 1.06 / 1.29 / 1.52 | 1.32 / 0.94 / 1.00 / 1.03 | 1.00 / 1.00 / 1.02 / 1.04 | raises at 20%, 50%, 99% | 0.96 / 1.03 / 1.25 / 1.70 | 1.50 / 0.94 / 1.00 / 1.02 | 0.94 / 1.00 / 1.10 / 1.10 |
| fallible_and_guard | 1.8 / 3.5 / 4.5 / 5.1 | 0.69 / 1.02 / 1.19 / 1.66 | 0.96 / 0.91 / 0.96 / 1.00 | 0.72 / 0.99 / 0.95 / 1.01 | raises at 20%, 50%, 99% | 0.92 / 1.01 / 1.29 / 1.64 | 1.22 / 0.92 / 1.06 / 1.03 | 0.77 / 0.98 / 1.12 / 1.04 |
| fallible_and_fallible | 1.7 / 3.7 / 4.6 / 5.3 | 1.03 / 1.02 / 1.22 / 1.47 | 1.13 / 0.90 / 0.99 / 1.03 | 0.91 / 0.99 / 0.97 / 1.03 | raises at 1%, 20%, 50%, 99% | 2.21 / 1.54 / 1.44 / 1.39 | 1.51 / 0.99 / 0.98 / 0.99 | 1.90 / 1.18 / 1.06 / 1.05 |
| guard_or_fallible | 4.9 / 5.2 / 4.5 / 1.7 | 1.03 / 1.07 / 1.26 / 2.90 | 0.99 / 0.91 / 0.97 / 1.02 | 1.01 / 0.96 / 0.97 / 1.00 | raises at 20%, 50%, 99% | 1.02 / 1.07 / 1.25 / 2.83 | 1.06 / 0.94 / 0.98 / 1.08 | 0.97 / 1.06 / 1.11 / 1.42 |
| fallible_or_guard | 5.1 / 5.3 / 4.4 / 1.8 | 0.98 / 1.01 / 1.24 / 2.78 | 1.01 / 0.92 / 0.98 / 0.93 | 0.99 / 0.96 / 1.01 / 0.96 | raises at 1%, 20%, 50%, 99% | 0.97 / 1.06 / 1.28 / 2.87 | 1.03 / 0.89 / 1.00 / 1.10 | 1.07 / 0.95 / 1.07 / 1.50 |
| fallible_or_fallible | 5.2 / 5.3 / 4.6 / 1.9 | 0.99 / 1.07 / 1.21 / 2.65 | 1.01 / 0.95 / 1.02 / 1.00 | 1.03 / 1.01 / 1.01 / 1.01 | raises at 1%, 20%, 50%, 99% | 1.24 / 1.24 / 1.51 / 2.68 | 1.05 / 0.94 / 1.06 / 1.46 | 1.10 / 0.96 / 1.09 / 1.59 |

**Reading.** D10 is affordable, and the strategy decides how affordable.

- Today's behaviour is what #252 describes. With failing rows, today's
  checked plan raised in 21 of 24 cells, and whether it raised depended on
  the operand order and on the share. It did not raise in three cells,
  all at 1%: both `&` forms with a guard, and `guard_or_fallible`, while
  `fallible_or_guard` raised there. All three prototypes returned the rows
  today's unchecked plan returns, in every cell.
- `errors` costs nothing measurable without failing rows (0.90 to 1.13,
  and 1.32 in one cell that takes 1.3 ms) and at most 1.5 times with them.
  Every cell above 1.1 is one where today's plan takes under 2 ms.
- `optimistic` also costs nothing without failing rows, and up to 1.9
  times when a batch raises and is evaluated again.
- `select` costs more as more rows are left open, because it filters,
  evaluates and scatters back: about 1.0 at 1% and 20%, 1.2 to 1.3 at
  50%, 1.5 to 1.7 for `&` and 2.65 to 2.9 for `|` at 99%.

These queries take 1 to 8 ms, so a single ratio can move by about 0.2.
At 1, 2 and 4 partitions with a 50% share, `select` is again the
costliest (1.06 to 1.61) and the other two are within noise; at 10K and
100K rows every mode is within about 0.2 of today.

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
  what an implementation that knows a key is fallible pays.

Every pipeline derives a key or argument with integer arithmetic, and
checked arithmetic (m1) is on in every mode, so only the pushdown differs.
A control, `scan_filter_off`, keeps today's pushdown but turns off the
scan's row filter and pruning (m5's `prune_off`). On the sample file no
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
1.3 to 2.0 GiB peak. Against the run at `bfa963a`, the ratios of cells
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
  265 times today's time (0.24 to 0.73 s against 3 to 4 ms), with the
  same memory except at 12 partitions (118 MiB against 40).

The lost pruning is the same order as m5's (D7). It survives where an
implementation can show a key cannot fail: hits_sorted's statistics bound
`counterid` at 262,115, so `counterid + 1` cannot overflow. M16 has no
such analysis, and whether to build one is part of deciding the placement.

## m5: `assume_sorted` validated without pruning (gates M3)

**What it prices.** Decision D7 (contract §9.5) checks each adjacent pair
of an `assume_sorted` declaration when its later row is read, across batch,
file and partition boundaries. It applies no statistics pruning or
pushdown below the declaration, so no pair goes unchecked. v0.4 trusts the
declaration.

**Prototypes** (`validate.rs`). `prune_off` turns pruning and pushdown off
without checking: D7's read on its own. `validate` adds the check, which
compares adjacent rows' keys in arrow's row format; the last partition to
finish checks the pairs between partitions.

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
  check), and 360 MiB instead of 22 at 12 partitions. The check adds 5 to
  7% on top of that read.
- Where statistics prune nothing, D7's read costs 1.17 to 1.24 times on
  `counter_events`, which loses the scan's row filter and page index, and
  nothing on `keys`, which has no filter. The check then costs 1.2 to 1.8
  times today's time. The ratio falls with the partition count because
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
- Both use less memory than today when a result is exported. A view keeps
  the decoded page buffer it points into alive, so `to_arrow` of the
  string columns holds 727 MiB today against 461 to 478 MiB with either
  prototype, and the filtered export holds 97 MiB against 25 to 30.

For M18 the choice is which pipelines pay about 1.3 times. A placement
that decodes plain strings when the plan keeps most of them and casts at
the root after a selective filter would avoid both costs. It was not
prototyped, and it needs an estimate of selectivity before execution.

## Questions for the owner

Recording a budget for each gated item (D9) is the owner's step; the
cost ranges above are its inputs. The measurements also leave these
decisions open:

1. **D7 (M3).** A lookup on a sort key costs 156 to 604 times today's
   time under D7 as written. Should the contract keep checking every
   adjacent pair, or accept a weaker check that keeps pruning (only the
   row groups read, or the skipped ones by their statistics)? Neither
   alternative was prototyped.
2. **D10a (M16).** Placing the filter between the operator and its input
   costs nothing here except lost pruning on sorted keys. Should M16 also
   prove keys infallible from statistics, so that pruning survives where
   overflow is impossible?
3. **D10 (M16).** Keeping errors as masks costs nothing measurable without
   failing rows and up to 1.5 times with them. Evaluating the fallible
   operand only on open rows costs up to 2.9 times. Is the mask strategy
   the one M16 should build?
4. **OD-W (#276).** Exact rolling `mean` and `std` cost 2 to 3.9 times
   today's time, compensated ones 1.4 to 1.6. Which does the contract
   require? And do exact grouped statistics over many groups (6 to 6.75
   times, nearly 3 times the memory) get a budget exception, or a leaner
   accumulator first?
5. **M1.** With 8 batches per unit the row-group merge is within 10% of
   today at 12 and 24 partitions but 1.18 times at 4. Is that within
   budget, or should another schedule be prototyped first?
6. **M18.** Normalize at the scan, at the root, or by estimated
   selectivity (not prototyped)?

One observation concerns today's engine rather than the contract. LTSeq
turns on the Parquet scan's row filter (`pushdown_filters`). In m4b it
cost 1.04 to 1.4 times on a filter that keeps 20% of rows. In m5, turning
it off together with the page index cost 17 to 24% on a filter that
keeps 0.7%. #307 asks for it to be measured more widely.

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
  stops short. The ones that matter most here: m1 checks Int64 only, m4
  keeps errors as masks only for the expression shapes it measures, m5
  needs the keys among the scan's columns, m3's row-group merge covers
  Parquet only, and m2's per-group state is unoptimized.
- Window pipelines (`cum_sum`, `rolling`) run over the declared order on
  one partition whatever the partition count. At two or more partitions
  today's plan materializes the input first (about 385 MiB at 10M rows)
  in every mode, so their peak memory says nothing about the prototypes.
- For a pipeline that returns one row (the aggregates, m4b's grouped
  filters), time to first row equals the total time.
- Peak memory is the resident growth over the first measured run; values
  under about 5 MiB are at its resolution.
