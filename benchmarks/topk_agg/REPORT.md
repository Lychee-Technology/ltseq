# Exact Top-K aggregate pruning: research report (#386)

**Question.** Is Pivot-style exact slot-bound pruning ("Know Your LIMIT") worth
building for LTSeq's `group by key, count, sort desc, first K` queries?

**Recommendation: Go, for an off-by-default, production-shaped
implementation of the shape the spike covers, and nothing wider.** The
second spike clears the issue's bar on this machine. On ClickBench at K=10 it
was 23.7% faster on Q16 (UserID) and 18.2% on Q34 (URL) at 8 partitions,
and 18–32% faster at 16 and 32 partitions against today's plan at the same
partition count. Outside the 40 ms cells at 1K keys, no comparison cell
regressed by more than 3.9%, and those cells reached +4.6% on a 6.3%
spread. That includes every cell where nothing can be pruned: uniform,
flat and hot-key data, K=1000, and 32 partitions on 8 cores. Peak memory
falls where it prunes and grows by at most 8% (13 MiB) elsewhere. What it still
lacks is production plumbing, not evidence: memory-pool accounting and
spilling for its buffer, a slot count and probe that do not depend on row
arrival order, and tests that pin it to DataFusion's plan shapes. The
decision is the owner's.

The first spike (commit `96a3b17`, kept in `results/v1/`) recommended No-Go.
It was 6–13% slower, with up to 3.8× the memory, on queries it could not
prune, and 38% slower at 32 partitions. The owner's interim assessment on
the issue traced that to the operator's mechanics, not to the algorithm,
and proposed a threshold before any merge, an early bail-out, and slot
counts sized from the data. The second spike implements those three. A
first rerun showed that bailing out only after buffering still cost 4–10%
(`results/late_bail/`), so it also gained a probe that gives up on pruning
after the first 131,072 rows. The sections below give the evidence.

Phase 0 and Phase 1 were measured once, between 21:07 and 23:10 UTC on
2026-11-07. The comparison of the second spike ran between 00:06 and 00:41
UTC on 2026-11-08, on the same machine. Numbers come from `results/`;
nothing is taken from historical runs.

## Setup

| | |
|---|---|
| Machine | Intel i7-8700K, 8 CPUs visible to the guest, 23 GiB, Linux 7.0.14 (Proxmox guest) |
| Build | `maturin develop --release --features topk_research` (fat LTO), rustc 1.97.1, Python 3.14.10, pyarrow 25.0.1, DataFusion 55.0.0 |
| Commit | Phase 0–1: `main` at `6715031` plus the research hook. Comparison: `main` at `c1a236a` plus this branch's second spike. `c1a236a` only changes `is_in`. With the spike off, a measurement build plans and runs exactly today's engine |
| Engine | LTSeq's session: `target_partitions` = CPU count (8) unless pinned, batch size 16384, `repartition_aggregations` on, Parquet filter pushdown on, DataFusion's partial-aggregation skip at its defaults (probe 100,000 rows, ratio 0.8) |
| Synthetic data | 12 Parquet files of 20M rows (`fixtures.py`, seed 386): uniform, Zipf (s=1.1), hot-key (50% one key) and flat (counts within ±1) over 1K, 100K and 1M keys; each keyed by int64 and by a ~40-byte URL-like string |
| ClickBench | `hits.parquet`, 99,997,497 rows, 226 row groups (14 GiB), from `prepare_data.py` |
| Commands | `run.py` (Phase 1, 156 synthetic cells, then `--groups clickbench`), `report.py`, `compare.py` (Phase 3, 44 cells), exactly as in [README.md](README.md) |
| Method | One process per cell; warm-up run, 5 timed runs (the comparison interleaves its modes in 5 rounds, alternating their order); the harness waits for a quiet machine and reruns a contended cell (`benchmarks/m0/harness.py`) |

Every result of every cell was checked against DuckDB. The check is
tie-aware: the returned counts must equal DuckDB's top-K counts, and every
returned key must have the count DuckDB gives it. All 162 Phase 1 cells
passed. All 138 results of the comparison passed: 44 of today's plan and
94 of the spike and its control.

Two measurement caveats apply throughout:

- Stage shares come from DataFusion's own operator metrics through the
  `_topk_analyze` hook (median of 3 runs), divided by the process's CPU time
  over the same run. `elapsed_compute` is time spent inside the operator's
  poll. At 8 partitions on 8 CPUs that is close to CPU time. At 32 partitions
  it includes time spent waiting for a CPU, so 32-partition shares can
  exceed 100%; read them as relative only.
- The spike's stage shares are unavailable wherever a partition bailed out
  early. The operator that produced `results/compare.json` charged such a
  partition with the elapsed time from its probe to the end of its
  streaming merge, minus only the time spent inside its input's polls. The
  intervals it spent suspended waiting for the next batch stayed in, so its
  `elapsed_compute` was mostly waiting; `uniform_1000000` at 8 partitions
  showed a 101.1% final-stage share. `compare.py` now leaves those shares
  blank, and the operator now counts only the time inside its own polls
  less its input's, a test with an input that suspends between batches
  holds it to that, and the probe's slotting is included. The cells were
  not rerun with it. Partitions that kept buffering were timed after their
  input was drained, so their shares stand, but they leave out the probe's
  slotting of its first 131,072 rows. End-to-end latencies are measured
  separately and are unaffected.
- Phase 1 peaks are resident growth over a warm run, which understates them.
  The comparison's peaks are cold single runs in a fresh process and are the
  ones to compare.

In Phase 1, `uniform_1000000 skey count_col` (0.80 other cores) and
`flat_1000000 skey` at K=100 (0.51) stayed contended after their retries.
Neither is used for a conclusion. No comparison cell was contended (at most
0.05 other cores).

## Phase 0: what the engine does today

At 8 partitions every variant measured plans the same way:

```text
SortPreservingMergeExec: [cnt DESC], fetch=K
  SortExec: TopK(fetch=K), expr=[cnt DESC], preserve_partitioning=[true], filter=[cnt IS NULL OR cnt > T]
    [ProjectionExec]                                   -- only with a post-aggregate derive
      AggregateExec: mode=FinalPartitioned, gby=[key], aggr=[count(1) as cnt]
        RepartitionExec: partitioning=Hash([key], 8)
          AggregateExec: mode=Partial, gby=[key], aggr=[count(1) as cnt]
            DataSourceExec (Parquet; a filter is pushed into the scan)
```

The variants are `COUNT(*)`, `COUNT(col)` (`count(flag)`), a post-aggregate
projection, and a filter below the aggregate. `results/plans/` holds the
plans with their metrics. At one partition the plan is `SortExec: TopK` over
`AggregateExec: mode=Single`. There is no partial/final split, and so
nothing for a final-merge pruner to act on.

What DataFusion 55 already does for this shape, and what it leaves undone:

- `SortExec` with a fetch keeps a K-row heap per partition. It publishes a
  dynamic filter (`cnt > T`), but the filter sits above the final aggregate,
  which computes `cnt`. It cannot reach below it, so it only spares the heap.
- The `TopKAggregation` rule pushes a limit into `AggregateExec` only for a
  single group key with no aggregate (ordered by the key), or with one
  `MIN`/`MAX` ordered by that aggregate. Any other aggregate, `COUNT`
  included, is rejected (`topk_aggregation.rs`, the `else { return None }`
  arm). The limit and order never reach the aggregates of a `COUNT DESC
  LIMIT K`.
- `LimitedDistinctAggregation` needs a grouping without aggregates and
  without an order, so it does not apply.
- Partial-aggregation skipping applies when the first 100,000 rows of an
  input partition hold more than 0.8 groups per row. It fired on the
  uniform and flat 1M-key fixtures: 19.1M of 20M rows went to the final
  aggregate unaggregated. It did not fire on ClickBench.

So the gap the issue describes is real. Every partial row reaches the final
aggregate's hash table, however small K is. This is unrelated to the
legacy `top_k(col, k)` aggregate, which returns values rather than grouped
rows.

Two side findings. First, the lambda DSL has no row count inside `agg`:
`agg(cnt=lambda g: g.count())` raises `ColumnNotFoundError`, because it
reads `count` as a column name. The harness builds the call the Rust side
already plans as `count(1)` (`CallExpr("count")`). Second, the threads of
#158, #230, #331, #336 and #345 were not available to this run (only #386
itself was), so I could not check them for reusable harness pieces or
known-bad fixtures. This harness does not reuse `bench_vs.py`'s R1 numbers
or its result check; it has its own fixtures and check.

## Phase 1: where the time goes

Selected cells at K=10 and 8 partitions. The full matrix (K = 1, 10, 100,
1000; 1, 8 and 32 partitions; four query shapes) is in
[results/cells.md](results/cells.md).

| Workload | Median s | Final aggregate | Partial aggregate | Scan | Final input rows | Groups | Prunable (simulated) | CPU ceiling |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| ClickBench Q16, UserID | 1.410 | 42.5% | 53.2% | 6.6% | 20.9M | 17.6M | 30.9% | 13.2% |
| ClickBench Q34, URL | 5.922 | 22.8% | 40.9% | 21.8% | 21.7M | 18.3M | 99.7% | 22.8% |
| ClickBench SearchPhrase ≠ '' | 1.434 | 18.1% | 26.3% | 50.6% | 7.0M | 6.0M | 99.7% | 18.1% |
| zipf 1M keys, int | 0.188 | 11.9% | 71.7% | 13.3% | 2.0M | 0.75M | 99.7% | 12.0% |
| zipf 1M keys, string | 0.658 | 9.1% | 53.6% | 22.5% | 2.0M | 0.75M | 99.7% | 9.2% |
| hot 1M keys, int | 0.356 | 23.5% | 69.3% | 8.1% | 5.6M | 1.0M | 0.2% | 0.0% |
| uniform 1M keys, int | 0.365 | 83.2% | 3.3% | 6.6% | 19.9M | 1.0M | 0.1% | 0.1% |
| uniform 1M keys, string | 1.256 | 58.7% | 2.7% | 16.2% | 19.9M | 1.0M | 0.1% | 0.0% |
| zipf 100K keys, int | 0.114 | 1.8% | 77.3% | 17.3% | 0.69M | 0.1M | 99.7% | 2.0% |

The final merge is a large share only in two cases. One is when partial
aggregation was skipped or barely reduced the rows: uniform and flat 1M
keys, and ClickBench, where 100M rows become 21M partial rows for 18M groups.
The other is skewed data whose partial aggregation collapses the heavy keys
but leaves a long tail of singletons. That tail is what a pruner skips. At
1K or 100K keys the final merge is under 3% of CPU whatever the
distribution, so there is nothing to win there.

"Prunable" comes from `ceiling.py`, which runs the pruning algorithm on the
exact per-group counts (from DuckDB) with 1, 1024 and 8192 slots per
partition and each partition's own threshold. The column shows the best
case. The CPU ceiling is that share times the final aggregate's (and Top-K
sort's) share of CPU. Uniform, flat and hot-key data prune nothing: their
K-th count is a few dozen, below any slot's bound. Zipf and ClickBench URL
and SearchPhrase prune nearly every group at K≤100. At K=1000 the
partition-local threshold drops sharply (Zipf int 48% of groups, URL 14%,
UserID 0.2%).

**Gate.** The issue's early stop applies "if final merge is not a
meaningful bottleneck on representative workloads". On ClickBench Q34 and
the filtered SearchPhrase count, the ceilings (22.8%, 18.1%) exceeded 15%.
So the gate did not stop the work, and the spike was built. On every
synthetic workload the ceiling was 12% or less.

The simulation used partition-local thresholds. The spike shares its
threshold across partitions, and that matters: Q16's simulated 30.9%
became 98.7% of partial rows skipped in the first spike. With a global threshold
the simulation's oracle column already showed 90.8% of Q16's groups
prunable. The ceiling is therefore an upper bound only for the local
variant. The other direction shows too: on Zipf 1M int the ceiling was 12%
of CPU, but the wall-clock gain was 2%, because the final merge overlaps
the partial aggregation it waits on.

## Phase 2: exactness

The unit of pruning is a slot inside a final-aggregation partition, placed
after `RepartitionExec: Hash(keys)`. The repartition sends every partial row
of a key to exactly one final partition. A slot is a hash of the key with a
second seed, so all of a key's partial rows land in one (partition, slot).
Input partitions are never units. A key's partial count in one input
partition is not a bound on its global count, because its other partial
rows sit elsewhere. The rule refuses a final aggregate whose input is not a
hash repartition.

**Bounds.** For COUNT, the partial states are non-negative Int64 values
whose sum is the final count. `COUNT(col)` partials exclude NULL values and
still sum to the final value. Equal keys hash alike: DataFusion's own hash
tables depend on that, and the slot is computed by the same `create_hashes`
over the same column values the final aggregate groups. Per slot s the
spike records `bound(s)`, the sum of its partial counts, and `maxp(s)`, its
largest single partial count. For every group g in slot s,
`count(g) = Σ partials(g) ≤ bound(s)`, and `maxp(s)` is at most the largest
final count in s, because a partial count never exceeds its group's
total. Collisions only loosen a bound. The bounds are complete only after the
partition's last input row, so no pruning decision is made before then.
Late input is impossible by construction. DataFusion 55 does not
re-partition adaptively; a future engine that moved keys between partitions
mid-stream would break the ownership the bounds rely on.

**Thresholds.** Each is at most the partition's K-th count, which is at
most the global K-th count:

- `T0`, before any merge: the K-th largest `maxp` over occupied slots
  (undefined with fewer than K). Slots hold disjoint groups, so K slots with
  `maxp ≥ T0` name K distinct groups whose counts reach `T0`.
- `T1`, after the first merge of the K slots with the largest `maxp`: the
  K-th count among their groups. Each of those slots holds a group of count
  at least `T0`, so `T0 ≤ T1`.
- the shared threshold: the largest `T0` or `T1` any partition has
  published (an atomic).

A slot outside the first merge is skipped only when its bound is strictly
below the largest of these. Its groups are then below the global K-th count
and in no top-K. Every group at or above the global K-th count is merged
and emitted, and the Top-K sort and merge above pick an exact top-K from
them. Equality never prunes: a group tied with the threshold stays a
candidate, so a secondary sort key would still see every tied group.

**Bail-outs** are always exact, since they merge every row. A partition
bails out after buffering when the first merge and the kept slots together
hold more than half its rows. Its probe bails out earlier, after 131,072
rows: it computes `T0` and the bounds of those rows alone. If more than 90%
of them lie in slots that reach that `T0`, the partition streams its
buffered batches and the rest of its input into one final aggregate. The
probe is a guess about speed. Its threshold and bounds are not the final
ones.

Edge cases:

- Fewer than K occupied slots, or K=0: no `T0`; the rule also requires
  K ≥ 1. A partition without `T0` prunes only by the shared threshold.
- A NULL group key hashes to a fixed value and is one group, like any other
  key.
- Float keys: slotting uses the values the aggregate groups, so `-0.0` and
  NaN fall in the same slot exactly when the aggregate puts them in the same
  group.
- Overflow: a bound is an i64 sum of at most the input row count.

**Errors and eligibility.** The rule plans the pruner only for this shape:
a `FinalPartitioned` aggregate over a hash repartition, one grouping set,
exactly one aggregate that is a non-distinct, unfiltered `count` with an
Int64 state, a single descending sort key that is that count (possibly
renamed by projections between it and the sort), and a fetch of at least 1.
Anything fallible below the aggregate, such as filters, derived keys or the
arguments of the count, is evaluated before the partial aggregate, so its
errors surface before the final stage. COUNT's finalization cannot fail.
That is why SUM, AVG, MIN and MAX are excluded. A SUM over values of either
sign has no such bound. Even a non-negative SUM finalizes with checked
arithmetic under the v0.5 contract, so skipping it could hide an overflow
error.

A projection between the aggregate and the sort is different: it evaluates
every expression on every group the aggregate emits, so a derive such as
`1 // (k - 20000)` raises on a group the pruner would discard. The first
version of the rule checked only the expression carrying the count, and on
sixteen heavy keys plus 20,000 singletons it returned a top-1 where today's
plan raises division by zero. The rule now plans the pruner only when every
expression of every such projection is a column or a literal, which cannot
fail; any computed column, including a checked `cnt * 2`, keeps today's
plan. No cell of the comparison had one; the Phase 1 `projected` cells ran
today's plan only.

**Test evidence.**

- `selfcheck.py` checks the first spike's wave rule on 20,000 random
  inputs heavy in ties and hot keys, 4,399 of which prune something. It
  checks the second spike's thresholds on 20,000 more, 5,382 of which prune
  something. Those inputs split each group's count over up to eight partial
  rows, as several input partitions' partial aggregates would. Every input
  satisfies `T0 ≤ T1 ≤` the partition's K-th count `≤` the global one, and the
  rule keeps every group at or above the global K-th count.
- `py-ltseq/tests/test_topk_research.py` (208 tests on a measurement build)
  holds the spike to the unpruned engine. It runs across the nine pinned
  engine layouts and every input layout of `layouts.LAYOUTS`, with fixed
  and data-sized slot counts (down to one row per slot). It covers ties,
  flat counts, K at and above the group count, empty input, NULL and string
  keys, float keys with `-0.0`, NaN and NULL, and `COUNT(col)` over NULLs.
  It checks that the control merges every row, that an unprunable input
  bails out, that the probe bails out before buffering everything, and that
  the slot count follows the input. It also checks that the rule leaves
  ascending sorts, unlimited sorts, other aggregates, two aggregates and
  sorts on derived columns alone, and that a failing derive between the
  aggregate and the sort still raises with the spike on.
- The 1,017 tests of the repository's aggregation, grouping, sort, slice
  and `top_k` files pass with the spike forced on for every eligible plan,
  once at one partial row per slot and once at 128. I did not count how many
  of their plans were eligible.

## Phase 3: architecture and spike

Three ways to build it, compared on whether they actually skip merge work:

1. **Extend DataFusion's aggregate executor.** A pruning mode inside
   `GroupedHashAggregateStream` would reuse its memory accounting, spilling
   and the repartition's hashes. It is also the only option that could
   avoid hashing every key a second time. But it is a fork of DataFusion, or
   an upstream proposal that changes how a final aggregate consumes its
   input. The existing Top-K aggregation machinery (`LimitOptions`, the
   priority map) does not transfer: it works because each row moves a
   MIN/MAX monotonically toward its final value, whereas a COUNT is not
   known until the last input row.
2. **Upstream-compatible extension point.** DataFusion's physical optimizer
   rules and custom `ExecutionPlan`s, which LTSeq already uses
   (`DropFloatStatistics`, `KeepFileOrder`), let a rule swap the final
   aggregate without forking. The spike takes this route. The cost is
   coupling: the rule matches DataFusion's plan shapes and `AggregateExec`'s
   accessors, which change between releases.
3. **A narrowly specialized LTSeq operator.** The spike's
   `SlotPrunedCountExec` is this, planned through (2). Its merges are
   DataFusion's `AggregateExec` in `Final` mode, so the group table is the
   baseline's. It does skip merge work: a pruned row is never copied,
   probed or inserted. It still hashes every row a second time, and it
   buffers every row unless its probe bails out.

The spike (`src/topk_research/prune.rs`) is off unless `_topk_set_prune`
turns it on in a measurement build. Per final partition it buffers its
input, with the probe described above, then sizes its slots (`rN`: one slot
per N buffered partial rows) and computes `bound`, `maxp` and `T0`. It runs
the first merge (the K slots with the largest `maxp`), takes `T1` and the
shared threshold, and either bails out or gathers the kept rows once and
merges them. That is at most three merges, against up to fourteen waves in
the first spike. Its metrics report rows in, rows merged, slots, merges,
bail-outs, and the time spent slotting and gathering. A **control** mode
(`prune=False`) probes alike and runs the same first merge, then merges
every row. The gap from today's plan to the control is the cost of the
mechanics; the gap from the control to the spike is what pruning buys.

### Baseline vs spike

Medians of 5 interleaved runs. Δ is the change against today's plan at the
same partition count (negative is faster); "vs control" is the spike against
its control, which is the share to credit to pruning. Peak is a cold single
run. "Merged" is the share of the final aggregate's input the spike merged
(first of three analyzed runs). The full table, including 1K and 100K keys,
is in [results/compare.md](results/compare.md).

ClickBench, `r128` unless noted:

| Query | K | P | Today s | Spike Δ | Control Δ | Spike vs control | Peak today → spike, MiB | Merged |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Q16, UserID | 10 | 8 | 1.320 | **−23.7%** | +7.1% | −28.8% | 1335 → 1002 | 25.1% |
| Q34, URL | 10 | 8 | 5.845 | **−18.2%** | +3.2% | −20.7% | 8596 → 7701 | 0.06% |
| SearchPhrase ≠ '' | 10 | 8 | 1.401 | −11.3% | +5.6% | −16.0% | 1599 → 1290 | 0.4% |
| Q16, UserID | 10 | 16 | 1.196 | −28.9% | −3.1% | −26.6% | 1517 → 1196 | 37.8% |
| Q34, URL | 10 | 16 | 5.710 | −17.8% | +2.3% | −19.7% | 8670 → 7816 | 0.1% |
| SearchPhrase ≠ '' | 10 | 16 | 1.355 | −13.9% | +2.4% | −15.9% | 1793 → 1366 | 0.7% |
| Q16, UserID | 10 | 32 | 1.065 | −31.6% | −1.0% | −31.0% | 1684 → 1028 | 1.8% |
| Q34, URL | 10 | 32 | 5.592 | −17.9% | +1.0% | −18.7% | 9043 → 7329 | 0.2% |
| SearchPhrase ≠ '' | 10 | 32 | 1.332 | −12.6% | +2.1% | −14.4% | 1693 → 1398 | 1.4% |
| Q16, UserID | 1000 | 8 | 1.355 | −2.6% | −2.1% | −0.5% | 1361 → 1348 | 100% |
| Q34, URL | 1000 | 8 | 5.831 | +0.3% | −0.1% | +0.4% | 8462 → 8436 | 100% |
| Q34, URL, `r32` | 1000 | 8 | 5.831 | −14.4% | | | 8462 → 7549 | 2.4% |
| SearchPhrase ≠ '' | 1000 | 8 | 1.414 | +0.0% | +0.8% | −0.7% | 1573 → 1601 | 100% |

Synthetic, 1M keys, `r128`:

| Workload | K | P | Today s | Spike Δ | Control Δ | Peak today → spike, MiB | Merged |
|---|---:|---:|---:|---:|---:|---:|---:|
| zipf, int | 10 | 8 | 0.188 | −9.9% | −0.4% | 149 → 140 | 0.6% |
| zipf, string | 10 | 8 | 0.637 | −6.0% | +0.3% | 478 → 447 | 0.5% |
| zipf, int | 10 | 32 | 0.159 | −4.7% | +1.9% | 170 → 166 | 1.8% |
| zipf, int | 1000 | 8 | 0.186 | +3.8% | +3.3% | 146 → 158 | 100% |
| uniform, int | 10 | 8 | 0.365 | −0.1% | −0.2% | 173 → 180 | 100% |
| uniform, string | 10 | 8 | 1.178 | −1.7% | −0.8% | 545 → 569 | 100% |
| uniform, int | 10 | 32 | 0.194 | +3.9% | +2.7% | 295 → 306 | 100% |
| uniform, string | 10 | 32 | 1.000 | +1.5% | +2.7% | 1182 → 1196 | 100% |
| flat, string | 10 | 8 | 1.162 | +1.0% | +0.2% | 578 → 547 | 100% |
| hot, string | 10 | 8 | 0.876 | −0.0% | +1.3% | 900 → 879 | 100% |

Run-to-run spread on these cells was 0.3–3.7% (cv), except UserID at 16
partitions (8.4%, see below). The 40–120 ms cells at 1K keys, which these
tables leave out, reached 9%. Their medians moved between −12.6% and +4.6%,
in both directions and for the control alike. That is noise, not an effect.

What the numbers say:

- **Pruning is credited with the spike-vs-control gap**, as the interim
  assessment asked: −29%, −21% and −16% on the three ClickBench queries at
  8 partitions. The final stage's CPU fell from 9.76 s to 1.34 s (URL) and
  from 1.63 s to 0.24 s (SearchPhrase). On UserID two of eight partitions
  bailed out early, so its final-stage CPU is unavailable (see Setup).
- **More final partitions are not the explanation.** Today's plan does get
  faster with more partitions: UserID 1.320 → 1.196 → 1.065 s at 8, 16 and
  32, URL 5.845 → 5.710 → 5.592 s. The spike's gain against the same
  partition count holds or grows (UserID −24%, −29%, −32%; URL −18% at
  all three), and the spike at 8 partitions beats today's plan at 32.
- **Where nothing can be pruned, the spike is within noise of today.** On
  the no-gain cells it ranged from −1.7% to +3.9%, against +4–13% for the
  first spike. Its peak memory grows by at most 8% (13 MiB), against
  3.5–3.8× for the first spike.
  The probe did it: in the rerun without it (`results/late_bail/`),
  bailing out only after buffering cost +6.8% to +9.6% on uniform and flat
  1M keys and +4–5% on hot keys, with 3× the peak memory on string keys.
  That is the price of the barrier: the final merge cannot overlap the
  partial aggregation it waits on.
- **The probe misjudges some partitions**, only on UserID at K=10. In the
  analyzed runs it bailed out in 2 of 8 partitions at 8 partitions, 2–10 of 16
  at 16, and 0–7 of 32 at 32. Those partitions merged every row; the
  partitions that kept buffering merged about 0.1%. The probe sees only the
  first rows to arrive, so its verdict can change from run to run, and it
  explains the 8.4% spread at 16 partitions. The query was still 24–32%
  faster. With `r32` no probe bailed out and the spike merged 0.01%, but it
  was slower (−20.5%): finer slots cost more to compute.
- **The slot count still matters.** At K=1000 the K-th count is low, and a
  slot's bound falls below it only when the slot is small. URL at K=1000 bailed
  out at `r128` (+0.3%) and gained 14.4% at `r32`. On URL at K=10, `r32`
  doubled the slotting time (2.33 s against 1.27 s) and gained less than
  `r128` (−16.3% against −18.2%). No single setting measured here is best
  everywhere.
- **The controls price the case where the probe keeps buffering and pruning
  then fails:** +3.2% to +7.1% at 8 partitions on ClickBench, under +2.5%
  at 16 and 32. No real query in the matrix landed there, but an
  adversarial one could.
- **The ceiling is the final stage's share.** On URL the process spent
  41.9 s of CPU: 16.8 s in the partial aggregate, 9.8 s in the final one.
  The spike's final stage took 1.34 s, of which 1.27 s was hashing the URL
  strings into slots, and the process 33.0 s. (The 1.27 s includes the
  probe's slotting of each partition's first 131,072 of about 2.7M rows,
  which the 1.34 s leaves out; see Setup.) Removing the second hash
  (the interim assessment's option E) could save at most that 1.3 s of
  CPU, about 3% of the process's CPU; the gains above already pay for the
  second hash.
- **Pruning ratio is not speedup.** On Zipf 1M keys the spike skipped
  99.5% of the final input and gained 6–10%, because the final merge was
  only 10–12% of CPU.

### The first spike against the second

Spike Δ against today's plan at 8 partitions, K=10 unless noted (first
spike: 8192 slots, from `results/v1/`; second: `r128`).

| Workload | First spike | Second spike | Peak, first → second, MiB |
|---|---:|---:|---:|
| ClickBench Q16, UserID | −25.7% | −23.7% | 1027 → 1002 |
| ClickBench Q34, URL | −17.8% | −18.2% | 7510 → 7701 |
| ClickBench SearchPhrase ≠ '' | −11.7% | −11.3% | 1301 → 1290 |
| ClickBench Q16, UserID, K=1000 | +6.5% | −2.6% | 1248 → 1348 |
| ClickBench SearchPhrase ≠ '', K=1000 | +8.3% | +0.0% | 1617 → 1601 |
| uniform 1M keys, string | +13.0% | −1.7% | 2078 → 569 |
| flat 1M keys, string | +12.7% | +1.0% | 2036 → 547 |
| hot 1M keys, string | +7.2% | −0.0% | 841 → 879 |
| uniform 1M keys, int, 32 partitions | +37.9% | +3.9% | 544 → 306 |
| uniform 1M keys, int | −24.5% | −0.1% | 594 → 180 |

The last row is the effect the interim assessment flagged. The first
spike's control was 30% faster there with nothing pruned, because it merged
in several small hash tables instead of one. The second spike merges in one
table, as today's plan does, and that effect is gone. It is not evidence
for pruning, and it is outside this issue.

## Recommendation

**Go for an off-by-default implementation of exactly this shape**: a single
`COUNT`, descending, `LIMIT K`, over a hash repartition.

- **Against the bar.** At least one representative high-cardinality
  workload gains ≥15%: Q16 −23.7% and Q34 −18.2% at 8 partitions, and
  −17.8% to −31.6% at 16 and 32. Each is far outside its cell's spread. No
  comparable negative control regressed by more than 5%: the worst was
  +3.9% with a 3.0% spread, or +4.6% with a 6.3% spread on a 37 ms
  cell. Peak memory falls where the spike prunes and grows by at most 8%
  elsewhere.
- **What it is worth.** The gain applies to one shape on skewed
  high-cardinality keys at small K. It matches ClickBench Q16/Q34 and
  LTSeq's R1 benchmark. It is a narrow slice of LTSeq's workload, and it is
  not where LTSeq's ordered-sequence operations spend their time.
- **What production needs**, none of it measured here:
  - Register the buffer with the memory pool and make it spill. On Q34 it
    holds a partition's 2.7M partial rows of URLs; a bounded pool would fail
    or need to spill them.
  - Make the slot count and the probe robust. A rule that picks the slot
    count from the probe's own `T0`, rather than a fixed rows-per-slot, is
    the obvious next experiment: it would address both the UserID probe
    misfires and URL at K=1000.
  - Add plan-shape tests against DataFusion upgrades. The rule matches
    `AggregateExec`'s accessors and the repartition shape of DataFusion 55.
  - Skip hashing once (option E). Replacing the hash `RepartitionExec` to
    carry the slot costs more to own than its ~3% of CPU on Q34 returns.
    Upstream, a final aggregate that receives the repartition's hashes would
    remove it and help every DataFusion user; that is worth proposing, not
    forking for.
- **What would reverse this.** If the memory-pool and spill integration
  costs more than the margin, or another machine does not reproduce the gains
  (one machine, 8 CPUs, so far), No-Go stands.

If the owner decides Go, the follow-up is one implementation issue covering
the production items above, behind an off-by-default setting, re-measured
against these same cells. This report opens none.

## Limitations

- One machine. A 6-core/12-thread CPU exposing 8 CPUs to a VM, measured
  with the harness's contention checks. Other core counts and memory
  bandwidths will move the ratios.
- ClickBench ran only on `hits.parquet` in its published order. The sorted
  copy was not run.
- The spike is a measurement tool. It is not memory-safe under a bounded
  pool, and its shared threshold is correct for a single execution of a
  plan (LTSeq plans every terminal afresh). Its probe and slot sizing are
  heuristics whose constants (131,072 rows, 90%, half, 128 rows per slot)
  were picked before the run, not tuned; only `r32` was tried besides.
- Stage shares beyond 8 partitions on 8 CPUs are not CPU time, and the
  spike's shares where a partition bailed out early are unavailable: they
  were measured before the operator stopped counting input waits, and were
  not rerun (see Setup).
