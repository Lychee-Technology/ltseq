# Exact Top-K aggregate pruning: research report (#386)

**Question.** Is Pivot-style exact slot-bound pruning ("Know Your LIMIT") worth
building for LTSeq's `group by key, count, sort desc, first K` queries?

**Recommendation: No-Go for a production implementation now.** The pruning
effect is real and large where it applies: on ClickBench at K=10 the spike
merged 0.1–1.3% of the final aggregate's input and was 25.7% faster on Q16
(UserID), 17.8% on Q34 (URL) and 11.7% on a Q13-like filtered SearchPhrase
count. That clears the issue's ≥15% bar. It fails the other half of the bar:
on comparable queries it cannot prune, it costs more than 5%. On the same
ClickBench file at K=1000 it was +6.5% (UserID) and +8.3% (SearchPhrase); on
uniform or flat string keys +12–13% with 3.8× the peak memory; at 32
partitions on 8 cores +38%. The control below shows this cost comes from the
operator's mechanics, not from pruning. The fallback that would cap it is not
demonstrated. Other parts are unbuilt: memory-pool integration and spilling,
a way to choose the slot count. The operator is also coupled to DataFusion 55's
plan shapes. The sections below give the evidence. The last one says what
would change the recommendation. The decision is the owner's.

Everything in this report was measured once, on one machine, between
21:07 and 23:10 UTC on 2026-11-07. Numbers come from `results/`; nothing is
taken from historical runs.

## Setup

| | |
|---|---|
| Machine | Intel i7-8700K, 8 CPUs visible to the guest, 23 GiB, Linux 7.0.14 (Proxmox guest) |
| Build | `maturin develop --release --features topk_research` (fat LTO), rustc 1.97.1, Python 3.14.10, pyarrow 25.0.1, DataFusion 55.0.0 |
| Commit | `main` at `6715031` plus this branch's uncommitted research code (the hook and spike in `src/topk_research/`); with the spike off, a measurement build plans and runs exactly today's engine |
| Engine | LTSeq's session: `target_partitions` = CPU count (8) unless pinned, batch size 16384, `repartition_aggregations` on, Parquet filter pushdown on, DataFusion's partial-aggregation skip at its defaults (probe 100,000 rows, ratio 0.8) |
| Synthetic data | 12 Parquet files of 20M rows (`fixtures.py`, seed 386): uniform, Zipf (s=1.1), hot-key (50% one key) and flat (counts within ±1) over 1K, 100K and 1M keys; each keyed by int64 and by a ~40-byte URL-like string |
| ClickBench | `hits.parquet`, 99,997,497 rows, 226 row groups (14 GiB), from `prepare_data.py` |
| Commands | `run.py` (Phase 1, 156 synthetic cells, then `--groups clickbench`), `report.py`, `compare.py` (Phase 3, 36 cells), exactly as in [README.md](README.md) |
| Method | One process per cell; warm-up run, 5 timed runs (the comparison interleaves its modes in 5 rounds, alternating their order); the harness waits for a quiet machine and reruns a contended cell (`benchmarks/m0/harness.py`) |

Every result of every cell was checked against DuckDB. The check is
tie-aware: the returned counts must equal DuckDB's top-K counts, and every
returned key must have the count DuckDB gives it. All 162 Phase 1 cells
passed. In the comparison, all 114 results passed: 36 of today's plan and 78
of the spike and its control.

Two measurement caveats apply throughout:

- Stage shares come from DataFusion's own operator metrics through the
  `_topk_analyze` hook (median of 3 runs), divided by the process's CPU time
  over the same run. `elapsed_compute` is time spent inside the operator's
  poll. At 8 partitions on 8 CPUs that is close to CPU time. At 32 partitions
  it includes time spent waiting for a CPU, so 32-partition shares can
  exceed 100%; read them as relative only.
- Phase 1 peaks are resident growth over a warm run, which understates them.
  The comparison's peaks are cold single runs in a fresh process and are the
  ones to compare.

Three cells stayed contended after their retries: in Phase 1,
`uniform_1000000 skey count_col` (0.80 other cores) and `flat_1000000 skey`
at K=100 (0.51); in the comparison, `zipf_1000000 skey` at K=1000 (3.0
other cores; its cv reached 30%). None is used for a conclusion.

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
So the gate did not stop the work, and I built the spike. On every
synthetic workload the ceiling was 12% or less.

The simulation used partition-local thresholds. The spike shares its
threshold across partitions, and that matters: Q16's simulated 30.9%
became 98.7% of partial rows skipped in the spike. With a global threshold
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

**Invariant.** For COUNT, the partial states are non-negative Int64 values
whose sum is the final count. `COUNT(col)` partials exclude NULL values and
still sum to the final value. Equal keys hash alike: DataFusion's own hash
tables depend on that, and the slot is computed by the same `create_hashes`
over the same column values the final aggregate groups. So for every group
g, `count(g) = Σ partials(g) ≤ Σ partials in slot(g) = bound(slot(g))`.
Collisions only make a bound looser. The bounds are complete only after
the partition's last input batch, so nothing is decided before then. Late
input is impossible by construction, at the cost of a pipeline barrier
(see the costs below). DataFusion 55 does not re-partition adaptively; a
future engine that moved keys between partitions mid-stream would break
the ownership the bound relies on.

**Threshold.** Slots are merged in descending bound order, in waves of
doubling size. A slot is skipped only when K groups are final and its bound
is strictly below the threshold. The threshold is the largest K-th count
any partition has finalized. A partition's K-th count is at most the global
K-th count, so a skipped group is below the global K-th count and in no
top-K. Every group at or above the global K-th count is merged and emitted,
and the Top-K sort and merge above pick an exact top-K from them. Equality
never prunes: a group tied with the threshold stays a candidate, so a
secondary sort key would still see every tied group.

Edge cases:

- Fewer than K groups, or K=0: nothing is pruned (the rule also requires
  K ≥ 1).
- A NULL group key hashes to a fixed value and is one group, like any other
  key.
- Float keys: slotting uses the values the aggregate groups, so `-0.0` and
  NaN fall in the same slot exactly when the aggregate puts them in the same
  group.
- Overflow: the bound is an i64 sum of at most the input row count.

**Errors and eligibility.** The rule plans the pruner only for this shape:
a `FinalPartitioned` aggregate over a hash repartition, one grouping set,
exactly one aggregate that is a non-distinct, unfiltered `count` with an
Int64 state, a single descending sort key that is that count (possibly
renamed by pass-through projections), and a fetch of at least 1. Anything
fallible, such as filters, derived keys or the arguments of the count, is
evaluated below the partial aggregate, and errors surface before the final
stage, so skipping finalization cannot suppress one. COUNT's finalization
cannot fail. That is why SUM, AVG, MIN and MAX are excluded. A SUM over
values of either sign has no such bound. Even a non-negative SUM finalizes
with checked arithmetic under the v0.5 contract, so skipping it could hide
an overflow error.

**Test evidence.**

- `selfcheck.py` checks the algorithm on 20,000 random inputs heavy in ties
  and hot keys, 4,399 of which prune something. Every count above the K-th is
  kept; at least K groups at or above it are kept, globally and per
  partition; a skipped group is strictly below the K-th count; local
  thresholds prune a subset of what the global one prunes.
- `py-ltseq/tests/test_topk_research.py` (123 tests on a measurement build)
  holds the spike to the unpruned engine across the nine pinned engine
  layouts and every input layout of `layouts.LAYOUTS`. It covers ties, flat
  counts, K at and above the group count, empty input, NULL and string keys,
  float keys with `-0.0`, NaN and NULL, and `COUNT(col)` over NULLs. It also
  checks that the rule leaves ascending sorts, unlimited sorts, other
  aggregates, two aggregates and sorts on derived columns alone.
- The repository's 831 aggregation, grouping, sort, slice and `top_k` tests
  pass with the spike forced on for every eligible plan. I did not count
  how many of their plans were eligible.

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
   `SlotPrunedCountExec` is this, planned through (2). It reuses
   DataFusion's `AggregateExec` in `Final` mode for each wave, so the group
   table is the baseline's. It does skip merge work: a pruned row is never
   copied, probed or inserted. It still hashes and buffers every row.

The spike (`src/topk_research/prune.rs`) is off unless
`_topk_set_prune(slots)` turns it on in a measurement build. It buffers a
partition's input and computes the slot bounds. It ranks slots by bound,
gathers each wave's rows and merges them, publishing the partition's K-th
count to the shared threshold. Its metrics report rows in, rows merged,
slots merged, waves, and the time spent slotting. A **control** mode
(`prune=False`) runs the same operator without ever pruning. The gap from
today's plan to the control is the cost of the mechanics; the gap from the
control to the spike is what pruning buys.

### Baseline vs spike

8 partitions. Medians of 5 interleaved runs. Δ is the change against
today's plan (negative is faster). Peak is a cold single run. "Merged" is
the share of the final aggregate's input the spike merged. The full table,
including 1K and 100K keys, 1024 slots and 32 partitions, is in
[results/compare.md](results/compare.md).

| Workload | K | Today s | Spike (8192 slots) | Control | Peak today → spike, MiB | Merged |
|---|---:|---:|---:|---:|---:|---:|
| ClickBench Q16, UserID | 10 | 1.343 | **−25.7%** | +6.1% | 1386 → 1027 | 1.3% |
| ClickBench Q34, URL | 10 | 5.894 | **−17.8%** | +6.3% | 8274 → 7510 | 0.1% |
| ClickBench SearchPhrase ≠ '' | 10 | 1.416 | −11.7% | +6.7% | 1528 → 1301 | 0.1% |
| ClickBench Q16, UserID | 1000 | 1.366 | +6.5% | +5.9% | 1391 → 1248 | 100% |
| ClickBench Q34, URL | 1000 | 5.896 | −1.1% | +5.7% | 8694 → 7804 | 73.5% |
| ClickBench SearchPhrase ≠ '' | 1000 | 1.401 | +8.3% | +10.7% | 1565 → 1617 | 99.9% |
| zipf 1M keys, int | 10 | 0.222 | −2.2% | +0.1% | 152 → 139 | 0.1% |
| zipf 1M keys, string | 10 | 0.633 | −4.5% | +3.0% | 487 → 444 | 0.2% |
| hot 1M keys, string | 10 | 0.889 | +7.2% | +5.0% | 889 → 841 | 100% |
| uniform 1M keys, int | 10 | 0.374 | −24.5% | −30.0% | 170 → 594 | 100% |
| uniform 1M keys, string | 10 | 1.168 | +13.0% | +11.0% | 545 → 2078 | 100% |
| flat 1M keys, string | 10 | 1.194 | +12.7% | +10.4% | 546 → 2036 | 100% |
| uniform 1M keys, int, 32 partitions | 10 | 0.193 | +37.9% | +28.1% | 308 → 544 | 95.5% |

Run-to-run spread on these cells was 0.4–5.6% (cv); the 40 ms cells at 1K
keys, which this table leaves out, reached 14%.

What the numbers say:

- **Pruning pays only where the final merge is big and nearly all of it
  can be skipped.** On ClickBench at K=10 the spike merged 0.1–1.3% of
  the final input. The final stage's CPU share fell from 42% to 7% (Q16)
  and from 23% to 4% (Q34). Peak memory fell with the group table. The
  wall-clock gain (12–26%) is a large part of the final stage's former
  share, because that stage was on the critical path.
- **Pruning ratio is not speedup.** On Zipf 1M keys the spike skipped
  99.8% of the final input and gained 2–5%. The final merge was 9–12% of
  CPU and ran concurrently with the partial aggregation that dominates.
- **The mechanics cost about 6% where nothing is pruned.** That is the
  control on every ClickBench query. The sources are hashing every key a
  second time, copying each wave's rows, up to 14 small aggregations per
  partition, and the barrier: the merge cannot start until the
  partition's last input row. When pruning fails (Q16 and SearchPhrase at
  K=1000) the spike pays this and gains nothing. With string keys and no
  skew it pays 11–13% plus 3.8× the peak memory, because it holds every
  partial row.
- **The slot count decides whether anything is pruned.** At 1024 slots,
  Q16 at K=10 merged 100% of its input and was 7.2% slower. At 8192 slots
  it merged 1.3% and was 25.7% faster. A bound falls below the K-th count
  only when a slot holds fewer rows than that count. The right slot count
  depends on the input size and the threshold, and the spike fixes it in
  advance.
- **One gain is not pruning.** On uniform and flat 1M int keys both the
  spike and the control were 23–30% faster, with 3.5× the memory. Nothing
  was pruned. Merging the final input in many small hash tables instead of
  one helps there, which looks like a cache effect. It reverses for string
  keys and at 32 partitions. It is not evidence for pruning, and it is
  outside this issue.

## Recommendation

**No-Go for building slot-bound pruning into LTSeq now.**

- **Against the bar.** At least one representative high-cardinality
  workload gains ≥15%: Q16 −25.7% and Q34 −17.8% at K=10, both well outside
  the 0.4–1.9% cv of those cells. The no-material-regression condition
  fails. The same file at K=1000 is +6.5% (Q16) and +8.3% (SearchPhrase);
  unskewed string keys are +12–13% with 3.8× memory; oversubscribed
  partitions are +38%. Each is several times the cell's run-to-run spread.
- **Complexity.** What the spike leaves out is what production needs. It
  has no memory-pool accounting and no spilling for a buffer that holds a
  partition's entire input (on Q34, all 21.7M partial rows of URLs at once). It has no slot
  count chosen from the data. It has no fallback that bounds the cost when
  nothing prunes. And it depends on DataFusion 55's plan shapes and
  aggregate accessors. The cheapest fallback, merging everything in one
  wave once the bounds show nothing will prune, still pays the barrier, the
  second hash and the buffer. It is not shown that any fallback gets under
  5%.
- **Scope.** The gain applies to one shape: a single COUNT, descending,
  small K, over a hash repartition, on skewed high-cardinality keys. It
  matches ClickBench Q16/Q34 and LTSeq's R1 benchmark. It is a narrow slice
  of LTSeq's workload, and it is not where LTSeq's ordered-sequence
  operations spend their time.

**What would change this.** A fallback that keeps the K=1000 and unskewed
cells within 5% of today, with the buffer accounted and spillable. The most
promising route is upstream, option 1: a final aggregate that receives the
repartition's hashes and can defer inserting rows. That removes the second
hash and the copies, which are most of the control's 6%. Upstream
DataFusion is also where a COUNT Top-K would benefit every user. If the
owner weighs the ClickBench gains differently and decides Go, the first
follow-up should be the fallback and slot-count study against these same
cells, not an implementation. No follow-up issue is opened, as the issue
asks, since this report concludes No-Go.

## Limitations

- One machine. A 6-core/12-thread CPU exposing 8 CPUs to a VM, measured
  over two hours with the harness's contention checks. Other core counts
  and memory bandwidths will move the ratios.
- ClickBench ran only on `hits.parquet` in its published order, at 8
  partitions. The sorted copy and other partition counts were not run.
- The spike is a measurement tool. It is not memory-safe under a bounded
  pool, it re-plans every wave, and its shared threshold is correct for a
  single execution of a plan (LTSeq plans every terminal afresh).
- Stage shares beyond 8 partitions on 8 CPUs are not CPU time (see Setup).
