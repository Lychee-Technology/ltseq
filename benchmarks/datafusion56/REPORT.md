# DataFusion 56 feasibility report (#411)

#296's owner decision (D1 = wait for 56, D2 = spike first) asks whether
DataFusion 56 fixes the simplifier's set rewrites of compound `IN` lists
without LTSeq-side logic, and what an upgrade would cost, before an upgrade
PR is chosen. This spike built LTSeq against **DataFusion 56.0.0-rc1**
(commit `cce6374e6256b3b8892ec60674578cf32bc3a32c`, the `56.0.0-rc1` tag and
the head of `branch-56` on 2026-11-17; arrow and parquet 60.1.0) in a
throwaway copy and ran the same checks on it as on 55.0.0. Nothing in this
PR changes the shipped build.

**Recommendation: Go, conditionally, for a separate upgrade PR, built on
the published 56.0.0 release, not on this candidate.** 56 fixes #296's
rewrites: every counterexample this spike could build is right on 56,
except on run-end-encoded columns (B1), and the plans show the two tests
kept apart, not hidden by another plan shape. The migration is small (nine
compile errors in four files of the default build). But 56 also brings two correctness
regressions that LTSeq's tests catch only in part, and two contract-visible
behavior changes, so the upgrade PR must carry fixes for B1 to B4 below and
rate two new optimizer rules before it can meet #296's acceptance gate.

| | Finding | Severity for the upgrade |
|---|---|---|
| A | #296 is fixed by 56: 302 matrix cases wrong on 55 are right on 56, the #296 strict xfail passes on the unsorted input that PR #402 kept failing, volatile values keep one draw per test on both | The goal; the xfail becomes a regular test |
| B1 | A run-end-encoded value tested against a list of four or more items holding a NULL gives FALSE where SQL gives NULL, on Int64 and float64 in the matrix (558 cases) and on strings in a probe; 4 suite tests. 55 decoded the value first; 56 coerces the list to the encoding instead | **Blocker**: wrong answers. LTSeq-side fix (decode the receiver, as #292 does elsewhere) or an upstream fix |
| B2 | `push_down_filter` now folds `Filter(false)` into an empty relation (apache/datafusion#26132), dropping a sort key that can fail and that D10a demands; LTSeq's `KeepBarriers` guard lets it through | **Blocker** (§20.2). The patch's guard extension fixes the Rust test; no Python test reaches it |
| B3 | A cast error raised inside the Parquet reader's pushed-down filter is wrapped as `Parquet error: External: ... caused by Arrow error: ...` (11 suite tests) | Must fix: filter and `derive` raise different messages for one error |
| B4 | `decimal128(10, -2) == "1"` failed at planning on 55; 56 rounds `"1"` to the column's scale, 0, and answers TRUE for 0 | Must fix: an error became a silent wrong answer (literal grid, #276 N14) |
| C | Rule inventory: rate `eliminate_aggregate_distinct` and `EliminateConstantJoin`, drop `SanityCheckPlan`, re-read the expression rewrites for 56 | Required by `src/rule_inventory.rs` |
| D | `KeepFileOrder`, `FloatListZeros` and `KeepSortsAboveOffsets` are redundant on 56 for everything their tests cover; `DropFloatStatistics`, `KeepStableSorts`, `NoZeroFetchSorts` and `KeepDemandedKeys` are not | Candidates for removal in the upgrade PR or after it |

## The candidate and the fixes in it

56.0.0 is not released: apache/datafusion#24461 is open, and `56.0.0-rc1`
is a release candidate. The spike pins its commit (`datafusion = { git =
..., rev = "cce6374e..." }`); the upgrade PR should use the crates.io
release, and rerun this spike's matrix and ablations on it.

Both upstream fixes are in the candidate's source tree, not inferred from the
version: commits `2648cdae8` (apache/datafusion#24952) and `46bbf0db1`
(apache/datafusion#24258) are ancestors of `cce6374e`, and at that commit
`expr_simplifier.rs` holds `inlists_have_set_comparable_literals` (#24952:
set algebra only on non-NULL, non-float literal items, no volatile operand)
and `simplify_inlist_set_operation` (#24258: a constant result keeps the
NULL branch of a nullable operand). Both are in `dev/changelog/56.0.0.md`.

## #296's counterexamples (`repro_296.py`, `results/matrix.md`)

The matrix asks every compound question of the form `A op ~B` (and its
negation) that today's API can write, with `op` `&` or `|`, `A` and `B`
membership tests of one value written as `is_in`, as `==` joined by `|`, or
as `~` of `!=` joined by `&`. Lists are below and above DataFusion's
three-item unfolding threshold, and hold NULL, both signed zeros, a column
that varies by row, or a constant staged in a derived column (which
DataFusion writes in place of the column when it moves the filter). Each of
1,352 questions per type is asked as a projection and as a filter, over an
unsorted table and the same table sorted, for Int64, string, float64,
float32, float16, and dictionary- and run-end-encoded Int64 and float64.
The reference is SQL's three-valued logic in Python, with `x == None` read
as the DSL reads it, `IS NULL`.

| | 55.0.0 projection / filter wrong (each layout) | 56.0.0-rc1 |
|---|---|---|
| Int64, string, dictionary Int64 | 43 / 24 | 0 / 0 |
| float64 | 2 / 8 | 0 / 0 |
| float32, float16 | 0 / 4 | 0 / 0 |
| dictionary float64 | 0 / 0 | 0 / 0 |
| run-end Int64 | 3 / 3 | **180 / 0** (B1) |
| run-end float64 | 0 / 0 | **102 / 0** (B1) |

Sorted and unsorted inputs give the same counts here.

**Fixed, not masked.** 302 cases are wrong on 55 and right on 56; 176 of
them had a wrong filter, and for 164 of those the filter's optimized plan
differs on 56, in that both tests are still there. `x.is_in([1, 2, 5]) &
~x.is_in([None, 3, 4, 6])` was simplified on 55 to `x IN (1, 2, 5)` (TRUE
where the answer is NULL) and on 56 to `x IN (1, 2, 5) AND NULL`. The other
12 are sorted float tables whose staged zeros are derived below the sort:
their outer plans are the same on both engines, because `TableOrder::scan`
plans the filter pushed into it in a plan of its own, which `explain_plan`
does not show, and it is there that 55 intersects the lists. So a sorted
input does not hide #296 in general; it hides it in the xfail below only
because there the zeros are derived above the sort.

**The strict xfail (PR #402).** `test_two_columns_holding_zeros_of_either_sign`
is wrong on 55 on the unsorted input it uses (the simplifier intersects the
two lists the optimizer built and plans an `EmptyRelation`) and right on the
sorted one, where the filter is pushed into the scan apart from the derive.
On 56 it is right on both, and the unsorted plan keeps both lists, each with
its zero range beside it. So the xfail's pass on 56 is a fix, and the
upgrade PR can make it a regular test. Note for that PR: 56's unsorted plan
prints the range's lower bound as `Float64(0)`, where the sorted plan prints
`Float64(-0)`. The matrix's answers with staged zeros are right on plain
and dictionary columns, but the reason should be understood before
`FloatListZeros` goes.

**#296 against #298.** The volatile cases test two draws of `rand()`, each
named once, joined by `&` or `|`: two independent draws give a share of TRUE
rows of 0.25 or 0.75; one merged draw gives 0 or 1. On both engines the
inline form gives the two-draw share and the staged control (one draw in a
column, both tests on it) the one-draw share, so neither engine merges
volatile lists here. That is #296's volatility half (apache/datafusion#24952),
which LTSeq already avoided on 55 by naming a volatile value once. #298, an
item that varies by row compared beside a volatile value by sign, is a
property of LTSeq's own lowering (`literals::membership`), which 56 does not
touch; this spike does not claim it fixed.

**The contracts kept.** `is_in` still refuses expression items (#294;
`test_is_in_literal_items.py` passes on 56); the matrix reaches column items
only through `==`. v0.5 §8.5's later never-NULL membership contract was not
implemented or changed here; on both engines `is_in` gives SQL's NULL, which
is today's shipped behavior.

## Findings that block or shape the upgrade

**B1: run-end-encoded `IN` lists lose NULL.** On 55 the plan is
`CAST(x AS Int64) IN (SET) ([NULL, 1, 2, 7])`; on 56 type coercion keeps the
encoding and casts the items instead, `x IN (SET) ([NULL, 1, 2, 7])`, and the
set filter over the encoded input returns FALSE for a value not in the list.
Lists of three or fewer items, which DataFusion unfolds into `=`, are right,
and so are filters (FALSE and NULL both drop the row); projections and
negations are wrong. Not bisected; the IN-list filters were rewritten
several times between 55 and 56. The narrowest LTSeq fix is to decode a
run-end-encoded receiver before an IN list, as `literals::decoded_float`
does for floats since #292; it should also go upstream.

**B2: `Filter(false)` over a fallible key.** apache/datafusion#26132 made
`push_down_filter` replace a filter whose predicate is the literal FALSE
with an empty relation, so a scan that would absorb the FALSE does not hide
the contradiction. The trace in `spike411_probe_eliminate_filter` shows it:
after `ltseq_keep_demanded_keys` places its barrier, `push_down_filter`
turns `Filter: false / Sort: 10 / x / ...` into `EmptyRelation`, and the
division by zero §20.2 demands is never evaluated. `Guard::KeepBarriers`
checks only that no filter moves below a barrier. The patch also refuses a
`push_down_filter` rewrite that drops a demanded fallible expression, as
`KeepFailures` does for `eliminate_filter`; with it,
`plan_guard_eliminate_filter` passes and no other test changes. The Python
suite passes with or without it, so the upgrade PR should add a Python test.

**B3: Parquet filter errors are wrapped.** The 11 failing
`test_cast_comparison.py` cases compare the message of a failing cast in a
filter with the same cast in `derive`. On Parquet layouts 56's reader
evaluates the pushed-down filter and wraps its error twice. The values are
right; the class and message are not what `derive` raises. LTSeq's error
mapping needs to read the error at the bottom of DataFusion's chain for
these too (the stream spike's `classify.rs` already walks the chain).

**B4: a string literal against a decimal of negative scale.** The literal
grid's `dneg` row is `decimal128(10, -2)`. 55 refused `x == "1"` with an
Arrow cast error; 56 casts `"1"` to `decimal128(10, -2)`, which rounds it to
0, so the row holding 0 compares TRUE. Contract item #276 N14 already says
such a column cannot hold the literal; the upgrade PR needs LTSeq's literal
placement to refuse it rather than inherit 56's cast.

**`d256`** (not a regression): arrow 60 casts `decimal256` 10^44 to the
nearest double, where arrow 59 truncated it to 9.999999999999999e43. The
grid's expected value follows arrow 59 and needs updating.

## Rule inventory and plan guards (#273)

`src/rule_inventory.rs` fails on 56 as designed. 56 adds
`EliminateAggregateDistinct` (logical, after `push_down_filter`:
`min(DISTINCT x)` becomes `min(x)`) and `EliminateConstantJoin` (physical,
after `AggregateStatistics`: a join with a one-row constant side becomes a
filter and projection over the other input). Both need an error and an order
rating; `EliminateConstantJoin` replaces a join, so its order rating should
be checked against `join_selection`'s `NoSwap` guard (X5). `SanityCheckPlan`
is no longer in the physical rule list; 56 runs it as a final check in the
planner, so its row goes. No existing rule was removed or reordered
otherwise.

Four plan-guard tests fail only in their controls, the half that shows
DataFusion's unguarded rule doing the hazardous thing; the guarded halves
pass. 56 lowers a `CASE` to `AND`/`OR` only when every condition and branch
is a column or literal, so the R3-F2 and R6-F1 controls no longer rewrite;
56's own `join_selection` no longer fails on clashing schema metadata; and
an uncoerced `CASE` now reports its coerced type. The controls need new
witnesses, or the guards a recorded reason to stay; this spike keeps every
guard.

The guard ablations (`results/suite.md`) are the evidence for D. On 55,
leaving out `KeepFileOrder`, `FloatListZeros` or `KeepSortsAboveOffsets`
fails 9, 66 and 18 tests; on 56, none beyond the control's. The other four
LTSeq rules fail the same tests on both engines without them (for
`DropFloatStatistics`, 93 on 55 and 53 on 56). A rule's tests were written
against 55's failure, so passing without it on 56 is evidence for removal,
not proof; the upgrade PR or a follow-up should remove each with a test that
looks for its hazard in 56's code.

## Migration inventory (`df56-migration.patch`)

The patch is what this spike needed, not the upgrade PR's diff.

| Where | 55 API | 56 / arrow 60 / parquet 60 |
|---|---|---|
| `src/arrow_ffi.rs` (PyCapsule import, #143) | `FFI_ArrowArrayStream`'s callback fields | private fields; accessor methods `get_schema()`, `get_next()`, `get_last_error()`, `release()` |
| `src/float_statistics.rs` (#291) | `ParquetMetaData::column_index()`, `ParquetMetaDataBuilder::take_column_index` / `set_column_index`, `ColumnIndexMetaData::NONE` | a `PageIndexProvider` trait object (`take_page_index` / `set_page_index`). The patch wraps the reader's provider and hides float leaves' column index; the offset index passes through |
| `src/ops/lookup.rs` (#351) | `aggregates::order::GroupOrdering` | renamed `GroupClustering` |
| `src/ops/sort.rs` (`TableOrder`) | `TableProvider::scan(projection: Option<&Vec<usize>>)` | `Option<&[usize]>` |
| `src/engine.rs` (`KeepFileOrder`) | `ExecutionPlan::with_preserve_order` | deprecated, never called by 56 |
| `src/transpiler/no_order_claim.rs` | `AggregateUDFImpl::is_nullable` | deprecated in favour of `return_field` |
| `src/stream_spike/capsule.rs` (feature `stream_spike`) | writes `FFI_ArrowArrayStream` fields; `ArrowArrayStreamReader` | not ported: needs `new_unchecked` / `set_release` / `set_private_data`, and `RecordBatchStreamReader` |

The lockfile goes from 294 to 299 packages: DataFusion 55.0.0 → 56.0.0,
arrow and parquet 59.2.0 → 60.1.0 (adding `arrow-cmp`), object_store
0.13.2 → 0.14.2, sqlparser 0.62 → 0.63, brotli 8 → 9, zstd 0.13 → 0.14
beside the old one, and smaller bumps. pyo3 stays at 0.29 (arrow-pyarrow 60
requires `^0.29`), so the PyO3 boundary and `pyproject.toml` are unchanged.
`LocalFiles` (`object_store`'s trait) compiled unchanged on 0.14.

## Performance

`results/bench.md`. Release builds of both engines, built alike (thin LTO,
not the shipped fat LTO), on an otherwise idle 8-CPU machine.
`bench_core.py`'s 47 benchmarks give a geometric mean of 0.995 for 56 / 55,
every ratio between 0.94 and 1.05, all within run-to-run noise: no
regression and no speedup to claim. Where #296's fix applies it has a
cost: on 10 million rows, intersecting two float lists takes 8.5 ms on 56
against 5.3 ms on 55 (1.6 times), because 56 evaluates both lists rather
than intersecting them. The Int64 difference with a NULL item, which 55
answered wrongly, drops from 8.6 ms to 5.3 ms. Both builds run every LTSeq
guard, so these numbers do not price removing the redundant ones.

## Commands and what was not run

`results/suite.md` lists each command and its outcome. Not run: `pyright`,
the tutorial notebooks, the memory harness, the macOS job, and
`maturin build` of a wheel with the shipped fat-LTO profile (the benchmark
builds use thin LTO). The stream spike feature does not compile on 56.

## Proposed upgrade scope and follow-up work

One PR, after 56.0.0 is on crates.io:

1. The migration above, on the release, with the deprecations handled
   rather than allowed (`with_preserve_order` goes with `KeepFileOrder`, see
   D; `is_nullable` is dropped from the wrapper).
2. Fixes for B1 (decode a run-end-encoded IN-list receiver), B2 (the
   `push_down_filter` guard and a Python test), B3 (read the root error of
   a pushed-down Parquet filter) and B4 (refuse a literal a negative-scale
   decimal cannot hold), each with its test.
3. The inventory: ratings for the two new rules, the `SanityCheckPlan` row,
   the 56 marker, and new witnesses for the four guard controls; the literal
   grid's `d256` expectation.
4. #296's strict xfail becomes a test; #296 closes when its acceptance gate
   is met on the release.

Separately: port or retire the `stream_spike` feature (M1 replaces it);
decide on removing `KeepFileOrder`, `FloatListZeros` and
`KeepSortsAboveOffsets` with tests that target their hazards on 56; report
B1 and B2 upstream. #298 stays open.
