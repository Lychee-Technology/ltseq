# Suites, lints and guard ablations (#411)

Machine: 8 CPUs visible (Intel Core i7-8700K under Proxmox), 23 GiB, Linux
7.0.14. rustc 1.97.1, CPython 3.13 (uv), pyarrow from `uv.lock`, maturin
debug builds (`maturin develop`, as CI builds them) unless noted.

- **55.0.0**: this branch's tree, unchanged (the matrix), or with
  `df55-spike-switches.patch` (the ablations, `--plans`, `witnesses.py`).
- **56.0.0-rc1**: the same tree with `df56-migration.patch` applied and
  `df56-Cargo.lock` as its `Cargo.lock` (built `--locked`; see the README
  for the commands).

The patch carries spike-only code the upgrade PR would not ship as is: the
`push_down_filter` guard extension (finding B2 in the report, on unless
`LTSEQ_SPIKE_NO_PDF_GUARD=1`), the `LTSEQ_SPIKE_SKIP` and
`LTSEQ_SPIKE_TRACE_INNER` switches (no-ops unless set), a trace test
(`spike411_probe_eliminate_filter`), `resolve.rs`'s 55 control replaced by
a print, and `#[allow(deprecated)]` on two calls. The README's *Patch
states* table names the state (S0, S1, S2) each run below used and how to
repeat it on the published patch.

## Rust

| Check | 55.0.0 | 56.0.0-rc1 |
|---|---|---|
| `cargo check --all-targets` | passes (CI) | passes after the patch; before it, 9 errors in the library and 15 with tests (see the report's migration inventory) |
| `cargo clippy --all-targets -- -D warnings` | passes (CI) | passes, with the two deprecations allowed |
| `cargo clippy --all-targets --features m0 -- -D warnings` | passes (CI) | passes |
| `cargo clippy --all-targets --features stream_spike -- -D warnings` | passes (CI) | **fails**: `src/stream_spike/capsule.rs` writes `FFI_ArrowArrayStream`'s fields, which arrow 60 made private (8 errors in the library), and uses the deprecated `ArrowArrayStreamReader`. Not ported |
| `cargo test --locked --lib` | 235 passed | the published patch (S2): **231 passed, 5 failed** of 236 (the patch adds the trace test). With `LTSEQ_SPIKE_NO_PDF_GUARD=1`: 230 passed, 6 failed (adds `plan_guard_eliminate_filter`). With `resolve.rs`'s 55 control restored (from the root of `/tmp/ltseq-df56`: `git apply -R --include=src/transpiler/resolve.rs benchmarks/datafusion56/df56-migration.patch`): 230 passed, 6 failed (adds `a_case_is_typed_by_every_branch`). The first run, in S0 (no trace test, original control, no guard), gave 228 passed, 7 failed of 235 |

The 56 failures (the first five fail on the published patch; the last two
only in the variants named):

| Test | Why it fails |
|---|---|
| `rule_inventory::every_rule_a_session_runs_has_a_row_and_every_row_a_rule` | 56 runs `eliminate_aggregate_distinct` (logical) and `EliminateConstantJoin` (physical), which have no row, and no longer runs `SanityCheckPlan` as an optimizer rule. Working as designed: the rules need ratings |
| `rule_inventory::the_expression_rewrites_were_rated_for_the_datafusion_in_use` | The marker says 55.0.0. Working as designed |
| `rewrite_guard::tests::plan_guard_case_rewrites`, `plan_guard_nested_case_rewrites` | The guarded halves pass. The control, that DataFusion still lowers a `CASE` none of whose branches can fail to `AND`, does not: 56 lowers a `CASE` to `AND`/`OR` only when every condition and branch is a column or a literal (`can_lower_case_to_boolean`) |
| `rewrite_guard::tests::join_selection_keeps_clashing_schema_metadata` | The guarded half passes. The control, that DataFusion's own `join_selection` fails on clashing schema metadata, no longer fails |
| `transpiler::resolve::tests::a_case_is_typed_by_every_branch` (only with the 55 control restored; the published patch replaces that assertion with a print, so the test passes there) | The control, that an uncoerced `CASE` reports its first branch's type (`Timestamp(Second)`), now reports the coerced `Timestamp(Microsecond)` |
| `rewrite_guard::tests::plan_guard_eliminate_filter` (only with `LTSEQ_SPIKE_NO_PDF_GUARD=1`, or in S0) | `push_down_filter` turns `Filter(false)` over a sort whose key can fail into an empty relation (finding B2) |

## Python

`pytest py-ltseq/tests/ -n 6` (pytest-xdist in the throwaway virtualenv
only), with `test_is_in_literal_items.py` run serially: its parameter ids
hold object addresses, which differ between xdist workers, so xdist refuses
the whole run otherwise.

| | 55.0.0 | 56.0.0-rc1 |
|---|---|---|
| Full suite | not rerun here; CI runs it on `main` | S0 (17 Nov): 18 failed, 24,293 passed, 203 skipped (17 min 58 s); `test_is_in_literal_items.py`: 117 passed. Rerun on the published patch (S2, 18 Nov): the same 18 ids failed, 24,293 passed, 203 skipped (16 min 37 s); `test_is_in_literal_items.py`: 117 passed |
| The 18 failing ids, on 55 | 17 passed, 1 xfailed (the strict xfail of #296) | |

The 18, by cause:

| Tests | Cause |
|---|---|
| `test_cast_comparison.py`: 3 × `test_filter_agrees_with_derive_on_integer_casts`, 8 × `test_filter_agrees_with_derive_on_other_casts` (Parquet layouts) | A cast error raised while the Parquet reader evaluates a pushed-down filter now reads `Parquet error: External: External error: Error evaluating filter predicate caused by Arrow error: Cast error: ...`; `derive` raises the bare `Cast error`. Same error, different message (finding B3) |
| `test_is_in_float_zero.py`: 4 × `run_end_encoded` | A run-end-encoded value tested against a list of four or more items with a NULL gives FALSE where SQL gives NULL (finding B1) |
| `test_is_in_float_zero.py::test_two_columns_holding_zeros_of_either_sign` | Strict xfail of #296 now passes: XPASS(strict) is reported as a failure |
| `literal_grid/test_literal_grid.py::test_gate_no_regression_no_undecided[dneg]` | `decimal128(10, -2)` compared with the string `"1"`: 55 failed at planning ("Cannot cast string to decimal with negative scale"); 56 casts `"1"` to the column's scale, rounding it to 0, and `0 == "1"` is TRUE (finding B4) |
| `literal_grid/test_literal_grid.py::test_gate_no_regression_no_undecided[d256]` | `decimal256` 10^44 cast to `float64` is now the nearest double (1.0000000000000000882e44) where arrow 59 gave 9.999999999999999e43. A better answer; the grid's expected value follows arrow 59 |

Not run: `pyright`, the tutorial notebooks (`pytest --nbmake tutorials/`),
`tutorials/render_markdown.py --check`, the memory harness
(`-m memory`), and the macOS job. None of them was expected to move with the
engine, but none is evidence either way.

## The matrix and the other scripts

| Run | 55.0.0 | 56.0.0-rc1 |
|---|---|---|
| `repro_296.py` (`results/repro-*.json.gz`) | this tree unchanged | S0, 17 Nov. Rerun on the published patch (S2, 18 Nov): the same answer and the same plan digest for each of the 24,336 case-layout pairs, and the same xfail results |
| `repro_296.py --plans` (`results/plans-*.json`) | with `df55-spike-switches.patch` | S2 |
| `probes.py` / `df_probe` (`results/probe-*.jsonl`) | `df_probe/df55`, `cargo build --locked` (debug) | `df_probe/df56`, `cargo build --locked` (debug) |
| `witnesses.py` (`results/witnesses-*.json`) | with `df55-spike-switches.patch` | S2 |

## Guard ablations

Each LTSeq rule left out with `LTSEQ_SPIKE_SKIP`, on both engines (55: this
tree with `df55-spike-switches.patch`; 56: the patch in state S1, which
behaves as the published patch), running the tests that cover it, from the
patched tree's root:

```bash
LTSEQ_SPIKE_SKIP=<rule> <venv>/bin/python -m pytest -q -p no:cacheprovider -n 4 <test files>
LTSEQ_SPIKE_NO_PDF_GUARD=1 /tmp/venv56/bin/python -m pytest -q -p no:cacheprovider -n 4 py-ltseq/tests/test_rewrite_guards.py
```

with `<rule>` and `<test files>` from the table. Control: all six files with
every rule in, no variable set.

| Rule left out | Tests | 55.0.0 | 56.0.0-rc1 | Reading |
|---|---|---|---|---|
| none (control) | the six files below | 3,502 passed, 1 xfailed | 5 failed, 3,498 passed | 56's 5 are finding B1 (4) and #296's xpass |
| `ltseq_keep_file_order` (#325) | `test_declared_order.py` | 9 failed, 2,144 passed | 2,153 passed | Redundant for what the tests cover: apache/datafusion#26434 is in 56, and 56 deprecates `with_preserve_order` as never called |
| `ltseq_float_list_zeros` (#245) | `test_is_in_float_zero.py` | 66 failed, 299 passed, 1 xfailed | 5 failed, 361 passed: the control's 5, the same ids | Redundant for what the tests cover; 56 backports "IN LIST: treat signed zeros as equal" (#25186) |
| `ltseq_keep_sorts_above_offsets` (#339) | `test_offset_slice.py` | 18 failed, 292 passed | 310 passed | Redundant for what the tests cover |
| `ltseq_drop_float_statistics` (#291) | `test_parquet_float_statistics.py` | 93 failed, 5 passed | 53 failed, 45 passed | Still needed |
| `ltseq_keep_stable_sorts` (X5) | `test_sort_order.py`, `test_rewrite_guards.py` | 9 failed, 567 passed | 9 failed, 567 passed | Still needed |
| `ltseq_no_zero_fetch_sorts` | `test_rewrite_guards.py` | 108 failed, 351 passed | 108 failed, 351 passed | Still needed |
| `ltseq_keep_demanded_keys` (X4) | `test_rewrite_guards.py` | 27 failed, 432 passed | 27 failed, 432 passed | Still needed |
| the patch's `push_down_filter` extension (`LTSEQ_SPIKE_NO_PDF_GUARD=1`) | `test_rewrite_guards.py` | (not in 55) | 459 passed | No Python test reaches finding B2; only the Rust `plan_guard_eliminate_filter` does |

"Redundant for what the tests cover" is evidence for removing a rule in the
upgrade PR, not proof: each rule's tests were written against 55's failure,
and none was written to search for others.
