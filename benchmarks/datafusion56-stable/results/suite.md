# Guard ablations on stable 56 (#296, PR #423)

Each LTSeq optimizer rule left out of every session, on both engines, running
the tests that cover it, as the #411 archive did on RC1
(`benchmarks/datafusion56/results/suite.md`), with the same rule-to-test
mapping.

Builds (debug, `maturin build --locked`, rustc 1.97.1, CPython 3.13; the
same machine as `../README.md`), each `.so` placed in its own tree's
`py-ltseq/ltseq/` and the tests run from that tree's root, so each engine
runs its own tests:

- **55.0.0**: `291aa14` with `benchmarks/datafusion56/df55-spike-switches.patch`
  (`LTSEQ_SPIKE_SKIP`, `LTSEQ_SPIKE_TRACE_INNER`; no-ops unless set).
- **56.0.0 stable**: PR #423's tree at `f3c9404` (its Rust sources are those
  of the reviewed head `5f93a2e`) with
  [`../df56-stable-switches.patch`](../df56-stable-switches.patch):
  `LTSEQ_SPIKE_SKIP=<rule>[,<rule>]` leaves LTSeq rules out,
  `LTSEQ_SPIKE_UNGUARD=<rule>[,<rule>]` runs DataFusion rules without their
  `rewrite_guard` guard, and `LTSEQ_SPIKE_TRACE_INNER=<file>` records a
  sorted scan's inner plans. Each is a no-op unless set; the patch is
  evidence-only and is not part of the PR.

```bash
LTSEQ_SPIKE_SKIP=<rule> python -m pytest -q -p no:cacheprovider -n 4 <test files>
LTSEQ_SPIKE_UNGUARD=push_down_filter python -m pytest -q -p no:cacheprovider -n 4 \
    py-ltseq/tests/test_rewrite_guards.py py-ltseq/tests/test_compound_in_lists.py
```

Control: the six files below with every rule in and no variable set; on 56
also `test_compound_in_lists.py`, which this PR adds (its 55-red cases are in
`matrix.md`, `probes.md` and `witnesses.md`, not in a 55 test run).

| Rule left out | Tests | 55.0.0 (`291aa14`) | 56.0.0 stable | Reading |
|---|---|---|---|---|
| none (control) | `test_declared_order.py`, `test_is_in_float_zero.py`, `test_offset_slice.py`, `test_parquet_float_statistics.py`, `test_sort_order.py`, `test_rewrite_guards.py` (56: and `test_compound_in_lists.py`) | 3,502 passed, 1 xfailed | 3,692 passed | 55's xfail is #296's strict xfail; on 56 it is a regular test |
| `ltseq_keep_file_order` (#325) | `test_declared_order.py` | 9 failed, 2,144 passed | (removed by this PR) | 56 reads a declared-sorted scan's files in order itself (apache/datafusion#26434); the control's `test_declared_order.py` passes without the rule |
| `ltseq_float_list_zeros` (#245) | `test_is_in_float_zero.py` | 66 failed, 299 passed, 1 xfailed | 1 failed, 365 passed | No wrong answer on 56. The one failure, `test_two_columns_holding_zeros_of_either_sign`, asserts the rule's own rewrite in the logical plan (`x IN ([Float64(1), Float64(2.5), Float64(7)])`); without the rule 56 keeps both lists, and the answers are right |
| `ltseq_keep_sorts_above_offsets` (#339) | `test_offset_slice.py` | 18 failed, 292 passed | 310 passed | Redundant for what the tests cover |
| `ltseq_drop_float_statistics` (#291) | `test_parquet_float_statistics.py` | 93 failed, 5 passed | 53 failed, 45 passed | Still needed |
| `ltseq_keep_stable_sorts` (X5) | `test_sort_order.py`, `test_rewrite_guards.py` | 9 failed, 567 passed | 36 failed, 648 passed | Still needed; 56's 36 include 27 of `test_a_filter_that_is_never_true_after_a_sort_evaluates_its_keys`, a test the 55 tree does not have |
| `ltseq_no_zero_fetch_sorts` | `test_rewrite_guards.py` | 108 failed, 351 passed | 108 failed, 459 passed | Still needed |
| `ltseq_keep_demanded_keys` (X4) | `test_rewrite_guards.py` | 27 failed, 432 passed | 27 failed, 540 passed | Still needed |
| `push_down_filter`'s guard (`KeepBarriers`, including B2's extension) | `test_rewrite_guards.py`, `test_compound_in_lists.py` | (not run) | 54 failed, 594 passed | Still needed: the 54 are `test_a_contradiction_beside_a_failing_conjunct_keeps_its_error` (27, B2) and `test_a_filter_that_is_never_true_after_a_sort_evaluates_its_keys` (27); `test_compound_in_lists.py` passes without it |

56's `test_rewrite_guards.py` holds 108 more tests than 55's (the B2 and
sort-key tests this PR adds), which is why its pass counts are higher.

"Redundant for what the tests cover" is evidence for removing a rule, not
proof: each rule's tests were written against 55's failure. This PR removes
only `ltseq_keep_file_order`.

The full Rust and Python suites and clippy on the PR's head are CI's, on
Linux and macOS (the owner's decision: macOS comes from CI).
