<!-- v0.5-modular:header -->

# v0.5 review history: API surface and interchange follow-ups (R)

[Index](../../README.md) › Review history · Previous: [Owner decision closure (Q)](owner-decision-closure.md)

**Non-normative, historical.** A dated record of the [review](../../README.md#ltseq-v05-api-review), kept as written. Later records name the earlier records they change. [Deliverable Q] is the decision register, and this record settles three of its deferred rows.

**Scope.** The rows of #278: the API surface and Arrow interchange questions that the PR #250 assessment classed as not blocking the merge. Each row's resolution, the choices that were not obvious, the probes behind them, and what is still open.

**Most cited from here.** [§2] Exports · [§16] Output and interchange · [§21] Performance contract · [§7] Basic table operations

<!-- /v0.5-modular:header -->

## R. API surface and interchange follow-ups

#278 listed seventeen rows, each to be settled in the contract before the work item that implements it, or before the release. This pass, of 2026-11-10, settles every row in the contract text and names the [§24] row that tests it. It changes no code: each rule is implemented by the M item in the last column, and the [§24] rows named are specifications until then.

### Owner rows

The owner recorded a choice for each of these on 2026-10-09 ([Deliverable Q]). This pass applies those choices; it makes none of its own.

| ID | Owner's choice | Applied in | Rows | Gate |
|---|---|---|---|---|
| AC3 | Prefer `len(t)` raising `TypeError` that points to `count()` over a hidden execution | [§3.2], [§5.3], [§22] (`__len__` is `NoReturn`) | S3 | M27, M29 |
| U2 | Add neither tightening now | [§21.1] says that `tail(n)` has no bound of *n* rows and that `distinct` stays out of the MUST list, by choice | R3 | Release |
| OD-E | The general path is the reference; a fast path ships only if it passes the same parity tests, and removing an unsafe one is preferred to accepting wrong results | [§21.2] | P5, F1 | M17 |
| AC17 | Exact conversion within a kind for declared and requested types | Already applied by Q ([§17.6]); nothing changed here | L3, L7, X3, D9, W9 | M18, M22 |

AC3 is the one place this pass turns a stated preference into a rule: Q recorded "prefer", with the contract change "Yes, if `len` raises". The final merge review should confirm it. The evidence for the preference: under CPython 3.13 and pandas 3.0.5, a class whose `__len__` raises `TypeError` and whose `__iter__` yields dicts gives the expected result from `list`, `tuple`, `sorted` and `pandas.DataFrame`, each iterating once, because Python treats a `TypeError` from the length hint as no hint. With an executing `__len__`, each of those runs the plan twice.

For U2, #274's R3 harness measured both candidates over 640 MiB of decoded Parquet at 2 partitions (#278, harness merged in PR #282). `tail(10)` held about 5 MiB, about what `count` holds on the same input, so a bound of *n* rows would match current behavior on that input. Keyed `distinct` went above 256 MiB, because it snapshots its input into one partition; moving it into the MUST list would require replacing that snapshot. Neither measurement decides the row, and neither tightening is needed for correctness, so the owner's choice stands. R3 now says that a run of either above the budget does not fail.

### Other rows

| ID | Resolution | Contract | Rows | Gate |
|---|---|---|---|---|
| AC6 | Every integer argument (counts, positions, offsets, widths, digits) is annotated `SupportsIndex` and read once with `operator.index`; a `bool` raises `LTSeqTypeError`, and so does a value without `__index__` | [§2.3]; signatures in [§4.7], [§7.9], [§7.10], [§8.5], [§9.8], [§9.9], [§10.1], [§10.2], [§15.1], [§22] | S2, W2, D10 | M18 |
| AC7 | For a terminal or eager call, the call and execution stages raise at the same moment, and the contract says so. The stages still differ observably: a call-stage error comes from the call's own checks, not from the demand rule | [§20.2] | E2 | M15 |
| AC8 | CPython 3.13+, pyarrow 25.0.1+, and the optional pandas 3.0.5+ and NumPy 2.4.4+, declared as extras; a floor is the oldest version CI tests | [§2.4] (new) | S6 (new) | Release |
| AC9 | `ltseq.typing.Literal` is renamed `LiteralValue`, and its value evaluates with or without NumPy | [§2.2], [§2.3], [§22] and every signature that used it | S1, S2 | M26 |
| AC10 | A `str`, `bytes` or `bytearray` as `is_in`'s `values` raises `LTSeqTypeError` | [§8.5], [§22] preamble | D7 | M12 |
| AC11 | One rule for `select` and `derive`: each key is carried under a result column that holds its column, by keeping it or by a bare reference, and `sort_keys` is truncated before the first key nothing holds | [§7.2], [§7.3], [§9.2], [§11.2] | B2, B3, O1, G3 | M2 |
| AC12 | `t.concat()` equals `t` except that `sort_keys` is `None`; the SQL `UNION` idiom is `concat(...).distinct(keep="any")` | [§13.1], [§13.2], [§23.16] | U2 | M28 |
| AC13 | Idioms for `top_k` (a `row_number` filter, then `string_agg` for the old string) and `skew` (central moments) | [§14.2] | A2 | M28 |
| AC16 | Kept: `r["x"]` raises `ColumnNotFoundError`, an `AttributeError` and not a `KeyError`, with the reason | [§8.1] | E1 | M15 |
| X8 | Each scan reads its source as it is when the scan opens it, so two scans in one execution may differ; `collect()` is the snapshot | [§5.1] | Z1 | M1 |
| Run-end, list views | Run-end encoding is normalized to its value type, as dictionaries are; `list_view` and `large_list_view` are normalized to `list` and `large_list` | [§6.2] | T2 | M18 |
| Importer exceptions | The code and the message prefix are normative; the class a consumer raises is informative | [§16.4] | X3, P4 | M1 |
| N20 | `float32` text is `repr(float(numpy.format_float_positional(x, unique=True)))`, laid out as `float64`'s `repr`, with the exponent form outside 10⁻⁴ ≤ \|*d*\| < 10¹⁶, where *d* is that shortest decimal and not the binary value | [§16.5] | X4 | M22 |

#294, which a comment on #278 placed next to AC10, was closed by PR #385 before this pass: [§8.5] and D7 already refuse expression items.

### Judgment

**AC6 covers every integer parameter, not only the five #278 named.** A rule for `head`, `slice`, `shift`, `rolling` and `ntile` alone would have left `show`, `insert`, `with_row_index`, `to_batches`, `round` and the string methods with `int` annotations, so `t.head(np.int64(3))` would type-check and `t.show(np.int64(3))` would not. `LTSeq.range`'s arguments are values rather than counts, but Python's own `range` reads them with `__index__`, so they follow the same rule. A `bool` is refused although `True.__index__()` is 1, because `head(True)` or `shift(False)` is almost always a flag passed in the wrong place; the annotation cannot express this, since `bool` is a subclass of `int`, so [§22]'s preamble lists it among the refusals checked at run time. `delete` and `update` already took `int | numpy.integer` and refused `bool`; their annotation becomes `SupportsIndex` with no change of meaning for those types.

**AC11 carries keys in both methods rather than in neither.** Truncating in `select` too would have been simpler, but it would drop an order that `select(y=lambda r: r.x)` keeps today and that costs nothing to keep. The carried key needs a column whose values are those of the key column; the contract does not decide which expressions preserve values, so only a bare reference counts, and `derive(x=lambda r: r.x + 0)` truncates. Under the rule, `derive(x=lambda r: r.x)` keeps the key where [§7.3] used to truncate it, and a swap `derive(a=r.b, b=r.a)` carries the keys under each other's names. The rule records one name per key, so it does not make `derive(y=lambda r: r.x).drop("x")` equal `select(y=lambda r: r.x)`: the derive keeps the key as `x`, the column of its own name, and the drop then truncates it, as [§7.5] says for any dropped key. Carrying the key over to `y` at the drop would need the order state to record every column that holds a key's values and to keep that record through every later operation, for one idiom that `rename` and `select` already cover. B3 tests what the rule gives.

**AC16 is kept.** `r.x` and `r["x"]` are one column reference spelled two ways, so they fail with one class (principle 2), and the same class is what `select`, `sort` and every other method raise for an unknown name. The proxy is not a mapping (no `keys`, `get` or `in`), so no code reads it under `except KeyError`. Adding `KeyError` as a base is possible (a probe of a class with `ValueError`, `AttributeError` and `KeyError` bases keeps `str()` as the message, without the quotes `KeyError` adds), and it would be a change to [§20.1], which OD-F (#266) is reviewing. Keeping the class unchanged leaves that review free.

**AC8 states as floors the versions CI already tests.** `pyproject.toml` already declares the two required floors (`requires-python = ">=3.13"`, `pyarrow>=25.0.1`), and `uv.lock` pins pandas 3.0.5 and NumPy 2.4.4, which CI installs on CPython 3.13 and 3.14. A floor below a tested version would be a claim with no test behind it, which the issue's second invariant rules out. NumPy 2.4.4 is probably higher than LTSeq needs; lowering it is possible once a CI job runs the oldest versions. pandas and NumPy are not dependencies, so `pyproject.toml` can state their floors only as extras (`py-ltseq[pandas]`). [§2.4] requires the extras `pandas` and `numpy`, so that the metadata states every floor and S6 can check it against the contract; adding them is a packaging change, left to the release, which is AC8's gate. S6 also requires the CI job that installs each floor exactly, which is what makes "the oldest version CI tests" true once `uv.lock` moves past the floors, and what lowering a floor would need.

**AC9 needs the alias to evaluate, not only to be renamed.** The alias is a `type` statement that names NumPy types under `TYPE_CHECKING`, so reading its value at run time raised `NameError` whenever NumPy had not been imported into the module. Because a `type` statement's value is evaluated only when it is read, the module can import NumPy at that point, and build the union without NumPy's types when the import fails. [§2.2] states the outcome and leaves the mechanism to M26.

**List views are normalized, not passed through.** `list_view` is a layout of a list, as `string_view` is of a string, and [§6.2] already reports `string_view` as `string`. Passing it through would expose a layout that DataFusion may or may not keep across an operation. Normalizing gives a consumer `list`, which every reader takes; a consumer that wants the view can ask for it through `requested_schema` ([§6.2]).

**N20 does not take NumPy's `str` as the reference.** NumPy switches a `float32` to the exponent form at different magnitudes than Python does for a `float` (`str(np.float32(1e15))` is `1e+15` and `str(np.float32(16777216))` is `1.6777216e+07`, where `repr(1e15)` is `1000000000000000.0`), so two float columns would have two layouts. The reference keeps NumPy's shortest digits and Python's layout. The bounds apply to the shortest decimal *d*, as Python's `repr` applies them, because that is what the reference computes: `np.float32(1e-4)` is 0.0000999999974737875163555145263671875, below 10⁻⁴, and the reference writes it `0.0001` (NumPy 2.4.4 and 2.5.4), which is the form a reader expects. Deciding by the binary value instead would write `1e-04` for a number whose shortest decimal is 0.0001, and would need a reference of its own. X4 tests that value and its neighbors. Over the finite values among 300,000 random `float32` bit patterns (NumPy 2.4.4), the form read back to the same `float32` and had the same significant digits as `numpy.format_float_scientific(x, unique=True)` in every case.

### Checks

- pyright 1.1.414 (standard mode) over [§22]'s two stubs, built as S2 describes, reports no error in them, before and after this pass. After it, the calls `head(np.int64(3))`, `slice(np.int64(1), np.int64(2))`, `r.x.shift(np.int64(1))`, `r.x.rolling(np.int64(3), min_periods=np.int64(1))`, `ntile(np.int64(4))`, `.str.slice(np.int64(0), 2)`, `round(np.int64(1))`, `LTSeq.range(np.int64(5))` and `to_batches(np.int64(8))` type-check; before it, pyright reported 16 errors on the NumPy integers of these and similar calls.
- Every `[§N]` reference in the contract modules resolves to a heading.

### Earlier records this pass changes

- [Deliverable Q]'s Deferred table: AC3, U2 and OD-E are settled here. Q is kept as written, with a pointer under its table.
- [Deliverable B]: the `len(t)` row moves from KEEP to REDESIGN, the counts change with it, and the rows for the type aliases and `is_in` name the new behavior.

### Open

- **Owner confirmation of AC3.** Q recorded a preference, and this pass applied it as a rule.
- **`read_parquet` of a file with no leaf columns whose footer records rows**, for example `num_rows = 5` from a writer other than pyarrow or the `parquet` crate. [§6.1] suggests a 5-row table without columns, but how DataFusion reads such a file was not probed. A comment on #278 asked for a probe and one sentence in [§4.3] before M22; it is still needed.
- **The D12 baseline defects** listed in Q (`from_rows([{}, {}]).count()` is 0, transforms on a table without columns raise `Schema not initialized`, the writers lose rows silently) remain for M22.
- **pandas and NumPy extras** in `pyproject.toml`, and a CI job at the floors once `uv.lock` moves past them, before the release.

<!-- v0.5-modular:footer -->

---

Previous: [Owner decision closure (Q)](owner-decision-closure.md) · [Index](../../README.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§2]: ../../contract/public-surface.md#2-exports
[§2.2]: ../../contract/public-surface.md#22-other-modules
[§2.3]: ../../contract/public-surface.md#23-type-aliases-used-in-signatures
[§2.4]: ../../contract/public-surface.md#24-supported-versions
[§3.2]: ../../contract/public-surface.md#32-ltseq-invariants
[§4.3]: ../../contract/loading-and-laziness.md#43-ltseqread_parquet
[§4.7]: ../../contract/loading-and-laziness.md#47-ltseqrange
[§5.1]: ../../contract/loading-and-laziness.md#51-plan-building-and-execution
[§5.3]: ../../contract/loading-and-laziness.md#53-eager-calls
[§6.1]: ../../contract/schema-and-table-operations.md#61-schema-and-columns
[§6.2]: ../../contract/schema-and-table-operations.md#62-supported-types
[§7]: ../../contract/schema-and-table-operations.md#7-basic-table-operations
[§7.2]: ../../contract/schema-and-table-operations.md#72-select
[§7.3]: ../../contract/schema-and-table-operations.md#73-derive
[§7.5]: ../../contract/schema-and-table-operations.md#75-drop
[§7.9]: ../../contract/schema-and-table-operations.md#79-count-and-show
[§7.10]: ../../contract/schema-and-table-operations.md#710-value-level-edits-insert-delete-update
[§8.1]: ../../contract/expressions.md#81-contexts-and-proxies
[§8.5]: ../../contract/expressions.md#85-general-expr-methods
[§9.2]: ../../contract/ordering.md#92-sources-and-propagation
[§9.8]: ../../contract/ordering.md#98-positional-selection
[§9.9]: ../../contract/ordering.md#99-with_row_index
[§10.1]: ../../contract/windows-and-grouping.md#101-window-methods
[§10.2]: ../../contract/windows-and-grouping.md#102-ranking-functions
[§11.2]: ../../contract/windows-and-grouping.md#112-nestedtable
[§13.1]: ../../contract/joins-and-sets.md#131-concat
[§13.2]: ../../contract/joins-and-sets.md#132-intersect-and-difference
[§14.2]: ../../contract/aggregation.md#142-aggregate-expressions
[§15.1]: ../../contract/streaming-and-output.md#151-to_batches
[§16]: ../../contract/streaming-and-output.md#16-output-and-interchange
[§16.4]: ../../contract/streaming-and-output.md#164-arrow-pycapsule-stream
[§16.5]: ../../contract/streaming-and-output.md#165-writers
[§17.6]: ../../contract/numeric-null-temporal.md#176-explicit-casts
[§20.1]: ../../contract/errors-and-performance.md#201-exception-classes
[§20.2]: ../../contract/errors-and-performance.md#202-stages
[§21]: ../../contract/errors-and-performance.md#21-performance-contract
[§21.1]: ../../contract/errors-and-performance.md#211-materialization
[§21.2]: ../../contract/errors-and-performance.md#212-fast-paths
[§22]: ../../contract/api-reference.md#22-complete-canonical-api-reference
[§23.16]: ../../contract/examples-semantics.md#2316-set-and-bag-operations
[§24]: ../../contract/acceptance.md#24-acceptance-criteria-and-contract-test-matrix
[Deliverable B]: ../inventory.md#b-api-inventory-and-review
[Deliverable Q]: owner-decision-closure.md#q-owner-decision-closure

<!-- /v0.5-modular:links -->
