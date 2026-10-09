<!-- v0.5-modular:header -->

# v0.5 review history: Owner decision closure (Q)

[Index](../../README.md) › Review history · Previous: [Assessment closure pass (P)](assessment-closure.md)

**Non-normative, historical.** A dated record of the [review](../../README.md#ltseq-v05-api-review), kept as written. Later records name the earlier records they change, and [Deliverable Q] is the current decision register.

**Scope.** The owner decisions of 2026-10-09: what was accepted, modified (D8, D12, D14, AC17) and deferred, what the modifications required of the contract, and the earlier records they supersede. This is the current decision register.

**Most cited from here.** [§17] Numeric semantics and literals · [§4] Loading · [§14] Aggregation, partitioning and pivot · [§16] Output and interchange

<!-- /v0.5-modular:header -->

## Q. Owner decision closure

The owner recorded a decision on every open item in [a comment on this PR](https://github.com/Lychee-Technology/ltseq/pull/250#issuecomment-6074764677) on 2026-10-09, after reading the contract at `3bdffd6`. Fourteen are accepted as applied, four are modified, and twelve are deferred to a named gate; the PR goes to final merge review once the modifications are applied and verified. This pass applied the four modifications, stated what the owner asked to be stated for D6, D10a and D13, and updated the contract's status line; nothing else in the contract changed. The three tables below are the current register. The owner tables of J through P keep what each revision applied and asked at the time; the rows this decision replaces carry a pointer here and are listed under "Superseded".

### Accepted

| ID | Rule as applied | Contract | What the owner attached |
|---|---|---|---|
| D1 | Integer text that `float64` would round reads as `uint64`, or as `float64` within ±2**53, else raises | [§4.2] | — |
| D2, U1 | `var`, `std`, `cov` and `corr` (D2), and `median` and `quantile` (U1), are the exact value rounded once | [§14.2] | #251 measurement 2 before the kernels it prices (M4, M13). Relaxing exactness later is a contract change |
| D3 | When several demanded values fail, the error of any one is raised | [§20.2] | — |
| D4 | A lambda returning a Python `bool` raises `LTSeqTypeError` | [§3.4] | — |
| D5 | Integer results take the common type, checked; `**` takes its base's type; `diff` widens | [§17.3], [§17.4] | — |
| D6 | `partition` finds its keys at the call; its values are lazy | [§14.5] | The API text must say that executing every value reads the source N + 1 times and that the dict is not an atomic snapshot. [§14.5] now says both |
| D7 | `assume_sorted` is checked strictly, with no pruning or pushdown below it | [§9.5] | #251 measurement 5 before M3. [§9.5] already shows filtering first to keep pruning |
| D9 | Budgets are set from M0's baselines | G (M0) | Keep the #251 dependencies |
| D10, D10a | `&` and `\|` guard symmetrically and chained filters stay ordered. Option (a): a sort, grouping, `distinct` or aggregate demands its keys and arguments on every input row, and a join on its right input | [§20.2], trade-off record | #273, and #251's measurement 4 and X4 measurement, before M16 and M1. Moving to (b) later is a contract change. E3 now lists the cases the owner named: each operator followed by a filter that removes the failing row, a join's right input, Parquet with pushdown and pruning, every evaluator |
| D11 | A `partition` key that `to_dicts` cannot convert raises `CastError` at the call | [§14.5] | — |
| D13 | An aware `partition` key is the UTC instant | [§14.5], [§16.3] | A4 now lists the cases the owner named: the repeated hour, the same instant in another zone, lookup, NULL and stable enumeration |
| U3 | A literal never widens a column's type | [§17.5] rule 1 | #247 stays open against M8 |

### Modified: OWNER MODIFICATION APPLIED

| ID | The owner's rule | Contract | Rows and examples |
|---|---|---|---|
| D8 | Integer `/` by zero raises `DivisionByZeroError` during execution, and its result type stays `float64`. Integer `//` and `%` by zero still raise, a float operand follows IEEE, and decimal division is unchanged | [§17.1], [§17.3]; [§10.1] keeps `pct_change` on IEEE | N3, P11, [§23.12] |
| D12 | A table without columns is a valid relation, and its row count does not depend on its columns | [§6.1]; applied in [§4.2], [§4.4]–[§4.6], [§7.2], [§7.5], [§7.6], [§16.5], [§20.2] | T1, L3, L5–L7, B2, B4, B5, X4; P2–P4 and P17 generate such tables; [§23.18] |
| D14 | `duration * integer`, `integer * duration` and `duration // integer` are exact in the duration's unit and checked. `//` floors, negative values included. A zero divisor raises `DivisionByZeroError` and overflow `ArithmeticOverflowError`. No operation changes the unit implicitly; a finer quotient needs a `cast` | [§19.2] | H1, [§23.13] |
| AC17 | A declared `from_dict` or `from_rows` type and `requested_schema` convert a value only by identity, by exact conversion within its kind, or as NULL; between other kinds the caller converts first. `cast`, `try_cast` and `read_csv`'s declared types keep their conversions | [§17.6] (five ways a value changes type); applied in [§4.6], [§10.7], [§16.4], [§17.4] rule 4, [§20.2] | L3, L7, X3, D9, W9; [§23.18] |

### Deferred

None blocks the merge. Each blocks the item named until the owner decides it. Until then the contract's current text holds, and an implementation does not substitute a rule of its own.

| ID | Current text | Owner's direction | Issue | Gate | Contract change needed |
|---|---|---|---|---|---|
| OD-W | Window statistics are exact, rounded once ([§14.2]) | Keep exactness; reconsider only on evidence | #251 measurement 2 | Before M4 and M13 | Only to relax exactness |
| U2 | No SHOULD bound for `tail(n)`; `distinct` not in [§21.1]'s MUST-stream list | Add neither now | #278; the R3 harness in #274 | Before release | Only to add either |
| AC3 | `len(t)` executes the plan, as `count()` does ([§3.2]) | Prefer `TypeError` pointing to `count()` | #278 | Before M27 and M29 | Yes, if `len` raises |
| N9 | A Python float literal is `float64` ([§17.2]), so `r.f32 == 0.1` is FALSE on every row | Prefer `LTSeqValueError` at plan time for a literal that `float32` cannot hold | #276 | Before M8 | Yes, if it raises |
| G5 | No weak integer literals ([§17.5]) | Keep | #276 | Before M8 | No, if kept |
| N7 | A nonexistent or repeated local time raises ([§19.4]) | Keep; no resolution options in v0.5 | #277 | Before M20 | No, if kept |
| OD-E | Specialized evaluators are allowed under the parity rule ([§21.2]) | The general path is the reference; a fast path ships only if it passes the same parity tests | #278 | Before M17 | No, if kept |
| OD-F | Fifteen exception classes ([§20.1]) | Keep fifteen unless implementation shows otherwise | #266 | Before M15 | Only to fold classes |
| OD-X6 | Closing a `to_batches` reader stops the execution ([§15.1]) | Keep, subject to #256's feasibility check | #256 | Before M1 | Only if #256 shows it infeasible |
| OD-C | — | Compiler research and benchmarks may start; work that depends on unfinished semantics waits | #229 | After M1, M16 and M24 | No |
| OD-S1 | — (v0.4) | Fail-loudly v0.4 safeguards for the affected `partition` and lookup keys | #258, #223 | A v0.4 fix, not postponed to v0.5 | No |
| OD-S2 | — (v0.4) | An urgent v0.4 fix for #259's file corruption and panic | #259 | A v0.4 fix | No |

### What the modifications required

**D8 departs from the earlier choice.** Until this decision, integer `/` by zero gave `±inf` or NaN: the option #218 recorded, which the gate recommended (J) and K kept, matching NumPy and pandas. The owner replaced it. Integer `/` by zero now raises `DivisionByZeroError`, as Python's `int / int` raises `ZeroDivisionError`, while the result type stays `float64` and a nonzero division is unchanged (`1 / 2` is `0.5`). The operands decide the error, not the result type: exact operands imply no infinity, and an `inf` would pass silently through later sums and comparisons. [§17.1]'s rule that the result type decides now names this as its one exception. A float operand keeps IEEE, so `r.a / r.b.cast("float64")` gives the old result. `pct_change` is defined as `x / x.shift(n) - 1` and would have inherited the new error. It was outside the decision, so [§10.1] now says it keeps IEEE for every input type: a zero base gives `inf`, integer input included.

**D12 is feasible, with one limit in the file formats.** The owner asked for a focused check that a row count survives without fields. The probes used pyarrow 25.0.1, pandas 3.0.5 and the baseline's DataFusion 55.0.0, driven through the Rust core to bypass v0.4's Python guards:

- Arrow carries the count in each batch's length. `pa.table({"a": [1, 2, 3]}).select([])` has 3 rows, and so does every hand-off LTSeq uses: the C stream (`pa.table` of a `__arrow_c_stream__` producer), `RecordBatchReader`, the IPC stream and file formats, Feather, and `to_pandas` (shape `(3, 0)`, a `RangeIndex` of 3).
- DataFusion plans a projection to no columns as `TableScan: projection=[]` and keeps the count, over 3 rows in memory, a 1000-row Parquet file, and 1000 rows split into four Parquet files: `count()` and the streamed rows are 3, 1000 and 1000. A slice gives the sliced count, a filter followed by the projection 998 of 1000, `union` twice the rows, `distinct` one row, and a later `derive` 1000 rows of one column.
- Neither file format holds the count. CSV has no way to write a row without fields: pyarrow writes no bytes and the baseline's `write_csv` writes `""` lines. Parquet records a row count, but pyarrow's writer and the `parquet` 59.2.0 crate both write 3 rows without columns as a file of 0 rows. [§16.5] therefore makes both writers raise `LTSeqValueError` at the call, decided from the schema, rather than write a file that reads back with other rows. That refusal is limited to the two writers; letting them write such a table later would turn an error into a result.
- Two inputs give no row count, and the contract assumes none: `from_dict({})`, whose row count would come only from its columns, and a 0-byte CSV without a `pa.Schema`, whose column count is unknown. Both raise `LTSeqValueError`.
- pyarrow 25.0.1 loses the count in three `Table` operations on such a table: `Table.slice(1)` without a length gives 3 rows of 3, `Table.take([0, 0, 2, 1])` gives 0, and `concat_tables` of two 3-row tables gives 0. `RecordBatch.slice(1)`, `RecordBatch.take` and `concat_batches` or `Table.from_batches` give 2, 4 and 6. An implementation must not route such a table through those three; the contract promises only the counts, so it is unaffected.
- The baseline already gets part of this wrong, and M22 fixes it: `from_rows([{}, {}]).count()` is 0; `from_arrow` of a 3-row table without columns counts and exports 3 rows, but every transform on it raises `Schema not initialized`; a `drop` of every column raises `Cannot drop all columns` while `select()` keeps the 3 rows; `write_parquet` writes such a table as 0 rows and `write_csv` as `""` lines, both silently. #278 lists them.

**D14 is stated in full.** The owner kept the three operations and asked for their behavior to be complete. [§19.2] now states the floor of the exact quotient in the duration's unit, negative values included, the zero divisor and overflow, and that the result depends on the unit and is not Python's: −5 s `// 2` is −3 s in `duration[s]` and −2.5 s in `duration[ms]` and as a `timedelta`. A `cast` to a finer unit keeps a finer quotient, and nothing widens the unit implicitly. Mixed units meet only in `duration ± duration`, whose row now names the finer unit as the result's, which settles that part of N19 (#277). H1 holds the cases the owner listed: negative operands, zero divisors, overflow, mixed units and exact divisibility.

**AC17 needed four readings.** [§17.6] now defines once the five ways a value changes type: identity; exact compatible conversion, the only one an implicit placement makes; an explicit cast; CSV text; and implicit cross-kind conversion, which never happens. [§4.6], [§10.7], [§16.4], [§17.4] rule 4 and [§20.2] cite it instead of restating it. The owner's text left four points to the contract's existing kinds, and the pass read them as follows:

- Integers, decimals and floats count as one kind for placement, so a declared `float64` takes `1` and a declared `int16` takes `2.0`, exactly or with `CastError`. As before, a Python `int` that no integer type holds is still a number (`2**64` goes into `decimal128(20, 0)`), and a `list`, `tuple` or `dict` goes into a nested type element by element. [§17.4] rule 4 already treats numbers this way, and the owner's text names string ↔ date, number → string and `bool` ↔ number as the forbidden cases.
- Date ↔ timestamp is a change of kind: [§17.6] lists them as separate kinds, and [§17.5] already refuses to share a column between them.
- `fold`'s `dtype`, which the owner's text does not name, places each state as a declared `from_dict` type does ([§10.7]).
- Nullability is unchanged: `None` is NULL in every declared type, a `null` column becomes NULLs of a requested type, and [§16.4] keeps the table's nullability.

### Superseded

These records applied a choice the owner replaced. They stay as written, and each carries a pointer here:

- J: V11's disposition and the D8 row, under which integer `/` by zero gave `±inf` or NaN.
- K: the D8 and D12 rows, and the row "Found while resolving F14", under which every constructor refused an input without columns.
- P: the AC17 row of the owner table, and in "Every implicit conversion against the rules" the `from_dict` and `requested_schema` rows, which then allowed conversions between kinds.

The owner's comment also settles what K through P left open for the owner: their requests for a choice on D1–D14, and the AC17 finding that P's final verification filed. D, the trade-off record, G, I's gate and P's "What remains" are current records and were updated in place.

### Open

- Final merge review. Three points need the owner's confirmation, and none changes another decision: the writers' refusal of tables without columns (D12), `pct_change` keeping IEEE for integer input (D8), and the four AC17 readings above.
- Each deferred decision, before its gate.
- Neither document is implemented. The [§24] rows these modifications changed have been checked against the contract only, not run against code.
- The issues carry the same state, each in a comment that points here: #218 (D8), #247 (U3), #276 (N9, G5, OD-W), #277 (D14, N7), #278 (AC17, AC3, U2, OD-E and D12's findings) and the roadmap #257. Each issue that describes today's runtime stays open until the M item that changes it.

<!-- v0.5-modular:footer -->

---

Previous: [Assessment closure pass (P)](assessment-closure.md) · [Index](../../README.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§3.2]: ../../contract/public-surface.md#32-ltseq-invariants
[§3.4]: ../../contract/public-surface.md#34-lambdas-and-proxies
[§4]: ../../contract/loading-and-laziness.md#4-loading
[§4.2]: ../../contract/loading-and-laziness.md#42-ltseqread_csv
[§4.4]: ../../contract/loading-and-laziness.md#44-ltseqfrom_arrow
[§4.6]: ../../contract/loading-and-laziness.md#46-ltseqfrom_dict-and-ltseqfrom_rows
[§6.1]: ../../contract/schema-and-table-operations.md#61-schema-and-columns
[§7.2]: ../../contract/schema-and-table-operations.md#72-select
[§7.5]: ../../contract/schema-and-table-operations.md#75-drop
[§7.6]: ../../contract/schema-and-table-operations.md#76-distinct
[§9.5]: ../../contract/ordering.md#95-assume_sorted
[§10.1]: ../../contract/windows-and-grouping.md#101-window-methods
[§10.7]: ../../contract/windows-and-grouping.md#107-fold
[§14]: ../../contract/aggregation.md#14-aggregation-partitioning-and-pivot
[§14.2]: ../../contract/aggregation.md#142-aggregate-expressions
[§14.5]: ../../contract/aggregation.md#145-partition
[§15.1]: ../../contract/streaming-and-output.md#151-to_batches
[§16]: ../../contract/streaming-and-output.md#16-output-and-interchange
[§16.3]: ../../contract/streaming-and-output.md#163-to_dicts
[§16.4]: ../../contract/streaming-and-output.md#164-arrow-pycapsule-stream
[§16.5]: ../../contract/streaming-and-output.md#165-writers
[§17]: ../../contract/numeric-null-temporal.md#17-numeric-semantics-and-literals
[§17.1]: ../../contract/numeric-null-temporal.md#171-integer-and-decimal-results-are-checked
[§17.2]: ../../contract/numeric-null-temporal.md#172-literals
[§17.3]: ../../contract/numeric-null-temporal.md#173-arithmetic-operators
[§17.4]: ../../contract/numeric-null-temporal.md#174-types-of-mixed-operands
[§17.5]: ../../contract/numeric-null-temporal.md#175-shared-values
[§17.6]: ../../contract/numeric-null-temporal.md#176-explicit-casts
[§19.2]: ../../contract/numeric-null-temporal.md#192-arithmetic-and-comparison
[§19.4]: ../../contract/numeric-null-temporal.md#194-dt-methods
[§20.1]: ../../contract/errors-and-performance.md#201-exception-classes
[§20.2]: ../../contract/errors-and-performance.md#202-stages
[§21.1]: ../../contract/errors-and-performance.md#211-materialization
[§21.2]: ../../contract/errors-and-performance.md#212-fast-paths
[§23.12]: ../../contract/examples-semantics.md#2312-checked-and-exact-arithmetic
[§23.13]: ../../contract/examples-semantics.md#2313-time-zones-and-dst
[§23.18]: ../../contract/examples-semantics.md#2318-arrow-and-pandas-round-trip
[§24]: ../../contract/acceptance.md#24-acceptance-criteria-and-contract-test-matrix
[Deliverable Q]: #q-owner-decision-closure

<!-- /v0.5-modular:links -->
