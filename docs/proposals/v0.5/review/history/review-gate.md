<!-- v0.5-modular:header -->

# v0.5 review history: API review gate and contract closure pass (J, K)

[Index](../../README.md) › Review history · Previous: [Adversarial review and final consistency check (H, I)](adversarial-review.md) · Next: [Third to sixth review closures (L–O)](review-closures.md)

**Non-normative, historical.** A dated record of the [review](../../README.md#ltseq-v05-api-review), kept as written. Later records name the earlier records they change, and [Deliverable Q] is the current decision register.

**Scope.** The API review gate on PR #250, findings V1–V12 and decisions D1–D9 as applied ([Deliverable J]); then the contract closure pass, findings F13–F16, the `&` and `|` decision and the owner decisions D1–D12 ([Deliverable K]).

**Most cited from here.** [§4] Loading · [§17] Numeric semantics and literals · [§20] Errors · [§14] Aggregation, partitioning and pivot

<!-- /v0.5-modular:header -->

## J. API review gate

The [API review gate](https://github.com/Lychee-Technology/ltseq/pull/250#issuecomment-6055064010) reviewed this PR at `fdec685`, after H and I. It checked twelve candidate problems against the contract, and against the baseline wherever a candidate described today's behavior. It found six defects that block approval, four architectural risks that need evidence and the owner's sign-off, and three candidates it downgraded to wording or rejected. V6 split into a defect (V6a) and a risk (V6b). This section records how the revision answers each finding, which option it applies for each of the nine owner decisions, and which earlier records it changes. It was written after the revision, so the reasoning below is the revision's, not the gate's.

### Findings and resolutions

| ID | Gate's verdict | Finding | Resolution | Contract |
|---|---|---|---|---|
| V1 | Defect, P1 | Float `mean`, `var`, `std`, `cov` and `corr` returned `inf`, `0.0` or NaN for representable results (`var([1e308, 1e308])` was `inf`), and P10 fixed those formulas bit for bit | Every float statistic is its exact value over the values taking part, rounded once (D2). `var` and `cov` come from exact Σ*x*, Σ*x*² and Σ*ab*, `std` is the correctly rounded root of the exact `var`, and `corr` lies in [−1, 1]. `rolling` statistics follow. P10 compares with an exact `Fraction` reference, and A2 holds the gate's counterexamples, each at its exact value | [§14.2], [§10.1], P10, A2, W4, [§24.4] |
| V2 | Defect, P2 | [§3.4] named one undetectable form of `is None` in a helper, while every value position passed silently | A Python `bool` raises `LTSeqTypeError` where a condition is required and as the whole value of an expression lambda (D4). [§3.4] lists what stays undetectable: `or`, `and`, the conditional expression, `~` (which Python applies to a `bool` as integer inversion), and a `bool` nested inside an expression. S5 runs on every supported Python minor version and includes a class-body lambda | [§3.4], [§7.3], S5, B3 |
| V3 | Defect, P2 | `//`, `%`, `**`, `-` and `diff` gave different integer result types, so whether a query raised depended on the operator written | One integer result-type table: `+ - * // %` take [§17.4]'s common type, `**` its base's type, `diff` widens (D5, which departs from the gate's recommendation; see below) | [§17.3], [§17.4], [§10.1], N1, W2 |
| V4 | Defect, P0 | CSV inference typed integer text outside `int64` as `float64`, rounding every value above 2**53 in the column | Integer syntax infers `int64`, then `uint64`, then `float64` only when every integer-syntax value is within ±2**53; anything else raises at the call and names `schema=` (D1). `from_dict` and `from_rows` follow the same rule, and [§17.6] lists inference among the implicit conversions that never round an integer | [§4.2], [§4.6], [§16.5], [§17.6], L2, L7 |
| V5 | Wording, P3 | "Referenced without copying" conflicted with converting `float16` and `date64` at ingestion | Buffers are referenced at the call; those two types are converted when read, copying only that column. L5 checks buffer identity for types [§6.2] does not convert | [§4.4], [§6.2], L5 |
| V6a | Defect, P1 | No rule chose among several failing demanded values, so P5 and P6 could not pass reliably | The error of any one failing value is raised (D3), a sixth named exception to principle 9. Tests compare against the set of errors the failing values raise, and the generators place two failures in different rows, batches, files, columns and operands | [§1.3], [§20.2], [§24.1], P2, P5, P6, E2, [§24.4] |
| V6b | Risk, P2 | Row-wise demand constrains the DataFusion rewrites the roadmap relies on, and two filters [§20.2] requires to succeed fail at the baseline | [§20.2] allows speculative evaluation provided errors from values not demanded are suppressed, and states that an operation never evaluates rows an earlier one removed. "Rewrites under row-wise demand" gives each rule LTSeq runs a status. M16 now precedes M5, and M0 measures masking | [§20.2], E3; trade-off record, G |
| V7 | Wording, P3 | [§21.1] named a mechanism for restoring order | It states the property: restoring the defined order MUST NOT materialize, and the mechanism is the implementation's choice | [§21.1] |
| V8 | Risk, P2 | `partition()` could take its keys and its values from different reads of a changing source | Documented, not changed (D6): [§14.5] states the consistency and the N + 1 reads, and `t.collect().partition(k)` is the one-read form | [§14.5], [§23.8], A4 |
| V9 | Risk | The performance commitments were measured only after being built | M0 holds six measurements, each run before the item it gates and reported without thresholds; the owner sets budgets from the baselines (D9) | G (M0), trade-off record |
| V10 | Risk, P2 | `assume_sorted` validation rules out pruning and pushdown below it | Strict validation stays (D7). [§9.5] says pruning and pushdown into the scan are not applied below the declaration and that filtering first keeps them | [§9.5], [§23.10], O4 |
| V11 | Rejected | Integer `1 / 0` is `inf` while `//` and `%` raise | Unchanged (#218), pending the owner's confirmation (D8). Superseded: the owner modified D8, and integer `1 / 0` raises (Q) | [§17.3] |
| V12 | Defect, P2 | P3 and P7 could not rebuild descending or NULLS FIRST keys | Both spell out the reconstruction with `descending=` and `nulls_last=` lists, and the generators include descending, NULLS FIRST, mixed and multi-key orders | P3, P7, [§24.4] |

No guarantee row or property was added. The gate's cases became cells of existing rows, so the counts in C, F and I still hold: 24 sections, 95 rows and 18 properties.

### Owner decisions as applied

The decisions belong to the owner. The revision applies one option for each, so that the contract is complete and testable as it stands; the owner confirms or replaces each before approval. Eight follow the gate's recommendation and D5 does not.

| ID | Decision | Applied | Gate's recommendation | What the owner should weigh |
|---|---|---|---|---|
| D1 | Integer text that `float64` would round | `uint64` above the `int64` range; `float64` only within ±2**53; otherwise raise. `1_000` and padded values are strings; leading zeros are integers | Same | ZIP codes such as `02134` read as `int64` unless declared `string` |
| D2 | `var`, `std`, `cov`, `corr` | Exact moments, rounded once | Same, subject to measurement | M0 measurement 2 prices the exact accumulator per group and per sliding frame |
| D3 | Error when several demanded values fail | Any one; tests compare against a set | Same | Which error is reported can change between runs on the same data |
| D4 | A lambda returning a Python `bool` | Raises `LTSeqTypeError` | Same | `lambda r: False` becomes `lit(False)` or a non-callable `False` |
| D5 | Integer result types | Common type for `+ - * // %`; base type for `**`; `diff` widens | Widen `//` and `**` to 64 bits | This revision departs from the recommendation (below) |
| D6 | `partition()` consistency | Lazy values, hazard documented | Same | N + 1 reads of the source; M0 does not measure them |
| D7 | Pruning below `assume_sorted` | Strict: no pruning or pushdown below the declaration | Same | M0 measurement 5 prices the lost pruning |
| D8 | Integer `/` by zero | `±inf` or NaN, as #218 decided; superseded by the owner's modification, under which it raises (Q) | Same | A departure from Python, matching NumPy and pandas |
| D9 | Performance budgets | M0 names the item each measurement gates | Same | Budgets are set from M0's baselines |

**D5.** The gate recommended keeping `//` and `**` widened to 64 bits. The revision gives `//` the same common type as `+`, `-`, `*` and `%` instead, because a second rule for some operators is the inconsistency V3 found: under widening, `r.i8 // r.i8` is `int64` while `r.i8 % r.i8` is `int8`. The common type is DataFusion's coercion for `+ - * %` (ADR 0018 D-a), and NumPy 2.4.4 also keeps `int8` for `int8 // int8` and `int8 % int8`, as pyarrow 25.0.1 does for integer `divide`. The baseline's `int64` for `//` is how `src/transpiler/floor_div.rs` computes every integer floor division in one 64-bit kernel, documented in `docs/api.md` but not weighed against the other operators in any ADR. `**` takes its base's type instead of the common type, because the exponent is a count: `r.u64 ** 1` stays `uint64`, which is the gate's counterexample, and `r.i8 ** 2` is `int8`, as in NumPy. `diff` is the one operation that widens, because a difference of unsigned values is signed and a decreasing `uint32` counter must not overflow. The cost is that `int8 // int8` raises where the baseline widens, for example `-128 // -1`, and a user who wants a wider result casts first.

### Earlier records this revision changes

- **AR7 is superseded.** AR7 defined `mean`, `var`, `std`, `cov` and `corr` as formulas over the rounded sum; V1 showed that they overflow and underflow. Its goal, bits that do not depend on order or batching, now follows from exact values rounded once.
- **AR17 is extended.** At `fdec685` an undeclared `from_dict` `int` in [2**63, 2**64) raised `LTSeqValueError`; it now infers `uint64`, as a literal does, so `from_dict` and `read_csv` share one inference rule.
- **AR3 is extended.** It settled early stopping under `assume_sorted`, and V10 adds pruning and pushdown, each with a trade-off row.
- **Acceptance gate items 8, 9, 13 and 15** above note the set-based error parity, the 12 further findings, the sixth exception to principle 9, the speculative-evaluation MAY, and that items 8, 13 and 15 stay pending until the owner confirms D1–D9.
- **Trade-off record and impact map.** The unmeasured rows name the M0 measurement that prices them, and five rows record D1, D3, D5, D6 and D7. M0 is added, and M16 now precedes M5.

### Baseline behavior found while resolving V1 and V6b

Probes at the baseline found two behaviors the gate did not report. Both are current behavior and are filed as issues; the second also made one contract rule explicit.

**A guarded `&` raises depending on the other rows in its batch.** `filter(lambda r: (r.b != 0) & (r.a // r.b > 1))` over inputs that each contain the row `a = 7, b = 0`:

| Rows where the guard is TRUE | Result |
|---|---|
| 1 of 3, 2 of 3, 1 of 4 | `Divide by zero error` |
| 1 of 5, 1 of 10 | The passing row |

DataFusion 55's `BinaryExpr` evaluates the right operand of `AND` on the selected rows only when the left operand is TRUE on at most 20% of the batch (`PRE_SELECTION_THRESHOLD`), and on every row otherwise. Tracked with the chained-filter failure in #252.

**Float `min` and `max` disagree across paths, and grouped `min` can return a value that is not the minimum.** [§14.2] and [§9.4] require `max` to be NaN when a NaN takes part and `min` to ignore NaN unless every value is NaN. At the baseline:

| Input | Ungrouped, in memory | `group_by` | Ungrouped, Parquet file |
|---|---|---|---|
| `[1.0, NaN, 2.0]` | `max` NaN, `min` `1.0` | `max` `2.0`, `min` `2.0` | `max` `2.0`, answered from statistics that omit NaN |
| `[1.0, 2.0, NaN]` | `max` NaN, `min` `1.0` | `max` NaN, `min` NaN | not probed |
| `[inf - inf, 1.0, 2.0]` | `max` `2.0`, `min` NaN | `max` `2.0`, `min` `1.0` | not probed |

DataFusion 55's grouped accumulators compare with `partial_cmp` and replace the running value whenever the comparison involves NaN: a NaN replaces the running value and the next value replaces the NaN, so the result depends on where NaNs fall ([apache/datafusion#24432](https://github.com/apache/datafusion/issues/24432), fixed on DataFusion's main after 55.2.0 and not in a release). The ungrouped kernels use IEEE total order, under which a NaN with the sign bit set, which `inf - inf` produces on x86-64, sorts below every number. `sort` places such a NaN first as well. Tracked in #253. The contract now says that every NaN, whatever its sign bit, is greater than `+inf`, and A2, O3 and the generators include one.

The Python 3.14 failure of the `is None` rewrite in a class-body lambda, which the gate reproduced, is tracked in #254.

### Open at this revision

The owner's confirmation of D1–D9, the `&` question, the rewrite-table rows not run, and a fresh independent review were open here. [Deliverable K] records where each stands now.

## K. Contract closure pass

The [second independent API review](https://github.com/Lychee-Technology/ltseq/pull/250#issuecomment-6064321978) read the contract at `a49af0b` and asked for a focused closure pass rather than a redesign. It found a property that contradicts a normative rule (F13), an input boundary left open (F14), a key type `partition` cannot represent (F15) and an unproven feasibility assumption (F16). It also asked for a decision on how `&` and `|` guard, explicit owner decisions, agreement among rules, properties, rows and examples, and a last independent consistency check. This section records how this revision answers each. The object model, the DSL, the ordered-sequence model and the error taxonomy are unchanged.

### Findings and resolutions

| ID | Review's priority | Finding | Root cause | Resolution | Contract |
|---|---|---|---|---|---|
| F13 | P1 | P12 required a literal to take part with its exact value or raise, while [§17.4] converts an integer operand of float arithmetic to the nearest `float64`, so `r.f + (2**53 + 1)` with `f = -2.0**53` had to be both `1.0` and `0.0` | P12 restated the placement rule of [§17.5] and [§17.6] (a literal placed into a type never rounds) as a rule for every use of a literal, which [§17.4] never made it. The implicit-conversion rule is restated across [§1.4], [§4], [§16] and [§17], and P12's restatement was the strictest | P12 becomes "Literals behave as columns": an expression with a literal equals the same expression over a column holding the literal's exact value, with five listed exceptions where a literal has its own rule (placement, NaN against an integer or decimal in a comparison, `None` and Python `bool`, out-of-range values, and the plan-time check of a duration added to a date). [§17.4] says a literal operand rounds as a column does, and [§17.6] names the only conversions that round. `0.0` is pinned in N4 and [§23.12] | [§17.4], [§17.6], P12, N4, [§23.12] |
| F14 | P1 | `from_arrow` accepted any `__arrow_c_array__` producer without saying what a bare array becomes | [§4.4] named the protocol rather than the data it accepts. The protocol represents a record batch as a struct array, so tabular data is a struct schema and anything else is a column without a name | The review's option A. `from_arrow` accepts tabular data: a schema that is a struct, whose fields are the columns. A `StructArray` or struct `ChunkedArray` reads as the table of its fields. A bare array or non-struct `ChunkedArray` raises `LTSeqTypeError` naming `pa.table({"x": arr})`, and a struct with a NULL row raises `LTSeqValueError`, as pyarrow refuses it. L5 holds the positive and negative cases | [§4.4], L5 |
| — | — | Found while resolving F14: `from_arrow(pa.table({}))` and `from_pandas(pd.DataFrame())` built a table without columns, which [§6.1] and #156 rule out for `select` and `drop` | The at-least-one-column rule was stated for transforms and not for constructors | Every constructor refuses an input without columns with `LTSeqValueError` at the call (D12); superseded: a table without columns is a valid relation (Q) | [§4.4]–[§4.6], [§6.1], L5–L7 |
| F15 | P2 | `partition` keys are `to_dicts` values, which do not exist for a `timestamp[ns]` that is not a whole microsecond | `to_dicts`' conversion raises for such values, and [§14.5] did not say what `partition` does then | The review's option 2 (D11). [§14.5] states the limitation: a key `to_dicts` cannot convert raises `CastError` at the call, naming the column, and no key is converted lossily, so two distinct key values never merge. The workaround is a derived key such as `r.ts.dt.truncate("second")`. A lossless representation was rejected because it would give `partition` dict keys of a type no other method returns, for one key type. A4 and P14 hold the cases | [§14.5], [§20.2], A4, P14 |
| F16 | P1, feasibility | [§15.1] required `to_batches()` reads and the C stream to raise LTSeq's exception class, which the Arrow C stream cannot carry | The contract treated two paths as one: Python reads of a pyarrow reader, and C-stream import. The prototype below shows that only the first can keep a class | [§15.1] keeps the class on Python reads only. [§16.4] defines what the stream carries: EIO for `LTSeqIOError` and its subclasses, EINVAL for the rest, and a message starting with the class name, which pyarrow raises as `OSError` or `ArrowInvalid`. P4, R1 and X3 assert each path. The parts not yet run gate M1 through #256 | [§15.1], [§16.4], [§4.4], [§20.1], P4, R1, X3 |

No row, property or section was added: the counts in C, F and I still hold at 24 sections, 95 rows and 18 properties.

### F16: what the prototype showed

The prototype raised stand-in classes with the contract's bases (`ArithmeticOverflowError(ArithmeticError)`, `CastError(ValueError)`, `OrderViolationError(ValueError)`, `SourceNotFoundError(FileNotFoundError)`) from a Python generator behind `pa.RecordBatchReader.from_batches`, with pyarrow 25.0.1 and Python 3.14.7. The script is in #256.

| Read | What is raised |
|---|---|
| `read_next_batch()`, iteration, `read_all()` | The generator's class and message, unchanged, for all four classes |
| `pa.RecordBatchReader.from_stream(reader).read_all()` | `ArrowInvalid` (`OSError` for `SourceNotFoundError`), the message prefixed with a status name (`Unknown error:`, `Invalid:`, `IOError:`) and followed by the Python traceback |
| `pa.table(t)` and `from_stream(t)` on the baseline extension, integer division by zero | `ArrowInvalid: External error: Arrow error: Divide by zero error`, arrow-rs's display, unchanged |
| `t.to_arrow()` on the same table | `ValueError: Failed to collect results: Arrow error: Divide by zero error` |

Three conclusions follow. A pyarrow reader keeps a Python exception's class on Python reads, so [§15.1] is feasible as stated if `to_batches()` raises from Python. No producer can carry a class through the C stream, so [§15.1] and [§16.4] now promise only the code and message there. pyarrow passes a native producer's message through unchanged, so LTSeq must write its own `get_last_error` text and errno; relaying arrow-rs's message would give neither the class name nor the right code. The same holds in the other direction: for an exception from a `pyarrow.RecordBatchReader` to reach the caller unchanged ([§4.4]), `from_arrow` has to read the reader through its Python methods, because importing it through its C stream loses the class.

Not run: an iterator implemented in Rust behind `to_batches()`, a native producer setting EIO or EINVAL, and DuckDB and Polars as importers. Also not measured: a consumer that imports the `to_batches()` reader through the C stream, rather than the table, calls back into Python for each batch. #256 runs all three before M1; if a Rust iterator cannot keep the class, [§15.1] is revised before M1 rather than weakened during it.

### The `&` and `|` decision (D10)

At `a49af0b`, [§20.2] let only the left operand of `&` guard the right, so `(r.n // r.d > 1) & (r.d != 0)` had to raise where `(r.d != 0) & (r.n // r.d > 1)` did not. This revision makes the rule symmetric: either operand of `&` guards the other on rows where it is FALSE, and either operand of `|` on rows where it is TRUE. Where both operands of one `&` fail on a row, both are demanded and one of their errors is raised (D3).

- **It matches what the operators mean.** Kleene `&` and `|` commute, and Python evaluates both operands of `&` and `|`, so nothing in the DSL suggests an order.
- **The baseline already reorders.** DataFusion's `PushDownFilter` moves cheap conjuncts ahead of function calls (`reorder_predicates`), so `filter((r.a // r.b > 1) & (r.b > 0))` plans as `b > 0 AND floor_div(a, b) > 1`. A probe found the reversed guard behaving exactly as the forward one: it raises when the guard is TRUE on 1 row of 4 and succeeds on 1 row of 5. Under the left-to-right rule, this rewrite and pruning on any conjunct but the first would have been restricted.
- **It frees later engines.** Compiled and GPU kernels can evaluate conjuncts in any order and prune on any FALSE conjunct whose column has no NULLs.

The cost is that both operands of a fallible `&` or `|` are evaluated under a mask, including the left one, which `BinaryExpr` evaluates unmasked on every row today. #251's measurement 4 now prices masking with the guard on either side and with two fallible operands, before M16.

Chained filters stay ordered: a later operation never guards an earlier one, so `t.filter(lambda r: 10 // r.x > 1).filter(lambda r: r.x != 0)` raises where `x` is 0 ([§20.2], [§23.14], E3). The two rules meet in DataFusion's filter merge. The merged `p AND q` must keep `p` demanded on every row: evaluated as [§20.2]'s `p & q`, it would hide `p`'s errors where `q` is FALSE. The rewrite table records this and the CSE rule that also assumed an ordered `AND`.

Rejected: demand that depends on the consumer, where a filter needs a predicate only on rows that could pass. It also makes the order unobservable, but the same expression would then raise in `select` and not in `filter`, which [§20.2]'s single demand rule exists to prevent.

### Agreement across the contract

Each fix touched a rule restated in several places. Those places were reread together and edited where they disagreed:

| Rule | Places |
|---|---|
| Implicit conversion (F13) | [§1.4], [§4.2], [§4.6], [§16.4], [§16.5], [§17.4]–[§17.6], [§19.2], P12, N4, [§23.12] |
| Tabular input and no-column tables (F14, D12) | [§4.2], [§4.4]–[§4.6], [§6.1], L2, L3, L5–L7, [§24.5]; the inventory row and #156 in this document |
| Partition keys (F15) | [§14.5], [§20.2]'s call stage, A4, P14 |
| Stream errors (F16) | [§4.4], [§15.1], [§16.4], [§20.1], P4, R1, X3, [§23.10] |
| Demand under `&` and `\|` (D10) | [§8.2], [§20.2], P6, P13, E3, [§23.14]; the trade-off record, the rewrite table, and M0 and M16 in this document |

Rereading them together found one more conflict: P14 checked `partition` against an equality reference that maps NaN to a canonical key, while [§14.5] raises on a NaN key. P14 now checks `partition` only on keys without NaN that `to_dicts` converts, and on any other key expects [§14.5]'s error. A fresh-context reread of the diff then found two contradictions this revision had introduced. The new P12 required a literal's error at the stage of a column's, while [§19.2] raises a duration literal that is not a whole number of days at plan time; P12 now lists that as exception (e). The rewrite table's pruning example used a float column, whose statistics omit NaN, which compares above 6; the example now uses an integer column. The same reread made [§6.1], [§4.2] and P13 more precise about `from_rows` schemas, 0-byte CSV files and where a failing `&` raises. The mechanical checks of I were rerun on this revision. They found no dangling section or test reference, no duplicate ID and no table with a wrong column count, and every subsection of [§4]–[§21] except the worked example [§11.3] is still cited by a [§24] row or property. They are not the independent consistency check the review asked for, which has not been done.

### Baseline behavior found during this pass

- **A struct NULL row reads as zeros.** `LTSeq.from_arrow(pa.chunked_array([sa]))`, where `sa` is a struct array with a NULL row, returns `0` and `""` for that row's fields, where pyarrow refuses the import. Filed as #255; the contract's `LTSeqValueError` fixes it.
- **A reversed chained filter hides its error.** `filter(lambda r: 10 // r.x > 1).filter(lambda r: r.x != 0)` over `x = [2, 0, 0, 0, 0]` returns `[{"x": 2}]`, where [§20.2] requires `DivisionByZeroError`; over four rows it raises. DataFusion merges the filters, `reorder_predicates` puts `x != 0` first, and pre-selection then evaluates `floor_div` only on the row that passes. Added to #252.
- **Tables without columns.** `from_arrow(pa.table({}))` and `from_pandas(pd.DataFrame())` return a table with no columns, and `from_pandas(pd.DataFrame(index=range(3)))` one with no columns and three rows, while `from_dict({})` raises `from_dict() requires at least one column`. D12 makes them consistent.

### Owner decisions

The review asked for an explicit decision on each of D1–D9 and on `&` and `|`. This pass adds D10–D12. None has been recorded yet. The contract applies one option for each, so it is complete and testable as it stands; the owner accepts it or names an alternative. The review singled out D5, D6 and D7.

| ID | Decision | Applied | Recommended by | What the owner should weigh |
|---|---|---|---|---|
| D1 | Integer text that `float64` would round | `uint64` above `int64`; `float64` only within ±2**53; otherwise raise | Gate | ZIP codes such as `02134` read as `int64` unless declared `string` |
| D2 | `var`, `std`, `cov`, `corr` | Exact moments, rounded once | Gate, subject to measurement | M0 measurement 2 prices the exact accumulator |
| D3 | Error when several demanded values fail | Any one; tests compare against a set | Gate | Which error is reported can change between runs |
| D4 | A lambda returning a Python `bool` | `LTSeqTypeError` | Gate | `lambda r: False` becomes `lit(False)` |
| **D5** | Integer result types | Common type for `+ - * // %`, base type for `**`, `diff` widens | This revision, departing from the gate's 64-bit `//` and `**` (J) | `int8 // int8` can raise where the baseline widens, such as `-128 // -1` |
| **D6** | `partition()` consistency | Lazy values; the hazard is documented | Gate | Keys and values can come from different reads of a changing source, and the call and N values read it N + 1 times; `t.collect().partition(k)` is the one-read form |
| **D7** | Pruning below `assume_sorted` | Strict validation; no pruning or pushdown below the declaration | Gate, and the review | Lost pruning, priced by M0 measurement 5; filtering first keeps it |
| D8 | Integer `/` by zero | `±inf` or NaN (#218); superseded: it raises (Q) | Gate | Departs from Python; matches NumPy and pandas |
| D9 | Performance budgets | Set from M0's baselines | Gate | No thresholds until M0 reports |
| **D10** | How `&` and `\|` guard | Symmetric; chained filters stay ordered | This revision; the review asked only for a decision | Masked evaluation of both operands (M0 measurement 4); the order of chained filters stays observable |
| D11 | `partition` keys `to_dicts` cannot convert | `CastError` at the call; a derived key is the workaround | The review's option 2 | Nanosecond data not on whole microseconds cannot be partitioned on the raw column |
| D12 | Inputs without columns | Every constructor raises `LTSeqValueError`; superseded: a table without columns is a valid relation (Q) | This revision | `from_pandas(pd.DataFrame())` and `from_arrow(pa.table({}))` raise where pandas, pyarrow and the baseline accept them |

### Prerequisites linked to implementation

The review asked that feasibility work able to change a guarantee be tied to the item it gates. Each of #251's six measurements runs under M0 before its item: 1 before M5, 2 before M13 and M4, 3 before M1, 4 before M16 (and so before M5, which depends on M16), 5 before M3, and 6 before M18. #256 runs before M1. The baseline failures are tied to the items that fix them and to the rows that keep them fixed: #252 to M16 (E3, P6), #253 to M13 and M19 (A2, O3, P5), #254 to M12 (S5) and #255 to M22 (L5).

### Open at this revision

The owner's acceptance of D1–D12, an independent consistency review, the F16 cases not run (#256) and the rewrite-table rows not run were open here. [Deliverable L] records where each stands now.

<!-- v0.5-modular:footer -->

---

Previous: [Adversarial review and final consistency check (H, I)](adversarial-review.md) · [Index](../../README.md) · Next: [Third to sixth review closures (L–O)](review-closures.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§1.3]: ../../contract/overview.md#13-design-principles
[§1.4]: ../../contract/overview.md#14-relation-to-earlier-decisions
[§3.4]: ../../contract/public-surface.md#34-lambdas-and-proxies
[§4]: ../../contract/loading-and-laziness.md#4-loading
[§4.2]: ../../contract/loading-and-laziness.md#42-ltseqread_csv
[§4.4]: ../../contract/loading-and-laziness.md#44-ltseqfrom_arrow
[§4.6]: ../../contract/loading-and-laziness.md#46-ltseqfrom_dict-and-ltseqfrom_rows
[§6.1]: ../../contract/schema-and-table-operations.md#61-schema-and-columns
[§6.2]: ../../contract/schema-and-table-operations.md#62-supported-types
[§7.3]: ../../contract/schema-and-table-operations.md#73-derive
[§8.2]: ../../contract/expressions.md#82-operators
[§9.4]: ../../contract/ordering.md#94-sort
[§9.5]: ../../contract/ordering.md#95-assume_sorted
[§10.1]: ../../contract/windows-and-grouping.md#101-window-methods
[§11.3]: ../../contract/windows-and-grouping.md#113-example
[§14]: ../../contract/aggregation.md#14-aggregation-partitioning-and-pivot
[§14.2]: ../../contract/aggregation.md#142-aggregate-expressions
[§14.5]: ../../contract/aggregation.md#145-partition
[§15.1]: ../../contract/streaming-and-output.md#151-to_batches
[§16]: ../../contract/streaming-and-output.md#16-output-and-interchange
[§16.4]: ../../contract/streaming-and-output.md#164-arrow-pycapsule-stream
[§16.5]: ../../contract/streaming-and-output.md#165-writers
[§17]: ../../contract/numeric-null-temporal.md#17-numeric-semantics-and-literals
[§17.3]: ../../contract/numeric-null-temporal.md#173-arithmetic-operators
[§17.4]: ../../contract/numeric-null-temporal.md#174-types-of-mixed-operands
[§17.5]: ../../contract/numeric-null-temporal.md#175-shared-values
[§17.6]: ../../contract/numeric-null-temporal.md#176-explicit-casts
[§19.2]: ../../contract/numeric-null-temporal.md#192-arithmetic-and-comparison
[§20]: ../../contract/errors-and-performance.md#20-errors
[§20.1]: ../../contract/errors-and-performance.md#201-exception-classes
[§20.2]: ../../contract/errors-and-performance.md#202-stages
[§21]: ../../contract/errors-and-performance.md#21-performance-contract
[§21.1]: ../../contract/errors-and-performance.md#211-materialization
[§23.8]: ../../contract/examples-sequences.md#238-partitions-across-processes
[§23.10]: ../../contract/examples-semantics.md#2310-streaming-and-interchange
[§23.12]: ../../contract/examples-semantics.md#2312-checked-and-exact-arithmetic
[§23.14]: ../../contract/examples-semantics.md#2314-demand-and-error-stages
[§24]: ../../contract/acceptance.md#24-acceptance-criteria-and-contract-test-matrix
[§24.1]: ../../contract/acceptance.md#241-acceptance-criteria
[§24.4]: ../../contract/acceptance.md#244-generators
[§24.5]: ../../contract/acceptance.md#245-coverage-of-the-required-dimensions
[Deliverable J]: #j-api-review-gate
[Deliverable K]: #k-contract-closure-pass
[Deliverable L]: review-closures.md#l-third-review-closure
[Deliverable Q]: owner-decision-closure.md#q-owner-decision-closure

<!-- /v0.5-modular:links -->
