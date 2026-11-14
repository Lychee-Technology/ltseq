<!-- v0.5-modular:header -->

# v0.5 review history: Numeric follow-ups (S)

[Index](../../README.md) › Review history · Previous: [API surface and interchange follow-ups (R)](follow-up-closure.md)

**Non-normative, historical.** A dated record of the [review](../../README.md#ltseq-v05-api-review), kept as written. Later records name the earlier records they change. [Deliverable Q] is the decision register, and this record applies three of its deferred rows.

**Scope.** The rows of #276: the numeric questions that the PR #250 assessment classed as not blocking the merge, and the kernel deviation it found in decimal division. Each row's resolution, the choices that were not obvious, the probes behind them, and what is still open.

**Most cited from here.** [§17] Numeric semantics and literals · [§14] Aggregation, partitioning and pivot · [§24] Acceptance criteria

<!-- /v0.5-modular:header -->

## S. Numeric follow-ups

#276 listed nine rows, each to be settled in the contract before the work item it names, and one kernel deviation. This pass, of 2026-11-14, settles every row in the contract text and names the [§24] row that tests it. It changes no engine code: each rule is implemented by the M item in the last column, and the [§24] rows named are specifications until then. The one code change is to the literal grid's model of the contract (`py-ltseq/tests/literal_grid/contract.py`, #275), which stated the old outcome of `r.f32 == 0.1`; the new rule is not in force there, so the grid's gate keeps grading those cells by what `main` does today.

### Owner rows

The owner recorded a direction for each of these on 2026-10-09 ([Deliverable Q]). This pass applies them; it makes no choice of its own.

| ID | Owner's direction | Applied in | Rows | Gate |
|---|---|---|---|---|
| N9 | Prefer `LTSeqValueError` at plan time for a float literal that `float32` cannot hold exactly, over rounding it silently | [§17.4] rule 3; [§17.2] and [§17.6] refer to it | N4 | M8 |
| G5 | Keep the current literal rules; no weak integer literals (NumPy's NEP 50) in v0.5 | [§17.2] now says so ("No weak literals"); no rule changed | N1 | M8 |
| OD-W | Keep exact window statistics (the exact value rounded once) as the target; reconsider only on evidence from #251 measurement 2 | No change: [§10.1] and [§14.2] already require it, and the contract carries no pending marker | W4, P10 | M4, M13 |

N9, like AC3 in [Deliverable R], turns a recorded preference into a rule, and it fixes a scope the owner's text left open (below). The final review should confirm both.

OD-W has its evidence: M0's measurement 2 is reported in `benchmarks/m0/REPORT.md` (PR #305). Exact rolling `mean` and `std` cost 2.0 to 3.8 times today's time with the prototype's BigInt finish, compensated (double-double) ones 1.4 to 1.6, which are deterministic for a fixed row order but neither exact nor order-independent. Whether that changes the target is the report's question 4, which the owner has not answered. Until then the text stands, and relaxing it is an amendment to [§10.1], [§14.2], W4 and P10.

### Other rows

| ID | Resolution | Contract | Rows | Gate |
|---|---|---|---|---|
| N14 | A `Decimal` literal's digits count its exponent: *p* = max(*d* + max(*e*, 0), *s*) with *s* = max(−*e*, 0), so `Decimal("1E+40")` has 41 digits and raises. No scale is ever negative: a negative-scale column is converted when read to scale 0 and precision *p* − *s*, widening when its width cannot hold that, and a negative scale as `cast`'s `dtype` raises `LTSeqTypeError`. A requested schema may still ask for one, exactly | [§17.2], [§6.2], [§17.3], [§17.6], [§16.5] | N2, T2, D9, X4 | M7, M8, M18 |
| N15 | All three outcomes are kept, with the reason stated: arithmetic gives `±inf` as IEEE does, a cast that would round a finite value to an infinity fails, and a literal is refused at plan time. The `float64` → `float32` bound is now exact, and a `decimal256` → `float32` cast fails at the same bound, where the table said "never fails" | [§17.6] | D9 | M7 |
| N17 | `quantile(q)` reads a `float` `q` as its shortest decimal, `Fraction(repr(q))` | [§14.2] | A2, P10 | M13 |
| N18 | `pct_change` is the exact value of the whole expression `x / x.shift(n) - 1` rounded once | [§10.1] | W2, P10 | M4 |
| AC14 | P10's reference settles overflow, NaN, infinities and signed zeros by the contract's own rules before it computes in `Fraction` | [§24] P10 | P10 | M13, and every item that tests P10 |
| AC15 | `nearest` compares exact distances for every key type, and so does `tolerance` | [§12.3] | J6 | M10 |
| N5 (kernel) | The contract stays as written; N3 has the failing cases and M6's row names the fix | [§24] N3, impact map M6 | N3 | M6 |

### Judgment

**N9 refuses comparisons only.** The owner's text names "a float literal that `float32` cannot hold exactly, next to a `float32` operand". In a comparison the literal's exact binary64 value meets the `float32` value, and the result is silently wrong for what the text says: `r.f32 == 0.1` is FALSE on every row, and `r.f32 <= 0.1` leaves out the rows that hold `float32` 0.1, which is 0.100000001490116… and so above binary64 0.1. In arithmetic nothing is lost: `r.f32 + 0.1` is `float64` ([§17.4]), the `float32` operand widens exactly, and the literal keeps its value. Refusing there would break `r.f32 * 1.1` for no gain. Shared values and fixed targets already refuse an inexact literal with `CastError` ([§17.5]). Integer and `Decimal` literals are left out because each is the number written, so an exact comparison answers what was asked (`r.f32 == 16777217` is FALSE because no `float32` is 16777217). A literal out of `float32`'s range raises too (`r.f32 < 1e39`): it is not a `float32` value, and `float("inf")` says the same thing exactly.

**N9 reads the literal's value, not its Python type.** [§17.2] makes `np.float32(0.1)` the `float64` literal of the `float32` value, so `r.f32 == np.float32(0.1)` passes the check and matches. That is the spelling the error message can suggest, next to `lit(0.1).cast("float32")`, which rounds explicitly. Making the rule depend on the Python type instead would refuse `np.float64` values that `float32` holds, such as `0.5`.

**G5 is stated, not only kept.** The contract already gave every `int` literal `int64` and every `float` literal `float64`, but said nothing about NumPy's rule, so a reader coming from NumPy 2 would expect `r.i8 + 1` to be `int8` and `r.f32 + 0.1` to be `float32`, as `np.int8` and `np.float32` arithmetic give (NumPy 2.5.4). The paragraph in [§17.2] says neither holds and points to where a literal does adapt to a fixed type: by being exact or refused, never by narrowing the result. N1 holds `r.i8 + 1` at 127, which is 128 in `int64` and would overflow `int8`.

**N14 converts negative scales on read instead of carrying them.** Carrying them would put negative *s* into every decimal formula of [§17.3] and [§14.2], which were written and checked for *s* ≥ 0: `sum` of `decimal128(5, -2)` would be `decimal128(15, -2)`, and `/` and `mean` would add their four digits of scale to a negative one. Python and Parquet handle them poorly too: pyarrow 25.0.1 gives a `decimal128(5, -2)` value as `Decimal('9.9999E+6')`, and Parquet has no type for one, which is why [§16.5] refused them. Refusing them at read would reject valid Arrow data. Conversion is exact, copies one column, and follows what [§6.2] already does for `float16` and `date64`. Its costs are that the reported type differs from the source's and can change width (`decimal128(38, -2)` becomes `decimal256(40, 0)`, and over 76 digits the read raises). A decimal inside a nested type keeps its type, because nothing computes on it, so [§16.5]'s Parquet refusal now applies only there.

**N14's literal sizing is what the code already does.** `_encode_decimal` (`py-ltseq/ltseq/expr/core_types.py`) computes the same *p* and *s*, a zero counting as one digit, and refuses *p* above 38. The contract now says it, and N2 holds the cases, so M8 has nothing to change for it. "Digits" counts the exponent because the value is an integer of *d* + *e* digits; reading `1E+40` as one digit would need a negative scale, which the previous paragraph rules out.

**N15's outcomes differ because the operations do.** IEEE arithmetic never raises, and `float32 * float32` overflowing is what the operands give, as in `float64` ([§17.1]). A cast is a request to round, but no rounding makes a finite value infinite; an infinity would be a value no input implies and would pass through later sums and comparisons, the reasoning behind D8's integer `/` ([Deliverable Q]) and behind the float → integer row's failure out of range. Python agrees (`struct.pack("f", 1e39)` raises `OverflowError`); NumPy and pyarrow 25.0.1 give `inf`, so D9 says neither is a reference. A literal's value is known at plan time, so its error comes then. Settling the row exposed two gaps. The `float64` → `float32` row said "beyond the `float32` range", which leaves the values between `float32`'s largest (3.4028234663852886e+38) and 2**128 − 2**103 (3.4028235677973366e+38) undecided: round-to-nearest takes them to the largest, and the row now says so. The integer, decimal → float row said "never fails", which a `decimal256` above that bound contradicts when the target is `float32`. Implicit conversions are unaffected: they convert integers and decimals to `float64` only, which holds every `decimal256`.

**N17 reads `q` as written.** NumPy computes *h* = *q*(*n* − 1) in floats, where `0.1 * 10` is `1.0`. The exact binary value of `0.1` exceeds 1/10, so the previous text made *h* exceed 1 by 2⁻⁵⁴, and `quantile(0.1)` of two `0` and nine `10**18` was `55.51115123125783`, where NumPy and the plain reading give the second value, `0.0`. The error is 2⁻⁵⁴ times the gap between neighbors, visible whenever that gap is large. The shortest decimal is what `repr` shows, so it is the `q` the user wrote whenever they wrote 17 or fewer significant digits, and P10's reference is `Fraction(repr(q))`. This departs from [§17.2]'s binary reading of a float literal, and is limited to `q`: `q` names a position among the values and is never compared or combined with them. `median` and the dyadic `q` (0.25, 0.5, 0.75) read the same either way.

**N18 rounds once.** Two roundings cost accuracy exactly where `pct_change` is used, on small changes: rounding the quotient leaves an error of up to half an ulp of 1, about 1.1e-16, which the `- 1` exposes in full. `int64` `[10**16, 10**16 + 2]` gave `2.220446049250313e-16` for a change of 2e-16, and `Decimal` `[100.00, 100.01]` gave `9.999999999998899e-05` for 0.0001. Once-rounding matches the statistics of [§14.2]. M4 already needs the exact quotient for integers above 2⁵³ and for decimals; the whole expression costs one exact subtraction more, (*x* − *b*) / *b* for base *b*, and for `float64` input exact arithmetic where one IEEE division used to do. The zero base, NaN and infinities keep IEEE's results, as D8 decided ([Deliverable Q]).

**AC14 states P10's reference where `Fraction` has no answer.** `Fraction` has no NaN, infinity or signed zero, and `float()` raises `OverflowError` for a value that rounds beyond the range, where the contract requires `±inf`. P10 now settles each by a rule the contract already has: overflow at the rounding boundary 2**1024 − 2**970; IEEE results for float arithmetic and `pct_change` with a non-finite operand or a zero divisor, and IEEE's sign for a zero from `+ - * /` ([§17.1]); [§14.2]'s non-finite rules for statistics, per running or window value. Its generated operands now include NaN, both infinities and both zeros, so the property tests these cases rather than skipping them.

**AC15 needs no new tie rule.** Comparing exact differences removes two failures. In `float64`, left key `0.25` against `-2.0**53` and `2.0**53` has distances 2**53 + 0.25 and 2**53 − 0.25, both of which round to `2.0**53`, so a rounded comparison ties and the tie rule picks `-2.0**53`, the farther key. In `int64`, left key `-1` against `2**63 - 1` is a distance of 2**63, which `int64` cannot hold. The existing tie rule (prefer `backward`) then applies only to true ties. Infinite keys needed a sentence: equal keys are at distance 0, infinities included, and two infinite distances tie. M10 replaces the baseline's binary search over `int64` times anyway, so this adds a comparison to the operator M10 builds rather than a fix to the old one.

**N5's deviation is confirmed, and `%` has it too.** On the baseline built from `83796a5`, `decimal128(38, 10)` 10¹⁵ `/` `decimal128(38, 10)` 1 raises `Arithmetic overflow: … 10000000000000000000000000 * 100000000000000`, although its `decimal128(38, 14)` result fits, and `decimal128(38, 0)` 10³⁰ `%` `decimal128(38, 10)` 7 raises the same way when it rescales the dividend by 10¹⁰, where the result is 1. `%` between operands of one scale does not rescale and does not fail. N3 holds both cases; M6 needs a 256-bit intermediate or a guarded fallback.

### Checks

- The values the contract cites were computed under CPython 3.13 with NumPy 2.5.4 and pyarrow 25.0.1: the `float32` bound and its neighbors, `struct.pack` raising at the bound, NumPy's and pyarrow's `inf`, NEP 50's `int8` and `float32` results, the `quantile`, `pct_change` and as-of examples in `fractions.Fraction`, `float()`'s `OverflowError` at 2**1024 − 2**970, and pyarrow's handling of negative scales. The two N3 cases were run against the baseline.
- `py-ltseq/tests/literal_grid` passes (777 tests, including the live gate) with the model's new rule `float32_literal_exact`.
- Every `[§N]` reference in the modules this pass changed resolves to a heading.

### Earlier records this pass changes

- [Deliverable Q]'s Deferred table: N9 and G5 are applied here, and OD-W is confirmed as the current text. Q is kept as written, with a pointer under its table.
- The impact map ([Deliverable G]): the rows of M4, M6, M7, M8, M10, M13 and M18 name what this pass gives them.

### Open

- **Owner confirmation of N9**, which applies a preference as a rule, and of its scope: comparisons only.
- **OD-W**: question 4 of the M0 report, exact or compensated windows and a budget for exact grouped statistics over many groups. M4 and M13 build to exact until it is answered.
- **N17** departs from [§17.2]'s binary reading for `q` alone; the final review should confirm it.
- **N5's deviation** is M6's to fix. The N3 cases are specifications until then.

<!-- v0.5-modular:footer -->

---

Previous: [API surface and interchange follow-ups (R)](follow-up-closure.md) · [Index](../../README.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§6.2]: ../../contract/schema-and-table-operations.md#62-supported-types
[§10.1]: ../../contract/windows-and-grouping.md#101-window-methods
[§12.3]: ../../contract/joins-and-sets.md#123-asof_join
[§14]: ../../contract/aggregation.md#14-aggregation-partitioning-and-pivot
[§14.2]: ../../contract/aggregation.md#142-aggregate-expressions
[§16.5]: ../../contract/streaming-and-output.md#165-writers
[§17]: ../../contract/numeric-null-temporal.md#17-numeric-semantics-and-literals
[§17.1]: ../../contract/numeric-null-temporal.md#171-integer-and-decimal-results-are-checked
[§17.2]: ../../contract/numeric-null-temporal.md#172-literals
[§17.3]: ../../contract/numeric-null-temporal.md#173-arithmetic-operators
[§17.4]: ../../contract/numeric-null-temporal.md#174-types-of-mixed-operands
[§17.5]: ../../contract/numeric-null-temporal.md#175-shared-values
[§17.6]: ../../contract/numeric-null-temporal.md#176-explicit-casts
[§24]: ../../contract/acceptance.md#24-acceptance-criteria-and-contract-test-matrix
[Deliverable G]: ../impact-map.md#g-implementation-impact-map
[Deliverable Q]: owner-decision-closure.md#q-owner-decision-closure
[Deliverable R]: follow-up-closure.md#r-api-surface-and-interchange-follow-ups

<!-- /v0.5-modular:links -->
