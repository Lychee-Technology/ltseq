<!-- v0.5-modular:header -->

# v0.5 review history: Temporal follow-ups (T)

[Index](../../README.md) › Review history · Previous: [Numeric follow-ups (S)](numeric-follow-up-closure.md)

**Non-normative, historical.** A dated record of the [review](../../README.md#ltseq-v05-api-review), kept as written. Later records name the earlier records they change. [Deliverable Q] is the decision register. This record applies one of its deferred rows, N7, with the direction the owner recorded on 2026-10-09: keep the fixed rule.

**Scope.** The rows of #277: the temporal questions that the PR #250 assessment classed as not blocking the merge. Each row's resolution, the choices that were not obvious, the probes behind them, and what is still open.

**Most cited from here.** [§19] Temporal semantics · [§12] Joins · [§24] Acceptance criteria

<!-- /v0.5-modular:header -->

## T. Temporal follow-ups

#277 listed five rows, N7, N10, N13, N16 and N19, each to be settled in the contract before the work item it names. [Deliverable Q] had already settled one of N19's four gaps: D14 gave `duration ± duration` of mixed units the finer unit. This pass, of 2026-11-15, settles the rest in the contract text and names the [§24] row that tests each. It changes no code. Each rule is implemented by the M item in the last column, and the [§24] rows named are specifications until then. The literal grid (#275) has no time or duration context, so its model of the contract gains no rule.

### Owner row

| ID | Owner's direction | Applied in | Rows | Gate |
|---|---|---|---|---|
| N7 | Keep [§19.4]'s strict rejection of nonexistent and ambiguous local times, with no `ambiguous=` or `nonexistent=` options in v0.5 | [§19.4] now says so, and states that the rule holds at every unit of `truncate`; [§14.5]'s advice for aware keys is corrected | H3, A4, P15 | M20 |

[Deliverable Q] lists N7 as deferred, with "no contract change needed, if kept". As with G5 in [Deliverable S], the direction keeps the current rule, so this pass states it rather than leaving the absence of an option to be inferred. An unknown keyword already raises `TypeError` under the closed signatures of [§22], so the sentence adds no rule, only the reason the keywords are missing. The row's second half did not depend on the decision. [§14.5] advised partitioning on `r.ts.dt.truncate("second")` when a key does not convert to Python, and for an aware key that raises in a repeated hour. It now advises truncating in UTC, and [§19.4] gives the reason.

### Other rows

| ID | Resolution | Contract | Rows | Gate |
|---|---|---|---|---|
| N10 | `total_seconds()` is the exact count divided by the units in a second, rounded once to nearest, as integer `/` is | [§19.4] | H2 | M20 |
| N13 | A `date64` value that is not a whole day, and a `time32` or `time64` value outside one day, raise `CastError` during execution, from every source | [§6.2], [§4.4], [§20.1] | T2 | M18 |
| N16 | "Duration, time" becomes two classes, so a duration key with a time key raises `LTSeqTypeError` at plan time | [§12.1] | J1 | M7 |
| N19: `-d`, `abs(d)` | A [§19.2] row: checked as for integers, in the duration's unit, so the count −2**63 raises `ArithmeticOverflowError`. On a date, time or timestamp both raise `LTSeqTypeError` | [§19.2], [§8.2] | H1 | M20 |
| N19: `dt.add` overflow | Each step adds one exact total; a step whose result the type cannot hold raises `ArithmeticOverflowError` during execution | [§19.4] | H3 | M20 |
| N19: date tolerance | An `asof_join` `tolerance` for date keys must be a whole number of days, else `CastError` at plan time | [§12.3] | J6 | M23 |
| N19: mixed units | Settled by D14 ([Deliverable Q]): the finer unit | [§19.2] | H1 | M20 |

### Judgment

**N7 keeps the fixed rule because adding the options later breaks nothing.** The PR #250 assessment recommended this, and the owner took it. With the rule fixed, every nonexistent or ambiguous result raises. An `ambiguous=` or `nonexistent=` keyword added later, defaulting to the current behavior, would turn some of those errors into values and change no result a program gets today. Adding the keywords now and removing them or changing their defaults later would change results, so the cautious order is this one.

**Truncation raises at every unit, so [§14.5]'s advice changes, not [§19.4].** `truncate("second")` of 01:30:00.5 EST on 2024-11-03 in New York gives the local time 01:30:00, which occurs twice. The input's offset says which occurrence is meant: Python's `replace(microsecond=0)` keeps `fold`, and `zoneinfo` then gives 06:30:00Z. A rule that kept the input's offset for sub-hour units was considered and not adopted. It would change the rule the owner's direction keeps, and whether `truncate` raises would then depend on its unit. It can still be added later, since it would only turn errors into values. Truncating in UTC needs no rule change and loses nothing for `partition`, whose dict key is the UTC instant ([§14.5]). Every IANA offset is a whole number of seconds, so for `second` the UTC result is the instant that local truncation gives wherever it does not raise. For `minute` the two can differ: `zoneinfo` gives `Africa/Monrovia` −00:44:30 in 1960, and `Europe/Amsterdam` +01:19:32 in 1930. [§19.4] says so, so that nobody reads the UTC form as a general replacement for local truncation.

**N10 rounds once, which is Python's result.** CPython computes `timedelta.total_seconds()` by dividing the microsecond count by 10**6 as `int / int`, which is correctly rounded. For `duration[us]` the rule therefore agrees with Python, and #277's invariant asks for that. pandas 3.1.1 gives the once-rounded value in nanoseconds too: `pd.Timedelta(9007199254740995, unit="ns").total_seconds()` is `9007199.254740994`. NumPy converts the count to `float64` first and gives `9007199.254740996`, so it is not a reference. The two readings differ only above 2**53 units, where the count does not convert exactly. Below that, one IEEE division of two exact operands is already correctly rounded, so an implementation needs the exact path only for longer durations. In nanoseconds, 2**53 units is about 104 days.

**N13 refuses invalid values instead of defining them.** Arrow's format requires a `date64` value to be a multiple of 86,400,000 ms and a time value to lie in [0, one day), and pyarrow 26.0.1's `validate(full=True)` refuses both. pyarrow's operations disagree about what such a value means. `to_pylist` floors a `date64` of −1 ms to 1969-12-31, while `cast(..., pa.date32(), safe=False)` truncates it toward zero to 1970-01-01, and the safe cast refuses it. A `time32[s]` of 86400 gives `hour()` 0 and `to_pylist` `00:00`, wrapping modulo a day. Defining a value would choose one of these readings and silently move rows by a day. Such values do reach LTSeq through files: pyarrow writes a `time32[s]` of 90,000 to Parquet as `time32[ms]` 90,000,000 and reads it back unchecked, which is T2's `read_parquet` case. It also writes a `date64` of 86,400,001 ms to Parquet as `date32` 1, dropping the millisecond without an error. `CastError` during execution is what [§6.2] already raised for a `date64` beyond `date32`, so the class and the stage follow that rule. For `date64`, the check is part of a conversion that already reads every value. Time columns were not converted before, so for them the check is new work: a range comparison per value read, with no copy. Under [§20.2] an undemanded value is never checked. This cost has not been measured.

**N16 makes `join` agree with `==`.** [§19.2] already refuses to compare a duration with a time ("any other pairing"), and [§17.6] has no cast between them. The join class was the only place a duration met a time of day, and it matched them because both count units from zero: a 1-hour duration matched 01:00. Two classes make the key rule agree with the comparison rule. A program that relied on the old pairing has no cast to fall back on, and none is added. `asof_join` was not affected, because its key classes already left out time.

**`-d` and `abs(d)` follow integers, and leave out instants.** A duration is an `int64` count of units, and Python defines `-timedelta` and `abs(timedelta)`, so the row negates the count or takes its absolute value in the duration's unit. Only the count −2**63 cannot be negated. Python has no such case, because `-timedelta.min` is `timedelta(999999999)`, which fits; the row states the overflow, which #277's invariant allows. Python raises `TypeError` for `-datetime`, and LTSeq raises `LTSeqTypeError` for the negation of a date, time or timestamp in the same way.

**`dt.add` checks each step and computes exact totals.** The months step clamps the day to the length of the resulting month, and a month beyond the type's range has no length, so the step's own result must fit even when the days step would bring the value back into range. Inside each step the total is exact, so `add(years=1, months=-12)` returns its input at the range's ends, and the result does not depend on how the caller split a total between `years` and `months`. The class is `ArithmeticOverflowError`, as for `timestamp ± duration`, because the result is arithmetic that does not fit, not a value that fails to convert. The bound is the type's and not Python's: Python raises `OverflowError` past year 9999, while `date32` holds dates millions of years away, and the contract already holds dates outside the years 1–9999, refusing them only where they convert to Python objects or to text ([§16.3], [§17.6]).

**A date tolerance is whole days, like a duration added to a date.** Distances between dates are whole days, so `timedelta(hours=36)` would act as one day. Flooring it silently would be the one place a temporal literal is rounded implicitly, which [§17.6] rules out, and [§19.2] already refuses `date + timedelta(hours=1)` with `CastError` at plan time. The tolerance follows that rule. For timestamp and duration keys nothing changes: `tolerance` is compared with the exact distance ([§12.3]), so a tolerance finer than the key's unit means what it says.

### Checks

- The values cited were computed under CPython 3.13.18 with NumPy 2.5.4, pandas 3.1.1 and pyarrow 26.0.1: the `total_seconds` values in `fractions.Fraction`, `timedelta`, pandas and NumPy, with 20,000 random microsecond counts above 2**53, within the `timedelta` range, showing no difference between Python and the once-rounded value; pyarrow's validation, casts, field functions and Parquet round trip of invalid `date64` and time values; the `zoneinfo` offsets and the repeated hour in New York; `-timedelta.min`.
- None of these rows was probed against the LTSeq baseline. Each is a specification for its M item, and no row depends on current behavior.
- Every `[§N]` reference in the modules this pass changed resolves to a heading.

### Earlier records this pass changes

- [Deliverable Q]'s Deferred table: N7 is applied with the owner's direction. Q is kept as written, with a pointer under its table.
- The impact map ([Deliverable G]): the rows of M7, M18, M20 and M23 name what this pass gives them, and M23 now lists J6.

### Open

- **Truncation at every unit**: [§19.4] read literally makes `truncate("second")` raise in a repeated hour although the input's offset settles it, and [§14.5]'s advice now relies on that reading. The final review should confirm it.
- **N13's check** on time columns adds work at every read of a time value. Its cost has not been measured.
- Every row is a specification until its M item implements it.

<!-- v0.5-modular:footer -->

---

Previous: [Numeric follow-ups (S)](numeric-follow-up-closure.md) · [Index](../../README.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§4.4]: ../../contract/loading-and-laziness.md#44-ltseqfrom_arrow
[§6.2]: ../../contract/schema-and-table-operations.md#62-supported-types
[§8.2]: ../../contract/expressions.md#82-operators
[§12]: ../../contract/joins-and-sets.md#12-joins
[§12.1]: ../../contract/joins-and-sets.md#121-join
[§12.3]: ../../contract/joins-and-sets.md#123-asof_join
[§14.5]: ../../contract/aggregation.md#145-partition
[§16.3]: ../../contract/streaming-and-output.md#163-to_dicts
[§17.6]: ../../contract/numeric-null-temporal.md#176-explicit-casts
[§19]: ../../contract/numeric-null-temporal.md#19-temporal-semantics
[§19.2]: ../../contract/numeric-null-temporal.md#192-arithmetic-and-comparison
[§19.4]: ../../contract/numeric-null-temporal.md#194-dt-methods
[§20.1]: ../../contract/errors-and-performance.md#201-exception-classes
[§20.2]: ../../contract/errors-and-performance.md#202-stages
[§22]: ../../contract/api-reference.md#22-complete-canonical-api-reference
[§24]: ../../contract/acceptance.md#24-acceptance-criteria-and-contract-test-matrix
[Deliverable G]: ../impact-map.md#g-implementation-impact-map
[Deliverable Q]: owner-decision-closure.md#q-owner-decision-closure
[Deliverable S]: numeric-follow-up-closure.md#s-numeric-follow-ups

<!-- /v0.5-modular:links -->
