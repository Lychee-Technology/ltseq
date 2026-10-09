<!-- v0.5-modular:header -->

# v0.5 review history: Third to sixth review closures (L–O)

[Index](../../README.md) › Review history · Previous: [API review gate and contract closure pass (J, K)](review-gate.md) · Next: [Assessment closure pass (P)](assessment-closure.md)

**Non-normative, historical.** A dated record of the [review](../../README.md#ltseq-v05-api-review), kept as written. Later records name the earlier records they change, and [Deliverable Q] is the current decision register.

**Scope.** Four independent review closures in order: findings F17–F21 with decisions D13 and D14 ([Deliverable L]), F22–F27 ([Deliverable M]), F28–F30 ([Deliverable N]), and F31 with the order of tuple `partition` keys ([Deliverable O]).

**Most cited from here.** [§17] Numeric semantics and literals · [§16] Output and interchange · [§24] Acceptance criteria and contract test matrix · [§20] Errors

<!-- /v0.5-modular:header -->

## L. Third review closure

The [third independent API review](https://github.com/Lychee-Technology/ltseq/pull/250#issuecomment-6065997539) read the contract at `fff58f5` and requested changes. It did not ask to reopen the design. It found two guarantees that no implementation could meet (its findings 1 and 2, here F17 and F18) and three rules that conflicted with others or with their own tests (F19–F21). This section records how this revision closes each. It was written after the revision.

### Findings and resolutions

| ID | Review's severity | Finding | Root cause | Resolution | Contract |
|---|---|---|---|---|---|
| F17 | High | `partition` merged two keys that [§18] keeps apart. In New York, 01:30 EDT and 01:30 EST on 2024-11-03 are an hour apart, but as `ZoneInfo` datetimes they compare equal and hash equally, so a `dict` keyed by them holds one partition. Tuple keys that contain them collapse the same way | [§14.5] took "converted without loss" to mean "distinct as dict keys". Python compares two `datetime`s that share a `tzinfo` by wall clock and ignores `fold` (PEP 495), so lossless values can still collide. K/F15 made the same assumption | [§14.5] now states the requirement that was missing: two dict keys are equal exactly when their key values are equal under [§18]. An aware timestamp key is its UTC instant, which compares by instant. A float zero key is always `0.0`, because a `dict` keeps whichever zero came first. A lookup with a `to_dicts` value in a repeated hour raises `KeyError` and never returns another key's table; `v.astimezone(timezone.utc)` finds every key (D13). [§16.3] warns that `to_dicts` values have the same equality, and says an aware value converts only when both its UTC instant and its wall clock fall in the years 1 to 9999, as pyarrow requires, so every key that converts has a UTC form. [§24.1] compares aware values as instants, so a test cannot pass by the same accident | [§14.5], [§16.3], [§17.2], [§24.1], P14, A4, N2, [§23.8] |
| F18 | High | P2 required `f(g(t))`, `f(g(t).collect())` and the pickle form to agree for every `f` and `g`. A snapshot demands values that `f` may drop: with a zero `d`, `t.derive(q=n // d).drop("q")` succeeds and `t.derive(q=n // d).collect().drop("q")` raises | P2 was written for plans that succeed, and its error clause assumed the failing value is demanded in every form. [§5.2] and [§16.6] never said that a snapshot demands every value | Lazy demand is unchanged; P2 is narrowed. It holds where `g(t).collect()` succeeds. Where the snapshot fails, `pickle.dumps` fails with it, and `f(g(t))` follows [§20.2] on its own. [§5.2], [§16.6] and [§20.2] state that `collect` and `pickle.dumps` demand every value. Z2 holds the `drop` and `head(0)` counterexamples, [§23.14] the `drop` one, and X5 the pickle error. Writing the new P2 exposed a second gap: equality also fails when `g` or `f` reads a clock function or chooses rows that principle 9 leaves open (`head` over an undefined order or through tied rows, `distinct(keep="any")`). P2 now excludes both | [§5.2], [§16.6], [§20.2], P2, Z2, X5, [§23.14] |
| F19 | Medium | [§21.1] bounded the memory of "every terminal" over the streaming pipelines, so the bound covered `to_arrow`, `to_pandas` and `to_dicts`, whose result is every output row | The bound was stated for the pipeline and then applied to its consumers, with no line between the execution's memory and the result a caller keeps | The bound covers the execution. A result that is returned or stored is outside it, and so is the Arrow data `to_pandas` and `to_dicts` convert from. That applies to `to_arrow`, `to_pandas`, `to_dicts`, `collect`, `pickle.dumps`, and a table built from the C stream. For the C stream, the bound covers LTSeq's side; what the consumer keeps is its own. R3 measures the process's peak memory over draining consumers, during the call and the reading of its result: `to_batches` and iteration that drop what they read, both writers, and `count` | [§21.1], [§15.1], [§23.10], R3 |
| F20 | Medium | [§22] makes annotations binding, but every value and condition argument was annotated `ExprFn` or `AggFn`, which are callables. So `derive(flag=False)`, `derive(x=None)` and `filter(lit(True))`, which [§7] accepts, failed type checking, and under [§22]'s own rule they raised | `ExprFn` meant "a lambda" in the prose and "any expression argument" in the signatures. The [§22] preamble also claimed that annotation and prose agree in both directions, which Python's types cannot express: a `bool` is an `int` | Two new aliases cover value arguments: `RowExpr = IntoExpr \| ExprFn` and `AggExpr = IntoExpr \| AggFn`. A condition argument is `Expr \| ExprFn` (`Expr \| AggFn` for `NestedTable.filter`), because no literal is a condition. The preamble now promises one direction only: the annotation admits every argument the prose accepts. Where it admits more (`delete(True)`, a lambda that returns a Python `bool`), the prose decides. `Literal` now names the NumPy scalars [§17.2] accepts, and positions take `numpy.integer`, which the consistency pass below found missing. Conditions inside an expression (`when`, `where=`, `&`, `\|`) stay `IntoExpr`, because the `is None` rewrite of [§3.4] needs it. S2 runs pyright over the documented call forms, positive and negative | [§2.2], [§2.3], [§7], [§10], [§11], [§14], [§22], S2, [§24.1] |
| F21 | Medium | [§19.2]'s table allowed `duration * integer` and `duration // integer`. The paragraph below it, and [§17.4] rule 4, raised for any number with a temporal value | [§17.4] and [§19.2] restated ADR 0018 D-l as a blanket rule, and the duration rows were added without amending those restatements | Integer scaling stays, as a stated exception to D-l (D14). `duration * integer`, `integer * duration` and `duration // integer` are checked and keep the duration's unit; `//` floors in that unit. Every other number with a temporal value raises `LTSeqTypeError` at plan time: a float factor, `/`, a `Decimal`, a `bool`, and `+` or a comparison with a number. A zero divisor raises `DivisionByZeroError`, and overflow `ArithmeticOverflowError`. H1 holds the positive, negative and boundary cases | [§1.4], [§17.4], [§19.2], [§20.1], H1 |

No row, property, example or section was added: the counts in C, F and I still hold at 24 sections, 95 rows, 18 properties and 18 examples.

### What the probes showed

Python 3.14.7, pyarrow 25.0.1, pyright 1.1.411, and the extension built at `3041b44`. The scripts are in the PR comment that answers the review.

- **F17.** `pa.array(...).to_pylist()` on the two New York instants gives `fold=0` and `fold=1` values that compare equal and hash equally. Converted with `astimezone(timezone.utc)`, they are two keys. Lookup by either `to_pylist` value misses both UTC keys; lookup by its `astimezone(timezone.utc)` hits. As `("E", ts)` tuples, the zone values give one key and the UTC values two. `{-0.0: 1, 0.0: 2}` is `{-0.0: 2}`.
- **F18.** The baseline already shows the demand difference. Over `n = [1]`, `d = [0]`, `derive(q=n // d).drop("q").to_dicts()` and `.head(0).to_dicts()` succeed, and `.collect()` raises. Pickling a table is not supported at baseline, so the pickle half is unprobed.
- **F20.** The probe transcribed [§2.3]'s aliases and the affected [§22] signatures. With the `fff58f5` aliases, pyright reports 14 errors on documented calls, the review's three among them. With the new aliases, it reports none on 24 documented call forms. It infers each lambda's parameter as `Row` or `Group`, and it reports each of the seven refused forms: `filter(True)`, `filter(1)`, `filter(None)`, `search_first("x")`, `NestedTable.filter(True)`, an `object()` value and a two-parameter lambda. With NumPy added to `Literal` and to the positions, it accepts seven more documented forms: NumPy positions, values and operands, the `datetime64` and `timedelta64` scalars, and `pd.Timestamp` and `pd.Timedelta` values. It refuses `filter(np.bool_(True))`, `delete(np.bool_(True))` and an array value. NumPy is installed in that environment; a type checker without NumPy was not tried.
- **F21.** At baseline, DataFusion 55 plans none of `d * 2`, `2 * d`, `d // 2`, `d * k` or `d // k` for a `duration[s]` `d`, so integer scaling is new work under M20. Python's `timedelta(seconds=-5) // 2` is −2.5 s: Python floors at microseconds, as `duration[us]` does under [§19.2].

### Consistency pass before commit

A separate read of the whole diff against the review found one more instance of F20 and ten smaller problems, all fixed before commit. The F20 instance: `Literal` named no NumPy scalar and positions took only `int`, so `derive(k=np.int64(3))` and `delete(np.int64(2))`, which [§17.2] and [§7.10] accept, failed type checking, and under [§22]'s rule they raised. The smaller problems:

- [§14.5] judged an aware key's year range in UTC, while `to_dicts` fails when either the UTC instant or the wall clock is out of range.
- P2's exclusions missed `head` over an undefined order.
- [§21.1] and [§23.10] bounded the memory of third-party stream consumers, and the DuckDB line built a lazy relation that never ran.
- R3 measured memory only during the call, before `to_batches` or iteration read anything.
- [§2.3] did not say why conditions inside an expression stay `IntoExpr`.
- Five wording errors, in this section and in [§19.2].

### Earlier records this revision changes

- **K/F15 and D11.** K said that no key is converted lossily, so two distinct key values never merge. F17 shows that the conclusion does not follow. D11 itself, raising `CastError` for a key `to_dicts` cannot convert, is unchanged. So is K's reason for not giving nanosecond keys another type.
- **K's agreement table.** Its partition-key row is superseded by L's row for F17 below.
- **Acceptance gate.** Items 7, 8, 10 and 13 and the pending-decisions note cite L. Items 8, 13 and 15 stay pending until the owner decides D1–D14. The Limit paragraph names F21 as one more restated-rule conflict. It also names F17 and F18 as kinds of conflict that a re-read does not find.
- **Impact map.** M10 takes the key representation of F17, M12 the stub check of F20, and M20 duration scaling with H1. M1 now says what it never materializes for: restoring the order. [Deliverable B]'s `partition` row and the #156 decision carry F17 and F18.

### Agreement across the contract

Each fix touched a rule restated in several places. Those places were reread together and edited where they disagreed:

| Rule | Places |
|---|---|
| Partition keys and aware equality (F17) | [§14.5], [§16.3], [§17.2], [§20.2]'s call stage, [§24.1], P14, A4, N2, [§23.8]; B's `partition` row, M10 |
| Snapshot demand (F18) | [§5.2], [§16.6], [§20.2], P2, Z2, X5, [§23.14]; #156 in D |
| Streaming memory (F19) | Principle 10, [§15.1], [§21.1], [§23.10], R3, M1 |
| Annotations (F20) | [§2.2], [§2.3], [§3.4], the [§7] preamble, [§7.10], [§17.2], the [§22] preamble and stubs, S2, [§24.1] item 1; B's added-names row, M12 |
| Numbers with temporal values (F21) | [§1.4], [§17.4] rule 4, [§19.2], [§20.1], H1, M20 |

### Baseline behavior found during this pass

- **`partition` fetches by SQL text.** `SQLPartitionedTable` formats the key into a `WHERE` clause. A `date32` key gives an empty table for every listed key, because `"k" = 2024-01-03` parses as integer subtraction. Timestamp, binary and NaN keys raise `KeyError`. Filed as #258. M24 removes the class, and A4 covers date, timestamp and DST-fold keys.

### Owner decisions

The contract applies one option for each. The owner accepts it or names an alternative.

| ID | Decision | Applied | Recommended by | What the owner should weigh |
|---|---|---|---|---|
| D13 | `partition` keys for aware timestamps | The UTC instant, `tzinfo=timezone.utc` | The review's first option | Keys print in UTC, and `parts[v]` with a `to_dicts` value in a repeated hour raises `KeyError`. Fixed-offset keys (`timezone(timedelta(hours=-5))`) would keep the local wall clock, but the key's `tzinfo` would vary with the offset and the lookup would fail the same way. Raising on a collision, the review's second option, would make hourly partitioning in New York fail every November |
| D14 | Durations with numbers | `duration * integer`, `integer * duration` and `duration // integer`, checked, in the duration's unit; any other number raises. The owner kept this and asked for the floor, the zero divisor and the unit sensitivity to be stated in full (Q) | This revision; the review left the choice open | Without the exception, the exact way to scale a duration is repeated addition, since `total_seconds()` is a `float64`. With it, `//` floors in the column's unit, so `duration[s]` −5 s `// 2` is −3 s where Python's microsecond `timedelta` gives −2.5 s |

### Open

- The owner's acceptance of, or alternative to, each of D1–D14.
- An independent review of this revision.
- The F16 cases not run, in #256.
- The rewrite-table rows whose evidence is "Source", which no probe has exercised.

## M. Fourth review closure

The [fourth independent API review](https://github.com/Lychee-Technology/ltseq/pull/250#issuecomment-6067156063) read the contract at `baf4ffd` and requested changes, without reopening the object model or the expression DSL. Its five findings (here F22–F26) are rules that conflicted with other rules, with their own tests or with the library they cite. Its notes also reported that pyarrow's Parquet writer rejects some types; checking that against the contract found F27. This section records how this revision closes each. It was written after the revision.

### Findings and resolutions

| ID | Review's severity | Finding | Root cause | Resolution | Contract |
|---|---|---|---|---|---|
| F22 | Medium | [§9.2] listed `NestedTable.derive` with `first`, `last` and `flatten` as keeping the input's `sort_keys`, and [§11.2] said the same, but grouped `derive` replaces columns as `LTSeq.derive` does. `sort("k").group_ordered("k").derive(k=lambda g: -g.k)` kept a key that the rows no longer satisfy | The propagation table sorted the grouped methods by whether they move rows, and grouped `derive` moves none. Whether a method rewrites key columns, which [§7.3]'s truncation rule turns on, was asked only for `LTSeq` methods | [§9.2] moves `NestedTable.derive` to the row of the column-changing methods: the input's order, with keys renamed or truncated as [§7] states. [§11.2] says that replacing a key column truncates `sort_keys` before that key. P7 now requires replacing, renaming and dropping each key of a multi-key `sort_keys` in turn, the first and a later one. G3 holds the grouped cases | [§9.2], [§11.2], P7, G3, [§24.5] |
| F23 | Medium | P10 required decimal `**` to equal the exact result or raise, and [§17.1] listed `**` among the checked operations, while [§17.3] gives `**` with a decimal operand a `float64` result computed from converted operands. `Decimal("0.1") ** 2` cannot be both `1/100` and a float | [§17.1] and P10 keyed exactness on the operand types, and [§17.3] keys behavior on the result type. The same mismatch hit two more operations. P10's float clause computed every float `/` from converted operands, which integer `/` does not do: `(2**54 + 3) / 3` differs. [§10.1] defined `pct_change` with [§17.3]'s `/`, which for decimals is a truncated decimal, in a column typed `float64` | [§17.1] keys the rule on the result type: an integer or decimal result of arithmetic or an aggregate is exact except where its own rule rounds, and checked; a float result follows the float rules, whatever its operands. It names the float results with integer or decimal operands and says which take exact operands (integer `/`, `pct_change`, the statistics) and which convert first (`**`, the math functions). P10 makes the same split. `pct_change` is the exact quotient rounded once for every input type, with IEEE results at a zero, as integer `/` is. N3 holds `Decimal("1.1") ** 2`, W2 a decimal `pct_change` | [§8.4], [§10.1], [§14.2], [§17.1], [§17.3], [§17.6], P10, N3, W2, A2 |
| F24 | Medium | P4 required `list(t)` to raise from `to_arrow()`'s error set. Converting to Python adds failures that Arrow output does not have: a `timestamp[ns]` value of 1 ns makes iteration raise `CastError`, ahead of a later Arrow error or with none at all | [§20.2] defines the error set over the values a terminal demands, and [§16.3]'s conversion failures were never placed in it. P4 then compared two terminals whose outputs live in different value domains | [§16.3] places them: a value Python cannot hold is one more failing demanded value of each operation that converts to Python (`to_dicts`, iteration, `fold`, `partition` keys, and `to_pandas` where its backend makes Python objects) and of no Arrow output. `to_dicts`' set is therefore `to_arrow`'s plus one `CastError` per such value. P4 compares within each domain: `to_batches` and the C stream with `to_arrow`, iteration with `to_dicts`, and its inputs include a value that cannot be converted, ahead of a later failure and alone. [§15.2], [§20.2] and [§24.1] say the same; R2 holds the review's example | [§10.7], [§15.2], [§16.2], [§16.3], [§20.2], [§24.1], [§24.4], P4, R2, X1, X2 |
| F25 | Medium | [§17.2] defined an aware literal's value as the instant `value.timestamp()` gives. That method returns binary64 seconds, so far from 1970 it loses the microsecond: for `datetime(2300, 1, 1, microsecond=1, tzinfo=timezone.utc)` it gives tick …002 where the value is …001 | L's fix for F17 named the method that honors `fold`, as the definition of the value, without checking that the method is exact | The value is the instant in integer microseconds, `(value - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(microseconds=1)`. Python computes it from `utcoffset()`, so `fold` still picks the occurrence, and nothing is rounded. pyarrow and the baseline encoder compute the same value. N2 holds the 2300 and year-1 values, and [§24.4] generates such literals | [§17.2], N2, [§24.4] |
| F26 | Low | [§4.2]'s recovery from an inference error suggested `schema={"h": "decimal128(38, 0)"}`, which [§6.3] refuses: `pa.type_for_alias` has no such alias | An example written apart from [§6.3]'s rule | `pa.decimal128(38, 0)`. T3 lists a parameterized type written as a string among the refused forms. A search of both documents found no other string type that `pa.type_for_alias` refuses | [§4.2], T3 |
| F27 | — (the review's notes) | [§16.5] promised that reading a written Parquet file gives back `t.schema`, and [§6.2] that pass-through columns can be written. Parquet has no type for `union`, its `INTERVAL` holds neither negative nor sub-millisecond intervals, its decimals have no negative scale, and it has no second unit. pyarrow writes `timestamp[s]` and `time32[s]` in milliseconds and reads them back so, and [§4.3] makes `read_parquet` report what pyarrow reads. [§16.2] defined `to_pandas` types through a Parquet file, so a `timestamp[s]` column became milliseconds there too and a `union` column had no defined type | The contract assumed that Parquet holds every Arrow type, in three places | `write_parquet` raises `LTSeqTypeError` at the call for a column whose type is or contains a `union`, an `interval` or a negative-scale decimal (N/F29 adds a `struct` with no fields and `fixed_size_binary(0)`), as `write_csv` does for pass-through types. The file stores the Arrow schema, as pyarrow does by default. A `timestamp` or `time32` in seconds, nested or not, reads back in milliseconds with the same values (for a `timestamp`, within ±9223372036854775 seconds: N/F28), and [§4.3] stands. `to_pandas` types are defined through a Feather (Arrow IPC) file, which holds every Arrow type: `timestamp[s]` stays in seconds, a column type that pandas cannot convert under the chosen backend (`union` with `"numpy_nullable"`) raises `LTSeqTypeError` at the call, and a value that backend cannot hold raises [§16.3]'s `CastError` | [§6.2], [§16.2], [§16.5], [§20.2], P16, T2, X1, X4 |

No row, property, example or section was added: the counts in C, F and I still hold at 24 sections, 95 rows, 18 properties and 18 examples.

**Two choices in F27.** A second unit could instead be restored from the Arrow schema that pyarrow stores in the file, which still records `timestamp[s]`. That would make `read_parquet` report a schema other Arrow readers do not report for the same file, against [§4.3], and `cast` restores the unit exactly where it is wanted. For `union` and `interval`, an LTSeq-private encoding would write files that other Parquet readers misread, so the writer refuses them.

### What the probes showed

Python 3.14.7, pyarrow 25.0.1, pandas 3.0.5, and the extension built at `3041b44`. The scripts are in the PR comment that answers the review.

- **F23.** `float(Decimal("0.1")) ** 2` is `0.010000000000000002`, the correctly rounded square of the binary64 value nearest 0.1. That square lies 0.02 ulp from a rounding midpoint, so a `pow` with an error above 0.02 ulp may return the double nearest `0.01` instead. N3 uses `Decimal("1.1") ** 2`, which gives `1.2100000000000002` and lies 0.46 ulp from the nearest midpoint, so any `pow` with an error below 0.46 ulp returns it. P10 compares `**` on decimal operands with `**` on their `float64` casts in the same implementation, so it does not depend on the platform's `pow` either. For integer `/`, `(2**54 + 3) / 3` is `6004799503160662.0` from the exact quotient and `6004799503160663.0` from converted operands.
- **F25.** Exact ticks against `round(value.timestamp() * 1_000_000)`, all aware:

  | Value | Exact tick | Through `timestamp()` |
  |---|---|---|
  | 2300-01-01 00:00:00.000001 UTC | 10413792000000001 | 10413792000000002 |
  | 0001-01-01 00:00:00.000001 UTC | -62135596799999999 | -62135596800000000 |
  | 9999-12-31 23:59:59.999999 +05:30 | 253402280999999999 | 253402281000000000 |
  | 1969-12-31 23:59:59.999999 UTC | -1 | -1 |

  New York 01:30 on 2024-11-03 is 1730611800000000 with `fold=0` and 1730615400000000 with `fold=1`, an hour apart; in the 02:30 gap on 2024-03-10 the two methods also agree. pyarrow's conversion and the baseline's `_encode_datetime` (`py-ltseq/ltseq/expr/core_types.py:90-109`, integer arithmetic on the `timedelta`) give the exact tick in every case.
- **F26.** `pa.type_for_alias` accepts `int32`, `int64`, `float64`, `string` and `double`, every string type the two documents use, and raises `ValueError: No type alias for decimal128(38, 0)`.
- **F27.** pyarrow's Parquet writer raises for `decimal128(5, -2)` ("Scale must be a non-negative integer"), for sparse and dense unions and for `month_day_nano_interval` ("Unhandled type for Arrow to Parquet schema conversion"). It writes `decimal32`, `decimal256`, `duration[ns]`, `large_string` and `map` and reads them back unchanged. `timestamp[s]`, `timestamp[s, tz=UTC]` and `time32[s]` are stored as milliseconds; the Arrow schema stored in the file still says seconds, but `pq.read_schema` reports milliseconds, while `duration[s]`, which Parquet stores as a plain integer, comes back in seconds. `pandas.read_parquet` gives `timestamp[ms][pyarrow]` for a `timestamp[s]` column; `pandas.read_feather` gives `timestamp[s][pyarrow]` with `"pyarrow"` and `datetime64[s]` with `"numpy_nullable"`, and converts `union` and `month_day_nano_interval` with `"pyarrow"`. With `"numpy_nullable"` it raises `ArrowNotImplementedError` for `union` and returns `object` for the interval.
- **Consistency pass.** pyarrow's Parquet writer raises for `struct<interval>`, `list<union>` and `list<decimal128(5, -2)>`, and reads `list<timestamp[s]>`, `struct<t: timestamp[s]>` and `map<string, time32[s]>` back in milliseconds. With `store_schema=False`, `duration[s]` reads back as `int64`, `large_string` as `string`, and `timestamp[us, tz=Europe/Paris]` as `timestamp[us, tz=UTC]`. `pandas.read_feather` with `"numpy_nullable"` returns `object` for `date32`, `time64` and `decimal128` columns and raises for a `date32` in year 10000 (`ValueError: year must be in 1..9999`) and a 1 ns `time64[ns]` (`ArrowInvalid: Value 1 has non-zero nanoseconds`); `timestamp[us]` in year 10000 and 1 ns `timestamp[ns]` and `duration[ns]` become `datetime64` and `timedelta64` without error, and `"pyarrow"` holds every case. `np.median`, `np.quantile` and `pyarrow.compute.quantile` give `4503599627370496.0` for `int64` `[1, 2**53 + 1]`; the exact median is `4503599627370497.0`. `np.quantile([1.0, inf], 0.5)` is NaN.

### Baseline behavior found during this pass

- **Grouped `derive` cannot replace a column.** Over `sort("a", "b")`, `group_ordered(lambda r: r.a).derive(lambda g: {...})` that adds a column keeps both sort keys. One that replaces `a` or `b` raises `RuntimeError: Group derive failed: Schema error: Schema contains qualified field name "?table?".b and unqualified field name b which would be ambiguous`. Replacement, and the truncation F22 adds, are new work under M2 and M27.
- **Decimal `pct_change` is a decimal.** On a `decimal128(10, 2)` column, `pct_change()` gives `decimal128(17, 6)`, and a zero previous value fails the whole query with `ValueError: Failed to collect results: Arrow error: Divide by zero error`. M4 replaces it with the `float64` rule of [§10.1].

Neither is filed: v0.5 replaces both behaviors, and neither returns a wrong value without an error.

### Consistency pass before commit

A separate reread of the revision against the whole contract found that four of these fixes did not reach every rule they touch, and a few phrases that no longer held. All were corrected before commit:

- **F24 stopped at `to_dicts`.** Values are also converted to Python by `fold`, whose `fn` receives rows as `to_dicts` gives them, by `partition`, whose keys [§14.5] already made raise `CastError` at the call (D11), and by `to_pandas`, whose `"numpy_nullable"` backend makes dates, times and decimals Python objects. [§16.3] now names these four as the operations that raise its `CastError`s; `fold`'s Errors and [§16.2] state theirs, and [§20.2], [§24.1] and X1 follow. `to_arrow`, `to_batches` and the C stream still raise none. [§20.2]'s Call stage now also covers a writer or `to_pandas` refusing a column type, which F27 added.
- **F23's [§17.1] took in conversions.** Its list included decimal rescaling, which made a `cast` that does not fit raise `ArithmeticOverflowError` where [§17.6] raises `CastError` and `try_cast` gives NULL. [§17.1] now covers arithmetic and aggregates and refers conversions to [§17.6], and P10's list drops rescaling. It also counted every math function as a float result, while `sign` keeps its input type ([§8.4]); both now except `sign`.
- **`median` and `quantile` had no exact definition.** [§17.1] says the statistics work from exact operands, but [§14.2]'s rule named only `sum`, `mean`, `var`, `std`, `cov` and `corr`, although `median` and `quantile` are also `float64` over integer and decimal input. Evaluated as NumPy and pyarrow do, converting first, the median of `[1, 2**53 + 1]` is `4503599627370496.0`; the exact value is `4503599627370497.0`. [§14.2] now defines `quantile` as the exact interpolation rounded once, with its infinities, and A2 and P10 hold it.
- **F27 checked only top-level types.** pyarrow also refuses a `union`, `interval` or negative-scale decimal nested in a `list` or `struct`, and writes a second unit nested in a `list`, `struct` or `map` in milliseconds. [§16.5], [§6.2], T2 and X4 now say "is or contains" and cover the nested unit. The same probe showed what the Arrow schema stored in the file carries (`duration`, `large_string`, the zone name), so [§16.5] requires the file to store it and [§6.2]'s rule that metadata is dropped excepts it.
- **Phrases.** P16's "a second unit" also covered `duration[s]`, which reads back in seconds; it now names `timestamp` and `time32`. [§11.2] said `flatten` adds no column, while `flatten(group_id=...)` appends one. P10 said `pct_change` is the exact quotient rounded once, while [§10.1] subtracts 1 from it in `float64`, as `x / x.shift(n) - 1` and pandas do; P10 now says that. Before the reread, X2 and [§17.6] were also brought in line with F24 and F23.

### Earlier records this revision changes

- **AR7 (H).** Its resolution said that P10 keeps exactness for integers and decimals. It now says integer and decimal results, as F23 requires.
- **Audit H1 and the #221 decision.** `**` is checked only where its result is an integer; with a decimal operand it is `float64`.
- **L/F17.** [§17.2]'s rule that `fold` picks the occurrence of a repeated hour stays. Only its reference to `timestamp()`, added with F17, is replaced (F25).
- **Impact map and inventory.** M2 takes grouped key truncation and G3, M4 the exact `pct_change`, M5 the narrower `**` and N3, and M22 the Parquet and `to_pandas` type rules. [Deliverable B]'s `NestedTable.derive` row says that replacement is new.
- **Acceptance gate.** Items 8 and 13 cite M. The Limit paragraph names F22 and F23 as restated-rule conflicts, F24 as a comparison across value domains, and F25 and F27 as conflicts that only a probe shows.

### Agreement across the contract

Each fix touched a rule restated in several places. Those places were reread together and edited where they disagreed:

| Rule | Places |
|---|---|
| Key truncation when a column is replaced (F22) | [§7.3], [§9.2], [§11.2], P7, G3, [§24.5]; B's `NestedTable.derive` row, M2 |
| Exactness keyed on the result type (F23) | [§8.4], [§10.1], [§14.2], [§17.1], [§17.3], [§17.6], P10, P11, P12, N3, W2, A2; Audit H1, #221, AR7, M4, M5, M13 |
| Error sets per output domain (F24) | [§10.7], [§14.5], [§15.1], [§15.2], [§16.2], [§16.3], [§16.4], [§20.2], [§24.1], [§24.4], P4, P14, A4, R2, X1, X2 |
| The exact value of an aware literal (F25) | [§17.2], P12, N2, [§24.4] |
| `DTypeLike` strings (F26) | [§4.2], [§6.3], T3 |
| What Parquet and pandas hold (F27) | [§4.3], [§6.2], [§16.2], [§16.5], [§20.2], P16, T2, L4, X1, X4; M22 |

### Owner decisions

None is added. Each finding is resolved by a rule the contract already states ([§7.3]'s truncation, [§17.3]'s result types, [§20.2]'s demand, [§4.3]'s schema), so each has one consistent fix. D2's applied rule, exact statistics rounded once, now also covers `median` and `quantile`. For them it costs one exact interpolation between two sorted values, not an exact accumulator, so M0 has nothing to price; the owner's decision on D2 covers them. D1–D14 still await the owner (Deliverables J, K and L).

### Open

- The owner's acceptance of, or alternative to, each of D1–D14.
- An independent review of this revision.
- The F16 cases not run, in #256. The review's reader prototype supports the split between Python reads and the C stream, but not the preservation of classes through a Rust iterator, native error codes, DuckDB or Polars integration, or the per-batch GIL cost; #256 gates M1 on them, and #251's measurements gate the items that M0 lists.
- The rewrite-table rows whose evidence is "Source", which no probe has exercised.

## N. Fifth review closure

The [fifth independent API review](https://github.com/Lychee-Technology/ltseq/pull/250#issuecomment-6068290168) read the contract at `a91d7ea` and requested changes for three Medium findings (here F28–F30). Each is a rule that promised a whole value or type domain where the format or library behind it holds only part: Parquet's millisecond unit, Parquet's physical schema, and NumPy's units. A non-blocking note found that P8 could pass without computing a window. This section records how this revision closes all four. It was written after the revision.

### Findings and resolutions

| ID | Review's severity | Finding | Root cause | Resolution | Contract |
|---|---|---|---|---|---|
| F28 | Medium | [§16.5] promised that a `timestamp[s]` column reads back from Parquet in milliseconds with the same values. A value beyond ±9223372036854775 seconds has no `int64` count of milliseconds, and pyarrow's writer raises `ArrowInvalid` for it, nested and aware too. X4 gave the failure no class or stage | The contract stated unit and calendar conversions as exact without the target's range. F27 added the Parquet case, and [§17.6] had the same gap: a cast to a finer unit, from `date32` to `timestamp` and from `timestamp` to `date32` can each leave the target's range. So could [§6.2]'s conversion of `date64` to `date32` when a table is read | The writer converts as `cast` to milliseconds does: a value out of range raises `CastError` during execution, and the atomic write leaves `path` as it was. [§17.6] names the range failure of each conversion that has one, and [§6.2] that of `date64`. [§20.1]'s `CastError` now names the conversions defined outside [§17.6], and [§4.4] the one `from_arrow` error that is not at the call. P16 covers the values [§16.5] writes, to CSV as to Parquet, since CSV's years 1 to 9999 are the same kind of limit. X4 holds ±9223372036854775 and one beyond, nested; D9 the casts; T2 `date64`; [§24.4] generates the boundary | [§4.4], [§6.2], [§16.5], [§17.6], [§20.1], P16, T2, D9, X4, [§24.4], [§24.5] |
| F29 | Medium | [§6.2] and [§16.5] let a `struct` be written to Parquet unless it contains a refused type. A `struct` with no fields contains none, and neither pyarrow nor the Rust `parquet` crate writes one, at any depth and with 0 rows | F27 listed the types Parquet has no logical type for, and missed those its physical schema cannot hold. Checking the other zero-width Arrow types found a second: pyarrow refuses `fixed_size_binary(0)`, a `FIXED_LEN_BYTE_ARRAY` of length 0. A third, `fixed_size_list` of size 0, can be written (below) | `write_parquet` refuses both, at any depth and with 0 rows, with `LTSeqTypeError` at the call, as it refuses a `union`. A dummy child field or a wider binary would change the schema that [§4.3] requires other readers to report. Feather, which defines the `to_pandas` types, holds both | [§6.2], [§16.5], P16, T2, X4 |
| F30 | Medium | [§17.2] gave a `numpy.timedelta64` the type `duration` at its own unit, and a `numpy.datetime64` `timestamp` at its own unit or in seconds for a coarser one. Arrow has only `s`, `ms`, `us` and `ns`, so a duration in days, or any value in `ps`, `fs` or `as`, named a type that does not exist | The literal table was written from Arrow's four units. NumPy has thirteen, multiplied units such as `10ms`, and a generic unit | A NumPy value keeps its unit where Arrow has it, and takes seconds for a coarser fixed unit and nanoseconds for a finer one; a multiplied unit counts in its base unit. The conversion is exact or raises `LTSeqValueError` at plan time, checked for a remainder below a nanosecond and for the `int64` range, because NumPy's own conversion truncates and wraps without an error. A `timedelta64` in years or months, or with the generic unit, raises `LTSeqValueError`: NumPy gives years and months average lengths, a value with the generic unit takes whichever unit it meets, and [§19.1] keeps calendar arithmetic apart from durations. A `datetime64` in years or months is the instant at the start of that period and converts. `np.timedelta64("NaT")` joins the other missing markers | [§17.2], P12, N2, [§24.4] |
| P8 | Note | P8 compared `t.derive(c=w).drop("c")` with `t`. [§20.2] lets an execution prune `c` without computing it, so P8 could pass with no window evaluated | A property stated over a plan whose output does not demand the value under test | P8 drops `c` from `t.derive(c=w).to_arrow()`, which demands it, and says why. W1 already tests windows with their output kept | P8 |

No row, property, example or section was added: the counts in C, F and I still hold at 24 sections, 95 rows, 18 properties and 18 examples.

**Classes and rejected alternatives.** F28's failure is a `CastError`: [§20.1] gives that class to a value a conversion cannot hold and `ArithmeticOverflowError` to arithmetic, and the writer's change of unit is a conversion. The same value then fails the same way under `t.cast(...)` and in the writer. An operand's change of unit inside `timestamp[s] − timestamp[ns]` stays `ArithmeticOverflowError`, as H1 already tests, because [§19.2] makes it part of checked arithmetic; [§20.1] now says so. Writing the seconds as a plain `INT64`, as the baseline does, would keep every value, but pyarrow then reads an `int64` column, against [§4.3]. Refusing `timestamp[s]` at the call, or narrowing `from_arrow`, would refuse a whole type for values 292 million years from 1970. F30's failures are `LTSeqValueError` because the Python type is supported and the value is not, as for `Decimal("NaN")` and a 39-digit `Decimal`. pandas draws the same line: `pd.Timedelta(1, "M")` raises `ValueError`.

### What the probes showed

Python 3.14.7, pyarrow 25.0.1, NumPy 2.4.4, pandas 3.0.5, and the extension built at `3041b44`. The scripts and their output are in the PR comment that answers the review.

- **F28.** At ±9223372036854775 seconds, pyarrow writes `timestamp[s]`, `timestamp[s, tz=UTC]`, `list<timestamp[s]>`, `struct<t: timestamp[s]>` and `map<string, timestamp[s]>` and reads each back in milliseconds with exact ticks. At ±9223372036854776 and at `2**63 - 1`, each raises `ArrowInvalid: Integer overflow when casting timestamp value … from timestamp[s] to timestamp[ms]`. `duration[s]` reads back in seconds at both `int64` ends, and `time32[s]` holds only [0, 86400), so neither can overflow. pyarrow's casts raise an out-of-bounds error for `timestamp[s]` and `duration[s]` 9223372036854776 to `ms`, and for `date32` 1677-09-21 and 2262-04-12 to `timestamp[ns]`; 1677-09-22 and 2262-04-11 convert. Its cast to `date32` wraps instead: `timestamp[s]` `2**62` gives day -1857971038, and `timestamp[ms]` at `2**31` days gives -2147483648, with no error. A `timestamp[us]` or `timestamp[ns]` never leaves `date32`'s range. A `date64` at `2**31` days or at `-2**31 - 1` passes full validation and fails the cast to `date32` ("would lose data"); `2**31 - 1` and `-2**31` convert.
- **F29.** pyarrow refuses `struct<>` at top level, with 0 rows, and inside a `list`, a `struct`, a `map` value and a `fixed_size_list` ("Cannot write struct type 'x' with no child field to Parquet"). It refuses `fixed_size_binary(0)` at top level, with 0 rows and in a `list` ("Invalid FIXED_LEN_BYTE_ARRAY length: 0"). Feather round-trips every one of these. The Rust `parquet` crate 59.2.0 rejects a struct with no fields (`src/arrow/schema/mod.rs:801–803`) and checks a fixed length only for being negative.
- **F30.** `pa.duration` accepts only `s`, `ms`, `us` and `ns`. NumPy's `W`, `D`, `h` and `m` are 604800, 86400, 3600 and 60 seconds; its `Y` and `M` convert to 31556952 and 2629746 seconds, an average Gregorian year and month. `np.timedelta64(5)` has the generic unit, while `np.datetime64(5)` raises for want of one. `np.datetime64(5, "10ms")` has the unit `('ms', 10)` and is 50 ms. NumPy's own conversion gives `np.datetime64(1001, "ps")` as 1 ns, `np.datetime64(2**60, "D")` as 0 s and `np.datetime64(2**62, "10s")` as `-2**63` s, all with no error. 106751991167300 is the largest day count whose seconds fit `int64`, at either sign. `np.datetime64("2024-03", "M")` is 1709251200 s. pandas raises `ValueError: Units 'M', 'Y', and 'y' are no longer supported` for `pd.Timedelta(1, "M")`.
- **No contract change.** pyarrow cannot read back a `fixed_size_list` column with a NULL row ("Expected all lists to be of size=2 but index 2 had size=0"), whether pyarrow or the baseline wrote the file; LTSeq reads its own file back unchanged. A `fixed_size_list<int64, 0>` column with two rows is the same case: pyarrow writes it and cannot read its own file back ("Expected all lists to be of size=0 but index 1 had size=1"), while LTSeq and pyarrow both read the baseline's file back equal. P16 is an LTSeq round trip and holds for both, but a test that takes pyarrow as the reference for such a column fails for pyarrow's reason.

### Baseline behavior found during this pass

- **A failed write destroys the old file.** Both writers truncate `path` before the step that can fail (`src/ops/io.rs:42` and `:115`). A `struct<>` column makes `write_parquet` raise `RuntimeError: Failed to create Parquet writer: Arrow: Parquet does not support writing empty structs` and leave a 0-byte file. A `fixed_size_binary(0)` column panics in the `parquet` crate ("chunk size must be non-zero"), which reaches Python as `pyo3_runtime.PanicException`, a `BaseException` that `except Exception` does not catch, and leaves a 4-byte file. `write_csv` of a `list` column raises `RuntimeError` and leaves 2 bytes. Filed as #259. In v0.5 the refusals at the call and M22's atomic writers remove all three.
- **Seconds are written as plain integers.** The baseline writes a `timestamp[s]` column, 9223372036854776 included, with no overflow, and pyarrow reads the column back as `int64`.
- **The literal encoder refuses a value it can hold.** `_encode_datetime64` (`py-ltseq/ltseq/expr/core_types.py:112-129`) checks exactness by converting back with NumPy, whose round trip through seconds turns day -106751991167300 into day +106751991167300. So that value raises although its seconds fit `int64`, while +106751991167300 converts. Every coarse-unit refusal says "cannot be represented in nanoseconds". `numpy.timedelta64` raises `TypeError`.

Only the first is filed: it destroys a file that was on disk. v0.5 replaces the other two under M22 and M8, and neither loses a value.

### Consistency pass before commit

A separate reread of this revision against both whole documents found five places the fixes had not reached. Each was fixed before commit:

- **P8 and P16 were stated for values that fail.** [§24.2]'s properties hold for every generated input, and [§24.4] generates both ends of every integer range, where `cum_sum` overflows once the export demands `c`. P8 now holds wherever `t.derive(c=w).to_arrow()` succeeds. P16's CSV clause still said "the types [§16.5] lists", although [§16.5] already refused dates outside the years 1 to 9999 there. It now says "types and values", as the Parquet clause does.
- **[§4.4] said all of `from_arrow`'s errors are at the call.** [§6.2]'s `date64` failure is during execution, so [§4.4] now lists it as the exception.
- **[§20.1]'s `CastError` named only [§17.6].** The `date64` conversion and the writers' conversions are defined elsewhere. The row now names them, and the `ArithmeticOverflowError` row says that a change of unit inside temporal arithmetic is overflow, as H1 already tests.
- **[§17.2] gave the wrong reason for refusing a duration with no unit.** NumPy gives it no average length; it takes whichever unit it meets.

Smaller changes from the same pass: [§16.5] and X4 say "at any depth of nesting" rather than naming `list`, `struct` and `map`, since a probe shows `large_list` and `fixed_size_list` behave the same; T2 and D9 hold both sides of each range bound and the `duration[s]` cast; N2 names `pd.NaT` and `np.datetime64("NaT")` separately; P12 (d) gives its refusals as examples; [§24.4] says whose range its NumPy values reach; [§24.5]'s Overflow row adds D9, T2, X4 and N2; and M18 is retitled to cover type normalization. The same pass found a third zero-width type, `fixed_size_list` of size 0, which needs no contract change (above).

### Earlier records this revision changes

- **M/F27.** Its resolution promised the millisecond round trip for every `timestamp[s]` value; it now cites F28's range. Its list of refused types points to F29's two.
- **Impact map.** M7's casts fail outside the target's range, M8 takes the NumPy units and names `core_types.py`, where `_encode_datetime64` lives, M18 takes the `date64` range and is retitled to cover type normalization, and M22 the two refused types and the millisecond conversion.
- **Acceptance gate.** Items 8 and 13 cite N, and the Limit paragraph names F28–F30 as domains that only a probe shows.

### Agreement across the contract

Each fix touched a rule restated in several places. Those places were reread together and edited where they disagreed:

| Rule | Places |
|---|---|
| The range of a unit or calendar conversion (F28) | [§4.4], [§6.2], [§16.5], [§17.6], [§19.2], [§20.1], P16, T2, D9, X4, [§24.4]; M/F27, M7, M18, M22 |
| Types Parquet cannot hold (F29) | [§6.2], [§16.2], [§16.5], P16, T2, X4; M/F27, M22 |
| NumPy temporal literals (F30) | [§17.2], [§19.1], [§19.4], P12, N2, [§24.4]; M8 |
| A property's output demands what it tests (P8 note) | [§20.2], P8, W1 |

### Owner decisions

None is added. F28's class follows `cast` and [§20.1], F29 extends F27's refusal, and F30 follows the rule for inexact literals, so each has one consistent fix. One choice could go the other way: `np.timedelta64(1, "M")` could mean one calendar month, but the contract has no calendar-duration type for it to become, and refusing it now leaves that open. D1–D14 still await the owner (Deliverables J, K and L).

### Open

- The owner's acceptance of, or alternative to, each of D1–D14.
- An independent review of this revision.
- #256 and #251, which still gate M1 and the items M0 lists.
- The rewrite-table rows whose evidence is "Source", which no probe has exercised.
- #259, whether to make the baseline writers atomic before v0.5.

## O. Sixth review closure

The [sixth independent API review](https://github.com/Lychee-Technology/ltseq/pull/250#issuecomment-6069469312) read the contract at `61af29e` and found nothing that should block it. It reported one Low finding (here F31) and asked how `partition` orders tuple keys that hold `None`. This section records how this revision closes both. It was written after the revision.

### Findings and resolutions

| ID | Review's severity | Finding | Root cause | Resolution | Contract |
|---|---|---|---|---|---|
| F31 | Low | [§21.1] said `reverse` and `distinct` SHOULD use memory bounded by the window, group or partition. `reverse` emits the last row first, so it holds its whole input unless the source can be read backwards, and `distinct` remembers every key it has seen | [§21.1] put six operations under one bound without checking each against its definition. Rows leave in input order, so while a row waits for a later row of its partition, every output row after it waits too, and with `partition_by` those are other partitions' rows, without bound. That breaks the bound for a negative `n` in table-order window methods and for `search_pattern`, each with `partition_by`. A `forward` or `nearest` `asof_join` with `by`, on inputs sorted by the as-of key first, breaks it another way: it reads right rows of other `by` values ahead to find a left row's match. That makes three more of the six. The operations the lists left out fared worse. Principle 10 allows whole-table materialization only where [§21.1] lists it, and the MUST bullet named `filter`, `select` and `search_first` with no exclusion and excluded only window methods from `derive`. In one pass, four kinds of operation need more than that allows: window methods and ranking functions with `.over(order_by=...)`, whose order is not table order; `ntile`, which numbers rows by their partition's size; aggregates over windows ([§10.3]), which put on a partition's first row a value that depends on its last; and `asof_join` on unsorted input, which can match a left row with any right row. No bullet let `join` hold its right input either, although the MUST bullet assumes it does | Each SHOULD operation gets the bound its definition implies, counting the rows that wait. Window methods in table order get the frame in each partition, plus, for a negative `n`, the rows up to the row it reads or to the end of the input. `row_number`, `rank` and `dense_rank` in table order get a count and the last row's sort values and rank in each partition. `group_ordered` gets one group, which also bounds the aggregates and windows that run within it, plus what the window methods in `starts_when` need. `search_pattern` gets the rows one match spans and what its steps' window methods need, plus, with `partition_by`, the matches that wait for an earlier start. `asof_join` on inputs sorted for a merge gets one right row per `by` value for `backward`, plus the right rows read ahead for `forward` and `nearest`. `distinct` gets each key, with its latest row for `keep="last"`. `reverse` MAY hold its whole input beside `sort`, as MAY the first three omitted kinds. `join`, `semi_join`, `anti_join` and any other `asof_join` MAY hold their right input. The MUST bullet keeps `filter`, `select`, `derive` and `search_first` only when their expressions have no window method, ranking function or aggregate over a window; the SHOULD and MAY bullets govern those. [§15.1] now names the pipelines [§21.1] requires to stream | [§15.1], [§21.1] |
| Question | — | [§14.5] ordered keys "ascending under [§9.4], with the `None` key last", which leaves `("E", None)` and `("W", "x")` unordered | The sentence was written for one key column | The order is that of `sort(*keys)` with its defaults: the first column most significant, NULL last in each column. [§14.5] gives the example `("E", "x")`, `("E", None)`, `("W", "x")`, `(None, "x")`, and A4 tests it | [§14.5], A4 |

No row, property, example or section was added: the counts in C, F and I still hold at 24 sections, 95 rows, 18 properties and 18 examples. R3 still measures the MUST bullet's pipelines, and [§23.10]'s pipeline has no window, so it is still one of them.

**What stays permissive.** The finding called `tail` the same shape as `reverse`. It is not: `tail(n)` needs only the last `n` rows, and the baseline's `tail` holds none of the table, since it counts the rows in one execution and slices in another (`py-ltseq/ltseq/transforms.py:849-869`). `tail` stays in the MAY bullet anyway. So do `pivot`, which needs one output row per group, and `intersect` and `difference`, which need their right input and, with `distinct=True`, the left rows already returned. `ntile` and aggregates over windows could likewise avoid holding rows by reading their input twice, first for each partition's size or aggregate. A MAY grants latitude and contradicts no definition, so narrowing one is a new requirement rather than a fix. For the same reason `distinct` stays a SHOULD rather than joining the MUST bullet, although `group_by().agg` with `first` or `last` holds one row per group in the same way and is a MUST.

### Earlier records this revision changes

- **Acceptance gate.** Item 7 cites O's F31 for which operations must stream, should hold only what they need, or may hold the whole input. Item 13 counts F31, and the Limit paragraph records its kind of conflict.
- **#202 in D.** Its Consequences said that restoring table order after a window MUST NOT materialize. A window with `.over(order_by=...)` cannot meet that in one pass, so the record now defers to the bound [§21.1] gives the window itself. [§21.1]'s bullet on restoring order after parallel execution (#148, V7) is unchanged, because restoring adds nothing to the pipeline's own bound.

### Agreement across the contract

| Rule | Places |
|---|---|
| Streaming memory (F31) | Principle 10, [§15.1], [§21.1], [§23.10], R3, M1; gate item 7; #202 in D |
| Partition dict order | [§14.5], A4, [§23.8] |

Principle 10 refers to every list in [§21.1], which now names each operation that may hold the whole input. [§23.10], R3 and M1 refer only to the MUST bullet and to restoring order, and the pipelines they use are still in that bullet. [§23.8]'s `list(parts) == ["E", "W", None]` already follows the new dict-order wording.

### Owner decisions

None is added. Two tightenings are possible, and consistency needs neither: holding `tail(n)` to its last `n` rows as a SHOULD, and moving `distinct` into the MUST bullet, which would add it to R3. D1–D14 still await the owner (Deliverables J, K and L).

### Open

- The owner's acceptance of, or alternative to, each of D1–D14.
- Whether to take either tightening above.
- An independent review of this revision, in particular of the [§21.1] bounds, which no implementation has tested.
- #256 and #251, which still gate M1 and the items M0 lists.
- The rewrite-table rows whose evidence is "Source", which no probe has exercised.
- #259, whether to make the baseline writers atomic before v0.5.

<!-- v0.5-modular:footer -->

---

Previous: [API review gate and contract closure pass (J, K)](review-gate.md) · [Index](../../README.md) · Next: [Assessment closure pass (P)](assessment-closure.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§1.4]: ../../contract/overview.md#14-relation-to-earlier-decisions
[§2.2]: ../../contract/public-surface.md#22-other-modules
[§2.3]: ../../contract/public-surface.md#23-type-aliases-used-in-signatures
[§3.4]: ../../contract/public-surface.md#34-lambdas-and-proxies
[§4.2]: ../../contract/loading-and-laziness.md#42-ltseqread_csv
[§4.3]: ../../contract/loading-and-laziness.md#43-ltseqread_parquet
[§4.4]: ../../contract/loading-and-laziness.md#44-ltseqfrom_arrow
[§5.2]: ../../contract/loading-and-laziness.md#52-ltseqcollect
[§6.2]: ../../contract/schema-and-table-operations.md#62-supported-types
[§6.3]: ../../contract/schema-and-table-operations.md#63-data-type-arguments-dtypelike
[§7]: ../../contract/schema-and-table-operations.md#7-basic-table-operations
[§7.3]: ../../contract/schema-and-table-operations.md#73-derive
[§7.10]: ../../contract/schema-and-table-operations.md#710-value-level-edits-insert-delete-update
[§8.4]: ../../contract/expressions.md#84-math-functions
[§9.2]: ../../contract/ordering.md#92-sources-and-propagation
[§9.4]: ../../contract/ordering.md#94-sort
[§10]: ../../contract/windows-and-grouping.md#10-windows-and-ordered-computation
[§10.1]: ../../contract/windows-and-grouping.md#101-window-methods
[§10.3]: ../../contract/windows-and-grouping.md#103-aggregates-over-windows
[§10.7]: ../../contract/windows-and-grouping.md#107-fold
[§11]: ../../contract/windows-and-grouping.md#11-ordered-grouping
[§11.2]: ../../contract/windows-and-grouping.md#112-nestedtable
[§14]: ../../contract/aggregation.md#14-aggregation-partitioning-and-pivot
[§14.2]: ../../contract/aggregation.md#142-aggregate-expressions
[§14.5]: ../../contract/aggregation.md#145-partition
[§15.1]: ../../contract/streaming-and-output.md#151-to_batches
[§15.2]: ../../contract/streaming-and-output.md#152-iteration
[§16]: ../../contract/streaming-and-output.md#16-output-and-interchange
[§16.2]: ../../contract/streaming-and-output.md#162-to_pandas
[§16.3]: ../../contract/streaming-and-output.md#163-to_dicts
[§16.4]: ../../contract/streaming-and-output.md#164-arrow-pycapsule-stream
[§16.5]: ../../contract/streaming-and-output.md#165-writers
[§16.6]: ../../contract/streaming-and-output.md#166-pickle
[§17]: ../../contract/numeric-null-temporal.md#17-numeric-semantics-and-literals
[§17.1]: ../../contract/numeric-null-temporal.md#171-integer-and-decimal-results-are-checked
[§17.2]: ../../contract/numeric-null-temporal.md#172-literals
[§17.3]: ../../contract/numeric-null-temporal.md#173-arithmetic-operators
[§17.4]: ../../contract/numeric-null-temporal.md#174-types-of-mixed-operands
[§17.6]: ../../contract/numeric-null-temporal.md#176-explicit-casts
[§18]: ../../contract/numeric-null-temporal.md#18-null-nan-and-boolean-logic
[§19.1]: ../../contract/numeric-null-temporal.md#191-types
[§19.2]: ../../contract/numeric-null-temporal.md#192-arithmetic-and-comparison
[§19.4]: ../../contract/numeric-null-temporal.md#194-dt-methods
[§20]: ../../contract/errors-and-performance.md#20-errors
[§20.1]: ../../contract/errors-and-performance.md#201-exception-classes
[§20.2]: ../../contract/errors-and-performance.md#202-stages
[§21.1]: ../../contract/errors-and-performance.md#211-materialization
[§22]: ../../contract/api-reference.md#22-complete-canonical-api-reference
[§23.8]: ../../contract/examples-sequences.md#238-partitions-across-processes
[§23.10]: ../../contract/examples-semantics.md#2310-streaming-and-interchange
[§23.14]: ../../contract/examples-semantics.md#2314-demand-and-error-stages
[§24]: ../../contract/acceptance.md#24-acceptance-criteria-and-contract-test-matrix
[§24.1]: ../../contract/acceptance.md#241-acceptance-criteria
[§24.2]: ../../contract/acceptance.md#242-properties
[§24.4]: ../../contract/acceptance.md#244-generators
[§24.5]: ../../contract/acceptance.md#245-coverage-of-the-required-dimensions
[Deliverable B]: ../inventory.md#b-api-inventory-and-review
[Deliverable L]: #l-third-review-closure
[Deliverable M]: #m-fourth-review-closure
[Deliverable N]: #n-fifth-review-closure
[Deliverable O]: #o-sixth-review-closure
[Deliverable Q]: owner-decision-closure.md#q-owner-decision-closure

<!-- /v0.5-modular:links -->
