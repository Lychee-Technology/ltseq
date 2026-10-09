<!-- v0.5-modular:header -->

# v0.5 review history: Adversarial review and final consistency check (H, I)

[Index](../../README.md) › Review history · Previous: [Examples, test matrix and implementation impact map (E, F, G)](../impact-map.md) · Next: [API review gate and contract closure pass (J, K)](review-gate.md)

**Non-normative, historical.** A dated record of the [review](../../README.md#ltseq-v05-api-review), kept as written. Later records name the earlier records they change, and [Deliverable Q] is the current decision register.

**Scope.** The adversarial review of the first draft and its 22 findings ([Deliverable H]), then the final consistency check: its mechanical checks, the problems found and fixed, and the acceptance gate ([Deliverable I]).

**Most cited from here.** [§17] Numeric semantics and literals · [§9] Ordering contract · [§24] Acceptance criteria and contract test matrix · [§8] Expression DSL

<!-- /v0.5-modular:header -->

## H. Adversarial review

After the first draft was complete, a reviewer who had not taken part in drafting it worked through the brief's fifteen-item checklist (§7 of the brief), looking for counterexamples rather than confirming happy paths. External claims were checked by running pyarrow 25.0.1, pandas 3.0.5 and numpy 2.4.4, and by reading the crate sources `Cargo.lock` pins (`datafusion-functions-aggregate` 55.0.0, `datafusion-functions-aggregate-common` 55.0.0, `arrow-arith` 59.2.0). Polars is not installed, so no finding relies on Polars behavior.

The review found 22 problems: 6 S1, 6 S2, 6 S3 and 4 S4. Severity runs from S1 (two conforming implementations can return different results, or a documented idiom returns a wrong one) to S4 (a small gap). Every finding was fixed in the contract and the fix carried into the test matrix ([§24]), the trade-off record, the impact map and [Deliverable D]. None was deferred to implementation. The contract text in this PR is the revised one.

### Findings and dispositions

| ID | Sev. | Finding | Disposition | Contract |
|---|---|---|---|---|
| AR1 | S1 | The `cast` prose said "exact or fail", while the [§17.6] table rounds and truncates. `lit(1.5).try_cast("int64")` could be `1` or NULL. | `cast` and `try_cast` follow the table. Where a rule rounds or truncates, both return that value; implicit conversions of literals and other supplied values never round. Recorded as a trade-off. | [§8.5], [§17.6], D9 |
| AR2 | S1 | `search_pattern(partition_by=)` did not scope windows inside steps, so `shift(1)` at a symbol's first row read the previous symbol's last row, and the contract's own "per symbol" example matched across symbols. | With `partition_by`, steps read only their partition, so `shift(1)` on a partition's first row is NULL. | [§10.1], [§10.6], W8 |
| AR3 | S1 | "Every execution checks each pair" for `assume_sorted` contradicted row-wise demand: one implementation would raise from `head(5)` and another would return rows. Reading every row also defeated streaming. | A violating pair is a failure of its later row, checked when that row is read; `count()` and full reads check every pair. No execution skips rows because of the declaration, so a violation can make a result raise but never makes it wrong. Recorded as a trade-off. | [§9.5], [§20.2], O4, B6 |
| AR4 | S1 | `distinct()` without keys did not say which of several equal rows survives, though `-0.0`/`0.0` and NaN payloads differ in bits, nor where the survivor appears. | No keys means all columns, with the same `keep`. `keep="first"` and `"last"` are positional with or without keys; `keep="any"` is legal on any table. Recorded as a trade-off. | [§7.6], B5 |
| AR5 | S1 | Aggregates over a group whose values are all NULL were unspecified; `0` and NULL were both plausible. | [§14.2]'s fourth column ("No non-NULL value taking part") applies when no row takes part or every value is NULL. `count()` counts rows; `var` and `std` are NULL with one value. | [§14.2], A2, A3 |
| AR6 | S1 | [§19.3] forbade `.dt` on durations while [§19.4] and [§22] defined `total_seconds` and `days` for them. | `.dt` is allowed on durations for those two methods only. | [§19.3], H2 |
| AR7 | S2 | Float aggregation order was unspecified, so evaluator and batching parity (P5, P6) could not be tested with equality for floats, and P10 demanded exact float arithmetic that [§17.1] rules out. | Float sums are exact sums rounded once; `mean`, `var`, `std`, `cov` and `corr` use defined formulas over them, so the bits do not depend on order or batching. P10 keeps exactness for integer and decimal results and states the float rule separately (keyed on the result type after M's F23). Recorded as a trade-off. | [§14.2], P10 |
| AR8 | S2 | An integer with `float32` gave `float32`, silently rounding integers above 2**24. | An integer with any float gives `float64`; `float32` with `float32` stays `float32`. NumPy and pandas already give `float64`. | [§17.3], [§17.4] |
| AR9 | S2 | Decimal result types: `mean` of `decimal128(38, 18)` lost integer digits and raised on in-range input, as DataFusion 55's `DecimalAverager` does; mean rounding was unspecified; `decimal256` sums were typed as `decimal128`; decimal `/` and `%` were defined as "DataFusion's" type. | Explicit formulas for `sum`, `mean`, `/`, `//` and `%`, with P = 38 or 76 and the input's width kept, truncation toward zero, and `ArithmeticOverflowError` when a result does not fit its type. A DataFusion upgrade cannot change them silently. | [§14.2], [§17.3], A2, N3 |
| AR10 | S2 | Text forms disagreed across `cast`, `write_csv` and `read_csv`, so the CSV round trip the contract required could not hold for valid data. | One canonical text form per type, written by `cast` to string and by `write_csv`, and parsed with the listed variants by `cast` from string and by `read_csv`. A read with the declared schema returns what was written. | [§4.2], [§16.5], [§17.6], P16, D9 |
| AR11 | S2 | A path containing glob characters was read as a glob even when a file of that name existed, so `report[2024].csv` read `report2.csv`. | An existing path is read as itself; only other paths containing `*`, `?` or `[` are globs. | [§4.1], L1 |
| AR12 | S2 | Principle 9 named three nondeterminism exceptions where the contract had five, and the tests had no equality rule for `keep="any"`. | Principle 9 lists all five. [§24.1] defines result equality for `keep="any"`. | [§1] (principle 9), [§24.1] |
| AR13 | S3 | `agg()` with no names returned the distinct keys on `GroupBy` but raised on `LTSeq` and `NestedTable`, adding a second spelling of an existing operation. | It raises `LTSeqValueError` on all three. Distinct keys are `select(*keys).distinct(keep="any")`. | [§14.1], A1 |
| AR14 | S3 | The type or error of `fill_null`, `if_else` and `when` depended on the column's integer width: `int32.fill_null(1.5)` was `float64`, while `int64.fill_null(1.5)` raised. | A literal never widens the type of the non-literal values; both calls raise `CastError`. This revises ADR 0018 D-b/D-i. Recorded as a trade-off. | [§17.5], [§1.4], N5 |
| AR15 | S3 | `dt.add` and `date ± timedelta` accepted only literals, so a per-row offset had no direct form. | `dt.add` takes integer expressions. `date ± duration` takes any duration and raises `CastError` for one that is not whole days. | [§19.2], [§19.4], H1 |
| AR16 | S3 | `.str.contains` was literal where pandas and Polars default to a regular expression, and the namespace mixed Python spellings with Polars-style ones. | Python's names (`startswith`, `endswith`, `rjust`, `ljust`), and two explicit substring methods, `contains_literal` and `contains_regex`. Recorded as a trade-off. | [§8.6], [§8.7], D11 |
| AR17 | S3 | `uint64` values above the `int64` maximum could not be literals or declared `from_dict` values. | An `int` in [2**63, 2**64) is a `uint64` literal; a declared type accepts every value it holds. | [§4.6], [§17.2], N2 |
| AR18 | S3 | `collect` was listed both as a terminal and, by [§5.3]'s completeness rule, as plan-building, and Z3 required it to succeed on a corrupt source. | `collect` is an eager call in [§5.3]'s table. | [§5.1], [§5.3], Z2, Z3 |
| AR19 | S4 | Methods taking `**named` could not create a column named `self`. | `self` is positional-only in `select`, `derive` and every `agg`. | [§7.2], [§7.3], [§11.2], [§14.1], [§14.3], [§22], S2 |
| AR20 | S4 | The documented replacement for `right(n)`, `slice(-n)`, is wrong for `n = 0`. | `slice(-n)` for `n > 0`, and `""` for `n = 0`. | [§8.6] |
| AR21 | S4 | A position in `delete` and `update` accepted `bool` and refused NumPy integers. | `int` and NumPy integers are positions; `bool` raises `LTSeqTypeError`. | [§7.10] |
| AR22 | S4 | Four smaller gaps: which key survives a `full` join when the two sides differ in bits; no tolerance kind for `duration` as-of keys; [§17.4] rule 3 naming join keys that [§12.1] refuses; `/` by zero missing from the list of departures from Python. | The left key when non-NULL, else the right; a `timedelta` tolerance for date, timestamp and duration keys; rule 3 limited to the key classes [§12.1] allows; `/` by zero listed. | [§12.1], [§12.3], [§17.3], [§17.4] |

### Checklist coverage

Every checklist item produced at least one finding.

| Checklist item | Findings |
|---|---|
| 1. One concept, several APIs | AR10, AR13 |
| 2. Name disagrees with behavior | AR1, AR11, AR16, AR20 |
| 3. Unnatural Python calling habits | AR8, AR16, AR19, AR21 |
| 4. Counterintuitive defaults | AR5, AR11, AR14, AR16 |
| 5. Implicit order changes | AR2, AR4 |
| 6. Silent precision loss | AR1, AR7, AR8, AR9, AR14 |
| 7. NULL and Boolean conflicts | AR4, AR5, AR17 |
| 8. Plan-time and execution-time disagreement | AR3, AR10 |
| 9. Expressions that do not compose | AR2, AR15 |
| 10. Unclear return types | AR18 |
| 11. Not understandable from type hints | AR6, AR14, AR21 |
| 12. Contract depending on DataFusion accidents | AR3, AR7, AR9 |
| 13. Simple-looking designs that make common tasks verbose | AR15, AR17 |
| 14. LTSeq semantics given up for Polars or pandas consistency | AR2, AR8 |
| 15. Unneeded abstractions, aliases and options | AR13 |

### Checked and found sound

The reviewer also confirmed, by recomputation or by running the libraries:

- every [§23] example, including the returns and share values of 23.2 and 23.3, the 23.4 matches, and the 23.18 New York instants one hour apart;
- N3's `(2**54 + 3) / 3`, which is `6004799503160662.0` rounded once and `…663.0` after converting first, and D8's `round(2.675, 2) == 2.67`;
- [§16.2] and [§23.18](../../contract/examples-semantics.md#2318-arrow-and-pandas-round-trip): NaN survives `dtype_backend="pyarrow"` and becomes NA with `numpy_nullable` (pandas 3.0.5);
- [§4.6](../../contract/loading-and-laziness.md#46-ltseqfrom_dict-and-ltseqfrom_rows): pyarrow refuses `[1, "a"]`, `[True, 1]` and `[2**53 + 1, 0.5]`, and infers `[1, 2.5]` as `float64`;
- [§16.3](../../contract/streaming-and-output.md#163-to_dicts): aware values from `to_pylist` carry `zoneinfo.ZoneInfo`;
- the floored `//` and `%` of [§17.3], Kleene logic against the [§20.2] guard rules, [§9.4]'s sort order against [§18]'s equality, the `asof_join` direction and tie rules, the `intersect`/`difference` occurrence rules, [§9.1]'s definition of unspecified ties, and [§21.2] against [§20.2];
- the [§22] signatures against the prose, apart from AR6.

## I. Final consistency check

After the adversarial review (H) and the inventory (B), the whole contract was re-read once more for rules that contradict each other, and the review was read against it. Four mechanical checks ran on the final text. Every problem found was fixed in the contract or the review as committed; the table below records them on H's severity scale so a reviewer can judge how much the text moved after H.

### Mechanical checks

| Check | Result |
|---|---|
| Every `§n.m` reference in both documents names a heading of the contract, and every test or property ID names a [§24] row | No dangling reference |
| Every function and method called in [§23] is defined in [§22] | Only names from elsewhere remain: the examples' own helpers `key` and `total`, pyarrow `equals`, pandas `ArrowDtype` and `tolist`, Polars `from_dataframe`, `ProcessPoolExecutor.map`, and `union` inside a `# was union()` comment |
| Every subsection of [§4]–[§21] is cited by a [§24] row or property | All 84 except [§11.3], which is a worked example |
| Every documented and exported name at baseline appears in the inventory | All 176 names in `docs/api.md` headings; all 50 names in `ltseq.__all__`; all 68 public `LTSeq` attributes; all 77 definitions in the `.pyi` stubs; every public attribute of `NestedTable` (8), `GroupBy` (1), `Expr` (13), `StringAccessor` (28) and `TemporalAccessor` (11) |

A word search found no "TBD", "TODO", "open question", "deferred" or "future version" in the contract, no "deprecat" or "legacy" in it, "backward" only as the `asof_join` direction, and "compat" only in "type-compatible" and "incompatible". "Alias" appears only for the `join(alias=)` keyword, the signature type aliases of [§2.3], pyarrow's type-name aliases ([§6.3]), and statements that aliases are removed.

### Problems found and fixed

S1: two conforming implementations can return different results, or a documented idiom returns a wrong one. S2: a contradiction that fails loudly, such as a documented idiom or test that raises. S3: a wrong or missing statement that does not change a result. S4: wording.

**Found by re-reading the contract**

| ID | Sev. | Finding | Fix | Contract |
|---|---|---|---|---|
| I1 | S1 | `requested_schema` converted with the rules of `cast`, which AR1 made round and truncate, so `float64` 2.5 requested as `int64` gave 2 where every other supplied value raises. | The conversion is implicit and never rounds or truncates; 2.5 raises `CastError` from the stream. | [§16.4], [§17.6], X3 |
| I2 | S1 | "Implicit conversions never round" covered integer and decimal operands converted to `float64` in arithmetic, `coalesce` and `var`, which [§17.3]–[§17.4] require. `r.i64 + r.f64` at `2**53 + 1` could round or raise. | Supplied values (literals, declared values, `requested_schema`) never round. The one implicit conversion that rounds is an integer or decimal operand meeting a float: nearest, half to even. | [§17.4], [§17.6] |
| I3 | S1 | Three statements of what `count()` demands: [§7.9] excluded sort keys, [§20.2] and [§9.5] included them and the `assume_sorted` pairs. | `count()` demands what decides which rows exist and in what order, and checks every `assume_sorted` pair. Audit and impact map M16 reworded to match. | [§7.9], [§20.2] |
| I4 | S1 | `search_first` MAY stop at the match ([§10.5]), but [§20.2] forbids reading past it, so a failing value after the match raised in one implementation and not another. | The predicate is demanded up to the first match only. W7 tests that a failure after the match does not raise. | [§10.5], W7 |
| I5 | S1 | The float `sum` was defined "as `math.fsum`", which raises on `[1e308, 1e308]` and `[inf, -inf]` where [§17.1] says floats never raise. | NaN and infinities are defined case by case, overflow gives `±inf`, and the `math.fsum` comparison is limited to finite inputs whose sum is in range. | [§14.2], P10 |
| I6 | S2 | [§1.4] kept ADR 0018 D-b/D-i (`int32.fill_null(0.0)` is `float64`) while [§17.5], after AR14, says a literal never widens a column's type. | [§1.4] records D-b/D-i as revised. | [§1.4] |
| I7 | S2 | [§6.1] let nullability be conservative, but [§24.1] compared schemas exactly, so two conforming implementations failed each other's tests. | A non-nullable field never holds NULL; beyond that nullability is not contract. Result equality checks nullability only that far. | [§6.1], [§24.1], T1 |
| I8 | S2 | The contract's own typed-NULL idiom, `lit(None).cast("int64")`, raised: the `null` type was pass-through and [§17.6] had no row for it. | `null` → any type gives a NULL of that type; `null` accepts `cast`, `try_cast` and the NULL functions. | [§6.2], [§17.6] |
| I9 | S2 | N2 listed `2**63` as an invalid literal after AR17 made it `uint64`. | The error cell reads `2**64` and `-2**63 - 1`. | N2 |
| I10 | S2 | P12 allowed only `CastError` and `LTSeqTypeError`, but [§17.4] rule 3 raises `LTSeqValueError` for a NaN literal next to an integer. | P12 names all three. | P12 |
| I11 | S2 | D9 said `cast` and `try_cast` truncate alike at ±2**63; `2.0**63` is out of range, so one raises and the other is NULL, and nothing near 2**63 has a fraction to truncate. | `-2.0**63` converts exactly, `2.0**63` fails, and truncation parity is tested on `±2.7`. | D9 |
| I12 | S3 | [§9.3] tied positional `distinct` to `keys`; AR4 made `keep` decide it. | Positional when `keep` is `"first"` or `"last"`, with or without keys. | [§9.3] |
| I13 | S3 | A [§23] comment called `collect` a terminal after AR18 made it an eager call. | Comment corrected. | [§23.14] |
| I14 | S3 | Integer `%` had "the common integer type", which `int64` with `uint64` lacks. | The [§17.4] common type, `decimal128(20, 0)` for that pair. | [§17.3] |
| I15 | S3 | Three subsections had no [§24] row: [§9.7], [§14.4] and [§19.1]. | P7, G3 and H1 cite and test them. | P7, G3, H1 |
| I16 | S3 | The review gave decimal `//` a fixed `decimal128(38, 0)`, though `decimal256` input keeps its width. | `decimal(P, 0)`. | [Deliverable D] (#241) |
| I17 | S4 | The trade-off record called P5 and P6 "evaluator parity"; P6 is batching parity. | Relabeled. | Trade-off record |

**Found while building the inventory.** [Deliverable B], "Gaps found", gives the evidence for each.

| ID | Sev. | Finding | Contract |
|---|---|---|---|
| I18 | S1 | The `union` replacement deduplicated; v0.4 `union` is `UNION ALL`. | [§13.2], [§23.16] |
| I19 | S1 | `except_`/`subtract` pointed to `difference`, which deduplicates and matches NULL; their behavior is `anti_join(other, on=t.columns)`. | [§13.2] |
| I20 | S1 | `pos` and `split_part` mapped to `find` and `split` without the 1-based offset. | [§8.6] |
| I21 | S2 | The `contain` replacement returned a table where v0.4 returns a `bool`. | [§13.2] |
| I22 | S2 | Lambda keys and removed keywords (`desc=`, `shift(default=)`, …) were removed only by the shape of [§22] signatures, so nothing said what they raise. | [§22] preamble; test S2 |
| I23 | S3 | `partition_by=` was removed from three window methods, not all six. | [§10.1] |
| I24 | S3 | `dt.diff`'s `month` and `year` units count calendar fields; the given replacement measures elapsed time. | [§19.4] |
| I25 | S3 | `_repr_html_`, which executes the plan in notebooks, was not mentioned. | [§3.2]; test S3 |
| I26 | S4 | Four renamed string methods were described as compositions. | [§8.6] |
| I27 | S4 | `seq` was removed without naming `LTSeq.range`. | [§8.4] |
| I28 | S4 | The `g.sum("x")` group helpers and `when(c).then(v)` were removed implicitly. | [§8.1], [§8.3] |

That is 8 S1, 8 S2, 8 S3 and 4 S4. The S1 findings in the second table were wrong migration idioms, not wrong v0.5 semantics. All five in the first table follow from H's fixes: each fix was stated where its finding arose, and another section still said otherwise. AR1 made `cast` round, which `requested_schema` inherited (I1), and the sentence keeping implicit conversions exact named [§17.4], where integers meet floats (I2). AR3 fixed demand in [§9.5] and [§20.2] while [§7.9] and [§10.5] still said otherwise (I3, I4). AR7 defined the float sum by `math.fsum` (I5).

### Acceptance gate

| Gate item (brief §8) | Status | Evidence |
|---|---|---|
| 1. `docs/api.md` fully reviewed | Met | Every heading of `docs/api.md` and every public name at baseline is a row of [Deliverable B] (398 rows; see Mechanical checks) |
| 2. All eleven issues decided | Met | [Deliverable D](../decisions.md#d-decisions-on-the-eleven-open-semantic-issues): one final decision for each of #202, #148, #156, #218, #221, #222, #228, #241, #246, #247 and #248, set against the issue's previous recommendation, with rationale, observable behavior, consequences and acceptance tests |
| 3. No compatibility baggage | Met | No deprecation, alias or legacy wording (word search above). [§24.1] requires every removed name to raise `AttributeError` or `ImportError`, and test row S1 checks it |
| 4. No unnecessary duplicate APIs | Met | [Deliverable B] merges 35 and removes 107 rows. Three near-duplicates are deliberate: `if_else` is `when(c, a).otherwise(b)` and is kept as the most common conditional ([§8.3]); `len(t)` is Python's size protocol and equals `t.count()` ([§3.2]); `median()` is `quantile(0.5)` ([§14.2]) and keeps its name because pandas ([`Series.median`](https://pandas.pydata.org/docs/reference/api/pandas.Series.median.html)), Polars ([`Expr.median`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.median.html)) and DuckDB ([`median`](https://duckdb.org/docs/current/sql/functions/aggregates.html)) all spell it that way |
| 5. Pythonic naming | Met | Principle 5 ([§1.3]); `.str` with Python's `str` method names ([§8.6]); `descending=`, `reverse`, `difference`, `startswith` ([Deliverable B], 14 renames) |
| 6. Ordered-sequence capability kept | Met | Table-order windows, `.over`, `search_first`, `search_pattern` and `fold` ([§10]); `group_ordered` ([§11]); `asof_join` ([§12.3]). Examples [§23.1]–[§23.5] and [§23.9] use them |
| 7. Lazy, streaming and materialization boundaries clear | Met | Plan-building versus execution ([§5.1]); the complete eager-call table ([§5.3]), tested by Z3 against a corrupt source; streaming ([§15]); which operations must stream, which should hold only what their definition needs, which may hold the whole input, and which memory the streaming bound covers ([§21.1], after L's F19 and O's F31) |
| 8. Ordering, numeric, coercion, temporal and NULL rules self-consistent | Met, within the limit below | Order ([§9]), numbers and coercion ([§17]), NULL, NaN and Boolean logic ([§18]), time ([§19]), demand and stages ([§20.2]). H checked them one by one; this check read them together and fixed I1–I5, I8 and I14; K fixed F13 and made `&` and `\|` symmetric (D10); L fixed F18 and F21; M fixed F22–F25 and F27; N fixed F28–F30; P fixed B1–B5 and P2–P6, and [§1.6] now names the one section that owns each rule restated across the contract; Q applied the owner's modifications of D8, D12, D14 and AC17 |
| 9. Same expression, same behavior on every path | Met | [§21.2] requires every specialized evaluator to match the general path in values, types, order and `sort_keys`, and to raise only errors the general path may raise (after J, V6a: one failing value's class and stage, from a set). [§24.1] runs every test with each evaluator forced on and off; P5 (evaluators) and P6 (batching) |
| 10. Accurate signatures and return types | Met | [§22] lists every public signature with parameter kinds, defaults and return type. The [§22] preamble makes annotations binding (I22). [§24.1] compares `inspect.signature` of every public callable with [§22]. H confirmed [§22] against the prose. L widened the annotations so that every documented call form type-checks, which S2 runs through pyright (F20). P fixed B6: [§2.3] and [§22], extracted to stub files, pass pyright 1.1.411 with every documented call form, and refuse exactly the five call forms S2 lists as errors |
| 11. Testable acceptance conditions | Met | [§24](../../contract/acceptance.md#24-acceptance-criteria-and-contract-test-matrix): 95 guarantee rows, each with a positive, an error and a boundary case and the class and stage of every error; 18 properties over generated inputs ([§24.4]); a map from the brief's fifteen dimensions to rows and properties ([§24.5]). Every [§4]–[§21] subsection is cited |
| 12. Examples use only the final API | Met | The 18 examples of [§23] call only [§22] names (Mechanical checks) |
| 13. No contradictory rules | Met, within the limit below | 22 problems fixed in H and 28 in this check; the API review gate found 12 more, resolved in J, the second review four more, resolved in K, and the third review five more, resolved in L; the fourth review five more, and its notes a sixth, resolved in M; the fifth review three more, resolved in N; the sixth review one more, resolved in O with the other [§21.1] bounds wrong in the same way; the seventh review two more, which the architectural reassessment confirmed among its six blockers and eight corrections, all resolved in P; Q applied the owner's four modifications |
| 14. Report matches the contract | Met | The cross-reference check passes on the review. Counts quoted in the review match the contract: 24 sections (C), 15 exception classes (G), 95 guarantee rows and 18 properties (F). The inventory corrected the contract where the two disagreed (I18–I28) |
| 15. No undecided naming or semantics | Met | No undecided wording (word search above). The eleven uses of "unspecified" all belong to three of the nondeterminism exceptions that principle 9 ([§1.3]) decides (five at this check, six after J added the choice among several errors, nine at O): unspecified ties ([§9.1]), with every value computed from a row's position among them; the row `distinct(keep="any")` keeps; and, as for that row, which equal row represents a value in `intersect` and `difference` ([§13.2]), which principle 9 counts under `distinct`. Each MAY grants a stated latitude: thread sharing, re-reading a changed source, conservative nullability, the order of a run of ties and the values computed from it ([§9.1], added in P), the row sequence of an unordered table, smaller batches, memory use, specialized evaluators, the test-only evaluator switch, and the clock between executions; J added speculative evaluation whose undemanded errors are suppressed ([§20.2]) |

**Owner decisions.** Items 8, 13 and 15 were marked pending until the owner confirmed the options that J, K, L and P applied for D1–D14 and D10a. The owner decided them on 2026-10-09: the applied options stand except D8, D12 and D14, which the owner modified, as well as AC17. Q records how the contract applies the four modifications and what was checked against them.

**Limit.** A re-read and these checks cannot prove that no contradiction is left; this pass and the inventory found 28 problems after H. The S1 contradictions show where the next one is most likely: a rule that several sections restate. The main ones are demand ([§7.9], [§8.2], [§9.5], [§10.5], [§20.2]), implicit conversion ([§4.6], [§16.4], [§17.4]–[§17.6], P12) and the `distinct` tiers ([§7.6], [§9.3]). F13, found after this check, was one: P12 restated the implicit-conversion rule more strictly than [§17.4]. F21 was another: [§17.4] and [§19.2] restated ADR 0018 D-l without the duration rows' exception. Two other kinds of conflict need more than a re-read. F17 contradicted Python's own `datetime` equality, which only a probe shows. F18 was a property that holds for every plan that does not fail and was never checked against one that does. The fourth review found two more restated-rule conflicts: F22 ([§9.2]'s table and [§11.2] against [§7.3]'s truncation) and F23 ([§17.1] and P10 promised exactness by operand type, while [§17.3] gives a decimal `**` a float result). F24 compared outputs over two value domains, Arrow's and Python's. F25 and F27 took a definition from a library whose limits only a probe shows: a float method for an exact instant, and Parquet's type coverage for the writer and `to_pandas`. F28–F30 were of the same kind, a domain that only a probe of the library shows: the range of a unit conversion, the types Parquet's physical schema cannot hold, and NumPy's units. F31 was a list that put operations under one memory bound without checking each against its definition, and it missed that rows leave in input order: while a row waits for a later row of its partition, every output row after it waits too. Principle 10 allows whole-table materialization only where [§21.1] lists it, so the operations the lists left out were denied memory their definitions need; O found four. The architectural reassessment found two kinds more. B1 was an exception stated narrowly: principle 9 left tie order open, and the rule did not reach the values computed from a tie's position, such as `row_number`, `first` or a cumulative sum over the run. B4 was a closed table whose rows were enumerated by hand: the `cast` table lacked pairs that other sections convert, so it is replaced by a rule over kinds ([§17.6]) that answers every pair. [§1.6] now names the section that owns each restated rule, so that a later edit changes the owner and checks the restatements against it. A conflict found during implementation is a contract bug, to be fixed in the contract rather than settled in code.

<!-- v0.5-modular:footer -->

---

Previous: [Examples, test matrix and implementation impact map (E, F, G)](../impact-map.md) · [Index](../../README.md) · Next: [API review gate and contract closure pass (J, K)](review-gate.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§1]: ../../contract/overview.md#1-overview
[§1.3]: ../../contract/overview.md#13-design-principles
[§1.4]: ../../contract/overview.md#14-relation-to-earlier-decisions
[§1.6]: ../../contract/overview.md#16-rule-ownership
[§2.3]: ../../contract/public-surface.md#23-type-aliases-used-in-signatures
[§3.2]: ../../contract/public-surface.md#32-ltseq-invariants
[§4]: ../../contract/loading-and-laziness.md#4-loading
[§4.1]: ../../contract/loading-and-laziness.md#41-source-paths
[§4.2]: ../../contract/loading-and-laziness.md#42-ltseqread_csv
[§4.6]: ../../contract/loading-and-laziness.md#46-ltseqfrom_dict-and-ltseqfrom_rows
[§5.1]: ../../contract/loading-and-laziness.md#51-plan-building-and-execution
[§5.3]: ../../contract/loading-and-laziness.md#53-eager-calls
[§6.1]: ../../contract/schema-and-table-operations.md#61-schema-and-columns
[§6.2]: ../../contract/schema-and-table-operations.md#62-supported-types
[§6.3]: ../../contract/schema-and-table-operations.md#63-data-type-arguments-dtypelike
[§7.2]: ../../contract/schema-and-table-operations.md#72-select
[§7.3]: ../../contract/schema-and-table-operations.md#73-derive
[§7.6]: ../../contract/schema-and-table-operations.md#76-distinct
[§7.9]: ../../contract/schema-and-table-operations.md#79-count-and-show
[§7.10]: ../../contract/schema-and-table-operations.md#710-value-level-edits-insert-delete-update
[§8]: ../../contract/expressions.md#8-expression-dsl
[§8.1]: ../../contract/expressions.md#81-contexts-and-proxies
[§8.2]: ../../contract/expressions.md#82-operators
[§8.3]: ../../contract/expressions.md#83-conditional-and-null-functions
[§8.4]: ../../contract/expressions.md#84-math-functions
[§8.5]: ../../contract/expressions.md#85-general-expr-methods
[§8.6]: ../../contract/expressions.md#86-string-methods-str
[§8.7]: ../../contract/expressions.md#87-the-closed-method-set
[§9]: ../../contract/ordering.md#9-ordering-contract
[§9.1]: ../../contract/ordering.md#91-order-state
[§9.2]: ../../contract/ordering.md#92-sources-and-propagation
[§9.3]: ../../contract/ordering.md#93-order-requirements
[§9.4]: ../../contract/ordering.md#94-sort
[§9.5]: ../../contract/ordering.md#95-assume_sorted
[§9.7]: ../../contract/ordering.md#97-sort_keys-and-is_ordered
[§10]: ../../contract/windows-and-grouping.md#10-windows-and-ordered-computation
[§10.1]: ../../contract/windows-and-grouping.md#101-window-methods
[§10.5]: ../../contract/windows-and-grouping.md#105-search_first
[§10.6]: ../../contract/windows-and-grouping.md#106-search_pattern
[§11]: ../../contract/windows-and-grouping.md#11-ordered-grouping
[§11.2]: ../../contract/windows-and-grouping.md#112-nestedtable
[§11.3]: ../../contract/windows-and-grouping.md#113-example
[§12.1]: ../../contract/joins-and-sets.md#121-join
[§12.3]: ../../contract/joins-and-sets.md#123-asof_join
[§13.2]: ../../contract/joins-and-sets.md#132-intersect-and-difference
[§14.1]: ../../contract/aggregation.md#141-group_by-and-groupbyagg
[§14.2]: ../../contract/aggregation.md#142-aggregate-expressions
[§14.3]: ../../contract/aggregation.md#143-ltseqagg
[§14.4]: ../../contract/aggregation.md#144-nestedtable-aggregation
[§15]: ../../contract/streaming-and-output.md#15-streaming
[§16.2]: ../../contract/streaming-and-output.md#162-to_pandas
[§16.4]: ../../contract/streaming-and-output.md#164-arrow-pycapsule-stream
[§16.5]: ../../contract/streaming-and-output.md#165-writers
[§17]: ../../contract/numeric-null-temporal.md#17-numeric-semantics-and-literals
[§17.1]: ../../contract/numeric-null-temporal.md#171-integer-and-decimal-results-are-checked
[§17.2]: ../../contract/numeric-null-temporal.md#172-literals
[§17.3]: ../../contract/numeric-null-temporal.md#173-arithmetic-operators
[§17.4]: ../../contract/numeric-null-temporal.md#174-types-of-mixed-operands
[§17.5]: ../../contract/numeric-null-temporal.md#175-shared-values
[§17.6]: ../../contract/numeric-null-temporal.md#176-explicit-casts
[§18]: ../../contract/numeric-null-temporal.md#18-null-nan-and-boolean-logic
[§19]: ../../contract/numeric-null-temporal.md#19-temporal-semantics
[§19.1]: ../../contract/numeric-null-temporal.md#191-types
[§19.2]: ../../contract/numeric-null-temporal.md#192-arithmetic-and-comparison
[§19.3]: ../../contract/numeric-null-temporal.md#193-dt-fields
[§19.4]: ../../contract/numeric-null-temporal.md#194-dt-methods
[§20.2]: ../../contract/errors-and-performance.md#202-stages
[§21]: ../../contract/errors-and-performance.md#21-performance-contract
[§21.1]: ../../contract/errors-and-performance.md#211-materialization
[§21.2]: ../../contract/errors-and-performance.md#212-fast-paths
[§22]: ../../contract/api-reference.md#22-complete-canonical-api-reference
[§23]: ../../contract/examples-sequences.md#23-end-to-end-examples
[§23.1]: ../../contract/examples-sequences.md#231-sessions-with-group_ordered
[§23.5]: ../../contract/examples-sequences.md#235-as-of-join
[§23.9]: ../../contract/examples-sequences.md#239-sequential-state-with-fold
[§23.14]: ../../contract/examples-semantics.md#2314-demand-and-error-stages
[§23.16]: ../../contract/examples-semantics.md#2316-set-and-bag-operations
[§24]: ../../contract/acceptance.md#24-acceptance-criteria-and-contract-test-matrix
[§24.1]: ../../contract/acceptance.md#241-acceptance-criteria
[§24.4]: ../../contract/acceptance.md#244-generators
[§24.5]: ../../contract/acceptance.md#245-coverage-of-the-required-dimensions
[Deliverable B]: ../inventory.md#b-api-inventory-and-review
[Deliverable D]: ../decisions.md#d-decisions-on-the-eleven-open-semantic-issues
[Deliverable H]: #h-adversarial-review
[Deliverable I]: #i-final-consistency-check
[Deliverable Q]: owner-decision-closure.md#q-owner-decision-closure

<!-- /v0.5-modular:links -->
