<!-- v0.5-modular:header -->

# v0.5 review history: Assessment closure pass (P)

[Index](../../README.md) › Review history · Previous: [Third to sixth review closures (L–O)](review-closures.md) · Next: [Owner decision closure (Q)](owner-decision-closure.md)

**Non-normative, historical.** A dated record of the [review](../../README.md#ltseq-v05-api-review), kept as written. Later records name the earlier records they change, and [Deliverable Q] is the current decision register.

**Scope.** The closure pass after the architectural reassessment: blockers B1–B6 and corrections P1–P8, the rule ownership map the contract carries in [§1.6], every implicit conversion checked against the rules, what remains and when it must be settled, and the owner decisions then still to record.

**Most cited from here.** [§17] Numeric semantics and literals · [§16] Output and interchange · [§4] Loading · [§14] Aggregation, partitioning and pivot

<!-- /v0.5-modular:header -->

## P. Assessment closure pass

The [seventh independent API review](https://github.com/Lychee-Technology/ltseq/pull/250#issuecomment-6070580210) read the contract at `1a00b40` and requested changes for two Medium findings. The Parquet writer converted `time32[s]` to milliseconds "as `cast` does", through a rule the closed `cast` table lacked, and a `timedelta` literal could exceed the `duration[us]` the contract gave it. An [architectural reassessment](https://github.com/Lychee-Technology/ltseq/pull/250#issuecomment-6071844247) of the PR ([second part](https://github.com/Lychee-Technology/ltseq/pull/250#issuecomment-6071844544)) then read the contract against the code, DataFusion's sources and pyarrow. It counted as a blocker any text that mandates a wrong or lossy result, contradicts itself, or makes its own acceptance criterion fail on a correct implementation. It confirmed both findings, generalized the first into blocker B4, downgraded the second to a correction (P2), and found five more blockers and seven more corrections required before merge. This section records how this revision closes all fourteen, how what remains is classified and tracked, and which owner decisions the merge waits for. It was written after the revision.

### Findings and resolutions

| ID | Source and severity | Finding | Root cause | Resolution | Contract |
|---|---|---|---|---|---|
| B1 | Reassessment, blocker | Principle 9 left the order within a run of unspecified ties open, but not the values computed from that order. `group_by("k").agg(n=...).sort("n").derive(i=row_number())` with two groups tied on `n` has two conforming results, `{(a, 1), (b, 2)}` and `{(a, 2), (b, 1)}`. They are different multisets, so [§24.1]'s result equality and P2 failed a correct implementation. The same holds for `cum_sum`, `shift`, `tail`, `slice`, `step`, `first` and `last`, `search_first`, `fold`, `search_pattern` and `asof_join`'s last match in right order | The exception named order but not what is derived from it, the kind of gap F18 was | Principle 9 and [§9.1] add every value computed from a row's position within a run, which MAY differ between executions. [§24.1]'s result equality is existential: the reference's result over some order of each run. P2 excludes a `g` or `f` that reads a position within a run. `intersect` and `difference` over an undefined left order keep an unspecified one of several equal rows, as `distinct(keep="any")` does, and [§24.1] and P2 accept any of them | Principle 9, [§9.1], [§13.2], [§24.1], P2 |
| B2 | Reassessment, blocker | Decimal `%` had precision `min(min(p1 − s1, p2 − s2) + s, P)`. A floored remainder takes the divisor's sign, so `decimal(4, 2)` −10.50 `%` `decimal(5, 2)` 360.00 is 349.50, which `decimal(4, 2)` cannot hold, and a defined operation raised `ArithmeticOverflowError` | DataFusion's type rule for a truncated remainder was kept when [§17.3] made `%` floored | Precision `min(p2 − s2 + s, P)`: the remainder is smaller in magnitude than the divisor, so the divisor's integer digits bound it below the clamp at `P`; above it the remainder is checked ([§17.1]). N3 has the case | [§17.3], N3 |
| B3 | Reassessment, blocker | `from_dict` and `from_rows` inferred types "with the semantics of `pyarrow.array`". On pyarrow 25.0.1 that drops the time from `[date, datetime]`, the zone from a naive with an aware `datetime`, and the nanoseconds of a `pd.Timestamp`, and reads the `1` of `[timedelta(1), 1]` as one microsecond | A boundary delegated to a library the contract neither controls nor pins | LTSeq's own inference: each value has its [§17.2] literal type, integers follow `read_csv`'s rule ([§4.2]), `Decimal`s take the smallest `decimal128` that holds each exactly, and every other mix raises `LTSeqTypeError` at the call | [§4.6], L6, L7 |
| B4 | Seventh review (Medium), generalized; blocker | "A pair with no rule raises `LTSeqTypeError`", and the `cast` table had no rows for time units, `float32` to `float64`, the string, binary and list layouts, or identity. The writer, `requested_schema`, join keys, `try_cast` and rows D9, N6, X4 and P16 rely on them | A closed table enumerated by hand where a rule was needed; F28's fix cited a row the table lacked | [§17.6] decides every pair in three steps. A type to itself is the identity. Within a kind, every pair converts exactly or fails per value, except where a table row rounds. Between kinds, only the table's rows apply. The kinds are integers and decimals, floats, four temporal kinds, text, bytes, and, in implicit conversions only, nested types; a dictionary converts as its value type. `float16` and `date64` as a `dtype` raise. The review's proposed time row is one instance of the second step; on its own it would have left `float32` to `float64` raising | [§17.6], D9 |
| B5 | Reassessment, blocker | [§14.2] said only rows where `where` is TRUE take part, and [§20.2] said an aggregate demands its inputs on every row. They disagreed on `(g.a // g.b).sum(where=g.b != 0)` over `b = [0, 2]`. DataFusion's `FILTER (WHERE ...)` evaluates the argument on every row first, so the natural lowering raised; the baseline's `*_if` lowering, `SUM(CASE WHEN ...)`, did not | `where=` replaced the `*_if` family without restating demand | An aggregate's arguments are demanded only on rows where its `where=` is TRUE, and `where=` itself on every row of the group; `pivot`, defined through `where=`, inherits this. E3 has the case | [§14.2], [§20.2], E3 |
| B6 | Reassessment, blocker | [§22] failed its own S2 check. Extracted as stubs, it gave 62 pyright errors from missing imports, `__hash__: None` was an override error, and reflected operators were comments, so `10 // r.x` ([§20.2], [§23.14]) did not type-check | The stubs were checked against the prose but never run through the checker S2 names | Imports added, `__hash__` declared as `ClassVar[None]`, reflected operators written as signatures. [§2.3] and [§22], extracted to stub files, pass pyright 1.1.411 on every documented call form and refuse exactly the five call forms S2 lists as errors | [§2.3], [§22] |
| P1 | Reassessment | [§1.4] omitted ADRs 0004, 0006 and 0014, called 0008 "Extended" though the contract reverses its trust in `assume_sorted`, and called 0013 "Kept" though `rolling` gains the `min_periods` that ADR records it as lacking. The owner approves these reversals partly through this table | The table predates the revisions that reversed those ADRs | Rows for 0004, 0006 and 0014; 0008 and 0013 "Revised". Acceptance criterion 5 adds `docs/api.cn.md` and `LINKING_GUIDE` | [§1.4], [§24.1] |
| P2 | Seventh review (Medium), downgraded to Low | `timedelta` to `duration[us]` had no range rule, and `timedelta.max` is 86,399,999,999,999,999,999 µs. In the other direction, `to_dicts` cannot return a `duration[s]` of 10**15 seconds as a `timedelta` | The range check was written for NumPy values only | A `timedelta` beyond `int64` microseconds raises `LTSeqValueError` at plan time. A duration outside `timedelta.min` to `timedelta.max` raises `CastError` in the conversions to Python of [§16.3]. The review's observation on fixed offsets is taken as well: an offset that is not a whole number of minutes, an aware `time` and a custom `tzinfo` raise `LTSeqValueError`. Low, because only durations beyond about 292,000 years reach it | [§16.3], [§17.2], N2, X2 |
| P3 | Reassessment | [§17.3] said the decimal formulas were written out, but those for `+`, `-` and `*` were not, and `decimal(38, 38) * decimal(38, 38)` has a scale above 38 | — | `+` and `-` give `decimal(min(max(p1 − s1, p2 − s2) + s + 1, P), s)` with `s` the larger scale; `*` gives `decimal(min(p1 + p2 + 1, P), s1 + s2)`, and `LTSeqTypeError` at plan time when `s1 + s2` exceeds `P`. N1 has the cases | [§17.3], N1 |
| P4 | Reassessment | `decimal32` and `decimal64` widened "where they meet another type", while [§17.3] gave every decimal result as `decimal128`, so two `decimal32(9, 2)` values 9999999.99 and 0.01 had two readings | — | Narrow decimals widen to `decimal128` before any binary arithmetic or aggregate; their sum is `decimal128(10, 2)` 10000000.00 (N4) | [§17.4] rule 2, N4 |
| P5 | Reassessment | Float `//` was "floor(a / b) with CPython's algorithm", which names two algorithms: CPython gives `1.0 // 0.1 == 9.0`, and `floor(1.0 / 0.1)` is 10 | — | CPython's `fmod`-based algorithm, written out, with the example in N3 | [§17.3], N3 |
| P6 | Reassessment | [§4.1] listed files at the call while [§5.1] read sources "as they are at that moment", so whether a file added later is read was unstated. At the baseline a deleted file counts 0 rows (#262) | — | The file set is fixed at the call. Each execution re-reads every listed file, its Parquet footer included; a missing file raises `SourceNotFoundError` and a changed schema `SchemaMismatchError` | [§4.1], [§5.1] |
| P7 | Reassessment | The trade-off record had no rows for hazards X1–X4 and rated no rule for order (X5). X4 changes what D10 costs | — | Rewrite rows X1–X4, the order paragraph, and D10a in the trade-off table | Trade-off record |
| P8 | Reassessment | The PR body was stale ("/ // % follow Python", "eleven decisions"), and the seventh review was unanswered | — | The body lists what the merge waits for; this section is posted as the reply to the seventh review | — |

The review's two observations are covered by P2: [§16.3] names the `timedelta` boundary for durations, and X2 tests a `duration[s]` of 10**15 seconds. N2 tests the sub-minute offset and the aware `time`.

### Rule ownership (§1.6)

Most conflicts that H through O fixed were rules restated in several sections, where one copy drifted from the others (I's Limit paragraph). [§1.6] now names, for each of ten concepts, the section that owns the rule, the sections that apply it, the [§24] rows that test it and the issues behind it. A restatement that differs from its owner is corrected to it. Building the map meant reading every application against its owner. That found twelve drifts, all fixed in this revision:

- `update` with an expression the column cannot hold exactly raised in [§7.10], while [§17.5] let an integer expression round into a float column. [§17.5] now requires a non-literal value placed into a fixed target to have a type the target holds exactly.
- Window and `search_pattern` partitions and `n_unique` stated NULL and NaN equality but not `-0.0 == 0.0`, and [§18] named none of them. [§18] now names them.
- `LTSeq.range` gave its order as "value ascending" in [§9.2], where [§4.7] gives `SortKey(name, step < 0, True)`, descending for a negative step. [§9.2] now cites [§4.7]'s key.
- Three places said `count()` reads every row, while `head(n)` and `search_first` stop reading. [§7.9] and [§20.2] now say `count()` reads what decides which rows exist.
- [§15.1] required a stream to raise the class `to_arrow` raises, where [§20.2] and P4 allow any error of the set. It now says "from the set".
- [§4.4] listed only `float16` and `date64` as the conversions `from_arrow` copies, omitting views and dictionaries. It now defers to [§6.2].
- [§14.5] restated [§16.3]'s failure list without the aware wall-clock rule. It now cites [§16.3].
- `pivot` named a column `str(v)` without saying which conversion makes the string. It now converts `v` as `to_dicts` does, raising that conversion's `CastError` at the call, and a value a later execution meets that the call did not find raises `LTSeqValueError`. [§20.2] says discovery demands only the `columns` column.
- Principle 10 forbids whole-table materialization where [§21.1] does not list it, and [§21.1] did not mention `update`, `delete` or `insert`. `update` and `delete` with a predicate MUST stream; `insert` and positional `update` and `delete` MAY hold their input.
- [§21.1] said the eager calls of [§5.3] MAY hold their whole input, which included the readers it also said MUST stream. It now says the work an eager call does at the call MAY hold its input.
- [§18] gave a `partition` key the quiet NaN as its representative, while [§14.5] raises on a NaN key. [§18] now says `partition` refuses NaN.
- A cast from `date32` to an aware timestamp failed only on a nonexistent midnight and was silent on an ambiguous one, as in Havana. It now fails on both, as [§19.4] does for every local time.

### Every implicit conversion against the rules

B4 came from a conversion that cited a rule the contract did not have. To check there is no other, every place the contract converts a value without an explicit `cast` was listed and checked against the rule it names: the result types of [§17.3] and [§17.4] (G1 in the reassessment), the pairs of [§17.6] (G2), and the literal map of [§17.2] with [§17.5] (G3).

| Conversion | Section | Rule | Rounds? |
|---|---|---|---|
| Arithmetic and comparison operands, math functions | [§8.4], [§17.1], [§17.3], [§17.4] | G1; an integer or decimal next to a float converts to the nearest float | Only that conversion to a float |
| Literals: alone, among shared values, into a fixed target | [§17.2], [§17.5] | G3: exact, or `LTSeqValueError` or `CastError` at plan time | No |
| `update`, `insert`, `shift(fill_value=)` expressions | [§7.10], [§17.5] | A type the target holds exactly, else `LTSeqTypeError` | No |
| `read_csv` inference, and a declared type | [§4.2] | [§4.2]'s integer rule; with a declared type, text parses as `cast` from `string` does (G2, step 3) | Float text only, to the nearest value ([§16.5]) |
| `from_dict`, `from_rows` and `fold` states: inferred and declared | [§4.6], [§10.7] | G3 for each value, then [§4.6]'s inference; a declared type exact or `CastError` (G2); superseded: only within a kind (AC17, Q) | No |
| `from_arrow`, `from_pandas`, `read_parquet`: views, dictionaries, extension types, `float16`, `date64` | [§4.3]–[§4.5], [§6.2] | G2 within a kind; a `date64` beyond `date32` raises `CastError` (one that is not a whole day: #277, N13) | No |
| Aggregates | [§14.2] | No conversion: a float statistic is the exact value rounded once | Once, at the result |
| Rows passed to `fold`, `partition` keys, `pivot` names, `to_dicts`, iteration | [§10.7], [§14.5], [§14.6], [§16.3] | [§16.3]'s conversion to Python, `CastError` where it cannot hold a value | No |
| `to_pandas` | [§16.2] | The types `pandas.read_feather` gives for the same Arrow data; with `numpy_nullable`, [§16.3] for values held as Python objects | No |
| `requested_schema` | [§16.4] | G2, nested types included; the table's nullability, no metadata; superseded: only within a kind (AC17, Q) | No |
| Writers: CSV text, Parquet `timestamp[s]` and `time32[s]` | [§16.5] | `cast` to `string`; to milliseconds by G2's unit change | No |
| `date32` to an aware timestamp | [§17.6], [§19.4] | G2 table row; a nonexistent or ambiguous local midnight fails | No |

No conversion names a rule the contract lacks, and none rounds except where [§17.6]'s opening paragraph says it does.

### What remains, and when it must be settled

The reassessment compared three dispositions (keep repairing this PR, replace it, or merge with follow-ups) and chose the third after one bounded closure pass: merge once the blockers are fixed and the owner has recorded the decisions, and track the rest as issues. This revision is that pass, so in the table below only the owner's decisions block the merge.

| Class | Items | Where tracked |
|---|---|---|
| Before merge | B1–B6, P1–P8, the final verification's three blockers and five corrections, the twelve ownership drifts, X7 (`requested_schema` nullability and metadata, [§16.4]), N12 (sub-minute offsets, an aware `time`, a custom `tzinfo`), the 39-digit `Decimal` of N14, and the date-to-aware half of N7 | Fixed in this revision |
| Before merge, owner | Decided: D1–D14, D10a, U1, U3 and AC17, with D8, D12, D14 and AC17 modified. Open: the final merge review's confirmation of the writers' refusal of tables without columns, of `pct_change` on IEEE and of the AC17 readings | Q |
| Before a named M item | Numeric gaps: N9, G5 and OD-W (owner, deferred: Q), the rest of N14, N15, N17, N18, AC14, AC15, and N5's kernel deviation (arrow-rs scales decimal `/` and `%` in `i128`) | #276, before M4, M6, M7, M8, M10 and M13 |
| | Temporal gaps: the rest of N7 (owner, deferred: Q), N10, N13, N16, the rest of N19 | #277, before M7, M18, M20 and M23 |
| | API and interchange gaps: AC3 and OD-E (owner, deferred: Q), AC6, AC7, AC9–AC13, AC16, X8, run-end encoding and list views, importer exception classes, N20 | #278, before M1, M2, M12, M15, M17, M18, M22 and M26–M29 |
| | X1–X5: rating every rewrite for errors and order, and pinning each Restricted rule with a plan-guard test | #273, before M16 and M1 |
| | X6: `close()` on a reader built from batches does not stop execution (OD-X6) | #256, before M1 |
| | X9: DataFusion behavior to replace, each in the item that changes it: wrapping `SUM`, `AVG`, negation and decimal `abs` (M5); no exact float accumulators (M13, M4); decimal rescaling half away from zero (M7); `SortPreservingMerge` unstable on ties (M1, M19); `AggregateStatistics` and `AND`/`OR` pre-selection (rewrite table, M3, M16) | The M rows of #257 |
| | M0 measurements, with a new one for X4 before M16 | #251 |
| | A partition-count override, layout fixtures and the R3 memory harness | #274, before the tests of M1, M3, M13, M16, M17 and M19 |
| | Migrating the literal grid and differential baselines | #275, with M5–M8, M15 and M20 |
| | M items with no issue before: M3, M14, M15, M18, M19, M22 (two parts), M27 and M29 | #264–#272 |
| | Grouped `derive` and decimal `pct_change` | #257's M2 and M4 rows, and #202 |
| Before release | AC8 (minimum Python, pyarrow, NumPy and pandas versions), U2 (owner, deferred: Q) | #278 |
| | M28: `docs/api.md`, `docs/api.cn.md`, the ADRs [§1.4] lists, `LINKING_GUIDE`, a migration table, release notes for D5, D8, D12, D14, AC17 and the removed names | #257 (M28) |
| Baseline (v0.4) | Wrong results found during the reassessment: `unwrap_cast` (#260), glob and missing paths (#261), stale listings and deleted files (#262), `asof_join` with a NULL right time (#263); the order-loss evidence on #148, grouped `max` of `[-inf, -inf]` on #253, `concat` of `int64` and `string` on #222. Each is fixed by its M item at the latest; OD-S1 and OD-S2 asked whether to stop #258, #223 and #259 in v0.4 first, and the owner's direction is to do so (Q) | The issues named |
| Optional | When to start the compiler epic (OD-C); restructuring the contract into one rule per concept, of which [§1.6] is the step taken | #229; none |

No row, property, example or section was added; [§1.6] is a subsection of [§1], and [§17.3] is retitled "Arithmetic operators". The counts in C, F and I still hold at 24 sections, 95 rows, 18 properties and 18 examples. Rows S2, L6, L7, B8, D9, O4, W6, A4, A5, R3, X2, X3, N1–N4 and E3, and properties P2, P7, P14 and P15, gained the cases these fixes need.

### Earlier records this revision changes

- **Principle 9 in A.2** names the values computed from tie positions and the `intersect` and `difference` representative.
- **#218 in D** names CPython's float `//` algorithm and the decimal `%` precision.
- **The trade-off record** adds D10a and rewrite rows X1–X4, rates nothing for order yet but lists what M1 must rate (X5), and narrows "Not assessed".
- **G** links the issues filed for this pass. M2, M10, M11, M14 and M27 depend on M15, whose classes their error cells assert. M22 also depends on M2 and M16, because pickle carries order state (P2) and `collect` and pickle demand every value. M10 names the `asof_join` rewrite, and M28 adds ADR 0004.
- **I's gate.** Items 8, 10, 13 and 15, the Pending decisions paragraph and the Limit paragraph count P's findings and D10a.

### Agreement across the contract

| Rule | Places |
|---|---|
| Ties and what is computed from them (B1) | Principle 9, [§9.1], [§12.1], [§13.2], [§24.1], P2; gate item 15 |
| Decimal result types (B2, P3, P4) | [§14.2], [§17.3], [§17.4] rule 2, N1, N3, N4; #218 in D |
| Constructor inference (B3) | [§4.2], [§4.6], [§17.2], [§17.6], L6, L7; the trade-off row on reading integer text |
| Which pairs convert (B4) | [§6.2], [§16.4], [§16.5], [§17.6], D9, X4, P16 |
| Demand under `where=` (B5) | [§14.2], [§14.6], [§20.2], E3 |
| Duration ranges (P2) | [§14.5], [§16.3], [§17.2], N2, X2 |
| Which files an execution reads (P6) | [§1.6], [§4.1], [§5.1], L1, Z1 |

### Owner decisions

The merge waits for the owner to record a choice on each decision below, in a review or a comment on this PR: accept the applied option or name another. The reassessment's recommendation is given for each; none is recorded as approved. The owner has since decided each, and Q is the current register.

| ID | Decision | Applied in the text | Recommendation | Blocks the merge? |
|---|---|---|---|---|
| D1–D14 | Deliverables J, K and L | The options those deliverables apply | Accept each | Yes |
| D10a | Whether a sort, grouping, `distinct` or aggregate demands its keys and arguments on rows a later filter removes (X4) | (a): demanded on every row of the operator's input, and on every row of a join's right input, so `PushDownFilter` past those operators is Restricted ([§20.2], trade-off record) | (a). Moving from (a) to (b) later only turns errors into results; (b) needs errors carried as values through sort and aggregate, or parity breaks | Yes, with D10 |
| U1 | Round 4 widened D2's exactness to `median` and `quantile` without a D number | Exact interpolation, rounded once ([§14.2]) | Accept | Yes |
| U3 | #247: the applied rule (a literal never widens a column's type, [§17.5] rule 1) is neither of the issue's two options | That rule | Ratify, and close #247 against M8 | Yes |
| OD-W | How exact window statistics must be | Exact, rounded once, as for grouped statistics; pending M0 measurement 2 (#251) | Decide on the measurement | No, while marked pending |
| U2 | A SHOULD bound for `tail(n)`, and `distinct` in the MUST-stream list | Neither | Decline both now; revisit with the R3 harness (#274) | No (#278) |
| AC3, N9, G5, N7, OD-E | `len(t)`; `float32` literals; weak integer literals; DST options; which evaluators ship | Current text | Raise; raise; keep; keep the fixed rule; general path only | No (#278, #276, #277) |
| AC17 | Whether a declared constructor type and `requested_schema` convert between kinds (strings parsed, numbers to text, `bool` ↔ number) | Yes, by [§17.6] step 3; superseded: no, by the owner's modification (Q) | None yet: restricting them to steps 1 and 2 matches [§17.4] rule 4, and keeping them saves a `cast` | No (#278, before M18 and M22) |
| OD-F, OD-X6, OD-C | Fold three exception classes; `close()` stops execution; when to start #229 | Fifteen classes; MUST | Keep fifteen; keep the MUST and gate #256 on it; start after M1, M16 and M24 | No (#266, #256, #229) |
| OD-S1, OD-S2 | v0.4 stopgaps for #258 and #223; fixing #259 in v0.4 | — | Raise on the affected keys and operators; fix #259 now | No (#258, #223, #259) |

### Final verification

One independent verification read this revision against the reassessment's blocker definition before it was committed. It found three defects of that class, each the B1 or B3 fix applied in one place and not another, and five corrections. All eight are fixed here.

- **P2 with a tie-reading `g`.** P2 excluded an `f` that reads a position within a run of unspecified ties, but not a `g`: `g = sort("n").derive(i=row_number())` over tied `n` has two conforming results, so `f(g(t)) == g(f(t))` could fail on a correct implementation. P2 now excludes both, and rows chosen by `intersect` or `difference` over an undefined left order.
- **[§24.1] had no representative clause for `intersect` and `difference`.** [§13.2] lets either keep any one of several left rows equal under [§18], but result equality named only `distinct(keep="any")`. With `-0.0` and `0.0` both on the left, U3 could fail a correct implementation that kept the other one. [§24.1] now accepts any such row.
- **`[2**64, 0.5]` had two error classes.** [§17.2] gives `2**64` no literal type, so [§4.6]'s first sentence raised `LTSeqValueError`, while its last sentence and row L7 raised `LTSeqTypeError`. An `int` that no integer type holds now raises `LTSeqValueError` whatever its neighbours, and L7 lists it there.
- **Corrections.** [§17.6] step 2 said every pair within a kind has a rule and then excluded naive ↔ aware, so the exclusion now opens the step. [§16.3]'s duration bound was ±999,999,999 days, but `timedelta.max` is 999,999,999 days 23:59:59.999999 and `timedelta.min` is −999,999,999 days (checked with pyarrow 25.0.1), so [§16.3] now names both. Float `//` said "rounded to the nearest integer", which leaves a tie at .5 open; it is now CPython's `floor(q)`, plus 1 when the fraction exceeds 0.5. Decimal `%` claimed the divisor's digits always bound the remainder, but at the clamp to `P` (`decimal(20, 10) % decimal(30, 0)` is `decimal(38, 10)`, and `-1 % 10**29` does not fit) it is checked as [§17.1] says; N3 has the case. [§1.6]'s label for [§17.6] said implicit conversions never round, against [§17.6]'s own list of those that do.

One more finding predates this revision and is the owner's call, so it is filed as AC17 in #278 rather than decided here. [§17.6] step 3 lets the implicit conversions of a declared `from_dict` or `from_rows` type and of `requested_schema` use the between-kind rows, so a declared `date32` parses `"2024-01-01"`, a declared `string` takes `1` as `"1"`, and `bool` converts to and from numbers. That sits uneasily with [§17.4] rule 4 ("There is no implicit parsing of strings") and with [§1.4]'s revision of ADR 0018 D-h and D-k ("no implicit string readings").

### Open

- The owner's choice on each decision in the first five rows of the owner table.
- [§17.6]'s rule over kinds and the existential result equality of [§24.1], which one verification has read and no implementation has tested.
- #256 and #251, which still gate M1 and the items M0 lists.
- The rewrite-table rows whose evidence is "Source", which no probe has exercised (#273).
- Follow-ups #276 (numeric), #277 (temporal) and #278 (API and interchange), each row to be settled before the M item it names.

<!-- v0.5-modular:footer -->

---

Previous: [Third to sixth review closures (L–O)](review-closures.md) · [Index](../../README.md) · Next: [Owner decision closure (Q)](owner-decision-closure.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§1]: ../../contract/overview.md#1-overview
[§1.4]: ../../contract/overview.md#14-relation-to-earlier-decisions
[§1.6]: ../../contract/overview.md#16-rule-ownership
[§2.3]: ../../contract/public-surface.md#23-type-aliases-used-in-signatures
[§4]: ../../contract/loading-and-laziness.md#4-loading
[§4.1]: ../../contract/loading-and-laziness.md#41-source-paths
[§4.2]: ../../contract/loading-and-laziness.md#42-ltseqread_csv
[§4.3]: ../../contract/loading-and-laziness.md#43-ltseqread_parquet
[§4.4]: ../../contract/loading-and-laziness.md#44-ltseqfrom_arrow
[§4.5]: ../../contract/loading-and-laziness.md#45-ltseqfrom_pandas
[§4.6]: ../../contract/loading-and-laziness.md#46-ltseqfrom_dict-and-ltseqfrom_rows
[§4.7]: ../../contract/loading-and-laziness.md#47-ltseqrange
[§5.1]: ../../contract/loading-and-laziness.md#51-plan-building-and-execution
[§5.3]: ../../contract/loading-and-laziness.md#53-eager-calls
[§6.2]: ../../contract/schema-and-table-operations.md#62-supported-types
[§7.9]: ../../contract/schema-and-table-operations.md#79-count-and-show
[§7.10]: ../../contract/schema-and-table-operations.md#710-value-level-edits-insert-delete-update
[§8.4]: ../../contract/expressions.md#84-math-functions
[§9.1]: ../../contract/ordering.md#91-order-state
[§9.2]: ../../contract/ordering.md#92-sources-and-propagation
[§10.7]: ../../contract/windows-and-grouping.md#107-fold
[§12.1]: ../../contract/joins-and-sets.md#121-join
[§13.2]: ../../contract/joins-and-sets.md#132-intersect-and-difference
[§14]: ../../contract/aggregation.md#14-aggregation-partitioning-and-pivot
[§14.2]: ../../contract/aggregation.md#142-aggregate-expressions
[§14.5]: ../../contract/aggregation.md#145-partition
[§14.6]: ../../contract/aggregation.md#146-pivot
[§15.1]: ../../contract/streaming-and-output.md#151-to_batches
[§16]: ../../contract/streaming-and-output.md#16-output-and-interchange
[§16.2]: ../../contract/streaming-and-output.md#162-to_pandas
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
[§18]: ../../contract/numeric-null-temporal.md#18-null-nan-and-boolean-logic
[§19.4]: ../../contract/numeric-null-temporal.md#194-dt-methods
[§20.2]: ../../contract/errors-and-performance.md#202-stages
[§21.1]: ../../contract/errors-and-performance.md#211-materialization
[§22]: ../../contract/api-reference.md#22-complete-canonical-api-reference
[§23.14]: ../../contract/examples-semantics.md#2314-demand-and-error-stages
[§24]: ../../contract/acceptance.md#24-acceptance-criteria-and-contract-test-matrix
[§24.1]: ../../contract/acceptance.md#241-acceptance-criteria
[Deliverable Q]: owner-decision-closure.md#q-owner-decision-closure

<!-- /v0.5-modular:links -->
