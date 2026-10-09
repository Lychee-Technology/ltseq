<!-- v0.5-modular:header -->

# v0.5 review: Decisions on the eleven open semantic issues

[Index](../README.md) › Review · Previous: [API inventory and review (B)](inventory.md) · Next: [Trade-off record](trade-offs.md)

**Non-normative.** Part of the [review](../README.md#ltseq-v05-api-review) behind the contract. It records evidence, reasoning and decisions; the behavior itself is specified by the contract modules.

**Scope.** The final v0.5 decision on each of the eleven open semantic issues (#202, #148, #156, #218, #221, #222, #228, #241, #246, #247, #248): a summary table, then one block per issue.

**Most cited from here.** [§17] Numeric semantics and literals · [§9] Ordering contract · [§16] Output and interchange · [§19] Temporal semantics

<!-- /v0.5-modular:header -->

## D. Decisions on the eleven open semantic issues

Each issue's earlier recommendation was treated as a hypothesis and decided again against v0.5's priorities. Correctness ranks first: no silent wrong results. Compatibility does not count. The verdict says what happened to that recommendation: **Accepted** (kept as stated), **Revised** (kept in substance, changed or extended), **Rejected** (replaced). Every decision is final, and each one is already written into the contract at the sections cited.

The summary table has the first four columns of the required matrix. The per-issue blocks below it carry the other five, because nine columns of this length do not fit one Markdown table.

| Issue | Previous recommendation | Final v0.5 decision | Verdict |
|---|---|---|---|
| [#202](https://github.com/Lychee-Technology/ltseq/issues/202) Window output order | `.over(order_by=)` and `partition_by` order the window computation only, not the table | Windows never reorder rows, for every window kind; `sort_keys` can never claim an order the rows lack | Accepted |
| [#148](https://github.com/Lychee-Technology/ltseq/issues/148) Cursor and write-path order | End-to-end guarantee for a declared order; no promise of source-file order for undeclared multi-file scans | Every reader defines file-then-row order; every terminal and writer delivers the defined order; restored by an order-preserving merge, never by materializing | Revised |
| [#156](https://github.com/Lychee-Technology/ltseq/issues/156) Pickle and naming | Pickle as a materialized snapshot keeping schema and sort metadata; 0-row Parquet; old keywords through a deprecation cycle | Pickle as an Arrow IPC snapshot with order state, same minor version only; 0-row files written; one keyword per concept and no aliases | Revised |
| [#218](https://github.com/Lychee-Technology/ltseq/issues/218) `/`, `//`, `%` | Python semantics: true division, floor division, floored modulo | Python semantics for integers, floats and decimals; IEEE where an operand is a float; integer `/` correctly rounded, and raising for a zero divisor as integer `//` and `%` do (D8 as the owner modified it) | Accepted |
| [#221](https://github.com/Lychee-Technology/ltseq/issues/221) Overflow | Keep wrapping for now and align the evaluators first | Checked integer and decimal arithmetic everywhere, aggregates and windows included; no option to wrap | Rejected |
| [#222](https://github.com/Lychee-Technology/ltseq/issues/222) `concat` schema | Strict schema, no lossy promotion, empty inputs included | Strict: same names, order and types; nullability may differ; empty inputs included; no relaxed variant; `union` removed | Accepted |
| [#228](https://github.com/Lychee-Technology/ltseq/issues/228) Float with decimal columns | `float64` in comparisons, arithmetic and shared values | `float64` in arithmetic and shared values; comparisons compare exact values without converting | Revised |
| [#241](https://github.com/Lychee-Technology/ltseq/issues/241) `decimal32`/`decimal64` with integers | Widen to `decimal128` at the coercion boundary, not at ingestion | Same, in every position and on every execution path | Accepted |
| [#246](https://github.com/Lychee-Technology/ltseq/issues/246) Cross-zone arithmetic | Option A: an aware literal is its instant in the receiving column's zone; DataFusion keeps the result unit | Every aware operation works on instants, for literals and columns, at any unit pair; naive with aware raises | Revised |
| [#247](https://github.com/Lychee-Technology/ltseq/issues/247) Timestamp widening | Option B: widen to the coarsest unit that holds the literal exactly | No widening: a literal must be exact at the column's unit, else `CastError` at plan time | Rejected |
| [#248](https://github.com/Lychee-Technology/ltseq/issues/248) All-literal shared values | Keep DataFusion's unification and folding | All-literal values must be exact in their common type, else `CastError` at plan time; each node is judged alone | Rejected |

v0.5 removes two of the questions outright. The API that #148 was filed against, `Cursor`, `scan` and `scan_parquet`, no longer exists, because readers are lazy and `to_batches()` is the streaming scan ([§15.1]). The alternatives #247 compared, choosing a widening unit, are gone because v0.5 never widens.

### #202: Window output order

- **Rationale.** The defect the issue's comment shows is a state that cannot be allowed: rows in `(g, id)` order while `sort_keys` says `id`, after which `head` and `search_first` return wrong rows without an error. The issue's other option was to let the output take the window's order and drop `sort_keys`. That makes a table's row order depend on which window ran last, and the comment shows exactly that dependence. A window computes a value per row, and a row's position is not part of that value. Keeping the rows in place is therefore the only rule under which `derive` composes. It also matches the two libraries users will compare with. In pandas, `df.groupby("g")["v"].shift(1)` keeps the frame's index `[0, 1, 2, 3]` (checked with pandas 3.0.5). Polars' default `over` mapping returns results "back to their row position in the DataFrame" ([`Expr.over`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.over.html)).
- **Observable API behavior.**
  - `t.sort("i").derive(o=lambda r: row_number().over(order_by="x"))` returns rows in `i` order with `sort_keys == (SortKey("i", False, True),)`.
  - On the comment's table, `a = t.derive(p=lambda r: r.v.shift(1).over(partition_by="g"))` gives `id == [0, 1, 2, 3, 4, 5]`. `a.head(3)` has ids `[0, 1, 2]`, and `a.search_first(lambda r: r.v > 11)` has id `2`.
  - `.over(order_by=)` works on a table with a defined order and no `sort_keys`. Ties in `order_by` are broken by table order, and are unspecified only when the table's order is undefined ([§9.1], [§10.1]).
  - A window without `order_by` uses table order and needs `sort_keys` (sequence tier, [§9.3]).
  - Sort keys are column names. A computed order is `derive`d first, so `sort_keys` never stands for an expression it cannot name. This also removes the truncated-prefix problem the #148 comment describes in `src/ops/sort.rs`.
  - `sort` is stable.
  - The invariant that `t.is_sorted_by` holds for `t.sort_keys` (each key's column, `descending` and `nulls_last` passed through) holds after windows, optimizer rewrites, `collect`, pickle and every terminal (P7).
- **Consequences.** Where the window's partitioning or order differs from table order, the implementation restores table order afterwards, holding no more than [§21.1] allows the window itself, which with `.over(order_by=...)` is the whole input (after O's F31); the cost is that of a sort-preserving merge or a sort on a carried position, or of a window plan that never loses the order; the contract leaves the mechanism to the implementation. Windows whose order matches table order cost nothing extra. The decision unblocks #236, which must keep this invariant in compiled pipelines. Results that were wrong today (`head`, `search_first`, `tail` after a mismatched window) become correct. Values do not change, and no precision is involved.
- **Required normative documentation.** Contract [§9.1]–[§9.3] and [§10.1] ("Window methods never reorder rows"), [§10.4], [§21.1]. ADR 0008/0009 are extended ([§1.4]). The "Unified `.over()` entry" of `docs/api.md` is replaced.
- **Acceptance tests.** P7 and P8. W1 and W6 with unsorted input. The repros in the body and the comment, including `head(3)` and `search_first`, on a physical plan forced to at least two partitions. Tie-breaking by table order in `order_by`. The window-order-then-table-order chain in both orders. Parity of the linear-scan and pattern evaluators (P5).

### #148: Cursor and write-path order

- **Rationale.** The hypothesis promised source order only when the user had declared an order. v0.5 promises it for every reader:
  - The file order is already deterministic: lexicographic within a directory or glob, sequence order across a list ([§4.1]). Restoring it is the same order-preserving merge that a declared order needs, so the extra promise costs nothing more.
  - Without it, `read_parquet(dir).tail(5)` or `with_row_index()` would be refused or meaningless for the most common source in a sequence library.
  - A promised order that the parallel scan breaks is exactly the silent wrong result the issue reports.

  Declared keys are still never inferred from files ([§4.3]): Parquet `sorting_columns` are not trusted, because `assume_sorted` is the one way to declare keys, and it validates them.
- **Observable API behavior.**
  - `read_csv` and `read_parquet` give a defined order: files in [§4.1] order, then row groups and rows in file order. `sort_keys` is `None` ([§4], [§9.2]).
  - `to_batches`, iteration, `to_arrow`, `to_dicts`, `to_pandas`, the PyCapsule stream, `write_csv`, `write_parquet` and pickle all deliver the same defined order, at any number of execution partitions ([§15], [§16]).
  - `assume_sorted` declares keys and is checked on every execution, including adjacent pairs across batch, file and partition boundaries. A violation raises `OrderViolationError` during execution ([§9.5]).
  - Positional operations (`tail`, `slice`, `step`, `reverse`, `with_row_index`, …) need only a defined order, so they work directly on a reader's result. Sequence operations need keys: `with_row_index()` declares `(index ascending)`, and `assume_sorted` declares the file's key ([§9.3]).
  - `write_csv` streams ([§21.1]). Both writers are atomic and write 0-row files ([§16.5]).
  - `Cursor`, `scan` and `scan_parquet` are removed.
- **Consequences.** A parallel scan can no longer emit partitions in arrival order: the implementation merges them in source order (`SortPreservingMergeExec` on a file and row position, or an order-preserving repartition). This costs some throughput at the final merge, and memory stays bounded. `assume_sorted` validation adds one comparison per row. A source whose order is wrong now fails loudly instead of corrupting `asof_join`, `group_ordered` or windows downstream. No values or types change.
- **Required normative documentation.** Contract [§4.1]–[§4.3], [§9.1]–[§9.5], [§15.1], [§16.5], [§21.1] (the last bullet names #148). `docs/api.md` IO sections are replaced. ADR 0005's eager-boundary entries for `assume_sorted` and the cursor are superseded ([§1.4]).
- **Acceptance tests.** These follow the issue's own review comments:
  - Multi-file input with a monotone `id` and a physical plan confirmed to have at least two file groups. `to_batches()` and a `write_parquet` round trip each give `id == range(n)`, asserted directly rather than compared with `to_arrow()`.
  - R3 (bounded memory, several partitions). L1, L4, O4 (a violation only across a file boundary). X4 (`sorting_columns` recorded and ignored on read). P4.

### #156: Pickle and API naming

- **Rationale.**
  - **Pickle.** Process pools pass arguments by pickling, and `partition()` is meant to feed them ([§23.8]). Refusing pickle would leave users to write their own Arrow round trip, which loses `sort_keys`. Pickling the lazy plan instead would re-read sources in another process, possibly on another machine, at another time. So pickle is an executed snapshot, as the issue's comments propose, and it stores the order state with the rows.
  - **Version binding.** Pickle is for moving tables between processes, not for storage, so payloads are bound to the same minor version and no format is frozen. Parquet is the storage format.
  - **Naming.** The deprecation cycle is dropped because v0.5 carries no compatibility constraint, and an alias doubles what has to be documented and tested.
- **Observable API behavior.**
  - `pickle.loads(pickle.dumps(t))` equals `t.collect()`: same rows, order state, `sort_keys` and exact schema (nullability, decimal, temporal and zone types). It works across processes. Another minor version raises `LTSeqValueError`. `NestedTable`, `GroupBy` and `Expr` raise `TypeError` ([§16.6], [§3.3]).
  - A 0-row table writes a valid Parquet file with its schema, and a header-only CSV ([§16.5]).
  - One keyword per concept:
    - `descending` and `nulls_last` everywhere (`desc=` removed);
    - `how=` on `join`;
    - `alias=` replaces `link(as_=…)`;
    - column names are positional `str` arguments, and expressions are lambdas ([§22]).
  - `rvs` becomes `reverse`. `xunion` and `contain` are removed, with replacements given in [§13.2]. `ifa` is not part of v0.5 ([§8.4]).
  - `search_first` always returns an `LTSeq` ([§10.5]).
  - `Expr` is unhashable, and the stub says why ([§22.1]).
  - An expression lambda of the wrong arity raises `LTSeqTypeError` at plan time ([§3.4]).
  - The compiled module is `ltseq._ltseq_core`, private and never re-exported ([§2.2]).
  - Empty tables: a 0-row table has a schema and round-trips through every writer and pickle. A table without columns is a valid relation whose row count does not depend on its columns: `select()` and a `drop` of every column keep the input's rows, an Arrow input without fields, a pandas frame without columns and `from_rows` keep theirs, and `count()`, `to_arrow`, `to_dicts` and pickle carry them. An input that gives no row count, `from_dict({})` or a 0-byte CSV without a `pa.Schema`, raises `LTSeqValueError`, and `write_csv` and `write_parquet` refuse a table without columns, because CSV cannot write its rows and the Parquet writers record it as 0 rows ([§6.1], [§16.5]; D12 as the owner modified it, Q; T1, B2, B4, L3, L5–L7, X4).
- **Consequences.** `pickle.dumps` executes the plan, holds the result in memory and demands every value, so it raises for a failing value that a later operation on the lazy plan would drop ([§5.2], [§16.6]). Both are stated, since they are the snapshot contract. Every renamed or removed name breaks existing code with an `AttributeError` or `TypeError`, not a silent change. A payload written by v0.5.x cannot be read by v0.6. No values change.
- **Required normative documentation.** Contract [§2], [§3.2]–[§3.4], [§16.5], [§16.6], [§22] (canonical signatures). [Deliverable B] lists every rename and removal.
- **Acceptance tests.** X5 covers a spawned process, an undefined-order table, an empty table, a source deleted after pickling, and a version mismatch. P2 covers staged parity through pickle where the snapshot succeeds, X5 the snapshot's own error where it does not, and P16 round trips. The schema matrix covers nullable, decimal, zone-aware and nanosecond columns over multiple batches. S1, S2 and D11 snapshot the exported names and signatures, so a reintroduced alias fails a test. S5 covers lambda arity.

### #218: `/`, `//` and `%`

- **Rationale.** The DSL reads as Python, so an operator that silently means something else is a wrong result. The three operators must satisfy `(a // b) * b + a % b == a`, which a floored `//` with a truncating `%` breaks for opposite signs.
  - **Decimals** follow the same floored rule, although Python's `decimal.Decimal` truncates (`Decimal(-7) // 2 == -3`). One operator keeps one meaning across numeric types, and a column's values do not change behavior when it is cast from integer to decimal.
  - **Float zero divisors** follow IEEE (`inf`/NaN) rather than raising `ZeroDivisionError`. A per-row exception in a vectorized float column would make a whole query fail on one value that has a representable answer, and float `/` already gives `inf`.
  - **Integer and decimal zero divisors** raise. `//`, `%` and decimal `/` have no representation for the result. Integer `/` has one, since its result is `float64`, but its operands are exact numbers: an `inf` or NaN would be a value no operand implies, passing silently through later sums and comparisons, so it raises as Python's `int / int` does. That is the owner's decision (D8, Q); until then this record gave integer `/` by zero the IEEE result.
  - **Integer `/`** is the exact quotient rounded once, as Python's `int / int`. Converting operands above `2**53` to `float64` first rounds twice: `(2**54 + 3) / 3` is `6004799503160662.0` in Python and `6004799503160663.0` by converting first (checked with Python 3.14).
- **Observable API behavior.** On the issue's table, `x / y` is `float64` `[3.5, -3.5, -3.5, 3.5]`, and `x % y` is `[1, 1, -1, -1]`. `-7.0 % 2.0 == 1.0`. `-7 // 2 == -4`.
  - Integer `/`, `//` and `%` by zero (`1 / 0` and `0 / 0` included), decimal `/`, `//` and `%` by zero, and `i64::MIN // -1` (overflow) raise during execution. Float `x / 0.0` is `±inf` or NaN, as is `1.0 / 0`, and `x // 0.0` follows IEEE division. Float `//` is CPython's `fmod`-based algorithm, not `floor(a / b)`, so `1.0 // 0.1 == 9.0`.
  - Integer `/` gives `float64`; with two `float32` operands it gives `float32`. Decimal `//` gives `decimal(P, 0)`: `decimal128(38, 0)`, or `decimal256(76, 0)` when an operand is `decimal256`.
  - NULL in either operand gives NULL and never raises ([§17.3]).
  - The same results come from every evaluator ([§21.2]).
- **Consequences.** Every integer `/` changes type from `int64` to `float64`, and every negative `%` changes value. Both are breaking and intended. Integer `/` needs a kernel that rounds the exact quotient when an operand exceeds `2**53`. Below that, plain float division is already correctly rounded. Decimal `%` changes sign for negative operands, and its precision follows the divisor's integer digits ([§17.3]), because a floored remainder can be as large as the divisor where DataFusion's type bounds only a truncated one. `pattern_match.rs`, `linear_scan.rs` and DataFusion share one kernel per operator, as `src/transpiler/floor_div.rs` already does for `//`.
- **Required normative documentation.** Contract [§8.2], [§17.3] (including the paragraph on deviations from Python). The operator table of `docs/api.md`.
- **Acceptance tests.** P11, which compares with `Fraction` for integer and decimal operands and with CPython for floats. N3, including the double-rounding case and zero divisors per type. `test_dsl_execution_alignment.py` references are replaced by Python references. The identity is checked for all sign combinations, and P5 checks evaluator parity.

### #221: Overflow

- **Rationale.** Wrapping returns a wrong number with no signal. That is the failure this contract ranks first, and `sum` of two `2**62` values giving `-9223372036854775808` is not an edge case anyone can reason about downstream. The alternatives:
  - **Widening** alone, which the issue notes DuckDB does for `SUM(BIGINT)` (to HUGEINT), only moves the boundary. It still needs a checked rule at the wider type.
  - **An opt-in option** makes the default wrong.
  - **Wrapping** is what the issue records Polars and NumPy doing, while PostgreSQL and DuckDB raise. Wrapping ranks speed over this guarantee; LTSeq does not.

  The implementation difficulty the issue records is real: DataFusion's `SUM` accumulators call `add_wrapping`, and the physical planner builds `BinaryExpr` with `fail_on_overflow` off. The brief rules out making that difficulty the contract. The work is an LTSeq-owned checked UDAF and checked arithmetic on every path, and [Deliverable G] schedules it first.
- **Observable API behavior.** These raise `ArithmeticOverflowError` (an `OverflowError`) during execution, and only when the overflowing value is demanded ([§20.2]):
  - `r.x * 4` with `x = 2**62`;
  - `sum` of two `2**62`;
  - `cum_sum` and `rolling().sum()` past the limit;
  - `-(-2**63)` and `abs(-2**63)`;
  - `r.u64 - r.u64` at `0 - 1`;
  - decimal results beyond precision 38.

  Floats follow IEEE and never raise. `sum` of `int64` is `int64`, and of `decimal128(p, s)` is `decimal128(min(p + 10, 38), s)` ([§14.2]). `search_pattern` and `linear_scan` raise only errors the DataFusion path may raise ([§20.2], [§21.2]). There is no session option and no per-call option.
- **Consequences.** Pipelines that returned wrapped values now fail. That is the intent. Checked kernels give up some SIMD speed on integer arithmetic. The issue asks for `bench_core` numbers, which do not exist yet; the cost must be measured when the kernels land, and it does not change the decision. A checked `SUM` needs its own accumulator for the plain, grouped and sliding (window) cases. The linear-scan fast path's `wrapping_sub` must become checked or the fast path must step aside ([§21.2]). The decision tightens precision: no integer or decimal result is ever silently wrong.
- **Required normative documentation.** Contract [§17.1], [§14.2] (sum types), [§8.2], [§20.1] (`ArithmeticOverflowError`). ADR 0018 is unaffected. The wrapping policy that #154 pinned is withdrawn from `docs/api.md`.
- **Acceptance tests.** P10 (exact or raises, for every integer and decimal result) over both ends of every integer and decimal range. N1, A2 (`int64` and `decimal128(38)` sum overflow), W3 and D2. P5 and F1 run each case on every evaluator with the fast paths forced on and off. E3 checks that an overflow in an undemanded column does not fail `count()`.

### #222: `concat` schema

- **Rationale.** A concatenation that changes a column's type because the other input is empty makes the output schema depend on data. A filter that happens to match nothing turns `int64` into `float64` downstream, and values above `2**53` lose precision. Polars separates `vertical` from `vertical_relaxed`, which "additionally coerces columns to their common supertype if they are mismatched" ([`polars.concat`](https://docs.pola.rs/api/python/stable/reference/api/polars.concat.html)). v0.5 provides only the strict form: the relaxed one is `cast` plus `concat`, written by the user, and a promotion the user writes is one they can see. Nullability may differ because it describes the plan, not the values, and refusing it would reject plans whose values are identical.
- **Observable API behavior.**
  - `ints.concat(empty_f)` raises `SchemaMismatchError` at plan time, listing each difference (`x: int64 vs float64`). The same holds for `intersect` and `difference`, and for any name or column-order difference.
  - `int32` with `int64` is refused.
  - Order is defined when every input's is; `sort_keys` is `None` ([§13.1], [§9.2]).
  - `union` is removed. It was an alias of `concat` and kept duplicates (`py-ltseq/ltseq/advanced_ops.py:26-61`), so `concat` replaces it; SQL's `UNION` is `concat(...).distinct()`, or `distinct(keep="any")` when an input's order is undefined.
- **Consequences.** Code that relied on promotion must cast first. No value can change type or precision in a concatenation. There is no performance effect.
- **Required normative documentation.** Contract [§13] introduction, [§13.1], [§13.2] (the removed names and their replacements).
- **Acceptance tests.** U1 (empty input on either side, a nullability-only difference accepted, `int32`/`int64` refused, column order) and U2 and U3. The issue's three untested behaviors (`inf` arithmetic, `r.x + 0.5` giving `float64`, a 10,000,000-character string) are kept as boundary cases of N4 and D10.

### #228: Float with decimal columns

- **Rationale.** Today DataFusion converts the float to `decimal128(30, 15)`, so NaN, infinities and magnitudes of `1e15` and more fail at collect, and the error depends on which rows `coalesce` happens to evaluate. In arithmetic and shared values, `float64` is the only type that holds every value of both inputs' ranges. The decimal side loses digits beyond binary64, and [§17.4] states that. Comparisons are different, because the answer is one bit: comparing exact values is cheap and loses nothing. Converting to either type would make `r.f > r.p` wrong near the rounding boundary. So comparisons do not adopt `float64`, which is the revision. A planning error for every float/decimal pair was rejected: it would make NaN, infinities and large floats unusable next to a decimal, which is common in financial data.
- **Observable API behavior.**
  - On the issue's table, `coalesce(r.p, r.f)`, `r.p.fill_null(r.f)` and `if_else(r.k < 3, r.f, r.p)` return `float64`, with `1e20`, `inf` and NaN intact. `r.f + r.p` stays `float64`.
  - `r.f > r.p` compares exact values. NaN is greater than every number.
  - A literal in a multi-column `coalesce` follows the same common type ([§17.5] rule 1).
  - Every position gives a result or a plan-time error, never an Arrow cast error at collect.
- **Consequences.** A `coalesce` of a decimal and a float column no longer returns exact decimals. That is the stated cost, and a user who needs exact decimals casts the float column to a decimal, where NaN fails visibly. Exact comparison needs a comparison kernel for mixed numeric pairs that DataFusion does not provide. The same kernel serves integer/float pairs (`2**53 + 1` vs `2.0**53`) under [§17.4] rule 3.
- **Required normative documentation.** Contract [§17.4] rules 1 and 3, [§17.5] rule 1, [§18] (NaN order). ADR 0018 D-a amended ([§1.4]). The "Literal values" section of `docs/api.md`.
- **Acceptance tests.** N4 (float with decimal gives `float64` in arithmetic and shared values). P10 (comparisons agree with `Fraction`, including NaN and `inf` against decimals). The issue's five expressions with `1e20`, `inf`, NaN and `0.1`, inline, staged and materialized. Both `coalesce` argument orders give the same type.

### #241: `decimal32`/`decimal64` with integers

- **Rationale.** Rounding `1.50` to `2` before an operation is a silent wrong result in the core arithmetic path. The decision is about where to widen:
  - **Ingestion** was rejected because it changes the type of every column a user reads, including those never combined with an integer.
  - **Upstream** cannot be a contract, because it depends on DataFusion's schedule.
  - **The coercion boundary** affects only the operations that need it.

  The widened type is `decimal128` with the same precision and scale, which holds every value of the narrower type exactly.
- **Observable API behavior.** On the issue's table with `decimal32(9, 2)` or `decimal64(18, 2)`:
  - `r.x > r.i` is `[True, True]` and `r.x == r.i` is `[False, False]`.
  - `r.x + 1` and `r.x + r.i` give `decimal128` `[2.50, 1.01]` and `[2.50, 0.01]`.
  - `r.x.fill_null(r.i)` keeps `[1.50, 0.01]` as `decimal128`.

  The same rule covers comparisons, `is_in`, `between`, arithmetic, `when`/`if_else`, `coalesce`, `fill_null`, aggregates, join keys, set operations and every fast path. An untouched `decimal32` column stays `decimal32` in the output schema.
- **Consequences.** Result types of expressions on narrow decimals become `decimal128`. No value changes except those that were wrong.
- **Required normative documentation.** Contract [§6.2] (`decimal32`/`decimal64` fully supported), [§17.4] rule 2.
- **Acceptance tests.** N4 (both widths against every integer width, in each position above). J1 (`int32` 7 joins `Decimal("7.00")`). P10 with `decimal32` and `decimal64` operands. P5 (evaluator parity).

### #246: Cross-zone arithmetic

- **Rationale.** An aware timestamp is an instant ([§19.1]). The difference of two instants does not depend on their display zones. Python's `datetime` agrees for operands of different zones, which it subtracts "as if a and b were first converted to naive UTC datetimes" ([`datetime` docs](https://docs.python.org/3/library/datetime.html), supported operations, note 3). Within one zone Python subtracts wall-clock readings, and v0.5 follows pandas there instead: 03:30 EDT minus 01:30 EST on 2024-03-10 is one hour in pandas 3.0.5 and in LTSeq, and two hours in Python. Otherwise a difference would depend on whether two values happen to share a `tzinfo` object ([§19.2]). Option A fixed this for literals only. It kept DataFusion's rule for columns, which refuses when both the zone and the unit differ. Its own brief showed that rule depends on the unit, and that it reads naive columns two different ways depending on the unit. v0.5 gives all three cases one rule:
  - Aware values compute on instants, whether they are literals or columns, at any pair of units.
  - Naive with aware raises, as in Python. This revises ADR 0018 D-d, which read a naive literal next to an aware column as wall-clock time there. That reading picks a zone the user did not write, and `replace_time_zone` states it explicitly.
  - The result unit is the finer of the two, which is DataFusion's choice wherever DataFusion succeeds.
- **Observable API behavior.**
  - On the issue's seconds column, `r.x - utc` is `duration[us]` `[None, 0]`. So are `utc - r.x` and the `ny` spelling.
  - `r.x - utc` is the instant difference at every column unit. `dt.diff` is removed ([§19.4]), so subtraction is the one spelling and the two can no longer disagree.
  - Within one zone, subtraction across a DST transition is the instant difference: one hour from 01:30 EST to 03:30 EDT on 2024-03-10.
  - Two aware columns of different zones and units subtract by instant ([§23.13](../contract/examples-semantics.md#2313-time-zones-and-dst): New York 09:00 minus Paris 15:00 on the same day is `timedelta(0)`).
  - Comparisons are by instant.
  - In shared values, an aware value of another zone keeps its instant and takes the column's zone ([§17.5]).
  - `r.x + utc` (timestamp plus timestamp) and naive with aware raise `LTSeqTypeError` at plan time.
  - DST: subtraction is by instant and never raises. `.dt` methods that produce a nonexistent or ambiguous local time raise `LTSeqValueError` during execution ([§19.4]).
- **Consequences.** Planning errors become correct values. No value that works today changes, because DataFusion already returns the instant difference where it succeeds. Code that relied on a naive literal being read as wall-clock time next to an aware column now fails at plan time and must use `replace_time_zone`. Converting units is checked: `timestamp[s]` values beyond the `ns` range raise `ArithmeticOverflowError` when the other operand is `ns`.
- **Required normative documentation.** Contract [§17.2] (aware literal types), [§17.5] (temporal shared values), [§19.1], [§19.2] (including the departure from Python within one zone), [§19.4]. ADR 0018 D-d and D-m amended ([§1.4]). The "Literal values" section of `docs/api.md`.
- **Acceptance tests.** H1 covers zoned columns at s, ms, µs and ns against `utc` and `pd.Timestamp` literals of other units, in both operand orders, with the NULL row; its expected values come from pandas `Timestamp` arithmetic and `utc.astimezone(column_zone)`. Same-zone subtraction across a DST transition. Column pairs at every unit and zone combination. A literal at the 2024-03-10 07:00 UTC transition. N5 for shared values. Inline, staged and materialized forms (P2). P15.

### #247: Timestamp widening

- **Rationale.** Both options widen the column to admit a literal. Each has a defect the brief documents:
  - Option A widens to the literal's declared unit, so an ns-captured literal of ms precision makes far-dated rows fail at collect.
  - Option B chooses the unit from the literal's value, so a parameterized query can change its output schema from run to run.

  Not widening at all removes both. The output type is the column's type, always. A literal the column cannot hold exactly is refused at plan time, and nothing is rounded. A user who wants more precision says so with `cast`, which makes the range trade-off visible at the point it is made. This matches how v0.5 treats every other shared value: a literal never widens the type of the non-literal values ([§17.5] rules 1 and 2). Value-dependent types (B) were rejected on principle: a schema known at plan time ([§3.2]) should not depend on a parameter's digits.
- **Observable API behavior.** On the issue's seconds column:
  - `r.x.fill_null(lit)` with `lit = pd.Timestamp(1704067200_001_000_000, unit="ns")` raises `CastError` at plan time, naming the literal. So does `lit.to_pydatetime()`, which also carries the .001 s.
  - `r.x.fill_null(pd.Timestamp("2024-01-01"))` is `timestamp[s]`.
  - `r.x.cast(pa.timestamp("ms")).fill_null(lit)` is `timestamp[ms]` with the 2300-01-01 row intact.

  The same holds for `coalesce`, `if_else`, `when`, `update`, `insert` and `shift(fill_value=)`, and for naive, same-zone and cross-zone spellings. Arithmetic and comparison are unaffected: there, units are combined exactly ([§19.2]).
- **Consequences.** Five grid cells (`ts_s/dt_1_5s` in `fill`, `coal`, `coal_rev`, `ifelse_t`, `ifelse_f`) change from `timestamp[us]` to `CastError`. The oracle in `oracle.py` and the mutant in `test_grid_oracle.py` change with them. Code that relied on silent widening fails at plan time with a message pointing at `cast`. Precision is never lost, and no range failure can surface at collect from a shared value.
- **Required normative documentation.** Contract [§17.5] (the temporal paragraph). ADR 0018 D-m revised ([§1.4]). The "Literal values" section of `docs/api.md`.
- **Acceptance tests.** N5 (exact finer literal accepted at the column's unit, inexact refused, in every shared-value position and every spelling). The issue's reproducer with `cast` succeeding. P12. The grid run shows 0 REGRESSION and 0 UNDECIDED after the five cells are reclassified under this decision.

### #248: All-literal shared values

- **Rationale.** With a column present, `2**53 + 1` next to `1.5` is refused. Without one, DataFusion rounds it. The principle that an exact integer is never silently rounded cannot depend on whether a column happens to share the result. Refusing costs ltseq no type authority: DataFusion still proposes the common type ([§17.5] rule 1), and ltseq only judges whether every literal fits it, as rule 1 already does with columns. The issue asks where the rule stops:
  - **Nested expressions.** It stops at each expression node. A nested all-literal node is judged on its own.
  - **`when` chains.** All branches of a chain are one node.
  - **D-j.** It covers a float next to a decimal: `Decimal("0.1")` with `1.5` has common type `float64`, which cannot hold 0.1.
  - **D-m.** It covers a date next to a timestamp, which never share a column under [§17.5].
  - **Literal arithmetic.** It does not cover literal arithmetic (D-f). An operator computes a new value, and its rounding is the operator's documented semantics, not a placement of a written value.
- **Observable API behavior.** These raise `CastError` at plan time naming `9007199254740993`:
  - `if_else(r.b, 2**53 + 1, 1.5)`;
  - `when(r.b, 2**53 + 1).otherwise(1.5)`;
  - `coalesce(2**53 + 1, 1.5)`;
  - the nested `if_else(r.b, r.i, if_else(r.b, 2**53 + 1, 1.5))`.

  The following do not raise:
  - `if_else(r.b, 2**53 + 1, 1)` is `int64`.
  - `if_else(r.b, Decimal("1.5"), 0.1)` is `float64`, because both are exact in `float64`.

  The following raise:
  - `if_else(r.b, Decimal("0.1"), 1.5)` raises `CastError`.
  - `if_else(r.b, date(2300, 1, 1), datetime(2024, 1, 1, 12))` raises `LTSeqTypeError`.

  `lit(2**53 + 1) + 0.5` is unchanged: binary64 arithmetic.
- **Consequences.** Some constant expressions that run today now fail at plan time. The issue rates them uncommon. `test_branches_that_are_all_literals_have_no_context` is replaced by tests of the refusal. The grid has no all-literal cell, so the new tests are separate.
- **Required normative documentation.** Contract [§17.5] rule 3 with its nested cases, and the temporal paragraph. ADR 0018's D-b/D-i row is extended to all-literal values ([§1.4]). The "Literal values" section of `docs/api.md`.
- **Acceptance tests.** N5 (all-literal cases above, nested case, `when` chain, D-j and date/timestamp cases). D5 (inexact literal gives `CastError`). P12. Every case of `test_pure_literal_case_branches_take_datafusions_type` keeps its type and values when its literals are exact.

<!-- v0.5-modular:footer -->

---

Previous: [API inventory and review (B)](inventory.md) · [Index](../README.md) · Next: [Trade-off record](trade-offs.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§1.4]: ../contract/overview.md#14-relation-to-earlier-decisions
[§2]: ../contract/public-surface.md#2-exports
[§2.2]: ../contract/public-surface.md#22-other-modules
[§3.2]: ../contract/public-surface.md#32-ltseq-invariants
[§3.3]: ../contract/public-surface.md#33-nestedtable-and-groupby-invariants
[§3.4]: ../contract/public-surface.md#34-lambdas-and-proxies
[§4]: ../contract/loading-and-laziness.md#4-loading
[§4.1]: ../contract/loading-and-laziness.md#41-source-paths
[§4.3]: ../contract/loading-and-laziness.md#43-ltseqread_parquet
[§5.2]: ../contract/loading-and-laziness.md#52-ltseqcollect
[§6.1]: ../contract/schema-and-table-operations.md#61-schema-and-columns
[§6.2]: ../contract/schema-and-table-operations.md#62-supported-types
[§8.2]: ../contract/expressions.md#82-operators
[§8.4]: ../contract/expressions.md#84-math-functions
[§9]: ../contract/ordering.md#9-ordering-contract
[§9.1]: ../contract/ordering.md#91-order-state
[§9.2]: ../contract/ordering.md#92-sources-and-propagation
[§9.3]: ../contract/ordering.md#93-order-requirements
[§9.5]: ../contract/ordering.md#95-assume_sorted
[§10.1]: ../contract/windows-and-grouping.md#101-window-methods
[§10.4]: ../contract/windows-and-grouping.md#104-over
[§10.5]: ../contract/windows-and-grouping.md#105-search_first
[§13]: ../contract/joins-and-sets.md#13-set-and-bag-operations
[§13.1]: ../contract/joins-and-sets.md#131-concat
[§13.2]: ../contract/joins-and-sets.md#132-intersect-and-difference
[§14.2]: ../contract/aggregation.md#142-aggregate-expressions
[§15]: ../contract/streaming-and-output.md#15-streaming
[§15.1]: ../contract/streaming-and-output.md#151-to_batches
[§16]: ../contract/streaming-and-output.md#16-output-and-interchange
[§16.5]: ../contract/streaming-and-output.md#165-writers
[§16.6]: ../contract/streaming-and-output.md#166-pickle
[§17]: ../contract/numeric-null-temporal.md#17-numeric-semantics-and-literals
[§17.1]: ../contract/numeric-null-temporal.md#171-integer-and-decimal-results-are-checked
[§17.2]: ../contract/numeric-null-temporal.md#172-literals
[§17.3]: ../contract/numeric-null-temporal.md#173-arithmetic-operators
[§17.4]: ../contract/numeric-null-temporal.md#174-types-of-mixed-operands
[§17.5]: ../contract/numeric-null-temporal.md#175-shared-values
[§18]: ../contract/numeric-null-temporal.md#18-null-nan-and-boolean-logic
[§19]: ../contract/numeric-null-temporal.md#19-temporal-semantics
[§19.1]: ../contract/numeric-null-temporal.md#191-types
[§19.2]: ../contract/numeric-null-temporal.md#192-arithmetic-and-comparison
[§19.4]: ../contract/numeric-null-temporal.md#194-dt-methods
[§20.1]: ../contract/errors-and-performance.md#201-exception-classes
[§20.2]: ../contract/errors-and-performance.md#202-stages
[§21.1]: ../contract/errors-and-performance.md#211-materialization
[§21.2]: ../contract/errors-and-performance.md#212-fast-paths
[§22]: ../contract/api-reference.md#22-complete-canonical-api-reference
[§22.1]: ../contract/api-reference.md#221-ltseq
[§23.8]: ../contract/examples-sequences.md#238-partitions-across-processes
[Deliverable B]: inventory.md#b-api-inventory-and-review
[Deliverable G]: impact-map.md#g-implementation-impact-map

<!-- /v0.5-modular:links -->
