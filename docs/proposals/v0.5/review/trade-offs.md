<!-- v0.5-modular:header -->

# v0.5 review: Trade-off record

[Index](../README.md) › Review · Previous: [Decisions on the eleven open semantic issues](decisions.md) · Next: [Examples, test matrix and implementation impact map (E, F, G)](impact-map.md)

**Non-normative.** Part of the [review](../README.md#ltseq-v05-api-review) behind the contract. It records evidence, reasoning and decisions; the behavior itself is specified by the contract modules.

**Scope.** Where two goals of the brief pulled in different directions: the side v0.5 chose and the cost accepted, and which DataFusion rewrites row-wise demand allows.

**Most cited from here.** [§17] Numeric semantics and literals · [§20] Errors · [§9] Ordering contract · [§4] Loading

<!-- /v0.5-modular:header -->

## Trade-off record

These are the places where two goals of the brief pulled in different directions. The record names the side v0.5 chose and the cost accepted. Correctness outranks ergonomics, which outranks performance. Costs marked "unmeasured" have no benchmark yet. The impact map ([Deliverable G]) measures each one in M0, before the item that builds it.

| Conflict | Decision | Cost accepted | Contract |
|---|---|---|---|
| Overflow checks against vectorized throughput | Integer and decimal arithmetic is checked everywhere, aggregates and windows included. There is no wrapping mode. | A check on every integer operation and accumulator (unmeasured; M0, before M5). | [§17.1], #221 |
| Deterministic order against parallel scan speed | Readers define file-then-row order, and every terminal restores it with an order-preserving merge. | A merge step, and waiting on a slow partition before later rows can be emitted (unmeasured; M0, before M1). | [§4.1], [§21.1], #148 |
| Reading "the previous row" in file order against naming the order | The sequence tier needs declared `sort_keys`, not just a defined order. `with_row_index()` declares file order in one call. | One extra call when file order is the intended order. | [§9.3] |
| Inspecting any table against determinism | `head` and `show` work on unordered tables and may return different rows per run. | A named exception to principle 9. | [§9.3] |
| One expression spelling against a deferred column object | Lambdas only. There is no `col()`. | Code that builds expressions outside a table call takes the row proxy as a parameter. | [§3.4] |
| `is None` reads naturally, and Python cannot overload `is` | The lambda's own `x is None` means `is_null()`. A helper's is checked, not rewritten, and refused when the lambda names the helper by a global or closure variable, through other helpers and wrappers (#185, #304). A Python `bool` raises where a condition is required and as the whole value of any expression lambda (D4), which catches the same mistake made in a helper the check does not reach in both positions. | Constants are written `lit(True)`, or as a non-callable `derive(flag=False)`. A helper that tests a Python value with `is None` is refused, possibly several calls away from the lambda, and tests against a sentinel instead. A function the lambda only passes to another one is checked as if called, so it is refused even when the receiver never calls it. For a helper called through an attribute, received as a default value or a `functools.partial` argument, behind a proxy or installed as a package, `helper(r) or expr`, `helper(r) and expr`, `a if helper(r) else b`, `~helper(r)` and a `bool` nested inside an expression stay undetectable, and [§3.4] lists them. The rewrite reads interpreter internals that change between Python minor versions, so S5 runs on each supported one. | [§3.4], [§7.3] |
| Convenience of promotion in `concat` against silent type changes | Exact schemas, and no relaxed variant. | Explicit casts before a mixed-type `concat`. | [§13], #222 |
| IEEE NaN against well-defined grouping | `NaN == NaN` in comparisons, grouping, joins and set operations, as in Polars and DuckDB. | A departure from IEEE 754 that the contract states. | [§18] |
| NULL join keys: pandas against SQL | Joins never match NULL keys. Grouping, `distinct` and set operations treat NULL as equal. | pandas users meet a difference: `merge` matches null keys, and its own documentation warns that "This is different from usual SQL join behaviour" (`pandas.merge` docstring, pandas 3.0.5). Polars defaults to `nulls_equal=False` ([`DataFrame.join`](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.join.html)). | [§12.1], [§18] |
| Python `datetime` against instant arithmetic | Aware values subtract and add as instants, even within one zone, as in pandas. | Code ported from plain `datetime` gets a different answer across a DST change. | [§19.2], #246 |
| Laziness against discovering output columns | `pivot` without `column_values=` and `partition` read the data when called. | Two eager calls, both listed. | [§5.3], [§14.5], [§14.6] |
| Join expressiveness against one key form | Keys are column names. | A computed key needs a `derive` first, and a non-equi join is `cross` followed by `filter`. | [§12.1] |
| Exact values against explicit conversion | An implicit conversion that places a supplied value into a type never rounds and never changes its kind: a literal among shared values or into a fixed target, a declared value, a `requested_schema` value (AC17). A literal operand of float arithmetic rounds as a column does (F13). `cast` and `try_cast` follow the [§17.6] table, and where a rule rounds or truncates they return that value: `cast("int64")` of 2.7 is 2, as Python's `int(2.7)`. An explicit cast is how a user asks for rounding. | A lossy `cast` does not raise. pyarrow's default safe cast does (`Float value 2.700000 was truncated converting to int64`, pyarrow 25.0.1), so code relying on that check loses it. `r.f + (2**53 + 1)` is `0.0` where `f` is `-2.0**53`, as in Python. A declared `date32` does not parse `"2024-01-01"` and `requested_schema` does not format numbers as text, where `cast` does both, so such input needs a `cast` or a value of the right kind first. | [§4.6], [§8.5], [§16.4], [§17.4], [§17.6] |
| A literal's precision against the column's type | A literal never widens a column's type ([§17.5] rule 1): `r.i32.fill_null(0.0)` stays `int32`, and `fill_null(1.5)` raises `CastError` at plan time. An all-literal expression takes the common type of its literals. This revises ADR 0018 D-b/D-i, under which both calls give `float64`. | A user who wants `float64` casts the column first. ADR 0018 is amended (M28). | [§17.5], #248 |
| Bit-identical float aggregates against the cheapest summation | Float `sum`, `mean`, `var`, `std`, `cov`, `corr`, running sums and rolling statistics are the exact value of their inputs rounded once (`std` is the correctly rounded square root of the exact `var`), so their bits do not depend on order, batch size or partitioning, evaluator and batching parity (P5, P6) can be tested with equality, and no representable result becomes `inf`, `0.0` or NaN (D2). | An exact accumulator of the sums, squares and products per group or frame in place of a float add (unmeasured; M0, before M13). | [§14.2], [§21.2] |
| Python's spelling against pandas and Polars `contains` | `.str` follows Python's method names (`startswith`, `endswith`, `rjust`, `ljust`), and substring search has two explicit names, `contains_literal` and `contains_regex`, with no bare `contains`. pandas reads `contains` as a regular expression by default (`regex=True`, pandas 3.0.5) and so does Polars ([`literal=False`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.str.contains.html)), while Python's `in` is literal. | Longer names; `contains` from pandas or Polars code raises `AttributeError` naming both replacements. | [§8.6], [§8.7] |
| A positional default for `distinct` against use on unordered tables | `keep="first"` stays the default, so `distinct()` is positional and raises `SortRequiredError` on an undefined order; the message names `keep="any"`. Without keys it compares all columns, and which duplicate survives matters because `-0.0` and `0.0` are equal but differ in bits. | One keyword on the output of `group_by` or a `right`/`full` join. | [§7.6] |
| Validating declared order against early stopping | `assume_sorted` is checked on the rows an execution reads, and an execution never skips rows because of the declaration. A violation therefore makes a result raise but never makes it wrong; `head(n)` over a source whose violation lies after row *n* succeeds. `validate=` on a join, by contrast, reads every key on each constrained side. | A violation can go unreported by a call that stops early, and `head(1)` with `validate=` reads all keys. | [§9.5], [§12.1], [§20.2] |
| Validating declared order against Parquet pruning | A filter above `assume_sorted` demands its predicate on every row, so statistics pruning and predicate pushdown into the scan are not applied below the declaration (D7, option A). A filter placed before `assume_sorted` prunes as usual. | `read_parquet(...).assume_sorted("ts").filter(...)` reads every row group, and the user moves the filter first to prune (unmeasured; M0, before M3). The alternative, a parity exception under which pruned rows go unvalidated, would add a seventh exception to principle 9. | [§9.5], [§23.10] |
| One read for `partition` against lazy values | `partition` finds its keys with one read when called, and each value reads the source again when it executes (D6, option B). | A source that changes between those reads gives a dict that partitions neither state: a key added later is missing, a key whose rows were removed maps to an empty table, and nothing raises. N partitions cost N + 1 reads. `t.collect().partition(k)` reads once and holds the table in memory. | [§14.5], [§23.8] |
| Deterministic errors against parallel execution | When several demanded values fail, the error of any one of them is raised (D3), a sixth named exception to principle 9. | Which error a failing execution reports can change with partitioning, batch size and evaluation order, so tests compare against the set of errors the failing values raise. No result is returned either way. | [§1.3], [§20.2], [§24.1] |
| Reading integer text without a declared type against never rounding | Integer syntax is `int64`, else `uint64`, and `float64` only when every integer-syntax value is within ±2**53; anything else raises at the call and names `schema=` (D1). `from_dict` and `from_rows` follow the same rule. | A column that mixes integers above 2**53 with fractions, or holds integers outside both integer types, needs a declared type. The baseline reads the first as rounded `float64` and the second as `string`. ZIP codes such as `02134` read as `int64` unless declared `string`. | [§4.2], [§4.6], [§17.6] |
| One integer result rule against widening by operator | `+`, `-`, `*`, `//` and `%` take [§17.4]'s common type, `**` takes its base's type, and `diff` alone widens, to `int64` (`decimal128(20, 0)` for `uint64`), because a difference of unsigned values is signed (D5). | `int8 // int8` is `int8` and raises past 127, where the baseline widens it to `int64`. A user who wants a wider result casts first. | [§10.1], [§17.3], [§17.4] |
| Guarding by the operand written first against reordering conjuncts | Either operand of `&` guards the other where it is FALSE, and of `\|` where it is TRUE, so the order of operands is not observable; chained filters stay ordered (D10). | A fallible operand is evaluated under a mask on either side, the left one included, which `BinaryExpr` evaluates unmasked on every row today; two fallible operands each need the other's mask (unmeasured; #251 measurement 4, before M16). The order of chained filters stays observable. | [§8.2], [§20.2] |
| Keeping a filter's pushdown past a sort, grouping or `distinct` against raising the errors of their keys | A sort, grouping or aggregate demands its keys and arguments on every row of its input, and a join on every row of its right input, so a later filter does not remove their errors (D10a, option (a)). `PushDownFilter` moves a filter below one of them only where nothing it skips can fail (X4 in P). | Once M5 makes integer arithmetic checked, a derived integer column used as a sort, group, `distinct` or right join key stops the filter above it from moving down, and with it the Parquet pruning that follows (unmeasured; #251, before M16). Option (b), demanding only on rows that reach the output, keeps the pushdown but needs errors carried as values through sort and aggregate on every path that cannot push the filter down, or parity breaks ([§21.2]). Moving from (a) to (b) later only turns errors into results; the reverse would not be compatible. | [§20.2] |
| Accepting every Arrow producer, and inputs without columns, against what each format can hold | `from_arrow` takes tabular data only (F14). A table without columns is a valid relation with a row count: an Arrow input without fields, a pandas frame without columns and `from_rows` keep their rows, and so do `select()` and a `drop` of every column (D12 as the owner modified it). | `from_arrow(arr)` becomes `from_arrow(pa.table({"x": arr}))`. `from_dict({})` raises, since a mapping without columns gives no row count. `write_csv` and `write_parquet` refuse a table without columns: CSV cannot write a row without fields, and pyarrow 25.0.1 and the `parquet` 59.2.0 crate both write a table of 3 rows without columns as a file of 0 rows, so a round trip would lose its rows. | [§4.4]–[§4.6], [§6.1], [§16.5] |
| One key form for `partition` against every key type | Keys are the values `to_dicts` returns, and a key it cannot convert raises `CastError` at the call (D11). | A `timestamp[ns]` key that is not a whole microsecond, or a date outside years 1–9999, needs a derived key such as `r.ts.dt.truncate("second")`. | [§14.5], [§16.3] |
| Python's integer `/` by zero against NumPy and pandas | Integer `/` by zero raises `DivisionByZeroError`, as Python's `int / int` does, although its result type is `float64`; an operation with a float operand follows IEEE (D8 as the owner modified it). | One zero divisor in an integer column fails the query where NumPy and pandas give `inf` or NaN; `r.a / r.b.cast("float64")` gives the IEEE result. | [§17.1], [§17.3] |
| Exact duration scaling against Python's `timedelta` | `duration * integer` and `duration // integer` are exact in the duration's unit and checked, `//` floors, and no operation moves to a finer unit implicitly (D14 as the owner modified it). | Results depend on the unit: −5 s `// 2` is −3 s in `duration[s]` and −2.5 s in `duration[ms]` and in Python's `timedelta`. A finer quotient needs a `cast` to the finer unit first. | [§19.2] |

### Rewrites under row-wise demand

[§20.2] fixes which values a terminal demands, and lets an execution evaluate anything else speculatively only if errors from values that are not demanded are suppressed. Once M5 makes integer and decimal arithmetic checked, most numeric expressions can fail, so each DataFusion rule or evaluator that moves an expression onto rows where it is not demanded, or skips rows where a value is demanded, needs a status. The table covers what LTSeq runs at the baseline: DataFusion 55's default rules plus the two Parquet options `src/engine.rs:51-52` turns on, `pushdown_filters` and `reorder_filters`.

- **Safe:** never evaluates a value on a row where it is not demanded and never skips a row where one is.
- **Masked:** safe only if errors of the moved expression are suppressed on rows where it is not demanded, by a selection mask or by carrying errors as values (M16).
- **Restricted:** not applied where an affected value can fail. Masking cannot help, because the skipped rows are never read.

"Probe" means reproduced at the baseline; "source" means read in the crate sources `Cargo.lock` pins. Nothing in the table was benchmarked.

| Rule or evaluator (DataFusion 55) | What it moves or skips | Status | Evidence |
|---|---|---|---|
| `AND`/`OR` evaluation (`BinaryExpr`) | For `AND`, evaluates the right operand on every row of the batch, unless the left operand has no NULL and is FALSE on every row (right operand skipped) or TRUE on at most 20% of the rows (`PRE_SELECTION_THRESHOLD`; right operand evaluated on those rows only). `OR` is the same with TRUE and FALSE swapped | Masked, for both operands: under D10 the left operand's errors are suppressed on rows where the right one is FALSE (TRUE for `OR`), and `BinaryExpr` evaluates the left operand unmasked on every row. At the baseline whether a guarded row raises depends on the other rows in its batch, which [§20.2] forbids | Probe: `filter((r.b != 0) & (r.a // r.b > 1))` with a row `a = 7, b = 0` raises `Divide by zero error` when the guard is TRUE on 1 row of 4, and returns the passing row when it is TRUE on 1 row of 5. The same conjuncts written in reverse behave identically, because the cheap-first rule below puts the guard first |
| `PushDownFilter`, merging adjacent filters | `filter(p).filter(q)` becomes one `p AND q`, so `q` meets rows `p` removed, subject to the row above | Masked, and not as `&`: the earlier filter's predicate is demanded on every row ([§20.2]), so the merged predicate raises `p`'s errors everywhere and suppresses `q`'s where `p` is not TRUE. Evaluated as [§20.2]'s `p & q` it would also suppress `p`'s errors where `q` is FALSE, so where `p` can fail the merge is masked this way or not made | Probe: `filter(r.x != 0).filter(10 // r.x > 1)` over `x = [0, 2, 5]` raises `Divide by zero error`. The reversed `filter(10 // r.x > 1).filter(r.x != 0)` over `x = [2, 0, 0, 0, 0]` is merged and reordered to `x != 0 AND floor_div(10, x) > 1` and returns `[{"x": 2}]`, where [§20.2] requires `DivisionByZeroError` |
| `PushDownFilter`, cheap-first conjunct order (`reorder_predicates`) | A stable reordering of a filter's conjuncts: those built only from columns, literals, operators other than LIKE and regex matching, `CASE`, casts, `IN` lists and NULL tests go first, and every function call, LTSeq's `floor_div` included, goes after. It runs after merging, so it also moves a later filter's conjunct ahead of an earlier one's | Safe within one filter: under D10 the order of conjuncts is not observable, and each conjunct is still masked as in the first row. Across merged filters, see the row above | Probe: `filter((r.a // r.b > 1) & (r.b > 0))` plans as `b > 0 AND floor_div(a, b) > 1`. Source: `push_down_filter.rs:815`, `simplify_expressions/reorder_predicates.rs` |
| `PushDownFilter`, conjuncts below projections and joins | Splits `p AND q` and moves each conjunct below the projection or to the join side its columns come from | Below a projection, safe: the conjunct reads only pass-through columns, and fewer rows are evaluated. Below a join, masked: the conjunct meets rows the join removes and rows another conjunct rejects | Source |
| Parquet filter pushdown with `reorder_filters` | Evaluates conjuncts inside the scan as successive row filters, reordered by estimated cost | Masked: whatever the order, a conjunct evaluated first must not raise on rows where a later one is FALSE (D10). The order itself is not observable | Source |
| Parquet statistics pruning (row groups, page index, bloom filters) and dynamic filters from TopK, joins and aggregates (on by default) | Skips rows that statistics or a running bound show cannot pass | Restricted: applied only where no value demanded on a skipped row can fail, and never below `assume_sorted` ([§9.5], D7). Under D10 a filter's other conjuncts are not demanded on rows where one conjunct is FALSE, so `filter((10 // r.y > 1) & (r.i >= 6))`, with `i` an integer column, can skip a row group whose `i` statistics show no NULL and a maximum below 6. Where `i` may be NULL, `i >= 6` is NULL there rather than FALSE, the left conjunct is demanded, and the row group is read. A float column's statistics omit NaN, which [§18] orders above every number, so they never show `x >= 6` FALSE on every row | Source |
| `OptimizeProjections` | Drops expressions no output uses and merges adjacent projections | Safe: a value no output uses is not demanded, and merging never evaluates a value on more rows | Source |
| `ProjectionPushdown` (physical) | Swaps a narrowing projection below a `FilterExec` (`FilterExec::try_swapping_with_projection`), so its expressions run before the filter | Masked, or not applied when the projection can fail | Source |
| `CommonSubexprEliminate` | Extracts repeated subexpressions. Occurrences in the right operand of `AND`/`OR`, in `CASE` branches after the first condition and in short-circuiting function arguments are conditional, and a subexpression is extracted only with an unconditional occurrence (`conditional_children`) | Masked under D10: `conditional_children` treats the first operand of `AND`/`OR` as always evaluated, but its values are demanded only where the other operand is not FALSE (not TRUE for `OR`). A subexpression extracted on the strength of an occurrence there is masked, or not extracted where it can fail | Source: `common_subexpr_eliminate.rs:652` |
| `PushDownLimit`, `LimitPushdown` | Move `head(n)` into scans and below projections, and run several partitions concurrently under the limit | Moving the limit down is safe. Concurrent partitions are masked: an error from a row after the *n*-th output row is not demanded, so it must not surface before the earlier partitions have delivered *n* rows | Source |
| `JoinSelection` | Swaps the build and probe sides by estimated size | Safe without an early stop, since a join reads every row of both sides then. Under `head(n)` over an `inner` or `left` join, building on the left reads it whole, so errors on left rows the output does not need are masked or the swap is not made | Source |
| `AggregateStatistics` | Answers `count()`, `min` and `max` from Parquet metadata without reading rows | Restricted: not below `assume_sorted` (`count()` checks every pair, [§9.5]), and not for float `min` and `max`, whose Parquet statistics omit NaN | Probe: `agg(m=lambda g: g.x.max())` over a Parquet file holding `[1.0, NaN, 2.0]` returns `2.0`; the same data in memory, ungrouped, returns NaN, which [§14.2] requires (#253) |
| Constant folding (`SimplifyExpressions`) | Evaluates an expression of literals at plan time | Restricted: a constant that can fail is left for execution, where [§17.2] raises its error only if the value is demanded. The evaluator keeps failing constants other than casts for execution already, and turns a failing `Cast` of a literal into a planning error. So `cast()` lowers a cast that can fail to `OpaqueCast`, a UDF, when its input is a constant too: the evaluator folds it where it succeeds and leaves it for execution where it fails (X1, guarded since #273). A cast that coercion inserts is still a `Cast` (`type_coercion` in the inventory below) | Probe at the baseline: `if_else(r.d == 0, lit("abc").cast("int32"), …)` fails in `simplify_expressions` on `to_arrow`, on `count` and on an empty table. Since #273 it raises only where a row has `d == 0`. Source: `expr_simplifier.rs:546-575` |
| Simplifier identities and absorption | Identities gated on non-nullability (`A * 0` to `0`, `A = A` to TRUE, `IS NULL` of a non-null input) and absorption (`A OR (A AND B)` to `A`) drop an operand | Restricted: applied only where the dropped operand cannot fail. Reporting every fallible expression as nullable stops the gated identities, and [§6.1] lets planned nullability be conservative. Absorption has no gate, and under D10 it drops `B` on rows where `A` is NULL, where `B` is demanded (X2) | Probe of the identities in an assessment audit pass, not re-run; absorption from source |
| `unwrap_cast` (`SimplifyExpressions`) | Rewrites `CAST(col AS narrower) > literal` to compare the wider column with the literal, which removes the cast | Restricted: applied only to casts that cannot fail. **A wrong result at the baseline** (#260) (X3) | Probe: with `a = 2**40` (`int64`), `r.a.cast("int32") > 5` returns `True`, while the cast alone raises. Source: `unwrap_cast.rs:110-150` |
| `PushDownFilter` past `Sort`, `Aggregate`, `Distinct` and a join's right input | Moves a filter below the operator, so the operator's keys and arguments are never evaluated on the rows the filter removes | Restricted under D10a (a): [§20.2] demands those values on every row of the operator's input, so the filter moves only where none of them can fail. The moved predicate's own errors are masked as in the row on joins above (X4) | Probe: `derive(k=10 // r.x).sort("k").filter(r.x != 0)` over `x = [0, 2, 5]` returns two rows, where [§20.2] requires `DivisionByZeroError`. Source: `push_down_filter.rs` |
| Evaluation order of the plan itself | A `derive` runs before a filter that cannot move below it (one reading the derived column through a window), and before an `inner`, `semi` or `anti` join on every left row, including rows without a match, whose non-key values are not demanded | Masked: errors carried as values and raised only for rows that reach the output | Contract reading |

Rows X1–X4 come from the architectural reassessment (P); #273 rated them, with every other rule, in the rule inventory below, and guarded X1 and X3; the guards of X2 and X4 are M16's. Not assessed beyond the inventory's reading of the source: expression simplification beyond those rows, outer-join elimination and join reordering beyond `JoinSelection`'s side swap. The subquery decorrelation rules rewrite plans LTSeq's lowering does not build. The reassessment found the `CASE` to `AND`/`OR` rewrite and `coalesce`'s short-circuit sound under D10 by reading the source, and pushdown through a union and through a window on its partition keys sound.

**Order (X5).** The table rates each rule for errors only. [§9.1] also requires every terminal to deliver the defined order, so M1 rates every rule for order as well. These are unrated: `JoinSelection`'s build and probe swap, which changes the order of an `inner` join's output; partitioned joins and aggregation, which `src/engine.rs:46-47` turns on (`repartition_joins`, `repartition_aggregations`) and which decide the row that `first`, `last` and `string_agg` see first; scan repartitioning; and `SortPreservingMerge`, which is unstable on ties. The rule inventory below gives each rule an order column, where these are Open until M1 rates them.

The cost of masking is M0's measurement 4, and its prototype gates M5: checked arithmetic makes nearly every numeric expression fallible, so M16's masked evaluation has to exist first. Under D10 the measurement covers both operands of a fallible `&` or `|`. The baseline failures in the `AND`/`OR` and merging rows are tracked in #252.

**`&` and `|`.** The statuses above follow D10 ([Deliverable K]): either operand of `&` guards the other, so the order of conjuncts in one predicate is not observable, while the order of chained filters is. Under the left-to-right rule of `a49af0b`, `reorder_predicates` and pruning on any conjunct but the first would have been restricted, because each can remove the error of a conjunct written first.

### Rule inventory

The table above rates what each kind of rewrite does. This inventory lists every rule a session LTSeq creates runs, by the name DataFusion gives it, with a rating for errors ([§20.2]) and one for order ([§9.1]), and the test that pins a rule rated Restricted or Disabled. `src/rule_inventory.rs` compares the rows between the markers below with the rules of a live session, so a DataFusion upgrade that adds or renames a rule fails a test until the rule has a row, and a row whose rule no longer runs fails too. It runs in the default build; a measurement build (`m0`) adds its prototypes.

Ratings are those of the table above, plus:

- **Disabled:** LTSeq turns the rule off by its flag (`keep_unrated_rules_off` in `src/engine.rs`), so a new default does not turn it on.
- **Unreached:** the rule rewrites only a plan shape LTSeq's lowering never builds, such as a subquery. A subquery written into `filter_where`'s SQL clause is the one way to reach these, and is not assessed.
- **Open:** not rated yet. For errors, M16 rates these; for order, M1 does, as the X5 paragraph above describes.

For order, **Safe** means the rule never changes the sequence of the rows it keeps, and **Restricted** that it does in a case a guard rules out. "Reading" in a row means the rating comes from the DataFusion 55 source, not a probe.

<!-- rule-inventory:begin -->

| Rule | Stage | Errors | Order | Guard |
|---|---|---|---|---|
| `resolve_grouping_function` | analyzer | Unreached: rewrites `GROUPING()` | Unreached | |
| `type_coercion` | analyzer | Open: the casts it inserts are part of the expression and run where it runs, but one next to a literal is a plain `Cast` that `unwrap_cast` may remove, and whether any of those can fail is unrated | Safe: changes expressions only | |
| `rewrite_set_comparison` | logical | Unreached: `= ANY` and `> ALL` subqueries | Unreached | |
| `optimize_unions` | logical | Safe: flattens nested unions and drops a union of one input | Safe | |
| `unions_to_filter` | logical | Disabled: merges the filters of `UNION DISTINCT` branches over one source into `p OR q`, which suppresses `p`'s errors where `q` is TRUE, while each branch demanded its own predicate on every row | Disabled | `engine::tests::the_rules_rated_disabled_stay_off` |
| `simplify_expressions` | logical | Restricted: the constant folding, identity, absorption and `unwrap_cast` rows above. X1 and X3 are guarded; the identities and absorption (X2) are not | Safe: changes expressions only | X1: `test_a_failing_constant_cast_raises_only_where_demanded` in `test_cast_comparison.py`, `opaque_cast` unit tests. X3: `test_cast_comparison.py` |
| `replace_distinct_aggregate` | logical | Safe: `distinct` becomes a grouping on the same columns, over the same rows | Open: which of the equal rows is kept and in what order the groups come out | |
| `eliminate_join` | logical | Open: an inner join whose condition is literally FALSE becomes an empty relation, and its inputs are not read (reading) | Safe | |
| `decorrelate_predicate_subquery` | logical | Unreached: `IN` and `EXISTS` subqueries | Unreached | |
| `scalar_subquery_to_join` | logical | Unreached: scalar subqueries | Unreached | |
| `decorrelate_lateral_join` | logical | Unreached: lateral joins | Unreached | |
| `extract_equijoin_predicate` | logical | Safe: moves `l = r` conjuncts of a join filter into its keys; under D10 the order of conjuncts is not observable | Safe | |
| `eliminate_duplicated_expr` | logical | Safe: drops a sort or grouping key identical to an earlier one | Safe: a repeated key decides no order | |
| `eliminate_filter` | logical | Restricted: a filter on a literal FALSE or NULL becomes an empty relation, so a sort, grouping or `distinct` below it never evaluates the keys D10a demands on its input (as X4; reading) | Safe | none yet |
| `eliminate_cross_join` | logical | Masked: turns a filter over a cross join into a join on its equalities, as the row on conjuncts below joins | Open: join output order | |
| `eliminate_limit` | logical | Restricted: `head(0)`, and a limit under an offset at least as large, become an empty relation, dropping the keys a sort below demands (as `eliminate_filter`; reading) | Safe | none yet |
| `propagate_empty_relation` | logical | Restricted: an operator over an empty relation becomes empty, and an inner join with an empty side is not read on the other, so its keys are not evaluated (as X4; reading) | Safe | none yet |
| `filter_null_join_keys` | logical | Disabled: unrated; adds `IS NOT NULL` filters on join keys below the join | Disabled | `engine::tests::the_rules_rated_disabled_stay_off` |
| `eliminate_outer_join` | logical | Restricted: turns an outer join into an inner one under a filter that rejects NULL on the padded side, so the filter is no longer evaluated on padded rows. In `(10 // l.a > 1) & (r.b > 0)` the right conjunct is NULL on a padded row, so under D10 the left one is demanded there and must raise where `l.a` is 0 (reading) | Open: join output order | none yet |
| `push_down_limit` | logical | Safe: the `PushDownLimit` row above | Safe | |
| `push_down_filter` | logical | Restricted: past a sort, grouping, `distinct` or a join's right side (X4, under D10a (a)); masked when merging filters and below joins, safe below projections (rows above) | Safe: a filter keeps its rows in order | none yet: X4's guard is M16's, after M0's m4b |
| `single_distinct_aggregation_to_group_by` | logical | Safe: `count(distinct x)` becomes a grouping on `x` and a count, which evaluate `x` on the same rows | Safe | |
| `eliminate_group_by_constant` | logical | Restricted: drops a constant grouping key, and with it the error of one that fails, which D10a demands; a constant that folds is safe (reading) | Safe | none yet |
| `common_sub_expression_eliminate` | logical | Masked: the `CommonSubexprEliminate` row above | Safe | |
| `extract_leaf_expressions` | logical | Safe: extracts only struct field access and file metadata functions, which cannot fail | Safe | |
| `push_down_leaf_projections` | logical | Safe: moves what `extract_leaf_expressions` extracted towards the scan | Safe | |
| `optimize_projections` | logical | Safe: the `OptimizeProjections` row above | Safe | |
| `ltseq_float_list_zeros` | logical | Safe: compares -0.0 equal to 0.0 in a float IN list (#245) | Safe | |
| `ltseq_drop_float_statistics` | physical | Safe: a guard; a Parquet scan reads no float statistics (#291), which takes float `min` and `max` out of `aggregate_statistics` and float columns out of pruning | Safe | |
| `ltseq_keep_sorts_above_offsets` | physical | Safe: a guard for `EnsureRequirements` (#339) | Safe | |
| `OutputRequirements` | physical | Safe: adds and later removes a marker of the root's requirement | Safe | |
| `aggregate_statistics` | physical | Restricted: the `AggregateStatistics` row above. Floats are guarded; below `assume_sorted` it waits for M3 | Safe: one row | `ltseq_drop_float_statistics`: `test_ungrouped_parquet_is_not_answered_from_statistics` in `test_float_min_max.py`, `test_parquet_float_statistics.py` |
| `join_selection` | physical | Masked: the `JoinSelection` row above | Open: swapping the build and probe sides changes the order of an `inner` join's output (X5) | |
| `LimitedDistinctAggregation` | physical | Restricted: under a limit, a `distinct` stops reading once it has enough groups, so the keys D10a demands on later rows are never evaluated (reading) | Open: which groups it keeps depends on the order partitions deliver rows | none yet |
| `FilterPushdown` | physical | Restricted: pushes a filter below a sort and into a grouping's keys (`gather_filters_for_pushdown`), as `push_down_filter` (X4); into a Parquet scan it is masked (row above) | Safe | none yet |
| `WindowTopN` | physical | Disabled: unrated; keeps the first rows of each window partition in a heap | Disabled | `engine::tests::the_rules_rated_disabled_stay_off` |
| `EnsureRequirements` | physical | Safe: adds repartitions and sorts, which evaluate keys their operators demand anyway | Open: repartitioning for joins and aggregation (`repartition_joins`, `repartition_aggregations`) and scans decides the order rows reach `first`, `last` and `string_agg` (X5). Pushing an order requirement through an offset limit is guarded (#339) | `ltseq_keep_sorts_above_offsets`: `test_offset_slice.py` |
| `CombinePartialFinalAggregate` | physical | Safe: merges the two stages of an aggregation over one partition | Safe | |
| `OptimizeAggregateOrder` | physical | Safe: changes the ordering an order-sensitive aggregate asks of its input, not what it evaluates | Open: whether the reversed requirement keeps which row `first` and `last` take among ties | |
| `ProjectionPushdown` | physical | Masked: the `ProjectionPushdown` row above | Safe | |
| `LimitAggregation` | physical | Safe: keeps accumulators only for the groups a `sort` and limit above keep, but evaluates every row's key and argument | Safe: the sort above orders | |
| `LimitPushPastWindows` | physical | Open: a limit below a window leaves its order keys unevaluated on the rows past it | Safe | |
| `HashJoinBuffering` | physical | Disabled: unrated; reads a join's probe side before its build side is complete (`hash_join_buffering_capacity = 0`) | Disabled | `engine::tests::the_rules_rated_disabled_stay_off` |
| `LimitPushdown` | physical | Masked: the `PushDownLimit`, `LimitPushdown` row above | Restricted: it cleared a declared-sorted scan's `preserve_order` (#325) | `ltseq_keep_file_order`: `test_a_limit_reads_the_first_rows_of_declared_parquet` and its `_at_scale` variant |
| `TopKRepartition` | physical | Safe: a TopK below a repartition still evaluates every row's keys | Safe: the TopK above merges in order | |
| `PushdownSort` | physical | Open: a scan that takes a sort may stop early under a limit | Open: a scan may return its rows in an order it only approximates | |
| `EnsureCooperative` | physical | Safe: adds yield points | Safe | |
| `FilterPushdown(Post)` | physical | Restricted: pushes the dynamic filters of TopK, joins and aggregates into scans, which skip rows by them (the pruning row above). The aggregates' filter is off (#253); those of TopK and joins run | Safe | aggregates: `engine::tests::the_rules_rated_disabled_stay_off`; TopK and joins: none yet |
| `SanityCheckPlan` | physical | Safe: changes nothing | Safe | |
| `ltseq_keep_file_order` | physical | Safe: a guard for `LimitPushdown` (#325) | Safe | |

<!-- rule-inventory:end -->

Every rule rated Restricted with "none yet" in its guard column is applied everywhere for now, whether its condition holds or not; their guards are M16's. The rules rated Open for order are M1's (X5).

<!-- v0.5-modular:footer -->

---

Previous: [Decisions on the eleven open semantic issues](decisions.md) · [Index](../README.md) · Next: [Examples, test matrix and implementation impact map (E, F, G)](impact-map.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§1.3]: ../contract/overview.md#13-design-principles
[§3.4]: ../contract/public-surface.md#34-lambdas-and-proxies
[§4]: ../contract/loading-and-laziness.md#4-loading
[§4.1]: ../contract/loading-and-laziness.md#41-source-paths
[§4.2]: ../contract/loading-and-laziness.md#42-ltseqread_csv
[§4.4]: ../contract/loading-and-laziness.md#44-ltseqfrom_arrow
[§4.6]: ../contract/loading-and-laziness.md#46-ltseqfrom_dict-and-ltseqfrom_rows
[§5.3]: ../contract/loading-and-laziness.md#53-eager-calls
[§6.1]: ../contract/schema-and-table-operations.md#61-schema-and-columns
[§7.3]: ../contract/schema-and-table-operations.md#73-derive
[§7.6]: ../contract/schema-and-table-operations.md#76-distinct
[§8.2]: ../contract/expressions.md#82-operators
[§8.5]: ../contract/expressions.md#85-general-expr-methods
[§8.6]: ../contract/expressions.md#86-string-methods-str
[§8.7]: ../contract/expressions.md#87-the-closed-method-set
[§9]: ../contract/ordering.md#9-ordering-contract
[§9.1]: ../contract/ordering.md#91-order-state
[§9.3]: ../contract/ordering.md#93-order-requirements
[§9.5]: ../contract/ordering.md#95-assume_sorted
[§10.1]: ../contract/windows-and-grouping.md#101-window-methods
[§12.1]: ../contract/joins-and-sets.md#121-join
[§13]: ../contract/joins-and-sets.md#13-set-and-bag-operations
[§14.2]: ../contract/aggregation.md#142-aggregate-expressions
[§14.5]: ../contract/aggregation.md#145-partition
[§14.6]: ../contract/aggregation.md#146-pivot
[§16.3]: ../contract/streaming-and-output.md#163-to_dicts
[§16.4]: ../contract/streaming-and-output.md#164-arrow-pycapsule-stream
[§16.5]: ../contract/streaming-and-output.md#165-writers
[§17]: ../contract/numeric-null-temporal.md#17-numeric-semantics-and-literals
[§17.1]: ../contract/numeric-null-temporal.md#171-integer-and-decimal-results-are-checked
[§17.2]: ../contract/numeric-null-temporal.md#172-literals
[§17.3]: ../contract/numeric-null-temporal.md#173-arithmetic-operators
[§17.4]: ../contract/numeric-null-temporal.md#174-types-of-mixed-operands
[§17.5]: ../contract/numeric-null-temporal.md#175-shared-values
[§17.6]: ../contract/numeric-null-temporal.md#176-explicit-casts
[§18]: ../contract/numeric-null-temporal.md#18-null-nan-and-boolean-logic
[§19.2]: ../contract/numeric-null-temporal.md#192-arithmetic-and-comparison
[§20]: ../contract/errors-and-performance.md#20-errors
[§20.2]: ../contract/errors-and-performance.md#202-stages
[§21.1]: ../contract/errors-and-performance.md#211-materialization
[§21.2]: ../contract/errors-and-performance.md#212-fast-paths
[§23.8]: ../contract/examples-sequences.md#238-partitions-across-processes
[§23.10]: ../contract/examples-semantics.md#2310-streaming-and-interchange
[§24.1]: ../contract/acceptance.md#241-acceptance-criteria
[Deliverable G]: impact-map.md#g-implementation-impact-map
[Deliverable K]: history/review-gate.md#k-contract-closure-pass

<!-- /v0.5-modular:links -->
