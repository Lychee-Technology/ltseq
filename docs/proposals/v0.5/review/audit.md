<!-- v0.5-modular:header -->

# v0.5 review: Audit of the current API

[Index](../README.md) › Review · Previous: [Executive verdict and the contract (A, C)](verdict.md) · Next: [API inventory and review (B)](inventory.md)

**Non-normative.** Part of the [review](../README.md#ltseq-v05-api-review) behind the contract. It records evidence, reasoning and decisions; the behavior itself is specified by the contract modules.

**Scope.** Findings for the nine audit areas, from the object model to the error model, each with its evidence at the baseline and its v0.5 resolution.

**Most cited from here.** [§3] Object model · [§8] Expression DSL · [§9] Ordering contract · [§14] Aggregation, partitioning and pivot

<!-- /v0.5-modular:header -->

## Audit of the current API

The audit read all of `docs/api.md` (sections 0–11 and Appendix A) against the code at the baseline, and probed each claim that could be checked by running it. Each finding names its evidence, and ends with the v0.5 resolution and the contract section that states it. [Deliverable B] carries the per-method detail. This section keeps the findings that shaped the design.

### Audit A. Object model

- **A1. Five table types for two roles.** `LTSeq` is the table. `NestedTable` is a table partitioned into consecutive groups, and that role cannot be expressed otherwise. The other types do not have a role of their own:
  - `LinkedTable` (`py-ltseq/ltseq/linking.py:11`) is a join with prefixed names, minus most of `LTSeq`'s methods. It has no `columns`, `to_pandas`, `count` or `agg`.
  - `PartitionedTable` is a mapping of tables. `partition("k")` returns a different class, `SQLPartitionedTable` (`py-ltseq/ltseq/core.py:703`), that is not an instance of it. `map` returns a `_PrecomputedPartitionedTable` whose second `map` raises `AttributeError` (`py-ltseq/ltseq/partitioning.py:390-408`).
  - `Cursor` is a batch iterator.

  **v0.5:** `LTSeq`, `NestedTable` and `GroupBy` (a builder whose only method is `agg`) ([§3.1]).
  - `join(..., alias=)` replaces `link` ([§12.1]).
  - `partition` returns a `dict` from key values to `LTSeq` ([§14.5]).
  - `to_batches()` returns a `pyarrow.RecordBatchReader` ([§15.1]).

  A separate materialized-table type was considered and rejected. `collect()` returns an `LTSeq` over an in-memory snapshot, so lazy and materialized tables compose without conversion ([§5.2]).
- **A2. The public surface is not defined.** The stub `__init__.pyi` exports names the runtime package does not, among them `Cursor`, `gcd`, `lcm`, `factorial`, `char`, `str_char` and `concat_ws`. Expression node classes and `SchemaProxy` are importable and documented in places.

  **v0.5:** `ltseq.__all__` is an exact list. The typing names live in `ltseq.typing`. The compiled module is private. Everything else is removed from the public surface ([§2]).
- **A3. Return kinds are inconsistent.** `count()` returns an `int` on `LTSeq` and an expression on `NestedTable`. `search_first` returns a table or `None` depending on the path. `partition` returns either of two classes.

  **v0.5:**
  - Every table-returning method returns `LTSeq` or a builder.
  - Terminals return Python or Arrow objects.
  - `search_first` always returns an `LTSeq` of zero or one row ([§10.5]).
  - `NestedTable.count()` returns the number of groups ([§3.3]).
- **A4. `LTSeq()` constructs an empty, schema-less table.** It then fails differently in each method: `to_dicts()` returns `[]`, `len()` raises `RuntimeError`, and `filter` raises "Schema not initialized".

  **v0.5:** `LTSeq()` raises `TypeError`, and every table has a schema ([§3.2]).

### Audit B. Naming and minimalism

- **B1. Aliases for one operation.**
  - `concat` and `union` are the same operation, and so are `subtract` and `except_`, `group_consecutive` and `group_ordered`, `with_columns` and `derive`, `nvl` and `coalesce`, and `char` and `str_char`.
  - `ifa(c, v)` is `if_else(c, v, None)`.

  **v0.5:** one name for each. The rule for choosing is the spelling a Python user would guess first, unless it misleads.
  - `concat`, not `union`, because SQL `UNION` deduplicates ([§13.1]).
  - `difference`, not `except_` or `subtract`, after Python's `set.difference`.
  - `derive`, not `with_columns`. It is LTSeq's established verb, and `with_columns` would suggest Polars' semantics, where a lambda is not a row expression.
- **B2. One concept, several keywords.**
  - `desc=` and `descending=`.
  - The join type is `how=` on `join` and `join_type=` on `link`.
  - The other table is `other` or `target_table`.
  - The link alias is `as_=` or `alias=`.
  - `strategy=` means hash versus merge on `join`, and direction on `asof_join`, whose deprecated `direction=` sits third positionally where `join` has `how`.

  **v0.5:** `descending`, `nulls_last`, `how`, `other`, `alias`, `direction`. Options are keyword-only ([§22]).
- **B3. Names that hide what they do.**
  - `rvs` reverses.
  - `contain(key_col, *values)` returns whether every value occurs, testing with Python equality outside the plan (`py-ltseq/ltseq/advanced_ops.py:203-228`).
  - `xunion` is a symmetric difference.
  - `TemporalAccessor.diff(other, unit)` shares a name with the window `Expr.diff`.

  **v0.5:**
  - `reverse`.
  - `contain` and `xunion` are removed. Their compositions are in [§13.2](../contract/joins-and-sets.md#132-intersect-and-difference): `t.filter(lambda r: r.k.is_in(values)).select("k").distinct(keep="any").count() == len(set(values))`, and `a.difference(b).concat(b.difference(a))`.
  - `dt.diff` is removed in favor of subtraction ([§19.4]).
- **B4. Verb classes.** v0.5 keeps the classes distinct by form:
  - predicates return `Expr` and read as questions (`is_null`, `is_in`, `str.startswith`);
  - transforms return `LTSeq`;
  - aggregates are methods on a `Group` proxy;
  - terminals are `to_*`, `write_*`, `count`, `show` and iteration;
  - `collect` is the only verb that returns a materialized `LTSeq`.
- **B5. `read_*` and `scan_*`.** In Polars, `read_*` is eager and `scan_*` is lazy ([`polars.read_parquet`](https://docs.pola.rs/api/python/stable/reference/api/polars.read_parquet.html)). In DuckDB's Python client, `read_*` is lazy ([DuckDB Python overview](https://duckdb.org/docs/current/clients/python/overview.html)). LTSeq's `read_*` already builds a lazy plan, and its `scan*` returns a `Cursor`, which matches neither library.

  **v0.5:** only `read_csv` and `read_parquet`, both lazy, with streaming through `to_batches()`. Two verbs that differ only in laziness would invite a reader of Polars code to pick the wrong one, and with every table lazy the distinction has no meaning ([§4]).

### Audit C. Expression DSL

- **C1. Lambdas versus `col()`.** The lambda form gives attribute completion over the schema and reads as Python.

  **v0.5:** lambdas only. `col()` is not added: a second spelling for every column reference would double the surface for no new capability. Column names are positional `str` arguments where a method takes columns (`select("a", "b")`, `sort("ts")`), and expressions are lambdas ([§3.4], [§7]).
- **C2. Typos fail late.** `Expr.__getattr__` (`py-ltseq/ltseq/expr/types.py:77`) turns any attribute into a call. `r.x.str.lenght()` therefore fails in `derive` with "Method 'lenght' not yet supported". `r.x.shift(offset=3)` is accepted and the kernel uses lag 1 (audit I5).

  **v0.5:** the method set is closed and listed. An unknown method raises `AttributeError` when the lambda runs, and an unknown keyword raises `TypeError` ([§8.7]).
- **C3. `and`, `or`, `not`.** `Expr.__bool__` already raises (`py-ltseq/ltseq/expr/base.py:681`), and `== None` already means `is_null()` (`base.py:621-636`).

  **v0.5:** both are kept and made normative ([§3.4], [§8.2]), with Kleene logic for `&`, `|` and `~` ([§18]).
- **C4. Calling conventions differ by method.**
  - `LTSeq.derive` takes keywords or a lambda returning a dict.
  - `NestedTable.derive` and `NestedTable.agg` take only the dict lambda.
  - `group_by().agg` takes only keywords.
  - Aggregates are `g.sum("col")` in one proxy and `g.col.sum()` in the other.

  **v0.5:** `name=lambda` keywords everywhere, and `g.col.sum()` in every aggregate context ([§7.3], [§14.2]).
- **C5. Window composition.** Only a few scalar functions can wrap a window call. The rest fail with an error that blames "another window function's result" (`src/ops/window.rs:79`).

  **v0.5:** a window method takes any row expression as input, and its result composes with every operator and function. Only an aggregate may not contain one ([§10.1]).
- **C6. String and temporal namespaces.**
  - The string accessor is `.s`, where pandas and Polars use `.str`.
  - `find` (0-based, -1 when absent) and `pos` (1-based, 0 when absent) answer the same question.
  - `split` and `split_part` overlap (`py-ltseq/ltseq/expr/accessors.py:138, 158`).
  - The docstring refers to a nonexistent `add_days` (`accessors.py:323`).

  **v0.5:** `.str` with Python-equivalent semantics and one method per question ([§8.6]), and a reduced `.dt` ([§19.3]–[§19.4]).

### Audit D. Ordered sequence semantics

- **D1. Unsorted multi-partition input has no stable order.**
  - The session uses `target_partitions = NUM_CPUS` (`src/engine.rs:43`), and terminals merge partitions as they finish.
  - In probes, these lost their order: `from_arrow` of a 200-batch table, `read_parquet` of a 1.16 MB file, `read_csv` of a 27 MB file, and a directory read.
  - `head(3)` changed between runs.
  - `snapshot_single_partition` (`src/ops/set_ops.rs:118`), which ADR 0005 treats as order-correct, collects that same merged stream. `reverse`, `step` and keyed `distinct` are therefore not order-correct either.

  **v0.5:** readers define file-then-row order. Every terminal delivers it through an order-preserving merge ([§4.1], [§9.1], [§21.1]; #148).
- **D2. Windows reorder rows (#202).** This covers `.over(order_by=)`, `.over(partition_by=)` and the `partition_by=` keyword, for shift, cumulative, rolling and ranking functions. `sort_keys` keeps the earlier order (`src/ops/common.rs:206` only truncates on overwrite), and `head(3)` after `partition_by` returned `[2, 4, 6]`.

  **v0.5:** windows never move rows ([§10.1]).
- **D3. `assume_sorted` declares an order the rows lack.** On non-Parquet input, `to_dicts()` came out of order in 15 of 15 runs. `assume_sorted("zz")` accepts a nonexistent column. The code's own comment says incorrect metadata "will produce wrong results" (`src/ops/sort.rs:105-108`).

  **v0.5:** the declaration is checked on every execution, across batch, file and partition boundaries, and raises `OrderViolationError` ([§9.5]).
- **D4. Ties and NULLs.**
  - `sort` is unstable even on single-batch input.
  - NULL sorts first when descending, with no option to change that.
  - NaN placement is undocumented.

  **v0.5:**
  - `sort` is stable.
  - `nulls_last=True` is the default in both directions, and a keyword can change it.
  - NaN sorts above `+inf` ([§9.4], [§18]).
- **D5. Undocumented order loss.**
  - `concat`, `intersect`, set differences, `join`, `asof_join`, `lookup`, `group_by().agg`, `pivot`, and `NestedTable.first`, `last` and `flatten` all lose order or `sort_keys` without saying so.
  - `rvs` and `step` clear `sort_keys` instead of flipping or keeping them.

  **v0.5:** every operation's effect on order is tabulated ([§9.2]), and each of these rows has a property test (P7).
- **D6. Tiers of order requirement.** Today `shift` and friends check `sort_keys`, but `head`, `tail` and `slice` accept any table and return arbitrary rows.

  **v0.5:**
  - The *positional* tier (`tail`, `slice`, `step`, `reverse`, `with_row_index`, `search_first`, …) needs a defined order.
  - The *sequence* tier (windows without `order_by`, `group_ordered`, `search_pattern`, `fold`) needs declared keys.
  - Both are checked at plan time ([§9.3]).
  - `head` and `show` work on any table, for inspection. Ranking functions and `.over(order_by=)` carry their own order.
- **D7. `group_ordered` regroups after `filter`.** `NestedTable.filter` rebuilds groups by re-running the grouping over the surviving rows (`py-ltseq/ltseq/grouping/nested_table.py:170-173`). Two kept groups that become adjacent and share a key merge into one, which silently changes the groups that a chained `filter`, `agg` or `first` sees.

  **v0.5:** group identity is fixed when `group_ordered` runs, and `filter` keeps whole groups with their identity ([§11.2], test G2).

### Audit E. Join and set semantics

- **E1. Set operations mix three families.** `concat` is UNION ALL, `intersect` is DISTINCT, and `except_` keeps duplicates. All except `contain` treat NULL as never equal (`semi_anti_on_keys`, `src/ops/set_ops.rs:367`). So `a ∩ a` drops NULL rows, `a − a` keeps them, and `a ⊆ a` is False.

  **v0.5:**
  - `concat` is a bag union.
  - `intersect` and `difference` take `distinct=`, with set semantics by default, as SQL's `INTERSECT` and `EXCEPT` have without `ALL`.
  - NULL equals NULL in set operations, grouping and `distinct`, but never in join keys ([§13], [§18]).
- **E2. Silent coercion at table boundaries.**
  - `concat` of `int64` and `string` stringifies the numbers.
  - Join keys `int64` and `string` match `"1"` to `1`.
  - `contain` treats `1 == 1.0 == True`.

  **v0.5:** set operations need identical schemas (#222). Each join key pair must be in one type class: string with number raises `LTSeqTypeError` at plan time, and so does integer with float ([§12.1]).
- **E3. As-of join reduces times to raw integers.** Floats truncate, NULL becomes `i64::MIN` and so matches as the earliest time, and timestamp units are compared as raw counts.

  **v0.5:** the as-of keys are compared as values of one type class, with units compared exactly. NULL and NaN keys never match. Neither input needs to be sorted. The right input needs a defined order, because its order breaks ties ([§12.3]).
- **E4. `lookup` is a join that hides it is one.** It fans out on duplicate keys and leaks target columns under `id()`-based names. A second lookup on the same target reuses the first join.

  **v0.5:** removed. `join(..., how="left", validate="m:1")` states the same intent and raises `DuplicateKeyError` on fan-out ([§12.1]).
- **E5. Join order.** Left order held in one probe and is not documented.

  **v0.5:** `inner`, `left`, `cross`, `semi`, `anti` and as-of joins keep left order. `right` and `full` are unordered ([§9.2]).
- **E6. Keys and suffixes.** `on` accepts a string, a list or a lambda depending on the method, and `left_on`/`right_on` exist only on `join`.

  **v0.5:** one form for `join`, `semi_join`, `anti_join` and `asof_join`: column names in `on=`, or in `left_on=` and `right_on=`, with `suffix="_right"` for name collisions ([§12.1]). A key is a column name, so a computed key is `derive`d first, and a non-equi condition is a `cross` join followed by `filter`.

### Audit F. Aggregation and grouping

- **F1. Three groupings that look alike.** `group_by` is relational, `group_ordered` groups consecutive runs, and `group_sorted` groups a sorted table. Only the first two have different results: `group_sorted` is `group_ordered` on sorted input.

  **v0.5:** `group_by` (unordered, keys) and `group_ordered` (consecutive, sequence tier). `group_sorted` is removed, and `sort(k).group_ordered(k)` replaces it ([§11], [§14.1]).
- **F2. Aggregate defects.**
  - `sum_if` with no match gives 0, where every sibling gives NULL.
  - `top_k` counts NULLs toward k.
  - `mode` returns the minimum.
  - `skew` is the population skew, undocumented.
  - `percentile` is approximate and visibly wrong on small groups: p90 of `[1, 2, 3, 4, 10]` is 10, where linear interpolation gives 7.6.

  **v0.5:**
  - Conditional aggregation is one keyword, `where=`, on every aggregate. With no rows taking part, `sum` is NULL whether or not `where=` is used.
  - `quantile` is exact, with linear interpolation (NumPy's default method), and `percentile` is removed.
  - `mode` breaks ties by the smallest value, stated.
  - `top_k` and `skew` are removed ([§14.2]).
- **F3. `pivot` and `partition` round-trip keys through SQL text.**
  - `pivot` re-parses `"01"` and `"1"` as the same number, orders numeric columns lexicographically, and drops NULL.
  - `partition` keys of type `date` give empty tables, and timestamp and NaN keys raise `KeyError`.

  **v0.5:**
  - Keys are compared as values under [§18].
  - `pivot` takes its column values from `column_values=`, or else from the data at the call, an eager step listed in [§5.3]. Output columns are ordered by value, not by text ([§14.6]).
  - `partition` returns a `dict` keyed by the Python value, or by a tuple of values for several keys. A NaN key raises at the call, because a `dict` cannot look it up ([§14.5]).
- **F4. `fold`** is the documented row-wise Python exception to the no-materialization rule.

  **v0.5:** kept, sequence tier, with its cost stated ([§10.7]).

### Audit G. IO, interchange and streaming

- **G1. Missing sources read as empty.** `read_csv` and `read_parquet` on a missing path or an empty glob return an empty table.

  **v0.5:** `SourceNotFoundError` at the call ([§4.1]).
- **G2. `requested_schema` is ignored for types.** The stream always carries the table's own types (`src/ops/io.rs:203-215`). Only names are checked (`src/arrow_ffi.rs:175`). A consumer that asks for `int32` silently receives `int64`.

  **v0.5:** names must match, and types convert by the rules of `cast` without rounding or truncating, or raise ([§16.4]).
- **G3. Empty tables.**
  - `write_parquet` of 0 rows raises.
  - `write_csv` of 0 rows writes no header.
  - A header-only CSV gives `null`-typed columns.

  **v0.5:** 0-row files carry the schema, and a header-only CSV reads as `string` columns unless `schema=` says otherwise ([§4.2], [§16.5]).
- **G4. Cursor.** Batches came in nondeterministic order (D1). `schema` reports Rust debug names (`"Int64"`). `to_arrow()` on an exhausted cursor loses the schema. `iter(t)`'s docstring points to a `to_cursor()` that does not exist (`py-ltseq/ltseq/core.py:205`).

  **v0.5:** `to_batches()` returns a standard `pyarrow.RecordBatchReader` that yields batches in the defined order and stops the execution when closed ([§15.1]).
- **G5. Pickle** is undefined (#156).

  **v0.5:** an Arrow IPC snapshot with order state, for the same minor version only ([§16.6]).

### Audit H. Numeric, NULL and temporal semantics

[Deliverable D] decides the eleven issues. The audit found these beyond them.

- **H1. Domain errors are inconsistent.**
  - `sqrt(-1)` raises, while `log(-1)` gives NaN.
  - `factorial(21)` raises on overflow, while multiplication wraps.
  - `power(0.0, -1)` raises, while float `/ 0` gives `inf`.

  **v0.5:** the math functions are IEEE float functions and never raise for domain errors ([§8.4]). Operators with an integer or decimal result are checked, integer `**` included; `**` with a decimal operand is `float64` ([§17.1], [§17.3]). `factorial` and `power` are removed.
- **H2. Result types drift.**
  - `weekday` and `millisecond` are `double`, though `weekday` is documented as an integer.
  - `floor` and `ceil` on `int64` give `double`.
  - `dt.diff` is `double` for days and `int32` for years.

  **v0.5:** every result type is stated: `floor` and `ceil` keep the type ([§8.5]), and every `.dt` field is `int32` ([§19.3]).
- **H3. NULL in predicates splits the mutation APIs.** `delete(pred)` removes rows where `pred` is NULL, and `update(pred, ...)` leaves them.

  **v0.5:** both act only on rows where the predicate is TRUE. A row where it is NULL is kept by `delete` and left unchanged by `update`, so `delete(p)` is not `filter(~p)` ([§7.10]).
- **H4. Windows and NULL.** `cum_sum` carries the running total through a NULL row. `pct_change` on integers truncates (2 to 3 gives 0). `rolling(n).mean()` returns partial windows.

  **v0.5:**
  - NULL input gives NULL output at that row, and the total continues after it.
  - `pct_change` gives `float64`.
  - `rolling` takes `min_periods`, which defaults to the window size ([§10.1]).
- **H5. pandas mappings.** Appendix A maps pandas `isna` and `fillna` to `is_null` and `fill_null`, which do not treat NaN as missing.

  **v0.5:** NaN is a value, not a missing value. The contract says where pandas converts between the two ([§18]).
- **H6. Clock functions.** `now()` is a naive `timestamp[ns]` in UTC. `today()` is the UTC date.

  **v0.5:** `now()` is aware UTC, and both are fixed for one execution ([§19.5]).
- **H7. Strings are not Python-equivalent despite the claim.**
  - `strip` trims spaces only.
  - `isalpha` is ASCII-only.
  - `islower` is True on strings with no cased character.

  **v0.5:** `.str` methods equal Python's `str` methods on every input, where the method exists in both ([§8.6]).

### Audit I. Error model

- **I1. The hierarchy is not kept.** `src/error.rs:88-90` maps most kernel errors to plain `ValueError`, `RuntimeError` or `TypeError`. Only `ColumnNotFoundError`, `SortRequiredError` and `SchemaMismatchError` are `LTSeqError`. pyarrow's `ArrowInvalid` and `ArrowTypeError` leak through `pa.table(t)` and `fold`.

  **v0.5:** fifteen classes, each a subclass of the built-in a Python user would catch ([§20.1]).
- **I2. "Column not found" takes five forms.**
  - `ColumnNotFoundError` in `filter` and `derive`.
  - `AttributeError` in `select("x")`.
  - `KeyError` in `rename` and `drop`.
  - `ValueError` in `sort` and `distinct`.
  - No error in `is_sorted_by` and `assume_sorted`.

  **v0.5:** `ColumnNotFoundError` at plan time everywhere, with close matches in the message ([§3.4], [§20.1]).
- **I3. Errors depend on the evaluator.** `filter` wraps on `int64` overflow, while `search_pattern` raises, for the same predicate. The linear-scan path wraps on subtraction (`src/ops/linear_scan.rs:420, 825`) and raises on addition (`linear_scan.rs:854`). Its errors are swallowed by `except Exception` (`py-ltseq/ltseq/grouping/nested_table.py:390`), which falls back to another path.

  **v0.5:** every path returns the same result, or raises an error the general path may raise ([§20.2], [§21.2]).
- **I4. Validation is late.**
  - `sort()` with no keys, `ntile(0)`, and a ranking function on an unsorted table are accepted, then fail at collect with DataFusion's internal text.

  **v0.5:** these are Plan-stage errors ([§20.2]).
- **I5. Arguments are silently ignored.** These produce plausible wrong results instead of errors, which makes them the most harmful class:
  - `shift(offset=k)`, `diff(periods=k)` and unknown keywords on window methods use the defaults;
  - `from_rows(schema=)` with non-empty rows is ignored;
  - unknown type names become `string`.

  **v0.5:** a closed signature for every callable ([§22]), so these raise `TypeError` or `LTSeqTypeError` at plan time.
- **I6. Stages.** The documentation's per-method exception lists say `RuntimeError` for most execution failures. In practice `to_dicts`, `to_arrow`, `show` and `write_csv` raise `ValueError`, `collect` and `write_parquet` raise `RuntimeError`, and `count()` on the same failing plan raises nothing (`src/lib.rs:417`).

  **v0.5:** three stages (Plan, Call, Execution), and a demand rule that fixes which values a terminal evaluates ([§20.2]). Under it, `count()` raises if and only if a value that decides which rows exist, or their order, fails.

<!-- v0.5-modular:footer -->

---

Previous: [Executive verdict and the contract (A, C)](verdict.md) · [Index](../README.md) · Next: [API inventory and review (B)](inventory.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§2]: ../contract/public-surface.md#2-exports
[§3]: ../contract/public-surface.md#3-object-model
[§3.1]: ../contract/public-surface.md#31-types
[§3.2]: ../contract/public-surface.md#32-ltseq-invariants
[§3.3]: ../contract/public-surface.md#33-nestedtable-and-groupby-invariants
[§3.4]: ../contract/public-surface.md#34-lambdas-and-proxies
[§4]: ../contract/loading-and-laziness.md#4-loading
[§4.1]: ../contract/loading-and-laziness.md#41-source-paths
[§4.2]: ../contract/loading-and-laziness.md#42-ltseqread_csv
[§5.2]: ../contract/loading-and-laziness.md#52-ltseqcollect
[§5.3]: ../contract/loading-and-laziness.md#53-eager-calls
[§7]: ../contract/schema-and-table-operations.md#7-basic-table-operations
[§7.3]: ../contract/schema-and-table-operations.md#73-derive
[§7.10]: ../contract/schema-and-table-operations.md#710-value-level-edits-insert-delete-update
[§8]: ../contract/expressions.md#8-expression-dsl
[§8.2]: ../contract/expressions.md#82-operators
[§8.4]: ../contract/expressions.md#84-math-functions
[§8.5]: ../contract/expressions.md#85-general-expr-methods
[§8.6]: ../contract/expressions.md#86-string-methods-str
[§8.7]: ../contract/expressions.md#87-the-closed-method-set
[§9]: ../contract/ordering.md#9-ordering-contract
[§9.1]: ../contract/ordering.md#91-order-state
[§9.2]: ../contract/ordering.md#92-sources-and-propagation
[§9.3]: ../contract/ordering.md#93-order-requirements
[§9.4]: ../contract/ordering.md#94-sort
[§9.5]: ../contract/ordering.md#95-assume_sorted
[§10.1]: ../contract/windows-and-grouping.md#101-window-methods
[§10.5]: ../contract/windows-and-grouping.md#105-search_first
[§10.7]: ../contract/windows-and-grouping.md#107-fold
[§11]: ../contract/windows-and-grouping.md#11-ordered-grouping
[§11.2]: ../contract/windows-and-grouping.md#112-nestedtable
[§12.1]: ../contract/joins-and-sets.md#121-join
[§12.3]: ../contract/joins-and-sets.md#123-asof_join
[§13]: ../contract/joins-and-sets.md#13-set-and-bag-operations
[§13.1]: ../contract/joins-and-sets.md#131-concat
[§14]: ../contract/aggregation.md#14-aggregation-partitioning-and-pivot
[§14.1]: ../contract/aggregation.md#141-group_by-and-groupbyagg
[§14.2]: ../contract/aggregation.md#142-aggregate-expressions
[§14.5]: ../contract/aggregation.md#145-partition
[§14.6]: ../contract/aggregation.md#146-pivot
[§15.1]: ../contract/streaming-and-output.md#151-to_batches
[§16.4]: ../contract/streaming-and-output.md#164-arrow-pycapsule-stream
[§16.5]: ../contract/streaming-and-output.md#165-writers
[§16.6]: ../contract/streaming-and-output.md#166-pickle
[§17.1]: ../contract/numeric-null-temporal.md#171-integer-and-decimal-results-are-checked
[§17.3]: ../contract/numeric-null-temporal.md#173-arithmetic-operators
[§18]: ../contract/numeric-null-temporal.md#18-null-nan-and-boolean-logic
[§19.3]: ../contract/numeric-null-temporal.md#193-dt-fields
[§19.4]: ../contract/numeric-null-temporal.md#194-dt-methods
[§19.5]: ../contract/numeric-null-temporal.md#195-clock-functions
[§20.1]: ../contract/errors-and-performance.md#201-exception-classes
[§20.2]: ../contract/errors-and-performance.md#202-stages
[§21.1]: ../contract/errors-and-performance.md#211-materialization
[§21.2]: ../contract/errors-and-performance.md#212-fast-paths
[§22]: ../contract/api-reference.md#22-complete-canonical-api-reference
[Deliverable B]: inventory.md#b-api-inventory-and-review
[Deliverable D]: decisions.md#d-decisions-on-the-eleven-open-semantic-issues

<!-- /v0.5-modular:links -->
