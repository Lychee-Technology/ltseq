<!-- v0.5-modular:header -->

# v0.5 review: API inventory and review (B)

[Index](../README.md) › Review · Previous: [Audit of the current API](audit.md) · Next: [Decisions on the eleven open semantic issues](decisions.md)

**Non-normative.** Part of the [review](../README.md#ltseq-v05-api-review) behind the contract. It records evidence, reasoning and decisions; the behavior itself is specified by the contract modules.

**Scope.** Every public name at the baseline and every name v0.5 adds, with its v0.5 action and the contract section that specifies the result, then the gaps found while building the inventory and its counts. Kept whole as one reference table.

**Most cited from here.** [§8] Expression DSL · [§14] Aggregation, partitioning and pivot · [§10] Windows and ordered computation · [§9] Ordering contract

<!-- /v0.5-modular:header -->

## B. API inventory and review

This inventory covers every public name in `docs/api.md` and in the package exports (`ltseq.__all__` and the shipped `.pyi` stubs) at baseline commit 3041b44, plus every name v0.5 adds. Parameters the contract changes on their own get their own rows. "Final API" gives the v0.5 spelling and the section of `docs/proposals/v0.5/contract/` that specifies it. Contract sections are cited rather than line numbers. A `—` in that column marks a removal, which per [§24.1] raises `AttributeError` or `ImportError`. KEEP means the name, the signature and the normal-case behavior do not change, though error classes and tier checks may. RENAME is the same behavior under a new name, REDESIGN changes the behavior or the signature, MERGE folds an alias or variant into an existing API, and REMOVE leaves no single successor, so its rationale gives the composition that replaces it. ADD appears only in the last table, which does not repeat successors already reached by RENAME, REDESIGN or MERGE.

### I/O

| Current API | Problem / Strength | v0.5 Action | Final API | Rationale |
|---|---|---|---|---|
| `LTSeq` (class) | One user-facing table type assembled from mixins (py-ltseq/ltseq/core.py:42). | KEEP | `LTSeq` ([§3.1]) | Stays the only table type; `LinkedTable` and `PartitionedTable` are absorbed into it ([§3.1]). |
| `LTSeq()` | A bare constructor yields an empty object that other methods must fill (py-ltseq/ltseq/core.py:53). | REMOVE | — | [§3.2] makes `LTSeq()` raise `TypeError`. Build tables with `read_csv`, `read_parquet`, `from_*` or `LTSeq.range`. |
| `LTSeq.read_csv(path, has_header=True)` | Takes one `str` path (py-ltseq/ltseq/io_ops.py:42). Headerless columns are named `column_0, column_1, …` (docs/api.md:153-156). | REDESIGN | `LTSeq.read_csv()` ([§4.2]) | Adds directories, globs, path lists, declared types and keyword-only options. Headerless names become 1-based `column_1…`, so existing code that reads `column_1` silently gets a different column. |
| `LTSeq.read_parquet(path)` | Takes one `str` path (py-ltseq/ltseq/io_ops.py:84). | REDESIGN | `LTSeq.read_parquet()` ([§4.3]) | Accepts directories, globs and path lists in a fixed file order, and checks at the call that every file has the same schema. `sort_keys` stays `None` even when the file records `sorting_columns`. |
| `LTSeq.from_rows(rows, schema=None)` | Columns come from the first row only, and declared types are ignored for non-empty input (py-ltseq/ltseq/io_ops.py:197, :266, :275). | REDESIGN | `LTSeq.from_rows()` ([§4.6]) | Columns become the union of keys in order of first appearance. A declared type must hold every value exactly, otherwise `CastError`. |
| `LTSeq.from_dict(data)` | Column-oriented constructor matching pandas and Polars (py-ltseq/ltseq/io_ops.py:201). | KEEP | `LTSeq.from_dict()` ([§4.6]) | Same call. `schema=` is added (Added table). |
| `LTSeq.from_arrow(arrow_table)` | Zero-copy entry from Arrow (py-ltseq/ltseq/io_ops.py:318). | KEEP | `LTSeq.from_arrow()` ([§4.4]) | Same call, for tabular data only: a schema that is a struct (F14). Tables, record batches, readers and struct `ChunkedArray`s are accepted as at the baseline. A `StructArray`, and any producer with only `__arrow_c_array__` and a struct schema, are newly accepted; the baseline raises `TypeError`. A bare array raises `LTSeqTypeError`. |
| `LTSeq.from_pandas(df)` | Uses pyarrow's default index handling (py-ltseq/ltseq/io_ops.py:386), so a non-range index becomes a column. | REDESIGN | `LTSeq.from_pandas()` ([§4.5]) | `preserve_index` defaults to `False`. An index that used to arrive as a column is now dropped unless the caller opts in. |
| `seq(start_or_stop, stop=None, step=1)` | A module-level function that builds every value in Python before planning (py-ltseq/ltseq/utils.py:9, :41-43). | RENAME | `LTSeq.range()` ([§4.7]) | Same `range()` semantics and `value` column, now a lazy classmethod that also declares its sort key. [§8.4] names it as the successor of `seq`. |
| `LTSeq.write_csv(path)` | Plain CSV writer (py-ltseq/ltseq/io_ops.py:282). | KEEP | `LTSeq.write_csv()` ([§16.5]) | Same call. It gains atomic replace and canonical text forms. |
| `LTSeq.write_parquet(path, compression=None)` | Writes uncompressed by default (docs/api.md:258-261). | REDESIGN | `LTSeq.write_parquet()` ([§16.5]) | The default becomes `zstd`, and `compression` becomes keyword-only with a closed set of values. Row groups record `sort_keys`. |
| `write_parquet(compression="zstandard"/"gz")` aliases | Undocumented aliases (src/ops/io.rs:93-94). | REMOVE | — | [§16.5]'s closed list has no aliases. Write `"zstd"` or `"gzip"`. |
| `Compression` (stub alias) | A typing alias that exists only in the stub (py-ltseq/ltseq/__init__.pyi:10). | REMOVE | — | [§2.2] limits typing names to `ltseq.typing`. Annotate with `typing.Literal["zstd", "snappy", "gzip", "lz4", "none"]`. |
| `LTSeq.schema` | Returns a `dict[str, str]` of type names (py-ltseq/ltseq/core.py:398-399). | REDESIGN | `LTSeq.schema` ([§6.1]) | Becomes the exact `pa.Schema`, equal to `to_arrow().schema`. Code that compares values to type-name strings has to change. |
| `LTSeq.columns` | Names without executing (py-ltseq/ltseq/core.py:433-434). | KEEP | `LTSeq.columns` ([§6.1]) | Unchanged. |
| `LTSeq.python_schema` | A second schema view (py-ltseq/ltseq/core.py:412-413). | REMOVE | — | [§22] is closed and omits it. Use `{f.name: f.type for f in t.schema}`. |
| `LTSeq.dtypes` | A third schema view as `list[tuple[str, str]]` (py-ltseq/ltseq/core.py:447-448). | REMOVE | — | Omitted from [§22]. Use `[(f.name, str(f.type)) for f in t.schema]`. |
| `LTSeq.collect()` | Snapshot API (py-ltseq/ltseq/core.py:345). | KEEP | `LTSeq.collect()` ([§5.2]) | Unchanged. |
| `LTSeq.to_arrow()` | Arrow export (py-ltseq/ltseq/core.py:254). | KEEP | `LTSeq.to_arrow()` ([§16.1]) | Unchanged. The schema now equals `t.schema` exactly. |
| `LTSeq.to_pandas()` | Converts with `pa.Table.to_pandas()` defaults (py-ltseq/ltseq/core.py:252), so NaN and NULL collapse in NumPy dtypes. | REDESIGN | `LTSeq.to_pandas()` ([§16.2]) | The default dtype backend is `pyarrow`, so NULL and NaN stay distinct. Dtype-sensitive callers see different column dtypes. |
| `LTSeq.to_dicts()` | Row export (py-ltseq/ltseq/core.py:372). | KEEP | `LTSeq.to_dicts()` ([§16.3]) | Same output. Nanosecond values that Python types cannot hold now raise `CastError`. |
| `LTSeq.count()` | Row count (py-ltseq/ltseq/core.py:328). | KEEP | `LTSeq.count()` ([§7.9]) | Unchanged. |
| `len(t)` / `LTSeq.__len__` | Same as `count()` (py-ltseq/ltseq/core.py:315). | KEEP | `len(t)` ([§3.2]) | Unchanged. |
| `iter(t)` / `LTSeq.__iter__` | Materializes the whole table through `to_dicts()` (py-ltseq/ltseq/core.py:200-207). | KEEP | `iter(t)` ([§15.2]) | Still yields one dict per row, but now streams through `to_batches()`. |
| `LTSeq.__arrow_c_stream__` | `requested_schema` is validated but not honored (docs/api.md:353-364). | REDESIGN | `LTSeq.__arrow_c_stream__` ([§16.4]) | The requested schema is honored with exact conversions, or refused. |
| `to_cursor()` | Referenced (docs/api.md:331, py-ltseq/ltseq/core.py:205) but defined nowhere. | REMOVE | — | A stale reference. Streaming is `t.to_batches()` ([§15.1]). |
| `repr(t)` / `LTSeq.__repr__` | Executes the plan to print a 5-row preview (py-ltseq/ltseq/core.py:142-170; docs/api.md:378). | REDESIGN | `repr(t)` ([§3.2]) | Shows names, types and order state, and never executes. |
| `LTSeq._repr_html_` | A Jupyter hook that executes the plan (py-ltseq/ltseq/core.py:178-198). | REMOVE | — | [§3.2] defines no display hook, so notebooks show `repr` and displaying a table never executes. Use `t.show()` or `t.head().to_pandas()`. |
| `LTSeq.show(n=10)` | Returns `self` and collects the whole table to print `n` rows (py-ltseq/ltseq/core.py:121-140; src/lib.rs:318-329). | REDESIGN | `LTSeq.show()` ([§7.9]) | Returns `None` and executes only `head(n)`. A chain such as `t.show().filter(...)` breaks. |

### Basic relational ops

| Current API | Problem / Strength | v0.5 Action | Final API | Rationale |
|---|---|---|---|---|
| `LTSeq.filter(predicate)` | Lazy row filter (py-ltseq/ltseq/transforms.py:196). | KEEP | `LTSeq.filter()` ([§7.1]) | Unchanged. A non-`bool` predicate now raises `LTSeqTypeError`. |
| `LTSeq.select(*cols)` | Mixes names and lambdas positionally (py-ltseq/ltseq/transforms.py:232; docs/api.md:436-447). | REDESIGN | `LTSeq.select()` ([§7.2]) | Positional arguments must be names and computed columns become keywords. The longest valid `sort_keys` prefix is kept. |
| `select(lambda r: [...])` list form | A second way to compute columns, with no names (docs/api.md:446). | MERGE | `LTSeq.select(**named)` ([§7.2]) | One spelling for computed columns. |
| `LTSeq.derive(**kwargs)` | Keyword form (py-ltseq/ltseq/transforms.py:275). | KEEP | `LTSeq.derive()` ([§7.3]) | Unchanged. Every expression reads the input row. |
| `derive(lambda r: {...})` dict form | A positional lambda that returns a dict (docs/api.md:449-461). | MERGE | `LTSeq.derive(**named)` ([§7.3]) | [§7.3] removes the dict form. |
| `LTSeq.with_columns` | Polars-style alias of `derive` (py-ltseq/ltseq/transforms.py:350). | MERGE | `LTSeq.derive()` ([§7.3]) | [§7.3] removes the alias. |
| `LTSeq.rename(mapping=None, **kwargs)` | Simultaneous renames (py-ltseq/ltseq/transforms.py:352). | KEEP | `LTSeq.rename()` ([§7.4]) | Unchanged. |
| `LTSeq.drop(*cols)` | Column removal (py-ltseq/ltseq/transforms.py:406). | KEEP | `LTSeq.drop()` ([§7.5]) | Unchanged. |
| `LTSeq.distinct(*key_exprs)` | Row or key deduplication (py-ltseq/ltseq/transforms.py:773). | KEEP | `LTSeq.distinct()` ([§7.6]) | Keeps the first row per key, and with `keep="first"` it is now in the positional tier. `keep=` is added (Added table). |
| `distinct(lambda r: ...)` keys | Computed keys (py-ltseq/ltseq/transforms.py:773). | REMOVE | — | [§7.6] accepts names only. Use `t.derive(k=...).distinct("k").drop("k")`. |
| `LTSeq.head(n=10)` | First rows (py-ltseq/ltseq/transforms.py:829). | KEEP | `LTSeq.head()` ([§9.8]) | Unchanged. Works on any order. |
| `LTSeq.tail(n=10)` | Last rows (py-ltseq/ltseq/transforms.py:849). | KEEP | `LTSeq.tail()` ([§9.8]) | Unchanged. Positional tier. |
| `LTSeq.slice(offset=0, length=None)` | Row range (py-ltseq/ltseq/transforms.py:799). | KEEP | `LTSeq.slice()` ([§9.8]) | Unchanged. Negative values raise `LTSeqValueError`. |
| `LTSeq.pipe(func, *args, **kwargs)` | Method-chaining helper (py-ltseq/ltseq/core.py:209). | KEEP | `LTSeq.pipe()` ([§7.7]) | Unchanged. |
| `LTSeq.explain_plan()` | Returns a `tuple[str, str]` (py-ltseq/ltseq/core.py:341). | REDESIGN | `LTSeq.explain()` ([§7.8]) | Returns one string, with the physical plan added only on request. [§7.8] removes `explain_plan`. |

### Windows and ordered ops

| Current API | Problem / Strength | v0.5 Action | Final API | Rationale |
|---|---|---|---|---|
| `LTSeq.sort(*keys, desc=, descending=)` | Sorts NULL as the largest value and accepts computed keys (py-ltseq/ltseq/transforms.py:539; docs/api.md:485-498). | REDESIGN | `LTSeq.sort()` ([§9.4]) | NULLs go last in both directions by default, so a v0.4 descending sort that put NULLs first now puts them last. [§9.4] acknowledges this. |
| `sort(desc=)` | `desc` and `descending` are aliases, and `descending` wins when both are given (docs/api.md:486-487). | MERGE | `sort(descending=)` ([§9.4]) | One spelling. |
| `sort(lambda r: ...)` keys | Computed sort keys (docs/api.md:491). | REMOVE | — | [§9.4] accepts names only. Use `t.derive(k=...).sort("k")`. |
| `LTSeq.sort_keys` | A `list[tuple[str, bool]]` (py-ltseq/ltseq/core.py:461-462). | REDESIGN | `LTSeq.sort_keys` ([§9.7]) | Becomes a tuple of `SortKey(column, descending, nulls_last)`. |
| `LTSeq.is_sorted_by(*keys, desc=False)` | Checks declared metadata by prefix and never looks at data (py-ltseq/ltseq/core.py:479-505). | REDESIGN | `LTSeq.is_sorted_by()` ([§9.6]) | Executes and checks the actual rows. Use `sort_keys` for the metadata question. |
| `is_sorted_by(desc=)` | The only spelling of direction here (py-ltseq/ltseq/core.py:479). | RENAME | `is_sorted_by(descending=)` ([§9.6]) | Same parameter, aligned with `sort`. |
| `LTSeq.assume_sorted(*keys, desc=False)` | Unvalidated: "wrong metadata produces wrong results" (docs/api.md:531-541). | REDESIGN | `LTSeq.assume_sorted()` ([§9.5]) | Every pair of rows read is validated, and a violation raises `OrderViolationError`. |
| `assume_sorted(desc=)` | The only spelling of direction here (py-ltseq/ltseq/transforms.py:596-600). | RENAME | `assume_sorted(descending=)` ([§9.5]) | Same parameter, aligned with `sort`. |
| `assume_sorted(lambda r: ...)` keys | Computed keys are accepted and then truncate the metadata (docs/api.md:533). | REMOVE | — | [§9.5] accepts names only. Derive the key column, then call `assume_sorted("k")`. |
| `r.col.shift(n, default=None, partition_by=None)` | Lag and lead (py-ltseq/ltseq/expr/types.pyi:21; docs/api.md:581-598). | KEEP | `Expr.shift()` ([§10.1]) | Same lag and lead semantics. |
| `shift(default=)` | Fill value at boundaries (docs/api.md:584). | RENAME | `shift(fill_value=)` ([§10.1]) | Same role, under a keyword-only name. |
| `shift(partition_by=)` | A second way to partition (docs/api.md:569-571). | REMOVE | — | [§10.1] and [§1.4] (ADR 0013) remove it. Use `r.x.shift(1).over(partition_by="k")`. |
| `r.col.rolling(window, partition_by=None)` | Partial frames, with `min_periods` rejected (docs/api.md:600-610). | REDESIGN | `Expr.rolling()` ([§10.1]) | Returns a `Rolling` builder. Frames with fewer than `min_periods` (default `window`) non-NULL values give NULL, so early rows that used to get a partial mean now get NULL. |
| `rolling(partition_by=)` | A second way to partition (src/transpiler/window_native.rs:14). | REMOVE | — | Removed by [§10.1]. Use `r.x.rolling(n).mean().over(partition_by="k")`. |
| `rolling(n).sum()` | Rolling aggregate (py-ltseq/ltseq/expr/types.py:30). | KEEP | `Rolling.sum()` ([§10.1]) | Same aggregate. Frame rule as in the `rolling` row. |
| `rolling(n).mean()` | Rolling aggregate (py-ltseq/ltseq/expr/types.py:30). | KEEP | `Rolling.mean()` ([§10.1]) | Same aggregate. Frame rule as in the `rolling` row. |
| `rolling(n).min()` | Rolling aggregate (py-ltseq/ltseq/expr/types.py:30). | KEEP | `Rolling.min()` ([§10.1]) | Same aggregate. Frame rule as in the `rolling` row. |
| `rolling(n).max()` | Rolling aggregate (py-ltseq/ltseq/expr/types.py:30). | KEEP | `Rolling.max()` ([§10.1]) | Same aggregate. Frame rule as in the `rolling` row. |
| `rolling(n).count()` | Rolling aggregate (py-ltseq/ltseq/expr/types.py:30). | KEEP | `Rolling.count()` ([§10.1]) | Always returns the non-NULL count. |
| `rolling(n).std()` | Rolling aggregate (py-ltseq/ltseq/expr/types.py:30). | KEEP | `Rolling.std()` ([§10.1]) | Same aggregate. Frame rule as in the `rolling` row. |
| `r.col.diff(n=1)` | `x - x.shift(n)` (docs/api.md:611-621). | KEEP | `Expr.diff()` ([§10.1]) | Unchanged. |
| `diff(partition_by=)` | Accepted by every window function (src/transpiler/window_native.rs:14). | REMOVE | — | Removed by [§10.1]. Use `r.x.diff().over(partition_by="k")`. |
| `r.col.pct_change()` | `(x - x.shift(1)) / x.shift(1)` with SQL division (py-ltseq/ltseq/expr/base.py:918-935), which truncates on integers (docs/api.md:1306). | REDESIGN | `Expr.pct_change()` ([§10.1]) | Always returns `float64` with true division, so integer columns stop truncating to 0. `n=` is added. |
| `r.col.cum_sum()` | Running sum (docs/api.md:631-643). | KEEP | `Expr.cum_sum()` ([§10.1]) | Unchanged. Overflow is now checked. |
| `r.col.cum_min()` | Running minimum (docs/api.md:631-643). | KEEP | `Expr.cum_min()` ([§10.1]) | Unchanged. |
| `r.col.cum_max()` | Running maximum (docs/api.md:631-643). | KEEP | `Expr.cum_max()` ([§10.1]) | Unchanged. |
| `partition_by=` on `cum_*` | "All accept the `partition_by=` kwarg" (docs/api.md:633). | REMOVE | — | Removed by [§10.1]. Use `.over(partition_by="k")`. |
| `LTSeq.cum_sum(*cols)` | A table method that adds `*_cumsum` columns (py-ltseq/ltseq/aggregation.py:73; docs/api.md:646-656). | REMOVE | — | Removed by [§10.1]. Use `t.derive(volume_cumsum=lambda r: r.volume.cum_sum())`. |
| `LTSeq.fold(fn, *, init, into, partition_by=None)` | Passes a `_FoldRow` that allows both `row.x` and `row["x"]` (py-ltseq/ltseq/transforms.py:142-190, :652). | REDESIGN | `LTSeq.fold()` ([§10.7]) | The row is a plain `dict`, so `row.x` raises `AttributeError`, and `fold` is in the sequence tier. `dtype=` is added. |
| `LTSeq.stateful_scan` | Declared only in the stub (py-ltseq/ltseq/__init__.pyi:207-212). No runtime definition. | REMOVE | — | [§10.7] removes it. Use `fold`. |
| `row_number()` | Requires `order_by` (py-ltseq/ltseq/expr/base.py:457; docs/api.md:681-684). | KEEP | `row_number()` ([§10.2]) | Same function. Without `order_by` it now uses table order (sequence tier). |
| `rank()` | Ranking with gaps (py-ltseq/ltseq/expr/base.py:478). | KEEP | `rank()` ([§10.2]) | Unchanged. |
| `dense_rank()` | Ranking without gaps (py-ltseq/ltseq/expr/base.py:499). | KEEP | `dense_rank()` ([§10.2]) | Unchanged. |
| `ntile(n)` | Buckets (py-ltseq/ltseq/expr/base.py:520). | KEEP | `ntile()` ([§10.2]) | Unchanged. |
| `CallExpr.over(partition_by, order_by, descending, desc)` | Takes single `Expr` arguments (py-ltseq/ltseq/expr/types.py:192-258; docs/api.md:749-752). | REDESIGN | `Expr.over()` ([§10.4]) | Takes column names or sequences of them, plus `nulls_last`. It also applies to aggregates. |
| `over(partition_by=r.x, order_by=r.y)` (Expr arguments) | Expression keys (py-ltseq/ltseq/expr/types.py:194-195). | REMOVE | — | [§10.4] takes names only. Derive the key, then pass its name: `.over(partition_by="k")`. |
| `over(desc=)` | An alias of `descending` (py-ltseq/ltseq/expr/types.py:211-213). | MERGE | `over(descending=)` ([§10.4]) | One spelling. |
| `LTSeq.search_first(predicate)` | Annotated `LTSeq \| None` but returns an empty table (py-ltseq/ltseq/advanced_ops.py:320). | KEEP | `LTSeq.search_first()` ([§10.5]) | Same behavior. The annotation now matches: always an `LTSeq`. |
| `LTSeq.search_pattern(*steps, partition_by=None)` | Fixed-length step patterns (py-ltseq/ltseq/transforms.py:871). | KEEP | `LTSeq.search_pattern()` ([§10.6]) | Unchanged. |
| `LTSeq.search_pattern_count(...)` | A terminal twin of `search_pattern` (py-ltseq/ltseq/transforms.py:915). | REMOVE | — | Removed by [§10.6]. Use `t.search_pattern(...).count()`. |
| `LTSeq.align(ref_sequence, key)` | Reorders and pads by a reference list (py-ltseq/ltseq/advanced_ops.py:265). | REMOVE | — | Removed by [§13.2]. Use `LTSeq.from_dict({"k": ref}).join(t, on="k", how="left")`, where `k` is the key column. |
| `LTSeq.rvs()` | Reverse (py-ltseq/ltseq/advanced_ops.py:153). | RENAME | `LTSeq.reverse()` ([§9.8]) | [§9.8](../contract/ordering.md#98-positional-selection): "`rvs` is renamed `reverse`". |
| `LTSeq.step(n)` | Every n-th row (py-ltseq/ltseq/advanced_ops.py:175). | KEEP | `LTSeq.step()` ([§9.8]) | Unchanged. `offset=` is added. |

### Ordered grouping

| Current API | Problem / Strength | v0.5 Action | Final API | Rationale |
|---|---|---|---|---|
| `LTSeq.group_ordered(grouping_fn)` | Takes one key lambda (py-ltseq/ltseq/aggregation.py:111). | REDESIGN | `LTSeq.group_ordered()` ([§11.1]) | Takes key names and/or a `starts_when` boundary predicate. Uses the sequence tier. |
| `LTSeq.group_consecutive` | An alias of `group_ordered` (py-ltseq/ltseq/aggregation.py:161). | MERGE | `LTSeq.group_ordered()` ([§11.1]) | Removed by [§11.1]. |
| `LTSeq.group_sorted(key)` | A variant for sorted data (py-ltseq/ltseq/aggregation.py:163). | MERGE | `LTSeq.group_ordered()` ([§11.1]) | On sorted data every run is a whole group ([§11.1]). |
| `NestedTable` (class) | Lazy consecutive groups (py-ltseq/ltseq/grouping/nested_table.py:12). | KEEP | `NestedTable` ([§11.2]) | Kept, and not constructible ([§3.3]). |
| `NestedTable.filter(group_predicate)` | Re-groups the surviving rows, so equal-key groups that a removed group separated merge (py-ltseq/ltseq/grouping/nested_table.py:165-173). | REDESIGN | `NestedTable.filter()` ([§11.2]) | Groups keep their identity across the filter. |
| `NestedTable.agg(group_mapper)` | A lambda that returns a dict, with no arithmetic over aggregates (docs/api.md:926-940). | REDESIGN | `NestedTable.agg(**named)` ([§11.2]) | Keyword aggregates, with expressions over aggregates allowed ([§14.2]). |
| `NestedTable.derive(group_mapper)` | A lambda that returns a dict (docs/api.md:910-924). | REDESIGN | `NestedTable.derive(**named)` ([§11.2]) | Keyword form. Windows run within each group. Replacing a column is allowed and truncates `sort_keys` as `LTSeq.derive` does; the baseline refuses it ([Deliverable M]). |
| `NestedTable.first()` | First row per group (py-ltseq/ltseq/grouping/nested_table.py:66). | KEEP | `NestedTable.first()` ([§11.2]) | Unchanged. |
| `NestedTable.last()` | Last row per group (py-ltseq/ltseq/grouping/nested_table.py:80). | KEEP | `NestedTable.last()` ([§11.2]) | Unchanged. |
| `NestedTable.flatten()` | Exposes the internal `__group_id__` column (docs/api.md:889-897). | REDESIGN | `NestedTable.flatten()` ([§11.2]) | Returns the input schema. A group number appears only when `group_id=` names it. |
| `NestedTable.count()` | Returns a `CallExpr`, not a number (py-ltseq/ltseq/grouping/nested_table.py:95-106). | REDESIGN | `NestedTable.count()` ([§11.2]) | Executes and returns the group count as an `int`. |
| `len(nested)` | Returns the row count, not the group count (py-ltseq/ltseq/grouping/nested_table.py:58-60; docs/api.md:942-949). | REMOVE | — | [§3.3] makes `len` raise `TypeError`. Use `nested.flatten().count()` for rows and `nested.count()` for groups. |
| `NestedTable.to_pandas()` | Exports the underlying rows (py-ltseq/ltseq/grouping/nested_table.py:62-64). | REMOVE | — | [§3.3] exposes no export on `NestedTable`. Use `nested.flatten().to_pandas()`. |
| `g.count()` | Group size (py-ltseq/ltseq/grouping/proxies/derive_proxy.py:30). | KEEP | `Group.count()` ([§8.1], [§22.2]) | The one reserved attribute on `Group`. |
| `g.sum("col")` | String-column aggregate form (py-ltseq/ltseq/grouping/proxies/derive_proxy.py:50). | MERGE | `g.col.sum()` ([§14.2]) | One aggregate syntax across all contexts. |
| `g.avg("col")` | String-column form (py-ltseq/ltseq/grouping/proxies/derive_proxy.py:54). | MERGE | `g.col.mean()` ([§14.2]) | `avg` is removed in favor of `mean` ([§14.2]). |
| `g.mean("col")` | String-column form (py-ltseq/ltseq/grouping/proxies/derive_proxy.py:58). | MERGE | `g.col.mean()` ([§14.2]) | One aggregate syntax. |
| `g.min("col")` | String-column form (py-ltseq/ltseq/grouping/proxies/derive_proxy.py:46). | MERGE | `g.col.min()` ([§14.2]) | One aggregate syntax. |
| `g.max("col")` | String-column form (py-ltseq/ltseq/grouping/proxies/derive_proxy.py:42). | MERGE | `g.col.max()` ([§14.2]) | One aggregate syntax. |
| `g.median("col")` | String-column form (py-ltseq/ltseq/grouping/proxies/derive_proxy.py:62). | MERGE | `g.col.median()` ([§14.2]) | One aggregate syntax. |
| `g.std("col")` | String-column form (py-ltseq/ltseq/grouping/proxies/derive_proxy.py:66). | MERGE | `g.col.std()` ([§14.2]) | One aggregate syntax. |
| `g.var("col")` | String-column form (py-ltseq/ltseq/grouping/proxies/derive_proxy.py:70). | MERGE | `g.col.var()` ([§14.2]) | One aggregate syntax. |
| `g.percentile("col", p)` | Approximate (docs/api.md:955-963). | REDESIGN | `g.col.quantile(q)` ([§14.2]) | Exact, with linear interpolation, so results differ from the approximate values. |
| `g.first()` | Row proxy for the first row (py-ltseq/ltseq/grouping/proxies/derive_proxy.py:34; docs/api.md:964-971). | MERGE | `g.col.first()` ([§14.2]) | The aggregate `first` reads each group in table order ([§14.4]). |
| `g.last()` | Row proxy for the last row (py-ltseq/ltseq/grouping/proxies/derive_proxy.py:38). | MERGE | `g.col.last()` ([§14.2]) | Same as `g.first()`. |
| `g.all(pred)` | Filter-only quantifier (py-ltseq/ltseq/grouping/proxies/filter_proxy.py:207; docs/api.md:981-995). | REDESIGN | `Expr.all()` ([§14.2]) | An ordinary `bool` aggregate usable in every group context: `(g.x > 0).all()`. |
| `g.any(pred)` | Filter-only quantifier (py-ltseq/ltseq/grouping/proxies/filter_proxy.py:213). | REDESIGN | `Expr.any()` ([§14.2]) | As `all`: `(g.x > 0).any()`. |
| `g.none(pred)` | Filter-only quantifier (py-ltseq/ltseq/grouping/proxies/filter_proxy.py:219). | REMOVE | — | Removed by [§8.1]. Use `~(g.x > 0).any()`. |
| `.is_null()` / `.is_not_null()` / `== None` on group aggregates | Filter-only null checks (py-ltseq/ltseq/grouping/proxies/filter_proxy.py:115-119; docs/api.md:972-980). | KEEP | `Expr.is_null()` ([§8.5], [§8.2]) | The same spellings, now valid on any aggregate expression. |

### Set algebra

| Current API | Problem / Strength | v0.5 Action | Final API | Rationale |
|---|---|---|---|---|
| `LTSeq.concat(other)` | `UNION ALL` of two tables. It is an alias of `union` (py-ltseq/ltseq/advanced_ops.py:61; docs/api.md:999-1002). | REDESIGN | `LTSeq.concat(*others)` ([§13.1]) | Variadic. Schemas must match exactly with no type promotion (#222). |
| `LTSeq.union(other)` | Despite its name, it does not deduplicate (py-ltseq/ltseq/advanced_ops.py:26-61; docs/api.md:1001). | REMOVE | — | Removed by [§13.2]. It was `UNION ALL`, so `a.concat(b)` replaces it; SQL's `UNION` is `a.concat(b).distinct()`. |
| `LTSeq.intersect(other, on=None)` | Set intersection (py-ltseq/ltseq/advanced_ops.py:63). | REDESIGN | `LTSeq.intersect()` ([§13.2]) | Whole-row comparison with `distinct=` multiset semantics. Keeps left order. |
| `intersect(on=)` | Keyed form (py-ltseq/ltseq/advanced_ops.py:63). | REMOVE | — | Removed by [§13.2]. Use `a.semi_join(b, on=...)`. |
| `LTSeq.except_(other, on=None)` | Set difference (py-ltseq/ltseq/advanced_ops.py:96). | REDESIGN | `LTSeq.difference()` ([§13.2]) | `difference` deduplicates by default, counts occurrences with `distinct=False`, and treats NULL as equal to NULL. `except_` kept every left row with no equal right row, duplicates included, and NULL never matched (`src/ops/set_ops.rs`, `diff_impl`); that behavior is `a.anti_join(b, on=a.columns)`. |
| `LTSeq.subtract` | An alias of `except_` (py-ltseq/ltseq/advanced_ops.py:130). | MERGE | `LTSeq.difference()` ([§13.2]) | One spelling. |
| `except_(on=)` | Keyed form (py-ltseq/ltseq/advanced_ops.py:96). | REMOVE | — | Removed by [§13.2]. Use `a.anti_join(b, on=...)`. |
| `LTSeq.xunion(other, on=None)` | Symmetric difference (py-ltseq/ltseq/advanced_ops.py:132). | REMOVE | — | Removed by [§13.2]. Use `a.difference(b).concat(b.difference(a))`. |
| `LTSeq.is_subset(other, on=None)` | Returns a `bool` (py-ltseq/ltseq/advanced_ops.py:231). | REMOVE | — | Removed by [§13.2]. Use `a.difference(b).count() == 0`. |
| `LTSeq.contain(key_col, *values)` | Returns `True` when all values are present (py-ltseq/ltseq/advanced_ops.py:204-229; docs/api.md:1056-1060). | REMOVE | — | Removed by [§13.2]. Use `t.filter(lambda r: r.k.is_in(values)).select("k").distinct(keep="any").count() == len(set(values))`. |

### Joins

| Current API | Problem / Strength | v0.5 Action | Final API | Rationale |
|---|---|---|---|---|
| `LTSeq.join(other, on=None, how="inner", strategy=None, *, left_on, right_on, suffix)` | `how` is positional, `cross` is missing, and nothing checks key cardinality (py-ltseq/ltseq/joins.py:166-176). | REDESIGN | `LTSeq.join()` ([§12.1]) | `how` becomes keyword-only and gains `cross`, plus `alias=` and `validate=`. Key-class rules are fixed. |
| `join(strategy=)` | An algorithm hint. `"merge"` only adds a sort check that raises on unsorted input (py-ltseq/ltseq/joins.py:171, :190-193, :203). | REMOVE | — | Removed by [§12.1]. Call `join(...)` without it; the engine picks the algorithm. |
| `join(on=lambda a, b: ...)` | Arbitrary-condition join (docs/api.md:1087-1089). | REMOVE | — | Removed by [§12.1]. Derive key columns and join on names, or use `how="cross"` followed by `filter` for non-equi conditions. |
| `JoinHow` (stub alias) | Stub-only alias without `cross` (py-ltseq/ltseq/__init__.pyi:5). | REMOVE | — | Not in [§2.2]. Annotate with `typing.Literal["inner", "left", "right", "full", "cross"]`, as [§12.1] does inline. |
| `JoinStrategy` (stub alias) | Stub-only alias (py-ltseq/ltseq/__init__.pyi:6). | REMOVE | — | Its parameter is removed ([§12.1]). Delete the annotation together with `strategy=`. |
| `LTSeq.semi_join(other, on)` | Existence filter (py-ltseq/ltseq/joins.py:534). | KEEP | `LTSeq.semi_join()` ([§12.2]) | Same call. `left_on`/`right_on` are added. |
| `LTSeq.anti_join(other, on)` | Non-existence filter (py-ltseq/ltseq/joins.py:554). | KEEP | `LTSeq.anti_join()` ([§12.2]) | Same call, with `NOT EXISTS` semantics for NULL keys. |
| `semi_join`/`anti_join(on=lambda ...)` | Lambda keys (py-ltseq/ltseq/joins.py:534, :554). | REMOVE | — | [§12.2] takes names. Derive the keys, then use `on=` or `left_on=`/`right_on=`. |
| `LTSeq.asof_join(other, on, direction, is_sorted, *, ..., strategy)` | `direction` and `is_sorted` are positional, and `strategy` duplicates `direction` (py-ltseq/ltseq/joins.py:314-326, :337, :370-376). | REDESIGN | `LTSeq.asof_join()` ([§12.3]) | Keyword-only options, with `tolerance`, `allow_exact_matches` and separate left and right `by` keys. Neither input needs to be sorted. |
| `asof_join(is_sorted=)` | A caller's promise that is never checked (py-ltseq/ltseq/joins.py:319). | REMOVE | — | Removed by [§12.3]. Omit it, or `assume_sorted` the input. |
| `asof_join(strategy=)` | The primary spelling of `direction` (py-ltseq/ltseq/joins.py:324, :337). | MERGE | `asof_join(direction=)` ([§12.3]) | One spelling. |
| `asof_join(on=lambda ...)` | Lambda form where the operator picks the direction (docs/api.md:1094, :1102-1103). | REMOVE | — | [§12.3] takes names. Use `on="ts", direction="forward"`. |
| `AsofStrategy` (stub alias) | Stub-only alias (py-ltseq/ltseq/__init__.pyi:7). | REMOVE | — | Not in [§2.2]. Annotate with `typing.Literal["backward", "forward", "nearest"]`, as [§12.3] does inline. |

### Aggregation

| Current API | Problem / Strength | v0.5 Action | Final API | Rationale |
|---|---|---|---|---|
| `LTSeq.group_by(key)` | A single key, as a name or a lambda (py-ltseq/ltseq/aggregation.py:277). | REDESIGN | `LTSeq.group_by(*keys)` ([§14.1]) | Several name keys. NULL and NaN each form one group. |
| `group_by(lambda r: ...)` | Expression key (docs/api.md:1199-1201). | REMOVE | — | [§14.1] takes names. Use `t.derive(k=...).group_by("k")`. |
| `GroupBy` (class) | Builder (py-ltseq/ltseq/aggregation.py:14). | KEEP | `GroupBy` ([§14.1]) | Unchanged. Not constructible ([§3.3]). |
| `GroupBy.agg(**aggregations)` | Keyword aggregates (py-ltseq/ltseq/aggregation.py:25). | KEEP | `GroupBy.agg()` ([§14.1]) | Unchanged. Output order is undefined. |
| `LTSeq.agg(by=None, **aggregations)` | Whole-table aggregates (py-ltseq/ltseq/aggregation.py:235). | KEEP | `LTSeq.agg()` ([§14.3]) | Same keyword form, returning one row. |
| `agg(by=)` | A second grouping entry point (docs/api.md:1203-1214). | REMOVE | — | Removed by [§14.3]. Use `t.group_by("k").agg(...)`. |
| `x.sum()` | Aggregate (py-ltseq/ltseq/expr/types.pyi:26). | KEEP | `Expr.sum()` ([§14.2]) | Same aggregate. Overflow is checked, and float sums no longer depend on order. |
| `x.mean()` | Aggregate (py-ltseq/ltseq/expr/types.pyi:27). | KEEP | `Expr.mean()` ([§14.2]) | Unchanged. |
| `x.count()` | Aggregate (py-ltseq/ltseq/expr/types.pyi:30). | KEEP | `Expr.count()` ([§14.2]) | Unchanged. |
| `x.min()` | Aggregate (py-ltseq/ltseq/expr/types.pyi:28). | KEEP | `Expr.min()` ([§14.2]) | Unchanged. |
| `x.max()` | Aggregate (py-ltseq/ltseq/expr/types.pyi:29). | KEEP | `Expr.max()` ([§14.2]) | Unchanged. |
| `x.median()` | Aggregate (py-ltseq/ltseq/expr/types.pyi:35). | KEEP | `Expr.median()` ([§14.2]) | Defined as `quantile(0.5)`. |
| `x.std()` | Sample standard deviation (docs/api.md:1217-1219). | KEEP | `Expr.std()` ([§14.2]) | Unchanged (`ddof = 1`). |
| `x.var()` | Sample variance (docs/api.md:1217-1219). | KEEP | `Expr.var()` ([§14.2]) | Unchanged (`ddof = 1`). |
| `x.first()` | Needs an explicit order column, otherwise "requires an order column argument" (src/ops/aggregation.rs:252-275). | REDESIGN | `Expr.first()` ([§14.2]) | Reads table order (positional tier). |
| `x.last()` | Same as `first` (src/ops/aggregation.rs:252-275). | REDESIGN | `Expr.last()` ([§14.2]) | Reads table order (positional tier). |
| `x.avg()` | An alias of `mean` (docs/api.md:1217-1219). | MERGE | `Expr.mean()` ([§14.2]) | Removed by [§14.2]. |
| `x.variance()` | An alias of `var` (docs/api.md:1217-1219). | MERGE | `Expr.var()` ([§14.2]) | Removed by [§14.2]. |
| `x.stddev()` | An alias of `std` (docs/api.md:1217-1219). | MERGE | `Expr.std()` ([§14.2]) | Removed by [§14.2]. |
| `x.percentile(p)` | Approximate (docs/api.md:1219). | REDESIGN | `Expr.quantile(q)` ([§14.2]) | Exact, with linear interpolation. |
| `x.top_k(k)` | Returns the k largest values as a `;`-joined string (docs/api.md:1219). | REMOVE | — | Removed by [§14.2]. Use `t.derive(rk=lambda r: row_number().over(partition_by="g", order_by="x", descending=True)).filter(lambda r: r.rk <= k).sort("g", "rk").group_by("g").agg(top=lambda g: g.x.cast("string").string_agg(";"))`. |
| `count_if(pred)` | Function-form conditional aggregate (py-ltseq/ltseq/expr/base.py:112). | MERGE | `g.count(where=)` ([§14.2]) | `where=` replaces the `*_if` family. |
| `sum_if(pred, col)` | (py-ltseq/ltseq/expr/base.py:130) | MERGE | `Expr.sum(where=)` ([§14.2]) | Same as `count_if`. |
| `avg_if(pred, col)` | (py-ltseq/ltseq/expr/base.py:149) | MERGE | `Expr.mean(where=)` ([§14.2]) | Same as `count_if`. |
| `min_if(pred, col)` | (py-ltseq/ltseq/expr/base.py:168) | MERGE | `Expr.min(where=)` ([§14.2]) | Same as `count_if`. |
| `max_if(pred, col)` | (py-ltseq/ltseq/expr/base.py:187) | MERGE | `Expr.max(where=)` ([§14.2]) | Same as `count_if`. |
| `skew(col)` | Population skewness (py-ltseq/ltseq/expr/base.py:406; src/ops/aggregation.rs:292). | REMOVE | — | Removed by [§14.2]. Use `t.derive(d=lambda r: r.x - r.x.mean().over(partition_by="k")).group_by("k").agg(skew=lambda g: (g.d ** 3).mean() / (g.d ** 2).mean() ** 1.5)`. |
| `corr(a, b)` | Pearson correlation (py-ltseq/ltseq/expr/base.py:412). | KEEP | `corr()` ([§14.2]) | Unchanged. `where=` is added. |
| `covar(a, b)` | Covariance (py-ltseq/ltseq/expr/base.py:418). | RENAME | `cov()` ([§14.2]) | Removed by [§14.2] in favor of `cov`. |
| `concat_agg(col, delimiter=",")` | Unordered string join (py-ltseq/ltseq/expr/base.py:424). | REDESIGN | `Expr.string_agg()` ([§14.2]) | Becomes a method, joined in table order (positional tier). |
| `LTSeq.pivot(index, columns, values, agg_fn)` | Always discovers pivot values eagerly (py-ltseq/ltseq/advanced_ops.py:360). | REDESIGN | `LTSeq.pivot()` ([§14.6]) | Keyword-only. It plans lazily when `column_values` is given, and output columns follow a fixed order. |
| `pivot(agg_fn=)` | Aggregate selector (docs/api.md:1291). | RENAME | `pivot(agg=)` ([§14.6]) | Same choice, shorter name. |
| `PivotAggFn` (stub alias) | Stub-only alias that lacks `first`/`last` (py-ltseq/ltseq/__init__.pyi:13). | REMOVE | — | Not in [§2.2]. Annotate with `typing.Literal["sum", "mean", "min", "max", "count", "first", "last"]`, as [§14.6] does inline. |

### Expression API

**Operators**

| Current API | Problem / Strength | v0.5 Action | Final API | Rationale |
|---|---|---|---|---|
| `a + b`, `a - b`, `a * b` | Integer overflow wraps (docs/api.md:1309). | KEEP | `Expr.__add__` etc. ([§8.2]) | Same arithmetic. Overflow raises `ArithmeticOverflowError` ([§17.1]). |
| `a / b` | Integer `/` truncates, following SQL (docs/api.md:1306). | REDESIGN | `Expr.__truediv__` ([§8.2], [§17.3]) | True division to `float64`, so `7 / 2` changes from `3` to `3.5` with the same spelling. |
| `a // b` | Python floor division (py-ltseq/ltseq/expr/base.py:607). | KEEP | `Expr.__floordiv__` ([§17.3]) | Unchanged. |
| `a % b` | Takes the dividend's sign (docs/api.md:1306). | REDESIGN | `Expr.__mod__` ([§17.3]) | Takes the divisor's sign, as in Python: `-7 % 2` changes from `-1` to `1`. |
| `a ** b` | Raises; `power()` is documented instead (py-ltseq/ltseq/expr/base.py:701-707; docs/api.md:1310). | REDESIGN | `Expr.__pow__`/`__rpow__` ([§17.3]) | Supported, with checked integer powers. |
| `-a` | Raises; `0 - x` is documented instead (py-ltseq/ltseq/expr/base.py:694-699; docs/api.md:1310). | REDESIGN | `Expr.__neg__` ([§8.2]) | Supported, with overflow checked. |
| `abs(a)` | (py-ltseq/ltseq/expr/base.py:686) | KEEP | `Expr.__abs__` ([§8.2]) | Unchanged. |
| `==`, `!=` | `== None` is a null check (py-ltseq/ltseq/expr/base.py:621, :630). | KEEP | `Expr.__eq__`/`__ne__` ([§8.2]) | Unchanged. |
| `<`, `<=`, `>`, `>=` | (py-ltseq/ltseq/expr/base.py:638-661) | KEEP | `Expr.__lt__` etc. ([§8.2]) | Comparisons become exact across numeric types ([§17.4]). |
| `a & b` | Logical AND (py-ltseq/ltseq/expr/base.py:663). | KEEP | `Expr.__and__` ([§8.2]) | Kleene AND. [§22.1] also lists `__rand__`, which baseline lacks (py-ltseq/ltseq/expr/base.py:709-745). |
| `a \| b` | Logical OR (py-ltseq/ltseq/expr/base.py:669). | KEEP | `Expr.__or__` ([§8.2]) | Kleene OR. [§22.1] also lists `__ror__`, which baseline lacks (py-ltseq/ltseq/expr/base.py:709-745). |
| `~a` | Logical NOT (py-ltseq/ltseq/expr/base.py:675). | KEEP | `Expr.__invert__` ([§8.2]) | Unchanged. |
| `bool(expr)` | Raises with a hint (py-ltseq/ltseq/expr/base.py:681). | KEEP | `Expr.__bool__` ([§3.4]) | Unchanged. |

**Methods and node classes**

| Current API | Problem / Strength | v0.5 Action | Final API | Rationale |
|---|---|---|---|---|
| `Expr` (class) | Abstract base for expression nodes (py-ltseq/ltseq/expr/base.py:564). | KEEP | `Expr` ([§3.1]) | Stays the one public expression type, now opaque. |
| `x.fill_null(v)` | (py-ltseq/ltseq/expr/base.py:746) | KEEP | `Expr.fill_null()` ([§8.5]) | Unchanged. |
| `x.is_null()` | (py-ltseq/ltseq/expr/base.py:763) | KEEP | `Expr.is_null()` ([§8.5]) | Unchanged. |
| `x.is_not_null()` | (py-ltseq/ltseq/expr/base.py:777) | KEEP | `Expr.is_not_null()` ([§8.5]) | Unchanged. |
| `x.is_in(values)` | (py-ltseq/ltseq/expr/base.py:791) | KEEP | `Expr.is_in()` ([§8.5]) | Same test, now never NULL. |
| `x.between(low, high)` | (py-ltseq/ltseq/expr/base.py:812) | KEEP | `Expr.between()` ([§8.5]) | Unchanged. |
| `x.cast(dtype: str)` | Takes type-name strings (py-ltseq/ltseq/expr/base.py:831; docs/api.md:1440-1446). | REDESIGN | `Expr.cast()` ([§8.5], [§17.6]) | Takes pyarrow aliases or `pa.DataType` and follows the [§17.6] rule table. A failed value raises `CastError`. |
| `x.abs()` | (py-ltseq/ltseq/expr/base.py:852) | KEEP | `Expr.abs()` ([§8.5]) | Unchanged. |
| `x.round(decimals=0)` | No tie rule is documented (docs/api.md:1448-1458). | REDESIGN | `Expr.round()` ([§8.5]) | Rounds half to even and keeps the input type. |
| `x.floor()` | (py-ltseq/ltseq/expr/base.py:890) | KEEP | `Expr.floor()` ([§8.5]) | Unchanged. |
| `x.ceil()` | (py-ltseq/ltseq/expr/base.py:904) | KEEP | `Expr.ceil()` ([§8.5]) | Unchanged. |
| `Expr.serialize()` | Internal wire format on the public class (py-ltseq/ltseq/expr/base.py:573). | REMOVE | — | Not in [§22], which is closed. `Expr` is opaque ([§3.1]), and there is no user-level replacement. |
| `__getattr__` catch-all on column and call nodes | Any unknown method name builds a call, so typos fail late (py-ltseq/ltseq/expr/types.py:77-89, :163-175). | REMOVE | — | [§8.7] closes the method set. An unknown name raises `AttributeError` with a suggestion. |
| `ColumnExpr` | Node class (py-ltseq/ltseq/expr/types.py:46). | REMOVE | — | Removed by [§2.2]. Annotate with `ltseq.Expr`. |
| `CallExpr` | Node class (py-ltseq/ltseq/expr/types.py:107). | REMOVE | — | Removed by [§2.2]. Annotate with `ltseq.Expr`. |
| `WindowExpr` | Node class (py-ltseq/ltseq/expr/types.py:261). | REMOVE | — | Removed by [§2.2]. Annotate with `ltseq.Expr`. |
| `BinOpExpr` | Node class (py-ltseq/ltseq/expr/core_types.py:217). | REMOVE | — | Removed by [§2.2]. Annotate with `ltseq.Expr`. |
| `UnaryOpExpr` | Node class (py-ltseq/ltseq/expr/core_types.py:243). | REMOVE | — | Removed by [§2.2]. Annotate with `ltseq.Expr`. |
| `LiteralExpr` | Node class, exported only by the stub (py-ltseq/ltseq/expr/core_types.py:189). | REMOVE | — | Removed by [§2.2]. Use `lit(value)`. |
| `LookupExpr` | Node class, exported only by the stub (py-ltseq/ltseq/expr/lookup_expr.py:8). | REMOVE | — | Removed by [§2.2], together with `Expr.lookup`. |
| `SchemaProxy` | Lambda proxy class (py-ltseq/ltseq/expr/proxy.py:6). | REMOVE | — | Removed by [§2.2]. Annotate lambdas with `ltseq.typing.Row`. |
| `NestedSchemaProxy` | Proxy for linked aliases (py-ltseq/ltseq/expr/proxy.py:80). | REMOVE | — | Removed by [§2.2]. Aliased columns are plain columns after `join(alias=)`. |
| `ltseq.expr` module | Documented import path for functions such as `char` (docs/api.md:1613-1622). | REMOVE | — | [§2.2](../contract/public-surface.md#22-other-modules): no module other than `ltseq` and `ltseq.typing` is public. Import from `ltseq`. |
| `ltseq.ltseq_core` | Public extension module name (pyproject.toml:18; Cargo.toml:7). | RENAME | `ltseq._ltseq_core` ([§2.2]) | Made private and not re-exported. |

**Conditional, null, math and clock functions**

| Current API | Problem / Strength | v0.5 Action | Final API | Rationale |
|---|---|---|---|---|
| `if_else(cond, a, b)` | (py-ltseq/ltseq/expr/base.py:14) | KEEP | `if_else()` ([§8.3]) | Kept as the two-branch form. |
| `when(cond, value=MISSING)` | Two chain styles: `when(c, v)` and `when(c).then(v)` (py-ltseq/ltseq/expr/base.py:87). | REDESIGN | `when()` ([§8.3]) | `when(c, v)` only, and the result is itself an `Expr` that defaults to NULL. |
| `when(c).then(v)` | Second chain style (py-ltseq/ltseq/expr/base.py:76-86). | REMOVE | — | Removed by [§8.3]. Write `when(c, v)`. |
| `WhenChain` | Builder that needs `.otherwise()` to become an expression (py-ltseq/ltseq/expr/base.py:44-73). | REDESIGN | `ltseq.typing.When` ([§8.3], [§22.2]) | `When` is an `Expr`, so a chain without `.otherwise()` is usable and yields NULL. |
| `coalesce(*args)` | (py-ltseq/ltseq/expr/base.py:430) | KEEP | `coalesce()` ([§8.3]) | Unchanged. Fewer than two arguments raise. |
| `nvl(x, default)` | Two-argument `coalesce` (py-ltseq/ltseq/expr/base.py:391). | MERGE | `coalesce()` ([§8.3]) | Listed as dropped in [§8.4]. |
| `ifa(cond, value)` | One-branch conditional (py-ltseq/ltseq/expr/base.py:396). | MERGE | `when()` ([§8.3]) | `when(c, v)` without `.otherwise()` is the same expression. |
| `sqrt(x)` | (py-ltseq/ltseq/expr/base.py:211) | KEEP | `sqrt()` ([§8.4]) | Unchanged. |
| `sign(x)` | (py-ltseq/ltseq/expr/base.py:223) | KEEP | `sign()` ([§8.4]) | Unchanged. |
| `log(x, base=None)` | (py-ltseq/ltseq/expr/base.py:229) | KEEP | `log()` ([§8.4]) | Unchanged. |
| `exp(x)` | (py-ltseq/ltseq/expr/base.py:243) | KEEP | `exp()` ([§8.4]) | Unchanged. |
| `sin(x)` | (py-ltseq/ltseq/expr/base.py:249) | KEEP | `sin()` ([§8.4]) | Unchanged. |
| `cos(x)` | (py-ltseq/ltseq/expr/base.py:255) | KEEP | `cos()` ([§8.4]) | Unchanged. |
| `tan(x)` | (py-ltseq/ltseq/expr/base.py:261) | KEEP | `tan()` ([§8.4]) | Unchanged. |
| `asin(x)` | (py-ltseq/ltseq/expr/base.py:267) | KEEP | `asin()` ([§8.4]) | Unchanged. |
| `acos(x)` | (py-ltseq/ltseq/expr/base.py:273) | KEEP | `acos()` ([§8.4]) | Unchanged. |
| `atan(x)` | (py-ltseq/ltseq/expr/base.py:279) | KEEP | `atan()` ([§8.4]) | Unchanged. |
| `atan2(y, x)` | (py-ltseq/ltseq/expr/base.py:285) | KEEP | `atan2()` ([§8.4]) | Unchanged. |
| `power(x, n)` | Exists because `**` was refused (py-ltseq/ltseq/expr/base.py:217). | MERGE | `Expr.__pow__` ([§17.3]) | Write `x ** n`. |
| `ln(x)` | (py-ltseq/ltseq/expr/base.py:237) | MERGE | `log()` ([§8.4]) | `log(x)` is the natural logarithm. |
| `rand()` | Nondeterministic per-row value (py-ltseq/ltseq/expr/base.py:291). | REMOVE | — | Dropped by [§8.4] without an idiom. Use `rng = random.Random(seed)`, then `t.with_row_index().fold(lambda _, row: rng.random(), init=0.0, into="u")`. |
| `gcd(a, b)` | Declared in the stub but not bound in the runtime `ltseq` namespace (py-ltseq/ltseq/expr/base.py:297). | REMOVE | — | Dropped by [§8.4]. Use `t.with_row_index().fold(lambda _, row: math.gcd(row["a"], row["b"]), init=0, into="g")`; `with_row_index()` is needed because `fold` needs `sort_keys` ([§9.3]). |
| `lcm(a, b)` | Same as `gcd` (py-ltseq/ltseq/expr/base.py:307). | REMOVE | — | Dropped by [§8.4]. Use `fold` with `math.lcm`, as for `gcd`. |
| `factorial(n)` | Same as `gcd` (py-ltseq/ltseq/expr/base.py:317). | REMOVE | — | Dropped by [§8.4]. Join against `LTSeq.from_dict({"n": list(range(21)), "fact": [math.factorial(i) for i in range(21)]})`, which covers every result that fits in `int64`. |
| `str_char(n)` | Alias of `char`, declared in the `ltseq` stub but not bound in the runtime `ltseq` namespace (py-ltseq/ltseq/expr/base.py:332). | REMOVE | — | Not in [§22], which is closed. Use `fold` with `chr`, as for `char`. |
| `char(n)` | Code point to character (py-ltseq/ltseq/expr/base.py:342; docs/api.md:1613-1622). | REMOVE | — | Dropped by [§8.4]. Use `t.with_row_index().fold(lambda _, row: chr(row["n"]), init="", into="c")`; `with_row_index()` is needed because `fold` needs `sort_keys` ([§9.3]). |
| `concat_ws(delimiter, *cols)` | NULL-skipping join (py-ltseq/ltseq/expr/base.py:353). | REMOVE | — | Dropped by [§8.4]. For two columns, `when(r.a.is_null(), r.b).when(r.b.is_null(), r.a).otherwise(r.a + "," + r.b)` skips a NULL part; longer lists extend the chain. |
| `now()` | (py-ltseq/ltseq/expr/base.py:374) | KEEP | `now()` ([§19.5]) | Fixed per execution. |
| `today()` | (py-ltseq/ltseq/expr/base.py:380) | KEEP | `today()` ([§19.5]) | Fixed per execution. |

**String accessor**

| Current API | Problem / Strength | v0.5 Action | Final API | Rationale |
|---|---|---|---|---|
| `.s` | Accessor name (py-ltseq/ltseq/expr/types.py:91-94). | MERGE | `.str` ([§8.6]) | [§8.6] removes `.s`. |
| `.str` | Pandas and Polars name (py-ltseq/ltseq/expr/types.py:96-99). | KEEP | `Expr.str` ([§8.6]) | The only spelling. |
| `StringAccessor` | Accessor class (py-ltseq/ltseq/expr/accessors.py:11). | RENAME | `ltseq.typing.StrNamespace` ([§22.2]) | Typing-only, not constructible. |
| `s.contains(pattern)` | Literal substring through DataFusion `contains` (py-ltseq/ltseq/expr/accessors.py:33; src/transpiler/mod.rs:551-559). | RENAME | `StrNamespace.contains_literal()` ([§8.6]) | Same literal test. The new name keeps it from being read as a regex. |
| `s.starts_with(prefix)` | (py-ltseq/ltseq/expr/accessors.py:37) | RENAME | `StrNamespace.startswith()` ([§8.6]) | Python's spelling. |
| `s.ends_with(suffix)` | (py-ltseq/ltseq/expr/accessors.py:41) | RENAME | `StrNamespace.endswith()` ([§8.6]) | Python's spelling. |
| `s.lower()` | (py-ltseq/ltseq/expr/accessors.py:45) | KEEP | `StrNamespace.lower()` ([§8.6]) | Unchanged. Full Unicode case mapping. |
| `s.upper()` | (py-ltseq/ltseq/expr/accessors.py:49) | KEEP | `StrNamespace.upper()` ([§8.6]) | Unchanged. |
| `s.len()` | (py-ltseq/ltseq/expr/accessors.py:57) | KEEP | `StrNamespace.len()` ([§8.6]) | Counts code points. |
| `s.replace(old, new)` | (py-ltseq/ltseq/expr/accessors.py:79) | KEEP | `StrNamespace.replace()` ([§8.6]) | Literal, every occurrence. |
| `s.find(sub)` | 0-based, -1 when absent (py-ltseq/ltseq/expr/accessors.py:227; docs/api.md:1567-1574). | KEEP | `StrNamespace.find()` ([§8.6]) | Unchanged. |
| `s.strip()` | Trims spaces only (src/transpiler/mod.rs:588-591). | REDESIGN | `StrNamespace.strip()` ([§8.6]) | Trims Unicode whitespace, including tabs and newlines, with the same spelling. `chars=` is added. |
| `s.lstrip()` | Spaces only (py-ltseq/ltseq/expr/accessors.py:271). | REDESIGN | `StrNamespace.lstrip()` ([§8.6]) | As `strip`. |
| `s.rstrip()` | Spaces only (py-ltseq/ltseq/expr/accessors.py:282). | REDESIGN | `StrNamespace.rstrip()` ([§8.6]) | As `strip`. |
| `s.slice(start, length)` | `length` is required (py-ltseq/ltseq/expr/accessors.py:61). | REDESIGN | `StrNamespace.slice()` ([§8.6]) | `length` is optional and a negative `offset` counts from the end. |
| `s.regex_match(pattern)` | (py-ltseq/ltseq/expr/accessors.py:65) | RENAME | `StrNamespace.contains_regex()` ([§8.6]) | Named for what it does: a match anywhere. |
| `s.concat(*others)` | (py-ltseq/ltseq/expr/accessors.py:94) | REMOVE | — | Removed by [§8.6]. Use `r.a + r.b`. |
| `s.pad_left(width, char=" ")` | Truncates strings longer than `width` (docs/api.md:1549-1555). | REDESIGN | `StrNamespace.rjust()` ([§8.6]) | Python's `rjust`, which never truncates. `char` becomes `fillchar`. |
| `s.pad_right(width, char=" ")` | Truncates the same way (py-ltseq/ltseq/expr/accessors.py:123). | REDESIGN | `StrNamespace.ljust()` ([§8.6]) | Python's `ljust`, which never truncates. `char` becomes `fillchar`. |
| `s.split(delimiter, index)` | An alias of `split_part`: 1-based, with `""` out of range and `ValueError` for `index <= 0` (docs/api.md:1557-1565). | REDESIGN | `StrNamespace.split()` ([§8.6]) | 0-based, NULL out of range, negative indexes from the end. `split(d, 1)` silently returns the second field instead of the first. |
| `s.split_part(delimiter, index)` | 1-based (py-ltseq/ltseq/expr/accessors.py:158). | MERGE | `StrNamespace.split()` ([§8.6]) | `split_part(d, n)` is `split(d, n - 1)`, which is NULL where `split_part` gave `""` ([§8.6]). |
| `s.like(pattern)` | SQL `LIKE` (py-ltseq/ltseq/expr/accessors.py:173). | REMOVE | — | Removed by [§8.6]. Use `startswith`, `endswith`, `contains_literal` or `contains_regex`. |
| `s.isalpha()` | ASCII-only regex `^[a-zA-Z]+$` (src/transpiler/mod.rs:689). | REDESIGN | `StrNamespace.isalpha()` ([§8.6]) | Python's Unicode-aware `str.isalpha`, so non-ASCII letters now pass. |
| `s.isdigit()` | ASCII `^[0-9]+$` (src/transpiler/mod.rs:694). | REDESIGN | `StrNamespace.isdigit()` ([§8.6]) | Python's `str.isdigit`, which also accepts other Unicode digits. |
| `s.islower()` | `s == lower(s)`, which is TRUE for digits-only strings (src/transpiler/mod.rs:699). | REDESIGN | `StrNamespace.islower()` ([§8.6]) | Python semantics, so a string without cased characters is FALSE. |
| `s.isupper()` | `s == upper(s)` (src/transpiler/mod.rs:705). | REDESIGN | `StrNamespace.isupper()` ([§8.6]) | Python semantics, as for `islower`. |
| `s.pos(sub)` | 1-based, 0 when absent (src/transpiler/mod.rs:711; docs/api.md:1575-1582). | REMOVE | — | Removed by [§8.6]. Use `find(sub) + 1`, which is 0 when absent. |
| `s.left(n)` | (py-ltseq/ltseq/expr/accessors.py:243) | REMOVE | — | Removed by [§8.6]. Use `slice(0, n)`. |
| `s.right(n)` | (py-ltseq/ltseq/expr/accessors.py:257) | REMOVE | — | Removed by [§8.6]. Use `slice(-n)` for `n > 0`, and `""` for `n = 0`. |
| `s.ord()` | First code point (py-ltseq/ltseq/expr/accessors.py:304; docs/api.md:1594-1601). | REMOVE | — | Removed by [§8.6] without an idiom. Use `t.with_row_index().fold(lambda _, row: ord(row["s"][0]) if row["s"] else None, init=None, into="o", dtype="int64")` ([§9.3]). |
| `s.asc()` | An alias of `ord` (py-ltseq/ltseq/expr/accessors.py:293). | REMOVE | — | Removed with `ord`. Same idiom. |

**Datetime accessor**

| Current API | Problem / Strength | v0.5 Action | Final API | Rationale |
|---|---|---|---|---|
| `.dt` | Temporal accessor (py-ltseq/ltseq/expr/types.py:101-104). | KEEP | `Expr.dt` ([§19.3]) | Unchanged. |
| `TemporalAccessor` | Accessor class (py-ltseq/ltseq/expr/accessors.py:316). | RENAME | `ltseq.typing.DtNamespace` ([§22.2]) | Typing-only, not constructible. |
| `dt.year()` | (py-ltseq/ltseq/expr/accessors.py:336) | KEEP | `DtNamespace.year()` ([§19.3]) | Returns `int32`. Local time for aware values. |
| `dt.month()` | (py-ltseq/ltseq/expr/accessors.py:340) | KEEP | `DtNamespace.month()` ([§19.3]) | As `year`. |
| `dt.day()` | (py-ltseq/ltseq/expr/accessors.py:344) | KEEP | `DtNamespace.day()` ([§19.3]) | As `year`. |
| `dt.hour()` | (py-ltseq/ltseq/expr/accessors.py:348) | KEEP | `DtNamespace.hour()` ([§19.3]) | As `year`. |
| `dt.minute()` | (py-ltseq/ltseq/expr/accessors.py:352) | KEEP | `DtNamespace.minute()` ([§19.3]) | As `year`. |
| `dt.second()` | (py-ltseq/ltseq/expr/accessors.py:356) | KEEP | `DtNamespace.second()` ([§19.3]) | As `year`. |
| `dt.millisecond()` | A float computed with `% 1000` (src/transpiler/mod.rs:887-892). | REDESIGN | `DtNamespace.millisecond()` ([§19.3]) | An `int32` sub-second field. |
| `dt.weekday()` | A float `(dow + 6) % 7`, Monday 0 (src/transpiler/mod.rs:894-899). | REDESIGN | `DtNamespace.weekday()` ([§19.3]) | Same numbering, typed `int32`. |
| `dt.add(days, months, years, hours, minutes, seconds, weeks)` | Mixes calendar and clock units, with int literals only (py-ltseq/ltseq/expr/accessors.py:360-385). | REDESIGN | `DtNamespace.add()` ([§19.4]) | Keyword-only calendar units that accept per-row integer expressions and clamp to month end. A DST gap or overlap raises. |
| `dt.add(hours=, minutes=, seconds=)` | Clock units on `add` (py-ltseq/ltseq/expr/accessors.py:365-367). | REMOVE | — | Removed by [§19.4]. Use `r.ts + timedelta(hours=2)`. |
| `dt.diff(other, unit="day")` | Unit-based difference. Month and year count calendar fields (src/transpiler/mod.rs:822-860). | REMOVE | — | Removed by [§19.4]. Use `(r.a - r.b).dt.days()` or `.dt.total_seconds()`. The `month` and `year` units counted calendar fields; [§19.4] gives their forms. |
| `dt.age()` | Years to today (py-ltseq/ltseq/expr/accessors.py:411). | REMOVE | — | Removed by [§19.4] without an idiom. Use `today().dt.year() - r.b.dt.year() - if_else((today().dt.month() < r.b.dt.month()) \| ((today().dt.month() == r.b.dt.month()) & (today().dt.day() < r.b.dt.day())), 1, 0)`. |
| `DateDiffUnit` (stub alias) | Stub-only alias (py-ltseq/ltseq/expr/accessors.pyi:8). | REMOVE | — | Its method is removed ([§19.4]). Delete the annotation together with `dt.diff`. |

### Mutation

| Current API | Problem / Strength | v0.5 Action | Final API | Rationale |
|---|---|---|---|---|
| `LTSeq.insert(pos, row_dict)` | One row. A negative `pos` is clamped to 0 (py-ltseq/ltseq/mutation_mixin.py:18-23). | REDESIGN | `LTSeq.insert()` ([§7.10]) | Takes one row or a sequence of rows with `list.insert` semantics. `insert(-1, row)` silently moves from the front to before the last row. |
| `LTSeq.delete(predicate_or_pos)` | Drops rows where the predicate is NULL, because it filters on `NOT` (py-ltseq/ltseq/mutation_mixin.py:80-81), and ignores an out-of-range position (:54). | REDESIGN | `LTSeq.delete()` ([§7.10]) | NULL rows are kept, and an out-of-range position raises `LTSeqIndexError`. The NULL change is silent. |
| `LTSeq.update(predicate, **updates)` | Predicate only. With no values it returns the table unchanged (py-ltseq/ltseq/mutation_mixin.py:89, :114). | REDESIGN | `LTSeq.update()` ([§7.10]) | Also takes a position, and a call with no values raises. Column types never change. |
| `LTSeq.modify(pos, **updates)` | Positional twin of `update` (py-ltseq/ltseq/mutation_mixin.py:125). | MERGE | `LTSeq.update(pos, ...)` ([§7.10]) | Removed by [§7.10]. |

### Cursor / streaming

| Current API | Problem / Strength | v0.5 Action | Final API | Rationale |
|---|---|---|---|---|
| `LTSeq.scan(path, has_header=True)` | Separate streaming reader for CSV (py-ltseq/ltseq/io_ops.py:123). | REMOVE | — | Removed by [§15.1]. Use `LTSeq.read_csv(path).to_batches()`. |
| `LTSeq.scan_parquet(path)` | Separate streaming reader for Parquet (py-ltseq/ltseq/io_ops.py:147). | REMOVE | — | Removed by [§15.1]. Use `LTSeq.read_parquet(path).to_batches()`. |
| `Cursor` (class) | A custom batch iterator (py-ltseq/ltseq/cursor.py:17). | REMOVE | — | Removed by [§2.2] and [§15.1]. The replacement is `pa.RecordBatchReader`. |
| `Cursor.__iter__` / `__next__` | Yields `RecordBatch` (py-ltseq/ltseq/cursor.py:46, :50). | REMOVE | — | Iterate the reader: `for b in t.to_batches(): ...`. |
| `Cursor.schema` | `dict[str, str]` (py-ltseq/ltseq/cursor.py:69). | REMOVE | — | Use `reader.schema`. |
| `Cursor.columns` | (py-ltseq/ltseq/cursor.py:76) | REMOVE | — | Use `reader.schema.names`. |
| `Cursor.source` | The path (py-ltseq/ltseq/cursor.py:81). | REMOVE | — | Keep the `path` passed to `read_csv` or `read_parquet`. |
| `Cursor.exhausted` | (py-ltseq/ltseq/cursor.py:86) | REMOVE | — | No equivalent. The reader ends with `StopIteration`. |
| `Cursor.to_pandas()` | Drains to pandas (py-ltseq/ltseq/cursor.py:90). | REMOVE | — | Use `reader.read_pandas()`. |
| `Cursor.to_arrow()` | Drains to Arrow (py-ltseq/ltseq/cursor.py:110). | REMOVE | — | Use `reader.read_all()`. |
| `Cursor.count()` | Drains and counts (py-ltseq/ltseq/cursor.py:127). | REMOVE | — | Use `t.count()`. |

### Partitioning

| Current API | Problem / Strength | v0.5 Action | Final API | Rationale |
|---|---|---|---|---|
| `LTSeq.partition(*args, by=None)` | Returns a wrapper type (py-ltseq/ltseq/core.py:642). Fetching a partition formats the key into SQL text, so date keys give empty tables and timestamp keys raise (#258). | REDESIGN | `LTSeq.partition()` ([§14.5]) | Returns a plain `dict` with sorted keys and lazy values; aware timestamp keys are UTC instants, so each instant has its own key. |
| `partition(by=lambda r: ...)` | Computed partition key (docs/api.md:1259-1262). | REMOVE | — | Removed by [§14.5]. Use `t.derive(k=...).partition("k")`. |
| `PartitionedTable` | Wrapper class (py-ltseq/ltseq/partitioning.py:212). | REMOVE | — | Removed by [§2.2] and [§14.5]. The replacement is `dict`. |
| `SQLPartitionedTable` | Wrapper class, exported only by the stub (py-ltseq/ltseq/partitioning.py:13). | REMOVE | — | Same as `PartitionedTable`. |
| `pt[key]` / `__getitem__` | (py-ltseq/ltseq/partitioning.py:46, :269) | REMOVE | — | Use `d[key]` on `d = t.partition("k")`. |
| `pt.keys()` | (py-ltseq/ltseq/partitioning.py:104, :287) | REMOVE | — | Use `d.keys()`. |
| `pt.values()` | (py-ltseq/ltseq/partitioning.py:132, :300) | REMOVE | — | Use `d.values()`. |
| `pt.items()` | (py-ltseq/ltseq/partitioning.py:144, :309) | REMOVE | — | Use `d.items()`. |
| `iter(pt)` / `__iter__` | Yields the partition tables (py-ltseq/ltseq/partitioning.py:158-165, :322-329). | REMOVE | — | Use `d.values()`. Iterating the dict itself yields keys, so the old loop silently changes meaning. |
| `len(pt)` | (py-ltseq/ltseq/partitioning.py:167, :331) | REMOVE | — | Use `len(d)`. |
| `pt.map(fn)` | Returns a precomputed wrapper (py-ltseq/ltseq/partitioning.py:179, :340). | REMOVE | — | Use `{k: fn(v) for k, v in t.partition("k").items()}` ([§14.5]). |
| `pt.to_list()` | (py-ltseq/ltseq/partitioning.py:202, :369) | REMOVE | — | Use `list(d.values())`. |

### Linking

| Current API | Problem / Strength | v0.5 Action | Final API | Rationale |
|---|---|---|---|---|
| `LTSeq.link(other, on, as_, join_type)` | A lazy prefix-aliased join with its own wrapper type (py-ltseq/ltseq/core.py:560; docs/api.md:1132-1143). | MERGE | `LTSeq.join(alias=)` ([§12.1]) | Superseded by `join(..., alias=...)`, which returns an ordinary `LTSeq` (ADR 0011 row of [§1.4]). |
| `LinkedTable` | Wrapper class (py-ltseq/ltseq/linking.py:11). | REMOVE | — | Removed by [§2.2] and [§3.1]. Use `t.join(o, ..., alias="a")`. |
| `LinkedTable.to_ltseq()` | (py-ltseq/ltseq/linking.py:119) | REMOVE | — | The join already returns an `LTSeq`. |
| `LinkedTable.collect()` | (py-ltseq/ltseq/linking.py:123) | REMOVE | — | Call `.collect()` on the join result. |
| `LinkedTable.show()` | (py-ltseq/ltseq/linking.py:131) | REMOVE | — | Call `.show()` on the join result. |
| `len(linked)` | (py-ltseq/ltseq/linking.py:135) | REMOVE | — | Call `.count()` on the join result. |
| `LinkedTable.filter()` | (py-ltseq/ltseq/linking.py:139) | REMOVE | — | Call `.filter()` on the join result, using `alias_col` names. |
| `LinkedTable.select()` | (py-ltseq/ltseq/linking.py:149) | REMOVE | — | Call `.select()` on the join result. |
| `LinkedTable.derive()` | (py-ltseq/ltseq/linking.py:160) | REMOVE | — | Call `.derive()` on the join result. |
| `LinkedTable.sort()` | (py-ltseq/ltseq/linking.py:164) | REMOVE | — | Call `.sort()` on the join result. |
| `LinkedTable.slice()` | (py-ltseq/ltseq/linking.py:168) | REMOVE | — | Call `.slice()` on the join result. |
| `LinkedTable.distinct()` | (py-ltseq/ltseq/linking.py:172) | REMOVE | — | Call `.distinct()` on the join result. |
| `LinkedTable.link()` | Chained link (py-ltseq/ltseq/linking.py:177). | REMOVE | — | Chain a second `join(..., alias=...)`. |
| `r.col.lookup(table, column, join_key=None)` | Expression-level lookup through a global table registry (py-ltseq/ltseq/expr/base.py:954; py-ltseq/ltseq/expr/lookup_expr.py:63-78). | REMOVE | — | Removed by [§12.1]. Use `t.join(o, left_on="k", right_on="id", how="left", validate="m:1")` followed by `select`. |

### Exceptions

| Current API | Problem / Strength | v0.5 Action | Final API | Rationale |
|---|---|---|---|---|
| `LTSeqError` | Root of the hierarchy (py-ltseq/ltseq/exceptions.py:10). | KEEP | `LTSeqError` ([§20.1]) | Still the root, never raised directly. |
| `SortRequiredError` | `LTSeqError, ValueError` (py-ltseq/ltseq/exceptions.py:14). | KEEP | `SortRequiredError` ([§20.1]) | Now based on `LTSeqValueError`, so it is still a `ValueError`. |
| `SchemaMismatchError` | `LTSeqError, ValueError` (py-ltseq/ltseq/exceptions.py:23). | KEEP | `SchemaMismatchError` ([§20.1]) | Same, through `LTSeqValueError`. |
| `ColumnNotFoundError` | `LTSeqError, ValueError, AttributeError` (py-ltseq/ltseq/exceptions.py:27). | KEEP | `ColumnNotFoundError` ([§20.1]) | Still a `ValueError` and an `AttributeError`, now through `LTSeqValueError`. |

### Added in v0.5

| Current API | Problem / Strength | v0.5 Action | Final API | Rationale |
|---|---|---|---|---|
| (none) | Sort metadata is an anonymous tuple list (py-ltseq/ltseq/core.py:461-462). | ADD | `SortKey` ([§3.1], [§9.7]) | A named tuple that also carries `nulls_last`. |
| (none) | `LiteralExpr` is declared in the `ltseq` stub but not bound at runtime (py-ltseq/ltseq/__init__.pyi:20). | ADD | `lit()` ([§8.3]) | Needed for `lit(None).cast(...)` and constant conditions ([§3.4]). |
| (none) | The package defines no `__version__` (py-ltseq/ltseq/__init__.py:1-120). | ADD | `ltseq.__version__` ([§2.1]) | A PEP 440 string. |
| (none) | There is no public typing module; `py-ltseq/ltseq/_typing.py` is private. | ADD | `ltseq.typing` ([§2.2]) | A typing-only module with no side effects. |
| (none) | The row proxy offers attribute access only (py-ltseq/ltseq/expr/proxy.py:30). | ADD | `ltseq.typing.Row` ([§8.1], [§22.2]) | `r["any name"]` reaches any column. |
| (none) | Group proxies are split by context (py-ltseq/ltseq/grouping/proxies/). | ADD | `ltseq.typing.Group` ([§8.1]) | One aggregate-context protocol. |
| (none) | `rolling(n)` returns an untyped `CallExpr` (py-ltseq/ltseq/expr/types.py:163-175). | ADD | `ltseq.typing.Rolling` ([§10.1]) | Typed builder. |
| (none) | `var` is not a rolling aggregate (py-ltseq/ltseq/expr/types.py:30). | ADD | `Rolling.var()` ([§10.1], [§22.2]) | Completes the set with `std`. |
| (none) | Stub aliases cover only option literals (py-ltseq/ltseq/__init__.pyi:5-13). | ADD | `PathLike`, `DTypeLike`, `Literal`, `IntoExpr`, `ExprFn`, `AggFn`, `RowExpr`, `AggExpr` ([§2.3]) | Shared annotation vocabulary. |
| (none) | Baseline defines only four exception classes (py-ltseq/ltseq/exceptions.py:10-27). | ADD | `LTSeqTypeError` ([§20.1]) | Catchable as `TypeError` and as `LTSeqError`. |
| (none) | Baseline defines only four exception classes (py-ltseq/ltseq/exceptions.py:10-27). | ADD | `LTSeqValueError` ([§20.1]) | Common base of the value-error classes. |
| (none) | `assume_sorted` is never checked (docs/api.md:531-541). | ADD | `OrderViolationError` ([§20.1]) | Raised by the validation in [§9.5]. |
| (none) | `join` has no `validate` (py-ltseq/ltseq/joins.py:166-176). | ADD | `DuplicateKeyError` ([§20.1]) | Raised by `join(validate=)`. |
| (none) | Baseline defines only four exception classes (py-ltseq/ltseq/exceptions.py:10-27). | ADD | `CastError` ([§20.1]) | Raised by inexact implicit conversions and failed casts. |
| (none) | An out-of-range position is ignored (py-ltseq/ltseq/mutation_mixin.py:54). | ADD | `LTSeqIndexError` ([§20.1]) | Raised for a position outside the table in `delete` and `update`. |
| (none) | Overflow wraps (docs/api.md:1309). | ADD | `ArithmeticOverflowError` ([§20.1]) | Raised by checked arithmetic ([§17.1]). |
| (none) | Baseline defines only four exception classes (py-ltseq/ltseq/exceptions.py:10-27). | ADD | `DivisionByZeroError` ([§20.1]) | Raised by `//`, `%` and decimal `/` ([§17.3]). |
| (none) | Writers raise `RuntimeError`/`ValueError` (docs/api.md:258-261). | ADD | `LTSeqIOError` ([§20.1]) | I/O failures, also an `OSError`. |
| (none) | Baseline defines only four exception classes (py-ltseq/ltseq/exceptions.py:10-27). | ADD | `SourceNotFoundError` ([§20.1]) | Raised at the call ([§4.1]), also a `FileNotFoundError`. |
| (none) | Baseline defines only four exception classes (py-ltseq/ltseq/exceptions.py:10-27). | ADD | `ExecutionError` ([§20.1]) | Internal errors, also a `RuntimeError`. |
| (none) | Only `sort_keys` describes order (py-ltseq/ltseq/core.py:461-462). | ADD | `LTSeq.is_ordered` ([§9.7]) | Plan-time order state. |
| (none) | There is no row-index method at baseline. | ADD | `LTSeq.with_row_index()` ([§9.9]) | Declares `(index ascending)` for the sequence tier. |
| (none) | Streaming needs `Cursor` and the `scan*` readers (py-ltseq/ltseq/io_ops.py:123, :147). | ADD | `LTSeq.to_batches()` ([§15.1]) | A standard `pa.RecordBatchReader`. |
| (none) | No `__bool__`, so `bool(t)` falls back to `__len__`, which executes a count (py-ltseq/ltseq/core.py:315). | ADD | `LTSeq.__bool__` ([§3.2]) | Raises `TypeError` to stop `if t:` mistakes. |
| (none) | No `__copy__`/`__deepcopy__` is defined. | ADD | `copy.copy(t)` / `copy.deepcopy(t)` ([§3.2]) | Same plan, never executes. |
| (none) | No pickle support is defined (no `__reduce__`/`__getstate__` in py-ltseq/ltseq or src). | ADD | `pickle.dumps(t)` ([§16.6]) | A materialized snapshot. |
| (none) | `Expr` defines no `__round__`/`__floor__`/`__ceil__` (py-ltseq/ltseq/expr/base.py:564). | ADD | `Expr.__round__`/`__floor__`/`__ceil__` ([§8.2]) | Python builtins map to the methods. |
| (none) | No `is_nan` at baseline, and `is_null()` does not match NaN (docs/api.md:1308). | ADD | `Expr.is_nan()` ([§8.5]) | NaN is not NULL ([§18]). |
| (none) | No `fill_nan` at baseline. | ADD | `Expr.fill_nan()` ([§8.5]) | Leaves NULL alone. |
| (none) | No `try_cast` at baseline; `cast` is the only conversion (py-ltseq/ltseq/expr/base.py:831). | ADD | `Expr.try_cast()` ([§8.5]) | NULL on failure. |
| (none) | No distinct-count aggregate is documented (docs/api.md:1218). | ADD | `Expr.n_unique()` ([§14.2]) | NaN counts as one value. |
| (none) | `mode` is reachable only through the catch-all and returns the minimum (src/ops/aggregation.rs:277-288). | ADD | `Expr.mode()` ([§14.2]) | A true mode, with the smallest value winning a tie. |
| (none) | Conditional aggregates need the `*_if` functions (py-ltseq/ltseq/expr/base.py:112-187). | ADD | `where=` on every aggregate ([§14.2]) | Replaces `*_if`. |
| (none) | `.over()` raises for calls that are not window functions (py-ltseq/ltseq/expr/types.py:229-233). | ADD | `.over()` on aggregates ([§10.3]) | Partition totals on every row. |
| (none) | `min_periods` raises (docs/api.md:602, :605). | ADD | `rolling(min_periods=)` ([§10.1]) | Controls the partial-frame rule. |
| (none) | `pct_change()` takes no argument (py-ltseq/ltseq/expr/base.py:918). | ADD | `pct_change(n=)` ([§10.1]) | Matches `shift` and `diff`. |
| (none) | NULL placement is fixed: NULL sorts as the largest value (docs/api.md:487). | ADD | `nulls_last=` on `sort`, `assume_sorted`, `is_sorted_by` and `over` ([§9.4], [§10.4]) | Explicit NULL placement. |
| (none) | No `replace_regex` at baseline. | ADD | `StrNamespace.replace_regex()` ([§8.6]) | Complements `contains_regex`. |
| (none) | Strip removes spaces only (src/transpiler/mod.rs:588-591). | ADD | `strip`/`lstrip`/`rstrip(chars=)` ([§8.6]) | Python's `chars` argument. |
| (none) | The only sub-second field is `millisecond` (py-ltseq/ltseq/expr/accessors.py:424). | ADD | `DtNamespace.microsecond()` ([§19.3]) | Sub-second field. |
| (none) | Same gap as `microsecond`. | ADD | `DtNamespace.nanosecond()` ([§19.3]) | Sub-second field. |
| (none) | `TemporalAccessor` has no such method (py-ltseq/ltseq/expr/accessors.py:316). | ADD | `DtNamespace.truncate()` ([§19.4]) | Calendar bucketing, in local time. |
| (none) | `TemporalAccessor` has no such method (py-ltseq/ltseq/expr/accessors.py:316). | ADD | `DtNamespace.total_seconds()` ([§19.4]) | Replaces `dt.diff` with clock units. |
| (none) | Same gap as `total_seconds`. | ADD | `DtNamespace.days()` ([§19.4]) | Floored whole days. |
| (none) | `TemporalAccessor` has no such method (py-ltseq/ltseq/expr/accessors.py:316). | ADD | `DtNamespace.replace_time_zone()` ([§19.4]) | Attaches or drops a zone. |
| (none) | Same gap as `replace_time_zone`. | ADD | `DtNamespace.convert_time_zone()` ([§19.4]) | Changes the zone and keeps the instant. |
| (none) | `distinct` takes no `keep` (py-ltseq/ltseq/transforms.py:773). | ADD | `distinct(keep=)` ([§7.6]) | `"last"`, or `"any"` for unordered tables. |
| (none) | `step` always starts at row 0 (py-ltseq/ltseq/advanced_ops.py:175). | ADD | `step(offset=)` ([§9.8]) | `rows[offset::n]`. |
| (none) | `explain_plan` always returns both plans (py-ltseq/ltseq/core.py:341). | ADD | `explain(physical=)` ([§7.8]) | The physical plan is shown on request. |
| (none) | `join` has no `validate` (py-ltseq/ltseq/joins.py:166-176). | ADD | `join(validate=)` ([§12.1]) | Raises `DuplicateKeyError`. |
| (none) | `cross` is not an option (py-ltseq/ltseq/__init__.pyi:5). | ADD | `join(how="cross")` ([§12.1]) | Cartesian product. |
| (none) | Semi and anti joins take same-name keys only (py-ltseq/ltseq/joins.py:534, :554). | ADD | `semi_join`/`anti_join(left_on=, right_on=)` ([§12.2]) | Matches `join`. |
| (none) | The as-of `by` keys must share names (py-ltseq/ltseq/joins.py:323). | ADD | `asof_join(left_by=, right_by=)` ([§12.3]) | Matches `left_on`/`right_on`. |
| (none) | `asof_join` has no `tolerance` (py-ltseq/ltseq/joins.py:314-326). | ADD | `asof_join(tolerance=)` ([§12.3]) | Drops matches that are too far away. |
| (none) | `asof_join` has no `allow_exact_matches` (py-ltseq/ltseq/joins.py:314-326). | ADD | `asof_join(allow_exact_matches=)` ([§12.3]) | Strict `<` or `>`. |
| (none) | `intersect` takes no multiset option (py-ltseq/ltseq/advanced_ops.py:63). | ADD | `intersect(distinct=)` ([§13.2]) | Multiset semantics. |
| (none) | `except_` takes no multiset option (py-ltseq/ltseq/advanced_ops.py:96). | ADD | `difference(distinct=)` ([§13.2]) | Multiset semantics. |
| (none) | `pivot` always executes to discover its columns (py-ltseq/ltseq/advanced_ops.py:360). | ADD | `pivot(column_values=)` ([§14.6]) | Keeps `pivot` lazy. |
| (none) | `fold` takes no `dtype` (py-ltseq/ltseq/transforms.py:652-659). | ADD | `fold(dtype=)` ([§10.7]) | Exact output type. |
| (none) | `group_ordered` takes only a key lambda (py-ltseq/ltseq/aggregation.py:111). | ADD | `group_ordered(starts_when=)` ([§11.1]) | Session-style splits. |
| (none) | The group id is the internal `__group_id__` column (docs/api.md:891). | ADD | `flatten(group_id=)` ([§11.2]) | A user-named group number. |
| (none) | CSV types are always inferred (docs/api.md:155). | ADD | `read_csv(schema=)` ([§4.2]) | Declared types. |
| (none) | `read_csv` takes no `delimiter` (py-ltseq/ltseq/io_ops.py:42). | ADD | `read_csv(delimiter=)` ([§4.2]) | One ASCII character. |
| (none) | `from_dict` takes no `schema` (py-ltseq/ltseq/io_ops.py:201). | ADD | `from_dict(schema=)` ([§4.6]) | Declared, exact types. |
| (none) | The index policy is fixed (py-ltseq/ltseq/io_ops.py:386). | ADD | `from_pandas(preserve_index=)` ([§4.5]) | Opt-in index columns. |
| (none) | `write_csv` takes only a path (py-ltseq/ltseq/io_ops.py:282). | ADD | `write_csv(include_header=)` ([§16.5]) | Headerless output. |
| (none) | `write_csv` takes only a path (py-ltseq/ltseq/io_ops.py:282). | ADD | `write_csv(delimiter=)` ([§16.5]) | Matches `read_csv`. |
| (none) | `to_pandas` takes no arguments (py-ltseq/ltseq/core.py:231). | ADD | `to_pandas(dtype_backend=)` ([§16.2]) | `pyarrow` or `numpy_nullable`. |
| (none) | The `seq` column is always `value` (py-ltseq/ltseq/utils.py:41). | ADD | `range(name=)` ([§4.7]) | A user-chosen column name. |

### Gaps found

Building the inventory against the draft contract turned up places where the contract was wrong, removed a name without saying how to replace it, or was silent. Each is fixed in the contract as presented. The list records what changed so a reviewer can check it.

**Contract errors, now fixed.**

- `union`. [§13.2] and [§23.16] gave `concat(...).distinct()` as its replacement. v0.4 `union` is `UNION ALL` and an alias of `concat` (py-ltseq/ltseq/advanced_ops.py:26-61; docs/api.md:1001), so that idiom changed results for any input with duplicates. [§13.2] now gives `concat`, and `concat(...).distinct()` as SQL's `UNION`.
- `except_` and `subtract`. [§13.2] named `difference` as their successor without saying that `difference` deduplicates and treats NULL as equal to NULL. v0.4 kept every unmatched left row and never matched NULL. [§13.2] now gives that behavior as `anti_join(other, on=t.columns)`.
- `contain`. [§13.2] gave `t.filter(lambda r: r.k.is_in(values))`, which returns a table, where v0.4 returns a `bool` that is `True` only when every value occurs (py-ltseq/ltseq/advanced_ops.py:203-228). [§13.2] now gives the count comparison in the Set algebra table.
- `pos` and `split_part`. [§8.6] mapped them to `find` and `split`, but both are 1-based (src/transpiler/mod.rs:711; docs/api.md:1557-1582). [§8.6] now gives `find(s) + 1` and `split(d, n - 1)`.
- `contains`, `starts_with`/`ends_with`, `pad_left`/`pad_right` and `regex_match`. [§8.6] listed them as compositions with no replacement method. [§8.6] now lists them as renames and says that `rjust`/`ljust` never truncate, where `pad_*` did (docs/api.md:1549-1555).
- `partition_by=` on `cum_sum`/`cum_min`/`cum_max` (docs/api.md:633). [§1.4] removes the keyword from every window method, but [§10.1] named only `shift`, `rolling` and `diff`. [§10.1] now names all six.
- `seq`. [§8.4] listed it as "not part of v0.5" without naming `LTSeq.range` ([§4.7]) as its successor. It now does.
- `dt.diff`. Its `month` and `year` units count calendar fields (src/transpiler/mod.rs:822-860), which "subtract and use `total_seconds()` or `days()`" does not reproduce. [§19.4] now gives both forms.

**Removals that were implicit, now explicit.** Each was removed only by the shape of a [§22] signature, so [§24.1] had nothing to test against.

- Lambda or `Expr` keys on `sort`, `assume_sorted`, `distinct`, `group_by`, `over`, `semi_join`, `anti_join` and `asof_join`, and the keywords `desc=`, `shift(default=)`, `pivot(agg_fn=)` and window `partition_by=`. The [§22] preamble now states that an argument of a type its annotation does not admit raises `LTSeqTypeError`, and a keyword the signature lacks raises `TypeError`. S2 tests one of each.
- `LTSeq._repr_html_` (py-ltseq/ltseq/core.py:178-198), a notebook hook that executes the plan. [§3.2] now says no display hook is defined, and S3 tests it.
- The group helpers `g.sum("col")` and its siblings, `g.first()`/`g.last()` and `g.none(pred)` (py-ltseq/ltseq/grouping/proxies/derive_proxy.py:34-74; py-ltseq/ltseq/grouping/proxies/filter_proxy.py:219). [§8.1] now says these names read as columns and gives their forms.
- `when(c).then(v)` (py-ltseq/ltseq/expr/base.py:76-86). [§8.3] now says it is removed.

**Names outside [§22] with no contract text.** [§22] is closed, so these are not public and need no further text. They are listed so the removal tests of [§24.1] can enumerate them.

- `LTSeq.python_schema` and `LTSeq.dtypes` (py-ltseq/ltseq/core.py:412-413, :447-448).
- `to_cursor()`, referenced at docs/api.md:331 and py-ltseq/ltseq/core.py:205 but defined nowhere.
- `Expr.serialize()` (py-ltseq/ltseq/expr/base.py:573).
- `StringAccessor` and `TemporalAccessor` (py-ltseq/ltseq/expr/accessors.py:11, :316), whose successors are `ltseq.typing.StrNamespace` and `DtNamespace` ([§22.2]).
- The stub-only aliases `DateDiffUnit`, `JoinHow`, `JoinStrategy`, `AsofStrategy`, `Compression` and `PivotAggFn` (py-ltseq/ltseq/expr/accessors.pyi:8; py-ltseq/ltseq/__init__.pyi:5-13).
- `str_char` (py-ltseq/ltseq/expr/base.py:332).
- `NestedTable.to_pandas()` (py-ltseq/ltseq/grouping/nested_table.py:62).
- `ltseq.ltseq_core` (pyproject.toml:18), which becomes the private `ltseq._ltseq_core` ([§2.2]).

**Removals the contract makes without an idiom.** The tables above supply one for `ord` and `asc`, `rand`, `gcd`, `lcm` and `factorial`, `char`, `concat_ws`, `top_k`, `skew` and `dt.age`.

**Silent behavior changes under an unchanged spelling.** None of these raises; a migration guide must call them out:

- `str.split(d, i)`: 1-based becomes 0-based, and out of range becomes NULL instead of `""`.
- `rolling(n).<agg>()`: early partial frames become NULL, except `count`.
- Integer `/` becomes true division, and `%` takes the divisor's sign (docs/api.md:1306).
- `pct_change` on integer columns stops truncating.
- `insert` with a negative `pos` stops clamping to 0.
- `delete(pred)` keeps rows where `pred` is NULL.
- `sort(..., descending=True)` puts NULLs last (acknowledged in [§9.4]).
- `from_pandas` drops a non-range index by default.
- Headerless `read_csv` names shift from `column_0…` to `column_1…`.
- Iterating `t.partition(...)` yields keys instead of tables.
- `NestedTable.filter` no longer merges equal-key groups that a removed group separated.
- `str.strip`/`lstrip`/`rstrip` also trim tabs and newlines.
- `str.is*` accept non-ASCII letters and digits, and `islower`/`isupper` are FALSE for strings without cased characters.
- `x.mode()` changes from the minimum to the true mode.
- `to_pandas()` returns pyarrow-backed dtypes.

**Stub and runtime mismatches at baseline.** These matter for building the [§24.1] removal list:

- `py-ltseq/ltseq/__init__.pyi` declares names the runtime `ltseq` namespace does not bind (py-ltseq/ltseq/__init__.py:1-120): `gcd`, `lcm`, `factorial`, `char`, `str_char`, `concat_ws`, `LiteralExpr`, `LookupExpr`, `NestedSchemaProxy`, `SQLPartitionedTable` and `Cursor`.
- `LTSeq.stateful_scan` (py-ltseq/ltseq/__init__.pyi:207-212) exists nowhere at runtime.
- `PivotAggFn` (py-ltseq/ltseq/__init__.pyi:13) lacks `first`/`last`, which docs/api.md:1291 documents.

### Counts

| Action | Rows |
|---|---|
| KEEP | 107 |
| RENAME | 14 |
| REDESIGN | 66 |
| MERGE | 35 |
| REMOVE | 107 |
| ADD | 69 |
| Total | 398 |

<!-- v0.5-modular:footer -->

---

Previous: [Audit of the current API](audit.md) · [Index](../README.md) · Next: [Decisions on the eleven open semantic issues](decisions.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§1.4]: ../contract/overview.md#14-relation-to-earlier-decisions
[§2.1]: ../contract/public-surface.md#21-the-ltseq-namespace
[§2.2]: ../contract/public-surface.md#22-other-modules
[§2.3]: ../contract/public-surface.md#23-type-aliases-used-in-signatures
[§3.1]: ../contract/public-surface.md#31-types
[§3.2]: ../contract/public-surface.md#32-ltseq-invariants
[§3.3]: ../contract/public-surface.md#33-nestedtable-and-groupby-invariants
[§3.4]: ../contract/public-surface.md#34-lambdas-and-proxies
[§4.1]: ../contract/loading-and-laziness.md#41-source-paths
[§4.2]: ../contract/loading-and-laziness.md#42-ltseqread_csv
[§4.3]: ../contract/loading-and-laziness.md#43-ltseqread_parquet
[§4.4]: ../contract/loading-and-laziness.md#44-ltseqfrom_arrow
[§4.5]: ../contract/loading-and-laziness.md#45-ltseqfrom_pandas
[§4.6]: ../contract/loading-and-laziness.md#46-ltseqfrom_dict-and-ltseqfrom_rows
[§4.7]: ../contract/loading-and-laziness.md#47-ltseqrange
[§5.2]: ../contract/loading-and-laziness.md#52-ltseqcollect
[§6.1]: ../contract/schema-and-table-operations.md#61-schema-and-columns
[§7.1]: ../contract/schema-and-table-operations.md#71-filter
[§7.2]: ../contract/schema-and-table-operations.md#72-select
[§7.3]: ../contract/schema-and-table-operations.md#73-derive
[§7.4]: ../contract/schema-and-table-operations.md#74-rename
[§7.5]: ../contract/schema-and-table-operations.md#75-drop
[§7.6]: ../contract/schema-and-table-operations.md#76-distinct
[§7.7]: ../contract/schema-and-table-operations.md#77-pipe
[§7.8]: ../contract/schema-and-table-operations.md#78-explain
[§7.9]: ../contract/schema-and-table-operations.md#79-count-and-show
[§7.10]: ../contract/schema-and-table-operations.md#710-value-level-edits-insert-delete-update
[§8]: ../contract/expressions.md#8-expression-dsl
[§8.1]: ../contract/expressions.md#81-contexts-and-proxies
[§8.2]: ../contract/expressions.md#82-operators
[§8.3]: ../contract/expressions.md#83-conditional-and-null-functions
[§8.4]: ../contract/expressions.md#84-math-functions
[§8.5]: ../contract/expressions.md#85-general-expr-methods
[§8.6]: ../contract/expressions.md#86-string-methods-str
[§8.7]: ../contract/expressions.md#87-the-closed-method-set
[§9]: ../contract/ordering.md#9-ordering-contract
[§9.3]: ../contract/ordering.md#93-order-requirements
[§9.4]: ../contract/ordering.md#94-sort
[§9.5]: ../contract/ordering.md#95-assume_sorted
[§9.6]: ../contract/ordering.md#96-is_sorted_by
[§9.7]: ../contract/ordering.md#97-sort_keys-and-is_ordered
[§9.8]: ../contract/ordering.md#98-positional-selection
[§9.9]: ../contract/ordering.md#99-with_row_index
[§10]: ../contract/windows-and-grouping.md#10-windows-and-ordered-computation
[§10.1]: ../contract/windows-and-grouping.md#101-window-methods
[§10.2]: ../contract/windows-and-grouping.md#102-ranking-functions
[§10.3]: ../contract/windows-and-grouping.md#103-aggregates-over-windows
[§10.4]: ../contract/windows-and-grouping.md#104-over
[§10.5]: ../contract/windows-and-grouping.md#105-search_first
[§10.6]: ../contract/windows-and-grouping.md#106-search_pattern
[§10.7]: ../contract/windows-and-grouping.md#107-fold
[§11.1]: ../contract/windows-and-grouping.md#111-group_ordered
[§11.2]: ../contract/windows-and-grouping.md#112-nestedtable
[§12.1]: ../contract/joins-and-sets.md#121-join
[§12.2]: ../contract/joins-and-sets.md#122-semi_join-and-anti_join
[§12.3]: ../contract/joins-and-sets.md#123-asof_join
[§13.1]: ../contract/joins-and-sets.md#131-concat
[§13.2]: ../contract/joins-and-sets.md#132-intersect-and-difference
[§14]: ../contract/aggregation.md#14-aggregation-partitioning-and-pivot
[§14.1]: ../contract/aggregation.md#141-group_by-and-groupbyagg
[§14.2]: ../contract/aggregation.md#142-aggregate-expressions
[§14.3]: ../contract/aggregation.md#143-ltseqagg
[§14.4]: ../contract/aggregation.md#144-nestedtable-aggregation
[§14.5]: ../contract/aggregation.md#145-partition
[§14.6]: ../contract/aggregation.md#146-pivot
[§15.1]: ../contract/streaming-and-output.md#151-to_batches
[§15.2]: ../contract/streaming-and-output.md#152-iteration
[§16.1]: ../contract/streaming-and-output.md#161-to_arrow
[§16.2]: ../contract/streaming-and-output.md#162-to_pandas
[§16.3]: ../contract/streaming-and-output.md#163-to_dicts
[§16.4]: ../contract/streaming-and-output.md#164-arrow-pycapsule-stream
[§16.5]: ../contract/streaming-and-output.md#165-writers
[§16.6]: ../contract/streaming-and-output.md#166-pickle
[§17.1]: ../contract/numeric-null-temporal.md#171-integer-and-decimal-results-are-checked
[§17.3]: ../contract/numeric-null-temporal.md#173-arithmetic-operators
[§17.4]: ../contract/numeric-null-temporal.md#174-types-of-mixed-operands
[§17.6]: ../contract/numeric-null-temporal.md#176-explicit-casts
[§18]: ../contract/numeric-null-temporal.md#18-null-nan-and-boolean-logic
[§19.3]: ../contract/numeric-null-temporal.md#193-dt-fields
[§19.4]: ../contract/numeric-null-temporal.md#194-dt-methods
[§19.5]: ../contract/numeric-null-temporal.md#195-clock-functions
[§20.1]: ../contract/errors-and-performance.md#201-exception-classes
[§22]: ../contract/api-reference.md#22-complete-canonical-api-reference
[§22.1]: ../contract/api-reference.md#221-ltseq
[§22.2]: ../contract/api-reference.md#222-ltseqtyping
[§23.16]: ../contract/examples-semantics.md#2316-set-and-bag-operations
[§24.1]: ../contract/acceptance.md#241-acceptance-criteria
[Deliverable M]: history/review-closures.md#m-fourth-review-closure

<!-- /v0.5-modular:links -->
