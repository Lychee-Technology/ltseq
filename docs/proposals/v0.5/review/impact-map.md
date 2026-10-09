<!-- v0.5-modular:header -->

# v0.5 review: Examples, test matrix and implementation impact map (E, F, G)

[Index](../README.md) › Review · Previous: [Trade-off record](trade-offs.md) · Next: [Adversarial review and final consistency check (H, I)](history/adversarial-review.md)

**Non-normative.** Part of the [review](../README.md#ltseq-v05-api-review) behind the contract. It records evidence, reasoning and decisions; the behavior itself is specified by the contract modules.

**Scope.** How the twelve required scenarios map to the [§23] examples ([Deliverable E]), a summary of the [§24] test matrix ([Deliverable F]), and the implementation impact map M0–M30 with issues, likely files, tests, dependencies and priorities ([Deliverable G]).

**Most cited from here.** [§24] Acceptance criteria and contract test matrix · [§17] Numeric semantics and literals · [§16] Output and interchange · [§4] Loading

<!-- /v0.5-modular:header -->

## E. Canonical examples

The examples are [§23] of the contract, and they are normative: each states its results, types and exceptions, and each is a test ([§24.1]). Every example uses only v0.5 names. The table maps the twelve required scenarios to them. Examples 23.8, 23.9 and 23.11 to 23.16 cover guarantees outside the twelve scenarios: partitions across processes, `fold`, NULL and NaN, checked arithmetic, time zones, error stages, order state and set operations.

| Required scenario | Contract example | What it shows |
|---|---|---|
| 1. CSV and Parquet lazy processing | 23.17, 23.10 | Two CSV files read with one type override; nothing executes until `write_parquet`, and a bad value past the inference sample raises `CastError` there. A Parquet directory streams in bounded memory. |
| 2. Select, filter, derive | 23.17 | `filter` with `&`, `derive` from plain functions, `select` of names; the output type `decimal128(33, 2)` is known before any row is read |
| 3. Expression composition | 23.17, 23.1, 23.3 | A plain function reused inside a `when` chain, `.str` and `.dt` in predicates and derivations, a window inside a comparison (`r.ts - r.ts.shift(1) > timedelta(...)`), a window aggregate inside arithmetic |
| 4. Window with partition and order | 23.3, 23.2 | `rank` and `row_number` with `.over(partition_by=, order_by=, descending=)`, `sum().over(partition_by=)`, and `shift(1).over(order_by=)` on an unsorted table, all leaving rows in place; the plan-time errors for an aggregate without `.over()` |
| 5. Ordered time series | 23.2 | `sort`, then `shift(fill_value=)` and `pct_change` in table order; the same call on the unsorted table raises `SortRequiredError` at plan time |
| 6. Consecutive groups | 23.1 | `group_ordered` with a key and `starts_when`, `filter` on group size, `agg`, and `flatten(group_id=)` |
| 7. Rolling and cumulative | 23.2 | `rolling(3).mean()` with NULL warm-up rows and `cum_max` |
| 8. Pattern search | 23.4 | Overlapping `search_pattern` matches, a NULL first step that starts no match, `search_first` returning a 0-row table |
| 9. Join and as-of join | 23.6, 23.5 | `join` with `alias=`, the default `_right` suffix, and `validate="m:1"` raising `DuplicateKeyError` during execution; `asof_join` with `by=` and `tolerance=`, ties resolved by right order |
| 10. Group aggregation | 23.17, 23.7 | `group_by().agg` with `count(where=)` and an exact decimal `sum`; `pivot` with and without `column_values` |
| 11. Streaming (the brief's "Cursor") | 23.10 | `to_batches()` used as a context manager, the PyCapsule stream into Polars, a `RecordBatchReader` scanned by DuckDB. `Cursor` does not exist in v0.5. |
| 12. Arrow and pandas round trip | 23.18 | `from_arrow` without copying, `to_pandas` on both dtype backends, NaN kept distinct from NULL, DST instants in an aware column |

## F. Contract test matrix

The matrix is [§24] of the contract. Its structure follows from the brief's requirement that no optimizer or execution path may change public semantics.

- **Acceptance criteria ([§24.1])** define conformance: the exported surface and every signature equal [§2] and [§22], every guarantee row and example passes, and the properties hold on generated inputs. Every test runs once per execution path, with each specialized evaluator forced on and off. A path cannot be tested only where it happens to be chosen. A private test-only switch provides this, and it is not public API.
- **Eighteen properties ([§24.2])** state laws over generated inputs rather than examples. They cover laziness, staged, materialized and streaming parity, evaluator parity, row-wise error demand, order propagation, windows keeping rows in place, a Python reference for sorting, exact numerics, Python division, literals behaving as columns, Kleene logic, an equality reference for NULL and NaN, a `zoneinfo` reference across DST transitions, round trips, schema at plan time, and internal-name collisions.
- **95 guarantee rows ([§24.3])** each have a positive, an error-path and a boundary cell, with the exception class and stage for every error.
- **Generators ([§24.4])** fix the value, shape and order distributions: both ends of every integer range, decimal precision 38, NaN payloads, `2**53 ± 1`, astral strings, DST instants, 0/1/2 rows and batch-boundary sizes, several files and partitions, and unordered input.

[§24.5] maps all fifteen dimensions the brief lists to rows and properties. The two that need more than one test to establish are **cross-evaluator parity** (P5, P6, F1, E2) and **lazy, staged and materialized parity** (P1–P3, Z1–Z3). Both are properties, run over every generator.

## G. Implementation impact map

Priority ranks the harm an item removes, in the brief's order:

1. silent wrong results;
2. semantic correctness;
3. public API coherence;
4. developer ergonomics;
5. performance.

Dependencies order the work, and the two orders differ. M0 (feasibility measurements) is priority 5 but comes first: P5, P6 and P10 make checked arithmetic, exact float accumulation, the ordered merge and masked demand normative, so each cost is measured before the item that builds it, and a cost the owner finds unacceptable changes the contract instead of a finished implementation. M15 (exception classes) is priority 2 but comes early, because most error-path cells assert its classes. M16 (row-wise demand) comes before M5, because checked arithmetic makes nearly every numeric expression fallible and so depends on masked evaluation. "Related issues" lists open issues the item resolves or makes obsolete. File paths and lines are at the baseline. Test IDs are [§24]'s. No item needs a compatibility shim, a deprecation window or an old signature kept alongside the new one.

| ID | API / contract change | Related issues | Likely files | Required tests | Dependencies | Priority |
|---|---|---|---|---|---|---|
| M0 | Feasibility measurements, each run before the item it gates and reported as throughput, peak memory, time to first row and scaling with partitions on the existing benchmark data: checked integer kernels and checked `SUM` (M5); exact float accumulation per group and per sliding frame (M13, M4); the order-preserving merge against an unordered parallel scan (M1); masked evaluation for row-wise demand, with the guard on either side of `&` and with two fallible operands (M16); `assume_sorted` validation and the pruning it rules out (M3); `string_view` to `string` normalization (M18). The owner sets budgets from the baselines | #251, #158, #230, #148 | `benchmarks/` | None; measurements, not contract tests | none | 5 |
| M1 | Every reader gives a defined file-then-row order, and every terminal, writer, `collect` and pickle delivers it without materializing to restore the order; `Cursor` is replaced by `to_batches()`, whose Python reads raise LTSeq's classes, and the PyCapsule stream reports a class-named message with EIO or EINVAL ([§15.1], [§16.4]) | #148, #196, #179, #256, #273 (order ratings, X5) | `src/engine.rs` (:43, `target_partitions`), `src/ops/io.rs`, `src/ops/parallel_scan.rs`, `src/ops/set_ops.rs` (:118, `snapshot_single_partition`), `src/arrow_ffi.rs`, `src/cursor.rs`, `src/lib.rs`, `py-ltseq/ltseq/io_ops.py`, `py-ltseq/ltseq/cursor.py` | L1, L4, O1, R1–R3, X3–X5, P4, P6, P7 | M0, #256 | 1 |
| M2 | Order state (`is_ordered`, `sort_keys`) follows the propagation table of [§9.2]; positional and sequence tiers are checked at plan time; `reverse` flips each key and `step` keeps them; `NestedTable.derive` truncates the keys it replaces, as `derive` does; `distinct` with `keep="first"` or `"last"` is positional, with or without keys | #212 | `src/metadata.rs`, `src/ops/common.rs` (:206), `src/ops/sort.rs`, `src/ops/set_ops.rs`, `src/lib.rs`, `py-ltseq/ltseq/core.py`, `py-ltseq/ltseq/transforms.py` | O1, O2, O6, O7, B5, G3, P7 | M1, M15 | 1 |
| M3 | `assume_sorted` is validated on every execution, on every row pair it reads, across batch, file and partition boundaries; no execution skips rows because of the declaration, so statistics pruning and predicate pushdown are not applied below it | #264 (audit D3) | `src/ops/sort.rs` (:105-108, the "caller is responsible" contract), `src/ops/io.rs` (sorted-Parquet path), `src/lib.rs` | O4, P3 | M0, M1, M15 | 1 |
| M4 | Windows never move rows, and window inputs and results compose; `cum_sum` gives NULL on NULL rows, `pct_change` is `float64` from the exact quotient for every input type, integer `diff` is `int64` (`decimal128(20, 0)` for `uint64`), `rolling` takes `min_periods=` and its statistics follow [§14.2]; `dt.diff` is removed | #202, #201 | `src/ops/window.rs` (:79), `src/transpiler/window_native.rs`, `src/ops/derive.rs`, `src/ops/common.rs`, `src/ops/group_window.rs`, `py-ltseq/ltseq/expr/accessors.py` (:387, `diff`) | W1–W6, P8, P10 | M2, M13 | 1 |
| M5 | Arithmetic with an integer or decimal result is checked on every path: operators, integer `**`, `abs`, `round`, `sum`, `cum_sum`, `diff`; integer results take the types of the [§17.3] matrix; `**` with a decimal operand is `float64` ([§17.1]); no wrapping kernel remains | #221, #189 | `src/transpiler/mod.rs`, `src/ops/aggregation.rs`, `src/ops/window.rs`, `src/ops/linear_scan.rs` (:420, :825, `wrapping_sub`), `src/ops/pattern_match.rs` | N1, N3, D2, D8, W3, A2, F1, P5, P10 | M0, M15, M16 | 1 |
| M6 | `/`, `//` and `%` follow Python for integers, floats and decimals; integer `//` keeps the common type, as `%` does, instead of widening to `int64` (`src/transpiler/floor_div.rs`); integer `/` is correctly rounded and raises for a zero divisor (D8) | #218 | `src/transpiler/floor_div.rs`, `src/transpiler/mod.rs`, `src/ops/linear_scan.rs`, `src/ops/pattern_match.rs` | N3, P11 | M15 | 1 |
| M7 | Mixed-type comparison and coercion are exact: `decimal32`/`decimal64` widen to `decimal128`, float with decimal is `float64` in arithmetic, comparisons and `is_in` compare exact values, `-0.0 == 0.0`, join keys match by type class; an integer with `float32` is `float64`; `cast` and `try_cast` follow the [§17.6] table, rounding or truncating only where it says so and failing for a value outside the target's range | #241, #228, #242, #244, #245, #188 | `src/transpiler/resolve.rs`, `src/transpiler/literal_policy.rs`, `src/transpiler/literals.rs`, `src/transpiler/exact.rs`, `src/ops/join.rs`, `src/ops/set_ops.rs`, `src/ops/linear_scan.rs`, `src/ops/pattern_match.rs` | N4, D7, D9, N6, J1, P10, P12, P14 | none | 1 |
| M8 | Literals are exact in context: a literal never widens a column's type ([§17.5] rule 1, revising ADR 0018 D-b/D-i), no timestamp widening, all-literal shared values exact in their common type, `int` literals up to `2**64 - 1` as `uint64`, aware operations on instants, duration literals, NumPy temporal literals in Arrow's units, checked for exactness and range ([§17.2]) | #246, #247, #248, #195, #194 | `src/transpiler/literal_policy.rs`, `src/transpiler/literals.rs`, `src/transpiler/exact.rs`, `src/transpiler/resolve.rs`, `src/ops/mutation.rs` (`python_value_to_scalar`), `py-ltseq/ltseq/expr/base.py`, `py-ltseq/ltseq/expr/core_types.py` (`_encode_datetime64`), `py-ltseq/tests/literal_grid/oracle.py` | N2, N5, H1, P12, P15 | M7 | 1 |
| M9 | `concat` requires exact schemas, empty inputs included | #222 | `src/ops/set_ops.rs`, `py-ltseq/ltseq/advanced_ops.py` | U1, U2 | M15 | 1 |
| M10 | Set operations, `pivot` and `partition` use the value equality of [§18]; `partition` keys are `to_dicts` values except that aware timestamps are UTC instants and a float zero is `0.0`, and a key that does not convert raises `CastError` at the call; `intersect` and `difference` take `distinct=`; NULL and NaN as-of keys never match; `asof_join` becomes a plan operator that merges its inputs sorted by the as-of key within [§21.1]'s bound, where the baseline collects both inputs and binary-searches `int64` times (`src/ops/asof_join.rs:113-121`) | #205, #207, #263, #258 | `src/ops/set_ops.rs` (:367, `semi_anti_on_keys`), `src/ops/asof_join.rs`, `src/ops/pivot.rs`, `py-ltseq/ltseq/partitioning.py` | U3, J6, A4, A5, P14 | M1, M7, M15 | 1 |
| M11 | `NestedTable.filter` keeps group identity; `search_pattern` steps use Kleene logic and, with `partition_by`, read only their partition | #196, #220 | `py-ltseq/ltseq/grouping/nested_table.py` (:170-173), `src/ops/group_window.rs`, `src/ops/grouping.rs`, `src/ops/pattern_match.rs` | G1–G3, W8, P13 | M2, M15 | 1 |
| M12 | No input is silently ignored: a closed `Expr` method set and closed signatures whose stub annotations admit every documented call form, unknown dtype names raise, `requested_schema` types are honored by exact conversion within a kind and refused across kinds ([§16.4], AC17), a Python `bool` as a condition or as a lambda's whole value is refused, a missing source raises at the call | #146, #184, #185, #153, #223, #254 | `py-ltseq/ltseq/expr/types.py` (:77, `__getattr__`), `py-ltseq/ltseq/expr/base.py`, `py-ltseq/ltseq/expr/transforms.py` (:32, `_null_check`), `py-ltseq/ltseq/io_ops.py`, `src/ops/io.rs` (:203-215), `src/arrow_ffi.rs` (:175), every `.pyi` | S2, S5, D11, T3, L1, X3, B1, D4 | M15 | 1 |
| M13 | Aggregates: `where=` replaces the `*_if` family, `quantile` is exact and rounded once, `mode` breaks ties by sort order, no non-NULL value taking part gives the [§14.2] column-4 value, decimal `sum` and `mean` have the [§14.2] result types, float statistics are exact values rounded once and so bit-identical across order and batching, `agg()` with no names raises; `top_k`, `skew` and approximate `percentile` are removed | #199, #153, #253 | `src/ops/aggregation.rs`, `src/ops/window.rs`, `py-ltseq/ltseq/aggregation.py`, `py-ltseq/ltseq/grouping/proxies/` | A1–A3, W3, W4, P5, P6, P10 | M0, M5 | 1 |
| M14 | `delete` and `update` act only where the predicate is TRUE | #265 (audit H3) | `src/ops/mutation.rs`, `py-ltseq/ltseq/mutation_mixin.py` | B8, P13 | M2, M15 | 1 |
| M15 | Fifteen exception classes with built-in bases; every documented failure maps to one class and one stage | #266, #152, #153 | `src/error.rs` (:88-90), `py-ltseq/ltseq/exceptions.py`, `py-ltseq/ltseq/exceptions.pyi` | E1, E2 | none | 2 |
| M16 | Row-wise demand: either operand of `&` or `\|` guards the other (D10); `when`, `if_else`, `coalesce`, `search_pattern` steps and chained filters guard in order; all evaluate per row, by masked evaluation where a rewrite or evaluator moves an expression (rewrite table in the trade-off record, X1–X4 included, with a sort, grouping or aggregate demanding its inputs on every row by D10a); `count()` raises exactly when a value that decides which rows exist, or their order, fails | #220, #252, #273 | `src/transpiler/mod.rs`, `src/lib.rs` (:417, `count`), `src/ops/pattern_match.rs` | E3, P6 | M0, M15 | 2 |
| M17 | Evaluator parity: every specialized path matches the general one or declines before executing; the bare `except Exception` fallback goes; a test-only switch forces each path on and off | #189, #211, #217, #244 | `src/ops/linear_scan.rs`, `src/ops/pattern_match.rs`, `src/ops/io.rs`, `py-ltseq/ltseq/grouping/nested_table.py` (:390) | F1, P5; every row run once per path | M5–M7, M15 | 2 |
| M18 | Plan-time validation and type normalization: `sort` with no keys, `ntile(0)`, unknown columns as `ColumnNotFoundError` with suggestions, pass-through types; views, dictionaries and extension types normalized wherever a schema is visible, and a `date64` value beyond `date32` a `CastError` ([§6.2]) | #267, #152, #239 (run-end encoding) | `src/ops/sort.rs`, `src/ops/window.rs`, `py-ltseq/ltseq/transforms.py`, `py-ltseq/ltseq/expr/proxy.py` | O3, W5, S5, T2, E2 | M0, M15 | 2 |
| M19 | Stable sort with `nulls_last=True` in both directions, NaN above `+inf` | #268; #253 (NaN with the sign bit set sorts first) | `src/ops/sort.rs`, `src/transpiler/window_native.rs` | O3, P9 | none | 2 |
| M20 | Temporal: `.dt` fields are `int32`, `dt.add` takes integer expressions and raises in DST gaps and overlaps, `date ± duration` is checked for whole days, `duration * integer` and `duration // integer` are exact and checked in the duration's unit, `//` floored, with no implicit change of unit (D14), and every other number with a temporal value raises, `.dt.total_seconds()` and `.dt.days()` on durations, `now()` is aware UTC and fixed per execution, `dt.age` and `dt.diff` are removed | #239 (encoded timestamps in `.dt`), #249 | `py-ltseq/ltseq/expr/accessors.py` (:336 `year`, :360 `add`, :411 `age`), `src/transpiler/mod.rs` | H1–H4, P15 | M8 | 2 |
| M21 | `.str` follows Python `str` on Unicode, with Python's method names (`startswith`, `endswith`, `rjust`, `ljust`) and `contains_literal`/`contains_regex` in place of `contains` | #192 | `py-ltseq/ltseq/expr/accessors.py` (:11, `StringAccessor`), `src/transpiler/mod.rs` (the `str_*` arms) | D10, D11 | M15 | 2 |
| M22 | IO and interchange: an existing path is read as a file even with glob characters, `from_arrow` takes tabular input and refuses a struct with a NULL row, tables without columns keep their row counts through the constructors, `select`, `drop` and interchange, and the writers refuse them ([§6.1]), declared `from_dict` and `from_rows` types convert exactly and within a kind ([§4.6], AC17), CSV and `from_dict` inference never rounds an integer ([§4.2], [§4.6]), header-only CSV reads as `string`, 0-row files are written, writers are atomic, CSV writes and parses the canonical text forms of [§16.5] and round-trips with `schema=t.schema`, `write_parquet` refuses the types Parquet cannot hold, a `struct` with no fields and `fixed_size_binary(0)` among them, and converts seconds to milliseconds as `cast` does, `to_pandas` types are those of a Feather file ([§16.2]), pickle as a same-minor-version Arrow IPC snapshot | #269, #270, #156, #206, #255, #259, #261, #262 | `src/ops/io.rs`, `py-ltseq/ltseq/io_ops.py`, `py-ltseq/ltseq/core.py` | T1, L2, L3, L5–L7, X1, X4, X5, P2, P16 | M1, M2, M15, M16 | 2 |
| M23 | Join `validate=` (reading every key on each constrained side) and `alias=`; the `full` join key is the left key, else the right; `asof_join` takes a `timedelta` tolerance for temporal keys; `semi_join`/`anti_join` with `EXISTS`/`NOT EXISTS` semantics | #156, #223 | `src/ops/join.rs`, `py-ltseq/ltseq/joins.py` | J2–J5 | M7, M15 | 2 |
| M24 | Three table types: `LinkedTable`, `link`, `lookup`, `PartitionedTable`, `SQLPartitionedTable`, `Cursor`, `scan`, `scan_parquet` and `align` are removed; `partition` returns a `dict` | #156, #179, #223, #258 (OD-S1: stopgaps on v0.4 until removal) | `py-ltseq/ltseq/linking.py`, `py-ltseq/ltseq/lookup.py`, `py-ltseq/ltseq/expr/lookup_expr.py`, `py-ltseq/ltseq/partitioning.py`, `py-ltseq/ltseq/cursor.py`, `py-ltseq/ltseq/io_ops.py` (:123, :147), `py-ltseq/ltseq/core.py` (:642, :703), `src/cursor.rs`, `src/ops/align.rs` | S1, S4, A4, R1 | M1, M10, M23 | 3 |
| M25 | One name and one keyword per concept (`.str`, `reverse`, `difference`, `descending`, `how`, `other`, `alias`, `direction`); aliases removed; options keyword-only | #156, #146 | `py-ltseq/ltseq/core.py`, `py-ltseq/ltseq/transforms.py`, `py-ltseq/ltseq/advanced_ops.py`, `py-ltseq/ltseq/joins.py`, `py-ltseq/ltseq/aggregation.py`, `py-ltseq/ltseq/expr/accessors.py`, every `.pyi` | S1, S2 | none | 3 |
| M26 | Exact exports: `__all__`, `ltseq.typing`, and the extension module renamed to the private `ltseq._ltseq_core` | #146 | `py-ltseq/ltseq/__init__.py`, `py-ltseq/ltseq/__init__.pyi`, `py-ltseq/ltseq/_typing.py`, `pyproject.toml` (:18, `module-name`), `Cargo.toml` | S1 | M24, M25 | 3 |
| M27 | One calling convention: `derive(self, /, **named)` and `agg(self, /, **named)` everywhere, so any column name is accepted, `g.col.sum()` in every aggregate context; `NestedTable.count()` returns the number of groups | #271 (audit C4) | `py-ltseq/ltseq/transforms.py`, `py-ltseq/ltseq/grouping/nested_table.py`, `py-ltseq/ltseq/grouping/proxies/`, `py-ltseq/ltseq/aggregation.py` | B3, G3, A1, A3 | M15 | 3 |
| M28 | Documentation equals the contract: `docs/api.md` and `docs/api.cn.md` rewritten; ADRs amended where [Deliverable D] says so (0005, 0008, 0009, 0018, including D-b and D-i, which [§17.5] rule 1 revises) and where v0.5 replaces what they record: 0004 (`Cursor`, `scan` and `scan_parquet`), 0006 (execution paths, by the parity rule), 0010 and 0011 (removed table types), 0013 (windows), 0014 (`ltseq.typing`) and 0017 (`Cursor`); `LINKING_GUIDE` retired | #175, #173 | `docs/` | [§24.1] criterion 5 (documentation examples run as tests) | M1–M27 | 3 |
| M29 | Error messages name the method, the column and the fix; `repr` does not execute; `with_row_index` and `explain` | #272; #155 | `src/error.rs`, `src/format.rs`, `py-ltseq/ltseq/core.py` | S3, B6, O7 | M15 | 4 |
| M30 | Recover performance against the M0 baselines: fast paths re-enabled under the parity harness, masked rewrites of the trade-off record's rewrite table re-enabled, `search_pattern` double execution | #149, #155, #158, #165, #229–#237 | `src/ops/linear_scan.rs`, `src/ops/parallel_scan.rs`, `src/ops/pattern_match.rs`, `benchmarks/` | R3, F1, the benchmark suite | M0, M1, M5, M16, M17 | 5 |

Two test issues sit across the rows. #274 (a `target_partitions` and `batch_size` override, layout fixtures and the R3 memory harness) is a prerequisite of the tests of M1, M3, M13, M16, M17 and M19, which must run on multi-partition inputs. #275 migrates the literal grid and the differential baselines alongside M5–M8, M15 and M20, so each of them is gated against v0.5 and not v0.4. The open questions that do not block the merge are filed by kind in #276 (numeric), #277 (temporal) and #278 (API and interchange), each row naming the M item it must be settled before.

#229 and its children (#230–#237), the epic for a whole-pipeline compiler, are listed under M30. If that compiler lands before v0.5, it is the natural home for M17's path selection and for M4's window grouping, and those rows should move with it rather than be implemented twice.

<!-- v0.5-modular:footer -->

---

Previous: [Trade-off record](trade-offs.md) · [Index](../README.md) · Next: [Adversarial review and final consistency check (H, I)](history/adversarial-review.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§2]: ../contract/public-surface.md#2-exports
[§4]: ../contract/loading-and-laziness.md#4-loading
[§4.2]: ../contract/loading-and-laziness.md#42-ltseqread_csv
[§4.6]: ../contract/loading-and-laziness.md#46-ltseqfrom_dict-and-ltseqfrom_rows
[§6.1]: ../contract/schema-and-table-operations.md#61-schema-and-columns
[§6.2]: ../contract/schema-and-table-operations.md#62-supported-types
[§9.2]: ../contract/ordering.md#92-sources-and-propagation
[§14.2]: ../contract/aggregation.md#142-aggregate-expressions
[§15.1]: ../contract/streaming-and-output.md#151-to_batches
[§16]: ../contract/streaming-and-output.md#16-output-and-interchange
[§16.2]: ../contract/streaming-and-output.md#162-to_pandas
[§16.4]: ../contract/streaming-and-output.md#164-arrow-pycapsule-stream
[§16.5]: ../contract/streaming-and-output.md#165-writers
[§17]: ../contract/numeric-null-temporal.md#17-numeric-semantics-and-literals
[§17.1]: ../contract/numeric-null-temporal.md#171-integer-and-decimal-results-are-checked
[§17.2]: ../contract/numeric-null-temporal.md#172-literals
[§17.3]: ../contract/numeric-null-temporal.md#173-arithmetic-operators
[§17.5]: ../contract/numeric-null-temporal.md#175-shared-values
[§17.6]: ../contract/numeric-null-temporal.md#176-explicit-casts
[§18]: ../contract/numeric-null-temporal.md#18-null-nan-and-boolean-logic
[§21.1]: ../contract/errors-and-performance.md#211-materialization
[§22]: ../contract/api-reference.md#22-complete-canonical-api-reference
[§23]: ../contract/examples-sequences.md#23-end-to-end-examples
[§24]: ../contract/acceptance.md#24-acceptance-criteria-and-contract-test-matrix
[§24.1]: ../contract/acceptance.md#241-acceptance-criteria
[§24.2]: ../contract/acceptance.md#242-properties
[§24.3]: ../contract/acceptance.md#243-guarantee-matrix
[§24.4]: ../contract/acceptance.md#244-generators
[§24.5]: ../contract/acceptance.md#245-coverage-of-the-required-dimensions
[Deliverable D]: decisions.md#d-decisions-on-the-eleven-open-semantic-issues
[Deliverable E]: #e-canonical-examples
[Deliverable F]: #f-contract-test-matrix
[Deliverable G]: #g-implementation-impact-map

<!-- /v0.5-modular:links -->
