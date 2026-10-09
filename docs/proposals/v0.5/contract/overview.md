<!-- v0.5-modular:header -->

# v0.5 contract: Overview, principles and rule ownership (§1)

[Index](../README.md) › Contract · Next: [Exports and object model (§2–§3)](public-surface.md)

**Normative.** One module of the LTSeq v0.5 public API contract. It uses the BCP 14 key words as the contract's [status block](../README.md#ltseq-v05-public-api-contract) declares, and [§1.6] names the section that owns each cross-cutting rule.

**Scope.** What LTSeq is and how v0.5 positions it, the ten design principles that decide what the other sections leave open, the ADRs this contract revises, the non-goals, and the table that names the section owning each cross-cutting rule.

**Most cited from here.** [§17] Numeric semantics and literals · [§9] Ordering contract · [§14] Aggregation, partitioning and pivot · [§10] Windows and ordered computation

<!-- /v0.5-modular:header -->

## 1. Overview

### 1.1 What LTSeq is

LTSeq is a Python library for ordered tables. Its data model is a sequence of rows with an explicit order, not a bag of rows. The user-facing API is Python; execution is a Rust kernel on Apache DataFusion and Arrow. v0.5 keeps the properties that distinguish LTSeq from pandas, Polars, DuckDB and Ibis and makes them contractual:

- **Row order is part of the value.** Every table either has a defined order, and every terminal delivers rows in it, or is marked unordered, and every operation whose result would depend on order refuses to run ([§9]).
- **Order-dependent computation is first class.** Table-order windows, consecutive grouping (`group_ordered`), ordered search (`search_first`, `search_pattern`) and sequential state (`fold`) are built in, not emulated with sort-and-self-join recipes.
- **Lazy by default.** Every table-returning method builds a plan. Execution happens only in the terminals and the eager calls listed in [§5.3].
- **Streaming.** Terminals stream batch by batch through a standard `pyarrow.RecordBatchReader` and the Arrow PyCapsule interface.
- **No silent wrong results.** Where pandas, Polars or SQL engines wrap, truncate, round or coerce silently, v0.5 either computes the mathematically correct value or raises a typed exception at a documented stage ([§17], [§20]).

### 1.2 Positioning

| | LTSeq v0.5 | pandas | Polars | DuckDB | Ibis |
|---|---|---|---|---|---|
| Row order | Defined or explicitly unordered; tracked per table | Index order, always defined | Defined in eager frames; joins and group-by do not keep it by default | Kept only by a listed set of operators | Backend-defined |
| Execution | Lazy plan, streaming terminals | Eager | Eager and lazy | Lazy relations | Lazy, compiled to a backend |
| Sequence ops | Table-order windows, consecutive groups, pattern search, fold | `shift`, `rolling`, `groupby` | `shift`, `rolling`, `rle_id` | SQL windows | SQL windows |
| Integer overflow | Raises | Wraps | Wraps | Raises | Backend |
| Expression input | Python lambdas over a row proxy | Vectorised Series | Expression objects | SQL or relational API | Expression objects |

The comparison cells summarize the sources cited in the review document ([Deliverable A]); they are context, not requirements.

### 1.3 Design principles

These principles decide every case the sections below do not spell out. When two conflict, the earlier one wins.

1. **No silent wrong results.** A value LTSeq returns is the correct value under this contract, or the call raises. Rounding, wrapping, truncation and reordering happen only where this document says so.
2. **One meaning per spelling.** A method name, a keyword and an operator each have one meaning, independent of which execution path runs it. Fast paths are optimizations, never semantics ([§21.2]).
3. **Order is explicit.** A table knows whether its order is defined and which keys declare it. Operations that need order say which kind ([§9.3]), and refuse when it is missing.
4. **Errors are typed and staged.** Every documented failure has an exception class ([§20.1]) and a stage: when the plan is built, when a source is opened, or when a value is demanded during execution ([§20.2]).
5. **Python spelling, Arrow types.** Names and argument conventions follow Python and the dominant DataFrame libraries; data types are Arrow types, and the type of every result is specified.
6. **Minimal and orthogonal.** One way to do each thing. v0.5 removes aliases and convenience variants that duplicate a composition of other methods.
7. **Lazy until a terminal.** Plan-building calls do not read data, except schema discovery and the eager calls listed in [§5.3].
8. **Composable.** Every table-returning method returns an `LTSeq` (or a `NestedTable`/`GroupBy` builder that returns one), so results compose without conversion.
9. **Deterministic.** The same plan over the same input data returns the same rows in the same order, or fails, with six named exceptions: the row sequence of a table whose order is undefined ([§9.1]), and so which rows `head()`/`show()` return from it; the relative order of the rows [§9.1] calls *unspecified ties*, and every value computed from a row's position among them; which row `distinct(keep="any")` keeps for each key ([§7.6]), and which of several equal rows `intersect` and `difference` keep when the left order is undefined ([§13.2]); when several demanded values fail, which of their errors is raised ([§20.2]); and `now()`/`today()` across executions.
10. **Streaming is the default terminal path.** Terminals produce batches incrementally; whole-table materialization happens only where [§21.1] lists it.

### 1.4 Relation to earlier decisions

This contract replaces `docs/api.md` as the normative description of the public API. Implementation PRs update `docs/api.md` to match it. It revises these ADRs; the review document gives the reasons:

| ADR | Change |
|---|---|
| 0004 (lazy execution) | Revised: the streaming `Cursor`, `scan` and `scan_parquet` are removed, because readers are lazy and `to_batches` is the streaming scan ([§15.1]); the eager calls are listed in [§5.3]. |
| 0005 (no materialization) | The eager-boundary inventory is replaced by [§5.3] and [§21.1]. |
| 0006 (multi-path execution) | Kept, with two limits: a fast path is allowed only where its results, errors and stages equal the general path's ([§21.2], [§20.2]), and none relies on an order that has not been validated ([§9.5]). |
| 0008 (explicit sort metadata) | Revised. `assume_sorted` is validated instead of trusted ([§9.5]); sort keys MUST be column names, so a computed key is `derive`d first instead of sorting untracked ([§9.4]); NULL sorts last by default in both directions ([§9.4]). |
| 0009 (metadata single source of truth) | Extended by the order model of [§9](ordering.md#9-ordering-contract): every table also carries an order state, and windows keep table order (#202). |
| 0010 (four table types) | Replaced. Public types are `LTSeq`, `NestedTable` and `GroupBy`; `LinkedTable` and `PartitionedTable` are removed ([§3]). |
| 0011 (link) | Superseded by `join(..., alias=...)` ([§12.1]). |
| 0013 (`.over()`) | Revised: the `partition_by=` keyword on individual window methods is removed, and `rolling` takes `min_periods` ([§10.1]), which ADR 0013 records `rolling` as deliberately lacking. |
| 0014 (typed surface) | Revised: [§22] is the normative typed surface; the closed method set ([§8.7]) replaces the `__getattr__` fallback on expressions, and the aliases the stubs carry are removed (principle 6). |
| 0017 (Arrow C stream) | Kept; `requested_schema` is honored or refused ([§16.4]). |
| 0018 (literal exactness) | D-a kept, amended by the four overrides of [§17.4]. D-c and D-f kept. D-b/D-i revised: a literal never widens the type of the non-literal values it shares a column with ([§17.5] rule 1), so `r.i32.fill_null(0.0)` stays `int32` where D-b gave `float64`; and extended to all-literal values (#248). D-e and D-j extended from literals to every numeric comparison, float contexts included ([§17.4]). D-d revised: naive with aware raises ([§19.2]). D-h/D-k revised: no implicit string readings; a type mismatch raises `LTSeqTypeError` (a `TypeError`, no longer a `ValueError`) and an inexact value `CastError` (still a `ValueError`). D-l kept, raising `LTSeqTypeError`, and extended to times and durations, except that a duration takes an integer factor or divisor ([§19.2]). D-m revised: no unit widening (#247), and an aware literal's instant is used in arithmetic and comparisons as well as shared values (#246). The `interpret` reading of a `Decimal` literal next to a float as the nearest float is kept in arithmetic only; in comparisons it compares exactly ([§17.4] rule 3) and in shared values it MUST be exact ([§17.5]). |

### 1.5 Non-goals

v0.5 has no SQL string interface, no user-defined vectorized functions, no distributed execution, no in-place mutation, and no regular-expression pattern language beyond the fixed-length step patterns of `search_pattern` ([§10.6]).

### 1.6 Rule ownership

Each concept below is defined in its owning sections. Other sections apply the rule to one operation and point to the owner. A restatement that differs from its owner is a defect in the restatement: the owner governs, and the restatement is corrected. An operation-specific rule (a `partition` key refuses NaN, a join never matches NULL keys) is stated in the operation's section and named by the owner. The [§24] rows listed are those that test the rule; they are specifications, since nothing in this contract is implemented yet.

| Concept | Owner | Applied in | [§24] rows | Issues |
|---|---|---|---|---|
| Numeric result types and checking | [§17.1] (checked results), [§17.3] (operator types), [§17.4] (mixed operands); [§14.2] for aggregates | [§8.1], [§8.4], [§8.5], [§10.1], [§10.2] | N1, N3, N4, D6, D8, W2–W4, A2, P10, P11 | #221, #218, #228, #241, #189 |
| Literal conversion and exactness | [§17.2] (Python value to type), [§17.5] (shared values, fixed targets), [§17.6] (the five ways a value changes type: an implicit placement is exact and keeps its kind); [§4.2] and [§4.6] for inference | [§4.4]–[§4.6], [§6.2], [§7.3], [§7.10], [§8.3], [§8.5], [§10.1], [§10.7], [§16.4], [§16.5], [§17.4], [§19.2], [§20.2] | N2, N4, N5, P12, D5, D7, D9, W2, W9, B7, B8, L3, L6, L7, X3 | #246, #247, #248, #195, #278 |
| NULL and NaN equality and ordering | [§18] (equality, representatives, logic), [§9.4] (sort placement), [§17.4] rule 3 (comparisons) | [§4.6], [§7.6], [§8.1], [§8.3], [§8.5], [§10.1], [§10.4], [§11.1], [§12.1], [§12.3], [§13], [§14.1], [§14.2], [§14.5], [§14.6] | P9, P14, Q1, B5, O3, J1, U3, A1, A2, A4, G1, W3, W6 | #205, #207, #253 |
| Logical row order and sort metadata | [§9.1] (state, ties), [§9.2] (propagation), [§9.3] (tiers), [§9.4] (stability); principle 9 for what may vary | [§4.7], [§7], [§10], [§11], [§12], [§13], [§14] | P3, P7, P8, P9, O1–O7, W5, W7, W8, G1, G3, J3, J6, U2, U3, A4, A5, B2–B4 | #202, #148, #212, #201 |
| Demand and error observation | [§20.2] (stages, demand), [§20.1] (classes), principle 9 (which error); [§21.2] for fast paths | [§5.2], [§7.9], [§8.1], [§8.3], [§9.5], [§10.5], [§10.6], [§10.7], [§12.1], [§14.2], [§16.6], [§17.2] | P1, P2, P4–P6, P13, E1–E3, B6, Z2, W7, J4, O4, X5, R2, F1 | #220, #252, #152, #153, #251 |
| Arrow and Python interchange | [§6.2] (normalization), [§16.3] (conversion to Python, and which operations raise for it), [§15.1] and [§16.4] (stream errors, `requested_schema`), [§16.2] (pandas), [§16.5] (writers, text forms) | [§4.3]–[§4.6], [§10.7], [§14.5], [§14.6], [§15.2], [§17.6], [§18], [§20.1], [§20.2] | L5–L7, T2, P3, P4, P16, X1–X5, R1, R2 | #256, #255, #259, #153, #223, #195 |
| Temporal units and time zones | [§19.2] (arithmetic, units, naive and aware), [§19.4] (local-time resolution), [§17.2] and [§17.5] (temporal literals and shared values), [§17.6] (unit and calendar casts) | [§1.4], [§6.2], [§9.4], [§12.1], [§14.5], [§16.3], [§17.4], [§20.1] | H1–H4, P12, P15, N2, N5, N6, D9, A4, T2 | #246, #247, #248, #258 |
| Aggregation | [§14.2] (`where=`, empty results, exactness, order independence) | [§10.1]–[§10.3], [§11.2], [§14.1], [§14.3], [§14.6], [§17.1], [§17.3], [§20.2] | A1–A5, G3, W3, W4, N1, Q1, P6, P10, E3 | #221, #251, #189 |
| Streaming and materialization | [§21.1], with [§5.3] for the work done at the call | principles 7 and 10, [§15.1], [§15.2], [§16.4] | R1, R3, P1, P4, Z3 | #148, #179, #196, #256, #251 |
| Snapshot and partition consistency | [§5.1] (what an execution reads, what is fixed at the call), [§5.2] (snapshots), [§14.5] (partition keys) | principle 9, [§3.1], [§4], [§9.2], [§14.6], [§15.1], [§16.6], [§19.5], [§20.2] | Z1–Z3, X5, P2, P14, A4, A5, H4 | #156, #258, #148 |
| Tables without columns | [§6.1] (row and column counts independent, what such a table does, which inputs give no row count) | [§4.2], [§4.4]–[§4.6], [§7.2], [§7.5], [§7.6], [§16.5], [§20.2] | T1, L3, L5–L7, B2, B4, B5, X4 | #156, #278 |

<!-- v0.5-modular:footer -->

---

[Index](../README.md) · Next: [Exports and object model (§2–§3)](public-surface.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§1.4]: #14-relation-to-earlier-decisions
[§1.6]: #16-rule-ownership
[§3]: public-surface.md#3-object-model
[§3.1]: public-surface.md#31-types
[§4]: loading-and-laziness.md#4-loading
[§4.2]: loading-and-laziness.md#42-ltseqread_csv
[§4.3]: loading-and-laziness.md#43-ltseqread_parquet
[§4.4]: loading-and-laziness.md#44-ltseqfrom_arrow
[§4.6]: loading-and-laziness.md#46-ltseqfrom_dict-and-ltseqfrom_rows
[§4.7]: loading-and-laziness.md#47-ltseqrange
[§5.1]: loading-and-laziness.md#51-plan-building-and-execution
[§5.2]: loading-and-laziness.md#52-ltseqcollect
[§5.3]: loading-and-laziness.md#53-eager-calls
[§6.1]: schema-and-table-operations.md#61-schema-and-columns
[§6.2]: schema-and-table-operations.md#62-supported-types
[§7]: schema-and-table-operations.md#7-basic-table-operations
[§7.2]: schema-and-table-operations.md#72-select
[§7.3]: schema-and-table-operations.md#73-derive
[§7.5]: schema-and-table-operations.md#75-drop
[§7.6]: schema-and-table-operations.md#76-distinct
[§7.9]: schema-and-table-operations.md#79-count-and-show
[§7.10]: schema-and-table-operations.md#710-value-level-edits-insert-delete-update
[§8.1]: expressions.md#81-contexts-and-proxies
[§8.3]: expressions.md#83-conditional-and-null-functions
[§8.4]: expressions.md#84-math-functions
[§8.5]: expressions.md#85-general-expr-methods
[§8.7]: expressions.md#87-the-closed-method-set
[§9]: ordering.md#9-ordering-contract
[§9.1]: ordering.md#91-order-state
[§9.2]: ordering.md#92-sources-and-propagation
[§9.3]: ordering.md#93-order-requirements
[§9.4]: ordering.md#94-sort
[§9.5]: ordering.md#95-assume_sorted
[§10]: windows-and-grouping.md#10-windows-and-ordered-computation
[§10.1]: windows-and-grouping.md#101-window-methods
[§10.2]: windows-and-grouping.md#102-ranking-functions
[§10.3]: windows-and-grouping.md#103-aggregates-over-windows
[§10.4]: windows-and-grouping.md#104-over
[§10.5]: windows-and-grouping.md#105-search_first
[§10.6]: windows-and-grouping.md#106-search_pattern
[§10.7]: windows-and-grouping.md#107-fold
[§11]: windows-and-grouping.md#11-ordered-grouping
[§11.1]: windows-and-grouping.md#111-group_ordered
[§11.2]: windows-and-grouping.md#112-nestedtable
[§12]: joins-and-sets.md#12-joins
[§12.1]: joins-and-sets.md#121-join
[§12.3]: joins-and-sets.md#123-asof_join
[§13]: joins-and-sets.md#13-set-and-bag-operations
[§13.2]: joins-and-sets.md#132-intersect-and-difference
[§14]: aggregation.md#14-aggregation-partitioning-and-pivot
[§14.1]: aggregation.md#141-group_by-and-groupbyagg
[§14.2]: aggregation.md#142-aggregate-expressions
[§14.3]: aggregation.md#143-ltseqagg
[§14.5]: aggregation.md#145-partition
[§14.6]: aggregation.md#146-pivot
[§15.1]: streaming-and-output.md#151-to_batches
[§15.2]: streaming-and-output.md#152-iteration
[§16.2]: streaming-and-output.md#162-to_pandas
[§16.3]: streaming-and-output.md#163-to_dicts
[§16.4]: streaming-and-output.md#164-arrow-pycapsule-stream
[§16.5]: streaming-and-output.md#165-writers
[§16.6]: streaming-and-output.md#166-pickle
[§17]: numeric-null-temporal.md#17-numeric-semantics-and-literals
[§17.1]: numeric-null-temporal.md#171-integer-and-decimal-results-are-checked
[§17.2]: numeric-null-temporal.md#172-literals
[§17.3]: numeric-null-temporal.md#173-arithmetic-operators
[§17.4]: numeric-null-temporal.md#174-types-of-mixed-operands
[§17.5]: numeric-null-temporal.md#175-shared-values
[§17.6]: numeric-null-temporal.md#176-explicit-casts
[§18]: numeric-null-temporal.md#18-null-nan-and-boolean-logic
[§19.2]: numeric-null-temporal.md#192-arithmetic-and-comparison
[§19.4]: numeric-null-temporal.md#194-dt-methods
[§19.5]: numeric-null-temporal.md#195-clock-functions
[§20]: errors-and-performance.md#20-errors
[§20.1]: errors-and-performance.md#201-exception-classes
[§20.2]: errors-and-performance.md#202-stages
[§21.1]: errors-and-performance.md#211-materialization
[§21.2]: errors-and-performance.md#212-fast-paths
[§22]: api-reference.md#22-complete-canonical-api-reference
[§24]: acceptance.md#24-acceptance-criteria-and-contract-test-matrix
[Deliverable A]: ../review/verdict.md#a-executive-verdict

<!-- /v0.5-modular:links -->
