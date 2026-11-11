# ADR 0019: Pipeline Compilation

- Status: Proposed (becomes Accepted when #232, the pipeline IR, merges)
- Decision date: proposed in #229 and #238; not yet accepted · Recorded: 2026-11-11

[中文版](0019-pipeline-compilation.cn.md)

## Context

Every LTSeq operation today builds its DataFusion plan when it is called. `filter`, `derive`, `sort` and the rest each turn their captured expression into a DataFusion `Expr` and extend the table's `DataFrame`. The kernel then hands the result to DataFusion's optimizer ([ADR 0006](0006-multi-path-execution-strategy.md)). Specialized sequence paths, such as the linear-scan kernel for `group_ordered().first().count()` and the sorted-Parquet pattern count, decide case by case whether a call fits them. Each one looks only at its own call. No step of the system sees the whole chain of operations, so a decision that depends on the chain cannot be made. Examples are merging adjacent steps, choosing one kernel for several ops, and saying why a kernel did not apply. The EPIC #229 proposes a whole-pipeline compiler to make those decisions, built in workstreams B (IR, #232), C (#233), D (#236), E1 (#235) and F (#237).

This ADR records the design that #229 and #238 set out, so the workstreams build against one statement of it. It is written before the IR exists. The reasons given under Alternatives are this record's reading of the existing ADRs, because the issues list the rejected alternatives without recording why. When B lands, it should confirm or amend them.

## Decision

1. **The IR is owned by Rust.** The pipeline IR, the ops a table has accumulated in a form the compiler can rewrite, lives in the Rust kernel. Python keeps capturing lambdas once, at the call ([ADR 0007](0007-lambda-dsl-schemaproxy.md)), and sends their serialized expressions across the boundary as it does today.
2. **Rust stays the metadata authority.** Schema and declared order are the kernel's ([ADR 0009](0009-metadata-single-source-of-truth.md), [ADR 0008](0008-explicit-sort-metadata.md)). The compiler computes them for every step of the IR, and Python reads them over FFI. It never derives them on its own.
3. **DataFusion is the generic optimizer.** A step without a specialized lowering lowers to DataFusion plan nodes, and DataFusion optimizes and executes them as it does now. LTSeq does not rebuild DataFusion's logical or physical optimizations.
4. **Kernels are recognized lowerings.** A specialized kernel, such as the linear scan or the parallel scan, applies when the compiler recognizes a shape of the IR that it implements. The eligibility check belongs to that recognition, not to each op's entry point.
5. **Decline is typed.** When a kernel does not apply, the compiler records a typed reason and lowers the step generically. The reason is data that diagnostics report and tests assert on, not a log line or a silent fallback.
6. **The generic path is the reference.** For every input, a kernel's result must equal what the generic DataFusion lowering returns. Parity tests compare the two, and a kernel that disagrees is a bug in the kernel.
7. **Batching is constrained.** The compiler does not merge just any run of consecutive ops: some ops end a compilable run, such as an eager boundary, a physical-position op or an op outside the IR. #232 defines the exact constraints. The supported-op matrix of #238 records them per op.
8. **No new materialization.** Compilation must not add an eager boundary. The no-materialization rule ([ADR 0005](0005-no-materialization-rule.md)) and its inventory of documented eager boundaries hold unchanged.

Diagnostics follow the compiler (#238). `LTSeq._explain_pipeline()` is private and returns a dict that reports what the kernel decided, without executing the plan. It holds the column environment, the declared order, both DataFusion plans, and the physical operator tree with per-operator counts. Tests read operator counts from it instead of searching plan text. Each workstream adds its fields: the original and normalized IR, the order state per step, the lowering decision, kernel eligibility and the decline reason. The result stays private until the IR is stable, because tests that read it make it a de facto contract.

## Alternatives Considered

- *DataFusion's `LogicalPlan` as the LTSeq IR.* This needs no second representation. But LTSeq's declared order is metadata the kernel keeps beside the plan (`sort_specs`, [ADR 0008](0008-explicit-sort-metadata.md)), not part of it, and the specialized kernels ([ADR 0006](0006-multi-path-execution-strategy.md)) run outside DataFusion's plan. A `LogicalPlan` holds neither, so the compiler would have to rebuild them from plan nodes.
- *A Python IR that owns the full schema.* This would bring back the dual schema model that #93 removed ([ADR 0009](0009-metadata-single-source-of-truth.md)), along with the drift it caused.
- *Deferring lambda capture until compilation.* Errors in a lambda would surface at a later, unrelated call instead of at the line that wrote it. The lambda would also be evaluated against a schema that may have changed in between. [ADR 0007](0007-lambda-dsl-schemaproxy.md) captures once, at the call.
- *Rebuilding a full generic optimizer in LTSeq.* This would duplicate the work [ADR 0003](0003-datafusion-arrow-substrate.md) and [ADR 0006](0006-multi-path-execution-strategy.md) assign to DataFusion. The compiler adds only the decisions DataFusion cannot make: sequence semantics and kernel selection.

## Consequences

- A kernel's eligibility conditions move from prose (`docs/BENCHMARK.md`) and per-op entry points into the compiler's recognition. The supported-op matrix records them per op, and a test keeps the matrix in sync with the code.
- Every kernel needs a parity test against the generic path before it lands, and each decline reason is visible through `_explain_pipeline()`.
- Ops that stay outside the IR remain barriers. The matrix lists them, so it does not understate what is not compiled.
- Until B merges, nothing in this ADR is implemented. Only the diagnostics base exists: structured operator counts and the plans, read from the same planning as `explain_plan()`.

## Sources

- #229 (EPIC: whole-pipeline compiler) and its workstreams #230–#237
- #238 (diagnostics, this ADR, the supported-op matrix), including the 2026-10-11 status note that assigns this number
- `src/ops/explain.rs`, `py-ltseq/tests/test_explain_pipeline.py` (diagnostics base)
- [ADR 0005](0005-no-materialization-rule.md), [ADR 0006](0006-multi-path-execution-strategy.md), [ADR 0007](0007-lambda-dsl-schemaproxy.md), [ADR 0008](0008-explicit-sort-metadata.md), [ADR 0009](0009-metadata-single-source-of-truth.md)
