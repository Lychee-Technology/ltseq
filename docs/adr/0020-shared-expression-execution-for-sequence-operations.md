# ADR 0020: Sequence Operations Share Expression Execution

- Status: Accepted (the implementation is tracked in #188 and is not done)
- Decision date: 2026-11-28, owner decision D3 on #188 (roadmap #281, step 53) · Recorded: 2026-11-28
- Amends: [ADR 0006](0006-multi-path-execution-strategy.md)

[中文版](0020-shared-expression-execution-for-sequence-operations.cn.md)

## Context

`search_pattern` and `search_pattern_count` evaluate their step predicates with their own interpreter of `PyExpr` (`src/ops/pattern_match.rs`: `eval_predicate`, `eval_expr`, `eval_binop`, `eval_call`). The sorted-Parquet count (`src/ops/parallel_scan.rs`) calls the same interpreter. `filter`, `derive` and `search_first` go through the transpiler instead (`src/transpiler/`), which resolves types with DataFusion's coercion ([ADR 0018](0018-literal-exactness-facts-and-policy.md)) and hands the expression to DataFusion.

Every semantic rule has had to be written twice, and the second copy has lagged behind. The step interpreter lacked coercion (#188), had non-Kleene `&` and `|` (#220), truncated integer `/` (#218), compared encoded floats by bit pattern (#292), and supports five functions (`starts_with`, `ends_with`, `contains`, `is_null`, `is_not_null`) where `filter` supports the whole DSL. Each gap was found separately and fixed separately. Some fixes already route work back to the transpiler: since PR #377 a comparison with a literal operand is transpiled and compiled into a DataFusion physical expression on every batch (`eval_lowered_binop`). The two copies still disagree. At `ca59086`, `r.x * 4 > 0` with `x = 2**62` raises in `search_pattern` (Arrow's checked kernels) and wraps silently in `filter` (DataFusion's `BinaryExpr`).

#188 listed two options: coerce inside the interpreter (option 1, in effect since PR #377) or evaluate steps as DataFusion expressions (option 2).

## Decision

The owner chose option 2 and made it a general rule:

> LTSeq must not maintain an independent production expression evaluator for sequence operations when the same semantics can be provided through shared expression resolution and DataFusion execution. Specialized sequence kernels must share the semantic authority or decline to the generic reference path.

1. **LTSeq owns the semantics. DataFusion executes.** The Python DSL, its serialized `PyExpr`, type resolution and exact coercion, the literal policy, checked arithmetic, Python division, NULL and NaN rules, which functions a context admits, row-wise demand, exception classes, declared order and the sequence matching itself stay LTSeq's. They are written once, in the shared lowering (`src/transpiler/` and the UDFs and kernels it calls). DataFusion's logical and physical expressions execute what that lowering produces. DataFusion's default behavior is not the contract where the two differ.
2. **Steps are compiled once.** A `search_pattern` step is lowered by the transpiler and compiled to one physical expression against the schema of the executed plan, at the operation's planning boundary. The compiled expression is evaluated on each batch. No DataFrame or query plan is built per row or per batch to evaluate a predicate.
3. **Steps are evaluated where they are demanded.** Step *k* is evaluated only for starts whose earlier steps were TRUE (contract §10.6, §20.2). The matcher does this by row selection or masking around the compiled expression. Inside one step, demand (`&`, `|`, `when`, `coalesce`) comes from the shared lowering (M16, #252), not from code specific to patterns.
4. **The interpreter is removed, not kept.** Once the shared path passes the gates in #188, the step interpreter and the code that exists only for it are deleted. It is not kept as a fallback for unsupported expressions, behind a feature flag or runtime switch, as a deprecated copy, or as a production correctness reference. An expression the shared path cannot evaluate is refused with a typed error; it is never interpreted another way.
5. **Specialized kernels share the authority or decline.** A sequence kernel (the sorted-Parquet count, a prefix loop) may stay only if it has demonstrated value specific to sequences, evaluates predicates through the shared lowering, has eligibility rules that are explicit and tested, and agrees with the general path on results and errors (contract §21.2). For an input it cannot handle it returns a typed decline before it reports anything (#152's `Miss::Unsupported`). An error on a demanded row is that error, not a decline. A kernel that cannot meet this is removed or narrowed. It does not keep an interpreter of its own.
6. **The context still decides what is legal.** A step is a `Row` predicate (contract §8.1) and admits what `filter` admits. Shared execution does not make aggregates, DataFusion functions outside the DSL, or anything else DataFusion could run legal in a step. Window methods in steps, which §10.6 allows, need staging through the shared window lowering. That is M11's work, and until it lands they are refused as they are now.
7. **Test oracles are not evaluators.** Brute-force pattern references and independent arithmetic models in tests (contract W8, P6) stay. They are not production paths.

## Alternatives Considered

- **Option 1: coerce inside the interpreter.** This is the state since PR #377. It fixes each gap where it is found but leaves every future rule (checked arithmetic, Python `%`, exact decimal comparison, new functions) to be written a second time. Rejected for that reason.
- **Make #234's DataFusion window lowering the production general path.** The lowering #234 sketches (`LEAD` over each step's mask) evaluates every step on every row, so it raises for rows the contract does not demand unless each step is masked by the earlier ones. It stays a reference path. Whether it later replaces the matcher is a separate decision.

## Consequences

- A semantic change to an operator or function reaches every operation that admits it, sequence operations included, in one change.
- Removing the interpreter waits for the shared lowering to hold everything the interpreter does today. The known gap is overflow: the interpreter raises and the shared lowering wraps until M5 (#221). Switching first would turn an error into a silently wrong value. The evaluator's other rules (coercion through `binary_input_types`, true division and floor division through the shared kernels) already come from the shared code.
- Today's interpreter does not meet the demand rule: `search_pattern` computes every step's mask on every row, so an error on a row no start demands is raised. Demand-aware evaluation is new work, not something the migration preserves.
- `group_ordered`'s linear-scan evaluator (`src/ops/linear_scan.rs`) is another independent evaluator. D3's removal mandate covers `search_pattern` only. The invariant applies to linear scan, but its removal or narrowing is M5 and M17 work (#221, roadmap step 85), and no decision on it is recorded here.
- Performance is unmeasured. #188 records a baseline of the current implementation before the interpreter is removed. A regression is a reason to optimize the shared path or to escalate to the owner. It is not a reason to keep the interpreter.

## Sources

- #188 (the decision record and implementation plan), #234 (reference lowering), #252 (M16), #221 (M5), #218 (M6), #372 (partition semantics), #281 (roadmap)
- Contract: `docs/proposals/v0.5/contract/expressions.md` §8.1, `windows-and-grouping.md` §10.6, `errors-and-performance.md` §20.2 and §21.2, `acceptance.md` W8 and P6
- Code at `ca59086`: `src/ops/pattern_match.rs`, `src/ops/parallel_scan.rs`, `src/transpiler/mod.rs` (`pyexpr_to_datafusion`)
