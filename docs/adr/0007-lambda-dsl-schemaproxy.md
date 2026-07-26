# ADR 0007: Lambda Expression DSL — SchemaProxy Capture, Dict Serialization, Rust Transpilation

- Status: Accepted
- Decision date: predates this record · Recorded: 2026-07-26

[中文版](0007-lambda-dsl-schemaproxy.cn.md)

## Context

The API should read like Python (`t.filter(lambda r: r.age > 18)`) without paying Python per-row execution cost. That requires turning Python lambdas into something Rust can plan and execute.

## Decision

Capture, serialize, transpile:

1. **Capture.** A lambda is executed **once** against a `SchemaProxy` (`expr/proxy.py`) — not real rows. Attribute access and operator overloads build an expression tree (`ColumnExpr`, `BinOpExpr`, `CallExpr`, …).
2. **Serialize.** The tree serializes to plain dicts, e.g. `{"type": "BinOp", "op": ">", "left": {"type": "Column", "name": "age"}, "right": {"type": "Literal", "value": 18}}`.
3. **Transpile.** Rust deserializes into `PyExpr` (`src/types.rs`, `dict_to_py_expr()`) and converts to DataFusion `Expr` via one of **two paths**: native expressions (`transpiler/mod.rs`) or native window construction (`transpiler/window_native.rs`). `transpiler/optimization.rs` simplifies expressions before execution. (A third path, SQL generation via `transpiler/sql_gen.rs`, existed historically and has been removed — see the Evolution section of [ADR 0006](0006-multi-path-execution-strategy.md). The one surviving SQL use is the `filter_where` WHERE-clause parser helper.)

**The DSL is intentionally constrained**: it supports Pythonic *column* expressions well but deliberately does not execute arbitrary Python row-by-row — "that tradeoff keeps the system serializable and Rust-executable." What Python cannot overload gets a limited AST transformation (`is None` / `is not None`, in `expr/transforms.py`). The escape hatch for genuinely arbitrary per-row logic is `fold()`, an explicitly-flagged slow path ([ADR 0005](0005-no-materialization-rule.md)).

## Consequences

- Expressions execute vectorized in Rust; the lambda itself never touches data.
- `r` is not a real row object — a documented common user mistake; Python control flow (`if`, loops) inside lambdas does not do what users may expect.
- `api.md` §11 documents several expression semantics by their SQL equivalents (e.g. `if_else` → `CASE WHEN`, `count_if` → `SUM(CASE WHEN …)`) — a specification device; execution is native.
- Static typing of the dynamic DSL surface needs hand-written stubs ([ADR 0014](0014-pyi-stubs-typed-surface.md)).

## Sources

- `docs/ARCHITECTURE.md` — Expression Pipeline
- `docs/DESIGN_SUMMARY.md` — §2.1–2.4
- `docs/USER_MODEL.md` — Core Mental Model #4, Expression Model, Common Mistakes
- `docs/MODULE_GUIDE.md` — Expression Subsystem Tour, transpiler tour
- `docs/api.md` — §0, §11
- `CLAUDE.md` — Expression Transpilation
