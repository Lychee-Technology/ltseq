# ADR 0007: Lambda 表达式 DSL（SchemaProxy 捕获、dict 序列化、Rust 转译）

- 状态：已采纳（Accepted）
- 决策日期：早于本记录 · 记录日期：2026-07-26

[English](0007-lambda-dsl-schemaproxy.md)

## 背景

API 应当读起来像 Python（`t.filter(lambda r: r.age > 18)`），又不付出 Python 逐行执行的代价。这要求把 Python lambda 变成 Rust 可规划、可执行的东西。

## 决策

捕获、序列化、转译三步：

1. **捕获。** lambda 对 `SchemaProxy`（`expr/proxy.py`）**只执行一次**，而不是对真实行。属性访问与运算符重载构建出表达式树（`ColumnExpr`、`BinOpExpr`、`CallExpr` 等）。
2. **序列化。** 表达式树序列化为普通 dict，例如 `{"type": "BinOp", "op": ">", "left": {"type": "Column", "name": "age"}, "right": {"type": "Literal", "value": 18}}`。
3. **转译。** Rust 反序列化为 `PyExpr`（`src/types.rs`、`dict_to_py_expr()`），再经**两条路径**之一转成 DataFusion `Expr`：原生表达式（`transpiler/mod.rs`）或原生窗口构建（`transpiler/window_native.rs`）。`transpiler/optimization.rs` 在执行前做表达式化简。（历史上曾有第三条路径，经 `transpiler/sql_gen.rs` 生成 SQL，现已移除，见 [ADR 0006](0006-multi-path-execution-strategy.cn.md) 的“演进”小节。SQL 仅存的用途是 `filter_where` 的 WHERE 子句解析 helper。）

**DSL 刻意受限**：它很好地支持 Pythonic 的*列*表达式，但有意不逐行执行任意 Python，这个取舍让表达式保持可序列化、可在 Rust 执行。Python 无法重载的语法用有限的 AST 变换处理（`is None` / `is not None`，见 `expr/transforms.py`）。真正需要任意逐行逻辑时的逃生口是 `fold()`，一条被显式标记的慢路径（[ADR 0005](0005-no-materialization-rule.cn.md)）。

## 影响与取舍

- 表达式在 Rust 中向量化执行；lambda 本身从不接触数据。
- `r` 不是真实的行对象，这是文档记录的常见用户误区；lambda 内的 Python 控制流（`if`、循环）不会按用户直觉工作。
- `api.cn.md` §11 用 SQL 等价物来记述部分表达式语义（如 `if_else` → `CASE WHEN`、`count_if` → `SUM(CASE WHEN …)`）。这是规格化表述手段，实际执行是原生的。
- 动态 DSL 表面的静态类型需要手写 stubs（[ADR 0014](0014-pyi-stubs-typed-surface.cn.md)）。

## 来源

- `docs/ARCHITECTURE.cn.md`: 表达式链路
- `docs/DESIGN_SUMMARY.cn.md`: §2.1–2.4
- `docs/USER_MODEL.cn.md`: 核心心智模型 #4、表达式模型
- `docs/MODULE_GUIDE.cn.md`: 表达式子系统导览
- `docs/api.cn.md`: §0、§11
- `CLAUDE.md`: Expression Transpilation
