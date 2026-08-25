# CI 补 cargo test/clippy + transpiler 表驱动单测 实现计划（issue #150）

> **状态：已完成（2026-08-25，PR #166）。** 本文档为归档的过程记录。

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** CI gate 加入 `cargo test` 与 `cargo clippy --all-targets -- -D warnings`，并为 transpiler 纯函数（运算符映射、字面量解析、错误分类）补表驱动 Rust 单测。

**Architecture:** 三段推进：先修 Cargo.toml 的 pyo3 feature 门控让 `cargo test` 在 Linux 上可链接；再清零 9 条存量 clippy 告警使 `-D warnings` 可落地；最后在 `src/transpiler/mod.rs` 内加 `#[cfg(test)]` 模块直接测私有纯函数，并把两条命令接进 `.github/workflows/ci.yml`。

**Tech Stack:** Rust (pyo3 0.29 / DataFusion 55), GitHub Actions, maturin。

**Spec:** https://github.com/Lychee-Technology/ltseq/issues/150（正文 + 两条评论；评论已把范围收敛为「先覆盖 operator/literal/错误分类映射表」，FloorDiv 端到端回归归 #147）

## Global Constraints

- 验收标准（issue 原文）：CI 包含 cargo test + clippy；transpiler 运算符映射表的每一行都有对应单测。
- clippy gate 命令固定为 `cargo clippy --all-targets -- -D warnings`（issue 评论指定 `--all-targets` 以覆盖 test target）。
- 不纳入 rustfmt gate（当前 `cargo fmt --check` 不干净，属另行 issue 的范围）。
- 不测 `dict_to_py_expr`（需要 Python 解释器；评论指定先覆盖纯函数映射表）。
- 提交信息沿用仓库惯例：conventional commit 前缀 + 中文描述 + `(#150)`。
- 每个 task 结束时 `cargo test` 与（Task 2 起）`cargo clippy --all-targets -- -D warnings` 必须通过。

---

### Task 1: Cargo.toml pyo3 feature 门控（让 Linux CI 能跑 cargo test）

**背景:** `Cargo.toml` 里 pyo3 硬编码 `extension-module` feature。macOS 上 pyo3 会注入 `-undefined dynamic_lookup` 所以本地 `cargo test` 能过，但 Linux 上测试二进制会因 Python C-API 符号未定义而链接失败。pyproject.toml 的 `[tool.maturin] features = ["pyo3/extension-module"]` 已存在，因此 maturin 构建不受影响——这正是 pyo3 FAQ 的标准修法。

**Files:**
- Modify: `Cargo.toml:12`

**Interfaces:**
- Produces: `cargo test` / `cargo clippy` 在无 Python 链接需求下可独立运行；`maturin develop` 行为不变（feature 由 pyproject 注入）。

- [x] **Step 1: 修改 Cargo.toml**

```toml
# 将
pyo3 = { version = "0.29.2", features = ["extension-module", "macros"] }
# 改为（extension-module 由 pyproject.toml [tool.maturin] features 在构建扩展时注入）
pyo3 = { version = "0.29.2", features = ["macros"] }
```

- [x] **Step 2: 验证 cargo test 仍通过**

Run: `cargo test`
Expected: 7 passed（format 5 + linear_scan 2）

- [x] **Step 3: 验证 maturin 构建与 Python 测试不回归**

Run: `uv run maturin develop && uv run pytest py-ltseq/tests/ -q`
Expected: 全部通过（当前 main 基线全绿）

- [x] **Step 4: Commit**

```bash
git add Cargo.toml
git commit -m "build: pyo3 extension-module 改由 maturin 注入，解除 cargo test 链接依赖 (#150)"
```

### Task 2: 清零 9 条存量 clippy 告警

**Files:**
- Modify: `src/ops/linear_scan.rs:700`（unneeded_wildcard_pattern）
- Modify: `src/ops/group_window.rs:344`（needless_borrow）
- Modify: `src/ops/parallel_scan.rs:647`（too_many_arguments）、`:706`（needless_range_loop）
- Modify: `src/ops/asof_join.rs:78`（too_many_arguments）
- Modify: `src/ops/join.rs:183`（type_complexity）
- Modify: `src/ops/pivot.rs:121`（redundant_closure）、`:155`（map_clone）
- Modify: `src/lib.rs:942`（too_many_arguments）

**Interfaces:**
- Produces: `cargo clippy --all-targets -- -D warnings` 零告警，Task 4 的 CI gate 才能落地。

- [x] **Step 1: 应用机械修复（4 条 clippy 建议）**

```rust
// linear_scan.rs:700 — 删除 `args: _,`（`..` 已覆盖）
PyExpr::Call { func, on, .. } => {

// group_window.rs:344 — 去掉多余 &
let node = dict_to_group_node(expr_dict)?;

// pivot.rs:121 — 闭包换函数本体
let group_exprs: Vec<Expr> = index_cols.iter().map(col).collect();
// 若 col 因 &String/&str 不匹配无法直接传，退回 |c| col(c.as_str()) 并按 clippy 输出调整

// pivot.rs:155-158 — map(|f| f.clone()) 换 cloned()
.filter_map(|c| schema.field_with_name(c).ok())
.cloned()
```

可先 `cargo clippy --fix --lib -p ltseq_core --allow-dirty` 自动应用，再人工核对 diff。

- [x] **Step 2: needless_range_loop（parallel_scan.rs:706）**

优先真实重构（按 clippy 建议改为 `partition_boundaries.iter_mut().enumerate().take(n).skip(1)` 形态）；若循环体同时索引多个数组导致改写明显变难读，则在循环前一行加：

```rust
#[allow(clippy::needless_range_loop)] // 循环体按 i 同步索引多个数组，改 iterator 反而难读
```

- [x] **Step 3: 结构性告警加定点 allow（3 处 too_many_arguments + 1 处 type_complexity）**

```rust
// parallel_scan.rs:647、asof_join.rs:78、lib.rs:942 各函数定义上方：
#[allow(clippy::too_many_arguments)]

// join.rs:183 函数定义上方：
#[allow(clippy::type_complexity)]
```

不做签名重构——参数打包成 struct 属于行为无关的大改，超出本 issue「守住零告警线」的范围。

- [x] **Step 4: 验证零告警且测试通过**

Run: `cargo clippy --all-targets -- -D warnings && cargo test`
Expected: clippy 无输出退出码 0；7 passed

- [x] **Step 5: 验证 Python 端不回归**

Run: `uv run maturin develop && uv run pytest py-ltseq/tests/ -q`
Expected: 全部通过

- [x] **Step 6: Commit**

```bash
git add src/
git commit -m "chore: 清零 clippy 1.98 存量告警，为 CI -D warnings 铺路 (#150)"
```

### Task 3: transpiler 表驱动单测

**Files:**
- Modify: `src/transpiler/mod.rs`（文件末尾追加 `#[cfg(test)] mod tests`）

**Interfaces:**
- Consumes: 同模块私有函数 `op_str_to_operator`、`parse_literal_expr`，公开函数 `pyexpr_to_datafusion`，`crate::types::PyExpr`。
- Produces: 运算符映射表 13 行逐行断言 + 未知运算符错误；字面量每个 dtype 分支 + 解析失败 + 未知 dtype；错误分类（列不存在 / 未知一元运算符 / Window 拒绝 / 未知方法 / 窗口函数进行级上下文 / 字符串·时间列类型校验）。

- [x] **Step 1: 写测试（先写全表，预期直接通过——被测函数已存在，这是补盲区不是 TDD 新功能；重点是每行断言与实现逐一对得上）**

在 `src/transpiler/mod.rs` 末尾追加：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;
    use datafusion::common::Column;

    fn test_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("s", DataType::Utf8, false),
            Field::new("d", DataType::Date32, false),
        ])
    }

    fn col_expr(name: &str) -> PyExpr {
        PyExpr::Column(name.to_string())
    }

    fn lit_expr(value: &str, dtype: &str) -> PyExpr {
        PyExpr::Literal { value: value.to_string(), dtype: dtype.to_string() }
    }

    // ---- op_str_to_operator: 映射表逐行 ----

    #[test]
    fn operator_mapping_table() {
        let table = [
            ("Add", Operator::Plus),
            ("Sub", Operator::Minus),
            ("Mul", Operator::Multiply),
            ("Div", Operator::Divide),
            ("Mod", Operator::Modulo),
            ("Eq", Operator::Eq),
            ("Ne", Operator::NotEq),
            ("Lt", Operator::Lt),
            ("Le", Operator::LtEq),
            ("Gt", Operator::Gt),
            ("Ge", Operator::GtEq),
            ("And", Operator::And),
            ("Or", Operator::Or),
        ];
        for (name, expected) in table {
            assert_eq!(op_str_to_operator(name), Ok(expected), "op {name}");
        }
    }

    #[test]
    fn operator_unknown_is_error() {
        // FloorDiv 可在 Python 端序列化但内核不支持（#147 跟进端到端行为）——
        // 必须走错误路径而不是静默映射
        for bad in ["FloorDiv", "Pow", "BitXor", ""] {
            let err = op_str_to_operator(bad).unwrap_err();
            assert!(err.contains("Unknown binary operator"), "op {bad}: {err}");
        }
    }

    // ---- parse_literal_expr: 每个 dtype 分支 ----

    #[test]
    fn literal_dtype_table() {
        let table: Vec<(&str, &str, Expr)> = vec![
            ("42", "Int64", lit(42_i64)),
            ("-7", "Int32", lit(-7_i32)),
            ("2.5", "Float64", lit(2.5_f64)),
            ("1.5", "Float32", lit(1.5_f32)),
            ("hello", "String", lit("hello")),
            ("hello", "Utf8", lit("hello")),
            ("True", "Boolean", lit(true)),
            ("False", "Boolean", lit(false)),
            ("true", "Bool", lit(true)),
            ("false", "Bool", lit(false)),
            ("", "Null", lit(ScalarValue::Null)),
        ];
        for (value, dtype, expected) in table {
            assert_eq!(
                parse_literal_expr(value, dtype),
                Ok(expected),
                "literal {value}:{dtype}"
            );
        }
    }

    #[test]
    fn literal_parse_failures() {
        let table = [
            ("abc", "Int64", "Failed to parse"),
            ("1.5", "Int64", "Failed to parse"),
            ("abc", "Int32", "Failed to parse"),
            ("abc", "Float64", "Failed to parse"),
            ("abc", "Float32", "Failed to parse"),
            ("maybe", "Boolean", "Failed to parse"),
            ("1", "Decimal128", "Unknown dtype"),
            ("x", "", "Unknown dtype"),
        ];
        for (value, dtype, expected_msg) in table {
            let err = parse_literal_expr(value, dtype).unwrap_err();
            assert!(err.contains(expected_msg), "literal {value}:{dtype}: {err}");
        }
    }

    // ---- pyexpr_to_datafusion: 结构与错误分类 ----

    #[test]
    fn column_resolves_case_sensitive_unqualified() {
        let schema = test_schema();
        let expr = pyexpr_to_datafusion(col_expr("a"), &schema).unwrap();
        assert_eq!(expr, Expr::Column(Column::new_unqualified("a")));
    }

    #[test]
    fn column_missing_is_error() {
        let schema = test_schema();
        let err = pyexpr_to_datafusion(col_expr("nope"), &schema).unwrap_err();
        assert!(err.contains("Column 'nope' not found in schema"), "{err}");
    }

    #[test]
    fn binop_builds_binary_expr() {
        let schema = test_schema();
        let expr = pyexpr_to_datafusion(
            PyExpr::BinOp {
                op: "Gt".to_string(),
                left: Box::new(col_expr("a")),
                right: Box::new(lit_expr("5", "Int64")),
            },
            &schema,
        )
        .unwrap();
        let expected = Expr::Column(Column::new_unqualified("a")).gt(lit(5_i64));
        assert_eq!(expr, expected);
    }

    #[test]
    fn binop_unknown_operator_is_error() {
        let schema = test_schema();
        let err = pyexpr_to_datafusion(
            PyExpr::BinOp {
                op: "FloorDiv".to_string(),
                left: Box::new(col_expr("a")),
                right: Box::new(col_expr("b")),
            },
            &schema,
        )
        .unwrap_err();
        assert!(err.contains("Unknown binary operator: FloorDiv"), "{err}");
    }

    #[test]
    fn unaryop_not_and_unknown() {
        let schema = test_schema();
        let ok = pyexpr_to_datafusion(
            PyExpr::UnaryOp { op: "Not".to_string(), operand: Box::new(col_expr("a")) },
            &schema,
        )
        .unwrap();
        assert_eq!(ok, Expr::Column(Column::new_unqualified("a")).not());

        let err = pyexpr_to_datafusion(
            PyExpr::UnaryOp { op: "Neg".to_string(), operand: Box::new(col_expr("a")) },
            &schema,
        )
        .unwrap_err();
        assert!(err.contains("Unknown unary operator: Neg"), "{err}");
    }

    #[test]
    fn alias_wraps_inner_expr() {
        let schema = test_schema();
        let expr = pyexpr_to_datafusion(
            PyExpr::Alias { expr: Box::new(col_expr("a")), alias: "renamed".to_string() },
            &schema,
        )
        .unwrap();
        assert_eq!(expr, Expr::Column(Column::new_unqualified("a")).alias("renamed"));
    }

    #[test]
    fn window_variant_rejected_in_row_context() {
        let schema = test_schema();
        let err = pyexpr_to_datafusion(
            PyExpr::Window {
                expr: Box::new(col_expr("a")),
                partition_by: None,
                order_by: None,
                descending: false,
            },
            &schema,
        )
        .unwrap_err();
        assert!(err.contains("window planner"), "{err}");
    }

    #[test]
    fn window_function_call_rejected_in_row_context() {
        let schema = test_schema();
        for func in ["shift", "rolling", "diff", "cum_sum", "cum_max", "cum_min"] {
            let err = pyexpr_to_datafusion(
                PyExpr::Call {
                    func: func.to_string(),
                    args: vec![],
                    kwargs: Default::default(),
                    on: Box::new(col_expr("a")),
                },
                &schema,
            )
            .unwrap_err();
            assert!(err.contains("requires DataFrame context"), "func {func}: {err}");
        }
    }

    #[test]
    fn unknown_method_is_error() {
        let schema = test_schema();
        let err = pyexpr_to_datafusion(
            PyExpr::Call {
                func: "made_up".to_string(),
                args: vec![],
                kwargs: Default::default(),
                on: Box::new(col_expr("a")),
            },
            &schema,
        )
        .unwrap_err();
        assert!(err.contains("Method 'made_up' not yet supported"), "{err}");
    }

    #[test]
    fn string_function_on_non_string_column_is_error() {
        let schema = test_schema();
        let err = pyexpr_to_datafusion(
            PyExpr::Call {
                func: "str_lower".to_string(),
                args: vec![],
                kwargs: Default::default(),
                on: Box::new(col_expr("a")),
            },
            &schema,
        )
        .unwrap_err();
        assert!(err.contains("requires a string column"), "{err}");
    }

    #[test]
    fn temporal_function_on_non_temporal_column_is_error() {
        let schema = test_schema();
        let err = pyexpr_to_datafusion(
            PyExpr::Call {
                func: "dt_year".to_string(),
                args: vec![],
                kwargs: Default::default(),
                on: Box::new(col_expr("a")),
            },
            &schema,
        )
        .unwrap_err();
        assert!(err.contains("requires a date/datetime column"), "{err}");
    }
}
```

注意事项：
- `op_str_to_operator` 返回 `Result<Operator, String>`，`Operator`/`Expr` 均实现 `PartialEq`，可直接 `assert_eq!`。
- `pyexpr_to_datafusion` 会先跑 `optimize_expr`（常量折叠）——上述用例都含列引用或单字面量，不会被折叠改形；如有断言因折叠失败，改为直接调 `pyexpr_to_datafusion_inner`（同模块可见）。
- `kwargs: Default::default()` 即空 `HashMap<String, PyExpr>`。

- [x] **Step 2: 跑测试**

Run: `cargo test transpiler`
Expected: 上述 13 个测试全部 PASS（若有断言与实现细节不符——如错误文案、`lit` 的 ScalarValue 形态——以实现为准修断言，但「表逐行覆盖」不得缩水）

- [x] **Step 3: clippy 含 test target 复验**

Run: `cargo clippy --all-targets -- -D warnings && cargo test`
Expected: 零告警；全部测试通过（7 + 13 = 20 个）

- [x] **Step 4: Commit**

```bash
git add src/transpiler/mod.rs
git commit -m "test: transpiler 运算符/字面量/错误分类表驱动单测 (#150)"
```

### Task 4: CI 接入 cargo test + clippy

**Files:**
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: Task 1 的可链接 `cargo test`、Task 2 的零告警基线。

- [x] **Step 1: 修改 workflow**

在 `Install Rust toolchain` step 加 clippy 组件，并在 `Cache Cargo` 之后、`Set up Python 3.13` 之前插入两个 step（Rust gate 不依赖 Python 环境，前置可快速失败）：

```yaml
      - name: Install Rust toolchain
        uses: dtolnay/rust-toolchain@stable
        with:
          components: clippy

      # ...（Cache Cargo 不变）...

      - name: Run clippy
        run: cargo clippy --all-targets -- -D warnings

      - name: Run Rust tests
        run: cargo test
```

- [x] **Step 2: 本地等价验证**

Run: `cargo clippy --all-targets -- -D warnings && cargo test`
Expected: 零告警、20 个测试通过

- [x] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: gate 加入 cargo test 与 clippy -D warnings (#150)"
```

### Task 5: 全量验收 + PR

- [x] **Step 1: 全量验证**

Run:
```bash
cargo clippy --all-targets -- -D warnings && cargo test
uv run maturin develop && uv run pytest py-ltseq/tests/ -q
```
Expected: 全绿

- [x] **Step 2: 推分支开 PR**

```bash
git push -u origin <branch>
gh pr create --title "ci: 补 cargo test/clippy gate；transpiler 补表驱动单测 (#150)" --body "Closes #150 ..."
```

PR body 需说明：pyo3 feature 门控的动机（Linux 链接）、clippy 清零策略（机械修复 vs 定点 allow 的划分）、单测覆盖面与 #147 的边界、rustfmt 未纳入的原因。

- [x] **Step 3: 确认 CI 两个新 step 真实执行且通过**

Run: `gh pr checks --watch`
Expected: CI 通过，日志中可见 clippy 与 cargo test step 运行（防止 step 被 YAML 错误静默跳过）
