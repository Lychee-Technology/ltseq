# ADR 0012: 代码组织 —— Rust 薄壳 + `src/ops/*`，Python Mixin 组合

- 状态：已采纳（Accepted）
- 日期：2026-07-26（补记；决策早于本 ADR）

[English](0012-rust-thin-shell-python-mixins.md)

## 背景

边界两侧都有巨型文件的风险：PyO3 每个 struct 只允许**一个 `#[pymethods]` 块**，天然会把所有实现吸进 `lib.rs`；而宽广的 Python API 表面则有 god-class 风险。

## 决策

**Rust 侧——薄壳 + helper。** 通过单个 `#[pymodule]` 暴露扩展。`LTSeqTable` 所有面向 Python 的方法集中在 `lib.rs` 唯一的 `#[pymethods]` 块里，以 1–3 行的委托 stub 调用 `src/ops/` 中的 helper 函数（basic、derive、window、sort、grouping、join、asof_join、aggregation、set_ops、pattern_match、linear_scan、parallel_scan、align、pivot、mutation、io、common）。这"既是 PyO3 的现实约束，也是可维护性选择"：签名集中、实现模块化，`lib.rs` 永远不会变成几千行的执行文件。给读者的推论：*不要假设逻辑在 `lib.rs` 里*。

**Python 侧——mixin 组合。** `LTSeq` 在 `core.py` 中由分类 mixin 组装（`io_ops.py`、`transforms.py`、`joins.py`、`aggregation.py`、`advanced_ops.py`、`mutation_mixin.py`、`lookup.py`），既保持公开 API 的广度，又避免单体文件，同时对用户仍是单一类。Python 侧调用 `_inner`，拿回新的 Rust 表，经 `_from_inner()` 包装。

**Rust 代码质量标准**（`docs/rust-coding-std.md`）：最小公开 API；优先借用而非 clone；`Result`/`Option` 配自定义错误类型（`thiserror`/`anyhow`），生产代码不 `unwrap`；CI 强制 Clippy + rustfmt；领域概念用 newtype；优先 trait/enum/组合而非经典 OO 模式；重构必须有测试安全网、以编译器检查的小步进行；把代码坏味道当作改进的前瞻信号而非失败。

## 影响与取舍

- 找一个操作的实现是两跳导航（`lib.rs` stub → `src/ops/<分类>.rs`），`MODULE_GUIDE.cn.md` 把这记为预期的阅读路径。
- 新增方法要同时改 `#[pymethods]` 块和某个 ops 模块（外加 Python mixin 与 stubs，见 [ADR 0014](0014-pyi-stubs-typed-surface.cn.md)）。
- mixin 按类别拆分组织代码，但不改变用户可见类型（*语义*包装类型见 [ADR 0010](0010-four-table-object-types.cn.md)）。

## 来源

- `CLAUDE.md` — Key Design Patterns（PyO3 单 `#[pymethods]` 约束、mixin 组合）
- `docs/ARCHITECTURE.cn.md` — PyO3 边界设计、Rust/Python 包结构
- `docs/DESIGN_SUMMARY.cn.md` — §1.3、§1.4
- `docs/MODULE_GUIDE.cn.md` — `src/lib.rs`、模块导览
- `docs/rust-coding-std.md`
