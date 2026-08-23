# ADR 0002: Rust 执行内核 + Python 薄表面，经 PyO3/maturin 绑定

- 状态：已采纳（Accepted）
- 决策日期：早于本记录 · 记录日期：2026-07-26

[English](0002-rust-core-python-surface.md)

## 背景

项目同时需要流畅的 Pythonic API（lambda、链式调用、良好的报错）和对大规模列式数据的高性能、一致性执行。全用 Python 太慢；全用 Rust 牺牲人体工学。

## 决策

把系统切成两层，边界职责严格划分：Python 负责表达语法，Rust 负责执行语义。

- **Python 层**（`py-ltseq/ltseq/`）：公开 API 表面。负责表达式捕获（见 [ADR 0007](0007-lambda-dsl-schemaproxy.cn.md)）以及决定返回哪种包装对象（见 [ADR 0010](0010-four-table-object-types.cn.md)）；schema/排序元数据从 Rust 内核读取（[ADR 0009](0009-metadata-single-source-of-truth.cn.md)）。这一层刻意保持"薄"，不得自己做重型数据处理。
- **Rust 层**（`src/`）：负责计划构建、表达式转译、执行，以及专用序列算法。
- **边界**：PyO3 0.27.2 绑定，maturin 构建（Rust 代码变更后需 `maturin develop`）。

这条分界贯穿整个架构。

## 影响与取舍

- 性能关键路径在 Rust 中向量化执行；Python 只是编排壳。
- 任何计划形状的变更都要跨越边界；schema 与排序元数据由 Rust 内核持有，Python 侧只做读取（或缓存）（[ADR 0009](0009-metadata-single-source-of-truth.cn.md)、[ADR 0008](0008-explicit-sort-metadata.cn.md)）。
- 贡献者需要 Rust 工具链，Rust 变更后必须重建扩展；纯 Python 的快速迭代只在表面层可行。
- 边界两侧各自的代码组织方式本身也是一条已记录的决策（见 [ADR 0012](0012-rust-thin-shell-python-mixins.cn.md)）。

## 来源

- `docs/ARCHITECTURE.cn.md`: 分层架构、PyO3 边界设计
- `docs/DESIGN_SUMMARY.cn.md`: §1.1
- `docs/USER_MODEL.cn.md`: 一句话总结
- `README.md`: Technology Stack
- `Cargo.toml`: pyo3 0.27.2、datafusion 54.0.0
