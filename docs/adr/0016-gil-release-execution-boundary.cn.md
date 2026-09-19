# ADR 0016: 执行阶段释放 GIL（解析 → detach 执行 → 转换）

- 状态：已采纳（Accepted）
- 决策日期：2026-09-19（issue #142） · 记录日期：2026-09-19

[English](0016-gil-release-execution-boundary.md)

## 背景

每个 `#[pymethods]` 入口都在调用线程持有 Python GIL 的状态下运行。#142 之前只有 `count()` 和 `to_arrow_ipc()` 释放了 GIL；其余约 34 处 `RUNTIME.block_on(...)`（全表 collect、文件扫描与写出、rayon Parquet 路径、cursor 的逐批拉取）全部持锁执行。任何一个大查询运行期间，进程内其他 Python 线程（Web worker、Jupyter 后台线程、进度条）全部停摆。

PyO3 0.29 的两个事实决定了修法：

- `Python::detach` 要求闭包及其返回值满足 `Ungil`，稳定版上就是 `Send`。因此编译器会拒绝被捕获的 `Bound<'_, T>`，但**不会**拒绝 `PyErr`（它是 `Send` 的）。
- `From<LtseqError> for PyErr`（经 `raise_custom`）和 `Display for PyErr` 都会调用 `Python::attach`。在 detach 段内构造或格式化 `PyErr`，等于让刚释放 GIL 的线程重新拿回它。今天这样做结果正确，但一旦有人把 future spawn 到别的线程，就是经典的"worker 等 GIL、持 GIL 者等 worker"死锁形态；而序列路径里的 `PARALLEL_FALLBACK` 控制流每次回退都要格式化错误。

## 决策

**所有做真实执行或 IO 的入口，都经 `gil::detached` 执行。** 计划构建（`filter`、`select`、`derive`、`sort`、`join`、`union`、`slice`、`align` 等）是同步且廉价的，留在 GIL 下；原来包裹它们的 `block_on` 已删除，因为其中没有任何 await。

分层按 API 固定：

1. **持 GIL 解析。** 一切触碰 Python 对象的工作（`Bound<PyDict>` → `PyExpr`，dict 值 → `ScalarValue`）在 `lib.rs` stub 或 op 的前半段完成。
2. **detach 执行。** `src/gil.rs::detached(py, || ...)` 为一个只看到纯 Rust 值的闭包释放 GIL。`F: Send` 在编译期把 `Bound`/`Py` 引用挡在外面。
3. **之后转换。** 闭包返回 `Result<T, LtseqError>`；`detached` 在重新持有 GIL 后才构造 `PyErr`。`src/ops/*` 的执行半段（`parallel_scan`、`linear_scan`、`pattern_match`、`asof_join`、`io`、`pivot`、`mutation` 的 `*_exec` 半段、`set_ops` 的快照路径、`grouping::group_ordered_count_impl`）返回 `LtseqError` 而非 `PyResult`，从类型上排除在其中构造 `PyErr` 的可能。`LTSeqTable::require_df/require_schema/from_batches*` 出于同样原因返回 `LtseqError`。

由此有两种 op 形态。整个 impl 是纯 Rust 时（`materialize`、`rvs`、`step`、`asof_join`、`pivot`、`write_*`、`load_arrow_ipc`、`delete_rows`，以及解析后的模式匹配与分组计数 impl），由 `lib.rs` stub 包裹调用。解析与执行交错时（`distinct`、`is_subset`、`insert_row`、`modify_row`、`assume_sorted`），impl 接收 `py: Python<'_>`，只包裹自己的执行部分。

**流式 cursor 在 detach 段内取锁。** `next_batch` 与 `is_exhausted` 绝不在持有 GIL 时等待 stream mutex：一个持 GIL 阻塞在 `lock()` 的线程，会让正处于 `stream.next()` 中的线程永远无法重新 attach，形成 GIL 与 mutex 之间的锁序死锁。争用线程在无 GIL 状态下等待，然后拿到下一批。

## 曾考虑的替代方案

- *在每个 stub 外层统一套 `py.detach`。* 否决：`search_pattern`、`search_pattern_count`、`group_ordered_count`、`insert_row`、`modify_row`、`distinct`、`is_subset`、`assume_sorted` 接收 `Bound<PyDict>` 参数，进不了闭包；必须先把解析拆出来（issue 的评审意见已指出这一点）。
- *闭包内保留 `PyResult`，依赖重新 attach。* 今天能工作，但保留了死锁形态，且每次 `PARALLEL_FALLBACK` 字符串检查都会重新拿 GIL；`LtseqError` 返回类型把规则变成结构性约束而非注释。

## 影响与取舍

- 一个线程上的重查询不再让解释器对其他线程停摆。`py-ltseq/tests/test_gil_release.py` 用心跳线程对每条 detach 路径做度量。
- 新增执行路径意味着：用 `Result<_, LtseqError>` 写它的纯 Rust 半段并用 `detached` 包裹；新增只建计划的变换则完全不需要 `block_on`。
- `format_table` 不再可失败，返回 `String`；`LTSeqCursor::serialize_batch_to_ipc` 与 `to_arrow_ipc` 共用，后者的 IPC 编码现在也在 detach 下运行。
- 未改动：`filter_where` await `session.sql()` 只为解析 WHERE 子句（纯计划，无执行），留在 GIL 下；`get_schema_dict`/`preview_join_schema` 在已 attach 的 pymethod 内调用 `Python::attach`，无害。

## 来源

- Issue #142 及其评审评论（路径清单修正、解析/执行拆分、cursor mutex）
- `src/gil.rs`、`src/lib.rs`、`src/cursor.rs`、`src/ops/*`
- `docs/ARCHITECTURE.md`：PyO3 Boundary Design
- [ADR 0012](0012-rust-thin-shell-python-mixins.cn.md)（薄 stub）、[ADR 0005](0005-no-materialization-rule.cn.md)（哪些路径会执行）
