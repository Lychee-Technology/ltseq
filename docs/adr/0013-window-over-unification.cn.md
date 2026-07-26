# ADR 0013: 全部窗口表达式统一 `.over()` 表面

- 状态：已采纳（Accepted）
- 决策日期：2026-07-07（设计规格） · 记录日期：2026-07-26
- Issue：[#117](https://github.com/Lychee-Technology/ltseq/issues/117)

[English](0013-window-over-unification.md)

## 背景

LTSeq 曾有两套窗口范式，用户要分别记两套规则：

1. **序列窗口**（`shift`/`rolling`/`diff`/`cum_sum`/`cum_max`/`cum_min`）：依赖前置 `sort()`（表序），分区用 `partition_by=` kwarg。
2. **排名窗口**（`row_number`/`rank`/`dense_rank`/`ntile`）：用 `.over(partition_by=, order_by=, desc=)`。

对序列窗口调用 `.over()` 会抛 `NotImplementedError`。关键前提：Rust 窗口 planner（`src/transpiler/window_native.rs`）**早已**为全部序列窗口实现了 `partition_by`（`extract_partition_by`/`finalize_window_expr`，由 `py-ltseq/tests/test_window_partition_by.py` 覆盖）——因此这是纯 API 统一，**不新增任何计算能力**。

## 决策

序列窗口表达式也接受可选 `.over()`，与排名函数共用一套窗口规格入口。规则收敛为一句话：

> **窗口表达式默认用表序，`.over()` 可覆盖分区/排序。**

- **共存规则**：同一表达式上 `.over(partition_by=...)` 与 `partition_by=` kwarg 同时出现 → **`ValueError`**（二选一）。不做隐式优先级——避免"我明明写了 kwarg 却没生效"的静默惊喜；日后想放宽很容易。
- **支持维度**：序列窗口的 `.over()` 支持 `partition_by` **和** `order_by`(+`desc`)；`order_by` 覆盖表序。
- **非窗口守卫**：对真正的非窗口表达式（如 `r.age.over(...)`）调用 `.over()` 仍然报错——现改为普通 `ValueError`，并删去旧的「#117 未实现」话术。
- **线格式不变**：现有 `{"type":"Window", expr, partition_by, order_by, descending}` 序列化已能携带全部信息——不涉及跨边界协议变更。
- 业界对照：PySpark 的 `.over(Window.partitionBy(...).orderBy(...))` 与 Polars 的 `.over()`；本决策让 LTSeq 两套窗口收敛为一致的心智模型。

## 备选方案

- **采纳——方案 A（注入 + 复用现有转换器）**：`PyExpr::Window` 分支识别到内层是序列窗口 Call 时，把 wrapper 的 `partition_by` 折叠进内层 Call 的 kwargs，算出有效 `order_by`（wrapper 自带的，否则退回表序），再重新分派给现有的 `convert_shift/diff/cum_agg/rolling_agg`。几乎无需新逻辑；kwarg 路径与 `test_window_partition_by.py` 完全不动。
- **否决——方案 B（重构转换器签名）**：改为显式 `partition_by_exprs` 参数——签名更干净，但对现有绿测与调用点的改动面大得多。

## 影响与取舍

- 所有窗口共用一个心智模型；kwarg 形态仍作为受支持的等价写法保留。
- 两个已接受的取舍：(a) 退回表序且带分区时可能多一个冗余排序键（正确性中性；与排名窗口既有行为相同）；(b) `.over(order_by=)` 为单列（与既有排名 `.over()` 对齐）；多键 `.over()` 排序作为可分离的后续项——表序退路仍是多键。
- 显式非目标（YAGNI）：多列 `.over(order_by=[...])`；弃用 `partition_by=` kwarg；改动任何排名函数语义。
- 相关的既有窗口语义（记录于 `api.cn.md`）：`rolling(n)` 遵循 SQL `ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW`（起始处部分帧；有意**不提供 `min_periods`**——传入即报错而非 NULL 填充，否决了 pandas 惯例）；`shift(offset)` 正数向后取，对齐 pandas `Series.shift()`。

## 来源

- `docs/superpowers/specs/2026-07-07-window-over-unification-design.md` — 完整设计（决策、方案 A/B、实现注记、测试计划、非目标）
- `docs/api.cn.md` — §3.1（统一 `.over()` 规则、rolling/shift 语义）
- `src/transpiler/window_native.rs`、`py-ltseq/ltseq/expr/types.py`
- `py-ltseq/tests/test_window_over.py`、`test_window_partition_by.py`
