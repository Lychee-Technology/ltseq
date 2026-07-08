# 窗口表达式统一 `.over()` 表面 — 设计（issue #117）

## 背景

LTSeq 当前有两套窗口范式，用户要记两套规则：

1. **序列窗口** `shift`/`rolling`/`diff`/`cum_sum`/`cum_max`/`cum_min`：依赖前置 `sort()`（表序），分区通过 `partition_by=` kwarg 指定。
2. **排名窗口** `row_number`/`rank`/`dense_rank`/`ntile`：用 `.over(partition_by=, order_by=, desc=)`。

`.over()`（`expr/types.py`，产出 `WindowExpr`）目前只服务排名函数；对序列窗口调用会抛
`NotImplementedError` 并指向本 issue。底层窗口 planner（`src/transpiler/window_native.rs`）
**已经**为全部序列窗口实现了 `partition_by`（`extract_partition_by`/`finalize_window_expr`），
`tests/test_window_partition_by.py` 有覆盖。因此本 issue 是**纯 API 统一**，不涉及新的窗口计算能力。

## 目标

让序列窗口表达式也接受可选 `.over()`，与排名窗口共用一套窗口规格入口。规则收敛为一句话：

> **窗口表达式默认用表序，`.over()` 可覆盖分区/排序。**

```python
t.derive(
    prev=lambda r: r.close.shift(1).over(partition_by=r.symbol),
    ma5=lambda r: r.close.rolling(5).mean().over(partition_by=r.symbol),
    peak=lambda r: r.price.cum_max().over(partition_by=r.symbol),
)
```

## 已决策项

- **共存规则**：`.over(partition_by=...)` 与 `partition_by=` kwarg 在同一表达式同时出现 → **报错**
  （`ValueError`，二选一）。不做隐式优先级，避免「我明明写了 kwarg 却没生效」的静默惊喜；日后想放宽很容易。
- **支持维度**：序列窗口的 `.over()` 支持 `partition_by` **和** `order_by`(+`desc`)。`order_by` 覆盖表序。

## 设计

### 方案选择（Rust 路由）

采用**方案 A：注入 + 复用现有转换器**。在 `PyExpr::Window` 分支识别被包裹的 Call 是序列窗口后，
把 wrapper 的 `partition_by` 折叠进内层 Call 的 kwargs，算出有效 `order_by`（wrapper 自带的，否则退回表序），
再重新分派给现有的 `convert_shift/diff/cum_agg/rolling_agg`。这四个转换器已从 kwargs 读 `partition_by`、
以 `order_by: &[Sort]` 接收排序，故几乎无需新逻辑，且 `test_window_partition_by.py` 的 kwarg 路径完全不动。

（否决方案 B：重构四个转换器签名改为显式 `partition_by_exprs` 参数——签名更干净，但改动面大，
对现有绿测与调用点扰动更多。）

### Python 表面（`expr/types.py`）

- 新增模块级 helper `_is_sequence_window(call: CallExpr) -> bool`，镜像 Rust 的 `is_window_call`：
  对 `shift/diff/cum_sum/cum_max/cum_min` 为真；对 `mean/sum/min/max/count/std` 当 `call.on` 是 `rolling(...)` 调用时为真。
- `CallExpr.over()`：当 `func in _RANKING_FUNCS` **或** `_is_sequence_window(self)` 时接受调用。
  只有真正的非窗口表达式（如 `r.age.over(...)`）仍然报错，但改为普通 `ValueError`，删去「#117 未实现」话术。
- **共存守卫**：构造 `WindowExpr` 前，若 `.over()` 传了 `partition_by`，且目标节点已带 `partition_by` kwarg
  （shift/diff/cum_* 看 `self.kwargs`，rolling 看 `self.on.kwargs`）→ 抛 `ValueError` 提示二选一。
- `WindowExpr` 的序列化**保持不变**：现有 `{"type":"Window", expr, partition_by, order_by, descending}`
  已能携带全部信息，`desc`/`descending` 也已在 `over()` 内解析为单一 `descending` bool。

### Rust 路由（`window_native.rs`）

- 把 `pyexpr_to_window_inner` 里的 `PyExpr::Window` 分支从「直接调 `convert_window_ranking`」
  改为一个小分派器：
  - 内层 Call 的 `func` 是排名函数 → `convert_window_ranking`（**不变**）。
  - 是序列窗口（复用 `is_window_call`）→ 新的 `convert_window_sequence`。
  - 否则 → 报错（防御；Python 已守卫）。
- `convert_window_sequence(window_expr, schema, table_order_by)`：
  1. 有效 `order_by`：wrapper 带 `order_by` 时由它 + `descending` 构造单个 `Sort`
     （镜像 `convert_window_ranking` 现有逻辑），否则退回 `table_order_by`。
  2. 把 wrapper 的 `partition_by` 注入内层 Call 的 kwargs——rolling-agg 注入到 `rolling` 节点，
     其余注入 Call 自身——得到合成的 Call `PyExpr`。
  3. 重新分派：`pyexpr_to_window_inner(synthesized_call, schema, &effective_order_by)`，
     落到现有 `convert_shift/diff/cum_agg/rolling_agg`。

### 实现注记

- **order-by 冗余过滤**：现有 kwarg 路径在公共入口 `pyexpr_to_window_expr` 用 `peek_partition_by_cols`
  过滤掉与分区列重复的表序键。由于 `PyExpr::Window` 今天 peek 为 `[]`，排名窗口本就跳过该过滤——
  故 `.over()` 序列路径与现状一致。用户显式给 `.over(order_by=)` 时按原样使用（无需过滤）。
  这在正确性上中性；最坏情况只是退回表序且带分区时多一个冗余排序键，与排名窗口今天的行为相同。
- **单列 `order_by`**：`WindowExpr.order_by` 今天是单表达式（与排名 `.over()` 一致），序列 `.over(order_by=)`
  保持单列以对齐；多键 `.over()` 排序作为可分离的后续项。表序退路仍是多键。

## 测试

- **新增 `test_window_over.py`**，对 `shift / diff / cum_sum / cum_max / cum_min / rolling(n).mean()` 各自覆盖：
  - `.over(partition_by=r.grp)` 与等价 `partition_by=` kwarg 结果一致（对现有分组 fixture 做 parity 断言）。
  - `.over(partition_by=..., order_by=..., desc=...)` 覆盖表序——对一个不同排序的表断言排序确实改变。
  - 仅 `.over(order_by=...)`（无分区）覆盖表序。
  - **共存报错**：`r.v.shift(1, partition_by="grp").over(partition_by=r.grp)` 抛 `ValueError`
    （以及 rolling 变体，kwarg 在内层调用上）。
  - 非窗口守卫：`r.age.over(...)` 仍报错。
- **回归**：`test_window_partition_by.py` 与 `test_ranking.py` 全绿——kwarg 路径与排名 `.over()` 不动。

## 文档

- 更新 `api.md` / `api.cn.md`：把 `.over()` 记为统一窗口入口，写明一句话规则与
  与 `partition_by=` kwarg 的互斥；`kwarg` 形态仍作为受支持的等价写法保留。
- 刷新 `types.py` 顶部指向 #117 的注释块（缺口已闭合）。

## 非目标（YAGNI）

- 多列 `.over(order_by=[...])`。
- 改动或弃用现有 `partition_by=` kwarg（保留为等价写法）。
- 任何排名函数语义的改动。

## 业界对照

PySpark 走 `.over(Window.partitionBy(...).orderBy(...))`；Polars 用 `.over()` 表达 partition。
本 issue 让 LTSeq 两套窗口在 API 表面收敛为一致的 `.over()` 心智模型。
