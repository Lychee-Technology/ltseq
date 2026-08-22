# ADR 0011: `link()` 是延迟的前缀别名化 equi-join（不是 pointer/take 结构）

- 状态：已采纳（Accepted）
- 取代：早期的 pointer/take linking 设计（见"演进"）
- 决策日期：早于本记录 · 记录日期：2026-07-26

[English](0011-link-lazy-prefix-aliased-join.md)

## 背景

外键导航与事实表→维表补全需要跨表访问；多跳链路需要无歧义的列名。问题在于 `link()` 底层到底*是*什么，以及它与普通 `join()` 的关系。

## 决策

`link(target, on, as_/alias, join_type)` 记录 join 条件与别名，并预先预览 join 后的 schema（`preview_join_schema`，不执行）。惰性的 DataFusion join 计划在首次消费时**按需**构建（`linking.py::_ensure_join_plan`）并缓存，后续多次变换复用同一个 join 节点；在终端调用之前不执行任何数据计算。

- **命名**：目标表列暴露为 `{alias}_{col}`；源表列保持原名。以前缀引用被关联列（`r.prod_price`，而非 `r.prod.price`）。
- **join 语义**：支持全部四种 join 类型（inner/left/right/full）；条件仅限等值；复合键用 `&`（不支持 `|`）。
- **变换在 join 后的计划上运行。** `LinkedTable` 上的每个 `select`/`filter`/`derive`/`sort`/`slice`/`distinct` 都作用于 join 后的计划并返回普通 `LTSeq`，因此行数跟随 join（未匹配行被丢弃/补入；一对多扇出可见）。链式 `link()` 叠加在上一个 join 的真实计划上，后续条件可以引用上一个别名的列。
- **`to_ltseq()`** 返回惰性的 join 后 `LTSeq`；**`collect()`** 触发执行。

### 演进：本决策取代了什么

- 早期设计把 linking 当作 **pointer/take 结构**（廉价的逐行导航）。现行文档明确否定：这是惰性 join，不是 pointer/take 结构，也不是廉价的逐行导航。
- 曾存在**只过滤源表的快捷路径**（"先过滤源表求快"），后被移除："不再有'先过滤源表求快'的快捷方式（它在未匹配/扇出 join 下产出了错误的行）。"正确性压倒手写的提速技巧；谓词/投影下推交给优化器。
- 旧设计的措辞残留仍在：`CLAUDE.md` 把 `linking.py` 描述为 "LinkedTable for pointer-based joins"（测试也写作 "Pointer-based join tests"）；`README.md` FAQ 仍拿 linking 与"全量物化"的 join 对比；`USER_MODEL.md`（Linking 与 Joining）也仍把 `join()` 写成产出物理结果、把 `link()` 写成能让 left-only 操作更便宜。三处相对 `LINKING_GUIDE.cn.md`/`api.cn.md` 及本 ADR 均已过时，应另行更新。

### 相关的 join 表面决策

- **两套并存的表面**：`link()` 把*整个*目标表命名空间化为 `{alias}_col`（保证多跳链路无歧义），并提供 `LinkedTable` 链式语法糖，用于补全场景。`join()` 采用 Polars 式**仅冲突列**后缀（`suffix="_right"`），直接返回 `LTSeq`，用于一次性关系 join。两者在被消费前都不物化。
- **冲突策略**：join 实现先对右表列激进改名、join 后再 alias 回用户可见形态（`src/ops/join.rs`）；正确性与可预期性都依赖这一步。inner/left join 合并（coalesce）重复的右侧键列；right/full join 保留双键。
- **策略矩阵**：`join`（hash，默认）；`join(strategy="merge")` 用于已预排序输入，会*校验*排序，否则抛 `SortRequiredError`；`semi_join`/`anti_join`（`WHERE EXISTS` / `NOT EXISTS`）；`asof_join`（有序/二分搜索，`src/ops/asof_join.rs`，API 对齐 Polars `join_asof`；右表时间列被有意保留，因为"asof 匹配是近似的，匹配到的时间戳本身是真实信息"）；表达式级 `r.col.lookup(target, column, join_key)` 在 `derive()` 期间解析为单列 left join（`lookup.py`），无需用户级 join 步骤即可完成类 join 的补全。

## 影响与取舍

- 心智模型统一：一切都是惰性 DataFusion join；差别只在命名约定与人体工学。
- join 类型固化在计划里，贯穿下游变换；"晚物化"是文档给出的最佳实践。
- 前缀方案以更长的列名换取多跳链路的无歧义。

## 来源

- `docs/LINKING_GUIDE.cn.md`: 全文（含 "link() 与 join()"、被移除的快捷路径、ProjectionPushdown 注意事项）
- `docs/DESIGN_SUMMARY.cn.md`: §4.1、§4.2
- `docs/USER_MODEL.cn.md`: Linking 与 Joining 的区别
- `docs/api.cn.md`: `LTSeq.link`、`LTSeq.join`、`asof_join`、Join 策略总览
- `docs/MODULE_GUIDE.cn.md`: `src/ops/join.rs`、`lookup.py`
- `CLAUDE.md`、`README.md`：过时的 "pointer-based" 措辞（见"演进"）
