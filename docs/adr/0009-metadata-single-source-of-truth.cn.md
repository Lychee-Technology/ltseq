# ADR 0009: Schema 与排序元数据（Rust 是唯一事实源）

- 状态：已采纳（Accepted）
- 取代：早期的双 schema 模型（见"演进"）
- 决策日期：issue #93 实现时 · 记录日期：2026-07-26

[English](0009-metadata-single-source-of-truth.md)

## 背景

表达式捕获和良好的报错需要在 Python 侧、在任何东西跨越边界之前掌握 schema；执行需要 Rust 侧的 Arrow schema。最初的答案是两侧各自独立维护，实践证明这很脆弱（见"演进"）。

## 决策

**Rust 内核拥有权威元数据**：Arrow schema 与声明排序（`sort_specs`）随计划存放。Python 侧不维护平行状态：

- `LTSeq._schema` 是对 `_inner.get_schema_dict()` 的**逐实例惰性缓存**（`core.py`）；setter 仅为迁移期兼容外部代码而保留。
- `LTSeq._sort_keys` 是对 `_inner.get_sort_keys()` 的**不缓存 FFI 读取**；读取很少发生，调用只需微秒级。
- 面向用户的 `schema` 与 `python_schema` 属性是同一份数据的两种视图（`python_schema` 把 Arrow 类型名映射为 Python 类型名），不是两个来源。

回归守卫是 `py-ltseq/tests/test_schema_source_of_truth.py`，其存在就是为了防止旧的双轨跟踪回归。

## 演进：被取代的双 schema 模型

最初，Python 在 Rust Arrow schema 之外手工维护一份 `_schema` dict，并要求"在每个计划形状变化的边界重新同步"。这被记录为设计教训 §7.2 与 #1 长期压力点，而它在实践中确实失败了：`select` 保留了已被投影掉的列，`derive`/`agg`/`pivot` 发明了 "Unknown" 占位类型，重命名排序列使 Python `_sort_keys` 与 Rust `sort_specs` 分叉。issue #93 用上述唯一事实源设计取代了镜像。`ARCHITECTURE.md`（Schema Management）与 `DESIGN_SUMMARY.md` §1.5/§7.2 仍在描述双模型，在这一点上已过时；它们记录的教训，*schema 同步是关键*，正是移除重复维护的原因。

## 影响与取舍

- Python 侧的校验与报错人体工学得以保留（缓存让读取廉价），同时消除了镜像造成的一整类漂移 bug。
- 需要做对的事从"同步"变成"缓存失效"：任何改变计划形状的操作必须返回新包装对象（新缓存）而非原地修改，这一点由不可变表设计（[ADR 0004](0004-lazy-execution-immutable-tables.cn.md)）保证。
- join 的"改名再 alias 回"策略（[ADR 0011](0011-link-lazy-prefix-aliased-join.cn.md)）让 join 之后权威的 Rust schema 保持可预期。

## 来源

- `py-ltseq/ltseq/core.py`: `_schema` 缓存、`_sort_keys` FFI 读取（issue #93 注释）
- `py-ltseq/tests/test_schema_source_of_truth.py`
- `docs/ARCHITECTURE.md`: Schema Management（已过时：描述的是被取代的双模型）
- `docs/DESIGN_SUMMARY.cn.md`: §1.5、§7.2（双模型为何必须退场的理由）
