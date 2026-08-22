# ADR 0010: 四种高层表对象类型，用类型表达语义上下文

- 状态：已采纳（Accepted）
- 决策日期：早于本记录 · 记录日期：2026-07-26

[English](0010-four-table-object-types.md)

## 背景

分组、关联（linking）、分区都会给表附加额外语义。全部塞进一个类会掩盖这些语义；SQL 式"分组即塌缩"会丢掉 LTSeq 关心的顺序性问题（"每个连续段内部的行是什么？""每段的第一行/最后一行？"）。

## 决策

API 有意**用不同的包装类型来表达语义**，返回哪种对象是 Python 编排层的显式职责：

- **`LTSeq`**：普通（有序）表。
- **`NestedTable`**：附加了分组语义的表。`group_ordered()`（别名 `group_consecutive`）与 `group_sorted()` 返回分组后的*顺序上下文*而非立即塌缩。内部列 `__group_id__`、`__group_count__`、`__rn__` 保存组标识与行位置。`NestedTable.derive` 把组值**广播**到每一行（SQL 窗口语义）；`NestedTable.agg` **塌缩**为每组一行（SQL GROUP BY 语义），并明确取代旧的 `derive(...) + distinct(...)` 惯用法。组按原始序列顺序出现；`group_ordered` 只对*连续*相等值分组，从不重排。`len(nested)` 返回行数而非组数；`to_pandas()` 会丢掉 `__group_id__`（需要时用 `flatten()`）。一处有意的不对称：组聚合用字符串列名（`g.sum("amount")`），而 `g.first()`/`g.last()` 返回支持属性访问的行代理。
- **`LinkedTable`**：被另一张表补全过、但仍然惰性的表（见 [ADR 0011](0011-link-lazy-prefix-aliased-join.cn.md)）。
- **`PartitionedTable`**：按键的 dict 式分组访问。`partition(*cols)` / `partition(by=callable)`；可调用键**必须是简单列表达式**（`lambda r: r.region`）；派生表达式（`lambda r: r.price + 1`）抛 `ValueError`，因为它们会被迫内部物化，违反 [ADR 0005](0005-no-materialization-rule.cn.md)。

这四种是**语义角色**，不是惰性保证。`LTSeq`、`NestedTable`、`LinkedTable` 的计划在被消费前保持延迟（[ADR 0004](0004-lazy-execution-immutable-tables.cn.md)）；分区只是部分惰性：发现分区键要执行一次 distinct 查询（`partitioning.py` 中的 `to_arrow()`），字符串键分区背后的具体类是 `SQLPartitionedTable`（对单个分区的*访问*仍是惰性的 SQL 查询），而 `PartitionedTable.map()` 返回 `_PrecomputedPartitionedTable`，其文档明言无惰性求值。

## 影响与取舍

- 类型本身即文档，说明当前生效的语义；用户按所问的问题选择对象（平坦变换 vs 分组上下文 vs 跨表导航 vs 按键访问）。
- 保持惰性 linked/grouped 抽象与平坦表 API 之间的一致性是一个已具名的压力点。

## 来源

- `docs/USER_MODEL.cn.md`: 如何理解不同对象类型、分组模型
- `docs/ARCHITECTURE.cn.md`: 主要运行时对象
- `docs/DESIGN_SUMMARY.cn.md`: §4.3、§4.4
- `docs/api.cn.md`: §0、§4、`LTSeq.partition`、`PartitionedTable`
- `README.md`: Limitations（`group_ordered` 语义）
