<!-- Generated from ../../cn/19_alloc_summary.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 19 · 汇总分摊到明细：组级值广播回行

> **难度** ★★☆ · **领域** 电商 · **前置** 案例 01
>
> **涉及 API**：`join(on=, how="left")` / `group_by().agg()` / `group_sorted` + `NestedTable.derive`（组级广播）/ `cast("float64")` / `sort`

**学习目标**

- 掌握"汇总值分摊回明细行"的两条路线：**join 回填**与**分组广播**（SQL 窗口语义）
- 理解 `NestedTable.derive` 与 `NestedTable.agg` 的区别：**广播保留每行**，折叠
  一组一行
- 再次演练整数除法陷阱——比率计算先 `cast("float64")`

## 问题描述

门店月度销售额 `months(year, month, sales)`，以及一张**年度费用表**
`fees(year, fee)`（如年度营销费）。目标：把年度费用按"**各月销售额占全年的
比例**"分摊到每个月明细行，得到每行 `share` 列。

本质是一个"组内占比"问题：`share = fee / sum(sales of year) * sales`——
分母是**组级**的，分子是**行级**的，两级数据要在一行里会合。

```python
from ltseq import LTSeq

months = LTSeq.from_rows([
    {"year": 2023, "month": "2023-01", "sales": 30},
    {"year": 2023, "month": "2023-02", "sales": 40},
    {"year": 2023, "month": "2023-03", "sales": 50},
    {"year": 2024, "month": "2024-01", "sales": 20},
    {"year": 2024, "month": "2024-02", "sales": 30},
    {"year": 2024, "month": "2024-03", "sales": 30},
    {"year": 2024, "month": "2024-04", "sales": 40},
])

fees = LTSeq.from_rows([
    {"year": 2023, "fee": 12},
    {"year": 2024, "fee": 24},
])

months.show()
fees.show();
```

> **预期输出**
>
> ```text
> +------+---------+-------+
> | year | month   | sales |
> +------+---------+-------+
> | 2023 | 2023-01 | 30    |
> | 2023 | 2023-02 | 40    |
> | 2023 | 2023-03 | 50    |
> | 2024 | 2024-01 | 20    |
> | 2024 | 2024-02 | 30    |
> | 2024 | 2024-03 | 30    |
> | 2024 | 2024-04 | 40    |
> +------+---------+-------+
>
> +------+-----+
> | year | fee |
> +------+-----+
> | 2023 | 12  |
> | 2024 | 24  |
> +------+-----+
> ```

预判：2023 年合计 120，分摊 12 → 3 / 4 / 5；2024 年合计 120，分摊 24 →
4 / 6 / 6 / 8。跑完验证。

## 第一步 · 路线一：join 回填组级值

最朴素的思路：先算出每组的合计，再 join 回明细表——两级数据在 join 后
的同一行会合。

```python
fees_joined = months.join(fees, on="year", how="left")

fees_joined.sort("month").show();
```

> **预期输出**
>
> ```text
> +------+---------+-------+-----+
> | year | month   | sales | fee |
> +------+---------+-------+-----+
> | 2023 | 2023-01 | 30    | 12  |
> | 2023 | 2023-02 | 40    | 12  |
> | 2023 | 2023-03 | 50    | 12  |
> | 2024 | 2024-01 | 20    | 24  |
> | 2024 | 2024-02 | 30    | 24  |
> | 2024 | 2024-03 | 30    | 24  |
> | 2024 | 2024-04 | 40    | 24  |
> +------+---------+-------+-----+
> ```

**解析**

- `join(on="year", how="left")`：year 是同名整数列，直接做键；左表 7 行全部
  保留，一对一匹配不增减行。
- 分摊用的两个组级值（fee、年销售额）一个来自**外部表**、一个来自**自身
  聚合**——所以路线一要 join 两次。路线二（分组广播）只需 join fee 这一次。

## 第二步 · 补上年合计并计算分摊

年合计来自 `months` 自身的分组聚合；`share` 的除法先转型——案例 14 的整数除法
陷阱再次出现。

```python
totals = months.group_by("year").agg(total=lambda g: g.sales.sum())

enriched = (
    fees_joined.join(totals, on="year", how="left")
    .derive(
        share=lambda r: r.fee.cast("float64") / r.total * r.sales,
    )
    .sort("month")
)

enriched.show();
```

> **预期输出**
>
> ```text
> +------+---------+-------+-----+-------+-------+
> | year | month   | sales | fee | total | share |
> +------+---------+-------+-----+-------+-------+
> | 2023 | 2023-01 | 30    | 12  | 120   | 3     |
> | 2023 | 2023-02 | 40    | 12  | 120   | 4     |
> | 2023 | 2023-03 | 50    | 12  | 120   | 5     |
> | 2024 | 2024-01 | 20    | 24  | 120   | 4     |
> | 2024 | 2024-02 | 30    | 24  | 120   | 6     |
> | 2024 | 2024-03 | 30    | 24  | 120   | 6     |
> | 2024 | 2024-04 | 40    | 24  | 120   | 8     |
> +------+---------+-------+-----+-------+-------+
> ```

**解析**

- `12 / 120 * 30`：若不 cast，整数 `/` 整数截断得 `0`，全表 share 全为 0——
  案例 14 的陷阱在业务数据上杀伤力更大，请形成肌肉记忆。
- 验算：每组的 share 之和应恰好等于该组 fee（3+4+5=12 ✔；4+6+6+8=24 ✔）。
  分摊类计算务必做"守恒校验"，这是它与普通派生列的区别。
- 两次 join 后表里带了 `total` 这种冗余列，下游若不需要可以 `drop`。

## 第三步 · 路线二：分组广播（只 join 一次）

`group_sorted` + `NestedTable.derive` 把**组级计算结果广播到组内每一行**——
SQL 窗口函数 `sum() OVER (PARTITION BY year)` 的 LTSeq 形态。外部的 fee 先
join 进来，年合计则由广播直接产生。

```python
nested = (
    fees_joined
    .sort("year", "month")
    .group_sorted(lambda r: r.year)
)

shared = nested.derive(lambda g: {"year_total": g.sum("sales")}).derive(
    share2=lambda r: r.fee.cast("float64") / r.year_total * r.sales,
)

shared.sort("month").show();
```

> **预期输出**
>
> ```text
> +------+---------+-------+-----+------------+--------+
> | year | month   | sales | fee | year_total | share2 |
> +------+---------+-------+-----+------------+--------+
> | 2023 | 2023-01 | 30    | 12  | 120        | 3      |
> | 2023 | 2023-02 | 40    | 12  | 120        | 4      |
> | 2023 | 2023-03 | 50    | 12  | 120        | 5      |
> | 2024 | 2024-01 | 20    | 24  | 120        | 4      |
> | 2024 | 2024-02 | 30    | 24  | 120        | 6      |
> | 2024 | 2024-03 | 30    | 24  | 120        | 6      |
> | 2024 | 2024-04 | 40    | 24  | 120        | 8      |
> +------+---------+-------+-----+------------+--------+
> ```

与路线一结果一致，少了一次 join。✔

**解析**

- `group_sorted(lambda r: r.year)`：分组键必须是**声明排序的首键**——所以先
  `sort("year", "month")`，否则 `group_sorted` 抛 `SortRequiredError`。分组按
  这个顺序里键值相同的连续行划分，和 `group_ordered` 一样。
- `nested.derive(...)` 的 lambda 返回**字典**：每个组算一个 `year_total`，然后
  广播到组内每行——保留全部明细行，这正是 `derive` 与 `agg`（折叠成一行）
  的分野。
- **两条路线怎么选**：组级值已经在别的表里 → join；组级值要从本表现算 →
  广播；两者都要（如本例）→ 先 join 外部值，再广播本表聚合，整体只 join
  一次。

## 关键点解析

- **组级-行级会合的三件武器**：join 回填、`NestedTable.derive` 广播、
  `NestedTable.agg` 折叠。分摊/占比类需求用前两个，摘要类需求用第三个。
- **广播 = 窗口语义**：`nested.derive(lambda g: {...})` 保留了所有行，每行带
  上组级值——熟悉 SQL 的读者可把它当作 `PARTITION BY` 窗口的对应物。
- **守恒校验是分摊的身份证**：组内 share 求和 == 组级 fee，一行代码的
  `group_by().agg()` 就能查，写进验收习惯。

## 延伸练习

1. 给 `shared` 加守恒校验：`group_by("year").agg(sum_share=..., fee=...)` 后
   `filter` 出两者不等的组（应为空）。
2. 把分摊规则改成"**按销售额与门店数的乘积**占比"（自造一个 stores 列），
   观察代码只动一个表达式。
3. 筛出"月销售额超过当年均值"的月份：先用 `nested.derive` 广播
   `g.mean("sales")`，再做表级 `filter`。`NestedTable.filter(lambda g: ...)`
   只能整组保留或丢弃，挑不出单个月份。
4. 尝试把 fees 表也换成 months 的聚合（如年销售额的 10% 作为费用），全流程
   就不需要任何 join 了——体会广播路线的极限形态。

---
**上一例**：[18 · 复合会话规则](18_session_composite.md) · **下一例**：[20 · 稀疏报表置空](20_sparse_report.md)
