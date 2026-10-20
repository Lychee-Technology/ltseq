<!-- Generated from ../../cn/15_payment_cycle.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 15 · 账期收口分组：group_ordered 的实战形态

> **难度** ★★★ · **领域** 金融 · **前置** 案例 03
>
> **涉及 API**：`sort` / `shift(1, partition_by=)` / `if_else` / `is_null` / `cum_sum(partition_by=)` / `s.concat` / `cast("string")` / `group_ordered` / `NestedTable.agg`

**学习目标**

- 把案例 03 的"连续段三件套"用到真实业务：**分组边界由业务事件定义**（开票收口）
- 学会"分组键不是现成列"时的标准动作：先 derive 出组号，再造复合键分组
- 对比 SQL 的绕行写法与 LTSeq 的直白写法，理解"有序分组"为什么值钱

## 问题描述

项目回款表 `bills(project, month, invoiced, amount)`：常规月只有当月金额、
无发票（`invoiced=0`）；收口月既有当月金额又有发票（`invoiced=1`）。规则：
**当上一个月 invoiced=1 时，开启一个新的付款周期**。目标：找出每个周期的
收口月，并算出该周期的总金额。

```
project  month     invoiced  amount
AAA      2023-01   0         10      ┐
AAA      2023-02   0         15      ├ 周期1 → 收口 2023-03，合计 40
AAA      2023-03   1         15      ┘
AAA      2023-04   0         10      ┐
AAA      2023-05   0         10      ├ 周期2 → 收口 2023-06，合计 30
AAA      2023-06   1         10      ┘
```

注意：分组依据与**顺序**有关——这不是等值分组（`group_by` 无能为力），而是
"事件驱动的连续分组"。

```python
from ltseq import LTSeq

bills = LTSeq.from_rows([
    {"project": "AAA", "month": "2023-01", "invoiced": 0, "amount": 10},
    {"project": "AAA", "month": "2023-02", "invoiced": 0, "amount": 15},
    {"project": "AAA", "month": "2023-03", "invoiced": 1, "amount": 15},
    {"project": "AAA", "month": "2023-04", "invoiced": 0, "amount": 10},
    {"project": "AAA", "month": "2023-05", "invoiced": 0, "amount": 10},
    {"project": "AAA", "month": "2023-06", "invoiced": 1, "amount": 10},
    {"project": "BBB", "month": "2022-05", "invoiced": 0, "amount": 40},
    {"project": "BBB", "month": "2022-06", "invoiced": 1, "amount": 20},
    {"project": "BBB", "month": "2022-07", "invoiced": 0, "amount": 30},
    {"project": "BBB", "month": "2022-08", "invoiced": 1, "amount": 30},
])

bills = bills.sort("project", "month")
bills.show();
```

> **预期输出**
>
> ```text
> +---------+---------+----------+--------+
> | project | month   | invoiced | amount |
> +---------+---------+----------+--------+
> | AAA     | 2023-01 | 0        | 10     |
> | AAA     | 2023-02 | 0        | 15     |
> | AAA     | 2023-03 | 1        | 15     |
> | AAA     | 2023-04 | 0        | 10     |
> | AAA     | 2023-05 | 0        | 10     |
> | AAA     | 2023-06 | 1        | 10     |
> | BBB     | 2022-05 | 0        | 40     |
> | BBB     | 2022-06 | 1        | 20     |
> | BBB     | 2022-07 | 0        | 30     |
> | BBB     | 2022-08 | 1        | 30     |
> +---------+---------+----------+--------+
> ```

预判答案：AAA 两期收口 2023-03 / 2023-06，合计 40 / 30；BBB 两期
收口 2022-06 / 2022-08，合计 60 / 60。跑完验证。

## 第一步 · 标记周期起点：组内 shift + 三值逻辑

"新周期"出现在两种行上：每个项目的**第一行**（prev 为 NULL），以及
**上一行 invoiced=1** 的行。`shift(1, partition_by="project")` 在组边界返回
NULL，一个表达式同时覆盖两种情况。

```python
from ltseq import if_else

marked = bills.derive(
    prev_invoiced=lambda r: r.invoiced.shift(1, partition_by="project"),
).derive(
    new_cycle=lambda r: if_else(
        r.prev_invoiced.is_null() | (r.prev_invoiced == 1), 1, 0
    ),
)

marked.show();
```

> **预期输出**
>
> ```text
> +---------+---------+----------+--------+---------------+-----------+
> | project | month   | invoiced | amount | prev_invoiced | new_cycle |
> +---------+---------+----------+--------+---------------+-----------+
> | AAA     | 2023-01 | 0        | 10     | None          | 1         |
> | AAA     | 2023-02 | 0        | 15     | 0             | 0         |
> | AAA     | 2023-03 | 1        | 15     | 0             | 0         |
> | AAA     | 2023-04 | 0        | 10     | 1             | 1         |
> | AAA     | 2023-05 | 0        | 10     | 0             | 0         |
> | AAA     | 2023-06 | 1        | 10     | 0             | 0         |
> | BBB     | 2022-05 | 0        | 40     | None          | 1         |
> | BBB     | 2022-06 | 1        | 20     | 0             | 0         |
> | BBB     | 2022-07 | 0        | 30     | 1             | 1         |
> | BBB     | 2022-08 | 1        | 30     | 0             | 0         |
> +---------+---------+----------+--------+---------------+-----------+
> ```

**解析**

- 与案例 03/10 相同的骨架：**间隙/事件标记 → 累加编号 → 有序分组**。变化的
  只是标记条件——这正是"模式"的含义。
- `r.prev_invoiced.is_null() | (r.prev_invoiced == 1)`：布尔表达式用 `|` 组合，
  不能用 Python 的 `or`（表达式要先构建成计划，不是立即求值）。
- 直接标记每个周期的"起点"，代码就贴着规则本身："上一个月开了票，就开启
  新周期"。

## 第二步 · 编号 + 复合键：把连续段变成可分组的东西

`new_cycle` 是 0/1 标记，组内 `cum_sum` 之后就成了**周期号**（1, 1, 1, 2, 2,
2 …）。分组需要 (project, cycle_no) 两个键（`cycle_no` 在每个项目里都从头
编号），而 `group_ordered` 只接受一个键表达式——拼一个复合键字符串即可。

```python
numbered = marked.derive(
    cycle_no=lambda r: r.new_cycle.cum_sum(partition_by="project"),
).derive(
    cycle_key=lambda r: r.project.s.concat("#", r.cycle_no.cast("string")),
)

numbered.show();
```

> **预期输出**
>
> ```text
> +---------+---------+----------+--------+---------------+-----------+----------+-----------+
> | project | month   | invoiced | amount | prev_invoiced | new_cycle | cycle_no | cycle_key |
> +---------+---------+----------+--------+---------------+-----------+----------+-----------+
> | AAA     | 2023-01 | 0        | 10     | None          | 1         | 1        | AAA#1     |
> | AAA     | 2023-02 | 0        | 15     | 0             | 0         | 1        | AAA#1     |
> | AAA     | 2023-03 | 1        | 15     | 0             | 0         | 1        | AAA#1     |
> | AAA     | 2023-04 | 0        | 10     | 1             | 1         | 2        | AAA#2     |
> | AAA     | 2023-05 | 0        | 10     | 0             | 0         | 2        | AAA#2     |
> | AAA     | 2023-06 | 1        | 10     | 0             | 0         | 2        | AAA#2     |
> | BBB     | 2022-05 | 0        | 40     | None          | 1         | 1        | BBB#1     |
> | BBB     | 2022-06 | 1        | 20     | 0             | 0         | 1        | BBB#1     |
> | BBB     | 2022-07 | 0        | 30     | 1             | 1         | 2        | BBB#2     |
> | BBB     | 2022-08 | 1        | 30     | 0             | 0         | 2        | BBB#2     |
> +---------+---------+----------+--------+---------------+-----------+----------+-----------+
> ```

**解析**

- `r.new_cycle.cum_sum(partition_by="project")` 是**表达式形式**的累计求和：
  输出列名由你控制，且支持 `partition_by`。表级 `t.cum_sum("new_cycle")` 加的
  `*_cumsum` 列是全表累计，跨项目会接着往下编（BBB 的周期会是 3 和 4）；表达式
  形式在每个项目里重新计数。
- 复合键 `project#cycle_no` 是 LTSeq 里"多键分组"的标准变通（分组键只能是
  一个表达式，传列表或元组会抛 `TypeError`）：拼字符串、分组、需要时再拆回。
  三行代码换来键的灵活性，值得。
- 排序键是 `(project, month)`，`cum_sum` 逐行推进的方向与它一致——这是有序
  累计的前提。

## 第三步 · 分组收口：收口月 = 组末行，总额 = 组内求和

周期以内 `invoiced=1` 的行**结束**，所以收口月就是组内**最后一行**的 month，
总额是组内全部 amount 之和。`group_ordered` 把 `cycle_key` 相同的连续行归成
一组，组内保持按月的顺序，`agg` 再把每组收成一行。

```python
summary = (
    numbered
    .group_ordered(lambda r: r.cycle_key)
    .agg(lambda g: {
        "project": g.first().project,
        "close_month": g.last().month,
        "total": g.sum("amount"),
    })
)

summary.show();
```

> **预期输出**
>
> ```text
> +---------+-------------+-------+
> | project | close_month | total |
> +---------+-------------+-------+
> | AAA     | 2023-03     | 40    |
> | AAA     | 2023-06     | 30    |
> | BBB     | 2022-06     | 60    |
> | BBB     | 2022-08     | 60    |
> +---------+-------------+-------+
> ```

与预判答案一致：AAA 40/30，BBB 60/60。✔

**解析**

- `g.first()` / `g.last()` 返回行代理，属性访问组内首末行的任意列——收口月
  就是 `g.last().month`，不需要再 filter `invoiced==1`（这里每个周期的末行都是
  收口月；如果最后一个周期还没开票，取到的是它最近的月份）。
- `group_ordered(...).agg()` 按序列顺序每个周期输出一行，末尾不需要再 `sort`。
  `group_by().agg()` 也能分组，但它是哈希分组，输出顺序没有定义；它的聚合作用
  在列上，没有首行末行可取（`g.first()` 会抛 `ColumnNotFoundError`）。
- SQL 常见的写法是用
  `sum(invoiced) OVER (PARTITION BY project ORDER BY month DESC)` 给周期编号：
  倒着累计发票数，同一周期的行就拿到同一个编号，因为发票行是周期的最后一行。这样能算对，但读者得把时间线倒过来才能验证。LTSeq 的
  标记-编号-分组三步顺着业务读下来，每一步都可以单独 show() 检查。

## 关键点解析

- **三件套的第三次复用**：标记（shift + is_null）→ 编号（cum_sum）→ 分组
  （按复合键 `group_ordered`）。03 用于多头段、10 用于会话、15 用于账期——同一
  骨架换标记条件就是换业务。
- **多键分组的变通**：LTSeq 的 `group_by` 和 `group_ordered` 键都是单列/单
  表达式，复合键拼字符串是常用做法（或先 derive 出复合列再 `group_sorted`，
  前提是声明排序的首键恰为它）。
- **表达式 cum_sum ≠ 表级 cum_sum**：需要自定义列名、需要 partition_by、需要
  嵌进更大的表达式时，一律用 `r.col.cum_sum(partition_by=...)` 形式。

## 延伸练习

1. 用 `fold` 重解本题：状态为"本周期已累计金额"，`invoiced==1` 时输出并清零。
   对比两种写法的可读性（fold 是逐行 Python 回调，注意案例 06 提到的性能
   语义）。
2. 给 summary 增加 `months`（周期长度）与 `avg_monthly`（月均金额）——注意
   agg 内不支持聚合算术，先 `g.count()` 出列再表级 derive 相除（相除前先转型，
   见案例 14）。
3. 若数据未按月连续（中间跳月），`shift` 标记还正确吗？构造一个反例，想想
   "月连续性"要不要参与分组定义。
4. 把规则改成"开票后**两个月**才收口"，标记条件该如何改写？

---
**上一例**：[14 · 同比与环比](14_yoy_mom.md) · **下一例**：[16 · 最长连涨区间](16_longest_rising_span.md)
