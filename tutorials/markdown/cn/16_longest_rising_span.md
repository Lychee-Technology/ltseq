<!-- Generated from ../../cn/16_longest_rising_span.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 16 · 最长连涨区间：起止日期与并列冠军

> **难度** ★★☆ · **领域** 金融 · **前置** 案例 03
>
> **涉及 API**：`sort` / `shift(1)` / `fill_null` / `group_ordered` / `NestedTable.agg` / `filter` / `rank().over(order_by=, descending=)` / `drop`

**学习目标**

- 从"最长连涨**天数**"进阶到"最长连涨**区间**"——业务要的往往是起止日期
- 用 `NestedTable.agg` 把每一段折叠成 (start, end, days) 摘要行
- 处理并列最长：`rank()` 同名次不丢段，`sort+slice` 会丢——选对工具

## 问题描述

日线收盘价表 `px(date, close)`，10 个交易日。目标：

1. 找出所有"连涨段"（连续收盘价上升的区间）；
2. 求最长连涨段的**起止日期**与天数；
3. 若多段并列最长，**全部保留**。

```python
from ltseq import LTSeq

px = LTSeq.from_rows([
    {"date": "d01", "close": 10.0},
    {"date": "d02", "close": 10.5},
    {"date": "d03", "close": 11.0},
    {"date": "d04", "close": 10.8},
    {"date": "d05", "close": 11.5},
    {"date": "d06", "close": 12.0},
    {"date": "d07", "close": 11.9},
    {"date": "d08", "close": 12.5},
    {"date": "d09", "close": 12.4},
    {"date": "d10", "close": 13.0},
]).sort("date")

px.show();
```

> **预期输出**
>
> ```text
> +------+-------+
> | date | close |
> +------+-------+
> | d01  | 10    |
> | d02  | 10.5  |
> | d03  | 11    |
> | d04  | 10.8  |
> | d05  | 11.5  |
> | d06  | 12    |
> | d07  | 11.9  |
> | d08  | 12.5  |
> | d09  | 12.4  |
> | d10  | 13    |
> +------+-------+
> ```

预判：上涨段为 d02-d03、d05-d06、d08、d10——**两段并列最长（各 2 天）**，
第二步要能同时保住它们。

## 第一步 · 涨跌标记与连续分组

案例 03 先用翻转标记和 `cum_sum` 给段编号；这里可以直接拿 `is_up` 当
`group_ordered` 的键，因为 `group_ordered` 本身就把连续相同的值归成一组。
首行没有前值，比较结果是 NULL——`fill_null(False)` 把它显式落成"未上涨"，
避免 NULL 单独成段。

```python
flagged = px.derive(
    is_up=lambda r: (r.close > r.close.shift(1)).fill_null(False),
)

runs = flagged.group_ordered(lambda r: r.is_up)

spans = runs.agg(lambda g: {
    "rising": g.first().is_up,
    "start": g.first().date,
    "end": g.last().date,
    "days": g.count(),
})

spans.show();
```

> **预期输出**
>
> ```text
> +--------+-------+-----+------+
> | rising | start | end | days |
> +--------+-------+-----+------+
> | false  | d01   | d01 | 1    |
> | true   | d02   | d03 | 2    |
> | false  | d04   | d04 | 1    |
> | true   | d05   | d06 | 2    |
> | false  | d07   | d07 | 1    |
> | true   | d08   | d08 | 1    |
> | false  | d09   | d09 | 1    |
> | true   | d10   | d10 | 1    |
> +--------+-------+-----+------+
> ```

**解析**

- `(r.close > r.close.shift(1))` 首行为 NULL；`fill_null(False)` 兜底成 False。
  若不兜底，NULL 会自成一个键组——结果碰巧也对（NULL 组不是 True 组），但
  显式落值让意图可读。
- `group_ordered` 按声明顺序把**连续相同** `is_up` 的行收进同一段；`agg` 把每
  段折叠成一行摘要：起点 `g.first().date`、终点 `g.last().date`、长度
  `g.count()`——三个聚合都是文档允许的"纯组聚合"。
- 段表 `spans` 已经是普通 LTSeq，可以继续 filter / sort / derive——"先降维成
  段表，再做表级分析"是有序分组的收尾惯例。

## 第二步 · 并列最长的区间：rank 而不是 slice

在段表里筛出上涨段，再对天数排名。`rank()` 对并列给同名次，`rk == 1` 就能
**同时留下两段**。

```python
from ltseq import rank

longest = (
    spans
    .filter(lambda r: r.rising)
    .derive(rk=lambda r: rank().over(order_by=r.days, descending=True))
    .filter(lambda r: r.rk == 1)
    .drop("rk")
    .sort("start")
)

longest.show();
```

> **预期输出**
>
> ```text
> +--------+-------+-----+------+
> | rising | start | end | days |
> +--------+-------+-----+------+
> | true   | d02   | d03 | 2    |
> | true   | d05   | d06 | 2    |
> +--------+-------+-----+------+
> ```

**解析**

- 两段并列 2 天，`rank()` 给双 1——`filter(rk == 1)` 全部保留。若用
  `sort("days", desc=True).slice(0, 1)` 会**悄悄丢掉一段**：排名语义与切片
  语义在并列场景分道扬镳，业务题"求最长"默认要并列全保。
- `rank().over(order_by=r.days, descending=True)` 不需要先 `sort()`（排名函数
  自带排序，见案例 13）。
- 末尾的 `sort("start")` 让并列的两段按日期排列：窗口会保留表声明的顺序
  （issue #202），但 `spans` 是 `agg` 的结果，没有声明顺序（`sort_keys` 为
  `None`），排名后的行没有定义好的先后。
- 想要"前两名"而非"最长（并列全保）"？用 `dense_rank()` 再
  `filter(rk <= 2)`——先想清楚业务口径，再选函数。

## 关键点解析

- **天数 → 区间**：段表回答的是"从哪天到哪天"，而不只是"涨了几天"。
  `NestedTable.agg` 里的 `g.first()/g.last()` 就是起止日期的来源（案例 03 的
  多头段也是这样取的）——段表一旦摘要成 (start, end, days)，后续全是普通表级
  操作。
- **fill_null 显式落值**：窗口边界 NULL 早晚要处理，与其依赖"NULL 碰巧不
  等于 True"，不如一行 `fill_null(False)` 写明意图。
- **并列的正确姿势**：`rank() + filter(==1)` 保并列；`slice` 只保一个。报表
  类需求里"并列全保"通常是默认口径。

## 延伸练习

1. 求最长**连跌**区间（提示：`rising == False` 已经很接近，但它也包含被
   `fill_null(False)` 标成"未上涨"的首日，有平盘时还会包含平盘日——怎么把它们
   排除？）。
2. 给 `spans` 加一列 `gain`（段内涨幅 = 末日收盘 / 首日收盘 - 1），找出**涨幅
   最大**的上涨段——最长和最强不是一回事。`agg` 里不能对组内值做算术（见案例
   03），先把 `g.first().close`、`g.last().close` 取成列，再在 `derive` 里相除。
3. 只报**最近一次**的最长连涨：对 `longest` 用
   `row_number().over(order_by=r.end, descending=True)` 编号并保留第 1 行，体会
   "最长"与"最近"两个维度的叠加。
4. 若收盘价有相等（平盘）日，`is_up` 会把"涨+平"切成两段——把条件改成
   `r.close >= r.close.shift(1)` 再跑一遍，讨论业务口径。

---
**上一例**：[15 · 账期收口分组](15_payment_cycle.md) · **下一例**：[17 · 榜单排名迁移](17_rank_migration.md)
