<!-- Generated from ../../cn/13_topn_within_group.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 13 · 组内 TopN：取前 N 的三种写法

> **难度** ★★☆ · **领域** 金融 · **前置** 案例 01、02
>
> **涉及 API**：`sort` + `slice` / `rank().over(partition_by=, order_by=, descending=)` / `filter` / `drop` / `group_by().agg()` + `top_k`

**学习目标**

- 掌握"取前 N"的三种姿势：排序切片、排名窗口、TopN 聚合，并知道各自适用的场景
- 理解 `rank().over()` 为什么**不需要先 `sort()`**——排序意图由 `.over(order_by=)` 自带
- 看清哪种写法省掉了全表排序：`sort + slice` 会执行成只保留 k 行的 **TopK**，
  组内的两种写法仍要排序

## 问题描述

逐笔成交表 `trades(symbol, amount)`，三个证券共 11 笔。目标：

1. **全局**成交额 Top 3（作对照）；
2. **每个证券**成交额前 3 名的**明细行**（并列时同名次都保留）；
3. 每个证券成交额最大的三笔**金额**（只要数值，不要整行）。

```python
from ltseq import LTSeq

trades = LTSeq.from_rows([
    {"symbol": "T1", "amount": 310.0},
    {"symbol": "T1", "amount": 120.0},
    {"symbol": "T1", "amount": 240.0},
    {"symbol": "T1", "amount": 90.0},
    {"symbol": "T2", "amount": 480.0},
    {"symbol": "T2", "amount": 55.0},
    {"symbol": "T2", "amount": 365.0},
    {"symbol": "T2", "amount": 200.0},
    {"symbol": "T2", "amount": 150.0},
    {"symbol": "T3", "amount": 75.0},
    {"symbol": "T3", "amount": 95.0},
])

# show() 默认只打印 10 行
trades.show(11);
```

> **预期输出**
>
> ```text
> +--------+--------+
> | symbol | amount |
> +--------+--------+
> | T1     | 310    |
> | T1     | 120    |
> | T1     | 240    |
> | T1     | 90     |
> | T2     | 480    |
> | T2     | 55     |
> | T2     | 365    |
> | T2     | 200    |
> | T2     | 150    |
> | T3     | 75     |
> | T3     | 95     |
> +--------+--------+
> ```

## 第一步 · 全局 TopN：排序 + 切片

SQL 的 `ORDER BY amount DESC LIMIT 3` 字面意思是"全表排序后取前三"。LTSeq 里
对应 `sort(desc=True)` + `slice(offset, length)`——这是最直白的写法。

```python
global_top3 = trades.sort("amount", desc=True).slice(0, 3)

global_top3.show();
```

> **预期输出**
>
> ```text
> +--------+--------+
> | symbol | amount |
> +--------+--------+
> | T2     | 480    |
> | T2     | 365    |
> | T1     | 310    |
> +--------+--------+
> ```

**解析**

- `sort` 后接 `slice(0, 3)` 等价于 LIMIT 3。注意 `slice` 的参数是
  `slice(offset, length)`，不是起止下标。
- 这里并不会对全表排序：引擎把 `sort` 和 `slice` 合成一个 **TopK** 算子，扫描时
  只保留最大的 3 行（`explain_plan()` 里能看到 `SortExec: TopK(fetch=3)`）。
  真正的麻烦在**组内** TopN——每种证券都要排一遍，SQL 得靠窗口函数绕一层
  子查询。这正是第二步要解决的。

## 第二步 · 组内 TopN：排名窗口 + filter

`rank().over(...)` 自带排序语义，**不要求先 `sort()`**。`partition_by=r.symbol`
让每个证券内部独立排名，`descending=True` 按成交额从大到小。

```python
from ltseq import rank

ranked = trades.derive(
    rk=lambda r: rank().over(
        partition_by=r.symbol, order_by=r.amount, descending=True
    ),
)
top3 = (
    ranked
    .filter(lambda r: r.rk <= 3)
    .drop("rk")
    .sort("symbol", "amount", desc=[False, True])
)

top3.show();
```

> **预期输出**
>
> ```text
> +--------+--------+
> | symbol | amount |
> +--------+--------+
> | T1     | 310    |
> | T1     | 240    |
> | T1     | 120    |
> | T2     | 480    |
> | T2     | 365    |
> | T2     | 200    |
> | T3     | 95     |
> | T3     | 75     |
> +--------+--------+
> ```

**解析**

- T3 只有两笔，`rk <= 3` 全部保留——**N 大于组大小时取全组**，无需特判。
- `rank()` 对并列值给同名次、后续跳号（1, 2, 2, 4）；若希望并列时随机只取
  一行，换 `row_number()`（并列表意不明，慎用）。想"并列不跳号"用
  `dense_rank()`。
- 这一步要排序：执行计划先把全部行按 `(symbol, amount)` 排好，窗口再排名。
  排名窗口省掉的是子查询，不是排序。
- 最后的 `sort` 连组内金额一起排：只按 `symbol` 排的话，同一证券内各行的
  顺序没有定义。

## 第三步 · 只要数值不要行：top_k 聚合

很多报表只要"每证券最大的三笔金额"，不需要整行。LTSeq 在聚合上下文提供
`g.col.top_k(k)`——返回前 k 大值拼接的字符串。

```python
by_symbol = (
    trades
    .group_by("symbol")
    .agg(top3_amounts=lambda g: g.amount.top_k(3))
    .sort("symbol")
)

by_symbol.show();
```

> **预期输出**
>
> ```text
> +--------+--------------+
> | symbol | top3_amounts |
> +--------+--------------+
> | T1     | 310;240;120  |
> | T2     | 480;365;200  |
> | T3     | 95;75        |
> +--------+--------------+
> ```

**解析**

- `top_k(k)` 是**聚合**：每组一行、不展开行，按从大到小输出为 `;` 分隔字符串。
  但它不是只保留 k 个值的堆：每组的值先按降序收集起来再取前 k 个，内存随组
  的大小增长，而不是随 k 增长。
- 需要整行明细（还要带时间戳等其他列）→ 用第二步的排名窗口；只要数值汇总
  → `top_k` 每组一行直接给出。
- `top_k` 的 `k` 必须是整数字面量（默认 10）；它取的是**最大**的 k 个。要组内
  最小的几个，用第二步的排名窗口按升序排（去掉 `descending=True`）。

## 关键点解析

- **三种姿势一张表**：全局取行 → `sort+slice`；组内取行 → `rank().over(partition_by=) + filter`；
  组内取值 → `group_by().agg(top_k)`。写之前先问自己：要不要行？要不要分组？
- **排名函数免排序声明**：`rank/row_number/dense_rank/ntile` 走 `.over(order_by=)`
  声明排序，与 `shift/rolling` 这类"先 `sort()` 再用"的序列窗口是两套规则——
  前者排序意图内建，后者消费表的声明顺序。
- **各写法的代价**：只有全局 `sort + slice` 会执行成有界的 TopK；排名窗口和
  `top_k` 都要先排序。`explain_plan()` 能看出实际走的是哪种计划。

## 延伸练习

1. 把 `rank()` 换成 `row_number()`，观察并列成交额时的行为差异（可给 T1 加一笔
   240.0 重复值）。
2. 用 `ntile(4).over(partition_by=r.symbol, order_by=r.amount)` 给每个证券内部
   划四分位，筛出最贵的一档。
3. 反向需求：每个证券**最便宜**的两笔。提示：组内升序的
   `row_number().over(partition_by=r.symbol, order_by=r.amount)`。
4. 对三种写法分别打印 `explain_plan()`（返回逻辑计划和物理计划），找出其中的
   `SortExec`：哪一个是 `TopK`？另外两个各按什么排序？

---
**上一例**：[12 · 超大文件流式处理](12_streaming_cursor.md) · **下一例**：[14 · 同比与环比](14_yoy_mom.md)
