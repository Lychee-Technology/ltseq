<!-- Generated from ../../cn/09_rfm_segments.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 09 · RFM 用户分层：聚合 + 排名 + 分箱

> **难度** ★★★ · **领域** 电商用户行为 · **前置** 案例 02、08
>
> **涉及 API**：`group_by().agg()` / `dt.diff` / `cast` / `ntile().over()` / `when` / 常量列 `derive`（`LiteralExpr`）

**学习目标**

- 一次聚合算出 R / F / M 三个原始指标
- 用 `ntile().over()` 做四分位打分（窗口自带排序，**不需要**先 sort 全表）
- 用 `when` 链输出经典分层标签

## 问题描述

订单表 `orders(user, order_id, date, amount)`。以 `2026-10-01` 为观察点：

- **R**（Recency）：最近一次购买距今天数，越小越好；
- **F**（Frequency）：购买次数；
- **M**（Monetary）：累计消费金额。

R / F / M 各按四分位打 1~4 分，再按经典规则输出分层：
重要价值 / 重要发展 / 重要保持 / 重要挽留 / 一般用户。

```python
from ltseq import LTSeq

orders = (
    LTSeq.from_rows([
        {"user": "u1", "order_id": "o1",  "date": "2026-09-28", "amount": 1200.0},
        {"user": "u1", "order_id": "o2",  "date": "2026-09-30", "amount": 380.0},
        {"user": "u2", "order_id": "o3",  "date": "2026-06-15", "amount": 90.0},
        {"user": "u3", "order_id": "o4",  "date": "2026-09-25", "amount": 260.0},
        {"user": "u3", "order_id": "o5",  "date": "2026-09-29", "amount": 640.0},
        {"user": "u3", "order_id": "o6",  "date": "2026-09-30", "amount": 150.0},
        {"user": "u4", "order_id": "o7",  "date": "2026-03-02", "amount": 45.0},
        {"user": "u5", "order_id": "o8",  "date": "2026-09-20", "amount": 800.0},
        {"user": "u6", "order_id": "o9",  "date": "2026-08-01", "amount": 300.0},
        {"user": "u7", "order_id": "o10", "date": "2026-09-30", "amount": 120.0},
    ])
    .sort("user", "date")
)
print(len(orders), "笔订单 /", orders.distinct("user").count(), "个用户")
```

> **预期输出**
>
> ```text
> 10 笔订单 / 7 个用户
> ```

## 第一步 · 聚合出 RFM 原始值

一次 `group_by().agg()` 同时算三个指标；ISO 日期字符串取 `max` 即最近购买日
（字典序 = 时间序）。

```python
rfm = orders.group_by("user").agg(
    last_date=lambda g: g.date.max(),          # 最近购买日（ISO 字符串 max）
    freq=lambda g: g.order_id.count(),         # 购买次数 F
    monetary=lambda g: g.amount.sum(),         # 累计金额 M
).sort("user")                                 # 聚合结果没有确定的行序
rfm.show();
```

> **预期输出**
>
> ```text
> +------+------------+------+----------+
> | user | last_date  | freq | monetary |
> +------+------------+------+----------+
> | u1   | 2026-09-30 | 2    | 1580     |
> | u2   | 2026-06-15 | 1    | 90       |
> | u3   | 2026-09-30 | 3    | 1050     |
> | u4   | 2026-03-02 | 1    | 45       |
> | u5   | 2026-09-20 | 1    | 800      |
> | u6   | 2026-08-01 | 1    | 300      |
> | u7   | 2026-09-30 | 1    | 120      |
> +------+------------+------+----------+
> ```

## 第二步 · 算 Recency

观察日作为**常量列**先 derive，再用 `dt.diff` 求日期差。日期列参与日期运算前先
`cast("date32")`（本例的日期是字符串；`read_csv` 读入的 ISO 日期已经是 `date32`）。

```python
from datetime import date
from ltseq.expr import LiteralExpr

rfm = (
    rfm
    .derive(snap=lambda r: LiteralExpr(date(2026, 10, 1)))   # 常量列：观察点
    .derive(
        recency=lambda r: r.snap.dt.diff(r.last_date.cast("date32")),
    )
)
rfm.select("user", "last_date", "recency", "freq", "monetary").show();
```

> **预期输出**
>
> ```text
> +------+------------+---------+------+----------+
> | user | last_date  | recency | freq | monetary |
> +------+------------+---------+------+----------+
> | u1   | 2026-09-30 | 1       | 2    | 1580     |
> | u2   | 2026-06-15 | 108     | 1    | 90       |
> | u3   | 2026-09-30 | 1       | 3    | 1050     |
> | u4   | 2026-03-02 | 213     | 1    | 45       |
> | u5   | 2026-09-20 | 11      | 1    | 800      |
> | u6   | 2026-08-01 | 61      | 1    | 300      |
> | u7   | 2026-09-30 | 1       | 1    | 120      |
> +------+------------+---------+------+----------+
> ```

**解析**

- `derive(snap=lambda r: LiteralExpr(date(2026, 10, 1)))`：`derive` 的 lambda 必须
  返回表达式，直接返回 `date` 会报 `TypeError`；`LiteralExpr`（来自 `ltseq.expr`）
  把字面量包成常量列，观察点参数化就改这一处。
- `dt.diff(other)` = `self - other`，`unit` 默认 `"day"`，日期对日期返回整天数
  （float）；也支持 `month / year / hour / minute / second`。
- `cast()` 支持的类型族：`int32/int64/float32/float64/utf8/string/bool/date32`。

## 第三步 · 四分位打分：ntile

`ntile(4).over(order_by=...)` 把行分成大致相等的 4 桶（1~4）。**排名窗口自带排序
声明，全表不需要先 sort**。分数方向统一成"越大越好"，所以用 `5 - 桶号` 翻转。

```python
from ltseq import ntile

# ntile 按行位置切桶，所以每个排序键都用消费额打破并列（见解析）
rfm = rfm.derive(lambda r: {
    "r_score": 5 - ntile(4).over(order_by=r.recency * 1e6 - r.monetary),                # 越近越好
    "f_score": 5 - ntile(4).over(order_by=r.freq * 1e6 + r.monetary, descending=True),  # 越多越好
    "m_score": 5 - ntile(4).over(order_by=r.monetary, descending=True),                 # 越高越好
}).derive(lambda r: {     # `5 - ntile(...)` 的结果是 decimal，转回整数
    "r_score": r.r_score.cast("int64"),
    "f_score": r.f_score.cast("int64"),
    "m_score": r.m_score.cast("int64"),
})
rfm.select("user", "recency", "freq", "monetary", "r_score", "f_score", "m_score").sort("user").show();
```

> **预期输出**
>
> ```text
> +------+---------+------+----------+---------+---------+---------+
> | user | recency | freq | monetary | r_score | f_score | m_score |
> +------+---------+------+----------+---------+---------+---------+
> | u1   | 1       | 2    | 1580     | 4       | 4       | 4       |
> | u2   | 108     | 1    | 90       | 2       | 2       | 2       |
> | u3   | 1       | 3    | 1050     | 4       | 4       | 4       |
> | u4   | 213     | 1    | 45       | 1       | 1       | 1       |
> | u5   | 11      | 1    | 800      | 3       | 3       | 3       |
> | u6   | 61      | 1    | 300      | 2       | 3       | 3       |
> | u7   | 1       | 1    | 120      | 3       | 2       | 2       |
> +------+---------+------+----------+---------+---------+---------+
> ```

**解析**

- `ntile` 的桶号沿 `order_by` **升序**分配（`descending=True` 反向）：桶 1 总是
  排在最前的 1/4 行。所以"越小越好"的 `recency` 升序即得高分方向；`freq` /
  `monetary` 要 `descending=True`。
- `5 - 桶号` 把 1~4 映射成 4~1：桶 1（最好的一档）→ 4 分。
- `ntile` 按行的**位置**切桶，而不是按值：键相同的行按引擎遇到的顺序落桶，
  每次运行都可能不同。`.over()` 只接受一个 `order_by` 表达式，所以排序键里折进
  消费额作决胜键：`r.recency * 1e6 - r.monetary` 先按 recency，再让消费高的
  排前（本例消费额远小于 1e6）。
- `5 - ntile(...)` 的结果是 `decimal`（整数字面量减 `ntile` 的无符号整数），而
  `show()` 目前打印不了 decimal，所以第二个 `derive` 把分数转成 `int64`。
- 7 行分 4 桶：桶大小 2/2/2/1——`ntile` 保证"大致相等"，除不尽时前面的桶多一行。
- 排名函数族：`row_number`（唯一序号）/ `rank`（有并列跳号）/ `dense_rank`
  （并列不跳号）/ `ntile`（分桶），全部 `.over(partition_by=, order_by=)`。

## 第四步 · 分层标签：when 链

经典规则（R/F/M 视角）：

- R 高 F 高 M 高 → **重要价值**；R 高 F 低 → **重要发展**（新客潜力）；
  R 低 F 高 M 高 → **重要保持**（老客回流）；R 低 F 低 M 高 → **重要挽留**；
- 其余 → 一般用户。

```python
from ltseq import when

rfm = rfm.derive(
    segment=lambda r: when(
        (r.r_score >= 3) & (r.f_score >= 3) & (r.m_score >= 3), "重要价值客户",
    )
    .when((r.r_score >= 3) & (r.f_score <= 2), "重要发展客户")
    .when((r.r_score <= 2) & (r.f_score >= 3) & (r.m_score >= 3), "重要保持客户")
    .when((r.r_score <= 2) & (r.m_score >= 3), "重要挽留客户")
    .otherwise("一般用户"),
).select(
    "user", "recency", "freq", "monetary",
    "r_score", "f_score", "m_score", "segment",
).sort("user")

rfm.show();
```

> **预期输出**
>
> ```text
> +------+---------+------+----------+---------+---------+---------+--------------------+
> | user | recency | freq | monetary | r_score | f_score | m_score | segment            |
> +------+---------+------+----------+---------+---------+---------+--------------------+
> | u1   | 1       | 2    | 1580     | 4       | 4       | 4       | 重要价值客户             |
> | u2   | 108     | 1    | 90       | 2       | 2       | 2       | 一般用户               |
> | u3   | 1       | 3    | 1050     | 4       | 4       | 4       | 重要价值客户             |
> | u4   | 213     | 1    | 45       | 1       | 1       | 1       | 一般用户               |
> | u5   | 11      | 1    | 800      | 3       | 3       | 3       | 重要价值客户             |
> | u6   | 61      | 1    | 300      | 2       | 3       | 3       | 重要保持客户             |
> | u7   | 1       | 1    | 120      | 3       | 2       | 2       | 重要发展客户             |
> +------+---------+------+----------+---------+---------+---------+--------------------+
> ```

（u5、u6 都只下过一单，F 却是 3 分：`ntile` 按位置切桶，5 个一单用户按消费额排开后落进了三个 F 桶。数据这么小、并列这么多时，四分位分数反映的更多是决胜键，而不是客户本身。）

**解析**

- `when(cond, val).when(...).otherwise(val)` 按**顺序**短路，先写的分支优先——
  规则叠加时把最严格的放前面。
- 布尔组合用 `&` / `|` / `~`（SQL 三值逻辑），**不要**用 Python 的 `and / or / not`。

## 关键点解析

1. **三类算子接力**：聚合（`group_by().agg`）→ 排名（`ntile().over`）→ 条件
   （`when` 链）——RFM 是检验三类算子协同的最小完整闭环。
2. **`.over()` 自带排序声明**：排名窗口不需要全表预 sort；需要 `partition_by` 时
   同样支持（如分渠道各打各的分）。
3. **常量列模式**：观察点/阈值等业务参数先用 `derive` 做成列，参数化只改一处。
4. **字符串日期够用但要有边界意识**：排序/`max` 没问题；一旦做**日期运算**就
   `cast("date32")`，或直接用 Parquet 原生日期类型。

## 延伸练习

- 十分位版：`ntile(10)` 打 1~10 分，分层规则改用 R/F/M 三分之和。
- 加一列 `channel`，用 `.over(partition_by=r.channel, ...)`：分渠道各自四分位。
- 把"观察点"从常量改成 `today()`（`from ltseq import today`）自动对齐运行日（UTC 日期）。

---
**上一例**：[08 · 用户留存](08_user_retention.md) · **下一例**：[10 · 会话切分](10_sessionization.md)
