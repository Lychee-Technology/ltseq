<!-- Generated from ../../cn/14_yoy_mom.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 14 · 同比与环比：shift(12) 的有序引用

> **难度** ★★☆ · **领域** 金融 · **前置** 案例 01
>
> **涉及 API**：`sort` / `shift(n)` / `shift(n, partition_by=)` / `cast("float64")` / `when` / `slice`

**学习目标**

- 用 `shift(1)` 与 `shift(12)` 实现环比与同比——这是窗口函数最经典的业务用法
- 认识 LTSeq 的**整数除法陷阱**：在 `main` 上，`/` 遵循 SQL 语义，整数相除会
  截断，比率必须先 `cast("float64")`
- 理解 NULL 的语义价值：`shift(12)` 前 12 行的 NULL 恰好表达"没有上年同期"

## 问题描述

单店月度 GMV 表 `monthly(shop, month, gmv)`，2023-01 至 2024-12 共 24 个月。
目标：

1. 计算**环比**（与上月比）与**同比**（与上年同月比）增长率；
2. 给环比打趋势标签（跳升 / 增长 / 回落 / 下滑）；
3. 扩展到多店铺：每店铺内部各自做环比。

```python
from ltseq import LTSeq

gmv_2023 = [820, 750, 910, 880, 960, 1020, 990, 1050, 1100, 1080, 1150, 1600]
gmv_2024 = [900, 830, 1000, 950, 1050, 1120, 1080, 1160, 1210, 1190, 1270, 1760]

rows = [
    {"shop": "S1", "month": f"2023-{m:02d}", "gmv": v}
    for m, v in enumerate(gmv_2023, start=1)
] + [
    {"shop": "S1", "month": f"2024-{m:02d}", "gmv": v}
    for m, v in enumerate(gmv_2024, start=1)
]

monthly = LTSeq.from_rows(rows).sort("month")

monthly.head(3).show();
```

> **预期输出**
>
> ```text
> +------+---------+-----+
> | shop | month   | gmv |
> +------+---------+-----+
> | S1   | 2023-01 | 820 |
> | S1   | 2023-02 | 750 |
> | S1   | 2023-03 | 910 |
> +------+---------+-----+
> ```

YYYY-MM 格式的字符串按字典序排序与按时间排序等价——这是这个格式设计得
好的地方。若月份来自 CSV，排序前无需转日期类型；要用 `dt.*` 时再转成日期
（`r.month.s.concat("-01").cast("date")`）。本案例只做位移，不需要转换。

## 第一步 · 位移：shift(1) 与 shift(12)

窗口函数要求先有声明顺序——`sort("month")` 已经登记。`shift(1)` 取上一行，
`shift(12)` 取 12 行之前，正好是"上年同月"。

```python
shifted = monthly.derive(
    prev_month=lambda r: r.gmv.shift(1),
    prev_year=lambda r: r.gmv.shift(12),
)

# 2023-12 至 2024-02，跨过年界
shifted.slice(11, 3).show();
```

> **预期输出**
>
> ```text
> +------+---------+------+------------+-----------+
> | shop | month   | gmv  | prev_month | prev_year |
> +------+---------+------+------------+-----------+
> | S1   | 2023-12 | 1600 | 1150       | None      |
> | S1   | 2024-01 | 900  | 1600       | 820       |
> | S1   | 2024-02 | 830  | 900        | 750       |
> +------+---------+------+------------+-----------+
> ```

**解析**

- `slice(11, 3)` 取第 12–14 行（offset 从 0 起）：2023 年最后一个月和 2024 年
  前两个月。
- 2023-12 的 `prev_year` 是 NULL：它前面只有 11 行，`shift(12)` 越过表头返回
  NULL，2023 年每个月都是如此。这个 NULL **不是缺陷而是信息**——"没有上年同期"
  就该是 NULL。从 2024-01 起，`shift(12)` 正好落在上年同月（2024-01 取到 820）。
- `shift(1)` / `shift(12)` 可以放在同一个 `derive` 里：两条新列互不引用，
  各自独立计算。

## 第二步 · 比率与趋势标签

增长率 = (本期 - 基期) / 基期。这里有一个 LTSeq 的必知陷阱，先看代码再说
为什么。

```python
from ltseq import when

rates = shifted.derive(
    mom=lambda r: (r.gmv.cast("float64") - r.prev_month) / r.prev_month,
    yoy=lambda r: (r.gmv.cast("float64") - r.prev_year) / r.prev_year,
).derive(
    mom_trend=lambda r: when(r.mom.is_null(), "无基期")
    .when(r.mom > 0.05, "跳升")
    .when(r.mom > 0, "增长")
    .when(r.mom > -0.05, "回落")
    .otherwise("下滑"),
)

rates.slice(11, 3).show();
```

> **预期输出**
>
> ```text
> +------+---------+------+------------+-----------+----------------------+---------------------+-----------+
> | shop | month   | gmv  | prev_month | prev_year | mom                  | yoy                 | mom_trend |
> +------+---------+------+------------+-----------+----------------------+---------------------+-----------+
> | S1   | 2023-12 | 1600 | 1150       | None      | 0.391304347826087    | None                | 跳升        |
> | S1   | 2024-01 | 900  | 1600       | 820       | -0.4375              | 0.0975609756097561  | 下滑        |
> | S1   | 2024-02 | 830  | 900        | 750       | -0.07777777777777778 | 0.10666666666666667 | 下滑        |
> +------+---------+------+------------+-----------+----------------------+---------------------+-----------+
> ```

**解析**

- **整数除法陷阱**：`gmv` 是 Int64 列，在 `main` 上 LTSeq 的 `/` 遵循 SQL 语义
  ——整数相除**截断取整**（issue #218），`(830 - 900) / 900` 不做 cast 会得到
  `0` 而不是 `-0.0778`！
  所以分子先 `cast("float64")`，浮点 - 整数 → 浮点，比率才正确。
- **NULL 的传播**：`prev_year` 为 NULL 时，`(x - NULL) / NULL` 结果为 NULL——
  SQL 三值逻辑自动帮你把"无基期"传导到最终指标，不用写特判。
- **when 链的第一分支优先**：NULL 条件不命中任何比较分支，会一路落到
  `otherwise`——所以"无基期"分支必须显式放在**最前面**，否则 2023-01 会被
  误标成"下滑"。
- yoy 的 0.1067 = (830 - 750) / 750：2024-02 对 2023-02，语义正确。
- 2024-01 环比"下滑"了 44%，只是因为 12 月是季节性高峰；与 2023-01 相比它增长了
  9.8%。这正是报表要同时给出环比和同比的原因。

## 第三步 · 多店铺：partition_by 让窗口在组边界重置

两个店铺的数据混在一张表里，`shift(1)` 会把 S2 的第一个月对到 S1 的最后
一个月——必须让窗口**按店铺分区**。

```python
rows2 = rows + [
    {"shop": "S2", "month": "2024-11", "gmv": 500},
    {"shop": "S2", "month": "2024-12", "gmv": 640},
]

multi = (
    LTSeq.from_rows(rows2)
    .sort("shop", "month")
    .derive(prev_gmv=lambda r: r.gmv.shift(1, partition_by="shop"))
    .derive(
        mom_by_shop=lambda r: (r.gmv.cast("float64") - r.prev_gmv) / r.prev_gmv,
    )
)

multi.filter(lambda r: r.shop == "S2").show();
```

> **预期输出**
>
> ```text
> +------+---------+-----+----------+-------------+
> | shop | month   | gmv | prev_gmv | mom_by_shop |
> +------+---------+-----+----------+-------------+
> | S2   | 2024-11 | 500 | None     | None        |
> | S2   | 2024-12 | 640 | 500      | 0.28        |
> +------+---------+-----+----------+-------------+
> ```

**解析**

- `shift(1, partition_by="shop")` 在**每个店铺的第一行**返回 NULL——窗口在组
  边界重置，这正是 SQL `LAG() OVER (PARTITION BY shop ORDER BY month)` 的
  LTSeq 写法。
- 注意 `sort("shop", "month")`：分区窗口按声明的排序给每个店铺内部的行排序，
  所以排序键里必须有 `month`。把 `shop` 放在最前，还能让输出保持声明顺序：
  分区窗口返回的行总是按分区聚在一起，不管声明的排序是什么（issue #202）。这只对
  这样的小数据成立：输入较大、分布在多个分区上时，各店铺的行可能不按声明顺序返回，
  而 `sort_keys` 仍然报告 `shop` 和 `month`（issue #311），展示之前要再排一次序。
- 先 derive 出 `prev_gmv` 再在下一步引用，避免 `shift` 写两遍——同一 derive 内
  的新列不能互相引用，这是 LTSeq 的硬规则（案例 02 讲过）。

## 关键点解析

- **一个 shift 值一句话**：`shift(1)` 环比、`shift(12)` 同比、
  `shift(12, partition_by=)` 分组同比——同一个函数改改参数，对应 SQL 的
  `LAG(gmv, 12) OVER (PARTITION BY shop ORDER BY month)`。
- **比率先转型**：在 `main` 上，整数 `/` 整数会像 SQL 一样截断（issue #218），
  写增长率、占比、均值差一律先 `cast("float64")`。
- **NULL 是语义的一部分**：越界 shift 的 NULL、聚合全空组的 NULL、when 链
  落空的 otherwise——理解它们，输出才不会"骗你"。

## 延伸练习

1. 给 `monthly` 加一列 `gmv_cum`：年内累计 GMV（提示：`r.gmv.cum_sum()` 需要
   全表连续排序，跨年会累计到一起——试试先按年 derive 年份再做
   `cum_sum(partition_by=年份)`）。
2. 多店铺数据上补齐同比：`shift(12, partition_by="shop")`，观察 S2 只有 2 个月
   时 yoy 全为 NULL。
3. 用 `pct_change()` 重写环比，与手写 `(x - prev) / prev` 对比。`pct_change()`
   的除法和 `/` 一样，所以要先转型（`r.gmv.cast("float64").pct_change()`）；它也
   没有 `partition_by` 参数，多店铺场景该怎么绕？
4. 把趋势标签改成四档：跳升(>10%) / 温和增长(0~10%) / 温和回落(-10%~0) /
   下滑(<-10%)，验证 when 链分支顺序的重要性。

---
**上一例**：[13 · 组内 TopN](13_topn_within_group.md) · **下一例**：[15 · 账期收口分组](15_payment_cycle.md)
