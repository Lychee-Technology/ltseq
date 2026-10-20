<!-- Generated from ../../cn/17_rank_migration.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 17 · 榜单排名迁移：今日 vs 昨日谁在上升

> **难度** ★★★ · **领域** 金融 · **前置** 案例 13
>
> **涉及 API**：`rank().over(partition_by=, order_by=, descending=)` / `cast("int64")` / `filter` / `select` / `rename` / `join(on=, how="left")` / `when` / `is_null`

**学习目标**

- 用"分区排名 + 自连接"回答报表经典题：**榜单变化**——谁上升、谁下降、谁
  新晋、谁掉榜
- 掌握 `join` 的列名规则：右表冲突列自动加 `_right` 后缀，配合 `rename` 提前
  改名最干净
- 理解 left join 的 NULL 恰好承载"新晋"语义——NULL 又一次成为信息

## 问题描述

两天的 GMV 榜单 `sales(day, product, gmv)`。目标：

1. 分别给两天算 GMV 排名（组内、降序）；
2. 以今日榜单为基准，标出每件商品的**名次变化**：上升 / 下降 / 持平 / 新晋；
3. 顺带思考：昨日有、今日无的商品（掉榜）该怎么找。

```python
from ltseq import LTSeq

sales = LTSeq.from_rows([
    {"day": "d1", "product": "A", "gmv": 900},
    {"day": "d1", "product": "B", "gmv": 700},
    {"day": "d1", "product": "C", "gmv": 500},
    {"day": "d1", "product": "D", "gmv": 300},
    {"day": "d1", "product": "E", "gmv": 100},
    {"day": "d1", "product": "F", "gmv": 400},
    {"day": "d2", "product": "A", "gmv": 850},
    {"day": "d2", "product": "B", "gmv": 950},
    {"day": "d2", "product": "C", "gmv": 520},
    {"day": "d2", "product": "D", "gmv": 610},
    {"day": "d2", "product": "E", "gmv": 90},
    {"day": "d2", "product": "G", "gmv": 700},
])

sales.show(12);
```

> **预期输出**
>
> ```text
> +-----+---------+-----+
> | day | product | gmv |
> +-----+---------+-----+
> | d1  | A       | 900 |
> | d1  | B       | 700 |
> | d1  | C       | 500 |
> | d1  | D       | 300 |
> | d1  | E       | 100 |
> | d1  | F       | 400 |
> | d2  | A       | 850 |
> | d2  | B       | 950 |
> | d2  | C       | 520 |
> | d2  | D       | 610 |
> | d2  | E       | 90  |
> | d2  | G       | 700 |
> +-----+---------+-----+
> ```

注意两天的商品集合**不同**：d2 没有 F，多出 G——所以两份榜单必须按商品
对齐（本案用 join，练习 4 用 `pivot`），不能按行位置对齐。`show(12)` 打印全部
12 行，默认只打印 10 行。

## 第一步 · 分区排名：每天榜单各自独立

`partition_by=r.day` 让两个榜单互不干扰；`descending=True` 按 GMV 从大到小。

```python
from ltseq import rank

ranked = sales.derive(
    rk=lambda r: rank().over(
        partition_by=r.day, order_by=r.gmv, descending=True
    ),
).derive(rk=lambda r: r.rk.cast("int64"))   # rank() 返回无符号整数，见解析

ranked.sort("day", "rk").show(12);
```

> **预期输出**
>
> ```text
> +-----+---------+-----+----+
> | day | product | gmv | rk |
> +-----+---------+-----+----+
> | d1  | A       | 900 | 1  |
> | d1  | B       | 700 | 2  |
> | d1  | C       | 500 | 3  |
> | d1  | F       | 400 | 4  |
> | d1  | D       | 300 | 5  |
> | d1  | E       | 100 | 6  |
> | d2  | B       | 950 | 1  |
> | d2  | A       | 850 | 2  |
> | d2  | G       | 700 | 3  |
> | d2  | D       | 610 | 4  |
> | d2  | C       | 520 | 5  |
> | d2  | E       | 90  | 6  |
> +-----+---------+-----+----+
> ```

**解析**

- 排名函数不需要先 `sort()`——`.over(order_by=...)` 自带排序，见案例 13。
- `rank()` 并列同名次；GMV 撞车时升级为 `row_number()`（并列强行分先后）或
  `dense_rank()`（并列不跳号），按业务口径选。
- `rank()` 目前返回无符号整数（`uint64`），尽管 `ranked.schema` 显示的是
  `int64`。无符号减法不会变成负数，而是回绕：不做转换的话，第二步的
  `prev_rank - rk` 会把 A 的 1 − 2 算成 18446744073709551615。所以第二个
  `derive` 先把 `rk` 转成 `int64`；之所以分两步，是因为 `cast` 暂时不能直接
  套在窗口表达式外面。
- 这一步的输出还是"长表"（day 在行上）；排名迁移要的是"两个榜单按商品对
  齐"——下一步把 d1 的名次**搬到** d2 的行上。

## 第二步 · 自连接对齐：昨天的名次挂到今天

拆出两天的名次列，给昨天的 `rk` 改名 `prev_rank`，然后以今日为主表 left
join 昨日——商品集合的差异转化为 NULL。

```python
today = (
    ranked
    .filter(lambda r: r.day == "d2")
    .select("product", "rk", "gmv")
)
yest = (
    ranked
    .filter(lambda r: r.day == "d1")
    .select("product", "rk")
    .rename(rk="prev_rank")
)

mig = (
    today.join(yest, on="product", how="left")
    .derive(delta=lambda r: r.prev_rank - r.rk)
    .sort("rk")
)

mig.show();
```

> **预期输出**
>
> ```text
> +---------+----+-----+-----------+-------+
> | product | rk | gmv | prev_rank | delta |
> +---------+----+-----+-----------+-------+
> | B       | 1  | 950 | 2         | 1     |
> | A       | 2  | 850 | 1         | -1    |
> | G       | 3  | 700 | None      | None  |
> | D       | 4  | 610 | 5         | 1     |
> | C       | 5  | 520 | 3         | -2    |
> | E       | 6  | 90  | 6         | 0     |
> +---------+----+-----+-----------+-------+
> ```

**解析**

- `rename(rk="prev_rank")`：参数名是**旧列名**、值是新列名。提前改名让
  join 后无需处理 `_right` 后缀——若不改名，右表冲突列会变成 `rk_right`。
- `join(on="product", how="left")`：同名列 product 作为连接键被合并保留；
  左表为主，右表没有的行（G）填 NULL。
- `delta = prev_rank - rk`：**正数=上升**（昨 5 今 4 → +1），负数=下降。G 的
  prev_rank 是 NULL，delta 也是 NULL——三值逻辑照常传播，正好留给下一步
  分类。
- 掉榜的 F 不在结果里：left join 以今日为主表，昨日独有的行自然消失。
  找掉榜要用 `anti_join`（延伸练习 2）。

## 第三步 · 变化标签：when 链收尾

NULL 优先判断（新晋），再按 delta 符号分类——和案例 14 的 when 链纪律
一致。

```python
from ltseq import when

labeled = mig.derive(
    move=lambda r: when(r.prev_rank.is_null(), "新晋")
    .when(r.delta > 0, "上升")
    .when(r.delta == 0, "持平")
    .otherwise("下降"),
)

labeled.show();
```

> **预期输出**
>
> ```text
> +---------+----+-----+-----------+-------+--------+
> | product | rk | gmv | prev_rank | delta | move   |
> +---------+----+-----+-----------+-------+--------+
> | B       | 1  | 950 | 2         | 1     | 上升     |
> | A       | 2  | 850 | 1         | -1    | 下降     |
> | G       | 3  | 700 | None      | None  | 新晋     |
> | D       | 4  | 610 | 5         | 1     | 上升     |
> | C       | 5  | 520 | 3         | -2    | 下降     |
> | E       | 6  | 90  | 6         | 0     | 持平     |
> +---------+----+-----+-----------+-------+--------+
> ```

**解析**

- `when(r.prev_rank.is_null(), ...)` 放在链首——NULL 不满足任何比较条件，
  若不显式拦截会落到 `otherwise` 被误标成"下降"。
- `delta > 0` 在 delta 为 NULL 时同样不命中（NULL 比较为 NULL），链首已拦，
  后续分支安全。
- B 从第 2 升到第 1、C 从第 3 掉到第 5——一张六行的表，比翻两份榜单直观
  得多。这就是"把顺序信息变成列"的价值。

## 关键点解析

- **报表迁移题的通用骨架**：分区排名 → 各取所需列 → 改名 → left join →
  差值 → when 分类。换成用户留存榜、主播礼物榜、关键词热搜榜，骨架不动。
- **join 列名规则**（Polars 语义）：右表与左表冲突的列自动加 `_right` 后缀，
  连接键合并保留；提前 `rename` 是最可读的防冲突手段。
- **NULL 承载"新晋"**：left join 的 unmatched NULL、delta 的 NULL、when 链的
  is_null 分支——三层 NULL 语义环环相扣，这正是案例 14 说的"NULL 是信息的
  延伸应用"。

## 延伸练习

1. 三日榜单迁移：先给 `sales` 加一天 d3 的榜单，以 d3 为主表，同时 join
   d2（昨）与 d1（前日）。注意列名冲突：都不改名时，第二次 join 会因
   `rk_right` 已存在而抛 `ValueError`，所以两个名次列都要 rename。
2. 找出**掉榜**商品：`yest.anti_join(today, on="product")`——F 应该被找出来。
3. 给 `labeled` 加一列 `rk_change_abs`（绝对变化幅度）并按它排序，输出"变化
   最大"的 TOP 2（提示：`r.delta.abs()`，注意 NULL 行要先过滤；A、B、D 都是 1，
   需要自己定一个次序键）。
4. 用 `ranked.pivot(index="product", columns="day", values="rk")` 做宽表对照——
   观察 F 的 d2 和 G 的 d1 都是 NULL，与 join 方案互相印证。

---
**上一例**：[16 · 最长连涨区间](16_longest_rising_span.md) · **下一例**：[18 · 复合会话规则](18_session_composite.md)
