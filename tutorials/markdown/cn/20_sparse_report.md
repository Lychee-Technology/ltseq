<!-- Generated from ../../cn/20_sparse_report.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 20 · 稀疏报表：组内重复值置空

> **难度** ★☆☆ · **领域** 电商 · **前置** 案例 01
>
> **涉及 API**：`sort` / `shift(1)` / `if_else` / `&` 组合 / `fold`（前值填充）

**学习目标**

- 实现 Excel 式"同值只留首行"的稀疏报表：与上一行相同则置 NULL
- 学会**多级表头**的级联置空：下级置空的前提是上级也与上一行相同
- 用 `fold` 反向操作——把 NULL 填充回完整值（前值填充 ffill），并理解为什么
  这个操作天然适合用 fold 表达

## 问题描述

门店销售表 `shops(region, city, sales)`，区域与城市大量重复。导出报表时
希望：**region 列只在换区域的首行显示，city 列只在 (region, city) 组合的
首行显示**——即稀疏化排版，多余重复置 NULL。

这是"相邻行比较"问题：每个值只和它的**上一行**有关。

```python
from ltseq import LTSeq

shops = LTSeq.from_rows([
    {"region": "east", "city": "shanghai", "sales": 120},
    {"region": "east", "city": "nanjing", "sales": 80},
    {"region": "east", "city": "suzhou", "sales": 60},
    {"region": "north", "city": "beijing", "sales": 200},
    {"region": "north", "city": "tianjin", "sales": 90},
    {"region": "south", "city": "guangzhou", "sales": 150},
    {"region": "south", "city": "guangzhou", "sales": 110},
    {"region": "south", "city": "shenzhen", "sales": 95},
]).sort("region", "city", "sales")

shops.show();
```

> **预期输出**
>
> ```text
> +--------+-----------+-------+
> | region | city      | sales |
> +--------+-----------+-------+
> | east   | nanjing   | 80    |
> | east   | shanghai  | 120   |
> | east   | suzhou    | 60    |
> | north  | beijing   | 200   |
> | north  | tianjin   | 90    |
> | south  | guangzhou | 110   |
> | south  | guangzhou | 150   |
> | south  | shenzhen  | 95    |
> +--------+-----------+-------+
> ```

字符串按字典序排序：east 区域内 nanjing < shanghai < suzhou。south 的
guangzhou 出现两次——正好检验"组合级"置空。第三个排序键 `sales` 只用来给
这两行定序，否则它们的先后不确定。

## 第一步 · 区域列置空：与上一行比较

`shift(1)` 取上一行；相同则置 NULL。首行没有上一行，比较为 NULL，不命中
相等条件，原值保留——NULL 语义在这里恰好是对的。

```python
from ltseq import if_else

sparse1 = shops.derive(
    region_show=lambda r: if_else(
        r.region == r.region.shift(1), None, r.region
    ),
)

sparse1.show();
```

> **预期输出**
>
> ```text
> +--------+-----------+-------+-------------+
> | region | city      | sales | region_show |
> +--------+-----------+-------+-------------+
> | east   | nanjing   | 80    | east        |
> | east   | shanghai  | 120   | None        |
> | east   | suzhou    | 60    | None        |
> | north  | beijing   | 200   | north       |
> | north  | tianjin   | 90    | None        |
> | south  | guangzhou | 110   | south       |
> | south  | guangzhou | 150   | None        |
> | south  | shenzhen  | 95    | None        |
> +--------+-----------+-------+-------------+
> ```

**解析**

- `r.region == r.region.shift(1)`：首行为 NULL → `if_else(NULL, ...)` 走 false
  分支 → 保留原值。**不需要** `is_null()` 特判，SQL 三值逻辑替你把首行安排
  妥当。
- `if_else` 的 true 分支传 `None` 字面量 → 生成 NULL。NULL 与任何字符串列
  共存没有类型问题。
- 注意 `region_show` 与 `region` 并存：稀疏列是**新增展示列**，原始列保留
  才能支撑后续计算与还原。

## 第二步 · 城市列置空：级联条件

城市置空的前提更严：region **和** city 都与上一行相同。guangzhou 的第二行
两条都满足 → 双双置空。

```python
sparse2 = sparse1.derive(
    city_show=lambda r: if_else(
        (r.region == r.region.shift(1)) & (r.city == r.city.shift(1)),
        None,
        r.city,
    ),
)

sparse2.show();
```

> **预期输出**
>
> ```text
> +--------+-----------+-------+-------------+-----------+
> | region | city      | sales | region_show | city_show |
> +--------+-----------+-------+-------------+-----------+
> | east   | nanjing   | 80    | east        | nanjing   |
> | east   | shanghai  | 120   | None        | shanghai  |
> | east   | suzhou    | 60    | None        | suzhou    |
> | north  | beijing   | 200   | north       | beijing   |
> | north  | tianjin   | 90    | None        | tianjin   |
> | south  | guangzhou | 110   | south       | guangzhou |
> | south  | guangzhou | 150   | None        | None      |
> | south  | shenzhen  | 95    | None        | shenzhen  |
> +--------+-----------+-------+-------------+-----------+
> ```

**解析**

- 级联规则 = 多个相邻比较的 `&`。若 region 不同，city 即使相同（本例不会
  出现，但换一份数据就可能）也**不该**置空——这就是为什么
  条件里必须带上 region 比较。
- 城市列引用的是原始 `region`/`city`，不是 `region_show`——置空列只做展示，
  判断逻辑永远挂在原始列上，两层职责不混淆。
- 想要"值变化检测"的通用形态？`r.x != r.x.shift(1)` 取反即"变化行"，可以
  派生变更多少、连续保持多少行等报表指标。

## 第三步 · 反向操作：把 NULL 填回来（ffill）

拿到稀疏表后要做计算，NULL 是碍事的。**前值填充**（forward fill）是
fold 的天然舞台：状态 = 最近一次见到的非空值。

```python
filled = sparse2.fold(
    lambda s, r: r["region_show"] if r["region_show"] is not None else s,
    init=None,
    into="region_fill",
)

filled.select("region_show", "city_show", "sales", "region_fill").show();
```

> **预期输出**
>
> ```text
> +-------------+-----------+-------+-------------+
> | region_show | city_show | sales | region_fill |
> +-------------+-----------+-------+-------------+
> | east        | nanjing   | 80    | east        |
> | None        | shanghai  | 120   | east        |
> | None        | suzhou    | 60    | east        |
> | north       | beijing   | 200   | north       |
> | None        | tianjin   | 90    | north       |
> | south       | guangzhou | 110   | south       |
> | None        | None      | 150   | south       |
> | None        | shenzhen  | 95    | south       |
> +-------------+-----------+-------+-------------+
> ```

**解析**

- `fold` 逐行推进状态：当前行非空 → 状态更新为该值；为空 → 状态原样传递，
  并写入 `region_fill`。这正是 pandas `ffill()` 的语义，LTSeq 里一行 fold
  完成。
- `fold` 要求声明过的顺序，所以直接对 `sparse2` 执行。若先 select 出展示列，
  排序键列被丢掉，声明的顺序也随之消失，`fold` 会抛 `SortRequiredError`。
  `select` 放在后面，只为展示。
- `fold` 按行返回的顺序传递状态。输入较大、分布在多个分区上时，`sparse1` 和
  `sparse2` 里的 `shift` 窗口可能让行不按声明顺序返回，而 `sort_keys` 仍然
  报告这个顺序（issue #311），`region_fill` 就会把一个区域的值填进另一个区域
  的行。修复之前，用真实数据时请在 `fold` 之前再调用一次
  `.sort("region", "city", "sales")`。
- fold 的 lambda 操作的是 **Python 值**（row 是只读的行视图，
  `r["region_show"]` 与 `r.region_show` 都可以），所以 `is not None` 是普通
  Python 判断——与表达式 lambda 里的 `.is_null()` 区分开（案例 25 还会对比
  这对场景）。
- **为什么用 fold 做 ffill**：NULL 的传播是"有状态"的（要记住上一个非空
  值），shift 只看固定偏移的邻居，fold 则直接携带这个状态。但它不是唯一的
  路线：先对"有值"标记做 `cum_sum` 给块编号，再用
  `group_ordered(...).derive(...)` 把每块的 `g.first().region_show` 广播下去
  （案例 10），全程保持惰性。fold 写起来更短，但案例 06 的性能提醒依然
  有效：fold 会物化整表。

## 关键点解析

- **相邻比较是稀疏化的全部**：`x == x.shift(1)` 一发子弹解决"留首行"；
  多级表头就是多个相邻比较的 `&` 级联。
- **展示列与原始列分离**：`*_show` 只管排版，判断永远用原始列——报表
  代码最常见的腐蚀就是拿展示列当下游输入。
- **ffill 天然是 fold**：需要"记住上一次"的计算就是状态，fold 一行就能
  携带。大表上可以走惰性路线（`cum_sum` 块编号 + `group_ordered` 广播），
  避开 fold 的整表物化。

## 延伸练习

1. 给 city_show 也做 ffill（`city_fill`），然后比较 `region_fill + city_fill`
   与原始组合列——验证还原无损。
2. 反向需求："**变化行**报表"——只保留与上一行不同的行（drop 重复）。
   `filter` 里不能直接写 `shift`，要先 derive 出标记：
   `derive(changed=lambda r: r.region != r.region.shift(1)).filter(lambda r: r.changed)`
   首行 NULL 不命中被丢了，想想怎么补回首行（条件里加
   `r.region.shift(1).is_null() |`）。
3. 用 `fold` 给稀疏表加一列 `run_len`：当前组合已连续出现的行数（状态为
   计数器，与上一行相同则 +1 否则归 1）。
4. 若数据未排序，本案例所有结论还成立吗？构造 region 交错的数据跑一遍，
   理解"相邻"永远是"声明顺序上的相邻"。

---
**上一例**：[19 · 汇总分摊到明细](19_alloc_summary.md) · **下一例**：[21 · 购物篮同现对](21_basket_pairs.md)
