<!-- Generated from ../../cn/21_basket_pairs.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 21 · 购物篮同现对：自连接与去重配对

> **难度** ★★★ · **领域** 电商 · **前置** 案例 17
>
> **涉及 API**：`join(on=, suffix=)`（自连接）/ `filter` / `s.concat` / `group_by().agg()` / `sort` / `count`

**学习目标**

- 用**自连接**把"订单内商品两两组合"展开成同现对——关联分析（购物篮）的
  第一步
- 掌握去重配对的小技巧：`product < product_b` 让每对只出现一次
- 正视自连接的行数膨胀（N 个商品 → N² 行），知道它为什么在单机数据集上
  可接受

## 问题描述

订单明细 `orders(order_id, product)`，5 张订单、4 种商品（每单内商品不
重复）。目标：

1. 展开每个订单内的**商品对**（无序对，(A,B) 与 (B,A) 算同一个）；
2. 统计每对商品在多少张订单中**共同出现**；
3. 按同现次数降序输出——越高越适合捆绑销售/相邻陈列。

```python
from ltseq import LTSeq

orders = LTSeq.from_rows([
    {"order_id": "O1", "product": "啤酒"},
    {"order_id": "O1", "product": "尿布"},
    {"order_id": "O2", "product": "啤酒"},
    {"order_id": "O2", "product": "薯片"},
    {"order_id": "O3", "product": "啤酒"},
    {"order_id": "O3", "product": "尿布"},
    {"order_id": "O3", "product": "薯片"},
    {"order_id": "O4", "product": "尿布"},
    {"order_id": "O4", "product": "可乐"},
    {"order_id": "O5", "product": "薯片"},
    {"order_id": "O5", "product": "可乐"},
])

orders.show(11);
```

> **预期输出**
>
> ```text
> +----------+---------+
> | order_id | product |
> +----------+---------+
> | O1       | 啤酒      |
> | O1       | 尿布      |
> | O2       | 啤酒      |
> | O2       | 薯片      |
> | O3       | 啤酒      |
> | O3       | 尿布      |
> | O3       | 薯片      |
> | O4       | 尿布      |
> | O4       | 可乐      |
> | O5       | 薯片      |
> | O5       | 可乐      |
> +----------+---------+
> ```

## 第一步 · 自连接：订单内的笛卡尔积

把 `orders` 与自己按 `order_id` 连接——每个订单的所有商品两两相遇。右表
列名冲突用 `suffix="_b"` 标记成 `product_b`。

```python
cartesian = orders.join(orders, on="order_id", suffix="_b")

cartesian.count()
```

> **预期输出**
>
> ```text
> 25
> ```

**解析**

- 行数验证：每订单 N 个商品 → N² 行：O1/O2/O4/O5 各 4 行，O3 有 3 个商品
  → 9 行，合计 25 ✔。这就是自连接的**行数膨胀**：组合爆炸随订单内商品数
  平方增长。
- 25 行里有 18 行是噪声：11 行 (A,A) 自配（每条订单明细一行），以及 7 行
  镜像——每个真实组合都以 (A,B) 和 (B,A) 各出现一次。下一步用一行 filter
  同时消灭两者。
- LTSeq 的 join 支持同表连接，默认的哈希 join 不要求输入有序。
  `strategy="merge"` 只检查两张输入是否已按连接键排好，随后执行的是同一个
  join——它多的是一道校验，不是速度。

## 第二步 · 去重配对：一行 filter 消灭自配与镜像

字符串可以比较大小（字典序）。`product < product_b` 一刀下去：(A,A) 不
满足自配消失；每对镜像只剩字典序小的那个方向。

```python
pairs = cartesian.filter(lambda r: r.product < r.product_b)

pairs.sort("order_id", "product", "product_b").show();
```

> **预期输出**
>
> ```text
> +----------+---------+-----------+
> | order_id | product | product_b |
> +----------+---------+-----------+
> | O1       | 啤酒      | 尿布        |
> | O2       | 啤酒      | 薯片        |
> | O3       | 啤酒      | 尿布        |
> | O3       | 啤酒      | 薯片        |
> | O3       | 尿布      | 薯片        |
> | O4       | 可乐      | 尿布        |
> | O5       | 可乐      | 薯片        |
> +----------+---------+-----------+
> ```

7 行——5 张订单的真实无序对：O1 一对、O2 一对、O3 三对、O4 一对、O5 一对。

**解析**

- 25 → 7：过滤掉 11 个 (A,A) 自配与 7 个镜像，剩下 7 个真实组合——filter 的
  表达力把"组合数学"压缩成一行代码。
- 字典序比较对中文字符串按 Unicode 码点生效，这里只用来**定方向**（谁是
  A 谁是 B），不涉及业务含义。
- 展示时还按 `product_b` 排序：O3 有两行的 `product` 都是啤酒，没有第三个
  排序键，它们的先后没有定义。
- 若订单内可能有重复商品，先 `distinct("order_id", "product")` 再连接，
  否则同一对会计数两次。

## 第三步 · 同现计数：组对聚合

给每对拼一个可读的键，`group_by` 计数，降序排列——关联规则的候选集就
出来了。

```python
counted = (
    pairs
    .derive(pair=lambda r: r.product.s.concat(" + ", r.product_b))
    .group_by("pair")
    .agg(n_orders=lambda g: g.order_id.count())
    .sort("n_orders", "pair", desc=[True, False])
)

counted.show();
```

> **预期输出**
>
> ```text
> +-----------------+----------+
> | pair            | n_orders |
> +-----------------+----------+
> | 啤酒 + 尿布         | 2        |
> | 啤酒 + 薯片         | 2        |
> | 可乐 + 尿布         | 1        |
> | 可乐 + 薯片         | 1        |
> | 尿布 + 薯片         | 1        |
> +-----------------+----------+
> ```

**解析**

- `啤酒 + 尿布` 同现 2 次（O1、O3）：经典"啤酒与尿布"故事的迷你复现。
- `啤酒 + 薯片` 也是 2 次，与之并列。`group_by().agg()` 返回的分组没有确定
  顺序，只按 `n_orders` 排序时并列的两对谁先谁后不确定；第二排序键 `pair`
  （升序，所以写 `desc=[True, False]`）把顺序固定下来。
- `g.order_id.count()` 数的是组内行数 = 含该对的订单数（前提：每单内商品
  唯一）。在 `group_by().agg()` 里，`g.列名` 指向一列，聚合挂在列上；裸的
  `g.count()` 是 `group_ordered`/`group_sorted` 分组的写法。想要更花哨的
  输出可用 `concat_agg(g.order_id)`（从 `ltseq` 导入）把订单号拼进单元格。

## 关键点解析

- **自连接 = 组合展开的通用逃生舱**：LTSeq 没有专门的组合算子，但 join +
  不等式 filter 可以表达任何"集合内部两两"语义——代价是平方级行数。
- **suffix 参数**：`join(..., suffix="_b")` 自定义冲突后缀，比默认
  `_right` 更有业务味道；连接键本身永远不带后缀。
- **留意行数膨胀**：自连接先生成订单内全部 N² 个有序组合，filter 再丢掉
  其中大部分；超大订单表上要先考虑内存，或先按订单预聚合。
- **延伸到关联规则**：support = n_orders / 总订单数，confidence =
  P(B|A) = 同现次数 / A 出现次数——两步 `group_by` 就能补全，见练习。

## 延伸练习

1. 计算每个**单商品**的出现订单数（一步 `group_by("product")`），然后
   derive 出 support 与 confidence 列，输出完整的"啤酒→尿布"式规则表。
2. 只保留 n_orders >= 2 的对（最小支持度阈值）——一行 filter，但想想该在
   计数前还是计数后做。
3. 订单内商品数直方图：`group_by("order_id").agg(n=lambda g:
   g.product.count())` 后再对 n 做一次 `group_by`——两级聚合看订单形态。
4. 用 `semi_join` 找出"含啤酒"的订单，再反推这些订单里的其他商品——这是
   关联分析的另一条路线（先筛篮子再组合）。

---
**上一例**：[20 · 稀疏报表置空](20_sparse_report.md) · **下一例**：[22 · 余额快照](22_balance_snapshot.md)
