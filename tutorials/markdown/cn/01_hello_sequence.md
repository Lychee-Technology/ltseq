<!-- Generated from ../../cn/01_hello_sequence.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 01 · 序列思维入门：像处理序列一样处理数据

> **难度** ★☆☆ · **领域** 通用 · **前置** 无
>
> **涉及 API**：`from_dict` / `filter` / `derive` / `select` / `sort` / `sort_keys` / `shift` / `cum_sum` / `show` / `to_dicts` / `to_pandas`

**学习目标**

- 建立 LTSeq 的核心心智模型：**表是有顺序的序列，顺序本身就是计算输入**
- 掌握最常用的链式操作 `filter → derive → sort`
- 第一次使用窗口函数 `shift` 与 `cum_sum`，理解"为什么序列窗口函数要求先排序"

> 💡 **预期输出说明**：代码单元不附执行输出。每个预期输出块都是手写的，并已对照 `main` 的实际运行核对；随机数据使用固定种子，重复运行结果一致。

用最小例子建立数据模型直觉。LTSeq 思想内核：**把数据当序列，而不是无序集合**。

## 问题描述

一张 6 行的订单明细表（订单号、用户、金额）。我们想做四件小事：

1. 挑出金额大于 100 的订单，并算出含税金额；
2. 按金额从高到低排序；
3. 看每笔订单的**上一笔订单金额**（窗口思维的起点）；
4. 算金额的**累计值**（有序聚合的起点）。

问题很小，但每一步都会用到 LTSeq 的一类核心能力。

```python
from ltseq import LTSeq

orders = LTSeq.from_dict({
    "order_id": ["ORD-001", "ORD-002", "ORD-003", "ORD-004", "ORD-005", "ORD-006"],
    "user":     ["u1", "u2", "u1", "u3", "u2", "u1"],
    "amount":   [120.0, 85.5, 240.0, 60.0, 310.0, 45.0],
})

orders.show();
```

> **预期输出**
>
> ```text
> +----------+------+--------+
> | order_id | user | amount |
> +----------+------+--------+
> | ORD-001  | u1   | 120    |
> | ORD-002  | u2   | 85.5   |
> | ORD-003  | u1   | 240    |
> | ORD-004  | u3   | 60     |
> | ORD-005  | u2   | 310    |
> | ORD-006  | u1   | 45     |
> +----------+------+--------+
> ```
>
> 在 Jupyter 里直接写 `orders`（不调用 `show()`）也可以——LTSeq 实现了
> `_repr_html_`，会以表格形式渲染前几行预览。
>
> `show()` 打印表格后返回表本身，方便继续链式调用；行尾的 `;` 让 Jupyter 不再把
> 这个返回值渲染第二遍。

## 第一步 · 关系操作：filter / derive / select

```python
big = (
    orders
    .filter(lambda r: r.amount > 100)
    .derive(amount_with_tax=lambda r: r.amount * 1.1)
    .select("order_id", "user", "amount", "amount_with_tax")
)
big.show();
```

> **预期输出**
>
> ```text
> +----------+------+--------+-----------------+
> | order_id | user | amount | amount_with_tax |
> +----------+------+--------+-----------------+
> | ORD-001  | u1   | 120    | 132             |
> | ORD-003  | u1   | 240    | 264             |
> | ORD-005  | u2   | 310    | 341             |
> +----------+------+--------+-----------------+
> ```

**解析**

- `filter` 的 lambda 返回的是**表达式**（不是 Python 布尔值）。LTSeq 把它转译成
  DataFusion 查询计划，下推到 Rust 引擎向量化执行——这也是它在大数据量上快的原因。
- `derive` 有两种风格：`derive(new=lambda r: ...)` 关键字风格，或
  `derive(lambda r: {"new": ...})` 字典风格（一次加多列更方便）。
- `select` 既接受字符串列名，也接受 `lambda r: [r.a, r.b]` 表达式列表。
- 到目前为止**还没有真正执行**：所有方法只构建惰性计划，调用 `show()` /
  `to_pandas()` / `count()` 等终端操作才触发计算。

## 第二步 · 排序，以及"顺序"元数据

LTSeq 与 pandas / SQL 最大的差异在这里：`sort()` 不只是重排行——它还会把
**声明顺序（declared sort order）**登记到表的元数据里，供后续窗口函数、有序分组读取。

```python
t = orders.sort("amount", desc=True)

print(t.sort_keys)        # 声明顺序：[(列名, 是否降序)]
```

> **预期输出**
>
> ```text
> [('amount', True)]
> ```

**解析**

- `sort_keys` 返回 `[(列名, 是否降序)]`；顺序未知时返回 `None`。
- 未经 `sort()`（或 `assume_sorted()` 声明已排序）就使用序列窗口函数（`shift` /
  `rolling` / `diff` / `cum_*`），会直接抛 `SortRequiredError`。这是**特性而不是
  缺陷**：顺序是计算输入，LTSeq 拒绝在顺序不明时给出不可复现的结果。
- 显式写了 `.over(order_by=...)` 的窗口自带顺序，不需要先排序；`rank()`、
  `ntile()` 通常这样用（案例 09、13）。
- 数据本身已经有序（比如预排序的 Parquet）？用 `assume_sorted("col")` 声明即可
  跳过物理排序。

## 第三步 · 第一个窗口函数：shift 与 cum_sum

```python
result = (
    t
    .derive(prev_amount=lambda r: r.amount.shift(1))
    .cum_sum("amount")
    .select("order_id", "amount", "prev_amount", "amount_cumsum")
)
result.show();
```

> **预期输出**
>
> ```text
> +----------+--------+-------------+---------------+
> | order_id | amount | prev_amount | amount_cumsum |
> +----------+--------+-------------+---------------+
> | ORD-005  | 310    | None        | 310           |
> | ORD-003  | 240    | 310         | 550           |
> | ORD-001  | 120    | 240         | 670           |
> | ORD-002  | 85.5   | 120         | 755.5         |
> | ORD-004  | 60     | 85.5        | 815.5         |
> | ORD-006  | 45     | 60          | 860.5         |
> +----------+--------+-------------+---------------+
> ```

**解析**

- `shift(1)` 取"上一行"：正数向后看，负数向前看，边界处为 NULL（可用
  `shift(1, default=0)` 填充）。注意因为表按金额**降序**排，累计值也是降序累计的
  ——**顺序决定语义**，这正是序列思维。
- `cum_sum("amount")` 是表级快捷方式，新增 `amount_cumsum` 列。想要自定义列名，
  用表达式形式：`derive(x=lambda r: r.amount.cum_sum())`。
- 同族还有 `cum_max()` / `cum_min()`（运行极值）与 `diff()`（行差分），案例 06
  用 `cum_max()` 计算回撤。

## 第四步 · 回到 Python 生态

```python
rows = result.to_dicts()
print(rows[0])

df = result.to_pandas()      # 需要 pandas；由 Arrow 表转换而来
print(type(df).__name__)
```

> **预期输出**
>
> ```text
> {'order_id': 'ORD-005', 'amount': 310.0, 'prev_amount': None, 'amount_cumsum': 310.0}
> DataFrame
> ```

**解析**

- `to_dicts()` 把行变成字典列表（NULL → `None`），适合小结果集的 Python 后处理。
- `to_arrow()` 走 Arrow C Data Interface，**共享缓冲不拷贝**；`to_pandas()` 再用
  pyarrow 把这张 Arrow 表转换成 DataFrame，这一步会拷贝。LTSeq 也能反向接收：
  `from_arrow(pa_table)` 同样共享缓冲，polars / duckdb 等任何实现了 Arrow
  PyCapsule 接口的对象都可以直接 `from_arrow` 进来；`from_pandas(df)` 先转换成
  Arrow 表。

## 关键点解析

1. **表 = 有序序列**。pandas / SQL 把表当无序集合，顺序只是展示问题；LTSeq 把
   顺序当计算输入。窗口、连续段检测、会话切分都建立在它之上。
2. **惰性 + 下推**。方法链构建查询计划，终端操作触发执行；filter 会下推到存储层，
   select 触发投影剪枝，底层是 Rust + DataFusion + Arrow 的向量化引擎。
3. **两种 lambda**。行级 lambda（`filter` / `derive` / `sort`）里 `r` 是行代理；
   分组聚合 lambda 里 `g` 是组代理——本案例只用到了前者，后面案例展开后者。
4. **先 sort 再窗口**。序列窗口函数（`shift` / `rolling` / `cum_*` / `diff`）默认
   沿表的声明顺序计算，没有声明就抛 `SortRequiredError`。
5. **与 Python 生态零摩擦**。进出 Arrow 零拷贝，pandas 经 Arrow 转换；CSV /
   Parquet 开箱即读。

## 延伸练习

- 用 `seq(1, 10)` 生成整数序列，配合 `cum_sum` 验证 1+2+…+n（提示：`from ltseq import seq`；它和 `range` 一样生成 1…9，且没有声明顺序，要先 `sort("value")`）。
- 把 `LTSeq.from_dict(...)` 换成 `LTSeq.read_csv("你自己的.csv")` 重跑一遍。
- 故意去掉 `.sort(...)` 直接用 `shift`，观察 `SortRequiredError` 的报错信息。
- 进阶阅读：仓库 `docs/USER_MODEL.cn.md` 的「核心心智模型」一节。

---
**上一例**：[目录](../../README.md) · **下一例**：[02 · 股价技术指标](02_stock_indicators.md)
