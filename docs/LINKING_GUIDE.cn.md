# Pointer-Based Linking - 用户指南

相关文档：

- `docs/README.md`：文档索引
- `docs/USER_MODEL.cn.md`：用户心智模型
- `docs/ARCHITECTURE.cn.md`：系统架构与执行模型
- `docs/api.cn.md`：中文 API 文档
- `docs/LINKING_GUIDE.md`：英文原版 Linking 指南

## 概览

LTSeq 的 **linking 功能** 提供了一种轻量、基于指针语义的外键关系建模方式。与一开始就物化昂贵 join 的做法不同，link 会先建立引用关系，只在真正需要数据时才执行。

**主要优点**：

- 轻量：link 是惰性的，在访问数据前不会执行 join
- 直观：关系通过 lambda 条件表达
- 灵活：支持 `INNER`、`LEFT`、`RIGHT`、`FULL` 四种 join 类型
- 安全：原表不会被修改，schema 会持续被跟踪

## 快速开始

### 基础关联

```python
from ltseq import LTSeq

# 读取数据
orders = LTSeq.read_csv("orders.csv")          # id, product_id, quantity
products = LTSeq.read_csv("products.csv")      # product_id, name, price

# 创建 link（此时还没有真正执行 join）
linked = orders.link(
    products,
    on=lambda o, p: o.product_id == p.product_id,
    as_="prod"
)

# 查看 linked 结果
linked.show()

# 当你需要实际 join 结果时再物化
result = linked._materialize()
```

### 物化前先过滤

```python
# 只基于源表列过滤（快，不需要 join）
high_qty = linked.filter(lambda r: r.quantity > 10)

# 结果仍然是 LinkedTable，真正的 join 会在物化时发生
high_qty.show()
```

### 使用不同的 Join 类型

```python
# INNER join（默认）
inner = orders.link(products, on=..., as_="prod", join_type="inner")

# LEFT join
left = orders.link(products, on=..., as_="prod", join_type="left")

# RIGHT join
right = orders.link(products, on=..., as_="prod", join_type="right")

# FULL join
full = orders.link(products, on=..., as_="prod", join_type="full")
```

## 核心概念

### Linking 与 Joining 的区别

**Link**：

- 惰性求值
- 存储的是关系引用，而不是立即生成数据
- 元数据开销较小
- 适合探索式分析和先过滤后使用的流程

**Join（传统 SQL）**：

- 立即求值
- 结果会直接物化
- 在大表上可能很昂贵

### Join 条件

用 lambda 表达 join 条件：

```python
# 单列 join
on=lambda o, p: o.product_id == p.product_id

# 复合键
on=lambda o, p: (o.product_id == p.product_id) & (o.year == p.year)

# 更复杂的条件
on=lambda o, p: (o.id == p.order_id) & (o.status == "active")
```

### 列命名规则

当你使用 alias `"prod"` 时，右表所有列都会带上该前缀：

```python
# orders 原始列：id, product_id, quantity
# products 原始列：product_id, name, price

# link(as_="prod") 后：
# 左表列保持不变：id, product_id, quantity
# 右表列变为：prod_product_id, prod_name, prod_price
```

在 filter 或 select 中，直接使用带前缀的列：

```python
linked.filter(lambda r: r.prod_price > 100)
linked.select("id", "quantity", "prod_name")
```

### 物化

物化会真正执行 join，并返回普通的 `LTSeq`：

```python
linked = orders.link(products, on=..., as_="prod")

result = linked._materialize()
result.show()
result.filter(lambda r: r.prod_price > 50)
```

注意：物化结果会被缓存。多次调用 `_materialize()` 会返回同一个对象。

```python
result1 = linked._materialize()
result2 = linked._materialize()
assert result1 is result2
```

## 典型用例

### 用例 1：给订单补充商品信息

```python
orders = LTSeq.read_csv("orders.csv")
products = LTSeq.read_csv("products.csv")

linked = orders.link(
    products,
    on=lambda o, p: o.product_id == p.product_id,
    as_="prod"
)

expensive = linked.filter(lambda r: r.prod_price > 500)
expensive.show()
```

### 用例 2：多次 Linking 后再过滤

```python
orders = LTSeq.read_csv("orders.csv")
products = LTSeq.read_csv("products.csv")
customers = LTSeq.read_csv("customers.csv")

linked1 = orders.link(
    products,
    on=lambda o, p: o.product_id == p.product_id,
    as_="prod"
)

linked2 = linked1.link(
    customers,
    on=lambda l, c: l.customer_id == c.id,
    as_="cust"
)

high_value = linked2.filter(lambda r: r.prod_price > 100)
high_value.show()
```

### 用例 3：LEFT Join 查缺失数据

```python
orders = LTSeq.read_csv("orders.csv")
products = LTSeq.read_csv("products.csv")

linked = orders.link(
    products,
    on=lambda o, p: o.product_id == p.product_id,
    as_="prod",
    join_type="left"
)

missing = linked.filter(lambda r: r.prod_name is None)
missing.show()
```

### 用例 4：复合键关联

```python
sales = LTSeq.read_csv("sales.csv")
pricing = LTSeq.read_csv("pricing.csv")

linked = sales.link(
    pricing,
    on=lambda s, p: (s.year == p.year) & (s.month == p.month) & (s.product_id == p.product_id),
    as_="price"
)

linked.show()
```

## API 摘要

### `LTSeq.link()`

```python
linked = table.link(
    target,
    on,
    as_,
    join_type="inner"
)
```

**参数**：

- `target`：目标表，类型为 `LTSeq`
- `on`：join 条件 lambda
- `as_`：右表列前缀
- `join_type`：`"inner"`、`"left"`、`"right"`、`"full"`

**返回值**：`LinkedTable`

### `LinkedTable._materialize()`

执行 join，并返回物化后的 `LTSeq`。

## 什么时候该用 Link

优先选择 `link()` 的场景：

- 你暂时还不确定是否真的需要右表数据
- 很多后续操作只依赖左表
- 你想延迟 join 成本
- 你在做探索式分析，希望保持查询尽量惰性

优先选择 `join()` 的场景：

- 你已经确定需要一个普通的平面 join 结果
- 下游步骤会立刻广泛使用左右两边列

一句话总结：`link()` 适合“先建立关系，按需取值”，`join()` 适合“马上要一张合并后的表”。
