# Linking 指南（LTSeq.link）

相关文档：

- `docs/README.md`：文档索引
- `docs/USER_MODEL.cn.md`：用户心智模型
- `docs/ARCHITECTURE.cn.md`：系统架构与执行模型
- `docs/api.cn.md`：中文 API 文档
- `docs/LINKING_GUIDE.md`：英文原版 Linking 指南

## 概览

`LTSeq.link()` 把外键关系表达为一个**延迟的、前缀别名的等值连接**。`link()`
记录连接条件与目标别名，并预先算出 joined schema，但在你物化结果之前不执行任何
计算。它是惰性 join——不是指针/take 结构，也没有廉价的逐行导航。

**要点**：

- 惰性：访问数据（collect / to_pandas / len ...）前不执行 join
- 前缀别名：目标列变为 `{alias}_{col}`，源表列保留原名
- 支持 `INNER`、`LEFT`、`RIGHT`、`FULL` 四种 join 类型
- 变换返回普通 `LTSeq`，其行数遵循 join 语义

## 快速开始

```python
from ltseq import LTSeq

orders = LTSeq.read_csv("orders.csv")          # id, product_id, quantity
products = LTSeq.read_csv("products.csv")      # product_id, name, price

# 建立 link（尚未 join——只记录条件 + schema）
linked = orders.link(
    products,
    on=lambda o, p: o.product_id == p.product_id,
    as_="prod",
)

# 显示 joined 数据（为展示而执行 join）
linked.show()

# 取惰性 joined 表（普通 LTSeq）
joined = linked.to_ltseq()

# 或执行并把结果缓存到内存
result = linked.collect()
```

**输出**：
```
| id | product_id | quantity | prod_product_id | prod_name | prod_price |
|----|------------|----------|-----------------|-----------|------------|
| 1  | 101        | 5        | 101             | Widget    | 50         |
| 2  | 102        | 3        | 102             | Gadget    | 75         |
```

### 变换返回 LTSeq

`LinkedTable` 上的每个变换都会构建惰性 join 计划并在其上执行，返回普通 `LTSeq`：

```python
# filter 可引用源列（原名）与 linked 列（{alias}_col）——两者都在 joined schema 中
cheap = linked.filter(lambda r: r.prod_price < 10)     # -> LTSeq
picked = linked.select("id", "quantity", "prod_name")  # -> LTSeq
```

因为变换作用在 join **之后**，其行数遵循 join 语义：inner/right/full 会丢弃或
新增不匹配行，一对多匹配会把一个源行扇出成多行——随后的 `slice`/`filter` 看到的
是 join 后的行，而非原始源行。

### 使用不同的 join 类型

```python
inner = orders.link(products, on=..., as_="prod", join_type="inner")  # 默认，仅匹配行
left  = orders.link(products, on=..., as_="prod", join_type="left")   # 保留全部 orders
right = orders.link(products, on=..., as_="prod", join_type="right")  # 保留全部 products
full  = orders.link(products, on=..., as_="prod", join_type="full")   # 两表全部行
```

## 概念

### link() 与 join()

`link()` 和 `join()` 构建同一类惰性 DataFusion join，区别在列命名约定与易用性：

- **`link()`** 把整个目标表命名为 `{alias}_col`，让多跳链式 join 不产生歧义，并
  提供 `LinkedTable` 链式糖。用于事实表→维表的富化。
- **`join()`** 使用 Polars 风格的仅冲突时加后缀，直接返回 `LTSeq`。用于一次性的
  关系连接。

两者都在结果被消费前不物化。

### 连接条件

用双参数 lambda 表达连接条件（仅等值；复合键用 `&`，不支持 `|`）：

```python
on=lambda o, p: o.product_id == p.product_id
on=lambda o, p: (o.product_id == p.product_id) & (o.year == p.year)
```

### 列命名

用别名 `"prod"` 关联时，每个目标列都会加前缀：

```python
# orders:   id, product_id, quantity          （保留原名）
# products: product_id, name, price           （加前缀）
# joined:   id, product_id, quantity,
#           prod_product_id, prod_name, prod_price
```

在变换中引用前缀列：

```python
linked.filter(lambda r: r.prod_price > 100)
linked.select("id", "quantity", "prod_name")
```

### to_ltseq() 与 collect()

- `to_ltseq()` 返回**惰性** joined 表（普通 `LTSeq`，不执行）——用它继续用完整的
  `LTSeq` API 组合。
- `collect()` **执行** join 并返回内存中的 `LTSeq`（语义同 `LTSeq.collect`）。

惰性计划只构建一次并缓存，重复变换复用同一个 join 节点。

## 用例

### 用产品明细富化订单

```python
linked = orders.link(
    products, on=lambda o, p: o.product_id == p.product_id, as_="prod"
)
expensive = linked.filter(lambda r: r.prod_price > 500)  # -> LTSeq
expensive.show()
```

### 多跳链式

链式 `link()` 建立在上一跳 join 的真实计划之上，因此下一个条件可引用上一跳别名的列：

```python
chained = (
    orders.link(products, on=lambda o, p: o.product_id == p.product_id, as_="prod")
          .link(customers, on=lambda r, c: r.prod_customer_id == c.id, as_="cust")
)
high_value = chained.filter(lambda r: r.prod_price > 100)  # -> LTSeq
high_value.show()
```

### LEFT join 处理缺失数据

```python
linked = orders.link(
    products,
    on=lambda o, p: o.product_id == p.product_id,
    as_="prod",
    join_type="left",  # 保留全部 orders
)
missing = linked.filter(lambda r: r.prod_name.is_null())
missing.show()
```

### 复合键连接

```python
linked = sales.link(
    pricing,
    on=lambda s, p: (s.year == p.year) & (s.month == p.month) & (s.product_id == p.product_id),
    as_="price",
)
linked.show()
```

## API 参考

### `LTSeq.link()`

```python
linked = table.link(
    target,              # 目标表（LTSeq）
    on,                  # 连接条件（双参数 lambda，仅等值）
    as_,                 # 给每个目标列加的前缀（str）
    join_type="inner",   # "inner" | "left" | "right" | "full"
)
```

**返回**：`LinkedTable`。

### LinkedTable 方法

- `to_ltseq() -> LTSeq` — 惰性 joined 表（不执行）。
- `collect() -> LTSeq` — 执行 join，返回内存中的 LTSeq。
- `show(n=10)` — 显示至多 `n` 行 joined 数据。
- `filter(predicate) -> LTSeq` — 在 joined 行上过滤。
- `select(*cols) -> LTSeq` — 从 joined 表投影列。
- `derive(*args, **kwargs) -> LTSeq` — 新增列（可引用 linked 列）。
- `sort(*keys, **kwargs) -> LTSeq` — 对 joined 行排序。
- `slice(offset, length) -> LTSeq` — 对 joined 行切片。
- `distinct(key_fn=None) -> LTSeq` — 对 joined 行去重。
- `link(target, on, as_, join_type="inner") -> LinkedTable` — 继续链式 link。

## 排错

### 连接条件里 “Column not found”

```
TypeError: Invalid join condition: Column 'id' not found in schema
```

条件引用了对应表中不存在的列。检查 `orders.columns` / `products.columns` 并修正
lambda。

### “Invalid join_type”

只能用 `"inner"`、`"left"`、`"right"`、`"full"`（不是 `"outer"`）。

### joined 列名不符合预期

目标列都带别名前缀。检查 joined schema：

```python
print(linked.to_ltseq().columns)   # id, ..., prod_name, prod_price, ...
```

### 内存（`from_pandas`）源 + 列投影

在内存源（`from_pandas` / `from_arrow`）之上 join 后再投影列的子集，可能触发
DataFusion 的 ProjectionPushdown bug（普通 `join().select(...)` 同样会）。遇到时
改用 CSV/Parquet 读入，或在 `collect()` 之后再 select。

## 最佳实践

1. **交给优化器下推**——谓词与投影下推是查询优化器的职责；写你真正想要的变换，它会
   在 joined 计划上执行。不再有“先过滤源表提速”的捷径（它对不匹配/扇出 join 会给出
   错误的行）。
2. **晚物化**——在 `to_ltseq()` 的惰性 `LTSeq` 上继续组合，只在需要把结果缓存到内存
   时才 `collect()`。
3. **选对 join 类型**——只要匹配用 INNER，要保留全部源行用 LEFT 等。join 类型烧进
   计划，会在所有下游变换中保留。
4. **用前缀引用 linked 列**——`r.prod_price`，而不是嵌套的 `r.prod.price`。

## 小结

`LTSeq.link()` 提供：
- 惰性、前缀别名的等值连接
- 四种 join 类型（INNER、LEFT、RIGHT、FULL）与复合键
- 返回普通 `LTSeq` 的变换，其行数遵循 join
- 多跳链式，每跳建立在上一跳 join 的真实计划之上
- `to_ltseq()`（惰性）与 `collect()`（执行）离开 LinkedTable 表面
