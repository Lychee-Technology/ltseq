# LTSeq 用户心智模型

相关文档：

- `docs/README.md`：文档索引
- `docs/USER_MODEL.md`：英文用户模型
- `docs/ARCHITECTURE.cn.md`：系统架构与执行模型
- `docs/api.cn.md`：中文 API 文档
- `docs/LINKING_GUIDE.cn.md`：中文 Linking 指南

## LTSeq 是什么

LTSeq 是一个面向有序数据的 Python 数据处理库。它适合行顺序本身带有业务含义的场景，而不是把顺序当成展示问题。

典型场景包括：

- 时间序列分析
- 事件流处理
- 连续段和 streak 检测
- 顺序分组
- 最近时刻 join
- 跨表惰性关系导航

LTSeq 提供 Python API，但大多数真正的执行发生在 Rust/DataFusion 引擎中。

---

## LTSeq 不是什么

LTSeq 无意做 pandas 或通用 SQL 表的完全替代品。

区别在于：多数 DataFrame 系统默认把表看作无序集合，排序只在展示前考虑；LTSeq 把顺序当作计算的输入。

所以某些 API 会要求你先排序，排序元数据会被跟踪，顺序未知时某些操作直接报错。

---

## 核心心智模型

四点基本够用。

### 1. 表是不可变的查询对象

大多数操作都会返回一个新对象，而不是原地修改原对象。

```python
t1 = LTSeq.read_csv("orders.csv")
t2 = t1.filter(lambda r: r.amount > 100)
```

这里 `t1` 仍然是原始表，`t2` 是基于它派生出的新查询。

### 2. 在你请求结果之前，大多数工作都是懒执行的

`filter`、`select`、`derive`、`sort` 这类操作通常只是构造查询计划，而不是立刻算出数据。

通常以下操作会真正触发执行：

- `show()`
- `count()`
- `collect()`
- `to_arrow()`
- `to_pandas()`
- `write_csv()`
- `write_parquet()`

### 3. 顺序是重要语义

如果一个操作依赖前后行关系，LTSeq 期望你显式建立这个顺序。

```python
prices = LTSeq.read_csv("prices.csv").sort("timestamp")
result = prices.derive(prev=lambda r: r.price.shift(1))
```

这里的 `sort()` 不是装饰，而是查询语义的一部分。

### 4. lambda 是在描述工作，不是在 Python 中逐行执行

当你写：

```python
t.filter(lambda r: r.age > 18)
```

这个 lambda 会被捕获成表达式，并交给 Rust 引擎执行。`r` 不是一个真实的 Python 行对象。

---

## LTSeq 的典型工作流

大多数 LTSeq 工作流都会遵循类似步骤。

### 第 1 步：加载数据

```python
t = LTSeq.read_csv("events.csv")
```

### 第 2 步：如果后续逻辑依赖顺序，先建立排序

```python
t = t.sort("user_id", "event_time")
```

### 第 3 步：以懒执行方式做变换

```python
t = t.filter(lambda r: r.event_type == "click")
t = t.derive(next_gap=lambda r: r.event_time.diff(1))
```

### 第 4 步：在需要时进入更高层语义对象

例如：

- `group_ordered()`：连续分组逻辑
- `link()`：惰性关系导航
- `partition()`：按分区访问

### 第 5 步：在真正需要结果时再物化

```python
t.show()
rows = t.to_dicts()
```

---

## 为什么排序必须显式写出

很多 LTSeq 用户第一次接触这个库时，会看到类似错误：

```text
window function used without sort
```

这个错误是故意设计的。

`shift`、`diff`、`rolling` 以及很多 ranking 模式如果没有已知顺序，语义就是不完整的。LTSeq 不去猜，而是要求查询把顺序假设写出来。结果因此可信，日后读这段查询的人也能一眼看出它假设了什么。

如果结果依赖顺序，那么顺序本身就应该出现在代码里。

---

## 如何理解不同对象类型

大多数工作都从 `LTSeq` 开始，但当查询进入更丰富的语义上下文时，会出现其他对象。

### `LTSeq`

适用于普通表变换、join、序列表达式和结果导出。

### `NestedTable`

由 `group_ordered()` 或 `group_sorted()` 返回。

当你想在保留组内行的前提下处理组、而不是立刻把每组压缩成一行时，用它。

### `LinkedTable`

由 `link()` 返回。

当你想用前缀别名列（`{alias}_{col}`）从另一张表补全当前表、并在结果被消费之前保持惰性时，用它。join 计划惰性构建，所有变换都在 join 后的计划上运行。可以理解成“这张被另一张表补全过的表，仍然是惰性的”。

### `PartitionedTable`

由 `partition()` 返回。

当你想按分区键访问数据，而不是永远围绕单个全局平面表工作时，应使用它。

---

## Linking 与 Joining 的区别

两者构建的是同一种惰性 DataFusion join，且都在结果被消费之前不物化。区别在于命名约定与人体工学。

### Join

`join()` 直接返回普通 `LTSeq`，采用 Polars 式命名：只有冲突的右表列才加后缀（`suffix="_right"`），不冲突的列保留原名。

适用于：

- 一次性的关系 join
- 下游步骤把结果当成一个普通平面表继续处理

### Link

`link()` 返回 `LinkedTable`，把*整个*目标表命名空间化为 `{alias}_{col}`。

适用于：

- 从维表补全事实表
- 多跳链式关联、需要无歧义列名
- 你想要一种更接近“关系导航”的工作流

`LinkedTable` 上的每个变换都在 join 后的计划上运行并返回普通 `LTSeq`，因此行数跟随 join（未匹配行与一对多扇出都会体现）。详见 Linking 指南。

---

## 分组模型

传统 SQL 分组会立即把行压缩掉。LTSeq 也支持这种风格，同时保留组内顺序上下文。

使用 `group_ordered()` 时，你想问的往往不是“这个 key 的聚合值是什么”，而是组内的行本身：连续组里有哪些行、每段 run 的第一行和最后一行是什么、哪些组满足基于组内顺序的条件。

所以这类场景返回 `NestedTable`，而不是立刻给你一个扁平聚合结果。

---

## 表达式模型

表达式是用 Python lambda 写出来的，但这些 lambda 是声明式的，不是逐行执行式的。

好的例子：

```python
t.filter(lambda r: r.amount > 100)
t.derive(total=lambda r: r.price * r.quantity)
t.select(lambda r: [r.id, r.total])
```

lambda 应该描述一个由列和表达式组成的计算，而不是写依赖真实行值的 Python 控制流。

---

## 物化模型

LTSeq 会避免在表到表的工作流中发生意外物化。从用户角度看，链式表操作能更长时间保持高效，导出到 pandas 或 Arrow 是一个明确边界；`show()` 常用于查看数据，但本质上仍是终结操作。

调用 `to_pandas()` 或 `collect()`，就是要求 LTSeq 离开懒查询世界、产出真实数据。这通常正是你要的，但成本模型也随之改变。
