# LTSeq API 设计文档

LTSeq 是面向有序序列的 Python 数据处理库，底层由 Rust/DataFusion 执行。与传统 DataFrame 不同，LTSeq 强调"顺序"语义，支持窗口、连续分组、游标式处理等 SPL 风格能力。

## 常见错误与解决方案

| 错误 | 原因 | 解决方法 |
|------|------|---------|
| `RuntimeError: window function used without sort` | 未排序直接调用 `shift`/`rolling`/`diff` | 在窗口函数前添加 `.sort(order_column)` |
| `AttributeError: column 'xxx' not found` | 列名拼写错误或列不存在 | 通过 `t.columns` 查看可用列名 |
| `ValueError: schema mismatch` | union/intersect 的表 schema 不匹配 | 确保两表列名和类型相同 |
| `ValueError: tables not sorted by join keys` | 对未排序的表调用 `join_sorted` | 先对双方调用 `.sort(join_key)` |
| `TypeError: predicate not boolean Expr` | filter lambda 返回非布尔值 | 确保谓词使用比较运算符（`>`、`==` 等）|
| `ValueError: desc length mismatch` | `desc` 列表长度与排序键数量不匹配 | 为每个排序键提供一个布尔值，或使用单个布尔值 |

## 快速参考

### 数据加载与输出
| 操作 | 方法 | 示例 |
|------|------|------|
| 加载 CSV | `LTSeq.read_csv()` | `t = LTSeq.read_csv("data.csv")` |
| 查看列名 | `.columns` | `print(t.columns)` |
| 流式输出 | `.to_cursor()` | `for batch in t.to_cursor(): ...` |
| 物化所有行 | `.collect()` | `rows = t.collect()` |
| 转 pandas | `.to_pandas()` | `df = t.to_pandas()` |
| 行数统计 | `.count()` | `n = t.count()` |

### 基础操作
| 操作 | 方法 | 示例 |
|------|------|------|
| 过滤行 | `.filter()` | `t.filter(lambda r: r.age > 18)` |
| 投影列 | `.select()` | `t.select("id", "name")` |
| 新增列 | `.derive()` | `t.derive(total=lambda r: r.a + r.b)` |
| 排序 | `.sort()` | `t.sort("date", desc=True)` |
| 去重 | `.distinct()` | `t.distinct("id")` |
| 截取行 | `.slice()` | `t.slice(offset=10, length=5)` |
| 取前 N 行 | `.head()` | `t.head(10)` |
| 取后 N 行 | `.tail()` | `t.tail(10)` |

### 窗口操作（需先调用 `.sort()`）
| 操作 | 方法 | 示例 |
|------|------|------|
| 前一行 | `.shift(n)` | `r.price.shift(1)` |
| 滑动聚合 | `.rolling(n).agg()` | `r.price.rolling(5).mean()` |
| 行差分 | `.diff(n)` | `r.price.diff(1)` |
| 累计求和 | `.cum_sum()` | `t.cum_sum("volume")` |

### 排名（使用 `.over()`）
| 操作 | 方法 | 示例 |
|------|------|------|
| 行编号 | `row_number()` | `row_number().over(order_by=r.date)` |
| 跳号排名 | `rank()` | `rank().over(order_by=r.score)` |
| 密集排名 | `dense_rank()` | `dense_rank().over(order_by=r.score)` |
| 分桶 | `ntile(n)` | `ntile(4).over(order_by=r.value)` |

### 聚合
| 操作 | 方法 | 示例 |
|------|------|------|
| 分组聚合 | `.agg()` | `t.agg(by=lambda r: r.region, total=lambda r: r.sales.sum())` |
| 分区 | `.partition()` | `parts = t.partition("region")` |
| 透视 | `.pivot()` | `t.pivot(index="date", columns="region", values="amount")` |

### 连接
| 操作 | 方法 | 示例 |
|------|------|------|
| 哈希连接 | `.join()` | `a.join(b, on=lambda a, b: a.id == b.id)` |
| 归并连接 | `.join_sorted()` | `a.sort("id").join_sorted(b.sort("id"), on="id")` |
| 半连接 | `.semi_join()` | `a.semi_join(b, on=lambda a, b: a.id == b.id)` |
| 反连接 | `.anti_join()` | `a.anti_join(b, on=lambda a, b: a.id == b.id)` |

### 集合操作
| 操作 | 方法 | 示例 |
|------|------|------|
| 纵向合并 | `.union()` | `t1.union(t2)` |
| 交集 | `.intersect()` | `t1.intersect(t2)` |
| 差集 | `.except_()` | `t1.except_(t2)` |

## 0. 基本约定与术语

- t: 当前 LTSeq 实例（不可变，所有操作返回新实例）
- r: 行代理（lambda 内部用于构造表达式，不在 Python 侧执行）
- g: 组代理（NestedTable 的 filter/derive 中使用）
- 绝大多数操作返回新的 LTSeq；`to_cursor` 返回迭代器；`is_subset` 返回布尔值
- 窗口/有序相关能力依赖已有排序（`sort`），未排序使用会导致运行期错误或不正确结果
- 表达式在 Python 侧被捕获为 AST 并在 Rust/DataFusion 层执行

## 1. 输入 / 输出

### `LTSeq.read_csv`
- **签名**: `LTSeq.read_csv(path: str, has_header: bool = True) -> LTSeq`
- **行为**: 从 CSV 加载数据并推断 schema，返回可链式计算的 LTSeq
- **参数**: `path` CSV 路径；`has_header` 是否把首行作为列名
- **返回**: `LTSeq`，包含加载后的数据与 schema
- **异常**: `FileNotFoundError`/`IOError`（路径无效或不可读），`ValueError`（解析失败或 schema 推断失败）
- **示例**:
```python
from ltseq import LTSeq

t = LTSeq.read_csv("data.csv")
```

### `LTSeq.schema`（属性）
- **签名**: `LTSeq.schema -> Schema`
- **行为**: 返回表的 schema，包含列名和类型信息
- **参数**: 无（属性）
- **返回**: `Schema` 对象，包含 `.names` 和 `.types` 属性
- **异常**: 无
- **示例**:
```python
t = LTSeq.read_csv("data.csv")
print(t.schema.names)   # ["id", "name", "age", "created_at"]
print(t.schema.types)   # [Int64, Utf8, Int64, Timestamp]

for name, dtype in zip(t.schema.names, t.schema.types):
    print(f"{name}: {dtype}")
```

### `LTSeq.columns`（属性）
- **签名**: `LTSeq.columns -> List[str]`
- **行为**: 返回列名列表（`schema.names` 的快捷方式）
- **参数**: 无（属性）
- **返回**: 列名字符串列表
- **异常**: 无
- **示例**:
```python
print(t.columns)  # ["id", "name", "age"]
```

### `LTSeq.to_cursor`
- **签名**: `LTSeq.to_cursor(chunk_size: int = 10000) -> Iterator[Record]`
- **行为**: 将结果以游标方式流式输出，避免一次性物化
- **参数**: `chunk_size` 每批次行数
- **返回**: 记录迭代器
- **异常**: `ValueError`（chunk_size 非法），`RuntimeError`（底层不支持流式或执行失败）
- **示例**:
```python
for batch in t.to_cursor(chunk_size=5000):
    print(batch)
```

### `LTSeq.collect`
- **签名**: `LTSeq.collect() -> List[Dict[str, Any]]`
- **行为**: 将所有行物化为字典列表
- **参数**: 无
- **返回**: 行字典列表
- **异常**: `MemoryError`（数据集过大），`RuntimeError`（执行失败）
- **示例**:
```python
rows = t.filter(lambda r: r.age > 18).collect()
for row in rows:
    print(row["name"])
```

### `LTSeq.to_pandas`
- **签名**: `LTSeq.to_pandas() -> pandas.DataFrame`
- **行为**: 转为 pandas DataFrame，便于互操作
- **参数**: 无
- **返回**: `pandas.DataFrame`
- **异常**: `ImportError`（未安装 pandas），`MemoryError`（数据集过大）
- **示例**:
```python
df = t.to_pandas()
df.plot(x="date", y="price")
```

### `LTSeq.count`
- **签名**: `LTSeq.count() -> int`
- **行为**: 返回行数
- **参数**: 无
- **返回**: 整数行数
- **异常**: `RuntimeError`（执行失败）
- **示例**:
```python
n = t.filter(lambda r: r.status == "active").count()
```

### `LTSeq.head`
- **签名**: `LTSeq.head(n: int = 10) -> LTSeq`
- **行为**: 返回前 n 行
- **参数**: `n` 行数（默认 10）
- **返回**: 包含前 n 行的 `LTSeq`
- **异常**: `ValueError`（n < 0）
- **示例**:
```python
top_10 = t.sort("score", desc=True).head(10)
```

### `LTSeq.tail`
- **签名**: `LTSeq.tail(n: int = 10) -> LTSeq`
- **行为**: 返回后 n 行
- **参数**: `n` 行数（默认 10）
- **返回**: 包含后 n 行的 `LTSeq`
- **异常**: `ValueError`（n < 0）
- **示例**:
```python
recent = t.sort("date").tail(5)
```

## 2. 基础关系操作

### `LTSeq.filter`
- **签名**: `LTSeq.filter(predicate: Callable[[Row], Expr]) -> LTSeq`
- **行为**: 过滤满足条件的行，谓词尽量下推到 Rust 引擎
- **参数**: `predicate` 行谓词表达式（返回布尔表达式）
- **返回**: 过滤后的新 `LTSeq`
- **SPL 对应**: `select()`
- **异常**: `ValueError`（schema 未初始化），`TypeError`（谓词不是布尔表达式），`AttributeError`（列不存在）
- **示例**:
```python
filtered = t.filter(lambda r: r.amount > 100)
```

### `LTSeq.select`
- **签名**: `LTSeq.select(*cols: Union[str, Callable]) -> LTSeq`
- **行为**: 投影指定列或表达式，支持列裁剪
- **参数**: `cols` 列名或 lambda（可返回单列或列表）
- **返回**: 投影后的新 `LTSeq`
- **SPL 对应**: `new()`
- **异常**: `ValueError`（schema 未初始化），`TypeError`（返回非表达式/列表），`AttributeError`（列不存在）
- **示例**:
```python
t.select("id", "name")
# 或
t.select(lambda r: [r.id, r.name])
```

### `LTSeq.derive`
- **签名**: `LTSeq.derive(**new_cols: Callable) -> LTSeq` 或 `LTSeq.derive(func: Callable[[Row], Dict[str, Expr]]) -> LTSeq`
- **行为**: 新增或覆盖列；保留原有列
- **参数**: `new_cols` 列名 -> lambda 映射；或返回字典的 lambda
- **返回**: 新 `LTSeq`（包含派生列）
- **SPL 对应**: `derive()`
- **异常**: `ValueError`（schema 未初始化），`TypeError`（返回非表达式/字典），`AttributeError`（列不存在）
- **示例**:
```python
# 方式 1
with_tax = t.derive(tax=lambda r: r.price * 0.1)
# 方式 2
with_tax = t.derive(lambda r: {"tax": r.price * 0.1})
```

### `LTSeq.sort`
- **签名**: `LTSeq.sort(*keys: Union[str, Callable], desc: Union[bool, list] = False) -> LTSeq`
- **行为**: 按一个或多个键排序；窗口/有序计算必须先排序。同时更新 `sort_keys` 属性以追踪排序状态。
- **参数**: `keys` 列名或表达式；`desc`（或 `descending`）全局或逐列降序标记
- **返回**: 排序后的新 `LTSeq`（含追踪的排序键）
- **SPL 对应**: `sort()`
- **异常**: `ValueError`（schema 未初始化或 desc 长度不匹配），`TypeError`（键类型非法），`AttributeError`（列不存在）
- **示例**:
```python
t_sorted = t.sort("date", "id", desc=[False, True])
print(t_sorted.sort_keys)  # [("date", False), ("id", True)]
```

### `LTSeq.sort_keys`（属性）
- **签名**: `LTSeq.sort_keys -> Optional[List[Tuple[str, bool]]]`
- **行为**: 返回当前排序键列表（列名, 是否降序），未排序时返回 None
- **参数**: 无（属性）
- **返回**: `Optional[List[Tuple[str, bool]]]`
- **异常**: 无
- **示例**:
```python
t = LTSeq.read_csv("data.csv")
print(t.sort_keys)         # None（未知排序顺序）

t_sorted = t.sort("date", "id")
print(t_sorted.sort_keys)  # [("date", False), ("id", False)]
```

### `LTSeq.is_sorted_by`
- **签名**: `LTSeq.is_sorted_by(*keys: str, desc: Union[bool, List[bool]] = False) -> bool`
- **行为**: 检查表是否已按给定键排序（前缀匹配）
- **参数**: `keys` 要检查的列名；`desc` 期望的降序标记
- **返回**: `bool`
- **异常**: `ValueError`（未提供键或 desc 长度不匹配）
- **示例**:
```python
t_sorted = t.sort("a", "b", "c")
t_sorted.is_sorted_by("a")           # True（前缀匹配）
t_sorted.is_sorted_by("a", "b")      # True
t_sorted.is_sorted_by("b")           # False（非前缀）
t_sorted.is_sorted_by("a", desc=True)  # False（方向不匹配）
```

### `LTSeq.distinct`
- **签名**: `LTSeq.distinct(*keys: Union[str, Callable]) -> LTSeq`
- **行为**: 去重；未指定 keys 时按所有列去重
- **参数**: `keys` 去重键（列名或表达式）
- **返回**: 去重后的新 `LTSeq`
- **SPL 对应**: `id()`
- **异常**: `ValueError`（schema 未初始化），`TypeError`（键类型非法），`AttributeError`（列不存在）
- **示例**:
```python
unique = t.distinct("customer_id")
```

### `LTSeq.slice`
- **签名**: `LTSeq.slice(offset: int = 0, length: Optional[int] = None) -> LTSeq`
- **行为**: 选择连续行区间，零拷贝语义
- **参数**: `offset` 起始行（0 基）；`length` 行数（None 表示直到末尾）
- **返回**: 截取后的新 `LTSeq`
- **SPL 对应**: `T([start,end])`
- **异常**: `ValueError`（offset/length 为负），`ValueError`（schema 未初始化）
- **示例**:
```python
t.slice(offset=10, length=5)
```

## 3. 有序与窗口函数

### 3.1 行级窗口操作

> 这些操作要求在调用前使用 `sort` 建立顺序。

#### `r.col.shift`
- **签名**: `r.col.shift(offset: int) -> Expr`
- **行为**: 访问相对行的值。正偏移量表示向前（历史行），负偏移量表示向后（未来行），与 pandas `Series.shift()` 一致。
- **参数**: `offset` 行偏移量（正 = 向前，负 = 向后）
- **返回**: 表达式（边界处为 NULL）
- **SPL 对应**: `col[-1]`
- **异常**: `TypeError`（offset 非 int），`RuntimeError`（未排序使用）
- **示例**:
```python
with_prev = t.sort("date").derive(prev=lambda r: r.close.shift(1))
```

#### `r.col.rolling`
- **签名**: `r.col.rolling(window_size: int).agg_func() -> Expr`
- **行为**: 滑动窗口聚合；常用聚合包括 `mean/sum/min/max/std`
- **参数**: `window_size` 窗口大小
- **返回**: 窗口聚合表达式
- **SPL 对应**: `col{-1,1}`
- **异常**: `ValueError`（window_size <= 0），`RuntimeError`（未排序使用）
- **示例**:
```python
ma5 = t.sort("date").derive(ma_5=lambda r: r.close.rolling(5).mean())
```

#### `r.col.diff`
- **签名**: `r.col.diff(offset: int = 1) -> Expr`
- **行为**: 差分计算，等价于 `r.col - r.col.shift(offset)`
- **参数**: `offset` 行偏移量
- **返回**: 差分表达式
- **SPL 对应**: `col - col[-1]`
- **异常**: `TypeError`（非数值列或 offset 非 int），`RuntimeError`（未排序使用）
- **示例**:
```python
changes = t.sort("date").derive(daily=lambda r: r.close.diff())
```

### 3.2 表级累积操作

#### `LTSeq.cum_sum`
- **签名**: `LTSeq.cum_sum(*cols: Union[str, Callable]) -> LTSeq`
- **行为**: 对指定列做累计求和，并追加 `*_cumsum` 列
- **参数**: `cols` 列名或表达式
- **返回**: 包含累计列的新 `LTSeq`
- **SPL 对应**: `cum(col)`
- **异常**: `ValueError`（未提供列或 schema 未初始化），`TypeError`（非数值列）
- **示例**:
```python
with_cum = t.sort("date").cum_sum("volume", "amount")
```

### 3.3 排名函数

> 排名函数通过 `.over()` 指定排序，**不需要**提前调用 `.sort()`。

#### `row_number`
- **签名**: `row_number().over(partition_by=None, order_by=Expr, descending=False) -> Expr`
- **行为**: 在每个分区内从 1 开始连续编号，并列时也保持唯一值
- **参数**: 通过 `.over()` 指定 `partition_by`（可选）、`order_by`（必填）、`descending`（可选）
- **返回**: 整数表达式
- **异常**: `RuntimeError`（未指定 order_by）
- **示例**:
```python
from ltseq.expr import row_number
t.derive(rn=lambda r: row_number().over(partition_by=r.region, order_by=r.date))
```

#### `rank`
- **签名**: `rank().over(partition_by=None, order_by=Expr, descending=False) -> Expr`
- **行为**: 并列时共享同一排名，后续排名跳过（1,1,3,...）
- **返回**: 整数表达式
- **示例**:
```python
from ltseq.expr import rank
t.derive(rk=lambda r: rank().over(order_by=r.score, descending=True))
```

#### `dense_rank`
- **签名**: `dense_rank().over(partition_by=None, order_by=Expr, descending=False) -> Expr`
- **行为**: 并列时共享同一排名，后续排名不跳过（1,1,2,...）
- **返回**: 整数表达式
- **示例**:
```python
from ltseq.expr import dense_rank
t.derive(dr=lambda r: dense_rank().over(order_by=r.score, descending=True))
```

#### `ntile`
- **签名**: `ntile(n: int).over(partition_by=None, order_by=Expr, descending=False) -> Expr`
- **行为**: 将行分为 n 个尽量等大的桶，返回桶号（1 ~ n）
- **参数**: `n` 桶数
- **返回**: 整数表达式（1 ~ n）
- **示例**:
```python
from ltseq.expr import ntile
t.derive(quartile=lambda r: ntile(4).over(order_by=r.value))
```

#### `.over()` 窗口说明
- **签名**: `expr.over(partition_by=None, order_by=None, descending=False) -> WindowExpr`
- **行为**: 为排名函数附加窗口规范
- **参数**:
  - `partition_by`: 分区列（单个 Expr 或列表）
  - `order_by`: 排序列（单个 Expr 或列表）
  - `descending`: 降序标记（单个 bool 或逐列列表）
- **示例**:
```python
# 单列分区
t.derive(rn=lambda r: row_number().over(partition_by=r.region, order_by=r.date))

# 多列分区
t.derive(rn=lambda r: row_number().over(
    partition_by=[r.region, r.category],
    order_by=[r.date, r.id],
    descending=[True, False]
))
```

### 3.4 有序搜索与对齐

#### `LTSeq.search_first`
- **签名**: `LTSeq.search_first(predicate: Callable[[Row], Expr]) -> LTSeq`
- **行为**: 返回首个匹配行（单行 LTSeq）；在有序数据上可实现二分搜索优化
- **参数**: `predicate` 行谓词
- **返回**: 单行 `LTSeq`（未找到则为空表）
- **SPL 对应**: `pselect`
- **异常**: `ValueError`（schema 未初始化），`TypeError`（谓词非法），`RuntimeError`（执行失败）
- **示例**:
```python
first_big = t.sort("price").search_first(lambda r: r.price > 100)
```

#### `LTSeq.align`
- **签名**: `LTSeq.align(ref_sequence: List[Any], key: Callable[[Row], Expr]) -> LTSeq`
- **行为**: 将序列对齐到 `ref_sequence` 的顺序，缺失键以 NULL 填充
- **参数**: `ref_sequence` 参考键序列；`key` 从行中提取对齐键
- **返回**: 对齐后的新 `LTSeq`
- **SPL 对应**: `align`
- **异常**: `TypeError`（key 非法），`ValueError`（ref_sequence 为空）
- **示例**:
```python
aligned = t.align(["2024-01-01", "2024-01-02"], key=lambda r: r.date)
```

## 4. 有序分组与过程计算

### `LTSeq.group_ordered`
- **签名**: `LTSeq.group_ordered(key: Callable[[Row], Expr]) -> NestedTable`
- **行为**: 仅按"连续相同值"分组，不会重新排序
- **参数**: `key` 分组键表达式
- **返回**: `NestedTable`（支持组级操作）
- **SPL 对应**: `group@o`
- **异常**: `ValueError`（schema 未初始化），`TypeError`（key 非法），`AttributeError`（列不存在）
- **示例**:
```python
groups = t.sort("date").group_ordered(lambda r: r.is_up)
```

### `LTSeq.group_sorted`
- **签名**: `LTSeq.group_sorted(key: Callable[[Row], Expr]) -> NestedTable`
- **行为**: 假设全局已按 key 排序，单次扫描完成分组（避免哈希）
- **参数**: `key` 分组键表达式
- **返回**: `NestedTable`
- **SPL 对应**: `groups@o`
- **异常**: `ValueError`（未排序或 schema 未初始化），`TypeError`（key 非法）
- **示例**:
```python
groups = t.sort("user_id").group_sorted(lambda r: r.user_id)
```

### `LTSeq.scan`
- **签名**: `LTSeq.scan(func: Callable[[State, Row], State], init: Any) -> LTSeq`
- **行为**: 按当前顺序进行状态累积，输出每行对应的累计状态
- **参数**: `func` 状态转移函数；`init` 初始状态
- **返回**: `LTSeq`（通常为单列状态序列或追加状态列）
- **SPL 对应**: `iterate`
- **异常**: `TypeError`（func 非法），`RuntimeError`（执行失败）
- **示例**:
```python
# 复利累积
rates = t.sort("date").scan(lambda s, r: s * (1 + r.rate), init=1.0)
```

### `NestedTable`（来自 `group_ordered` / `group_sorted`）

#### `NestedTable.first`
- **签名**: `nested.first() -> LTSeq`
- **行为**: 取每组首行
- **示例**:
```python
first_rows = groups.first()
```

#### `NestedTable.last`
- **签名**: `nested.last() -> LTSeq`
- **行为**: 取每组末行
- **示例**:
```python
last_rows = groups.last()
```

#### `NestedTable.count`
- **签名**: `nested.count() -> LTSeq`
- **行为**: 返回每组的行数
- **示例**:
```python
sizes = groups.count()
```

#### `NestedTable.flatten`
- **签名**: `nested.flatten() -> LTSeq`
- **行为**: 将嵌套表展开为普通表，附带 `__group_id__`
- **示例**:
```python
flat = groups.flatten()
```

#### `NestedTable.filter`
- **签名**: `nested.filter(predicate: Callable[[GroupProxy], Expr]) -> NestedTable`
- **行为**: 按组级条件过滤组
- **示例**:
```python
a = groups.filter(lambda g: g.count() > 3)
```

#### `NestedTable.derive`
- **签名**: `nested.derive(func: Callable[[GroupProxy], Dict[str, Expr]]) -> LTSeq`
- **行为**: 生成组级派生列
- **示例**:
```python
spans = groups.derive(lambda g: {"start": g.first().date, "end": g.last().date})
```

### `GroupProxy` 聚合方法

#### 基础聚合（列访问风格）
- **签名**: `g.col.sum()`, `g.col.avg()`, `g.col.min()`, `g.col.max()`
- **行为**: 在组范围内对指定列做基础聚合
- **示例**:
```python
groups.derive(lambda g: {"avg": g.price.avg(), "hi": g.price.max()})
```

#### `GroupProxy.variance` / `GroupProxy.std`
- **签名**: `g.variance(column: str) -> Expr` / `g.std(column: str) -> Expr`
- **行为**: 组内方差 / 标准差
- **示例**:
```python
groups.derive(lambda g: {"vol": g.variance("price")})
```

#### `GroupProxy.median`
- **签名**: `g.median(column: str) -> Expr`
- **行为**: 组内中位数
- **示例**:
```python
groups.derive(lambda g: {"mid": g.median("price")})
```

#### `GroupProxy.percentile`
- **签名**: `g.percentile(column: str, p: float) -> Expr`
- **行为**: 组内百分位数（0 ≤ p ≤ 1）
- **示例**:
```python
groups.derive(lambda g: {"p95": g.percentile("response_ms", 0.95)})
```

#### `GroupProxy.mode`
- **签名**: `g.mode(column: str) -> Expr`
- **行为**: 组内众数（出现频率最高的值）
- **示例**:
```python
groups.derive(lambda g: {"common_category": g.mode("category")})
```

#### `GroupProxy.top_k`
- **签名**: `g.top_k(column: str, k: int) -> List[Any]`
- **行为**: 组内 Top-K 值列表（降序排列）
- **参数**: `column` 列名；`k` 个数
- **示例**:
```python
groups.derive(lambda g: {"top_3_prices": g.top_k("price", 3)})
```

#### `GroupProxy.all` / `GroupProxy.any` / `GroupProxy.none`
- **签名**: `g.all(predicate)` / `g.any(predicate)` / `g.none(predicate)`
- **行为**: 检查组内所有/任意/无行满足谓词
- **示例**:
```python
groups.filter(lambda g: g.all(lambda r: r.amount > 0))
groups.filter(lambda g: g.any(lambda r: r.status == "error"))
groups.filter(lambda g: g.none(lambda r: r.is_deleted == True))
```

#### `GroupProxy.count_if` / `GroupProxy.sum_if` / `GroupProxy.avg_if` / `GroupProxy.min_if` / `GroupProxy.max_if`
- **签名**: `g.count_if(predicate)` / `g.sum_if(predicate, column)` / 等
- **行为**: 组内满足谓词的行的条件聚合
- **示例**:
```python
groups.derive(lambda g: {"high_count": g.count_if(lambda r: r.price > 100)})
groups.derive(lambda g: {"vip_sales": g.sum_if(lambda r: r.is_vip, "amount")})
```

## 5. 集合代数

### `LTSeq.union`
- **签名**: `LTSeq.union(other: LTSeq) -> LTSeq`
- **行为**: 纵向拼接（类似 SQL UNION ALL）
- **参数**: `other` 另一个同 schema 的 LTSeq
- **返回**: 合并后的 `LTSeq`
- **SPL 对应**: `A & B`
- **异常**: `TypeError`（other 非 LTSeq），`ValueError`（schema 不匹配）
- **示例**:
```python
combined = t1.union(t2)
```

### `LTSeq.intersect`
- **签名**: `LTSeq.intersect(other: LTSeq, on: Optional[Callable] = None) -> LTSeq`
- **行为**: 返回两表交集
- **参数**: `other` 另一表；`on` 指定比较键（None 表示全列）
- **返回**: 交集 `LTSeq`
- **SPL 对应**: `A ^ B`
- **异常**: `TypeError`（other 非 LTSeq 或 on 非法），`ValueError`（schema 未初始化）
- **示例**:
```python
common = t1.intersect(t2, on=lambda r: r.id)
```

### `LTSeq.except_`（集合差）
- **签名**: `LTSeq.except_(other: LTSeq, on: Optional[Callable] = None) -> LTSeq`
- **行为**: 返回左表中不存在于右表的行（SQL EXCEPT 语义）
- **参数**: `other` 另一表；`on` 指定比较键
- **返回**: 差集 `LTSeq`
- **SQL 对应**: `EXCEPT` / `MINUS`
- **SPL 对应**: `A \ B`
- **异常**: `TypeError`（other 非 LTSeq 或 on 非法），`ValueError`（schema 未初始化）
- **示例**:
```python
only_left = t1.except_(t2, on=lambda r: r.id)
```

> ⚠️ `LTSeq.diff()` 是 `except_()` 的废弃别名。注意：`Expr.diff()`（第 3 节中的行级差分）是不同的操作。

### `LTSeq.is_subset`
- **签名**: `LTSeq.is_subset(other: LTSeq, on: Optional[Callable] = None) -> bool`
- **行为**: 判断当前表是否为另一表的子集
- **参数**: `other` 另一表；`on` 指定比较键
- **返回**: `bool`
- **异常**: `TypeError`（other 非 LTSeq 或 on 非法），`ValueError`（schema 未初始化）
- **示例**:
```python
flag = t_small.is_subset(t_big, on=lambda r: r.id)
```

## 6. 关联与连接

### `LTSeq.join`
- **签名**: `LTSeq.join(other: LTSeq, on: Callable, how: str = "inner") -> LTSeq`
- **行为**: 标准哈希连接，不要求排序
- **参数**: `other` 另一表；`on` 连接条件；`how` in {inner,left,right,full}
- **返回**: 连接后的 `LTSeq`（冲突列名会加后缀）
- **SPL 对应**: `join`
- **异常**: `TypeError`（other/on 非法），`ValueError`（how 非法或 schema 未初始化）
- **示例**:
```python
joined = users.join(orders, on=lambda u, o: u.id == o.user_id, how="left")
```

### `LTSeq.join_merge`
- **签名**: `LTSeq.join_merge(other: LTSeq, on: Callable, join_type: str = "inner") -> LTSeq`
- **行为**: 归并连接，要求双方按连接键排序，复杂度 O(N+M)
- **参数**: `other` 另一表；`on` 连接条件；`join_type` in {inner,left,right,full}
- **返回**: 连接后的 `LTSeq`
- **SPL 对应**: `join@m`
- **异常**: `TypeError`（other/on 非法），`ValueError`（join_type 非法或未排序）
- **示例**:
```python
result = t1.sort("id").join_merge(t2.sort("id"), on=lambda a, b: a.id == b.id)
```

### `LTSeq.join_sorted`
- **签名**: `LTSeq.join_sorted(other: LTSeq, on: Union[str, List[str]], how: str = "inner") -> LTSeq`
- **行为**: 归并连接，严格验证双方按连接键排序后执行；通过 `is_sorted_by()` 验证排序状态
- **参数**: `other` 已排序的另一表；`on` 连接列名（或列名列表）；`how` in {inner,left,right,full}
- **返回**: 连接后的 `LTSeq`
- **SPL 对应**: `joinx`
- **异常**: `TypeError`（other/on 非法），`ValueError`（表未按连接键排序）
- **示例**:
```python
result = t1.sort("id").join_sorted(t2.sort("id"), on="id")
# 复合键
result = t1.sort("region", "date").join_sorted(t2.sort("region", "date"), on=["region", "date"])
```

### `LTSeq.semi_join`
- **签名**: `LTSeq.semi_join(other: LTSeq, on: Callable) -> LTSeq`
- **行为**: 返回左表中在右表有匹配的行（仅返回左表列，无重复）
- **参数**: `other` 用于匹配的右表；`on` 连接条件 lambda
- **返回**: 匹配行的 `LTSeq`（仅含左表列）
- **SQL 对应**: `WHERE EXISTS`
- **异常**: `TypeError`（other/on 非法），`ValueError`（schema 未初始化）
- **示例**:
```python
# 找出有订单的用户
active_users = users.semi_join(orders, on=lambda u, o: u.id == o.user_id)
```

### `LTSeq.anti_join`
- **签名**: `LTSeq.anti_join(other: LTSeq, on: Callable) -> LTSeq`
- **行为**: 返回左表中在右表**无匹配**的行（仅返回左表列）
- **参数**: `other` 用于匹配的右表；`on` 连接条件 lambda
- **返回**: 非匹配行的 `LTSeq`（仅含左表列）
- **SQL 对应**: `WHERE NOT EXISTS`
- **异常**: `TypeError`（other/on 非法），`ValueError`（schema 未初始化）
- **示例**:
```python
# 找出没有任何订单的用户
inactive_users = users.anti_join(orders, on=lambda u, o: u.id == o.user_id)
```

### `LTSeq.asof_join`
- **签名**: `LTSeq.asof_join(other: LTSeq, on: Callable, direction: str = "backward") -> LTSeq`
- **行为**: 时间序列最近匹配连接（as-of join）
- **参数**: `other` 另一表；`on` 连接条件；`direction` in {"backward","forward","nearest"}
- **返回**: as-of 连接结果 `LTSeq`
- **SPL 对应**: `joinx`（区间/最近匹配）
- **异常**: `TypeError`（other/on 非法），`ValueError`（direction 非法或未排序）
- **示例**:
```python
quotes = trades.asof_join(quotes, on=lambda t, q: t.time >= q.time, direction="backward")
```

### `LTSeq.link`
- **签名**: `LTSeq.link(target_table: LTSeq, on: Callable, as_: str, join_type: str = "inner") -> LinkedTable`
- **行为**: 指针式关联，不立即物化连接，按 alias 访问右表字段
- **参数**: `target_table` 目标表；`on` 连接条件；`as_` 关联别名；`join_type` 连接类型
- **返回**: `LinkedTable`（可当作 LTSeq 使用）
- **SPL 对应**: `switch`（指针关联）
- **异常**: `TypeError`（on 非法），`ValueError`（join_type 非法或 schema 未初始化）
- **示例**:
```python
linked = orders.link(products, on=lambda o, p: o.product_id == p.id, as_="prod")
result = linked.select(lambda r: [r.id, r.prod.name, r.prod.price])
```

### `LTSeq.lookup`（表级地址化）
- **签名**: `LTSeq.lookup(dim_table: LTSeq, on: Callable, as_: str) -> LTSeq`
- **行为**: 维表装入内存，构建直接索引以加速事实表访问
- **参数**: `dim_table` 维表；`on` 连接条件；`as_` 维表别名
- **返回**: `LTSeq`（内部持有指针/索引）
- **SPL 对应**: `switch`（地址化）
- **异常**: `MemoryError`/`RuntimeError`（维表过大或构建失败），`TypeError`（参数非法）
- **示例**:
```python
fact = orders.lookup(products, on=lambda o, p: o.product_id == p.id, as_="prod")
```

### 连接策略对比

| 方法 | 适用场景 | 算法 | SQL 对应 |
| --- | --- | --- | --- |
| `join` | 无序通用数据 | Hash Join | `JOIN` |
| `join_sorted` / `join_merge` | 已排序大表 | Merge Join | `JOIN`（优化） |
| `semi_join` | 按键存在性过滤 | Hash Semi-Join | `WHERE EXISTS` |
| `anti_join` | 按键不存在性过滤 | Hash Anti-Join | `WHERE NOT EXISTS` |
| `link` | 事实表到维表的指针访问 | Pointer | `LEFT JOIN`（懒加载） |
| `lookup` | 事实表 + 小维表 | Direct Address | `LEFT JOIN`（索引） |
| `asof_join` | 金融时间序列 | Ordered Search | `LATERAL JOIN` |

## 7. 聚合、分区与透视

### `LTSeq.agg`
- **签名**: `LTSeq.agg(by: Optional[Callable] = None, **aggs: Callable[[GroupProxy], Expr]) -> LTSeq`
- **行为**: 分组聚合（或全表聚合），返回每组一行
- **参数**: `by` 分组键（可返回单列或列表）；`aggs` 组聚合表达式
- **返回**: 聚合结果 `LTSeq`
- **SPL 对应**: `groups`
- **异常**: `ValueError`（schema 未初始化），`TypeError`（表达式非法）
- **示例**:
```python
summary = t.agg(by=lambda r: r.region, total=lambda r: r.sales.sum())
```

### `top_k`（聚合函数）
- **签名**: `top_k(col: Union[Expr, Callable], k: int) -> List[Value]`
- **行为**: 返回指定列 Top-K（聚合语义）
- **参数**: `col` 目标列（表达式或可调用）；`k` Top 个数
- **异常**: `ValueError`（k <= 0），`TypeError`（col 非法）
- **示例**:
```python
result = t.agg(top_prices=lambda r: top_k(r.price, 5))
```

### `LTSeq.partition`
- **签名**: `LTSeq.partition(*cols: str) -> PartitionedTable` 或 `LTSeq.partition(by: Callable) -> PartitionedTable`
- **行为**: 按键拆分为子表序列（不聚合）
- **参数**: 列名或 lambda
- **返回**: `PartitionedTable`（键 -> LTSeq）
- **SPL 对应**: `group`
- **异常**: `TypeError`（参数非法），`AttributeError`（列不存在），`ValueError`（schema 未初始化）
- **示例**:
```python
parts = t.partition("region")
west = parts["West"]
```

### `LTSeq.pivot`
- **签名**: `LTSeq.pivot(index: Union[str, List[str]], columns: str, values: str, agg_fn: str = "sum") -> LTSeq`
- **行为**: 长表转宽表的透视操作
- **参数**:
  - `index` 行索引列
  - `columns` 列展开字段（其值成为新列名）
  - `values` 值字段
  - `agg_fn` 聚合函数：`"sum"` | `"mean"` | `"min"` | `"max"` | `"count"` | `"first"` | `"last"`（默认 `"sum"`）
- **返回**: 透视后的 `LTSeq`
- **SPL 对应**: `pivot`
- **异常**: `ValueError`（agg_fn 非法或 schema 未初始化），`AttributeError`（列不存在）
- **示例**:
```python
pivoted = t.pivot(index="date", columns="region", values="amount", agg_fn="sum")
```

## 8. 表达式 API（lambda 内）

### 表达式运算符
- **签名**: `+ - * / // %`，`== != > >= < <=`，`& | ~`
- **行为**: 构建表达式树，不在 Python 侧执行
- **参数**: 左右操作数（Expr 或常量）
- **返回**: 表达式对象
- **异常**: `TypeError`（类型不匹配）
- **示例**:
```python
expr = (r.price * r.qty) > 100
```

### `if_else`
- **签名**: `if_else(condition: Expr, true_value: Any, false_value: Any) -> Expr`
- **行为**: 条件表达式（SQL CASE WHEN）
- **参数**: `condition` 条件；`true_value` 真值；`false_value` 假值
- **返回**: 表达式
- **SQL 对应**: `CASE WHEN condition THEN true_value ELSE false_value END`
- **异常**: `TypeError`（condition 非布尔表达式）
- **示例**:
```python
from ltseq.expr import if_else
status = if_else(r.amount > 100, "VIP", "Normal")
```

### `r.col.fill_null`
- **签名**: `r.col.fill_null(default: Any) -> Expr`
- **行为**: NULL 填充（SQL COALESCE）
- **参数**: `default` 默认值
- **返回**: 表达式
- **SQL 对应**: `COALESCE(col, default)`
- **异常**: `TypeError`（默认值类型不兼容）
- **示例**:
```python
safe_price = r.price.fill_null(0)
```

### `r.col.is_null`
- **签名**: `r.col.is_null() -> Expr`
- **行为**: 判断是否为 NULL
- **返回**: 布尔表达式
- **SQL 对应**: `col IS NULL`
- **示例**:
```python
missing = t.filter(lambda r: r.email.is_null())
```

### `r.col.is_not_null`
- **签名**: `r.col.is_not_null() -> Expr`
- **行为**: 判断是否非 NULL
- **返回**: 布尔表达式
- **SQL 对应**: `col IS NOT NULL`
- **示例**:
```python
valid = t.filter(lambda r: r.email.is_not_null())
```

### 字符串操作（`r.col.s.*`）

#### `contains`
- **签名**: `r.col.s.contains(pattern: str) -> Expr`
- **行为**: 判断是否包含子串
- **SQL 对应**: `POSITION(pattern IN col) > 0`
- **示例**:
```python
gmail = t.filter(lambda r: r.email.s.contains("gmail"))
```

#### `starts_with`
- **签名**: `r.col.s.starts_with(prefix: str) -> Expr`
- **行为**: 判断是否以指定前缀开头
- **示例**:
```python
orders = t.filter(lambda r: r.code.s.starts_with("ORD"))
```

#### `ends_with`
- **签名**: `r.col.s.ends_with(suffix: str) -> Expr`
- **行为**: 判断是否以指定后缀结尾
- **示例**:
```python
pdfs = t.filter(lambda r: r.filename.s.ends_with(".pdf"))
```

#### `lower`
- **签名**: `r.col.s.lower() -> Expr`
- **行为**: 转小写
- **SQL 对应**: `LOWER(col)`
- **示例**:
```python
normalized = t.derive(email_lower=lambda r: r.email.s.lower())
```

#### `upper`
- **签名**: `r.col.s.upper() -> Expr`
- **行为**: 转大写
- **SQL 对应**: `UPPER(col)`
- **示例**:
```python
normalized = t.derive(email_upper=lambda r: r.email.s.upper())
```

#### `strip`
- **签名**: `r.col.s.strip() -> Expr`
- **行为**: 去除首尾空白
- **SQL 对应**: `TRIM(col)`
- **示例**:
```python
clean = t.derive(name_clean=lambda r: r.name.s.strip())
```

#### `lstrip`
- **签名**: `r.col.s.lstrip() -> Expr`
- **行为**: 仅去除左侧（前置）空白
- **SQL 对应**: `LTRIM(col)`
- **SPL 对应**: `ltrim(s)`
- **示例**:
```python
clean = t.derive(name=lambda r: r.name.s.lstrip())  # "  hello" → "hello"
```

#### `rstrip`
- **签名**: `r.col.s.rstrip() -> Expr`
- **行为**: 仅去除右侧（后置）空白
- **SQL 对应**: `RTRIM(col)`
- **SPL 对应**: `rtrim(s)`
- **示例**:
```python
clean = t.derive(name=lambda r: r.name.s.rstrip())  # "hello  " → "hello"
```

#### `len`
- **签名**: `r.col.s.len() -> Expr`
- **行为**: 字符串字符数（Unicode 字符数）
- **SQL 对应**: `CHARACTER_LENGTH(col)`
- **示例**:
```python
long_names = t.filter(lambda r: r.name.s.len() > 50)
```

#### `slice`
- **签名**: `r.col.s.slice(start: int, length: int) -> Expr`
- **行为**: 子串截取（0 基起始）
- **参数**: `start` 起始位置（0 基）；`length` 长度
- **SQL 对应**: `SUBSTRING(col, start+1, length)`
- **示例**:
```python
year = t.derive(year=lambda r: r.date.s.slice(0, 4))
```

#### `regex_match`
- **签名**: `r.col.s.regex_match(pattern: str) -> Expr`
- **行为**: 正则匹配（返回布尔值）
- **参数**: `pattern` 正则表达式
- **SQL 对应**: `REGEXP_LIKE(col, pattern)`
- **示例**:
```python
valid = t.filter(lambda r: r.email.s.regex_match(r"^[a-z]+@"))
```

#### `replace`
- **签名**: `r.col.s.replace(old: str, new: str) -> Expr`
- **行为**: 替换所有出现的子串
- **参数**: `old` 要替换的子串；`new` 替换内容
- **SQL 对应**: `REPLACE(col, old, new)`
- **示例**:
```python
clean = t.derive(clean_name=lambda r: r.name.s.replace("-", "_"))
```

#### `concat`
- **签名**: `r.col.s.concat(*others) -> Expr`
- **行为**: 将当前字符串与其他字符串或列表达式连接
- **参数**: `others` 字符串或列表达式
- **示例**:
```python
greeting = t.derive(msg=lambda r: r.name.s.concat(" says hello"))
full = t.derive(full_name=lambda r: r.first.s.concat(" ", r.last))
```

#### `pad_left`
- **签名**: `r.col.s.pad_left(width: int, char: str = " ") -> Expr`
- **行为**: 左填充至指定宽度；若字符串本身超过宽度则截断（SQL LPAD 行为）
- **参数**: `width` 目标宽度；`char` 填充字符（默认空格）
- **SQL 对应**: `LPAD(col, width, char)`
- **示例**:
```python
padded = t.derive(padded_id=lambda r: r.id.s.pad_left(5, "0"))
```

#### `pad_right`
- **签名**: `r.col.s.pad_right(width: int, char: str = " ") -> Expr`
- **行为**: 右填充至指定宽度；若字符串本身超过宽度则截断（SQL RPAD 行为）
- **参数**: `width` 目标宽度；`char` 填充字符（默认空格）
- **SQL 对应**: `RPAD(col, width, char)`
- **示例**:
```python
padded = t.derive(padded_name=lambda r: r.name.s.pad_right(20, "."))
```

#### `split`
- **签名**: `r.col.s.split(delimiter: str, index: int) -> Expr`
- **行为**: 按分隔符切分字符串，返回第 index 部分（1 基，与 SQL SPLIT_PART 一致）
- **参数**: `delimiter` 分隔符；`index` 部分编号（1 = 第一部分）
- **SQL 对应**: `SPLIT_PART(col, delimiter, index)`
- **示例**:
```python
domain = t.derive(domain=lambda r: r.email.s.split("@", 2))     # "user@example.com" → "example.com"
first  = t.derive(first_name=lambda r: r.username.s.split(".", 1))
```

#### `pos`
- **签名**: `r.col.s.pos(sub: str) -> Expr`
- **行为**: 返回子串第一次出现的 1-based 位置；未找到返回 0
- **参数**: `sub` 要查找的子串
- **SQL 对应**: `STRPOS(col, sub)`
- **SPL 对应**: `pos(s, sub)`
- **示例**:
```python
at_pos = t.derive(pos=lambda r: r.email.s.pos("@"))  # "user@example" → 5
```

#### `left`
- **签名**: `r.col.s.left(n: int) -> Expr`
- **行为**: 取左 n 个字符；若 n 超过字符串长度则返回完整字符串
- **SQL 对应**: `LEFT(col, n)`
- **SPL 对应**: `left(s, n)`
- **示例**:
```python
prefix = t.derive(pfx=lambda r: r.code.s.left(3))  # "ABC123" → "ABC"
```

#### `right`
- **签名**: `r.col.s.right(n: int) -> Expr`
- **行为**: 取右 n 个字符；若 n 超过字符串长度则返回完整字符串
- **SQL 对应**: `RIGHT(col, n)`
- **SPL 对应**: `right(s, n)`
- **示例**:
```python
suffix = t.derive(sfx=lambda r: r.code.s.right(3))  # "ABC123" → "123"
```

#### `asc`
- **签名**: `r.col.s.asc() -> Expr`
- **行为**: 返回字符串首字符的 ASCII/Unicode 码点
- **SQL 对应**: `ASCII(col)`
- **SPL 对应**: `asc(s)`
- **示例**:
```python
code = t.derive(code=lambda r: r.ch.s.asc())  # "A" → 65，"a" → 97
```

### 字符串全局函数

#### `str_char`
- **签名**: `str_char(n: Expr) -> Expr`
- **行为**: 将 Unicode 码点整数转为对应的单字符字符串
- **参数**: `n` 整数表达式（Unicode 码点）
- **SQL 对应**: `CHR(n)`
- **SPL 对应**: `char(n)`
- **导入**: `from ltseq.expr import str_char`
- **示例**:
```python
from ltseq.expr import str_char
ch = t.derive(ch=lambda r: str_char(r.code))  # 65 → "A"，97 → "a"
```

#### `concat_ws`
- **签名**: `concat_ws(delimiter: str, *cols: Expr) -> Expr`
- **行为**: 用分隔符连接两个或多个列表达式
- **参数**: `delimiter` 分隔符字符串；`cols` 两个或多个字符串列表达式
- **SQL 对应**: `CONCAT_WS(delimiter, col1, col2, ...)`
- **导入**: `from ltseq.expr import concat_ws`
- **示例**:
```python
from ltseq.expr import concat_ws
full = t.derive(full_name=lambda r: concat_ws(" ", r.first, r.last))
row  = t.derive(row=lambda r: concat_ws(",", r.a, r.b, r.c))
```

### 时间操作（`r.col.dt.*`）

#### `year` / `month` / `day`
- **签名**: `r.col.dt.year() -> Expr`（month/day 同理）
- **行为**: 提取日期部件
- **SQL 对应**: `EXTRACT(YEAR/MONTH/DAY FROM col)`
- **异常**: `TypeError`（非日期列）
- **示例**:
```python
by_date = t.derive(year=lambda r: r.date.dt.year())
```

#### `hour` / `minute` / `second`
- **签名**: `r.col.dt.hour() -> Expr`（minute/second 同理）
- **行为**: 提取时间部件
- **SQL 对应**: `EXTRACT(HOUR/MINUTE/SECOND FROM col)`
- **异常**: `TypeError`（非时间列）
- **示例**:
```python
with_time = t.derive(hour=lambda r: r.ts.dt.hour())
```

#### `add`
- **签名**: `r.col.dt.add(days: int = 0, months: int = 0, years: int = 0, hours: int = 0, minutes: int = 0, seconds: int = 0, weeks: int = 0) -> Expr`
- **行为**: 日期/时间加减。`weeks` 自动转为 `days × 7`；`hours/minutes/seconds` 通过纳秒精度传入。
- **参数**: 所有参数默认为 0
- **SPL 对应**: `elapse(t, k, unit)`
- **异常**: `TypeError`（非日期列）
- **示例**:
```python
delivery   = t.derive(delivery=lambda r: r.order_date.dt.add(days=5))
after_2h   = t.derive(ts2=lambda r: r.ts.dt.add(hours=2, minutes=30))
next_week  = t.derive(d2=lambda r: r.date.dt.add(weeks=1))
next_month = t.derive(d3=lambda r: r.date.dt.add(days=10, months=1))
```

#### `diff`
- **签名**: `r.col.dt.diff(other: Expr, unit: str = "day") -> Expr`
- **行为**: 返回 `self` 与 `other` 之间的整数差值（以 unit 为单位）。`unit` 可为 `"day"`（默认）、`"month"`、`"year"`、`"hour"`、`"minute"`、`"second"`。
- **参数**: `other` 另一日期/时间列；`unit` 时间单位
- **SPL 对应**: `interval(t1, t2, unit)`
- **异常**: `TypeError`（非日期列），`ValueError`（unit 非法）
- **示例**:
```python
days   = t.derive(d=lambda r: r.end.dt.diff(r.start))
months = t.derive(m=lambda r: r.end.dt.diff(r.start, unit="month"))
years  = t.derive(y=lambda r: r.end.dt.diff(r.start, unit="year"))
hours  = t.derive(h=lambda r: r.end.dt.diff(r.start, unit="hour"))
```

#### `age`
- **签名**: `r.col.dt.age() -> Expr`
- **行为**: 返回日期列与今天之间的完整年数（今天 - 列日期）；未来日期返回负数
- **SPL 对应**: `age(dt)`
- **异常**: `TypeError`（非日期列）
- **示例**:
```python
age = t.derive(age=lambda r: r.birth_date.dt.age())
```

### 数学全局函数

#### `gcd`
- **签名**: `gcd(a: Expr, b: Expr) -> Expr`
- **行为**: 两个整数表达式的最大公约数
- **SQL 对应**: `GCD(a, b)`（DataFusion）
- **SPL 对应**: `gcd(a, b)`
- **导入**: `from ltseq.expr import gcd`
- **示例**:
```python
from ltseq.expr import gcd
t.derive(g=lambda r: gcd(r.a, r.b))  # gcd(12, 8) → 4
```

#### `lcm`
- **签名**: `lcm(a: Expr, b: Expr) -> Expr`
- **行为**: 两个整数表达式的最小公倍数
- **SQL 对应**: `LCM(a, b)`（DataFusion）
- **SPL 对应**: `lcm(a, b)`
- **导入**: `from ltseq.expr import lcm`
- **示例**:
```python
from ltseq.expr import lcm
t.derive(l=lambda r: lcm(r.a, r.b))  # lcm(4, 6) → 12
```

#### `factorial`
- **签名**: `factorial(n: Expr) -> Expr`
- **行为**: 非负整数的阶乘；`n! = 1 × 2 × ... × n`，`0! = 1`
- **SQL 对应**: `FACTORIAL(n)`（DataFusion）
- **SPL 对应**: `fact(n)`
- **导入**: `from ltseq.expr import factorial`
- **示例**:
```python
from ltseq.expr import factorial
t.derive(f=lambda r: factorial(r.n))  # factorial(5) → 120
```

### `r.col.lookup`（表达式查找）
- **签名**: `r.col.lookup(target_table: LTSeq, column: str, join_key: Optional[str] = None) -> Expr`
- **行为**: 在表达式中进行轻量查找，返回目标表单列值
- **参数**: `target_table` 目标表；`column` 返回列；`join_key` 目标表匹配键
- **返回**: 查找表达式
- **异常**: `TypeError`（参数非法），`RuntimeError`（执行失败）
- **示例**:
```python
enriched = orders.derive(product_name=lambda r: r.product_id.lookup(products, "name"))
```

### 条件聚合函数（全局）

用于在 `.agg()` 上下文中基于谓词表达式进行条件聚合，与 `GroupProxy g` 参数配合使用。

#### `count_if`
- **签名**: `count_if(predicate: Expr) -> Expr`
- **行为**: 统计谓词为 True 的行数
- **SQL 对应**: `COUNT(CASE WHEN predicate THEN 1 END)`
- **示例**:
```python
from ltseq.expr import count_if
result = t.agg(by=lambda r: r.region, high_count=lambda r: count_if(r.price > 100))
```

#### `sum_if`
- **签名**: `sum_if(predicate: Expr, column: Expr) -> Expr`
- **行为**: 对谓词为 True 的行求列的和
- **SQL 对应**: `SUM(CASE WHEN predicate THEN column END)`
- **示例**:
```python
from ltseq.expr import sum_if
result = t.agg(by=lambda r: r.region, high_sales=lambda r: sum_if(r.price > 100, r.quantity))
```

#### `avg_if`
- **签名**: `avg_if(predicate: Expr, column: Expr) -> Expr`
- **行为**: 对谓词为 True 的行求列的均值
- **示例**:
```python
from ltseq.expr import avg_if
result = t.agg(by=lambda r: r.region, avg_high=lambda r: avg_if(r.price > 100, r.sales))
```

#### `min_if`
- **签名**: `min_if(predicate: Expr, column: Expr) -> Expr`
- **行为**: 对谓词为 True 的行求列的最小值
- **示例**:
```python
from ltseq.expr import min_if
result = t.agg(by=lambda r: r.region, min_active=lambda r: min_if(r.is_active, r.score))
```

#### `max_if`
- **签名**: `max_if(predicate: Expr, column: Expr) -> Expr`
- **行为**: 对谓词为 True 的行求列的最大值
- **示例**:
```python
from ltseq.expr import max_if
result = t.agg(by=lambda r: r.region, max_active=lambda r: max_if(r.is_active, r.score))
```

## 9. 综合示例

### 有序计算 + 连续分组
```python
from ltseq import LTSeq

# 任务：找出连续上涨超过 3 天的区间
result = (
    LTSeq.read_csv("stock.csv")
    .sort(lambda r: r.date)
    .derive(lambda r: {"is_up": r.price > r.price.shift(1)})
    .group_ordered(lambda r: r.is_up)
    .filter(lambda g: (g.first().is_up == True) & (g.count() > 3))
    .derive(lambda g: {
        "start": g.first().date,
        "end": g.last().date,
        "gain": (g.last().price - g.first().price) / g.first().price,
    })
)
```

### 字符串/时间/条件与 Lookup 混合
```python
from ltseq import LTSeq
from ltseq.expr import if_else, concat_ws

orders = LTSeq.read_csv("orders.csv")
products = LTSeq.read_csv("products.csv")

result = orders.derive(
    order_id_clean=lambda r: r.order_id.s.strip(),
    full_name=lambda r: concat_ws(" ", r.first_name, r.last_name),
    product_name=lambda r: r.product_id.lookup(products, "name"),
    order_year=lambda r: r.order_date.dt.year(),
    customer_age=lambda r: r.birth_date.dt.age(),
    status=lambda r: if_else(r.quantity > 10, "Bulk", "Standard"),
)
```

### 连接与条件聚合
```python
from ltseq import LTSeq
from ltseq.expr import count_if, sum_if

users = LTSeq.read_csv("users.csv")
orders = LTSeq.read_csv("orders.csv")

# 找有订单的活跃用户
active = users.semi_join(orders, on=lambda u, o: u.id == o.user_id)

# 按地区统计高价值订单
summary = orders.agg(
    by=lambda r: r.region,
    total_orders=lambda g: g.count(),
    high_value_count=lambda g: count_if(g.amount > 1000),
    high_value_total=lambda g: sum_if(g.amount > 1000, g.amount),
)
```

## 10. 执行与性能说明

- 所有表达式会被序列化并下推到 Rust/DataFusion 层执行，不在 Python 侧逐行计算
- 字符串/时间/NULL 等扩展操作会映射为底层 SQL/DataFusion 函数
- `to_cursor` 提供流式处理能力，适合超大数据集

### 表达式 SQL 转译速查表

所有表达式在执行前均被序列化 → JSON → Rust/DataFusion 转译，不发生 Python 层的求值。

| 表达式 | SQL / DataFusion 等价 |
|-------|----------------------|
| `if_else(cond, a, b)` | `CASE WHEN cond THEN a ELSE b END` |
| `r.col.fill_null(v)` | `COALESCE(col, v)` |
| `r.col.is_null()` | `col IS NULL` |
| `r.col.is_not_null()` | `col IS NOT NULL` |
| `r.col.s.contains(p)` | `POSITION(p IN col) > 0` |
| `r.col.s.lower()` | `LOWER(col)` |
| `r.col.s.upper()` | `UPPER(col)` |
| `r.col.s.strip()` | `TRIM(col)` |
| `r.col.s.lstrip()` | `LTRIM(col)` |
| `r.col.s.rstrip()` | `RTRIM(col)` |
| `r.col.s.len()` | `CHARACTER_LENGTH(col)` |
| `r.col.s.slice(s, n)` | `SUBSTRING(col, s+1, n)` |
| `r.col.s.pos(sub)` | `STRPOS(col, sub)` |
| `r.col.s.left(n)` | `LEFT(col, n)` |
| `r.col.s.right(n)` | `RIGHT(col, n)` |
| `r.col.s.asc()` | `ASCII(col)` |
| `r.col.s.replace(o, n)` | `REPLACE(col, o, n)` |
| `r.col.s.pad_left(n, c)` | `LPAD(col, n, c)` |
| `r.col.s.pad_right(n, c)` | `RPAD(col, n, c)` |
| `r.col.s.split(d, i)` | `SPLIT_PART(col, d, i)` |
| `str_char(n)` | `CHR(n)` |
| `concat_ws(d, ...)` | `CONCAT_WS(d, ...)` |
| `r.col.dt.year()` | `EXTRACT(YEAR FROM col)` |
| `r.col.dt.month()` | `EXTRACT(MONTH FROM col)` |
| `r.col.dt.day()` | `EXTRACT(DAY FROM col)` |
| `r.col.dt.hour()` | `EXTRACT(HOUR FROM col)` |
| `r.col.dt.add(days=n)` | `col + INTERVAL 'n' DAY` |
| `r.col.dt.add(hours=n)` | `col + INTERVAL 'n' HOUR` |
| `r.col.dt.diff(other)` | `DATEDIFF('day', other, col)` |
| `r.col.dt.diff(other, unit="month")` | `(year(col)-year(other))*12 + month(col)-month(other)` |
| `r.col.dt.age()` | 年份差（含日期修正） |
| `gcd(a, b)` | `GCD(a, b)` |
| `lcm(a, b)` | `LCM(a, b)` |
| `factorial(n)` | `FACTORIAL(n)` |
| `count_if(cond)` | `COUNT(CASE WHEN cond THEN 1 END)` |
| `sum_if(cond, col)` | `SUM(CASE WHEN cond THEN col END)` |

## 附录 A：从 Pandas 迁移

| Pandas | LTSeq | 说明 |
|--------|-------|------|
| `df[df.age > 18]` | `t.filter(lambda r: r.age > 18)` | lambda 捕获表达式 |
| `df[['id', 'name']]` | `t.select("id", "name")` | |
| `df.assign(total=df.a + df.b)` | `t.derive(total=lambda r: r.a + r.b)` | |
| `df.sort_values('date')` | `t.sort("date")` | |
| `df.sort_values('date', ascending=False)` | `t.sort("date", desc=True)` | |
| `df.drop_duplicates('id')` | `t.distinct("id")` | |
| `df.groupby('region').agg({'sales': 'sum'})` | `t.agg(by=lambda r: r.region, sales=lambda r: r.sales.sum())` | |
| `df.merge(df2, on='id')` | `t.join(t2, on=lambda a, b: a.id == b.id)` | |
| `df.merge(df2, on='id', how='left')` | `t.join(t2, on=lambda a, b: a.id == b.id, how="left")` | |
| `df[df.id.isin(df2.id)]` | `t.semi_join(t2, on=lambda a, b: a.id == b.id)` | 半连接 |
| `df[~df.id.isin(df2.id)]` | `t.anti_join(t2, on=lambda a, b: a.id == b.id)` | 反连接 |
| `df['col'].shift(1)` | `t.sort(...).derive(prev=lambda r: r.col.shift(1))` | 需要先排序 |
| `df['col'].rolling(5).mean()` | `t.sort(...).derive(ma=lambda r: r.col.rolling(5).mean())` | 需要先排序 |
| `df['col'].diff()` | `t.sort(...).derive(d=lambda r: r.col.diff())` | 需要先排序 |
| `df['col'].cumsum()` | `t.sort(...).cum_sum("col")` | 需要先排序 |
| `df.head(10)` | `t.head(10)` | |
| `df.tail(10)` | `t.tail(10)` | |
| `df.to_dict('records')` | `t.collect()` | |
| `len(df)` | `t.count()` | |
| `df.columns.tolist()` | `t.columns` | |
| `df.isna()` | `t.filter(lambda r: r.col.is_null())` | 逐列操作 |
| `df.fillna(0)` | `t.derive(col=lambda r: r.col.fill_null(0))` | 逐列操作 |
| `df['col'].str.strip()` | `t.derive(c=lambda r: r.col.s.strip())` | |
| `df['col'].str.lower()` | `t.derive(c=lambda r: r.col.s.lower())` | |
| `df['col'].str.contains('x')` | `t.filter(lambda r: r.col.s.contains("x"))` | |
| `np.where(cond, a, b)` | `if_else(cond, a, b)` | |
| `df.rank(method='first')` | `row_number().over(order_by=r.col)` | 需 `.over()` |
| `df.rank(method='dense')` | `dense_rank().over(order_by=r.col)` | 需 `.over()` |
