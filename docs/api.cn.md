# LTSeq API 设计文档

相关文档：

- `docs/README.md`：文档索引
- `docs/USER_MODEL.cn.md`：用户心智模型与使用说明
- `docs/ARCHITECTURE.cn.md`：系统架构与执行模型
- `docs/MODULE_GUIDE.cn.md`：面向贡献者的代码导览
- `docs/DESIGN_SUMMARY.cn.md`：中文设计摘要与设计归档
- `docs/LINKING_GUIDE.cn.md`：中文 Linking 专题文档

LTSeq 是面向有序序列的 Python 数据处理库，底层由 Rust/DataFusion 执行。与传统 DataFrame 不同，LTSeq 强调"顺序"语义，支持窗口、连续分组、游标式流处理等 SPL 风格能力。

本文档描述的是**当前已实现**的 API，所有签名均与 `py-ltseq/ltseq/` 源码逐一核对。

## 0. 基本约定与术语

- t: 当前 LTSeq 实例（不可变，所有操作返回新实例）
- r: 行代理（lambda 内部用于构造表达式，不在 Python 侧执行）
- g: 组代理（NestedTable 的 filter/derive 中使用）；组聚合以字符串列名调用（`g.sum("amount")`），`g.first()`/`g.last()` 返回可属性访问的行代理（`g.first().date`）
- 绝大多数操作返回新的 LTSeq；`LTSeq.scan()`/`scan_parquet()` 返回流式 `Cursor`；`is_subset`/`contain` 返回布尔值
- 窗口/有序相关能力依赖已有排序（`sort` 或 `assume_sorted`），未排序使用会导致运行期错误或不正确结果；`shift`/`rolling`/`diff` 还支持 `partition_by=` 参数做分组窗口
- 表达式在 Python 侧被捕获为 AST 并在 Rust/DataFusion 层执行

## 常见错误与解决方案

| 错误 | 原因 | 解决方法 |
|------|------|---------|
| `RuntimeError: window function used without sort` | 未排序直接调用 `shift`/`rolling`/`diff` | 在窗口函数前添加 `.sort(order_column)`（或 `.assume_sorted(...)`）|
| `AttributeError: column 'xxx' not found` | 列名拼写错误或列不存在 | 通过 `t.columns` 查看可用列名 |
| `ValueError: schema mismatch` | union/intersect 的表 schema 不匹配 | 确保两表列名和类型相同 |
| `ValueError: merge strategy requires sorted tables` | 对未排序的表调用 `join(..., strategy="merge")` | 先对双方调用 `.sort(join_key)` |
| `TypeError: predicate not boolean Expr` | filter lambda 返回非布尔值 | 确保谓词使用比较运算符（`>`、`==` 等）|
| `ValueError: desc length mismatch` | `desc` 列表长度与排序键数量不匹配 | 为每个排序键提供一个布尔值，或使用单个布尔值 |
| `ValueError: Schema not initialized` | 对空的 `LTSeq()` 调用操作 | 先加载数据（`read_csv`、`from_pandas` 等）|

## 快速参考

### 数据加载与输出
| 操作 | 方法 | 示例 |
|------|------|------|
| 加载 CSV | `LTSeq.read_csv()` | `t = LTSeq.read_csv("data.csv")` |
| 加载 Parquet | `LTSeq.read_parquet()` | `t = LTSeq.read_parquet("data.parquet")` |
| 流式读取大文件 | `LTSeq.scan()` | `for batch in LTSeq.scan("big.csv"): ...` |
| 从 Python 数据构造 | `LTSeq.from_dict()` | `t = LTSeq.from_dict({"id": [1, 2]})` |
| 查看 schema | `.columns` / `.schema` | `print(t.columns)` |
| 行字典导出 | `.to_dicts()` | `rows = t.to_dicts()` |
| 物化惰性计划 | `.collect()` | `result = t.collect()` |
| 转 pandas | `.to_pandas()` | `df = t.to_pandas()` |
| 写文件 | `.write_csv()` / `.write_parquet()` | `t.write_csv("out.csv")` |
| 行数统计 | `.count()` / `len(t)` | `n = t.count()` |
| 表格打印 | `.show()` | `t.show(20)` |

### 基础操作
| 操作 | 方法 | 示例 |
|------|------|------|
| 过滤行 | `.filter()` | `t.filter(lambda r: r.age > 18)` |
| 投影列 | `.select()` | `t.select("id", "name")` |
| 新增列 | `.derive()`（别名 `.with_columns()`）| `t.derive(total=lambda r: r.a + r.b)` |
| 重命名列 | `.rename()` | `t.rename(old="new")` |
| 删除列 | `.drop()` | `t.drop("tmp")` |
| 排序 | `.sort()` | `t.sort("date", desc=True)` |
| 去重 | `.distinct()` | `t.distinct("id")` |
| 截取行 | `.slice()` | `t.slice(offset=10, length=5)` |
| 取前 N 行 | `.head()` | `t.head(10)` |
| 取后 N 行 | `.tail()` | `t.tail(10)` |

### 窗口操作（需先 `.sort()`；`partition_by=` 支持分组窗口）
| 操作 | 方法 | 示例 |
|------|------|------|
| 前一行 | `.shift(n)` | `r.price.shift(1)` |
| 滑动聚合 | `.rolling(n).agg()` | `r.price.rolling(5).mean()` |
| 行差分 | `.diff(n)` | `r.price.diff(1)` |
| 环比变化率 | `.pct_change()` | `r.close.pct_change()` |
| 累计求和 | `.cum_sum()` | `t.cum_sum("volume")` 或 `r.volume.cum_sum()` |

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
| 分组聚合（链式）| `.group_by().agg()` | `t.group_by("region").agg(total=lambda g: g.sales.sum())` |
| 分组聚合 | `.agg()` | `t.agg(by=lambda r: r.region, total=lambda g: g.sales.sum())` |
| 分区 | `.partition()` | `parts = t.partition("region")` |
| 透视 | `.pivot()` | `t.pivot(index="date", columns="region", values="amount")` |

### 连接
| 操作 | 方法 | 示例 |
|------|------|------|
| 哈希连接 | `.join()` | `a.join(b, on="id")` 或 `a.join(b, on=lambda a, b: a.id == b.id)` |
| 归并连接 | `.join(..., strategy="merge")` | `a.sort("id").join(b.sort("id"), on=lambda a, b: a.id == b.id, strategy="merge")` |
| 时序就近连接 | `.asof_join()` | `trades.asof_join(quotes, on=lambda t, q: t.time >= q.time)` |
| 半连接 | `.semi_join()` | `a.semi_join(b, on=lambda a, b: a.id == b.id)` |
| 反连接 | `.anti_join()` | `a.anti_join(b, on=lambda a, b: a.id == b.id)` |
| 指针关联 | `.link()` | `orders.link(products, on=lambda o, p: o.product_id == p.id, as_="prod")` |

### 集合操作
| 操作 | 方法 | 示例 |
|------|------|------|
| 纵向合并（保留重复）| `.union()` | `t1.union(t2)` |
| 交集 | `.intersect()` | `t1.intersect(t2)` |
| 差集 | `.except_()` | `t1.except_(t2)` |
| 对称差 | `.xunion()` | `t1.xunion(t2)` |

### 行变更（写时复制）
| 操作 | 方法 | 示例 |
|------|------|------|
| 插入行 | `.insert()` | `t.insert(0, {"id": 99})` |
| 删除行 | `.delete()` | `t.delete(lambda r: r.expired)` |
| 条件更新 | `.update()` | `t.update(lambda r: r.age > 65, discount=0.2)` |
| 修改单行 | `.modify()` | `t.modify(0, status="active")` |

## 1. 输入 / 输出

### `LTSeq.read_csv`
- **签名**: `LTSeq.read_csv(path: str, has_header: bool = True) -> LTSeq`
- **行为**: 从 CSV 加载数据并推断 schema，返回可链式计算的 LTSeq。`has_header=False` 时列名为 `column_0`、`column_1`、...
- **参数**: `path` CSV 路径；`has_header` 是否把首行作为列名
- **返回**: `LTSeq`，包含加载后的数据与 schema
- **异常**: `FileNotFoundError`（路径无效），`ValueError`（解析失败或 schema 推断失败）
- **示例**:
```python
from ltseq import LTSeq

t = LTSeq.read_csv("data.csv")
t2 = LTSeq.read_csv("raw.csv", has_header=False)
```

### `LTSeq.read_parquet`
- **签名**: `LTSeq.read_parquet(path: str) -> LTSeq`
- **行为**: 加载 Parquet 文件并返回 LTSeq 表
- **参数**: `path` Parquet 路径
- **返回**: `LTSeq`，包含加载后的数据与 schema
- **异常**: `FileNotFoundError`（路径无效），`RuntimeError`（解析失败）
- **示例**:
```python
t = LTSeq.read_parquet("data.parquet")
```

### `LTSeq.scan` / `LTSeq.scan_parquet`（流式读取）
- **签名**: `LTSeq.scan(path: str, has_header: bool = True) -> Cursor`；`LTSeq.scan_parquet(path: str) -> Cursor`
- **行为**: 为 CSV/Parquet 文件创建流式游标，按批次惰性迭代，无需将整个文件载入内存
- **参数**: `path` 文件路径；`has_header`（仅 CSV）首行是否为列名
- **返回**: `Cursor`（可迭代的批次流）
- **异常**: `FileNotFoundError`（路径无效），`RuntimeError`（扫描失败）
- **示例**:
```python
for batch in LTSeq.scan("large.csv"):
    process(batch)

cursor = LTSeq.scan_parquet("large.parquet")
```

### `Cursor`（流式句柄）
- **签名**: 由 `scan`/`scan_parquet` 返回的可迭代对象
- **行为**: 惰性产出记录批次；也支持整体物化
- **成员**:
  - `__iter__()`: 逐批迭代
  - `schema -> dict[str, str]`（属性）、`columns -> list[str]`（属性）
  - `source -> str`（属性）：源文件路径；`exhausted -> bool`（属性）
  - `to_pandas()`、`to_arrow()`: 物化剩余流
  - `count() -> int`: 消费整个流并统计行数
- **示例**:
```python
cursor = LTSeq.scan("large.csv")
print(cursor.columns)
for batch in cursor:
    ...
```

### `LTSeq.from_rows`
- **签名**: `LTSeq.from_rows(rows: list[dict[str, Any]], schema: dict[str, str] | None = None) -> LTSeq`
- **行为**: 从行字典列表构造表。未提供 `schema` 时从第一行推断类型；`rows` 为空时必须显式给出 schema
- **参数**: `rows` 行字典列表（各行键一致）；`schema` 可选的 `{列名: arrow 类型}` 映射
- **返回**: 新 `LTSeq`
- **异常**: `ValueError`（空 rows 且无 schema），`TypeError`（非字典列表）
- **示例**:
```python
t = LTSeq.from_rows([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
empty = LTSeq.from_rows([], schema={"id": "Int64", "name": "Utf8"})
```

### `LTSeq.from_dict`
- **签名**: `LTSeq.from_dict(data: dict[str, list[Any]]) -> LTSeq`
- **行为**: 从列式字典构造表
- **参数**: `data` 列名到取值列表的映射（各列等长）
- **返回**: 新 `LTSeq`
- **异常**: `ValueError`（列长度不一致），`TypeError`（非字典）
- **示例**:
```python
t = LTSeq.from_dict({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
```

### `LTSeq.from_pandas` / `LTSeq.from_arrow`
- **签名**: `LTSeq.from_pandas(df) -> LTSeq`；`LTSeq.from_arrow(arrow_table) -> LTSeq`
- **行为**: 从 pandas DataFrame / PyArrow Table 构造表
- **返回**: 新 `LTSeq`
- **异常**: `ImportError`（未安装 pandas/pyarrow），`TypeError`（输入类型错误）
- **示例**:
```python
t = LTSeq.from_pandas(df)
t = LTSeq.from_arrow(arrow_table)
```

### `seq`（整数序列构造器）
- **签名**: `seq(start_or_stop: int, stop: int | None = None, step: int = 1) -> LTSeq`
- **行为**: 生成整数序列，返回单列 LTSeq（列名 `value`，Int64）。接口与 Python 内置 `range()` 一致
- **导入**: `from ltseq import seq`
- **示例**:
```python
from ltseq import seq

seq(5)         # 0, 1, 2, 3, 4
seq(2, 7)      # 2, 3, 4, 5, 6
seq(0, 10, 2)  # 0, 2, 4, 6, 8
```

### `LTSeq.write_csv` / `LTSeq.write_parquet`
- **签名**: `LTSeq.write_csv(path: str) -> None`；`LTSeq.write_parquet(path: str, compression: str | None = None) -> None`
- **行为**: 将表写入 CSV/Parquet 文件。Parquet 的 `compression` 可选 `"snappy" | "zstd" | "gzip" | "lz4" | "none"`（默认不压缩）
- **异常**: `RuntimeError`（写入失败），`ValueError`（未知压缩算法）
- **示例**:
```python
t.write_csv("output.csv")
t.write_parquet("output.parquet", compression="zstd")
```

### `LTSeq.schema`（属性）
- **签名**: `LTSeq.schema -> dict[str, str]`
- **行为**: 以字典形式返回表的 schema，列名映射到 Arrow 类型字符串
- **示例**:
```python
print(t.schema)   # {"id": "Int64", "name": "Utf8", ...}

for name, dtype in t.schema.items():
    print(f"{name}: {dtype}")
```

### `LTSeq.python_schema`（属性）
- **签名**: `LTSeq.python_schema -> dict[str, str]`
- **行为**: 以 Python 友好的类型名返回 schema（`Int64` → `int`、`Utf8` → `str`、`Float64` → `float`、`Boolean` → `bool` 等）。未知类型原样返回
- **示例**:
```python
print(t.python_schema)  # {"id": "int", "name": "str", "score": "float"}
```

### `LTSeq.columns`（属性）
- **签名**: `LTSeq.columns -> list[str]`
- **行为**: 返回列名列表（`list(schema.keys())` 的快捷方式）
- **示例**:
```python
print(t.columns)  # ["id", "name", "age"]
```

### `LTSeq.dtypes`（属性）
- **签名**: `LTSeq.dtypes -> list[tuple[str, str]]`
- **行为**: 返回 `(列名, 类型)` 元组列表
- **示例**:
```python
print(t.dtypes)  # [("id", "Int64"), ("name", "Utf8"), ...]
```

### `LTSeq.collect`
- **签名**: `LTSeq.collect() -> LTSeq`
- **行为**: 物化惰性计划，返回新的内存 `LTSeq`（与 Polars/PySpark `collect()` 语义一致）。挂起的管道只执行一次，下游操作复用已计算的数据而非重新执行计划。行序、schema、排序元数据均保留，结果可继续链式调用。如需行字典，请用 `to_dicts()`。
- **参数**: 无
- **返回**: 由物化后内存数据支撑的新 `LTSeq`
- **异常**: `MemoryError`（数据集过大），`RuntimeError`（执行失败）
- **示例**:
```python
result = t.filter(lambda r: r.age > 18).collect()  # 计划只执行一次
result.count()
rows = result.to_dicts()
```

### `LTSeq.to_dicts`
- **签名**: `LTSeq.to_dicts() -> list[dict[str, Any]]`
- **行为**: 将所有行物化为字典列表（与 Polars `to_dicts()` 同名同语义）
- **参数**: 无
- **返回**: 行字典列表
- **异常**: `MemoryError`（数据集过大），`RuntimeError`（执行失败）
- **示例**:
```python
rows = t.filter(lambda r: r.age > 18).to_dicts()
for row in rows:
    print(row["name"])
```

### `LTSeq.to_pandas` / `LTSeq.to_arrow`
- **签名**: `LTSeq.to_pandas() -> pandas.DataFrame`；`LTSeq.to_arrow() -> pyarrow.Table`
- **行为**: 物化为 pandas DataFrame / PyArrow Table
- **异常**: `ImportError`（未安装 pandas/pyarrow），`MemoryError`（数据集过大）
- **示例**:
```python
df = t.to_pandas()
df.plot(x="date", y="price")

arrow_table = t.to_arrow()
```

### `LTSeq.count` / `len(t)`
- **签名**: `LTSeq.count() -> int`；`LTSeq.__len__() -> int`
- **行为**: 返回行数；`count()` 与 `len(t)` 等价
- **异常**: `RuntimeError`（执行失败）
- **示例**:
```python
n = t.filter(lambda r: r.status == "active").count()
n = len(t)
```

### `LTSeq.show`
- **签名**: `LTSeq.show(n: int = 10) -> LTSeq`
- **行为**: 以 ASCII 表格打印最多 `n` 行；返回 `self` 以便继续链式调用。LTSeq 的 REPL `repr` 也会展示行列数、schema 与 5 行预览
- **示例**:
```python
t.show()
t.filter(lambda r: r.active).show().derive(upper=lambda r: r.name.s.upper())
```

### `LTSeq.head`
- **签名**: `LTSeq.head(n: int = 10) -> LTSeq`
- **行为**: 返回前 n 行
- **异常**: `ValueError`（n < 0）
- **示例**:
```python
top_10 = t.sort("score", desc=True).head(10)
```

### `LTSeq.tail`
- **签名**: `LTSeq.tail(n: int = 10) -> LTSeq`
- **行为**: 返回后 n 行
- **异常**: `ValueError`（n < 0）
- **示例**:
```python
recent = t.sort("date").tail(5)
```

### `LTSeq.pipe`
- **签名**: `LTSeq.pipe(func: Callable[..., LTSeq], *args, **kwargs) -> LTSeq`
- **行为**: 在链式调用中应用用户函数（同 pandas 的 `DataFrame.pipe`）
- **示例**:
```python
def add_tax(t, rate):
    return t.derive(tax=lambda r: r.price * rate)

result = t.filter(lambda r: r.qty > 0).pipe(add_tax, 0.1).sort("tax")
```

### `LTSeq.explain_plan`
- **签名**: `LTSeq.explain_plan() -> tuple[str, str]`
- **行为**: 返回优化后的逻辑计划与物理计划（DataFusion），用于调试
- **示例**:
```python
logical, physical = t.filter(lambda r: r.age > 18).explain_plan()
print(logical)
```

## 2. 基础关系运算

### `LTSeq.filter`
- **签名**: `LTSeq.filter(predicate: Callable[[Row], Expr]) -> LTSeq`
- **行为**: 过滤满足谓词的行；下推至 Rust 引擎执行
- **参数**: `predicate` 行谓词表达式（返回布尔 Expr）
- **返回**: 过滤后的 `LTSeq`
- **异常**: `ValueError`（schema 未初始化），`TypeError`（谓词非布尔 Expr），`AttributeError`（列不存在）
- **示例**:
```python
filtered = t.filter(lambda r: r.amount > 100)
```

### `LTSeq.select`
- **签名**: `LTSeq.select(*cols: str | Callable) -> LTSeq`
- **行为**: 投影指定列或表达式；支持列裁剪
- **参数**: `cols` 列名或 lambda（单个表达式或列表）
- **返回**: 投影后的 `LTSeq`
- **异常**: `ValueError`（schema 未初始化），`TypeError`（返回类型无效），`AttributeError`（列不存在）
- **示例**:
```python
t.select("id", "name")
# 或
t.select(lambda r: [r.id, r.name])
```

### `LTSeq.derive`（别名: `with_columns`）
- **签名**: `LTSeq.derive(**new_cols: Callable) -> LTSeq` 或 `LTSeq.derive(func: Callable[[Row], dict[str, Expr]]) -> LTSeq`
- **行为**: 新增或覆盖列，保留已有列。`with_columns` 是面向 Polars 用户的别名
- **参数**: `new_cols` 列名到 lambda 的映射；或返回字典的 lambda
- **返回**: 带派生列的新 `LTSeq`
- **异常**: `ValueError`（schema 未初始化），`TypeError`（返回类型无效），`AttributeError`（列不存在）
- **示例**:
```python
# 风格 1
with_tax = t.derive(tax=lambda r: r.price * 0.1)
# 风格 2
with_tax = t.derive(lambda r: {"tax": r.price * 0.1})
# Polars 风格别名
with_tax = t.with_columns(tax=lambda r: r.price * 0.1)
```

### `LTSeq.rename`
- **签名**: `LTSeq.rename(mapping: dict[str, str] | None = None, **kwargs: str) -> LTSeq`
- **行为**: 通过字典或关键字参数（`旧名=新名`）重命名列
- **返回**: 重命名后的新 `LTSeq`
- **异常**: `AttributeError`（列不存在），`ValueError`（schema 未初始化）
- **示例**:
```python
t.rename({"old_name": "new_name"})
t.rename(price="unit_price", qty="quantity")
```

### `LTSeq.drop`
- **签名**: `LTSeq.drop(*cols: str) -> LTSeq`
- **行为**: 删除指定列，保留其余列
- **异常**: `AttributeError`（列不存在），`ValueError`（schema 未初始化）
- **示例**:
```python
t.drop("tmp", "debug_flag")
```

### `LTSeq.sort`
- **签名**: `LTSeq.sort(*keys: str | Callable, desc: bool | list[bool] = False, descending: bool | list[bool] | None = None) -> LTSeq`
- **行为**: 按一个或多个键排序；窗口/有序计算的前置条件。同时填充 `sort_keys` 用于排序状态追踪。`descending` 是 `desc` 的别名（Polars 命名），两者同时给出时 `descending` 优先
- **参数**: `keys` 列名或表达式；`desc`/`descending` 全局或逐键降序标志
- **返回**: 排序后的 `LTSeq`（带排序键追踪）
- **异常**: `ValueError`（schema 未初始化或 desc 长度不匹配），`TypeError`（键类型无效），`AttributeError`（列不存在）
- **示例**:
```python
t_sorted = t.sort("date", "id", desc=[False, True])
print(t_sorted.sort_keys)  # [("date", False), ("id", True)]

t.sort("price", descending=True)  # Polars 风格别名
```

### `LTSeq.sort_keys`（属性）
- **签名**: `LTSeq.sort_keys -> list[tuple[str, bool]] | None`
- **行为**: 以 (列名, 是否降序) 元组列表返回当前排序键；排序状态未知时返回 None
- **示例**:
```python
t = LTSeq.read_csv("data.csv")
print(t.sort_keys)  # None（排序状态未知）

t_sorted = t.sort("date", "id")
print(t_sorted.sort_keys)  # [("date", False), ("id", False)]

t_desc = t.sort("price", desc=True)
print(t_desc.sort_keys)  # [("price", True)]
```

### `LTSeq.is_sorted_by`
- **签名**: `LTSeq.is_sorted_by(*keys: str, desc: bool | list[bool] = False) -> bool`
- **行为**: 检查表是否按给定键排序（前缀匹配）
- **参数**: `keys` 待检查的列名；`desc` 期望的降序标志（单个布尔或逐键列表）
- **返回**: `bool` - 按给定键（作为前缀）排序则为 True
- **异常**: `ValueError`（未提供键或 desc 长度不匹配）
- **示例**:
```python
t_sorted = t.sort("a", "b", "c")
t_sorted.is_sorted_by("a")          # True（前缀匹配）
t_sorted.is_sorted_by("a", "b")     # True（前缀匹配）
t_sorted.is_sorted_by("a", "b", "c") # True（完全匹配）
t_sorted.is_sorted_by("b")          # False（非前缀）
t_sorted.is_sorted_by("a", desc=True)  # False（方向不匹配）
```

### `LTSeq.assume_sorted`
- **签名**: `LTSeq.assume_sorted(*keys: str, desc: bool | list[bool] = False) -> LTSeq`
- **行为**: 声明数据已按给定键排序，**不做物理排序**。可对预排序数据（如预排序的 Parquet）跳过排序开销，同时启用窗口函数与归并连接。正确性由调用方负责——错误的声明会产生错误结果
- **参数**: `keys` 声明排序顺序的列名；`desc` 降序标志
- **返回**: 设置了排序元数据的 `LTSeq`（底层数据不变）
- **异常**: `ValueError`（schema 未初始化或 desc 长度不匹配），`TypeError`（desc 类型无效）
- **示例**:
```python
t = LTSeq.read_parquet("presorted.parquet").assume_sorted("userid", "eventtime")
t.is_sorted_by("userid")  # True
```

### `LTSeq.distinct`
- **签名**: `LTSeq.distinct(*keys: str | Callable) -> LTSeq`
- **行为**: 去重；不传键时按所有列去重
- **参数**: `keys` 键列或表达式
- **返回**: 去重后的 `LTSeq`
- **异常**: `ValueError`（schema 未初始化），`TypeError`（键类型无效），`AttributeError`（列不存在）
- **示例**:
```python
unique = t.distinct("customer_id")
```

### `LTSeq.slice`
- **签名**: `LTSeq.slice(offset: int = 0, length: int | None = None) -> LTSeq`
- **行为**: 选取连续行区间，逻辑零拷贝
- **参数**: `offset` 起始行（0-based）；`length` 行数（None 表示到末尾）
- **返回**: 截取后的 `LTSeq`
- **异常**: `ValueError`（负 offset/length 或 schema 未初始化）
- **示例**:
```python
t.slice(offset=10, length=5)
```

## 3. 有序与窗口函数

### 3.1 行级窗口操作

> 这些操作依赖先行的 `.sort()`（或 `.assume_sorted()`）建立顺序。`shift` 还支持 `partition_by=` 参数（列名字符串或表达式），在分组边界处重置窗口。

#### `r.col.shift`
- **签名**: `r.col.shift(offset: int, default: Any = None, partition_by: str | Expr | None = None) -> Expr`
- **行为**: 访问相对行。正偏移向前看（前面的行），负偏移向后看（后面的行），与 pandas `Series.shift()` 一致。指定 `partition_by` 时窗口在分组边界处重置（LAG/LEAD OVER PARTITION BY）
- **参数**: `offset` 行偏移（正=向前，负=向后）；`default` 边界填充值（默认 NULL）；`partition_by` 可选分区列
- **返回**: 表达式（偏移超出可用行的边界处为 NULL 或 `default`）
- **异常**: `TypeError`（offset 非整数），`RuntimeError`（未排序使用）
- **示例**:
```python
with_prev = t.sort("date").derive(prev=lambda r: r.close.shift(1))

# 分组 shift：每组边界处为 NULL
t.sort("grp", "date").derive(
    prev=lambda r: r.value.shift(1, partition_by="grp")
)

# 边界处填充默认值而非 NULL
t.sort("date").derive(prev=lambda r: r.value.shift(1, default=0))
```

#### `r.col.rolling`
- **签名**: `r.col.rolling(window_size: int).agg_func() -> Expr`
- **行为**: 滑动窗口聚合；支持的聚合：`mean/sum/min/max/count/std`
- **参数**: `window_size` 窗口大小
- **返回**: 窗口聚合表达式
- **异常**: `ValueError`（window_size <= 0），`RuntimeError`（未排序使用）
- **示例**:
```python
ma5 = t.sort("date").derive(ma_5=lambda r: r.close.rolling(5).mean())
```

#### `r.col.diff`
- **签名**: `r.col.diff(offset: int = 1) -> Expr`
- **行为**: 行差分，等价于 `r.col - r.col.shift(offset)`
- **参数**: `offset` 行偏移
- **返回**: 差分表达式
- **异常**: `TypeError`（非数值或 offset 非整数），`RuntimeError`（未排序使用）
- **示例**:
```python
changes = t.sort("date").derive(daily=lambda r: r.close.diff())
```

#### `r.col.pct_change`
- **签名**: `r.col.pct_change() -> Expr`
- **行为**: 相对上一行的变化率：`(x - x.shift(1)) / x.shift(1)`。需要先排序
- **返回**: 数值表达式
- **示例**:
```python
returns = t.sort("date").derive(daily_return=lambda r: r.close.pct_change())
```

#### `r.col.cum_sum`（表达式形式）
- **签名**: `r.col.cum_sum() -> Expr`
- **行为**: 按当前顺序累计求和，可在 `derive()` 内使用，输出列名由用户掌控
- **异常**: `TypeError`（非数值），`RuntimeError`（未排序使用）
- **示例**:
```python
t.sort("date").derive(cum_vol=lambda r: r.volume.cum_sum())
```

### 3.2 表级累计操作

#### `LTSeq.cum_sum`
- **签名**: `LTSeq.cum_sum(*cols: str | Callable) -> LTSeq`
- **行为**: 添加带 `*_cumsum` 后缀的累计求和列。若需自定义列名，请在 `derive()` 中使用表达式形式 `r.col.cum_sum()`
- **参数**: `cols` 列名或表达式
- **返回**: 带累计列的新 `LTSeq`
- **异常**: `ValueError`（无列或 schema 未初始化），`TypeError`（非数值）
- **示例**:
```python
with_cum = t.sort("date").cum_sum("volume", "amount")
# → 新增 volume_cumsum、amount_cumsum
```

### 3.3 排名函数

> 排名函数使用 `.over()` 指定排序，不需要先行的 `.sort()`。

排名函数在分区内为行分配位置或名次，必须配合 `.over()` 指定排序。

#### `row_number`
- **签名**: `row_number().over(partition_by=None, order_by=Expr, descending=False) -> Expr`
- **行为**: 为每行分配唯一的连续编号（1, 2, 3, ...）。与 `rank()` 不同，相同值也会得到不同编号
- **参数**: 通过 `.over()` 指定 `partition_by`（可选）、`order_by`（必需）、`descending`（可选）
- **返回**: 整数表达式
- **异常**: `RuntimeError`（未指定 order_by）
- **示例**:
```python
from ltseq import row_number

# 简单行编号
t.derive(rn=lambda r: row_number().over(order_by=r.date))

# 分区内行编号
t.derive(rn=lambda r: row_number().over(partition_by=r.group, order_by=r.date))
```

#### `rank`
- **签名**: `rank().over(partition_by=None, order_by=Expr, descending=False) -> Expr`
- **行为**: 分配带跳号的排名。相同值得到相同名次，其后的名次被跳过（1, 2, 2, 4, 5）
- **参数**: 通过 `.over()` 指定 `partition_by`（可选）、`order_by`（必需）、`descending`（可选）
- **返回**: 整数表达式
- **异常**: `RuntimeError`（未指定 order_by）
- **示例**:
```python
from ltseq import rank

# 按分数排名（并列同名次，随后跳号）
t.derive(rnk=lambda r: rank().over(order_by=r.score))

# 部门内排名
t.derive(rnk=lambda r: rank().over(partition_by=r.dept, order_by=r.salary, descending=True))
```

#### `dense_rank`
- **签名**: `dense_rank().over(partition_by=None, order_by=Expr, descending=False) -> Expr`
- **行为**: 分配不跳号的排名。相同值得到相同名次，其后名次不跳过（1, 2, 2, 3, 4）
- **参数**: 通过 `.over()` 指定 `partition_by`（可选）、`order_by`（必需）、`descending`（可选）
- **返回**: 整数表达式
- **异常**: `RuntimeError`（未指定 order_by）
- **示例**:
```python
from ltseq import dense_rank

# 按分数密集排名（并列后不跳号）
t.derive(drnk=lambda r: dense_rank().over(order_by=r.score))

# 部门内密集排名
t.derive(drnk=lambda r: dense_rank().over(partition_by=r.dept, order_by=r.salary))
```

#### `ntile`
- **签名**: `ntile(n: int).over(partition_by=None, order_by=Expr, descending=False) -> Expr`
- **行为**: 将行划分为 `n` 个大致等量的桶（1 到 n）。适用于分位数计算
- **参数**: `n` 桶数；通过 `.over()` 指定分区/排序
- **返回**: 整数表达式（桶号 1 到 n）
- **异常**: `ValueError`（n <= 0），`RuntimeError`（未指定 order_by）
- **示例**:
```python
from ltseq import ntile

# 四分位
t.derive(quartile=lambda r: ntile(4).over(order_by=r.score))

# 组内十分位
t.derive(decile=lambda r: ntile(10).over(partition_by=r.group, order_by=r.value))
```

#### `CallExpr.over`（窗口规格）
- **签名**: `expr.over(partition_by: Expr | None = None, order_by: Expr | None = None, descending: bool = False) -> WindowExpr`
- **行为**: 为排名函数应用窗口规格。`partition_by` 和 `order_by` 各接受**单个列表达式**；`descending` 是作用于 `order_by` 的单个布尔值
- **参数**:
  - `partition_by` 分区列（可选）
  - `order_by` 排序列（排名函数必需）
  - `descending` order_by 的排序方向（默认 False）
- **返回**: 可用于 derive() 的 `WindowExpr`
- **异常**: `TypeError`（partition_by/order_by 类型无效）
- **示例**:
```python
t.derive(rn=lambda r: row_number().over(
    partition_by=r.region,
    order_by=r.date,
    descending=True,
))
```

### 3.4 有序查找

#### `LTSeq.search_first`
- **签名**: `LTSeq.search_first(predicate: Callable[[Row], Expr]) -> LTSeq`
- **行为**: 返回第一个匹配行（单行 LTSeq）；对已排序数据可做二分查找
- **参数**: `predicate` 行谓词
- **返回**: 单行 `LTSeq`（未找到时为空）
- **异常**: `ValueError`（schema 未初始化），`TypeError`（谓词无效），`RuntimeError`（谓词无法转译）
- **示例**:
```python
first_big = t.sort("price").search_first(lambda r: r.price > 100)
```

#### `LTSeq.search_pattern`
- **签名**: `LTSeq.search_pattern(*step_predicates: Callable, partition_by: str | None = None) -> LTSeq`
- **行为**: 查找**连续行**依次匹配一组谓词的位置（漏斗/序列匹配）。返回第 1 步匹配的行，即满足 `step1(i), step2(i+1), ..., stepN(i+N-1)` 全部成立的行 `i`。指定 `partition_by` 时模式不跨分区边界
- **参数**: `step_predicates` 每步一个 lambda；`partition_by` 可选分区列
- **返回**: 第 1 步所在行组成的 `LTSeq`
- **异常**: `ValueError`（无谓词或 schema 未初始化）
- **示例**:
```python
# 按用户查找三步 URL 漏斗
matches = t.search_pattern(
    lambda r: r.url.s.starts_with("/landing"),
    lambda r: r.url.s.starts_with("/product"),
    lambda r: r.url.s.starts_with("/checkout"),
    partition_by="userid",
)
```

#### `LTSeq.search_pattern_count`
- **签名**: `LTSeq.search_pattern_count(*step_predicates: Callable, partition_by: str | None = None) -> int`
- **行为**: 同 `search_pattern`，但只返回匹配数量
- **返回**: `int`
- **示例**:
```python
n = t.search_pattern_count(
    lambda r: r.event == "view",
    lambda r: r.event == "buy",
    partition_by="userid",
)
```

#### `LTSeq.align`
- **签名**: `LTSeq.align(ref_sequence: list[Any], key: Callable[[Row], Expr]) -> LTSeq`
- **行为**: 按 `ref_sequence` 的顺序对齐，缺失键处填 NULL
- **参数**: `ref_sequence` 参考键序列；`key` 键提取器
- **返回**: 对齐后的 `LTSeq`
- **异常**: `TypeError`（键无效），`ValueError`（ref_sequence 为空）
- **示例**:
```python
aligned = t.align(["2024-01-01", "2024-01-02"], key=lambda r: r.date)
```

### 3.5 物理行序操作

#### `LTSeq.rvs`
- **签名**: `LTSeq.rvs() -> LTSeq`
- **行为**: 反转表的行序
- **示例**:
```python
reversed_t = t.sort("date").rvs()
```

#### `LTSeq.step`
- **签名**: `LTSeq.step(n: int) -> LTSeq`
- **行为**: 每隔 n 行取一行（0-based：第 0, n, 2n, ... 行）
- **参数**: `n` 步长（必须 >= 1）
- **异常**: `ValueError`（n < 1）
- **示例**:
```python
every_other = t.step(2)   # 第 0, 2, 4, ... 行
every_tenth = t.step(10)
```

## 4. 有序分组与过程化计算

### `LTSeq.group_ordered`（别名: `group_consecutive`）
- **签名**: `LTSeq.group_ordered(key: Callable[[Row], Expr]) -> NestedTable`
- **行为**: 只把连续相等的值归为一组；不重排数据。`group_consecutive` 是同一方法的更直白别名
- **参数**: `key` 分组键表达式
- **返回**: `NestedTable`（组级操作）
- **异常**: `ValueError`（schema 未初始化），`TypeError`（键无效），`AttributeError`（列不存在）
- **示例**:
```python
groups = t.sort("date").group_ordered(lambda r: r.is_up)
# 等价：
groups = t.sort("date").group_consecutive(lambda r: r.is_up)
```

### `LTSeq.group_sorted`
- **签名**: `LTSeq.group_sorted(key: Callable[[Row], Expr]) -> NestedTable`
- **行为**: 假定数据已按键全局排序；单遍分组，无需哈希
- **参数**: `key` 分组键表达式
- **返回**: `NestedTable`
- **异常**: `ValueError`（未排序或 schema 未初始化），`TypeError`（键无效）
- **示例**:
```python
groups = t.sort("user_id").group_sorted(lambda r: r.user_id)
```

### `NestedTable`（由 `group_ordered` / `group_sorted` 产生）

#### `NestedTable.first`
- **签名**: `nested.first() -> LTSeq`
- **行为**: 每组的第一行
- **返回**: 首行组成的 `LTSeq`（每组一行）
- **示例**:
```python
first_rows = groups.first()
```

#### `NestedTable.last`
- **签名**: `nested.last() -> LTSeq`
- **行为**: 每组的最后一行
- **返回**: 末行组成的 `LTSeq`（每组一行）
- **示例**:
```python
last_rows = groups.last()
```

#### `NestedTable.flatten`
- **签名**: `nested.flatten() -> LTSeq`
- **行为**: 返回带 `__group_id__` 列的底层行，该列标识每个连续分组（另含内部辅助列 `__group_count__` 与 `__rn__`）
- **返回**: 展平后的 `LTSeq`
- **示例**:
```python
flat = groups.flatten()
```

#### `NestedTable.filter`
- **签名**: `nested.filter(predicate: Callable[[GroupProxy], Expr]) -> NestedTable`
- **行为**: 只保留满足组级谓词的组；保留组的全部行
- **参数**: `predicate` 组谓词（`g` 为组代理，见下）
- **返回**: 过滤后的 `NestedTable`
- **异常**: `TypeError`（谓词无效），`RuntimeError`（执行失败）
- **示例**:
```python
big_groups = groups.filter(lambda g: g.count() > 3)
```

#### `NestedTable.derive`
- **签名**: `nested.derive(func: Callable[[GroupProxy], dict[str, Expr]]) -> LTSeq`
- **行为**: 计算组级值并**广播到组内每一行**。结果保留所有原始行和列，外加新列。（若需坍缩为每组一行，可对派生列接 `distinct()`，或使用 `first()`/`last()`；哈希式的每组一行聚合见第 7 节 `agg()`/`group_by()`）
- **参数**: `func` 返回组表达式字典
- **返回**: 原始行 + 广播组级列的 `LTSeq`
- **异常**: `ValueError`（lambda 未返回字典），`RuntimeError`（执行失败）
- **示例**:
```python
# 每行都拿到所在组的大小与起止
enriched = groups.derive(lambda g: {
    "group_size": g.count(),
    "start": g.first().date,
    "end": g.last().date,
})
```

#### `len(nested)` / `NestedTable.to_pandas`
- **签名**: `NestedTable.__len__() -> int`；`NestedTable.to_pandas()`
- **行为**: `len()` 返回底层表的**行数**（不是组数）；`to_pandas()` 原样物化底层行，**不含** `__group_id__` 列（需要该列请用 `flatten().to_pandas()`）
- **示例**:
```python
n_rows = len(groups)                  # 行数，不是组数
df = groups.flatten().to_pandas()     # 带 __group_id__ 的行
```

### 组代理（NestedTable filter/derive 中的 `g`）

组聚合以**字符串列名**为参数；`first()`/`last()` 返回可属性访问的行代理。

#### 聚合: `g.count()`、`g.sum()`、`g.avg()`、`g.min()`、`g.max()`
- **签名**: `g.count() -> Expr`；`g.sum(column: str) -> Expr`（`avg`/`min`/`max` 同）
- **行为**: 组内行数 / 对单列的组内聚合
- **示例**:
```python
groups.derive(lambda g: {"avg_price": g.avg("price"), "hi": g.max("price")})
groups.filter(lambda g: g.sum("amount") > 1000)
```

#### 行访问: `g.first()`、`g.last()`
- **签名**: `g.first() -> RowProxy`；`g.last() -> RowProxy`
- **行为**: 组的第一行/最后一行；以属性访问列
- **示例**:
```python
groups.derive(lambda g: {"start": g.first().date, "end": g.last().date})
```

#### 量词（仅 filter）: `g.all()`、`g.any()`、`g.none()`
- **签名**: `g.all(predicate: Callable[[Row], Expr]) -> Expr`（`any`/`none` 同）
- **行为**: 组内所有行 / 至少一行 / 没有任何行满足行级谓词时为 True。用于 `NestedTable.filter`
- **参数**: `predicate` 行级谓词（可使用完整的行表达式能力）
- **示例**:
```python
# 所有金额均为正的组
groups.filter(lambda g: g.all(lambda r: r.amount > 0))

# 至少包含一条错误记录的组
groups.filter(lambda g: g.any(lambda r: r.status == "error"))

# 不含已删除行的组
groups.filter(lambda g: g.none(lambda r: r.is_deleted == True))
```

## 5. 集合运算

### `LTSeq.union`
- **签名**: `LTSeq.union(other: LTSeq) -> LTSeq`
- **行为**: 纵向拼接，**保留重复行**（SQL UNION ALL 语义）
- **参数**: `other` 另一个 schema 相同的 LTSeq
- **返回**: 合并后的 `LTSeq`
- **异常**: `TypeError`（other 非 LTSeq），`ValueError`（schema 不匹配）
- **示例**:
```python
combined = t1.union(t2)
```

### `LTSeq.intersect`
- **签名**: `LTSeq.intersect(other: LTSeq, on: Callable | str | None = None) -> LTSeq`
- **行为**: 两表交集
- **参数**: `other` 另一个表；`on` 键选择器（None 表示全列）
- **返回**: 交集 `LTSeq`
- **异常**: `TypeError`（other 非 LTSeq 或 on 无效），`ValueError`（schema 未初始化）
- **示例**:
```python
common = t1.intersect(t2, on=lambda r: r.id)
```

### `LTSeq.except_`（差集）
- **签名**: `LTSeq.except_(other: LTSeq, on: Callable | str | None = None) -> LTSeq`
- **行为**: 左表中有而右表中没有的行（SQL EXCEPT 语义）
- **参数**: `other` 另一个表；`on` 键选择器
- **返回**: 差集 `LTSeq`
- **SQL 等价**: `EXCEPT` / `MINUS`
- **异常**: `TypeError`（other 非 LTSeq 或 on 无效），`ValueError`（schema 未初始化）
- **示例**:
```python
only_left = t1.except_(t2, on=lambda r: r.id)
```

> 注意：`Expr.diff()`（行级差分，第 3 节）与表级差集无关。表级差集请使用 `except_()`。

### `LTSeq.xunion`（对称差）
- **签名**: `LTSeq.xunion(other: LTSeq, on: Callable | str | None = None) -> LTSeq`
- **行为**: 只在其中一个表中出现的行；等价于 `(a except b) union (b except a)`
- **参数**: `other` 另一个表；`on` 键选择器（None 表示全列）
- **返回**: 对称差 `LTSeq`
- **示例**:
```python
unique_to_either = t1.xunion(t2, on=lambda r: r.id)
```

### `LTSeq.is_subset`
- **签名**: `LTSeq.is_subset(other: LTSeq, on: Callable | str | None = None) -> bool`
- **行为**: 检查本表是否为另一个表的子集
- **参数**: `other` 另一个表；`on` 键选择器
- **返回**: `bool`
- **异常**: `TypeError`（other 非 LTSeq 或 on 无效），`ValueError`（schema 未初始化）
- **示例**:
```python
flag = t_small.is_subset(t_big, on=lambda r: r.id)
```

### `LTSeq.contain`
- **签名**: `LTSeq.contain(key_col: str, *values) -> bool`
- **行为**: 检查给定的**所有**值是否都出现在该列中
- **参数**: `key_col` 列名；`values` 待查找的值
- **返回**: `bool`（全部找到为 True；values 为空时为 True）
- **异常**: `AttributeError`（列不存在）
- **示例**:
```python
t.contain("status", "active", "pending")
t.contain("id", 1, 2, 3)
```

## 6. 关联与连接

### `LTSeq.join`
- **签名**: `LTSeq.join(other: LTSeq, on: Callable | str | list[str] | None = None, how: str = "inner", strategy: str | None = None, *, left_on=None, right_on=None, suffix: str = "_right") -> LTSeq`
- **行为**: 两表连接。默认哈希连接（无需排序）；`strategy="merge"` 对预排序输入做归并连接并校验排序状态。**列命名（Polars 语义）**：与左表冲突的右列保留原名并加 `suffix`（如 `val` → `val_right`），不冲突的右列保持原名。inner/left 连接会丢弃重复的右键列（合并），right/full 连接保留两侧键列（右键冲突时加后缀）
- **参数**: `other` 另一个表；`on` 等值连接的列名 / 列名列表，或双参 lambda（如 `lambda a, b: a.id == b.id`）表达任意条件；`left_on`/`right_on` 异名键的列名；`how` 取值 {inner,left,right,full}；`strategy` 取值 {None,"hash","merge"}；`suffix` 冲突右列的后缀（默认 `"_right"`）
- **返回**: 连接后的 `LTSeq`
- **异常**: `TypeError`（other/on 无效），`ValueError`（how/strategy 无效、merge 输入未排序、列不存在或后缀冲突）
- **示例**:
```python
# 字符串键捷径（同名列）
users.join(orders, on="id", how="left")

# 异名键 + 自定义后缀
users.join(orders, left_on="id", right_on="user_id", suffix="_o")

# 复合键
a.join(b, on=["region", "year"])

# 任意条件（逃生舱）
users.join(orders, on=lambda u, o: u.id == o.user_id, how="left")
```

### `LTSeq.asof_join`
- **签名**: `LTSeq.asof_join(other: LTSeq, on: Callable, direction: str = "backward", is_sorted: bool = False, *, suffix: str = "_right") -> LTSeq`
- **行为**: 时序就近连接（as-of join）。`is_sorted=True` 时跳过排序校验（已知双方有序时更快）。冲突右列加 `suffix`（Polars 语义）；与等值连接不同，右侧时间列会保留（as-of 是近似匹配，匹配到的时间戳是真实信息）
- **参数**: `other` 另一个表；`on` 连接条件（如 `lambda t, q: t.time >= q.time`）；`direction` 取值 {"backward","forward","nearest"}；`is_sorted` 跳过排序检查；`suffix` 冲突右列的后缀
- **返回**: as-of 连接后的 `LTSeq`
- **异常**: `TypeError`（other/on 无效），`ValueError`（direction 无效）
- **示例**:
```python
result = trades.asof_join(quotes, on=lambda t, q: t.time >= q.time, direction="backward")
```

### `LTSeq.semi_join`
- **签名**: `LTSeq.semi_join(other: LTSeq, on: Callable | str | list[str]) -> LTSeq`
- **行为**: 返回左表中键存在于右表的行。只返回左表列，多次匹配不产生重复
- **参数**: `other` 用于匹配的右表；`on` 等值连接的列名（列表）或双参 lambda 条件
- **返回**: 左表匹配行组成的 `LTSeq`
- **异常**: `TypeError`（other/on 无效），`ValueError`（schema 未初始化）
- **示例**:
```python
# 至少下过一单的用户
active_users = users.semi_join(orders, on=lambda u, o: u.id == o.user_id)
```

### `LTSeq.anti_join`
- **签名**: `LTSeq.anti_join(other: LTSeq, on: Callable | str | list[str]) -> LTSeq`
- **行为**: 返回左表中键**不**存在于右表的行。只返回左表列
- **参数**: `other` 用于匹配的右表；`on` 等值连接的列名（列表）或双参 lambda 条件
- **返回**: 左表未匹配行组成的 `LTSeq`
- **异常**: `TypeError`（other/on 无效），`ValueError`（schema 未初始化）
- **示例**:
```python
# 从未下单的用户
inactive_users = users.anti_join(orders, on=lambda u, o: u.id == o.user_id)
```

### `LTSeq.link`
- **签名**: `LTSeq.link(target_table: LTSeq, on: Callable, as_: str, join_type: str = "inner") -> LinkedTable`
- **行为**: 指针式关联；按需物化；通过别名访问目标表的列
- **参数**: `target_table` 目标表；`on` 连接条件；`as_` 别名；`join_type` 取值 {inner,left,right,full}
- **返回**: `LinkedTable`
- **异常**: `TypeError`（on 无效），`ValueError`（join_type 无效或 schema 未初始化）
- **示例**:
```python
linked = orders.link(products, on=lambda o, p: o.product_id == p.id, as_="prod")
result = linked.select(lambda r: [r.id, r.prod.name, r.prod.price])
```

### `LinkedTable`
- **行为**: 关联表对上的可链式视图。支持 `select`、`filter`、`derive`、`sort`、`slice`、`distinct`、`show`，以及继续 `link`（多跳链式关联）。仅在需要时通过底层连接物化
- **示例**:
```python
linked = orders.link(products, on=lambda o, p: o.product_id == p.id, as_="prod")
cheap = linked.filter(lambda r: r.prod.price < 10)
chained = linked.link(categories, on=lambda o, c: o.category_id == c.id, as_="cat")
```

完整说明见 `docs/LINKING_GUIDE.cn.md`。

### `r.col.lookup`（表达式级 lookup）
- **签名**: `r.col.lookup(target_table: LTSeq, column: str, join_key: str | None = None) -> Expr`
- **行为**: 表达式内的轻量查表，从关联表取回单列值。可作用于任意表达式（列或变换后的值）。在 `derive()` 中通过连接解析
- **参数**: `target_table` 目标表；`column` 输出列；`join_key` 目标表中的连接键（可选）
- **返回**: lookup 表达式
- **异常**: `TypeError`（参数无效），`RuntimeError`（执行失败）
- **示例**:
```python
enriched = orders.derive(product_name=lambda r: r.product_id.lookup(products, "name"))

# 对变换后的键做 lookup
enriched = orders.derive(
    product_name=lambda r: r.product_id.lower().lookup(products, "name")
)
```

### 连接策略速查

| 方法 | 适用场景 | 算法 | SQL 等价 |
| --- | --- | --- | --- |
| `join` | 未排序的一般数据 | Hash Join | `JOIN` |
| `join(..., strategy="merge")` | 预排序的大表 | Merge Join | `JOIN`（优化）|
| `semi_join` | 按键存在性过滤 | Hash Semi-Join | `WHERE EXISTS` |
| `anti_join` | 按键不存在性过滤 | Hash Anti-Join | `WHERE NOT EXISTS` |
| `link` | 事实表→维表指针访问 | Pointer | `LEFT JOIN`（惰性）|
| `r.col.lookup(...)` | 从维表取单列 | derive 中的连接 | `LEFT JOIN`（单列）|
| `asof_join` | 金融时序 | 有序查找 | `LATERAL JOIN` |

## 7. 聚合、分区、透视

### `LTSeq.group_by`（链式）
- **签名**: `LTSeq.group_by(key: str | Callable) -> GroupBy`；`GroupBy.agg(**aggregations: Callable) -> LTSeq`
- **行为**: Polars/pandas 风格的两段式分组聚合。`key` 可以是列名字符串或 lambda；`agg` 每组产出一行
- **参数**: `key` 分组键；`aggregations` 命名聚合 lambda
- **返回**: `GroupBy` 中间对象，随后是聚合后的 `LTSeq`
- **异常**: `TypeError`（键无效），`AttributeError`（列不存在），`ValueError`（无聚合表达式）
- **示例**:
```python
summary = t.group_by("region").agg(
    total=lambda g: g.sales.sum(),
    avg_price=lambda g: g.price.avg(),
)

# 表达式分组键
summary = t.group_by(lambda r: r.date.dt.year()).agg(n=lambda g: g.id.count())
```

### `LTSeq.agg`
- **签名**: `LTSeq.agg(by: Callable | None = None, **aggs: Callable) -> LTSeq`
- **行为**: 分组聚合（`by=None` 时为全表聚合）；每组一行。`by` 必须是 lambda（字符串列名请用 `group_by`）
- **参数**: `by` 分组键 lambda；`aggs` 聚合表达式
- **返回**: 聚合后的 `LTSeq`
- **异常**: `ValueError`（schema 未初始化或无聚合表达式），`TypeError`（by/聚合非 callable）
- **示例**:
```python
summary = t.agg(by=lambda r: r.region, total=lambda g: g.sales.sum())

# 全表聚合
total = t.agg(total=lambda g: g.sales.sum())
```

### 聚合列方法（`agg` / `group_by().agg()` 的 lambda 内）
- **签名**: `g.col.sum() / .avg() / .count() / .min() / .max() / .median() / .var() / .variance() / .std() / .stddev() / .percentile(p)`
- **行为**: 聚合上下文可用的列聚合。`var`/`variance` 为样本方差；`std`/`stddev` 为样本标准差；`percentile(p)` 的 `p` 取 0–1（近似分位数）
- **示例**:
```python
stats = t.group_by("region").agg(
    total=lambda g: g.sales.sum(),
    med=lambda g: g.sales.median(),
    sd=lambda g: g.sales.std(),
    p95=lambda g: g.latency.percentile(0.95),
    n=lambda g: g.id.count(),
)
```

### 条件聚合函数
- **签名**: `count_if(predicate: Expr)`、`sum_if(predicate: Expr, column: Expr)`、`avg_if(...)`、`min_if(...)`、`max_if(...)`
- **行为**: 只对谓词成立的行聚合。用于 `agg`/`group_by().agg()` 的 lambda 内
- **导入**: `from ltseq import count_if, sum_if, avg_if, min_if, max_if`
- **示例**:
```python
from ltseq import count_if, sum_if

result = t.group_by("region").agg(
    high_count=lambda g: count_if(g.price > 100),
    high_sales=lambda g: sum_if(g.price > 100, g.quantity),
)
```

### 统计聚合函数
- **签名**: `skew(col: Expr)`、`corr(a: Expr, b: Expr)`、`covar(a: Expr, b: Expr)`、`concat_agg(col: Expr, delimiter: str = ",")`
- **行为**: 偏度、皮尔逊相关系数、样本协方差、字符串拼接聚合。用于 `agg`/`group_by().agg()` 的 lambda 内
- **导入**: `from ltseq import skew, corr, covar, concat_agg`
- **示例**:
```python
from ltseq import corr, concat_agg

result = t.group_by("region").agg(
    price_qty_corr=lambda g: corr(g.price, g.quantity),
    names=lambda g: concat_agg(g.name, delimiter="|"),
)
```

### `LTSeq.partition`
- **签名**: `LTSeq.partition(*cols: str) -> PartitionedTable` 或 `LTSeq.partition(by: Callable) -> PartitionedTable`
- **行为**: 按键拆分为子表（不聚合）。callable 键必须是**简单列表达式**（如 `lambda r: r.region`）；派生表达式（如 `lambda r: r.price + 1`）会抛 `ValueError`（会迫使内部物化，已不支持）
- **参数**: 列名、单个 callable，或 `by=` callable
- **返回**: `PartitionedTable`（字典式：键 → LTSeq）
- **异常**: `TypeError`（参数无效），`AttributeError`（列不存在），`ValueError`（schema 未初始化，或 callable 不是简单列表达式）
- **示例**:
```python
parts = t.partition("region")
west = parts["West"]

parts = t.partition("year", "region")       # 多列
parts = t.partition(by=lambda r: r.region)  # lambda 键
```

### `PartitionedTable`
- **行为**: 字典式分区容器。支持 `parts[key]`、迭代、`keys()`、`values()`、`items()`、`map(fn)`（对每个分区应用函数）与 `to_list()`
- **示例**:
```python
for key, sub in parts.items():
    print(key, len(sub))

filtered = parts.map(lambda sub: sub.filter(lambda r: r.amount > 0))
```

### `LTSeq.pivot`
- **签名**: `LTSeq.pivot(index: str | list[str], columns: str, values: str, agg_fn: str = "sum") -> LTSeq`
- **行为**: 长表转宽表
- **参数**:
  - `index` 行索引列
  - `columns` 透视列（取值变为列名）
  - `values` 待聚合的值列
  - `agg_fn` 聚合函数：`"sum"` | `"mean"` | `"min"` | `"max"` | `"count"` | `"first"` | `"last"`（默认 `"sum"`）
- **返回**: 透视后的 `LTSeq`
- **异常**: `ValueError`（agg_fn 无效或 schema 未初始化），`AttributeError`（列不存在）
- **示例**:
```python
pivoted = t.pivot(index="date", columns="region", values="amount", agg_fn="sum")
```

## 8. 表达式 API（lambda 内部）

### 表达式运算符
- **签名**: `+ - * / // %`、`== != > >= < <=`、`& | ~`
- **行为**: 构建表达式树，不在 Python 侧执行
- **参数**: 左右操作数（Expr 或字面量）
- **返回**: 表达式对象
- **异常**: `TypeError`（类型不匹配）
- **示例**:
```python
expr = (r.price * r.qty) > 100
```

### `if_else`
- **签名**: `if_else(condition: Expr, true_value: Any, false_value: Any) -> Expr`
- **行为**: 条件表达式（SQL CASE WHEN）
- **参数**: `condition`、`true_value`、`false_value`
- **返回**: 表达式
- **导入**: `from ltseq import if_else`
- **异常**: `TypeError`（condition 非布尔）
- **示例**:
```python
from ltseq import if_else
status = if_else(r.amount > 100, "VIP", "Normal")
```

### `ifa` / `nvl` / `coalesce`
- **签名**: `ifa(cond: Expr, value: Expr) -> Expr`；`nvl(x: Expr, default: Expr) -> Expr`；`coalesce(*args: Expr) -> Expr`
- **行为**: `ifa(cond, v)` = `if_else(cond, v, NULL)`；`nvl(x, d)` = `coalesce(x, d)`；`coalesce` 返回第一个非 NULL 参数
- **导入**: `from ltseq import ifa, nvl, coalesce`
- **示例**:
```python
from ltseq import nvl, coalesce
t.derive(price2=lambda r: nvl(r.price, 0))
t.derive(contact=lambda r: coalesce(r.mobile, r.phone, r.email))
```

### NULL / 成员 / 类型辅助（`Expr` 方法）

#### `r.col.fill_null`
- **签名**: `r.col.fill_null(default: Any) -> Expr`
- **行为**: NULL 填充（SQL COALESCE）
- **示例**:
```python
safe_price = r.price.fill_null(0)
```

#### `r.col.is_null` / `r.col.is_not_null`
- **签名**: `r.col.is_null() -> Expr`；`r.col.is_not_null() -> Expr`
- **行为**: NULL / NOT NULL 判断
- **示例**:
```python
missing = t.filter(lambda r: r.email.is_null())
valid = t.filter(lambda r: r.email.is_not_null())
```

#### `r.col.is_in`
- **签名**: `r.col.is_in(values: list[Any]) -> Expr`
- **行为**: 成员判断（SQL `IN`）
- **示例**:
```python
t.filter(lambda r: r.status.is_in(["active", "pending", "review"]))
```

#### `r.col.between`
- **签名**: `r.col.between(low: Any, high: Any) -> Expr`
- **行为**: 闭区间判断，等价于 `(x >= low) & (x <= high)`
- **示例**:
```python
t.filter(lambda r: r.price.between(10, 100))
```

#### `r.col.cast`
- **签名**: `r.col.cast(dtype: str) -> Expr`
- **行为**: 类型转换。支持：`"int32"`、`"int64"`、`"float32"`、`"float64"`、`"utf8"`/`"string"`、`"bool"`、`"date32"`、`"timestamp"`
- **示例**:
```python
t.derive(amount=lambda r: r.amount_str.cast("float64"))
```

#### `r.col.abs` / `r.col.round` / `r.col.floor` / `r.col.ceil`
- **签名**: `r.col.abs() -> Expr`；`r.col.round(decimals: int = 0) -> Expr`；`r.col.floor() -> Expr`；`r.col.ceil() -> Expr`
- **行为**: 数值取整辅助函数
- **示例**:
```python
t.derive(
    abs_change=lambda r: (r.price - r.prev).abs(),
    rounded=lambda r: r.score.round(2),
)
```

### 字符串操作（`r.col.s.*`）

#### `contains`
- **签名**: `r.col.s.contains(pattern: str) -> Expr`
- **行为**: 子串包含判断
- **示例**:
```python
gmail = t.filter(lambda r: r.email.s.contains("gmail"))
```

#### `starts_with` / `ends_with`
- **签名**: `r.col.s.starts_with(prefix: str) -> Expr`；`r.col.s.ends_with(suffix: str) -> Expr`
- **行为**: 前缀 / 后缀匹配
- **示例**:
```python
orders = t.filter(lambda r: r.code.s.starts_with("ORD"))
pdfs = t.filter(lambda r: r.filename.s.ends_with(".pdf"))
```

#### `lower` / `upper`
- **签名**: `r.col.s.lower() -> Expr`；`r.col.s.upper() -> Expr`
- **行为**: 大小写转换
- **示例**:
```python
normalized = t.derive(email_lower=lambda r: r.email.s.lower())
```

#### `strip` / `lstrip` / `rstrip`
- **签名**: `r.col.s.strip() -> Expr`；`r.col.s.lstrip() -> Expr`；`r.col.s.rstrip() -> Expr`
- **行为**: 去除空白（两侧 / 仅左侧 / 仅右侧）
- **SQL 等价**: `TRIM` / `LTRIM` / `RTRIM`
- **示例**:
```python
clean = t.derive(name_clean=lambda r: r.name.s.strip())
```

#### `len`
- **签名**: `r.col.s.len() -> Expr`
- **行为**: 字符串长度
- **示例**:
```python
long_names = t.filter(lambda r: r.name.s.len() > 50)
```

#### `slice`
- **签名**: `r.col.s.slice(start: int, length: int) -> Expr`
- **行为**: 子串截取
- **参数**: `start` 起始下标（0-based）；`length` 长度
- **示例**:
```python
year = t.derive(year=lambda r: r.date.s.slice(0, 4))
```

#### `regex_match`
- **签名**: `r.col.s.regex_match(pattern: str) -> Expr`
- **行为**: 正则匹配（布尔）
- **异常**: `ValueError`（正则无效）
- **示例**:
```python
valid = t.filter(lambda r: r.email.s.regex_match(r"^[a-z]+@"))
```

#### `like`
- **签名**: `r.col.s.like(pattern: str) -> Expr`
- **行为**: SQL LIKE 模式匹配（`%` 任意字符，`_` 单个字符）
- **示例**:
```python
t.filter(lambda r: r.code.s.like("ORD-%"))
```

#### `replace`
- **签名**: `r.col.s.replace(old: str, new: str) -> Expr`
- **行为**: 替换所有出现的子串
- **示例**:
```python
clean = t.derive(clean_name=lambda r: r.name.s.replace("-", "_"))
```

#### `concat`
- **签名**: `r.col.s.concat(*others) -> Expr`
- **行为**: 与其他字符串或列表达式拼接
- **示例**:
```python
greeting = t.derive(msg=lambda r: r.name.s.concat(" says hello"))
full = t.derive(full_name=lambda r: r.first.s.concat(" ", r.last))
```

#### `pad_left` / `pad_right`
- **签名**: `r.col.s.pad_left(width: int, char: str = " ") -> Expr`；`r.col.s.pad_right(width: int, char: str = " ") -> Expr`
- **行为**: 填充到指定宽度。注意：字符串超出宽度时会被截断（SQL LPAD/RPAD 行为）
- **示例**:
```python
padded = t.derive(padded_id=lambda r: r.id.s.pad_left(5, "0"))
```

#### `split`
- **签名**: `r.col.s.split(delimiter: str, index: int) -> Expr`
- **行为**: 按分隔符拆分并返回指定位置的部分。下标为 **1-based**（1 = 第一段），与 SQL SPLIT_PART 一致。越界返回空字符串
- **异常**: `ValueError`（index <= 0）
- **示例**:
```python
# 从 "user@example.com" 取域名
domain = t.derive(domain=lambda r: r.email.s.split("@", 2))
```

#### `pos`
- **签名**: `r.col.s.pos(sub: str) -> Expr`
- **行为**: 返回子串首次出现的 **1-based** 位置；未找到返回 0
- **SQL 等价**: `STRPOS(col, sub)`
- **示例**:
```python
at_pos = t.derive(pos=lambda r: r.email.s.pos("@"))  # "user@example" → 5
```

#### `left` / `right`
- **签名**: `r.col.s.left(n: int) -> Expr`；`r.col.s.right(n: int) -> Expr`
- **行为**: 最左 / 最右 `n` 个字符；`n` 超长时返回整串
- **SQL 等价**: `LEFT(col, n)` / `RIGHT(col, n)`
- **示例**:
```python
prefix = t.derive(pfx=lambda r: r.code.s.left(3))    # "ABC123" → "ABC"
suffix = t.derive(sfx=lambda r: r.code.s.right(3))   # "ABC123" → "123"
```

#### `asc`
- **签名**: `r.col.s.asc() -> Expr`
- **行为**: 返回字符串首字符的 ASCII/Unicode 码点（类似 Python 的 `ord()`）
- **SQL 等价**: `ASCII(col)`
- **示例**:
```python
code = t.derive(code=lambda r: r.ch.s.asc())  # "A" → 65, "a" → 97
```

#### `isalpha` / `isdigit` / `islower` / `isupper`
- **签名**: `r.col.s.isalpha() -> Expr`（其余同）
- **行为**: 字符类别判断，与 Python `str` 同名方法对应
- **示例**:
```python
numeric_codes = t.filter(lambda r: r.code.s.isdigit())
```

### 字符串全局函数

#### `str_char`
- **签名**: `str_char(n: Expr) -> Expr`
- **行为**: 将 Unicode 码点整数转换为单字符字符串（类似 Python 的 `chr()`）
- **SQL 等价**: `CHR(n)`
- **导入**: `from ltseq.expr import str_char`
- **示例**:
```python
from ltseq.expr import str_char
ch = t.derive(ch=lambda r: str_char(r.code))  # 65 → "A", 97 → "a"
```

#### `concat_ws`
- **签名**: `concat_ws(delimiter: str, *cols: Expr) -> Expr`
- **行为**: 以分隔符拼接两个及以上的列表达式
- **SQL 等价**: `CONCAT_WS(delimiter, col1, col2, ...)`
- **导入**: `from ltseq.expr import concat_ws`
- **示例**:
```python
from ltseq.expr import concat_ws
full = t.derive(full_name=lambda r: concat_ws(" ", r.first, r.last))
```

### 时间操作（`r.col.dt.*`）

#### `year` / `month` / `day`
- **签名**: `r.col.dt.year() -> Expr`（month/day 同）
- **行为**: 提取日期分量
- **示例**:
```python
by_date = t.derive(year=lambda r: r.date.dt.year())
```

#### `hour` / `minute` / `second` / `millisecond`
- **签名**: `r.col.dt.hour() -> Expr`（minute/second/millisecond 同）
- **行为**: 提取时间分量（`millisecond` 返回 0–999）
- **示例**:
```python
with_time = t.derive(hour=lambda r: r.ts.dt.hour())
```

#### `weekday`
- **签名**: `r.col.dt.weekday() -> Expr`
- **行为**: 提取星期（0-based：周一=0 ... 周日=6）
- **示例**:
```python
t.derive(wd=lambda r: r.date.dt.weekday())
```

#### `add`
- **签名**: `r.col.dt.add(days: int = 0, months: int = 0, years: int = 0, hours: int = 0, minutes: int = 0, seconds: int = 0, weeks: int = 0) -> Expr`
- **行为**: 日期/时间运算。`weeks` 转换为 `days × 7`；`hours/minutes/seconds` 内部使用纳秒精度
- **参数**: 整数偏移量（默认全为 0；负数表示减）
- **返回**: 日期/时间戳表达式
- **SPL 等价**: `elapse(t, k, unit)`
- **示例**:
```python
delivery   = t.derive(delivery=lambda r: r.order_date.dt.add(days=5))
after_2h   = t.derive(ts2=lambda r: r.ts.dt.add(hours=2, minutes=30))
next_week  = t.derive(d2=lambda r: r.date.dt.add(weeks=1))
```

#### `diff`
- **签名**: `r.col.dt.diff(other: Expr, unit: str = "day") -> Expr`
- **行为**: 返回 `self` 与 `other` 在指定单位下的整数差。`unit` 可取 `"day"`（默认）、`"month"`、`"year"`、`"hour"`、`"minute"`、`"second"`
- **SPL 等价**: `interval(t1, t2, unit)`
- **示例**:
```python
days   = t.derive(d=lambda r: r.end.dt.diff(r.start))
months = t.derive(m=lambda r: r.end.dt.diff(r.start, unit="month"))
```

#### `age`
- **签名**: `r.col.dt.age() -> Expr`
- **行为**: 返回该日期到今天的完整年数。未来日期返回负数
- **SPL 等价**: `age(dt)`
- **示例**:
```python
age = t.derive(age=lambda r: r.birth_date.dt.age())
```

#### `now` / `today`（全局函数）
- **签名**: `now() -> Expr`；`today() -> Expr`
- **行为**: 当前时间戳 / 当前日期
- **导入**: `from ltseq import now, today`
- **示例**:
```python
from ltseq import now
t.derive(fetched_at=lambda r: now())
```

### 数学全局函数

#### `gcd` / `lcm` / `factorial`
- **签名**: `gcd(a: Expr, b: Expr) -> Expr`；`lcm(a: Expr, b: Expr) -> Expr`；`factorial(n: Expr) -> Expr`
- **行为**: 最大公约数、最小公倍数、阶乘（DataFusion `GCD`/`LCM`/`FACTORIAL`）
- **导入**: `from ltseq.expr import gcd, lcm, factorial`
- **示例**:
```python
from ltseq.expr import gcd, lcm, factorial
t.derive(g=lambda r: gcd(r.a, r.b))       # gcd(12, 8) → 4
t.derive(l=lambda r: lcm(r.a, r.b))       # lcm(4, 6) → 12
t.derive(f=lambda r: factorial(r.n))      # factorial(5) → 120
```

#### 通用数学: `sqrt`、`power`、`sign`、`log`、`ln`、`exp`、三角函数、`rand`
- **签名**: `sqrt(x)`、`power(x, n)`、`sign(x)`、`log(x, base=None)`、`ln(x)`、`exp(x)`、`sin/cos/tan/asin/acos/atan(x)`、`atan2(y, x)`、`rand()`
- **行为**: 标准数学函数，映射到 DataFusion 等价物
- **导入**: `from ltseq import sqrt, power, log, ...`
- **示例**:
```python
from ltseq import sqrt, power, log

t.derive(
    dist=lambda r: sqrt(power(r.x, 2) + power(r.y, 2)),
    log_amt=lambda r: log(r.amount, 10),
)
```

## 9. 行变更操作（写时复制）

所有变更操作都返回**新的** LTSeq，原表不变。

### `LTSeq.insert`
- **签名**: `LTSeq.insert(pos: int, row_dict: dict[str, Any]) -> LTSeq`
- **行为**: 在指定的 0-based 位置插入一行。`pos` 超出末尾时追加；负数被截断为 0
- **示例**:
```python
t2 = t.insert(0, {"id": 99, "name": "Alice"})     # 头部插入
t2 = t.insert(len(t), {"id": 100, "name": "Bob"}) # 尾部追加
```

### `LTSeq.delete`
- **签名**: `LTSeq.delete(predicate_or_pos: Callable | int) -> LTSeq`
- **行为**: 删除满足谓词的行，或删除 0-based 下标处的单行
- **示例**:
```python
t2 = t.delete(lambda r: r.status == "expired")
t2 = t.delete(0)   # 删除第一行
```

### `LTSeq.update`
- **签名**: `LTSeq.update(predicate: Callable, **updates: Any) -> LTSeq`
- **行为**: 条件更新——谓词为 True 的行更新列值。每列变为 `if_else(predicate, 新值, 旧值)`
- **示例**:
```python
t2 = t.update(lambda r: r.age > 65, discount=0.2)
t2 = t.update(lambda r: r.status == "old", status="archived")
```

### `LTSeq.modify`
- **签名**: `LTSeq.modify(pos: int, **updates: Any) -> LTSeq`
- **行为**: 修改 0-based 位置 `pos` 处单行的指定列。`pos` 越界时静默忽略
- **示例**:
```python
t2 = t.modify(0, status="active", score=100)
```

## 10. 端到端示例

### 有序计算 + 连续分组
```python
from ltseq import LTSeq

# 任务：找出股价连续上涨超过 3 天的区间
runs = (
    LTSeq.read_csv("stock.csv")
    .sort("date")
    .derive(is_up=lambda r: r.price > r.price.shift(1))
    .group_ordered(lambda r: r.is_up)
    .filter(lambda g: g.count() > 3)
    .filter(lambda g: g.all(lambda r: r.is_up == True))
    .derive(lambda g: {          # 广播：每行都拿到所在组的区间信息
        "start": g.first().date,
        "end": g.last().date,
        "gain": g.last().price - g.first().price,
    })
)
intervals = runs.distinct("start", "end")   # 每个区间一行
```

### 字符串 + 时间 + 条件 + Lookup
```python
from ltseq import LTSeq, if_else

orders = LTSeq.read_csv("orders.csv")
products = LTSeq.read_csv("products.csv")

result = orders.derive(
    order_id_clean=lambda r: r.order_id.s.strip(),
    product_name=lambda r: r.product_id.lookup(products, "name"),
    order_year=lambda r: r.order_date.dt.year(),
    status=lambda r: if_else(r.quantity > 10, "Bulk", "Standard"),
)
```

### 流式处理大文件
```python
from ltseq import LTSeq

total = 0
for batch in LTSeq.scan("huge.csv"):
    total += process(batch)
```

## 11. 执行与性能说明

- 所有表达式序列化后下推到 Rust/DataFusion 执行，不在 Python 中逐行求值
- 字符串/时间/NULL 扩展映射到 SQL/DataFusion 函数
- `LTSeq.scan()`/`scan_parquet()` 支持超大数据集的流式处理
- `assume_sorted()` 对预排序输入跳过物理排序，同时启用窗口函数与归并连接

### 表达式 SQL 转译对照

所有表达式在执行前转译到 Rust/DataFusion 层，lambda 内不发生 Python 级求值。

| 表达式 | SQL / DataFusion 等价 |
|-----------|----------------------------|
| `if_else(cond, a, b)` | `CASE WHEN cond THEN a ELSE b END` |
| `r.col.fill_null(v)` | `COALESCE(col, v)` |
| `r.col.is_null()` | `col IS NULL` |
| `r.col.is_not_null()` | `col IS NOT NULL` |
| `r.col.is_in([a, b])` | `col IN (a, b)` |
| `r.col.between(lo, hi)` | `col >= lo AND col <= hi` |
| `r.col.cast("int64")` | `CAST(col AS BIGINT)` |
| `r.col.abs()` | `ABS(col)` |
| `r.col.round(n)` | `ROUND(col, n)` |
| `r.col.floor()` / `r.col.ceil()` | `FLOOR(col)` / `CEIL(col)` |
| `r.col.s.contains(p)` | `POSITION(p IN col) > 0` |
| `r.col.s.starts_with(p)` | `STARTS_WITH(col, p)` |
| `r.col.s.ends_with(p)` | `ENDS_WITH(col, p)` |
| `r.col.s.lower()` / `.upper()` | `LOWER(col)` / `UPPER(col)` |
| `r.col.s.strip()` / `.lstrip()` / `.rstrip()` | `TRIM` / `LTRIM` / `RTRIM` |
| `r.col.s.len()` | `CHARACTER_LENGTH(col)` |
| `r.col.s.slice(s, n)` | `SUBSTRING(col, s+1, n)` |
| `r.col.s.pos(sub)` | `STRPOS(col, sub)` |
| `r.col.s.left(n)` / `.right(n)` | `LEFT(col, n)` / `RIGHT(col, n)` |
| `r.col.s.asc()` | `ASCII(col)` |
| `r.col.s.like(p)` | `col LIKE p` |
| `r.col.s.replace(old, new)` | `REPLACE(col, old, new)` |
| `r.col.s.pad_left(n, c)` / `.pad_right(n, c)` | `LPAD(col, n, c)` / `RPAD(col, n, c)` |
| `r.col.s.split(d, i)` | `SPLIT_PART(col, d, i)` |
| `str_char(n)` | `CHR(n)` |
| `concat_ws(d, ...)` | `CONCAT_WS(d, ...)` |
| `r.col.dt.year()` 等 | `EXTRACT(YEAR FROM col)` 等 |
| `r.col.dt.add(days=n)` | `col + INTERVAL 'n' DAY` |
| `r.col.dt.diff(other)` | `DATEDIFF('day', other, col)` |
| `r.col.dt.age()` | 相对 `CURRENT_DATE` 的年差（含年内日修正）|
| `gcd(a, b)` / `lcm(a, b)` / `factorial(n)` | `GCD` / `LCM` / `FACTORIAL` |
| `count_if(cond)` | `SUM(CASE WHEN cond THEN 1 ELSE 0 END)` |
| `sum_if(cond, col)` | `SUM(CASE WHEN cond THEN col END)` |
| `g.col.percentile(p)` | `APPROX_PERCENTILE_CONT(col, p)` |

## 附录 A: 从 Pandas 迁移

| Pandas | LTSeq | 说明 |
|--------|-------|-------|
| `df[df.age > 18]` | `t.filter(lambda r: r.age > 18)` | lambda 捕获表达式 |
| `df[['id', 'name']]` | `t.select("id", "name")` | |
| `df.assign(total=df.a + df.b)` | `t.derive(total=lambda r: r.a + r.b)` | `with_columns` 为别名 |
| `df.rename(columns={'a': 'b'})` | `t.rename(a="b")` | |
| `df.drop(columns=['tmp'])` | `t.drop("tmp")` | |
| `df.sort_values('date')` | `t.sort("date")` | |
| `df.sort_values('date', ascending=False)` | `t.sort("date", desc=True)` | 也接受 `descending=` |
| `df.drop_duplicates('id')` | `t.distinct("id")` | |
| `df.groupby('region').agg({'sales': 'sum'})` | `t.group_by("region").agg(sales=lambda g: g.sales.sum())` | |
| `df.merge(df2, on='id')` | `t.join(t2, on="id")` | |
| `df.merge(df2, on='id', how='left')` | `t.join(t2, on="id", how="left")` | |
| `df.merge(df2, left_on='a', right_on='b', suffixes=('','_r'))` | `t.join(t2, left_on="a", right_on="b", suffix="_r")` | |
| `pd.merge_asof(t, q, on='time')` | `t.asof_join(q, on=lambda t, q: t.time >= q.time)` | |
| `df['col'].shift(1)` | `t.sort(...).derive(prev=lambda r: r.col.shift(1))` | 需要排序 |
| `df['col'].rolling(5).mean()` | `t.sort(...).derive(ma=lambda r: r.col.rolling(5).mean())` | 需要排序 |
| `df['col'].diff()` | `t.sort(...).derive(d=lambda r: r.col.diff())` | 需要排序 |
| `df['col'].pct_change()` | `t.sort(...).derive(p=lambda r: r.col.pct_change())` | 需要排序 |
| `df['col'].cumsum()` | `t.sort(...).cum_sum("col")` 或 `r.col.cum_sum()` | 需要排序 |
| `df['col'].isin([1, 2])` | `t.filter(lambda r: r.col.is_in([1, 2]))` | |
| `df['col'].astype('float')` | `t.derive(col=lambda r: r.col.cast("float64"))` | |
| `df.head(10)` / `df.tail(10)` | `t.head(10)` / `t.tail(10)` | |
| `df.to_dict('records')` | `t.to_dicts()` | |
| `len(df)` | `len(t)` 或 `t.count()` | |
| `df.columns.tolist()` | `t.columns` | |
| `df.isna()` | `t.filter(lambda r: r.col.is_null())` | 按列 |
| `df.fillna(0)` | `t.derive(col=lambda r: r.col.fill_null(0))` | 按列 |
| `df.pipe(fn, arg)` | `t.pipe(fn, arg)` | |
