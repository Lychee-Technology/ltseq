# LTSeq API 设计文档

LTSeq 是面向有序序列的 Python 数据处理库，底层由 Rust/DataFusion 执行。与传统 DataFrame 不同，LTSeq 强调“顺序”语义，支持窗口、连续分组、游标式处理等 SPL 风格能力。

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

### `LTSeq.to_cursor`
- **签名**: `LTSeq.to_cursor(chunk_size: int = 10000) -> Iterator[Record]`
- **行为**: 将结果以游标方式流式输出，避免一次性物化
- **参数**: `chunk_size` 每批次行数
- **返回**: 记录迭代器（如 Record/RecordBatch）
- **异常**: `ValueError`（chunk_size 非法），`RuntimeError`（底层不支持流式或执行失败）
- **示例**:
```python
for batch in t.to_cursor(chunk_size=5000):
    print(batch)
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
- **行为**: 按一个或多个键排序；窗口/有序计算必须先排序
- **参数**: `keys` 列名或表达式；`desc`（或 `descending`）全局或逐列降序标记
- **返回**: 排序后的新 `LTSeq`
- **SPL 对应**: `sort()`
- **异常**: `ValueError`（schema 未初始化或 desc 长度不匹配），`TypeError`（键类型非法），`AttributeError`（列不存在）
- **示例**:
```python
t_sorted = t.sort("date", "id", desc=[False, True])
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

> 窗口函数（shift/rolling/diff/cum_sum）要求在调用前使用 `sort` 建立顺序。

### `Expr.shift`
- **签名**: `r.col.shift(offset: int) -> Expr`
- **行为**: 访问相对行的值；`offset > 0` 取前 N 行，`offset < 0` 取后 N 行
- **参数**: `offset` 行偏移量
- **返回**: 表达式（边界处为 NULL）
- **SPL 对应**: `col[-1]`
- **异常**: `TypeError`（offset 非 int），`RuntimeError`（未排序使用）
- **示例**:
```python
with_prev = t.sort("date").derive(prev=lambda r: r.close.shift(1))
```

### `Expr.rolling`
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

### `Expr.diff`
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

### `LTSeq.cum_sum`
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

### `LTSeq.search_first`
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

### `LTSeq.align`
- **签名**: `LTSeq.align(ref_sequence: list, key: Callable) -> LTSeq`
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
- **行为**: 仅按“连续相同值”分组，不会重新排序
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
- **参数**: 无
- **返回**: 按组首行组成的 `LTSeq`
- **异常**: `RuntimeError`（执行失败）
- **示例**:
```python
first_rows = groups.first()
```

#### `NestedTable.last`
- **签名**: `nested.last() -> LTSeq`
- **行为**: 取每组末行
- **参数**: 无
- **返回**: 按组末行组成的 `LTSeq`
- **异常**: `RuntimeError`（执行失败）
- **示例**:
```python
last_rows = groups.last()
```

#### `NestedTable.count`
- **签名**: `nested.count() -> LTSeq`
- **行为**: 返回每组的行数
- **参数**: 无
- **返回**: 包含每组计数的 `LTSeq`
- **异常**: `RuntimeError`（执行失败）
- **示例**:
```python
sizes = groups.count()
```

#### `NestedTable.flatten`
- **签名**: `nested.flatten() -> LTSeq`
- **行为**: 将嵌套表展开为普通表，附带 `__group_id__`
- **参数**: 无
- **返回**: 展开后的 `LTSeq`
- **异常**: `RuntimeError`（执行失败）
- **示例**:
```python
flat = groups.flatten()
```

#### `NestedTable.filter`
- **签名**: `nested.filter(predicate: Callable[[GroupProxy], Expr]) -> NestedTable`
- **行为**: 按组级条件过滤组
- **参数**: `predicate` 组谓词（`g` 作为 GroupProxy）
- **返回**: 过滤后的 `NestedTable`
- **异常**: `TypeError`（谓词非法），`RuntimeError`（执行失败）
- **示例**:
```python
a = groups.filter(lambda g: g.count() > 3)
```

#### `NestedTable.derive`
- **签名**: `nested.derive(func: Callable[[GroupProxy], Dict[str, Expr]]) -> LTSeq`
- **行为**: 生成组级派生列
- **参数**: `func` 返回字典的组表达式
- **返回**: 组级派生结果 `LTSeq`
- **异常**: `TypeError`（func 非法），`RuntimeError`（执行失败）
- **示例**:
```python
spans = groups.derive(lambda g: {"start": g.first().date, "end": g.last().date})
```

#### `GroupProxy` 常用聚合
- **签名**: `g.count()`, `g.first()`, `g.last()`, `g.col.sum()`, `g.col.avg()`, `g.col.min()`, `g.col.max()`
- **行为**: 在组范围内计算聚合或访问首末行
- **参数**: 无（或指定列）
- **返回**: 组级表达式
- **异常**: `TypeError`（列类型不支持），`RuntimeError`（执行失败）
- **示例**:
```python
groups.derive(lambda g: {"avg": g.price.avg(), "hi": g.price.max()})
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

### `LTSeq.diff`（集合差）
- **签名**: `LTSeq.diff(other: LTSeq, on: Optional[Callable] = None) -> LTSeq`
- **行为**: 返回左表中不存在于右表的行
- **参数**: `other` 另一表；`on` 指定比较键
- **返回**: 差集 `LTSeq`
- **SPL 对应**: `A \\ B`
- **异常**: `TypeError`（other 非 LTSeq 或 on 非法），`ValueError`（schema 未初始化）
- **示例**:
```python
only_left = t1.diff(t2, on=lambda r: r.id)
```

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
- **签名**: `LTSeq.join_sorted(other: LTSeq, on: Callable, how: str = "inner") -> LTSeq`
- **行为**: 语义同归并连接；强调“已排序”前提
- **参数**: `other` 另一表；`on` 连接条件；`how` in {inner,left,right,full}
- **返回**: 连接后的 `LTSeq`
- **SPL 对应**: `joinx`
- **异常**: `TypeError`（other/on 非法），`ValueError`（how 非法或未排序）
- **示例**:
```python
result = t1.sort("id").join_sorted(t2.sort("id"), on=lambda a, b: a.id == b.id)
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

### 连接策略对比（简表）

| 方法 | 适用场景 | 算法 | SPL 对应 |
| --- | --- | --- | --- |
| `join` | 无序通用数据 | Hash Join | SQL Join |
| `join_sorted` / `join_merge` | 已排序大表 | Merge Join | `joinx` / `join@m` |
| `link` | 事实表到维表的指针访问 | Pointer | `switch` |
| `lookup` | 事实表 + 小维表 | Direct Address | `switch`（地址化） |
| `asof_join` | 金融时间序列 | Ordered Search | `joinx`（区间） |

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
summary = t.agg(by=lambda r: r.region, total=lambda g: g.sales.sum())
```

### `top_k`（聚合函数）
- **签名**: `top_k(col: Union[Expr, Callable], k: int) -> List[Value]`
- **行为**: 返回指定列 Top-K（聚合语义）
- **参数**: `col` 目标列（表达式或可调用）；`k` Top 个数
- **返回**: Top-K 列表
- **SPL 对应**: `top`
- **异常**: `ValueError`（k <= 0），`TypeError`（col 非法）
- **示例**:
```python
# 用于 agg 中
result = t.agg(top_prices=lambda g: top_k(g.price, 5))
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
- **签名**: `LTSeq.pivot(index: Union[str, list], columns: str, values: str, agg_fn: str = "sum") -> LTSeq`
- **行为**: 长表转宽表的透视操作
- **参数**: `index` 行索引列；`columns` 列展开字段；`values` 值字段；`agg_fn` 聚合函数
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
- **异常**: `TypeError`（condition 非布尔表达式）
- **示例**:
```python
from ltseq.expr import if_else
status = if_else(r.amount > 100, "VIP", "Normal")
```

### `Expr.fill_null`
- **签名**: `r.col.fill_null(default: Any) -> Expr`
- **行为**: NULL 填充（SQL COALESCE）
- **参数**: `default` 默认值
- **返回**: 表达式
- **异常**: `TypeError`（默认值类型不兼容）
- **示例**:
```python
safe_price = r.price.fill_null(0)
```

### `Expr.is_null`
- **签名**: `r.col.is_null() -> Expr`
- **行为**: 判断是否为 NULL
- **参数**: 无
- **返回**: 布尔表达式
- **异常**: 无（执行时可能因类型不支持失败）
- **示例**:
```python
missing = t.filter(lambda r: r.email.is_null())
```

### `Expr.is_not_null`
- **签名**: `r.col.is_not_null() -> Expr`
- **行为**: 判断是否非 NULL
- **参数**: 无
- **返回**: 布尔表达式
- **异常**: 无（执行时可能因类型不支持失败）
- **示例**:
```python
valid = t.filter(lambda r: r.email.is_not_null())
```

### 字符串操作（`r.col.s.*`）

#### `contains`
- **签名**: `r.col.s.contains(pattern: str) -> Expr`
- **行为**: 判断是否包含子串
- **参数**: `pattern` 子串
- **返回**: 布尔表达式
- **异常**: `TypeError`（非字符串列）
- **示例**:
```python
gmail = t.filter(lambda r: r.email.s.contains("gmail"))
```

#### `starts_with`
- **签名**: `r.col.s.starts_with(prefix: str) -> Expr`
- **行为**: 判断是否以指定前缀开头
- **参数**: `prefix` 前缀
- **返回**: 布尔表达式
- **异常**: `TypeError`（非字符串列）
- **示例**:
```python
orders = t.filter(lambda r: r.code.s.starts_with("ORD"))
```

#### `ends_with`
- **签名**: `r.col.s.ends_with(suffix: str) -> Expr`
- **行为**: 判断是否以指定后缀结尾
- **参数**: `suffix` 后缀
- **返回**: 布尔表达式
- **异常**: `TypeError`（非字符串列）
- **示例**:
```python
pdfs = t.filter(lambda r: r.filename.s.ends_with(".pdf"))
```

#### `lower`
- **签名**: `r.col.s.lower() -> Expr`
- **行为**: 转小写
- **参数**: 无
- **返回**: 字符串表达式
- **异常**: `TypeError`（非字符串列）
- **示例**:
```python
normalized = t.derive(email_lower=lambda r: r.email.s.lower())
```

#### `upper`
- **签名**: `r.col.s.upper() -> Expr`
- **行为**: 转大写
- **参数**: 无
- **返回**: 字符串表达式
- **异常**: `TypeError`（非字符串列）
- **示例**:
```python
normalized = t.derive(email_upper=lambda r: r.email.s.upper())
```

#### `strip`
- **签名**: `r.col.s.strip() -> Expr`
- **行为**: 去除首尾空白
- **参数**: 无
- **返回**: 字符串表达式
- **异常**: `TypeError`（非字符串列）
- **示例**:
```python
clean = t.derive(name_clean=lambda r: r.name.s.strip())
```

#### `len`
- **签名**: `r.col.s.len() -> Expr`
- **行为**: 字符串长度
- **参数**: 无
- **返回**: 整数表达式
- **异常**: `TypeError`（非字符串列）
- **示例**:
```python
long_names = t.filter(lambda r: r.name.s.len() > 50)
```

#### `slice`
- **签名**: `r.col.s.slice(start: int, length: int) -> Expr`
- **行为**: 子串截取
- **参数**: `start` 起始位置（0 基）；`length` 长度
- **返回**: 字符串表达式
- **异常**: `ValueError`（start/length 非法），`TypeError`（非字符串列）
- **示例**:
```python
year = t.derive(year=lambda r: r.date.s.slice(0, 4))
```

#### `regex_match`
- **签名**: `r.col.s.regex_match(pattern: str) -> Expr`
- **行为**: 正则匹配（返回布尔值）
- **参数**: `pattern` 正则表达式
- **返回**: 布尔表达式
- **异常**: `ValueError`（正则非法），`TypeError`（非字符串列）
- **示例**:
```python
valid = t.filter(lambda r: r.email.s.regex_match(r"^[a-z]+@"))
```

### 时间操作（`r.col.dt.*`）

#### `year` / `month` / `day`
- **签名**: `r.col.dt.year() -> Expr`（同 month/day）
- **行为**: 提取日期部件
- **参数**: 无
- **返回**: 整数表达式
- **异常**: `TypeError`（非日期列）
- **示例**:
```python
by_date = t.derive(year=lambda r: r.date.dt.year())
```

#### `hour` / `minute` / `second`
- **签名**: `r.col.dt.hour() -> Expr`（同 minute/second）
- **行为**: 提取时间部件
- **参数**: 无
- **返回**: 整数表达式
- **异常**: `TypeError`（非时间列）
- **示例**:
```python
with_time = t.derive(hour=lambda r: r.ts.dt.hour())
```

#### `add`
- **签名**: `r.col.dt.add(days: int = 0, months: int = 0, years: int = 0) -> Expr`
- **行为**: 日期加减
- **参数**: `days/months/years` 偏移量
- **返回**: 日期表达式
- **异常**: `TypeError`（非日期列）
- **示例**:
```python
delivery = t.derive(delivery=lambda r: r.order_date.dt.add(days=5))
```

#### `diff`
- **签名**: `r.col.dt.diff(other: Expr) -> Expr`
- **行为**: 日期差（天）
- **参数**: `other` 另一日期表达式
- **返回**: 整数表达式
- **异常**: `TypeError`（非日期列）
- **示例**:
```python
age_days = t.derive(age=lambda r: r.end_date.dt.diff(r.start_date))
```

### `Expr.lookup`（表达式查找）
- **签名**: `r.key.lookup(target_table: LTSeq, column: str, join_key: Optional[str] = None) -> Expr`
- **行为**: 在表达式中进行轻量查找，返回目标表单列值
- **参数**: `target_table` 目标表；`column` 返回列；`join_key` 目标表匹配键
- **返回**: 查找表达式
- **异常**: `TypeError`（参数非法），`RuntimeError`（执行失败）
- **示例**:
```python
enriched = orders.derive(product_name=lambda r: r.product_id.lookup(products, "name"))
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
from ltseq.expr import if_else

orders = LTSeq.read_csv("orders.csv")
products = LTSeq.read_csv("products.csv")

result = orders.derive(
    order_id_clean=lambda r: r.order_id.s.strip(),
    product_name=lambda r: r.product_id.lookup(products, "name"),
    order_year=lambda r: r.order_date.dt.year(),
    status=lambda r: if_else(r.quantity > 10, "Bulk", "Standard"),
)
```

## 10. 执行与性能说明

- 所有表达式会被序列化并下推到 Rust/DataFusion 层执行，不在 Python 侧逐行计算
- 字符串/时间/NULL 等扩展操作会映射为底层 SQL/表达式函数
- `to_cursor` 提供流式处理能力，适合超大数据集
