# API 文档修改方案

基于对 `docs/api.md` 的 review，以下是详细的修改计划。

---

## 一、命名与语义修正

### 1.1 解决 `diff` 命名冲突

**问题**：`Expr.diff` (行差分) 与 `LTSeq.diff` (集合差集) 使用相同名称，语义完全不同。

**修改方案**：

| 原名称 | 新名称 | 位置 | 理由 |
|-------|-------|------|------|
| `LTSeq.diff` | `LTSeq.except_` | Section 5 | 对齐 SQL `EXCEPT`，避免与时序 diff 混淆 |
| `Expr.diff` | 保持不变 | Section 3 | 时序场景下 diff 是自然术语 |

**代码影响**：
- Rust: `src/ops/set_ops.rs` 中的 `diff` 方法重命名
- Python: `py-ltseq/ltseq/set_mixin.py` 中的方法重命名
- 测试: `test_set_ops.py` 更新

**文档修改**：
```markdown
### `LTSeq.except_` (set difference)
- **Signature**: `LTSeq.except_(other: LTSeq, on: Optional[Callable] = None) -> LTSeq`
- **Behavior**: Rows in left table but not in right table
- **SPL Equivalent**: `A \ B`
```

---

## 二、结构重组

### 2.1 拆分 Section 3 为子节

**当前结构**：
```
## 3. Ordered and Window Functions
  - Expr.shift
  - Expr.rolling
  - Expr.diff
  - LTSeq.cum_sum
  - LTSeq.search_first
  - Ranking Functions (row_number, rank, dense_rank, ntile)
  - LTSeq.align
```

**建议结构**：
```
## 3. Ordered and Window Functions

### 3.1 Row-Level Window Operations
> Require prior `.sort()` to establish order.
  - Expr.shift
  - Expr.rolling
  - Expr.diff

### 3.2 Table-Level Cumulative Operations
  - LTSeq.cum_sum

### 3.3 Ranking Functions
> Use `.over()` to specify ordering; do NOT require prior `.sort()`.
  - row_number
  - rank
  - dense_rank
  - ntile
  - CallExpr.over

### 3.4 Ordered Search
  - LTSeq.search_first
  - LTSeq.align
```

**理由**：
- 明确区分需要 `.sort()` 的操作和使用 `.over()` 的操作
- 分离表级操作 (`cum_sum`) 和表达式级操作 (`shift/rolling`)

---

## 三、补充缺失内容

### 3.1 添加终止操作 (Section 1 扩展)

在 `LTSeq.to_cursor` 后添加：

```markdown
### `LTSeq.collect`
- **Signature**: `LTSeq.collect() -> List[Dict[str, Any]]`
- **Behavior**: Materialize all rows as a list of dictionaries
- **Parameters**: none
- **Returns**: List of row dictionaries
- **Exceptions**: `MemoryError` (dataset too large), `RuntimeError` (execution failure)
- **Example**:
```python
rows = t.filter(lambda r: r.age > 18).collect()
for row in rows:
    print(row["name"])
```

### `LTSeq.to_pandas`
- **Signature**: `LTSeq.to_pandas() -> pd.DataFrame`
- **Behavior**: Convert to pandas DataFrame for interoperability
- **Parameters**: none
- **Returns**: `pandas.DataFrame`
- **Exceptions**: `ImportError` (pandas not installed), `MemoryError` (dataset too large)
- **Example**:
```python
df = t.to_pandas()
df.plot(x="date", y="price")
```

### `LTSeq.count`
- **Signature**: `LTSeq.count() -> int`
- **Behavior**: Return the number of rows
- **Parameters**: none
- **Returns**: integer row count
- **Exceptions**: `RuntimeError` (execution failure)
- **Example**:
```python
n = t.filter(lambda r: r.status == "active").count()
```

### `LTSeq.head`
- **Signature**: `LTSeq.head(n: int = 10) -> LTSeq`
- **Behavior**: Return the first n rows
- **Parameters**: `n` number of rows (default 10)
- **Returns**: `LTSeq` with first n rows
- **Exceptions**: `ValueError` (n < 0)
- **Example**:
```python
top_10 = t.sort("score", desc=True).head(10)
```

### `LTSeq.tail`
- **Signature**: `LTSeq.tail(n: int = 10) -> LTSeq`
- **Behavior**: Return the last n rows
- **Parameters**: `n` number of rows (default 10)
- **Returns**: `LTSeq` with last n rows
- **Exceptions**: `ValueError` (n < 0)
- **Example**:
```python
recent = t.sort("date").tail(5)
```
```

### 3.2 添加 Schema API (Section 1 扩展)

```markdown
### `LTSeq.schema` (property)
- **Signature**: `LTSeq.schema -> Schema`
- **Behavior**: Return the table schema with column names and types
- **Parameters**: none (property)
- **Returns**: `Schema` object with `.names` and `.types` attributes
- **Exceptions**: none
- **Example**:
```python
t = LTSeq.read_csv("data.csv")
print(t.schema.names)   # ["id", "name", "age", "created_at"]
print(t.schema.types)   # [Int64, Utf8, Int64, Timestamp]

# Iterate columns
for name, dtype in zip(t.schema.names, t.schema.types):
    print(f"{name}: {dtype}")
```

### `LTSeq.columns` (property)
- **Signature**: `LTSeq.columns -> List[str]`
- **Behavior**: Return list of column names (shortcut for `schema.names`)
- **Parameters**: none (property)
- **Returns**: list of column name strings
- **Example**:
```python
print(t.columns)  # ["id", "name", "age"]
```
```

### 3.3 添加错误处理指南 (Section 0.1)

在 Section 0 后插入：

```markdown
## 0.1 Common Errors and Solutions

| Error | Cause | Solution |
|-------|-------|----------|
| `RuntimeError: window function used without sort` | Called `shift/rolling/diff` without prior `.sort()` | Add `.sort(order_column)` before window operations |
| `AttributeError: column 'xxx' not found` | Typo in column name or column doesn't exist | Check `t.columns` for available columns |
| `ValueError: schema mismatch` | Union/intersect with incompatible tables | Ensure both tables have same columns and types |
| `ValueError: tables not sorted by join keys` | `join_sorted` called on unsorted tables | Call `.sort(join_key)` on both tables first |
| `TypeError: predicate not boolean Expr` | Filter lambda returns non-boolean | Ensure predicate uses comparison operators |
```

---

## 四、一致性修正

### 4.1 统一签名风格

**规则**：
- 表达式方法：使用 `r.col.method()` 格式
- 表级方法：使用 `LTSeq.method()` 格式
- 模块函数：使用 `function_name()` 格式

**需要修改的签名**：

| 原签名 | 新签名 | 行号 |
|-------|-------|------|
| `Expr.shift(offset: int)` | `r.col.shift(offset: int)` | 160 |
| `Expr.rolling(window_size)` | `r.col.rolling(window_size)` | 172 |
| `Expr.diff(offset: int)` | `r.col.diff(offset: int)` | 183 |
| `Expr.fill_null(default)` | `r.col.fill_null(default)` | 863 |
| `Expr.is_null()` | `r.col.is_null()` | 874 |
| `Expr.is_not_null()` | `r.col.is_not_null()` | 885 |
| `Expr.lookup(...)` | `r.col.lookup(...)` | 1108 |

### 4.2 统一 GroupProxy 方法风格

**问题**：当前混用 `g.median("col")` 和 `g.col.sum()` 两种风格。

**决策**：保留两种风格，但明确区分：
- **列方法** `g.col.agg()`：用于简单聚合 (sum, avg, min, max)
- **组方法** `g.method("col")`：用于统计函数 (median, percentile, variance, std, mode, top_k)

**文档补充说明**：
```markdown
#### GroupProxy Method Styles

GroupProxy supports two calling styles:

1. **Column accessor style** for common aggregations:
   ```python
   g.price.sum()    # Sum of price column
   g.price.avg()    # Average of price column
   ```

2. **Method style** for statistical functions:
   ```python
   g.median("price")         # Median of price column
   g.percentile("price", 0.95)  # 95th percentile
   ```
```

### 4.3 补全或删除 SPL Equivalent

**建议**：删除 SPL Equivalent 字段。

**理由**：
- 目标用户群体主要是 Python/SQL 背景，不熟悉 SPL
- 当前覆盖不完整，维护成本高
- 如需保留，应单独创建 SPL 迁移指南文档

**替代方案**：添加 SQL Equivalent 字段（更通用）

---

## 五、细节修正

### 5.1 明确 `over()` 多列分区支持

修改 Section 3 的 `CallExpr.over`：

```markdown
### `CallExpr.over` (Window Specification)
- **Signature**: `expr.over(partition_by: Optional[Union[Expr, List[Expr]]] = None, order_by: Optional[Union[Expr, List[Expr]]] = None, descending: Union[bool, List[bool]] = False) -> WindowExpr`
- **Behavior**: Apply window specification to a ranking function
- **Parameters**:
  - `partition_by` column(s) to partition by (single Expr or list)
  - `order_by` column(s) to order by (single Expr or list)
  - `descending` sort direction (single bool or per-column list)
- **Example**:
```python
# Single column partition
t.derive(rn=lambda r: row_number().over(partition_by=r.region, order_by=r.date))

# Multiple column partition
t.derive(rn=lambda r: row_number().over(
    partition_by=[r.region, r.category],
    order_by=[r.date, r.id],
    descending=[True, False]
))
```
```

### 5.2 补充 `pivot` 支持的 agg_fn 列表

修改 Section 7 的 `LTSeq.pivot`：

```markdown
- **Parameters**:
  - `index` row index columns
  - `columns` column dimension
  - `values` value field
  - `agg_fn` aggregation function, one of: `"sum"`, `"mean"`, `"min"`, `"max"`, `"count"`, `"first"`, `"last"` (default: `"sum"`)
```

### 5.3 改进 `shift` 说明

修改 Section 3 的 `Expr.shift`：

```markdown
### `r.col.shift`
- **Signature**: `r.col.shift(offset: int) -> Expr`
- **Behavior**: Access relative rows. Positive offset looks backward (previous rows), negative offset looks forward (future rows). Consistent with pandas `Series.shift()`.
- **Parameters**: `offset` row offset (positive = backward, negative = forward)
- **Returns**: expression (NULL at boundaries where offset exceeds available rows)
```

### 5.4 `align` 参数类型明确化

```markdown
- **Signature**: `LTSeq.align(ref_sequence: List[Any], key: Callable[[Row], Expr]) -> LTSeq`
```

---

## 六、新增章节

### 6.1 Quick Reference (放在 Section 0 后)

```markdown
## Quick Reference

### Data Loading & Output
| Operation | Method | Example |
|-----------|--------|---------|
| Load CSV | `LTSeq.read_csv()` | `t = LTSeq.read_csv("data.csv")` |
| Stream results | `.to_cursor()` | `for batch in t.to_cursor(): ...` |
| Collect all | `.collect()` | `rows = t.collect()` |
| To pandas | `.to_pandas()` | `df = t.to_pandas()` |

### Basic Operations
| Operation | Method | Example |
|-----------|--------|---------|
| Filter rows | `.filter()` | `t.filter(lambda r: r.age > 18)` |
| Select columns | `.select()` | `t.select("id", "name")` |
| Add columns | `.derive()` | `t.derive(total=lambda r: r.a + r.b)` |
| Sort | `.sort()` | `t.sort("date", desc=True)` |
| Deduplicate | `.distinct()` | `t.distinct("id")` |
| Slice rows | `.slice()` | `t.slice(offset=10, length=5)` |

### Window Operations (require `.sort()` first)
| Operation | Method | Example |
|-----------|--------|---------|
| Previous row | `.shift(n)` | `r.price.shift(1)` |
| Rolling agg | `.rolling(n).agg()` | `r.price.rolling(5).mean()` |
| Row difference | `.diff(n)` | `r.price.diff(1)` |

### Aggregation
| Operation | Method | Example |
|-----------|--------|---------|
| Group aggregate | `.agg()` | `t.agg(by=lambda r: r.region, total=lambda g: g.sales.sum())` |
| Partition | `.partition()` | `parts = t.partition("region")` |
| Pivot | `.pivot()` | `t.pivot(index="date", columns="region", values="amount")` |

### Joins
| Operation | Method | Example |
|-----------|--------|---------|
| Hash join | `.join()` | `a.join(b, on=lambda a, b: a.id == b.id)` |
| Merge join | `.join_sorted()` | `a.sort("id").join_sorted(b.sort("id"), on="id")` |
| Semi join | `.semi_join()` | `a.semi_join(b, on=lambda a, b: a.id == b.id)` |
| Anti join | `.anti_join()` | `a.anti_join(b, on=lambda a, b: a.id == b.id)` |
```

### 6.2 Migrating from Pandas (可选，作为附录)

```markdown
## Appendix A: Migrating from Pandas

| Pandas | LTSeq | Notes |
|--------|-------|-------|
| `df[df.age > 18]` | `t.filter(lambda r: r.age > 18)` | Lambda captures expression |
| `df[['id', 'name']]` | `t.select("id", "name")` | |
| `df.assign(total=df.a + df.b)` | `t.derive(total=lambda r: r.a + r.b)` | |
| `df.sort_values('date')` | `t.sort("date")` | |
| `df.drop_duplicates('id')` | `t.distinct("id")` | |
| `df.groupby('region').sum()` | `t.agg(by=lambda r: r.region, ...)` | |
| `df.merge(df2, on='id')` | `t.join(t2, on=lambda a, b: a.id == b.id)` | |
| `df.shift(1)` | `t.sort(...).derive(prev=lambda r: r.col.shift(1))` | Requires sort |
| `df.rolling(5).mean()` | `t.sort(...).derive(ma=lambda r: r.col.rolling(5).mean())` | Requires sort |
| `df.head(10)` | `t.head(10)` | |
| `df.to_dict('records')` | `t.collect()` | |
```

---

## 七、实施优先级

### P0 - 必须修改 (影响正确性或可用性)
1. [ ] 解决 `diff` 命名冲突 → 改名为 `except_`
2. [ ] 添加终止操作 `collect()`, `count()`, `head()`, `tail()`
3. [ ] 添加 `schema` / `columns` 属性

### P1 - 建议修改 (提升一致性)
4. [ ] 统一签名风格 (`r.col.method()`)
5. [ ] 拆分 Section 3 为子节
6. [ ] 明确 `over()` 多列支持
7. [ ] 补充 `pivot` 的 agg_fn 列表

### P2 - 可选增强 (提升用户体验)
8. [ ] 添加 Quick Reference 表
9. [ ] 添加常见错误处理指南
10. [ ] 添加 Pandas 迁移指南
11. [ ] 删除或补全 SPL Equivalent

---

## 八、验证清单

修改完成后，需验证：
- [ ] 所有示例代码可运行
- [ ] 方法签名与实际代码一致
- [ ] 无遗漏的交叉引用 (如文档中提到的方法都有定义)
- [ ] 目录/锚点链接有效
