# API 实施计划

根据 `docs/api.md` 的调整，以下是代码更新的详细实施计划。

---

## 当前状态总结

| 方法/属性 | 状态 | 位置 | 备注 |
|----------|------|------|------|
| `diff()` | ✓ 存在 | `advanced_ops.py`, `set_ops.rs` | 需重命名为 `except_()` |
| `collect()` | ✗ 缺失 | - | 需实现 |
| `to_pandas()` | ✓ 存在 | `core.py` | 无需修改 |
| `count()` | ✓ 作为 `__len__` | `core.py` | 需添加显式方法 |
| `head()` | ✗ 缺失 | - | 需实现 |
| `tail()` | ✗ 缺失 | - | 需实现 |
| `schema` 属性 | ✗ 缺失 | - | `_schema` 存在，需公开 |
| `columns` 属性 | ✗ 缺失 | - | 可从 `_schema.keys()` 获取 |

---

## 实施任务

### Task 1: 添加 `schema` 和 `columns` 属性 (简单)

**文件**: `py-ltseq/ltseq/core.py`

**修改内容**:
```python
@property
def schema(self) -> Dict[str, str]:
    """Return the table schema with column names and types."""
    return self._schema.copy()

@property
def columns(self) -> List[str]:
    """Return list of column names."""
    return list(self._schema.keys())
```

**测试**: 添加到 `py-ltseq/tests/test_basic.py`

---

### Task 2: 添加 `count()` 方法 (简单)

**文件**: `py-ltseq/ltseq/core.py`

**修改内容**:
```python
def count(self) -> int:
    """Return the number of rows."""
    return len(self)
```

**测试**: 添加到 `py-ltseq/tests/test_basic.py`

---

### Task 3: 实现 `head()` 和 `tail()` 方法 (中等)

**文件**: `py-ltseq/ltseq/transform.py` (TransformMixin)

**修改内容**:
```python
def head(self, n: int = 10) -> "LTSeq":
    """Return the first n rows."""
    if n < 0:
        raise ValueError("n must be non-negative")
    return self.slice(offset=0, length=n)

def tail(self, n: int = 10) -> "LTSeq":
    """Return the last n rows."""
    if n < 0:
        raise ValueError("n must be non-negative")
    total = len(self)
    offset = max(0, total - n)
    return self.slice(offset=offset, length=n)
```

**测试**: 添加到 `py-ltseq/tests/test_transform.py`

---

### Task 4: 实现 `collect()` 方法 (中等)

**文件**: `py-ltseq/ltseq/core.py`

**设计决策**: `collect()` 返回 `List[Dict[str, Any]]`，与 pandas `to_dict('records')` 一致。

**修改内容**:
```python
def collect(self) -> List[Dict[str, Any]]:
    """Materialize all rows as a list of dictionaries."""
    df = self.to_pandas()
    return df.to_dict('records')
```

**备选方案**: 如果需要避免 pandas 依赖，可在 Rust 侧实现序列化。

**测试**: 添加到 `py-ltseq/tests/test_basic.py`

---

### Task 5: 重命名 `diff()` → `except_()` (中等)

**需要修改的文件**:

1. **Python 层**:
   - `py-ltseq/ltseq/advanced_ops.py`: 重命名方法
   - 保留 `diff()` 作为别名并标记 deprecated

2. **Rust 层**:
   - `src/lib.rs`: 重命名 `diff` → `except_` 方法
   - `src/ops/set_ops.rs`: 重命名内部函数（可选）

3. **测试**:
   - `py-ltseq/tests/test_set_ops.py`: 更新测试用例

**Python 修改示例** (`advanced_ops.py`):
```python
def except_(self, other: "LTSeq", on: Optional[Callable] = None) -> "LTSeq":
    """Rows in this table but not in other table (SQL EXCEPT semantics)."""
    # ... existing implementation ...

def diff(self, other: "LTSeq", on: Optional[Callable] = None) -> "LTSeq":
    """Deprecated: Use except_() instead."""
    import warnings
    warnings.warn(
        "diff() is deprecated, use except_() instead",
        DeprecationWarning,
        stacklevel=2
    )
    return self.except_(other, on)
```

**Rust 修改示例** (`lib.rs`):
```rust
/// Set difference (EXCEPT)
fn except_(&self, other: &LTSeqTable, on: Option<PyObject>) -> PyResult<LTSeqTable> {
    // ... existing diff_impl logic ...
}

/// Deprecated: Use except_() instead
fn diff(&self, other: &LTSeqTable, on: Option<PyObject>) -> PyResult<LTSeqTable> {
    self.except_(other, on)
}
```

---

## 实施顺序

```
Phase 1: 简单属性和方法 (30 分钟)
├── Task 1: schema/columns 属性
├── Task 2: count() 方法
└── 运行测试验证

Phase 2: head/tail 方法 (30 分钟)
├── Task 3: head() 和 tail()
└── 运行测试验证

Phase 3: collect 方法 (20 分钟)
├── Task 4: collect()
└── 运行测试验证

Phase 4: diff → except_ 重命名 (45 分钟)
├── Task 5: Python 层重命名
├── Task 5: Rust 层重命名
├── Task 5: 更新测试
└── 运行全部测试验证
```

---

## 测试计划

### 新增测试用例

**`test_basic.py`**:
```python
def test_schema_property():
    t = LTSeq.read_csv("test.csv")
    assert isinstance(t.schema, dict)
    assert "id" in t.schema

def test_columns_property():
    t = LTSeq.read_csv("test.csv")
    assert isinstance(t.columns, list)
    assert "id" in t.columns

def test_count():
    t = LTSeq.read_csv("test.csv")
    assert t.count() == len(t)

def test_collect():
    t = LTSeq.read_csv("test.csv")
    rows = t.collect()
    assert isinstance(rows, list)
    assert all(isinstance(r, dict) for r in rows)
```

**`test_transform.py`**:
```python
def test_head():
    t = LTSeq.read_csv("test.csv")
    result = t.head(3)
    assert len(result) == 3

def test_head_default():
    t = LTSeq.read_csv("test.csv")  # assume > 10 rows
    result = t.head()
    assert len(result) == 10

def test_tail():
    t = LTSeq.read_csv("test.csv")
    result = t.tail(3)
    assert len(result) == 3

def test_head_exceeds_length():
    t = LTSeq.read_csv("test.csv")  # assume 5 rows
    result = t.head(100)
    assert len(result) == len(t)
```

**`test_set_ops.py`** (更新):
```python
def test_except_basic():
    # existing diff test renamed
    result = t1.except_(t2, on=lambda r: r.id)
    assert len(result) == expected

def test_diff_deprecation_warning():
    with pytest.warns(DeprecationWarning):
        t1.diff(t2)
```

---

## 验收标准

- [ ] 所有新方法/属性已实现
- [ ] `pytest py-ltseq/tests/ -v` 全部通过
- [ ] `diff()` 调用产生 DeprecationWarning
- [ ] `except_()` 功能与原 `diff()` 一致
- [ ] 文档示例代码可运行

---

## 风险与注意事项

1. **向后兼容性**: 保留 `diff()` 别名，避免破坏现有代码
2. **Rust 编译**: 重命名 Rust 方法后需运行 `maturin develop` 重新编译
3. **测试数据**: 确保测试 CSV 文件有足够行数测试 `head()`/`tail()` 边界情况
4. **性能**: `tail()` 需要先获取 count，对大数据集可能有开销

---

## 命令参考

```bash
# 构建 Rust 扩展
maturin develop

# 运行全部测试
pytest py-ltseq/tests/ -v

# 运行单个测试文件
pytest py-ltseq/tests/test_basic.py -v

# 检查 Rust 编译
cargo check
```
