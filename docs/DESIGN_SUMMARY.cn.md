# LTSeq 设计摘要

相关文档：

- `docs/README.md`：文档索引
- `docs/ARCHITECTURE.cn.md`：完整架构说明
- `docs/MODULE_GUIDE.cn.md`：面向贡献者的代码导览
- `docs/USER_MODEL.cn.md`：面向用户的设计模型
- `docs/api.cn.md`：中文 API 文档
- `docs/LINKING_GUIDE.cn.md`：中文 Linking 指南
- `docs/DESIGN_SUMMARY.md`：英文原版设计摘要

**最后更新**：2026 年 5 月 20 日  
**状态**：当前架构摘要与设计归档

## 概览

LTSeq 是一个混合式 Python-Rust 库，用于面向顺序语义的数据处理。它把 Python 风格的 lambda DSL 与 Rust/DataFusion 执行引擎结合起来，并把行顺序视为查询模型的一部分，而不是仅用于展示的元数据。

它的核心理念包括：

- 懒执行
- 显式顺序保证
- 流畅的 Python 使用体验
- Rust 侧执行带来的性能和一致性

这份文档现在承担两类角色：

- 当前架构的精简摘要
- 重要实现决策与历史经验的设计归档

如果需要更深入阅读：

- `docs/ARCHITECTURE.cn.md`：完整架构 walkthrough
- `docs/MODULE_GUIDE.cn.md`：贡献者代码地图
- `docs/USER_MODEL.cn.md`：面向用户的设计与使用模型
- `docs/api.cn.md`：API 参考
- `docs/LINKING_GUIDE.cn.md`：Linking 专题说明

---

## 目录

1. [当前架构](#1-当前架构)
2. [表达式系统](#2-表达式系统)
3. [顺序语义](#3-顺序语义)
4. [Linking Joining 与 Grouping](#4-linking-joining-与-grouping)
5. [执行策略](#5-执行策略)
6. [测试与性能](#6-测试与性能)
7. [设计经验](#7-设计经验)
8. [历史说明](#8-历史说明)

---

## 1. 当前架构

### 1.1 Python-Rust 混合设计

LTSeq 主要由两层组成。

- **Python 层** 位于 `py-ltseq/ltseq/`：负责公共 API、mixin 组织、schema 跟踪、排序元数据跟踪、lambda 捕获和高层对象包装
- **Rust 层** 位于 `src/`：负责 PyO3 扩展模块、懒 DataFusion 计划处理、表达式转译、终结执行以及顺序相关的专用实现

在当前代码中，主要的 Python 用户对象是 `LTSeq`，主要的 Rust 执行对象是 `LTSeqTable`。

### 1.2 懒执行模型

大多数操作都会返回一个新的惰性查询对象，而不是立即执行。

典型的惰性操作包括：

- `filter()`
- `select()`
- `derive()`
- `sort()`
- `slice()`
- `join()`
- `group_ordered()`

真正触发执行的边界包括：

- `show()`
- `count()`
- `collect()`
- `to_arrow()`
- `to_pandas()`
- 文件写出

这让 DataFusion 有机会优化逻辑计划，也让 LTSeq 保持统一的 query-first 模型。

### 1.3 Python API 组合方式

`LTSeq` 是通过 mixin 组合构建的，而不是单一巨大类。

关键 Python 模块包括：

- `core.py`
- `io_ops.py`
- `transforms.py`
- `joins.py`
- `aggregation.py`
- `advanced_ops.py`
- `mutation_mixin.py`

这样既保持了公共 API 的宽度，也让职责分布更合理。

### 1.4 Rust 执行壳层与模块化 Ops

Rust 通过 `src/lib.rs` 中单一的 `#[pymethods]` 暴露 `LTSeqTable`，再把大部分实际工作委派给 `src/ops/*` 中的辅助模块。

采用这个模式有两个原因：

- PyO3 更适合集中定义面向 Python 的方法暴露层
- 如果不按能力拆分执行逻辑，实现会很难维护

代表性的 Rust 模块包括：

- `src/ops/derive.rs`
- `src/ops/window.rs`
- `src/ops/grouping.rs`
- `src/ops/join.rs`
- `src/ops/asof_join.rs`
- `src/ops/parallel_scan.rs`

### 1.5 核心元数据模型

有两套元数据系统是架构核心：

- Python 侧 `_schema` / Rust 侧 Arrow schema
- Python 侧 `_sort_keys` / Rust 侧 `sort_exprs`

前者用于保持用户可见验证与执行状态一致，后者用于让顺序相关操作显式且安全。

---

## 2. 表达式系统

### 2.1 基于 SchemaProxy 的 Lambda DSL

Python lambda 在 LTSeq 中充当声明式 DSL。

例如：

```python
t.filter(lambda r: r.age > 18)
```

这里的 `r` 是 `SchemaProxy`，而不是一个真实行对象。属性访问和运算符会构造表达式节点，而不是在 Python 中直接计算值。

### 2.2 序列化边界

捕获到的表达式会在进入 Rust 之前序列化为字典。

例如：

```python
{"type": "Column", "name": "age"}
{"type": "Literal", "value": 18, "dtype": "Int64"}
```

在 Rust 侧，这些会在 `src/types.rs` 中被还原为 `PyExpr`。

### 2.3 转译路径

表达式在反序列化后可能走以下几条路径：

- 原生 DataFusion 表达式生成
- 原生窗口表达式生成
- SQL 生成回退路径

关键文件：

- `src/transpiler/mod.rs`
- `src/transpiler/window_native.rs`
- `src/transpiler/sql_gen.rs`
- `src/transpiler/optimization.rs`

### 2.4 实际边界

LTSeq 的表达式系统是有意受约束的。它非常擅长支持 Python 风格的列表达式，但不会尝试把任意 Python 逻辑逐行搬到执行引擎中。这种取舍保证了表达式可序列化、可在 Rust 侧执行。

---

## 3. 顺序语义

### 3.1 显式排序要求

LTSeq 最强的设计决定之一是：依赖顺序的操作必须建立在显式顺序之上。

- `shift()`
- `diff()`
- `rolling()`
- 很多 ranking / window 工作流

如果排序状态未知，LTSeq 会优先报错，而不是猜测。

### 3.2 排序元数据传播

一些操作因为保留了行顺序含义，所以会保留排序元数据，比如很多 filter、derive 和 slice。

另一些操作可能使排序元数据失效，比如重排行或生成新结构的操作。

这种传播行为是核心语义的一部分，而不只是优化细节。

### 3.3 基于排序的快速路径

排序元数据还支持一些特定工作负载的优化执行路径，尤其是在已排序的 Parquet 输入和顺序扫描场景中。

---

## 4. Linking Joining 与 Grouping

### 4.1 Pointer-Based LinkedTable

`link()` 返回的是 `LinkedTable`，即一个惰性关系对象，而不是立刻拍平后的 join 结果表。

这样可以让只依赖源表的操作保持较低成本，并把 join 成本延迟到真正需要 linked-side 列的时候。

### 4.2 Join 列冲突处理策略

Join 执行必须谨慎处理右表列名冲突或重复的问题。LTSeq 的 join 实现会在 join 前积极地重命名右表列，再把它们别名恢复为用户可见的 schema 形状。

实践证明，这对正确性和可预测性都是必要的。

### 4.3 顺序分组上下文

`group_ordered()` 和 `group_sorted()` 返回的是 `NestedTable`，而不是拍平的聚合结果。这类工作流会通过以下内部列保留组内上下文：

- `__group_id__`
- `__group_count__`
- `__rn__`

这样就可以在不立即 flatten 的前提下做 group-aware filter、derive 和 first/last 风格分析。

### 4.4 Partitioning

`partition()` 提供按组访问模式，同时尽可能停留在懒执行查询路径上。callable partition 会被有意限制在仍可被捕获并交由引擎执行的表达式范围内。

---

## 5. 执行策略

### 5.1 DataFusion 优先

首选路径是把工作表示为原生 DataFusion 逻辑计划和表达式。

### 5.2 SQL 回退

一些操作，尤其是 group-heavy 或 window-heavy 的变换，在当前实现中仍然会使用生成 SQL 和临时表的方式，因为这是更实际的实现路径。

### 5.3 专用 Rust 路径

LTSeq 还包含针对有序工作负载的专用实现，在这些场景下通用规划并不是最佳选择。

例如：

- 直接顺序扫描
- 并行扫描
- 模式匹配
- as-of join 的二分搜索逻辑

### 5.4 No-Materialization Rule

任何返回 `LTSeq`、`NestedTable`、`LinkedTable` 或 `PartitionedTable` 的 API，都应尽可能停留在惰性的 Rust/DataFusion 路径上。物化应仅发生在显式导出或终结 API 中。

这是 LTSeq 最有代表性的架构约束之一，并且由测试保护。

---

## 6. 测试与性能

### 6.1 按能力组织的测试

`py-ltseq/tests/` 中的测试主要按行为和能力组织，而不是按源文件组织。

主要类别包括：

- expressions
- transforms
- sort tracking 和 windows
- grouping 和 nested tables
- joins 和 as-of joins
- linking 和 pointer syntax
- partitioning 和 set operations
- no accidental materialization 等架构保护项

### 6.2 Benchmark 作为架构反馈

Benchmark 被用来验证架构选择是否在真实有序工作负载下站得住脚。项目通过它们定位 materialization 瓶颈，并判断哪些专用原生实现值得保留或引入。

---

## 7. 设计经验

### 7.1 顺序必须显式表达

如果试图让顺序语义隐式存在，最终会得到令人困惑的结果和脆弱的 API。强制要求显式排序状态，整体上是更好的设计。

### 7.2 Schema 同步至关重要

Python 侧 schema 跟踪和 Rust 侧 Arrow schema 跟踪的组合提升了易用性，但也要求在 join 和物化边界之后严格保持同步。

### 7.3 DataFusion 很强，但并不能解决一切

DataFusion 已经覆盖了 LTSeq 大量执行需求，但对于某些序列型工作负载，专用逻辑仍然是合理的。

### 7.4 Materialization 是最主要的架构成本中心

最重要的性能问题通常不在底层计算内核，而在复杂操作周围的 collect / register / re-query 模式。

---

## 8. 历史说明

这个文档的旧版本曾经承担“大而全设计归档”的角色。这个历史角色仍然有价值，但现在仓库已经把“当前架构”“贡献者导览”和“用户心智模型”拆分到了独立文档中。

未来更新这份文档时，建议遵循以下原则：

- 保持高层架构摘要始终与当前代码一致
- 保留重要设计经验和决策原因
- 过深的实现 walkthrough 应迁移到 `docs/ARCHITECTURE.cn.md` 或更专门的文档中
