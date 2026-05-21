# LTSeq 架构文档

相关文档：

- `docs/README.md`：文档索引
- `docs/ARCHITECTURE.md`：英文架构文档
- `docs/MODULE_GUIDE.cn.md`：面向贡献者的代码导览
- `docs/USER_MODEL.cn.md`：面向用户的设计理念与使用模型
- `docs/DESIGN_SUMMARY.cn.md`：中文设计摘要
- `docs/api.cn.md`：中文 API 参考

## 概览

LTSeq 是一个面向有序序列的数据处理库，采用 Python 用户接口层与 Rust 执行内核的双层设计。与把表视为无序集合的传统 DataFrame 系统不同，LTSeq 把行顺序视为数据模型的一部分。这一设计选择影响了 API 设计、元数据维护、执行策略和测试策略。

从整体上看：

- Python 定义公共 API 并捕获用户表达式
- Rust 负责查询规划与执行
- DataFusion 和 Arrow 提供执行底座
- 测试和 benchmark 保护行为正确性与架构约束

---

## 设计目标

LTSeq 的架构主要围绕五个目标展开：

1. 将顺序语义作为一等概念保留
2. 保持 Python API 的流畅性与表达力
3. 通过懒执行让 DataFusion 有机会优化查询计划
4. 避免在返回表对象的 API 中发生意外物化
5. 在通用查询规划不足时允许专用快速路径

这些目标解释了仓库中大部分结构设计。

---

## 分层架构

整个系统可以分为四层。

### 1. 用户 API 层

用户主要与以下 Python 对象交互：

- `LTSeq`
- `NestedTable`
- `LinkedTable`
- `PartitionedTable`

这一层提供链式调用、基于 lambda 的表达式能力，以及通过返回类型表达语义上下文的机制。

### 2. Python 编排层

`py-ltseq/ltseq/` 下的 Python 包有意保持轻量，主要职责是：

- 通过 mixin 组织公共 API
- 跟踪 Python 侧 schema 与排序元数据
- 将 lambda 捕获为可序列化表达式树
- 决定高层 API 应返回哪种对象类型
- 作为 Rust 绑定的调用桥梁

这一层尽量不承担重型数据计算。

### 3. Rust 执行层

`src/` 中的 Rust crate 通过 PyO3 暴露 `LTSeqTable`。它负责：

- 构造并转换懒执行的 DataFusion 计划
- 将序列化表达式转换为 DataFusion 表达式或 SQL
- 维护序列语义所需的排序元数据
- 执行 count、collect、display、export 等终结操作
- 为序列密集型场景提供专门实现

### 4. 验证与性能层

仓库把测试和 benchmark 视作架构的一部分，而不只是工程卫生。

- `py-ltseq/tests/`：验证能力行为与架构约束
- `benchmarks/`：评估典型有序工作负载下的性能
- `docs/BENCHMARK_AUTORESEARCH.md`：说明 benchmark 研究流程

---

## 主要运行时对象

### `LTSeq`

`LTSeq` 定义于 `py-ltseq/ltseq/core.py`，是用户可见的主表对象。它通过 `_inner` 持有一个 Rust `LTSeqTable`，同时维护 Python 侧元数据：

- `_schema`
- `_sort_keys`
- `_name`

它通过 mixin 组合行为，而不是把所有逻辑放进单一巨型类中。

### `LTSeqTable`

`LTSeqTable` 定义于 `src/lib.rs`，是通过 PyO3 暴露给 Python 的 Rust 核心对象。它持有：

- `SessionContext`
- 可选的懒 `DataFrame`
- 可选的 Arrow schema
- `sort_exprs`
- 用于排序扫描优化的可选 Parquet 源路径

它是大多数 Python 操作背后的权威执行对象。

### `NestedTable`

定义于 `py-ltseq/ltseq/grouping/nested_table.py`。`NestedTable` 表示带有顺序分组上下文的表，而不是已经拍平成聚合结果的普通表。它适用于组身份和组内顺序都重要的场景。

### `LinkedTable`

定义于 `py-ltseq/ltseq/linking.py`。`LinkedTable` 表示两个表之间的惰性关系，只有在确实访问右表数据时才会物化 join。

### `PartitionedTable`

定义于 `py-ltseq/ltseq/partitioning.py`。它暴露逻辑分区视图，并尽量维持项目范围内的 query-first 设计。

---

## Python 包结构

Python 包通过 mixin 组合顶层 API：

- `core.py`：主 `LTSeq` 类和公共辅助方法
- `io_ops.py`：读写、构造、Arrow/pandas 互操作
- `transforms.py`：filter/select/derive/sort/distinct/slice 等变换
- `joins.py`：join、sorted join、as-of join 入口
- `aggregation.py`：聚合、分组入口、分区入口
- `advanced_ops.py`：高级操作和集合操作
- `mutation_mixin.py`：copy-on-write 变更辅助
- `lookup.py`：在 derive 中把 lookup 表达式重写为 join 逻辑

这种组织方式在保持公共 API 宽度的同时，避免了单一实现文件变得过于庞大。

---

## Rust 包结构

Rust crate 采用明确拆分：

- `src/lib.rs`：PyO3 入口、`LTSeqTable`、面向 Python 的薄方法层
- `src/engine.rs`：运行时和 `SessionContext` 创建
- `src/cursor.rs`：流式 cursor 支持
- `src/types.rs`：`PyExpr` 和表达式反序列化
- `src/error.rs`：内部错误模型和 Python 错误映射
- `src/transpiler/`：表达式转换与优化
- `src/ops/`：按能力拆分的操作实现

最重要的结构模式是：`lib.rs` 保持为薄外壳，而 `src/ops/*` 持有真正的执行逻辑。

---

## 表达式链路

LTSeq 的关键架构特征之一是 lambda 捕获链路。

### Python 侧

用户会写出如下表达式：

```python
t.filter(lambda r: r.age > 18)
```

这个 lambda 并不是对真实行值执行，而是对 `SchemaProxy` 执行。代理对象的属性访问和运算符重载会构造表达式节点，再序列化为字典。

关键文件：

- `py-ltseq/ltseq/expr/proxy.py`
- `py-ltseq/ltseq/expr/base.py`
- `py-ltseq/ltseq/expr/types.py`
- `py-ltseq/ltseq/expr/transforms.py`

### Rust 侧

序列化字典在 `src/types.rs` 中转换为 `PyExpr`，可选地经过优化，然后走以下三类路径之一：

- 原生 DataFusion `Expr`
- 原生窗口表达式构造
- SQL 生成回退路径

关键文件：

- `src/transpiler/mod.rs`
- `src/transpiler/window_native.rs`
- `src/transpiler/sql_gen.rs`
- `src/transpiler/optimization.rs`

这个分工是 LTSeq 架构的核心：Python 负责表达语法，Rust 负责执行语义。

---

## 懒执行模型

大多数 LTSeq 操作只是对查询计划做惰性变换。一个新表对象通常意味着：

- 复用同一个 session
- 包装一个新的 DataFusion 计划
- 保留或更新 schema 元数据
- 根据操作类型保留或失效排序元数据

会触发真正执行的通常是：

- `show()`
- `count()`
- `to_arrow()` / `to_arrow_ipc()`
- `to_pandas()`
- `collect()`
- 文件写出

项目依赖 DataFusion 做计划优化和执行，同时保留对目标序列操作绕过通用执行路径的能力。

---

## 排序元数据与顺序语义

LTSeq 不会隐式推断顺序，而是在 Python/Rust 两侧都显式维护排序状态：

- Python 侧：`_sort_keys`
- Rust 侧：`sort_exprs`

这些元数据支持：

- 对序列操作进行验证
- 对 merge-style sorted join 进行验证
- 在安全变换中保留排序状态
- 基于排序 Parquet 的优化和直接扫描快速路径

核心规则很简单：依赖顺序的操作必须建立在已知顺序之上。如果顺序未知，LTSeq 倾向于报错，而不是悄悄给出误导性结果。

---

## No-Materialization Rule

项目最强的架构约束之一，是 no-materialization rule。

凡是返回 `LTSeq`、`NestedTable`、`LinkedTable`、`PartitionedTable` 的查询 API，都应尽量停留在懒执行的 Rust/DataFusion 路径上，不应偷偷通过 pandas、Arrow 往返或 Python 逐行逻辑完成计算。物化应只发生在显式导出或终结 API 中。

这一规则存在的原因包括：

- 性能可预测性
- `LTSeq`、`NestedTable`、`LinkedTable`、`PartitionedTable` 之间的语义一致性

仓库通过专门测试来保护这一约束，而不是只依赖约定。
