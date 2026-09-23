# LTSeq 模块导览

相关文档：

- `docs/README.md`：文档索引
- `docs/ARCHITECTURE.cn.md`：系统架构
- `docs/MODULE_GUIDE.md`：英文贡献者指南
- `docs/DESIGN_SUMMARY.md`：设计历史与取舍
- `docs/api.cn.md`：公开 API 行为

## 目的

面向贡献者的代码地图，为回答一个问题而写：如果你要改某个功能，应该先从哪里读起？

建议配合以下文档一起使用：

- `docs/ARCHITECTURE.cn.md`：系统结构
- `docs/DESIGN_SUMMARY.md`：设计历史与关键取舍
- `docs/api.cn.md`：公开 API 行为

---

## 仓库地图

具有架构意义的顶层目录：

- `src/`：Rust 执行核心
- `py-ltseq/ltseq/`：Python 包与公共 API 层
- `py-ltseq/tests/`：按能力组织的测试
- `examples/`：面向用户的工作流示例
- `docs/`：API、架构和设计文档
- `benchmarks/`：性能实验与 autoresearch 工具

---

## 按任务入口阅读

### 如果你在排查用户侧 API 行为

优先看：

- `py-ltseq/ltseq/core.py`
- `py-ltseq/ltseq/` 中对应的 mixin 文件
- `py-ltseq/tests/` 中对应的测试文件

### 如果你在排查执行行为或 Rust 错误

优先看：

- `src/lib.rs`
- `src/ops/` 中对应文件
- 如果涉及表达式，再看 `src/transpiler/`

### 如果你在排查表达式捕获

优先看：

- `py-ltseq/ltseq/expr/transforms.py`
- `py-ltseq/ltseq/expr/proxy.py`
- `py-ltseq/ltseq/expr/types.py`
- `src/types.rs`
- `src/transpiler/mod.rs`

### 如果你在排查排序敏感或窗口行为

优先看：

- `py-ltseq/ltseq/transforms.py`
- `py-ltseq/ltseq/joins.py`
- `src/ops/sort.rs`
- `src/ops/window.rs`
- 对应的排序/窗口测试

---

## Python 包导览

### `py-ltseq/ltseq/__init__.py`

公共包导出面，统一导出 `LTSeq`、表达式工具和高层包装类型。

### `py-ltseq/ltseq/core.py`

主表类定义。这里负责把 `LTSeq` 通过 mixin 组装起来，并维护公共元数据。

重点关注：

- Rust 绑定导入与可用性检查
- `_from_inner()` 包装构造
- 通用展示方法
- `_schema`、`_sort_keys` 等顶层状态

### `py-ltseq/ltseq/io_ops.py`

读写和互操作接口。

改这些功能时优先看这里：

- CSV/Parquet I/O
- Arrow 或 pandas 转换
- `from_dict`、`from_rows`、`from_arrow`、`from_pandas` 等构造器
- 流式入口

### `py-ltseq/ltseq/transforms.py`

核心变换 API。

改这些功能时优先看这里：

- `filter`
- `select`
- `derive`
- `sort`
- `slice`
- 排序元数据传播
- derive 中 lookup 相关行为

### `py-ltseq/ltseq/joins.py`

join 相关的 Python 入口层。

改这些功能时优先看这里：

- 普通 join
- sorted join
- join 前置条件与用户侧校验
- as-of join API 行为

### `py-ltseq/ltseq/aggregation.py`

聚合、分组和分区相关入口。

改这些功能时优先看这里：

- `agg`
- `group_by`
- `group_ordered`
- `group_sorted`
- `partition`

### `py-ltseq/ltseq/advanced_ops.py`

不适合放在基础变换中的高级操作与集合操作。

### `py-ltseq/ltseq/mutation_mixin.py`

copy-on-write 风格的变更辅助逻辑。适用于那些用户看起来像“修改”但内部仍需保持不可变表风格的方法。

### `py-ltseq/ltseq/lookup.py`

lookup 表达式重写逻辑。它让 derive 表达式内部可以触发 join 风格的行为，而无需暴露单独的用户级 join 步骤。

---

## 表达式子系统导览

表达式系统拆分在几个职责清晰的文件里。

### `py-ltseq/ltseq/expr/proxy.py`

定义用于捕获 lambda 表达式的代理对象。只要 lambda 引用了列，这个文件通常就在调用链里。

### `py-ltseq/ltseq/expr/base.py`

表达式基础机制与序列化行为。

### `py-ltseq/ltseq/expr/types.py`

列、调用、别名、窗口、字面量等具体表达式节点类型。

### `py-ltseq/ltseq/expr/accessors.py`

字符串和日期时间访问器接口，如 `.s` 和 `.dt`。

### `py-ltseq/ltseq/expr/transforms.py`

核心的 lambda 到表达式转换逻辑。如果你想理解 `lambda r: r.a > 1` 是如何变成可序列化树的，应先读这里。

这里也包含针对 lambda 中 `is None` / `is not None` 的有限 AST 改写逻辑。Python 无法重载 `is`，所以 `_transform_lambda_for_none_checks` 会在所属模块的 AST 中定位该 lambda（把同一行上的每个 lambda 放在与原始定义相同的上下文中编译：相同的自由变量、所属模块的 `from __future__` 特性、以及同名的外层类以保证私有名称的改写一致；然后仅忽略嵌套标志位，与函数的 code object 比较，因此同一行多个 lambda、f-string、跨行 lambda 以及模块级 lambda 都不会产生歧义），把每个 `x is None` / `x is not None` 改写为一个调用：操作数是表达式时得到 `x.is_null()` / `x.is_not_null()`，否则保持普通的身份判断；然后基于原始 `__globals__` 与闭包 cell 重建函数。改写由 `_invoke_row_lambda` 统一施加：所有接受 `lambda r: ...` 的公开 API（`filter`、`derive`、`select`、`delete`、`update`、`sort` 等）都通过这一个入口把 lambda 应用到 `SchemaProxy` 上；新增 API 必须调用它而不是自行构造代理，并且必须直接捕获用户的 lambda，而不是捕获包裹它的另一个 lambda。若函数中的 null 身份判断未被改写（REPL 或 `exec`/`eval` 字符串中源码不可用、普通 `def` 函数、或磁盘上的源码已与加载的 lambda 不一致），该函数不会被执行：`_invoke_row_lambda` 读取其字节码并抛出指向 `.is_null()` / `.is_not_null()` 的 `TypeError`。原因是未改写的判断是一个 Python bool，`and`、`or` 或条件表达式会把它从捕获的表达式中静默丢掉。该检查覆盖函数自身及其内部嵌套的函数，不覆盖它调用的辅助函数；并且在没有源码时无法区分列与 Python 值，因此这类函数中对 Python 值的 `is None` 判断同样会被拒绝。

---

## Grouping 与 NestedTable 导览

### `py-ltseq/ltseq/grouping/nested_table.py`

`NestedTable` 的主要实现文件。

以下功能都应优先从这里开始：

- `group_ordered` 行为
- `group_sorted` 行为
- group-level `filter` / `derive`
- flatten 分组结果
- 分组内部元数据语义

### `py-ltseq/ltseq/grouping/proxies/`

组代理表达式逻辑。如果 group filter 或 group derive 的 SQL/表达式转换有问题，通常要看这里。

---

## Linking 导览

### `py-ltseq/ltseq/linking.py`

`LinkedTable` 的实现文件。

以下功能都应优先从这里开始：

- `link()` 行为
- 惰性与物化的边界
- linked 列访问
- 链式 linking 行为
- alias 与前缀列语义

如果 bug 表现为 linked table 意外变成 flat table，或 linking 后 schema 不一致，这个文件通常最相关。

---

## Partitioning 导览

### `py-ltseq/ltseq/partitioning.py`

分区视图与 SQL-backed 分区行为的实现文件。

以下功能都应优先从这里开始：

- `partition()` 返回类型
- callable partition 的约束
- partition 查询行为

---

## Rust 核心导览

### `src/lib.rs`

暴露给 Python 的 Rust 模块边界。

当你需要理解以下问题时，应先看这里：

- Python 能直接调用哪些方法
- `LTSeqTable` 如何构造与返回
- Rust 执行从哪里开始

但不要假设具体逻辑都在这里，大多数重要实现都被委派到别处。

### `src/engine.rs`

Session 与运行时配置。

适用于排查：

- DataFusion 配置
- Tokio runtime 设置
- 执行上下文默认值

### `src/cursor.rs`

惰性 scan API 的流式支持。

### `src/types.rs`

将表达式反序列化为 `PyExpr`。

### `src/error.rs`

内部错误类型与 Python 异常映射。
