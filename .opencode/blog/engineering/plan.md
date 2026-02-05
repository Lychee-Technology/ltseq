# LTSeq 博客系列规划（全中文）

## 系列总名
《LTSeq 代码低语者：序列分析的架构解剖》

## 系列目标
- 逐个功能 deep dive：每篇聚焦 1 个核心能力 + 1 个关键机制 + 1 个工程权衡
- 建立“序列优先”的心智模型：LTSeq 不是另一个 DataFrame
- 面向中级工程师：可读性与架构深度并重

## 发布节奏
- 每周 1 篇，共 13–15 篇（含导读）
- 前 6 篇搭建核心框架，后 7–9 篇进入高级能力与性能路径

## 篇幅与深度
- 每篇 2000–3000 字，技术深度适中
- 结构固定：痛点引入 → 心智模型 → 源码机制 → 权衡 → 价值落点

## 系列目录与逐篇提纲

### 00. 整体介绍（导读，约 1000 字）
**标题建议**：LTSeq 入门：为什么“序列优先”的表能改变分析方式

**目标**
- 用 1000 字完成概念澄清 + 心智模型 + 能力地图
- 轻量但有架构视角，点到关键模块名

**结构提纲**
1) 工程痛点引入（事件流、时间序列的语义缺失）
2) 一句话定义 LTSeq
3) 心智模型（流水线/队列）
4) 能力地图（窗口、连续分组、惰性关联）
5) 实现方式简述（Python DSL + Rust 内核 + 表达式捕获）
6) 工程权衡（强语义 vs 使用门槛）
7) 后续阅读导航

### 01. LTSeq 为什么存在：当你的数据是“序列”，而不是“集合”
**Hook**：当数据来自事件流时，集合思维制造偏差

**心智模型**：把表视为有方向的队列

**源码机制（轻量点到）**
- `LTSeq` + mixin 组合式 API
- `_inner` 作为 Rust 执行内核的边界

**权衡**
- 强语义 vs 使用门槛（显式排序、表达式可序列化）

**Why It Matters**
- 适合序列分析、时序策略、状态机

### 02. Lambda 如何变成执行计划：表达式捕获管线
**Hook**：为什么 LTSeq 能把 lambda 变成执行计划

**心智模型**：不是执行 Python，而是记录你的意图

**源码机制**
- `SchemaProxy` → `ColumnExpr`
- `Expr` 序列化为 dict 树 → Rust

**权衡**
- 可序列化性 vs 语法自由度

**Why It Matters**
- 稳定、可优化、可跨语言执行

### 03. 排序是第一公民：为何很多操作要求显式 sort
**Hook**：窗口函数为什么需要先 sort

**心智模型**：没有顺序就没有前后关系

**源码机制**
- `_sort_keys` 跟踪与约束

**权衡**
- 确定性 vs 用户负担

**Why It Matters**
- 避免隐式排序带来的隐形开销

### 04. 窗口函数的执行路径：shift/rolling/diff 的底层逻辑
**Hook**：window 不只是语法糖

**心智模型**：滑动窗口观察序列

**源码机制**
- window 检测与执行路径选择
- Rust 矢量化处理

**权衡**
- 快路径性能 vs 表达式检测复杂度

**Why It Matters**
- 金融、指标序列的核心能力

### 05. 连续分组：group_ordered 与 group_sorted 的真实语义
**Hook**：为什么需要连续分组

**心智模型**：同值连续区间才是“状态段”

**源码机制**
- `NestedTable` 与 `__group_id__`

**权衡**
- 强语义 vs 与传统 groupby 的差异

**Why It Matters**
- 趋势段、事件爆发段分析

### 06. Group Derive：把组操作翻译成窗口 SQL
**Hook**：g.first().price 是 window 计划

**心智模型**：组轨道上的窗口函数

**源码机制**
- `DeriveSQLParser` / `group_expr_to_sql`

**权衡**
- SQL 可表达性 vs pandas fallback

**Why It Matters**
- 让组级计算变成可优化的计划

### 07. LinkedTable：指针式关联与惰性 Join
**Hook**：能不能先连线后取值

**心智模型**：外键引用，按需物化

**源码机制**
- `LinkedTable` lazy materialize
- 嵌套访问的 `SchemaProxy`

**权衡**
- 惰性访问 vs 物化成本

**Why It Matters**
- 大表低频关联访问

### 08. Lookup：把列级增强偷偷变成 Join
**Hook**：lookup 看似函数，本质 join 重写

**心智模型**：字典查表

**源码机制**
- `LookupMixin` 表达式重写

**权衡**
- 语法简洁 vs join 透明度降低

**Why It Matters**
- 业务语言更自然的数据增强

### 09. Join 家族：hash / merge / asof 的权衡
**Hook**：不同 join 是对不同秩序的承诺

**心智模型**：乱序合并 vs 时间就近配对

**源码机制**
- `_extract_join_keys` / `_extract_asof_keys`

**权衡**
- 适用面广 vs 表达式限制

**Why It Matters**
- 金融与事件流的 join 关键场景

### 10. Partitioning vs Grouping：两套切片语义
**Hook**：partition 和 groupby 不是一个东西

**心智模型**：分区容器 vs 连续语义

**源码机制**
- `PartitionedTable` / `SQLPartitionedTable`

**权衡**
- SQL 快路径 vs Python 灵活路径

**Why It Matters**
- 批量策略、区域并行处理

### 11. 集合操作：union/intersect/diff 的数据策略
**Hook**：集合操作是数据契约的约束

**心智模型**：schema 对齐的合并

**源码机制**
- `SetOpsMixin` 交给 Rust

**权衡**
- 强一致性 vs 使用门槛

**Why It Matters**
- 数据对齐与消重

### 12. Streaming 与 Cursor：面向大数据的迭代模型
**Hook**：不把全表读进内存

**心智模型**：批次流水线

**源码机制**
- `Cursor` 与 `scan()`

**权衡**
- 流式省内存 vs 操作消耗性

**Why It Matters**
- 超大 CSV/Parquet 的第一道防线

### 13. Fallback 策略：当快路径失败时发生什么
**Hook**：系统健壮性来自 fallback 设计

**心智模型**：主引擎失败切换应急发电机

**源码机制**
- pandas fallback 的触发点

**权衡**
- 正确性优先 vs 性能波动

**Why It Matters**
- 生产环境稳定性保障

### 14. 性能故事：矢量化与 Rust 执行路径
**Hook**：快不是口号，是执行路径

**心智模型**：Arrow 批处理 + Rust 内核

**源码机制**
- Rust binding 入口与数据类型边界

**权衡**
- Rust 依赖 vs 生态兼容

**Why It Matters**
- 解释 LTSeq 速度来源

## 写作模板（每篇统一结构）
1) 工程痛点引入（Why hook）
2) 心智模型（1 段比喻 + 1 段“文字架构图”）
3) 源码机制拆解（2–3 个关键类/模块）
4) 权衡分析（清晰点出取舍）
5) 价值落点（适用场景）
