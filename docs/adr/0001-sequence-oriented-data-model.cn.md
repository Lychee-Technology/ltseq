# ADR 0001: 序列导向数据模型（行序是数据模型的一等公民）

- 状态：已采纳（Accepted）
- 决策日期：早于本记录 · 记录日期：2026-07-26

[English](0001-sequence-oriented-data-model.md)

## 背景

传统的 dataframe 与 SQL 系统（pandas、关系数据库）把表视为无序的行集合，行序至多是展示层的元数据。而 LTSeq 面向的工作负载（时间序列、事件流、连续段 streak/run 检测、漏斗分析、状态机式计算）本质上是关于*有序*数据的：“上一行”、“一段连续相等的值”、“此行之后第一个满足条件的行”都是一等问题。

## 决策

把行序当作计算的语义输入，数据模型与查询模型中的一等公民。数据按有序序列处理，而非无序集合。

这一个选择驱动了后续整个架构：

- **API**：引用相邻行的窗口函数（`shift`、`diff`、`rolling`、累计类操作）、按连续段分组的顺序分组（`group_ordered`）、有序搜索（首个匹配即止的 `search_first`、漏斗匹配的 `search_pattern`）、merge join 与 as-of join。（旧文档把 `search_first` 描述为二分搜索；当前实现是原生惰性的 `filter(...).limit(1)`，无排序前置条件，见 [ADR 0006](0006-multi-path-execution-strategy.cn.md)。）
- **元数据**：排序状态必须在查询管道中全程跟踪与传播（见 [ADR 0008](0008-explicit-sort-metadata.cn.md)）。
- **执行**：已排序的输入可解锁专用快速路径（见 [ADR 0006](0006-multi-path-execution-strategy.cn.md)）。
- **测试**：顺序语义本身作为产品能力被测试覆盖（见 [ADR 0015](0015-tests-benchmarks-as-architecture.cn.md)）。

## 备选方案

pandas/SQL 式的集合语义被否决为核心模型。`README.md` 的 FAQ 明确以此作为差异点：正因为在无序模型里顺序依赖的计算既别扭又不可靠，LTSeq 才存在。

## 影响与取舍

- 序列操作表达自然、校验成本低。
- 部分 API 要求先显式 `sort()`（或 `assume_sorted()`）；结果依赖顺序的操作在顺序未知时抛 `SortRequiredError`，宁可报错也不静默产出误导性结果。
- 排序元数据的维护成为贯穿所有表变换操作的长期义务。

## 来源

- `docs/ARCHITECTURE.cn.md`: 概览、设计目标
- `docs/USER_MODEL.cn.md`: “LTSeq 不是什么”
- `docs/DESIGN_SUMMARY.cn.md`: 概览
- `README.md`: Design Philosophy、FAQ
- `CLAUDE.md`: Architecture Overview
