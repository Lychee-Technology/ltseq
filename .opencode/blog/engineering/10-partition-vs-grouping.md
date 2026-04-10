# Partitioning vs Grouping：两套切片语义

在很多数据工具里，“分组”几乎等同于“切片”，你习惯用 `groupby` 解决所有“按某个键拆开”的需求。但在序列分析里，切片有两种完全不同的语义：

- **分区（partition）**：把所有相同 key 的行放进同一个容器
- **连续分组（group_ordered）**：只关心“连续相同”的区间

LTSeq 把这两件事明确分开，这是非常工程化的设计。否则，你很容易在“连续语义”和“全局语义”之间犯错。

这一篇讲清楚一个关键区别：**partition 是容器语义，grouping 是连续语义**。

## 1. 痛点：用 groupby 解决所有切片需求

在传统工具里，你通常会这么做：

- 想按地区拆分 → `groupby(region)`
- 想按用户拆分 → `groupby(user_id)`

这在统计场景里没问题，但在序列场景里会出现混淆：

- 你想的是“连续区间”，却得到了“全局聚合”
- 你想的是“按键分容器”，却用的是“连续语义”

LTSeq 的设计从根源上拆开了这两个语义，避免了隐性误解。

## 2. 心智模型：容器 vs 片段

可以用两个直观模型来理解：

- **Partition 是容器**：把所有同 key 的行装进一个桶，不管它们在序列中出现在哪里
- **Grouping 是片段**：只有“连续出现”的同 key 行才算一段

这意味着：

- Partition 面向“全局切片”
- Grouping 面向“序列区间”

两者都很重要，但解决的是不同问题。

## 3. Partitioning 的机制：PartitionedTable

LTSeq 的 `partition()` 返回的是 `PartitionedTable`（见 `py-ltseq/ltseq/partitioning.py`）。它的核心行为是：

1) 计算每一行的 partition key
2) 把拥有相同 key 的行聚合在同一个子表里
3) 提供 dict-like 的访问方式（`partitions[key]`）

这是一种“容器语义”，适合做并行处理、分区计算或按键批量操作。

### SQLPartitionedTable：快路径

如果你用 `partition_by("col")`，LTSeq 会尝试走 SQL 快路径：

- 通过 DISTINCT 获取 keys
- 用 SQL WHERE 抽取分区

这避免了逐行 Python 计算，也更适合大表场景。

## 4. Grouping 的机制：NestedTable

相比之下，`group_ordered()` 或 `group_sorted()` 返回的是 `NestedTable`。它强调的是“连续段”，并且会生成 `__group_id__` 来标记段落。

这意味着：

- 同一个 key 在序列里出现两次，会生成两个不同的 group
- group 的意义是“片段”，不是“全局集合”

## 5. 什么时候用 partition，什么时候用 grouping

### 用 partition 的场景

- 你要按地区、用户、品类分桶
- 你想对每个桶独立计算
- 你关心的是“所有相同 key 的数据”

### 用 grouping 的场景

- 你关心“连续区间”
- 你想识别趋势段、状态段
- 你关注的是序列过程，而非全局聚合

一个直观原则：**如果你关心“连续”，就用 grouping；如果你关心“全局归类”，就用 partition。**

## 6. 工程权衡：灵活性 vs 语义精准

LTSeq 把这两种语义拆开，带来一些工程权衡：

- **学习成本增加**：用户需要理解两套切片语义
- **语义更精准**：避免了“误用 groupby”的隐性错误
- **性能路径更明确**：partition 可以走 SQL 快路径，grouping 可以走窗口路径

这种选择强调的是“正确表达”优先，而不是让 API 看起来更简单。

## 7. 为什么这对序列分析很关键

在序列世界里，很多问题不是“按 key 分桶”，而是“识别过程”。

- 一段持续活跃的时间
- 一段连续上涨的趋势
- 一个故障连续出现的窗口

这些都属于 grouping 的语义。如果把它们误用 partition 去做，结果会失真。

LTSeq 通过明确区分 partition 与 grouping，让序列语义不再被“集合思维”吞没。

## 8. 下一篇预告

下一篇会进入集合操作：`union`、`intersect`、`diff`。这些操作看似简单，但在 LTSeq 中它们更多是在维护“schema 契约”与“序列一致性”。
