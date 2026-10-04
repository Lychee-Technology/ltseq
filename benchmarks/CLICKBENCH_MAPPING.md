# LTSeq ClickBench 与标准 ClickBench 对应关系

## 一、标准 ClickBench 简介

[标准 ClickBench](https://github.com/ClickHouse/ClickBench) 是 ClickHouse 团队发布的 OLAP 基准测试，使用 Yandex Metrica 的 Web 点击流数据集（`hits` 表，约 1 亿行），包含 **43 条 SQL 查询**，覆盖以下场景：

| 查询范围 | 场景类型 |
|---|---|
| Q1–Q7 | 全表聚合（COUNT、SUM、AVG、MIN/MAX、COUNT DISTINCT） |
| Q8–Q19 | GROUP BY 聚合（按 RegionID、UserID、SearchPhrase 等分组） |
| Q20 | 点查（UserID = 具体值） |
| Q21–Q27 | LIKE 过滤 + ORDER BY |
| Q28–Q29 | 带 HAVING 的复杂 GROUP BY |
| Q30 | 宽列 SUM（SUM(Width+0) … SUM(Width+89)，90 个表达式） |
| Q31–Q36 | 多列 GROUP BY（WatchID、ClientIP 等高基数字段） |
| Q37–Q43 | 带日期范围过滤的聚合（CounterID=62，2013-07-01 至 07-31） |

**标准 ClickBench 的特点**：全部是无序的批量集合运算（Batch Set Operations），不涉及行间依赖、顺序匹配或状态追踪。

---

## 二、LTSeq ClickBench 设计思路

LTSeq 的 ClickBench 使用**完全相同的数据集**（`hits.parquet`，1 亿行），但采用 **3 轮递进式**对比设计，而非直接复现全部 43 条查询。

设计原则：**扬长避短（Play to Strengths）**

| 轮次 | 场景 | 预期结果 | 意图 |
|---|---|---|---|
| R1 | 基础聚合（Top URLs） | DuckDB 胜出 | 证明 LTSeq 基础性能可接受 |
| R2 | 用户会话切割（Sessionization） | LTSeq 反超 | 展示有序状态计算的优势 |
| R3 | 严格顺序漏斗（Funnel） | LTSeq 大幅领先 | 展示 LTSeq 的核心差异化能力 |

---

## 三、各轮次与标准 ClickBench 的对应关系

### Round 1：基础聚合（Top 10 URLs）

**LTSeq 实现：**
```python
t.agg(by=lambda r: r.url, cnt=lambda g: g.url.count())
 .sort("cnt", desc=True)
 .slice(0, 10)
 .to_dicts()
```

**等价 SQL（DuckDB）：**
```sql
SELECT url, count(*) as cnt
FROM 'hits_sorted.parquet'
GROUP BY url
ORDER BY cnt DESC
LIMIT 10
```

**对应标准 ClickBench 查询：**

| 标准查询编号 | 标准查询（节选） | 相似度 |
|---|---|---|
| **Q34** | `SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10` | ★★★★★ 完全等价 |
| Q16 | `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10` | ★★★★ 结构相同，分组字段不同 |
| Q13 | `SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10` | ★★★ 相同模式，加了过滤条件 |
| Q9 | `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10` | ★★ COUNT DISTINCT 而非 COUNT |

**说明**：R1 是标准 ClickBench 中最常见的查询模式的简化版——单字段 GROUP BY + COUNT + ORDER BY + LIMIT 10。标准 ClickBench 的 Q34 是最直接的对应，两者在语义上完全等价。

**实测结果（macOS arm64，32GB RAM，1亿行）：**
- DuckDB：2.49s，内存 +172 MB
- LTSeq：3.99s，内存 +1349 MB
- 结论：DuckDB 快约 1.6×（符合预期，DuckDB 的 Hash Aggregation 高度优化）

---

### Round 2：用户会话切割（Sessionization）

**LTSeq 实现：**
```python
# 定义切分条件：用户变了，或时间间隔 > 1800 秒
cond = lambda r: (r.userid != r.userid.shift(1)) | (r.eventtime - r.eventtime.shift(1) > 1800)
grouped = t_sorted.group_ordered(cond)
return grouped.first().count()
```

**等价 SQL（DuckDB）：**
```sql
WITH Diff AS (
    SELECT userid,
        CASE WHEN eventtime - LAG(eventtime)
            OVER (PARTITION BY userid ORDER BY eventtime) > 1800
        THEN 1 ELSE 0 END AS is_new
    FROM 'hits_sorted.parquet'
)
SELECT sum(is_new) + count(DISTINCT userid) as total_sessions
FROM Diff
```

**对应标准 ClickBench 查询：无直接对应**

标准 ClickBench **不包含任何窗口函数查询**，也没有行间依赖（Inter-row Dependency）类型的查询。R2 代表了标准 ClickBench 刻意回避的一类问题——需要维护跨行状态的顺序计算。

| 特征 | 标准 ClickBench（Q1-Q43） | LTSeq R2（Sessionization） |
|---|---|---|
| 行间依赖 | 无 | 有（每行依赖前一行的时间戳） |
| 窗口函数 | 无 | LAG / shift |
| 状态追踪 | 无 | 会话边界检测 |
| 结果类型 | 聚合统计 | 有序分组计数 |

**实测结果：**
- DuckDB：1.57s，内存 +58 MB
- LTSeq：0.21s，内存 +2 MB
- 结论：**LTSeq 快 7.6×，内存仅 1/29**

性能差异来源：DuckDB 的 `LAG OVER (PARTITION BY ... ORDER BY ...)` 需要物化中间列并维护排序状态。LTSeq 在这一轮并不执行 `shift`：`group_ordered(cond).first().count()` 命中专用内核，不经过 DataFusion，直接并行读取已排序的 Parquet，比较相邻行来数会话边界，也不生成逐行分组数组。这条快路径只对特定查询形状生效，见“六、快路径的适用条件与回退”。

---

### Round 3：严格顺序漏斗（URL Funnel）

**LTSeq 实现：**
```python
t_sorted.search_pattern_count(
    lambda r: r.url.s.starts_with("http://liver.ru/saint-peterburg"),
    lambda r: r.url.s.starts_with("http://liver.ru/place_rukodel"),
    lambda r: r.url.s.starts_with("http://liver.ru/belgorod"),
    partition_by="userid",
)
```

**等价 SQL（DuckDB）：**
```sql
SELECT count(*)
FROM (
    SELECT
        url,
        LEAD(url) OVER (PARTITION BY userid ORDER BY eventtime) as next1,
        LEAD(url, 2) OVER (PARTITION BY userid ORDER BY eventtime) as next2
    FROM 'hits_sorted.parquet'
)
WHERE starts_with(url, 'http://liver.ru/saint-peterburg')
  AND starts_with(next1, 'http://liver.ru/place_rukodel')
  AND starts_with(next2, 'http://liver.ru/belgorod')
```

**对应标准 ClickBench 查询：无直接对应**

标准 ClickBench 同样不包含任何顺序模式匹配（Sequential Pattern Matching）查询。R3 代表了 LTSeq 的核心差异化场景。

| 特征 | 标准 ClickBench（Q1-Q43） | LTSeq R3（Funnel） |
|---|---|---|
| 多步骤顺序匹配 | 无 | 3 步严格顺序（Step1 → Step2 → Step3） |
| LEAD 窗口函数 | 无 | LEAD(url), LEAD(url, 2) |
| 中间列物化 | 无 | DuckDB 需要物化 2 个 LEAD 列 |
| 算法复杂度 | O(n) 扫描 | DuckDB O(n log n)，LTSeq O(n) |

**实测结果：**
- DuckDB：15.03s，内存 +675 MB
- LTSeq：4.00s，内存 +17 MB
- 结论：**LTSeq 快 3.76×，内存仅 1/40**

性能差异来源：DuckDB 的 `LEAD(url)` 和 `LEAD(url, 2)` 需要为每行计算并存储 2 个新列（涉及大量字符串复制），再过滤。LTSeq 的 `search_pattern_count` 命中专用内核：按 row group 并行读取已排序的 Parquet；三个步骤都是同一字符串列上的 `starts_with`，所以直接对原始字符串做前缀比较，不生成中间列；跨 row group 的匹配单独拼接。适用条件见“六、快路径的适用条件与回退”。

---

## 四、整体对应关系总结

```
标准 ClickBench（43 条查询）
│
├── Q1–Q43：无序批量集合运算（GROUP BY, COUNT, SUM, LIKE, ORDER BY）
│   │
│   └── LTSeq R1 对应 → Q34（URL GROUP BY COUNT LIMIT 10）
│                        及同类型的 Q16、Q13、Q9 等
│
└── [标准 ClickBench 的盲区]
    │
    ├── 行间依赖 / 窗口函数（LAG/LEAD）
    │   └── LTSeq R2 对应 → 用户会话切割（Sessionization）
    │
    └── 顺序模式匹配（Sequential Pattern Matching）
        └── LTSeq R3 对应 → URL 漏斗分析（Funnel）
```

| 维度 | 标准 ClickBench | LTSeq ClickBench R1 | LTSeq ClickBench R2 | LTSeq ClickBench R3 |
|---|---|---|---|---|
| **数据集** | hits（1 亿行） | 同左 | 同左 | 同左 |
| **查询类型** | 集合运算（Set Ops） | 集合运算 | 序列运算（Seq Ops） | 序列运算 |
| **行间依赖** | 无 | 无 | 有（LAG） | 有（LEAD×2） |
| **最接近标准查询** | — | **Q34** | 无等价查询 | 无等价查询 |
| **LTSeq 优势** | — | 无（DuckDB 1.6×） | 7.6× 快，1/29 内存 | 3.76× 快，1/40 内存 |

---

## 五、设计意图解读

标准 ClickBench 考察的是 OLAP 引擎对**无序批量集合运算**的吞吐能力——这正是 DuckDB 的核心优化方向（向量化 Hash Aggregation、列式存储、SIMD）。

LTSeq 的 ClickBench 使用相同数据集，但针对标准 ClickBench **刻意回避**的一类问题：**有序、状态相关的序列计算（Ordered, Stateful Sequence Computation）**。这类问题在真实的用户行为分析场景（会话切割、漏斗分析、留存追踪）中极为普遍，而 SQL 窗口函数处理它们时代价高昂：

- **物化开销**：每个 LEAD/LAG 列都要分配新的内存列
- **排序开销**：PARTITION BY + ORDER BY 需要 O(n log n) 排序
- **代码复杂度**：多步骤漏斗需要嵌套 CTE，可读性差

R2/R3 的数量级优势来自为这两种查询形状专门编写的 Rust 内核：它们直接并行读取已排序的 Parquet，只读所需列，不物化 LAG/LEAD 中间列。谓词写法、数据来源或分区列类型稍有不同，就会回退到通用路径，结果相同但慢一个数量级以上（见第六节）。因此这两轮说明的是 LTSeq 在这些形状上的表现，不代表任意序列查询都能获得同等加速。

---

## 六、快路径的适用条件与回退

R2/R3 中 LTSeq 的耗时来自按查询形状选择的专用 Rust 内核（`src/ops/parallel_scan.rs`、`src/ops/linear_scan.rs`、`src/ops/pattern_match.rs`）。查询不满足下列条件时，LTSeq 回退到通用路径：结果相同，但要先把所需列收集到内存。在一个合成的 500 万行已排序 Parquet 上，两轮的通用路径都比快路径慢 13–24 倍；仅把 R2 谓词写成 `r.userid.shift(1) != r.userid` 就足以离开快路径。R1 没有快路径，是普通的 DataFusion 聚合。

### R2：`group_ordered(cond).first().count()`

快路径（并行直读 Parquet，只数分组边界）要求同时满足：

1. 表直接来自对单个 Parquet 文件的 `LTSeq.read_parquet(...)`，再接 `assume_sorted(...)`。其他来源（CSV、`from_arrow`、`collect()`）或两者之间的任何变换（包括 `sort()`）都会断开与文件的关联。
2. 以 `first().count()`（或 `len()` 作用于 `first()`）消费分组。对 `first()` 的其他用法会通过 DataFusion 窗口函数物化分组表。
3. `cond` 由 `|`、`&` 组合而成，且每个叶子恰好是 `r.c != r.c.shift(1)` 或 `(r.c - r.c.shift(1)) > N`：`c` 是 Int32/Int64/UInt32/UInt64 或 timestamp 列，`N` 是整数字面量，或有限、取整数值且绝对值小于 `2**53` 的浮点字面量。R2 的谓词正是这个形状。操作数交换（`r.c.shift(1) != r.c`）、其他比较（`>=`、`<`、`==`）、`n != 1` 的 `shift(n)`、字符串列、`is_null()` 都不在其内。

条件 1 或 3 不满足时走通用线性扫描路径：按声明顺序收集谓词列和全部排序键（数据源未声明该顺序时会先排序），用中间数组求值谓词，构造三个逐行数组，再经 DataFusion 计数。该路径仍要求谓词只由列、字面量、`shift(1)`、`is_null()`、比较、算术、`&`、`|`、`~` 组成，且至少含一个 `shift(1)`；否则物化分组表后计数。条件 3 不接受的浮点 `N` 作用于整数列时也会物化，但要等线性扫描路径收集完数据之后：该路径的求值器不做整数与浮点之间的类型转换，会直接报错（#189）。线性扫描计数与 DataFusion 路径目前在 NULL 上结果不一致（#189），因此传给 `read_parquet` 的目录（内核无法读取）不走线性扫描路径，而是物化分组表。

### R3：`search_pattern_count(*steps, partition_by=col)`

快路径（按 row group 并行匹配，再拼接跨 row group 的匹配）要求同时满足：

1. 表直接来自对单个文件的 `read_parquet(...)` + `assume_sorted(...)`，与 R2 相同。目录目前会直接报错，而不是回退（#217）。
2. 指定了 `partition_by`，且分区列是 Int32/Int64/UInt32/UInt64。字符串分区列目前会直接报错，而不是回退（#211）。

内核中，若所有步骤都是同一字符串列上的 `r.c.s.starts_with("字面量")`，就对原始字符串做前缀循环比较。这只取决于形状而不是列名，R3 在 `url` 上的三个前缀只是其中一例。其他步骤谓词在每个 row group 上向量化求值；内核无法求值的谓词会让整个计数回退到通用路径。

不满足条件 1、未指定 `partition_by` 或内核放弃时，通用路径按声明顺序把所需列收集为一个批次，并在整批上求值第 1 步。同一列上的后续 `starts_with` 步骤只在第 1 步命中的行上做前缀比较；其他步骤各自在整批上求值。两条路径都用 LTSeq 自己的求值器而不是 DataFusion，该求值器没有隐式类型转换（#188）。
