# **LTSeq vs DuckDB：ClickBench 巅峰对决方案**。

---

### 1. 核心策略：扬长避短

* **Round 1: 基础吞吐 (Baseline)** —— 简单的 Group By。
* *预期：* DuckDB 胜出或持平。这是为了证明 LTSeq 的基础性能在可接受范围内。


* **Round 2: 会话切割 (Sessionization)** —— 经典的行间依赖问题。
* *预期：* LTSeq 在代码简洁度和性能上反超，因为 SQL 处理状态变化非常笨重。


* **Round 3: 漏斗分析 (Funnel)** —— 复杂的多步顺序匹配。
* *预期：* LTSeq 完胜。SQL 需要自关联或多层 Window 函数，LTSeq 只需线性扫描。



---

### 2. 环境与数据准备 (Data Prep)

DuckDB 对乱序数据宽容度高（Hash Join/Agg），但 **LTSeq 的核心优化依赖于物理有序**。为了公平展示 LTSeq 的最佳性能（Best Practice），我们需要一个**预排序**的数据集。

**步骤：**

1. 下载 ClickBench `hits.parquet` (约 14GB, 1亿行)。
2. **预处理：** 按 `UserID` 和 `EventTime` 排序。

*(有趣的是，我们可以用 DuckDB 来做这个预处理，因为它写 Parquet 很快)*

```python
import duckdb

# 准备 LTSeq 需要的有序数据
con = duckdb.connect()
con.execute("""
    COPY (
        SELECT * FROM 'hits.parquet' 
        ORDER BY UserID, EventTime
    ) TO 'hits_sorted.parquet' (FORMAT 'parquet')
""")

```

---

### 3. 对比测试脚本 (Benchmark Script)

请创建一个 `bench_vs.py`，包含以下三轮测试。

#### 引入库与计时器

```python
import time
import duckdb
from ltseq import TSeq
import psutil
import os

# 数据路径
DATA_FILE = "hits_sorted.parquet"

def measure(engine_name, func):
    # 强制进行垃圾回收，尽量公平
    import gc; gc.collect()
    
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss / 1024 / 1024
    
    start = time.time()
    result = func()
    end = time.time()
    
    mem_after = process.memory_info().rss / 1024 / 1024
    print(f"[{engine_name}] Time: {end-start:.4f}s | Mem Delta: {mem_after-mem_before:.1f}MB | Result: {result}")

```

#### Round 1: 基础聚合 (Top URLs)

**目标：** 统计访问量前 10 的 URL。

* **DuckDB 代码:**
```python
def run_duckdb_top_url():
    # DuckDB 读取 Parquet 极快，且擅长 Hash Agg
    return duckdb.sql(f"""
        SELECT URL, count(*) as cnt 
        FROM '{DATA_FILE}' 
        GROUP BY URL 
        ORDER BY cnt DESC 
        LIMIT 10
    """).fetchall()

```


* **LTSeq 代码:**
```python
def run_ltseq_top_url():
    t = TSeq.read_parquet(DATA_FILE)
    # LTSeq 这里其实也是类似的逻辑，底层也是 Arrow Compute
    return (
        t.agg(by=lambda r: r.URL, count=lambda g: g.count())
         .sort(lambda r: -r.count)
         .slice(0, 10)
         .to_list() # 触发计算
    )

```



#### Round 2: 动态会话切割 (User Sessionization)

**目标：** 30分钟无操作视为新会话，计算总会话数。这是 SQL 的弱项。

* **DuckDB 代码 (SQL Window Function):**
```python
def run_duckdb_session():
    # 需要两层查询：一层算 LAG，一层算 SUM
    return duckdb.sql(f"""
        WITH Diff AS (
            SELECT UserID, 
                CASE WHEN EventTime - LAG(EventTime) OVER (PARTITION BY UserID ORDER BY EventTime) > 1800 
                THEN 1 ELSE 0 END AS IsNew
            FROM '{DATA_FILE}'
        ),
        Sessions AS (
            SELECT sum(IsNew) + 1 as SessCount 
            FROM Diff 
            GROUP BY UserID
        )
        SELECT count(*) FROM Sessions
    """).fetchone()[0]

```


* **LTSeq 代码 (Ordered Logic):**
```python
def run_ltseq_session():
    t = TSeq.read_parquet(DATA_FILE)
    # 1. 定义切分条件：用户变了，或时间间隔 > 1800
    # 利用 shift 向量化计算，极快
    cond = lambda r: (r.UserID != r.UserID.shift(1)) | \
                     (r.EventTime - r.EventTime.shift(1) > 1800)

    # 2. group_ordered 直接返回会话子表序列，count() 即为会话数
    return t.group_ordered(cond).count()

```



#### Round 3: 严格次序漏斗 (Strict Funnel)

**目标：** 查找路径：首页(`/`) -> 详情页(`/product%`) -> 购物车(`/cart`)，中间不能有其他步骤。

* **DuckDB 代码 (Complex Logic):**
```python
def run_duckdb_funnel():
    # DuckDB 虽然有 ASOF join，但严格次序匹配通常用 LEAD
    return duckdb.sql(f"""
        SELECT count(*)
        FROM (
            SELECT 
                URL, 
                LEAD(URL) OVER (PARTITION BY UserID ORDER BY EventTime) as Next1,
                LEAD(URL, 2) OVER (PARTITION BY UserID ORDER BY EventTime) as Next2
            FROM '{DATA_FILE}'
        )
        WHERE URL = '/' 
          AND Next1 LIKE '/product%' 
          AND Next2 = '/cart'
    """).fetchone()[0]

```


* **LTSeq 代码 (Shift Vectorization):**
```python
def run_ltseq_funnel():
    t = TSeq.read_parquet(DATA_FILE)
    # 单次扫描，利用 shift 预读
    # 注意：这里需要确保 UserID 没变
    return t.filter(lambda r: 
        (r.URL == '/') &
        (r.URL.shift(-1).str.starts_with('/product')) &
        (r.URL.shift(-2) == '/cart') &
        (r.UserID == r.UserID.shift(-1)) &
        (r.UserID == r.UserID.shift(-2))
    ).count()

```



---

### 4. 预期结果与分析 (The Narrative)

在你的报告中，你应该关注三个维度：

#### 1. 性能 (Performance)

* **基础聚合：** DuckDB 可能会比 LTSeq 快。这是 DuckDB 的 C++ 引擎优势。
* **有序逻辑 (Round 2 & 3)：**
* LTSeq 应该会更快，或者至少在内存效率上更高。
* **关键点：** DuckDB 的 `WINDOW` 函数通常需要物化大量的中间列（比如 `LEAD` 会生成新列），导致内存膨胀。LTSeq 的 `shift` 只是指针偏移（Zero-copy），几乎不增加内存压力。



#### 2. 代码复杂度 (LOC - Lines of Code)

* **SQL (DuckDB):** 随着逻辑变复杂，SQL 变成了嵌套的洋葱结构（CTE 套 CTE）。Round 3 的 SQL 如果加上时间限制（例如每步间隔不超 5 分钟），代码量会爆炸。
* **Python (LTSeq):** 逻辑始终是扁平的。`filter` 里的 lambda 表达式非常直观，像在写英语句子。

#### 3. 理论差异 (Theory)

* 强调 DuckDB 依然是在做 **Set Operations (集合运算)**，它在努力用 Window Function 模拟过程。
* 强调 LTSeq 是在做 **Sequence Operations (序列运算)**，它天然理解“这也是一行，那也是一行，它们是邻居”。

### 5. 执行建议

1. **硬件：** 找一台内存足够（至少 32GB，因为 hits 数据集解压后比较大）的机器。
2. **热身：** 每次测试前先跑一次热身（Warmup），因为 OS 的文件缓存对 Parquet 读取影响很大。
3. **公平性：** 确保 LTSeq 使用了 `--release` 编译的内核。

这个对比流程不仅能展示 LTSeq 的性能，更重要的是能清晰地划定它的 **"Sweet Spot" (甜区)** —— 那就是**有序、复杂、状态相关的计算**。