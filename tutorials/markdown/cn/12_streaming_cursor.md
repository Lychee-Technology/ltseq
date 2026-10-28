<!-- Generated from ../../cn/12_streaming_cursor.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 12 · 超大文件流式处理：游标 scan

> **难度** ★★☆ · **领域** 通用 / 日志 · **前置** 案例 01
>
> **涉及 API**：`LTSeq.scan` / `Cursor`（迭代 / `count` / `to_pandas` / `schema` / `columns`）/ `read_csv` / `write_parquet` / `scan_parquet`

**学习目标**

- 数据装不进内存时的正确姿势：**游标分批拉取**，处理完即释放
- 掌握 Cursor 的三种消费方式：逐批迭代、纯计数、物化剩余
- 理解为什么大文件场景首选 Parquet + zstd

数据装不进内存时用游标分批拉取。LTSeq 的 `Cursor` 每批直接是 `pyarrow.RecordBatch`，无缝接入 Arrow 生态。

## 问题描述

一个 20 万行的访问日志 CSV。虽然它内存装得下，但请假设装不下（换成 2 亿行结论
不变）。用游标完成：

1. 不加载文件，先看列与类型；
2. 逐批迭代——只处理前 3 批就提前退出；
3. 纯计数（不物化任何行到 Python）；
4. 迭代一部分后，把**剩余**部分物化成 pandas；
5. 转 Parquet（zstd）后再流式读回，对比体验。

```python
import random

rng = random.Random(9)
with open("access_log.csv", "w") as f:
    f.write("ts,user_id,status,latency_ms\n")
    ts = 1_700_000_000
    for i in range(200_000):
        ts += rng.randint(1, 5)
        status = rng.choice([200, 200, 200, 404, 500])
        f.write(f"{ts},u{rng.randint(1, 5000)},{status},{rng.randint(5, 900)}\n")

print("access_log.csv 就绪")
```

> **预期输出**
>
> ```text
> access_log.csv 就绪
> ```

## 第一步 · 游标：看 schema、逐批迭代、提前退出

`LTSeq.scan(path)` 返回 `Cursor`，**不加载文件**；迭代时每批是一个
`pyarrow.RecordBatch`。

```python
from ltseq import LTSeq

cursor = LTSeq.scan("access_log.csv")
print("columns:", cursor.columns)
print("schema: ", cursor.schema)

batch_sizes = []
for batch in cursor:                    # batch: pyarrow.RecordBatch
    batch_sizes.append(batch.num_rows)
    if len(batch_sizes) == 3:
        break                           # 提前退出：不再拉取后续批次

print("前 3 批行数:", batch_sizes)
```

> **预期输出**
>
> ```text
> columns: ['ts', 'user_id', 'status', 'latency_ms']
> schema:  {'ts': 'Int64', 'user_id': 'Utf8', 'status': 'Int64', 'latency_ms': 'Int64'}
> 前 3 批行数: [16384, 16384, 16384]
> ```

批大小与机器有关：扫描默认按 CPU 核数切成同样多的分区，每个分区把自己那段文件
切成最多 16,384 行的批，而且各分区并发竞争，所以即使在同一台机器上，前三批里偶尔
也会混进某个分区较短的最后一批。

**解析**

- `cursor.columns` / `cursor.schema` 返回的是 `scan()` 打开文件时确定的 schema
  （CSV 由开头若干行推断，Parquet 自带 schema），读取它们不加载数据。
- `break` 只是停止拉取：引擎最多再预读一点就停下等待，游标停在原位，再写一个循环
  或 `next(cursor)` 会从下一批接着读。游标对象本身被释放时（`del cursor`，或像
  第二步那样给变量重新赋值），底层执行才**被取消**、资源才释放。
- 批次不按文件顺序到达：各分区的批次交错输出，顺序每次运行都可能不同
  （issue #148）。逐批处理的代码不能假设这一批紧接着上一批。
- 批大小由引擎决定（与 Arrow 批约定一致），业务代码永远按批处理、不依赖具体值。

## 第二步 · 纯计数与物化剩余

```python
# 2) 纯计数：消费整条流，但一行都不物化成 Python 对象
total = LTSeq.scan("access_log.csv").count()
print("总行数:", total)

# 3) 部分迭代 + 物化"剩余"
cursor = LTSeq.scan("access_log.csv")
first = next(iter(cursor))              # 只拉第一批
rest = cursor.to_pandas()               # 从当前进度起，把剩余流物化完
print("首批行数:", first.num_rows, "/ 剩余行数:", len(rest))
```

> **预期输出**
>
> ```text
> 总行数: 200000
> 首批行数: 16384 / 剩余行数: 183616
> ```

这两个数同样随机器变化，原因同上。

**解析**

- `count()` 是"最便宜的完整消费"：流式过一遍、只累加行数，内存占用与文件大小
  解耦。
- `to_pandas()` 物化的是**剩余**流——迭代到哪，就从哪继续；`to_arrow()` 同理。
- 200k 行的演示里物化没问题；真到 2 亿行，`to_pandas()` 之前请先想清楚下游
  消费方式（分批写库 / 聚合后再物化）。

## 第三步 · Parquet：更小、更快、保留类型

```python
t = LTSeq.read_csv("access_log.csv")
t.write_parquet("access_log.parquet", compression="zstd")

cursor = LTSeq.scan_parquet("access_log.parquet")
print("parquet 行数:", cursor.count())
```

> **预期输出**
>
> ```text
> parquet 行数: 200000
> ```

**解析**

- `write_parquet(path, compression=)` 支持 `snappy / zstd / gzip / lz4 / none`；
  日志类数据 zstd 压缩比与速度兼顾。
- Parquet 是列式存储并自带 schema：写进去什么类型，读出来就是什么类型，下游
  `dt.*` 运算免 cast。秒精度的时间戳和时间读回来是毫秒，因为 Parquet 没有秒这个单位。
  CSV 的类型要从文本重新推断：ISO 格式的日期、时间戳能认出来，
  但 Decimal 会变成 float64，`2024/01/02` 这样的日期会变成字符串。
- **预排序 + Parquet 是窗口计算的黄金组合**：`LTSeq.read_parquet(...)` 之后
  `assume_sorted("ts")` 声明顺序，跳过物理排序直接进窗口计算（游标没有
  `assume_sorted`）。声明必须属实，所以这种文件要从排好序的表写出：
  `t.sort("ts").write_parquet(...)`。在 issue #148 修复之前，像上面 `t` 这样
  没有声明顺序的表，写出的行序不是 CSV 里的顺序。

## 关键点解析

1. **Cursor 三种消费**：逐批迭代（自定义处理）、`count()`（纯计数）、
   `to_pandas()/to_arrow()`（物化剩余）——按下游需求选，内存占用天差地别。
2. **提前退出是特性**：`break` 之后引擎不再往下读，释放游标即取消执行、回收
   资源——对大文件来说，"只看前 N 批"只花几批的代价，不用读完整个文件。
3. **CSV 进、Parquet 存**：一次性转换，长期受益（体积、速度、类型）。
4. **LTSeq 也是 Arrow 生产者**：任何 `LTSeq` 都实现了 `__arrow_c_stream__`，
   `pa.table(t)`、`duckdb.sql("... FROM t")`、`pl.from_arrow(t)` 直接消费——
   流式管道的每一环都能拼 Arrow 生态的零件。

## 延伸练习

- 大文件会话化：把日志按 `user_id, ts` 排好序写成 Parquet，用
  `LTSeq.read_parquet` + `assume_sorted("user_id", "ts")` 读回，再套案例 10 的
  会话三件套，组合成完整的大文件管道（游标只产出原始批次，三件套要在表上跑）。
- 分批写库：迭代 cursor，每批 `pa.Table.from_batches([batch])` 后交给你的写入器。
- 打开仓库 `benchmarks/README.md` 与 ClickBench 对比脚本，看看 LTSeq 与
  DuckDB 在 ClickBench `hits` 数据集上的差距。

---
**上一例**：[11 · 错误风暴检测](11_error_burst.md) · **下一例**：[13 · 组内 TopN](13_topn_within_group.md)
