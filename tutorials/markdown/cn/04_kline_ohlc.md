<!-- Generated from ../../cn/04_kline_ohlc.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 04 · 从逐笔成交到 K 线：有序分组聚合

> **难度** ★★★ · **领域** 金融时序 · **前置** 案例 01、02
>
> **涉及 API**：`dt.hour()/dt.minute()` / `//` 整除 / `partition` / `group_sorted` / `g.first()/g.max()/g.sum()` / `concat` / `pivot`

**学习目标**

- 用时间成分提取 + 整除做**时间分桶**（分钟线聚合的基础功）
- 分清 `group_by`（哈希分组）与 `group_sorted`（有序分组）的适用场景
- 用 `partition` 拆分数据、逐组构建 K 线再 `concat` 合并，最后 `pivot` 转宽表

把任意离散时刻对齐到固定桶，是聚合类计算的入口题。

## 问题描述

逐笔成交表（时间戳、代码、价格、数量）太细，看不清趋势。目标：

1. 按 5 分钟桶聚合出 K 线：`open / high / low / close / vol / ticks`；
2. 两只代码分别出 K 线，纵向合并成一张长表；
3. 用 `pivot` 把成交量转成"时刻 × 代码"的宽表，直观看两股活跃度对比。

**open/close 为什么重要？** 桶内必须保持时间顺序，第一笔是 open、最后一笔是
close——这正是**有序分组**的用武之地。

```python
import random
from datetime import datetime, timedelta

from ltseq import LTSeq

rng = random.Random(11)
start = datetime(2026, 9, 1, 9, 30)
rows, px = [], {"600519": 1800.0, "000858": 160.0}
sec = 0
while sec < 3600:                                   # 09:30:00 起 1 小时
    for sym in ("600519", "000858"):
        for k in range(rng.randint(1, 3)):          # 1-3 笔，间隔 1 秒
            px[sym] = max(1.0, px[sym] * (1 + rng.gauss(0, 0.0015)))
            rows.append({
                "ts":     start + timedelta(seconds=sec + k),
                "symbol": sym,
                "price":  round(px[sym], 2),
                "volume": rng.randint(100, 900),
            })
    sec += rng.randint(8, 25)

ticks = LTSeq.from_rows(rows)
print(len(ticks), "笔成交")
```

> **预期输出**
>
> ```text
> 863 笔成交
> ```

**解析**

- `ts` 存的是 Python `datetime`，`from_rows` 会把它建成 timestamp 列；第一步的
  `dt` 访问器只接受时间类型的列，字符串列会报错。
- 每轮 1-3 笔成交间隔 1 秒，同一代码的每笔成交都有自己的时间戳；否则桶内第一笔
  （open）和最后一笔（close）会取决于相同 `ts` 之间碰巧的排列顺序。
- `from_rows` 按行字典建表：列名取自首行，各列类型由 Arrow 根据值推断。

## 第一步 · 时间分桶

把 `HH:MM` 换算成"当日第几分钟"再整除 5，得到 5 分钟桶编号；再由桶编号反推
`HH:MM` 标签（供宽表列名/行名使用）。

```python
from ltseq.expr import concat_ws

staged = (
    ticks
    .derive(lambda r: {
        "bucket": (r.ts.dt.hour() * 60 + r.ts.dt.minute()) // 5,
    })
    .derive(lambda r: {
        "hh": r.bucket * 5 // 60,
        "mm": (r.bucket * 5) % 60,
    })
    .derive(lambda r: {
        "kline": concat_ws(":", r.hh.cast("string").s.pad_left(2, "0"),
                                r.mm.cast("string").s.pad_left(2, "0")),
    })
)
staged.select("ts", "symbol", "bucket", "kline").head(3).show();
```

> **预期输出**
>
> ```text
> +---------------------+--------+--------+-------+
> | ts                  | symbol | bucket | kline |
> +---------------------+--------+--------+-------+
> | 2026-09-01 09:30:00 | 600519 | 114    | 09:30 |
> | 2026-09-01 09:30:01 | 600519 | 114    | 09:30 |
> | 2026-09-01 09:30:00 | 000858 | 114    | 09:30 |
> +---------------------+--------+--------+-------+
> ```

**解析**

- `r.ts.dt.hour()` / `dt.minute()` 提取时间成分；`//` 是整除（Python 语义，整数
  运算精确 floor），`%` 取余——分桶就是一行表达式。
- `.s.pad_left(2, "0")` 左补零成定宽字符串；它只接受字符串列，所以整数先
  `cast("string")`。`concat_ws(":", a, b)` 拼接（来自 `ltseq.expr`，等价 SQL 的
  `CONCAT_WS`）。

## 第二步 · 分代码构建 K 线：partition + group_sorted

`partition("symbol")` 把长表拆成字典式的子表容器；每个子表内部**先按
`("bucket", "ts")` 排序**，再用 `group_sorted` 做有序分组——分组键恰好是声明排序
的首键，每个桶恰好成为一组，且桶内保持时间序（open = 第一笔，close = 最后一笔）。

```python
def build_kline(sub):
    return (
        sub.sort("bucket", "ts")
        .group_sorted(lambda r: r.bucket)
        .agg(lambda g: {
            "kline":  g.first().kline,
            "symbol": g.first().symbol,
            "open":   g.first().price,
            "high":   g.max("price"),
            "low":    g.min("price"),
            "close":  g.last().price,
            "vol":    g.sum("volume"),
            "ticks":  g.count(),
        })
    )

kline_long = None
for sym, sub in staged.partition("symbol").items():
    part = build_kline(sub)
    kline_long = part if kline_long is None else kline_long.concat(part)

kline_long = kline_long.sort("kline", "symbol")
kline_long.head(6).show();
```

> **预期输出**
>
> ```text
> +-------+--------+---------+---------+---------+---------+-------+-------+
> | kline | symbol | open    | high    | low     | close   | vol   | ticks |
> +-------+--------+---------+---------+---------+---------+-------+-------+
> | 09:30 | 000858 | 159.68  | 162.3   | 158.38  | 162.08  | 22194 | 43    |
> | 09:30 | 600519 | 1803.54 | 1822.55 | 1797.11 | 1800.71 | 16135 | 36    |
> | 09:35 | 000858 | 162.22  | 163.26  | 161.14  | 161.75  | 17944 | 36    |
> | 09:35 | 600519 | 1801.46 | 1804.1  | 1781.34 | 1784.69 | 14294 | 30    |
> | 09:40 | 000858 | 161.39  | 161.39  | 158.71  | 160.16  | 15739 | 32    |
> | 09:40 | 600519 | 1786.83 | 1805.01 | 1776.38 | 1801.66 | 18078 | 40    |
> +-------+--------+---------+---------+---------+---------+-------+-------+
> ```

**解析**

- **`group_by` vs `group_sorted`**：`group_by("col").agg(...)` 是哈希分组，组序
  无保证；`group_sorted` 要求分组键是声明排序的**首键**（否则抛
  `SortRequiredError`），换来组按序列顺序输出，以及 `g.first()` / `g.last()` 依赖
  的组内顺序。
- `agg` 只返回 lambda 里映射的列，所以要用 `g.first().symbol` 把 `symbol` 带出来，
  否则合并后的长表既分不清代码，也没法按 `symbol` 排序。
- 桶内 open/close 的正确性完全依赖 `sort("bucket", "ts")` 中 `ts` 的次序——又一次
  "顺序决定语义"。
- `partition` 返回 `PartitionedTable`：支持 `parts[key]`、`items()`、`map(fn)`、
  `to_list()`，是"逐组建模"的通用容器。`items()` 给出的分区顺序不固定，所以合并
  后要再排序。
- `concat` 是 UNION ALL 语义（保留重复行），两张同构 K 线表纵向拼接。

## 第三步 · 转宽表：pivot

两股活跃度对比，最自然的形态是"时刻 × 代码"矩阵：

```python
wide = kline_long.pivot(
    index="kline",
    columns="symbol",
    values="vol",
    agg_fn="sum",
).sort("kline")
wide.show();
```

> **预期输出**
>
> ```text
> +-------+--------+--------+
> | kline | 000858 | 600519 |
> +-------+--------+--------+
> | 09:30 | 22194  | 16135  |
> | 09:35 | 17944  | 14294  |
> | 09:40 | 15739  | 18078  |
> | 09:45 | 18476  | 17965  |
> | 09:50 | 19011  | 15301  |
> | 09:55 | 16980  | 18077  |
> | 10:00 | 17582  | 15761  |
> | 10:05 | 19874  | 15870  |
> | 10:10 | 18225  | 20471  |
> | 10:15 | 15149  | 16019  |
> +-------+--------+--------+
> ```

**解析**

- `pivot(index=, columns=, values=, agg_fn=)` 从长转宽；`agg_fn` 支持
  `sum`（默认）`/ mean / min / max / count`。
- pivot 结果的行序不固定，所以接 `.sort("kline")`；`show()` 只打印 12 个桶中的
  前 10 个。
- 宽表的空缺组合会以 NULL 呈现（`count` 为 0）。想补零就在 `derive` 里用
  `.fill_null(0)` 覆盖该列；这里的列名以数字开头，要写成 `getattr(r, "600519")`。

## 关键点解析

1. **时间分桶三板斧**：`dt.hour()/dt.minute()` 提成分 → 算术对齐（`// 5 * 5`）→
   （可选）格式化标签。不依赖任何重采样函数，透明可组合。
2. **有序分组的两问**：分组键是不是声明排序的首键？组内顺序要不要保留？
   两者都"要"就用 `group_sorted`；只关心组统计、不关心组序，用 `group_by` 即可。
3. **先拆再合**：`partition → 逐组计算 → concat` 是处理"分类别建模"的通用套路，
   比一条巨型 SQL 更可读，也更贴合 LTSeq 的序列语义。
4. **K 线 = 有序聚合**：`g.first()/g.last()` 是行代理（点列名），`g.max()/g.min()/
   g.sum()/g.count()` 是聚合代理（字符串传列名）——两种风格别混。

## 延伸练习

- 加入 `vwap`：先逐笔 derive `pv = price * volume`，聚合时分别取 `g.sum("pv")`
  与 `g.sum("volume")` 两列，agg 之后再用 `derive` 相除——注意 agg 内暂不支持
  聚合算术。
- 找出每桶最大三笔成交：`sub.group_by("bucket").agg(top3=lambda g: g.volume.top_k(3))`。
  `top_k` 是 `group_by` 的聚合（`group_sorted` 的组代理没有 `top_k`），结果是用
  `;` 连接的一个字符串。
- 把桶宽参数化（1m/5m/15m），封装 `to_kline(ticks, minutes)` 复用。

---
**上一例**：[03 · 金叉死叉](03_golden_cross.md) · **下一例**：[05 · 成交对齐行情](05_asof_join_quotes.md)
