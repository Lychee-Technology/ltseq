<!-- Generated from ../../cn/05_asof_join_quotes.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 05 · 成交对齐行情：最近时刻连接 asof_join

> **难度** ★★☆ · **领域** 金融时序 · **前置** 案例 01
>
> **涉及 API**：`from_rows` / `sort` / `asof_join(on=, by=, strategy=)` / `derive` / `group_by().agg()`

**学习目标**

- 理解 as-of 连接的语义：**在另一条时间线上找"不晚于我的最近一条"**
- 掌握 `on`（时间列）与 `by`（分组键）的职责分离
- 知道 `strategy` 的三种方向，以及右表时间列为何会被保留

## 问题描述

成交表 `trades`（时刻、代码、成交价、数量）与行情表 `quotes`（时刻、代码、买一
价、卖一价）是两条**独立时间线**：行情每隔几秒一条，成交不定时。目标：

1. 为每笔成交找到**不晚于成交时刻的最近一条同代码行情**（backward 对齐）；
2. 计算成交时的买卖价差 `spread = ask - bid`；
3. 按代码汇总平均价差与成交笔数。

```python
from ltseq import LTSeq

trades = (
    LTSeq.from_rows([
        {"time": 10, "symbol": "A", "px": 101.5, "qty": 200},
        {"time": 15, "symbol": "B", "px": 200.0, "qty": 100},
        {"time": 22, "symbol": "A", "px": 101.8, "qty": 300},
        {"time": 31, "symbol": "B", "px": 199.5, "qty": 150},
    ])
    .sort("time")                     # 左表按时间列排序
)

quotes = (
    LTSeq.from_rows([
        {"time": 5,  "symbol": "A", "bid": 100.9, "ask": 101.2},
        {"time": 5,  "symbol": "B", "bid": 199.8, "ask": 200.3},
        {"time": 20, "symbol": "A", "bid": 101.4, "ask": 101.9},
        {"time": 20, "symbol": "B", "bid": 199.6, "ask": 200.1},
    ])
    .sort("time")
)
print("trades:", trades.count(), "rows / quotes:", quotes.count(), "rows")
```

> **预期输出**
>
> ```text
> trades: 4 rows / quotes: 4 rows
> ```

## 第一步 · backward 对齐

一个 `asof_join` 完成：对每笔成交，在同代码的行情里找 `quote.time <= trade.time`
中时间最大的一条。

```python
aligned = trades.asof_join(
    quotes,
    on="time",             # 双方同名的时间列
    by="symbol",           # 每只代码是独立时间线，绝不串线
    strategy="backward",   # 方向：找不晚于左行的最近右行
)
aligned.show();
```

> **预期输出**
>
> ```text
> +------+--------+-------+-----+------------+--------------+-------+-------+
> | time | symbol | px    | qty | time_right | symbol_right | bid   | ask   |
> +------+--------+-------+-----+------------+--------------+-------+-------+
> | 10   | A      | 101.5 | 200 | 5          | A            | 100.9 | 101.2 |
> | 15   | B      | 200   | 100 | 5          | B            | 199.8 | 200.3 |
> | 22   | A      | 101.8 | 300 | 20         | A            | 101.4 | 101.9 |
> | 31   | B      | 199.5 | 150 | 20         | B            | 199.6 | 200.1 |
> +------+--------+-------+-----+------------+--------------+-------+-------+
> ```

**解析**

- **`on` 是时间列，`by` 是分组键**，职责分离：`by="symbol"` 保证 A 的成交永远不会
  匹到 B 的行情。时间列两侧不同名时用 `left_on=` / `right_on=`。
- `strategy` 有三种：`backward`（往回找最近）、`forward`（往前找最近）、
  `nearest`（找最近，两侧都可能）。
- **右表时间列被保留**（冲突时加 `_right` 后缀，如 `time_right`；右表的 `by` 列
  也会保留，即 `symbol_right`）：与等值连接不同，as-of 是近似匹配，"实际对上的
  行情时刻"本身就是重要信息（可算行情延迟）。
- 时间列两侧同名时直接写 `on="time"`；也可以用 lambda 形式
  `on=lambda t, q: t.time >= q.time`——**比较运算符即方向**（`>=` 向后，`<=` 向前）。

## 第二步 · 价差与汇总

连接结果是一张普通 `LTSeq`，后面的处理与案例 01 完全同构：derive → group_by → agg。

```python
enriched = aligned.derive(
    spread=lambda r: (r.ask - r.bid).round(3),
    slippage=lambda r: (r.px - r.bid).round(3),
)

summary = enriched.group_by("symbol").agg(
    avg_spread=lambda g: g.spread.mean(),
    avg_slippage=lambda g: g.slippage.mean(),
    trades=lambda g: g.px.count(),
).sort("symbol")
summary.show();
```

> **预期输出**
>
> ```text
> +--------+------------+--------------+--------+
> | symbol | avg_spread | avg_slippage | trades |
> +--------+------------+--------------+--------+
> | A      | 0.4        | 0.5          | 2      |
> | B      | 0.5        | 0.05         | 2      |
> +--------+------------+--------------+--------+
> ```

**解析**

- `group_by().agg()` 的结果组序不固定（这里每次运行都可能不同），展示前先排序。
- `group_by("col").agg(...)` 里聚合 lambda 的参数是**列代理**：
  `g.spread.mean()` / `g.px.count()`——列名点出来，方法链上去。
- 全部聚合方法：`sum / avg(=mean) / count / min / max / median / var / std /
  percentile(p) / top_k(k)`；条件聚合 `count_if / sum_if / ...` 与统计聚合
  `corr / covar / skew / concat_agg` 也在同一个上下文可用（`from ltseq import ...`）。

## 关键点解析

1. **as-of ≠ 等值连接**：等值连接要求时刻完全相等（大量 miss），as-of 按"最近"
   语义对齐，是时序数据工程的基础算子，对应 SQL 里要写一大坨 `LATERAL JOIN`。
2. **按时间列有序**：两侧都要按各自的时间列有序；声明的排序不以时间列开头时，
   `asof_join` 会自动补一次排序。可以自己用 `sort("time")` 声明，预排序的 Parquet
   用 `assume_sorted("time")` 跳过物理排序。
3. **`by` 防串线**：多标的场景一定要给 `by`，否则不同标的的时间线会被混着对齐。
4. **右表时间戳是信息不是冗余**：`time_right` 与 `time` 的差 = 行情时效，直接
   可用于监控数据质量。

## 延伸练习

- 用 `strategy="forward"` 对齐"下一条行情"，比较成交价与下一报价的关系。
- 行情列名不同（`quote_ts`）时改用 `left_on="time", right_on="quote_ts"`。
- 把本案例与案例 02 结合：对齐后再算滚动均价，构造完整滑点监控管道。

---
**上一例**：[04 · 从逐笔成交到 K 线](04_kline_ohlc.md) · **下一例**：[06 · 最大回撤与复利](06_max_drawdown.md)
