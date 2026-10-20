<!-- Generated from ../../cn/03_golden_cross.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 03 · 金叉死叉：状态段检测与信号定位

> **难度** ★★★ · **领域** 金融时序 · **前置** 案例 02
>
> **涉及 API**：`group_ordered` / `NestedTable.agg` / `g.first()/g.last()/g.count()` / `cum_sum` / `search_first` / `shift` / `is_null` / `if_else`

**学习目标**

- 掌握 LTSeq 最具特色的三件套：**状态翻转标记 → cum_sum 编号 → group_ordered 连续分组**
- 理解"点式信号"与"区间视角"的差别，学会把任意点判断升维成段
- 用 `search_first` 做"找到第一条就停"的定位

## 问题描述

短期均线上穿长期均线称为**金叉**，下穿称为**死叉**。比起孤立的金叉时点，实战更
关心**多头区间**：`ma_5 > ma_20` 成立的完整时间段——起点、终点、持续天数、区间
涨幅。请：

1. 找出所有多头区间（状态段）；
2. 定位第一个"站上均线"的日期；
3. 顺带输出区间涨幅，挑出涨幅最大的段。

## 思路：把"点"升维成"段"

```
above:    F F T T T F F T T T T
changed:  1 0 1 0 0 1 0 1 0 0 0     ← 状态翻转处记 1
seg_id:   1 1 2 2 2 3 3 4 4 4 4     ← cum_sum 累计翻转号 = 段编号
```

- `group_ordered` 只把**连续相同**的键值分为一组——正好是"段"的数学定义；
- 每段用 `agg` 折叠成一行：`g.first()` 取段首、`g.last()` 取段尾、`g.count()` 数天数。

```python
import random

from ltseq import LTSeq, if_else

rng = random.Random(11)
price, closes = 50.0, []
for _ in range(40):
    price = max(5.0, price * (1 + rng.gauss(0.003, 0.025)))
    closes.append(round(price, 2))

t = (
    LTSeq.from_dict({"date": [f"D{i:02d}" for i in range(1, 41)], "close": closes})
    .sort("date")
    .derive(lambda r: {
        "ma_5":  r.close.rolling(5).mean().round(2),
        "ma_20": r.close.rolling(20).mean().round(2),
    })
    .derive(lambda r: {
        "above":   r.ma_5 > r.ma_20,
        "changed": if_else(
            r.ma_5.shift(1).is_null() | r.ma_20.shift(1).is_null(),
            1,
            if_else((r.ma_5 > r.ma_20) != (r.ma_5.shift(1) > r.ma_20.shift(1)), 1, 0),
        ),
    })
    .cum_sum("changed")                       # → changed_cumsum 段编号
)
t.select("date", "close", "ma_5", "ma_20", "above", "changed_cumsum").head(5).show();
```

> **预期输出**
>
> ```text
> +------+-------+-------+-------+-------+----------------+
> | date | close | ma_5  | ma_20 | above | changed_cumsum |
> +------+-------+-------+-------+-------+----------------+
> | D01  | 48.62 | 48.62 | 48.62 | false | 1              |
> | D02  | 49.22 | 48.92 | 48.92 | false | 1              |
> | D03  | 50.6  | 49.48 | 49.48 | false | 1              |
> | D04  | 50.1  | 49.64 | 49.64 | false | 1              |
> | D05  | 48.59 | 49.43 | 49.43 | false | 1              |
> +------+-------+-------+-------+-------+----------------+
> ```

**解析**

- `changed` 的首行特判用 `shift(1).is_null()`：首行没有上一行的均线，翻转判断会得到
  NULL，`if_else` 就会返回 0；有了这个特判，首行必然被判定为"新段开始"（案例 10 的
  会话切分也这样处理首行）。
- 前 5 行里 `ma_5` 与 `ma_20` 是同一批行的均值（部分帧），两者相等，所以 `above` 为 false。
- `cum_sum("changed")` 生成 `changed_cumsum`：每翻转一次 +1，于是**段编号单调
  递增且段内恒定**。

## 第一步 · 折叠成段：group_ordered + agg

```python
runs = (
    t.group_ordered(lambda r: r.changed_cumsum)
    .agg(lambda g: {
        "state":   g.first().above,
        "start":   g.first().date,
        "end":     g.last().date,
        "days":    g.count(),
        "close_0": g.first().close,
        "close_1": g.last().close,
    })
)

bull_runs = (
    runs
    .filter(lambda r: r.state == True)          # 只看多头段
    .derive(gain_pct=lambda r: ((r.close_1 / r.close_0 - 1) * 100).round(2))
    .select("start", "end", "days", "gain_pct")
)
bull_runs.show();
```

> **预期输出**
>
> ```text
> +-------+-----+------+----------+
> | start | end | days | gain_pct |
> +-------+-----+------+----------+
> | D06   | D08 | 3    | 4.58     |
> | D11   | D33 | 23   | 8.57     |
> | D39   | D40 | 2    | -3       |
> +-------+-----+------+----------+
> ```

**解析**

- `group_ordered` 返回 `NestedTable`；`.agg()` 把每段折叠成**一行**（SQL GROUP BY
  语义），组的输出顺序 = 段的出现顺序。若想把组级统计**广播回每一行**，用
  `.derive()`（SQL 窗口语义）——两个方法配套记忆。
- `g.first()` / `g.last()` 返回**行代理**，直接点列名；`g.count()` / `g.sum("col")`
  等聚合用**字符串**传列名。
- **当前限制**：`agg` 内不支持对聚合结果做算术组合（如 `g.last().close - g.first().close`），
  所以本例先取出 `close_0` / `close_1` 两列，再在表级 `derive` 里算涨幅。

## 第二步 · 定位第一个信号：search_first

找"第一条满足条件的行"不需要全表 filter——`search_first` 是带 `LIMIT 1` 早退出的
惰性扫描，找到即停。

```python
first_bull = t.search_first(lambda r: r.above == True)
first_bull.show()

best = (
    bull_runs
    .sort("gain_pct", desc=True)
    .head(1)
)
best.show();
```

> **预期输出**
>
> ```text
> +------+-------+-------+-------+-------+---------+----------------+
> | date | close | ma_5  | ma_20 | above | changed | changed_cumsum |
> +------+-------+-------+-------+-------+---------+----------------+
> | D06  | 48.65 | 49.43 | 49.3  | true  | 1       | 2              |
> +------+-------+-------+-------+-------+---------+----------------+
>
> +-------+-----+------+----------+
> | start | end | days | gain_pct |
> +-------+-----+------+----------+
> | D11   | D33 | 23   | 8.57     |
> +-------+-----+------+----------+
> ```

**解析**

- `search_first(谓词)` 返回单行 `LTSeq`（找不到则为 0 行），适合"第一个穿均线日 /
  第一笔大单 / 第一个报警"这类定位问题。
- 口径提醒：`ma_20` 是部分帧均值，前 19 天的"金叉"用的是不满窗的均线。要严格
  口径，先过滤掉 `row_number().over(order_by=r.date) > 19` 之前的行再分析。

## 关键点解析

1. **段三件套是 LTSeq 的灵魂**：`翻转标记 → cum_sum 编号 → group_ordered`。
   连续上涨、会话、报警事件段、刷卡配对……全是同一个骨架换一层皮。
2. **group_ordered ≠ group_by**：前者按**连续相同值**分组，组按序列顺序输出，
   同一个键隔几行再出现会另起一组；后者把同键的行不论位置都归为一组，不保序。
   要"全局分组但有序"，用 `group_sorted`（要求分组键恰好是声明排序的首键，
   案例 04 演示）。
3. **NestedTable 的两组方法**：`agg()` 折叠（每组一行）、`derive()` 广播（每行
   一份组统计）、`filter()` 用组级谓词筛段（还能用 `g.all()/any()/none()` 量词，
   案例 11 演示）、`flatten()` 展开并带 `__group_id__` 列（案例 10 演示）。
4. **`== True` 显式比较**布尔列是仓库官方示例的惯用写法，明确表达意图。

## 延伸练习

- 求最长**连续上涨**区间（`close > close.shift(1)` 作为状态）。
- 输出金叉/死叉**事件表**：段边界处 state 翻转的行，附上翻转方向。
- 把"段三件套"封装成 `find_runs(t, flag_col)` 函数，用 `pipe()` 挂进链式调用。

---
**上一例**：[02 · 股价技术指标](02_stock_indicators.md) · **下一例**：[04 · 从逐笔成交到 K 线](04_kline_ohlc.md)
