<!-- Generated from ../../cn/02_stock_indicators.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 02 · 股价技术指标：移动平均与日收益率

> **难度** ★★☆ · **领域** 金融时序 · **前置** 案例 01
>
> **涉及 API**：`rolling().mean()/std()` / `shift` / `pct_change` / `when` / `fill_null` / `round` / `write_csv` / `read_csv`

**学习目标**

- 用 `rolling(n)` 计算移动平均与滚动波动率，理解**部分帧语义**
- 用 `pct_change()` 算日收益率，用 `when().otherwise()` 做状态标注
- 体验 CSV 读写闭环，并知道如何接入真实行情数据

## 问题描述

给定 60 个交易日的收盘价，计算：

- `ma_5` / `ma_20`：5 日、20 日移动平均线；
- `ret`：日收益率（相对前一日收盘）；
- `vol_20`：20 日滚动波动率（收益率的标准差）；
- `trend`：收盘价相对 5 日线的位置标注（`above_ma5` / `below_ma5` / `at_ma5`）。

最后筛选出波动率异常高的交易日。

## 生成示例数据

用固定种子的随机游走模拟收盘价，先落盘成 CSV，再读回来——顺便演示 I/O 闭环。

```python
import random

from ltseq import LTSeq

rng = random.Random(42)
price, closes = 100.0, []
for _ in range(60):
    price = max(5.0, price * (1 + rng.gauss(0.0005, 0.02)))
    closes.append(round(price, 2))

dates = [f"2026-{1 + i // 28:02d}-{1 + i % 28:02d}" for i in range(60)]

LTSeq.from_dict({"date": dates, "close": closes}).write_csv("stock.csv")

stock = LTSeq.read_csv("stock.csv")
stock.head(3).show();
```

> **预期输出**
>
> ```text
> +------------+-------+
> | date       | close |
> +------------+-------+
> | 2026-01-01 | 99.76 |
> | 2026-01-02 | 99.47 |
> | 2026-01-03 | 99.3  |
> +------------+-------+
> ```

**解析**

- `write_csv()` / `read_csv()` 开箱即用；`read_csv()` 会把 ISO 格式 `YYYY-MM-DD`
  的列推断为原生 `date32`，所以 `sort("date")` 就是按时间排序，`r.date.dt.*` 也能直接用。
- Parquet 直接存列类型，读取时不必重新推断：`write_parquet("s.parquet", compression="zstd")`。

## 第一步 · 两次 derive 算全指标

`derive` 的字典风格允许一次添加多列；列间若有依赖（要用到本次新增的列），拆成
两次 `derive`（原因见关键点 2）。本例的 `vol_20` 是新列 `ret` 的滚动标准差，所以放在
第二次 `derive` 里。

```python
indicators = (
    stock
    .sort("date")
    .derive(lambda r: {
        "prev_close": r.close.shift(1),
        "ma_5":   r.close.rolling(5).mean().round(2),
        "ma_20":  r.close.rolling(20).mean().round(2),
        "ret":    r.close.pct_change(),
    })
    .derive(vol_20=lambda r: r.ret.rolling(20).std().round(4))   # 用到上一步新增的 `ret`
)
indicators.select("date", "close", "ma_5", "ma_20", "ret", "vol_20").head(4).show();
```

> **预期输出**
>
> ```text
> +------------+--------+-------+-------+-----------------------+--------+
> | date       | close  | ma_5  | ma_20 | ret                   | vol_20 |
> +------------+--------+-------+-------+-----------------------+--------+
> | 2026-01-01 | 99.76  | 99.76 | 99.76 | None                  | None   |
> | 2026-01-02 | 99.47  | 99.62 | 99.62 | -0.002906976744186109 | None   |
> | 2026-01-03 | 99.3   | 99.51 | 99.51 | -0.001709058007439446 | 0.0008 |
> | 2026-01-04 | 100.74 | 99.82 | 99.82 | 0.014501510574018104  | 0.0097 |
> +------------+--------+-------+-------+-----------------------+--------+
> ```

**解析**

- `rolling(5).mean()` 的语义是 SQL 的 `ROWS BETWEEN 4 PRECEDING AND CURRENT ROW`：
  **部分帧**——序列头部不满 5 行时，按已有行数计算，而不是像 pandas
  `min_periods=5` 那样给 NULL。这是 LTSeq 与 pandas 的行为差异点，做指标对齐时
  要留意。
- `rolling` 支持的聚合：`mean / sum / min / max / count / std`。
- `pct_change()` = `(x - x.shift(1)) / x.shift(1)`，首行是 NULL。所以 `vol_20` 从
  第 3 行才有值：样本标准差至少需要两个非 NULL 的收益率。
- `.round(2)` 这类数值修饰符可以直接挂在窗口表达式上。

## 第二步 · 状态标注与筛选

```python
from ltseq import when

labeled = indicators.derive(
    ret_pct=lambda r: (r.ret * 100).round(2).fill_null(0.0),
    trend=lambda r: when(r.close > r.ma_5, "above_ma5")
                    .when(r.close < r.ma_5, "below_ma5")
                    .otherwise("at_ma5"),
)

labeled.filter(lambda r: r.vol_20 > 0.018).select(
    "date", "close", "ma_5", "vol_20", "trend"
).head(5).show();
```

> **预期输出**
>
> ```text
> +------------+-------+--------+--------+-----------+
> | date       | close | ma_5   | vol_20 | trend     |
> +------------+-------+--------+--------+-----------+
> | 2026-02-05 | 95.55 | 101.56 | 0.0194 | below_ma5 |
> | 2026-02-06 | 94.43 | 99.46  | 0.0194 | below_ma5 |
> | 2026-02-07 | 92.75 | 97.48  | 0.0196 | below_ma5 |
> | 2026-02-08 | 94.42 | 95.59  | 0.0198 | below_ma5 |
> | 2026-02-09 | 95.72 | 94.57  | 0.02   | above_ma5 |
> +------------+-------+--------+--------+-----------+
> ```

**解析**

- `when(cond, val).when(cond2, val2).otherwise(default)` 是 SQL `CASE WHEN` 的链式
  写法，比嵌套 `if_else` 可读得多（`if_else` 也存在，两分支时更紧凑）。
- `fill_null(0.0)` 把首日收益率的 NULL 填成 0；反向操作是 `is_null()` /
  `is_not_null()` 过滤。
- `vol_20` 是日收益率的标准差，示例随机游走按 2% 的日波动生成，所以 `vol_20 > 0.018`
  筛出的是实际波动逼近这一水平的区间：从 2026-02-05 那天 −5.2% 的下跌开始。
- `select` 会剪枝整个计划：`ma_20`、`ret_pct` 这类没被选中的列根本不会计算。
  `vol_20` 上的 `filter` 无法下推到产生它的窗口之下，只能在窗口之后执行。

## 接入真实行情数据

把数据源换成真实数据，只要列名对齐 `date` / `close`。其余代码不用改：

```python
# 三种常见接法（任选其一）：
#
# 1) 本地 CSV：行情软件 / akshare / yfinance 导出后重命名列
#    stock = LTSeq.read_csv("my_stock.csv")           # 列: date, close
#
# 2) Parquet（大体量 + 保留日期类型）：
#    stock = LTSeq.read_parquet("my_stock.parquet")
#
# 3) pandas / polars / duckdb 现有管道直接桥接（Arrow 来源零拷贝；pandas 先转成 Arrow）：
#    stock = LTSeq.from_pandas(df)
#    stock = LTSeq.from_arrow(pa_table)
#    stock = LTSeq.from_arrow(duckdb.sql("SELECT date, close FROM quotes"))
#
# 用其中一种代替生成示例数据的单元格，再重新运行第一步和第二步。
```

## 关键点解析

1. **rolling 的部分帧语义**：头部样本不足时按已有行计算（SQL 语义），不是 pandas
   的 `min_periods` NULL 填充。需要"满窗才出数"时，可以配合
   `row_number().over(order_by=r.date)` 过滤掉前 n-1 行。
2. **同一个 `derive` 内新列不能互相引用**：表达式读取的是**原始列**。要引用本次
   新增列，拆成两次 `derive`（本案例的 `vol_20`、`ret_pct` 都引用 `ret`，所以都放在
   后面的 `derive` 里）。
3. **先 `sort` 再窗口**：`shift` / `rolling` / `pct_change` 都依赖声明顺序，
   去掉 `.sort("date")` 会直接抛 `SortRequiredError`。
4. **`read_csv` 把 ISO 日期读成原生 `date32`**：直接按时间排序；需要日期运算
   （加减、取整月）时直接用 `r.date.dt.*`（案例 04 演示）。

## 延伸练习

- 计算 5 日均线斜率：`r.ma_5 - r.ma_5.shift(3)`。
- 用 `ntile(4).over(order_by=r.vol_20)` 给波动率分四档。
- 把 `ma_5` 与 `ma_20` 的上下关系做成信号列，为案例 03 的金叉检测做铺垫。

---
**上一例**：[01 · 序列思维入门](01_hello_sequence.md) · **下一例**：[03 · 金叉死叉](03_golden_cross.md)
