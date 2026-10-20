<!-- Generated from ../../cn/06_max_drawdown.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 06 · 最大回撤与复利收益：cum_max 与 fold

> **难度** ★★★ · **领域** 金融时序 · **前置** 案例 02
>
> **涉及 API**：`cum_max()` / 全表 `agg` / `pct_change` / `fold(fn, init=, into=)` / `tail`

**学习目标**

- 用 `cum_max()` 求"历史最高水位线"，一行算出回撤序列
- 掌握 LTSeq 的**状态机算子** `fold`：把"带状态循环"写成一行声明
- 学会用全表 `agg`（不传 `by`）提取标量结论

## 问题描述

给定 90 天基金净值序列（NAV），计算：

1. **回撤序列**：每天相对"历史最高净值"的跌幅 `dd = nav / cummax(nav) - 1`；
2. **最大回撤**：`dd` 的最小值（回撤越深越负）；
3. **复利累计收益**：`(1+r1)(1+r2)...` 逐日复合——本质是一个带状态的循环。

```python
import random

from ltseq import LTSeq

rng = random.Random(5)
nav, navs = 1.0, []
for _ in range(90):
    nav = max(0.2, nav * (1 + rng.gauss(0.001, 0.02)))
    navs.append(round(nav, 4))

t = LTSeq.from_dict({"day": list(range(1, 91)), "nav": navs}).sort("day")
t.head(3).show();
```

> **预期输出**
>
> ```text
> +-----+--------+
> | day | nav    |
> +-----+--------+
> | 1   | 0.9774 |
> | 2   | 0.956  |
> | 3   | 0.9697 |
> +-----+--------+
> ```

## 第一步 · 回撤序列：cum_max 一行搞定

`cum_max()` 是"运行最大值"：到当前行为止见过的最大 `nav`，即历史水位线。

```python
nav_t = (
    t
    .derive(lambda r: {
        "peak": r.nav.cum_max(),
        "dd":   (r.nav / r.nav.cum_max() - 1).round(4),
    })
)

nav_mdd = nav_t.agg(mdd=lambda g: g.dd.min()).to_dicts()[0]["mdd"]
print("最大回撤 =", round(nav_mdd * 100, 2), "%")

nav_t.select("day", "nav", "peak", "dd").tail(3).show();
```

> **预期输出**
>
> ```text
> 最大回撤 = -9.59 %
> +-----+--------+--------+---------+
> | day | nav    | peak   | dd      |
> +-----+--------+--------+---------+
> | 88  | 1.2898 | 1.3723 | -0.0601 |
> | 89  | 1.2597 | 1.3723 | -0.0821 |
> | 90  | 1.3093 | 1.3723 | -0.0459 |
> +-----+--------+--------+---------+
> ```

**解析**

- `r.nav.cum_max()` 表达式形式可以放在 `derive` 里自定义列名；表级快捷方式
  `t.cum_sum("nav")` 家族只有 `cum_sum`，`cum_max` / `cum_min` 走表达式形式。
- `agg(mdd=...)` **不传 `by`** 就是全表聚合，直接得到标量结论（包在单行表里）。
- 注意同一个 `derive` 里 `peak` 与 `dd` 各自独立计算（都读原始 `nav` 列）——
  想引用本次新增列必须拆两次 `derive`（案例 02 关键点 2）。

## 第二步 · 复利收益：fold 状态机

累计收益没法用"行与行"的窗口表达——它要**逐行传递状态**：`s ← s × (1 + ret)`。
这正是 `fold` 的领地：三要素 `fn(state, row)`、`init`、`into`（结果列名）。

```python
with_ret = nav_t.derive(ret=lambda r: r.nav.pct_change())

compounded = with_ret.fold(
    lambda s, r: s * (1 + (r["ret"] or 0.0)),   # 首行 ret 为 NULL → 当作 0
    init=1.0,
    into="cum_return",
)

compounded.select("day", "nav", "ret", "cum_return").tail(3).show();
```

> **预期输出**
>
> ```text
> +-----+--------+-----------------------+--------------------+
> | day | nav    | ret                   | cum_return         |
> +-----+--------+-----------------------+--------------------+
> | 88  | 1.2898 | -0.027740087441579936 | 1.3196234908942095 |
> | 89  | 1.2597 | -0.023336951465343474 | 1.2888275015346842 |
> | 90  | 1.3093 | 0.03937445423513524   | 1.3395743810108454 |
> +-----+--------+-----------------------+--------------------+
> ```

**解析**

- `fold` 的行参数 `r` 是**只读的行视图**（`r["ret"]` 或 `r.ret` 取值，NULL →
  `None`），回调就是纯 Python：可以写 `or`、`if`、调函数——窗口表达式做不到的事
  它都能做。
- 代价是**逐行 Python 回调、非惰性、整表物化**，大表上明显慢于窗口表达式。
  官方建议：**能用窗口表达就不要用 fold**；只有真正需要"任意状态"时才上。
- `fold` 按行返回的顺序传递状态。输入较大、分布在多个分区上时，`nav_t` 和
  `with_ret` 里的窗口可能让行不按 `day` 顺序返回，而 `sort_keys` 仍然报告这个顺序
  （issue #311），`cum_return` 就会按错误的顺序复利。修复之前，用真实数据时请在
  `fold` 之前再调用一次 `.sort("day")`。
- `partition_by="fund"` 可以让状态在每组开头重置为 `init`——多基金并行复利一行
  搞定（见延伸）。

## 关键点解析

1. **窗口 vs 状态机**：`cum_max` / `cum_sum` / `shift` 覆盖"看邻居"类需求；
   `fold` 覆盖"传状态"类需求。先想窗口，再想 fold。
2. **全表 `agg`**：不传 `by` 即全表聚合，配 `to_dicts()[0]` 直接取标量，常用于
   把结论注入标题/告警。
3. **fold 三要素**：`fn(s, r) -> 新状态`、`init` 初值、`into` 落地列名；结果列
   同时作为下一行输入。
4. **诚实提示**：fold 会破坏惰性并整表物化，10 万行以上的表先考虑窗口表达。

## 延伸练习

- 多基金并行：数据加一列 `fund`，`fold(..., partition_by="fund")` 一次算完。
- 定位最大回撤**区间**：结合案例 03 的段三件套，找 `dd` 谷底所在的时间段。
- 用 `fold` 实现"连续 ERROR 计数器"，对比案例 11 的窗口实现。

---
**上一例**：[05 · 成交对齐行情](05_asof_join_quotes.md) · **下一例**：[07 · 转化漏斗](07_funnel_analysis.md)
