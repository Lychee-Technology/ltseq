<!-- Generated from ../../cn/27_resample_align.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 27 · 重采样与日历对齐：补齐缺失的时间点

> **难度** ★★★ · **领域** IoT · **前置** 案例 06、14
>
> **涉及 API**：`from_rows`（`datetime` 值） / `dt.diff(other, unit=)` / `//` 取整分桶 / `group_by().agg()` / `align(ref, key=)` / `join(how="left")` / `fold`（前值填充）

**学习目标**

- 把不规则时间戳**重采样**到固定粒度：`dt.diff(基准, unit)` + `//` 取整 =
  时间分桶
- 把聚合结果**对齐到完整日历**，缺失时间点以显式 NULL 行出现：`align()`
  能补出这些行，从轴表出发的左连接补出这些行并保留它们的键
- 用 `fold` 做**前值填充**（ffill），让下游计算拿到连续序列

## 问题描述

设备遥测 `tel(device, ts, temp)`：理想情况每分钟一条，实际 00:02 的读数
**丢了**。目标：

1. 把时间戳**分桶**到分钟粒度并聚合（重采样）；
2. 把结果**对齐**到 0-4 的完整分钟轴，缺失分钟显式出现；
3. 缺失处的温度用**前值填充**，得到可下游计算的连续序列。

这是 IoT/监控数据的日常：采集端永远不完美，分析端必须先"补轴"再计算。

```python
from datetime import datetime

from ltseq import LTSeq

tel = LTSeq.from_rows([
    {"device": "T1", "ts": datetime(2024, 1, 1, 0, 0), "temp": 20.5},
    {"device": "T1", "ts": datetime(2024, 1, 1, 0, 1), "temp": 21.0},
    {"device": "T1", "ts": datetime(2024, 1, 1, 0, 3), "temp": 19.5},
    {"device": "T1", "ts": datetime(2024, 1, 1, 0, 4), "temp": 20.0},
]).sort("device", "ts")

print(tel.schema)   # ts 是原生 timestamp
tel.show();
```

> **预期输出**
>
> ```text
> {'device': 'string', 'ts': 'timestamp', 'temp': 'float64'}
> +--------+---------------------+------+
> | device | ts                  | temp |
> +--------+---------------------+------+
> | T1     | 2024-01-01 00:00:00 | 20.5 |
> | T1     | 2024-01-01 00:01:00 | 21   |
> | T1     | 2024-01-01 00:03:00 | 19.5 |
> | T1     | 2024-01-01 00:04:00 | 20   |
> +--------+---------------------+------+
> ```

**解析**

- `ts` 存的是 Python `datetime` 值，`from_rows` 直接建出原生 `timestamp`
  列，后续全部 `dt.*` 运算无需再转。`read_csv` 读到 ISO 格式
  `YYYY-MM-DD HH:MM:SS` 文本时同样推断为 `timestamp`。
- 以**字符串**进来的时间戳目前要在源头转换：`main` 上 `cast("timestamp")`
  会抛 `ValueError`（案例 09 用的 `cast("date32")` 可用，但只保留日期）。
  ISO 字符串本身排序没有问题（字典序 = 时间序），但 `dt.diff` 这类时间
  **运算**必须真类型，对字符串列会直接报错。

## 第一步 · 分桶：dt.diff 基准 + floor 取整

以一个固定基准时刻（epoch）做差，单位取分钟，`// 1` 向下取整——得到的
整数就是**分钟桶号**。这比拆年月日时分再拼键要短得多。

```python
EPOCH = datetime(2024, 1, 1)

bucketed = tel.derive(
    minute=lambda r: (r.ts.dt.diff(EPOCH, unit="minute") // 1).cast("int64"),
)

per_min = (
    bucketed
    .group_by("minute")
    .agg(
        avg_temp=lambda g: g.temp.mean(),
        max_temp=lambda g: g.temp.max(),
        n=lambda g: g.temp.count(),
    )
    .sort("minute")
)

per_min.show();
```

> **预期输出**
>
> ```text
> +--------+----------+----------+---+
> | minute | avg_temp | max_temp | n |
> +--------+----------+----------+---+
> | 0      | 20.5     | 20.5     | 1 |
> | 1      | 21       | 21       | 1 |
> | 3      | 19.5     | 19.5     | 1 |
> | 4      | 20       | 20       | 1 |
> +--------+----------+----------+---+
> ```

**解析**

- `dt.diff(EPOCH, unit="minute")` 返回**浮点**分钟数（0.0、1.0、3.0、
  4.0）；`// 1` 是 floor 取整——秒级数据也能正确落桶（59.5 秒 → 0.99 →
  桶 0）。
- 桶粒度换挡：小时桶改 `unit="hour"`，5 分钟桶改 `// 5`——一行参数的事。
- `group_by` 聚合后**桶 2 不在结果里**：哈希分组只见数据有的键。缺失不是
  被丢弃，而是从未存在——补轴是下一步的专门动作。
- `n=lambda g: g.temp.count()` 借一列来数行；直接写 `g.count()` 会去找
  名为 `count` 的列。
- 真实管道里基准与桶范围应从数据算出（`to_dicts()` 取 min/max），本案例
  手造数据直接写常量，聚焦方法本身。

## 第二步 · 日历对齐：align() 补出缺失行

`align(ref_sequence, key=)` 把表按参考序列重排，参考里**多出来的键自动补
NULL 行**——0-4 的完整分钟轴上，缺失的那一分钟有了自己的一行。

```python
aligned = per_min.align([0, 1, 2, 3, 4], key=lambda r: r.minute)

aligned.show();
```

> **预期输出**
>
> ```text
> +--------+----------+----------+------+
> | minute | avg_temp | max_temp | n    |
> +--------+----------+----------+------+
> | 0      | 20.5     | 20.5     | 1    |
> | 1      | 21       | 21       | 1    |
> | None   | None     | None     | None |
> | 3      | 19.5     | 19.5     | 1    |
> | 4      | 20       | 20       | 1    |
> +--------+----------+----------+------+
> ```

**解析**

- 缺失的分钟现在是一行，按参考序列排在第三——但这一行**所有列**都是
  NULL，连键 `minute` 也不例外：`align` 用表里的数据填这一行，而表里没有
  分钟 2。只有行的位置说明它是哪一分钟。
- `align` 是 LTSeq 少有的"**以序列为纲**"的算子：表的行序服从参考序列，
  不在表中的参考键补 NULL 行。
- 没有键的行在下游碍事：`align` 会丢掉声明过的排序，而 `sort("minute")`
  又会把键为 NULL 的行排到最后。要在这条序列上继续计算，应把轴建成一张
  表，再把数据左连接上去。

```python
axis = LTSeq.from_rows([{"minute": m} for m in range(5)])

grid = axis.join(per_min, on="minute", how="left").sort("minute")

grid.show();
```

> **预期输出**
>
> ```text
> +--------+----------+----------+------+
> | minute | avg_temp | max_temp | n    |
> +--------+----------+----------+------+
> | 0      | 20.5     | 20.5     | 1    |
> | 1      | 21       | 21       | 1    |
> | 2      | None     | None     | None |
> | 3      | 19.5     | 19.5     | 1    |
> | 4      | 20       | 20       | 1    |
> +--------+----------+----------+------+
> ```

**解析**

- 同样五行，但分钟 2 保住了键：键由轴表提供，`how="left"` 保留轴表的每
  一行，不管 `per_min` 有没有匹配。左连接会去掉 `per_min` 那份重复的键列。
- join 的结果行序不确定，所以用 `sort("minute")` 恢复时间序并声明它——
  第三步的 `fold` 要求声明过的顺序。
- 补齐的网格可以直接画图（折线的空洞一眼可见）、可以计算差分（缺失处
  diff 为 NULL 不会伪造趋势）、可以按 NULL 统计丢包率——**补轴的价值在
  下游每一步**。

## 第三步 · 前值填充：fold 收尾

下游很多计算不接受空洞（比如累计能耗）。用 fold 把最后一个有效值向前
传递——案例 20 的 ffill 姿势原样复用。

```python
filled = grid.fold(
    lambda s, r: r["avg_temp"] if r["avg_temp"] is not None else s,
    init=None,
    into="avg_ffill",
)

filled.show();
```

> **预期输出**
>
> ```text
> +--------+----------+----------+------+-----------+
> | minute | avg_temp | max_temp | n    | avg_ffill |
> +--------+----------+----------+------+-----------+
> | 0      | 20.5     | 20.5     | 1    | 20.5      |
> | 1      | 21       | 21       | 1    | 21        |
> | 2      | None     | None     | None | 21        |
> | 3      | 19.5     | 19.5     | 1    | 19.5      |
> | 4      | 20       | 20       | 1    | 20        |
> +--------+----------+----------+------+-----------+
> ```

**解析**

- fold 状态 = 最近一次见到的非空 avg_temp；NULL 行保持状态并写入
  `avg_ffill`——一维前值填充的最小实现。
- **保留原始列**：`avg_temp`（空洞可见）与 `avg_ffill`（连续可算）并存，
  各服务各的下游——与案例 20 的"展示列/计算列分离"同一纪律。
- 若业务要**后值填充**或**插值**：后值填充就是倒序上的同一个 fold——
  `grid.sort("minute", desc=True).fold(...)`，再排回正序；线性插值建议
  `to_pandas()` 后交给专业库——LTSeq 不必包打一切。

## 关键点解析

- **重采样三板斧**：分桶（diff+取整）→ 聚合（group_by）→ 补轴（左连接到
  轴表）。三步各自独立可验证，比 pandas 的 `resample()` 魔法更透明。
- **轴是有序世界的"坐标系"**：声明"完整的轴长什么样"，缺失就从"不存在"
  变成"显式 NULL"——监控丢包、日历报表、实验对齐全靠它。`align()` 用一个
  Python list 声明轴，但补出的行丢了键；轴表能保住键。
- **ffill 属于 fold 的第二次证明**：状态式传递（"记住上一个有效值"）在
  窗口表达式之外，fold 一行拿下。

## 延伸练习

1. 多设备对齐：`group_by` 只接受一个键，所以先 `partition("device")` 得到
   每设备的子表，各自分桶后左连接到同一张轴表，用常量列（`LiteralExpr`，
   案例 09）把设备补回来，再用 `union` 拼接、`fold` 时加
   `partition_by="device"`——两台设备的轴一致，靠的是共用同一张轴表。
2. 丢包率报表：`grid.agg(missing=lambda g: count_if(g.avg_temp.is_null()))`
   ——补轴后再统计，缺失才数得出来。
3. 把分桶改成 5 分钟（`// 5`），用 30 分钟的手造数据重跑——验证桶号、
   轴表、ffill 三处的联动修改点。
4. 延伸到滑动聚合：分桶聚合是"不重叠窗口"，试试对 `grid` 做
   `rolling(3).mean()`，比较两种"平滑"的语义差异。

---
**上一例**：[26 · 波动带异常检测](26_sensor_anomaly.md) · **下一例**：[28 · 心跳离线段检测](28_offline_segments.md)
