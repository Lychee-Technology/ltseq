<!-- Generated from ../../cn/26_sensor_anomaly.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 26 · 波动带异常检测：滚动均值 ± 3σ

> **难度** ★★☆ · **领域** IoT · **前置** 案例 02
>
> **涉及 API**：`sort` / `rolling(n, partition_by=).mean()/.std()` / `round` / `derive` / `shift(1, partition_by=)` / `|` 组合 / `group_by().agg()` + `count_if`

**学习目标**

- 用 `rolling` 的均值与标准差构造**动态波动带**（3σ 原则），代替拍脑袋的
  固定阈值
- 理解并修复一个真实陷阱：**异常值会污染自己的基准窗口**——把带整体
  `shift(1)`，让比较只看历史
- 直面 rolling 的**部分帧语义**：前几行窗口不满，σ 为 NULL，异常判定为
  NULL——预热期不是漏报而是"无判断"

## 问题描述

温感器读数 `temps(device, ts, temp)`，两台设备各 8 个时刻，D1 在 ts=5 有
一个 41 度的尖峰（正常温度带 19-22）。目标：

1. 为每台设备构造滚动 4 期波动带：`mean ± 3σ`；
2. 判定每行读数是否**落在上一期波动带之外**（异常）；
3. 汇总每设备的异常计数。

为什么不设固定阈值 30？正常波动可能随季节漂移——**带应该跟着数据走**。

```python
from ltseq import LTSeq

temps = LTSeq.from_rows([
    {"device": "D1", "ts": 0, "temp": 20.0},
    {"device": "D1", "ts": 1, "temp": 21.0},
    {"device": "D1", "ts": 2, "temp": 19.0},
    {"device": "D1", "ts": 3, "temp": 22.0},
    {"device": "D1", "ts": 4, "temp": 20.0},
    {"device": "D1", "ts": 5, "temp": 41.0},
    {"device": "D1", "ts": 6, "temp": 21.0},
    {"device": "D1", "ts": 7, "temp": 20.0},
    {"device": "D2", "ts": 0, "temp": 21.0},
    {"device": "D2", "ts": 1, "temp": 20.0},
    {"device": "D2", "ts": 2, "temp": 22.0},
    {"device": "D2", "ts": 3, "temp": 21.0},
    {"device": "D2", "ts": 4, "temp": 20.0},
    {"device": "D2", "ts": 5, "temp": 22.0},
    {"device": "D2", "ts": 6, "temp": 21.0},
    {"device": "D2", "ts": 7, "temp": 20.0},
]).sort("device", "ts")
```

## 第一步 · 滚动带：mean ± 3σ

`rolling(4, partition_by="device")` 在每台设备内部滑动 4 期窗口；注意
LTSeq rolling 的**部分帧语义**——窗口不满时按实际行数算，第 0 行的 std
为 NULL（单值样本无标准差）。

```python
banded = temps.derive(
    ma=lambda r: r.temp.rolling(4, partition_by="device").mean().round(2),
    sd=lambda r: r.temp.rolling(4, partition_by="device").std().round(3),
).derive(
    upper=lambda r: (r.ma + 3 * r.sd).round(2),
    lower=lambda r: (r.ma - 3 * r.sd).round(2),
)

banded.filter(lambda r: r.device == "D1").show();
```

> **预期输出**
>
> ```text
> +--------+----+------+------+--------+-------+-------+
> | device | ts | temp | ma   | sd     | upper | lower |
> +--------+----+------+------+--------+-------+-------+
> | D1     | 0  | 20   | 20   | None   | None  | None  |
> | D1     | 1  | 21   | 20.5 | 0.707  | 22.62 | 18.38 |
> | D1     | 2  | 19   | 20   | 1      | 23    | 17    |
> | D1     | 3  | 22   | 20.5 | 1.291  | 24.37 | 16.63 |
> | D1     | 4  | 20   | 20.5 | 1.291  | 24.37 | 16.63 |
> | D1     | 5  | 41   | 25.5 | 10.408 | 56.72 | -5.72 |
> | D1     | 6  | 21   | 26   | 10.033 | 56.1  | -4.1  |
> | D1     | 7  | 20   | 25.5 | 10.344 | 56.53 | -5.53 |
> +--------+----+------+------+--------+-------+-------+
> ```

**解析**

- ts5 行自己泄露了陷阱：41 进入窗口后 ma 被拉到 25.5、σ 涨到 10.4，
  **upper 变成 56.72**——用这一行的带判这一行，41 反而是"正常"的！
  异常值污染了自己的基准。
- lower 出现负数不奇怪：σ 被尖峰撑大后带变宽，负下界只是数学结果，不是
  物理温度。
- ts0 的 sd 为 NULL：样本量 1 无标准差，`mean + 3*σ` 随之为 NULL——预热期
  天然"无判断"，SQL 三值逻辑替我们传播了它。
- `.round()` 只为让打印结果窄一些。σ 先保留 3 位小数再参与算带；保留到
  2 位小数时，带与用未取整 σ 算出的完全一致。

## 第二步 · 关键一招：把带 shift(1)，只跟历史比

修复污染：比较基准取**上一行**的带。当前读数异常与否，由截至上一行的
历史决定——异常值再大也污染不了它自己的裁判。

```python
guard = banded.derive(
    prev_upper=lambda r: r.upper.shift(1, partition_by="device"),
    prev_lower=lambda r: r.lower.shift(1, partition_by="device"),
).derive(
    is_anomaly=lambda r: (r.temp > r.prev_upper) | (r.temp < r.prev_lower),
)

guard.filter(lambda r: r.device == "D1").select(
    "device", "ts", "temp", "prev_upper", "prev_lower", "is_anomaly"
).show();
```

> **预期输出**
>
> ```text
> +--------+----+------+------------+------------+------------+
> | device | ts | temp | prev_upper | prev_lower | is_anomaly |
> +--------+----+------+------------+------------+------------+
> | D1     | 0  | 20   | None       | None       | None       |
> | D1     | 1  | 21   | None       | None       | None       |
> | D1     | 2  | 19   | 22.62      | 18.38      | false      |
> | D1     | 3  | 22   | 23         | 17         | false      |
> | D1     | 4  | 20   | 24.37      | 16.63      | false      |
> | D1     | 5  | 41   | 24.37      | 16.63      | true       |
> | D1     | 6  | 21   | 56.72      | -5.72      | false      |
> | D1     | 7  | 20   | 56.1       | -4.1       | false      |
> +--------+----+------+------------+------------+------------+
> ```

**解析**

- **ts5 抓住了**：它的裁判是 ts4 的带（24.37 / 16.63），41 远超上界。
- **ts6/ts7 的滞后窗口是代价**：尖峰要在 4 期窗口里停留 4 期，离开之前
  带宽约 ±31。21、20 怎么判都正常（它们本来就落在尖峰前的带内），但第二
  个故障也会被放过：ts6 若为 30，同样判 False（练习 1）。若希望快速收带，
  缩短窗口或在判定后把异常行替换成带内值再重算。
- ts0/ts1 的 is_anomaly 为 NULL：裁判本身还没建立（shift 边界 + σ 预热）。
  **NULL ≠ False**：filter 不选它们、count 不数它们——预热期被诚实表达，
  而不是伪装成"正常"。
- 不 shift 直接判 `r.temp > r.upper` 会怎样？ts5 的 upper 是 56.72，41
  判 False——**漏报**。把两版都跑一遍，记住这组对照。

## 第三步 · 设备级汇总与守恒检查

`count_if` 数异常；NULL 行（预热期）不计入——这正是想要的口径。

```python
from ltseq import count_if

summary = (
    guard
    .group_by("device")
    .agg(
        total=lambda g: g.ts.count(),
        n_anom=lambda g: count_if(g.is_anomaly == True),
    )
    .sort("device")
)

summary.show();
```

> **预期输出**
>
> ```text
> +--------+-------+--------+
> | device | total | n_anom |
> +--------+-------+--------+
> | D1     | 8     | 1      |
> | D2     | 8     | 0      |
> +--------+-------+--------+
> ```

**解析**

- D1 抓到 1 个（41 度尖峰），D2 全绿——与目检一致。
- `count_if(g.is_anomaly == True)`：NULL == True 为 NULL，不计数——预热期
  的 2 行没有污染分子（total 仍是 8 而不是 6，要不要排除预热行看业务
  口径，见练习 3）。
- `total` 借一列来数行（`g.ts.count()`），与案例 25 相同。
- 到这里，"滚动基准 + 历史比较 + 量词汇总"三件套就是一个可上线的轻量
  异常检测器——单机数据集上它比引入一套流式监控平台轻快得多。

## 关键点解析

- **rolling 的部分帧语义**：没有 min_periods，窗口不满就按短窗口算——
  早期行的 σ 可能失真甚至 NULL。预热期要么接受"无判断"（本案例），要么
  显式排除（练习 3）。
- **基准与读数分离**：`shift(1)` 一行代码切开"裁判"与"选手"。所有
  "用历史评价现在"的题（异常检测、回撤预警、增量校验）都该过这道闸。
- **σ 的样本语义**：rolling 的 `std` 是样本标准差（除以 n-1）——小窗口下
  它明显大于总体标准差，带更宽，误报更少、检出更慢；窗口大小与 k 值是
  一对联动的旋钮。

## 延伸练习

1. 尖峰会让检测"失明"多久？把 ts6/ts7 的 temp 改成 30、25（都远高于尖峰
   前的上界 24.37），看 `is_anomaly` 怎么判；再给 D1 追加几个时刻，找出
   第一个用"不含尖峰的带"来判定的时刻。
2. 双向检测的对称性：构造一个 -10 度的**骤降**尖峰，验证 lower 侧同样
   生效。
3. 预热期处理：用 `row_number().over(partition_by=r.device, order_by=r.ts)`
   给行编号，`filter` 掉编号 ≤ 4 的行后再汇总——比较 total 的口径差异。
4. 加一列 `z_score=lambda r: (r.temp - r.prev_ma) / r.prev_sd`（先像第二步
   处理带那样把 ma/sd shift 到上一期），去掉预热期的 NULL，再用
   `rank().over(order_by=r.z_score.abs(), descending=True)` 输出 Top 异常
   ——比布尔标记更可解释。

---
**上一例**：[25 · 告警冷却抑制](25_alert_suppress.md) · **下一例**：[27 · 重采样与日历对齐](27_resample_align.md)
