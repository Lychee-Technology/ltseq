<!-- Generated from ../../cn/25_alert_suppress.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 25 · 告警冷却抑制：fold 状态机救场

> **难度** ★★★ · **领域** 日志/时序监控 · **前置** 案例 06、18
>
> **涉及 API**：`sort` / `shift(1, partition_by=)` / `if_else` / `fold(init=, into=, partition_by=)` / `derive` / `group_by().agg()` + `count_if`

**学习目标**

- 实现"同类告警 30 分钟冷却"：与**最近一次告警**比较，而不是与上一条记录
  比较——两者在链式场景下分道扬镳
- 先故意用 `shift` 写出**错误解法**，看它在告警链上翻车，再请 fold 出场
- 掌握 fold 的状态设计：状态 = 最近一次告警时刻，被抑制的行不改状态

## 问题描述

告警流水 `alerts(ts, rule)`，ts 为分钟。冷却规则：**同一 rule 距离它最近一次
实际发出的告警不足 30 分钟 → 抑制**。目标：

1. 标记每条告警是 **fired**（发出）还是 suppressed（抑制）；
2. 统计每条 rule 的发出/抑制数量；
3. 重点看清：为什么"与上一条记录比较"的 naive 解法在告警链上是错的。

```python
from ltseq import LTSeq

alerts = LTSeq.from_rows([
    {"rule": "cpu", "ts": 0},
    {"rule": "cpu", "ts": 10},
    {"rule": "cpu", "ts": 35},
    {"rule": "cpu", "ts": 80},
    {"rule": "cpu", "ts": 95},
    {"rule": "disk", "ts": 5},
    {"rule": "disk", "ts": 20},
    {"rule": "mem", "ts": 15},
]).sort("rule", "ts")

alerts.show();
```

> **预期输出**
>
> ```text
> +------+----+
> | rule | ts |
> +------+----+
> | cpu  | 0  |
> | cpu  | 10 |
> | cpu  | 35 |
> | cpu  | 80 |
> | cpu  | 95 |
> | disk | 5  |
> | disk | 20 |
> | mem  | 15 |
> +------+----+
> ```

预判（按"距最近一次发出 > 30 才发"）：cpu 发出 0、35、80，抑制 10、95；
disk 发出 5、抑制 20；mem 发出 15。**关键在 cpu@35**：它与上一条记录
（10）只差 25，但与最近一次发出（0）差 35——应当发出。

## 第一步 · naive 解法：与上一条记录比较（故意翻车）

`shift(1)` 拿上一条 ts，间隔 > 30 就发——看起来天衣无缝，跑一遍 cpu 链。

```python
from ltseq import if_else

naive = (
    alerts
    .derive(prev_ts=lambda r: r.ts.shift(1, partition_by="rule"))
    .derive(
        naive_fire=lambda r: if_else(
            r.prev_ts.is_null() | ((r.ts - r.prev_ts) > 30), True, False
        ),
    )
)

naive.filter(lambda r: r.rule == "cpu").show();
```

> **预期输出**
>
> ```text
> +------+----+---------+------------+
> | rule | ts | prev_ts | naive_fire |
> +------+----+---------+------------+
> | cpu  | 0  | None    | true       |
> | cpu  | 10 | 0       | false      |
> | cpu  | 35 | 10      | false      |
> | cpu  | 80 | 35      | true       |
> | cpu  | 95 | 80      | false      |
> +------+----+---------+------------+
> ```

**解析**

- cpu@35 的判断基准错了：它比较的是**上一条记录**（10，本身是被抑制的）
  而不是**最近一次发出的告警**（0）。25 ≤ 30 → 误判抑制。
- 连锁后果：真正的告警被吞掉，且错误会**传染**——基准随每一条记录移动，
  不管它发没发出。80 判对纯属巧合：它的上一条记录 35 恰好也是本该发出的
  最近一次告警。换成每 20 分钟一条的告警链（0, 20, 40, 60, ...），naive
  只在 0 发一次，此后整条链持续静默；冷却规则则会在 40、80 再次发出。
- 结论：**基准必须是"最近一次发出"这个被规则筛选过的量**，shift 只能看
  固定偏移的邻居，够不着它——这正是 fold 的舞台。

## 第二步 · fold 状态机：状态 = 最近一次发出的时刻

状态 `s` 即冷却基准：发出（s 为空或间隔 > 30）时状态更新为本行 ts；抑制
时状态**原样传递**。fold 的返回值既写入新列又传给下一行——正好一个通道
完成"存储 + 接力"。

```python
suppressed = alerts.fold(
    lambda s, r: (
        r["ts"]
        if (s is None) or (r["ts"] - s > 30)
        else s
    ),
    init=None,
    into="last_alert",
    partition_by="rule",
)

fired = suppressed.derive(
    is_fired=lambda r: r.last_alert == r.ts,
)

fired.filter(lambda r: r.rule == "cpu").show();
```

> **预期输出**
>
> ```text
> +------+----+------------+----------+
> | rule | ts | last_alert | is_fired |
> +------+----+------------+----------+
> | cpu  | 0  | 0          | true     |
> | cpu  | 10 | 0          | false    |
> | cpu  | 35 | 35         | true     |
> | cpu  | 80 | 80         | true     |
> | cpu  | 95 | 80         | false    |
> +------+----+------------+----------+
> ```

**解析**

- cpu@35 被正确发出：fold 的状态在 10 被抑制后**仍是 0**，35 - 0 > 30 →
  发出并更新状态为 35。naive 解法翻车的点被状态接力修复。
- `is_fired = (last_alert == ts)`：发出的行 last_alert 恰好等于本行 ts；抑制
  行的 last_alert 停在更早的时刻——一个等式把"存储值"翻译回"事件标记"。
  本例同一 rule 内 ts 互不相同，等式判断是安全的；若同一 rule 同一分钟
  有两条告警，被抑制的那条也满足等式。有重复 ts 时改判"状态是否变化"：
  `last_alert` 与 `r.last_alert.shift(1, partition_by="rule")` 不同，或该
  shift 为 NULL，即为发出。
- fold 的 lambda 是 **Python 世界**：`s is None`、`or`、三元表达式——与
  表达式 lambda 的 `& |`、`.is_null()` 划清界限（案例 20/24 的对比在
  这里收口）。
- `partition_by="rule"`：冷却按 rule 独立计数，换 rule 即重置。

## 第三步 · 汇总：每条 rule 的发出/抑制数

fired 表是普通 LTSeq，`count_if` 条件计数收尾。

```python
from ltseq import count_if

summary = (
    fired
    .group_by("rule")
    .agg(
        total=lambda g: g.ts.count(),
        n_fired=lambda g: count_if(g.is_fired == True),
    )
    .sort("rule")
)

summary.show();
```

> **预期输出**
>
> ```text
> +------+-------+---------+
> | rule | total | n_fired |
> +------+-------+---------+
> | cpu  | 5     | 3       |
> | disk | 2     | 1       |
> | mem  | 1     | 1       |
> +------+-------+---------+
> ```

**解析**

- 8 条告警收敛成 5 条通知（抑制率 37.5%）——风暴场景里这就是值守手机
  的安静程度。
- `total=lambda g: g.ts.count()` 借一列来数行：`group_by().agg()` 的
  lambda 拿到的是列代理（案例 05），直接写 `g.count()` 会去找名为
  `count` 的列。
- `count_if(g.is_fired == True)`：NULL 与 False 都不计入——本例无 NULL
  （is_fired 由等式产生，永不为 NULL），但写成条件计数总是对的。
- 优雅的副产品：`last_alert` 列本身就是"每行视角下的最近告警时刻"，可供
  根因分析（抑制发生时，是谁压住了我）。

## 关键点解析

- **"与上一条比" ≠ "与最近一次满足条件者比"**：前者 shift 一步可达，后者
  需要状态。链式抑制、粘性会话、加权累计……凡"基准会被规则更新"的题，
  都是 fold 的领地。
- **状态设计三问**：初值是什么（None = 尚无告警）；何时更新（发出时）；
  何时保持（抑制时）。想清三问，fold 的 lambda 十行内写完。
- **fold 不懒**：整表物化、逐行 Python 回调——告警表这类"每 rule 每天几十
  条"的量级正合适；亿行流水请先预聚合再用 fold。

## 延伸练习

1. 把冷却规则改成"滑动窗口内最多 2 条"（窗口 60 分钟内已发 2 条则抑制）：
   状态升级为"最近发出的 ts 列表"，fold 里用 Python 列表维护。`show()`
   会把列表列打印成 `[unsupported type]`，用 `to_dicts()` 读回结果。
2. 给 alerts 加 severity 列，规则改为"critical 无冷却、warning 有冷却"——
   状态更新条件怎么写？
3. 在 `naive` 上用
   `search_pattern(lambda r: r.ts - r.prev_ts <= 30, partition_by="rule")`
   找出"距上一条记录不超过 30 分钟"的告警（`search_pattern` 的谓词里不能
   调用 `shift`，所以借用 `prev_ts`），与 summary 的抑制数对照——它们为
   什么不相等？
4. 换成时间戳列（`from_rows` 里直接用 Python 的 `datetime` 值构造 `ts`），
   fold 里改与 `timedelta(minutes=30)` 比较——体会状态机对"间隔单位"的
   透明性。

---
**上一例**：[24 · 进出事件配对](24_pair_events.md) · **下一例**：[26 · 波动带异常检测](26_sensor_anomaly.md)
