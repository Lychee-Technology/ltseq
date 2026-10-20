<!-- Generated from ../../cn/24_pair_events.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 24 · 进出事件配对：相邻匹配与状态接力

> **难度** ★★★ · **领域** 日志/时序监控 · **前置** 案例 06、18
>
> **涉及 API**：`sort` / `shift(-1, partition_by=)`（向前看）/ `filter` / `group_by().agg()` + `count_if` / `fold(partition_by=)` / `is_not_null`

**学习目标**

- 用 `shift(-1)` **向前看一行**实现"相邻 in→out 配对"——最朴素的配对解法
- 认识相邻匹配的盲区：中间插了无关事件就断——用 `fold` 的状态接力补位
- 用 `count_if` 做进出数量对账，识别不平衡的记录

## 问题描述

门禁刷卡流水 `swipes(person, ts, direction)`，ts 为分钟，direction ∈
{in, out, deny}。目标：

1. 把 in 与其对应的 out **配对**，计算每次在场时长；
2. 对每个人的 in/out 数量**对账**，找出不平衡的记录；
3. 处理两类脏数据：**连进**（in 后又 in）与**噪声插入**（in 和 out 之间
   夹了 deny）。

```python
from ltseq import LTSeq

swipes = LTSeq.from_rows([
    {"person": "p1", "ts": 0, "direction": "in"},
    {"person": "p1", "ts": 25, "direction": "out"},
    {"person": "p1", "ts": 120, "direction": "in"},
    {"person": "p1", "ts": 150, "direction": "out"},
    {"person": "p2", "ts": 5, "direction": "in"},
    {"person": "p2", "ts": 40, "direction": "in"},
    {"person": "p2", "ts": 60, "direction": "out"},
    {"person": "p3", "ts": 10, "direction": "in"},
    {"person": "p3", "ts": 35, "direction": "out"},
    {"person": "p3", "ts": 50, "direction": "out"},
    {"person": "p4", "ts": 0, "direction": "in"},
    {"person": "p4", "ts": 10, "direction": "deny"},
    {"person": "p4", "ts": 20, "direction": "out"},
]).sort("person", "ts")

swipes.show(13);
```

> **预期输出**
>
> ```text
> +--------+-----+-----------+
> | person | ts  | direction |
> +--------+-----+-----------+
> | p1     | 0   | in        |
> | p1     | 25  | out       |
> | p1     | 120 | in        |
> | p1     | 150 | out       |
> | p2     | 5   | in        |
> | p2     | 40  | in        |
> | p2     | 60  | out       |
> | p3     | 10  | in        |
> | p3     | 35  | out       |
> | p3     | 50  | out       |
> | p4     | 0   | in        |
> | p4     | 10  | deny      |
> | p4     | 20  | out       |
> +--------+-----+-----------+
> ```

预判：p1 两对（25、30 分钟）；p2 一对（40→60，20 分钟，5 的进被 40 顶
掉）；p3 一对（10→35，25 分钟，50 是无主 out）；p4 一对但被 deny 隔断
——相邻匹配会漏，fold 能救。

## 第一步 · 相邻配对：shift(-1) 向前看

`shift(1)` 看上一行，`shift(-1)` 看**下一行**——正负号就是方向。in 行只要
下一行是 out 即配对成功，时长 = next_ts - ts。

```python
stamped = swipes.derive(
    next_dir=lambda r: r.direction.shift(-1, partition_by="person"),
    next_ts=lambda r: r.ts.shift(-1, partition_by="person"),
)

pairs = stamped.filter(
    lambda r: (r.direction == "in") & (r.next_dir == "out"),
).derive(
    minutes=lambda r: r.next_ts - r.ts,
)

pairs.show();
```

> **预期输出**
>
> ```text
> +--------+-----+-----------+----------+---------+---------+
> | person | ts  | direction | next_dir | next_ts | minutes |
> +--------+-----+-----------+----------+---------+---------+
> | p1     | 0   | in        | out      | 25      | 25      |
> | p1     | 120 | in        | out      | 150     | 30      |
> | p2     | 40  | in        | out      | 60      | 20      |
> | p3     | 10  | in        | out      | 35      | 25      |
> +--------+-----+-----------+----------+---------+---------+
> ```

**解析**

- p2 只有 40 行配对成功：5 行的下一行是 in 不是 out——"连进"被相邻匹配
  **自动按后进站顶掉**（与闸机真实行为一致）。
- **p4 完全失踪**：0 行的下一行是 deny，20 行是 out 没有 in——中间插入的
  噪声让相邻匹配失明。这是它的结构性盲区，不是 bug。
- 4 对 vs 预判 5 对：差的就是 p4。数量对账（下一步）能系统性地暴露这类
  缺口。

## 第二步 · 进出对账：count_if 条件计数

配对解法的完整性需要独立验证。按人聚合 in/out 各自条数——条数不等必有
蹊跷。

```python
from ltseq import count_if

audit = (
    swipes
    .group_by("person")
    .agg(
        n_in=lambda g: count_if(g.direction == "in"),
        n_out=lambda g: count_if(g.direction == "out"),
    )
    .sort("person")
)

audit.show();
```

> **预期输出**
>
> ```text
> +--------+------+-------+
> | person | n_in | n_out |
> +--------+------+-------+
> | p1     | 2    | 2     |
> | p2     | 2    | 1     |
> | p3     | 1    | 2     |
> | p4     | 1    | 1     |
> +--------+------+-------+
> ```

**解析**

- `count_if(g.direction == "in")`：条件计数只数满足谓词的行——import 自
  `ltseq` 顶层，聚合上下文专用。
- 对账读数：p1 平衡；p2 多一个 in（连进）；p3 多一个 out（无主 out）；
  p4 数量平衡**但配对失败**——对账是必要条件不充分，两类检查要并用。
- p4 这种"数量平衡却失配"正是相邻匹配盲区的数据学证据：in 与 out 都在，
  只是**不相邻**。

## 第三步 · fold 状态接力：跨过噪声的配对

fold 维护一个状态：**最近一次未配对的 in 时刻**。in 更新状态；out 消费
状态；deny 之类对状态无感——噪声自然被跨过。

```python
paired = swipes.fold(
    lambda s, r: (
        r["ts"]
        if r["direction"] == "in"
        else s
    ),
    init=None,
    into="open_in",
    partition_by="person",
)

matched = paired.filter(
    lambda r: (r.direction == "out") & r.open_in.is_not_null(),
).derive(
    minutes=lambda r: r.ts - r.open_in,
).sort("person", "ts")

matched.show();
```

> **预期输出**
>
> ```text
> +--------+-----+-----------+---------+---------+
> | person | ts  | direction | open_in | minutes |
> +--------+-----+-----------+---------+---------+
> | p1     | 25  | out       | 0       | 25      |
> | p1     | 150 | out       | 120     | 30      |
> | p2     | 60  | out       | 40      | 20      |
> | p3     | 35  | out       | 10      | 25      |
> | p3     | 50  | out       | 10      | 40      |
> | p4     | 20  | out       | 0       | 20      |
> +--------+-----+-----------+---------+---------+
> ```

**解析**

- **p4 被救回来了**：in@0 开启状态，deny 保持状态不动，out@20 消费
  open_in=0 → 20 分钟——fold 的状态接力跨过了噪声行。
- **p3 出现一条可疑记录**：out@50 的 open_in 还是 10——第一次 out（35）
  消费后状态**没有被清掉**（fold 的返回值既存储又传递，单通道），于是
  第二个 out 又"配"了一次 10→50。对账表（p3 out 比 in 多 1）正好标记这
  类人；要严格处理需在状态里维护"已闭合"标志或栈结构——见延伸练习。
- fold 的 lambda 里是 **Python 值**：`r["direction"] == "in"` 是普通比较、
  三元表达式随便写——与表达式 lambda 的表达力边界截然不同（案例 20/25
  反复对比）。
- `partition_by="person"` 让状态在换人时重置为 None——防串人的老规矩。

## 关键点解析

- **shift 的负偏移 = 向前看**：`shift(-1)` 取下一行，与 `shift(1)` 对称。
  "配对相邻记录"类问题（进出、请求响应、开关事件）都从它起步。
- **相邻匹配 vs 状态接力**：前者一行表达式、失败于噪声与间隔；后者一行
  fold、失败于复杂嵌套——按脏数据形态选工具，并用对账表兜底。
- **fold 单通道的代价**：返回值既要存储又要传递，"消费状态"与"保留状态"
  不可兼得——需要栈/队列语义时，把状态设计成编码值或拆两遍 fold。

## 延伸练习

1. 严格版 fold：状态改为"(open_in, is_open) 编码值"（如 open_in + 0.5 表示
   已闭合），让 p3 的第二个 out 被正确判为无主——体验单通道的绕行术。
2. 无主 out 报表：对练习 1 的严格版 fold 结果执行 `filter(lambda r:
   (r.direction == "out") & r.open_in.is_null())`——p3 的 50 应该在列。在简化
   版 `paired` 上同一个 filter 一行也没有，因为它把这条误配了。
3. 用 `search_pattern` 数"相邻 in→out"的出现次数
   （`search_pattern_count`），与 pairs 的 count 互相印证。
4. 进阶：同一人一天多次进出且有午休规则（12:00-13:00 的 out 不计）——
   规则进 fold 的状态机还是先 filter 再配对？写下你的取舍。

---
**上一例**：[23 · 重叠区间合并](23_interval_merge.md) · **下一例**：[25 · 告警冷却抑制](25_alert_suppress.md)
