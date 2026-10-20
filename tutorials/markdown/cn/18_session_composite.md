<!-- Generated from ../../cn/18_session_composite.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 18 · 复合会话规则：不止看时间间隔

> **难度** ★★★ · **领域** 电商 · **前置** 案例 10
>
> **涉及 API**：`sort` / `shift(1, partition_by=)` / `if_else` / `is_null` / `& |` 组合 / `cum_sum(partition_by=)` / `group_by().agg()` / `s.concat` / `cast("string")`

**学习目标**

- 在案例 10 的"纯间隔会话"之上，加入**依赖上一事件类型**的切分规则
- 练习复合布尔条件的组合：`(间隔 > 600) | (上一动作是退出 且 间隔 > 300)`
- 从同一张打好标记的表产出两级汇总：每用户会话数（一行）与每会话明细（多行）

## 问题描述

行为流水 `events(user, ts, action)`，`ts` 为当日起始秒数。会话终止规则
（复合条件）：

- 相邻两条记录**间隔超过 600 秒**（10 分钟无动作）→ 新会话；
- **上一条 action 是 `exit`** 且间隔超过 300 秒（退出后 5 分钟没回来）→ 新会话。

目标：给每条记录打会话号；输出每用户会话数与每会话摘要。

预判两个易错点：u2 在 760→900 处间隔只有 140 秒，虽然上一条是 `exit`，
但 140 ≤ 300，**不该断开**；u3 的 10→3800 间隔 3790 秒，**只靠时间规则就
该断开**。

```python
from ltseq import LTSeq

events = LTSeq.from_rows([
    {"user": "u1", "ts": 0, "action": "login"},
    {"user": "u1", "ts": 60, "action": "view"},
    {"user": "u1", "ts": 300, "action": "add_cart"},
    {"user": "u1", "ts": 340, "action": "exit"},
    {"user": "u1", "ts": 1200, "action": "login"},
    {"user": "u1", "ts": 1250, "action": "view"},
    {"user": "u1", "ts": 1500, "action": "exit"},
    {"user": "u2", "ts": 5, "action": "login"},
    {"user": "u2", "ts": 400, "action": "view"},
    {"user": "u2", "ts": 700, "action": "add_cart"},
    {"user": "u2", "ts": 760, "action": "exit"},
    {"user": "u2", "ts": 900, "action": "view"},
    {"user": "u2", "ts": 2000, "action": "exit"},
    {"user": "u3", "ts": 10, "action": "login"},
    {"user": "u3", "ts": 3800, "action": "view"},
]).sort("user", "ts")
```

## 第一步 · 组内位移：上一时刻 + 上一动作

两条 `shift` 互不引用，可放同一个 `derive`。组边界返回 NULL 的老规矩，
`prev_ts` 为 NULL 即"该用户第一行"。

```python
stamped = events.derive(
    prev_ts=lambda r: r.ts.shift(1, partition_by="user"),
    prev_action=lambda r: r.action.shift(1, partition_by="user"),
)
```

## 第二步 · 复合断点条件：把业务规则翻译成布尔式

规则里的"且/或"对应 `&` / `|`；**括号必须手动加全**——在 Python 里 `&`、`|`
的优先级高于 `>`、`==`，每个比较都要单独加括号，否则 Python 会把操作数配错
（`600 | (...)`）并抛 `TypeError`。`&` 也高于 `|`，把这层分组也写出来，规则
更好读。

```python
from ltseq import if_else

flagged = stamped.derive(
    brk=lambda r: if_else(
        r.prev_ts.is_null(),
        1,
        if_else(
            ((r.ts - r.prev_ts) > 600)
            | ((r.prev_action == "exit") & ((r.ts - r.prev_ts) > 300)),
            1,
            0,
        ),
    ),
).derive(
    sid=lambda r: r.brk.cum_sum(partition_by="user"),
)

flagged.show(15);
```

> **预期输出**
>
> ```text
> +------+------+----------+---------+-------------+-----+-----+
> | user | ts   | action   | prev_ts | prev_action | brk | sid |
> +------+------+----------+---------+-------------+-----+-----+
> | u1   | 0    | login    | None    | None        | 1   | 1   |
> | u1   | 60   | view     | 0       | login       | 0   | 1   |
> | u1   | 300  | add_cart | 60      | view        | 0   | 1   |
> | u1   | 340  | exit     | 300     | add_cart    | 0   | 1   |
> | u1   | 1200 | login    | 340     | exit        | 1   | 2   |
> | u1   | 1250 | view     | 1200    | login       | 0   | 2   |
> | u1   | 1500 | exit     | 1250    | view        | 0   | 2   |
> | u2   | 5    | login    | None    | None        | 1   | 1   |
> | u2   | 400  | view     | 5       | login       | 0   | 1   |
> | u2   | 700  | add_cart | 400     | view        | 0   | 1   |
> | u2   | 760  | exit     | 700     | add_cart    | 0   | 1   |
> | u2   | 900  | view     | 760     | exit        | 0   | 1   |
> | u2   | 2000 | exit     | 900     | view        | 1   | 2   |
> | u3   | 10   | login    | None    | None        | 1   | 1   |
> | u3   | 3800 | view     | 10      | login       | 1   | 2   |
> +------+------+----------+---------+-------------+-----+-----+
> ```

**解析**

- 关键验证点 u2 的 900 行：间隔 140，`(140 > 600)` 假、`(prev==exit 且
  140 > 300)` 假 → `brk=0`，**没断**——若只写 `prev==exit` 就断（无时间
  约束），这一行就会被错误地切成新会话。
- u3 的 3800 行：间隔 3790 > 600 → `brk=1`——时间规则独立生效，不需要
  exit 参与。
- 这份数据里 exit 子句从未单独起作用：u1 的 1200 行虽然紧跟 `exit`，但
  860 秒的间隔本身就触发了时间规则。练习 4 挪动这次登录，让只有 exit 子句
  生效。
- `brk` 全表只有 0/1，组内 `cum_sum` 后即会话号——案例 10/15 的编号手法
  原样复用。
- 嵌套 `if_else`：外层处理 NULL（首行），内层放复合条件。也可以用
  `when(...).when(...).otherwise(...)` 改写，二选一。

## 第三步 · 用户级汇总：一行一个用户

会话号是组内递增的——**最大会话号就是会话数**。一行聚合直接出结果，
不必先展开会话明细。

```python
counts = (
    flagged
    .group_by("user")
    .agg(n_sessions=lambda g: g.sid.max())
    .sort("user")
)

counts.show();
```

> **预期输出**
>
> ```text
> +------+------------+
> | user | n_sessions |
> +------+------------+
> | u1   | 2          |
> | u2   | 2          |
> | u3   | 2          |
> +------+------------+
> ```

**解析**

- `g.sid.max()`：对**派生列**聚合与对原始列聚合没有任何区别——derive 的
  输出就是普通列。
- `max(sid) = 会话数` 依赖 sid 的生成方式（从 1 连续递增）。这是有序编号
  带来的"免费汇总"：有序世界里的很多统计，是对序号的 min/max/count。

## 第四步 · 会话级明细：复合键分组

要每会话的起止与页数，就需要 (user, sid) 二级分组——复用案例 15 的复合
键手法。

```python
keyed = flagged.derive(
    session_key=lambda r: r.user.s.concat("#", r.sid.cast("string")),
)

sessions = (
    keyed
    .group_by("session_key")
    .agg(
        user=lambda g: g.user.min(),
        start=lambda g: g.ts.min(),
        end=lambda g: g.ts.max(),
        actions=lambda g: g.ts.count(),
    )
    .sort("user", "start")
)

sessions.show();
```

> **预期输出**
>
> ```text
> +-------------+------+-------+------+---------+
> | session_key | user | start | end  | actions |
> +-------------+------+-------+------+---------+
> | u1#1        | u1   | 0     | 340  | 4       |
> | u1#2        | u1   | 1200  | 1500 | 3       |
> | u2#1        | u2   | 5     | 900  | 5       |
> | u2#2        | u2   | 2000  | 2000 | 1       |
> | u3#1        | u3   | 10    | 10   | 1       |
> | u3#2        | u3   | 3800  | 3800 | 1       |
> +-------------+------+-------+------+---------+
> ```

**解析**

- u2#1 的 end=900：760 的 exit **不是**会话终点——规则只说 exit 后 300 秒
  未归才断，900 回来了，会话延续到 900，之后 2000 才开新会话。
- `group_by().agg()` 是哈希分组，组内没有行序，也就没有 `first()` /
  `last()`。会话内 `ts` 递增，所以 `min` / `max` 就是起点和终点；`user`
  在会话内不变，`min` 只是把它取出来。真要取组内第一行或最后一行，用
  `group_ordered(...).agg()`（案例 10）里的 `g.first()` / `g.last()`。
- 单行动作（u2#2）start == end 是正常形态，不用特判。
- 两级汇总（用户级 counts、会话级 sessions）都从同一张 `flagged` 出发——
  标记好断点的中间表是会话类需求的"公共底座"。

## 关键点解析

- **复合规则 = 复合布尔式**：`& |` 加全括号，是表达式风格最容易翻车也最
  值得练熟的地方。先在注释里写一遍业务规则，再逐词翻译。
- **"上一行状态"是会话题的钥匙**：`prev_action` 与 `prev_ts` 同源同界（同一
  个 partition_by），一起 shift 保证对齐。
- **两级汇总共用底座**：max(sid) 出用户数、复合键分组出明细——先打标记，
  汇总只是顺手的姿势。

## 延伸练习

1. 把规则改成三条：间隔 > 600、exit 后 > 300、**连续 3 次 view 后**强制新
   会话（提示：`view` 计数需在组内滑动——
   `if_else(r.action == "view", 1, 0).rolling(3, partition_by="user").sum()`，
   或再加一列计数标记，想想哪个更直白）。
2. 时长 Top 2 的会话：对 sessions 表 `derive(duration=r.end - r.start)`，
   降序排序后 `slice(0, 2)`，注意 u2#2 时长为 0 的排序位置。
3. 换成真实时间戳：用 Python `datetime` 值构造 `ts`，再用
   `r.ts.dt.diff(r.prev_ts, unit="second")` 计算间隔，体会它与数值差写法的
   差异。
4. 让 exit 子句单独起作用：把 u1 的第二次登录从 1200 挪到 800（exit 后 460
   秒）。这一行的 `brk` 仍是 1，但这次只靠 exit 子句；去掉该子句，u1 就只剩
   一个会话。

---
**上一例**：[17 · 榜单排名迁移](17_rank_migration.md) · **下一例**：[19 · 汇总分摊到明细](19_alloc_summary.md)
