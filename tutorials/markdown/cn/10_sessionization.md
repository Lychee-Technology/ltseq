<!-- Generated from ../../cn/10_sessionization.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 10 · 会话切分：间隙检测与有序分组

> **难度** ★★★ · **领域** 日志/时序监控 · **前置** 案例 03
>
> **涉及 API**：`shift(1, partition_by=)` / `if_else` / `cum_sum` / `group_ordered` / `agg` / `flatten()` / `__group_id__`

**学习目标**

- 实现 ClickBench 式**会话化**：给无 session 字段的流水打会话号
- 掌握 `shift` 的组内模式：`shift(1, partition_by=)`
- 用 `flatten()` 给每条记录回填组号，形成"明细 + 会话号"双输出

**同类场景**：LTSeq 自己的 benchmark（`benchmarks/bench_vs.py` 第 2 轮）对
ClickBench `hits` 数据集做会话化，用的是同样的间隙标记——换用户或间隔超过 30 分钟
就开新会话——再用 `group_ordered` 数出会话数。

## 问题描述

行为流水 `events(user, ts, page)`，`ts` 为当日起始秒数。规则：**同一用户相邻两条
记录间隔超过 30 分钟则开启新会话**。目标：

1. 给每条记录打会话号；
2. 汇总每个会话：用户、起止、页数、时长（分钟）；
3. 展开回填：每条明细带上 `__group_id__` 会话编号。

```python
from ltseq import LTSeq

plans = {
    "u1": [(0, "home"), (120, "list"), (300, "detail"), (5400, "home"), (5700, "cart")],
    "u2": [(10, "home"), (60, "search")],
    "u3": [(5, "home"), (100, "list"), (2600, "detail"), (3000, "order")],
}
rows = []
for u, evs in plans.items():
    for ts, page in evs:
        rows.append({"user": u, "ts": ts, "page": page})

events = LTSeq.from_rows(rows).sort("user", "ts")
events.show();
```

> **预期输出**
>
> ```text
> +------+------+--------+
> | user | ts   | page   |
> +------+------+--------+
> | u1   | 0    | home   |
> | u1   | 120  | list   |
> | u1   | 300  | detail |
> | u1   | 5400 | home   |
> | u1   | 5700 | cart   |
> | u2   | 10   | home   |
> | u2   | 60   | search |
> | u3   | 5    | home   |
> | u3   | 100  | list   |
> | u3   | 2600 | detail |
> +------+------+--------+
> ```

`show()` 默认只打印前 10 行（`show(n)` 可以多打印），所以这里看不到 u3 的最后一条
事件。

预判答案，跑完验证：u1 有 2 个会话（gap 5100 > 1800 断开）、u2 有 1 个、
u3 有 2 个（gap 2500 断开）——共 5 个会话。

## 第一步 · 间隙标记：组内 shift + 三值逻辑

`shift(1, partition_by="user")` 在**组内**取上一行、组边界返回 NULL——这是会话
类问题的万能起手式。

```python
from ltseq import if_else

flagged = (
    events
    .derive(prev_ts=lambda r: r.ts.shift(1, partition_by="user"))
    .derive(
        new_session=lambda r: if_else(
            r.prev_ts.is_null() | ((r.ts - r.prev_ts) > 1800), 1, 0
        ),
    )
    .cum_sum("new_session")           # → new_session_cumsum：会话号
)
flagged.show();
```

> **预期输出**
>
> ```text
> +------+------+--------+---------+-------------+--------------------+
> | user | ts   | page   | prev_ts | new_session | new_session_cumsum |
> +------+------+--------+---------+-------------+--------------------+
> | u1   | 0    | home   | None    | 1           | 1                  |
> | u1   | 120  | list   | 0       | 0           | 1                  |
> | u1   | 300  | detail | 120     | 0           | 1                  |
> | u1   | 5400 | home   | 300     | 1           | 2                  |
> | u1   | 5700 | cart   | 5400    | 0           | 2                  |
> | u2   | 10   | home   | None    | 1           | 3                  |
> | u2   | 60   | search | 10      | 0           | 3                  |
> | u3   | 5    | home   | None    | 1           | 4                  |
> | u3   | 100  | list   | 5       | 0           | 4                  |
> | u3   | 2600 | detail | 100     | 1           | 5                  |
> +------+------+--------+---------+-------------+--------------------+
> ```

**解析**

- 首行 `prev_ts` 为 NULL。三值逻辑里 `TRUE | NULL = TRUE`，所以
  `is_null() | (间隔 > 1800)` 对首行**必然为真**——不用额外特判，一个条件两种情况
  全覆盖（与案例 03 的 `changed` 同一模式）。
- `cum_sum("new_session")` 新增 `new_session_cumsum` 列：**会话号全局单调**，
  且用户排序下同一会话的行必然连续——这正是 `group_ordered` 的理想输入。

## 第二步 · 会话汇总：连续分组折叠

`group_ordered` 按连续相同的会话号分组，`agg` 折叠成一行一个会话。

```python
sessions = (
    flagged
    .group_ordered(lambda r: r.new_session_cumsum)
    .agg(lambda g: {
        "user":  g.first().user,
        "start": g.first().ts,
        "end":   g.last().ts,
        "pages": g.count(),
    })
    .derive(dur_min=lambda r: (r.end - r.start) / 60.0)
)
sessions.show();
```

> **预期输出**
>
> ```text
> +------+-------+------+-------+--------------------+
> | user | start | end  | pages | dur_min            |
> +------+-------+------+-------+--------------------+
> | u1   | 0     | 300  | 3     | 5                  |
> | u1   | 5400  | 5700 | 2     | 5                  |
> | u2   | 10    | 60   | 2     | 0.8333333333333334 |
> | u3   | 5     | 100  | 2     | 1.5833333333333333 |
> | u3   | 2600  | 3000 | 2     | 6.666666666666667  |
> +------+-------+------+-------+--------------------+
> ```

**解析**

- **为什么时长放在 agg 之后的 derive？** `agg` 内目前不支持聚合结果的算术组合
  （`(g.last().ts - g.first().ts)` 会报错）——先把 `start` / `end` 折叠成两列，
  再在表级 `derive` 里相减。这是官方文档明确的当前限制与推荐绕行写法。
- `group_ordered` 的分组键是 lambda（可以是表达式），组按出现顺序输出——与输入
  顺序一致，汇总后不需要再 `sort`。

## 第三步 · 明细回填：flatten

要"每条明细 + 会话号"的宽表，用 `flatten()` 展开，组号在 `__group_id__` 列：

```python
labeled = (
    flagged
    .group_ordered(lambda r: r.new_session_cumsum)
    .flatten()
    .select("user", "ts", "page", "__group_id__")
)
labeled.show();
```

> **预期输出**
>
> ```text
> +------+------+--------+--------------+
> | user | ts   | page   | __group_id__ |
> +------+------+--------+--------------+
> | u1   | 0    | home   | 1            |
> | u1   | 120  | list   | 1            |
> | u1   | 300  | detail | 1            |
> | u1   | 5400 | home   | 2            |
> | u1   | 5700 | cart   | 2            |
> | u2   | 10   | home   | 3            |
> | u2   | 60   | search | 3            |
> | u3   | 5    | home   | 4            |
> | u3   | 100  | list   | 4            |
> | u3   | 2600 | detail | 5            |
> +------+------+--------+--------------+
> ```

**解析**

- `flatten()` 还带内部辅助列 `__group_count__` / `__rn__`（组内行号），做"会话内
  第 N 步"分析直接可用。
- `NestedTable` 的四种消费方式至此集齐：`agg` 折叠 / `derive` 广播 / `filter` 筛段
  / `flatten` 展开——按输出形态选方法。

## 关键点解析

1. **会话三件套**（也是案例 03 的段三件套）：`组内 shift 标记间隙 → cum_sum 编号
   → group_ordered 折叠`。改一改间隔阈值和键就是刷卡配对、停车计费、断点续传
   分段……
2. **`partition_by` 让窗口函数组内化**：`shift` / `rolling` / `cum_*` 全部支持
   `partition_by=` 参数，等价 SQL 的 `OVER (PARTITION BY ...)`。`cum_*` 指的是
   表达式写法 `r.x.cum_sum(partition_by="user")`；上面用的表级 `cum_sum("col")`
   不接受 `partition_by`。
3. **会话号 = 全局单调编号**：靠"先按 user 排序 + cum_sum 不回落"保证连续性；
   表没有声明顺序时，序列窗口函数直接抛 `SortRequiredError`，不会默默算乱。但声明的顺序
   不以 `user` 开头（比如 `sort("ts")`）时不会报错：不同用户的会话交错，
   `group_ordered` 会把它们切碎。
4. **限制要背**：`agg` 内聚合值不能做算术；拆两步（agg 出列 → derive 组合）是
   标准绕行。

## 延伸练习

- 会话内行为序号：`flatten()` 取 `__rn__`，配合 `shift(1)` 做"上一步 → 当前页"
  转移统计。
- 复合键会话：先 `derive` 把 `user` 和 `device` 拼成键列（如
  `r.user.s.concat("|", r.device)`），再 `partition_by`。
- 大文件版：把事件按 `user, ts` 排好序写成 Parquet，用
  `LTSeq.read_parquet(...).assume_sorted("user", "ts")`（案例 12）读回，三件套
  原样照跑；`benchmarks/bench_vs.py` 也是这样加载 ClickBench `hits` 的。`scan`
  游标在这里用不上：它产出的是原始 Arrow 批，不是表。

---
**上一例**：[09 · RFM 用户分层](09_rfm_segments.md) · **下一例**：[11 · 错误风暴检测](11_error_burst.md)
