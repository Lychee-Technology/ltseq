<!-- Generated from ../../cn/23_interval_merge.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 23 · 重叠区间合并：cum_max 与状态接力

> **难度** ★★★ · **领域** 日志/时序监控 · **前置** 案例 15
>
> **涉及 API**：`sort` / `cum_max(partition_by=)` / `shift(1, partition_by=)` / `if_else` / `is_null` / `cum_sum(partition_by=)` / `s.concat` / `cast("string")` / `group_by().agg()` + `min` / `max`

**学习目标**

- 把"重叠区间合并"拆成有序计算的四步：**接力最大值 → 回看 → 断块标记 →
  分块聚合**
- 认识 `cum_max` 的角色：它不是求最大值，而是在维护"到目前为止的接力棒"
- 理解为什么不需要循环或递归：按 start 排好序后，接力最大值让每一行自己
  就能判断是否开新块

## 问题描述

会议室预约表 `booking(room, start, end)`，分钟数，区间闭开 [start, end)。
同一房间的预约**重叠或相接**时合并为一个占用块。目标：输出每个房间的
合并后占用区间。

```
R1:  [9,11) [10,12) [13,15)   →  [9,12) [13,15)
R2:  [8,10) [9,11)  [12,13)   →  [8,11) [12,13)
R3:  [14,16)                  →  [14,16)
R4:  [8,12) [9,10)  [11,13)   →  [8,13)
```

注意 R2 的第三段 [12,13)：与合并块 [8,11) **不相交**（11 < 12），必须断开。
R4 是反方向的陷阱：[11,13) 与上一段 [9,10) 不相接，却与 [8,12) 重叠，三段
要合成一块。合并的依据是"新起点是否落入**当前接力区间**"，不是只看相邻
两条。

```python
from ltseq import LTSeq

rooms = LTSeq.from_rows([
    {"room": "R1", "start": 9, "end": 11},
    {"room": "R1", "start": 10, "end": 12},
    {"room": "R1", "start": 13, "end": 15},
    {"room": "R2", "start": 8, "end": 10},
    {"room": "R2", "start": 9, "end": 11},
    {"room": "R2", "start": 12, "end": 13},
    {"room": "R3", "start": 14, "end": 16},
    {"room": "R4", "start": 8, "end": 12},
    {"room": "R4", "start": 9, "end": 10},
    {"room": "R4", "start": 11, "end": 13},
]).sort("room", "start")
```

## 第一步 · 接力棒：组内累计最大值

`cum_max(partition_by="room")` 维护"到目前为止见过的最大 end"——这就是
当前合并块的右端点接力棒。第二列再 `shift(1)` 拿到**上一行**的接力棒，
用于判断当前区间是否落块内。

```python
step1 = rooms.derive(
    running_end=lambda r: r.end.cum_max(partition_by="room"),
)
step2 = step1.derive(
    prev_running=lambda r: r.running_end.shift(1, partition_by="room"),
)

step2.show();
```

> **预期输出**
>
> ```text
> +------+-------+-----+-------------+--------------+
> | room | start | end | running_end | prev_running |
> +------+-------+-----+-------------+--------------+
> | R1   | 9     | 11  | 11          | None         |
> | R1   | 10    | 12  | 12          | 11           |
> | R1   | 13    | 15  | 15          | 12           |
> | R2   | 8     | 10  | 10          | None         |
> | R2   | 9     | 11  | 11          | 10           |
> | R2   | 12    | 13  | 13          | 11           |
> | R3   | 14    | 16  | 16          | None         |
> | R4   | 8     | 12  | 12          | None         |
> | R4   | 9     | 10  | 12          | 12           |
> | R4   | 11    | 13  | 13          | 12           |
> +------+-------+-----+-------------+--------------+
> ```

**解析**

- **两列分两个 derive**：`prev_running` 引用了 `running_end`——同一 derive
  内新列不能互相引用（硬规则），拆开即可，语义不变。
- R4 的 9 行：running_end=12 而**本行 end=10**——接力棒仍带着 [8,12) 的
  右端点，所以下一行看到的 prev_running 是 12，而不是上一行的 end 10。
  R1–R3 的 end 逐行递增，接力棒恰好等于本行 end。
- 每个房间的首行 prev_running 为 NULL——partition_by 的组边界 NULL，照例
  当作"新块"处理。

## 第二步 · 断块标记与编号

新块的条件：**上一行接力棒落在当前起点之前**（不相交），或本行是组首。
标记 0/1 后组内 `cum_sum` 编号——案例 03、15 的"标记 → 编号 → 分组"
三件套再次登场。

```python
from ltseq import if_else

step3 = step2.derive(
    new_block=lambda r: if_else(
        r.prev_running.is_null() | (r.prev_running < r.start), 1, 0
    ),
).derive(
    block=lambda r: r.new_block.cum_sum(partition_by="room"),
)

step3.show();
```

> **预期输出**
>
> ```text
> +------+-------+-----+-------------+--------------+-----------+-------+
> | room | start | end | running_end | prev_running | new_block | block |
> +------+-------+-----+-------------+--------------+-----------+-------+
> | R1   | 9     | 11  | 11          | None         | 1         | 1     |
> | R1   | 10    | 12  | 12          | 11           | 0         | 1     |
> | R1   | 13    | 15  | 15          | 12           | 1         | 2     |
> | R2   | 8     | 10  | 10          | None         | 1         | 1     |
> | R2   | 9     | 11  | 11          | 10           | 0         | 1     |
> | R2   | 12    | 13  | 13          | 11           | 1         | 2     |
> | R3   | 14    | 16  | 16          | None         | 1         | 1     |
> | R4   | 8     | 12  | 12          | None         | 1         | 1     |
> | R4   | 9     | 10  | 12          | 12           | 0         | 1     |
> | R4   | 11    | 13  | 13          | 12           | 0         | 1     |
> +------+-------+-----+-------------+--------------+-----------+-------+
> ```

**解析**

- `r.prev_running < r.start`：R1 的 13 行 prev=12 < 13 → 断块；R2 的 9 行
  prev=10 >= 9 → 续块。**相接也算合并**：起点恰好等于接力棒终点
  （prev_running == start）时 `<` 不成立，归入当前块——业务上"下一段紧跟着
  上一段"通常不拆；若要相接也拆开，条件改成 `<=`。
- 关键在于比较对象是 **prev_running（上一行的接力棒）** 而不是上一行的
  end：R4 的 11 行 prev_running=12 >= 11 → 续块；若只看上一行的 end（10），
  就会把与 [8,12) 重叠的 [11,13) 误拆出去。
- `block` 是房间内块号，配 `room` 才全局唯一——下一步拼复合键。

## 第三步 · 分块聚合：起点取首行，终点取最大 end

每块的 start = 块内首行起点，也就是最小的 start（行按 start 排序）；
end = 块内最大的 end（接力棒终值）。复合键分组收口——与案例 15/18 同款。

```python
keyed = step3.derive(
    block_key=lambda r: r.room.s.concat("#", r.block.cast("string")),
)

merged = (
    keyed
    .group_by("block_key")
    .agg(
        room=lambda g: g.room.min(),     # 每块只有一个房间，min() 只是把它带出来
        start=lambda g: g.start.min(),   # 按 start 排序：即首行起点
        end=lambda g: g.end.max(),
    )
    .sort("room", "start")
)

merged.show();
```

> **预期输出**
>
> ```text
> +-----------+------+-------+-----+
> | block_key | room | start | end |
> +-----------+------+-------+-----+
> | R1#1      | R1   | 9     | 12  |
> | R1#2      | R1   | 13    | 15  |
> | R2#1      | R2   | 8     | 11  |
> | R2#2      | R2   | 12    | 13  |
> | R3#1      | R3   | 14    | 16  |
> | R4#1      | R4   | 8     | 13  |
> +-----------+------+-------+-----+
> ```

与题面预判完全一致。✔

**解析**

- `g.end.max()` 与接力棒终值等价（块内最后一行的 running_end 就是最大
  end）——用原始列聚合更直白，也少一个对中间列的依赖。在 `group_by().agg()`
  里，`g.列名` 指向一列，聚合挂在列上；`g.first()`/`g.last()` 这类取行写法
  属于 `group_ordered`/`group_sorted` 分组。
- 十行进、六行出：R1 的前两段与 R2 的前两段各自合并，R3 原样保留，R4 的
  三段合成一块——合并结果集**不需要**再去重，分组天然给出互不重叠的块。
- **为什么不用 fold**：本题的"状态"（当前块终点）可以被 cum_max 这种
  可下推的窗口算子表达，fold 的逐行 Python 回调就是杀鸡用牛刀——先找
  窗口表达，fold 才是兜底（与案例 06/25 的取舍一致）。

## 关键点解析

- **接力棒模式**：`cum_max`/`cum_min` + `shift(1)` 是"与历史最好成绩比较"
  的通用形态——区间合并只是它的一个应用，库存高水位、峰值检测同源。
- **标记-编号-分组再一课**：15 的标记看"上一行业务事件"，23 的标记看
  "上一行的累计状态"——变量是条件，骨架纹丝不动。
- **分两个 derive 是规矩不是偏好**：新列互相引用必须拆步；好在每一步都
  能 `show()` 验证，拆步反而利于调试。
- **不需要递归**：按 start 排好序后，接力最大值让每一行的合并判断只依赖
  它之前的行，几个普通窗口函数加一次聚合就能完成——不需要循环，也不需要
  递归查询。

## 延伸练习

1. "相接也拆开"版本：把条件改成 `r.prev_running <= r.start`，R1 的
   [9,11) [10,12) 还合并吗？构造一组首尾相接的区间验证边界。
2. 用 `fold` 重写：状态为当前块终点（新块时更新为本行 end，块内取
   max），对比两版的行数与可读性。
3. 计算每房间**总占用时长**：对 merged 再来一次 `group_by("room").agg()`，
   注意 end - start 是行级算术，先 derive 再聚合。
4. 区间覆盖检测：给定一个查询窗口 [a, b)，判断每个房间的合并块是否
   **完全覆盖**它（提示：semi_join 或 group 量词，想想哪种更直白）。

---
**上一例**：[22 · 余额快照](22_balance_snapshot.md) · **下一例**：[24 · 进出事件配对](24_pair_events.md)
