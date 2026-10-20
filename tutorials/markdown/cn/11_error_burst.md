<!-- Generated from ../../cn/11_error_burst.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 11 · 错误风暴检测：滚动统计与事件段

> **难度** ★★★ · **领域** 日志/时序监控 · **前置** 案例 03、10
>
> **涉及 API**：`rolling(10, partition_by=)` / `when` / `group_ordered.filter` / `g.all/any/none` 量词

**学习目标**

- 分服务滚动统计：`rolling(n, partition_by="svc")` 一行完成"分组滚动"
- 把"连续同级别日志"折叠成**事件段**，值班看到的是"风暴"而不是孤立告警
- 在组级 `filter` 里用量词 `g.all()/any()/none()` 表达"整段全错 / 至少一条 / 没有错误"

## 问题描述

服务日志 `logs(ts, svc, level, latency_ms)`。目标：

1. 每个服务维护最近 10 条的**滚动错误率**与平均时延；
2. 按 `err_rate` 分级：`≥0.5` critical、`≥0.3` warning、否则 normal；
3. 连续同级别的日志折叠成**事件段**；输出持续 ≥5 条的 critical 段，并检查段内
   是否全部为 ERROR（`g.all` 量词）。

```python
import random

from ltseq import LTSeq, if_else

rng = random.Random(21)
levels = (
    ["INFO"] * 12
    + ["ERROR"] * 10            # 注入风暴：第 13~22 条
    + ["ERROR", "INFO", "ERROR", "INFO", "INFO"]
    + ["INFO"] * 3
)
rows = []
for i, lv in enumerate(levels):
    rows.append({
        "ts":         i * 30,
        "svc":        rng.choice(["api"]),           # 单服务版；多服务见延伸
        "level":      lv,
        "latency_ms": 80 + (i % 7) * 15 + (900 if lv == "ERROR" else 0),
    })

logs = LTSeq.from_rows(rows).sort("ts")
print(len(logs), "条日志")
```

> **预期输出**
>
> ```text
> 30 条日志
> ```

## 第一步 · 分服务滚动统计与分级

两次 `derive` 串联：先造 `is_err` 0/1 列（第二次 derive 要用它），再滚动均值。

```python
from ltseq import when

staged = (
    logs
    .sort("ts")
    .derive(is_err=lambda r: if_else(r.level == "ERROR", 1, 0))
    .derive(lambda r: {
        "err_rate": r.is_err.rolling(10, partition_by="svc").mean().round(2),
        "lat_avg":  r.latency_ms.rolling(10, partition_by="svc").mean().round(0),
    })
    .derive(
        severity=lambda r: when(r.err_rate >= 0.5, "critical")
                            .when(r.err_rate >= 0.3, "warning")
                            .otherwise("normal"),
    )
)
staged.select("ts", "level", "err_rate", "lat_avg", "severity").tail(8).show();
```

> **预期输出**
>
> ```text
> +-----+-------+----------+---------+----------+
> | ts  | level | err_rate | lat_avg | severity |
> +-----+-------+----------+---------+----------+
> | 660 | ERROR | 1        | 1022    | critical |
> | 690 | INFO  | 0.9      | 926     | critical |
> | 720 | ERROR | 0.9      | 931     | critical |
> | 750 | INFO  | 0.8      | 845     | critical |
> | 780 | INFO  | 0.7      | 760     | critical |
> | 810 | INFO  | 0.6      | 674     | critical |
> | 840 | INFO  | 0.5      | 578     | critical |
> | 870 | INFO  | 0.4      | 482     | warning  |
> +-----+-------+----------+---------+----------+
> ```

**解析**

- `rolling(10, partition_by="svc")`：窗口带分组参数，多服务一行搞定（本例单服务，
  参数保留以展示通用写法）；语义仍是部分帧——头部不满 10 条按已有条数算。
- 滚动统计让"风暴"自然显形：ERROR 密集段的 `err_rate` 爬升、风暴过后缓慢回落。
- `when` 链顺序短路，阈值从高到低排列。

## 第二步 · 折叠事件段

又是熟悉的三件套：shift 标记级别翻转 → cum_sum 编号 → group_ordered 折叠。

```python
flagged = (
    staged
    .derive(
        changed=lambda r: if_else(
            r.severity.shift(1).is_null() | (r.severity != r.severity.shift(1)),
            1, 0,
        ),
    )
    .cum_sum("changed")
)

episodes = (
    flagged
    .group_ordered(lambda r: r.changed_cumsum)
    .agg(lambda g: {
        "svc":      g.first().svc,
        "severity": g.first().severity,
        "start_ts": g.first().ts,
        "end_ts":   g.last().ts,
        "n":        g.count(),
    })
)
episodes.show();
```

> **预期输出**
>
> ```text
> +-----+----------+----------+--------+----+
> | svc | severity | start_ts | end_ts | n  |
> +-----+----------+----------+--------+----+
> | api | normal   | 0        | 390    | 14 |
> | api | warning  | 420      | 450    | 2  |
> | api | critical | 480      | 840    | 13 |
> | api | warning  | 870      | 870    | 1  |
> +-----+----------+----------+--------+----+
> ```

## 第三步 · 筛出风暴段：组级 filter + 量词

`NestedTable.filter` 的谓词参数是**组代理 g**：可以比较组级聚合，也可以用
`g.all()/any()/none()` 对组内每行做判断。

```python
runs = flagged.group_ordered(lambda r: r.changed_cumsum)

def summary(g):
    return {
        "severity": g.first().severity,
        "start_ts": g.first().ts,
        "end_ts":   g.last().ts,
        "n":        g.count(),
    }

# 组级聚合：持续 >= 5 条的 critical 段
storms = runs.filter(lambda g: (g.first().severity == "critical") & (g.count() >= 5))
storms.agg(summary).show()

# 量词：对组内每一行检查同一个行谓词
all_error = storms.filter(lambda g: g.all(lambda r: r.level == "ERROR"))
any_slow  = storms.filter(lambda g: g.any(lambda r: r.latency_ms > 900))
no_error  = runs.filter(lambda g: g.none(lambda r: r.level == "ERROR"))
print("全部是 ERROR 的风暴段:", len(all_error.agg(summary)))
print("有一条超过 900 ms 的风暴段:", len(any_slow.agg(summary)))
print("没有 ERROR 的事件段:")
no_error.agg(summary).show();
```

> **预期输出**
>
> ```text
> +----------+----------+--------+----+
> | severity | start_ts | end_ts | n  |
> +----------+----------+--------+----+
> | critical | 480      | 840    | 13 |
> +----------+----------+--------+----+
>
> 全部是 ERROR 的风暴段: 0
> 有一条超过 900 ms 的风暴段: 1
> 没有 ERROR 的事件段:
> +----------+----------+--------+---+
> | severity | start_ts | end_ts | n |
> +----------+----------+--------+---+
> | warning  | 870      | 870    | 1 |
> +----------+----------+--------+---+
> ```

**解析**

- 组级谓词用 `&` 组合（**不能用** Python 的 `and`，会被拒绝并提示改写）。
- `g.all(行谓词)` / `g.any(...)` / `g.none(...)`：把 SQL 里要写两遍的
  `COUNT(CASE WHEN...) = n` 收成一行声明。
- 量词是组级谓词，只能写在 `filter` 里；`agg` 和 `derive` 的组代理没有
  `all/any/none`。想把检查结果作为一列输出，就在 `agg` 里同时聚合
  `g.sum("is_err")` 和 `g.count()`，再在表级 `derive` 里比较两者。
- 没有一段风暴全是 ERROR，因为 `err_rate` 是滞后的滑动均值，分级跟不上原始级别：
  风暴最初的几条 ERROR 落在 `normal` 和 `warning` 段，critical 段一直延续到风暴
  之后的 INFO 行，而 ts 870 的 `warning` 段里一条 ERROR 都没有。
- `filter` 之后的 `agg` 只处理存活段——筛段与折叠在同一个 NestedTable 上接力。

## 关键点解析

1. **监控数据的两级抽象**：行级窗口（滚动统计）回答"现在什么水位"，组级段
   （group_ordered + filter）回答"异常持续了多久"——两级各有算子，各司其职。
2. **rolling 的分组参数**：`partition_by=` 直接挂在 rolling 上，比先 group 再
   window 的两段式更直观。
3. **量词让意图显形**：`all/any/none` 读起来就是需求原文，审计友好。
4. **单服务 → 多服务零成本**：本例把 `partition_by="svc"` 留在原地，多服务数据
   直接换数据源即可（先按 `svc, ts` 排序，`changed` 翻转标记再加上 svc 变化判断，
   见延伸）。

## 延伸练习

- 多服务版：先 `sort("svc", "ts")`，`changed` 条件再加上
  `r.svc != r.svc.shift(1)`，风暴段按服务隔离。
- 时延风暴：对 `latency_ms` 单独做 `rolling(20).std()`，波动突增即告警。
- 与案例 07 组合：`search_pattern_count` 检测 `ERROR → ERROR → FATAL` 序列前奏。

---
**上一例**：[10 · 会话切分](10_sessionization.md) · **下一例**：[12 · 超大文件流式处理](12_streaming_cursor.md)
