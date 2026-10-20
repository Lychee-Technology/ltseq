<!-- Generated from ../../cn/08_user_retention.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 08 · 用户留存：集合运算的日常应用

> **难度** ★★☆ · **领域** 电商用户行为 · **前置** 案例 01
>
> **涉及 API**：`group_by().agg()` / `filter` / `intersect(on=)` / `semi_join` / `anti_join` / `from_rows`

**学习目标**

- 用**集合交运算**表达"留存"：D 日活跃 ∩ D+1 日活跃 = 次日留存
- 掌握 `intersect / semi_join / anti_join` 三兄弟的分工
- 用一个 Python 循环批量算出逐日留存表

## 问题描述

登录表 `logins(user, date)`，共 7 天。目标：

1. 每日活跃用户数（DAU）；
2. **次日留存率**：D 日活跃用户中，D+1 仍活跃的比例，逐日一张表；
3. 对任意指定日，列出留存用户与**流失用户**（活跃过但次日没来）。

```python
from ltseq import LTSeq

daily = {
    "D1": ["u1", "u2", "u3", "u4"],
    "D2": ["u1", "u2", "u5"],
    "D3": ["u2", "u3", "u5", "u6"],
    "D4": ["u1", "u2", "u3"],
    "D5": ["u2", "u4"],
    "D6": ["u1", "u2", "u3", "u5", "u6"],
    "D7": ["u2", "u3", "u7"],
}
rows = [{"user": u, "date": d} for d, users in daily.items() for u in users]
logins = LTSeq.from_rows(rows).sort("date", "user")
print(len(logins), "条登录记录")
```

> **预期输出**
>
> ```text
> 24 条登录记录
> ```

## 第一步 · DAU：分组计数

```python
dau = logins.group_by("date").agg(active=lambda g: g.user.count())
# 聚合结果没有确定的行序，展示前先排序
dau.sort("date").show();
```

> **预期输出**
>
> ```text
> +------+--------+
> | date | active |
> +------+--------+
> | D1   | 4      |
> | D2   | 3      |
> | D3   | 4      |
> | D4   | 3      |
> | D5   | 2      |
> | D6   | 5      |
> | D7   | 3      |
> +------+--------+
> ```

## 第二步 · 次日留存：逐日做交集

留存 = 两天活跃集合的**交**。LTSeq 的 `intersect(other, on=键)` 直接表达：

```python
days = sorted(daily.keys())

retention = []
for d_prev, d_next in zip(days, days[1:]):
    prev_u = logins.filter(lambda r: r.date == d_prev).select("user")
    next_u = logins.filter(lambda r: r.date == d_next).select("user")
    kept   = prev_u.intersect(next_u, on=lambda r: r.user)

    retention.append({
        "date":     d_next,
        "active":   len(prev_u),
        "retained": len(kept),
        "rate":     round(len(kept) / len(prev_u), 3),
    })

LTSeq.from_rows(retention).show();
```

> **预期输出**
>
> ```text
> +------+--------+----------+-------+
> | date | active | retained | rate  |
> +------+--------+----------+-------+
> | D2   | 4      | 2        | 0.5   |
> | D3   | 3      | 2        | 0.667 |
> | D4   | 4      | 2        | 0.5   |
> | D5   | 3      | 1        | 0.333 |
> | D6   | 2      | 1        | 0.5   |
> | D7   | 5      | 2        | 0.4   |
> +------+--------+----------+-------+
> ```

**解析**

- `intersect(other, on=lambda r: r.user)`：按 `on` 键取交集，返回左表形态的行。
  `on` 不传时按全列去重比较。
- 循环里每天两个 filter 都是**独立的惰性计划**，天天全量扫会有放大——日常分析
  量级没问题；超大表建议一次自连接 + 条件聚合（见延伸）。
- `len(kept)` 即 `kept.count()`：LTSeq 实现了 `__len__`。
- D6 的 active 显示的是 D5（前一日）的活跃数 2，rate = 1/2——注意表里 `date`
  指的是**留存观察日**。

## 第三步 · 留存与流失名单：semi_join / anti_join

交集是集合运算（相同的行会合并），名单往往要原样保留左表的每一行。
`semi_join` 返回左表中键存在于右表的行（不重复扩展），`anti_join` 反之——一个是"留下来的"，一个是"流失的"：

```python
d6 = logins.filter(lambda r: r.date == "D6").select("user")
d7 = logins.filter(lambda r: r.date == "D7").select("user")

print("== D6 → D7 留存用户 ==")
d6.semi_join(d7, on=lambda a, b: a.user == b.user).sort("user").show()

print("== D6 → D7 流失用户 ==")
d6.anti_join(d7, on=lambda a, b: a.user == b.user).sort("user").show();
```

> **预期输出**
>
> ```text
> == D6 → D7 留存用户 ==
> +------+
> | user |
> +------+
> | u2   |
> | u3   |
> +------+
>
> == D6 → D7 流失用户 ==
> +------+
> | user |
> +------+
> | u1   |
> | u5   |
> | u6   |
> +------+
> ```

**解析**

- `semi_join` / `anti_join` 只保留**左表列**，且一对多匹配不会让左表行翻倍——
  语义上就是 SQL 的 `WHERE EXISTS / NOT EXISTS`。
- 它们与 `intersect` 的区别：两者都返回**左表的行**，但 `intersect` 是集合运算，
  完全相同的行会合并成一行；semi/anti 保留**左表的每一行**，重复行也保留。本例
  只有一列 user，两者结果一致。做名单类需求用 semi/anti，做比率类需求用 intersect。

## 关键点解析

1. **留存 = 集合交**：把业务问题翻译成集合语言，剩下的交给引擎。
2. **三兄弟分工**：`intersect` 求比率、`semi_join` 取留存名单、`anti_join` 取
   流失名单——记住"集合去重还是逐行过滤"就不会用混。
3. **循环 + 惰性**：Python 循环里每次构建新计划是清晰的路子；性能敏感时改写成
   一次连接 + 聚合，两种风格 LTSeq 都支持。
4. **`sort` 的位置**：本案例排序只为展示稳定（分组聚合和 join 的结果没有确定
   行序，所以展示前再排一次）；集合运算本身不依赖顺序——知道
   哪些算子吃顺序、哪些不吃，是序列思维的一部分。

## 延伸练习

- **7 日留存**：把"次日"换成"7 天后"，提示：先把样例数据扩到 7 天以上，再对
  `date32` 日期列用 `r.date.dt.add(days=7)`，或直接比较日期字符串偏移。
- 新老用户拆分：`当日.anti_join(历史活跃, on=...)` 得当日新增名单。
- 一条计划算全周期留存：`logins` 自连接（`on=lambda a, b: a.user == b.user`）+
  `filter` 日期差为 1 + `group_by` 首日聚合。

---
**上一例**：[07 · 转化漏斗](07_funnel_analysis.md) · **下一例**：[09 · RFM 用户分层](09_rfm_segments.md)
