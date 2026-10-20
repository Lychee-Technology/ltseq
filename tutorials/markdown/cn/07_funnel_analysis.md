<!-- Generated from ../../cn/07_funnel_analysis.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 07 · 转化漏斗：search_pattern 序列匹配

> **难度** ★★★ · **领域** 电商用户行为 · **前置** 案例 01
>
> **涉及 API**：`search_pattern` / `search_pattern_count` / `partition_by` / `sort` / `filter` / `distinct`

**学习目标**

- 用**原生算子**做漏斗：`search_pattern` 直接表达"浏览→加购→下单"的**连续**序列
- 理解"相邻行"语义与 `partition_by` 防串人
- 学会从命中明细到转化率的两步汇总

## 问题描述

事件表 `events(user, ts, event)`，`event ∈ {view, cart, order, pay}`。运营要
"浏览 → 加购 → 下单"三步漏斗的转化人数与转化率。

**关键口径**：LTSeq 的 `search_pattern` 匹配的是**相邻行**——step1 在第 i 行，
step2 必须在第 i+1 行，step3 在第 i+2 行；`partition_by` 保证三步不跨用户。
下面的示例数据特意设计了几种典型形态，帮你看清口径：

```python
from ltseq import LTSeq

plans = {
    "u1": ["view", "cart", "order"],                    # 完整命中
    "u2": ["view", "pay", "cart", "order"],             # 中间插了 pay → 断
    "u3": ["cart", "order"],                            # 缺开头 view
    "u4": ["view", "cart", "view", "cart", "order"],    # 第二轮命中
    "u5": ["view", "order"],                            # 跳过加购
    "u6": ["order", "cart", "view"],                    # 逆序
    "u7": ["view", "view", "cart", "cart", "order"],    # 相邻三步不存在
    "u8": ["view", "cart", "order"],                    # 完整命中
}
rows, ts = [], 1000
for u, seq_ in plans.items():
    uts = ts
    for ev in seq_:
        uts += 10
        rows.append({"user": u, "ts": uts, "event": ev})
    ts += 1000

events = LTSeq.from_rows(rows).sort("user", "ts")     # 先按 (user, ts) 排序
events.select("user", "ts", "event").head(6).show();
```

> **预期输出**
>
> ```text
> +------+------+-------+
> | user | ts   | event |
> +------+------+-------+
> | u1   | 1010 | view  |
> | u1   | 1020 | cart  |
> | u1   | 1030 | order |
> | u2   | 2010 | view  |
> | u2   | 2020 | pay   |
> | u2   | 2030 | cart  |
> +------+------+-------+
> ```

**解析**

- 手造数据让每种口径都看得见：预判一下，`u1 / u4 / u8` 应命中，`u2` 因中间插了
  `pay` 断开——跑完对着结果验证你的理解。

## 第一步 · 三步漏斗匹配

```python
hits = events.search_pattern(
    lambda r: r.event == "view",
    lambda r: r.event == "cart",
    lambda r: r.event == "order",
    partition_by="user",
)
hits.select("user", "ts", "event").show()

n_hits = events.search_pattern_count(
    lambda r: r.event == "view",
    lambda r: r.event == "cart",
    lambda r: r.event == "order",
    partition_by="user",
)
print("三步漏斗命中数:", n_hits)
```

> **预期输出**
>
> ```text
> +------+------+-------+
> | user | ts   | event |
> +------+------+-------+
> | u1   | 1010 | view  |
> | u4   | 4030 | view  |
> | u8   | 8010 | view  |
> +------+------+-------+
>
> 三步漏斗命中数: 3
> ```

**解析**

- `search_pattern` 返回 **step1 命中行**（每处匹配一行）；`search_pattern_count`
  只要计数，只读取各步谓词用到的列，物化开销更小。
- 每一步是一个行级谓词 lambda，可以写任意复杂条件（如 `r.url.s.starts_with("/pay")`）。
- `partition_by="user"`：模式**不跨用户边界**；漏掉它会跨人串线。
- `u4` 的命中是**第二轮**（第 3 行 view 开始）——`search_pattern` 会找出所有起点，
  而不是每个用户只报一次。

## 第二步 · 从命中到转化率

```python
viewed    = events.filter(lambda r: r.event == "view").distinct("user").count()
converted = hits.distinct("user").count()

print(f"触达(view)用户: {viewed}")
print(f"完成三步漏斗用户: {converted}")
print(f"转化率: {converted / viewed:.0%}")
```

> **预期输出**
>
> ```text
> 触达(view)用户: 7
> 完成三步漏斗用户: 3
> 转化率: 43%
> ```

**解析**

- 两步汇总：先 `distinct("user")` 去重再 `count()`；`distinct` 无参时按全列去重。
- 转化率分母的口径（触达用户 vs 全部用户）由业务定，代码只是忠实表达。

## 关键点解析

1. **漏斗是一等公民**：不用自连接三次、不用窗口拼标志位，一个算子说完意图。
2. **相邻行语义要背下来**：中间夹任何未匹配行都会断链。真实埋点里"浏览→广告
   →加购"会被判为断链——需要宽松漏斗时，见延伸练习。
3. **先排序**：模式沿**表当前顺序**匹配，`sort("user", "ts")` 是正确性的前提。
4. **两个出口**：要明细用 `search_pattern`，要数字用 `search_pattern_count`；
   监控大盘场景后者更省。

## 延伸练习

- **宽松漏斗**（允许中间夹杂事件）：给每个用户做三列标志
  `has_view / has_cart_after_view / has_order_after_cart`：先用
  `cum_max().over(partition_by=r.user)` 做逐行累计标志，再用 `group_by + max` 汇总。
- 四步漏斗加 `pay`，对比三步与四步的流失差。
- 把 `search_pattern_count` 用到日志场景：连续三条 `ERROR → ERROR → FATAL` 的
  风暴前奏检测（案例 11 是窗口版实现）。

---
**上一例**：[06 · 最大回撤与复利](06_max_drawdown.md) · **下一例**：[08 · 用户留存](08_user_retention.md)
