<!-- Generated from ../../cn/22_balance_snapshot.ipynb by tutorials/render_markdown.py. Edit the notebook, not this file. -->

# 22 · 余额快照：分组累计与每组最后一行

> **难度** ★★☆ · **领域** 电商 · **前置** 案例 01、14
>
> **涉及 API**：`sort` / `cum_sum(partition_by=)`（表达式形式）/ `group_sorted` / `NestedTable.last` / `NestedTable.filter` + `g.any` 量词

**学习目标**

- 用**分区累计**给每个账户维护流水余额——`shift` 的兄弟算子 `cum_sum` 的
  表达式形态
- 用 `group_sorted(...).last()` 一次性取"每组最后一行"——最新快照的标配
- 用 `g.any()` 组级量词实现"存在透支的账户"这类**组级存在判断**

## 问题描述

钱包流水 `ledger(account, seq, delta)`，delta 为充值/消费金额。目标：

1. 给每笔流水算出**该笔完成后的余额**（组内累计）；
2. 取每个账户的**最新快照**（最后一行）——账户余额表；
3. 找出**曾经透支**（余额出现过负数）的账户的当前快照。

```python
from ltseq import LTSeq

ledger = LTSeq.from_rows([
    {"account": "A1", "seq": 1, "delta": 100},
    {"account": "A1", "seq": 2, "delta": -30},
    {"account": "A1", "seq": 3, "delta": 20},
    {"account": "A1", "seq": 4, "delta": -200},
    {"account": "A2", "seq": 1, "delta": 50},
    {"account": "A2", "seq": 2, "delta": 50},
    {"account": "A2", "seq": 3, "delta": -20},
    {"account": "A3", "seq": 1, "delta": 10},
    {"account": "A3", "seq": 2, "delta": -5},
]).sort("account", "seq")

ledger.show();
```

> **预期输出**
>
> ```text
> +---------+-----+-------+
> | account | seq | delta |
> +---------+-----+-------+
> | A1      | 1   | 100   |
> | A1      | 2   | -30   |
> | A1      | 3   | 20    |
> | A1      | 4   | -200  |
> | A2      | 1   | 50    |
> | A2      | 2   | 50    |
> | A2      | 3   | -20   |
> | A3      | 1   | 10    |
> | A3      | 2   | -5    |
> +---------+-----+-------+
> ```

预判：A1 余额轨迹 100 → 70 → 90 → **-110**（透支）；A2 → 80；A3 → 5。

## 第一步 · 分区累计：每账户独立的余额轨迹

`r.delta.cum_sum(partition_by="account")` ——表达式形式的累计求和，窗口在
账户边界重置。与案例 15 的 `cum_sum` 同款，只是这次累加的是带符号金额。

```python
balances = ledger.derive(
    balance=lambda r: r.delta.cum_sum(partition_by="account"),
)

balances.show();
```

> **预期输出**
>
> ```text
> +---------+-----+-------+---------+
> | account | seq | delta | balance |
> +---------+-----+-------+---------+
> | A1      | 1   | 100   | 100     |
> | A1      | 2   | -30   | 70      |
> | A1      | 3   | 20    | 90      |
> | A1      | 4   | -200  | -110    |
> | A2      | 1   | 50    | 50      |
> | A2      | 2   | 50    | 100     |
> | A2      | 3   | -20   | 80      |
> | A3      | 1   | 10    | 10      |
> | A3      | 2   | -5    | 5       |
> +---------+-----+-------+---------+
> ```

**解析**

- `partition_by` 让累计在**每个账户的第一行从零开始**——A2 的第一个 50 累计
  成 50 而不是接着 A1 的 -110。对照：不带 partition_by 的全表 `cum_sum`
  会把所有账户串成一条线，余额表直接作废。
- 排序键 `(account, seq)` 让每个账户的流水按流水号递增——这是累计需要的；
  同时让同账户行相邻——这是第二步 `group_sorted` 需要的（案例 14/15/18 也是
  这样排序）。
- 负余额 -110 会被直接写出：LTSeq 不会替你"截断"业务异常，识别它是下一
  步的事。

## 第二步 · 最新快照：group_sorted().last()

每组最后一行 = 该账户的当前余额。`group_sorted` 单趟分组后 `last()` 取组
尾行——比 `group_by` 聚合多保留**整行的所有列**。

```python
snapshot = (
    balances
    .sort("account", "seq")
    .group_sorted(lambda r: r.account)
    .last()
)

snapshot.show();
```

> **预期输出**
>
> ```text
> +---------+-----+-------+---------+
> | account | seq | delta | balance |
> +---------+-----+-------+---------+
> | A1      | 4   | -200  | -110    |
> | A2      | 3   | -20   | 80      |
> | A3      | 2   | -5    | 5       |
> +---------+-----+-------+---------+
> ```

**解析**

- `group_sorted(key)` 要求分组键是**声明排序的首键**：`sort("account",
  "seq")` 首键恰是 account，满足约束。它是单趟扫描的有序分组，无哈希
  开销。
- `last()` 返回普通 LTSeq——组尾行的**全部原始列 + 派生列**（含 balance）。
  聚合只留数值：`agg(balance=lambda g: g.balance.max())` 每个账户只剩一个
  数，丢了 seq 与 delta 的上下文——而且这个数是历史最高余额（A1 为 100），
  不是最新余额（-110）。"取行"与"取值"在此分野。
- 案例 16 在 `NestedTable.agg` 里读取段首/段尾行的值（`g.first().date`、
  `g.last().date`）；`NestedTable.first()`/`last()` 则把这些行整行返回——
  "每组最新/最早一条"成为一行代码。

## 第三步 · 曾经透支：组级量词 g.any

"存在透支"是**组级存在判断**：组内任意一行 balance < 0。`NestedTable.filter`
配 `g.any(行级谓词)` 正为此而生。

```python
risk = (
    balances
    .sort("account", "seq")
    .group_sorted(lambda r: r.account)
    .filter(lambda g: g.any(lambda r: r.balance < 0))
    .last()
)

risk.show();
```

> **预期输出**
>
> ```text
> +---------+-----+-------+---------+
> | account | seq | delta | balance |
> +---------+-----+-------+---------+
> | A1      | 4   | -200  | -110    |
> +---------+-----+-------+---------+
> ```

**解析**

- `g.any(lambda r: r.balance < 0)`：行级谓词交给组级量词——三个量词
  `g.all() / g.any() / g.none()` 对应 SQL 的 EVERY / ANY / NOT ANY。
- filter 之后的 NestedTable 仍然是 NestedTable，可以继续 `.last()`——
  "筛组"与"取行"两个动作串联，各自语义不变。
- 注意判断的是**曾经**透支：A1 当前 -110 也透支，两个口径此例重合；若
  A1 后续充值转正，`g.any` 仍会抓住它——这正是风控要的"历史污点"视角，
  当前口径请用 `snapshot.filter(lambda r: r.balance < 0)`。

## 关键点解析

- **cum_sum 的两种形态选型**：要自定义列名或分区 → 表达式形式
  `r.col.cum_sum(partition_by=)`；只是顺手加列 → 表级 `t.cum_sum("col")`。
  涉及分组的一律表达式形式。
- **取行 vs 取值的分野**：`NestedTable.last()` 保留整行（含上下文），
  `agg` 只留聚合值。快照类需求（最新一条、最早一条）属于前者。
- **组级量词是"存在性"问题的开关**："有透支行"（any）、"全部成功"（all）、
  "无失败"（none）——把 SQL 的 EXISTS 子查询翻译成一个谓词。

## 延伸练习

1. 给 `balances` 加一列 `is_overdraft`（balance < 0），再用
   `g.all(lambda r: r.is_overdraft == True)` 找出"全程透支"的账户（本题没有，
   构造一条流水让它出现）。
2. 每账户**最大回撤**式指标：最低余额 `g.balance.min()` 与最新余额在一张
   表会合（提示：`group_by("account").agg()` 出 min，再按 account 与 snapshot
   join）。
3. 把 seq 换成时间戳列并保证有序，验证结论不变——快照逻辑与"序号"还是
   "时间"无关，只与**声明顺序**有关。
4. 不用 `group_sorted` 重做快照：
   `group_by("account").agg(last_seq=lambda g: g.seq.max())`，再用
   `left_on=["account", "seq"], right_on=["account", "last_seq"]` 与
   `balances` join 回来。对比两条路线：哪条多了一次 join，为什么？

---
**上一例**：[21 · 购物篮同现对](21_basket_pairs.md) · **下一例**：[23 · 重叠区间合并](23_interval_merge.md)
