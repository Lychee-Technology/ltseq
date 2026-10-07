# ADR 0018：字面量精确性：事实、策略与唯一的类型权威

- 状态：Accepted
- 决策日期：2026-10-06（D-a–D-h）与 2026-10-07（D-i–D-m），PR #225 · 记录日期：2026-10-07

[English version](0018-literal-exactness-facts-and-policy.md)

## 背景

#145 之前，lambda 里的 Python 值以字符串形式进入内核再被解析回来，于是 `Decimal("1.236")` 丢失位数，`2**53 + 1` 变成浮点数，日期按 DataFusion 从文本推断出的任何类型理解。PR #225 让字面量在捕获时就有类型。它最初的设计会预测字面量将遇到的类型，这不断产生组合性 bug：两种列类型的 CASE、`coalesce` 或窗口参数把字面量带到预测没见过的类型前。2026-10-06 的设计评审和决策记录（D-a 到 D-h）围绕一条规则重建了流水线：DataFusion 自己的逐节点类型强制是类型的唯一来源（`transpiler::Resolver`），LTSeq 只保留一张"字面量遇到某类型时如何理解"的表，以及对 DataFusion 结果是否精确的检查。

到 `35c07e8` 时仍然有错的正是这个精确性检查。一个模块同时回答"类型 T 是否容纳类型 S 的每个值"和"LTSeq 究竟判断哪些种类"，而它用第一个问题的词汇回答第二个问题。`cast_loss(_, floating) = Loss::None` 的本意是"浮点结果归 DataFusion 管"，读起来却是"Int64 → Float64 什么都不丢"；默认分支 `_ => Loss::None` 的本意是"其他组合不归我们管"，读起来却是"Date32 → Timestamp(µs) 和 Int64 → Utf8 无损"；`exact_cast(number, float) = Exact` 的本意是"数字遇到浮点数就是最接近的浮点数"，读起来却是"0.1 是一个 double"。取值门把这些当事实消费，于是 Int64 列上的 `coalesce(r.x, 0.0)` 变成 Float64，并把 `2**53 + 1` 存成 `9007199254740992.0`，而这正是 D-b 禁止的。对 `35c07e8` 的架构评审把之前五轮评审的精确性发现（其中三轮落在该模块）和它自己的新发现追溯到同一处混淆，建议重构这一层而不是给浮点分支打补丁。评审无法独自裁定的五个问题由 owner 于 2026-10-07 决定（D-i 到 D-m）。

## 决策

**事实与策略放在不同模块，且没有任何事实默认为"精确"。**

`src/transpiler/exact.rs` 只陈述关于 Arrow 类型和值的事实，不含策略。`cast_class(from, to)` 把一次转换归为 `Exact`、`RangeOnly`、`Precision`、`RangeAndPrecision`、`Kind` 或 `Unjudged`；`holds(to, value)` 回答某类型是否精确容纳某值，返回该类型下的值，或者它落在哪里（`Between` 该类型的两个值之间、`Beyond` 其范围之外、`NotANumber`），或者 `Unjudged`。两者都是全函数：每个分支都写出来，模块没有判断过的组合是 `Unjudged`，任何调用方都不得把 `Unjudged` 或 `Kind` 读作精确。这些是可表示性的事实，而不是 DataFusion 的转换行为：整数类型的量级放得进尾数（11、24 或 53 位）时才被浮点类型容纳，小数按位数和小数位判断，浮点数按其精确的二进制值判断（一个精确的有理数，所以 `0.1` 是 0.1000000000000000055511151231257827…），时刻按更细单位下的刻度判断，Date64 既按天也按毫秒判断。

`src/transpiler/literal_policy.rs` 是唯一决定 LTSeq 判断什么、把什么交给 DataFusion 的地方。`interpret` 读取字面量遇到某类型时的含义（`Decimal` 遇到浮点数就是该浮点数，不带时区的 `datetime` 遇到带时区的列是该时区的墙上时间，字符串沿用 DataFusion 的理解）；`exact_domain` 列出 LTSeq 亲自放置其值的类型（整数、小数、日期、时间戳）；`held` 和 `fit` 实施 D-b 与 D-i；`widening` 实施 D-m。`src/transpiler/literals.rs` 中的各道门（比较、`is_in`、`fill_null`/`coalesce`/`if_else`/`when` 的共享取值、`shift(default=)`、`dt.diff`、算术）只咨询策略，从不直接调用 `exact.rs`，因此新增一种字面量或一种上下文类型只是一条策略条目，而不是每道门里的一个新分支。

策略编码的规则：

| 决策 | 规则 |
|---|---|
| D-a | 表达式自底向上解析，DataFusion 的类型强制是类型的唯一来源。LTSeq 不计算公共类型。 |
| D-b、D-i | 共享一个结果列的取值：当每个已有取值的转换在类型层面精确、每个字面量在该类型下在值层面精确时，采用 DataFusion 的统一类型。否则，不含字面量的上下文类型能精确容纳的字面量采用该类型；仍不精确的在计划期抛出 `ValueError` 并给出该字面量。浮点数不是例外：Int64 列上的 `r.i.fill_null(0.0)` 保持 Int64，`fill_null(1.5)` 抛出错误；在 Float64 能容纳的 `int32` 列上两者都是 Float64。 |
| D-c | `shift(default=)` 要么精确要么报错，绝不扩宽列。 |
| D-d | 在 `dt.diff` 和减法中，不带时区的字面量遇到带时区的列是该列时区的墙上时间；`date` 是本地午夜。 |
| D-e | 整数和 `Decimal` 字面量在比较和 `is_in` 中精确放置，超过 38 位也是如此（#227）。 |
| D-f | 常量由 DataFusion 的简化器按它的类型折叠（#193、#209）。 |
| D-h、D-k | 计划期的字面量错误是 `ValueError`。字符串，以及遇到字符串列的布尔值和数字，沿用 DataFusion 的理解；改变种类的转换是 `Kind` 或 `Unjudged`，绝不是精确。 |
| D-j | Python `float` 表示其 binary64 值。遇到整数列或小数列时精确比较（`r.x == 2.0**53` 只匹配 `2**53`；`r.p == 0.1` 在任何小数列上都为假）。遇到浮点列时沿用 DataFusion 的浮点语义。NaN 和无穷大遇到整数、小数、日期或时间戳列时在计划期抛出 `ValueError`。 |
| D-l | 数字或布尔值遇到日期或时间戳操作数时，在任何位置都在计划期抛出 `ValueError`；`dt.add` 用于加时长。 |
| D-m | 仅在共享取值中，比列更细的时间戳字面量扩宽单位并保持时区（只有范围风险）。日期列绝不变成时间戳，时区绝不被重新标记，`shift(default=)` 绝不扩宽。 |

**字面量网格是合并门。** `py-ltseq/tests/literal_grid/` 记录每个（上下文，字面量，位置）单元：32 个上下文（每种数值、小数、日期、时间戳、字符串和布尔列，一个字典编码的 Int64，以及六个混合类型的 CASE 表达式）、49 个字面量、14 个位置，共 20,874 个单元。基线是 `68d6114` 处的 `main`，作为 `expected/main.jsonl` 提交，绝不由测试重新生成。当前构建改变的每个单元都必须匹配一条给出决策（INTENDED_CHANGE）或 issue（PREEXISTING_BUG）的分类规则，或者是独立 oracle 确认的 BUG_FIXED 单元；REGRESSION 和 UNDECIDED 使测试套件失败。设计所依赖的不变量以生成的属性而非例子来测试：报告的类型就是 DataFusion 的类型；字面量内联、在分阶段的惰性计划中、在物化的表上读法相同；CASE 分支、`coalesce` 顺序和 n 元组合一致；`L < x` 与 `x > L` 互为镜像；`x.is_in([L])` 就是 `x == L`；上下文能容纳的数在每种写法下含义相同；数字遇到时间列时在每个位置都被拒绝；线性扫描内核的计数与物化计数一致，除钉在 #189 和 #244 上的单元之外。

## 备选方案

- *只给浮点分支打补丁（仅 c27 F1）。* 去掉一条错误事实，留下默认分支和数字到浮点的分支；下一种字面量或上下文类型会重新打开同一类 bug。评审和 owner 都拒绝。
- *浮点域例外（U1 选项 b）：取值位置的浮点数按 DataFusion 的读法理解。* 保留 `main` 对 `coalesce(r.i64, 0.0)` 的 Float64 结果，并记录 D-b 对浮点数不成立。拒绝：它在进入公共类型的路上悄悄取整超过 `2**53` 的 Int64 值，而这正是 D-b 要阻止的缺陷。
- *浮点字面量表示其 `repr` 十进制数（`0.1` 读作 `Decimal("0.1")`）。* 遇到小数列时更友好，但它让字面量的含义取决于 Python 最短往返打印，并且与同一字面量遇到浮点列时的含义不同。拒绝；想要小数的用户写 `Decimal`。
- *与位置相关的时间规则：取值中拒绝数字，比较中保留 DataFusion 的纪元读法。* 为位置一致性而拒绝（D-l）；从未有人要求 `r.d > 5` 读作"在 1970-01-06 之后"。
- *在共享取值中把 Date32 → Timestamp 当作扩宽接受。* 这是种类变化而不是范围变化：pyarrow 在第 109,000,000 天溢出。拒绝（D-m）。
- *在门里为 `Int64 -> Float64` 加特例。* 门里的特例正是产生这次混淆的东西。拒绝。

## 影响

- 相对 `main` 的行为变化全部列在 changelog 中：Int64、UInt64 或窄小数上下文的取值位置中的非整浮点数抛出错误而不是扩宽列；整数值的浮点数保持上下文类型而不是 Float64；NaN 和无穷大遇到精确列时抛出错误而不是比较为假或扩宽；数字遇到日期或时间戳时在每个位置都抛出错误而不是从纪元起算；浮点字面量与整数列和小数列精确比较（#240）。网格中 9,977 个单元不变，9,949 个按决策改变，947 个是修复的 bug，1 个是既有 bug（#245）；没有单元回归或未裁定。
- 每道门先向 DataFusion 要统一类型，再对它分类。代价在计划期按字面量付一次；执行计划不变。
- 改变类型强制规则的 DataFusion 升级会改变提出的统一类型。网格把每个受影响的单元报告为 REGRESSION 或 UNDECIDED，由升级者逐个分类；不会为了让套件通过而重新生成基线。
- 按决策留给 DataFusion 的部分：浮点上下文（`r.f32 == 2**24 + 1` 匹配 `2**24`，因为整数被强制为 float32，而 `2.0**24 + 1` 按 float64 比较）、字符串和布尔值（D-h），以及列表超过三项时按位比较浮点数的 `IN` 列表哈希集（#245）。
- 已知的内核分歧被钉住而不是隐藏：线性扫描内核对精确整数列上 `>` 的 NULL 语义（#189）及其按位的 Float64 比较（#244）。修复或新的分歧都会使一致性测试失败。
- 仍然开放且不在本次范围内：#228（浮点列遇到小数列被转换为 `decimal(30, 15)`）、#241（decimal32/64 遇到整数列）、#242（负小数位的类型强制在 debug 和 release 构建间不同）。
- 测试套件增加约四分钟：网格针对当前构建实时运行其 20,874 个单元。

## 来源

- PR #225：[设计评审](https://github.com/Lychee-Technology/ltseq/pull/225#issuecomment-6007628376)、[决策记录 D-a–D-h](https://github.com/Lychee-Technology/ltseq/pull/225#issuecomment-6008650391)、[对 `35c07e8` 的架构评审](https://github.com/Lychee-Technology/ltseq/pull/225#issuecomment-6032928492)、[决策记录 D-i–D-m](https://github.com/Lychee-Technology/ltseq/pull/225#issuecomment-6039847840)
- Issue #145、#240、#243（网格）、#189、#244、#245
- `src/transpiler/exact.rs`、`src/transpiler/literal_policy.rs`、`src/transpiler/literals.rs`、`src/transpiler/resolve.rs`
- `py-ltseq/tests/literal_grid/`、`docs/api.md` § "Literal values"
