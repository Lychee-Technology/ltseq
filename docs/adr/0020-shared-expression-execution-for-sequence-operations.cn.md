# ADR 0020：序列操作共享表达式执行

- 状态：已采纳（Accepted；实现由 #188 跟踪，尚未完成）
- 决策日期：2026-11-28，#188 上的 owner 决策 D3（路线图 #281 第 53 步）· 记录日期：2026-11-28
- 修订：[ADR 0006](0006-multi-path-execution-strategy.cn.md)

[English](0020-shared-expression-execution-for-sequence-operations.md)

## 背景

`search_pattern` 与 `search_pattern_count` 用一套自己的 `PyExpr` 解释器求值步骤谓词（`src/ops/pattern_match.rs`：`eval_predicate`、`eval_expr`、`eval_binop`、`eval_call`）。排序 Parquet 计数（`src/ops/parallel_scan.rs`）调用同一个解释器。`filter`、`derive`、`search_first` 则走转译器（`src/transpiler/`）：转译器用 DataFusion 的类型强制解析类型（[ADR 0018](0018-literal-exactness-facts-and-policy.cn.md)），再把表达式交给 DataFusion。

每条语义规则都不得不写两遍，第二份总是落后。步骤解释器曾缺少类型强制（#188），`&` 与 `|` 不是 Kleene 逻辑（#220），整数 `/` 截断（#218），按位模式比较编码浮点（#292），而且只支持五个函数（`starts_with`、`ends_with`、`contains`、`is_null`、`is_not_null`），`filter` 却支持整个 DSL。每个缺口都是单独发现、单独修复的。有些修复已经把工作交回转译器：自 PR #377 起，带字面量操作数的比较在每个批次上都被转译并编译成 DataFusion 物理表达式（`eval_lowered_binop`）。两份实现仍有分歧。在 `ca59086` 上，`x = 2**62` 时 `r.x * 4 > 0` 在 `search_pattern` 中抛错（Arrow 的检查型 kernel），在 `filter` 中静默回绕（DataFusion 的 `BinaryExpr`）。

#188 列出两个方案：在解释器内做类型强制（方案 1，自 PR #377 起生效），或把步骤作为 DataFusion 表达式求值（方案 2）。

## 决策

Owner 选择方案 2，并把它定为一条通用规则：

> 当同样的语义可以通过共享的表达式解析和 DataFusion 执行提供时，LTSeq 不得为序列操作维护独立的生产表达式求值器。专用序列内核必须共享语义权威，否则拒绝并转到通用参考路径。

1. **LTSeq 拥有语义，DataFusion 负责执行。** Python DSL、其序列化后的 `PyExpr`、类型解析与精确类型强制、字面量策略、检查型算术、Python 除法、NULL 与 NaN 规则、某个上下文接受哪些函数、按行需求、异常类别、声明顺序以及序列匹配本身，都归 LTSeq。它们只在共享 lowering（`src/transpiler/` 及其调用的 UDF 与 kernel）中写一次。DataFusion 的逻辑与物理表达式执行该 lowering 的产物。两者不一致时，DataFusion 的默认行为不是契约。
2. **步骤只编译一次。** `search_pattern` 的步骤由转译器 lowering，并在操作的规划边界上针对已执行计划的 schema 编译成一个物理表达式。编译后的表达式在每个批次上求值。不为求值谓词而逐行或逐批次构造 DataFrame 或查询计划。
3. **步骤只在被需求处求值。** 只有在前面各步都为 TRUE 的起点上才求值第 *k* 步（契约 §10.6、§20.2）。匹配器通过行选择或掩码包裹编译后的表达式来实现。单个步骤内部的需求（`&`、`|`、`when`、`coalesce`）来自共享 lowering（M16，#252），而不是模式专用的代码。
4. **解释器被删除，而不是保留。** 共享路径一旦通过 #188 的门槛，步骤解释器及只为它存在的代码即被删除。它不作为不支持表达式的 fallback 保留，不藏在 feature flag 或运行时开关后，不作为已弃用的副本，也不作为生产环境的正确性参考。共享路径无法求值的表达式以有类型的错误拒绝，绝不改用别的方式解释。
5. **专用内核要么共享权威，要么拒绝。** 序列内核（排序 Parquet 计数、前缀循环）只有同时满足以下条件才能保留：有已证明的序列特有价值；谓词经共享 lowering 求值；资格规则明确且有测试；结果与错误和通用路径一致（契约 §21.2）。对它处理不了的输入，它在报告任何结果之前返回有类型的拒绝（#152 的 `Miss::Unsupported`）。被需求的行上的错误就是错误，不是拒绝。做不到这些的内核被删除或收窄，不保留自己的解释器。
6. **上下文仍决定什么合法。** 步骤是 `Row` 谓词（契约 §8.1），接受 `filter` 所接受的内容。共享执行并不让聚合、DSL 之外的 DataFusion 函数或任何 DataFusion 能运行的东西在步骤中变得合法。§10.6 允许步骤中使用窗口方法，这需要经共享窗口 lowering 分段计算，属于 M11 的工作；在其落地之前，它们仍像现在一样被拒绝。
7. **测试 oracle 不是求值器。** 测试中的暴力模式参考与独立算术模型（契约 W8、P6）保留。它们不是生产路径。

## 备选方案

- **方案 1：在解释器内做类型强制。** 这是 PR #377 以来的现状。它在发现缺口的地方修复缺口，但以后的每条规则（检查型算术、Python `%`、精确十进制比较、新函数）都要再写一遍。因此不采用。
- **把 #234 的 DataFusion 窗口 lowering 作为生产通用路径。** #234 设想的 lowering（对每个步骤的掩码做 `LEAD`）在每一行上求值每一步，因此除非每一步都被前面的步骤掩住，它会为契约不需求的行抛错。它继续作为参考路径。以后是否用它替代匹配器，是另一个决策。

## 影响

- 对运算符或函数的语义修改，一次改动即作用于所有接受它的操作，包括序列操作。
- 删除解释器要等共享 lowering 涵盖解释器今天做的全部事情。已知的缺口是溢出：在 M5（#221）之前，解释器抛错而共享 lowering 回绕。先切换会把一个错误变成静默的错误值。解释器的其他规则（经 `binary_input_types` 的类型强制、经共享 kernel 的真除法与整除）已经来自共享代码。
- 现在的解释器不满足需求规则：`search_pattern` 在每一行上计算每一步的掩码，所以没有任何起点需求的行上的错误也会抛出。按需求求值是新工作，不是迁移中"保留"下来的东西。
- `group_ordered` 的线性扫描求值器（`src/ops/linear_scan.rs`）是另一个独立求值器。D3 的删除要求只覆盖 `search_pattern`。上述不变量适用于线性扫描，但其删除或收窄属于 M5 与 M17 的工作（#221，路线图第 85 步），这里没有记录关于它的决策。
- 性能尚未测量。#188 在删除解释器之前记录当前实现的基线。性能回退是优化共享路径或上报 owner 的理由，不是保留解释器的理由。

## 来源

- #188（决策记录与实现计划）、#234（参考 lowering）、#252（M16）、#221（M5）、#218（M6）、#372（分区语义）、#281（路线图）
- 契约：`docs/proposals/v0.5/contract/expressions.md` §8.1、`windows-and-grouping.md` §10.6、`errors-and-performance.md` §20.2 与 §21.2、`acceptance.md` W8 与 P6
- `ca59086` 上的代码：`src/ops/pattern_match.rs`、`src/ops/parallel_scan.rs`、`src/transpiler/mod.rs`（`pyexpr_to_datafusion`）
