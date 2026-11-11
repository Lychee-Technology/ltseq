# ADR 0019：流水线编译

- 状态：Proposed（#232 流水线 IR 合并时改为 Accepted）
- 决策日期：由 #229 与 #238 提出，尚未接受 · 记录日期：2026-11-11

[English version](0019-pipeline-compilation.md)

## 背景

今天 LTSeq 的每个操作在调用时各自构建 DataFusion 计划：`filter`、`derive`、`sort` 等把捕获的表达式转成 DataFusion `Expr`，追加到表的 `DataFrame` 上，再交给 DataFusion 的优化器（[ADR 0006](0006-multi-path-execution-strategy.md)）。专用序列路径（例如 `group_ordered().first().count()` 的线性扫描内核、排序 Parquet 的模式计数）逐个判断某次调用是否适用。每条路径只看自己那一次调用，没有任何环节看到整条操作链，所以依赖整条链的决策无从做出，例如合并相邻步骤、为几个操作选同一个内核、说明内核为何没有被采用。EPIC #229 提出用整条流水线的编译器来做这些决策，分为若干工作流：B（IR，#232）、C（#233）、D（#236）、E1（#235）与 F（#237）。

本 ADR 记录 #229 与 #238 定下的设计，使各工作流依据同一份表述来构建。它写于 IR 存在之前。备选方案中的理由是本记录依据现有 ADR 的解读，因为 issue 只列出了被否决的方案，没有记录原因。B 落地时应确认或修订这些理由。

## 决策

1. **IR 由 Rust 持有。** 流水线 IR（表累积的操作，以编译器可以改写的形式保存）位于 Rust 内核。Python 仍在调用时一次性捕获 lambda（[ADR 0007](0007-lambda-dsl-schemaproxy.md)），并像现在一样把序列化的表达式发过边界。
2. **Rust 仍是元数据权威。** schema 与声明顺序归内核所有（[ADR 0009](0009-metadata-single-source-of-truth.md)、[ADR 0008](0008-explicit-sort-metadata.md)）。编译器为 IR 的每一步计算它们，Python 经 FFI 读取，从不自行推导。
3. **DataFusion 是通用优化器。** 没有专用 lowering 的步骤 lower 为 DataFusion 计划节点，由 DataFusion 照常优化和执行。LTSeq 不重建 DataFusion 的逻辑或物理优化。
4. **内核是被识别的 lowering。** 专用内核（线性扫描、并行扫描等）在编译器识别出它所实现的 IR 形状时适用。适用性检查属于这一识别，而不属于各个操作的入口。
5. **拒绝是有类型的。** 内核不适用时，编译器记录一个有类型的原因并对该步骤做通用 lowering。这个原因是诊断报告、测试断言的数据，而不是一行日志或一次静默回退。
6. **通用路径是参照。** 对任何输入，内核的结果都必须等于通用 DataFusion lowering 的结果。一致性测试比较二者，内核与之不一致即是内核的 bug。
7. **合批有约束。** 编译器不会合并任意一段相邻操作：有些操作会结束一段可编译的区间，例如 eager 边界、按物理位置的操作、IR 之外的操作。确切约束由 #232 定义，#238 的支持操作矩阵按操作记录它们。
8. **不新增物化。** 编译不得增加 eager 边界。no-materialization 规则（[ADR 0005](0005-no-materialization-rule.md)）及其已记录 eager 边界的清单保持不变。

诊断随编译器一起演进（#238）。`LTSeq._explain_pipeline()` 是私有的，它返回一个 dict，报告内核做出的决定而不执行计划，内容包括列环境、声明顺序、两份 DataFusion 计划，以及物理算子树和每种算子的计数。测试从中读取算子计数，而不是搜索计划文本。各工作流加入各自的字段：原始 IR 与规范化 IR、每一步的顺序状态、lowering 决策、内核适用性与拒绝原因。在 IR 稳定之前它保持私有，因为读取它的测试会使它成为事实上的契约。

## 备选方案

- *以 DataFusion 的 `LogicalPlan` 作为 LTSeq IR。* 这样不需要第二种表示。但 LTSeq 的声明顺序是内核放在计划旁边的元数据（`sort_specs`，[ADR 0008](0008-explicit-sort-metadata.md)），不是计划的一部分；专用内核（[ADR 0006](0006-multi-path-execution-strategy.md)）也在 DataFusion 计划之外运行。`LogicalPlan` 两者都不承载，编译器只能从计划节点重建它们。
- *由 Python IR 持有完整 schema。* 这会带回 #93 移除的双 schema 模型（[ADR 0009](0009-metadata-single-source-of-truth.md)）及其造成的漂移。
- *推迟到编译时才捕获 lambda。* lambda 中的错误将在之后某个无关的调用处出现，而不是在写下它的那一行；lambda 还要面对一个期间可能已经改变的 schema。[ADR 0007](0007-lambda-dsl-schemaproxy.md) 在调用时一次性捕获。
- *在 LTSeq 中重建完整的通用优化器。* 这会重复 [ADR 0003](0003-datafusion-arrow-substrate.md) 与 [ADR 0006](0006-multi-path-execution-strategy.md) 交给 DataFusion 的工作。编译器只增加 DataFusion 做不了的决策：序列语义与内核选择。

## 影响

- 内核的适用条件从文字描述（`docs/BENCHMARK.md`）和各操作入口移入编译器的识别。支持操作矩阵按操作记录它们，并由测试保持与代码同步。
- 每个内核落地前都需要与通用路径对照的一致性测试，每个拒绝原因都可经 `_explain_pipeline()` 看到。
- 留在 IR 之外的操作仍是屏障。矩阵列出它们，因此不会少报未被编译的部分。
- B 合并之前，本 ADR 的内容都没有实现。目前只有诊断的基础：结构化的算子计数和计划，读自与 `explain_plan()` 相同的规划。

## 来源

- #229（EPIC：整条流水线的编译器）及其工作流 #230–#237
- #238（诊断、本 ADR、支持操作矩阵），包括 2026-10-11 指定本编号的状态说明
- `src/ops/explain.rs`、`py-ltseq/tests/test_explain_pipeline.py`（诊断基础）
- [ADR 0005](0005-no-materialization-rule.md)、[ADR 0006](0006-multi-path-execution-strategy.md)、[ADR 0007](0007-lambda-dsl-schemaproxy.md)、[ADR 0008](0008-explicit-sort-metadata.md)、[ADR 0009](0009-metadata-single-source-of-truth.md)
