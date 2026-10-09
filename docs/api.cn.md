# LTSeq API 文档

[English](api.md) | 中文

本页是 LTSeq API 文档的入口。它本身不规定任何 API 行为，只说明各份文档分别管什么，以及它们在多大程度上描述你安装的包。

## 版本与实现状态

**v0.5 是目标公开 API，目前尚未实现。**

- [v0.5 API 契约][index]已于 2026-10-09 合入 `main`（[PR #250]，之后由 [PR #279] 按主题拆分为多个模块）。它取代原来的 `docs/api.md`，成为公开 API 的规范性描述（[§1.4]）。它的状态说明写于合入之前，仍写着 "proposal"（提案）；[Deliverable Q] 记录了项目负责人对评审遗留的每一项的决定，其中十二项推迟到指定的关口再定。
- 合入契约并没有改动代码。评审的[状态说明][review-status]写明："Neither document is implemented. Every v0.5 name and behavior below is proposed contract, not a description of the code."（两份文档都尚未实现；其中的每个 v0.5 名称和行为都是拟议的契约，不是对代码的描述。）实现工作，即[影响图][Deliverable G]中的工作项 M0–M30，在 [#257] 中跟踪。
- 不要假定你安装的包已经有某个 v0.5 名称，或者其行为已与契约一致。契约的很多内容与编写时的代码一致，但并非全部；[API 清单][Deliverable B]列出了 v0.5 对此前每个名称的处理。
- 实现完成后，工作项 M28 会改写本页和 [api.md](api.md)，使其准确描述契约，并让其中的示例作为测试运行（[§24.1] 第 5 条）。

## v0.5 API 契约

**从 [v0.5 契约与评审索引][index]开始。** 契约由 `proposals/v0.5/contract/` 下的十六个模块组成，章节编号为 §1–§24。这些模块是规范性的，使用 BCP 14 关键词（MUST、SHOULD、MAY）。索引只提供导航：按问题查找的入口、[章节对照表][section-map]，以及指出每条跨领域规则由哪一节负责的[规则归属表][rule-ownership]。

**契约和评审只有英文版，没有中文译本。** 下面的链接都指向英文原文。

查某个方法时，先在 [§22] 找到它的签名，§22 会指向规定其行为的章节。

| 主题 | 章节 |
|---|---|
| 概述、设计原则、规则归属 | [§1] |
| 导出名称与对象模型 | [§2–§3][§2] |
| 加载、构造函数、惰性求值与物化 | [§4–§5][§4] |
| Schema、类型与基础表操作 | [§6–§7][§6] |
| 表达式 DSL | [§8] |
| 顺序契约 | [§9] |
| 窗口、有序查找、`fold` 与有序分组 | [§10–§11][§10] |
| 连接、集合与多重集合运算 | [§12–§13][§12] |
| 聚合、分区与透视 | [§14] |
| 流式处理、输出与 Arrow 互操作 | [§15–§16][§15] |
| 数值、NULL、NaN 与时间语义 | [§17–§19][§17] |
| 错误与性能 | [§20–§21][§20] |
| 全部公开名称的规范签名 | [§22] |
| 端到端示例 | [§23]，续见 [§23.10–§23.18][§23.10] |
| 验收标准与契约测试矩阵 | [§24] |

## 设计依据与迁移：v0.5 评审

[评审模块][review-modules]解释契约为何如此规定，不具规范性。

- [现有 API 审计][audit]：评审在 v0.5 之前的 API 中发现的问题，按领域列出。
- [API 清单][Deliverable B]：v0.5 之前的每个公开名称在 v0.5 中的处理（保留、重命名、合并、重新设计、删除、新增）及替代写法。迁移代码时查它。
- 十一个未决语义问题的[决定][Deliverable D]，以及[取舍记录][trade-offs]。
- [实现影响图][Deliverable G]：工作项 M0–M30 及其顺序，在 [#257] 中跟踪。
- [负责人决定登记][Deliverable Q]，以及[评审历史][review-history]中每一轮评审的记录。

## v0.5 之前的 API

[archive/pre-v0.5-api.cn.md](archive/pre-v0.5-api.cn.md) 是本页所取代的参考文档，除加上一段归档说明外原样保留。它描述的是提交 3041b44（2026-10-08）时已实现的 API，也就是 v0.5 评审所审查的基线。其中的签名和示例不是 v0.5 的，很多地方差别很大。它只是一份记录，不随代码更新，因此可能与 3041b44 之后合入的代码不一致。

较早的文档按章节引用它：ADR（如 `docs/api.cn.md` §3.2、附录 A）、代码注释，以及 v0.5 评审（如 `docs/api.md` § "Literal values"、`docs/api.md:153`）。归档中的章节编号、标题和锚点都没有变，按章节引用的内容可以在归档中找到。按行号的引用指的是 [3041b44 时的英文文件][api-3041b44]，即去掉归档说明的英文归档。`main` 上指向旧页面标题的链接（例如 issue 评论里的 `docs/api.md#literal-values`）无法重定向，现在会落到入口页顶部。

## 英文文档

[api.md](api.md) 是本页的英文版。v0.5 之前的 API 参考也有英文版：[archive/pre-v0.5-api.md](archive/pre-v0.5-api.md)。

[index]: proposals/v0.5/README.md
[review-status]: proposals/v0.5/README.md#ltseq-v05-api-review
[section-map]: proposals/v0.5/README.md#section-map
[rule-ownership]: proposals/v0.5/README.md#rule-ownership
[review-modules]: proposals/v0.5/README.md#review-modules
[review-history]: proposals/v0.5/README.md#review-history
[§1]: proposals/v0.5/contract/overview.md#1-overview
[§1.4]: proposals/v0.5/contract/overview.md#14-relation-to-earlier-decisions
[§2]: proposals/v0.5/contract/public-surface.md#2-exports
[§4]: proposals/v0.5/contract/loading-and-laziness.md#4-loading
[§6]: proposals/v0.5/contract/schema-and-table-operations.md#6-schema-and-types
[§8]: proposals/v0.5/contract/expressions.md#8-expression-dsl
[§9]: proposals/v0.5/contract/ordering.md#9-ordering-contract
[§10]: proposals/v0.5/contract/windows-and-grouping.md#10-windows-and-ordered-computation
[§12]: proposals/v0.5/contract/joins-and-sets.md#12-joins
[§14]: proposals/v0.5/contract/aggregation.md#14-aggregation-partitioning-and-pivot
[§15]: proposals/v0.5/contract/streaming-and-output.md#15-streaming
[§17]: proposals/v0.5/contract/numeric-null-temporal.md#17-numeric-semantics-and-literals
[§20]: proposals/v0.5/contract/errors-and-performance.md#20-errors
[§22]: proposals/v0.5/contract/api-reference.md#22-complete-canonical-api-reference
[§23]: proposals/v0.5/contract/examples-sequences.md#23-end-to-end-examples
[§23.10]: proposals/v0.5/contract/examples-semantics.md#2310-streaming-and-interchange
[§24]: proposals/v0.5/contract/acceptance.md#24-acceptance-criteria-and-contract-test-matrix
[§24.1]: proposals/v0.5/contract/acceptance.md#241-acceptance-criteria
[audit]: proposals/v0.5/review/audit.md#audit-of-the-current-api
[Deliverable B]: proposals/v0.5/review/inventory.md#b-api-inventory-and-review
[Deliverable D]: proposals/v0.5/review/decisions.md#d-decisions-on-the-eleven-open-semantic-issues
[trade-offs]: proposals/v0.5/review/trade-offs.md#trade-off-record
[Deliverable G]: proposals/v0.5/review/impact-map.md#g-implementation-impact-map
[Deliverable Q]: proposals/v0.5/review/history/owner-decision-closure.md#q-owner-decision-closure
[api-3041b44]: https://github.com/Lychee-Technology/ltseq/blob/3041b444094321b3795e24faa7858e09195e8ef6/docs/api.md
[PR #250]: https://github.com/Lychee-Technology/ltseq/pull/250
[PR #279]: https://github.com/Lychee-Technology/ltseq/pull/279
[#257]: https://github.com/Lychee-Technology/ltseq/issues/257
