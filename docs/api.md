# LTSeq API Documentation

English | [中文](api.cn.md)

This page is the entry point to LTSeq's API documentation. It states no API rules itself. It tells you which document governs what, and how far each one describes the package you have installed.

## Version status

**v0.5 is the target public API, and it is not implemented yet.**

- The [v0.5 API contract][index] was merged into `main` on 2026-10-09 ([PR #250], then split into topic modules by [PR #279]). It replaces the earlier `docs/api.md` as the normative description of the public API ([§1.4]). Its status block predates the merge and still reads "proposal"; [Deliverable Q] records the owner's decision on every item the review left open, twelve of them deferred to a named gate.
- Merging the contract did not change the code. The review's [status block][review-status] says so: "Neither document is implemented. Every v0.5 name and behavior below is proposed contract, not a description of the code." The implementation, work items M0–M30 of the [impact map][Deliverable G], is tracked in [#257].
- Do not assume that the package you have installed has a given v0.5 name or behaves as the contract says. Much of the contract matches the code it was written against, but not all of it; the [API inventory][Deliverable B] lists what v0.5 does with every earlier name.
- When the implementation is complete, work item M28 rewrites this page and [api.cn.md](api.cn.md) so that they describe the contract exactly and their examples run as tests ([§24.1], criterion 5).

## The v0.5 API contract

**Start at the [v0.5 contract and review index][index].** The contract is the sixteen modules under `proposals/v0.5/contract/`, sections §1–§24. They are normative and use the BCP 14 key words (MUST, SHOULD, MAY). The index is navigation only: where to look for what, a [section map][section-map], and a [rule ownership map][rule-ownership] that names the section owning each cross-cutting rule.

The contract and the review are in English only. Neither has a Chinese translation.

To look up a method, find its signature in [§22], which points to the section that specifies its behavior.

| Topic | Sections |
|---|---|
| Overview, design principles, rule ownership | [§1] |
| Exports and object model | [§2–§3][§2] |
| Loading, constructors, laziness and materialization | [§4–§5][§4] |
| Schema, types and basic table operations | [§6–§7][§6] |
| Expression DSL | [§8] |
| Ordering contract | [§9] |
| Windows, ordered search, `fold` and ordered grouping | [§10–§11][§10] |
| Joins, set and bag operations | [§12–§13][§12] |
| Aggregation, partitioning and pivot | [§14] |
| Streaming, output and Arrow interchange | [§15–§16][§15] |
| Numeric, NULL, NaN and temporal semantics | [§17–§19][§17] |
| Errors and performance | [§20–§21][§20] |
| Canonical signatures of every public name | [§22] |
| End-to-end examples | [§23], continued in [§23.10–§23.18][§23.10] |
| Acceptance criteria and contract test matrix | [§24] |

## Rationale and migration: the v0.5 review

The [review modules][review-modules] explain the contract. They are non-normative.

- [Audit of the current API][audit]: what the review found wrong with the API before v0.5, area by area.
- [API inventory][Deliverable B]: every pre-v0.5 public name with its v0.5 action (keep, rename, merge, redesign, remove, add) and its replacement. Use it to migrate code.
- [Decisions][Deliverable D] on the eleven open semantic issues, and the [trade-off record][trade-offs].
- [Implementation impact map][Deliverable G]: work items M0–M30 and their order, tracked in [#257].
- The [owner decision register][Deliverable Q], and the dated record of each review round under [review history][review-history].

## The API before v0.5

[archive/pre-v0.5-api.md](archive/pre-v0.5-api.md) is the reference this page replaced, moved unchanged apart from an archive notice. It describes the API as implemented at commit 3041b44 (2026-10-08), the baseline the v0.5 review audited. Its signatures and examples are not v0.5's, and many differ materially. It is a record and is not updated as the code changes, so it may not match code merged after 3041b44.

Older documents cite it as `docs/api.md` by section: ADRs (`docs/api.md` §3.2, Appendix A), code comments, and the v0.5 review (`docs/api.md` § "Literal values", `docs/api.md:153`). Section numbers, titles and anchors are unchanged in the archive, so a citation by section finds it there. A citation by line refers to [the file at 3041b44][api-3041b44], which is the archive without its notice. Links to headings of the old page on `main`, such as `docs/api.md#literal-values` in issue comments, cannot be redirected and now land at the top of this page.

## Chinese documentation

[api.cn.md](api.cn.md) is the Chinese version of this page. The pre-v0.5 reference also has a Chinese version, [archive/pre-v0.5-api.cn.md](archive/pre-v0.5-api.cn.md).

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
