# LTSeq v0.5 public API contract and review

This directory holds the proposed public API contract for LTSeq v0.5 and the review behind it. Until 2026-10-09 they were two single files, `docs/proposals/v0.5-api-contract.md` and `docs/proposals/v0.5-api-review.md`, merged in PR #250 as 20b4e5a. They are now split into modules by topic. The text, the section numbers ([§1]–[§24]) and the deliverable letters (A–Q) are unchanged; [About this reorganization](#about-this-reorganization) lists exactly what was added or edited.

## Where to look

- **What a method does:** its signature is in [§22]; its behavior is in the section [§22] points to. The [section map](#section-map) gives the module of every section.
- **Which section governs a cross-cutting rule** (numeric types, literals, NULL and NaN, order, errors, interchange, time zones, aggregation, streaming, snapshots, tables without columns): [Rule ownership](#rule-ownership).
- **What happens to a current API name in v0.5:** the inventory, [Deliverable B].
- **Why a semantic issue was decided the way it was:** [Deliverable D], and the [trade-off record](review/trade-offs.md#trade-off-record) for costs accepted.
- **The owner's current decisions** (accepted, modified, deferred, superseded): [Deliverable Q].
- **Implementation order and the work items M0–M30:** [Deliverable G], tracked in #257.
- **How conformance is tested:** [§24]; the worked examples, which are tests too, are [§23].
- **When and why an earlier rule changed:** the dated records H to Q under [review/history/](#review-history), each of which lists the earlier records it changes.

## Status and scope

The two status blocks below opened the original files and are kept as written, apart from the paths of the files they name. "This document" in the first means the contract and in the second the review.

### LTSeq v0.5 Public API Contract

<!-- v0.5-modular:original v0.5-api-contract.md lines 3-6 -->

- Status: proposal awaiting final merge review, with the owner's decisions of 2026-10-09 applied (the companion review's [Deliverable Q]). It specifies the target behavior of v0.5. Much of it matches the baseline already; the companion review lists every difference ([Deliverable B]) and the work to close it ([Deliverable G]).
- Baseline: `main` at `3041b444094321b3795e24faa7858e09195e8ef6` (2026-10-08). Every statement about current behavior refers to that commit.
- Companion: `docs/proposals/v0.5/review/` records the audit, the API inventory, the decisions on the eleven open issues, the implementation impact map and the adversarial review of this document.
- Language: the key words MUST, MUST NOT, SHOULD, SHOULD NOT and MAY are used as defined in BCP 14 (RFC 2119, RFC 8174). This document has no "future" clauses: everything it specifies is part of v0.5.

<!-- /v0.5-modular:original -->

### LTSeq v0.5 API Review

<!-- v0.5-modular:original v0.5-api-review.md lines 3-33 -->

- Status: proposal awaiting final merge review. The owner recorded a decision on every open item on 2026-10-09, and [Deliverable Q] holds the current decision register. This document holds the review behind the proposed contract `docs/proposals/v0.5/contract/`. Neither document is implemented. Every v0.5 name and behavior below is proposed contract, not a description of the code.
- Baseline: `main` at [`3041b44`](https://github.com/Lychee-Technology/ltseq/commit/3041b444094321b3795e24faa7858e09195e8ef6) (2026-10-08). File and line references are to that commit. "Probe" means the behavior was observed by running the built extension at that commit on a 12-CPU machine.
- Scope: the whole public API as `docs/api.md` documents it and as the package exports it, plus the eleven open semantic issues filed under [EPIC #159](https://github.com/Lychee-Technology/ltseq/issues/159).
- Method: the API was redesigned from first principles, with no backward-compatibility constraint, and with this priority order:
  1. correctness, meaning no silent wrong results;
  2. reasonableness, ease of use and learnability, composability, and consistency with the Python ecosystem;
  3. performance and implementation effort.

  Every earlier recommendation, including those in the issues and ADR 0018, was treated as a hypothesis. A second-round adversarial review of the first draft found 22 problems ([Deliverable H]). Building the inventory and a final consistency check found 28 more, among them wrong migration idioms and contradictions that H's fixes left between sections ([Deliverable I]). All 50 are fixed in the contract as presented. The API review gate on this PR then found 12 more (V1–V12) and asked for nine owner decisions (D1–D9); [Deliverable J] records how this revision resolves each and which decisions still need the owner's confirmation. A second independent review then found four more (F13–F16) and asked for a decision on how `&` and `|` guard their operands; [Deliverable K] records that closure pass and the twelve decisions (D1–D12) awaiting the owner. A third independent review found five more (F17–F21), two of them a guarantee the contract could not keep; [Deliverable L] records how they are closed and adds D13 and D14. A fourth independent review found five more (F22–F26), and checking its note on Parquet writers found a sixth (F27); [Deliverable M] records how they are closed, with no new decision. A fifth independent review found three more (F28–F30), rules that promised a whole value or type domain where the format or library they rely on holds only part of it; [Deliverable N] records how they are closed, with no new decision. A sixth independent review found one more (F31), a memory bound that two operations cannot meet. Checking every operation against [§21.1] found more bounds of the same kind, and [Deliverable O] records how all of them are closed, with no new decision. A seventh independent review found two more defects in type conversions. An architectural reassessment of the PR then confirmed both, counting the first as a blocker and the second as a correction, and found five more blockers and seven more corrections needed before merge. [Deliverable P] records the closure pass that fixes all of them, the rule ownership map the contract now carries in [§1.6], the follow-up issues for what does not block, and the owner decisions still to be recorded, D10a among them. The owner then decided all of them, accepting fourteen, deferring twelve to named gates and modifying D8, D12, D14 and AC17; [Deliverable Q] is the current decision register and records how the contract applies the four modifications.

| Section | Contents |
|---|---|
| [A](review/verdict.md#a-executive-verdict) | Executive verdict: assessment, design principles, positioning |
| [Audit](review/audit.md#audit-of-the-current-api) | Findings for the nine audit areas, with evidence and the v0.5 resolution |
| [B](review/inventory.md#b-api-inventory-and-review) | The inventory of every current public API, with its v0.5 action |
| [C](review/verdict.md#c-the-contract) | The contract itself (pointer) |
| [D](review/decisions.md#d-decisions-on-the-eleven-open-semantic-issues) | Final decisions on the eleven issues |
| [Trade-offs](review/trade-offs.md#trade-off-record) | Where goals conflicted: the side chosen and the cost accepted; which DataFusion rewrites row-wise demand allows |
| [E](review/impact-map.md#e-canonical-examples) | The canonical examples (pointer and scenario map) |
| [F](review/impact-map.md#f-contract-test-matrix) | The contract test matrix (pointer and summary) |
| [G](review/impact-map.md#g-implementation-impact-map) | Implementation impact map |
| [H](review/history/adversarial-review.md#h-adversarial-review) | Adversarial review: findings and dispositions |
| [I](review/history/adversarial-review.md#i-final-consistency-check) | Final consistency check |
| [J](review/history/review-gate.md#j-api-review-gate) | API review gate: findings V1–V12, decisions D1–D9 as applied, and what this revision changed in earlier records |
| [K](review/history/review-gate.md#k-contract-closure-pass) | Contract closure pass: findings F13–F16, the `&`/`\|` decision, baseline behavior found, and the owner decisions D1–D12 |
| [L](review/history/review-closures.md#l-third-review-closure) | Third review closure: findings F17–F21, earlier records they change, and the owner decisions D13 and D14 |
| [M](review/history/review-closures.md#m-fourth-review-closure) | Fourth review closure: findings F22–F27 and the earlier records they change |
| [N](review/history/review-closures.md#n-fifth-review-closure) | Fifth review closure: findings F28–F30, the P8 note, and the earlier records they change |
| [O](review/history/review-closures.md#o-sixth-review-closure) | Sixth review closure: finding F31, the order of tuple `partition` keys, and the earlier records they change |
| [P](review/history/assessment-closure.md#p-assessment-closure-pass) | Assessment closure pass: blockers B1–B6 and corrections P1–P8, the rule ownership map, the classification of what remains, and the owner decisions to record |
| [Q](review/history/owner-decision-closure.md#q-owner-decision-closure) | Owner decision closure: the current decision register, how the contract applies the four modifications, and the earlier records they supersede |

<!-- /v0.5-modular:original -->

## Normative and non-normative text

The modules under [`contract/`](#contract-modules) are the contract: normative, written with the BCP 14 key words its status block declares ([Deliverable C]). Within the contract, [§1.6] names the section that owns each cross-cutting rule; a section that applies a rule points back to its owner.

The modules under [`review/`](#review-modules) are non-normative. They record the audit of the current API, the inventory, the decisions on the open issues, the trade-offs and the implementation impact map. The records under [`review/history/`](#review-history) are dated and chronological: each records one review round as it was closed, and later records name the earlier ones they change. [Deliverable Q] is the current decision register.

This README, and the header and footer of each module, are navigation only. They add no rule.

## Design principles

[§1.3] states ten principles that decide every case the other sections do not spell out, and the order in which they win when they conflict. The review explains each against the findings that motivated it ([Deliverable A], section A.2). In order:

1. No silent wrong results.
2. One meaning per spelling.
3. Order is explicit.
4. Errors are typed and staged.
5. Python spelling, Arrow types.
6. Minimal and orthogonal.
7. Lazy until a terminal.
8. Composable.
9. Deterministic.
10. Streaming is the default terminal path.

## Modules

### Contract modules

| Module | Sections | Contents |
|---|---|---|
| [overview.md](contract/overview.md) | [§1] | Overview, principles and rule ownership |
| [public-surface.md](contract/public-surface.md) | [§2]–[§3] | Exports and object model |
| [loading-and-laziness.md](contract/loading-and-laziness.md) | [§4]–[§5] | Loading and lazy evaluation |
| [schema-and-table-operations.md](contract/schema-and-table-operations.md) | [§6]–[§7] | Schema, types and basic table operations |
| [expressions.md](contract/expressions.md) | [§8] | Expression DSL |
| [ordering.md](contract/ordering.md) | [§9] | Ordering contract |
| [windows-and-grouping.md](contract/windows-and-grouping.md) | [§10]–[§11] | Windows and ordered grouping |
| [joins-and-sets.md](contract/joins-and-sets.md) | [§12]–[§13] | Joins and set operations |
| [aggregation.md](contract/aggregation.md) | [§14] | Aggregation, partitioning and pivot |
| [streaming-and-output.md](contract/streaming-and-output.md) | [§15]–[§16] | Streaming, output and interchange |
| [numeric-null-temporal.md](contract/numeric-null-temporal.md) | [§17]–[§19] | Numeric, NULL and temporal semantics |
| [errors-and-performance.md](contract/errors-and-performance.md) | [§20]–[§21] | Errors and performance |
| [api-reference.md](contract/api-reference.md) | [§22] | Canonical API reference |
| [examples-sequences.md](contract/examples-sequences.md) | [§23]–[§23.9] | Examples: ordered computation, joins and state |
| [examples-semantics.md](contract/examples-semantics.md) | [§23.10]–[§23.18] | Examples: streaming, values, errors and interchange |
| [acceptance.md](contract/acceptance.md) | [§24] | Acceptance criteria and test matrix |

### Review modules

| Module | Deliverables | Contents |
|---|---|---|
| [verdict.md](review/verdict.md) | A, C | Executive verdict and the contract |
| [audit.md](review/audit.md) | Audit A–I | Audit of the current API |
| [inventory.md](review/inventory.md) | B | API inventory and review |
| [decisions.md](review/decisions.md) | D | Decisions on the eleven open semantic issues |
| [trade-offs.md](review/trade-offs.md) | Trade-offs | Trade-off record |
| [impact-map.md](review/impact-map.md) | E, F, G | Examples, test matrix and implementation impact map |

### Review history

| Module | Deliverables | Contents |
|---|---|---|
| [adversarial-review.md](review/history/adversarial-review.md) | H, I | Adversarial review and final consistency check |
| [review-gate.md](review/history/review-gate.md) | J, K | API review gate and contract closure pass |
| [review-closures.md](review/history/review-closures.md) | L–O | Third to sixth review closures |
| [assessment-closure.md](review/history/assessment-closure.md) | P | Assessment closure pass |
| [owner-decision-closure.md](review/history/owner-decision-closure.md) | Q | Owner decision closure |

## Section map

Every subsection §N.M is in the module of chapter §N, except that [§23.10]–[§23.18] continue in a second examples module. Old links that name a line of the original files cannot be redirected; find the section number or deliverable letter here.

| Section | Title | Module |
|---|---|---|
| [§1] | Overview | [contract/overview.md](contract/overview.md) |
| [§2] | Exports | [contract/public-surface.md](contract/public-surface.md) |
| [§3] | Object model | [contract/public-surface.md](contract/public-surface.md) |
| [§4] | Loading | [contract/loading-and-laziness.md](contract/loading-and-laziness.md) |
| [§5] | Lazy evaluation and materialization | [contract/loading-and-laziness.md](contract/loading-and-laziness.md) |
| [§6] | Schema and types | [contract/schema-and-table-operations.md](contract/schema-and-table-operations.md) |
| [§7] | Basic table operations | [contract/schema-and-table-operations.md](contract/schema-and-table-operations.md) |
| [§8] | Expression DSL | [contract/expressions.md](contract/expressions.md) |
| [§9] | Ordering contract | [contract/ordering.md](contract/ordering.md) |
| [§10] | Windows and ordered computation | [contract/windows-and-grouping.md](contract/windows-and-grouping.md) |
| [§11] | Ordered grouping | [contract/windows-and-grouping.md](contract/windows-and-grouping.md) |
| [§12] | Joins | [contract/joins-and-sets.md](contract/joins-and-sets.md) |
| [§13] | Set and bag operations | [contract/joins-and-sets.md](contract/joins-and-sets.md) |
| [§14] | Aggregation, partitioning and pivot | [contract/aggregation.md](contract/aggregation.md) |
| [§15] | Streaming | [contract/streaming-and-output.md](contract/streaming-and-output.md) |
| [§16] | Output and interchange | [contract/streaming-and-output.md](contract/streaming-and-output.md) |
| [§17] | Numeric semantics and literals | [contract/numeric-null-temporal.md](contract/numeric-null-temporal.md) |
| [§18] | NULL, NaN and Boolean logic | [contract/numeric-null-temporal.md](contract/numeric-null-temporal.md) |
| [§19] | Temporal semantics | [contract/numeric-null-temporal.md](contract/numeric-null-temporal.md) |
| [§20] | Errors | [contract/errors-and-performance.md](contract/errors-and-performance.md) |
| [§21] | Performance contract | [contract/errors-and-performance.md](contract/errors-and-performance.md) |
| [§22] | Complete canonical API reference | [contract/api-reference.md](contract/api-reference.md) |
| [§23] (introduction), [§23.1]–[§23.9] | End-to-end examples | [contract/examples-sequences.md](contract/examples-sequences.md) |
| [§23.10]–[§23.18] | End-to-end examples (continued) | [contract/examples-semantics.md](contract/examples-semantics.md) |
| [§24] | Acceptance criteria and contract test matrix | [contract/acceptance.md](contract/acceptance.md) |

| Deliverable | Title | Module |
|---|---|---|
| [A](review/verdict.md#a-executive-verdict) | Executive verdict | [review/verdict.md](review/verdict.md) |
| Audit | Audit of the current API (areas A–I) | [review/audit.md](review/audit.md) |
| [B](review/inventory.md#b-api-inventory-and-review) | API inventory and review | [review/inventory.md](review/inventory.md) |
| [C](review/verdict.md#c-the-contract) | The contract | [review/verdict.md](review/verdict.md) |
| [D](review/decisions.md#d-decisions-on-the-eleven-open-semantic-issues) | Decisions on the eleven open semantic issues | [review/decisions.md](review/decisions.md) |
| Trade-offs | Trade-off record | [review/trade-offs.md](review/trade-offs.md) |
| [E](review/impact-map.md#e-canonical-examples) | Canonical examples | [review/impact-map.md](review/impact-map.md) |
| [F](review/impact-map.md#f-contract-test-matrix) | Contract test matrix | [review/impact-map.md](review/impact-map.md) |
| [G](review/impact-map.md#g-implementation-impact-map) | Implementation impact map | [review/impact-map.md](review/impact-map.md) |
| [H](review/history/adversarial-review.md#h-adversarial-review) | Adversarial review | [review/history/adversarial-review.md](review/history/adversarial-review.md) |
| [I](review/history/adversarial-review.md#i-final-consistency-check) | Final consistency check | [review/history/adversarial-review.md](review/history/adversarial-review.md) |
| [J](review/history/review-gate.md#j-api-review-gate) | API review gate | [review/history/review-gate.md](review/history/review-gate.md) |
| [K](review/history/review-gate.md#k-contract-closure-pass) | Contract closure pass | [review/history/review-gate.md](review/history/review-gate.md) |
| [L](review/history/review-closures.md#l-third-review-closure) | Third review closure | [review/history/review-closures.md](review/history/review-closures.md) |
| [M](review/history/review-closures.md#m-fourth-review-closure) | Fourth review closure | [review/history/review-closures.md](review/history/review-closures.md) |
| [N](review/history/review-closures.md#n-fifth-review-closure) | Fifth review closure | [review/history/review-closures.md](review/history/review-closures.md) |
| [O](review/history/review-closures.md#o-sixth-review-closure) | Sixth review closure | [review/history/review-closures.md](review/history/review-closures.md) |
| [P](review/history/assessment-closure.md#p-assessment-closure-pass) | Assessment closure pass | [review/history/assessment-closure.md](review/history/assessment-closure.md) |
| [Q](review/history/owner-decision-closure.md#q-owner-decision-closure) | Owner decision closure | [review/history/owner-decision-closure.md](review/history/owner-decision-closure.md) |

## Rule ownership

[§1.6] is the authoritative table: for each concept it names the owning sections, the sections that apply the rule, the [§24] rows that test it and the issues behind it. This map only translates its section numbers into modules, so a reader can find the owner from any module. It adds and changes nothing; where it and [§1.6] differ, [§1.6] is right.

| Concept ([§1.6]) | Owner module | Modules that apply the rule |
|---|---|---|
| Numeric result types and checking | [numeric-null-temporal](contract/numeric-null-temporal.md) ([§17.1], [§17.3], [§17.4]) · [aggregation](contract/aggregation.md) ([§14.2]) | [expressions](contract/expressions.md) ([§8.1], [§8.4], [§8.5]) · [windows-and-grouping](contract/windows-and-grouping.md) ([§10.1], [§10.2]) |
| Literal conversion and exactness | [numeric-null-temporal](contract/numeric-null-temporal.md) ([§17.2], [§17.5], [§17.6]) · [loading-and-laziness](contract/loading-and-laziness.md) ([§4.2], [§4.6]) | [loading-and-laziness](contract/loading-and-laziness.md) ([§4.4]–[§4.6]) · [schema-and-table-operations](contract/schema-and-table-operations.md) ([§6.2], [§7.3], [§7.10]) · [expressions](contract/expressions.md) ([§8.3], [§8.5]) · [windows-and-grouping](contract/windows-and-grouping.md) ([§10.1], [§10.7]) · [streaming-and-output](contract/streaming-and-output.md) ([§16.4], [§16.5]) · [numeric-null-temporal](contract/numeric-null-temporal.md) ([§17.4], [§19.2]) · [errors-and-performance](contract/errors-and-performance.md) ([§20.2]) |
| NULL and NaN equality and ordering | [numeric-null-temporal](contract/numeric-null-temporal.md) ([§18], [§17.4]) · [ordering](contract/ordering.md) ([§9.4]) | [loading-and-laziness](contract/loading-and-laziness.md) ([§4.6]) · [schema-and-table-operations](contract/schema-and-table-operations.md) ([§7.6]) · [expressions](contract/expressions.md) ([§8.1], [§8.3], [§8.5]) · [windows-and-grouping](contract/windows-and-grouping.md) ([§10.1], [§10.4], [§11.1]) · [joins-and-sets](contract/joins-and-sets.md) ([§12.1], [§12.3], [§13]) · [aggregation](contract/aggregation.md) ([§14.1], [§14.2], [§14.5], [§14.6]) |
| Logical row order and sort metadata | [ordering](contract/ordering.md) ([§9.1], [§9.2], [§9.3], [§9.4]) · [overview](contract/overview.md) (principle 9) | [loading-and-laziness](contract/loading-and-laziness.md) ([§4.7]) · [schema-and-table-operations](contract/schema-and-table-operations.md) ([§7]) · [windows-and-grouping](contract/windows-and-grouping.md) ([§10], [§11]) · [joins-and-sets](contract/joins-and-sets.md) ([§12], [§13]) · [aggregation](contract/aggregation.md) ([§14]) |
| Demand and error observation | [errors-and-performance](contract/errors-and-performance.md) ([§20.2], [§20.1], [§21.2]) · [overview](contract/overview.md) (principle 9) | [loading-and-laziness](contract/loading-and-laziness.md) ([§5.2]) · [schema-and-table-operations](contract/schema-and-table-operations.md) ([§7.9]) · [expressions](contract/expressions.md) ([§8.1], [§8.3]) · [ordering](contract/ordering.md) ([§9.5]) · [windows-and-grouping](contract/windows-and-grouping.md) ([§10.5], [§10.6], [§10.7]) · [joins-and-sets](contract/joins-and-sets.md) ([§12.1]) · [aggregation](contract/aggregation.md) ([§14.2]) · [streaming-and-output](contract/streaming-and-output.md) ([§16.6]) · [numeric-null-temporal](contract/numeric-null-temporal.md) ([§17.2]) |
| Arrow and Python interchange | [schema-and-table-operations](contract/schema-and-table-operations.md) ([§6.2]) · [streaming-and-output](contract/streaming-and-output.md) ([§16.3], [§15.1], [§16.4], [§16.2], [§16.5]) | [loading-and-laziness](contract/loading-and-laziness.md) ([§4.3]–[§4.6]) · [windows-and-grouping](contract/windows-and-grouping.md) ([§10.7]) · [aggregation](contract/aggregation.md) ([§14.5], [§14.6]) · [streaming-and-output](contract/streaming-and-output.md) ([§15.2]) · [numeric-null-temporal](contract/numeric-null-temporal.md) ([§17.6], [§18]) · [errors-and-performance](contract/errors-and-performance.md) ([§20.1], [§20.2]) |
| Temporal units and time zones | [numeric-null-temporal](contract/numeric-null-temporal.md) ([§19.2], [§19.4], [§17.2], [§17.5], [§17.6]) | [overview](contract/overview.md) ([§1.4]) · [schema-and-table-operations](contract/schema-and-table-operations.md) ([§6.2]) · [ordering](contract/ordering.md) ([§9.4]) · [joins-and-sets](contract/joins-and-sets.md) ([§12.1]) · [aggregation](contract/aggregation.md) ([§14.5]) · [streaming-and-output](contract/streaming-and-output.md) ([§16.3]) · [numeric-null-temporal](contract/numeric-null-temporal.md) ([§17.4]) · [errors-and-performance](contract/errors-and-performance.md) ([§20.1]) |
| Aggregation | [aggregation](contract/aggregation.md) ([§14.2]) | [windows-and-grouping](contract/windows-and-grouping.md) ([§10.1]–[§10.3], [§11.2]) · [aggregation](contract/aggregation.md) ([§14.1], [§14.3], [§14.6]) · [numeric-null-temporal](contract/numeric-null-temporal.md) ([§17.1], [§17.3]) · [errors-and-performance](contract/errors-and-performance.md) ([§20.2]) |
| Streaming and materialization | [errors-and-performance](contract/errors-and-performance.md) ([§21.1]) · [loading-and-laziness](contract/loading-and-laziness.md) ([§5.3]) | [overview](contract/overview.md) (principles 7 and 10) · [streaming-and-output](contract/streaming-and-output.md) ([§15.1], [§15.2], [§16.4]) |
| Snapshot and partition consistency | [loading-and-laziness](contract/loading-and-laziness.md) ([§5.1], [§5.2]) · [aggregation](contract/aggregation.md) ([§14.5]) | [overview](contract/overview.md) (principle 9) · [public-surface](contract/public-surface.md) ([§3.1]) · [loading-and-laziness](contract/loading-and-laziness.md) ([§4]) · [ordering](contract/ordering.md) ([§9.2]) · [aggregation](contract/aggregation.md) ([§14.6]) · [streaming-and-output](contract/streaming-and-output.md) ([§15.1], [§16.6]) · [numeric-null-temporal](contract/numeric-null-temporal.md) ([§19.5]) · [errors-and-performance](contract/errors-and-performance.md) ([§20.2]) |
| Tables without columns | [schema-and-table-operations](contract/schema-and-table-operations.md) ([§6.1]) | [loading-and-laziness](contract/loading-and-laziness.md) ([§4.2], [§4.4]–[§4.6]) · [schema-and-table-operations](contract/schema-and-table-operations.md) ([§7.2], [§7.5], [§7.6]) · [streaming-and-output](contract/streaming-and-output.md) ([§16.5]) · [errors-and-performance](contract/errors-and-performance.md) ([§20.2]) |

Other single sources the contract names: the exported names are [§2] and the signatures [§22] ([§24.1]); the exception classes [§20.1]; the eager calls [§5.3]; the operations that may hold their whole input [§21.1]; the guarantee rows [§24.3].

## Related issues and ADRs

- PR #250 proposed both documents and holds their review discussion.
- #257 is the implementation roadmap epic for the impact map (M0–M30, [Deliverable G]).
- The eleven open semantic issues decided in [Deliverable D](review/decisions.md#d-decisions-on-the-eleven-open-semantic-issues): #202, #148, #156, #218, #221, #222, #228, #241, #246, #247, #248, filed under EPIC #159.
- Issues opened while PR #250 was under review, all cited by the review: #251–#256 and #258–#278. Among them, #251 holds the feasibility measurements (M0), #274 and #275 the test infrastructure, and #276, #277 and #278 the deferred contract follow-ups (numeric, temporal, and API surface and interchange).
- ADRs the contract revises, keeps or replaces ([§1.4]): [0004](../../adr/0004-lazy-execution-immutable-tables.md), [0005](../../adr/0005-no-materialization-rule.md), [0006](../../adr/0006-multi-path-execution-strategy.md), [0008](../../adr/0008-explicit-sort-metadata.md), [0009](../../adr/0009-metadata-single-source-of-truth.md), [0010](../../adr/0010-four-table-object-types.md), [0011](../../adr/0011-link-lazy-prefix-aliased-join.md), [0013](../../adr/0013-window-over-unification.md), [0014](../../adr/0014-pyi-stubs-typed-surface.md), [0017](../../adr/0017-arrow-c-data-interface-boundary.md), [0018](../../adr/0018-literal-exactness-facts-and-policy.md). Work item M28 amends them.

## Contents

Every heading of every module, in reading order. Each list is collapsed; expand it to browse.

### Contract

<details>
<summary>16 modules, §1–§24</summary>

- **[contract/overview.md](contract/overview.md)**
  - [1. Overview](contract/overview.md#1-overview)
    - [1.1 What LTSeq is](contract/overview.md#11-what-ltseq-is)
    - [1.2 Positioning](contract/overview.md#12-positioning)
    - [1.3 Design principles](contract/overview.md#13-design-principles)
    - [1.4 Relation to earlier decisions](contract/overview.md#14-relation-to-earlier-decisions)
    - [1.5 Non-goals](contract/overview.md#15-non-goals)
    - [1.6 Rule ownership](contract/overview.md#16-rule-ownership)
- **[contract/public-surface.md](contract/public-surface.md)**
  - [2. Exports](contract/public-surface.md#2-exports)
    - [2.1 The `ltseq` namespace](contract/public-surface.md#21-the-ltseq-namespace)
    - [2.2 Other modules](contract/public-surface.md#22-other-modules)
    - [2.3 Type aliases used in signatures](contract/public-surface.md#23-type-aliases-used-in-signatures)
  - [3. Object model](contract/public-surface.md#3-object-model)
    - [3.1 Types](contract/public-surface.md#31-types)
    - [3.2 `LTSeq` invariants](contract/public-surface.md#32-ltseq-invariants)
    - [3.3 `NestedTable` and `GroupBy` invariants](contract/public-surface.md#33-nestedtable-and-groupby-invariants)
    - [3.4 Lambdas and proxies](contract/public-surface.md#34-lambdas-and-proxies)
- **[contract/loading-and-laziness.md](contract/loading-and-laziness.md)**
  - [4. Loading](contract/loading-and-laziness.md#4-loading)
    - [4.1 Source paths](contract/loading-and-laziness.md#41-source-paths)
    - [4.2 `LTSeq.read_csv`](contract/loading-and-laziness.md#42-ltseqread_csv)
    - [4.3 `LTSeq.read_parquet`](contract/loading-and-laziness.md#43-ltseqread_parquet)
    - [4.4 `LTSeq.from_arrow`](contract/loading-and-laziness.md#44-ltseqfrom_arrow)
    - [4.5 `LTSeq.from_pandas`](contract/loading-and-laziness.md#45-ltseqfrom_pandas)
    - [4.6 `LTSeq.from_dict` and `LTSeq.from_rows`](contract/loading-and-laziness.md#46-ltseqfrom_dict-and-ltseqfrom_rows)
    - [4.7 `LTSeq.range`](contract/loading-and-laziness.md#47-ltseqrange)
  - [5. Lazy evaluation and materialization](contract/loading-and-laziness.md#5-lazy-evaluation-and-materialization)
    - [5.1 Plan-building and execution](contract/loading-and-laziness.md#51-plan-building-and-execution)
    - [5.2 `LTSeq.collect`](contract/loading-and-laziness.md#52-ltseqcollect)
    - [5.3 Eager calls](contract/loading-and-laziness.md#53-eager-calls)
- **[contract/schema-and-table-operations.md](contract/schema-and-table-operations.md)**
  - [6. Schema and types](contract/schema-and-table-operations.md#6-schema-and-types)
    - [6.1 `schema` and `columns`](contract/schema-and-table-operations.md#61-schema-and-columns)
    - [6.2 Supported types](contract/schema-and-table-operations.md#62-supported-types)
    - [6.3 Data type arguments (`DTypeLike`)](contract/schema-and-table-operations.md#63-data-type-arguments-dtypelike)
    - [6.4 Column names](contract/schema-and-table-operations.md#64-column-names)
  - [7. Basic table operations](contract/schema-and-table-operations.md#7-basic-table-operations)
    - [7.1 `filter`](contract/schema-and-table-operations.md#71-filter)
    - [7.2 `select`](contract/schema-and-table-operations.md#72-select)
    - [7.3 `derive`](contract/schema-and-table-operations.md#73-derive)
    - [7.4 `rename`](contract/schema-and-table-operations.md#74-rename)
    - [7.5 `drop`](contract/schema-and-table-operations.md#75-drop)
    - [7.6 `distinct`](contract/schema-and-table-operations.md#76-distinct)
    - [7.7 `pipe`](contract/schema-and-table-operations.md#77-pipe)
    - [7.8 `explain`](contract/schema-and-table-operations.md#78-explain)
    - [7.9 `count` and `show`](contract/schema-and-table-operations.md#79-count-and-show)
    - [7.10 Value-level edits: `insert`, `delete`, `update`](contract/schema-and-table-operations.md#710-value-level-edits-insert-delete-update)
- **[contract/expressions.md](contract/expressions.md)**
  - [8. Expression DSL](contract/expressions.md#8-expression-dsl)
    - [8.1 Contexts and proxies](contract/expressions.md#81-contexts-and-proxies)
    - [8.2 Operators](contract/expressions.md#82-operators)
    - [8.3 Conditional and NULL functions](contract/expressions.md#83-conditional-and-null-functions)
    - [8.4 Math functions](contract/expressions.md#84-math-functions)
    - [8.5 General `Expr` methods](contract/expressions.md#85-general-expr-methods)
    - [8.6 String methods: `.str`](contract/expressions.md#86-string-methods-str)
    - [8.7 The closed method set](contract/expressions.md#87-the-closed-method-set)
- **[contract/ordering.md](contract/ordering.md)**
  - [9. Ordering contract](contract/ordering.md#9-ordering-contract)
    - [9.1 Order state](contract/ordering.md#91-order-state)
    - [9.2 Sources and propagation](contract/ordering.md#92-sources-and-propagation)
    - [9.3 Order requirements](contract/ordering.md#93-order-requirements)
    - [9.4 `sort`](contract/ordering.md#94-sort)
    - [9.5 `assume_sorted`](contract/ordering.md#95-assume_sorted)
    - [9.6 `is_sorted_by`](contract/ordering.md#96-is_sorted_by)
    - [9.7 `sort_keys` and `is_ordered`](contract/ordering.md#97-sort_keys-and-is_ordered)
    - [9.8 Positional selection](contract/ordering.md#98-positional-selection)
    - [9.9 `with_row_index`](contract/ordering.md#99-with_row_index)
- **[contract/windows-and-grouping.md](contract/windows-and-grouping.md)**
  - [10. Windows and ordered computation](contract/windows-and-grouping.md#10-windows-and-ordered-computation)
    - [10.1 Window methods](contract/windows-and-grouping.md#101-window-methods)
    - [10.2 Ranking functions](contract/windows-and-grouping.md#102-ranking-functions)
    - [10.3 Aggregates over windows](contract/windows-and-grouping.md#103-aggregates-over-windows)
    - [10.4 `over`](contract/windows-and-grouping.md#104-over)
    - [10.5 `search_first`](contract/windows-and-grouping.md#105-search_first)
    - [10.6 `search_pattern`](contract/windows-and-grouping.md#106-search_pattern)
    - [10.7 `fold`](contract/windows-and-grouping.md#107-fold)
  - [11. Ordered grouping](contract/windows-and-grouping.md#11-ordered-grouping)
    - [11.1 `group_ordered`](contract/windows-and-grouping.md#111-group_ordered)
    - [11.2 `NestedTable`](contract/windows-and-grouping.md#112-nestedtable)
    - [11.3 Example](contract/windows-and-grouping.md#113-example)
- **[contract/joins-and-sets.md](contract/joins-and-sets.md)**
  - [12. Joins](contract/joins-and-sets.md#12-joins)
    - [12.1 `join`](contract/joins-and-sets.md#121-join)
    - [12.2 `semi_join` and `anti_join`](contract/joins-and-sets.md#122-semi_join-and-anti_join)
    - [12.3 `asof_join`](contract/joins-and-sets.md#123-asof_join)
  - [13. Set and bag operations](contract/joins-and-sets.md#13-set-and-bag-operations)
    - [13.1 `concat`](contract/joins-and-sets.md#131-concat)
    - [13.2 `intersect` and `difference`](contract/joins-and-sets.md#132-intersect-and-difference)
- **[contract/aggregation.md](contract/aggregation.md)**
  - [14. Aggregation, partitioning and pivot](contract/aggregation.md#14-aggregation-partitioning-and-pivot)
    - [14.1 `group_by` and `GroupBy.agg`](contract/aggregation.md#141-group_by-and-groupbyagg)
    - [14.2 Aggregate expressions](contract/aggregation.md#142-aggregate-expressions)
    - [14.3 `LTSeq.agg`](contract/aggregation.md#143-ltseqagg)
    - [14.4 `NestedTable` aggregation](contract/aggregation.md#144-nestedtable-aggregation)
    - [14.5 `partition`](contract/aggregation.md#145-partition)
    - [14.6 `pivot`](contract/aggregation.md#146-pivot)
- **[contract/streaming-and-output.md](contract/streaming-and-output.md)**
  - [15. Streaming](contract/streaming-and-output.md#15-streaming)
    - [15.1 `to_batches`](contract/streaming-and-output.md#151-to_batches)
    - [15.2 Iteration](contract/streaming-and-output.md#152-iteration)
  - [16. Output and interchange](contract/streaming-and-output.md#16-output-and-interchange)
    - [16.1 `to_arrow`](contract/streaming-and-output.md#161-to_arrow)
    - [16.2 `to_pandas`](contract/streaming-and-output.md#162-to_pandas)
    - [16.3 `to_dicts`](contract/streaming-and-output.md#163-to_dicts)
    - [16.4 Arrow PyCapsule stream](contract/streaming-and-output.md#164-arrow-pycapsule-stream)
    - [16.5 Writers](contract/streaming-and-output.md#165-writers)
    - [16.6 Pickle](contract/streaming-and-output.md#166-pickle)
- **[contract/numeric-null-temporal.md](contract/numeric-null-temporal.md)**
  - [17. Numeric semantics and literals](contract/numeric-null-temporal.md#17-numeric-semantics-and-literals)
    - [17.1 Integer and decimal results are checked](contract/numeric-null-temporal.md#171-integer-and-decimal-results-are-checked)
    - [17.2 Literals](contract/numeric-null-temporal.md#172-literals)
    - [17.3 Arithmetic operators](contract/numeric-null-temporal.md#173-arithmetic-operators)
    - [17.4 Types of mixed operands](contract/numeric-null-temporal.md#174-types-of-mixed-operands)
    - [17.5 Shared values](contract/numeric-null-temporal.md#175-shared-values)
    - [17.6 Explicit casts](contract/numeric-null-temporal.md#176-explicit-casts)
  - [18. NULL, NaN and Boolean logic](contract/numeric-null-temporal.md#18-null-nan-and-boolean-logic)
  - [19. Temporal semantics](contract/numeric-null-temporal.md#19-temporal-semantics)
    - [19.1 Types](contract/numeric-null-temporal.md#191-types)
    - [19.2 Arithmetic and comparison](contract/numeric-null-temporal.md#192-arithmetic-and-comparison)
    - [19.3 `.dt` fields](contract/numeric-null-temporal.md#193-dt-fields)
    - [19.4 `.dt` methods](contract/numeric-null-temporal.md#194-dt-methods)
    - [19.5 Clock functions](contract/numeric-null-temporal.md#195-clock-functions)
- **[contract/errors-and-performance.md](contract/errors-and-performance.md)**
  - [20. Errors](contract/errors-and-performance.md#20-errors)
    - [20.1 Exception classes](contract/errors-and-performance.md#201-exception-classes)
    - [20.2 Stages](contract/errors-and-performance.md#202-stages)
  - [21. Performance contract](contract/errors-and-performance.md#21-performance-contract)
    - [21.1 Materialization](contract/errors-and-performance.md#211-materialization)
    - [21.2 Fast paths](contract/errors-and-performance.md#212-fast-paths)
- **[contract/api-reference.md](contract/api-reference.md)**
  - [22. Complete canonical API reference](contract/api-reference.md#22-complete-canonical-api-reference)
    - [22.1 `ltseq`](contract/api-reference.md#221-ltseq)
    - [22.2 `ltseq.typing`](contract/api-reference.md#222-ltseqtyping)
- **[contract/examples-sequences.md](contract/examples-sequences.md)**
  - [23. End-to-end examples](contract/examples-sequences.md#23-end-to-end-examples)
    - [23.1 Sessions with `group_ordered`](contract/examples-sequences.md#231-sessions-with-group_ordered)
    - [23.2 Table-order windows](contract/examples-sequences.md#232-table-order-windows)
    - [23.3 Ranking and window aggregates](contract/examples-sequences.md#233-ranking-and-window-aggregates)
    - [23.4 Pattern search and `search_first`](contract/examples-sequences.md#234-pattern-search-and-search_first)
    - [23.5 As-of join](contract/examples-sequences.md#235-as-of-join)
    - [23.6 Join with `alias` and `validate`](contract/examples-sequences.md#236-join-with-alias-and-validate)
    - [23.7 Pivot](contract/examples-sequences.md#237-pivot)
    - [23.8 Partitions across processes](contract/examples-sequences.md#238-partitions-across-processes)
    - [23.9 Sequential state with `fold`](contract/examples-sequences.md#239-sequential-state-with-fold)
- **[contract/examples-semantics.md](contract/examples-semantics.md)**
    - [23.10 Streaming and interchange](contract/examples-semantics.md#2310-streaming-and-interchange)
    - [23.11 NULL and NaN](contract/examples-semantics.md#2311-null-and-nan)
    - [23.12 Checked and exact arithmetic](contract/examples-semantics.md#2312-checked-and-exact-arithmetic)
    - [23.13 Time zones and DST](contract/examples-semantics.md#2313-time-zones-and-dst)
    - [23.14 Demand and error stages](contract/examples-semantics.md#2314-demand-and-error-stages)
    - [23.15 Order state](contract/examples-semantics.md#2315-order-state)
    - [23.16 Set and bag operations](contract/examples-semantics.md#2316-set-and-bag-operations)
    - [23.17 Lazy CSV pipeline with composed expressions](contract/examples-semantics.md#2317-lazy-csv-pipeline-with-composed-expressions)
    - [23.18 Arrow and pandas round trip](contract/examples-semantics.md#2318-arrow-and-pandas-round-trip)
- **[contract/acceptance.md](contract/acceptance.md)**
  - [24. Acceptance criteria and contract test matrix](contract/acceptance.md#24-acceptance-criteria-and-contract-test-matrix)
    - [24.1 Acceptance criteria](contract/acceptance.md#241-acceptance-criteria)
    - [24.2 Properties](contract/acceptance.md#242-properties)
    - [24.3 Guarantee matrix](contract/acceptance.md#243-guarantee-matrix)
    - [24.4 Generators](contract/acceptance.md#244-generators)
    - [24.5 Coverage of the required dimensions](contract/acceptance.md#245-coverage-of-the-required-dimensions)

</details>

### Review

<details>
<summary>11 modules, deliverables A–Q</summary>

- **[review/verdict.md](review/verdict.md)**
  - [A. Executive verdict](review/verdict.md#a-executive-verdict)
    - [A.1 The current API](review/verdict.md#a1-the-current-api)
    - [A.2 Design principles](review/verdict.md#a2-design-principles)
    - [A.3 Direction for v0.5](review/verdict.md#a3-direction-for-v05)
    - [A.4 Positioning](review/verdict.md#a4-positioning)
  - [C. The contract](review/verdict.md#c-the-contract)
- **[review/audit.md](review/audit.md)**
  - [Audit of the current API](review/audit.md#audit-of-the-current-api)
    - [Audit A. Object model](review/audit.md#audit-a-object-model)
    - [Audit B. Naming and minimalism](review/audit.md#audit-b-naming-and-minimalism)
    - [Audit C. Expression DSL](review/audit.md#audit-c-expression-dsl)
    - [Audit D. Ordered sequence semantics](review/audit.md#audit-d-ordered-sequence-semantics)
    - [Audit E. Join and set semantics](review/audit.md#audit-e-join-and-set-semantics)
    - [Audit F. Aggregation and grouping](review/audit.md#audit-f-aggregation-and-grouping)
    - [Audit G. IO, interchange and streaming](review/audit.md#audit-g-io-interchange-and-streaming)
    - [Audit H. Numeric, NULL and temporal semantics](review/audit.md#audit-h-numeric-null-and-temporal-semantics)
    - [Audit I. Error model](review/audit.md#audit-i-error-model)
- **[review/inventory.md](review/inventory.md)**
  - [B. API inventory and review](review/inventory.md#b-api-inventory-and-review)
    - [I/O](review/inventory.md#io)
    - [Basic relational ops](review/inventory.md#basic-relational-ops)
    - [Windows and ordered ops](review/inventory.md#windows-and-ordered-ops)
    - [Ordered grouping](review/inventory.md#ordered-grouping)
    - [Set algebra](review/inventory.md#set-algebra)
    - [Joins](review/inventory.md#joins)
    - [Aggregation](review/inventory.md#aggregation)
    - [Expression API](review/inventory.md#expression-api)
    - [Mutation](review/inventory.md#mutation)
    - [Cursor / streaming](review/inventory.md#cursor--streaming)
    - [Partitioning](review/inventory.md#partitioning)
    - [Linking](review/inventory.md#linking)
    - [Exceptions](review/inventory.md#exceptions)
    - [Added in v0.5](review/inventory.md#added-in-v05)
    - [Gaps found](review/inventory.md#gaps-found)
    - [Counts](review/inventory.md#counts)
- **[review/decisions.md](review/decisions.md)**
  - [D. Decisions on the eleven open semantic issues](review/decisions.md#d-decisions-on-the-eleven-open-semantic-issues)
    - [#202: Window output order](review/decisions.md#202-window-output-order)
    - [#148: Cursor and write-path order](review/decisions.md#148-cursor-and-write-path-order)
    - [#156: Pickle and API naming](review/decisions.md#156-pickle-and-api-naming)
    - [#218: `/`, `//` and `%`](review/decisions.md#218---and-)
    - [#221: Overflow](review/decisions.md#221-overflow)
    - [#222: `concat` schema](review/decisions.md#222-concat-schema)
    - [#228: Float with decimal columns](review/decisions.md#228-float-with-decimal-columns)
    - [#241: `decimal32`/`decimal64` with integers](review/decisions.md#241-decimal32decimal64-with-integers)
    - [#246: Cross-zone arithmetic](review/decisions.md#246-cross-zone-arithmetic)
    - [#247: Timestamp widening](review/decisions.md#247-timestamp-widening)
    - [#248: All-literal shared values](review/decisions.md#248-all-literal-shared-values)
- **[review/trade-offs.md](review/trade-offs.md)**
  - [Trade-off record](review/trade-offs.md#trade-off-record)
    - [Rewrites under row-wise demand](review/trade-offs.md#rewrites-under-row-wise-demand)
- **[review/impact-map.md](review/impact-map.md)**
  - [E. Canonical examples](review/impact-map.md#e-canonical-examples)
  - [F. Contract test matrix](review/impact-map.md#f-contract-test-matrix)
  - [G. Implementation impact map](review/impact-map.md#g-implementation-impact-map)
- **[review/history/adversarial-review.md](review/history/adversarial-review.md)**
  - [H. Adversarial review](review/history/adversarial-review.md#h-adversarial-review)
    - [Findings and dispositions](review/history/adversarial-review.md#findings-and-dispositions)
    - [Checklist coverage](review/history/adversarial-review.md#checklist-coverage)
    - [Checked and found sound](review/history/adversarial-review.md#checked-and-found-sound)
  - [I. Final consistency check](review/history/adversarial-review.md#i-final-consistency-check)
    - [Mechanical checks](review/history/adversarial-review.md#mechanical-checks)
    - [Problems found and fixed](review/history/adversarial-review.md#problems-found-and-fixed)
    - [Acceptance gate](review/history/adversarial-review.md#acceptance-gate)
- **[review/history/review-gate.md](review/history/review-gate.md)**
  - [J. API review gate](review/history/review-gate.md#j-api-review-gate)
    - [Findings and resolutions](review/history/review-gate.md#findings-and-resolutions)
    - [Owner decisions as applied](review/history/review-gate.md#owner-decisions-as-applied)
    - [Earlier records this revision changes](review/history/review-gate.md#earlier-records-this-revision-changes)
    - [Baseline behavior found while resolving V1 and V6b](review/history/review-gate.md#baseline-behavior-found-while-resolving-v1-and-v6b)
    - [Open at this revision](review/history/review-gate.md#open-at-this-revision)
  - [K. Contract closure pass](review/history/review-gate.md#k-contract-closure-pass)
    - [Findings and resolutions](review/history/review-gate.md#findings-and-resolutions-1)
    - [F16: what the prototype showed](review/history/review-gate.md#f16-what-the-prototype-showed)
    - [The `&` and `|` decision (D10)](review/history/review-gate.md#the--and--decision-d10)
    - [Agreement across the contract](review/history/review-gate.md#agreement-across-the-contract)
    - [Baseline behavior found during this pass](review/history/review-gate.md#baseline-behavior-found-during-this-pass)
    - [Owner decisions](review/history/review-gate.md#owner-decisions)
    - [Prerequisites linked to implementation](review/history/review-gate.md#prerequisites-linked-to-implementation)
    - [Open at this revision](review/history/review-gate.md#open-at-this-revision-1)
- **[review/history/review-closures.md](review/history/review-closures.md)**
  - [L. Third review closure](review/history/review-closures.md#l-third-review-closure)
    - [Findings and resolutions](review/history/review-closures.md#findings-and-resolutions)
    - [What the probes showed](review/history/review-closures.md#what-the-probes-showed)
    - [Consistency pass before commit](review/history/review-closures.md#consistency-pass-before-commit)
    - [Earlier records this revision changes](review/history/review-closures.md#earlier-records-this-revision-changes)
    - [Agreement across the contract](review/history/review-closures.md#agreement-across-the-contract)
    - [Baseline behavior found during this pass](review/history/review-closures.md#baseline-behavior-found-during-this-pass)
    - [Owner decisions](review/history/review-closures.md#owner-decisions)
    - [Open](review/history/review-closures.md#open)
  - [M. Fourth review closure](review/history/review-closures.md#m-fourth-review-closure)
    - [Findings and resolutions](review/history/review-closures.md#findings-and-resolutions-1)
    - [What the probes showed](review/history/review-closures.md#what-the-probes-showed-1)
    - [Baseline behavior found during this pass](review/history/review-closures.md#baseline-behavior-found-during-this-pass-1)
    - [Consistency pass before commit](review/history/review-closures.md#consistency-pass-before-commit-1)
    - [Earlier records this revision changes](review/history/review-closures.md#earlier-records-this-revision-changes-1)
    - [Agreement across the contract](review/history/review-closures.md#agreement-across-the-contract-1)
    - [Owner decisions](review/history/review-closures.md#owner-decisions-1)
    - [Open](review/history/review-closures.md#open-1)
  - [N. Fifth review closure](review/history/review-closures.md#n-fifth-review-closure)
    - [Findings and resolutions](review/history/review-closures.md#findings-and-resolutions-2)
    - [What the probes showed](review/history/review-closures.md#what-the-probes-showed-2)
    - [Baseline behavior found during this pass](review/history/review-closures.md#baseline-behavior-found-during-this-pass-2)
    - [Consistency pass before commit](review/history/review-closures.md#consistency-pass-before-commit-2)
    - [Earlier records this revision changes](review/history/review-closures.md#earlier-records-this-revision-changes-2)
    - [Agreement across the contract](review/history/review-closures.md#agreement-across-the-contract-2)
    - [Owner decisions](review/history/review-closures.md#owner-decisions-2)
    - [Open](review/history/review-closures.md#open-2)
  - [O. Sixth review closure](review/history/review-closures.md#o-sixth-review-closure)
    - [Findings and resolutions](review/history/review-closures.md#findings-and-resolutions-3)
    - [Earlier records this revision changes](review/history/review-closures.md#earlier-records-this-revision-changes-3)
    - [Agreement across the contract](review/history/review-closures.md#agreement-across-the-contract-3)
    - [Owner decisions](review/history/review-closures.md#owner-decisions-3)
    - [Open](review/history/review-closures.md#open-3)
- **[review/history/assessment-closure.md](review/history/assessment-closure.md)**
  - [P. Assessment closure pass](review/history/assessment-closure.md#p-assessment-closure-pass)
    - [Findings and resolutions](review/history/assessment-closure.md#findings-and-resolutions)
    - [Rule ownership (§1.6)](review/history/assessment-closure.md#rule-ownership-16)
    - [Every implicit conversion against the rules](review/history/assessment-closure.md#every-implicit-conversion-against-the-rules)
    - [What remains, and when it must be settled](review/history/assessment-closure.md#what-remains-and-when-it-must-be-settled)
    - [Earlier records this revision changes](review/history/assessment-closure.md#earlier-records-this-revision-changes)
    - [Agreement across the contract](review/history/assessment-closure.md#agreement-across-the-contract)
    - [Owner decisions](review/history/assessment-closure.md#owner-decisions)
    - [Final verification](review/history/assessment-closure.md#final-verification)
    - [Open](review/history/assessment-closure.md#open)
- **[review/history/owner-decision-closure.md](review/history/owner-decision-closure.md)**
  - [Q. Owner decision closure](review/history/owner-decision-closure.md#q-owner-decision-closure)
    - [Accepted](review/history/owner-decision-closure.md#accepted)
    - [Modified: OWNER MODIFICATION APPLIED](review/history/owner-decision-closure.md#modified-owner-modification-applied)
    - [Deferred](review/history/owner-decision-closure.md#deferred)
    - [What the modifications required](review/history/owner-decision-closure.md#what-the-modifications-required)
    - [Superseded](review/history/owner-decision-closure.md#superseded)
    - [Open](review/history/owner-decision-closure.md#open)

</details>

## About this reorganization

The split was made by line ranges of the two files at 20b4e5a, and is checked mechanically: removing what is listed below from the modules gives back the original lines, in order, with nothing repeated and nothing missing except the separators noted below. What changed:

- Each module starts with a header (title, navigation, status, scope, most-cited sections) and ends with a footer and link definitions. Each of these blocks is fenced by `<!-- v0.5-modular:... -->` comments.
- Section references such as [§17.4] and deliverable references such as [Deliverable Q] are now links. Their text is unchanged; two references to sections of the brief that preceded the contract (in [Deliverable H] and in the acceptance gate of [Deliverable I]) are left as text.
- The status blocks that opened the two files moved to [Status and scope](#status-and-scope). The table of deliverables in the review's status block links each letter to its module.
- Four mentions of the old file paths now name the new directories: the contract's "Companion" line, the review's "Status" line, the introduction of [Deliverable B], and [Deliverable C].
- Fifteen `---` separators of the contract are dropped: the one after its status block and those that closed the last chapter of a module, where the footer's rule takes their place. The separators between chapters inside a module stay. The review had none.
- [§23] spans two modules. The second opens with a heading "23. End-to-end examples (continued)" inside its header block.

Headings that repeat within one history module (for example "Findings and resolutions") get GitHub's numbered anchors (`#findings-and-resolutions-1`). Links to the original files that are pinned to a commit, such as those in PR #250 and in the issue comments that cite `blob/20b4e5a/...`, still show the single files as they were. Links that name a line (`v0.5-api-contract.md:1321`, `#L1321`) or a heading anchor of the old files on `main` land on the short stubs left at the old paths, which map each section to its module.

<!-- v0.5-modular:links -->

[§1]: contract/overview.md#1-overview
[§1.3]: contract/overview.md#13-design-principles
[§1.4]: contract/overview.md#14-relation-to-earlier-decisions
[§1.6]: contract/overview.md#16-rule-ownership
[§2]: contract/public-surface.md#2-exports
[§3]: contract/public-surface.md#3-object-model
[§3.1]: contract/public-surface.md#31-types
[§4]: contract/loading-and-laziness.md#4-loading
[§4.2]: contract/loading-and-laziness.md#42-ltseqread_csv
[§4.3]: contract/loading-and-laziness.md#43-ltseqread_parquet
[§4.4]: contract/loading-and-laziness.md#44-ltseqfrom_arrow
[§4.6]: contract/loading-and-laziness.md#46-ltseqfrom_dict-and-ltseqfrom_rows
[§4.7]: contract/loading-and-laziness.md#47-ltseqrange
[§5]: contract/loading-and-laziness.md#5-lazy-evaluation-and-materialization
[§5.1]: contract/loading-and-laziness.md#51-plan-building-and-execution
[§5.2]: contract/loading-and-laziness.md#52-ltseqcollect
[§5.3]: contract/loading-and-laziness.md#53-eager-calls
[§6]: contract/schema-and-table-operations.md#6-schema-and-types
[§6.1]: contract/schema-and-table-operations.md#61-schema-and-columns
[§6.2]: contract/schema-and-table-operations.md#62-supported-types
[§7]: contract/schema-and-table-operations.md#7-basic-table-operations
[§7.2]: contract/schema-and-table-operations.md#72-select
[§7.3]: contract/schema-and-table-operations.md#73-derive
[§7.5]: contract/schema-and-table-operations.md#75-drop
[§7.6]: contract/schema-and-table-operations.md#76-distinct
[§7.9]: contract/schema-and-table-operations.md#79-count-and-show
[§7.10]: contract/schema-and-table-operations.md#710-value-level-edits-insert-delete-update
[§8]: contract/expressions.md#8-expression-dsl
[§8.1]: contract/expressions.md#81-contexts-and-proxies
[§8.3]: contract/expressions.md#83-conditional-and-null-functions
[§8.4]: contract/expressions.md#84-math-functions
[§8.5]: contract/expressions.md#85-general-expr-methods
[§9]: contract/ordering.md#9-ordering-contract
[§9.1]: contract/ordering.md#91-order-state
[§9.2]: contract/ordering.md#92-sources-and-propagation
[§9.3]: contract/ordering.md#93-order-requirements
[§9.4]: contract/ordering.md#94-sort
[§9.5]: contract/ordering.md#95-assume_sorted
[§10]: contract/windows-and-grouping.md#10-windows-and-ordered-computation
[§10.1]: contract/windows-and-grouping.md#101-window-methods
[§10.2]: contract/windows-and-grouping.md#102-ranking-functions
[§10.3]: contract/windows-and-grouping.md#103-aggregates-over-windows
[§10.4]: contract/windows-and-grouping.md#104-over
[§10.5]: contract/windows-and-grouping.md#105-search_first
[§10.6]: contract/windows-and-grouping.md#106-search_pattern
[§10.7]: contract/windows-and-grouping.md#107-fold
[§11]: contract/windows-and-grouping.md#11-ordered-grouping
[§11.1]: contract/windows-and-grouping.md#111-group_ordered
[§11.2]: contract/windows-and-grouping.md#112-nestedtable
[§12]: contract/joins-and-sets.md#12-joins
[§12.1]: contract/joins-and-sets.md#121-join
[§12.3]: contract/joins-and-sets.md#123-asof_join
[§13]: contract/joins-and-sets.md#13-set-and-bag-operations
[§14]: contract/aggregation.md#14-aggregation-partitioning-and-pivot
[§14.1]: contract/aggregation.md#141-group_by-and-groupbyagg
[§14.2]: contract/aggregation.md#142-aggregate-expressions
[§14.3]: contract/aggregation.md#143-ltseqagg
[§14.5]: contract/aggregation.md#145-partition
[§14.6]: contract/aggregation.md#146-pivot
[§15]: contract/streaming-and-output.md#15-streaming
[§15.1]: contract/streaming-and-output.md#151-to_batches
[§15.2]: contract/streaming-and-output.md#152-iteration
[§16]: contract/streaming-and-output.md#16-output-and-interchange
[§16.2]: contract/streaming-and-output.md#162-to_pandas
[§16.3]: contract/streaming-and-output.md#163-to_dicts
[§16.4]: contract/streaming-and-output.md#164-arrow-pycapsule-stream
[§16.5]: contract/streaming-and-output.md#165-writers
[§16.6]: contract/streaming-and-output.md#166-pickle
[§17]: contract/numeric-null-temporal.md#17-numeric-semantics-and-literals
[§17.1]: contract/numeric-null-temporal.md#171-integer-and-decimal-results-are-checked
[§17.2]: contract/numeric-null-temporal.md#172-literals
[§17.3]: contract/numeric-null-temporal.md#173-arithmetic-operators
[§17.4]: contract/numeric-null-temporal.md#174-types-of-mixed-operands
[§17.5]: contract/numeric-null-temporal.md#175-shared-values
[§17.6]: contract/numeric-null-temporal.md#176-explicit-casts
[§18]: contract/numeric-null-temporal.md#18-null-nan-and-boolean-logic
[§19]: contract/numeric-null-temporal.md#19-temporal-semantics
[§19.2]: contract/numeric-null-temporal.md#192-arithmetic-and-comparison
[§19.4]: contract/numeric-null-temporal.md#194-dt-methods
[§19.5]: contract/numeric-null-temporal.md#195-clock-functions
[§20]: contract/errors-and-performance.md#20-errors
[§20.1]: contract/errors-and-performance.md#201-exception-classes
[§20.2]: contract/errors-and-performance.md#202-stages
[§21]: contract/errors-and-performance.md#21-performance-contract
[§21.1]: contract/errors-and-performance.md#211-materialization
[§21.2]: contract/errors-and-performance.md#212-fast-paths
[§22]: contract/api-reference.md#22-complete-canonical-api-reference
[§23]: contract/examples-sequences.md#23-end-to-end-examples
[§23.1]: contract/examples-sequences.md#231-sessions-with-group_ordered
[§23.9]: contract/examples-sequences.md#239-sequential-state-with-fold
[§23.10]: contract/examples-semantics.md#2310-streaming-and-interchange
[§23.18]: contract/examples-semantics.md#2318-arrow-and-pandas-round-trip
[§24]: contract/acceptance.md#24-acceptance-criteria-and-contract-test-matrix
[§24.1]: contract/acceptance.md#241-acceptance-criteria
[§24.3]: contract/acceptance.md#243-guarantee-matrix
[Deliverable A]: review/verdict.md#a-executive-verdict
[Deliverable B]: review/inventory.md#b-api-inventory-and-review
[Deliverable C]: review/verdict.md#c-the-contract
[Deliverable D]: review/decisions.md#d-decisions-on-the-eleven-open-semantic-issues
[Deliverable G]: review/impact-map.md#g-implementation-impact-map
[Deliverable H]: review/history/adversarial-review.md#h-adversarial-review
[Deliverable I]: review/history/adversarial-review.md#i-final-consistency-check
[Deliverable J]: review/history/review-gate.md#j-api-review-gate
[Deliverable K]: review/history/review-gate.md#k-contract-closure-pass
[Deliverable L]: review/history/review-closures.md#l-third-review-closure
[Deliverable M]: review/history/review-closures.md#m-fourth-review-closure
[Deliverable N]: review/history/review-closures.md#n-fifth-review-closure
[Deliverable O]: review/history/review-closures.md#o-sixth-review-closure
[Deliverable P]: review/history/assessment-closure.md#p-assessment-closure-pass
[Deliverable Q]: review/history/owner-decision-closure.md#q-owner-decision-closure

<!-- /v0.5-modular:links -->
