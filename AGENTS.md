# AGENTS.md

Guidance for Claude Code (claude.ai/code) and other coding agents working in this repository.

## Build and development commands

```bash
# Build the Rust extension (required after Rust changes)
maturin develop

# Run all tests
pytest py-ltseq/tests/ -v

# Run a single test file
pytest py-ltseq/tests/test_derive.py -v

# Run a specific test by keyword
pytest py-ltseq/tests/test_derive.py -k test_derive_count -v

# Check Rust code compiles
cargo check

# Run Rust tests (if any)
cargo test
```

## Architecture overview

LTSeq is a sequence-oriented data processing library with a Rust core and Python bindings. It treats data as ordered sequences rather than unordered sets, which is what makes window functions, sequential grouping, and ordered searches possible.

### Technology stack

- Rust core: DataFusion 55.0 (SQL engine), Apache Arrow (columnar format), PyO3 0.29 (Python bindings)
- Python layer: thin wrapper with an expression DSL and mixin-based API organization
- Build system: Maturin compiles the Rust extension for Python

### Code organization

```
src/                           # Rust kernel
├── lib.rs                     # LTSeqTable struct + single #[pymethods] block
├── engine.rs                  # DataFusion session/context management
├── ops/                       # Operation implementations (helper functions)
│   ├── basic.rs              # filter, select, search_first
│   ├── derive.rs             # derive, cum_sum
│   ├── window.rs             # shift, rolling, diff
│   ├── join.rs               # semi_join, anti_join
│   ├── asof_join.rs          # as-of joins
│   ├── set_ops.rs            # union, intersect, diff, distinct
│   ├── aggregation.rs        # native GROUP BY aggregation
│   ├── grouping.rs           # group_ordered / group_id helpers
│   ├── linear_scan.rs        # linear-scan fast path for ordered predicates
│   ├── pivot.rs              # pivoting
│   └── ...                   # align, io, mutation, pattern_match, sort, ...
├── transpiler/               # PyExpr → DataFusion Expr conversion
│   ├── mod.rs                # Main transpilation logic
│   ├── window_native.rs      # Native window expression builders
│   └── optimization.rs       # Expression optimizations
└── cursor.rs                 # Streaming cursor for large datasets

py-ltseq/ltseq/               # Python package
├── core.py                   # LTSeq class (combines mixins)
├── expr/                     # Expression DSL
│   ├── proxy.py              # SchemaProxy for lambda capture
│   ├── base.py               # Expr base class
│   ├── accessors.py          # .s (string) and .dt (datetime) accessors
│   └── lookup_expr.py        # cross-table lookup expressions
├── grouping/                 # NestedTable for group_ordered()
│   └── proxies/              # DeriveGroupProxy (derive exprs) / FilterGroupProxy (group predicates)
├── linking.py                # LinkedTable for lazy prefix-aliased joins
├── partitioning.py           # PartitionedTable for partition()
├── io_ops.py                 # IOMixin
├── transforms.py             # TransformMixin
├── joins.py                  # JoinMixin
├── aggregation.py            # AggregationMixin
├── advanced_ops.py           # SetOpsMixin, AdvancedOpsMixin
├── mutation_mixin.py         # MutationMixin
└── lookup.py                 # LookupMixin (mixed into TransformMixin)
```

### Key design patterns

**PyO3 single #[pymethods] constraint**: Rust only allows one `#[pymethods]` block per struct. Most methods are defined in `lib.rs` as thin delegation stubs (1-3 lines) that call helper functions in `src/ops/`; constructors, IO/terminal methods, and a few basics remain inline (see ADR 0012). Don't assume the logic lives in `lib.rs`.

**Expression transpilation**: Python lambdas → SchemaProxy captures → serialized dict → Rust deserializes → DataFusion Expr. The `_capture_expr()` method in Python and `dict_to_py_expr()` in Rust handle this pipeline.

**Mixin composition**: The `LTSeq` class combines multiple mixins (IOMixin, TransformMixin, JoinMixin, etc.) so operations are organized by category while users still see a single class.

**Lazy evaluation**: LinkedTable and NestedTable are deferred wrappers. Constructing one does no join or grouping work; the cost lands when something consumes them. What that consumption costs varies by operation, and the split does not follow the wrapper boundary: most relational transforms stay on the lazy DataFusion plan, while several documented paths collect mid-plan. Two that are easy to misjudge: keyed `distinct` on a linked table snapshots the joined plan (`set_ops.rs::snapshot_single_partition`), and `first().count()` on a shift-based predicate outside pre-sorted Parquet runs `general_linear_scan_group_id`, which collects. So do not infer from "the wrapper is lazy" that a given call is. ADR 0005 enumerates every eager boundary; check it before estimating the cost of a pipeline rather than relying on this summary.

**No materialization rule**: Any API that returns `LTSeq`, `NestedTable`, `LinkedTable`, or `PartitionedTable` must stay on the Rust/DataFusion query path. Do not call `to_pandas()`, `to_arrow()`, `from_arrow()`, `from_pandas()`, or `_from_rows()` inside table-returning query APIs. Internal materialization is only allowed in explicit export or construction APIs such as `to_pandas()`, `to_arrow()`, `to_dicts()`, `collect()`, `from_arrow()`, and `from_pandas()`. There are two documented exceptions. First, physical-position ops (`rvs`, `step`, keyed `distinct`) snapshot the table into a single in-order partition (collect → read_batch) before assigning row positions, because an unordered or partitioned window over a lazy multi-partition plan does not preserve input order; the snapshot is required for correctness, not a shortcut (see `set_ops.rs::snapshot_single_partition`). Second, `fold()` runs a user-supplied Python callback `fn(state, row)` per row to thread sequential state; arbitrary Python cannot be expressed as a DataFusion plan, so the row-wise Python path (`to_dicts()` → accumulate → `_from_rows()`) is inherent to the operation, not a shortcut. Its docstring flags it as a non-lazy slow path; `docs/api.md` and ADR 0005 compare it to Polars `cumulative_eval`. These are the two correctness exceptions; ADR 0005 is the authoritative inventory of all documented eager boundaries, including implementation-status ones such as non-Parquet `assume_sorted`, `asof_join`, `pivot`, the mutation APIs, `search_pattern`, and the general `linear_scan` path.

### Expression system

The expression DSL allows Pythonic lambdas that get serialized and executed in Rust:

```python
# Lambda captured by SchemaProxy
t.filter(lambda r: r.age > 18)

# Becomes serialized dict
{"type": "BinOp", "op": ">", "left": {"type": "Column", "name": "age"}, "right": {"type": "Literal", "value": 18}}

# Transpiled to DataFusion in Rust
col("age").gt(lit(18))
```

Sequence window functions default to table order and require a prior `.sort()`, or `.assume_sorted()` for data that is already physically sorted. Ranking functions and windows with an explicit `.over(order_by=...)` carry their own order and need no prior sort. The declared order is owned by the Rust kernel (`sort_specs`); Python's `_sort_keys` reads it over FFI.

## Testing

Tests are in `py-ltseq/tests/`. Key test files:
- `test_core_api.py`, `test_derive.py`, `test_expr.py` - Basic operations and the expression DSL
- `test_window_over.py`, `test_window_partition_by.py`, `test_ranking.py` - Window functions
- `test_linking_*.py` - Lazy prefix-aliased join (link) tests
- `test_group_ordered.py`, `test_group_sorted.py` - Sequential grouping
- `test_set_ops.py` - Union, intersect, diff
- `test_no_materialization_rule.py` - Guards the no-materialization rule described above


## Non-code artifacts

Anything produced while working an issue that is not code must end up on GitHub, not just on disk. This covers design drafts, specs, implementation plans, research notes, investigation and verification notes, assessments, and rulings made mid-execution. Post each one as a comment on the relevant issue, not as files committed to the repo. If the work has no issue yet, create one first; if the artifact is about changes already under review, post it to the PR instead.

- Write non-code artifacts in English by default.
- Post the full content, not a summary or a file path, and post it when it is produced: a design draft goes up as a draft (say so), a plan goes up when written, a mid-execution decision goes up the moment it is taken. The issue is the complete decision record; nothing load-bearing may live only in a chat transcript or a local file.
- A local working copy is fine, but it is invisible to everyone else and does not survive the branch. Several child repos keep planning notes in gitignored local directories (for example `__ref__/plan/` in `ltbase.api`, see #497). Do not force-add gitignored planning files to make them shareable; the issue comment is the sharing mechanism.
- Say in the comment which artifact it is and where the working copy lives, so a later reader knows whether they are looking at a plan, a spec, or a review.

Scope: per-issue artifacts only. Reference documentation of the system itself (e.g. `docs/ARCHITECTURE.md`, `docs/MODULE_GUIDE.md`) lives in `docs/` and is committed as before. Anything that must become a durable repository convention also belongs in `docs/` (an ADR, runbook, or reference page): the issue comment records the thinking, and `docs/` records the decision. Review artifacts belong on the PR; see PR rules.

## PR rules

- Do not merge a PR unless I explicitly ask you to.
- When reviewing a PR, post everything (findings, spec and standards checks, assessment, observations, verification, summary) as one comment on the PR.
- When I ask you to merge a PR, squash-merge by default unless I ask for something else.
- After a PR is merged, clean up local branches and worktrees, fast-forward main, then update and close related issues.

## Git conventions

Never include AI attribution in commit messages, PR titles, or PR descriptions, in any form. That means no

- `Co-Authored-By: ...`
- `Generated with ...` footers
- sign-offs or footers naming an LLM or AI agent (OpenAI, GPT, Claude, Anthropic, and the like)

When squash-merging, write a clean commit message that describes only the change itself.
