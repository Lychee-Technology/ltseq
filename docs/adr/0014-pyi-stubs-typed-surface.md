# ADR 0014: Handwritten `.pyi` Stubs for the Dynamic Surface + Deprecated Method Removal

- Status: Accepted
- Decision date: 2026-04-05 (design spec) · Recorded: 2026-07-26
- Issue: [#8](https://github.com/Lychee-Technology/ltseq/issues/8)

[中文版](0014-pyi-stubs-typed-surface.cn.md)

## Context

The expression DSL is dynamic by design ([ADR 0007](0007-lambda-dsl-schemaproxy.md)): `SchemaProxy`, `ColumnExpr`, and `CallExpr` all rely on `__getattr__` at runtime, so IDEs and type checkers see nothing. At the same time, three deprecated methods lingered in the API.

## Decision

**1. Handwritten `.pyi` stub files** shipped alongside the `.py` modules — pure type declarations, no runtime change. Key typing decisions:

- `SchemaProxy.__getattr__` statically promises an expression type for any attribute (runtime still validates column existence). Spec said `-> ColumnExpr`; today's stub returns a `_SchemaAttr(ColumnExpr)` subclass that also types nested access (`expr/proxy.pyi`).
- **Explicit-plus-fallback on `ColumnExpr`/`CallExpr`**: declare the most common methods explicitly for discoverability (window ops `shift`/`rolling`/`diff`; aggregations `sum`/`mean`/`min`/`max`/`count`/`std`/`var`/`first`/`last`/`median`; rolling-chain targets), then add a `__getattr__` fallback. This is the central trade-off: the explicit list buys IDE discoverability, the fallback preserves the dynamic runtime — at the cost that anything *not* listed type-checks as valid. The spec typed the fallback `-> Callable[..., CallExpr]`; the current stubs use `-> Any` (`expr/types.pyi`), loosening it further.
- `.s` / `.dt` accessors declared as `@property` returning `StringAccessor`/`TemporalAccessor`; `TemporalAccessor.diff(other: Expr, unit)` takes an `Expr`, not an `int`.
- **The stub flattens the mixins**: runtime `LTSeq` is mixin-composed ([ADR 0012](0012-rust-thin-shell-python-mixins.md)), but `__init__.pyi` declares one flat `LTSeq` class with all methods — a deliberate divergence between declared and runtime structure, chosen for IDE ergonomics. Lambdas are typed `Callable[[SchemaProxy], Expr]`; aliases (`with_columns`, `group_consecutive`) are included; factory functions get full signatures.
- The spec excluded private methods and internal helpers from stubbing; the current stubs do declare a handful used across module boundaries (`_inner`, `_schema`, `_sort_keys`, `_from_rows`, `_capture_expr` in `__init__.pyi`).

**2. Remove three deprecated methods** in the same change (breaking change accepted at the development stage): `join_merge()` and `join_sorted()` (superseded by `join(..., strategy="merge")`) and the set-difference alias `diff()` (superseded by `except_()`). Removal is TDD'd via `py-ltseq/tests/test_deprecated_removed.py`.

### Implementation deviations from the spec (recorded as-built)

- Scope grew from the spec's 7 stub files to 14 in the implementation plan; the package today ships **15** (adding `cursor.pyi`, `linking.pyi`, `partitioning.pyi`, `aggregation.pyi`, `grouping/nested_table.pyi`, `expr/lookup_expr.pyi`, `exceptions.pyi`, `ltseq_core.pyi`, …).
- `Cursor` **was** stubbed, although the spec's "not stubbed" list excluded it.
- Stubs use `TYPE_CHECKING` guards to break circular imports (stated only in the implementation plan).
- Stubs have no unit tests of their own, but CI now runs **pyright as a gate** — over the library (`py-ltseq/ltseq`) and over dedicated typecheck tests (`py-ltseq/typecheck_tests`), see `.github/workflows/ci.yml` (the spec-era state had no type-checker gate).

### Related ergonomics decisions

- **Compatibility aliases** lower migration cost from pandas/Polars: `with_columns` (= `derive`), `descending=` (= `desc=`), `group_consecutive` (= `group_ordered`), `except_`/`subtract`, `alias`/`as_` on `link`; plus a "Migrating from Pandas" appendix in `api.md`.
- **Exception hierarchy with dual inheritance**: all errors derive from `LTSeqError`, and each concrete error also subclasses the builtin it historically raised — `SortRequiredError(LTSeqError, ValueError)`, `SchemaMismatchError(LTSeqError, ValueError)`, `ColumnNotFoundError(LTSeqError, ValueError, AttributeError)` (the `AttributeError` specifically keeps `hasattr()` probes working). Error messages carry a fix hint; `api.md` maintains an Error→Cause→Solution table.

## Consequences

- Full IDE autocomplete and mypy/pyright support over a runtime that remains fully dynamic.
- Stubs are hand-maintained: every new public method must be added in (at least) the mixin, the stub, and the docs — a standing synchronization cost with no automated check.

## Sources

- `docs/superpowers/specs/2026-04-05-pyi-stubs-design.md` — design spec
- `docs/superpowers/plans/2026-04-05-pyi-stubs.md` — implementation plan (deviations, full stub sources)
- `docs/api.md` — Appendix A (pandas migration), Common Errors and Solutions, Exception Hierarchy
- `README.md` — Developer Experience
- `py-ltseq/ltseq/*.pyi`, `py-ltseq/tests/test_deprecated_removed.py`
