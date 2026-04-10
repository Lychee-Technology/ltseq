# Design: .pyi Type Stubs + Deprecated Method Removal

**Date**: 2026-04-05
**Issue**: [#8](https://github.com/Lychee-Technology/ltseq/issues/8)

## Summary

Add handwritten `.pyi` stub files for full IDE autocomplete and type checker support. Simultaneously remove three deprecated methods from source code.

## Scope

### Part 1: Remove deprecated methods

Remove from source and stubs:
- `join_merge()` in `joins.py` — superseded by `join(..., strategy="merge")`
- `join_sorted()` in `joins.py` — superseded by `join(..., strategy="merge")`
- `diff()` (set-difference alias) in `advanced_ops.py` — superseded by `except_()`

Breaking change accepted at current development stage.

### Part 2: Add .pyi stub files

Seven new files alongside their corresponding `.py` modules:

```
py-ltseq/ltseq/
├── __init__.pyi
└── expr/
    ├── __init__.pyi
    ├── base.pyi
    ├── core_types.pyi
    ├── types.pyi
    ├── accessors.pyi
    └── proxy.pyi
```

## Key Typing Decisions

### `SchemaProxy.__getattr__`
Returns `ColumnExpr` for any attribute access (the runtime also validates column existence, but statically we can only promise `ColumnExpr`):
```python
def __getattr__(self, name: str) -> ColumnExpr: ...
```

### `ColumnExpr` and `CallExpr` — dynamic method dispatch
Both classes use `__getattr__` at runtime to create `CallExpr` for arbitrary method names (e.g. `r.col.shift(1)`, `r.col.rolling(3).mean()`). In stubs, we:
1. Explicitly declare the most common methods for discoverability
2. Add `def __getattr__(self, name: str) -> Callable[..., CallExpr]` as fallback

Common methods to declare explicitly on `ColumnExpr` and `CallExpr`:
- Window ops: `shift(n)`, `rolling(window)`, `diff(n=1)`
- Aggregation ops: `sum()`, `mean()`, `min()`, `max()`, `count()`, `std()`, `var()`, `first()`, `last()`, `median()`
- Window chain targets: `.mean()`, `.sum()`, `.std()`, `.min()`, `.max()` (on `CallExpr` for rolling chain)

### `.s` and `.dt` accessors
Declared as `@property` on both `ColumnExpr` and `CallExpr`:
```python
@property
def s(self) -> StringAccessor: ...
@property
def dt(self) -> TemporalAccessor: ...
```

### `TemporalAccessor.diff`
Takes an `Expr` argument (not `int`), since it computes date difference between two datetime expressions:
```python
def diff(self, other: Expr, unit: str = ...) -> CallExpr: ...
```

### `LTSeq` in `__init__.pyi`
All mixin methods are merged into a single flat `LTSeq` class stub. Lambda parameters typed as `Callable[[SchemaProxy], Expr]` where a single-row lambda is expected. The `with_columns` alias for `derive` and `group_consecutive` alias for `group_ordered` are included.

### Factory functions (`__init__.pyi`)
All re-exported top-level functions (`if_else`, `row_number`, `rank`, `dense_rank`, `ntile`, math functions, datetime functions, etc.) are declared with full signatures.

## File-by-file Responsibilities

| File | Contents |
|------|----------|
| `expr/base.pyi` | `Expr` ABC with all operators + methods; all standalone functions |
| `expr/core_types.pyi` | `LiteralExpr`, `BinOpExpr`, `UnaryOpExpr` |
| `expr/accessors.pyi` | `StringAccessor` (21 methods), `TemporalAccessor` (10 methods) |
| `expr/types.pyi` | `ColumnExpr`, `CallExpr`, `WindowExpr`; explicit common methods + `__getattr__` fallback |
| `expr/proxy.pyi` | `SchemaProxy`, `NestedSchemaProxy` |
| `expr/__init__.pyi` | Re-exports all expr public symbols |
| `__init__.pyi` | Flat `LTSeq` class with all methods; all top-level exports |

## What Is NOT Stubbed

- Private methods (`_capture_expr`, `_schema`, `_inner`, `_sort_keys`, etc.)
- Internal helpers (`_from_rows`, `_execute_join`, `_filtering_join`, etc.)
- `GroupBy.agg` internal implementation (only public interface)
- `Cursor` class (streaming, less commonly used in IDE context — can be added later)
