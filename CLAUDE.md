# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Development Commands

```bash
# Build the Rust extension (required after Rust changes)
maturin develop

# Run all tests
pytest py-ltseq/tests/ -v

# Run a single test file
pytest py-ltseq/tests/test_filter.py -v

# Run a specific test
pytest py-ltseq/tests/test_filter.py::test_filter_basic -v

# Check Rust code compiles
cargo check

# Run Rust tests (if any)
cargo test
```

## Architecture Overview

LTSeq is a sequence-oriented data processing library with a Rust core and Python bindings. It treats data as ordered sequences (not unordered sets), enabling window functions, sequential grouping, and ordered searches.

### Technology Stack
- **Rust Core**: DataFusion 51.0 (SQL engine), Apache Arrow (columnar format), PyO3 (Python bindings)
- **Python Layer**: Thin wrapper with expression DSL and mixin-based API organization
- **Build System**: Maturin for Rust→Python extension compilation

### Code Organization

```
src/                           # Rust kernel
├── lib.rs                     # LTSeqTable struct + single #[pymethods] block
├── ops/                       # Operation implementations (helper functions)
│   ├── basic.rs              # filter, select, search_first
│   ├── derive.rs             # derive, cum_sum
│   ├── window.rs             # shift, rolling, diff
│   ├── join.rs               # semi_join, anti_join
│   ├── set_ops.rs            # union, intersect, diff, distinct
│   ├── aggregation.rs        # SQL GROUP BY
│   └── ...
├── transpiler/               # PyExpr → DataFusion Expr conversion
│   ├── mod.rs                # Main transpilation logic
│   ├── sql_gen.rs            # SQL generation for window functions
│   └── optimization.rs       # Expression optimizations
└── cursor.rs                 # Streaming cursor for large datasets

py-ltseq/ltseq/               # Python package
├── core.py                   # LTSeq class (combines mixins)
├── expr/                     # Expression DSL
│   ├── proxy.py              # SchemaProxy for lambda capture
│   ├── base.py               # Expr base class
│   └── accessors.py          # .s (string) and .dt (datetime) accessors
├── grouping/                 # NestedTable for group_ordered()
│   └── proxies/              # GroupProxy for group aggregations
├── linking.py                # LinkedTable for pointer-based joins
├── partitioning.py           # PartitionedTable for partition()
└── [mixins].py               # IOMixin, TransformMixin, JoinMixin, etc.
```

### Key Design Patterns

**PyO3 Single #[pymethods] Constraint**: Rust only allows one `#[pymethods]` block per struct. All methods are defined in `lib.rs` as thin delegation stubs (1-3 lines) that call helper functions in `src/ops/`.

**Expression Transpilation**: Python lambdas → SchemaProxy captures → serialized dict → Rust deserializes → DataFusion Expr. The `_capture_expr()` method in Python and `dict_to_py_expr()` in Rust handle this pipeline.

**Mixin Composition**: The `LTSeq` class combines multiple mixins (IOMixin, TransformMixin, JoinMixin, etc.) to organize operations by category while maintaining a single user-facing class.

**Lazy Evaluation**: LinkedTable and NestedTable defer expensive operations (joins, grouping) until materialization is required.

### Expression System

The expression DSL allows Pythonic lambdas that get serialized and executed in Rust:

```python
# Lambda captured by SchemaProxy
t.filter(lambda r: r.age > 18)

# Becomes serialized dict
{"type": "BinOp", "op": ">", "left": {"type": "Column", "name": "age"}, "right": {"type": "Literal", "value": 18}}

# Transpiled to DataFusion in Rust
col("age").gt(lit(18))
```

Window functions require explicit `.sort()` before use. The sort order is tracked in `_sort_keys` and `sort_exprs`.

## Testing

Tests are in `py-ltseq/tests/`. Key test files:
- `test_filter.py`, `test_derive.py` - Basic operations
- `test_window.py`, `test_ranking.py` - Window functions
- `test_linking_*.py` - Pointer-based join tests
- `test_group_ordered.py` - Sequential grouping
- `test_set_ops.py` - Union, intersect, diff
