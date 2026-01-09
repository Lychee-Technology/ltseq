# LTSeq Development Roadmap

**Last Updated**: January 8, 2026  
**Current Status**: Core API Complete (585 tests passing)

---

## Overview

This roadmap tracks LTSeq's development from initial concept to production-ready library. LTSeq is a Rust-backed Python library for time-series/sequence data processing, built on DataFusion 51.0 with PyO3 bindings.

**Core Philosophy**: Treat data as ordered sequences rather than unordered sets, enabling powerful window functions and sequential operations.

---

## Current Status

| Feature Area | Status | Tests |
|-------------|--------|-------|
| Basic Relational | ✅ Complete | filter, select, derive, sort, distinct, slice |
| Window Functions | ✅ Complete | shift, rolling, diff, cum_sum |
| Ordered Computing | ✅ Complete | group_ordered, search_first |
| Linking/Joins | ✅ Complete | link, join, join_merge, lookup |
| Expression Extensions | ✅ Complete | if_else, fill_null, string ops, temporal ops |
| Set Algebra | ✅ Complete | union, intersect, diff, is_subset |
| GROUP BY Aggregation | ✅ Complete | agg() with grouping and aggregations |
| Pandas Dependency | ✅ Minimal | Only partition(by=callable) uses pandas |

---

## Completed Milestones

### Walking Skeleton & Data Foundation
**Status**: ✅ Complete

Established the Rust-Python bridge using uv and maturin. Data loads from CSV via Rust (DataFusion) and displays in Python.

**Key Deliverables**:
- Project structure with uv (Python) and cargo (Rust)
- `LTSeq` wrapper class and `RustTable` struct
- DataFusion SessionContext integration
- `LTSeq.read_csv()` and `LTSeq.show()` methods

```python
import ltseq
t = ltseq.LTSeq.read_csv("data/sample.csv")
t.show()
```

---

### Symbolic Expression Engine
**Status**: ✅ Complete

Lambda DSL that transforms Python lambda functions into Rust Logical Plans.

**Key Deliverables**:
- `SchemaProxy` and `Expr` classes using magic methods
- `PyExpr` enum in Rust for serialized expression trees
- `filter()` and `select()` APIs

```python
t = LTSeq.read_csv("users.csv")
t.filter(lambda r: r.age > 18).select(lambda r: [r.name]).show()
```

---

### Relative Position & Sequence Arithmetic
**Status**: ✅ Complete

Core "Sequence" capabilities: accessing previous rows and sliding windows.

**Key Deliverables**:
- `shift(n)` operator mapped to DataFusion LAG/LEAD
- `rolling(window).mean()` with WindowFrame
- Vector arithmetic: `r.price - r.price.shift(1)`

```python
t.derive(lambda r: {
    "growth": (r.price - r.price.shift(1)) / r.price.shift(1),
    "ma_3d":  r.price.rolling(3).mean()
}).show()
```

---

### State-Aware Grouping
**Status**: ✅ Complete

Ordered grouping for consecutive segment analysis.

**Key Deliverables**:
- `cum_sum()` operator for group ID generation
- `group_ordered(by=...)` returning `NestedTable`
- Group-level filter and derive operations

```python
groups = t.group_ordered(lambda r: r.trend_flag)
groups.filter(lambda g: g.count() > 3).show()
```

---

### Pointer-Based Linking
**Status**: ✅ Complete

Foreign key pointers replacing expensive hash joins with direct memory access.

**Key Deliverables**:
- `link(target, on=..., as_=...)` method
- Transparent materialization on linked column access
- All four join types (INNER, LEFT, RIGHT, FULL)
- Composite key support

```python
orders = LTSeq.read_csv("orders.csv")
products = LTSeq.read_csv("products.csv")

orders = orders.link(products, on=lambda o, p: o.pid == p.id, as_="prod")
orders.select(lambda r: [r.id, r.prod_name, r.prod_price]).show()
```

---

### Expression Extensions
**Status**: ✅ Complete

Rich expression capabilities for data transformation.

**Key Deliverables**:
- `if_else()` for conditional logic
- `fill_null()`, `is_null()`, `is_not_null()` for NULL handling
- String operations via `.s` accessor (8 methods)
- Temporal operations via `.dt` accessor (7 methods)
- `lookup()` pattern for simple foreign key lookups

```python
from ltseq.expr import if_else

t.derive(
    category=lambda r: if_else(r.price > 100, "expensive", "cheap"),
    year=lambda r: r.date.dt.year(),
    clean_name=lambda r: r.name.s.strip().s.lower()
)
```

---

### Set Algebra & Aggregation
**Status**: ✅ Complete

Set operations and GROUP BY aggregation.

**Key Deliverables**:
- `union()`, `intersect()`, `diff()`, `is_subset()`
- `agg(by=..., **aggregations)` for GROUP BY
- `rolling().std()` for volatility calculations

```python
# Set operations
all_sales = t1.union(t2)
repeat_customers = t1.intersect(t2, on=["cust_id"])

# Aggregation
totals = t.agg(
    by=lambda r: r.region,
    total_sales=lambda g: g.sales.sum(),
    avg_price=lambda g: g.price.avg()
)
```

---

### Pandas Migration
**Status**: ✅ Complete (99% migrated)

Migrated pandas-dependent operations to pure Rust/SQL.

**Migrated Operations**:
- `pivot()` → SQL GROUP BY with CASE WHEN
- `NestedTable.filter()` → SQL window functions
- `NestedTable.derive()` → SQL window functions
- `partition_by()` → SQL DISTINCT + WHERE

**Remaining** (by design):
- `partition(by=callable)` - Requires arbitrary Python callable execution

---

## Future Work

### Performance Optimizations
- SIMD vectorization for string/temporal operations
- Streaming aggregations for very large datasets
- Multi-core parallelization via Rust threading

### API Enhancements
- Object-style pointer access: `r.prod.name` (currently `r.prod_name`)
- Expression-based alternatives for callable-dependent operations
- Additional window function variants

### Ecosystem Integration
- Arrow IPC for zero-copy data exchange
- Parquet read/write support
- Integration with other Arrow-based tools

---

## Architecture Reference

### Hybrid Python-Rust Design
- **Python Layer**: API surface, DSL parsing, schema tracking
- **Rust Layer**: DataFusion execution, PyO3 bindings
- **Lazy Evaluation**: Operations return new instances; execution on terminal actions

### Key Design Decisions
1. **Strict Sort Requirement**: Window functions require explicit `.sort()` call
2. **Transparent Materialization**: Linked columns auto-materialize on access
3. **CallExpr Pattern**: Generic expression representation reduces boilerplate by ~40%

### File Structure
```
ltseq/
├── py-ltseq/          # Python package
│   ├── ltseq/
│   │   ├── core.py        # LTSeq class
│   │   ├── grouping.py    # NestedTable
│   │   ├── linking.py     # LinkedTable
│   │   └── expr/          # Expression system
│   └── tests/
└── src/               # Rust crate
    ├── lib.rs             # PyO3 bindings
    ├── ops/               # Operations
    ├── transpiler.rs      # Expression → SQL
    └── types.rs           # Type definitions
```

---

## References

- [API Reference](api.md) - Complete API documentation
- [API Extensions](API_EXTENSIONS.md) - Expression extensions reference
- [Linking Guide](LINKING_GUIDE.md) - Pointer-based linking user guide
- [Design Summary](DESIGN_SUMMARY.md) - Architecture and design decisions
