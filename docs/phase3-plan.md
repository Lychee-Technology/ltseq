# Phase 3 Plan: Python AST Builder (Symbolic Expression Engine)

## Executive Summary

**Phase 3** implements the Python-side symbolic expression engine that captures lambda functions as serializable AST (Abstract Syntax Trees). This critical infrastructure enables:

- Lambda expressions like `lambda r: r.age > 18` to be **captured without executing**
- Expression trees to be **serialized to dictionaries** for Rust transpilation
- **Method chaining** like `r.col.shift(1).rolling(3).mean()` to build complex operations
- **Type inference** during expression serialization

### Current Status

The expression system (`expr.py`) is **already 95% complete** from the project scaffolding! Phase 3 focuses on:

1. ✅ Verify existing implementations work correctly
2. ✅ Add missing expression types and methods (strings, temporal, null handling)
3. ✅ Enhance Python bindings to use expressions properly
4. ✅ Create comprehensive test suite

---

## Understanding the Current Expression System

The existing `expr.py` (520+ lines) already provides:

| Class | Purpose | Status |
|-------|---------|--------|
| `Expr` (ABC) | Base class for all expressions | ✅ Complete |
| `ColumnExpr` | References like `r.age` | ✅ Complete |
| `LiteralExpr` | Constants like `42`, `"hello"` | ✅ Complete |
| `BinOpExpr` | Binary ops: `+`, `-`, `>`, `&`, `\|`, etc. | ✅ Complete |
| `UnaryOpExpr` | Unary ops: `~` (not) | ✅ Complete |
| `CallExpr` | Method calls: `.shift(1)`, `.rolling(3)` | ✅ Complete |
| `SchemaProxy` | Row proxy `r` in lambdas | ✅ Complete |
| `_lambda_to_expr()` | Captures lambda → serializes | ✅ Complete |

### Supported Operations (Already Implemented)

**Arithmetic Operators:**
- `__add__` (+), `__sub__` (-), `__mul__` (*), `__truediv__` (/), `__mod__` (%), `__floordiv__` (//)

**Comparison Operators:**
- `__eq__` (==), `__ne__` (!=), `__lt__` (<), `__le__` (<=), `__gt__` (>), `__ge__` (>=)

**Logical Operators:**
- `__and__` (&), `__or__` (|), `__invert__` (~)

**Right-Hand Operators:**
- `__radd__`, `__rsub__`, `__rmul__`, `__rtruediv__`, `__rfloordiv__`, `__rmod__` (for reversed operations like `5 + r.col`)

**Type System:**
- Type coercion via `_coerce()` (converts literals to `LiteralExpr`)
- Dtype inference: bool, int, float, string, null

**Method Chaining:**
- Via `__getattr__` - enables `r.col.shift(1).rolling(3).mean()` syntax

### Existing Test Coverage

Comprehensive test suites already written:
- `test_expr.py` (10,470 bytes) - Expression capture and serialization
- `test_schema_proxy.py` (11,653 bytes) - SchemaProxy validation
- `test_integration.py` (13,868 bytes) - Expression integration with LTSeq
- `test_ltseq_methods.py` (11,653 bytes) - Method integration

---

## Phase 3 Objectives

### Objective 1: Verify Existing Expression System

**Tasks:**
- [ ] Run all existing expression tests (`test_expr.py`, `test_schema_proxy.py`, `test_integration.py`)
- [ ] Verify 100% pass rate
- [ ] Document coverage
- [ ] Fix any failures (expected: minimal)

**Success Criteria:**
- All existing tests pass
- 90%+ code coverage in `expr.py`

### Objective 2: Enhance Expression System (Minor Additions)

The existing system handles basic operations. Phase 3 should validate and document:

#### 2.1 String Methods

These already work via `__getattr__` → `CallExpr`, just need testing:

```python
# Signature examples (from api.md)
r.s.contains(substring)        # CallExpr("contains", ...)
r.s.starts_with(prefix)        # CallExpr("starts_with", ...)
r.s.slice(start, end)          # CallExpr("slice", ...)
r.s.regex_match(pattern)       # CallExpr("regex_match", ...)
r.s.upper()                    # CallExpr("upper", ...)
r.s.lower()                    # CallExpr("lower", ...)
r.s.length()                   # CallExpr("length", ...)
```

#### 2.2 Temporal Methods

These already work via `__getattr__` → `CallExpr`:

```python
# Signature examples (from api.md)
r.dt.year()                    # CallExpr("year", ...)
r.dt.month()                   # CallExpr("month", ...)
r.dt.day()                     # CallExpr("day", ...)
r.dt.hour()                    # CallExpr("hour", ...)
r.dt.add_days(n)               # CallExpr("add_days", ...)
r.dt.diff(other_dt)            # CallExpr("diff", ...)
r.dt.format(fmt)               # CallExpr("format", ...)
```

#### 2.3 Null Handling

These already work via `__getattr__` → `CallExpr`:

```python
# Signature examples (from api.md)
r.col.is_null()                # CallExpr("is_null", ...)
r.col.fill_null(val)           # CallExpr("fill_null", ...)
r.col.coalesce(other)          # CallExpr("coalesce", ...)
```

#### 2.4 Utility Functions

New helpers to implement:

```python
# Helper function
from ltseq.expr import if_else

if_else(condition, true_val, false_val)  # Special function
```

**Implementation Strategy:**
- No changes to `Expr` base class (already generic)
- String/Temporal/Null methods already work via `__getattr__` → `CallExpr`
- Only need to **document and test** that they work
- Add `if_else()` utility function if not present

### Objective 3: Python Bindings Enhancement

The Python wrapper (`__init__.py`) currently has stubs for:
- `filter(predicate)` - captures filter expression
- `select(*cols)` - captures column selection/computation
- `derive(**kwargs)` - captures derived column expressions

**Phase 3 Tasks:**
- [ ] Ensure `_capture_expr()` properly invokes `_lambda_to_expr()`
- [ ] Test that filter, select, derive capture expressions correctly
- [ ] Add comprehensive error handling and validation
- [ ] Verify schema is required before expression capture
- [ ] Test error messages are clear and actionable

**Success Criteria:**
- All three methods properly capture and serialize expressions
- Schema validation prevents invalid column access
- Error messages are helpful (e.g., "Column 'nonexistent' not found in schema: [id, name, age]")

### Objective 4: Comprehensive Testing Suite

**Create new test file: `test_phase3_expression_system.py`**

Tests to include (50-100 new test cases):

**4.1 String Method Expressions:**
- [ ] `r.s.contains()` serializes correctly
- [ ] `r.s.starts_with()` serializes correctly
- [ ] `r.s.slice()` serializes correctly
- [ ] `r.s.regex_match()` serializes correctly
- [ ] String methods in filter expressions
- [ ] String methods in derived expressions
- [ ] Chained string methods: `r.s.upper().contains("A")`

**4.2 Temporal Method Expressions:**
- [ ] `r.dt.year()` serializes correctly
- [ ] `r.dt.month()` serializes correctly
- [ ] Temporal methods in filter expressions
- [ ] Temporal methods in derived expressions

**4.3 Null Handling Expressions:**
- [ ] `r.col.is_null()` serializes correctly
- [ ] `r.col.fill_null()` serializes correctly
- [ ] Null checks in filter expressions

**4.4 Complex Nested Expressions:**
- [ ] Method chaining: `r.col.shift(1).rolling(3).mean()`
- [ ] Nested operations: `(r.age > 18) & (r.salary > 50000)`
- [ ] Mixed types: `(r.name.contains("John")) | (r.age < 30)`
- [ ] Complex arithmetic: `(r.price * r.qty) - (r.discount / 100)`

**4.5 Integration Tests:**
- [ ] Load CSV → capture filter expression → serialize
- [ ] Load CSV → capture select expression → serialize
- [ ] Load CSV → capture derive expression → serialize
- [ ] Method chaining of operations
- [ ] Multiple data types in single expression

**4.6 Error Handling Tests:**
- [ ] Non-existent column raises clear error
- [ ] Non-expression return raises clear error
- [ ] Empty schema validation
- [ ] Type mismatch handling

### Objective 5: Documentation & Examples

**Create documentation:**
- [ ] Expression types documentation
- [ ] Lambda DSL user guide
- [ ] Serialization format specification
- [ ] Examples and cookbook
- [ ] API reference with method signatures

---

## Phase 3 Architecture

```
Phase 3 Symbolic Expression Engine
│
├─ Python Side (Expression Capture)
│  │
│  ├─ expr.py (EXISTING, 95% complete)
│  │  ├─ Expr (base class with magic methods)
│  │  ├─ ColumnExpr (column references)
│  │  ├─ LiteralExpr (constants with dtype inference)
│  │  ├─ BinOpExpr (binary operations)
│  │  ├─ UnaryOpExpr (unary operations)
│  │  ├─ CallExpr (method calls with chaining)
│  │  ├─ SchemaProxy (row proxy for lambdas)
│  │  └─ _lambda_to_expr() (capture & serialize)
│  │
│  └─ __init__.py (ENHANCE)
│     ├─ LTSeq._capture_expr() → invokes _lambda_to_expr()
│     ├─ LTSeq.filter() → captures predicate → serializes
│     ├─ LTSeq.select() → captures columns → serializes
│     └─ LTSeq.derive() → captures expressions → serializes
│
├─ Serialization Format (JSON-like dicts)
│  │
│  ├─ Column: {"type": "Column", "name": "age"}
│  ├─ Literal: {"type": "Literal", "value": 18, "dtype": "Int64"}
│  ├─ BinOp: {
│  │  "type": "BinOp",
│  │  "op": "Gt",
│  │  "left": {"type": "Column", "name": "age"},
│  │  "right": {"type": "Literal", "value": 18, "dtype": "Int64"}
│  │}
│  ├─ UnaryOp: {
│  │  "type": "UnaryOp",
│  │  "op": "Not",
│  │  "operand": {...}
│  │}
│  └─ Call: {
│     "type": "Call",
│     "func": "shift",
│     "args": [{"type": "Literal", "value": 1, "dtype": "Int64"}],
│     "kwargs": {},
│     "on": {"type": "Column", "name": "price"}
│  }
│
└─ Test Suite (VERIFY & ENHANCE)
   ├─ test_expr.py (existing 376+ tests) ✅
   ├─ test_schema_proxy.py (existing tests) ✅
   ├─ test_integration.py (existing tests) ✅
   ├─ test_ltseq_methods.py (existing tests) ✅
   └─ test_phase3_expression_system.py (NEW 50-100 tests)
```

---

## Phase 3 Tasks & Implementation Plan

### Task 1: Run & Validate Existing Tests
**Estimated Time**: 2-3 hours

**Steps:**
1. [ ] Run `test_expr.py` - verify all 376+ tests pass
2. [ ] Run `test_schema_proxy.py` - verify all tests pass
3. [ ] Run `test_integration.py` - verify all tests pass
4. [ ] Run `test_ltseq_methods.py` - verify all tests pass
5. [ ] Identify and fix any failures
6. [ ] Document test coverage percentage

**Success Criteria:**
- 100% pass rate on all existing tests
- No test failures or warnings
- Coverage > 90% for `expr.py`

### Task 2: Enhance Expression Documentation
**Estimated Time**: 2-3 hours

**Steps:**
1. [ ] Add docstrings to SchemaProxy for string methods (e.g., `.s.contains()`)
2. [ ] Add docstrings to SchemaProxy for temporal methods (e.g., `.dt.year()`)
3. [ ] Add docstrings to SchemaProxy for null-handling methods (e.g., `.is_null()`)
4. [ ] Add usage examples for each method group
5. [ ] Create METHOD_REFERENCE.md documenting all available methods

**Output:**
- Enhanced docstrings in `expr.py`
- New `docs/method_reference.md` file
- Examples for each method category

### Task 3: Implement `if_else()` Utility Function
**Estimated Time**: 1-2 hours

**Steps:**
1. [ ] Add `if_else(cond, true_val, false_val)` function to `expr.py`
2. [ ] Implement as special `CallExpr` or custom function
3. [ ] Test serialization
4. [ ] Document usage

**Example Usage:**
```python
from ltseq.expr import if_else

t.derive(
    age_group=lambda r: if_else(r.age < 18, "minor", "adult")
)
```

### Task 4: Create Comprehensive Test Suite
**Estimated Time**: 3-4 hours

**Create new file: `test_phase3_expression_system.py`**

**Test Classes to Implement:**
1. [ ] `TestStringMethodExpressions` (10-15 tests)
2. [ ] `TestTemporalMethodExpressions` (10-15 tests)
3. [ ] `TestNullHandlingExpressions` (5-8 tests)
4. [ ] `TestComplexNestedExpressions` (10-15 tests)
5. [ ] `TestExpressionIntegration` (10-15 tests)
6. [ ] `TestErrorHandling` (5-10 tests)

**Total: 50-80 new test cases**

**Success Criteria:**
- All new tests pass
- High coverage of expression scenarios
- Tests are well-documented

### Task 5: Enhance Python Bindings
**Estimated Time**: 2-3 hours

**Steps:**
1. [ ] Review `__init__.py` implementation of `filter()`, `select()`, `derive()`
2. [ ] Verify `_capture_expr()` works correctly
3. [ ] Add better error messages for missing schema
4. [ ] Add validation that columns exist before capturing expressions
5. [ ] Test end-to-end: CSV load → expression capture → serialization

**Test Cases to Add:**
- [ ] Filter with simple predicate
- [ ] Filter with complex predicate
- [ ] Select with column names
- [ ] Select with lambda expressions
- [ ] Derive with single column
- [ ] Derive with multiple columns
- [ ] Error: schema not initialized
- [ ] Error: invalid column name
- [ ] Error: non-expression lambda return

### Task 6: Integration Testing
**Estimated Time**: 2-3 hours

**Steps:**
1. [ ] Load CSV with `LTSeq.read_csv()`
2. [ ] Capture and serialize filter expression
3. [ ] Capture and serialize select expression
4. [ ] Capture and serialize derive expression
5. [ ] Test with multiple data types
6. [ ] Test method chaining
7. [ ] Verify serialization format matches expected structure

**Test Scenarios:**
- [ ] Simple filter: `lambda r: r.age > 18`
- [ ] Complex filter: `lambda r: (r.age > 18) & (r.salary > 50000)`
- [ ] String filter: `lambda r: r.name.contains("John")`
- [ ] Date filter: `lambda r: r.date.year() == 2024`
- [ ] Null filter: `lambda r: ~r.optional_field.is_null()`
- [ ] Select columns: `select("id", "name", "age")`
- [ ] Select computed: `select(lambda r: [r.id, r.price * r.qty])`
- [ ] Derive: `derive(total=lambda r: r.price * r.qty)`
- [ ] Chained operations: `filter(...).select(...).derive(...)`

### Task 7: Documentation & User Guide
**Estimated Time**: 2-3 hours

**Create/Update Documentation Files:**

1. [ ] **docs/phase3_guide.md** - Complete Phase 3 guide
   - Expression system overview
   - Lambda DSL syntax
   - Method reference
   - Examples and cookbook

2. [ ] **docs/expression_format.md** - Serialization specification
   - BinOp format
   - UnaryOp format
   - CallExpr format
   - Examples for each type

3. [ ] **docs/lambda_dsl.md** - User guide for lambdas
   - Writing filter predicates
   - Writing select expressions
   - Writing derive expressions
   - Common patterns
   - Error handling

4. [ ] Update **README.md** with Phase 3 features
   - Link to expression guide
   - Quick examples
   - Supported operations

**Examples to Include:**
- Basic filtering: `t.filter(lambda r: r.age > 18)`
- Complex filtering: `t.filter(lambda r: (r.age > 18) & (r.name.contains("John")))`
- Column selection: `t.select("id", "name")`
- Computed selection: `t.select(lambda r: [r.id, r.price * r.qty])`
- Derived columns: `t.derive(total=lambda r: r.price * r.qty)`
- Conditional derivation: `t.derive(category=lambda r: if_else(r.price > 100, "expensive", "cheap"))`
- Method chaining: `t.filter(...).select(...).derive(...).show()`

---

## Design Decisions & Rationale

### Decision 1: Expression System Scope
**Question**: Should Phase 3 implement ALL expression features (strings, temporal, aggregations)?

**Decision**: Focus on **core + strings + temporal**
- ✅ All scaffolded and mostly working
- ✅ Will be thoroughly tested
- ⚠️ Aggregations defer to Phase 5
- ⚠️ Custom operators defer to Phase 6

**Rationale**: Aggregations require group semantics (Phase 7), custom operators need Rust support.

### Decision 2: Error Handling Strategy
**Question**: How strict should expression capture be?

**Decision**: **Strict** - fail fast with clear errors
- Current implementation raises `AttributeError` for unknown columns
- Current implementation raises `TypeError` for non-Expr returns

**Rationale**: Phase 4 needs precise expression info; ambiguity causes silent bugs.

**Example Error Messages:**
```
AttributeError: Column 'nonexistent' not found in schema.
Available columns: ['id', 'name', 'age', 'salary']

TypeError: Lambda must return an Expr, got <class 'int'>.
Did you forget to use the 'r' parameter?
```

### Decision 3: Serialization Format
**Question**: Should we optimize serialization, or keep it human-readable?

**Decision**: **Human-readable JSON-compatible dicts** for Phase 3
- Easier debugging
- Easier to validate format
- Can optimize in Phase 7+ if needed

**Example:**
```python
{
    "type": "BinOp",
    "op": "Gt",
    "left": {"type": "Column", "name": "age"},
    "right": {"type": "Literal", "value": 18, "dtype": "Int64"}
}
```

### Decision 4: Testing Approach
**Question**: Unit tests only, or include integration tests?

**Decision**: **Both**
- Unit tests for individual expression types
- Integration tests for full pipeline (CSV → capture → serialize → display)
- Performance benchmarks optional (defer to Phase 7)

---

## Serialization Format Specification

### Column Expression
```json
{
    "type": "Column",
    "name": "age"
}
```

### Literal Expression
```json
{
    "type": "Literal",
    "value": 18,
    "dtype": "Int64"
}
```

### Binary Operation Expression
```json
{
    "type": "BinOp",
    "op": "Gt",
    "left": {
        "type": "Column",
        "name": "age"
    },
    "right": {
        "type": "Literal",
        "value": 18,
        "dtype": "Int64"
    }
}
```

### Unary Operation Expression
```json
{
    "type": "UnaryOp",
    "op": "Not",
    "operand": {
        "type": "Column",
        "name": "active"
    }
}
```

### Method Call Expression
```json
{
    "type": "Call",
    "func": "shift",
    "args": [
        {
            "type": "Literal",
            "value": 1,
            "dtype": "Int64"
        }
    ],
    "kwargs": {},
    "on": {
        "type": "Column",
        "name": "price"
    }
}
```

### Chained Method Call Expression
```json
{
    "type": "Call",
    "func": "mean",
    "args": [],
    "kwargs": {},
    "on": {
        "type": "Call",
        "func": "rolling",
        "args": [
            {
                "type": "Literal",
                "value": 3,
                "dtype": "Int64"
            }
        ],
        "kwargs": {},
        "on": {
            "type": "Column",
            "name": "price"
        }
    }
}
```

---

## Success Criteria for Phase 3

✅ **Test Suite Complete**
- [ ] All existing tests pass (100% pass rate)
- [ ] New `test_phase3_expression_system.py` passes (50-80 tests)
- [ ] Total coverage > 95% for `expr.py`

✅ **Expression System Complete**
- [ ] All arithmetic/comparison/logical operators work
- [ ] Method chaining works: `.shift(1).rolling(3).mean()`
- [ ] String methods serialize: `.contains()`, `.starts_with()`, etc.
- [ ] Temporal methods serialize: `.year()`, `.month()`, `.add_days()`, etc.
- [ ] Null-handling serializes: `.is_null()`, `.fill_null()`, etc.
- [ ] Complex nested expressions serialize correctly
- [ ] `if_else()` utility function works

✅ **Python Bindings Working**
- [ ] `_capture_expr()` captures and serializes lambdas correctly
- [ ] `filter()` captures filter predicates
- [ ] `select()` captures column/derived expressions
- [ ] `derive()` captures derived column expressions
- [ ] Schema validation prevents invalid column access
- [ ] Error messages are clear and actionable
- [ ] Integration tests pass for CSV → capture → serialize pipeline

✅ **Documentation Complete**
- [ ] Expression types documentation
- [ ] Lambda DSL user guide
- [ ] Serialization format specification
- [ ] Method reference documentation
- [ ] Examples and cookbook
- [ ] Updated README with Phase 3 features

---

## Effort Estimation

| Task | Hours | Risk | Dependency |
|------|-------|------|------------|
| Run existing tests | 2-3 | Low | None |
| Enhance documentation | 2-3 | Low | Task 1 |
| Implement `if_else()` | 1-2 | Low | Task 1 |
| Create test suite | 3-4 | Medium | Task 1 |
| Enhance bindings | 2-3 | Medium | Task 1 |
| Integration testing | 2-3 | Medium | Task 5 |
| Final documentation | 2-3 | Low | All |
| **TOTAL** | **14-21** | **Low-Medium** | Sequential |

---

## Phase 3 Deliverables

1. **Enhanced `expr.py`**
   - Improved docstrings for all method groups
   - `if_else()` utility function
   - Possibly minor refactoring for clarity

2. **Enhanced `__init__.py`**
   - Verified expression capture in filter/select/derive
   - Better error messages
   - Enhanced documentation

3. **New Test Suite: `test_phase3_expression_system.py`**
   - 50-100 new test cases
   - Comprehensive coverage of expression types
   - Integration tests with CSV I/O

4. **Documentation Files**
   - `docs/phase3_guide.md` - Complete Phase 3 overview
   - `docs/expression_format.md` - Serialization specification
   - `docs/lambda_dsl.md` - User guide for writing lambdas
   - `docs/method_reference.md` - All available methods
   - Updated `README.md` with examples

5. **Git Commit**
   - Clear commit message documenting all Phase 3 deliverables
   - All tests passing
   - Documentation complete

---

## Phase 4 Dependency

Phase 3 delivers **serialized expression trees** that Phase 4 will consume:

**Phase 4 (Rust Transpiler) will:**
1. Receive serialized expression dicts from Python
2. Deserialize to Rust `PyExpr` enum
3. Translate `PyExpr` to DataFusion `logical_expr::Expr`
4. Build logical plans
5. Execute on DataFrames

**Example Flow:**
```
Python:  lambda r: r.age > 18
    ↓
_lambda_to_expr()
    ↓
Dict: {"type": "BinOp", "op": "Gt", ...}
    ↓ (send to Rust)
Phase 4:
    ↓
Deserialize to PyExpr
    ↓
Transpile to DataFusion Expr
    ↓
Build and execute plan
```

---

## Open Questions & Decisions

1. **Documentation Level**: Would you like just docstrings, or full markdown guides with examples?
2. **if_else() Implementation**: Special CallExpr, or custom function class?
3. **Testing Depth**: Just correctness tests, or include performance benchmarks?
4. **Error Messages**: Simple or verbose with suggestions?
5. **Method Extensions**: Are there additional string/temporal methods beyond those in docs/api.md?

---

## Related Files & Documentation

- `expr.py` - Expression system implementation (520+ lines)
- `test_expr.py` - Existing 376+ tests
- `test_schema_proxy.py` - SchemaProxy tests
- `test_integration.py` - Integration tests
- `test_ltseq_methods.py` - Method tests
- `docs/api.md` - API specification
- `docs/milestones.md` - Milestone overview
- `plan.md` - Original implementation plan

---

## Timeline

Assuming 2-3 hours/day of development time:

- **Days 1-2**: Run existing tests, fix any failures
- **Days 2-3**: Enhance documentation, implement `if_else()`
- **Days 3-5**: Create new test suite
- **Days 5-6**: Enhance Python bindings
- **Days 6-7**: Integration testing
- **Days 7-8**: Final documentation and commit

**Estimated Duration**: 7-8 business days for full Phase 3 completion

---

**Ready to proceed with Phase 3 implementation!**
