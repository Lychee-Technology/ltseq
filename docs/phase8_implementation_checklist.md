# Phase 8 Enhancement: Implementation Checklist

Use this checklist to track progress on Phase 8 Enhancement implementation.

## Pre-Implementation

- [ ] Read all analysis documents in `/docs/phase8_*.md`
- [ ] Review current LinkedTable implementation (lines 993-1079 in `__init__.py`)
- [ ] Review RustTable structure and existing methods in `src/lib.rs`
- [ ] Familiarize yourself with DataFusion join API
- [ ] Review expression system in `expr.py`
- [ ] Set up test environment and run existing tests

## Phase 8A: Lambda Expression Parsing

### Implementation
- [ ] Create `_extract_join_keys()` function in `__init__.py`
- [ ] Handle two-parameter lambdas: `lambda o, p: ...`
- [ ] Extract left and right key expressions
- [ ] Validate expressions are ColumnExpr (not Literals)
- [ ] Validate operators are equality (`==`)
- [ ] Return tuple of (left_dict, right_dict, join_type)

### Error Handling
- [ ] Catch AttributeError for invalid columns
- [ ] Catch ValueError for invalid operators
- [ ] Provide clear error messages
- [ ] Handle composite keys (optional for Phase 8A)

### Testing
- [ ] `test_extract_join_keys_simple_equality()` - Basic case
- [ ] `test_extract_join_keys_invalid_operator()` - Reject non-equality
- [ ] `test_extract_join_keys_invalid_column()` - Reject invalid columns
- [ ] `test_extract_join_keys_reversed_order()` - Handle p.id == o.product_id
- [ ] `test_extract_join_keys_with_literals()` - Reject literal comparisons

### Integration with LinkedTable
- [ ] Modify `LinkedTable.__init__()` to extract and cache keys
- [ ] Store `_left_key` and `_right_key` as instance variables
- [ ] Validate early (in `__init__`, not lazy)
- [ ] Raise clear TypeError if validation fails

## Phase 8B: Rust Join Implementation

### Implementation in src/lib.rs
- [ ] Add import: `use datafusion::logical_expr::JoinType;`
- [ ] Implement `RustTable.join()` method signature
- [ ] Add input validation (both tables have data)
- [ ] Add schema validation
- [ ] Deserialize PyExpr from dictionaries
- [ ] Transpile PyExpr to DataFusion Expr
- [ ] Map join_type string to JoinType enum
- [ ] Execute join using `DataFrame.join()`
- [ ] Extract schema from joined result
- [ ] Return new RustTable with joined data

### Join Type Handling
- [ ] Support "inner" join
- [ ] Support "left" join (optional for MVP)
- [ ] Support "right" join (optional for MVP)
- [ ] Support "full" join (optional for MVP)
- [ ] Reject unknown join types with clear error

### Error Handling
- [ ] Check for missing dataframes
- [ ] Check for missing schemas
- [ ] Handle deserialization errors
- [ ] Handle transpilation errors
- [ ] Handle join execution errors
- [ ] Provide informative error messages

### Testing (Manual Tests)
- [ ] Create simple test RustTable instances with CSV data
- [ ] Call join() with simple equality keys
- [ ] Verify output has both tables' columns
- [ ] Verify row count is correct for inner join
- [ ] Check schema of result

## Phase 8C: Python Materialization

### Implementation in py-ltseq/ltseq/__init__.py

#### LinkedTable.__init__ modifications
- [ ] Add `self._materialized = None` for caching
- [ ] Extract and cache join keys early (use Phase 8A)
- [ ] Store `_left_key` and `_right_key`

#### New _materialize() method
- [ ] Check cache and return if already materialized
- [ ] Extract join keys (if not cached)
- [ ] Call Rust join: `self._source._rust_table.join(...)`
- [ ] Create new LTSeq wrapping result
- [ ] Set schema to merged schema (includes prefixed columns)
- [ ] Cache result and return

#### Update existing methods
- [ ] `show()` - call `_materialize()` first
- [ ] `select()` - call `_materialize()` first
- [ ] `filter()` - call `_materialize()` first
- [ ] `derive()` - call `_materialize()` first
- [ ] `slice()` - call `_materialize()` first
- [ ] `distinct()` - call `_materialize()` first

### Error Handling
- [ ] Catch exceptions from Rust join
- [ ] Wrap in RuntimeError with context
- [ ] Provide helpful error messages

### Testing
- [ ] `test_link_lazy_materialization()` - No join until accessed
- [ ] `test_link_materialize_on_show()` - show() triggers join
- [ ] `test_link_caches_materialized()` - Result is cached
- [ ] `test_link_materializes_with_select()` - select() triggers join
- [ ] `test_link_materializes_with_filter()` - filter() triggers join
- [ ] `test_materialized_returns_ltseq()` - Result is proper LTSeq

## Phase 8D: Pointer Column Access

### Implementation in py-ltseq/ltseq/expr.py

#### Enhance SchemaProxy
- [ ] Add `prefix` parameter to `__init__`
- [ ] Implement nested attribute logic in `__getattr__`
- [ ] Check for prefixed columns when alias is used
- [ ] Return NestedSchemaProxy for linked table references
- [ ] Map nested access to prefixed column names

#### Create NestedSchemaProxy
- [ ] Subclass or helper that handles second-level access
- [ ] Override `__getattr__` to map to prefixed columns
- [ ] Throw clear errors for invalid columns

### Support for Complex Access Paths
- [ ] `r.name` → `Column("name")` (simple)
- [ ] `r.prod.name` → `Column("prod_name")` (linked)
- [ ] `r.prod.price` → `Column("prod_price")` (linked)
- [ ] Chain with operators: `r.prod.price > 100` works
- [ ] Chain with arithmetic: `r.qty * r.prod.price` works

### Error Handling
- [ ] Reject private attributes (starting with `_`)
- [ ] Clear errors for invalid columns
- [ ] Clear errors for invalid linked tables
- [ ] Suggest available columns/tables in error messages

### Testing
- [ ] `test_schema_proxy_simple_column()` - Direct column access
- [ ] `test_schema_proxy_linked_column()` - Two-level access
- [ ] `test_schema_proxy_invalid_column()` - Reject invalid
- [ ] `test_schema_proxy_invalid_linked_column()` - Reject invalid linked
- [ ] `test_schema_proxy_with_operators()` - Works in expressions
- [ ] `test_schema_proxy_with_arithmetic()` - Works in computations

## Phase 8E: Testing & Validation

### Unit Tests (Phase 8A-D)
- [ ] 25+ unit tests covering all functions
- [ ] 100% code coverage for new code
- [ ] Edge cases and error conditions
- [ ] Schema validation
- [ ] Column name validation

### Integration Tests
- [ ] `test_link_select_joined_columns()` - End-to-end
- [ ] `test_link_filter_on_linked()` - Filter with linked columns
- [ ] `test_link_derive_with_linked()` - Compute with linked columns
- [ ] `test_link_multiple_tables()` - Chain multiple links
- [ ] `test_link_show_displays_correctly()` - Output is correct
- [ ] `test_link_complex_workflow()` - Multiple operations chained

### Data Integrity Tests
- [ ] `test_join_correctness()` - Results match expected data
- [ ] `test_row_counts()` - Correct number of rows
- [ ] `test_null_values()` - NULL handling in join keys
- [ ] `test_column_order()` - Columns in expected order
- [ ] `test_column_types()` - Types are correct
- [ ] `test_data_values()` - Actual values correct

### Performance Tests (Optional)
- [ ] [ ] Test with medium dataset (10k rows)
- [ ] [ ] Verify lazy materialization works
- [ ] [ ] Check caching prevents re-joining

### Example Workflow Test
- [ ] [ ] orders.link(products, ...) works
- [ ] [ ] linked.select(...) works
- [ ] [ ] linked.filter(...) works
- [ ] [ ] linked.derive(...) works
- [ ] [ ] linked.show() displays all columns
- [ ] [ ] Matches SQL join semantics

## Documentation

- [ ] Add docstrings to all new functions/methods
- [ ] Add type hints to all new functions
- [ ] Document join limitations (MVP: inner only, equality only)
- [ ] Add usage examples in docstrings
- [ ] Update API documentation
- [ ] Add examples to README if appropriate

## Code Quality

- [ ] All code follows project style (check existing patterns)
- [ ] No unused imports
- [ ] No debug print statements
- [ ] Error messages are clear and helpful
- [ ] No hardcoded strings (use constants)
- [ ] Comments explain non-obvious logic

## Final Validation

### Manual Testing
- [ ] Run example from README with joins
- [ ] Test with different data types (int, string, float)
- [ ] Test error cases (invalid columns, etc.)
- [ ] Test with edge cases (empty tables, NULL values)

### Regression Testing
- [ ] All existing tests still pass
- [ ] No breakage to LinkedTable MVP
- [ ] No breakage to other LTSeq operations

### Code Review
- [ ] Self-review before submission
- [ ] Check for edge cases missed
- [ ] Verify error handling is comprehensive
- [ ] Check for performance issues

## Milestone Tracking

### Milestone 1: Lambda Parsing (Days 1-2)
- [ ] Phase 8A complete
- [ ] 5+ unit tests passing
- [ ] Can parse simple equality joins

### Milestone 2: Rust Join (Days 3-5)
- [ ] Phase 8B complete
- [ ] Rust method tested standalone
- [ ] Can execute basic joins

### Milestone 3: Integration (Days 5-7)
- [ ] Phase 8C complete
- [ ] Materialization works
- [ ] link().show() produces joined data

### Milestone 4: Column Access (Days 7-8)
- [ ] Phase 8D complete
- [ ] Can use r.prod.name syntax
- [ ] All expressions work

### Milestone 5: Completion (Days 8-10)
- [ ] All tests passing
- [ ] Documentation complete
- [ ] Code review passed
- [ ] Ready for merge

## Sign-Off

- [ ] Implementation complete
- [ ] All tests passing
- [ ] Code review approved
- [ ] Documentation updated
- [ ] Merged to main branch

---

**Estimated Effort:** 2-3 weeks
**Lines of Code:** 1,500-2,000 (MVP 920 + error handling + docs)
**Test Coverage:** 25+ tests

