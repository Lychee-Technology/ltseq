# LTSeq Tasks

**Last Updated**: January 9, 2026  
**Current Status**: Phase 9 Complete (681 tests), ready for Phase 10

---

## Completed: Phase 8 - Streaming & Big Data ✅

| Task | Description | Status |
|------|-------------|--------|
| 1 | `LTSeqCursor` struct in Rust | ✅ |
| 2 | `scan_csv` / `scan_parquet` methods | ✅ |
| 3 | Python `Cursor` wrapper | ✅ |
| 4 | `LTSeq.scan()` / `scan_parquet()` API | ✅ |
| 5 | Streaming tests (9 tests) | ✅ |

---

## Completed: Phase 9 - Pointer Syntax Sugar ✅

Already implemented via `SchemaProxy` and `NestedSchemaProxy` in `expr/proxy.py`.

**Syntax:**
```python
linked = orders.link(products, on=lambda o,p: o.product_id == p.product_id, as_='prod')

# Pointer syntax: r.prod.name instead of r.prod_name
result = linked.select(lambda r: r.prod.name)
result = linked.filter(lambda r: r.prod.price > 10)
```

| Tests Added | Status |
|-------------|--------|
| `test_pointer_select_single_column` | ✅ |
| `test_pointer_select_multiple_columns` | ✅ |
| `test_pointer_in_filter` | ✅ |
| `test_pointer_in_derive` | ✅ |
| `test_pointer_invalid_column_error` | ✅ |
| `test_pointer_invalid_alias_error` | ✅ |

---

## Next: Phase 10 - Performance Optimization

| Task | Description | Effort | Status |
|------|-------------|--------|--------|
| 1 | SIMD vectorization | 2 days | [ ] |
| 2 | Multi-core parallelization | 2 days | [ ] |
| 3 | Expression optimization | 1 day | [ ] |

---

## Quick Reference

```bash
uv run python -m pytest py-ltseq/tests -v
uv run maturin develop
```

---

## Notes
- **681 tests passing**, 0 skipped
- **Pandas**: Only `partition(by=callable)` uses pandas (by design)
