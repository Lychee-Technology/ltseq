# LTSeq Tasks

**Last Updated**: January 9, 2026  
**Current Status**: Core API Complete (585 tests), Phase 8 In Progress

---

## Current Phase: Streaming & Big Data

### Overview
Enable processing of datasets larger than RAM by exposing DataFusion's streaming capabilities.

### Pending Tasks

| Task | Description | Files | Effort | Status |
|------|-------------|-------|--------|--------|
| 1 | Implement `RustCursor` struct | `src/cursor.rs` (new), `src/lib.rs` | 2h | [x] |
| 2 | Expose `scan_csv` method | `src/lib.rs` | 1h | [x] |
| 3 | Create Python `Cursor` wrapper | `py-ltseq/ltseq/cursor.py` (new) | 1h | [x] |
| 4 | Add `LTSeq.scan()` class method | `py-ltseq/ltseq/core.py` | 30m | [x] |
| 5 | Add Parquet read/write support | `src/lib.rs`, `core.py` | 2h | [x] |
| 6 | Streaming memory boundedness test | `py-ltseq/tests/test_streaming.py` | 1h | [x] |
| 7 | Performance benchmarking suite | `benchmarks/` (new) | 1d | [ ] |

---

### Task 1: RustCursor Struct

**Goal**: Rust struct holding `SendableRecordBatchStream` for lazy iteration.

```rust
// src/cursor.rs
#[pyclass]
pub struct RustCursor {
    stream: Option<SendableRecordBatchStream>,
    schema: Arc<ArrowSchema>,
}

#[pymethods]
impl RustCursor {
    fn next_batch(&mut self) -> PyResult<Option<PyObject>> {
        // stream.next() -> RecordBatch -> PyArrow
    }
}
```

---

### Task 2: scan_csv Method

**Goal**: Return `RustCursor` instead of loading all data.

```rust
fn scan_csv(path: String) -> PyResult<RustCursor> {
    // session.read_csv() -> df.execute_stream() -> RustCursor
}
```

---

### Task 3-4: Python Cursor Wrapper

**Goal**: Pythonic iterator over batches.

```python
class Cursor:
    def __iter__(self): return self
    def __next__(self):
        batch = self._inner.next_batch()
        if batch is None: raise StopIteration
        return batch

# Usage: for batch in LTSeq.scan("file.csv"): ...
```

---

### Task 5: Parquet Support

**Goal**: Efficient columnar format for large datasets.

```python
t = LTSeq.read_parquet("data.parquet")
t.write_parquet("output.parquet")
cursor = LTSeq.scan_parquet("large.parquet")
```

---

### Task 6: Memory Boundedness Test

**Goal**: Verify streaming uses bounded memory.

```python
def test_streaming_memory():
    row_count = 0
    for batch in LTSeq.scan("1gb_file.csv"):
        row_count += len(batch)
    assert peak_memory < 200MB
```

---

## Future Phases

### Pointer Syntax Sugar
- `r.prod.name` instead of `r.prod_name`
- `__getattr__` for linked table access

### Performance Optimization
- SIMD vectorization
- Multi-core via Rayon/Tokio
- Expression optimization

### Ecosystem Integration
- Arrow IPC zero-copy
- Better IDE support

---

## Quick Reference

### Commands
```bash
# Run tests
pytest py-ltseq/tests -v

# Rebuild after Rust changes
cargo build --release && uv pip install -e . --reinstall
```

### Key Files
| File | Purpose |
|------|---------|
| `py-ltseq/ltseq/core.py` | Main Python API |
| `py-ltseq/ltseq/grouping.py` | NestedTable |
| `src/lib.rs` | PyO3 bindings |
| `src/transpiler.rs` | Expression → SQL |

---

## Notes
- **585 tests passing**, 0 skipped
- **Pandas**: Only `partition(by=callable)` uses pandas (by design)
