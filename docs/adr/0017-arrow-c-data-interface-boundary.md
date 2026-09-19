# ADR 0017: Arrow C Data Interface at the Python Boundary (No IPC Tunnel)

- Status: Accepted
- Decision date: 2026-09-19 (issue #143) · Recorded: 2026-09-19

[中文版](0017-arrow-c-data-interface-boundary.cn.md)

## Context

Until #143 every dataset crossing between the Rust kernel and Python went through an Arrow IPC byte tunnel. `to_arrow_ipc` encoded each collected `RecordBatch` into a `Vec<u8>` with `StreamWriter`, copied it into a `PyBytes`, and Python parsed it back with `pa.ipc.open_stream(...).read_all()` and `pa.concat_tables`. `from_arrow` mirrored this: `pa.ipc.new_stream` → `bytes` → `Vec<Vec<u8>>` → `StreamReader`. The cursor repeated the export leg for every batch, each IPC stream carrying its own schema. None of this was required by the data model: both sides already hold Arrow buffers, and pyarrow has implemented the [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html) and its [PyCapsule protocol](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html) (`__arrow_c_stream__`) for years. Arrow 59.2, the version DataFusion 55 resolves to, ships the `arrow-pyarrow` crate against pyo3 0.29, which the project already uses.

The issue's review comments fixed two boundaries for the decision: "zero-copy" means the buffer hand-off between Rust and Python, not that `to_arrow()` stops executing the lazy plan; and a whole-stream export of the `Cursor` has ownership semantics of its own and is not part of this change.

## Decision

**Data crosses the boundary as shared Arrow buffers over the C Data Interface, never as IPC bytes.** The kernel contains no IPC reader or writer; `py-ltseq/tests/test_arrow_boundary.py` guards the source for their return.

Three shapes cross the boundary (`src/arrow_ffi.rs`, `src/ops/io.rs`, `src/cursor.rs`):

1. **Export, collected (`to_arrow()`).** `LTSeqTable.to_arrow_reader` executes the plan detached (ADR 0016), wraps the batches in a `RecordBatchReader`, and hands it to pyarrow as a `pyarrow.RecordBatchReader`; Python calls `read_all()`. The reader's schema is the first batch's schema (the logical schema only when there are no batches), so nullability and metadata match what pyarrow sees per batch. Errors keep their `RuntimeError` mapping. An unloaded table exports an empty reader with whatever schema it has, so `LTSeq().to_arrow()` no longer needs Python-side fallbacks that produced untyped columns.
2. **Export, lazy (`__arrow_c_stream__`).** `LTSeqTable.__arrow_c_stream__` runs `execute_stream` detached, wraps the DataFusion stream in `DataFrameBatchReader`, and returns an `arrow_array_stream` capsule. Semantics: planning happens at the call, so planning errors raise immediately; batches are pulled by the consumer inside its `get_next` callback; each call executes the plan anew and leaves the table untouched; the capsule, or the consumer that imported it, owns the execution stream and dropping it cancels execution; `requested_schema` is accepted and ignored (the protocol allows a producer to return its own schema); execution errors are reported through the C interface and surface as the consumer's Arrow error (`pyarrow.ArrowInvalid`); a panic inside DataFusion is caught and reported as an error instead of unwinding across the C ABI. The exporter neither takes nor releases the GIL, so whether the interpreter is free during execution depends on the consumer (pyarrow's `read_all` / `read_next_batch` release it).
3. **Import (`from_arrow`).** `LTSeqTable.from_arrow` takes `PyArrowType<ArrowArrayStreamReader>`: argument extraction calls the source object's `__arrow_c_stream__` under the GIL; draining the stream and building the `MemTable` run detached (the reader is `Send`; pyarrow-exported streams need no GIL and Python-backed readers re-acquire it themselves). Any object implementing the protocol is accepted (`pyarrow.Table`, `RecordBatch`, `RecordBatchReader`, polars, duckdb, ...); Python raises `TypeError` for anything else. A schema with zero batches becomes one empty batch, so the table keeps a real plan and its typed schema, as the IPC path did by synthesizing an empty batch in Python.

The cursor's `next_batch` returns each pulled `RecordBatch` as a `pyarrow.RecordBatch` (`PyArrowType<RecordBatch>`), still pulled detached with the mutex taken inside the detached section (ADR 0016). Imported and exported buffers are reference-counted across the boundary by the interface's release callbacks, so results outlive the object they were taken from in both directions.

`Cargo.toml` pins `arrow = "59.2.0"` with the `pyarrow` feature next to the `parquet` pin, both required to match DataFusion's Arrow version.

## Alternatives Considered

- *Borrow the `PyBytes` (`Vec<Bound<PyBytes>>`) but keep IPC.* Removes one of the copies and nothing else; still encodes, parses, and couples the kernel to pyarrow's IPC reader. Rejected as a stopgap.
- *`PyArrowType<Table>` for `to_arrow()`.* `Table::try_new` requires schema equality including metadata across batches, stricter than the `pa.concat_tables` behavior it would replace; the reader hand-off keeps that tolerance and is one FFI call instead of one per batch.
- *Route `to_arrow()` through the lazy `__arrow_c_stream__`.* One primitive instead of two, but execution errors would change type (`RuntimeError` → `ArrowInvalid`) and GIL release during execution would depend on the consumer. The primary export API keeps the collect-then-export path under the kernel's own GIL contract.
- *Eager `__arrow_c_stream__` (collect first).* Simpler ownership story, but it forfeits streaming for consumers such as duckdb and polars and duplicates `to_arrow()`.

## Consequences

- No serialization or parsing on any Python↔Rust data path; the boundary cost is a pointer hand-off per batch. Measured on the same machine (26.8M rows, ~1 GB, 107 chunks; `benchmarks/bench_arrow_boundary.py`, before/after release builds):

  | Path | Before (s) | After (s) | Speedup | Before peak RSS growth (MB) | After peak RSS growth (MB) |
  |---|---|---|---|---|---|
  | `from_arrow` (883 MB `pyarrow.Table`, 107 chunks) | 1.435 | 0.032 | 45x | 2487 | 0 |
  | `to_arrow()` on a collected table | 0.719 | 0.011 | 65x | 1396 | 0 |
  | `to_arrow()` on a lazy Parquet scan | 0.984 | 0.261 | 3.8x | 2218 | 1134 |
  | `pa.table(t)` on a collected table (`__arrow_c_stream__`) | n/a | 0.181 | n/a | n/a | 24 |
  | `scan_parquet` cursor, all batches | 0.284 | 0.188 | 1.5x | 164 | 111 |

  The collected paths now cost only the pointer hand-off. The growth left on the lazy Parquet export is the decoded result itself (about 883 MB of Arrow buffers the returned table owns) plus decoding scratch; the IPC copies on both sides are gone.

- `LTSeq` participates in the Arrow ecosystem as a producer: `pa.table(t)`, `pa.RecordBatchReader.from_stream(t)`, `pl.from_arrow(t)`, `duckdb.sql("... FROM t")` work without `to_arrow()`. As a consumer, `from_arrow` accepts anything with `__arrow_c_stream__`, which removes the implicit coupling to pyarrow's IPC format and version.
- Empty results keep typed schemas in both directions.
- `Cursor` still yields one batch per pull. A whole-stream `__arrow_c_stream__` on the cursor (handing the remaining DataFusion stream to the consumer, which would consume the cursor) is a separate design; see the follow-up issue linked from #143.
- Adding a boundary path means returning a `RecordBatchReader` or `RecordBatch` from the detached half and converting with `arrow_pyarrow` under the GIL; `ipc::` and `StreamWriter` in `src/` fail the guard test.

## Sources

- Issue #143 and its review comments (scope of "zero-copy", cursor separation, verification matrix)
- `src/arrow_ffi.rs`, `src/ops/io.rs`, `src/cursor.rs`, `src/lib.rs`
- `py-ltseq/tests/test_arrow_boundary.py`, `benchmarks/bench_arrow_boundary.py`
- [ADR 0016](0016-gil-release-execution-boundary.md) (GIL contract the boundary paths follow), [ADR 0004](0004-lazy-execution-immutable-tables.md) (terminal boundaries)
