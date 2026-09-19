//! Arrow C Data Interface boundary between the Rust kernel and Python (issue #143).
//!
//! Data crosses the PyO3 boundary as Arrow buffers shared through the
//! [C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
//! rather than through IPC serialization. In both directions the buffers are
//! borrowed, not copied: an exported `RecordBatch` keeps its `Arc` buffers
//! alive until the consumer runs the release callback, and an imported batch
//! holds the exporter's release callback until the Rust `ArrayData` is dropped.
//!
//! Three shapes cross the boundary:
//!
//! - **Export, collected**: [`collected_reader`] wraps already-executed batches
//!   in a `RecordBatchReader`; `arrow_pyarrow::IntoPyArrow` turns it into a
//!   `pyarrow.RecordBatchReader` (used by `to_arrow`).
//! - **Export, lazy**: [`DataFrameBatchReader`] adapts a DataFusion stream to a
//!   `RecordBatchReader`; [`stream_capsule`] wraps it in the `arrow_array_stream`
//!   capsule that `__arrow_c_stream__` must return. The consumer pulls batches
//!   on demand; dropping the capsule (or the imported stream) drops the
//!   DataFusion stream and cancels execution.
//! - **Import**: [`collect_imported`] drains an `ArrowArrayStreamReader`
//!   obtained from any object with `__arrow_c_stream__`.
//!
//! GIL contract (ADR 0016): nothing here touches Python except
//! [`stream_capsule`], which only allocates the capsule. The lazy reader runs
//! `RUNTIME.block_on` inside the consumer's `get_next` callback and neither
//! acquires nor releases the GIL; pyarrow's `read_all` / `read_next_batch`
//! release it before calling in. Imported streams may be drained detached:
//! `ArrowArrayStreamReader` is `Send`, pyarrow-exported streams do not need the
//! GIL, and Python-backed readers re-acquire it themselves.

use std::ffi::CStr;
use std::panic::{catch_unwind, AssertUnwindSafe};

use datafusion::arrow::array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures_util::StreamExt;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::engine::RUNTIME;
use crate::error::LtseqError;

/// Capsule name mandated by the Arrow PyCapsule Interface for streams.
const ARROW_STREAM_CAPSULE: &CStr = c"arrow_array_stream";

/// A boxed reader that can be handed to the C stream interface.
pub(crate) type BoxedBatchReader = Box<dyn RecordBatchReader + Send>;

/// Reader over batches that have already been collected.
pub(crate) fn collected_reader(schema: SchemaRef, batches: Vec<RecordBatch>) -> BoxedBatchReader {
    Box::new(RecordBatchIterator::new(
        batches.into_iter().map(Ok),
        schema,
    ))
}

/// Lazy adapter from a DataFusion stream to an Arrow `RecordBatchReader`.
///
/// Each `next()` pulls one batch from the stream on the caller's thread via
/// `RUNTIME.block_on`. A DataFusion error ends the stream (the next pull
/// returns `None`) after being reported once; a panic is caught and reported
/// as an error rather than unwinding across the C ABI, which would abort.
pub(crate) struct DataFrameBatchReader {
    stream: Option<SendableRecordBatchStream>,
    schema: SchemaRef,
}

impl DataFrameBatchReader {
    pub(crate) fn new(stream: SendableRecordBatchStream) -> Self {
        let schema = stream.schema();
        DataFrameBatchReader {
            stream: Some(stream),
            schema,
        }
    }
}

impl Iterator for DataFrameBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let stream = self.stream.as_mut()?;
        let pulled = catch_unwind(AssertUnwindSafe(|| RUNTIME.block_on(stream.next())));
        match pulled {
            Ok(Some(Ok(batch))) => Some(Ok(batch)),
            Ok(None) => {
                self.stream = None;
                None
            }
            Ok(Some(Err(e))) => {
                self.stream = None;
                Some(Err(ArrowError::ExternalError(Box::new(e))))
            }
            Err(panic) => {
                self.stream = None;
                let msg = panic
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "unknown panic".to_string());
                Some(Err(ArrowError::ExternalError(
                    format!("Execution panicked: {msg}").into(),
                )))
            }
        }
    }
}

impl RecordBatchReader for DataFrameBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Wrap a reader in the `arrow_array_stream` capsule returned by `__arrow_c_stream__`.
///
/// The capsule owns the `FFI_ArrowArrayStream`. A consumer that imports it
/// moves the stream out (leaving a released placeholder behind); a capsule
/// that is never imported releases the stream, and with it the reader, from
/// its destructor.
pub(crate) fn stream_capsule(
    py: Python<'_>,
    reader: BoxedBatchReader,
) -> PyResult<Bound<'_, PyCapsule>> {
    let stream = FFI_ArrowArrayStream::new(reader);
    PyCapsule::new_with_value(py, stream, ARROW_STREAM_CAPSULE)
}

/// Drain a stream imported from Python into its schema and batches.
///
/// Pure Rust and `Send`, so it may run detached. The batches reference the
/// exporter's buffers; they stay valid for as long as the batches live, even
/// after the Python source object is garbage collected.
pub(crate) fn collect_imported(
    reader: ArrowArrayStreamReader,
) -> Result<(SchemaRef, Vec<RecordBatch>), LtseqError> {
    let schema = reader.schema();
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| LtseqError::with_context("Failed to import Arrow stream", e))?;
    Ok((schema, batches))
}
