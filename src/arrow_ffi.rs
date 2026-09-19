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
//! [`schema_from_capsule`], which reads a borrowed `arrow_schema` capsule, and
//! [`stream_capsule`], which only allocates the capsule. The lazy reader runs
//! `RUNTIME.block_on` inside the consumer's `get_next` callback and neither
//! acquires nor releases the GIL; pyarrow's `read_all` / `read_next_batch`
//! release it before calling in. Imported streams may be drained detached:
//! `ArrowArrayStreamReader` is `Send`, pyarrow-exported streams do not need the
//! GIL, and Python-backed readers re-acquire it themselves.

use std::ffi::CStr;
use std::panic::{catch_unwind, AssertUnwindSafe};

use datafusion::arrow::array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ffi::FFI_ArrowSchema;
use datafusion::arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures_util::StreamExt;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::engine::RUNTIME;
use crate::error::LtseqError;

/// Capsule name mandated by the Arrow PyCapsule Interface for streams.
const ARROW_STREAM_CAPSULE: &CStr = c"arrow_array_stream";

/// Capsule name mandated by the Arrow PyCapsule Interface for schemas.
const ARROW_SCHEMA_CAPSULE: &CStr = c"arrow_schema";

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

/// Read the schema out of a borrowed `arrow_schema` capsule.
///
/// This is the `requested_schema` argument of `__arrow_c_stream__`. The
/// caller keeps ownership of the capsule: the `ArrowSchema` is copied into a
/// Rust `Schema` and never released here. Anything other than a capsule of
/// that name is a `TypeError`, as the protocol defines the argument as a
/// capsule.
pub(crate) fn schema_from_capsule(obj: &Bound<'_, PyAny>) -> PyResult<Schema> {
    let capsule = obj.cast::<PyCapsule>().map_err(|_| {
        pyo3::exceptions::PyTypeError::new_err(format!(
            "requested_schema must be an 'arrow_schema' PyCapsule, got {}",
            obj.get_type()
                .name()
                .map(|n| n.to_string())
                .unwrap_or_default()
        ))
    })?;
    let ptr = capsule
        .pointer_checked(Some(ARROW_SCHEMA_CAPSULE))?
        .cast::<FFI_ArrowSchema>();
    // SAFETY: the capsule is named `arrow_schema`, so by the PyCapsule
    // Interface its pointer is a live `ArrowSchema` owned by the caller; it is
    // only read, for the duration of this call, under the GIL.
    let ffi_schema = unsafe { ptr.as_ref() };
    Schema::try_from(ffi_schema).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid requested_schema: {e}"))
    })
}

/// Check that a `requested_schema` describes the same data as `native`.
///
/// The PyCapsule Interface lets a consumer ask for a different Arrow
/// *representation* of the same columns (other integer widths, string
/// encodings, ...); a producer that cannot provide it may return its own
/// schema, and this producer always does. A request for different columns
/// (another field count, other names, another order) is not a representation
/// of this data, and the protocol says the producer should raise instead of
/// returning a stream the consumer did not ask for.
pub(crate) fn check_requested_schema(
    native: &Schema,
    requested: &Schema,
) -> Result<(), LtseqError> {
    fn names(schema: &Schema) -> Vec<&str> {
        schema.fields().iter().map(|f| f.name().as_str()).collect()
    }
    let native_names = names(native);
    let requested_names = names(requested);
    if native_names != requested_names {
        return Err(LtseqError::Validation(format!(
            "requested_schema is not compatible with the table: table fields are \
             {native_names:?}, requested fields are {requested_names:?}; a schema request \
             may only ask for a different representation of the same fields"
        )));
    }
    Ok(())
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
