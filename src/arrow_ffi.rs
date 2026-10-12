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
//! - **Import**: [`ImportedStream`] takes the stream of any object with
//!   `__arrow_c_stream__` and reads it as record batches.
//!
//! GIL contract (ADR 0016): nothing here touches Python except
//! [`schema_from_capsule`], which reads a borrowed `arrow_schema` capsule,
//! [`stream_capsule`], which only allocates the capsule, and
//! [`ImportedStream::from_py`], which calls `__arrow_c_stream__`. The lazy
//! reader runs `RUNTIME.block_on` inside the consumer's `get_next` callback and
//! neither acquires nor releases the GIL; pyarrow's `read_all` /
//! `read_next_batch` release it before calling in. Imported streams may be
//! drained detached: [`ImportedStream`] is `Send`, pyarrow-exported streams do
//! not need the GIL, and Python-backed readers re-acquire it themselves.

use std::ffi::CStr;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use datafusion::arrow::array::{
    RecordBatch, RecordBatchIterator, RecordBatchOptions, RecordBatchReader, StructArray,
};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ffi::{from_ffi_and_data_type, FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::arrow::ffi_stream::FFI_ArrowArrayStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures_util::StreamExt;
use pyo3::intern;
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

/// Context of every failure to read an imported stream.
const IMPORT_FAILED: &str = "Failed to import Arrow stream";

/// A stream taken from a Python object's `__arrow_c_stream__`.
///
/// Each array the stream yields is a struct whose fields are the columns. A
/// struct array can also be NULL at the top level: a `pyarrow.ChunkedArray` of
/// a struct type exports its NULL rows that way. A table row cannot be NULL as
/// a whole, so such a row is refused, as pyarrow refuses it (`pa.table`,
/// `pa.record_batch`). arrow-rs's `ArrowArrayStreamReader` (59.2) builds each
/// batch from the struct's children and drops its validity, so the values the
/// children hold under a NULL row, zeros and empty strings, would be read as
/// data (#255). That is why this reader imports the arrays itself.
pub(crate) struct ImportedStream {
    stream: FFI_ArrowArrayStream,
    schema: SchemaRef,
}

impl ImportedStream {
    /// Call `obj.__arrow_c_stream__()` and take the stream out of its capsule.
    ///
    /// Runs under the GIL: the call is Python, and so may be the producer's
    /// `get_schema`. The capsule is left holding a released stream, so its
    /// destructor does nothing and this value alone releases the stream.
    pub(crate) fn from_py(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        let capsule = obj.call_method0(intern!(obj.py(), "__arrow_c_stream__"))?;
        let capsule = capsule.cast::<PyCapsule>().map_err(|_| {
            pyo3::exceptions::PyTypeError::new_err(
                "__arrow_c_stream__ must return an 'arrow_array_stream' PyCapsule",
            )
        })?;
        let ptr = capsule
            .pointer_checked(Some(ARROW_STREAM_CAPSULE))?
            .cast::<FFI_ArrowArrayStream>();
        // SAFETY: the capsule is named `arrow_array_stream`, so by the PyCapsule
        // Interface its pointer is a live `ArrowArrayStream` that the consumer
        // may move out. `from_raw` moves it and leaves a released stream behind.
        let stream = unsafe { FFI_ArrowArrayStream::from_raw(ptr.as_ptr()) };
        Ok(Self::try_new(stream)?)
    }

    fn try_new(mut stream: FFI_ArrowArrayStream) -> Result<Self, LtseqError> {
        if stream.release.is_none() {
            return Err(import_error("the stream is already released"));
        }
        let get_schema = stream
            .get_schema
            .ok_or_else(|| import_error("the stream has no get_schema"))?;
        let mut ffi_schema = FFI_ArrowSchema::empty();
        // SAFETY: `get_schema` is the callback of this live stream, called with
        // the stream itself and an empty schema for it to fill.
        if unsafe { get_schema(&mut stream, &mut ffi_schema) } != 0 {
            return Err(stream_error(&mut stream));
        }
        let schema = Schema::try_from(&ffi_schema)
            .map_err(|e| LtseqError::with_context(IMPORT_FAILED, e))?;
        Ok(Self {
            stream,
            schema: Arc::new(schema),
        })
    }

    /// Read the stream to its end into its schema and batches.
    ///
    /// Pure Rust and `Send`, so it may run detached. The batches reference the
    /// exporter's buffers; they stay valid for as long as the batches live,
    /// even after the Python source object is garbage collected.
    pub(crate) fn read_all(mut self) -> Result<(SchemaRef, Vec<RecordBatch>), LtseqError> {
        let mut batches = Vec::new();
        let mut rows_read = 0;
        while let Some(batch) = self.next_batch(rows_read)? {
            rows_read += batch.num_rows();
            batches.push(batch);
        }
        Ok((self.schema, batches))
    }

    /// The next batch, or `None` at the end of the stream. `first_row` is the
    /// position of the batch's first row in the stream, for the error message.
    fn next_batch(&mut self, first_row: usize) -> Result<Option<RecordBatch>, LtseqError> {
        let get_next = self
            .stream
            .get_next
            .ok_or_else(|| import_error("the stream has no get_next"))?;
        let mut array = FFI_ArrowArray::empty();
        // SAFETY: `get_next` is the callback of this live stream, called with
        // the stream itself and an empty array for it to fill.
        if unsafe { get_next(&mut self.stream, &mut array) } != 0 {
            return Err(stream_error(&mut self.stream));
        }
        if array.is_released() {
            return Ok(None);
        }
        let struct_type = DataType::Struct(self.schema.fields().clone());
        // SAFETY: the producer exports each array under the schema it reported,
        // a struct of `self.schema`'s fields.
        let data = unsafe { from_ffi_and_data_type(array, struct_type) }
            .map_err(|e| LtseqError::with_context(IMPORT_FAILED, e))?;
        let rows = data.len();
        let (_, columns, nulls) = StructArray::from(data).into_parts();
        if let Some(nulls) = nulls.filter(|n| n.null_count() > 0) {
            let null_row = nulls.iter().position(|valid| !valid).unwrap_or_default();
            return Err(LtseqError::Validation(format!(
                "The row at index {} of the struct input is NULL as a whole; a table row \
                 cannot be NULL. Flatten the input first to read that row's fields as NULL \
                 (for example with pyarrow.Table.from_struct_array), or drop the NULL rows",
                first_row + null_row
            )));
        }
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        RecordBatch::try_new_with_options(Arc::clone(&self.schema), columns, &options)
            .map(Some)
            .map_err(|e| LtseqError::with_context(IMPORT_FAILED, e))
    }
}

fn import_error(message: &str) -> LtseqError {
    LtseqError::with_context(
        IMPORT_FAILED,
        ArrowError::CDataInterface(message.to_string()),
    )
}

/// The error a stream reported through a non-zero return code, with the
/// producer's message when it gives one.
fn stream_error(stream: &mut FFI_ArrowArrayStream) -> LtseqError {
    let message = stream
        .get_last_error
        .and_then(|get_last_error| {
            // SAFETY: `get_last_error` is the callback of this live stream; the
            // string it returns stays valid until the next call on the stream,
            // and is copied before then.
            let ptr = unsafe { get_last_error(stream) };
            // SAFETY: a non-null result is a NUL-terminated string (see above).
            (!ptr.is_null()).then(|| {
                unsafe { CStr::from_ptr(ptr) }
                    .to_string_lossy()
                    .into_owned()
            })
        })
        .unwrap_or_else(|| "the producer reported an error without a message".to_string());
    import_error(&message)
}
