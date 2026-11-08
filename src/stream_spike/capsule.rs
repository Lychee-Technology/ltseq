//! A C stream whose errors carry LTSeq's errno and message (question 2 of
//! #256, contract §16.4).
//!
//! arrow-rs's exporter (`FFI_ArrowArrayStream::new`) reports an error as
//! `get_error_code(&ArrowError)` and `ArrowError`'s `Display`, so today's
//! stream ends a division by zero with `EINVAL` and "External error: Arrow
//! error: Divide by zero error". §16.4 asks for `EIO` for `LTSeqIOError`
//! and its subclasses, `EINVAL` for every other class, and a message that
//! starts with the class name. The exporter's callbacks are private, so this
//! module fills in the `FFI_ArrowArrayStream` struct with its own.
//!
//! The callbacks run on the consumer's thread, without the GIL, and never
//! touch Python. A panic is caught in each, because unwinding across the C
//! ABI aborts the process.

use std::ffi::{c_char, c_int, c_void, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;

use datafusion::arrow::array::{Array, StructArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::arrow::ffi_stream::FFI_ArrowArrayStream;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use super::classify::Classified;
use super::Execution;

/// `EIO` and `EINVAL` have these values on Linux and macOS, the platforms
/// LTSeq supports (and in arrow-rs's own fallback for targets without libc).
const EIO: c_int = 5;
const EINVAL: c_int = 22;

const ARROW_STREAM_CAPSULE: &std::ffi::CStr = c"arrow_array_stream";

struct Private {
    /// `None` once the stream ended or failed: the execution is released at
    /// once, not when the consumer releases the stream.
    execution: Option<Execution>,
    schema: SchemaRef,
    /// The errno and message of the error that ended the stream; every later
    /// `get_next` reports it again.
    error: Option<(c_int, CString)>,
}

impl Private {
    fn fail(&mut self, e: Classified) -> c_int {
        let code = if e.is_io() { EIO } else { EINVAL };
        let message =
            CString::new(e.to_string().replace('\0', "\\0")).expect("interior NULs were replaced");
        self.error = Some((code, message));
        self.execution = None;
        code
    }
}

/// The stream's `private_data`, which only this module's callbacks create.
///
/// # Safety
///
/// `stream` must be a live stream made by [`export`] and not yet released.
unsafe fn private<'a>(stream: *mut FFI_ArrowArrayStream) -> &'a mut Private {
    unsafe { &mut *((*stream).private_data as *mut Private) }
}

unsafe extern "C" fn get_schema(
    stream: *mut FFI_ArrowArrayStream,
    out: *mut FFI_ArrowSchema,
) -> c_int {
    let private = unsafe { private(stream) };
    let exported = catch_unwind(AssertUnwindSafe(|| {
        FFI_ArrowSchema::try_from(private.schema.as_ref())
    }));
    match exported {
        Ok(Ok(schema)) => {
            unsafe { ptr::write(out, schema) };
            0
        }
        Ok(Err(e)) => private.fail(Classified::new("ExecutionError", e.to_string())),
        Err(_) => private.fail(Classified::new(
            "ExecutionError",
            "internal error: exporting the schema panicked",
        )),
    }
}

unsafe extern "C" fn get_next(
    stream: *mut FFI_ArrowArrayStream,
    out: *mut FFI_ArrowArray,
) -> c_int {
    let private = unsafe { private(stream) };
    if let Some((code, _)) = &private.error {
        return *code;
    }
    let Some(execution) = private.execution.as_mut() else {
        unsafe { ptr::write(out, FFI_ArrowArray::empty()) };
        return 0;
    };
    // `Execution::pull` catches a panic of the engine; this catches one of
    // the export.
    let pulled = catch_unwind(AssertUnwindSafe(|| {
        execution
            .pull()
            .map(|r| r.map(|batch| FFI_ArrowArray::new(&StructArray::from(batch).to_data())))
    }));
    match pulled {
        Ok(Some(Ok(array))) => {
            unsafe { ptr::write_unaligned(out, array) };
            0
        }
        Ok(None) => {
            private.execution = None;
            unsafe { ptr::write(out, FFI_ArrowArray::empty()) };
            0
        }
        Ok(Some(Err(e))) => private.fail(e),
        Err(_) => private.fail(Classified::new(
            "ExecutionError",
            "internal error: exporting a batch panicked",
        )),
    }
}

unsafe extern "C" fn get_last_error(stream: *mut FFI_ArrowArrayStream) -> *const c_char {
    let private = unsafe { private(stream) };
    match &private.error {
        Some((_, message)) => message.as_ptr(),
        None => ptr::null(),
    }
}

unsafe extern "C" fn release(stream: *mut FFI_ArrowArrayStream) {
    if stream.is_null() {
        return;
    }
    let stream = unsafe { &mut *stream };
    if !stream.private_data.is_null() {
        let private = unsafe { Box::from_raw(stream.private_data as *mut Private) };
        // Dropping the execution aborts its tasks; a panic there must not
        // cross the C ABI either.
        let _ = catch_unwind(AssertUnwindSafe(move || drop(private)));
    }
    stream.private_data = ptr::null_mut();
    stream.get_schema = None;
    stream.get_next = None;
    stream.get_last_error = None;
    stream.release = None;
}

/// A C stream over `execution` with this module's callbacks.
pub(crate) fn export(execution: Execution) -> FFI_ArrowArrayStream {
    let private = Box::new(Private {
        schema: execution.schema(),
        execution: Some(execution),
        error: None,
    });
    FFI_ArrowArrayStream {
        get_schema: Some(get_schema),
        get_next: Some(get_next),
        get_last_error: Some(get_last_error),
        release: Some(release),
        private_data: Box::into_raw(private) as *mut c_void,
    }
}

/// [`export`] in the `arrow_array_stream` capsule `__arrow_c_stream__`
/// returns. A capsule that is never imported releases the stream from its
/// destructor (`FFI_ArrowArrayStream`'s `Drop` calls `release`).
pub(crate) fn stream_capsule(
    py: Python<'_>,
    execution: Execution,
) -> PyResult<Bound<'_, PyCapsule>> {
    PyCapsule::new_with_value(py, export(execution), ARROW_STREAM_CAPSULE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::ffi_stream::ArrowArrayStreamReader;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use std::sync::Arc;

    fn execution(batches: usize, fault: Option<super::super::Fault>) -> Execution {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let rows: Vec<_> = (0..batches as i64)
            .map(|i| {
                Ok(RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from(vec![i]))],
                )
                .expect("batch"))
            })
            .collect();
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures_util::stream::iter(rows),
        ));
        Execution {
            stream,
            fault,
            pulled: 0,
            _live: super::super::Live::new(),
        }
    }

    /// Read the exported stream back with arrow-rs's importer, which reports
    /// a producer's error as `"<errno text>: <get_last_error>"`.
    fn read_back(stream: FFI_ArrowArrayStream) -> (usize, Option<String>) {
        let mut reader = ArrowArrayStreamReader::try_new(stream).expect("schema");
        let mut rows = 0;
        for batch in &mut reader {
            match batch {
                Ok(b) => rows += b.num_rows(),
                Err(e) => return (rows, Some(e.to_string())),
            }
        }
        (rows, None)
    }

    #[test]
    fn a_stream_without_errors_yields_every_batch() {
        assert_eq!(read_back(export(execution(3, None))), (3, None));
    }

    #[test]
    fn an_error_ends_the_stream_with_its_code_and_a_message_starting_with_the_class() {
        let fault = super::super::Fault {
            after: 2,
            error: Classified::new("SourceNotFoundError", "no file at /x.parquet"),
        };
        let mut stream = export(execution(5, Some(fault)));
        let mut array = FFI_ArrowArray::empty();
        let get_next = stream.get_next.expect("callback");
        for _ in 0..2 {
            assert_eq!(unsafe { get_next(&mut stream, &mut array) }, 0);
            drop(std::mem::replace(&mut array, FFI_ArrowArray::empty()));
        }
        assert_eq!(unsafe { get_next(&mut stream, &mut array) }, EIO);
        // The error is sticky.
        assert_eq!(unsafe { get_next(&mut stream, &mut array) }, EIO);
        let message = unsafe { std::ffi::CStr::from_ptr(get_last_error(&mut stream)) };
        assert_eq!(
            message.to_str().expect("utf-8"),
            "SourceNotFoundError: no file at /x.parquet"
        );

        let fault = super::super::Fault {
            after: 0,
            error: Classified::new("ArithmeticOverflowError", "int64 overflow"),
        };
        let (rows, error) = read_back(export(execution(5, Some(fault))));
        assert_eq!(rows, 0);
        let error = error.expect("error");
        assert!(
            error.ends_with("ArithmeticOverflowError: int64 overflow"),
            "{error}"
        );
    }
}
