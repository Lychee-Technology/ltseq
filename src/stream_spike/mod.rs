//! Spike for #256: the two error paths of v0.5's streams (contract §15.1
//! and §16.4), and what reading `to_batches()` through a Rust iterator costs.
//!
//! Compiled only with the `stream_spike` cargo feature, so no shipped build
//! contains it. It is not M1's `to_batches()` or `__arrow_c_stream__`; it is
//! as much of each as the spike needs to answer three questions, and goes
//! away when M1 lands:
//!
//! 1. Does a `pyarrow.RecordBatchReader` built over an iterator implemented
//!    in Rust ([`reader::SpikeBatches`]) raise the LTSeq class on
//!    `read_next_batch`, iteration and `read_all`?
//! 2. Does a C stream that sets its own errno and `get_last_error` text
//!    ([`capsule`]) reach pyarrow, DuckDB and Polars as `EIO`/`EINVAL` with a
//!    message that starts with the class name?
//! 3. What does the iterator cost against today's C stream, which never
//!    calls into Python?
//!
//! v0.5's exception classes do not exist yet. The Python side passes
//! stand-ins with the contract's bases (`ltseq._stream_spike`), keyed by
//! class name, and [`classify`] names the class of an execution error. One
//! of the four classes the issue names, `OrderViolationError`, has no
//! execution error today (`ArithmeticOverflowError` has `INT64_MIN // -1`),
//! so a [`Fault`] injects it (and any other class) after a given number of
//! batches.
//!
//! [`LIVE`] counts the executions a reader or stream holds, so a test can
//! tell whether closing or dropping one stopped its execution.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::array::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures_util::{FutureExt, StreamExt};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::engine::RUNTIME;
use crate::error::LtseqError;
use crate::gil::detached;
use crate::LTSeqTable;

pub(crate) mod capsule;
pub(crate) mod classify;
pub(crate) mod reader;

use classify::{Classified, CLASSES};

/// Executions held by a spike reader or stream that has not dropped them.
static LIVE: AtomicUsize = AtomicUsize::new(0);

/// Counts one execution in [`LIVE`] for as long as it is held.
struct Live;

impl Live {
    fn new() -> Self {
        LIVE.fetch_add(1, Ordering::SeqCst);
        Live
    }
}

impl Drop for Live {
    fn drop(&mut self) {
        LIVE.fetch_sub(1, Ordering::SeqCst);
    }
}

/// An error of the given class, raised instead of the batch after the
/// first `after` batches.
#[derive(Debug, Clone)]
pub(crate) struct Fault {
    after: usize,
    error: Classified,
}

/// One execution of a table's plan, in the order its rows are defined in,
/// with an optional injected fault.
pub(crate) struct Execution {
    stream: SendableRecordBatchStream,
    fault: Option<Fault>,
    pulled: usize,
    _live: Live,
}

impl Execution {
    /// Start executing `table`: plan it, as `__arrow_c_stream__` does today,
    /// and start the execution. A planning error is today's `LtseqError`;
    /// an error starting the execution (a Parquet footer read at every
    /// execution, #360, so a file deleted since the reader's call) is
    /// classified like one raised by a read, and raises at the call.
    fn start(
        table: &LTSeqTable,
        fault: Option<Fault>,
    ) -> Result<Result<Self, Classified>, LtseqError> {
        let df = table.ordered_df()?;
        Ok(match RUNTIME.block_on(df.execute_stream()) {
            Ok(stream) => Ok(Execution {
                stream,
                fault,
                pulled: 0,
                _live: Live::new(),
            }),
            Err(e) => Err(classify::classify(&e)),
        })
    }

    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.stream.schema()
    }

    /// The next batch, `None` at the end, or the classified error that ends
    /// the stream. Blocks the calling thread on the engine's runtime; a
    /// panic is caught and reported as an `ExecutionError`, because it must
    /// not unwind across the C ABI or into Python.
    fn pull(&mut self) -> Option<Result<RecordBatch, Classified>> {
        self.pull_with(|stream| Some(RUNTIME.block_on(stream.next())))
            .expect("block_on always finishes")
    }

    /// The next batch if the stream has one ready, without waiting: `None`
    /// when it would have to wait. A ready poll runs on the calling thread,
    /// so in a plan without a repartition it computes the batch there.
    fn try_pull(&mut self) -> Option<Option<Result<RecordBatch, Classified>>> {
        self.pull_with(|stream| {
            let _runtime = RUNTIME.enter();
            stream.next().now_or_never()
        })
    }

    fn pull_with(
        &mut self,
        next: impl FnOnce(
            &mut SendableRecordBatchStream,
        ) -> Option<Option<datafusion::error::Result<RecordBatch>>>,
    ) -> Option<Option<Result<RecordBatch, Classified>>> {
        if let Some(fault) = &self.fault {
            if self.pulled == fault.after {
                return Some(Some(Err(fault.error.clone())));
            }
        }
        let pulled = catch_unwind(AssertUnwindSafe(|| next(&mut self.stream)));
        Some(match pulled {
            Ok(None) => return None,
            Ok(Some(Some(Ok(batch)))) => {
                self.pulled += 1;
                Some(Ok(batch))
            }
            Ok(Some(Some(Err(e)))) => Some(Err(classify::classify(&e))),
            Ok(Some(None)) => None,
            Err(panic) => {
                let msg = panic
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "unknown panic".to_string());
                Some(Err(Classified::new(
                    "ExecutionError",
                    format!("internal error: execution panicked: {msg}"),
                )))
            }
        })
    }
}

/// Read `fault` from Python: `None`, or `(after, class, message)`.
fn parse_fault(fault: Option<(usize, String, String)>) -> PyResult<Option<Fault>> {
    let Some((after, class, message)) = fault else {
        return Ok(None);
    };
    let Some(&class) = CLASSES.iter().find(|c| **c == class) else {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "unknown exception class {class:?}; the contract's classes are {CLASSES:?}"
        )));
    };
    Ok(Some(Fault {
        after,
        error: Classified::new(class, message),
    }))
}

/// The Python exception for `e`: an instance of the class `classes`
/// registers under its name, with its message, or a `RuntimeError` naming
/// the class when there is none.
pub(crate) fn raise(classes: &Bound<'_, PyDict>, e: Classified) -> PyErr {
    match classes.get_item(e.class) {
        Ok(Some(class)) => match class.call1((e.message,)) {
            Ok(instance) => PyErr::from_value(instance),
            Err(err) => err,
        },
        _ => PyRuntimeError::new_err(e.to_string()),
    }
}

/// Start an execution of `table` and return the iterator `to_batches()`
/// would wrap in `pyarrow.RecordBatchReader.from_batches`. `classes` maps a
/// class name to the Python class raised for it.
///
/// `gil` says how `next()` waits for a batch (see [`reader::Gil`]):
/// `"detach"` (the default, ADR 0016), `"attached"` or `"poll"`.
#[pyfunction]
#[pyo3(signature = (table, classes, fault=None, gil="detach"))]
pub(crate) fn _spike_batches(
    py: Python<'_>,
    table: &LTSeqTable,
    classes: Bound<'_, PyDict>,
    fault: Option<(usize, String, String)>,
    gil: &str,
) -> PyResult<reader::SpikeBatches> {
    let fault = parse_fault(fault)?;
    let gil = reader::Gil::parse(gil)?;
    match detached(py, || Execution::start(table, fault))? {
        Ok(execution) => Ok(reader::SpikeBatches::new(execution, classes.unbind(), gil)),
        Err(e) => Err(raise(&classes, e)),
    }
}

/// Start an execution of `table` and return it as an `arrow_array_stream`
/// capsule whose errors carry LTSeq's own errno and message. An error
/// starting the execution raises its class from this call, which is a
/// Python call (`__arrow_c_stream__`), not a read of the stream.
#[pyfunction]
#[pyo3(signature = (table, classes, fault=None))]
pub(crate) fn _spike_stream<'py>(
    py: Python<'py>,
    table: &LTSeqTable,
    classes: Bound<'py, PyDict>,
    fault: Option<(usize, String, String)>,
) -> PyResult<Bound<'py, pyo3::types::PyCapsule>> {
    let fault = parse_fault(fault)?;
    match detached(py, || Execution::start(table, fault))? {
        Ok(execution) => capsule::stream_capsule(py, execution),
        Err(e) => Err(raise(&classes, e)),
    }
}

/// The executions spike readers and streams hold now.
#[pyfunction]
pub(crate) fn _spike_live() -> usize {
    LIVE.load(Ordering::SeqCst)
}

/// The class names [`classify`] can return, in the contract's order.
#[pyfunction]
pub(crate) fn _spike_classes() -> Vec<&'static str> {
    CLASSES.to_vec()
}
