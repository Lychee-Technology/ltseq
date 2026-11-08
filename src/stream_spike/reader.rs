//! The iterator `to_batches()` would wrap in
//! `pyarrow.RecordBatchReader.from_batches` (question 1 of #256).
//!
//! pyarrow's reader over a Python iterator calls `next()` on it under the
//! GIL for every batch and re-raises the exception `next()` raised, class
//! and all. This iterator raises the class [`super::classify`] names, from
//! the `classes` the Python side passed.
//!
//! How `next()` waits for a batch is a [`Gil`] mode, so that question 3 can
//! tell the GIL hand-offs this iterator adds from the ones pyarrow makes.

use std::sync::Mutex;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::pyarrow::ToPyArrow;
use datafusion::arrow::record_batch::RecordBatch;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::classify::Classified;
use super::Execution;

/// How `next()` waits for the next batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Gil {
    /// Release the GIL for the whole pull, as the cursor does (ADR 0016).
    Detach,
    /// Keep the GIL while the engine computes the batch: no hand-off of
    /// this iterator's own, and no other Python thread runs meanwhile. A
    /// lower bound for measurement, not a candidate.
    Attached,
    /// Take a batch the stream already has without releasing the GIL;
    /// release it only to wait.
    Poll,
}

impl Gil {
    pub(crate) fn parse(text: &str) -> PyResult<Self> {
        match text {
            "detach" => Ok(Gil::Detach),
            "attached" => Ok(Gil::Attached),
            "poll" => Ok(Gil::Poll),
            other => Err(PyValueError::new_err(format!(
                "gil must be 'detach', 'attached' or 'poll', not {other:?}"
            ))),
        }
    }
}

/// What one `next()` found.
enum Pulled {
    Batch(RecordBatch),
    End,
    Failed(Classified),
}

impl Pulled {
    /// Record `found` in `slot`: the stream is released once it ends or
    /// fails.
    fn from(slot: &mut Option<Execution>, found: Option<Result<RecordBatch, Classified>>) -> Self {
        match found {
            Some(Ok(batch)) => Pulled::Batch(batch),
            None => {
                *slot = None;
                Pulled::End
            }
            Some(Err(e)) => {
                *slot = None;
                Pulled::Failed(e)
            }
        }
    }
}

#[pyclass(module = "ltseq.ltseq_core", name = "_SpikeBatches")]
pub(crate) struct SpikeBatches {
    /// `None` once the stream ended, failed or was closed. The mutex is
    /// waited on only with the GIL released (see `LTSeqCursor::pull_batch`);
    /// with the GIL held it is only tried.
    execution: Mutex<Option<Execution>>,
    schema: SchemaRef,
    classes: Py<PyDict>,
    gil: Gil,
}

impl SpikeBatches {
    pub(crate) fn new(execution: Execution, classes: Py<PyDict>, gil: Gil) -> Self {
        SpikeBatches {
            schema: execution.schema(),
            execution: Mutex::new(Some(execution)),
            classes,
            gil,
        }
    }

    /// Wait for the next batch.
    fn pull(&self) -> Pulled {
        let mut guard = match self.execution.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let Some(execution) = guard.as_mut() else {
            return Pulled::End;
        };
        let found = execution.pull();
        Pulled::from(&mut guard, found)
    }

    /// The next batch if one is ready and the mutex is free; `None` if
    /// getting it means waiting.
    fn try_pull(&self) -> Option<Pulled> {
        let mut guard = self.execution.try_lock().ok()?;
        let Some(execution) = guard.as_mut() else {
            return Some(Pulled::End);
        };
        let found = execution.try_pull()?;
        Some(Pulled::from(&mut guard, found))
    }
}

#[pymethods]
impl SpikeBatches {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// The next batch as a `pyarrow.RecordBatch`; `StopIteration` at the end;
    /// the classified exception when execution fails.
    fn __next__<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let pulled = match self.gil {
            Gil::Detach => py.detach(|| self.pull()),
            Gil::Attached => self.pull(),
            Gil::Poll => match self.try_pull() {
                Some(pulled) => pulled,
                None => py.detach(|| self.pull()),
            },
        };
        match pulled {
            Pulled::Batch(batch) => batch.to_pyarrow(py).map(Some),
            Pulled::End => Ok(None),
            Pulled::Failed(e) => Err(super::raise(self.classes.bind(py), e)),
        }
    }

    /// The stream's schema as a `pyarrow.Schema`.
    fn schema<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.schema.as_ref().to_pyarrow(py)
    }

    /// Stop the execution. pyarrow's reader never calls this (its `close()`
    /// does not reach the iterator); it exists to show what a wrapper that
    /// did call it would get.
    fn close(&self, py: Python<'_>) {
        let execution = py.detach(|| match self.execution.lock() {
            Ok(mut guard) => guard.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        });
        // Dropped with the GIL released too: dropping a stream aborts its
        // tasks.
        py.detach(move || drop(execution));
    }
}
