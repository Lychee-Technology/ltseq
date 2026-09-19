//! GIL release boundary for heavy execution paths (issue #142).
//!
//! Every `#[pymethods]` entry point that performs real work — DataFusion
//! `collect` / `execute_stream` / `count`, file scans and writes, the rayon
//! Parquet paths, Arrow stream import — runs that work through [`detached`],
//! which releases the GIL for its duration so other Python threads keep
//! running. Plan building (`filter`, `select`, `sort`, `join`, ...) is
//! synchronous and cheap and stays under the GIL.
//!
//! # Layering contract
//!
//! 1. **Parse under the GIL.** Anything that touches Python objects
//!    (`Bound<'_, PyDict>` → `PyExpr`, dict → `ScalarValue`) happens in the
//!    stub or in the first half of the op, before `detached`.
//! 2. **Execute detached.** The closure sees only plain Rust values. The
//!    `Send` bound on `F` rejects a captured `Bound` at compile time.
//! 3. **Convert after.** The closure reports failures as [`LtseqError`];
//!    the `PyErr` is built here, once the GIL is held again. Constructing or
//!    formatting a `PyErr` inside the closure would re-attach the GIL from a
//!    detached thread (`From<LtseqError> for PyErr` and `Display for PyErr`
//!    both call `Python::attach`), so the execution halves of `src/ops/*`
//!    return `Result<_, LtseqError>` rather than `PyResult`.

use crate::error::LtseqError;
use pyo3::{PyErr, PyResult, Python};

/// Run a pure-Rust computation with the GIL released.
///
/// See the module docs for the contract the closure must follow.
pub(crate) fn detached<T, F>(py: Python<'_>, f: F) -> PyResult<T>
where
    T: Send,
    F: Send + FnOnce() -> Result<T, LtseqError>,
{
    py.detach(f).map_err(PyErr::from)
}
