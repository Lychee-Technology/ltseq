//! Error types for LTSeq operations and PyExpr transpilation
//!
//! Provides two error types:
//! - [`PyExprError`]: Expression parsing errors (used in `types.rs` for dict → PyExpr)
//! - [`LtseqError`]: Operational errors for table operations, I/O, and transpilation
//!
//! Both implement `From<...> for PyErr` for automatic conversion in `#[pymethods]`.

use pyo3::sync::GILOnceCell;
use pyo3::types::{PyAnyMethods, PyType};
use pyo3::{exceptions, Bound, Py, PyErr, Python};
use std::error::Error as StdError;
use std::fmt;

/// Cached handle to the `ltseq.exceptions` module (looked up once under the GIL).
/// `None` means the import failed — e.g. `ltseq_core` used standalone before the
/// Python package is importable — in which case callers fall back to builtins.
static LTSEQ_EXCEPTIONS: GILOnceCell<Option<Py<pyo3::types::PyModule>>> = GILOnceCell::new();

/// Fetch a custom exception class from `ltseq.exceptions` by name, if importable.
fn ltseq_exc<'py>(py: Python<'py>, name: &str) -> Option<Bound<'py, PyType>> {
    let module = LTSEQ_EXCEPTIONS
        .get_or_init(py, || py.import("ltseq.exceptions").map(|m| m.unbind()).ok())
        .as_ref()?;
    module
        .bind(py)
        .getattr(name)
        .ok()
        .and_then(|obj| obj.downcast_into::<PyType>().ok())
}

/// Raise `message` as the custom exception `name`, or fall back to `Fallback`
/// (a builtin) when the Python package is not importable. The fallback is
/// behaviorally invisible because every custom class also subclasses its
/// builtin counterpart.
fn raise_custom<Fallback>(name: &str, message: String) -> PyErr
where
    Fallback: pyo3::PyTypeInfo,
{
    Python::attach(|py| match ltseq_exc(py, name) {
        Some(exc_type) => PyErr::from_type(exc_type, message),
        None => PyErr::new::<Fallback, _>(message),
    })
}

// ---------------------------------------------------------------------------
// PyExprError — Expression parsing (types.rs)
// ---------------------------------------------------------------------------

/// Error type for PyExpr deserialization from Python dicts.
#[derive(Debug)]
pub enum PyExprError {
    MissingField(String),
    InvalidType(String),
    UnknownVariant(String),
}

impl fmt::Display for PyExprError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PyExprError::MissingField(field) => write!(f, "Missing field: {}", field),
            PyExprError::InvalidType(msg) => write!(f, "Invalid type: {}", msg),
            PyExprError::UnknownVariant(var) => write!(f, "Unknown expression type: {}", var),
        }
    }
}

impl std::error::Error for PyExprError {}

impl From<PyExprError> for PyErr {
    fn from(err: PyExprError) -> Self {
        PyErr::new::<exceptions::PyValueError, _>(err.to_string())
    }
}

// ---------------------------------------------------------------------------
// LtseqError — Operational errors
// ---------------------------------------------------------------------------

/// Unified error type for all LTSeq table operations.
///
/// Each variant maps to a Python exception type:
/// - `NoData`, `NoSchema`, `Runtime`, `Io`, `Collect` → `PyRuntimeError`
/// - `Validation`, `ColumnNotFound`, `Transpile`, `Type` → `PyValueError`
/// - `TypeMismatch` → `PyTypeError`
///
/// The `Context` variant wraps a source error with additional context,
/// enabling error chain traversal via `std::error::Error::source()`.
#[derive(Debug)]
pub enum LtseqError {
    /// Table has no data loaded (dataframe is None).
    NoData,

    /// Table has no schema available.
    NoSchema,

    /// Column not found in the table schema.
    ColumnNotFound(String),

    /// Operation requires sorted input but the table is unsorted.
    SortRequired(String),

    /// Two tables have incompatible schemas for the operation.
    SchemaMismatch(String),

    /// Validation error — invalid arguments, missing parameters, etc.
    Validation(String),

    /// Expression transpilation error.
    Transpile(String),

    /// Type mismatch error.
    TypeMismatch(String),

    /// Runtime error with context message wrapping a source error.
    Runtime(String),

    /// I/O error with context.
    Io(String),

    /// DataFrame collect/execution error.
    Collect(String),

    /// Context error: wraps a source error with additional context.
    /// Used for preserving error chains.
    Context {
        message: String,
        source: Box<dyn StdError + Send + Sync>,
    },
}

impl fmt::Display for LtseqError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LtseqError::NoData => write!(f, "No data loaded"),
            LtseqError::NoSchema => write!(f, "Schema not available"),
            LtseqError::ColumnNotFound(col) => write!(f, "Column '{}' not found in schema", col),
            LtseqError::SortRequired(msg) => write!(f, "{}", msg),
            LtseqError::SchemaMismatch(msg) => write!(f, "{}", msg),
            LtseqError::Validation(msg) => write!(f, "{}", msg),
            LtseqError::Transpile(msg) => write!(f, "Transpilation error: {}", msg),
            LtseqError::TypeMismatch(msg) => write!(f, "Type error: {}", msg),
            LtseqError::Runtime(msg) => write!(f, "{}", msg),
            LtseqError::Io(msg) => write!(f, "{}", msg),
            LtseqError::Collect(msg) => write!(f, "{}", msg),
            LtseqError::Context { message, source } => {
                write!(f, "{}: {}", message, source)
            }
        }
    }
}

impl StdError for LtseqError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            LtseqError::Context { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}

impl From<LtseqError> for PyErr {
    fn from(err: LtseqError) -> Self {
        match &err {
            // Custom exception hierarchy (ltseq.exceptions), falling back to the
            // builtin each class subclasses when the package is not importable.
            LtseqError::ColumnNotFound(_) => {
                raise_custom::<exceptions::PyValueError>("ColumnNotFoundError", err.to_string())
            }
            LtseqError::SortRequired(_) => {
                raise_custom::<exceptions::PyValueError>("SortRequiredError", err.to_string())
            }
            LtseqError::SchemaMismatch(_) => {
                raise_custom::<exceptions::PyValueError>("SchemaMismatchError", err.to_string())
            }
            // Map to PyValueError
            LtseqError::Validation(_)
            | LtseqError::Transpile(_)
            | LtseqError::Context { .. } => {
                PyErr::new::<exceptions::PyValueError, _>(err.to_string())
            }
            // Map to PyTypeError
            LtseqError::TypeMismatch(_) => {
                PyErr::new::<exceptions::PyTypeError, _>(err.to_string())
            }
            // Map to PyRuntimeError
            LtseqError::NoData
            | LtseqError::NoSchema
            | LtseqError::Runtime(_)
            | LtseqError::Io(_)
            | LtseqError::Collect(_) => {
                PyErr::new::<exceptions::PyRuntimeError, _>(err.to_string())
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Convenience constructors for common error patterns
// ---------------------------------------------------------------------------

impl LtseqError {
    /// Create a context error by wrapping any source error with context.
    ///
    /// This preserves the error chain for debugging via `std::error::Error::source()`.
    ///
    /// Usage: `LtseqError::with_context("Failed to collect", err)`
    pub fn with_context(context: &str, source: impl StdError + Send + Sync + 'static) -> Self {
        LtseqError::Context {
            message: context.to_string(),
            source: Box::new(source),
        }
    }

    /// Create a runtime error by formatting a Display-able source error with context.
    ///
    /// Unlike `with_context`, this does NOT preserve the error chain.
    /// Use when you only need the error message, not the source.
    ///
    /// Usage: `LtseqError::runtime("Failed to collect: {}", err)`
    pub fn runtime(context: &str, source: impl fmt::Display) -> Self {
        LtseqError::Runtime(format!("{}: {}", context, source))
    }

    /// Create an I/O error by wrapping a source error with context.
    ///
    /// Preserves the error chain if source implements StdError.
    pub fn io(context: &str, source: impl StdError + Send + Sync + 'static) -> Self {
        LtseqError::Context {
            message: format!("I/O error ({})", context),
            source: Box::new(source),
        }
    }

    /// Create a collect/execution error from a DataFusion error.
    ///
    /// Preserves the error chain.
    pub fn collect(source: impl StdError + Send + Sync + 'static) -> Self {
        LtseqError::Context {
            message: "Failed to collect results".to_string(),
            source: Box::new(source),
        }
    }
}
