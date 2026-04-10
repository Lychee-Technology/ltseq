//! Error types for LTSeq operations and PyExpr transpilation
//!
//! Provides two error types:
//! - [`PyExprError`]: Expression parsing errors (used in `types.rs` for dict → PyExpr)
//! - [`LtseqError`]: Operational errors for table operations, I/O, and transpilation
//!
//! Both implement `From<...> for PyErr` for automatic conversion in `#[pymethods]`.

use pyo3::{exceptions, PyErr};
use std::fmt;
use std::error::Error as StdError;

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
            // Map to PyValueError
            LtseqError::Validation(_)
            | LtseqError::ColumnNotFound(_)
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
    /// Maps to PyRuntimeError (same as `Collect` variant).
    pub fn collect(source: impl fmt::Display) -> Self {
        LtseqError::Collect(format!("Failed to collect results: {}", source))
    }
}
