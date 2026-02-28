//! Error types for LTSeq operations and PyExpr transpilation
//!
//! Provides two error types:
//! - [`PyExprError`]: Expression parsing errors (used in `types.rs` for dict → PyExpr)
//! - [`LtseqError`]: Operational errors for table operations, I/O, and transpilation
//!
//! Both implement `From<...> for PyErr` for automatic conversion in `#[pymethods]`.

use pyo3::{exceptions, PyErr};
use std::fmt;

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
        }
    }
}

impl std::error::Error for LtseqError {}

impl From<LtseqError> for PyErr {
    fn from(err: LtseqError) -> Self {
        match &err {
            // Map to PyValueError
            LtseqError::Validation(_)
            | LtseqError::ColumnNotFound(_)
            | LtseqError::Transpile(_) => {
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
    /// Create a runtime error by wrapping any Display-able source error with context.
    ///
    /// Usage: `LtseqError::with_context("Failed to collect", err)`
    pub fn with_context(context: &str, source: impl fmt::Display) -> Self {
        LtseqError::Runtime(format!("{}: {}", context, source))
    }

    /// Create an I/O error by wrapping a source error with context.
    pub fn io(context: &str, source: impl fmt::Display) -> Self {
        LtseqError::Io(format!("{}: {}", context, source))
    }

    /// Create a collect/execution error from a DataFusion error.
    pub fn collect(source: impl fmt::Display) -> Self {
        LtseqError::Collect(format!("Failed to collect results: {}", source))
    }
}
