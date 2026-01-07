//! Error types for PyExpr operations and transpilation

use pyo3::{exceptions, PyErr};

/// Error type for PyExpr operations
#[derive(Debug)]
pub enum PyExprError {
    MissingField(String),
    InvalidType(String),
    UnknownVariant(String),
    DeserializationFailed(String),
    TranspilationFailed(String),
}

impl std::fmt::Display for PyExprError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PyExprError::MissingField(field) => write!(f, "Missing field: {}", field),
            PyExprError::InvalidType(msg) => write!(f, "Invalid type: {}", msg),
            PyExprError::UnknownVariant(var) => write!(f, "Unknown expression type: {}", var),
            PyExprError::DeserializationFailed(msg) => write!(f, "Deserialization failed: {}", msg),
            PyExprError::TranspilationFailed(msg) => write!(f, "Transpilation failed: {}", msg),
        }
    }
}

impl std::error::Error for PyExprError {}

/// Wrapper to convert PyExprError to PyErr
impl From<PyExprError> for PyErr {
    fn from(err: PyExprError) -> Self {
        PyErr::new::<exceptions::PyValueError, _>(err.to_string())
    }
}
