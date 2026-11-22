//! Error types for LTSeq operations and PyExpr transpilation
//!
//! Provides two error types:
//! - [`PyExprError`]: Expression parsing errors (used in `types.rs` for dict → PyExpr)
//! - [`LtseqError`]: Operational errors for table operations, I/O, and transpilation
//!
//! Both implement `From<...> for PyErr` for automatic conversion in `#[pymethods]`.

use pyo3::sync::PyOnceLock;
use pyo3::types::{PyAnyMethods, PyType};
use pyo3::{exceptions, Bound, Py, PyErr, Python};
use std::error::Error as StdError;
use std::fmt;

/// Cached handle to the `ltseq.exceptions` module (looked up once, cached for the process lifetime).
/// `None` means the import failed — e.g. `ltseq_core` used standalone before the
/// Python package is importable — in which case callers fall back to builtins.
static LTSEQ_EXCEPTIONS: PyOnceLock<Option<Py<pyo3::types::PyModule>>> = PyOnceLock::new();

/// Fetch a custom exception class from `ltseq.exceptions` by name, if importable.
fn ltseq_exc<'py>(py: Python<'py>, name: &str) -> Option<Bound<'py, PyType>> {
    let module = LTSEQ_EXCEPTIONS
        .get_or_init(py, || py.import("ltseq.exceptions").map(|m| m.unbind()).ok())
        .as_ref()?;
    module
        .bind(py)
        .getattr(name)
        .ok()
        .and_then(|obj| obj.cast_into::<PyType>().ok())
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
    /// A Literal payload that does not match the wire contract. Python's
    /// encoder never produces one, so reaching this means an LTSeq bug or a
    /// hand-built payload, not a user typo.
    InvalidLiteral(String),
    /// An expression deeper than the limit given (`crate::stack`).
    TooDeep(usize),
}

impl fmt::Display for PyExprError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PyExprError::MissingField(field) => write!(f, "Missing field: {}", field),
            PyExprError::InvalidType(msg) => write!(f, "Invalid type: {}", msg),
            PyExprError::UnknownVariant(var) => write!(f, "Unknown expression type: {}", var),
            PyExprError::InvalidLiteral(msg) => write!(f, "Malformed literal payload: {}", msg),
            PyExprError::TooDeep(limit) => write!(
                f,
                "Expression is nested more than {limit} levels deep, the most LTSeq plans; \
                 build it from shallower parts (for example, a balanced tree of `|` \
                 rather than a chain)"
            ),
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
/// Each variant raises one Python exception class (`From<LtseqError> for
/// PyErr` below):
/// - `ColumnNotFound`, `SortRequired`, `SchemaMismatch` → the
///   `ltseq.exceptions` class of that name, each a `ValueError`
/// - `Validation`, `Transpile`, `Io`, `Context` → `ValueError`
/// - `TypeMismatch` → `TypeError`
/// - `NoData`, `NoSchema`, `Runtime` → `RuntimeError`
///
/// A failure is classed by the variant that carries it, never by its
/// message. The transpiler reports a column it cannot find as
/// `ColumnNotFound`, a literal argument of the wrong kind (`shift("1")`,
/// `rolling("3")`, `LiteralValue::require_*`) as `Validation`, and every
/// other refusal as `Transpile`, whichever operation lowers the expression. An I/O failure is `Io`
/// (`LtseqError::io`) and a failed execution is `Context`
/// (`LtseqError::collect`, `with_context`), so a
/// reader, a writer and a terminal raise one class for the same kind of
/// failure. These are the classes LTSeq raises today; the v0.5 contract
/// (§20.1) gives I/O and execution failures classes of their own, which is
/// M15 (#266).
///
/// `Io` and `Context` keep the error they wrap, reachable through
/// `std::error::Error::source()`.
#[derive(Debug)]
pub enum LtseqError {
    /// The table was never loaded from a data source (`LTSeqTable::unloaded`).
    /// An empty result is a zero-row table, never this error.
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

    /// Opening, reading, creating or writing a file failed. Built by
    /// `LtseqError::io`, which keeps the underlying error as `source`.
    Io {
        message: String,
        source: Box<dyn StdError + Send + Sync>,
    },

    /// Context error: wraps a source error with additional context.
    /// Used for preserving error chains; a failed execution is one
    /// (`LtseqError::collect`).
    Context {
        message: String,
        source: Box<dyn StdError + Send + Sync>,
    },
}

impl fmt::Display for LtseqError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LtseqError::NoData => write!(
                f,
                "No data loaded: this table was never loaded from a data source \
                 (create it with read_csv(), read_parquet(), from_arrow() or from_pandas())"
            ),
            LtseqError::NoSchema => write!(f, "Schema not available"),
            LtseqError::ColumnNotFound(col) => write!(f, "Column '{}' not found in schema", col),
            LtseqError::SortRequired(msg) => write!(f, "{}", msg),
            LtseqError::SchemaMismatch(msg) => write!(f, "{}", msg),
            LtseqError::Validation(msg) => write!(f, "{}", msg),
            LtseqError::Transpile(msg) => write!(f, "Transpilation error: {}", msg),
            LtseqError::TypeMismatch(msg) => write!(f, "Type error: {}", msg),
            LtseqError::Runtime(msg) => write!(f, "{}", msg),
            LtseqError::Io { message, source } | LtseqError::Context { message, source } => {
                write!(f, "{}: {}", message, source)
            }
        }
    }
}

impl StdError for LtseqError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            LtseqError::Io { source, .. } | LtseqError::Context { source, .. } => {
                Some(source.as_ref())
            }
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
            // Map to PyValueError. `Io` and `Context` are listed apart from
            // the argument errors because M15 (#266) moves them to classes
            // of their own.
            LtseqError::Validation(_) | LtseqError::Transpile(_) => {
                PyErr::new::<exceptions::PyValueError, _>(err.to_string())
            }
            LtseqError::Io { .. } | LtseqError::Context { .. } => {
                PyErr::new::<exceptions::PyValueError, _>(err.to_string())
            }
            // Map to PyTypeError
            LtseqError::TypeMismatch(_) => {
                PyErr::new::<exceptions::PyTypeError, _>(err.to_string())
            }
            // Map to PyRuntimeError
            LtseqError::NoData | LtseqError::NoSchema | LtseqError::Runtime(_) => {
                PyErr::new::<exceptions::PyRuntimeError, _>(err.to_string())
            }
        }
    }
}

/// `LtseqError::Transpile` with a `format!` message:
/// `transpile_error!("{func} requires an argument")`.
macro_rules! transpile_error {
    ($($arg:tt)*) => {
        $crate::error::LtseqError::Transpile(format!($($arg)*))
    };
}
pub(crate) use transpile_error;

/// `LtseqError::Validation` with a `format!` message.
macro_rules! validation_error {
    ($($arg:tt)*) => {
        $crate::error::LtseqError::Validation(format!($($arg)*))
    };
}
pub(crate) use validation_error;

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
    /// Preserves the error chain.
    pub fn io(context: &str, source: impl StdError + Send + Sync + 'static) -> Self {
        LtseqError::Io {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// `io()` builds the variant its name says, which decides the Python
    /// class (#152): it used to build a generic `Context`, so an I/O failure
    /// raised one class or another depending on the constructor its site
    /// happened to use.
    #[test]
    fn io_and_collect_build_their_own_variant_and_keep_the_source() {
        let io = LtseqError::io(
            "Failed to create file 'x'",
            std::io::Error::from(std::io::ErrorKind::NotFound),
        );
        assert!(matches!(io, LtseqError::Io { .. }), "{io:?}");
        assert!(io.source().is_some());
        assert!(
            io.to_string()
                .starts_with("I/O error (Failed to create file 'x'): "),
            "{io}"
        );

        let collect =
            LtseqError::collect(datafusion::error::DataFusionError::Execution("boom".into()));
        assert!(matches!(collect, LtseqError::Context { .. }), "{collect:?}");
        assert!(collect.source().is_some());
        assert!(
            collect
                .to_string()
                .starts_with("Failed to collect results: "),
            "{collect}"
        );
    }
}
