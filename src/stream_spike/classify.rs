//! Which v0.5 exception class an execution error belongs to (contract §20.1).
//!
//! DataFusion wraps an operator's error as it crosses the plan: a
//! repartition shares it (`Shared`), a context adds a line (`Context`), an
//! Arrow kernel's error arrives as `ArrowError` or inside `External`. The
//! class is decided by the error at the bottom of that chain, so
//! [`classify`] walks it. Only the errors today's engine raises during
//! execution are mapped here; M1 owns the full mapping, and every error not
//! recognized is an `ExecutionError`.

use std::fmt;
use std::io;

use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use datafusion::object_store;

/// The classes of contract §20.1 that an execution error can have, in its
/// order. `LTSeqError` itself is never raised.
pub(crate) const CLASSES: &[&str] = &[
    "LTSeqTypeError",
    "LTSeqValueError",
    "ColumnNotFoundError",
    "SchemaMismatchError",
    "SortRequiredError",
    "OrderViolationError",
    "DuplicateKeyError",
    "CastError",
    "LTSeqIndexError",
    "ArithmeticOverflowError",
    "DivisionByZeroError",
    "LTSeqIOError",
    "SourceNotFoundError",
    "ExecutionError",
];

/// The classes that end a C stream with `EIO`: `LTSeqIOError` and its
/// subclasses (§16.4).
const IO_CLASSES: &[&str] = &["LTSeqIOError", "SourceNotFoundError"];

/// An execution error with its class: what a Python read raises, and what a
/// C stream reports as `"<class>: <message>"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Classified {
    pub(crate) class: &'static str,
    pub(crate) message: String,
}

impl Classified {
    pub(crate) fn new(class: &'static str, message: impl Into<String>) -> Self {
        Classified {
            class,
            message: message.into(),
        }
    }

    /// Whether the class is `LTSeqIOError` or one of its subclasses.
    pub(crate) fn is_io(&self) -> bool {
        IO_CLASSES.contains(&self.class)
    }
}

impl fmt::Display for Classified {
    /// The C stream's message: the class name, then the message (§16.4).
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: {}", self.class, self.message)
    }
}

/// An injected fault travels through DataFusion as `External`.
impl std::error::Error for Classified {}

/// The class and message of an execution error.
pub(crate) fn classify(e: &DataFusionError) -> Classified {
    match e {
        DataFusionError::ArrowError(a, _) => classify_arrow(a),
        DataFusionError::External(inner) => classify_external(inner.as_ref())
            .unwrap_or_else(|| Classified::new("ExecutionError", inner.to_string())),
        DataFusionError::Context(_, inner) => classify(inner),
        DataFusionError::Shared(inner) => classify(inner),
        DataFusionError::IoError(io) => classify_io(io),
        DataFusionError::ObjectStore(store) => classify_store(store),
        other => Classified::new("ExecutionError", other.to_string()),
    }
}

fn classify_arrow(e: &ArrowError) -> Classified {
    match e {
        ArrowError::DivideByZero => Classified::new("DivisionByZeroError", "division by zero"),
        ArrowError::ArithmeticOverflow(m) => Classified::new("ArithmeticOverflowError", m.clone()),
        ArrowError::CastError(m) => Classified::new("CastError", m.clone()),
        ArrowError::IoError(_, io) => classify_io(io),
        ArrowError::ExternalError(inner) => classify_external(inner.as_ref())
            .unwrap_or_else(|| Classified::new("ExecutionError", inner.to_string())),
        other => Classified::new("ExecutionError", other.to_string()),
    }
}

/// An error boxed as `dyn Error`: an injected fault, or one of the engine's
/// own errors one level down.
fn classify_external(e: &(dyn std::error::Error + Send + Sync + 'static)) -> Option<Classified> {
    if let Some(c) = e.downcast_ref::<Classified>() {
        return Some(c.clone());
    }
    if let Some(df) = e.downcast_ref::<DataFusionError>() {
        return Some(classify(df));
    }
    if let Some(a) = e.downcast_ref::<ArrowError>() {
        return Some(classify_arrow(a));
    }
    if let Some(io) = e.downcast_ref::<io::Error>() {
        return Some(classify_io(io));
    }
    if let Some(store) = e.downcast_ref::<object_store::Error>() {
        return Some(classify_store(store));
    }
    None
}

fn classify_io(e: &io::Error) -> Classified {
    // The object store reports a missing file as an `io::Error` wrapping
    // its own error, so look one level down first.
    if let Some(store) = e
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<object_store::Error>())
    {
        return classify_store(store);
    }
    if e.kind() == io::ErrorKind::NotFound {
        Classified::new("SourceNotFoundError", e.to_string())
    } else {
        Classified::new("LTSeqIOError", e.to_string())
    }
}

/// A store error is `SourceNotFoundError` when a missing file is anywhere
/// in its chain: the session's `LocalFiles` reports a footer read of a
/// missing file as `Generic` around `NotFound` (`source::LocalFiles`, so
/// that the listing does not take the path for a directory).
fn classify_store(e: &object_store::Error) -> Classified {
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(e);
    while let Some(err) = cause {
        let missing = matches!(
            err.downcast_ref::<object_store::Error>(),
            Some(object_store::Error::NotFound { .. })
        ) || err
            .downcast_ref::<io::Error>()
            .is_some_and(|io| io.kind() == io::ErrorKind::NotFound);
        if missing {
            return Classified::new("SourceNotFoundError", e.to_string());
        }
        cause = err.source();
    }
    Classified::new("LTSeqIOError", e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn arrow_kernel_errors_keep_their_class_through_wrapping() {
        let divide = DataFusionError::ArrowError(Box::new(ArrowError::DivideByZero), None);
        let wrapped = DataFusionError::Shared(Arc::new(divide.context("in a repartition")));
        assert_eq!(classify(&wrapped).class, "DivisionByZeroError");

        let cast = DataFusionError::External(Box::new(ArrowError::CastError(
            "Cannot cast string 'x' to value of Int64 type".into(),
        )));
        let c = classify(&cast);
        assert_eq!(c.class, "CastError");
        assert_eq!(c.message, "Cannot cast string 'x' to value of Int64 type");

        let overflow = DataFusionError::ArrowError(
            Box::new(ArrowError::ArithmeticOverflow(
                "Overflow happened on: 1 + 2".into(),
            )),
            None,
        );
        assert_eq!(classify(&overflow).class, "ArithmeticOverflowError");
    }

    #[test]
    fn a_missing_file_is_source_not_found_and_other_io_is_ltseq_io_error() {
        let missing = DataFusionError::ObjectStore(Box::new(object_store::Error::NotFound {
            path: "/data/x.parquet".into(),
            source: "gone".into(),
        }));
        let c = classify(&missing);
        assert_eq!(c.class, "SourceNotFoundError");
        assert!(c.is_io());

        let in_io = DataFusionError::IoError(io::Error::other(object_store::Error::NotFound {
            path: "/data/x.parquet".into(),
            source: "gone".into(),
        }));
        assert_eq!(classify(&in_io).class, "SourceNotFoundError");

        let footer = DataFusionError::ObjectStore(Box::new(object_store::Error::Generic {
            store: "LocalFileSystem",
            source: Box::new(object_store::Error::NotFound {
                path: "/data/x.parquet".into(),
                source: Box::new(io::Error::from(io::ErrorKind::NotFound)),
            }),
        }));
        assert_eq!(classify(&footer).class, "SourceNotFoundError");

        let denied = DataFusionError::IoError(io::Error::from(io::ErrorKind::PermissionDenied));
        let c = classify(&denied);
        assert_eq!(c.class, "LTSeqIOError");
        assert!(c.is_io());
    }

    #[test]
    fn an_injected_fault_keeps_its_class_and_message() {
        let fault = Classified::new("OrderViolationError", "row 7 precedes row 6");
        let e = DataFusionError::Context(
            "outer".into(),
            Box::new(DataFusionError::External(Box::new(fault.clone()))),
        );
        assert_eq!(classify(&e), fault);
        assert_eq!(
            fault.to_string(),
            "OrderViolationError: row 7 precedes row 6"
        );
        assert!(!fault.is_io());
    }

    #[test]
    fn anything_else_is_an_execution_error() {
        let e = DataFusionError::Internal("boom".into());
        assert_eq!(classify(&e).class, "ExecutionError");
    }
}
