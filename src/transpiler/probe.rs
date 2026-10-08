//! Test probes into the exactness facts, for the Python oracle tests.
//!
//! These expose [`exact::cast_class`] and [`exact::holds`] to Python so an
//! oracle written with exact rational arithmetic in Python can check them
//! over types and values that Python can build; they are not public API.

use std::cmp::Ordering;

use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::pyarrow::{IntoPyArrow, PyArrowType};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::exact::{cast_class, holds, Holding, Placement};
use crate::types::{dict_to_py_expr, PyExpr};

/// What a cast from one Arrow type to another can lose, named as the
/// `CastClass` variant.
#[pyfunction]
pub(crate) fn _cast_class(from: PyArrowType<DataType>, to: PyArrowType<DataType>) -> String {
    format!("{:?}", cast_class(&from.0, &to.0))
}

/// Whether an Arrow type holds a literal, given as the Python literal
/// protocol's dict: the holding's name (`Exactly`, `Between`, `Beyond`,
/// `NotANumber`, `Unjudged`), the held value or floor as a one-element
/// pyarrow array, and the side (`Greater` or `Less`) of a `Beyond`.
#[pyfunction]
pub(crate) fn _holds<'py>(
    py: Python<'py>,
    to: PyArrowType<DataType>,
    literal: &Bound<'py, PyDict>,
) -> PyResult<(String, Option<Bound<'py, PyAny>>, Option<String>)> {
    let value = match dict_to_py_expr(literal)? {
        PyExpr::Literal(value) => value.to_scalar_value(),
        _ => return Err(PyValueError::new_err("expected a literal")),
    };
    let array = |value: datafusion::scalar::ScalarValue| -> PyResult<Bound<'py, PyAny>> {
        value
            .to_array()
            .map_err(|e| PyValueError::new_err(e.to_string()))?
            .to_data()
            .into_pyarrow(py)
    };
    Ok(match holds(&to.0, &value) {
        Holding::Exactly(held) => ("Exactly".into(), Some(array(held)?), None),
        Holding::Not(Placement::Between(floor)) => ("Between".into(), Some(array(floor)?), None),
        Holding::Not(Placement::Beyond(side)) => {
            let side = match side {
                Ordering::Greater => "Greater",
                Ordering::Less => "Less",
                Ordering::Equal => "Equal",
            };
            ("Beyond".into(), None, Some(side.into()))
        }
        Holding::Not(Placement::NotANumber) => ("NotANumber".into(), None, None),
        Holding::Unjudged => ("Unjudged".into(), None, None),
    })
}
