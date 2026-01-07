use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};

/// Deserialize a Python expression dict to verify and validate it
/// Returns the expression dict back for now (can be extended later for Rust execution)
fn validate_expr(expr_dict: &Bound<'_, PyAny>) -> PyResult<()> {
    // Try to treat as PyAny and cast to PyDict
    if let Ok(dict) = expr_dict.downcast::<PyDict>() {
        validate_expr_from_dict(&dict)
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Expression must be a dict",
        ))
    }
}

fn validate_expr_from_dict(dict: &Bound<'_, PyDict>) -> PyResult<()> {
    let expr_type = dict
        .get_item("type")?
        .ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Missing 'type' in expression")
        })?
        .extract::<String>()?;

    match expr_type.as_str() {
        "Column" => {
            let _ = dict
                .get_item("name")?
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("Missing 'name' in Column")
                })?
                .extract::<String>()?;
            Ok(())
        }

        "Literal" => {
            let _ = dict.get_item("value")?.ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("Missing 'value' in Literal")
            })?;
            Ok(())
        }

        "BinOp" => {
            let _ = dict
                .get_item("op")?
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("Missing 'op' in BinOp")
                })?
                .extract::<String>()?;
            let left_dict = dict.get_item("left")?.ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("Missing 'left' in BinOp")
            })?;
            let right_dict = dict.get_item("right")?.ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("Missing 'right' in BinOp")
            })?;

            validate_expr(&left_dict)?;
            validate_expr(&right_dict)?;
            Ok(())
        }

        "UnaryOp" => {
            let _ = dict
                .get_item("op")?
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("Missing 'op' in UnaryOp")
                })?
                .extract::<String>()?;
            let operand_dict = dict.get_item("operand")?.ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("Missing 'operand' in UnaryOp")
            })?;

            validate_expr(&operand_dict)?;
            Ok(())
        }

        _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Unknown expression type: {}",
            expr_type
        ))),
    }
}

/// LTSeq Rust kernel - minimal implementation for Phase 2
#[pyclass]
pub struct RustTable {
    data: std::sync::Mutex<Vec<Vec<String>>>,
}

#[pymethods]
impl RustTable {
    #[new]
    fn new() -> Self {
        RustTable {
            data: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Validate a filter expression
    fn validate_filter(&self, expr: Py<PyAny>) -> PyResult<String> {
        // Don't call Python::with_gil - just validate directly
        // expr is a Py<PyAny> which is already properly bound
        Ok("Filter validation deferred to Phase 2".to_string())
    }

    /// Validate a select expression  
    fn validate_select(&self, _exprs: Py<PyAny>) -> PyResult<String> {
        Ok("Select validation not yet implemented".to_string())
    }

    /// Validate a derive expression
    fn validate_derive(&self, _cols: Py<PyAny>) -> PyResult<String> {
        Ok("Derive validation not yet implemented".to_string())
    }

    fn hello(&self) -> String {
        "Hello from LTSeq Kernel!".to_string()
    }
}

#[pyfunction]
fn hello() -> PyResult<String> {
    Ok("hello from ltseq_core".to_string())
}

#[pymodule]
fn ltseq_core(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hello, m)?)?;
    m.add_class::<RustTable>()?;
    Ok(())
}
