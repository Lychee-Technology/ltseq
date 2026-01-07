use pyo3::prelude::*;
use pyo3::types::PyModuleMethods;

/// Minimal LTSeq kernel placeholder
#[pyclass]
struct RustTable {
    // Defer DataFusion imports to avoid GIL issues
    _placeholder: bool,
}

#[pymethods]
impl RustTable {
    #[new]
    fn new() -> Self {
        RustTable { _placeholder: true }
    }

    fn read_csv(&mut self, _path: &str) -> PyResult<()> {
        Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
            "RustTable.read_csv() not yet implemented - use Python schema inference instead",
        ))
    }

    fn show(&self, _n: Option<usize>) -> PyResult<String> {
        Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
            "RustTable.show() not yet implemented",
        ))
    }

    fn hello(&self) -> String {
        "Hello from LTSeq Kernel (stub)!".to_string()
    }

    fn get_schema(&self) -> PyResult<std::collections::HashMap<String, String>> {
        Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
            "RustTable.get_schema() not yet implemented",
        ))
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
