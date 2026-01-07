use pyo3::prelude::*;

/// LTSeq Rust kernel - minimal implementation for Phase 2
#[pyclass]
pub struct RustTable {}

#[pymethods]
impl RustTable {
    #[new]
    fn new() -> Self {
        RustTable {}
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
fn ltseq_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Try adding items one by one to see where the error occurs
    m.add_function(wrap_pyfunction!(hello, m)?)?;
    m.add_class::<RustTable>()?;
    Ok(())
}
