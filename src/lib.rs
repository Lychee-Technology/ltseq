use pyo3::prelude::*;

/// Rust 侧的占位符结构
#[pyclass]
struct RustTable {
    row_count: usize,
}

#[pymethods]
impl RustTable {
    #[new]
    fn new() -> Self {
        RustTable { row_count: 0 }
    }

    fn hello(&self) -> String {
        format!("Hello from LTSeq Kernel! row_count{}", self.row_count).to_string()
    }
}

#[pymodule]
fn ltseq_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RustTable>()?;
    Ok(())
}