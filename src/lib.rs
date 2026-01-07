use datafusion::arrow::util::pretty;
use datafusion::prelude::*;
use futures::executor::block_on;
use pyo3::prelude::*;
use pyo3::types::PyModuleMethods;

/// Minimal LTSeq kernel: holds a DataFusion SessionContext and optional DataFrame
#[pyclass]
struct RustTable {
    ctx: SessionContext,
    df: Option<datafusion::dataframe::DataFrame>,
}

#[pymethods]
impl RustTable {
    #[new]
    fn new() -> Self {
        let ctx = SessionContext::new();
        RustTable { ctx, df: None }
    }

    /// Read CSV into an in-memory DataFrame using DataFusion's CSV reader
    fn read_csv(&mut self, path: &str) -> PyResult<()> {
        let options = CsvReadOptions::new().schema_infer_max_records(100usize);
        let df = block_on(self.ctx.read_csv(path, options)).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("CSV read error: {}", e))
        })?;
        self.df = Some(df);
        Ok(())
    }

    /// Collect and pretty-print record batches
    fn show(&self, _n: Option<usize>) -> PyResult<String> {
        if let Some(df) = &self.df {
            let df_clone = df.clone();
            let batches = block_on(df_clone.collect()).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Execution error: {}", e))
            })?;
            pretty::print_batches(&batches).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Print error: {}", e))
            })?;
            Ok(format!("Printed {} record batches", batches.len()))
        } else {
            Ok("<no data>".to_string())
        }
    }

    fn hello(&self) -> String {
        format!("Hello from LTSeq Kernel! df_exists={}", self.df.is_some())
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
