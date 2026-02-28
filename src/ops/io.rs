//! I/O operations for LTSeqTable
//!
//! This module contains helper functions for file I/O operations including:
//! - CSV writing (via Arrow's CSV writer)
//! - Future: Parquet writing, JSON export, etc.

use crate::engine::RUNTIME;
use crate::LTSeqTable;
use pyo3::prelude::*;

/// Write table data to a CSV file using Arrow's native CSV writer.
///
/// Args:
///     table: Reference to LTSeqTable
///     path: Path to the output CSV file
pub fn write_csv_impl(table: &LTSeqTable, path: String) -> PyResult<()> {
    let df = table.require_df()?;

    RUNTIME.block_on(async {
        use datafusion::arrow::csv::WriterBuilder;
        use std::fs::File;

        let df_clone = (**df).clone();
        let batches = df_clone.collect().await.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to collect data: {}",
                e
            ))
        })?;

        let file = File::create(&path).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create file '{}': {}",
                path, e
            ))
        })?;

        let builder = WriterBuilder::new().with_header(true);
        let mut writer = builder.build(file);

        for batch in &batches {
            writer.write(batch).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to write CSV batch: {}",
                    e
                ))
            })?;
        }

        Ok(())
    })
}
