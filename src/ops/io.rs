//! I/O operations for LTSeqTable
//!
//! This module contains helper functions for file I/O operations including:
//! - CSV writing
//! - Future: Parquet writing, JSON export, etc.

use crate::engine::RUNTIME;
use crate::LTSeqTable;
use pyo3::prelude::*;

/// Write table data to a CSV file
///
/// Args:
///     table: Reference to LTSeqTable
///     path: Path to the output CSV file
pub fn write_csv_impl(table: &LTSeqTable, path: String) -> PyResult<()> {
    let df = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No data loaded. Call read_csv() first.")
    })?;

    let schema = table.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Schema not available.")
    })?;

    RUNTIME.block_on(async {
        use std::fs::File;
        use std::io::Write;

        let df_clone = (**df).clone();
        let batches = df_clone.collect().await.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to collect data: {}",
                e
            ))
        })?;

        let mut file = File::create(&path).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create file '{}': {}",
                path, e
            ))
        })?;

        // Write header
        let fields: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        writeln!(file, "{}", fields.join(",")).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to write CSV header: {}",
                e
            ))
        })?;

        // Write data rows
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let mut row_values = Vec::new();
                for col_idx in 0..batch.num_columns() {
                    let column = batch.column(col_idx);
                    let value_str = if column.is_null(row_idx) {
                        "".to_string()
                    } else {
                        // Use format_cell which has comprehensive type handling
                        let formatted = crate::format::format_cell(&**column, row_idx);
                        // Replace "[unsupported type]" with empty string for CSV
                        // and remove "None" for null values
                        if formatted == "[unsupported type]" || formatted == "None" {
                            "".to_string()
                        } else {
                            formatted
                        }
                    };
                    row_values.push(value_str);
                }
                writeln!(file, "{}", row_values.join(",")).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to write CSV row: {}",
                        e
                    ))
                })?;
            }
        }

        Ok(())
    })
}
