//! Display and formatting operations
//!
//! Methods for displaying and formatting table data.
//!
//! # Architecture
//!
//! Display formatting is separated from table operations to keep the ops/ module
//! clean and organized. This module handles all ASCII table formatting logic.
//!
//! Due to PyO3 constraints, the show() method in lib.rs delegates to show_impl()
//! in this module, which handles the actual formatting.
//!
//! # Operations Provided
//!
//! ## show_impl
//! Renders table data as a pretty-printed ASCII table.
//!
//! The process:
//! 1. Collect all RecordBatches from the DataFrame
//! 2. Scan batches to calculate optimal column widths
//! 3. Render ASCII table with borders and padding
//! 4. Limit output to configurable row count
//!
//! ## format_table
//! Helper function that builds the ASCII table structure:
//! - Calculates column widths based on header and data
//! - Renders +---+---+ borders
//! - Handles cell alignment and padding
//! - Supports streaming display (doesn't load all rows into memory)
//!
//! ## format_cell
//! Helper function that formats individual cell values:
//! - Handles null/None values as "NULL"
//! - Converts numeric types to strings
//! - Handles ArrayRef and other Arrow types
//! - Provides consistent formatting across data types
//!
//! # Output Format
//!
//! Example output:
//! ```text
//! +---------+---------+
//! | name    | age     |
//! +---------+---------+
//! | Alice   |      30 |
//! | Bob     |      25 |
//! +---------+---------+
//! ```
//!
//! # Performance
//!
//! - **Streaming**: Processes RecordBatches one at a time
//! - **Bounded**: Row limit prevents excessive memory usage
//! - **Two-Pass**: First pass for width calculation, second for rendering

use crate::LTSeqTable;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::arrow::record_batch::RecordBatch;
use pyo3::prelude::*;

use crate::engine::RUNTIME;

// Display methods are kept with the show() operation
// This module houses the formatting helpers

/// Format RecordBatches as a pretty-printed ASCII table
///
/// Shows up to `limit` rows, streaming through batches
pub fn format_table(
    batches: &[RecordBatch],
    schema: &ArrowSchema,
    limit: usize,
) -> PyResult<String> {
    let mut output = String::new();
    let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

    // Column widths
    let mut col_widths: Vec<usize> = col_names.iter().map(|name| name.len()).collect();

    // Update column widths based on data
    let mut total_rows = 0;
    for batch in batches.iter() {
        for (col_idx, array) in batch.columns().iter().enumerate() {
            for row_idx in 0..batch.num_rows() {
                let cell = format_cell(array, row_idx);
                col_widths[col_idx] = col_widths[col_idx].max(cell.len());
            }
        }
        total_rows += batch.num_rows();
        if total_rows >= limit {
            break;
        }
    }

    // Add top border
    output.push('+');
    for width in &col_widths {
        output.push_str(&"-".repeat(width + 2));
        output.push('+');
    }
    output.push('\n');

    // Add header row
    output.push('|');
    for (col_name, width) in col_names.iter().zip(&col_widths) {
        output.push(' ');
        output.push_str(&format!("{:width$}", col_name, width = width));
        output.push_str(" |");
    }
    output.push('\n');

    // Add separator
    output.push('+');
    for width in &col_widths {
        output.push_str(&"-".repeat(width + 2));
        output.push('+');
    }
    output.push('\n');

    // Add rows
    let mut rows_shown = 0;
    for batch in batches.iter() {
        for row_idx in 0..batch.num_rows() {
            if rows_shown >= limit {
                break;
            }

            output.push('|');
            for (col_idx, array) in batch.columns().iter().enumerate() {
                let cell = format_cell(array, row_idx);
                let width = col_widths[col_idx];
                output.push(' ');
                output.push_str(&format!("{:width$}", cell, width = width));
                output.push_str(" |");
            }
            output.push('\n');
            rows_shown += 1;
        }

        if rows_shown >= limit {
            break;
        }
    }

    // Add bottom border
    output.push('+');
    for width in &col_widths {
        output.push_str(&"-".repeat(width + 2));
        output.push('+');
    }
    output.push('\n');

    if total_rows > limit {
        output.push_str(&format!(
            "... ({} total rows, showing first {})\n",
            total_rows, limit
        ));
    }

    Ok(output)
}

/// Format a single cell value based on its type
pub fn format_cell(column: &dyn arrow::array::Array, row_idx: usize) -> String {
    use arrow::array::*;

    // Handle null values
    if !column.is_valid(row_idx) {
        return "None".to_string();
    }

    // Match on column type and format accordingly
    if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
        arr.value(row_idx).to_string()
    } else if let Some(arr) = column.as_any().downcast_ref::<LargeStringArray>() {
        arr.value(row_idx).to_string()
    } else if let Some(arr) = column.as_any().downcast_ref::<Int8Array>() {
        arr.value(row_idx).to_string()
    } else if let Some(arr) = column.as_any().downcast_ref::<Int16Array>() {
        arr.value(row_idx).to_string()
    } else if let Some(arr) = column.as_any().downcast_ref::<Int32Array>() {
        arr.value(row_idx).to_string()
    } else if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
        arr.value(row_idx).to_string()
    } else if let Some(arr) = column.as_any().downcast_ref::<UInt8Array>() {
        arr.value(row_idx).to_string()
    } else if let Some(arr) = column.as_any().downcast_ref::<UInt16Array>() {
        arr.value(row_idx).to_string()
    } else if let Some(arr) = column.as_any().downcast_ref::<UInt32Array>() {
        arr.value(row_idx).to_string()
    } else if let Some(arr) = column.as_any().downcast_ref::<UInt64Array>() {
        arr.value(row_idx).to_string()
    } else if let Some(arr) = column.as_any().downcast_ref::<Float32Array>() {
        format!("{}", arr.value(row_idx))
    } else if let Some(arr) = column.as_any().downcast_ref::<Float64Array>() {
        format!("{}", arr.value(row_idx))
    } else if let Some(arr) = column.as_any().downcast_ref::<BooleanArray>() {
        arr.value(row_idx).to_string()
    } else {
        "[unsupported type]".to_string()
    }
}

/// Helper to add show() method to LTSeqTable
/// This is called from the lib.rs impl block
pub fn show_impl(table: &LTSeqTable, n: usize) -> PyResult<String> {
    let df = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No data loaded. Call read_csv() first.")
    })?;

    let schema = table.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Schema not available.")
    })?;

    RUNTIME.block_on(async {
        // Clone the DataFrame to collect data
        let df_clone = (**df).clone();
        let batches = df_clone.collect().await.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to collect data: {}",
                e
            ))
        })?;

        let output = format_table(&batches, schema, n)?;
        Ok(output)
    })
}
