//! Formatting functions for displaying RecordBatches as ASCII tables

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::arrow::record_batch::RecordBatch;
use pyo3::PyResult;

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

    // Calculate column widths for pretty printing
    let mut col_widths: Vec<usize> = col_names.iter().map(|name| name.len()).collect();

    // Scan through batches to find max column widths
    let mut row_count = 0;
    for batch in batches {
        let num_rows = batch.num_rows();
        for row_idx in 0..num_rows {
            if row_count >= limit {
                break;
            }
            for (col_idx, _) in schema.fields().iter().enumerate() {
                let col = batch.column(col_idx);
                let value_str = format_cell(col, row_idx);
                col_widths[col_idx] = col_widths[col_idx].max(value_str.len());
            }
            row_count += 1;
        }
        if row_count >= limit {
            break;
        }
    }

    // Limit width to 50 chars per column for readability
    for width in &mut col_widths {
        *width = (*width).min(50);
    }

    // Draw top border
    output.push('+');
    for width in &col_widths {
        output.push_str(&"-".repeat(width + 2));
        output.push('+');
    }
    output.push('\n');

    // Draw header
    output.push('|');
    for (i, col_name) in col_names.iter().enumerate() {
        output.push(' ');
        output.push_str(&format!("{:<width$}", col_name, width = col_widths[i]));
        output.push_str(" |");
    }
    output.push('\n');

    // Draw header border
    output.push('+');
    for width in &col_widths {
        output.push_str(&"-".repeat(width + 2));
        output.push('+');
    }
    output.push('\n');

    // Draw rows
    row_count = 0;
    for batch in batches {
        let num_rows = batch.num_rows();
        for row_idx in 0..num_rows {
            if row_count >= limit {
                break;
            }
            output.push('|');
            for (col_idx, _) in schema.fields().iter().enumerate() {
                output.push(' ');
                let col = batch.column(col_idx);
                let value_str = format_cell(col, row_idx);
                // Truncate long values
                let truncated = if value_str.len() > 50 {
                    format!("{}...", &value_str[..47])
                } else {
                    value_str
                };
                output.push_str(&format!(
                    "{:<width$}",
                    truncated,
                    width = col_widths[col_idx]
                ));
                output.push_str(" |");
            }
            output.push('\n');
            row_count += 1;
        }
        if row_count >= limit {
            break;
        }
    }

    // Draw bottom border
    output.push('+');
    for width in &col_widths {
        output.push_str(&"-".repeat(width + 2));
        output.push('+');
    }
    output.push('\n');

    Ok(output)
}

/// Format a single cell value from an Arrow column
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
