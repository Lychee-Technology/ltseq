//! Formatting functions for displaying RecordBatches as ASCII tables

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::temporal_conversions;
use pyo3::PyResult;

/// A typed column reference that avoids repeated `downcast_ref` per cell.
///
/// By matching `DataType` once per column, we get a typed reference that
/// formats individual rows without any runtime type dispatch overhead.
enum TypedColumn<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
    Date32(&'a Date32Array),
    Date64(&'a Date64Array),
    TimestampSec(&'a TimestampSecondArray),
    TimestampMs(&'a TimestampMillisecondArray),
    TimestampUs(&'a TimestampMicrosecondArray),
    TimestampNs(&'a TimestampNanosecondArray),
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    UInt8(&'a UInt8Array),
    UInt16(&'a UInt16Array),
    UInt32(&'a UInt32Array),
    UInt64(&'a UInt64Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Boolean(&'a BooleanArray),
    /// Fallback for unsupported types — stores the raw dyn Array
    Unsupported,
}

impl<'a> TypedColumn<'a> {
    /// Create a TypedColumn by matching on the column's DataType once.
    fn new(column: &'a dyn Array) -> Self {
        match column.data_type() {
            DataType::Utf8 => {
                TypedColumn::Utf8(column.as_any().downcast_ref().expect("type matched Utf8"))
            }
            DataType::LargeUtf8 => TypedColumn::LargeUtf8(
                column
                    .as_any()
                    .downcast_ref()
                    .expect("type matched LargeUtf8"),
            ),
            DataType::Utf8View => TypedColumn::Utf8View(
                column
                    .as_any()
                    .downcast_ref()
                    .expect("type matched Utf8View"),
            ),
            DataType::Date32 => {
                TypedColumn::Date32(column.as_any().downcast_ref().expect("type matched Date32"))
            }
            DataType::Date64 => {
                TypedColumn::Date64(column.as_any().downcast_ref().expect("type matched Date64"))
            }
            DataType::Timestamp(TimeUnit::Second, _) => TypedColumn::TimestampSec(
                column
                    .as_any()
                    .downcast_ref()
                    .expect("type matched TimestampSec"),
            ),
            DataType::Timestamp(TimeUnit::Millisecond, _) => TypedColumn::TimestampMs(
                column
                    .as_any()
                    .downcast_ref()
                    .expect("type matched TimestampMs"),
            ),
            DataType::Timestamp(TimeUnit::Microsecond, _) => TypedColumn::TimestampUs(
                column
                    .as_any()
                    .downcast_ref()
                    .expect("type matched TimestampUs"),
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => TypedColumn::TimestampNs(
                column
                    .as_any()
                    .downcast_ref()
                    .expect("type matched TimestampNs"),
            ),
            DataType::Int8 => {
                TypedColumn::Int8(column.as_any().downcast_ref().expect("type matched Int8"))
            }
            DataType::Int16 => {
                TypedColumn::Int16(column.as_any().downcast_ref().expect("type matched Int16"))
            }
            DataType::Int32 => {
                TypedColumn::Int32(column.as_any().downcast_ref().expect("type matched Int32"))
            }
            DataType::Int64 => {
                TypedColumn::Int64(column.as_any().downcast_ref().expect("type matched Int64"))
            }
            DataType::UInt8 => {
                TypedColumn::UInt8(column.as_any().downcast_ref().expect("type matched UInt8"))
            }
            DataType::UInt16 => {
                TypedColumn::UInt16(column.as_any().downcast_ref().expect("type matched UInt16"))
            }
            DataType::UInt32 => {
                TypedColumn::UInt32(column.as_any().downcast_ref().expect("type matched UInt32"))
            }
            DataType::UInt64 => {
                TypedColumn::UInt64(column.as_any().downcast_ref().expect("type matched UInt64"))
            }
            DataType::Float32 => TypedColumn::Float32(
                column
                    .as_any()
                    .downcast_ref()
                    .expect("type matched Float32"),
            ),
            DataType::Float64 => TypedColumn::Float64(
                column
                    .as_any()
                    .downcast_ref()
                    .expect("type matched Float64"),
            ),
            DataType::Boolean => TypedColumn::Boolean(
                column
                    .as_any()
                    .downcast_ref()
                    .expect("type matched Boolean"),
            ),
            _ => TypedColumn::Unsupported,
        }
    }

    /// Format a single cell value. The type dispatch is already resolved.
    fn format(&self, row_idx: usize, column: &dyn Array) -> String {
        if !column.is_valid(row_idx) {
            return "None".to_string();
        }
        match self {
            TypedColumn::Utf8(arr) => arr.value(row_idx).to_string(),
            TypedColumn::LargeUtf8(arr) => arr.value(row_idx).to_string(),
            TypedColumn::Utf8View(arr) => arr.value(row_idx).to_string(),
            TypedColumn::Date32(arr) => {
                let date = arr.value(row_idx);
                match temporal_conversions::date32_to_datetime(date) {
                    Some(dt) => dt.format("%Y-%m-%d").to_string(),
                    None => "".to_string(),
                }
            }
            TypedColumn::Date64(arr) => {
                let ms = arr.value(row_idx);
                match temporal_conversions::date64_to_datetime(ms) {
                    Some(dt) => dt.format("%Y-%m-%d").to_string(),
                    None => "".to_string(),
                }
            }
            TypedColumn::TimestampSec(arr) => {
                let secs = arr.value(row_idx);
                match temporal_conversions::timestamp_s_to_datetime(secs) {
                    Some(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                    None => "".to_string(),
                }
            }
            TypedColumn::TimestampMs(arr) => {
                let ms = arr.value(row_idx);
                match temporal_conversions::timestamp_ms_to_datetime(ms) {
                    Some(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                    None => "".to_string(),
                }
            }
            TypedColumn::TimestampUs(arr) => {
                let us = arr.value(row_idx);
                match temporal_conversions::timestamp_us_to_datetime(us) {
                    Some(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                    None => "".to_string(),
                }
            }
            TypedColumn::TimestampNs(arr) => {
                let ns = arr.value(row_idx);
                match temporal_conversions::timestamp_ns_to_datetime(ns) {
                    Some(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                    None => "".to_string(),
                }
            }
            TypedColumn::Int8(arr) => arr.value(row_idx).to_string(),
            TypedColumn::Int16(arr) => arr.value(row_idx).to_string(),
            TypedColumn::Int32(arr) => arr.value(row_idx).to_string(),
            TypedColumn::Int64(arr) => arr.value(row_idx).to_string(),
            TypedColumn::UInt8(arr) => arr.value(row_idx).to_string(),
            TypedColumn::UInt16(arr) => arr.value(row_idx).to_string(),
            TypedColumn::UInt32(arr) => arr.value(row_idx).to_string(),
            TypedColumn::UInt64(arr) => arr.value(row_idx).to_string(),
            TypedColumn::Float32(arr) => format!("{}", arr.value(row_idx)),
            TypedColumn::Float64(arr) => format!("{}", arr.value(row_idx)),
            TypedColumn::Boolean(arr) => arr.value(row_idx).to_string(),
            TypedColumn::Unsupported => "[unsupported type]".to_string(),
        }
    }
}

/// Calculate optimal column widths by scanning through all rows (up to limit)
fn calculate_column_widths(
    batches: &[RecordBatch],
    schema: &ArrowSchema,
    limit: usize,
) -> Vec<usize> {
    let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let mut col_widths: Vec<usize> = col_names.iter().map(|name| name.len()).collect();

    let mut row_count = 0;
    for batch in batches {
        // Resolve typed columns once per batch (type dispatch happens here)
        let typed_cols: Vec<TypedColumn> = (0..batch.num_columns())
            .map(|i| TypedColumn::new(batch.column(i)))
            .collect();

        let num_rows = batch.num_rows();
        for row_idx in 0..num_rows {
            if row_count >= limit {
                break;
            }
            for (col_idx, typed_col) in typed_cols.iter().enumerate() {
                let col = batch.column(col_idx);
                let value_str = typed_col.format(row_idx, col);
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

    col_widths
}

/// Draw a border line with given column widths
fn draw_border(col_widths: &[usize]) -> String {
    let mut border = String::new();
    border.push('+');
    for width in col_widths {
        border.push_str(&"-".repeat(width + 2));
        border.push('+');
    }
    border.push('\n');
    border
}

/// Draw the header row with column names
fn draw_header(schema: &ArrowSchema, col_widths: &[usize]) -> String {
    let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let mut header = String::new();
    header.push('|');
    for (i, col_name) in col_names.iter().enumerate() {
        header.push(' ');
        header.push_str(&format!("{:<width$}", col_name, width = col_widths[i]));
        header.push_str(" |");
    }
    header.push('\n');
    header
}

/// Draw data rows from batches (up to limit)
fn draw_rows(batches: &[RecordBatch], col_widths: &[usize], limit: usize) -> String {
    let mut rows = String::new();
    let mut row_count = 0;

    for batch in batches {
        // Resolve typed columns once per batch
        let typed_cols: Vec<TypedColumn> = (0..batch.num_columns())
            .map(|i| TypedColumn::new(batch.column(i)))
            .collect();

        let num_rows = batch.num_rows();
        for row_idx in 0..num_rows {
            if row_count >= limit {
                break;
            }
            rows.push('|');
            for (col_idx, typed_col) in typed_cols.iter().enumerate() {
                rows.push(' ');
                let col = batch.column(col_idx);
                let value_str = typed_col.format(row_idx, col);
                // Truncate long values
                let truncated = if value_str.len() > 50 {
                    format!("{}...", &value_str[..47])
                } else {
                    value_str
                };
                rows.push_str(&format!(
                    "{:<width$}",
                    truncated,
                    width = col_widths[col_idx]
                ));
                rows.push_str(" |");
            }
            rows.push('\n');
            row_count += 1;
        }
        if row_count >= limit {
            break;
        }
    }

    rows
}

/// Format RecordBatches as a pretty-printed ASCII table
///
/// Shows up to `limit` rows, streaming through batches
pub fn format_table(
    batches: &[RecordBatch],
    schema: &ArrowSchema,
    limit: usize,
) -> PyResult<String> {
    // Calculate column widths
    let col_widths = calculate_column_widths(batches, schema, limit);

    let mut output = String::new();

    // Draw top border
    output.push_str(&draw_border(&col_widths));

    // Draw header
    output.push_str(&draw_header(schema, &col_widths));

    // Draw header border
    output.push_str(&draw_border(&col_widths));

    // Draw rows
    output.push_str(&draw_rows(batches, &col_widths, limit));

    // Draw bottom border
    output.push_str(&draw_border(&col_widths));

    Ok(output)
}
