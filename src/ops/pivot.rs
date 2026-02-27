//! Pivot operation for LTSeqTable
//!
//! Transforms table from long to wide format using SQL CASE WHEN aggregation.

use crate::engine::RUNTIME;
use crate::LTSeqTable;
use datafusion::arrow::array;
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::datasource::MemTable;
use pyo3::prelude::*;
use std::sync::Arc;

/// Pivot table from long to wide format using SQL CASE WHEN aggregation
///
/// Transforms data where unique values in a column become new columns,
/// and row groups are aggregated based on specified columns.
///
/// Example:
///   Input: year, region, amount
///   pivot(index="year", columns="region", values="amount", agg_fn="sum")
///   Output: year, West, East, Central
pub fn pivot_impl(
    table: &LTSeqTable,
    index_cols: Vec<String>,
    pivot_col: String,
    value_col: String,
    agg_fn: String,
) -> PyResult<LTSeqTable> {
    // Validate inputs
    if table.dataframe.is_none() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Cannot pivot empty table",
        ));
    }

    let df = table.dataframe.as_ref().unwrap();
    let schema = table.schema.as_ref().unwrap();

    // Validate that all required columns exist
    let col_names: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();

    for col in &index_cols {
        if !col_names.contains(col) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Index column '{}' not found in table",
                col
            )));
        }
    }

    if !col_names.contains(&pivot_col) {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Pivot column '{}' not found in table",
            pivot_col
        )));
    }

    if !col_names.contains(&value_col) {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Value column '{}' not found in table",
            value_col
        )));
    }

    // Validate aggregation function
    let agg_fn_upper = agg_fn.to_uppercase();
    match agg_fn_upper.as_str() {
        "SUM" | "MEAN" | "COUNT" | "MIN" | "MAX" => {}
        _ => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Invalid aggregation function '{}'. Must be one of: sum, mean, count, min, max",
                agg_fn
            )));
        }
    }

    // Find the index of the pivot column
    let pivot_col_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == &pivot_col)
        .ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Pivot column '{}' not found",
                pivot_col
            ))
        })?;

    // Collect all batches from the dataframe
    let all_batches = RUNTIME.block_on((**df).clone().collect()).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to collect data from table: {}",
            e
        ))
    })?;

    // Extract distinct pivot values
    let mut pivot_values_set = std::collections::HashSet::new();
    for batch in &all_batches {
        let col = batch.column(pivot_col_idx);
        extract_pivot_values(col, &mut pivot_values_set)?;
    }

    // Sort pivot values for consistent output
    let mut pivot_values: Vec<String> = pivot_values_set.into_iter().collect();
    pivot_values.sort();

    // Register the source data as a temporary table
    let source_table_name = "__pivot_source";
    let source_mem_table = MemTable::try_new(schema.clone(), vec![all_batches]).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to create source table: {}",
            e
        ))
    })?;

    table
        .session
        .register_table(source_table_name, Arc::new(source_mem_table))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to register source table: {}",
                e
            ))
        })?;

    // Build CASE WHEN expressions
    let mut case_exprs = Vec::new();
    for val in &pivot_values {
        let value_expr = if val.parse::<f64>().is_ok() {
            format!(
                "{}(CASE WHEN {} = {} THEN {} ELSE NULL END) as \"{}\"",
                agg_fn_upper, pivot_col, val, value_col, val
            )
        } else {
            format!(
                "{}(CASE WHEN {} = '{}' THEN {} ELSE NULL END) as \"{}\"",
                agg_fn_upper,
                pivot_col,
                val.replace("'", "''"),
                value_col,
                val
            )
        };
        case_exprs.push(value_expr);
    }

    // Build full pivot query
    let index_cols_str = index_cols.join(", ");
    let case_str = case_exprs.join(", ");
    let pivot_sql = format!(
        "SELECT {}, {} FROM {} GROUP BY {}",
        index_cols_str, case_str, source_table_name, index_cols_str
    );

    // Execute pivot query
    let result_df = RUNTIME
        .block_on(table.session.sql(&pivot_sql))
        .map_err(|e| {
            let _ = table.session.deregister_table(source_table_name);
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to execute pivot query: {}",
                e
            ))
        })?;

    // Collect result
    let result_batches = RUNTIME.block_on(result_df.collect()).map_err(|e| {
        let _ = table.session.deregister_table(source_table_name);
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to collect pivot results: {}",
            e
        ))
    })?;

    // Deregister source table
    let _ = table.session.deregister_table(source_table_name);

    // Create result schema from the result batches' schema
    let result_schema = if !result_batches.is_empty() {
        result_batches[0].schema()
    } else {
        Arc::new(ArrowSchema::empty())
    };

    // Create result table via helper
    LTSeqTable::from_batches_with_schema(
        Arc::clone(&table.session),
        result_batches,
        result_schema,
        Vec::new(),
        None,
    )
}

/// Extract pivot values from a column into a HashSet
fn extract_pivot_values(
    col: &std::sync::Arc<dyn datafusion::arrow::array::Array>,
    pivot_values_set: &mut std::collections::HashSet<String>,
) -> PyResult<()> {
    match col.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => {
            if let Some(string_col) = col.as_any().downcast_ref::<array::StringArray>() {
                for i in 0..string_col.len() {
                    if !string_col.is_null(i) {
                        pivot_values_set.insert(string_col.value(i).to_string());
                    }
                }
            }
        }
        DataType::Int32 => {
            if let Some(int_col) = col.as_any().downcast_ref::<array::Int32Array>() {
                for i in 0..int_col.len() {
                    if !int_col.is_null(i) {
                        pivot_values_set.insert(int_col.value(i).to_string());
                    }
                }
            }
        }
        DataType::Int64 => {
            if let Some(int_col) = col.as_any().downcast_ref::<array::Int64Array>() {
                for i in 0..int_col.len() {
                    if !int_col.is_null(i) {
                        pivot_values_set.insert(int_col.value(i).to_string());
                    }
                }
            }
        }
        DataType::Float32 => {
            if let Some(float_col) = col.as_any().downcast_ref::<array::Float32Array>() {
                for i in 0..float_col.len() {
                    if !float_col.is_null(i) {
                        pivot_values_set.insert(float_col.value(i).to_string());
                    }
                }
            }
        }
        DataType::Float64 => {
            if let Some(float_col) = col.as_any().downcast_ref::<array::Float64Array>() {
                for i in 0..float_col.len() {
                    if !float_col.is_null(i) {
                        pivot_values_set.insert(float_col.value(i).to_string());
                    }
                }
            }
        }
        _ => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Pivot column must be string, int, or float type",
            ));
        }
    }
    Ok(())
}
