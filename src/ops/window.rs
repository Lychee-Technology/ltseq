//! Window function operations for derived columns
//!
//! This module handles SQL-based window operations including:
//! - Shift operations (LAG/LEAD)
//! - Rolling aggregations (SUM, AVG, MIN, MAX, COUNT over windows)
//! - Cumulative sums with window frames
//! - Difference calculations

use crate::LTSeqTable;
use crate::types::{dict_to_py_expr, PyExpr};
use crate::transpiler::pyexpr_to_sql;
use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema, DataType};
use datafusion::datasource::MemTable;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;
use crate::engine::RUNTIME;

/// Helper function to apply window function SQL transformations
///
/// Converts special window function markers (e.g., __ROLLING_SUM__(col)__size)
/// into proper SQL window expressions with OVER clauses.
fn apply_window_frame(expr_sql: &str, order_by: &str, is_rolling: bool, is_diff: bool) -> String {
    if is_rolling {
        // Handle rolling aggregations: replace marker with actual window frame
        // Pattern: __ROLLING_FUNC__(col)__size
        let window_size_start = expr_sql.rfind("__").unwrap_or(0) + 2;
        let window_size_str = expr_sql[window_size_start..].trim_end_matches(|c: char| !c.is_ascii_digit());
        let window_size = window_size_str.parse::<i32>().unwrap_or(1);

        let start = expr_sql.find("__ROLLING_").unwrap_or(0) + 10;
        let end = expr_sql.find("__(").unwrap_or(start);
        let func_part = &expr_sql[start..end];

        let col_start = expr_sql.find("__(").unwrap_or(0) + 3;
        let col_end = expr_sql.rfind(")__").unwrap_or(expr_sql.len());
        let col_part = &expr_sql[col_start..col_end];

        if !order_by.is_empty() {
            format!(
                "{}({}) OVER (ORDER BY {} ROWS BETWEEN {} PRECEDING AND CURRENT ROW)",
                func_part, col_part, order_by, window_size - 1
            )
        } else {
            format!(
                "{}({}) OVER (ROWS BETWEEN {} PRECEDING AND CURRENT ROW)",
                func_part, col_part, window_size - 1
            )
        }
    } else if is_diff {
        // Handle diff: replace marker with actual calculation
        // Pattern: __DIFF_periods__(col)__periods
        let diff_start = expr_sql.find("__DIFF_").unwrap_or(0);
        let periods_start = diff_start + 7;
        let periods_end = expr_sql[periods_start..].find("__").unwrap_or(0) + periods_start;
        let periods_str = &expr_sql[periods_start..periods_end];

        let col_start = expr_sql.find("__(").unwrap_or(0) + 3;
        let col_end = expr_sql.rfind(")__").unwrap_or(expr_sql.len());
        let col_part = &expr_sql[col_start..col_end];

        if !order_by.is_empty() {
            format!(
                "({} - LAG({}, {}) OVER (ORDER BY {}))",
                col_part, col_part, periods_str, order_by
            )
        } else {
            format!(
                "({} - LAG({}, {}) OVER ())",
                col_part, col_part, periods_str
            )
        }
    } else if expr_sql.contains("LAG(") || expr_sql.contains("LEAD(") {
        // Handle shift() window functions - apply OVER to LAG/LEAD
        crate::apply_over_to_window_functions(expr_sql, order_by)
    } else {
        expr_sql.to_string()
    }
}

/// Build SELECT parts for derived window function columns
async fn build_derived_select_parts(
    schema: &ArrowSchema,
    derived_cols: &Bound<'_, PyDict>,
    table: &LTSeqTable,
) -> PyResult<Vec<String>> {
    let mut select_parts = Vec::new();

    // Add all existing columns
    for field in schema.fields() {
        select_parts.push(format!("\"{}\"", field.name()));
    }

    // Add derived columns
    for (col_name, expr_item) in derived_cols.iter() {
        let col_name_str = col_name.extract::<String>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Column name must be string")
        })?;

        let expr_dict = expr_item.cast::<PyDict>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Expression must be dict")
        })?;

        let py_expr = dict_to_py_expr(&expr_dict)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        // Convert expression to SQL
        let expr_sql = pyexpr_to_sql(&py_expr, schema)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;

        // Check what type of window function this is
        let is_rolling_agg = expr_sql.contains("__ROLLING_");
        let is_diff = expr_sql.contains("__DIFF_");

        // Build ORDER BY clause from sort expressions
        let order_by = if !table.sort_exprs.is_empty() {
            table
                .sort_exprs
                .iter()
                .map(|col| format!("\"{}\"", col))
                .collect::<Vec<_>>()
                .join(", ")
        } else {
            "".to_string()
        };

        // Apply window frame transformation
        let full_expr = apply_window_frame(&expr_sql, &order_by, is_rolling_agg, is_diff);
        select_parts.push(format!("{} AS \"{}\"", full_expr, col_name_str));
    }

    Ok(select_parts)
}

/// Derive columns with window functions using SQL
///
/// This handles complex window functions like shifts, rolling windows, and differences
/// by translating them to SQL OVER clauses and executing via DataFusion's SQL engine.
pub fn derive_with_window_functions_impl(
    table: &LTSeqTable,
    derived_cols: &Bound<'_, PyDict>,
) -> PyResult<LTSeqTable> {
    let schema = table.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Schema not available")
    })?;

    let df = table
        .dataframe
        .as_ref()
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No data loaded"))?;

    RUNTIME.block_on(async {
        // Get the data from the current DataFrame as record batches
        let current_batches = (**df).clone().collect().await.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to collect current DataFrame: {}",
                e
            ))
        })?;

        // Get schema from the actual batches, not from the stored schema
        // This ensures consistency between schema and data, especially after window operations
        let batch_schema = if let Some(first_batch) = current_batches.first() {
            first_batch.schema()
        } else {
            // If no batches, use the stored schema
            Arc::new((**schema).clone())
        };

        // Use unique temp table name to avoid conflicts
        let temp_table_name = format!("__ltseq_temp_{}", std::process::id());
        let temp_table = MemTable::try_new(Arc::clone(&batch_schema), vec![current_batches]).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create memory table: {}",
                e
            ))
        })?;

        // Deregister existing table if it exists (cleanup from previous calls)
        let _ = table.session.deregister_table(&temp_table_name);

        table
            .session
            .register_table(&temp_table_name, Arc::new(temp_table))
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to register temporary table: {}",
                    e
                ))
            })?;

        // Build SELECT parts with derived columns using the batch schema
        let select_parts = build_derived_select_parts(&batch_schema, derived_cols, table).await?;

        // Build and execute the SQL query
        let sql = format!(
            "SELECT {} FROM \"{}\"",
            select_parts.join(", "),
            temp_table_name
        );

        // Execute SQL using sql() which returns a DataFrame directly
        let new_df = table.session.sql(&sql).await.map_err(|e| {
            // Clean up temp table on error
            let _ = table.session.deregister_table(&temp_table_name);
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Window function query failed: {}",
                e
            ))
        })?;

        // Clean up temporary table
        let _ = table.session.deregister_table(&temp_table_name);

        // Get schema from the new DataFrame
        let df_schema = new_df.schema();
        let arrow_fields: Vec<Field> = df_schema.fields().iter().map(|f| (**f).clone()).collect();
        let new_arrow_schema = ArrowSchema::new(arrow_fields);

        // Return new LTSeqTable
        Ok(LTSeqTable {
            session: Arc::clone(&table.session),
            dataframe: Some(Arc::new(new_df)),
            schema: Some(Arc::new(new_arrow_schema)),
            sort_exprs: table.sort_exprs.clone(),
        })
    })
}

/// Process cumulative sum expressions and build SELECT parts
async fn build_cumsum_select_parts(
    schema: &ArrowSchema,
    cum_exprs: &[Bound<'_, PyDict>],
    table: &LTSeqTable,
) -> PyResult<Vec<String>> {
    let mut select_parts = Vec::new();

    // Add all existing columns
    for field in schema.fields() {
        select_parts.push(format!("\"{}\"", field.name()));
    }

    // Add cumulative sum columns for each input column
    for (idx, expr_item) in cum_exprs.iter().enumerate() {
        let expr_dict = expr_item.cast::<PyDict>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Expression must be dict")
        })?;

        let py_expr = dict_to_py_expr(&expr_dict)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        // Convert expression to SQL
        let expr_sql = pyexpr_to_sql(&py_expr, schema)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;

        // Extract column name from expression
        let col_name = if let PyExpr::Column(name) = &py_expr {
            name.clone()
        } else {
            format!("cum_sum_{}", idx)
        };

        // Validate numeric column
        if let PyExpr::Column(col_name_ref) = &py_expr {
            if let Some(field) = schema.field_with_name(col_name_ref).ok() {
                let field_type = field.data_type();
                if !crate::transpiler::is_numeric_type(field_type) {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                        "cum_sum() requires numeric columns. Column '{}' has type {:?}",
                        col_name_ref, field_type
                    )));
                }
            }
        }

        // Build ORDER BY clause from sort expressions
        let order_by = if !table.sort_exprs.is_empty() {
            let order_cols = table
                .sort_exprs
                .iter()
                .map(|col| format!("\"{}\"", col))
                .collect::<Vec<_>>()
                .join(", ");
            format!(" ORDER BY {}", order_cols)
        } else {
            "".to_string()
        };

        // Build cumulative sum expression
        let cumsum_expr = if !order_by.is_empty() {
            format!(
                "SUM({}) OVER ({}ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
                expr_sql, order_by
            )
        } else {
            format!(
                "SUM({}) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
                expr_sql
            )
        };

        let cumsum_col_name = format!("{}_cumsum", col_name);
        select_parts.push(format!("{} AS \"{}\"", cumsum_expr, cumsum_col_name));
    }

    Ok(select_parts)
}

/// Add cumulative sum columns with proper window functions
pub fn cum_sum_impl(table: &LTSeqTable, cum_exprs: Vec<Bound<'_, PyDict>>) -> PyResult<LTSeqTable> {
    // If no dataframe, return empty result (for unit tests)
    if table.dataframe.is_none() {
        return Ok(LTSeqTable {
            session: Arc::clone(&table.session),
            dataframe: None,
            schema: table.schema.as_ref().map(|s| Arc::clone(s)),
            sort_exprs: table.sort_exprs.clone(),
        });
    }

    let schema = table.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Schema not available. Call read_csv() first.",
        )
    })?;

    let df = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "No data loaded. Call read_csv() first.",
        )
    })?;

    RUNTIME.block_on(async {
        // Get the data from the current DataFrame as record batches
        let current_batches = (**df).clone().collect().await.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to collect current DataFrame: {}",
                e
            ))
        })?;

        // Check if the table is empty
        let is_empty = current_batches.iter().all(|batch| batch.num_rows() == 0);
        if is_empty {
            return handle_empty_cum_sum(table, schema, &cum_exprs).await;
        }

        // Get schema from the actual batches, not from the stored schema
        // This ensures consistency between schema and data, especially after window operations
        let batch_schema = if let Some(first_batch) = current_batches.first() {
            first_batch.schema()
        } else {
            // If no batches, use the stored schema
            Arc::new((**schema).clone())
        };

        // Use unique temp table name to avoid conflicts
        let temp_table_name = format!("__ltseq_cumsum_{}", std::process::id());
        let temp_table = MemTable::try_new(Arc::clone(&batch_schema), vec![current_batches]).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create memory table: {}",
                e
            ))
        })?;

        // Deregister existing table if it exists (cleanup from previous calls)
        let _ = table.session.deregister_table(&temp_table_name);

        table
            .session
            .register_table(&temp_table_name, Arc::new(temp_table))
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to register temporary table: {}",
                    e
                ))
            })?;

        // Build SELECT parts and execute query using batch schema
        let select_parts = build_cumsum_select_parts(&batch_schema, &cum_exprs, table).await?;

        // Build and execute the SQL query
        let sql = format!(
            "SELECT {} FROM \"{}\"",
            select_parts.join(", "),
            temp_table_name
        );

        let new_df = table.session.sql(&sql).await.map_err(|e| {
            // Clean up temp table on error
            let _ = table.session.deregister_table(&temp_table_name);
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "cum_sum query failed: {}",
                e
            ))
        })?;

        // Clean up temporary table
        let _ = table.session.deregister_table(&temp_table_name);

        // Get schema from the new DataFrame
        let df_schema = new_df.schema();
        let arrow_fields: Vec<Field> = df_schema.fields().iter().map(|f| (**f).clone()).collect();
        let new_arrow_schema = ArrowSchema::new(arrow_fields);

        // Return new LTSeqTable
        Ok(LTSeqTable {
            session: Arc::clone(&table.session),
            dataframe: Some(Arc::new(new_df)),
            schema: Some(Arc::new(new_arrow_schema)),
            sort_exprs: table.sort_exprs.clone(),
        })
    })
}

/// Helper function to handle empty tables for cumulative sum
async fn handle_empty_cum_sum(
    table: &LTSeqTable,
    schema: &ArrowSchema,
    cum_exprs: &[Bound<'_, PyDict>],
) -> PyResult<LTSeqTable> {
    // Build schema with cumulative sum columns added
    let mut new_fields = schema.fields().to_vec();
    for (idx, expr_item) in cum_exprs.iter().enumerate() {
        let expr_dict = expr_item.cast::<PyDict>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Expression must be dict")
        })?;

        let py_expr = dict_to_py_expr(&expr_dict).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
        })?;

        let col_name = if let PyExpr::Column(name) = &py_expr {
            name.clone()
        } else {
            format!("cum_sum_{}", idx)
        };

        // Add cumsum column with float64 type
        let cumsum_col_name = format!("{}_cumsum", col_name);
        new_fields.push(Arc::new(Field::new(
            cumsum_col_name,
            DataType::Float64,
            true,
        )));
    }

    let new_arrow_schema = ArrowSchema::new(new_fields);

    // Return table with expanded schema but no data
    Ok(LTSeqTable {
        session: Arc::clone(&table.session),
        dataframe: table.dataframe.as_ref().map(|d| Arc::clone(d)),
        schema: Some(Arc::new(new_arrow_schema)),
        sort_exprs: table.sort_exprs.clone(),
    })
}
