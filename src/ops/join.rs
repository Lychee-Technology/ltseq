//! Join operations for LTSeqTable
//!
//! Provides cross-table join functionality with automatic schema conflict handling.
//!
//! # Design
//!
//! DataFusion rejects joins where column names overlap. This is resolved via:
//! 1. Register both tables as temporary MemTables
//! 2. Build SQL JOIN with explicit column aliasing
//! 3. Right table columns get user-specified prefix (e.g., `right_id`)
//!
//! # Supported Join Types
//!
//! - Inner: Only matching rows from both tables
//! - Left: All left rows, matching right rows or NULL
//! - Right: All right rows, matching left rows or NULL  
//! - Full: All rows from both tables

use crate::engine::RUNTIME;
use crate::types::{dict_to_py_expr, PyExpr};
use crate::LTSeqTable;
use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema};
use datafusion::datasource::MemTable;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

/// Extract join key column names from either a Column or And-expression
///
/// For composite keys like: (o.id == p.id) & (o.year == p.year)
/// This function extracts the column pairs:
/// - Returns (left_cols, right_cols) where left_cols[i] should equal right_cols[i]
fn extract_join_key_columns(
    left_expr: &PyExpr,
    right_expr: &PyExpr,
) -> PyResult<(Vec<String>, Vec<String>)> {
    // Handle simple case: single column on each side
    if let (PyExpr::Column(left_name), PyExpr::Column(right_name)) = (left_expr, right_expr) {
        return Ok((vec![left_name.clone()], vec![right_name.clone()]));
    }

    // Handle composite case: And-expressions (should be same tree)
    if let (PyExpr::BinOp { op: op_left, .. }, PyExpr::BinOp { op: op_right, .. }) =
        (left_expr, right_expr)
    {
        if op_left == "And" && op_right == "And" {
            let mut left_cols = Vec::new();
            let mut right_cols = Vec::new();

            extract_cols_from_and(left_expr, &mut left_cols, &mut right_cols)?;
            return Ok((left_cols, right_cols));
        }
    }

    Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
        "Join keys must be either simple columns or matching And-expressions",
    ))
}

/// Recursively extract column names from And-expressions containing Eq operations
fn extract_cols_from_and(
    expr: &PyExpr,
    left_cols: &mut Vec<String>,
    right_cols: &mut Vec<String>,
) -> PyResult<()> {
    match expr {
        PyExpr::BinOp { op, left, right } => {
            match op.as_str() {
                "And" => {
                    // Recursively process both sides of And
                    extract_cols_from_and(left, left_cols, right_cols)?;
                    extract_cols_from_and(right, left_cols, right_cols)?;
                    Ok(())
                }
                "Eq" => {
                    // Extract left and right column names from equality
                    if let PyExpr::Column(left_name) = left.as_ref() {
                        left_cols.push(left_name.clone());
                    } else {
                        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "Left side of join equality must be a column reference",
                        ));
                    }

                    if let PyExpr::Column(right_name) = right.as_ref() {
                        right_cols.push(right_name.clone());
                    } else {
                        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "Right side of join equality must be a column reference",
                        ));
                    }
                    Ok(())
                }
                other => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Unsupported operator in join condition: {}. Expected 'And' or 'Eq'",
                    other
                ))),
            }
        }
        _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Expected And or Eq expression in composite join",
        )),
    }
}

/// Extract and validate join key column names from expressions
fn extract_and_validate_join_keys(
    left_key_expr_dict: &Bound<'_, PyDict>,
    right_key_expr_dict: &Bound<'_, PyDict>,
    schema_left: &ArrowSchema,
    schema_right: &ArrowSchema,
) -> PyResult<(Vec<String>, Vec<String>)> {
    let left_key_expr = dict_to_py_expr(left_key_expr_dict)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    let right_key_expr = dict_to_py_expr(right_key_expr_dict)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    let (left_col_names, right_col_names) =
        extract_join_key_columns(&left_key_expr, &right_key_expr)?;

    if left_col_names.is_empty() || right_col_names.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "No join keys found in expressions",
        ));
    }

    // Validate left columns exist
    for left_col in &left_col_names {
        if !schema_left.fields().iter().any(|f| f.name() == left_col) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Column '{}' not found in left table",
                left_col
            )));
        }
    }

    // Validate right columns exist
    for right_col in &right_col_names {
        if !schema_right.fields().iter().any(|f| f.name() == right_col) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Column '{}' not found in right table",
                right_col
            )));
        }
    }

    Ok((left_col_names, right_col_names))
}

/// Build SQL JOIN query with proper column aliasing for the result
fn build_join_sql(
    left_table: &str,
    right_table: &str,
    left_schema: &ArrowSchema,
    right_schema: &ArrowSchema,
    left_keys: &[String],
    right_keys: &[String],
    join_type: &str,
    alias: &str,
) -> String {
    let mut select_parts: Vec<String> = Vec::new();

    // All left columns (preserve as-is)
    for field in left_schema.fields() {
        select_parts.push(format!("L.\"{}\"", field.name()));
    }

    // All right columns with alias prefix
    for field in right_schema.fields() {
        select_parts.push(format!(
            "R.\"{}\" AS \"{}_{}\"",
            field.name(),
            alias,
            field.name()
        ));
    }

    // Build ON clause for potentially composite keys
    let on_conditions: Vec<String> = left_keys
        .iter()
        .zip(right_keys.iter())
        .map(|(l, r)| format!("L.\"{}\" = R.\"{}\"", l, r))
        .collect();
    let on_clause = on_conditions.join(" AND ");

    // Map join_type to SQL keyword
    let sql_join_type = match join_type {
        "inner" => "INNER JOIN",
        "left" => "LEFT JOIN",
        "right" => "RIGHT JOIN",
        "full" => "FULL OUTER JOIN",
        _ => "INNER JOIN",
    };

    format!(
        "SELECT {} FROM \"{}\" L {} \"{}\" R ON {}",
        select_parts.join(", "),
        left_table,
        sql_join_type,
        right_table,
        on_clause
    )
}

/// Perform cross-table join with automatic schema conflict handling
pub fn join_impl(
    table: &LTSeqTable,
    other: &LTSeqTable,
    left_key_expr_dict: &Bound<'_, PyDict>,
    right_key_expr_dict: &Bound<'_, PyDict>,
    join_type: &str,
    alias: &str,
) -> PyResult<LTSeqTable> {
    // 1. Validate both tables have data
    let df_left = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Left table has no data. Call read_csv() first.",
        )
    })?;

    let df_right = other.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Right table has no data. Call read_csv() first.",
        )
    })?;

    let stored_schema_left = table.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Left table schema not available.")
    })?;

    let stored_schema_right = other.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Right table schema not available.")
    })?;

    // 2. Collect both DataFrames to batches
    let (left_batches, right_batches) = RUNTIME
        .block_on(async {
            let left_future = (**df_left).clone().collect();
            let right_future = (**df_right).clone().collect();

            let left_result = left_future
                .await
                .map_err(|e| format!("Failed to collect left table: {}", e))?;
            let right_result = right_future
                .await
                .map_err(|e| format!("Failed to collect right table: {}", e))?;

            Ok::<_, String>((left_result, right_result))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // 3. Get actual schemas from batches
    let left_schema = left_batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new((**stored_schema_left).clone()));

    let right_schema = right_batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new((**stored_schema_right).clone()));

    // 4. Parse and validate join keys
    let (left_col_names, right_col_names) = extract_and_validate_join_keys(
        left_key_expr_dict,
        right_key_expr_dict,
        &left_schema,
        &right_schema,
    )?;

    // 5. Validate join type
    match join_type {
        "inner" | "left" | "right" | "full" => {}
        _ => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Unknown join type: {}",
                join_type
            )))
        }
    };

    // 6. Register both tables as temp MemTables
    let unique_suffix = std::process::id();
    let left_table_name = format!("__ltseq_join_left_{}", unique_suffix);
    let right_table_name = format!("__ltseq_join_right_{}", unique_suffix);

    let _ = table.session.deregister_table(&left_table_name);
    let _ = table.session.deregister_table(&right_table_name);

    let left_mem =
        MemTable::try_new(Arc::clone(&left_schema), vec![left_batches]).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create left temp table: {}",
                e
            ))
        })?;

    let right_mem =
        MemTable::try_new(Arc::clone(&right_schema), vec![right_batches]).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create right temp table: {}",
                e
            ))
        })?;

    table
        .session
        .register_table(&left_table_name, Arc::new(left_mem))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to register left temp table: {}",
                e
            ))
        })?;

    table
        .session
        .register_table(&right_table_name, Arc::new(right_mem))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to register right temp table: {}",
                e
            ))
        })?;

    // 7. Build and execute SQL JOIN query
    let sql_query = build_join_sql(
        &left_table_name,
        &right_table_name,
        &left_schema,
        &right_schema,
        &left_col_names,
        &right_col_names,
        join_type,
        alias,
    );

    let result_batches = RUNTIME
        .block_on(async {
            let result_df = table
                .session
                .sql(&sql_query)
                .await
                .map_err(|e| format!("Failed to execute join query: {}", e))?;

            result_df
                .collect()
                .await
                .map_err(|e| format!("Failed to collect join results: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // 8. Cleanup temp tables
    let _ = table.session.deregister_table(&left_table_name);
    let _ = table.session.deregister_table(&right_table_name);

    // 9. Build result LTSeqTable
    if result_batches.is_empty() {
        let mut result_fields: Vec<Field> = Vec::new();

        for field in left_schema.fields() {
            result_fields.push((**field).clone());
        }

        for field in right_schema.fields() {
            result_fields.push(Field::new(
                format!("{}_{}", alias, field.name()),
                field.data_type().clone(),
                true,
            ));
        }

        let empty_schema = Arc::new(ArrowSchema::new(result_fields));
        return Ok(LTSeqTable {
            session: Arc::clone(&table.session),
            dataframe: None,
            schema: Some(empty_schema),
            sort_exprs: Vec::new(),
        });
    }

    let result_schema = result_batches[0].schema();
    let result_mem =
        MemTable::try_new(Arc::clone(&result_schema), vec![result_batches]).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create result table: {}",
                e
            ))
        })?;

    let result_df = table
        .session
        .read_table(Arc::new(result_mem))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to read result table: {}",
                e
            ))
        })?;

    Ok(LTSeqTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(result_df)),
        schema: Some(result_schema),
        sort_exprs: Vec::new(),
    })
}
