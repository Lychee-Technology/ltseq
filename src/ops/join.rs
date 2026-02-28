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
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

// ============================================================================
// Helper Functions
// ============================================================================

/// Collect both left and right DataFrames to batches
async fn collect_both_tables(
    df_left: &datafusion::dataframe::DataFrame,
    df_right: &datafusion::dataframe::DataFrame,
) -> Result<(Vec<RecordBatch>, Vec<RecordBatch>), String> {
    let left_result = df_left
        .clone()
        .collect()
        .await
        .map_err(|e| format!("Failed to collect left table: {}", e))?;
    let right_result = df_right
        .clone()
        .collect()
        .await
        .map_err(|e| format!("Failed to collect right table: {}", e))?;
    Ok((left_result, right_result))
}

/// Get schema from batches or use stored schema as fallback
fn get_schema_from_batches(
    batches: &[RecordBatch],
    stored_schema: &Arc<ArrowSchema>,
) -> Arc<ArrowSchema> {
    batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new((**stored_schema).clone()))
}

/// Register both tables as temp MemTables and return their names
fn register_join_tables(
    session: &SessionContext,
    left_schema: &Arc<ArrowSchema>,
    left_batches: Vec<RecordBatch>,
    right_schema: &Arc<ArrowSchema>,
    right_batches: Vec<RecordBatch>,
    prefix: &str,
) -> PyResult<(String, String)> {
    let unique_suffix = std::process::id();
    let left_name = format!("__ltseq_{}_left_{}", prefix, unique_suffix);
    let right_name = format!("__ltseq_{}_right_{}", prefix, unique_suffix);

    // Deregister any existing tables
    let _ = session.deregister_table(&left_name);
    let _ = session.deregister_table(&right_name);

    let left_mem = MemTable::try_new(Arc::clone(left_schema), vec![left_batches]).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to create left temp table: {}",
            e
        ))
    })?;

    let right_mem =
        MemTable::try_new(Arc::clone(right_schema), vec![right_batches]).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create right temp table: {}",
                e
            ))
        })?;

    session
        .register_table(&left_name, Arc::new(left_mem))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to register left temp table: {}",
                e
            ))
        })?;

    session
        .register_table(&right_name, Arc::new(right_mem))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to register right temp table: {}",
                e
            ))
        })?;

    Ok((left_name, right_name))
}

/// Execute SQL query and collect results
async fn execute_and_collect(
    session: &SessionContext,
    sql: &str,
    op_name: &str,
) -> Result<Vec<RecordBatch>, String> {
    let result_df = session
        .sql(sql)
        .await
        .map_err(|e| format!("Failed to execute {} query: {}", op_name, e))?;

    result_df
        .collect()
        .await
        .map_err(|e| format!("Failed to collect {} results: {}", op_name, e))
}

/// Build result LTSeqTable from batches
fn build_result_table(
    session: &Arc<SessionContext>,
    result_batches: Vec<RecordBatch>,
    empty_schema: Arc<ArrowSchema>,
    sort_exprs: &[String],
) -> PyResult<LTSeqTable> {
    LTSeqTable::from_batches_with_schema(
        Arc::clone(session),
        result_batches,
        empty_schema,
        sort_exprs.to_vec(),
        None,
    )
}

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

// ============================================================================
// Public API Functions
// ============================================================================

/// Perform cross-table join with automatic schema conflict handling
pub fn join_impl(
    table: &LTSeqTable,
    other: &LTSeqTable,
    left_key_expr_dict: &Bound<'_, PyDict>,
    right_key_expr_dict: &Bound<'_, PyDict>,
    join_type: &str,
    alias: &str,
) -> PyResult<LTSeqTable> {
    let (df_left, stored_schema_left) = table.require_df_and_schema()?;
    let (df_right, stored_schema_right) = other.require_df_and_schema()?;

    // Validate join type
    if !matches!(join_type, "inner" | "left" | "right" | "full") {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Unknown join type: {}",
            join_type
        )));
    }

    // Collect batches and get schemas
    let (left_batches, right_batches) = RUNTIME
        .block_on(collect_both_tables(df_left, df_right))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    let left_schema = get_schema_from_batches(&left_batches, stored_schema_left);
    let right_schema = get_schema_from_batches(&right_batches, stored_schema_right);

    // Parse and validate join keys
    let (left_col_names, right_col_names) = extract_and_validate_join_keys(
        left_key_expr_dict,
        right_key_expr_dict,
        &left_schema,
        &right_schema,
    )?;

    // Register temp tables and execute query
    let (left_name, right_name) = register_join_tables(
        &table.session,
        &left_schema,
        left_batches,
        &right_schema,
        right_batches,
        "join",
    )?;

    let sql_query = build_join_sql(
        &left_name,
        &right_name,
        &left_schema,
        &right_schema,
        &left_col_names,
        &right_col_names,
        join_type,
        alias,
    );

    let result_batches = RUNTIME
        .block_on(execute_and_collect(&table.session, &sql_query, "join"))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Cleanup
    let _ = table.session.deregister_table(&left_name);
    let _ = table.session.deregister_table(&right_name);

    // Build empty schema for empty results
    let empty_schema = build_join_result_schema(&left_schema, &right_schema, alias);

    build_result_table(&table.session, result_batches, empty_schema, &[])
}

/// Build the result schema for a join (combines left + aliased right)
fn build_join_result_schema(
    left_schema: &ArrowSchema,
    right_schema: &ArrowSchema,
    alias: &str,
) -> Arc<ArrowSchema> {
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
    Arc::new(ArrowSchema::new(result_fields))
}

/// Build SQL for semi-join or anti-join (returns only left table columns)
fn build_semi_anti_join_sql(
    left_table: &str,
    right_table: &str,
    left_schema: &ArrowSchema,
    left_keys: &[String],
    right_keys: &[String],
    is_anti: bool,
) -> String {
    // Select all columns from left table only
    let select_parts: Vec<String> = left_schema
        .fields()
        .iter()
        .map(|f| format!("L.\"{}\"", f.name()))
        .collect();

    // Build WHERE EXISTS/NOT EXISTS condition
    let where_conditions: Vec<String> = left_keys
        .iter()
        .zip(right_keys.iter())
        .map(|(l, r)| format!("L.\"{}\" = R.\"{}\"", l, r))
        .collect();

    let exists_keyword = if is_anti { "NOT EXISTS" } else { "EXISTS" };

    // Semi-join uses DISTINCT to deduplicate when right table has multiple matches
    let distinct = if is_anti { "" } else { "DISTINCT " };

    format!(
        "SELECT {}{} FROM \"{}\" L WHERE {} (SELECT 1 FROM \"{}\" R WHERE {})",
        distinct,
        select_parts.join(", "),
        left_table,
        exists_keyword,
        right_table,
        where_conditions.join(" AND ")
    )
}

/// Semi-join: Return rows from left table where keys exist in right table
pub fn semi_join_impl(
    table: &LTSeqTable,
    other: &LTSeqTable,
    left_key_expr_dict: &Bound<'_, PyDict>,
    right_key_expr_dict: &Bound<'_, PyDict>,
) -> PyResult<LTSeqTable> {
    semi_anti_join_impl(table, other, left_key_expr_dict, right_key_expr_dict, false)
}

/// Anti-join: Return rows from left table where keys do NOT exist in right table
pub fn anti_join_impl(
    table: &LTSeqTable,
    other: &LTSeqTable,
    left_key_expr_dict: &Bound<'_, PyDict>,
    right_key_expr_dict: &Bound<'_, PyDict>,
) -> PyResult<LTSeqTable> {
    semi_anti_join_impl(table, other, left_key_expr_dict, right_key_expr_dict, true)
}

/// Shared implementation for semi-join and anti-join
fn semi_anti_join_impl(
    table: &LTSeqTable,
    other: &LTSeqTable,
    left_key_expr_dict: &Bound<'_, PyDict>,
    right_key_expr_dict: &Bound<'_, PyDict>,
    is_anti: bool,
) -> PyResult<LTSeqTable> {
    let (df_left, stored_schema_left) = table.require_df_and_schema()?;
    let (df_right, stored_schema_right) = other.require_df_and_schema()?;

    // Collect batches and get schemas
    let (left_batches, right_batches) = RUNTIME
        .block_on(collect_both_tables(df_left, df_right))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    let left_schema = get_schema_from_batches(&left_batches, stored_schema_left);
    let right_schema = get_schema_from_batches(&right_batches, stored_schema_right);

    // Parse and validate join keys
    let (left_col_names, right_col_names) = extract_and_validate_join_keys(
        left_key_expr_dict,
        right_key_expr_dict,
        &left_schema,
        &right_schema,
    )?;

    // Register temp tables
    let join_type_name = if is_anti { "anti" } else { "semi" };
    let (left_name, right_name) = register_join_tables(
        &table.session,
        &left_schema,
        left_batches,
        &right_schema,
        right_batches,
        join_type_name,
    )?;

    // Build and execute SQL query
    let sql_query = build_semi_anti_join_sql(
        &left_name,
        &right_name,
        &left_schema,
        &left_col_names,
        &right_col_names,
        is_anti,
    );

    let result_batches = RUNTIME
        .block_on(execute_and_collect(
            &table.session,
            &sql_query,
            join_type_name,
        ))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Cleanup
    let _ = table.session.deregister_table(&left_name);
    let _ = table.session.deregister_table(&right_name);

    build_result_table(
        &table.session,
        result_batches,
        Arc::clone(&left_schema),
        &table.sort_exprs,
    )
}
