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
use crate::error::LtseqError;
use crate::ops::common::{
    build_equality_conditions, build_qualified_column_list, build_aliased_join_schema,
    JoinType, MultiTempTableGuard, schema_from_batches_or_fallback,
};
use crate::types::{dict_to_py_expr, PyExpr};
use crate::LTSeqTable;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::arrow::record_batch::RecordBatch;
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
    schema_from_batches_or_fallback(batches, stored_schema)
}

/// Register both tables as temp MemTables and return a guard for automatic cleanup
fn register_join_tables(
    session: &Arc<SessionContext>,
    left_schema: &Arc<ArrowSchema>,
    left_batches: Vec<RecordBatch>,
    right_schema: &Arc<ArrowSchema>,
    right_batches: Vec<RecordBatch>,
    prefix: &str,
) -> PyResult<MultiTempTableGuard> {
    let unique_suffix = std::process::id();
    let left_name = format!("__ltseq_{}_left_{}", prefix, unique_suffix);
    let right_name = format!("__ltseq_{}_right_{}", prefix, unique_suffix);

    MultiTempTableGuard::register_two(
        session,
        &left_name,
        Arc::clone(left_schema),
        left_batches,
        &right_name,
        Arc::clone(right_schema),
        right_batches,
    )
    .map_err(|e| LtseqError::Runtime(e).into())
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

    Err(LtseqError::Validation(
        "Join keys must be either simple columns or matching And-expressions".into(),
    )
    .into())
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
                        return Err(LtseqError::Validation(
                            "Left side of join equality must be a column reference".into(),
                        )
                        .into());
                    }

                    if let PyExpr::Column(right_name) = right.as_ref() {
                        right_cols.push(right_name.clone());
                    } else {
                        return Err(LtseqError::Validation(
                            "Right side of join equality must be a column reference".into(),
                        )
                        .into());
                    }
                    Ok(())
                }
                other => Err(LtseqError::Validation(format!(
                    "Unsupported operator in join condition: {}. Expected 'And' or 'Eq'",
                    other
                ))
                .into()),
            }
        }
        _ => Err(LtseqError::Validation(
            "Expected And or Eq expression in composite join".into(),
        )
        .into()),
    }
}

/// Extract and validate join key column names from expressions
fn extract_and_validate_join_keys(
    left_key_expr_dict: &Bound<'_, PyDict>,
    right_key_expr_dict: &Bound<'_, PyDict>,
    schema_left: &ArrowSchema,
    schema_right: &ArrowSchema,
) -> PyResult<(Vec<String>, Vec<String>)> {
    let left_key_expr = dict_to_py_expr(left_key_expr_dict)?;

    let right_key_expr = dict_to_py_expr(right_key_expr_dict)?;

    let (left_col_names, right_col_names) =
        extract_join_key_columns(&left_key_expr, &right_key_expr)?;

    if left_col_names.is_empty() || right_col_names.is_empty() {
        return Err(LtseqError::Validation(
            "No join keys found in expressions".into(),
        )
        .into());
    }

    // Validate left columns exist
    for left_col in &left_col_names {
        if !schema_left.fields().iter().any(|f| f.name() == left_col) {
            return Err(LtseqError::Validation(format!(
                "Column '{}' not found in left table",
                left_col
            ))
            .into());
        }
    }

    // Validate right columns exist
    for right_col in &right_col_names {
        if !schema_right.fields().iter().any(|f| f.name() == right_col) {
            return Err(LtseqError::Validation(format!(
                "Column '{}' not found in right table",
                right_col
            ))
            .into());
        }
    }

    Ok((left_col_names, right_col_names))
}

struct JoinSqlConfig<'a> {
    left_table: &'a str,
    right_table: &'a str,
    left_schema: &'a ArrowSchema,
    right_schema: &'a ArrowSchema,
    key_pairs: &'a [(String, String)],
    join_type: JoinType,
    alias: &'a str,
}

/// Build SQL JOIN query with proper column aliasing for the result
fn build_join_sql(cfg: &JoinSqlConfig<'_>) -> String {
    let left_table = cfg.left_table;
    let right_table = cfg.right_table;
    let left_schema = cfg.left_schema;
    let right_schema = cfg.right_schema;
    let key_pairs = cfg.key_pairs;
    let join_type = cfg.join_type;
    let alias = cfg.alias;
    // Build SELECT clause using common helpers
    let left_cols = build_qualified_column_list(left_schema, "L");
    
    let right_cols: String = right_schema
        .fields()
        .iter()
        .map(|f| format!("R.\"{}\" AS \"{}_{}\"", f.name(), alias, f.name()))
        .collect::<Vec<_>>()
        .join(", ");
    
    let select_clause = format!("{}, {}", left_cols, right_cols);

    // Build ON clause using paired columns
    let on_clause = build_equality_conditions("L", "R", key_pairs);

    format!(
        "SELECT {} FROM \"{}\" L {} \"{}\" R ON {}",
        select_clause,
        left_table,
        join_type.to_sql(),
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
    join_type_str: &str,
    alias: &str,
) -> PyResult<LTSeqTable> {
    let (df_left, stored_schema_left) = table.require_df_and_schema()?;
    let (df_right, stored_schema_right) = other.require_df_and_schema()?;

    // Parse join type
    let join_type = JoinType::from_str(join_type_str).ok_or_else(|| {
        LtseqError::Validation(format!("Unknown join type: {}", join_type_str))
    })?;

    // Collect batches and get schemas
    let (left_batches, right_batches) = RUNTIME
        .block_on(collect_both_tables(df_left, df_right))
        .map_err(LtseqError::Runtime)?;

    let left_schema = get_schema_from_batches(&left_batches, stored_schema_left);
    let right_schema = get_schema_from_batches(&right_batches, stored_schema_right);

    // Parse and validate join keys
    let (left_col_names, right_col_names) = extract_and_validate_join_keys(
        left_key_expr_dict,
        right_key_expr_dict,
        &left_schema,
        &right_schema,
    )?;

    let key_pairs: Vec<(String, String)> = left_col_names
        .into_iter()
        .zip(right_col_names.into_iter())
        .collect();

    // Register temp tables (RAII guard will handle cleanup)
    let table_guard = register_join_tables(
        &table.session,
        &left_schema,
        left_batches,
        &right_schema,
        right_batches,
        "join",
    )?;
    
    let table_names = table_guard.names();
    let left_name = table_names[0];
    let right_name = table_names[1];

    let sql_query = build_join_sql(&JoinSqlConfig {
        left_table: left_name,
        right_table: right_name,
        left_schema: &left_schema,
        right_schema: &right_schema,
        key_pairs: &key_pairs,
        join_type,
        alias,
    });

    let result_batches = RUNTIME
        .block_on(execute_and_collect(&table.session, &sql_query, "join"))
        .map_err(LtseqError::Runtime)?;

    // Build empty schema for empty results
    let empty_schema = build_aliased_join_schema(&left_schema, &right_schema, alias);

    build_result_table(&table.session, result_batches, empty_schema, &[])
}

/// Build SQL for semi-join or anti-join (returns only left table columns)
fn build_semi_anti_join_sql(
    left_table: &str,
    right_table: &str,
    left_schema: &ArrowSchema,
    key_pairs: &[(String, String)],
    is_anti: bool,
) -> String {
    // Select all columns from left table only
    let select_cols = build_qualified_column_list(left_schema, "L");

    // Build WHERE condition using paired columns
    let where_conditions = build_equality_conditions("L", "R", key_pairs);

    let exists_keyword = if is_anti { "NOT EXISTS" } else { "EXISTS" };

    // Semi-join uses DISTINCT to deduplicate when right table has multiple matches
    let distinct = if is_anti { "" } else { "DISTINCT " };

    format!(
        "SELECT {}{} FROM \"{}\" L WHERE {} (SELECT 1 FROM \"{}\" R WHERE {})",
        distinct,
        select_cols,
        left_table,
        exists_keyword,
        right_table,
        where_conditions
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
        .map_err(LtseqError::Runtime)?;

    let left_schema = get_schema_from_batches(&left_batches, stored_schema_left);
    let right_schema = get_schema_from_batches(&right_batches, stored_schema_right);

    // Parse and validate join keys
    let (left_col_names, right_col_names) = extract_and_validate_join_keys(
        left_key_expr_dict,
        right_key_expr_dict,
        &left_schema,
        &right_schema,
    )?;

    // Register temp tables (RAII guard will handle cleanup)
    let join_type_name = if is_anti { "anti" } else { "semi" };
    let table_guard = register_join_tables(
        &table.session,
        &left_schema,
        left_batches,
        &right_schema,
        right_batches,
        join_type_name,
    )?;
    
    let table_names = table_guard.names();
    let left_name = table_names[0];
    let right_name = table_names[1];

    // Build and execute SQL query
    let key_pairs: Vec<(String, String)> = left_col_names
        .into_iter()
        .zip(right_col_names.into_iter())
        .collect();

    let sql_query = build_semi_anti_join_sql(
        left_name,
        right_name,
        &left_schema,
        &key_pairs,
        is_anti,
    );

    let result_batches = RUNTIME
        .block_on(execute_and_collect(
            &table.session,
            &sql_query,
            join_type_name,
        ))
        .map_err(LtseqError::Runtime)?;

    build_result_table(
        &table.session,
        result_batches,
        Arc::clone(&left_schema),
        &table.sort_exprs,
    )
}
