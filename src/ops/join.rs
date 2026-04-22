//! Join operations for LTSeqTable
//!
//! Provides cross-table join functionality with automatic schema conflict handling.
//!
//! # Design
//!
//! Uses DataFusion's native DataFrame::join() API for lazy evaluation.
//! Right table columns are renamed before joining to avoid duplicate-column conflicts,
//! then the result is projected to match the expected aliased schema.
//!
//! # Supported Join Types
//!
//! - Inner: Only matching rows from both tables
//! - Left: All left rows, matching right rows or NULL
//! - Right: All right rows, matching left rows or NULL
//! - Full: All rows from both tables
//! - Semi: Returns left rows where keys exist in right table
//! - Anti: Returns left rows where keys do NOT exist in right table

use crate::engine::RUNTIME;
use crate::error::LtseqError;
use crate::ops::common::{build_aliased_join_schema, JoinType};
use crate::types::{dict_to_py_expr, PyExpr};
use crate::LTSeqTable;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::logical_expr::col;
use datafusion::prelude::*;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

// ============================================================================
// Helper Functions
// ============================================================================

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

/// Rename columns in a DataFrame to avoid conflicts during join.
///
/// Returns a new DataFrame with columns renamed according to the alias prefix,
/// plus a mapping of old->new column names for the join key adjustment.
fn rename_right_df_for_join(
    df: DataFrame,
    right_schema: &ArrowSchema,
    alias: &str,
    right_key_cols: &[String],
) -> PyResult<(DataFrame, Vec<String>)> {
    let mut new_key_cols = Vec::with_capacity(right_key_cols.len());
    let mut select_exprs = Vec::with_capacity(right_schema.fields().len());

    for field in right_schema.fields() {
        let old_name = field.name();
        let new_name = format!("{}_{}", alias, old_name);

        // If this is a join key column, track the mapping
        if right_key_cols.contains(&old_name.to_string()) {
            new_key_cols.push(new_name.clone());
        }

        // Create alias expression: col(old_name).alias(new_name)
        select_exprs.push(col(old_name).alias(&new_name));
    }

    let renamed_df = df
        .select(select_exprs)
        .map_err(|e| LtseqError::Runtime(format!("Failed to rename right columns: {}", e)))?;

    Ok((renamed_df, new_key_cols))
}

/// Build result LTSeqTable from a DataFrame
fn build_result_table_from_df(
    session: &Arc<SessionContext>,
    result_df: DataFrame,
    expected_schema: Arc<ArrowSchema>,
    sort_exprs: &[String],
) -> PyResult<LTSeqTable> {
    Ok(LTSeqTable::from_df_with_schema(
        Arc::clone(session),
        result_df,
        expected_schema,
        sort_exprs.to_vec(),
        None,
    ))
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

    // Parse and validate join keys
    let (left_col_names, right_col_names) = extract_and_validate_join_keys(
        left_key_expr_dict,
        right_key_expr_dict,
        stored_schema_left,
        stored_schema_right,
    )?;

    // Convert to DataFusion JoinType
    let df_join_type = match join_type {
        JoinType::Inner => datafusion::logical_expr::JoinType::Inner,
        JoinType::Left => datafusion::logical_expr::JoinType::Left,
        JoinType::Right => datafusion::logical_expr::JoinType::Right,
        JoinType::Full => datafusion::logical_expr::JoinType::Full,
    };

    // Rename right columns to avoid conflicts
    let (renamed_right_df, new_right_keys) = rename_right_df_for_join(
        (**df_right).clone(),
        stored_schema_right,
        alias,
        &right_col_names,
    )?;

    // Execute native join
    let result_df = RUNTIME
        .block_on(async {
            (**df_left)
                .clone()
                .join(
                    renamed_right_df,
                    df_join_type,
                    &left_col_names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                    &new_right_keys.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                    None, // No additional filter
                )
                .map_err(|e| format!("Native join failed: {}", e))
        })
        .map_err(LtseqError::Runtime)?;

    // Build expected schema for the result
    let expected_schema = build_aliased_join_schema(stored_schema_left, stored_schema_right, alias);

    build_result_table_from_df(&table.session, result_df, expected_schema, &[])
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

    // Parse and validate join keys
    let (left_col_names, right_col_names) = extract_and_validate_join_keys(
        left_key_expr_dict,
        right_key_expr_dict,
        stored_schema_left,
        stored_schema_right,
    )?;

    // Convert to DataFusion JoinType
    let df_join_type = if is_anti {
        datafusion::logical_expr::JoinType::LeftAnti
    } else {
        datafusion::logical_expr::JoinType::LeftSemi
    };

    // Execute native semi/anti join (no column renaming needed - only left columns returned)
    let result_df = RUNTIME
        .block_on(async {
            (**df_left)
                .clone()
                .join(
                    (**df_right).clone(),
                    df_join_type,
                    &left_col_names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                    &right_col_names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                    None, // No additional filter
                )
                .map_err(|e| format!("Native semi/anti join failed: {}", e))
        })
        .map_err(LtseqError::Runtime)?;

    build_result_table_from_df(
        &table.session,
        result_df,
        Arc::clone(stored_schema_left),
        &table.sort_exprs,
    )
}
