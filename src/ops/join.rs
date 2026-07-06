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
use crate::ops::common::{
    build_prefixed_join_schema, build_suffixed_join_schema, right_rename_map, JoinType,
};
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

/// Rename right-table columns that collide with the left table by appending
/// `suffix` (Polars semantics: only conflicting columns are renamed). Returns
/// the renamed DataFrame, the `(old, new)` rename map, and the renamed names of
/// the join key columns.
fn rename_right_df_for_join(
    df: DataFrame,
    left_schema: &ArrowSchema,
    right_schema: &ArrowSchema,
    suffix: &str,
    right_key_cols: &[String],
) -> PyResult<(DataFrame, Vec<(String, String)>, Vec<String>)> {
    let rename_map = right_rename_map(left_schema, right_schema, suffix)?;

    let select_exprs: Vec<Expr> = rename_map
        .iter()
        .map(|(old, new)| {
            if old == new {
                col(old)
            } else {
                col(old).alias(new)
            }
        })
        .collect();

    let new_key_cols: Vec<String> = right_key_cols
        .iter()
        .map(|k| {
            rename_map
                .iter()
                .find(|(old, _)| old == k)
                .map(|(_, new)| new.clone())
                .unwrap_or_else(|| k.clone())
        })
        .collect();

    let renamed_df = df
        .select(select_exprs)
        .map_err(|e| LtseqError::Runtime(format!("Failed to rename right columns: {}", e)))?;

    Ok((renamed_df, rename_map, new_key_cols))
}

/// Build result LTSeqTable from a DataFrame.
///
/// Regular joins merge rows from two sources, so neither input's sort
/// metadata describes the result (pass empty). Semi/anti joins are row
/// filters on the left table — they preserve its order, so they pass the
/// left table's sort specs through.
fn build_result_table_from_df(
    session: &Arc<SessionContext>,
    result_df: DataFrame,
    expected_schema: Arc<ArrowSchema>,
    sort_specs: Vec<crate::SortSpec>,
) -> PyResult<LTSeqTable> {
    Ok(LTSeqTable::from_df_with_schema(
        Arc::clone(session),
        result_df,
        expected_schema,
        sort_specs,
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
    suffix: &str,
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

    // Rename only the right columns that collide with the left table (suffix).
    let (renamed_right_df, rename_map, new_right_keys) = rename_right_df_for_join(
        (**df_right).clone(),
        stored_schema_left,
        stored_schema_right,
        suffix,
        &right_col_names,
    )?;

    // Execute native join
    let joined_df = RUNTIME
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

    // For inner/left joins the right equi-key duplicates the left key, so drop
    // it (Polars coalesce default). Right/full keep both keys — the left key can
    // be NULL there, so coalescing would need an explicit projection (deferred).
    let drop_right: Vec<String> = match join_type {
        JoinType::Inner | JoinType::Left => right_col_names.clone(),
        JoinType::Right | JoinType::Full => Vec::new(),
    };

    // Project the joined frame down to the final column set (left cols as-is,
    // then non-dropped right cols by their renamed names).
    let mut proj: Vec<Expr> = stored_schema_left
        .fields()
        .iter()
        .map(|f| col(f.name()))
        .collect();
    for (old, new) in &rename_map {
        if drop_right.contains(old) {
            continue;
        }
        proj.push(col(new));
    }
    let result_df = joined_df
        .select(proj)
        .map_err(|e| LtseqError::Runtime(format!("Failed to project join result: {}", e)))?;

    let expected_schema = build_suffixed_join_schema(
        stored_schema_left,
        stored_schema_right,
        suffix,
        &drop_right,
    )?;

    build_result_table_from_df(&table.session, result_df, expected_schema, Vec::new())
}

/// Prefix-aliased join used by the pointer-navigation `link()` path.
///
/// Unlike `join_impl` (Polars conflict-only suffix), this prefixes ALL right
/// columns with `{alias}_` and keeps every column, so linked fields are reached
/// as `alias_col`. Kept separate so `link()`'s naming contract is stable while
/// `join()` follows Polars semantics.
pub fn join_prefixed_impl(
    table: &LTSeqTable,
    other: &LTSeqTable,
    left_key_expr_dict: &Bound<'_, PyDict>,
    right_key_expr_dict: &Bound<'_, PyDict>,
    join_type_str: &str,
    alias: &str,
) -> PyResult<LTSeqTable> {
    let (df_left, stored_schema_left) = table.require_df_and_schema()?;
    let (df_right, stored_schema_right) = other.require_df_and_schema()?;

    let join_type = JoinType::from_str(join_type_str).ok_or_else(|| {
        LtseqError::Validation(format!("Unknown join type: {}", join_type_str))
    })?;

    let (left_col_names, right_col_names) = extract_and_validate_join_keys(
        left_key_expr_dict,
        right_key_expr_dict,
        stored_schema_left,
        stored_schema_right,
    )?;

    let df_join_type = match join_type {
        JoinType::Inner => datafusion::logical_expr::JoinType::Inner,
        JoinType::Left => datafusion::logical_expr::JoinType::Left,
        JoinType::Right => datafusion::logical_expr::JoinType::Right,
        JoinType::Full => datafusion::logical_expr::JoinType::Full,
    };

    // Prefix every right column with `{alias}_`.
    let mut new_right_keys = Vec::with_capacity(right_col_names.len());
    let mut select_exprs = Vec::with_capacity(stored_schema_right.fields().len());
    for field in stored_schema_right.fields() {
        let old_name = field.name();
        let new_name = format!("{}_{}", alias, old_name);
        if right_col_names.contains(&old_name.to_string()) {
            new_right_keys.push(new_name.clone());
        }
        select_exprs.push(col(old_name).alias(&new_name));
    }
    let renamed_right_df = (**df_right)
        .clone()
        .select(select_exprs)
        .map_err(|e| LtseqError::Runtime(format!("Failed to rename right columns: {}", e)))?;

    let result_df = RUNTIME
        .block_on(async {
            (**df_left)
                .clone()
                .join(
                    renamed_right_df,
                    df_join_type,
                    &left_col_names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                    &new_right_keys.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                    None,
                )
                .map_err(|e| format!("Native join failed: {}", e))
        })
        .map_err(LtseqError::Runtime)?;

    let expected_schema =
        build_prefixed_join_schema(stored_schema_left, stored_schema_right, alias);

    build_result_table_from_df(&table.session, result_df, expected_schema, Vec::new())
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

    // Semi/anti join = order-preserving filter on the left table
    build_result_table_from_df(
        &table.session,
        result_df,
        Arc::clone(stored_schema_left),
        table.sort_specs.clone(),
    )
}
