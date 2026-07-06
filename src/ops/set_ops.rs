//! Set operations: distinct, union, intersect, diff, is_subset, rvs, step
//!
//! This module contains helper functions for set-theoretic table operations.
//! The actual methods are implemented in the #[pymethods] impl block in lib.rs
//! due to PyO3 constraints (only one #[pymethods] impl block per struct is allowed).
//!
//! # Operations Provided
//!
//! ## Deduplication
//! - **distinct_impl**: Remove duplicate rows based on key columns
//!
//! ## Set Algebra
//! - **union_impl**: Vertically concatenate two tables (UNION ALL)
//! - **intersect_impl**: Return rows present in both tables (deduplicated)
//! - **diff_impl**: Return rows in first table but not in second
//! - **is_subset_impl**: Check if first table is a subset of second
//!
//! ## Sequence
//! - **rvs_impl**: Reverse row order
//! - **step_impl**: Take every nth row
//!
//! # Implementation Pattern
//!
//! Fully native DataFusion plans (issue #91 PR 6): semi/anti joins for the
//! set algebra, row_number windows for keyed distinct / rvs / step. No SQL
//! strings, no temp-table registration; the only materialization is the
//! scalar `is_subset` count.

use crate::engine::RUNTIME;
use crate::error::LtseqError;
use crate::LTSeqTable;
use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::common::Column;
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::{Expr, ExprFunctionExt, JoinType, SortExpr};
use datafusion::prelude::*;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

// ============================================================================
// Helper Functions for Set Operations
// ============================================================================

/// Determine comparison columns from optional key expression or use all columns
fn get_compare_columns(
    key_expr_dict: Option<&Bound<'_, PyDict>>,
    schema: &ArrowSchema,
) -> PyResult<Vec<String>> {
    let compare_cols: Vec<String> = if let Some(expr_dict) = key_expr_dict {
        extract_key_columns(expr_dict, schema)?
    } else {
        schema
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect()
    };

    if compare_cols.is_empty() {
        return Err(LtseqError::Validation(
            "No columns specified for comparison".into(),
        )
        .into());
    }
    Ok(compare_cols)
}

/// Create result LTSeqTable from DataFrame.
///
/// Set operations (union/intersect/diff) and rvs merge or reorder rows, so
/// the input sort metadata no longer describes the result: always empty.
fn create_result_table(
    session: &Arc<SessionContext>,
    result_df: datafusion::dataframe::DataFrame,
) -> LTSeqTable {
    LTSeqTable::from_df(
        Arc::clone(session),
        result_df,
        Vec::new(),
        None,
    )
}

/// All columns of the schema as unqualified column expressions.
fn all_column_exprs(schema: &ArrowSchema) -> Vec<Expr> {
    schema
        .fields()
        .iter()
        .map(|f| Expr::Column(Column::new_unqualified(f.name())))
        .collect()
}

/// A 0-based row-position column over the whole (single-partition) input.
///
/// `ROW_NUMBER() OVER ()` carries no ORDER BY, so it is only deterministic
/// over a single in-order partition. The UInt64 output is cast to Int64 so
/// downstream arithmetic stays in plain integer types (the legacy SQL's
/// `% n = 0` comparison broke on Decimal128 type-inference — step(1) never
/// worked before this migration).
fn row_position_expr() -> Expr {
    cast(row_number(), DataType::Int64) - lit(1i64)
}

/// Snapshot the table into a single in-order partition so a row-position
/// window over it is deterministic.
///
/// rvs/step operate on PHYSICAL row positions: on a lazy multi-partition
/// plan, the coalesce feeding an unordered window does not preserve order,
/// so positions must be assigned over a materialized snapshot. The legacy
/// SQL path did the same (collect → MemTable → re-scan); this is one scan
/// fewer and registers nothing.
fn snapshot_single_partition(
    table: &LTSeqTable,
    df: &Arc<datafusion::dataframe::DataFrame>,
) -> PyResult<datafusion::dataframe::DataFrame> {
    let batches = RUNTIME
        .block_on(async {
            (**df)
                .clone()
                .collect()
                .await
                .map_err(|e| format!("Failed to collect for position snapshot: {}", e))
        })
        .map_err(LtseqError::Runtime)?;

    let schema = batches
        .first()
        .map(|b| b.schema())
        .ok_or_else(|| LtseqError::NoData)?;
    let combined = datafusion::arrow::compute::concat_batches(&schema, &batches)
        .map_err(|e| LtseqError::Runtime(format!("Failed to combine batches: {}", e)))?;

    table
        .session
        .read_batch(combined)
        .map_err(|e| LtseqError::Runtime(format!("Failed to read snapshot: {}", e)).into())
}

// ============================================================================
// Public API Functions
// ============================================================================

/// Helper function to remove duplicate rows
///
/// Args:
///     table: Reference to LTSeqTable
///     key_exprs: Column expressions to determine uniqueness (empty = use all columns)
pub fn distinct_impl(
    table: &LTSeqTable,
    key_exprs: Vec<Bound<'_, PyDict>>,
) -> PyResult<LTSeqTable> {
    // If no dataframe, return empty result (for unit tests)
    if table.dataframe.is_none() {
        return Ok(LTSeqTable::empty(
            Arc::clone(&table.session),
            table.schema.as_ref().map(Arc::clone),
            Vec::new(),
            None,
        ));
    }

    let (df, schema) = table.require_df_and_schema()?;

    // If no key columns specified, use simple distinct on all columns
    if key_exprs.is_empty() {
        return distinct_all_columns(table, df);
    }

    // Convert key expressions to column names
    let key_cols = extract_key_cols_from_exprs(&key_exprs, schema)?;

    // Execute distinct with key columns
    distinct_with_keys(table, df, schema, &key_cols)
}

/// Simple distinct on all columns
fn distinct_all_columns(
    table: &LTSeqTable,
    df: &Arc<datafusion::dataframe::DataFrame>,
) -> PyResult<LTSeqTable> {
    let distinct_df = RUNTIME
        .block_on(async {
            (**df)
                .clone()
                .distinct()
                .map_err(|e| format!("Distinct execution failed: {}", e))
        })
        .map_err(LtseqError::Runtime)?;

    Ok(LTSeqTable::from_df_with_schema(
        Arc::clone(&table.session),
        distinct_df,
        // SAFETY: schema validated by require_df_and_schema() before this function
        Arc::clone(table.schema.as_ref().expect("schema validated by caller")),
        Vec::new(), // distinct reorders rows: sort metadata no longer holds
        None,
    ))
}

/// Extract column names from key expressions
fn extract_key_cols_from_exprs(
    key_exprs: &[Bound<'_, PyDict>],
    schema: &ArrowSchema,
) -> PyResult<Vec<String>> {
    let mut key_cols = Vec::new();
    for expr_dict in key_exprs.iter() {
        let py_expr = crate::types::dict_to_py_expr(expr_dict)?;

        let col_name = match &py_expr {
            crate::types::PyExpr::Column(name) => name.clone(),
            _ => {
                return Err(LtseqError::Validation(
                    "distinct() key expressions must be simple column references".into(),
                )
                .into())
            }
        };

        if !schema.fields().iter().any(|f| f.name() == &col_name) {
            return Err(LtseqError::Validation(format!(
                "Column '{}' not found in schema",
                col_name
            ))
            .into());
        }
        key_cols.push(col_name);
    }
    Ok(key_cols)
}

/// Distinct with specific key columns: keep the FIRST occurrence of each key
/// in the table's current row order (e.g. sort desc then distinct-by-id keeps
/// each id's maximum). The legacy SQL got this via materialization plus the
/// implicit tie-break of `ORDER BY <partition keys>`; natively we make the
/// tie-break explicit — a row-position column over a single-partition
/// snapshot — because a partitioned window over a lazy multi-partition plan
/// does not preserve input order.
fn distinct_with_keys(
    table: &LTSeqTable,
    df: &Arc<datafusion::dataframe::DataFrame>,
    schema: &Arc<ArrowSchema>,
    key_cols: &[String],
) -> PyResult<LTSeqTable> {
    let key_exprs: Vec<Expr> = key_cols
        .iter()
        .map(|c| Expr::Column(Column::new_unqualified(c)))
        .collect();

    let mut stage = all_column_exprs(schema);
    stage.push(row_position_expr().alias("__distinct_pos__"));

    let rn = row_number()
        .partition_by(key_exprs)
        .order_by(vec![SortExpr::new(col("__distinct_pos__"), true, false)])
        .build()
        .map_err(|e| LtseqError::Runtime(format!("Failed to build distinct window: {}", e)))?
        .alias("__distinct_rn__");

    let distinct_df = snapshot_single_partition(table, df)?
        .select(stage)
        .and_then(|d| {
            let mut with_rn = all_column_exprs(schema);
            with_rn.push(col("__distinct_pos__"));
            with_rn.push(rn);
            d.select(with_rn)
        })
        .and_then(|d| d.filter(col("__distinct_rn__").eq(lit(1i64))))
        .and_then(|d| {
            d.sort(vec![SortExpr::new(col("__distinct_pos__"), true, false)])
        })
        .and_then(|d| d.select(all_column_exprs(schema)))
        .map_err(|e| LtseqError::Runtime(format!("Distinct execution failed: {}", e)))?;

    Ok(LTSeqTable::from_df_with_schema(
        Arc::clone(&table.session),
        distinct_df,
        // SAFETY: schema validated by require_df_and_schema() before this function
        Arc::clone(table.schema.as_ref().expect("schema validated by caller")),
        Vec::new(), // distinct reorders rows: sort metadata no longer holds
        None,
    ))
}

/// Union: Vertically concatenate two tables with compatible schemas
///
/// Combines all rows from both tables (equivalent to UNION ALL).
/// Schemas must match exactly in column names and order.
pub fn union_impl(table1: &LTSeqTable, table2: &LTSeqTable) -> PyResult<LTSeqTable> {
    let (df1, schema1) = table1.require_df_and_schema()?;
    let (df2, schema2) = table2.require_df_and_schema()?;

    // Check that column names match
    let cols1: Vec<&str> = schema1.fields().iter().map(|f| f.name().as_str()).collect();
    let cols2: Vec<&str> = schema2.fields().iter().map(|f| f.name().as_str()).collect();

    if cols1 != cols2 {
        return Err(LtseqError::Validation(format!(
            "Schema mismatch: {:?} vs {:?}",
            cols1, cols2
        ))
        .into());
    }

    let union_df = RUNTIME
        .block_on(async {
            (**df1)
                .clone()
                .union((**df2).clone())
                .map_err(|e| format!("Union failed: {}", e))
        })
        .map_err(LtseqError::Runtime)?;

    Ok(create_result_table(&table1.session, union_df))
}

/// Intersect: Return rows present in both tables
///
/// Returns rows that appear in both tables based on key columns, deduplicated
/// (the legacy SQL used SELECT DISTINCT ... WHERE EXISTS).
/// If no key expression provided, uses all columns.
pub fn intersect_impl(
    table1: &LTSeqTable,
    table2: &LTSeqTable,
    key_expr_dict: Option<Bound<'_, PyDict>>,
) -> PyResult<LTSeqTable> {
    let joined = semi_anti_on_keys(table1, table2, key_expr_dict, JoinType::LeftSemi)?;
    // LeftSemi does not deduplicate the left side; the legacy semantics did.
    let deduped = joined
        .distinct()
        .map_err(|e| LtseqError::Runtime(format!("Intersect distinct failed: {}", e)))?;
    Ok(create_result_table(&table1.session, deduped))
}

/// Diff: Return rows in first table but not in second
///
/// Returns rows from the left table that don't appear in the right table,
/// preserving left-side duplicates (the legacy SQL had no DISTINCT here).
/// If no key expression provided, uses all columns.
pub fn diff_impl(
    table1: &LTSeqTable,
    table2: &LTSeqTable,
    key_expr_dict: Option<Bound<'_, PyDict>>,
) -> PyResult<LTSeqTable> {
    let joined = semi_anti_on_keys(table1, table2, key_expr_dict, JoinType::LeftAnti)?;
    Ok(create_result_table(&table1.session, joined))
}

/// Is Subset: Check if first table is a subset of second table
///
/// Returns true if all rows in the left table also appear in the right table.
/// If no key expression provided, uses all columns.
pub fn is_subset_impl(
    table1: &LTSeqTable,
    table2: &LTSeqTable,
    key_expr_dict: Option<Bound<'_, PyDict>>,
) -> PyResult<bool> {
    // t1 ⊆ t2 iff anti-join(t1, t2) is empty. count() is a scalar terminal,
    // so executing it here is within the no-materialization rule.
    let anti = semi_anti_on_keys(table1, table2, key_expr_dict, JoinType::LeftAnti)?;
    let missing = RUNTIME
        .block_on(async { anti.count().await })
        .map_err(|e| LtseqError::Runtime(format!("Subset count failed: {}", e)))?;
    Ok(missing == 0)
}

/// Native LeftSemi / LeftAnti join of the two tables on the comparison
/// columns. NULL keys never match (join equality), which is exactly the
/// legacy EXISTS / NOT EXISTS behavior.
fn semi_anti_on_keys(
    table1: &LTSeqTable,
    table2: &LTSeqTable,
    key_expr_dict: Option<Bound<'_, PyDict>>,
    join_type: JoinType,
) -> PyResult<datafusion::dataframe::DataFrame> {
    let (df1, schema1) = table1.require_df_and_schema()?;
    let (df2, _schema2) = table2.require_df_and_schema()?;
    let compare_cols = get_compare_columns(key_expr_dict.as_ref(), schema1)?;

    let keys: Vec<&str> = compare_cols.iter().map(|s| s.as_str()).collect();
    (**df1)
        .clone()
        .join((**df2).clone(), join_type, &keys, &keys, None)
        .map_err(|e| LtseqError::Runtime(format!("Set operation join failed: {}", e)).into())
}

/// Reverse: Return rows in reversed order
///
/// Assigns a native row-position column, sorts by it descending, and projects
/// it away. Determinism matches the legacy SQL: single in-order partition.
pub fn rvs_impl(table: &LTSeqTable) -> PyResult<LTSeqTable> {
    let (df, schema) = table.require_df_and_schema()?;

    let mut stage = all_column_exprs(schema);
    stage.push(row_position_expr().alias("__rvs_pos__"));

    let result_df = snapshot_single_partition(table, df)?
        .select(stage)
        .and_then(|d| {
            d.sort(vec![SortExpr::new(col("__rvs_pos__"), false, true)])
        })
        .and_then(|d| d.select(all_column_exprs(schema)))
        .map_err(|e| LtseqError::Runtime(format!("rvs failed: {}", e)))?;

    Ok(LTSeqTable::from_df_with_schema(
        Arc::clone(&table.session),
        result_df,
        Arc::clone(schema),
        Vec::new(), // sort order is no longer valid
        None,
    ))
}

/// Step: Take every nth row (0-based, rows 0, n, 2n, …)
pub fn step_impl(table: &LTSeqTable, n: usize) -> PyResult<LTSeqTable> {
    if n == 0 {
        return Err(LtseqError::Validation("step() n must be >= 1".into()).into());
    }

    let (df, schema) = table.require_df_and_schema()?;

    let mut stage = all_column_exprs(schema);
    stage.push(row_position_expr().alias("__step_pos__"));

    let result_df = snapshot_single_partition(table, df)?
        .select(stage)
        .and_then(|d| {
            d.filter((col("__step_pos__") % lit(n as i64)).eq(lit(0i64)))
        })
        .and_then(|d| d.select(all_column_exprs(schema)))
        .map_err(|e| LtseqError::Runtime(format!("step failed: {}", e)))?;

    Ok(LTSeqTable::from_df_with_schema(
        Arc::clone(&table.session),
        result_df,
        Arc::clone(schema),
        Vec::new(),
        None,
    ))
}

/// Helper function to extract column names from a key expression dict
///
/// Currently handles single column keys: lambda r: r.id
/// For multiple columns, leave on=None (uses all columns)
fn extract_key_columns(
    expr_dict: &Bound<'_, PyDict>,
    schema: &ArrowSchema,
) -> PyResult<Vec<String>> {
    use crate::types::dict_to_py_expr;
    use crate::types::PyExpr;

    let py_expr = dict_to_py_expr(expr_dict)?;

    let mut columns = Vec::new();

    match &py_expr {
        PyExpr::Column(name) => columns.push(name.clone()),
        _ => {
            return Err(LtseqError::Validation(
                "on= argument must be a single column reference (e.g., lambda r: r.id). \
                 For multiple columns, leave on=None to use all columns.".into(),
            )
            .into());
        }
    }

    for col in &columns {
        if schema.field_with_name(col).is_err() {
            return Err(LtseqError::Validation(format!(
                "Column '{}' not found in schema. Available columns: {:?}",
                col,
                schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
            ))
            .into());
        }
    }

    Ok(columns)
}
