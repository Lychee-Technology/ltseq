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
use datafusion::arrow::array::{Array, ArrayRef, UInt32Array};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
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
    left_keys: &'a [String],
    right_keys: &'a [String],
    join_type: JoinType,
    alias: &'a str,
}

/// Build SQL JOIN query with proper column aliasing for the result
fn build_join_sql(cfg: &JoinSqlConfig<'_>) -> String {
    let left_table = cfg.left_table;
    let right_table = cfg.right_table;
    let left_schema = cfg.left_schema;
    let right_schema = cfg.right_schema;
    let left_keys = cfg.left_keys;
    let right_keys = cfg.right_keys;
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

    // Build ON clause using both left and right key names
    let on_clause = left_keys
        .iter()
        .zip(right_keys.iter())
        .map(|(l, r)| format!("L.\"{}\" = R.\"{}\"", l, r))
        .collect::<Vec<_>>()
        .join(" AND ");

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
        left_keys: &left_col_names,
        right_keys: &right_col_names,
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
    left_keys: &[String],
    right_keys: &[String],
    is_anti: bool,
) -> String {
    // Select all columns from left table only
    let select_cols = build_qualified_column_list(left_schema, "L");

    // Build WHERE condition using both left and right key names
    let where_conditions = left_keys
        .iter()
        .zip(right_keys.iter())
        .map(|(l, r)| format!("L.\"{}\" = R.\"{}\"", l, r))
        .collect::<Vec<_>>()
        .join(" AND ");

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
    let sql_query = build_semi_anti_join_sql(
        left_name,
        right_name,
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
        .map_err(LtseqError::Runtime)?;

    build_result_table(
        &table.session,
        result_batches,
        Arc::clone(&left_schema),
        &table.sort_exprs,
    )
}

// ============================================================================
// Merge Join (sorted key join)
// ============================================================================

/// Merge join: efficient join for pre-sorted data using a two-pointer algorithm.
///
/// Falls back to SQL join for non-inner/left types or composite keys.
pub fn merge_join_impl(
    table: &LTSeqTable,
    other: &LTSeqTable,
    left_key_expr_dict: &Bound<'_, PyDict>,
    right_key_expr_dict: &Bound<'_, PyDict>,
    join_type: &str,
    alias: &str,
) -> PyResult<LTSeqTable> {
    // Only inner and left joins are supported by merge join;
    // right/full fall back to standard SQL join
    if !matches!(join_type, "inner" | "left") {
        return join_impl(
            table,
            other,
            left_key_expr_dict,
            right_key_expr_dict,
            join_type,
            alias,
        );
    }

    let (df_left, stored_schema_left) = table.require_df_and_schema()?;
    let (df_right, stored_schema_right) = other.require_df_and_schema()?;

    // Collect batches
    let (left_batches, right_batches) = RUNTIME
        .block_on(collect_both_tables(df_left, df_right))
        .map_err(|e| LtseqError::Runtime(e))?;

    let left_schema = get_schema_from_batches(&left_batches, stored_schema_left);
    let right_schema = get_schema_from_batches(&right_batches, stored_schema_right);

    // Parse and validate join keys — must be single column for merge join
    let (left_col_names, right_col_names) = extract_and_validate_join_keys(
        left_key_expr_dict,
        right_key_expr_dict,
        &left_schema,
        &right_schema,
    )?;

    if left_col_names.len() != 1 || right_col_names.len() != 1 {
        // Composite keys not supported by merge join; fall back to SQL
        return join_impl(
            table,
            other,
            left_key_expr_dict,
            right_key_expr_dict,
            join_type,
            alias,
        );
    }

    let left_key = &left_col_names[0];
    let right_key = &right_col_names[0];

    // Concatenate batches into single RecordBatch each
    let left_batch = concat_batches_or_empty(&left_batches, &left_schema)?;
    let right_batch = concat_batches_or_empty(&right_batches, &right_schema)?;

    // Extract key columns
    let left_key_idx = left_schema
        .index_of(left_key)
        .map_err(|_| LtseqError::Validation(format!("Left key column '{}' not found", left_key)))?;
    let right_key_idx = right_schema
        .index_of(right_key)
        .map_err(|_| LtseqError::Validation(format!("Right key column '{}' not found", right_key)))?;

    let left_key_col = Arc::clone(left_batch.column(left_key_idx));
    let right_key_col = Arc::clone(right_batch.column(right_key_idx));

    let is_left_join = join_type == "left";

    // Try GPU merge join first (when gpu feature is enabled)
    #[cfg(feature = "gpu")]
    {
        let left_dt = left_key_col.data_type();
        let right_dt = right_key_col.data_type();
        if crate::gpu::merge_join::is_gpu_merge_join_eligible(
            left_batch.num_rows(),
            right_batch.num_rows(),
            left_dt,
            right_dt,
        ) {
            let gpu_join_type = if is_left_join {
                crate::gpu::merge_join::GpuJoinType::Left
            } else {
                crate::gpu::merge_join::GpuJoinType::Inner
            };

            match crate::gpu::merge_join::gpu_merge_join(&left_key_col, &right_key_col, gpu_join_type)
            {
                Ok((left_idx, right_idx)) => {
                    match build_merge_join_result(
                        &left_batch,
                        &right_batch,
                        &left_idx,
                        &right_idx,
                        alias,
                        &table.session,
                        left_key,
                    ) {
                        Ok(result) => return Ok(result),
                        Err(e) => {
                            eprintln!("GPU merge join gather failed, falling back to CPU: {}", e);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("GPU merge join failed, falling back to CPU: {}", e);
                }
            }
        }
    }

    // CPU two-pointer merge join
    let (left_idx, right_idx) = cpu_merge_join(&left_key_col, &right_key_col, is_left_join)?;

    build_merge_join_result(
        &left_batch,
        &right_batch,
        &left_idx,
        &right_idx,
        alias,
        &table.session,
        left_key,
    )
}

/// Concatenate batches into a single RecordBatch, or return empty batch if none.
fn concat_batches_or_empty(
    batches: &[RecordBatch],
    schema: &Arc<ArrowSchema>,
) -> PyResult<RecordBatch> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }
    compute::concat_batches(schema, batches.iter()).map_err(|e| {
        LtseqError::Runtime(format!("Failed to concatenate batches: {}", e)).into()
    })
}

/// CPU two-pointer merge join on sorted key columns.
fn cpu_merge_join(
    left_keys: &ArrayRef,
    right_keys: &ArrayRef,
    is_left_join: bool,
) -> PyResult<(UInt32Array, UInt32Array)> {
    let left_n = left_keys.len();
    let right_n = right_keys.len();

    if left_n == 0 {
        return Ok((UInt32Array::from(Vec::<u32>::new()), UInt32Array::from(Vec::<u32>::new())));
    }

    if right_n == 0 {
        if is_left_join {
            let left_idx: Vec<u32> = (0..left_n as u32).collect();
            let right_idx: Vec<Option<u32>> = vec![None; left_n];
            return Ok((UInt32Array::from(left_idx), UInt32Array::from(right_idx)));
        } else {
            return Ok((UInt32Array::from(Vec::<u32>::new()), UInt32Array::from(Vec::<u32>::new())));
        }
    }

    match left_keys.data_type() {
        DataType::Int64 => cpu_merge_join_typed::<i64>(left_keys, right_keys, is_left_join),
        DataType::Int32 => cpu_merge_join_typed::<i32>(left_keys, right_keys, is_left_join),
        DataType::Float64 => cpu_merge_join_typed::<f64>(left_keys, right_keys, is_left_join),
        DataType::Float32 => cpu_merge_join_typed::<f32>(left_keys, right_keys, is_left_join),
        DataType::Utf8 => cpu_merge_join_utf8(left_keys, right_keys, is_left_join),
        dt => Err(LtseqError::Validation(format!(
            "Unsupported key type for merge join: {:?}", dt
        )).into()),
    }
}

trait MergeJoinKey: PartialOrd + Copy + 'static {
    type ArrowArray: Array + 'static;
    fn downcast(arr: &ArrayRef) -> Option<&Self::ArrowArray>;
    fn value(arr: &Self::ArrowArray, idx: usize) -> Self;
}

impl MergeJoinKey for i64 {
    type ArrowArray = datafusion::arrow::array::Int64Array;
    fn downcast(arr: &ArrayRef) -> Option<&Self::ArrowArray> { arr.as_any().downcast_ref() }
    fn value(arr: &Self::ArrowArray, idx: usize) -> Self { arr.value(idx) }
}

impl MergeJoinKey for i32 {
    type ArrowArray = datafusion::arrow::array::Int32Array;
    fn downcast(arr: &ArrayRef) -> Option<&Self::ArrowArray> { arr.as_any().downcast_ref() }
    fn value(arr: &Self::ArrowArray, idx: usize) -> Self { arr.value(idx) }
}

impl MergeJoinKey for f64 {
    type ArrowArray = datafusion::arrow::array::Float64Array;
    fn downcast(arr: &ArrayRef) -> Option<&Self::ArrowArray> { arr.as_any().downcast_ref() }
    fn value(arr: &Self::ArrowArray, idx: usize) -> Self { arr.value(idx) }
}

impl MergeJoinKey for f32 {
    type ArrowArray = datafusion::arrow::array::Float32Array;
    fn downcast(arr: &ArrayRef) -> Option<&Self::ArrowArray> { arr.as_any().downcast_ref() }
    fn value(arr: &Self::ArrowArray, idx: usize) -> Self { arr.value(idx) }
}

fn cpu_merge_join_typed<T: MergeJoinKey>(
    left_keys: &ArrayRef,
    right_keys: &ArrayRef,
    is_left_join: bool,
) -> PyResult<(UInt32Array, UInt32Array)> {
    let left_arr = T::downcast(left_keys)
        .ok_or_else(|| LtseqError::Runtime("Failed to downcast left key array".into()))?;
    let right_arr = T::downcast(right_keys)
        .ok_or_else(|| LtseqError::Runtime("Failed to downcast right key array".into()))?;

    merge_join_loop(
        left_arr.len(),
        right_arr.len(),
        is_left_join,
        |li, ri| T::value(left_arr, li).partial_cmp(&T::value(right_arr, ri)),
        |li1, li2| T::value(left_arr, li1) == T::value(left_arr, li2),
        |ri1, ri2| T::value(right_arr, ri1) == T::value(right_arr, ri2),
    )
}

fn cpu_merge_join_utf8(
    left_keys: &ArrayRef,
    right_keys: &ArrayRef,
    is_left_join: bool,
) -> PyResult<(UInt32Array, UInt32Array)> {
    use datafusion::arrow::array::StringArray;

    let left_arr = left_keys.as_any().downcast_ref::<StringArray>()
        .ok_or_else(|| LtseqError::Runtime("Failed to downcast left key to StringArray".into()))?;
    let right_arr = right_keys.as_any().downcast_ref::<StringArray>()
        .ok_or_else(|| LtseqError::Runtime("Failed to downcast right key to StringArray".into()))?;

    merge_join_loop(
        left_arr.len(),
        right_arr.len(),
        is_left_join,
        |li, ri| left_arr.value(li).partial_cmp(right_arr.value(ri)),
        |li1, li2| left_arr.value(li1) == left_arr.value(li2),
        |ri1, ri2| right_arr.value(ri1) == right_arr.value(ri2),
    )
}

fn merge_join_loop(
    left_n: usize,
    right_n: usize,
    is_left_join: bool,
    cmp_lr: impl Fn(usize, usize) -> Option<std::cmp::Ordering>,
    eq_ll: impl Fn(usize, usize) -> bool,
    eq_rr: impl Fn(usize, usize) -> bool,
) -> PyResult<(UInt32Array, UInt32Array)> {
    let mut left_indices: Vec<u32> = Vec::new();
    let mut right_indices: Vec<Option<u32>> = Vec::new();

    let mut li = 0usize;
    let mut ri = 0usize;

    while li < left_n && ri < right_n {
        match cmp_lr(li, ri) {
            Some(std::cmp::Ordering::Less) => {
                if is_left_join {
                    left_indices.push(li as u32);
                    right_indices.push(None);
                }
                li += 1;
            }
            Some(std::cmp::Ordering::Greater) => {
                ri += 1;
            }
            Some(std::cmp::Ordering::Equal) => {
                let ri_start = ri;
                while ri < right_n && eq_rr(ri_start, ri) { ri += 1; }
                let li_start = li;
                while li < left_n && eq_ll(li_start, li) { li += 1; }
                for l in li_start..li {
                    for r in ri_start..ri {
                        left_indices.push(l as u32);
                        right_indices.push(Some(r as u32));
                    }
                }
            }
            None => {
                if is_left_join {
                    left_indices.push(li as u32);
                    right_indices.push(None);
                }
                li += 1;
            }
        }
    }

    if is_left_join {
        while li < left_n {
            left_indices.push(li as u32);
            right_indices.push(None);
            li += 1;
        }
    }

    Ok((UInt32Array::from(left_indices), UInt32Array::from(right_indices)))
}

fn build_merge_join_result(
    left_batch: &RecordBatch,
    right_batch: &RecordBatch,
    left_idx: &UInt32Array,
    right_idx: &UInt32Array,
    alias: &str,
    session: &Arc<SessionContext>,
    left_key: &str,
) -> PyResult<LTSeqTable> {
    let mut result_columns: Vec<ArrayRef> = Vec::new();

    for col_idx in 0..left_batch.num_columns() {
        let col = left_batch.column(col_idx);
        let taken = compute::take(col.as_ref(), left_idx, None)
            .map_err(|e| LtseqError::Runtime(format!("take() failed on left column: {}", e)))?;
        result_columns.push(taken);
    }

    for col_idx in 0..right_batch.num_columns() {
        let col = right_batch.column(col_idx);
        let taken = compute::take(col.as_ref(), right_idx, None)
            .map_err(|e| LtseqError::Runtime(format!("take() failed on right column: {}", e)))?;
        result_columns.push(taken);
    }

    let left_schema = left_batch.schema();
    let right_schema = right_batch.schema();
    let result_schema = build_aliased_join_schema(&left_schema, &right_schema, alias);

    if result_columns.is_empty() {
        return LTSeqTable::from_batches_with_schema(
            Arc::clone(session),
            vec![],
            result_schema,
            vec![left_key.to_string()],
            None,
        );
    }

    let result_batch = RecordBatch::try_new(Arc::clone(&result_schema), result_columns)
        .map_err(|e| LtseqError::Runtime(format!("Failed to build result batch: {}", e)))?;

    LTSeqTable::from_batches_with_schema(
        Arc::clone(session),
        vec![result_batch],
        result_schema,
        vec![left_key.to_string()],
        None,
    )
}
