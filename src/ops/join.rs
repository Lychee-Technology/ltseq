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
use crate::types::{dict_to_py_expr, PyExpr};
use crate::LTSeqTable;
use datafusion::arrow::array::{Array, ArrayRef, UInt32Array};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
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
        LtseqError::Runtime(format!("Failed to create left temp table: {}", e))
    })?;

    let right_mem =
        MemTable::try_new(Arc::clone(right_schema), vec![right_batches]).map_err(|e| {
            LtseqError::Runtime(format!("Failed to create right temp table: {}", e))
        })?;

    session
        .register_table(&left_name, Arc::new(left_mem))
        .map_err(|e| {
            LtseqError::Runtime(format!("Failed to register left temp table: {}", e))
        })?;

    session
        .register_table(&right_name, Arc::new(right_mem))
        .map_err(|e| {
            LtseqError::Runtime(format!("Failed to register right temp table: {}", e))
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
        return Err(LtseqError::Validation(format!(
            "Unknown join type: {}",
            join_type
        ))
        .into());
    }

    // Collect batches and get schemas
    let (left_batches, right_batches) = RUNTIME
        .block_on(collect_both_tables(df_left, df_right))
        .map_err(|e| LtseqError::Runtime(e))?;

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
        .map_err(|e| LtseqError::Runtime(e))?;

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
        .map_err(|e| LtseqError::Runtime(e))?;

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
        .map_err(|e| LtseqError::Runtime(e))?;

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

// ============================================================================
// Merge Join — GPU-accelerated with CPU fallback
// ============================================================================

/// Merge join for pre-sorted tables.
///
/// Uses GPU merge join when available and eligible (single numeric key column,
/// sufficient row count), otherwise falls back to a CPU two-pointer merge.
///
/// Both tables must be sorted by their respective join key columns (ascending).
/// Only single-column equi-joins are supported.
///
/// # Arguments
///
/// * `table` - Left table (sorted by left join key)
/// * `other` - Right table (sorted by right join key)
/// * `left_key_expr_dict` - Serialized expression for left join key
/// * `right_key_expr_dict` - Serialized expression for right join key
/// * `join_type` - "inner" or "left" (right/full fall back to SQL)
/// * `alias` - Prefix for right table columns in result
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
    let (left_idx, right_idx) =
        cpu_merge_join(&left_key_col, &right_key_col, is_left_join)?;

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
///
/// Both key columns must be sorted ascending. Returns `(left_indices, right_indices)`
/// as UInt32Arrays suitable for `arrow::compute::take()`.
fn cpu_merge_join(
    left_keys: &ArrayRef,
    right_keys: &ArrayRef,
    is_left_join: bool,
) -> PyResult<(UInt32Array, UInt32Array)> {
    let left_n = left_keys.len();
    let right_n = right_keys.len();

    if left_n == 0 {
        return Ok((
            UInt32Array::from(Vec::<u32>::new()),
            UInt32Array::from(Vec::<u32>::new()),
        ));
    }

    if right_n == 0 {
        if is_left_join {
            let left_idx: Vec<u32> = (0..left_n as u32).collect();
            let right_idx: Vec<Option<u32>> = vec![None; left_n];
            return Ok((UInt32Array::from(left_idx), UInt32Array::from(right_idx)));
        } else {
            return Ok((
                UInt32Array::from(Vec::<u32>::new()),
                UInt32Array::from(Vec::<u32>::new()),
            ));
        }
    }

    // Dispatch based on data type
    match left_keys.data_type() {
        DataType::Int64 => cpu_merge_join_typed::<i64>(left_keys, right_keys, is_left_join),
        DataType::Int32 => cpu_merge_join_typed::<i32>(left_keys, right_keys, is_left_join),
        DataType::Float64 => cpu_merge_join_typed::<f64>(left_keys, right_keys, is_left_join),
        DataType::Float32 => cpu_merge_join_typed::<f32>(left_keys, right_keys, is_left_join),
        DataType::Utf8 => cpu_merge_join_utf8(left_keys, right_keys, is_left_join),
        dt => Err(LtseqError::Validation(format!(
            "Unsupported key type for merge join: {:?}",
            dt
        ))
        .into()),
    }
}

/// Trait to unify numeric array downcasting for merge join.
trait MergeJoinKey: PartialOrd + Copy + 'static {
    type ArrowArray: Array + 'static;
    fn downcast(arr: &ArrayRef) -> Option<&Self::ArrowArray>;
    fn value(arr: &Self::ArrowArray, idx: usize) -> Self;
}

impl MergeJoinKey for i64 {
    type ArrowArray = datafusion::arrow::array::Int64Array;
    fn downcast(arr: &ArrayRef) -> Option<&Self::ArrowArray> {
        arr.as_any().downcast_ref()
    }
    fn value(arr: &Self::ArrowArray, idx: usize) -> Self {
        arr.value(idx)
    }
}

impl MergeJoinKey for i32 {
    type ArrowArray = datafusion::arrow::array::Int32Array;
    fn downcast(arr: &ArrayRef) -> Option<&Self::ArrowArray> {
        arr.as_any().downcast_ref()
    }
    fn value(arr: &Self::ArrowArray, idx: usize) -> Self {
        arr.value(idx)
    }
}

impl MergeJoinKey for f64 {
    type ArrowArray = datafusion::arrow::array::Float64Array;
    fn downcast(arr: &ArrayRef) -> Option<&Self::ArrowArray> {
        arr.as_any().downcast_ref()
    }
    fn value(arr: &Self::ArrowArray, idx: usize) -> Self {
        arr.value(idx)
    }
}

impl MergeJoinKey for f32 {
    type ArrowArray = datafusion::arrow::array::Float32Array;
    fn downcast(arr: &ArrayRef) -> Option<&Self::ArrowArray> {
        arr.as_any().downcast_ref()
    }
    fn value(arr: &Self::ArrowArray, idx: usize) -> Self {
        arr.value(idx)
    }
}

/// Two-pointer merge join for a specific numeric type.
fn cpu_merge_join_typed<T: MergeJoinKey>(
    left_keys: &ArrayRef,
    right_keys: &ArrayRef,
    is_left_join: bool,
) -> PyResult<(UInt32Array, UInt32Array)> {
    let left_arr = T::downcast(left_keys).ok_or_else(|| {
        LtseqError::Runtime("Failed to downcast left key array".into())
    })?;
    let right_arr = T::downcast(right_keys).ok_or_else(|| {
        LtseqError::Runtime("Failed to downcast right key array".into())
    })?;

    let left_n = left_arr.len();
    let right_n = right_arr.len();

    let mut left_indices: Vec<u32> = Vec::new();
    let mut right_indices: Vec<Option<u32>> = Vec::new();

    let mut li = 0usize;
    let mut ri = 0usize;

    while li < left_n && ri < right_n {
        let lv = T::value(left_arr, li);
        let rv = T::value(right_arr, ri);

        if lv < rv {
            // Left key < right key: no match for this left row
            if is_left_join {
                left_indices.push(li as u32);
                right_indices.push(None);
            }
            li += 1;
        } else if lv > rv {
            // Left key > right key: advance right
            ri += 1;
        } else {
            // Equal: find the range of equal keys in right
            let ri_start = ri;
            while ri < right_n && T::value(right_arr, ri) == lv {
                ri += 1;
            }
            // Also find range of equal keys in left
            let li_start = li;
            while li < left_n && T::value(left_arr, li) == lv {
                li += 1;
            }
            // Cross product of matching ranges
            for l in li_start..li {
                for r in ri_start..ri {
                    left_indices.push(l as u32);
                    right_indices.push(Some(r as u32));
                }
            }
        }
    }

    // Remaining left rows (unmatched) for left join
    if is_left_join {
        while li < left_n {
            left_indices.push(li as u32);
            right_indices.push(None);
            li += 1;
        }
    }

    // Convert Option<u32> to nullable UInt32Array
    Ok((UInt32Array::from(left_indices), UInt32Array::from(right_indices)))
}

/// Two-pointer merge join for Utf8 (string) keys.
fn cpu_merge_join_utf8(
    left_keys: &ArrayRef,
    right_keys: &ArrayRef,
    is_left_join: bool,
) -> PyResult<(UInt32Array, UInt32Array)> {
    use datafusion::arrow::array::StringArray;

    let left_arr = left_keys
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| LtseqError::Runtime("Failed to downcast left key to StringArray".into()))?;
    let right_arr = right_keys
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| LtseqError::Runtime("Failed to downcast right key to StringArray".into()))?;

    let left_n = left_arr.len();
    let right_n = right_arr.len();

    let mut left_indices: Vec<u32> = Vec::new();
    let mut right_indices: Vec<Option<u32>> = Vec::new();

    let mut li = 0usize;
    let mut ri = 0usize;

    while li < left_n && ri < right_n {
        let lv = left_arr.value(li);
        let rv = right_arr.value(ri);

        match lv.cmp(rv) {
            std::cmp::Ordering::Less => {
                if is_left_join {
                    left_indices.push(li as u32);
                    right_indices.push(None);
                }
                li += 1;
            }
            std::cmp::Ordering::Greater => {
                ri += 1;
            }
            std::cmp::Ordering::Equal => {
                let ri_start = ri;
                while ri < right_n && right_arr.value(ri) == lv {
                    ri += 1;
                }
                let li_start = li;
                while li < left_n && left_arr.value(li) == lv {
                    li += 1;
                }
                for l in li_start..li {
                    for r in ri_start..ri {
                        left_indices.push(l as u32);
                        right_indices.push(Some(r as u32));
                    }
                }
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

/// Build the final LTSeqTable from merge join index arrays.
///
/// Uses `arrow::compute::take()` to gather result columns from both tables,
/// then constructs the combined schema with aliased right columns.
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

    // Gather left columns
    for col_idx in 0..left_batch.num_columns() {
        let col = left_batch.column(col_idx);
        let taken = compute::take(col.as_ref(), left_idx, None)
            .map_err(|e| LtseqError::Runtime(format!("take() failed on left column: {}", e)))?;
        result_columns.push(taken);
    }

    // Gather right columns
    for col_idx in 0..right_batch.num_columns() {
        let col = right_batch.column(col_idx);
        let taken = compute::take(col.as_ref(), right_idx, None)
            .map_err(|e| LtseqError::Runtime(format!("take() failed on right column: {}", e)))?;
        result_columns.push(taken);
    }

    // Build result schema
    let left_schema = left_batch.schema();
    let right_schema = right_batch.schema();
    let result_schema = build_join_result_schema(&left_schema, &right_schema, alias);

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
