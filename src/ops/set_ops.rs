//! Set operations: distinct, union, intersect, diff, is_subset
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
//! - **intersect_impl**: Return rows present in both tables (INTERSECT)
//! - **diff_impl**: Return rows in first table but not in second (EXCEPT)
//! - **is_subset_impl**: Check if first table is a subset of second
//!
//! # Implementation Pattern
//!
//! All operations return PyResult<T> for seamless Python exception handling.
//! Set operations use SQL-based implementations via DataFusion for correctness
//! and performance.

use crate::engine::RUNTIME;
use crate::LTSeqTable;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
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
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "No columns specified for comparison",
        ));
    }
    Ok(compare_cols)
}

/// Collect DataFrame to RecordBatches
async fn collect_batches(
    df: &datafusion::dataframe::DataFrame,
    table_name: &str,
) -> Result<Vec<RecordBatch>, String> {
    df.clone()
        .collect()
        .await
        .map_err(|e| format!("Failed to collect {}: {}", table_name, e))
}

/// Register two temp tables for set operations
fn register_temp_tables(
    session: &SessionContext,
    schema1: &Arc<ArrowSchema>,
    batches1: Vec<RecordBatch>,
    t1_name: &str,
    schema2: &Arc<ArrowSchema>,
    batches2: Vec<RecordBatch>,
    t2_name: &str,
) -> Result<(), String> {
    let temp1 = MemTable::try_new(Arc::clone(schema1), vec![batches1])
        .map_err(|e| format!("Failed to create temp table 1: {}", e))?;
    let temp2 = MemTable::try_new(Arc::clone(schema2), vec![batches2])
        .map_err(|e| format!("Failed to create temp table 2: {}", e))?;

    session
        .register_table(t1_name, Arc::new(temp1))
        .map_err(|e| format!("Failed to register temp table 1: {}", e))?;
    session
        .register_table(t2_name, Arc::new(temp2))
        .map_err(|e| format!("Failed to register temp table 2: {}", e))?;
    Ok(())
}

/// Deregister temp tables (ignores errors)
fn deregister_temp_tables(session: &SessionContext, t1_name: &str, t2_name: &str) {
    let _ = session.deregister_table(t1_name);
    let _ = session.deregister_table(t2_name);
}

/// Build SELECT column list for t1
fn build_select_cols(schema: &ArrowSchema) -> String {
    schema
        .fields()
        .iter()
        .map(|f| format!("t1.\"{}\"", f.name()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Build WHERE condition for column comparison
fn build_where_conditions(compare_cols: &[String]) -> String {
    compare_cols
        .iter()
        .map(|c| format!("t1.\"{}\" = t2.\"{}\"", c, c))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// Create result LTSeqTable from DataFrame
fn create_result_table(
    session: &Arc<SessionContext>,
    result_df: datafusion::dataframe::DataFrame,
    sort_exprs: &[String],
) -> LTSeqTable {
    LTSeqTable::from_df(
        Arc::clone(session),
        result_df,
        sort_exprs.to_vec(),
        None,
    )
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
            table.sort_exprs.clone(),
            table.source_parquet_path.clone(),
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
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    Ok(LTSeqTable::from_df_with_schema(
        Arc::clone(&table.session),
        distinct_df,
        Arc::clone(table.schema.as_ref().unwrap()),
        table.sort_exprs.clone(),
        table.source_parquet_path.clone(),
    ))
}

/// Extract column names from key expressions
fn extract_key_cols_from_exprs(
    key_exprs: &[Bound<'_, PyDict>],
    schema: &ArrowSchema,
) -> PyResult<Vec<String>> {
    let mut key_cols = Vec::new();
    for expr_dict in key_exprs.iter() {
        let py_expr = crate::types::dict_to_py_expr(expr_dict)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        let col_name = match &py_expr {
            crate::types::PyExpr::Column(name) => name.clone(),
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "distinct() key expressions must be simple column references",
                ))
            }
        };

        if !schema.fields().iter().any(|f| f.name() == &col_name) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Column '{}' not found in schema",
                col_name
            )));
        }
        key_cols.push(col_name);
    }
    Ok(key_cols)
}

/// Distinct with specific key columns using ROW_NUMBER()
fn distinct_with_keys(
    table: &LTSeqTable,
    df: &Arc<datafusion::dataframe::DataFrame>,
    schema: &Arc<ArrowSchema>,
    key_cols: &[String],
) -> PyResult<LTSeqTable> {
    let all_cols_str = schema
        .fields()
        .iter()
        .map(|f| format!("\"{}\"", f.name()))
        .collect::<Vec<_>>()
        .join(", ");

    let partition_by = key_cols
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "SELECT {} FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {} ORDER BY {}) as __rn FROM __distinct_source) WHERE __rn = 1",
        all_cols_str, partition_by, partition_by
    );

    let schema_clone = Arc::clone(schema);
    let distinct_df = RUNTIME
        .block_on(async {
            let batches = collect_batches(df, "data for distinct").await?;
            let temp_table = MemTable::try_new(schema_clone, vec![batches])
                .map_err(|e| format!("Failed to create temp table: {}", e))?;

            table
                .session
                .register_table("__distinct_source", Arc::new(temp_table))
                .map_err(|e| format!("Failed to register source table: {}", e))?;

            let result = table
                .session
                .sql(&sql)
                .await
                .map_err(|e| format!("Distinct SQL failed: {}", e))?;

            let _ = table.session.deregister_table("__distinct_source");
            Ok(result)
        })
        .map_err(|e: String| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    Ok(LTSeqTable::from_df_with_schema(
        Arc::clone(&table.session),
        distinct_df,
        Arc::clone(table.schema.as_ref().unwrap()),
        table.sort_exprs.clone(),
        table.source_parquet_path.clone(),
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
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Schema mismatch: {:?} vs {:?}",
            cols1, cols2
        )));
    }

    let union_df = RUNTIME
        .block_on(async {
            (**df1)
                .clone()
                .union((**df2).clone())
                .map_err(|e| format!("Union failed: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    Ok(create_result_table(
        &table1.session,
        union_df,
        &table1.sort_exprs,
    ))
}

/// Intersect: Return rows present in both tables
///
/// Returns rows that appear in both tables based on key columns.
/// If no key expression provided, uses all columns.
pub fn intersect_impl(
    table1: &LTSeqTable,
    table2: &LTSeqTable,
    key_expr_dict: Option<Bound<'_, PyDict>>,
) -> PyResult<LTSeqTable> {
    set_operation_impl(table1, table2, key_expr_dict, SetOperation::Intersect)
}

/// Diff: Return rows in first table but not in second
///
/// Returns rows from the left table that don't appear in the right table.
/// If no key expression provided, uses all columns.
pub fn diff_impl(
    table1: &LTSeqTable,
    table2: &LTSeqTable,
    key_expr_dict: Option<Bound<'_, PyDict>>,
) -> PyResult<LTSeqTable> {
    set_operation_impl(table1, table2, key_expr_dict, SetOperation::Diff)
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
    let (df1, schema1) = table1.require_df_and_schema()?;
    let (df2, schema2) = table2.require_df_and_schema()?;
    let compare_cols = get_compare_columns(key_expr_dict.as_ref(), schema1)?;

    // t1 is a subset of t2 if diff(t1, t2) is empty (count rows not in t2 = 0)
    let is_subset = RUNTIME
        .block_on(async {
            let batches1 = collect_batches(df1, "left table").await?;
            let batches2 = collect_batches(df2, "right table").await?;

            let t1_name = "__subset_t1__";
            let t2_name = "__subset_t2__";
            register_temp_tables(&table1.session, schema1, batches1, t1_name, schema2, batches2, t2_name)?;

            let where_cond = build_where_conditions(&compare_cols);
            let sql = format!(
                "SELECT COUNT(*) as cnt FROM \"{}\" t1 WHERE NOT EXISTS (SELECT 1 FROM \"{}\" t2 WHERE {})",
                t1_name, t2_name, where_cond
            );

            let result = table1.session.sql(&sql).await
                .map_err(|e| format!("Subset query failed: {}", e))?;
            let batches = result.collect().await
                .map_err(|e| format!("Subset collect failed: {}", e))?;

            deregister_temp_tables(&table1.session, t1_name, t2_name);

            // Empty table is subset of anything
            if batches.is_empty() || batches[0].num_rows() == 0 {
                return Ok::<_, String>(true);
            }

            use datafusion::arrow::array::Int64Array;
            let cnt_array = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "Failed to extract count".to_string())?;

            Ok(cnt_array.value(0) == 0)
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    Ok(is_subset)
}

// ============================================================================
// Internal Helper for Set Operations
// ============================================================================

/// Type of set operation
enum SetOperation {
    Intersect, // WHERE EXISTS
    Diff,      // WHERE NOT EXISTS
}

/// Common implementation for intersect and diff operations
fn set_operation_impl(
    table1: &LTSeqTable,
    table2: &LTSeqTable,
    key_expr_dict: Option<Bound<'_, PyDict>>,
    op: SetOperation,
) -> PyResult<LTSeqTable> {
    let (df1, schema1) = table1.require_df_and_schema()?;
    let (df2, schema2) = table2.require_df_and_schema()?;
    let compare_cols = get_compare_columns(key_expr_dict.as_ref(), schema1)?;

    let (t1_name, t2_name, op_name) = match op {
        SetOperation::Intersect => ("__intersect_t1__", "__intersect_t2__", "Intersect"),
        SetOperation::Diff => ("__diff_t1__", "__diff_t2__", "Diff"),
    };

    let result_df = RUNTIME
        .block_on(async {
            let batches1 = collect_batches(df1, "left table").await?;
            let batches2 = collect_batches(df2, "right table").await?;

            register_temp_tables(&table1.session, schema1, batches1, t1_name, schema2, batches2, t2_name)?;

            let select_cols = build_select_cols(schema1);
            let where_cond = build_where_conditions(&compare_cols);

            let sql = match op {
                SetOperation::Intersect => format!(
                    "SELECT DISTINCT {} FROM \"{}\" t1 WHERE EXISTS (SELECT 1 FROM \"{}\" t2 WHERE {})",
                    select_cols, t1_name, t2_name, where_cond
                ),
                SetOperation::Diff => format!(
                    "SELECT {} FROM \"{}\" t1 WHERE NOT EXISTS (SELECT 1 FROM \"{}\" t2 WHERE {})",
                    select_cols, t1_name, t2_name, where_cond
                ),
            };

            let result = table1.session.sql(&sql).await
                .map_err(|e| format!("{} query failed: {}", op_name, e))?;

            deregister_temp_tables(&table1.session, t1_name, t2_name);
            Ok::<_, String>(result)
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    Ok(create_result_table(
        &table1.session,
        result_df,
        &table1.sort_exprs,
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

    let py_expr = dict_to_py_expr(expr_dict)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    let mut columns = Vec::new();

    // Extract column name(s) from the expression
    match &py_expr {
        PyExpr::Column(name) => columns.push(name.clone()),
        _ => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "on= argument must be a single column reference (e.g., lambda r: r.id). \
                 For multiple columns, leave on=None to use all columns.",
            ));
        }
    }

    // Validate columns exist in schema
    for col in &columns {
        if schema.field_with_name(col).is_err() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Column '{}' not found in schema. Available columns: {:?}",
                col,
                schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
            )));
        }
    }

    Ok(columns)
}
