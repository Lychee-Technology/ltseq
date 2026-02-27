//! Grouping operations for LTSeqTable
//!
//! Provides consecutive group identification and first/last row selection.
//!
//! # Operations
//!
//! ## group_id_impl
//!
//! Adds `__group_id__` column to identify consecutive groups with identical values.
//! Uses SQL window functions: ROW_NUMBER() for position, CASE WHEN for change detection,
//! SUM() for cumulative group IDs.
//!
//! ## first_row_impl / last_row_impl
//!
//! Filters table to only first/last row per group. Requires `__group_id__` column
//! from prior flatten() call. Uses ROW_NUMBER() OVER (PARTITION BY __group_id__).

use crate::engine::RUNTIME;
use crate::types::{dict_to_py_expr, PyExpr};
use crate::LTSeqTable;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

// ============================================================================
// Helper Functions
// ============================================================================

/// Get validated dataframe and schema, or return empty table
fn get_df_and_schema_or_empty(
    table: &LTSeqTable,
) -> Result<(&Arc<datafusion::dataframe::DataFrame>, &Arc<ArrowSchema>), LTSeqTable> {
    if table.dataframe.is_none() {
        return Err(LTSeqTable::empty(
            Arc::clone(&table.session),
            table.schema.as_ref().map(Arc::clone),
            Vec::new(),
        ));
    }
    let df = table.dataframe.as_ref().unwrap();
    let schema = table.schema.as_ref().ok_or_else(|| {
        LTSeqTable::empty(
            Arc::clone(&table.session),
            None,
            Vec::new(),
        )
    })?;
    Ok((df, schema))
}

/// Collect DataFrame to batches and get schema
fn collect_and_get_schema(
    df: &datafusion::dataframe::DataFrame,
    stored_schema: &Arc<ArrowSchema>,
) -> PyResult<(Vec<RecordBatch>, Arc<ArrowSchema>)> {
    let batches = RUNTIME
        .block_on(async {
            df.clone()
                .collect()
                .await
                .map_err(|e| format!("Failed to collect data: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    let batch_schema = batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new((**stored_schema).clone()));

    Ok((batches, batch_schema))
}

/// Register a temp table and return its name
fn register_temp_table(
    session: &SessionContext,
    schema: &Arc<ArrowSchema>,
    batches: Vec<RecordBatch>,
    prefix: &str,
) -> PyResult<String> {
    let table_name = format!("__ltseq_{}_temp_{}", prefix, std::process::id());
    let _ = session.deregister_table(&table_name);

    let temp_table = MemTable::try_new(Arc::clone(schema), vec![batches]).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to create memory table: {}",
            e
        ))
    })?;

    session
        .register_table(&table_name, Arc::new(temp_table))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to register temp table: {}",
                e
            ))
        })?;

    Ok(table_name)
}

/// Execute SQL query and collect results
fn execute_sql_query(
    session: &SessionContext,
    sql: &str,
    op_name: &str,
) -> PyResult<Vec<RecordBatch>> {
    RUNTIME
        .block_on(async {
            let result_df = session
                .sql(sql)
                .await
                .map_err(|e| format!("Failed to execute {} query: {}", op_name, e))?;

            result_df
                .collect()
                .await
                .map_err(|e| format!("Failed to collect {} results: {}", op_name, e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))
}

/// Create result LTSeqTable from batches
fn create_result_from_batches(
    session: &Arc<SessionContext>,
    result_batches: Vec<RecordBatch>,
    fallback_schema: Option<&Arc<ArrowSchema>>,
    _prefix: &str,
) -> PyResult<LTSeqTable> {
    match fallback_schema {
        Some(schema) => LTSeqTable::from_batches_with_schema(
            Arc::clone(session),
            result_batches,
            Arc::clone(schema),
            Vec::new(),
        ),
        None => LTSeqTable::from_batches(
            Arc::clone(session),
            result_batches,
            Vec::new(),
        ),
    }
}

/// Build column list string from schema, optionally filtering internal columns
fn build_column_list(schema: &ArrowSchema, filter_internal: bool) -> String {
    schema
        .fields()
        .iter()
        .filter(|f| {
            if filter_internal {
                let name = f.name();
                !name.starts_with("__rn")
                    && !name.starts_with("__row_num")
                    && !name.starts_with("__mask")
                    && !name.starts_with("__cnt")
            } else {
                true
            }
        })
        .map(|f| format!("\"{}\"", f.name()))
        .collect::<Vec<_>>()
        .join(", ")
}

// ============================================================================
// Public API Functions
// ============================================================================

/// Add __group_id__ column to identify consecutive identical grouping values
pub fn group_id_impl(table: &LTSeqTable, grouping_expr: Bound<'_, PyDict>) -> PyResult<LTSeqTable> {
    let (df, schema) = match get_df_and_schema_or_empty(table) {
        Ok(v) => v,
        Err(empty_table) => return Ok(empty_table),
    };

    // Deserialize grouping expression and extract column name
    let py_expr = dict_to_py_expr(&grouping_expr)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    let grouping_col_name = if let PyExpr::Column(ref col_name) = py_expr {
        col_name.clone()
    } else {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "group_ordered currently only supports simple column references",
        ));
    };

    // Collect data and register temp table
    let (batches, batch_schema) = collect_and_get_schema(df, schema)?;
    let temp_table_name = register_temp_table(&table.session, &batch_schema, batches, "group_id")?;
    let columns_str = build_column_list(&batch_schema, false);

    // Build and execute SQL query
    let sql_query = format!(
        r#"WITH numbered AS (
          SELECT {cols}, ROW_NUMBER() OVER () as __row_num FROM "{table}"
        ),
        masked AS (
          SELECT {cols}, __row_num, 
            CASE 
              WHEN __row_num = 1 THEN 1
              WHEN "{groupcol}" IS DISTINCT FROM LAG("{groupcol}") OVER (ORDER BY __row_num) THEN 1
              ELSE 0
            END as __mask
          FROM numbered
        ),
        grouped AS (
          SELECT {cols}, __row_num,
            SUM(__mask) OVER (ORDER BY __row_num ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as __group_id__
          FROM masked
        )
        SELECT {cols}, __group_id__,
          COUNT(*) OVER (PARTITION BY __group_id__) as __group_count__,
          ROW_NUMBER() OVER (PARTITION BY __group_id__ ORDER BY __row_num) as __rn__
        FROM grouped"#,
        cols = columns_str,
        table = temp_table_name,
        groupcol = grouping_col_name
    );

    let result_batches = execute_sql_query(&table.session, &sql_query, "group_id")?;

    // Cleanup and return result
    let _ = table.session.deregister_table(&temp_table_name);
    create_result_from_batches(
        &table.session,
        result_batches,
        table.schema.as_ref(),
        "group_id",
    )
}

/// Get only the first row of each group
pub fn first_row_impl(table: &LTSeqTable) -> PyResult<LTSeqTable> {
    first_or_last_row_impl(table, true)
}

/// Get only the last row of each group
pub fn last_row_impl(table: &LTSeqTable) -> PyResult<LTSeqTable> {
    first_or_last_row_impl(table, false)
}

/// Shared implementation for first_row and last_row
fn first_or_last_row_impl(table: &LTSeqTable, is_first: bool) -> PyResult<LTSeqTable> {
    let (df, schema) = match get_df_and_schema_or_empty(table) {
        Ok(v) => v,
        Err(empty_table) => return Ok(empty_table),
    };

    // Verify __group_id__ column exists
    if !schema.fields().iter().any(|f| f.name() == "__group_id__") {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "__group_id__ column not found. Call flatten() first.",
        ));
    }

    // Collect data and register temp table
    let (batches, batch_schema) = collect_and_get_schema(df, schema)?;
    let op_name = if is_first { "first_row" } else { "last_row" };
    let temp_table_name = register_temp_table(&table.session, &batch_schema, batches, op_name)?;
    let columns_str = build_column_list(&batch_schema, true);

    // Build SQL query
    let rn_alias = format!("__rn_{}_{}", op_name, std::process::id());
    let sql_query = if is_first {
        format!(
            r#"WITH ranked AS (
              SELECT {cols}, ROW_NUMBER() OVER (PARTITION BY "__group_id__") as "{rn}" FROM "{table}"
            )
            SELECT {cols} FROM ranked WHERE "{rn}" = 1"#,
            cols = columns_str,
            table = temp_table_name,
            rn = rn_alias
        )
    } else {
        let cnt_alias = format!("__cnt_{}_{}", op_name, std::process::id());
        format!(
            r#"WITH ranked AS (
              SELECT {cols}, 
                      ROW_NUMBER() OVER (PARTITION BY "__group_id__") as "{rn}",
                      COUNT(*) OVER (PARTITION BY "__group_id__") as "{cnt}"
              FROM "{table}"
            )
            SELECT {cols} FROM ranked WHERE "{rn}" = "{cnt}""#,
            cols = columns_str,
            table = temp_table_name,
            rn = rn_alias,
            cnt = cnt_alias
        )
    };

    let result_batches = execute_sql_query(&table.session, &sql_query, op_name)?;

    // Cleanup and return result
    let _ = table.session.deregister_table(&temp_table_name);
    create_result_from_batches(
        &table.session,
        result_batches,
        table.schema.as_ref(),
        op_name,
    )
}
