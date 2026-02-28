//! Sort operation for LTSeqTable
//!
//! Sorts table rows by one or more key expressions with optional descending order.

use crate::engine::RUNTIME;
use crate::transpiler::pyexpr_to_datafusion;
use crate::types::{dict_to_py_expr, PyExpr};
use crate::LTSeqTable;
use datafusion::common::Column;
use datafusion::datasource::file_format::options::ParquetReadOptions;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{Expr, SortExpr};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

/// Helper function to sort rows by one or more key expressions
///
/// Sorts the table in ascending order by default. Captures the sort keys
/// for use in subsequent window function operations.
pub fn sort_impl(
    table: &LTSeqTable,
    sort_exprs: Vec<Bound<'_, PyDict>>,
    desc_flags: Vec<bool>,
) -> PyResult<LTSeqTable> {
    // If no dataframe, return empty result (for unit tests)
    if table.dataframe.is_none() {
        return Ok(LTSeqTable::empty(
            Arc::clone(&table.session),
            table.schema.as_ref().map(Arc::clone),
            Vec::new(),
            table.source_parquet_path.clone(),
        ));
    }

    let (df, schema) = table.require_df_and_schema()?;

    // Capture sort column names for Phase 6 window functions
    let mut captured_sort_keys = Vec::new();

    // Deserialize and transpile each sort expression
    let mut df_sort_exprs = Vec::new();
    for (i, expr_dict) in sort_exprs.iter().enumerate() {
        let py_expr = dict_to_py_expr(expr_dict)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        // Capture column name if this is a simple column reference
        if let PyExpr::Column(ref col_name) = py_expr {
            captured_sort_keys.push(col_name.clone());
        }

        let df_expr = pyexpr_to_datafusion(py_expr, schema)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;

        // Get desc flag for this sort key
        let is_desc = desc_flags.get(i).copied().unwrap_or(false);

        // Create SortExpr with asc = !is_desc
        // For descending, nulls_first = true by default
        df_sort_exprs.push(SortExpr {
            expr: df_expr,
            asc: !is_desc,
            nulls_first: is_desc,
        });
    }

    // Apply sort (async operation)
    let sorted_df = RUNTIME
        .block_on(async {
            (**df)
                .clone()
                .sort(df_sort_exprs)
                .map_err(|e| format!("Sort execution failed: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Return new LTSeqTable with sorted data and captured sort keys (schema unchanged)
    Ok(LTSeqTable::from_df_with_schema(
        Arc::clone(&table.session),
        sorted_df,
        Arc::clone(table.schema.as_ref().unwrap()),
        captured_sort_keys,
        table.source_parquet_path.clone(),
    ))
}

/// Build DataFusion `Vec<Vec<SortExpr>>` from column name strings.
///
/// Each column is assumed ascending with nulls_first = true, matching
/// `build_sort_exprs` in linear_scan.rs.
fn build_df_sort_order(sort_keys: &[String]) -> Vec<Vec<SortExpr>> {
    vec![sort_keys
        .iter()
        .map(|col_name| SortExpr {
            expr: Expr::Column(Column::new_unqualified(col_name)),
            asc: true,
            nulls_first: true,
        })
        .collect()]
}

/// Declare sort order metadata without physically sorting the data.
///
/// Use this when the data is already sorted (e.g., pre-sorted Parquet files).
/// Unlike the old implementation that only set metadata, this function now
/// actually communicates the sort order to DataFusion's optimizer so that
/// downstream `.sort()` calls can be elided (no redundant SortExec).
///
/// **Parquet path (lazy):** If the table was loaded from a Parquet file,
/// re-registers the source with `ParquetReadOptions::file_sort_order` set.
/// This flows through `ListingOptions` → `ListingTable` →
/// `EquivalenceProperties`, enabling the `enforce_sorting` optimizer pass
/// to skip the sort.
///
/// **Non-Parquet fallback (materializing):** Collects the DataFrame into
/// batches and creates a `MemTable` with `with_sort_order()`.
///
/// # Safety
/// The caller is responsible for ensuring the data is actually sorted in the
/// declared order. Incorrect metadata will produce wrong results in window
/// functions and shift operations.
pub fn assume_sorted_impl(
    table: &LTSeqTable,
    sort_exprs: Vec<Bound<'_, PyDict>>,
) -> PyResult<LTSeqTable> {
    // If no dataframe, return empty result
    if table.dataframe.is_none() {
        return Ok(LTSeqTable::empty(
            Arc::clone(&table.session),
            table.schema.as_ref().map(Arc::clone),
            Vec::new(),
            table.source_parquet_path.clone(),
        ));
    }

    let (df, _schema) = table.require_df_and_schema()?;

    // Capture sort column names (same logic as sort_impl, but skip the actual sort)
    let mut captured_sort_keys = Vec::new();
    for expr_dict in sort_exprs.iter() {
        let py_expr = dict_to_py_expr(expr_dict)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        if let PyExpr::Column(ref col_name) = py_expr {
            captured_sort_keys.push(col_name.clone());
        }
    }

    // Build DataFusion sort order for the optimizer
    let sort_order = build_df_sort_order(&captured_sort_keys);

    // Try Parquet re-read path first (fully lazy, no materialization)
    if let Some(ref path) = table.source_parquet_path {
        let parquet_path = path.clone();
        let session = Arc::clone(&table.session);
        let result_df = RUNTIME
            .block_on(async {
                let options =
                    ParquetReadOptions::default().file_sort_order(sort_order.clone());
                session
                    .read_parquet(&parquet_path, options)
                    .await
                    .map_err(|e| format!("Failed to re-read Parquet with sort order: {}", e))
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

        let result_schema = LTSeqTable::schema_from_df(result_df.schema());

        return Ok(LTSeqTable {
            session: Arc::clone(&table.session),
            dataframe: Some(Arc::new(result_df)),
            schema: Some(result_schema),
            sort_exprs: captured_sort_keys,
            source_parquet_path: table.source_parquet_path.clone(),
        });
    }

    // Non-Parquet fallback: collect batches and create MemTable with sort order
    let batches = RUNTIME
        .block_on(async {
            (**df)
                .clone()
                .collect()
                .await
                .map_err(|e| format!("Failed to collect data for assume_sorted: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    if batches.is_empty() {
        return Ok(LTSeqTable::empty(
            Arc::clone(&table.session),
            table.schema.as_ref().map(Arc::clone),
            captured_sort_keys,
            None,
        ));
    }

    let batch_schema = batches[0].schema();
    let mem_table =
        MemTable::try_new(Arc::clone(&batch_schema), vec![batches])
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to create MemTable: {}",
                    e
                ))
            })?
            .with_sort_order(sort_order);

    let result_df = table
        .session
        .read_table(Arc::new(mem_table))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to read sorted MemTable: {}",
                e
            ))
        })?;

    Ok(LTSeqTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(result_df)),
        schema: Some(batch_schema),
        sort_exprs: captured_sort_keys,
        source_parquet_path: None,
    })
}
