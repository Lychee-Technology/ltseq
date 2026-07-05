//! Sort operation for LTSeqTable
//!
//! Sorts table rows by one or more key expressions with optional descending order.

use crate::engine::RUNTIME;
use crate::error::LtseqError;
use crate::metadata::{sort_specs_to_file_sort_order, SortSpec};
use crate::transpiler::pyexpr_to_datafusion;
use crate::types::{dict_to_py_expr, PyExpr};
use crate::LTSeqTable;
use datafusion::datasource::file_format::options::ParquetReadOptions;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::SortExpr;
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
            None, // row set / columns diverge from the raw file: drop fast-path token
        ));
    }

    let (df, schema) = table.require_df_and_schema()?;

    // Capture full sort specs (column + direction) for downstream window ops
    let mut captured_specs = Vec::new();

    // Deserialize and transpile each sort expression
    let mut df_sort_exprs = Vec::new();
    for (i, expr_dict) in sort_exprs.iter().enumerate() {
        let py_expr = dict_to_py_expr(expr_dict)?;

        // Get desc flag for this sort key
        let is_desc = desc_flags.get(i).copied().unwrap_or(false);

        // Capture column name if this is a simple column reference
        if let PyExpr::Column(ref col_name) = py_expr {
            captured_specs.push(SortSpec::new(col_name.clone(), is_desc));
        }

        let df_expr = pyexpr_to_datafusion(py_expr, schema)
            .map_err(LtseqError::Validation)?;

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
        .map_err(LtseqError::Runtime)?;

    // Return new LTSeqTable with sorted data and captured sort specs
    // (schema unchanged). The physical sort defines a new row order, so the
    // raw-file fast-path token must not survive: the Parquet file's physical
    // order no longer matches the table's logical order.
    Ok(LTSeqTable::from_df_with_schema(
        Arc::clone(&table.session),
        sorted_df,
        // SAFETY: require_df_and_schema() validated schema above
        Arc::clone(table.schema.as_ref().expect("schema validated by require_df_and_schema")),
        captured_specs,
        None,
    ))
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
    desc_flags: Vec<bool>,
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

    // Capture sort specs (same logic as sort_impl, but skip the actual sort)
    let mut captured_specs = Vec::new();
    for (i, expr_dict) in sort_exprs.iter().enumerate() {
        let py_expr = dict_to_py_expr(expr_dict)?;

        if let PyExpr::Column(ref col_name) = py_expr {
            let is_desc = desc_flags.get(i).copied().unwrap_or(false);
            captured_specs.push(SortSpec::new(col_name.clone(), is_desc));
        }
    }

    // Build DataFusion sort order for the optimizer
    let sort_order = sort_specs_to_file_sort_order(&captured_specs);

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
            .map_err(LtseqError::Runtime)?;

        let result_schema = LTSeqTable::schema_from_df(result_df.schema());

        return Ok(LTSeqTable::from_df_with_schema(
            Arc::clone(&table.session),
            result_df,
            result_schema,
            captured_specs,
            table.source_parquet_path.clone(),
        ));
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
        .map_err(LtseqError::Runtime)?;

    if batches.is_empty() {
        return Ok(LTSeqTable::empty(
            Arc::clone(&table.session),
            table.schema.as_ref().map(Arc::clone),
            captured_specs,
            None,
        ));
    }

    let batch_schema = batches[0].schema();
    let mem_table =
        MemTable::try_new(Arc::clone(&batch_schema), vec![batches])
            .map_err(|e| {
                LtseqError::Runtime(format!(
                    "Failed to create MemTable: {}",
                    e
                ))
            })?
            .with_sort_order(sort_order);

    let result_df = table
        .session
        .read_table(Arc::new(mem_table))
        .map_err(|e| {
            LtseqError::Runtime(format!(
                "Failed to read sorted MemTable: {}",
                e
            ))
        })?;

    Ok(LTSeqTable::from_df_with_schema(
        Arc::clone(&table.session),
        result_df,
        batch_schema,
        captured_specs,
        None,
    ))
}
