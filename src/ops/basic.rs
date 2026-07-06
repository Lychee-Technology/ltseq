//! Basic table operations: filter, select, search_first
//!
//! This module contains helper functions for basic table operations.
//! The actual methods are implemented in the #[pymethods] impl block in lib.rs
//! due to PyO3 constraints (only one #[pymethods] impl block per struct is allowed).
//!
//! # Architecture
//!
//! Due to PyO3 constraints, all actual operation implementations are extracted as helper
//! functions here. The #[pymethods] impl block in lib.rs simply delegates to these
//! helpers via one-line stubs.
//!
//! # Operations Provided
//!
//! - **filter_impl**: Filter rows based on a predicate expression
//! - **select_impl**: Select columns or derived expressions
//! - **search_first_impl**: Find the first row matching a predicate (optimized filter + limit 1)
//! - **materialize_impl**: Execute the lazy plan and snapshot the result into memory

use crate::error::LtseqError;
use crate::transpiler::pyexpr_to_datafusion;
use crate::types::dict_to_py_expr;
use crate::LTSeqTable;
use datafusion::datasource::MemTable;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

use crate::engine::RUNTIME;

/// Helper function to filter rows based on a predicate expression
///
/// Args:
///     table: Reference to LTSeqTable
///     expr_dict: Serialized expression dict (from Python)
pub fn filter_impl(table: &LTSeqTable, expr_dict: &Bound<'_, PyDict>) -> PyResult<LTSeqTable> {
    // 1. Deserialize expression
    let py_expr = dict_to_py_expr(expr_dict)?;

    // If no dataframe, return empty result (for unit tests)
    if table.dataframe.is_none() {
        return Ok(LTSeqTable::empty(
            Arc::clone(&table.session),
            table.schema.as_ref().map(Arc::clone),
            table.sort_specs.clone(),
            None, // row set / columns diverge from the raw file: drop fast-path token
        ));
    }

    // 2. Get schema (required for transpilation)
    let schema = table.require_schema()?;

    // 3. Transpile to DataFusion expr
    let df_expr = pyexpr_to_datafusion(py_expr, schema).map_err(LtseqError::Transpile)?;

    // 4. Get DataFrame
    let df = table.require_df()?;

    // 5. Apply filter (async operation)
    let filtered_df = RUNTIME
        .block_on(async {
            (**df)
                .clone()
                .filter(df_expr)
                .map_err(|e| format!("Filter execution failed: {}", e))
        })
        .map_err(LtseqError::Runtime)?;

    // 6. Return new LTSeqTable with filtered data (schema unchanged)
    Ok(LTSeqTable::from_df_with_schema(
        Arc::clone(&table.session),
        filtered_df,
        Arc::clone(schema),
        table.sort_specs.clone(),
        None, // row set / columns diverge from the raw file: drop fast-path token
    ))
}

/// Helper function to select columns or derived expressions
///
/// Args:
///     table: Reference to LTSeqTable
///     exprs: List of serialized expression dicts (from Python)
pub fn select_impl(table: &LTSeqTable, exprs: Vec<Bound<'_, PyDict>>) -> PyResult<LTSeqTable> {
    // If no dataframe, return empty result (for unit tests)
    if table.dataframe.is_none() {
        return Ok(LTSeqTable::empty(
            Arc::clone(&table.session),
            table.schema.as_ref().map(Arc::clone),
            table.sort_specs.clone(),
            None, // row set / columns diverge from the raw file: drop fast-path token
        ));
    }

    // 1. Get schema
    let schema = table.require_schema()?;

    // 2. Deserialize and transpile all expressions
    let mut df_exprs = Vec::new();

    for expr_dict in exprs {
        // Deserialize
        let py_expr = dict_to_py_expr(&expr_dict)?;

        // Transpile
        let df_expr = pyexpr_to_datafusion(py_expr, schema).map_err(LtseqError::Transpile)?;

        df_exprs.push(df_expr);
    }

    // 3. Get DataFrame
    let df = table.require_df()?;

    // 4. Apply select (async operation)
    let selected_df = RUNTIME
        .block_on(async {
            (**df)
                .clone()
                .select(df_exprs)
                .map_err(|e| format!("Select execution failed: {}", e))
        })
        .map_err(LtseqError::Runtime)?;

    // 5. Keep the longest prefix of sort keys whose columns survived the
    // projection: a table sorted by (a, b) is still sorted by (a) after b is
    // projected away, but nothing is guaranteed past the first missing column.
    let result_schema = LTSeqTable::schema_from_df(selected_df.schema());
    let sort_specs: Vec<crate::SortSpec> = table
        .sort_specs
        .iter()
        .take_while(|spec| {
            result_schema
                .fields()
                .iter()
                .any(|f| f.name() == &spec.column)
        })
        .cloned()
        .collect();

    // 6. Return new LTSeqTable with recomputed schema
    Ok(LTSeqTable::from_df(
        Arc::clone(&table.session),
        selected_df,
        sort_specs,
        None, // row set / columns diverge from the raw file: drop fast-path token
    ))
}

/// Rename columns via an aliased projection, remapping sort metadata.
///
/// Unlike a plain `select` with aliases (which cannot know a projection is a
/// rename and therefore drops sort specs whose columns "disappear"), this
/// remaps each sort spec's column to its new name so declared order survives.
/// Execution cost is identical to the aliased select it wraps.
///
/// Args:
///     table: Reference to LTSeqTable
///     mapping: old column name -> new column name
pub fn rename_columns_impl(
    table: &LTSeqTable,
    mapping: &std::collections::HashMap<String, String>,
) -> PyResult<LTSeqTable> {
    use datafusion::logical_expr::col;

    let schema = table.require_schema()?;

    // Validate all old names exist
    for old_name in mapping.keys() {
        if !schema.fields().iter().any(|f| f.name() == old_name) {
            return Err(LtseqError::Validation(format!(
                "Column '{}' not found in schema",
                old_name
            ))
            .into());
        }
    }

    // Build aliased projection preserving column order
    let df_exprs: Vec<datafusion::logical_expr::Expr> = schema
        .fields()
        .iter()
        .map(|f| {
            let name = f.name();
            match mapping.get(name) {
                Some(new_name) => col(format!("\"{}\"", name)).alias(new_name),
                None => col(format!("\"{}\"", name)),
            }
        })
        .collect();

    let df = table.require_df()?;
    let renamed_df = RUNTIME
        .block_on(async {
            (**df)
                .clone()
                .select(df_exprs)
                .map_err(|e| format!("Rename execution failed: {}", e))
        })
        .map_err(LtseqError::Runtime)?;

    // Remap sort metadata to the new names (a rename preserves row order)
    let sort_specs: Vec<crate::SortSpec> = table
        .sort_specs
        .iter()
        .map(|spec| {
            let mut spec = spec.clone();
            if let Some(new_name) = mapping.get(&spec.column) {
                spec.column = new_name.clone();
            }
            spec
        })
        .collect();

    Ok(LTSeqTable::from_df(
        Arc::clone(&table.session),
        renamed_df,
        sort_specs,
        None, // columns diverge from the raw file: drop fast-path token
    ))
}

/// Helper function to find the first row matching a predicate
///
/// This is an optimized version of filter() that returns only the first matching row.
/// Much faster than filtering and then taking the first row, especially for large tables.
///
/// Args:
///     table: Reference to LTSeqTable
///     expr_dict: Serialized filter expression
pub fn search_first_impl(
    table: &LTSeqTable,
    expr_dict: &Bound<'_, PyDict>,
) -> PyResult<LTSeqTable> {
    let (df, schema) = table.require_df_and_schema()?;

    // Deserialize the expression
    let py_expr = dict_to_py_expr(expr_dict)?;

    // Transpile to DataFusion Expr
    let df_expr = pyexpr_to_datafusion(py_expr, schema)
        .map_err(LtseqError::Validation)?;

    // Apply filter and limit to first result
    let result_df = RUNTIME
        .block_on(async {
            (**df)
                .clone()
                .filter(df_expr)
                .map_err(|e| format!("Filter failed: {}", e))?
                .limit(0, Some(1))
                .map_err(|e| format!("Limit failed: {}", e))
        })
        .map_err(LtseqError::Runtime)?;

    // Return new LTSeqTable with recomputed schema
    Ok(LTSeqTable::from_df(
        Arc::clone(&table.session),
        result_df,
        table.sort_specs.clone(),
        None, // row set / columns diverge from the raw file: drop fast-path token
    ))
}

/// Helper function to materialize the lazy plan into an in-memory table
///
/// Executes the pending DataFusion plan once and snapshots all batches into a
/// single-partition MemTable-backed DataFrame, so downstream operations reuse
/// the computed data instead of re-executing the plan. Rows, schema, and row
/// order are unchanged, so sort metadata is carried over.
///
/// Args:
///     table: Reference to LTSeqTable
pub fn materialize_impl(table: &LTSeqTable) -> PyResult<LTSeqTable> {
    // No dataframe loaded (fresh/empty table): materialization is a no-op
    if table.dataframe.is_none() {
        return Ok(LTSeqTable::empty(
            Arc::clone(&table.session),
            table.schema.as_ref().map(Arc::clone),
            table.sort_specs.clone(),
            None,
        ));
    }

    let df = table.require_df()?;
    let batches = RUNTIME
        .block_on(async {
            (**df)
                .clone()
                .collect()
                .await
                .map_err(|e| format!("collect() materialization failed: {}", e))
        })
        .map_err(LtseqError::Runtime)?;

    // The data now lives in memory: drop source_parquet_path so downstream ops
    // scan the snapshot instead of re-reading the original file.
    match table.schema.as_ref() {
        // A 0-row plan must still materialize to a scannable (empty) table,
        // not a schema-only shell, so count/filter keep working downstream.
        Some(schema) if batches.is_empty() => {
            let mem_table = MemTable::try_new(Arc::clone(schema), vec![vec![]])
                .map_err(|e| LtseqError::with_context("Failed to create table", e))?;
            let empty_df = table
                .session
                .read_table(Arc::new(mem_table))
                .map_err(|e| LtseqError::with_context("Failed to read table", e))?;
            Ok(LTSeqTable::from_df_with_schema(
                Arc::clone(&table.session),
                empty_df,
                Arc::clone(schema),
                table.sort_specs.clone(),
                None,
            ))
        }
        Some(schema) => LTSeqTable::from_batches_with_schema(
            Arc::clone(&table.session),
            batches,
            Arc::clone(schema),
            table.sort_specs.clone(),
            None,
        ),
        None => LTSeqTable::from_batches(
            Arc::clone(&table.session),
            batches,
            table.sort_specs.clone(),
            None,
        ),
    }
}
