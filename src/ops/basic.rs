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
use crate::transpiler::{pyexpr_to_datafusion, pyexpr_to_named_datafusion};
use crate::types::{dict_to_py_expr, PyExpr};
use crate::LTSeqTable;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

/// Helper function to filter rows based on a predicate expression
///
/// Args:
///     table: Reference to LTSeqTable
///     expr_dict: Serialized expression dict (from Python)
pub fn filter_impl(table: &LTSeqTable, expr_dict: &Bound<'_, PyDict>) -> PyResult<LTSeqTable> {
    // 1. Deserialize expression
    let py_expr = dict_to_py_expr(expr_dict)?;

    // A never-loaded table yields another never-loaded table
    if table.dataframe.is_none() {
        return Ok(LTSeqTable::unloaded(Arc::clone(&table.session)));
    }

    // 2. Get schema (required for transpilation)
    let schema = table.require_schema()?;

    // 3. Transpile to DataFusion expr
    let df_expr = pyexpr_to_datafusion(py_expr, schema).map_err(LtseqError::Transpile)?;

    // 4. Get DataFrame
    let df = table.require_df()?;

    // 5. Apply filter (plan building only; nothing executes here)
    let filtered_df = (**df)
        .clone()
        .filter(df_expr)
        .map_err(|e| LtseqError::Runtime(format!("Filter execution failed: {}", e)))?;

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
    // A never-loaded table yields another never-loaded table
    if table.dataframe.is_none() {
        return Ok(LTSeqTable::unloaded(Arc::clone(&table.session)));
    }

    // 1. Get schema
    let schema = table.require_schema()?;

    // 2. Deserialize and transpile all expressions, noting the columns that
    // pass through unchanged
    let mut df_exprs = Vec::new();
    let mut passed = std::collections::HashSet::new();

    for expr_dict in exprs {
        // Deserialize
        let py_expr = dict_to_py_expr(&expr_dict)?;
        if let PyExpr::Column(name) = &py_expr {
            passed.insert(name.clone());
        }

        // Transpile
        let df_expr = pyexpr_to_named_datafusion(py_expr, schema).map_err(LtseqError::Transpile)?;

        df_exprs.push(df_expr);
    }

    // 3. Keep the longest prefix of sort keys that pass through: a table
    // sorted by (a, b) is still sorted by (a) after b is projected away, but
    // nothing is declared past the first key that is dropped. A computed
    // column can carry a key's name (a cast keeps its input's), so a key
    // survives only as a column reference.
    let sort_specs: Vec<crate::SortSpec> = table
        .sort_specs
        .iter()
        .take_while(|spec| passed.contains(&spec.column))
        .cloned()
        .collect();

    // 4. Apply select (plan building only; nothing executes here), keeping
    // the input's full order in this result when it drops a key
    let df = table.require_df()?;
    let selected_df = crate::ops::common::select_in_order(
        (**df).clone(),
        df_exprs,
        &table.sort_specs,
        sort_specs.len(),
    )
    .map_err(|e| LtseqError::Runtime(format!("Select execution failed: {}", e)))?;

    // 5. Return new LTSeqTable with recomputed schema
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
    let renamed_df = (**df)
        .clone()
        .select(df_exprs)
        .map_err(|e| LtseqError::Runtime(format!("Rename execution failed: {}", e)))?;

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
    let schema = table.require_schema()?;

    // Deserialize the expression
    let py_expr = dict_to_py_expr(expr_dict)?;

    // Transpile to DataFusion Expr
    let df_expr = pyexpr_to_datafusion(py_expr, schema)
        .map_err(LtseqError::Validation)?;

    // Apply filter and limit to first result (plan building only). "First"
    // is in table order, so the limit goes above the order; DataFusion moves
    // the filter beneath it.
    let result_df = table
        .ordered_df()?
        .filter(df_expr)
        .map_err(|e| LtseqError::Runtime(format!("Filter failed: {}", e)))?
        .limit(0, Some(1))
        .map_err(|e| LtseqError::Runtime(format!("Limit failed: {}", e)))?;

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
/// Executes the pending DataFusion plan once, in table order, and snapshots
/// all batches into one in-order partition (`Collected`), so downstream
/// operations reuse the computed data instead of re-executing the plan. Rows,
/// schema, and row order are unchanged, so sort metadata is carried over.
///
/// Runs with the GIL released (`lib.rs::materialize` wraps it in
/// `gil::detached`), so it takes no Python objects and returns `LtseqError`.
///
/// Args:
///     table: Reference to LTSeqTable
pub fn materialize_impl(table: &LTSeqTable) -> Result<LTSeqTable, LtseqError> {
    // A never-loaded table has nothing to materialize
    if table.dataframe.is_none() {
        return Ok(LTSeqTable::unloaded(Arc::clone(&table.session)));
    }

    let schema = table.require_schema()?;
    let batches = crate::engine::RUNTIME
        .block_on(table.ordered_df()?.collect())
        .map_err(|e| LtseqError::Runtime(format!("collect() materialization failed: {}", e)))?;

    // The data now lives in memory, so the snapshot drops source_parquet_path
    // and downstream ops scan it instead of re-reading the file. A 0-row plan
    // still materializes to a scannable zero-row table.
    let schema = batches
        .first()
        .map_or_else(|| Arc::clone(schema), |b| b.schema());
    let sort_order = crate::metadata::sort_specs_to_df_sort_exprs(&table.sort_specs);
    let snapshot = StreamingTable::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Collected {
            schema: Arc::clone(&schema),
            batches: batches.into(),
        })],
    )
    .map_err(|e| LtseqError::with_context("Failed to create table", e))?
    .with_sort_order(sort_order);
    let df = table
        .session
        .read_table(Arc::new(snapshot))
        .map_err(|e| LtseqError::with_context("Failed to read table", e))?;
    Ok(LTSeqTable::from_df_with_schema(
        Arc::clone(&table.session),
        df,
        schema,
        table.sort_specs.clone(),
        None,
    ))
}

/// The rows `collect()` snapshot, read back as one partition in the order
/// they were collected.
///
/// DataFusion splits a `MemTable` into partitions once it has as many
/// batches as the session has target partitions, and the partitions come
/// back in any order. A table with declared keys would get its order back
/// from the root sort, but a table whose keys a projection dropped has
/// none to sort by (#311). A streaming source is never split, and its
/// declared order spares the root sort.
#[derive(Debug)]
struct Collected {
    schema: SchemaRef,
    batches: Arc<[RecordBatch]>,
}

impl PartitionStream for Collected {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        let batches = Arc::clone(&self.batches);
        let rows = (0..batches.len()).map(move |i| Ok(batches[i].clone()));
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures_util::stream::iter(rows),
        ))
    }
}
