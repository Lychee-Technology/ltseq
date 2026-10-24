//! Basic table operations: filter, select, search_first, slice
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
//! - **filter_key_row_impl**: Keep the rows of one partition key
//! - **select_impl**: Select columns or derived expressions
//! - **search_first_impl**: Find the first row matching a predicate (optimized filter + limit 1)
//! - **slice_impl**: Select a contiguous range of rows in table order
//! - **materialize_impl**: Execute the lazy plan and snapshot the result into memory

use crate::arrow_ffi::ImportedStream;
use crate::error::LtseqError;
use crate::metadata::{sort_specs_to_window_sorts, SortSpec};
use crate::transpiler::{pyexpr_to_datafusion, pyexpr_to_named_datafusion};
use crate::types::{dict_to_py_expr, PyExpr};
use crate::LTSeqTable;
use datafusion::common::Column;
use datafusion::dataframe::DataFrame;
use datafusion::functions::math::expr_fn::isnan;
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, ExprFunctionExt};
use datafusion::prelude::lit;
use datafusion::scalar::ScalarValue;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashSet;
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

/// Keep the rows whose columns equal the one row of `key`, a NULL matching
/// NULL: the partition of that key (#258).
///
/// `key` is a row of the table's distinct keys, so each value is compared as
/// a scalar of its column's own Arrow type. It never passes through a
/// Python value or SQL text, which read a date as integer arithmetic and had
/// no notation for timestamps, binary or NaN.
///
/// A NaN key matches every NaN row, whatever its bits: `partition` lists one
/// NaN key (contract §18), and `= NaN` would also lose NaN rows to Parquet
/// row-group pruning, whose float statistics leave NaN out (#291).
pub fn filter_key_row_impl(
    table: &LTSeqTable,
    key: ImportedStream,
) -> Result<LTSeqTable, LtseqError> {
    let (key_schema, batches) = key.read_all()?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let Some(row) = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .filter(|_| rows == 1)
    else {
        return Err(LtseqError::Validation(format!(
            "a partition key is one row, got {rows}"
        )));
    };
    let conditions = key_schema
        .fields()
        .iter()
        .zip(row.columns())
        .map(|(field, values)| {
            let column = Expr::Column(Column::new_unqualified(field.name()));
            let value = ScalarValue::try_from_array(values, 0)?;
            Ok(if value.is_null() {
                column.is_null()
            } else if is_nan(&value) {
                isnan(column)
            } else {
                column.eq(lit(value))
            })
        })
        .collect::<datafusion::error::Result<Vec<_>>>()
        .map_err(|e| LtseqError::with_context("Failed to read the partition key", e))?;
    let predicate = conjunction(conditions)
        .ok_or_else(|| LtseqError::Validation("a partition key has no columns".into()))?;

    let filtered_df = (**table.require_df()?)
        .clone()
        .filter(predicate)
        .map_err(|e| LtseqError::Runtime(format!("Partition filter failed: {}", e)))?;

    Ok(LTSeqTable::from_df_with_schema(
        Arc::clone(&table.session),
        filtered_df,
        Arc::clone(table.require_schema()?),
        table.sort_specs.clone(),
        None, // row set diverges from the raw file: drop fast-path token
    ))
}

fn is_nan(value: &ScalarValue) -> bool {
    match value {
        ScalarValue::Float16(Some(v)) => v.is_nan(),
        ScalarValue::Float32(Some(v)) => v.is_nan(),
        ScalarValue::Float64(Some(v)) => v.is_nan(),
        ScalarValue::Dictionary(_, value) => is_nan(value),
        _ => false,
    }
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

/// Helper function to select a contiguous range of rows in table order
///
/// A slice is a limit above the declared order, except a slice without a
/// length that skips rows of a table with a declared order: that one numbers
/// the rows in the order and keeps those after the offset (#339).
///
/// DataFusion 55 meets an order required above a limit that skips rows
/// beneath the limit. `KeepSortsAboveOffsets` stops that for a limit with a
/// fetch, but a requirement above a limit without one carries no fetch to
/// stop. A sort or window in another order then ran before the offset, so
/// `sort("a").slice(95).sort("b")` kept the wrong 5 rows, and where nothing
/// above needed the order, as in `len()` after a filter, the sort beneath
/// the offset was dropped. The window needs the order, and a sort above its
/// filter stays above it. A fetch standing in for "no length" would let the
/// rule stop the requirement too, but DataFusion then runs the sort beneath
/// as a top-k heap, which took 7 times as long as the sort on 5 million rows;
/// the window took 1.6 times as long. A table with no declared order has no
/// order to number its rows in, so its slice stays a limit.
///
/// Args:
///     table: Reference to LTSeqTable
///     offset: First row to keep (0-based, validated in Python)
///     length: Number of rows to keep; None keeps the rest
pub fn slice_impl(table: &LTSeqTable, offset: i64, length: Option<i64>) -> PyResult<LTSeqTable> {
    // A never-loaded table slices to another never-loaded table
    if table.dataframe.is_none() {
        return Ok(LTSeqTable::unloaded(Arc::clone(&table.session)));
    }

    // Plan building only. The rows are counted in table order, so the slice
    // goes above the order.
    let skip = offset as usize;
    let fetch = length.map(|len| len as usize);
    let ordered = table.ordered_df()?;
    let sliced_df = if skip > 0 && fetch.is_none() && !table.sort_specs.is_empty() {
        after_offset(ordered, skip, &table.sort_specs)
    } else {
        ordered.limit(skip, fetch)
    }
    .map_err(|e| LtseqError::Runtime(format!("Slice execution failed: {}", e)))?;

    // Return new LTSeqTable with sliced data (schema unchanged).
    // Row subset preserves order → keep sort_specs; the raw file no
    // longer matches the row set → drop the Parquet fast-path token.
    Ok(LTSeqTable::from_df_with_schema(
        Arc::clone(&table.session),
        sliced_df,
        Arc::clone(table.require_schema()?),
        table.sort_specs.clone(),
        None,
    ))
}

/// `df`'s rows after the first `skip` in the order of `specs`.
///
/// The row numbers go in a column named unlike every column of `df`, since
/// no column name is reserved (contract §6.4).
fn after_offset(
    df: DataFrame,
    skip: usize,
    specs: &[SortSpec],
) -> datafusion::error::Result<DataFrame> {
    let names: HashSet<&str> = df
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let position = (0..)
        .map(|i| format!("__ltseq_slice_pos_{i}"))
        .find(|n| !names.contains(n.as_str()))
        .expect("an unbounded range has a free name");
    let columns: Vec<Expr> = df
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect();
    let numbered = row_number()
        .order_by(sort_specs_to_window_sorts(specs))
        .build()?
        .alias(&position);
    df.window(vec![numbered])?
        .filter(Expr::Column(Column::new_unqualified(position)).gt(lit(skip as u64)))?
        .select(columns)
}

/// Helper function to materialize the lazy plan into an in-memory table
///
/// Executes the pending DataFusion plan once, in table order, and snapshots
/// all batches into one in-order partition (`from_collected`), so downstream
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
    LTSeqTable::from_collected(
        Arc::clone(&table.session),
        batches,
        Arc::clone(schema),
        table.sort_specs.clone(),
    )
}
