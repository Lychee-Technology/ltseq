//! Basic table operations: search_first
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
//! - **search_first_impl**: Find the first row matching a predicate (optimized filter + limit 1)

use crate::error::LtseqError;
use crate::transpiler::pyexpr_to_datafusion;
use crate::types::dict_to_py_expr;
use crate::LTSeqTable;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

use crate::engine::RUNTIME;

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
        .map_err(|e| LtseqError::Validation(e))?;

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
        .map_err(|e| LtseqError::Runtime(e))?;

    // Return new LTSeqTable with recomputed schema
    Ok(LTSeqTable::from_df(
        Arc::clone(&table.session),
        result_df,
        table.sort_exprs.clone(),
        table.source_parquet_path.clone(),
    ))
}
