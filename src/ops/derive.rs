//! Standard column derivation operations
//!
//! This module handles the derivation of new columns from existing data using
//! standard DataFusion expressions. For window functions, it delegates to the
//! window module.
//!
//! # Standard vs Window Derivations
//!
//! Standard derivations:
//! - Use DataFusion's native expression evaluation
//! - Examples: arithmetic (a + b), comparisons (x > 5), simple functions
//! - Performance: Single table scan, no sorting required
//!
//! Window derivations:
//! - Use SQL-based window functions with OVER clauses
//! - Examples: LAG/LEAD (shift), rolling aggregations, cumulative sums
//! - Handled by: crate::ops::window module
//! - This module detects window functions and delegates

use crate::engine::RUNTIME;
use crate::transpiler::pyexpr_to_datafusion;
use crate::types::dict_to_py_expr;
use crate::LTSeqTable;
use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema};
use datafusion::prelude::*;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

/// Helper function to derive new columns from expressions
///
/// This performs non-window function derivations using DataFusion expressions.
/// For window functions, it delegates to derive_with_window_functions_impl.
///
/// Args:
///     table: Reference to LTSeqTable
///     derived_cols: Dictionary mapping column names to expression dicts
pub fn derive_impl(table: &LTSeqTable, derived_cols: &Bound<'_, PyDict>) -> PyResult<LTSeqTable> {
    // 1. Get schema and DataFrame
    let schema = table.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Schema not available. Call read_csv() first.",
        )
    })?;

    let df = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No data loaded. Call read_csv() first.")
    })?;

    // 2. Check if any derived column contains window functions
    let mut has_window_functions = false;
    for (_, expr_item) in derived_cols.iter() {
        let expr_dict = expr_item.cast::<PyDict>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Expression must be dict")
        })?;

        let py_expr = dict_to_py_expr(&expr_dict)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        if crate::transpiler::contains_window_function(&py_expr) {
            has_window_functions = true;
            break;
        }
    }

    // 3. Handle window functions using SQL
    if has_window_functions {
        return crate::ops::window::derive_with_window_functions_impl(table, derived_cols);
    }

    // 4. Standard (non-window) derivation using DataFusion expressions
    let mut df_exprs = Vec::new();
    let mut col_names = Vec::new();

    for (col_name, expr_item) in derived_cols.iter() {
        let col_name_str = col_name.extract::<String>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Column name must be string")
        })?;

        let expr_dict = expr_item.cast::<PyDict>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Expression must be dict")
        })?;

        let py_expr = dict_to_py_expr(&expr_dict)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        let df_expr = pyexpr_to_datafusion(py_expr, schema)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?
            .alias(&col_name_str);

        df_exprs.push(df_expr);
        col_names.push(col_name_str);
    }

    // 5. Apply derivation via select (adds columns to existing data)
    let result_df = RUNTIME
        .block_on(async {
            // Select all existing columns + new derived columns
            use datafusion::common::Column;
            let mut all_exprs = Vec::new();

            // Add all existing columns
            for field in schema.fields() {
                // Use Column::new_unqualified to preserve case
                all_exprs.push(Expr::Column(Column::new_unqualified(field.name())));
            }

            // Add derived columns
            all_exprs.extend(df_exprs);

            (**df)
                .clone()
                .select(all_exprs)
                .map_err(|e| format!("Derive execution failed: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // 6. Get schema from the resulting DataFrame
    let df_schema = result_df.schema();
    let arrow_fields: Vec<Field> = df_schema.fields().iter().map(|f| (**f).clone()).collect();
    let new_schema = ArrowSchema::new(arrow_fields);

    // 7. Return new LTSeqTable with derived columns added
    Ok(LTSeqTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(result_df)),
        schema: Some(Arc::new(new_schema)),
        sort_exprs: table.sort_exprs.clone(),
    })
}

/// Helper function to add cumulative sum columns
///
/// Delegates to window module which handles the SQL-based cumulative sum.
/// This is a delegation point for the cum_sum() method in lib.rs.
pub fn cum_sum_impl(table: &LTSeqTable, cum_exprs: Vec<Bound<'_, PyDict>>) -> PyResult<LTSeqTable> {
    crate::ops::window::cum_sum_impl(table, cum_exprs)
}

/// Helper function to derive columns with window functions using SQL
///
/// This is a re-export delegation that routes to the window module.
/// Kept here for backward compatibility with lib.rs references.
pub fn derive_with_window_functions_impl(
    table: &LTSeqTable,
    derived_cols: &Bound<'_, PyDict>,
) -> PyResult<LTSeqTable> {
    crate::ops::window::derive_with_window_functions_impl(table, derived_cols)
}
