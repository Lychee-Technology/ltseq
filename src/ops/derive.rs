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
//! - Use native DataFusion window expressions
//! - Examples: LAG/LEAD (shift), rolling aggregations, cumulative sums
//! - Handled by: crate::ops::window module
//! - This module detects window functions and delegates

use crate::engine::RUNTIME;
use crate::error::LtseqError;
use crate::transpiler::pyexpr_to_datafusion;
use crate::types::{dict_to_py_expr, PyExpr};
use crate::LTSeqTable;
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
    let (df, schema) = table.require_df_and_schema()?;

    // 2. Deserialize all expressions once (avoids double deserialization)
    let mut parsed_cols: Vec<(String, PyExpr)> = Vec::with_capacity(derived_cols.len());
    let mut has_window_functions = false;

    for (col_name, expr_item) in derived_cols.iter() {
        let col_name_str = col_name.extract::<String>().map_err(|_| {
            LtseqError::Validation("Column name must be string".into())
        })?;

        let expr_dict = expr_item.cast::<PyDict>().map_err(|_| {
            LtseqError::Validation("Expression must be dict".into())
        })?;

        let py_expr = dict_to_py_expr(expr_dict)?;

        if crate::transpiler::contains_window_function(&py_expr) {
            has_window_functions = true;
        }

        parsed_cols.push((col_name_str, py_expr));
    }

    // 3. Handle window functions — pass pre-parsed expressions to avoid re-deserialization
    if has_window_functions {
        return crate::ops::window::derive_with_window_functions_from_parsed(
            table, schema, df, &parsed_cols,
        );
    }

    // 4. Standard (non-window) derivation using already-parsed expressions
    let mut derived_exprs = Vec::new();
    let mut derived_names = std::collections::HashSet::new();

    for (col_name_str, py_expr) in parsed_cols {
        let df_expr = pyexpr_to_datafusion(py_expr, schema)
            .map_err(LtseqError::Validation)?
            .alias(&col_name_str);

        derived_names.insert(col_name_str.clone());
        derived_exprs.push((col_name_str, df_expr));
    }

    // 5. Apply derivation via select with replace-or-add semantics (issue
    // #125 finding 4): an existing name is replaced in place, new names
    // append — never a duplicate column.
    let all_exprs = crate::ops::common::merge_derived_columns(schema, derived_exprs);
    let result_df = RUNTIME
        .block_on(async {
            (**df)
                .clone()
                .select(all_exprs)
                .map_err(|e| format!("Derive execution failed: {}", e))
        })
        .map_err(LtseqError::Runtime)?;

    // 6. Return new LTSeqTable with derived columns added (schema recomputed).
    // Overwriting a sort key invalidates the declared order from that key on.
    let sort_specs =
        crate::ops::common::truncate_specs_at_overwrite(&table.sort_specs, &derived_names);
    Ok(LTSeqTable::from_df(
        Arc::clone(&table.session),
        result_df,
        sort_specs,
        None, // row set / columns diverge from the raw file: drop fast-path token
    ))
}

/// Helper function to add cumulative sum columns
///
/// Delegates to the window module's native cumulative-sum builder.
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
