//! Basic table operations: read, filter, select, slice, count, distinct
//!
//! This module contains helper functions for basic table operations.
//! The actual methods are implemented in the #[pymethods] impl block in lib.rs
//! due to PyO3 constraints (only one #[pymethods] impl block per struct is allowed).

use crate::RustTable;
use crate::transpiler::pyexpr_to_datafusion;
use crate::types::dict_to_py_expr;
use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema};
use datafusion::prelude::*;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

use crate::engine::RUNTIME;

/// Helper function to read CSV file into DataFusion DataFrame
///
/// Args:
///     table: Mutable reference to RustTable
///     path: Path to CSV file
pub async fn read_csv_impl(table: &mut RustTable, path: String) -> PyResult<()> {
    let df = table
        .session
        .read_csv(&path, CsvReadOptions::new())
        .await
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to read CSV: {}",
                e
            ))
        })?;

    // Get the DFSchema and convert to Arrow schema
    let df_schema = df.schema();
    let arrow_fields: Vec<Field> =
        df_schema.fields().iter().map(|f| (**f).clone()).collect();
    let arrow_schema = ArrowSchema::new(arrow_fields);

    table.schema = Some(Arc::new(arrow_schema));
    table.dataframe = Some(Arc::new(df));
    Ok(())
}

/// Helper function to get the number of rows in the table
///
/// Returns:
///     The number of rows
pub fn count_impl(table: &RustTable) -> PyResult<usize> {
    let df = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "No data loaded. Call read_csv() first.",
        )
    })?;

    let mut row_count = 0;
    RUNTIME.block_on(async {
        let batches = (**df).clone().collect().await.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to collect data: {}",
                e
            ))
        })?;
        for batch in batches {
            row_count += batch.num_rows();
        }
        Ok::<(), PyErr>(())
    })?;

    Ok(row_count)
}

/// Helper function to filter rows based on an expression
///
/// Args:
///     table: Reference to RustTable
///     expr_dict: Serialized expression dict
pub fn filter_impl(table: &RustTable, expr_dict: &Bound<'_, PyDict>) -> PyResult<RustTable> {
    let df = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "No data loaded. Call read_csv() first.",
        )
    })?;

    let schema = table.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Schema not available.")
    })?;

    // Deserialize the expression
    let py_expr = dict_to_py_expr(expr_dict).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
    })?;

    // Transpile to DataFusion Expr
    let df_expr = pyexpr_to_datafusion(py_expr, schema)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;

    // Apply filter
    let filtered_df = RUNTIME.block_on(async {
        (**df)
            .clone()
            .filter(df_expr)
            .map_err(|e| format!("Filter failed: {}", e))
    })
    .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Get schema from the new DataFrame
    let df_schema = filtered_df.schema();
    let arrow_fields: Vec<Field> =
        df_schema.fields().iter().map(|f| (**f).clone()).collect();
    let new_arrow_schema = ArrowSchema::new(arrow_fields);

    // Return new RustTable
    Ok(RustTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(filtered_df)),
        schema: Some(Arc::new(new_arrow_schema)),
        sort_exprs: table.sort_exprs.clone(),
    })
}

/// Helper function to select specific columns
///
/// Args:
///     table: Reference to RustTable
///     exprs: List of serialized expression dicts
pub fn select_impl(table: &RustTable, exprs: Vec<Bound<'_, PyDict>>) -> PyResult<RustTable> {
    let df = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "No data loaded. Call read_csv() first.",
        )
    })?;

    let schema = table.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Schema not available.")
    })?;

    // Transpile all expressions
    let mut df_exprs = Vec::new();
    for expr_dict in exprs {
        let py_expr = dict_to_py_expr(&expr_dict).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
        })?;

        let df_expr = pyexpr_to_datafusion(py_expr, schema)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;

        df_exprs.push(df_expr);
    }

    // Apply select
    let selected_df = RUNTIME.block_on(async {
        (**df)
            .clone()
            .select(df_exprs)
            .map_err(|e| format!("Select failed: {}", e))
    })
    .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Get schema
    let df_schema = selected_df.schema();
    let arrow_fields: Vec<Field> =
        df_schema.fields().iter().map(|f| (**f).clone()).collect();
    let new_arrow_schema = ArrowSchema::new(arrow_fields);

    // Return new RustTable
    Ok(RustTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(selected_df)),
        schema: Some(Arc::new(new_arrow_schema)),
        sort_exprs: table.sort_exprs.clone(),
    })
}

/// Helper function to slice the table (offset and limit)
///
/// Args:
///     table: Reference to RustTable
///     offset: Starting row index
///     length: Maximum number of rows to return
pub fn slice_impl(table: &RustTable, offset: i64, length: Option<i64>) -> PyResult<RustTable> {
    let df = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "No data loaded. Call read_csv() first.",
        )
    })?;

    let sliced_df = RUNTIME.block_on(async {
        // DataFusion's limit(skip: usize, fetch: Option<usize>)
        let skip = offset as usize;
        let fetch = length.map(|len| len as usize);

        (**df)
            .clone()
            .limit(skip, fetch)
            .map_err(|e| format!("Slice execution failed: {}", e))
    })
    .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Get schema from the new DataFrame (or reuse existing)
    let df_schema = sliced_df.schema();
    let arrow_fields: Vec<Field> =
        df_schema.fields().iter().map(|f| (**f).clone()).collect();
    let new_arrow_schema = ArrowSchema::new(arrow_fields);

    // Return new RustTable
    Ok(RustTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(sliced_df)),
        schema: Some(Arc::new(new_arrow_schema)),
        sort_exprs: table.sort_exprs.clone(),
    })
}

/// Helper function to remove duplicate rows
///
/// Args:
///     table: Reference to RustTable
///     _key_exprs: Not used in Phase 1 (future: expressions to determine uniqueness)
pub fn distinct_impl(table: &RustTable, _key_exprs: Vec<Bound<'_, PyDict>>) -> PyResult<RustTable> {
    let df = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "No data loaded. Call read_csv() first.",
        )
    })?;

    let distinct_df = RUNTIME.block_on(async {
        (**df)
            .clone()
            .distinct()
            .map_err(|e| format!("Distinct failed: {}", e))
    })
    .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Get schema
    let df_schema = distinct_df.schema();
    let arrow_fields: Vec<Field> =
        df_schema.fields().iter().map(|f| (**f).clone()).collect();
    let new_arrow_schema = ArrowSchema::new(arrow_fields);

    // Return new RustTable
    Ok(RustTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(distinct_df)),
        schema: Some(Arc::new(new_arrow_schema)),
        sort_exprs: table.sort_exprs.clone(),
    })
}
