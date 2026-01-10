//! SQL-based derive operations for LTSeqTable
//!
//! Provides derive operations using raw SQL window expressions.

use crate::engine::RUNTIME;
use crate::LTSeqTable;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::datasource::MemTable;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

/// Add derived columns based on group properties (placeholder)
///
/// Takes a table with __group_id__ column and computes derived columns
/// like group size, first/last values, etc.
pub fn derive_impl(
    table: &LTSeqTable,
    _derived_cols_spec: Bound<'_, PyDict>,
) -> PyResult<LTSeqTable> {
    // Placeholder: Just return the input table unchanged
    Ok(LTSeqTable {
        session: Arc::clone(&table.session),
        dataframe: table.dataframe.as_ref().map(|df| Arc::clone(df)),
        schema: table.schema.as_ref().map(|s| Arc::clone(s)),
        sort_exprs: Vec::new(),
    })
}

/// Derive columns using raw SQL window expressions
///
/// Executes a SELECT query with the provided SQL expressions,
/// typically containing window functions like FIRST_VALUE, LAST_VALUE, COUNT, etc.
///
/// # Arguments
///
/// * `table` - The source table (must already have __group_id__ and __rn__ columns)
/// * `derive_exprs` - HashMap mapping column name -> SQL expression string
///
/// # Returns
///
/// A new LTSeqTable with the derived columns added (and __group_id__, __rn__ removed)
pub fn derive_window_sql_impl(
    table: &LTSeqTable,
    derive_exprs: std::collections::HashMap<String, String>,
) -> PyResult<LTSeqTable> {
    let df = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No data loaded. Call read_csv() first.")
    })?;

    let schema = table
        .schema
        .as_ref()
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No schema available."))?;

    let current_batches = RUNTIME
        .block_on(async {
            (**df)
                .clone()
                .collect()
                .await
                .map_err(|e| format!("Failed to collect data: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    let batch_schema = if let Some(first_batch) = current_batches.first() {
        first_batch.schema()
    } else {
        Arc::new((**schema).clone())
    };

    let temp_table_name = format!("__ltseq_derive_temp_{}", std::process::id());
    let temp_table =
        MemTable::try_new(Arc::clone(&batch_schema), vec![current_batches]).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create memory table: {}",
                e
            ))
        })?;

    let _ = table.session.deregister_table(&temp_table_name);

    table
        .session
        .register_table(&temp_table_name, Arc::new(temp_table))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to register temp table: {}",
                e
            ))
        })?;

    // Build SELECT clause: all existing columns + derived expressions
    let mut select_parts: Vec<String> = Vec::new();

    // Add all existing columns except __group_id__ and __rn__
    for field in batch_schema.fields() {
        let col_name = field.name();
        if col_name != "__group_id__" && col_name != "__rn__" {
            select_parts.push(format!("\"{}\"", col_name));
        }
    }

    // Add derived columns
    for (col_name, sql_expr) in &derive_exprs {
        select_parts.push(format!("{} AS \"{}\"", sql_expr, col_name));
    }

    let sql_query = format!(
        "SELECT {} FROM \"{}\"",
        select_parts.join(", "),
        temp_table_name
    );

    let result_batches = RUNTIME
        .block_on(async {
            let result_df = table.session.sql(&sql_query).await.map_err(|e| {
                format!(
                    "Failed to execute derive query: {} -- SQL: {}",
                    e, sql_query
                )
            })?;

            result_df
                .collect()
                .await
                .map_err(|e| format!("Failed to collect derive results: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    let _ = table.session.deregister_table(&temp_table_name);

    if result_batches.is_empty() {
        // Build schema for empty result
        let mut result_fields: Vec<datafusion::arrow::datatypes::Field> = Vec::new();
        for field in batch_schema.fields() {
            let col_name = field.name();
            if col_name != "__group_id__" && col_name != "__rn__" {
                result_fields.push((**field).clone());
            }
        }
        // Add derived columns as nullable Float64
        for col_name in derive_exprs.keys() {
            result_fields.push(datafusion::arrow::datatypes::Field::new(
                col_name,
                datafusion::arrow::datatypes::DataType::Float64,
                true,
            ));
        }
        let empty_schema = Arc::new(ArrowSchema::new(result_fields));
        return Ok(LTSeqTable {
            session: Arc::clone(&table.session),
            dataframe: None,
            schema: Some(empty_schema),
            sort_exprs: Vec::new(),
        });
    }

    let result_schema = result_batches[0].schema();
    let result_mem_table = MemTable::try_new(Arc::clone(&result_schema), vec![result_batches])
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create result table: {}",
                e
            ))
        })?;

    let result_df = table
        .session
        .read_table(Arc::new(result_mem_table))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create result table: {}",
                e
            ))
        })?;

    Ok(LTSeqTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(result_df)),
        schema: Some(Arc::clone(&result_schema)),
        sort_exprs: Vec::new(),
    })
}
