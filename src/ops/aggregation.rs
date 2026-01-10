//! Aggregation operations for LTSeqTable
//!
//! Provides SQL GROUP BY aggregation and raw SQL WHERE filtering.

use crate::engine::RUNTIME;
use crate::types::{dict_to_py_expr, PyExpr};
use crate::LTSeqTable;
use datafusion::datasource::MemTable;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

/// Aggregate rows into a summary table with one row per group
///
/// Performs SQL GROUP BY aggregation, optionally grouping by specified columns
/// and computing aggregate functions (sum, count, min, max, avg).
pub fn agg_impl(
    table: &LTSeqTable,
    group_expr: Option<Bound<'_, PyDict>>,
    agg_dict: &Bound<'_, PyDict>,
) -> PyResult<LTSeqTable> {
    // Validate tables exist
    let df = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No data loaded. Call read_csv() first.")
    })?;

    let schema = table
        .schema
        .as_ref()
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No schema available."))?;

    // Helper function to extract aggregate function from PyExpr
    fn extract_agg_function(py_expr: &PyExpr) -> Option<(String, String)> {
        if let PyExpr::Call { func, on, .. } = py_expr {
            match func.as_str() {
                "sum" | "count" | "min" | "max" | "avg" => {
                    if let PyExpr::Column(col_name) = on.as_ref() {
                        return Some((func.clone(), col_name.clone()));
                    }
                    if func == "count" {
                        return Some((func.clone(), "*".to_string()));
                    }
                    None
                }
                _ => None,
            }
        } else {
            None
        }
    }

    // Collect all agg expressions
    let mut agg_select_parts: Vec<String> = Vec::new();

    for (key, value) in agg_dict.iter() {
        let key_str = key.extract::<String>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("Agg key must be string")
        })?;

        let val_dict = value.cast::<PyDict>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("Agg value must be dict")
        })?;

        let py_expr = dict_to_py_expr(&val_dict).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Failed to parse agg expr: {}",
                e
            ))
        })?;

        if let Some((agg_func, col_name)) = extract_agg_function(&py_expr) {
            let sql_expr_str = match agg_func.as_str() {
                "sum" => format!("SUM({})", col_name),
                "count" => {
                    if col_name == "*" {
                        "COUNT(*)".to_string()
                    } else {
                        format!("COUNT({})", col_name)
                    }
                }
                "min" => format!("MIN({})", col_name),
                "max" => format!("MAX({})", col_name),
                "avg" => format!("AVG({})", col_name),
                _ => {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "Unknown aggregate function: {}",
                        agg_func
                    )))
                }
            };
            agg_select_parts.push(format!("{} as {}", sql_expr_str, key_str));
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Invalid aggregate expression - must be g.column.agg_func() or g.count()"
                    .to_string(),
            ));
        }
    }

    // Build the GROUP BY clause
    let group_cols: Option<Vec<String>> = if let Some(group_expr_dict) = group_expr {
        let py_expr = dict_to_py_expr(&group_expr_dict).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Failed to parse group expr: {}",
                e
            ))
        })?;

        match py_expr {
            PyExpr::Column(col_name) => Some(vec![col_name]),
            _ => {
                let sql_expr =
                    crate::transpiler::pyexpr_to_datafusion(py_expr, schema).map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                            "Failed to transpile group expr: {}",
                            e
                        ))
                    })?;
                let expr_str = format!("{}", sql_expr);
                Some(vec![expr_str])
            }
        }
    } else {
        None
    };

    // Build SQL query
    let temp_table_name = "__ltseq_agg_temp";

    let current_batches = RUNTIME
        .block_on(async {
            (**df)
                .clone()
                .collect()
                .await
                .map_err(|e| format!("Failed to collect data: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    let arrow_schema = Arc::new((**schema).clone());
    let temp_table = MemTable::try_new(arrow_schema, vec![current_batches]).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to create memory table: {}",
            e
        ))
    })?;

    table
        .session
        .register_table(temp_table_name, Arc::new(temp_table))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to register temp table: {}",
                e
            ))
        })?;

    let sql_query = if let Some(group_cols) = group_cols {
        let group_col_list = group_cols.join(", ");
        let agg_col_list = agg_select_parts.join(", ");
        format!(
            "SELECT {}, {} FROM {} GROUP BY {}",
            group_col_list, agg_col_list, temp_table_name, group_col_list
        )
    } else {
        let agg_col_list = agg_select_parts.join(", ");
        format!("SELECT {} FROM {}", agg_col_list, temp_table_name)
    };

    let result_batches = RUNTIME
        .block_on(async {
            let result_df = table
                .session
                .sql(&sql_query)
                .await
                .map_err(|e| format!("Failed to execute agg query: {}", e))?;

            result_df
                .collect()
                .await
                .map_err(|e| format!("Failed to collect agg results: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    table
        .session
        .deregister_table(temp_table_name)
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to deregister temp table: {}",
                e
            ))
        })?;

    if result_batches.is_empty() {
        return Ok(LTSeqTable {
            session: Arc::clone(&table.session),
            dataframe: None,
            schema: None,
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

/// Filter rows using a raw SQL WHERE clause
///
/// Used internally by group_ordered().filter() to execute SQL filtering
/// on the flattened grouped table.
pub fn filter_where_impl(table: &LTSeqTable, where_clause: &str) -> PyResult<LTSeqTable> {
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

    let temp_table_name = format!("__ltseq_filter_temp_{}", std::process::id());
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

    let column_list: Vec<String> = batch_schema
        .fields()
        .iter()
        .map(|f| format!("\"{}\"", f.name()))
        .collect();
    let columns_str = column_list.join(", ");

    let sql_query = format!(
        "SELECT {} FROM \"{}\" WHERE {}",
        columns_str, temp_table_name, where_clause
    );

    let result_batches = RUNTIME
        .block_on(async {
            let result_df = table
                .session
                .sql(&sql_query)
                .await
                .map_err(|e| format!("Failed to execute filter query: {}", e))?;

            result_df
                .collect()
                .await
                .map_err(|e| format!("Failed to collect filter results: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    let _ = table.session.deregister_table(&temp_table_name);

    if result_batches.is_empty() {
        return Ok(LTSeqTable {
            session: Arc::clone(&table.session),
            dataframe: None,
            schema: Some(Arc::clone(schema)),
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
