//! Grouping operations for LTSeqTable
//!
//! Provides consecutive group identification and first/last row selection.
//!
//! # Operations
//!
//! ## group_id_impl
//!
//! Adds `__group_id__` column to identify consecutive groups with identical values.
//! Uses SQL window functions: ROW_NUMBER() for position, CASE WHEN for change detection,
//! SUM() for cumulative group IDs.
//!
//! ## first_row_impl / last_row_impl
//!
//! Filters table to only first/last row per group. Requires `__group_id__` column
//! from prior flatten() call. Uses ROW_NUMBER() OVER (PARTITION BY __group_id__).

use crate::engine::RUNTIME;
use crate::types::{dict_to_py_expr, PyExpr};
use crate::LTSeqTable;
use datafusion::datasource::MemTable;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

/// Add __group_id__ column to identify consecutive identical grouping values
///
/// Phase B2: Implements the "consecutive identical grouping" algorithm:
/// 1. Compute mask: grouping_col != LAG(grouping_col) with first row = 1
/// 2. Compute group_id: SUM(mask) OVER (ORDER BY rownum)
/// 3. Add __group_id__ column to result
///
/// Uses SQL execution for clean, efficient implementation.
pub fn group_id_impl(table: &LTSeqTable, grouping_expr: Bound<'_, PyDict>) -> PyResult<LTSeqTable> {
    // If no dataframe, return empty result (for unit tests)
    if table.dataframe.is_none() {
        return Ok(LTSeqTable {
            session: Arc::clone(&table.session),
            dataframe: None,
            schema: table.schema.as_ref().map(|s| Arc::clone(s)),
            sort_exprs: Vec::new(),
        });
    }

    // Get schema
    let schema = table.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Schema not available. Call read_csv() first.",
        )
    })?;

    // Deserialize grouping expression
    let py_expr = dict_to_py_expr(&grouping_expr)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    // Extract column name if this is a simple column reference
    let grouping_col_name = if let PyExpr::Column(ref col_name) = py_expr {
        col_name.clone()
    } else {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "group_ordered currently only supports simple column references",
        ));
    };

    // Get DataFrame
    let df = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No data loaded. Call read_csv() first.")
    })?;

    // Collect current data into record batches
    let current_batches = RUNTIME
        .block_on(async {
            (**df)
                .clone()
                .collect()
                .await
                .map_err(|e| format!("Failed to collect data: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Get schema from actual batches for consistency
    let batch_schema = if let Some(first_batch) = current_batches.first() {
        first_batch.schema()
    } else {
        Arc::new((**schema).clone())
    };

    // Use unique temp table name to avoid conflicts
    let temp_table_name = format!("__ltseq_group_id_temp_{}", std::process::id());
    let temp_table =
        MemTable::try_new(Arc::clone(&batch_schema), vec![current_batches]).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create memory table: {}",
                e
            ))
        })?;

    // Deregister existing table if it exists
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

    // Build list of columns for explicit SELECT instead of SELECT *
    let column_list: Vec<String> = batch_schema
        .fields()
        .iter()
        .map(|f| format!("\"{}\"", f.name()))
        .collect();
    let columns_str = column_list.join(", ");

    // Execute SQL query to compute group_id, group_count, and row number within group
    let sql_query = format!(
        r#"WITH numbered AS (
          SELECT {cols}, ROW_NUMBER() OVER () as __row_num FROM "{table}"
        ),
        masked AS (
          SELECT {cols}, __row_num, 
            CASE 
              WHEN __row_num = 1 THEN 1
              WHEN "{groupcol}" IS DISTINCT FROM LAG("{groupcol}") OVER (ORDER BY __row_num) THEN 1
              ELSE 0
            END as __mask
          FROM numbered
        ),
        grouped AS (
          SELECT {cols}, __row_num,
            SUM(__mask) OVER (ORDER BY __row_num ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as __group_id__
          FROM masked
        )
        SELECT {cols}, __group_id__,
          COUNT(*) OVER (PARTITION BY __group_id__) as __group_count__,
          ROW_NUMBER() OVER (PARTITION BY __group_id__ ORDER BY __row_num) as __rn__
        FROM grouped"#,
        cols = columns_str,
        table = temp_table_name,
        groupcol = grouping_col_name
    );

    // Execute the query
    let result_batches = RUNTIME
        .block_on(async {
            let result_df = table
                .session
                .sql(&sql_query)
                .await
                .map_err(|e| format!("Failed to execute group_id query: {}", e))?;

            result_df
                .collect()
                .await
                .map_err(|e| format!("Failed to collect group_id results: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Deregister temporary table
    table
        .session
        .deregister_table(temp_table_name)
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to deregister temp table: {}",
                e
            ))
        })?;

    // Create a new MemTable from the result batches
    if result_batches.is_empty() {
        return Ok(LTSeqTable {
            session: Arc::clone(&table.session),
            dataframe: None,
            schema: table.schema.as_ref().map(|s| Arc::clone(s)),
            sort_exprs: Vec::new(),
        });
    }

    let result_schema = result_batches[0].schema();
    let result_mem_table =
        MemTable::try_new(result_schema.clone(), vec![result_batches]).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create result memory table: {}",
                e
            ))
        })?;

    // Create a new DataFrame from the memory table via a temporary registration
    let result_table_name = "__ltseq_result";
    table
        .session
        .register_table(result_table_name, Arc::new(result_mem_table))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to register result table: {}",
                e
            ))
        })?;

    let result_df = RUNTIME
        .block_on(async {
            table
                .session
                .table(result_table_name)
                .await
                .map_err(|e| format!("Failed to get result table: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Deregister the result table since we're materializing it
    table
        .session
        .deregister_table(result_table_name)
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to deregister result table: {}",
                e
            ))
        })?;

    // Return new LTSeqTable with __group_id__ column
    Ok(LTSeqTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(result_df)),
        schema: Some(result_schema),
        sort_exprs: Vec::new(),
    })
}

/// Get only the first row of each group (Phase B4)
///
/// Requires __group_id__ column to already exist. Filters to rows where
/// ROW_NUMBER() OVER (PARTITION BY __group_id__) = 1
pub fn first_row_impl(table: &LTSeqTable) -> PyResult<LTSeqTable> {
    // If no dataframe, return empty result
    if table.dataframe.is_none() {
        return Ok(LTSeqTable {
            session: Arc::clone(&table.session),
            dataframe: None,
            schema: table.schema.as_ref().map(|s| Arc::clone(s)),
            sort_exprs: Vec::new(),
        });
    }

    // Get schema
    let schema = table.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Schema not available. Call flatten() first to add __group_id__ column.",
        )
    })?;

    // Verify __group_id__ column exists
    if !schema.fields().iter().any(|f| f.name() == "__group_id__") {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "__group_id__ column not found. Call flatten() first.",
        ));
    }

    // Get DataFrame
    let df = table
        .dataframe
        .as_ref()
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No data loaded."))?;

    // Collect current data into record batches
    let current_batches = RUNTIME
        .block_on(async {
            (**df)
                .clone()
                .collect()
                .await
                .map_err(|e| format!("Failed to collect data: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Get schema from actual batches for consistency
    let batch_schema = if let Some(first_batch) = current_batches.first() {
        first_batch.schema()
    } else {
        Arc::new((**schema).clone())
    };

    // Use unique temp table name to avoid conflicts
    let temp_table_name = format!("__ltseq_first_row_temp_{}", std::process::id());
    let temp_table =
        MemTable::try_new(Arc::clone(&batch_schema), vec![current_batches]).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create memory table: {}",
                e
            ))
        })?;

    // Deregister existing table if it exists
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

    // Build list of columns - filter out internal temp columns
    let column_list: Vec<String> = batch_schema
        .fields()
        .iter()
        .filter(|f| {
            let name = f.name();
            !name.starts_with("__rn")
                && !name.starts_with("__row_num")
                && !name.starts_with("__mask")
        })
        .map(|f| format!("\"{}\"", f.name()))
        .collect();
    let columns_str = column_list.join(", ");

    // Execute SQL query to get first row per group
    let rn_alias = format!("__rn_first_{}", std::process::id());
    let sql_query = format!(
        r#"WITH ranked AS (
          SELECT {cols}, ROW_NUMBER() OVER (PARTITION BY "__group_id__") as "{rn}" FROM "{table}"
        )
        SELECT {cols} FROM ranked WHERE "{rn}" = 1"#,
        cols = columns_str,
        table = temp_table_name,
        rn = rn_alias
    );

    // Execute the query
    let result_batches = RUNTIME
        .block_on(async {
            let result_df = table
                .session
                .sql(&sql_query)
                .await
                .map_err(|e| format!("Failed to execute first_row query: {}", e))?;

            result_df
                .collect()
                .await
                .map_err(|e| format!("Failed to collect first_row results: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Deregister temporary table
    let _ = table.session.deregister_table(&temp_table_name);

    // Create a new MemTable from the result batches
    if result_batches.is_empty() {
        return Ok(LTSeqTable {
            session: Arc::clone(&table.session),
            dataframe: None,
            schema: table.schema.as_ref().map(|s| Arc::clone(s)),
            sort_exprs: Vec::new(),
        });
    }

    let result_schema = result_batches[0].schema();
    let result_mem_table =
        MemTable::try_new(result_schema.clone(), vec![result_batches]).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create result memory table: {}",
                e
            ))
        })?;

    // Create a new DataFrame from the memory table
    let result_table_name = format!("__ltseq_first_row_result_{}", std::process::id());

    // Deregister existing result table if it exists
    let _ = table.session.deregister_table(&result_table_name);

    table
        .session
        .register_table(&result_table_name, Arc::new(result_mem_table))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to register result table: {}",
                e
            ))
        })?;

    let result_df = RUNTIME
        .block_on(async {
            table
                .session
                .table(&result_table_name)
                .await
                .map_err(|e| format!("Failed to get result table: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Deregister the result table
    let _ = table.session.deregister_table(&result_table_name);

    // Return new LTSeqTable with only first rows
    Ok(LTSeqTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(result_df)),
        schema: Some(result_schema),
        sort_exprs: Vec::new(),
    })
}

/// Get only the last row of each group (Phase B4)
///
/// Requires __group_id__ column to already exist. Filters to rows where
/// ROW_NUMBER() OVER (PARTITION BY __group_id__ ORDER BY desc) = 1
pub fn last_row_impl(table: &LTSeqTable) -> PyResult<LTSeqTable> {
    // If no dataframe, return empty result
    if table.dataframe.is_none() {
        return Ok(LTSeqTable {
            session: Arc::clone(&table.session),
            dataframe: None,
            schema: table.schema.as_ref().map(|s| Arc::clone(s)),
            sort_exprs: Vec::new(),
        });
    }

    // Get schema
    let schema = table.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Schema not available. Call flatten() first to add __group_id__ column.",
        )
    })?;

    // Verify __group_id__ column exists
    if !schema.fields().iter().any(|f| f.name() == "__group_id__") {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "__group_id__ column not found. Call flatten() first.",
        ));
    }

    // Get DataFrame
    let df = table
        .dataframe
        .as_ref()
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No data loaded."))?;

    // Collect current data into record batches
    let current_batches = RUNTIME
        .block_on(async {
            (**df)
                .clone()
                .collect()
                .await
                .map_err(|e| format!("Failed to collect data: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Get schema from actual batches for consistency
    let batch_schema = if let Some(first_batch) = current_batches.first() {
        first_batch.schema()
    } else {
        Arc::new((**schema).clone())
    };

    // Use unique temp table name to avoid conflicts
    let temp_table_name = format!("__ltseq_last_row_temp_{}", std::process::id());
    let temp_table =
        MemTable::try_new(Arc::clone(&batch_schema), vec![current_batches]).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create memory table: {}",
                e
            ))
        })?;

    // Deregister existing table if it exists
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

    // Build list of columns - filter out internal temp columns
    let column_list: Vec<String> = batch_schema
        .fields()
        .iter()
        .filter(|f| {
            let name = f.name();
            !name.starts_with("__rn")
                && !name.starts_with("__row_num")
                && !name.starts_with("__mask")
                && !name.starts_with("__cnt")
        })
        .map(|f| format!("\"{}\"", f.name()))
        .collect();
    let columns_str = column_list.join(", ");

    // Execute SQL query to get last row per group
    let rn_alias = format!("__rn_last_{}", std::process::id());
    let cnt_alias = format!("__cnt_last_{}", std::process::id());
    let sql_query = format!(
        r#"WITH ranked AS (
          SELECT {cols}, 
                  ROW_NUMBER() OVER (PARTITION BY "__group_id__") as "{rn}",
                  COUNT(*) OVER (PARTITION BY "__group_id__") as "{cnt}"
          FROM "{table}"
        )
        SELECT {cols} FROM ranked WHERE "{rn}" = "{cnt}""#,
        cols = columns_str,
        table = temp_table_name,
        rn = rn_alias,
        cnt = cnt_alias
    );

    // Execute the query
    let result_batches = RUNTIME
        .block_on(async {
            let result_df = table
                .session
                .sql(&sql_query)
                .await
                .map_err(|e| format!("Failed to execute last_row query: {}", e))?;

            result_df
                .collect()
                .await
                .map_err(|e| format!("Failed to collect last_row results: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Deregister temporary table
    let _ = table.session.deregister_table(&temp_table_name);

    // Create a new MemTable from the result batches
    if result_batches.is_empty() {
        return Ok(LTSeqTable {
            session: Arc::clone(&table.session),
            dataframe: None,
            schema: table.schema.as_ref().map(|s| Arc::clone(s)),
            sort_exprs: Vec::new(),
        });
    }

    let result_schema = result_batches[0].schema();
    let result_mem_table =
        MemTable::try_new(result_schema.clone(), vec![result_batches]).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create result memory table: {}",
                e
            ))
        })?;

    // Create a new DataFrame from the memory table
    let result_table_name = format!("__ltseq_last_row_result_{}", std::process::id());

    // Deregister existing result table if it exists
    let _ = table.session.deregister_table(&result_table_name);

    table
        .session
        .register_table(&result_table_name, Arc::new(result_mem_table))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to register result table: {}",
                e
            ))
        })?;

    let result_df = RUNTIME
        .block_on(async {
            table
                .session
                .table(&result_table_name)
                .await
                .map_err(|e| format!("Failed to get result table: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Deregister the result table
    let _ = table.session.deregister_table(&result_table_name);

    // Return new LTSeqTable with only last rows
    Ok(LTSeqTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(result_df)),
        schema: Some(result_schema),
        sort_exprs: Vec::new(),
    })
}
