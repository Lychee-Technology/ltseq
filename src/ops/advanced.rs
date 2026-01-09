//! Advanced table operations
//!
//! This module contains helper functions for advanced operations like sorting and joining tables.
//!
//! # Architecture
//!
//! Due to PyO3 constraints (only one #[pymethods] impl block per struct), all actual
//! operation implementations are extracted as helper functions here. The #[pymethods]
//! impl block in lib.rs delegates to these helpers via simple stubs.
//!
//! # Operations Provided
//!
//! ## Sort (sort_impl)
//! 
//! Sorts table rows by one or more key expressions with optional descending order. The implementation:
//! - Deserializes Python expression dicts → PyExpr
//! - Transpiles PyExpr → DataFusion SortExpr (with configurable asc/desc per key)
//! - Captures sort keys for later window function use (Phase 6)
//! - Executes multi-level sort via DataFusion
//!
//! **Phase B1 Enhancement**: Added desc_flags parameter to support descending sort
//! - asc = !is_desc (invert the flag)
//! - nulls_first = is_desc (for descending, put NULLs first by default)
//!
//! **Key Feature**: Stores sort_exprs in RustTable for window function context
//!
//! ## Group ID (group_id_impl)
//!
//! Adds `__group_id__` column to identify consecutive groups with identical grouping values.
//! Foundation for group_ordered() operations.
//!
//! **Phase B2**: Full implementation with SQL execution:
//! 1. Collect dataframe into record batches
//! 2. Register as temporary memory table
//! 3. Execute SQL with window functions:
//!    - ROW_NUMBER() for position tracking
//!    - CASE WHEN to detect value changes (creates mask)
//!    - SUM(mask) OVER (ORDER BY) to compute cumulative group IDs
//! 4. Deregister temporary table
//! 5. Return materialized dataframe with __group_id__ column
//!
//! **Algorithm**: mask = col != LAG(col, 1) with first row = 1
//!               group_id = SUM(mask) OVER (ORDER BY rownum)
//!
//! ## First/Last Row Selection (first_row_impl, last_row_impl)
//!
//! **Phase B4**: Filters table to only first/last row per group.
//! These operations assume __group_id__ column already exists.
//! 
//! Implementation:
//! 1. Takes a table with existing __group_id__ column
//! 2. Adds ROW_NUMBER() OVER (PARTITION BY __group_id__) as __rn__
//! 3. For first_row: keeps rows where __rn__ = 1
//! 4. For last_row: computes max(__rn__) per group and filters
//! 5. Removes temporary __rn__ column
//!
//! ## Join (join_impl)
//!
//! Performs cross-table joins with automatic schema conflict handling.
//!
//! **Design Challenge**: DataFusion rejects joins where column names overlap between tables.
//! This is resolved via:
//! 1. Rename right table join keys temporarily (e.g., `id` → `__tmp_id`)
//! 2. Execute join on renamed keys
//! 3. Final SELECT renames right table columns with user-specified alias (e.g., `right_id`)
//!
//! **Supported Join Types**: Inner, Left, Right, Full
//!
//! # Implementation Pattern
//!
//! All operations follow:
//! 1. Validate schema and dataframe exist
//! 2. Deserialize Python expression dicts → PyExpr
//! 3. Transpile PyExpr → DataFusion Expr
//! 4. Execute via DataFusion query engine
//! 5. Collect results into new RustTable
//!
//! # Schema Handling
//!
//! - Sort: Preserves original schema
//! - Join: Merges schemas with:
//!   - Left table columns (unchanged)
//!   - Right table columns prefixed with alias (e.g., `right_id`, `right_value`)

use crate::RustTable;
use crate::types::{dict_to_py_expr, PyExpr};
use crate::transpiler::pyexpr_to_datafusion;
use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema, DataType};
use datafusion::arrow::array;
use datafusion::arrow::array::Array;
use datafusion::logical_expr::{JoinType, SortExpr};
use datafusion::prelude::*;
use datafusion::datasource::MemTable;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

use crate::engine::RUNTIME;

/// Extract join key column names from either a Column or And-expression
/// 
/// Phase 8H: Support both simple column references and composite And-expressions
/// 
/// For composite keys like: (o.id == p.id) & (o.year == p.year)
/// This function extracts the column pairs:
/// - Returns (left_cols, right_cols) where left_cols[i] should equal right_cols[i]
/// 
/// Examples:
/// - Column("id") passed as left_key_expr → returns (["id"], [])
/// - Column("id") passed as right_key_expr → returns ([], ["id"])
/// - And( Eq(Column("id"), Column("id")), Eq(Column("year"), Column("year")) )
///   → (["id", "year"], ["id", "year"])  // Composite case (same tree)
/// 
/// Returns: (left_col_names, right_col_names) tuple of column names
fn extract_join_key_columns(left_expr: &PyExpr, right_expr: &PyExpr) -> PyResult<(Vec<String>, Vec<String>)> {
    // Handle simple case: single column on each side
    if let (PyExpr::Column(left_name), PyExpr::Column(right_name)) = (left_expr, right_expr) {
        return Ok((vec![left_name.clone()], vec![right_name.clone()]));
    }
    
    // Handle composite case: And-expressions (should be same tree)
    if let (PyExpr::BinOp { op: op_left, .. }, PyExpr::BinOp { op: op_right, .. }) = (left_expr, right_expr) {
        if op_left == "And" && op_right == "And" {
            let mut left_cols = Vec::new();
            let mut right_cols = Vec::new();
            
            extract_cols_from_and(left_expr, &mut left_cols, &mut right_cols)?;
            return Ok((left_cols, right_cols));
        }
    }
    
    Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
        "Join keys must be either simple columns or matching And-expressions",
    ))
}

/// Recursively extract column names from And-expressions containing Eq operations
fn extract_cols_from_and(
    expr: &PyExpr,
    left_cols: &mut Vec<String>,
    right_cols: &mut Vec<String>,
) -> PyResult<()> {
    match expr {
        PyExpr::BinOp { op, left, right } => {
            match op.as_str() {
                "And" => {
                    // Recursively process both sides of And
                    extract_cols_from_and(left, left_cols, right_cols)?;
                    extract_cols_from_and(right, left_cols, right_cols)?;
                    Ok(())
                }
                "Eq" => {
                    // Extract left and right column names from equality
                    if let PyExpr::Column(left_name) = left.as_ref() {
                        left_cols.push(left_name.clone());
                    } else {
                        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "Left side of join equality must be a column reference",
                        ));
                    }
                    
                    if let PyExpr::Column(right_name) = right.as_ref() {
                        right_cols.push(right_name.clone());
                    } else {
                        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "Right side of join equality must be a column reference",
                        ));
                    }
                    Ok(())
                }
                other => {
                    Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        format!(
                            "Unsupported operator in join condition: {}. Expected 'And' or 'Eq'",
                            other
                        ),
                    ))
                }
            }
        }
        _ => {
            Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Expected And or Eq expression in composite join",
            ))
        }
    }
}

/// Helper function to sort rows by one or more key expressions
///
/// Sorts the table in ascending order by default. Captures the sort keys
/// for use in subsequent window function operations.
///
/// Args:
///     table: Reference to RustTable
///     sort_exprs: List of serialized expression dicts (from Python)
pub fn sort_impl(table: &RustTable, sort_exprs: Vec<Bound<'_, PyDict>>, desc_flags: Vec<bool>) -> PyResult<RustTable> {
    // If no dataframe, return empty result (for unit tests)
    if table.dataframe.is_none() {
        return Ok(RustTable {
            session: Arc::clone(&table.session),
            dataframe: None,
            schema: table.schema.as_ref().map(|s| Arc::clone(s)),
            sort_exprs: Vec::new(),
        });
    }

    // Get schema (required for transpilation)
    let schema = table.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Schema not available. Call read_csv() first.",
        )
    })?;

    // Capture sort column names for Phase 6 window functions
    let mut captured_sort_keys = Vec::new();

    // Deserialize and transpile each sort expression
    let mut df_sort_exprs = Vec::new();
    for (i, expr_dict) in sort_exprs.iter().enumerate() {
        let py_expr = dict_to_py_expr(expr_dict)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        // Capture column name if this is a simple column reference
        if let PyExpr::Column(ref col_name) = py_expr {
            captured_sort_keys.push(col_name.clone());
        }

        let df_expr = pyexpr_to_datafusion(py_expr, schema)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;
        
        // Get desc flag for this sort key
        let is_desc = desc_flags.get(i).copied().unwrap_or(false);
        
        // Create SortExpr with asc = !is_desc
        // For descending, nulls_first = true by default
        df_sort_exprs.push(SortExpr {
            expr: df_expr,
            asc: !is_desc,
            nulls_first: is_desc,
        });
    }

    // Get DataFrame
    let df = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "No data loaded. Call read_csv() first.",
        )
    })?;

    // Apply sort (async operation)
    let sorted_df = RUNTIME
        .block_on(async {
            (**df)
                .clone()
                .sort(df_sort_exprs)
                .map_err(|e| format!("Sort execution failed: {}", e))
    })
    .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Return new RustTable with sorted data and captured sort keys
    Ok(RustTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(sorted_df)),
        schema: table.schema.as_ref().map(|s| Arc::clone(s)),
        sort_exprs: captured_sort_keys,
    })
}

/// Add __group_id__ column to identify consecutive identical grouping values
///
/// Phase B2: Implements the "consecutive identical grouping" algorithm:
/// 1. Compute mask: grouping_col != LAG(grouping_col) with first row = 1
/// 2. Compute group_id: SUM(mask) OVER (ORDER BY rownum)
/// 3. Add __group_id__ column to result
///
/// Uses SQL execution for clean, efficient implementation.
pub fn group_id_impl(table: &RustTable, grouping_expr: Bound<'_, PyDict>) -> PyResult<RustTable> {
    // If no dataframe, return empty result (for unit tests)
    if table.dataframe.is_none() {
        return Ok(RustTable {
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
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "No data loaded. Call read_csv() first.",
        )
    })?;

    // Collect current data into record batches
    let current_batches = RUNTIME
        .block_on(async {
            (**df).clone().collect().await
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
    let temp_table = MemTable::try_new(Arc::clone(&batch_schema), vec![current_batches]).map_err(|e| {
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
    // Algorithm:
    // 1. Add ROW_NUMBER() for tracking row positions
    // 2. Use CASE WHEN to create mask (1 when value changes or first row, 0 otherwise)
    // 3. SUM(mask) OVER (ORDER BY rownum) gives cumulative group IDs
    // 4. COUNT(*) OVER (PARTITION BY group_id) gives row count per group
    // 5. ROW_NUMBER() OVER (PARTITION BY group_id) gives row number within each group
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
            let result_df = table.session.sql(&sql_query)
                .await
                .map_err(|e| format!("Failed to execute group_id query: {}", e))?;
            
            result_df.collect()
                .await
                .map_err(|e| format!("Failed to collect group_id results: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Deregister temporary table
    table.session.deregister_table(temp_table_name)
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to deregister temp table: {}",
                e
            ))
        })?;

    // Create a new MemTable from the result batches
    if result_batches.is_empty() {
        return Ok(RustTable {
            session: Arc::clone(&table.session),
            dataframe: None,
            schema: table.schema.as_ref().map(|s| Arc::clone(s)),
            sort_exprs: Vec::new(),
        });
    }

    let result_schema = result_batches[0].schema();
    let result_mem_table = MemTable::try_new(result_schema.clone(), vec![result_batches])
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to create result memory table: {}",
            e
        )))?;

    // Create a new DataFrame from the memory table via a temporary registration
    let result_table_name = "__ltseq_result";
    table.session.register_table(result_table_name, Arc::new(result_mem_table))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to register result table: {}",
                e
            ))
        })?;

    let result_df = RUNTIME
        .block_on(async {
            table.session.table(result_table_name)
                .await
                .map_err(|e| format!("Failed to get result table: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Deregister the result table since we're materializing it
    table.session.deregister_table(result_table_name)
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to deregister result table: {}",
                e
            ))
        })?;

    // Return new RustTable with __group_id__ column
    Ok(RustTable {
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
pub fn first_row_impl(table: &RustTable) -> PyResult<RustTable> {
    // If no dataframe, return empty result
    if table.dataframe.is_none() {
        return Ok(RustTable {
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
    let df = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "No data loaded.",
        )
    })?;

    // Collect current data into record batches
    let current_batches = RUNTIME
        .block_on(async {
            (**df).clone().collect().await
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
    let temp_table = MemTable::try_new(Arc::clone(&batch_schema), vec![current_batches]).map_err(|e| {
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
    // This avoids schema conflicts when the data comes from previous window operations
    // Filter out internal columns like __rn__, __row_num, __mask, etc. that were added by previous operations
    let column_list: Vec<String> = batch_schema
        .fields()
        .iter()
        .filter(|f| {
            let name = f.name();
            // Keep user columns and our group columns, but filter out internal temp columns
            !name.starts_with("__rn") && !name.starts_with("__row_num") && !name.starts_with("__mask")
        })
        .map(|f| format!("\"{}\"", f.name()))
        .collect();
    let columns_str = column_list.join(", ");

    // Execute SQL query to get first row per group
    // Use a unique suffix for the row number alias to avoid conflicts with existing columns
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
            let result_df = table.session.sql(&sql_query)
                .await
                .map_err(|e| format!("Failed to execute first_row query: {}", e))?;
            
            result_df.collect()
                .await
                .map_err(|e| format!("Failed to collect first_row results: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Deregister temporary table
    let _ = table.session.deregister_table(&temp_table_name);

    // Create a new MemTable from the result batches
    if result_batches.is_empty() {
        return Ok(RustTable {
            session: Arc::clone(&table.session),
            dataframe: None,
            schema: table.schema.as_ref().map(|s| Arc::clone(s)),
            sort_exprs: Vec::new(),
        });
    }

    let result_schema = result_batches[0].schema();
    let result_mem_table = MemTable::try_new(result_schema.clone(), vec![result_batches])
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to create result memory table: {}",
            e
        )))?;

    // Create a new DataFrame from the memory table
    let result_table_name = format!("__ltseq_first_row_result_{}", std::process::id());
    
    // Deregister existing result table if it exists
    let _ = table.session.deregister_table(&result_table_name);
    
    table.session.register_table(&result_table_name, Arc::new(result_mem_table))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to register result table: {}",
                e
            ))
        })?;

    let result_df = RUNTIME
        .block_on(async {
            table.session.table(&result_table_name)
                .await
                .map_err(|e| format!("Failed to get result table: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Deregister the result table
    let _ = table.session.deregister_table(&result_table_name);

    // Return new RustTable with only first rows
    Ok(RustTable {
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
pub fn last_row_impl(table: &RustTable) -> PyResult<RustTable> {
    // If no dataframe, return empty result
    if table.dataframe.is_none() {
        return Ok(RustTable {
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
    let df = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "No data loaded.",
        )
    })?;

    // Collect current data into record batches
    let current_batches = RUNTIME
        .block_on(async {
            (**df).clone().collect().await
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
    let temp_table = MemTable::try_new(Arc::clone(&batch_schema), vec![current_batches]).map_err(|e| {
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
    // Filter out internal columns like __rn__, __row_num, __mask, etc. that were added by previous operations
    let column_list: Vec<String> = batch_schema
        .fields()
        .iter()
        .filter(|f| {
            let name = f.name();
            // Keep user columns and our group columns, but filter out internal temp columns
            !name.starts_with("__rn") && !name.starts_with("__row_num") && !name.starts_with("__mask") && !name.starts_with("__cnt")
        })
        .map(|f| format!("\"{}\"", f.name()))
        .collect();
    let columns_str = column_list.join(", ");

    // Execute SQL query to get last row per group
    // We assign row numbers within each group (using original insertion order)
    // and then select where that row number equals the max row number per group
    // Use unique suffixes for row number and count aliases to avoid conflicts with existing columns
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
            let result_df = table.session.sql(&sql_query)
                .await
                .map_err(|e| format!("Failed to execute last_row query: {}", e))?;
            
            result_df.collect()
                .await
                .map_err(|e| format!("Failed to collect last_row results: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Deregister temporary table
    let _ = table.session.deregister_table(&temp_table_name);

    // Create a new MemTable from the result batches
    if result_batches.is_empty() {
        return Ok(RustTable {
            session: Arc::clone(&table.session),
            dataframe: None,
            schema: table.schema.as_ref().map(|s| Arc::clone(s)),
            sort_exprs: Vec::new(),
        });
    }

    let result_schema = result_batches[0].schema();
    let result_mem_table = MemTable::try_new(result_schema.clone(), vec![result_batches])
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to create result memory table: {}",
            e
        )))?;

    // Create a new DataFrame from the memory table
    let result_table_name = format!("__ltseq_last_row_result_{}", std::process::id());
    
    // Deregister existing result table if it exists
    let _ = table.session.deregister_table(&result_table_name);
    
    table.session.register_table(&result_table_name, Arc::new(result_mem_table))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to register result table: {}",
                e
            ))
        })?;

    let result_df = RUNTIME
        .block_on(async {
            table.session.table(&result_table_name)
                .await
                .map_err(|e| format!("Failed to get result table: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Deregister the result table
    let _ = table.session.deregister_table(&result_table_name);

    // Return new RustTable with only last rows
    Ok(RustTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(result_df)),
        schema: Some(result_schema),
        sort_exprs: Vec::new(),
    })
}

/// Add derived columns based on group properties (Phase B7)
///
/// Takes a table with __group_id__ column and computes derived columns
/// like group size, first/last values, etc.
///
/// For now, this is a placeholder. Full implementation requires:
/// 1. Evaluating Python lambdas for each group
/// 2. Broadcasting computed values back to all rows in group
/// 3. Adding new columns to result schema
pub fn derive_impl(table: &RustTable, _derived_cols_spec: Bound<'_, PyDict>) -> PyResult<RustTable> {
    // Phase B7 placeholder: Just return the input table unchanged
    // Full implementation would compute derived columns and add them
    
    Ok(RustTable {
        session: Arc::clone(&table.session),
        dataframe: table.dataframe.as_ref().map(|df| Arc::clone(df)),
        schema: table.schema.as_ref().map(|s| Arc::clone(s)),
        sort_exprs: Vec::new(),
    })
}

fn validate_join_tables(
    table: &RustTable,
    other: &RustTable,
) -> PyResult<(Arc<DataFrame>, Arc<DataFrame>, Arc<ArrowSchema>, Arc<ArrowSchema>)> {
    let df_left = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Left table has no data. Call read_csv() first.",
        )
    })?;

    let df_right = other.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Right table has no data. Call read_csv() first.",
        )
    })?;

    let schema_left = table.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Left table schema not available.",
        )
    })?;

    let schema_right = other.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Right table schema not available.",
        )
    })?;

    Ok((
        Arc::clone(df_left),
        Arc::clone(df_right),
        Arc::clone(schema_left),
        Arc::clone(schema_right),
    ))
}

/// Extract and validate join key column names from expressions
fn extract_and_validate_join_keys(
    left_key_expr_dict: &Bound<'_, PyDict>,
    right_key_expr_dict: &Bound<'_, PyDict>,
    schema_left: &ArrowSchema,
    schema_right: &ArrowSchema,
) -> PyResult<(Vec<String>, Vec<String>)> {
    let left_key_expr = dict_to_py_expr(left_key_expr_dict)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    let right_key_expr = dict_to_py_expr(right_key_expr_dict)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    let (left_col_names, right_col_names) = extract_join_key_columns(&left_key_expr, &right_key_expr)?;

    if left_col_names.is_empty() || right_col_names.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "No join keys found in expressions",
        ));
    }

    // Validate left columns exist
    for left_col in &left_col_names {
        if !schema_left.fields().iter().any(|f| f.name() == left_col) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Column '{}' not found in left table", left_col),
            ));
        }
    }

    // Validate right columns exist
    for right_col in &right_col_names {
        if !schema_right.fields().iter().any(|f| f.name() == right_col) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Column '{}' not found in right table", right_col),
            ));
        }
    }

    Ok((left_col_names, right_col_names))
}

/// Rename right table columns with unique temporary names
/// This renames ALL columns (both join keys and non-join keys) to avoid naming conflicts during join
fn rename_right_table_keys(
    df_right: &Arc<DataFrame>,
    schema_right: &ArrowSchema,
    right_col_names: &[String],
) -> PyResult<(DataFrame, Vec<String>)> {
    use datafusion::common::Column;
    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let mut right_select_exprs = Vec::new();
    let mut temp_right_col_names = Vec::new();

    for (idx, field) in schema_right.fields().iter().enumerate() {
        let col_name = field.name();
        let temp_col_name = if right_col_names.contains(&col_name.to_string()) {
            // Join key column: use special prefix for join keys
            format!("__ltseq_rkey_{}_{}__{}", idx, unique_suffix, col_name.to_lowercase())
        } else {
            // Non-join-key column: use generic prefix to avoid conflicts
            format!("__ltseq_rcol_{}_{}__{}", idx, unique_suffix, col_name.to_lowercase())
        };
        
        // Use Column::new_unqualified to preserve case on actual column name
        right_select_exprs.push(
            Expr::Column(Column::new_unqualified(col_name)).alias(&temp_col_name)
        );
        temp_right_col_names.push(temp_col_name);
    }

    let df_right_renamed = RUNTIME
        .block_on(async {
            (**df_right)
                .clone()
                .select(right_select_exprs)
                .map_err(|e| format!("Right table column rename failed: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    Ok((df_right_renamed, temp_right_col_names))
}

/// Execute the join operation
fn execute_join(
    df_left: &DataFrame,
    df_right: &DataFrame,
    left_join_keys: &[&str],
    right_join_keys: &[&str],
    join_type: JoinType,
) -> PyResult<DataFrame> {
    let joined_df = RUNTIME
        .block_on(async {
            df_left
                .clone()
                .join(
                    df_right.clone(),
                    join_type,
                    left_join_keys,
                    right_join_keys,
                    None,
                )
                .map_err(|e| format!("Join execution failed: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    Ok(joined_df)
}

/// Build final schema and rename columns after join
fn build_final_schema_and_select(
    joined_df: &DataFrame,
    schema_left: &ArrowSchema,
    schema_right: &ArrowSchema,
    alias: &str,
    renamed_col_mapping: &[(String, String)], // (original_name, temp_name) pairs
) -> PyResult<(ArrowSchema, DataFrame)> {
    use datafusion::common::Column;
    let mut final_fields = Vec::new();
    let mut select_exprs: Vec<Expr> = Vec::new();

    // Add left table columns
    for field in schema_left.fields().iter() {
        final_fields.push((**field).clone());
        // Use Column::new_unqualified to preserve case
        select_exprs.push(Expr::Column(Column::new_unqualified(field.name())));
    }

    // Add right table columns with alias prefix
    for field in schema_right.fields().iter() {
        let col_name = field.name();
        let new_col_name = format!("{}_{}", alias, col_name);
        let new_field = Field::new(
            new_col_name.clone(),
            field.data_type().clone(),
            field.is_nullable(),
        );
        final_fields.push(new_field);
        
        // Check if this column was renamed for the join
        let actual_col_name = renamed_col_mapping
            .iter()
            .find(|(orig, _)| orig == col_name)
            .map(|(_, temp)| temp.as_str())
            .unwrap_or(col_name);
        
        // Use the actual column name in the joined_df
        select_exprs.push(
            Expr::Column(Column::new_unqualified(actual_col_name)).alias(&new_col_name)
        );
    }

    let final_arrow_schema = ArrowSchema::new(final_fields);

    let final_df = RUNTIME
        .block_on(async {
            joined_df
                .clone()
                .select(select_exprs)
                .map_err(|e| format!("Final column rename SELECT failed: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    Ok((final_arrow_schema, final_df))
}

/// Helper function to join two tables using pointer-based foreign keys
///
/// This method implements joins between two tables based on specified join keys.
/// It supports lazy evaluation - the join is only executed when the result is accessed.
///
/// # DataFusion Schema Conflict Workaround
///
/// DataFusion's join() API rejects tables with duplicate column names in the join input,
/// even if the columns are from different tables. This implementation works around this
/// limitation by:
///
/// 1. Temporarily renaming the right table's join key to `__join_key_right__`
/// 2. Executing the join on `left.join_key == right.__join_key_right__`
/// 3. After joining, selecting all columns and renaming back to the desired schema with alias
/// 4. The final schema has: `{all_left_cols, all_right_cols_prefixed_with_alias}`
///
/// # Example
///
/// For join(orders, products, left_key="product_id", right_key="product_id", alias="prod"):
///
/// - orders schema: {id, product_id, quantity}
/// - products schema: {product_id, name, price}
/// - result schema: {id, product_id, quantity, prod_product_id, prod_name, prod_price}
///
/// # Arguments
///
/// * `table` - The left table
/// * `other` - The table to join with (right table)
/// * `left_key_expr_dict` - Serialized Column expression dict for left join key
/// * `right_key_expr_dict` - Serialized Column expression dict for right join key
/// * `join_type` - Type of join (currently only "inner" is supported)
/// * `alias` - Prefix for right table columns (used to avoid name conflicts)

/// Aggregate rows into a summary table with one row per group
///
/// Performs SQL GROUP BY aggregation, optionally grouping by specified columns
/// and computing aggregate functions (sum, count, min, max, avg).
///
/// # Arguments
///
/// * `table` - The table to aggregate
/// * `group_expr` - Optional expression dict specifying grouping key(s)
/// * `agg_dict` - Dictionary mapping {agg_name: agg_expression_dict}
///
/// # Returns
///
/// A new RustTable with one row per group (or one row for full-table agg)
pub fn agg_impl(
    table: &RustTable,
    group_expr: Option<Bound<'_, PyDict>>,
    agg_dict: &Bound<'_, PyDict>,
) -> PyResult<RustTable> {
    // Validate tables exist
    let df = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "No data loaded. Call read_csv() first.",
        )
    })?;
    
    let schema = table.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "No schema available.",
        )
    })?;

    // Helper function to extract aggregate function from PyExpr
    fn extract_agg_function(py_expr: &PyExpr) -> Option<(String, String)> {
        // Pattern: g.col.sum() or g.count()
        // These are represented as Call expressions
        if let PyExpr::Call { func, on, .. } = py_expr {
            // func is "sum", "count", "min", "max", "avg"
            // on is the object we're calling on (either Column or another Call)
            match func.as_str() {
                "sum" | "count" | "min" | "max" | "avg" => {
                    // Extract column name from on
                    if let PyExpr::Column(col_name) = on.as_ref() {
                        return Some((func.clone(), col_name.clone()));
                    }
                    // Special case: g.count() without column (func="count", on="g")
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

    // Collect all agg expressions and convert to DataFusion expressions
    let mut agg_select_parts: Vec<String> = Vec::new();
    
    // Extract aggregation expressions from dict
    for (key, value) in agg_dict.iter() {
        let key_str = key.extract::<String>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("Agg key must be string"))?;
        
        let val_dict = value.cast::<PyDict>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("Agg value must be dict"))?;
        
        let py_expr = dict_to_py_expr(&val_dict)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Failed to parse agg expr: {}", e)))?;
        
        // Try to extract aggregate function
        if let Some((agg_func, col_name)) = extract_agg_function(&py_expr) {
            // Build aggregate SQL
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
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        format!("Unknown aggregate function: {}", agg_func)
                    ))
                }
            };
            agg_select_parts.push(format!("{} as {}", sql_expr_str, key_str));
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Invalid aggregate expression - must be g.column.agg_func() or g.count()".to_string()
            ));
        }
    }

    // Build the GROUP BY clause
    let group_cols: Option<Vec<String>> = if let Some(group_expr_dict) = group_expr {
        let py_expr = dict_to_py_expr(&group_expr_dict)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Failed to parse group expr: {}", e)))?;
        
        // Handle both single column and list of columns
        match py_expr {
            PyExpr::Column(col_name) => {
                // Single group column
                Some(vec![col_name])
            }
            _ => {
                // For complex expressions, transpile to SQL and use as-is
                let sql_expr = crate::transpiler::pyexpr_to_datafusion(py_expr, schema)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Failed to transpile group expr: {}", e)))?;
                let expr_str = format!("{}", sql_expr);
                Some(vec![expr_str])
            }
        }
    } else {
        None
    };

    // Build SQL query
    let temp_table_name = "__ltseq_agg_temp";
    
    // Register temp table
    let current_batches = RUNTIME
        .block_on(async {
            (**df).clone().collect().await
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

    // Build SQL query
    let sql_query = if let Some(group_cols) = group_cols {
        let group_col_list = group_cols.join(", ");
        let agg_col_list = agg_select_parts.join(", ");
        format!(
            "SELECT {}, {} FROM {} GROUP BY {}",
            group_col_list, agg_col_list, temp_table_name, group_col_list
        )
    } else {
        // Full-table aggregation (no grouping)
        let agg_col_list = agg_select_parts.join(", ");
        format!("SELECT {} FROM {}", agg_col_list, temp_table_name)
    };

    // Execute query
    let result_batches = RUNTIME
        .block_on(async {
            let result_df = table.session.sql(&sql_query)
                .await
                .map_err(|e| format!("Failed to execute agg query: {}", e))?;
            
            result_df.collect()
                .await
                .map_err(|e| format!("Failed to collect agg results: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Deregister temporary table
    table.session.deregister_table(temp_table_name)
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to deregister temp table: {}",
                e
            ))
        })?;

    // Create result RustTable
    if result_batches.is_empty() {
        return Ok(RustTable {
            session: Arc::clone(&table.session),
            dataframe: None,
            schema: None,
            sort_exprs: Vec::new(),
        });
    }

    let result_schema = result_batches[0].schema();
    let result_mem_table = MemTable::try_new(Arc::clone(&result_schema), vec![result_batches]).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to create result table: {}",
            e
        ))
    })?;

    // Create DataFrame from the result using read_table
    let result_df = table.session.read_table(Arc::new(result_mem_table)).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to create result table: {}",
            e
        ))
    })?;

    Ok(RustTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(result_df)),
        schema: Some(Arc::clone(&result_schema)),
        sort_exprs: Vec::new(),
    })
}

/// Filter rows using a raw SQL WHERE clause
///
/// This is used internally by group_ordered().filter() to execute SQL filtering
/// on the flattened grouped table.
///
/// # Arguments
///
/// * `table` - The table to filter
/// * `where_clause` - Raw SQL WHERE clause string (e.g., "__group_count__ > 2")
///
/// # Returns
///
/// A new RustTable with rows matching the WHERE clause
pub fn filter_where_impl(
    table: &RustTable,
    where_clause: &str,
) -> PyResult<RustTable> {
    // Validate table exists
    let df = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "No data loaded. Call read_csv() first.",
        )
    })?;
    
    let schema = table.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "No schema available.",
        )
    })?;

    // Register the current table
    let temp_table_name = "__ltseq_filter_temp";
    
    let current_batches = RUNTIME
        .block_on(async {
            (**df).clone().collect().await
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

    // Build SQL query
    let sql_query = format!(
        "SELECT * FROM {} WHERE {}",
        temp_table_name, where_clause
    );

    // Execute query
    let result_batches = RUNTIME
        .block_on(async {
            let result_df = table.session.sql(&sql_query)
                .await
                .map_err(|e| format!("Failed to execute filter query: {}", e))?;
            
            result_df.collect()
                .await
                .map_err(|e| format!("Failed to collect filter results: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Deregister temporary table
    table.session.deregister_table(temp_table_name)
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to deregister temp table: {}",
                e
            ))
        })?;

    // Create result RustTable
    if result_batches.is_empty() {
        return Ok(RustTable {
            session: Arc::clone(&table.session),
            dataframe: None,
            schema: Some(Arc::clone(schema)),
            sort_exprs: Vec::new(),
        });
    }

    let result_schema = result_batches[0].schema();
    let result_mem_table = MemTable::try_new(Arc::clone(&result_schema), vec![result_batches]).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to create result table: {}",
            e
        ))
    })?;

    // Create DataFrame from the result using read_table
    let result_df = table.session.read_table(Arc::new(result_mem_table)).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to create result table: {}",
            e
        ))
    })?;

    Ok(RustTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(result_df)),
        schema: Some(Arc::clone(&result_schema)),
        sort_exprs: Vec::new(),
    })
}


pub fn join_impl(
    table: &RustTable,
    other: &RustTable,
    left_key_expr_dict: &Bound<'_, PyDict>,
    right_key_expr_dict: &Bound<'_, PyDict>,
    join_type: &str,
    alias: &str,
) -> PyResult<RustTable> {
    // Validate and extract tables and schemas
    let (df_left, df_right, schema_left, schema_right) = validate_join_tables(table, other)?;

    // Extract and validate join keys
    let (left_col_names, right_col_names) =
        extract_and_validate_join_keys(left_key_expr_dict, right_key_expr_dict, &schema_left, &schema_right)?;

    // Map join type string to DataFusion JoinType
    let df_join_type = match join_type {
        "inner" => JoinType::Inner,
        "left" => JoinType::Left,
        "right" => JoinType::Right,
        "full" => JoinType::Full,
        _ => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Unknown join type: {}", join_type),
            ))
        }
    };

    // Rename right table join keys
    let (df_right_renamed, temp_right_col_names) =
        rename_right_table_keys(&df_right, &schema_right, &right_col_names)?;

    // Build mapping of original names to temp names for ALL columns
    let renamed_mapping: Vec<(String, String)> = schema_right
        .fields()
        .iter()
        .zip(temp_right_col_names.iter())
        .map(|(field, temp)| (field.name().to_string(), temp.clone()))
        .collect();

    // Find indices of join key columns in the schema
    let mut join_key_indices = Vec::new();
    for (idx, field) in schema_right.fields().iter().enumerate() {
        if right_col_names.contains(&field.name().to_string()) {
            join_key_indices.push(idx);
        }
    }

    // Extract the temp names for join keys (in the same order as right_col_names)
    let right_join_keys: Vec<&str> = right_col_names
        .iter()
        .filter_map(|col_name| {
            // Find the index of this column in the schema
            schema_right
                .fields()
                .iter()
                .position(|f| f.name() == col_name)
                .map(|idx| temp_right_col_names[idx].as_str())
        })
        .collect();

    // Execute join
    let left_join_keys: Vec<&str> = left_col_names.iter().map(|s| s.as_str()).collect();

    let joined_df = execute_join(
        &df_left,
        &df_right_renamed,
        left_join_keys.as_slice(),
        right_join_keys.as_slice(),
        df_join_type,
    )?;

    // Build final schema and rename columns
    let (final_arrow_schema, final_df) =
        build_final_schema_and_select(&joined_df, &schema_left, &schema_right, alias, &renamed_mapping)?;

    // Return new RustTable
    Ok(RustTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(final_df)),
        schema: Some(Arc::new(final_arrow_schema)),
        sort_exprs: Vec::new(),
    })
}

/// Pivot table from long to wide format using SQL CASE WHEN aggregation
///
/// Transforms data where unique values in a column become new columns,
/// and row groups are aggregated based on specified columns.
///
/// Example:
///   Input: year, region, amount
///   pivot(index="year", columns="region", values="amount", agg_fn="sum")
///   Output: year, West, East, Central
///
/// SQL Pattern:
///   SELECT year,
///       SUM(CASE WHEN region = 'West' THEN amount ELSE NULL END) as "West",
///       SUM(CASE WHEN region = 'East' THEN amount ELSE NULL END) as "East"
///   FROM table GROUP BY year
pub fn pivot_impl(
    table: &RustTable,
    index_cols: Vec<String>,
    pivot_col: String,
    value_col: String,
    agg_fn: String,
) -> PyResult<RustTable> {
    // Validate inputs
    if table.dataframe.is_none() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Cannot pivot empty table",
        ));
    }

    let df = table.dataframe.as_ref().unwrap();
    let schema = table.schema.as_ref().unwrap();

    // Validate that all required columns exist
    let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().to_string()).collect();
    
    for col in &index_cols {
        if !col_names.contains(col) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Index column '{}' not found in table", col),
            ));
        }
    }
    
    if !col_names.contains(&pivot_col) {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!("Pivot column '{}' not found in table", pivot_col),
        ));
    }
    
    if !col_names.contains(&value_col) {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!("Value column '{}' not found in table", value_col),
        ));
    }

    // Validate aggregation function
    let agg_fn_upper = agg_fn.to_uppercase();
    match agg_fn_upper.as_str() {
        "SUM" | "MEAN" | "COUNT" | "MIN" | "MAX" => {},
        _ => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Invalid aggregation function '{}'. Must be one of: sum, mean, count, min, max", agg_fn),
            ));
        }
    }

    // Find the index of the pivot column in the schema
    let pivot_col_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == &pivot_col)
        .ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Pivot column '{}' not found", pivot_col),
            )
        })?;

    // Collect all batches from the dataframe
    let all_batches = RUNTIME
        .block_on((**df).clone().collect())
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to collect data from table: {}",
                e
            ))
        })?;

    // Extract distinct pivot values from all batches
    let mut pivot_values_set = std::collections::HashSet::new();
    for batch in &all_batches {
        let col = batch.column(pivot_col_idx);
        match col.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                if let Some(string_col) = col.as_any().downcast_ref::<array::StringArray>() {
                    for i in 0..string_col.len() {
                        if !string_col.is_null(i) {
                            pivot_values_set.insert(string_col.value(i).to_string());
                        }
                    }
                }
            }
            DataType::Int32 => {
                if let Some(int_col) = col.as_any().downcast_ref::<array::Int32Array>() {
                    for i in 0..int_col.len() {
                        if !int_col.is_null(i) {
                            pivot_values_set.insert(int_col.value(i).to_string());
                        }
                    }
                }
            }
            DataType::Int64 => {
                if let Some(int_col) = col.as_any().downcast_ref::<array::Int64Array>() {
                    for i in 0..int_col.len() {
                        if !int_col.is_null(i) {
                            pivot_values_set.insert(int_col.value(i).to_string());
                        }
                    }
                }
            }
            DataType::Float32 => {
                if let Some(float_col) = col.as_any().downcast_ref::<array::Float32Array>() {
                    for i in 0..float_col.len() {
                        if !float_col.is_null(i) {
                            pivot_values_set.insert(float_col.value(i).to_string());
                        }
                    }
                }
            }
            DataType::Float64 => {
                if let Some(float_col) = col.as_any().downcast_ref::<array::Float64Array>() {
                    for i in 0..float_col.len() {
                        if !float_col.is_null(i) {
                            pivot_values_set.insert(float_col.value(i).to_string());
                        }
                    }
                }
            }
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Pivot column must be string, int, or float type",
                ));
            }
        }
    }

    // Sort pivot values for consistent output
    let mut pivot_values: Vec<String> = pivot_values_set.into_iter().collect();
    pivot_values.sort();

    // Register the source data as a temporary table
    let source_table_name = "__pivot_source";
    let source_mem_table =
        MemTable::try_new(schema.clone(), vec![all_batches]).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create source table: {}",
                e
            ))
        })?;

    table.session
        .register_table(source_table_name, Arc::new(source_mem_table))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to register source table: {}",
                e
            ))
        })?;

    // Step 1: Build CASE WHEN expressions
    let mut case_exprs = Vec::new();
    for val in &pivot_values {
        // Determine if value is numeric or string
        let value_expr = if val.parse::<f64>().is_ok() {
            // Numeric value
            format!(
                "{}(CASE WHEN {} = {} THEN {} ELSE NULL END) as \"{}\"",
                agg_fn_upper, pivot_col, val, value_col, val
            )
        } else {
            // String value (quote it)
            format!(
                "{}(CASE WHEN {} = '{}' THEN {} ELSE NULL END) as \"{}\"",
                agg_fn_upper, pivot_col, val.replace("'", "''"), value_col, val
            )
        };
        case_exprs.push(value_expr);
    }

    // Step 2: Build full pivot query
    let index_cols_str = index_cols.join(", ");
    let case_str = case_exprs.join(", ");
    let pivot_sql = format!(
        "SELECT {}, {} FROM {} GROUP BY {}",
        index_cols_str, case_str, source_table_name, index_cols_str
    );

    // Step 3: Execute pivot query
    let result_df = RUNTIME.block_on(table.session.sql(&pivot_sql))
        .map_err(|e| {
            let _ = table.session.deregister_table(source_table_name);
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to execute pivot query: {}",
                e
            ))
        })?;

    // Step 4: Collect result
    let result_batches = RUNTIME
        .block_on(result_df.collect())
        .map_err(|e| {
            let _ = table.session.deregister_table(source_table_name);
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to collect pivot results: {}",
                e
            ))
        })?;

    // Deregister source table
    let _ = table.session.deregister_table(source_table_name);

    // Create result schema from the result batches' schema
    let result_schema = if !result_batches.is_empty() {
        result_batches[0].schema()
    } else {
        Arc::new(ArrowSchema::empty())
    };

    // Create result table and dataframe
    let result_mem_table =
        MemTable::try_new(result_schema.clone(), vec![result_batches]).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create result table: {}",
                e
            ))
        })?;

    let final_df = table.session.read_table(Arc::new(result_mem_table))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create result dataframe: {}",
                e
            ))
        })?;

    // Return new RustTable
    Ok(RustTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(final_df)),
        schema: Some(result_schema),
        sort_exprs: Vec::new(),
    })
}

/// Derive columns using raw SQL window expressions
///
/// This function executes a SELECT query with the provided SQL expressions,
/// typically containing window functions like FIRST_VALUE, LAST_VALUE, COUNT, etc.
///
/// # Arguments
///
/// * `table` - The source table (must already have __group_id__ and __rn__ columns)
/// * `derive_exprs` - HashMap mapping column name -> SQL expression string
///
/// # Returns
///
/// A new RustTable with the derived columns added (and __group_id__, __rn__ removed)
pub fn derive_window_sql_impl(
    table: &RustTable,
    derive_exprs: std::collections::HashMap<String, String>,
) -> PyResult<RustTable> {
    // Validate table exists
    let df = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "No data loaded. Call read_csv() first.",
        )
    })?;
    
    let schema = table.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "No schema available.",
        )
    })?;

    // Register the current table
    let temp_table_name = "__ltseq_derive_temp";
    
    let current_batches = RUNTIME
        .block_on(async {
            (**df).clone().collect().await
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

    // Build SELECT clause: all existing columns + derived expressions
    let mut select_parts: Vec<String> = Vec::new();
    
    // Add all existing columns except __group_id__ and __rn__
    for field in schema.fields() {
        let col_name = field.name();
        if col_name != "__group_id__" && col_name != "__rn__" {
            select_parts.push(format!("\"{}\"", col_name));
        }
    }
    
    // Add derived columns
    for (col_name, sql_expr) in &derive_exprs {
        select_parts.push(format!("{} AS \"{}\"", sql_expr, col_name));
    }

    // Build SQL query
    let sql_query = format!(
        "SELECT {} FROM {}",
        select_parts.join(", "),
        temp_table_name
    );

    // Execute query
    let result_batches = RUNTIME
        .block_on(async {
            let result_df = table.session.sql(&sql_query)
                .await
                .map_err(|e| format!("Failed to execute derive query: {} -- SQL: {}", e, sql_query))?;
            
            result_df.collect()
                .await
                .map_err(|e| format!("Failed to collect derive results: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Deregister temporary table
    table.session.deregister_table(temp_table_name)
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to deregister temp table: {}",
                e
            ))
        })?;

    // Create result RustTable
    if result_batches.is_empty() {
        // Build schema for empty result
        let mut result_fields: Vec<datafusion::arrow::datatypes::Field> = Vec::new();
        for field in schema.fields() {
            let col_name = field.name();
            if col_name != "__group_id__" && col_name != "__rn__" {
                result_fields.push((**field).clone());
            }
        }
        // Add derived columns as nullable Float64 (common for aggregates)
        for col_name in derive_exprs.keys() {
            result_fields.push(datafusion::arrow::datatypes::Field::new(
                col_name,
                datafusion::arrow::datatypes::DataType::Float64,
                true,
            ));
        }
        let empty_schema = Arc::new(ArrowSchema::new(result_fields));
        return Ok(RustTable {
            session: Arc::clone(&table.session),
            dataframe: None,
            schema: Some(empty_schema),
            sort_exprs: Vec::new(),
        });
    }

    let result_schema = result_batches[0].schema();
    let result_mem_table = MemTable::try_new(Arc::clone(&result_schema), vec![result_batches]).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to create result table: {}",
            e
        ))
    })?;

    // Create DataFrame from the result using read_table
    let result_df = table.session.read_table(Arc::new(result_mem_table)).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to create result table: {}",
            e
        ))
    })?;

    Ok(RustTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(result_df)),
        schema: Some(Arc::clone(&result_schema)),
        sort_exprs: Vec::new(),
    })
}
