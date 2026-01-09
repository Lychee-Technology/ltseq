//! Basic table operations: read, filter, select, slice, count, distinct
//!
//! This module contains helper functions for basic table operations.
//! The actual methods are implemented in the #[pymethods] impl block in lib.rs
//! due to PyO3 constraints (only one #[pymethods] impl block per struct is allowed).
//!
//! # Architecture
//!
//! Due to PyO3 constraints, all actual operation implementations are extracted as helper
//! functions here. The #[pymethods] impl block in lib.rs simply delegates to these
//! helpers via one-line stubs like:
//!
//! ```rust,ignore
//! fn read_csv(&mut self, path: &str) -> PyResult<()> {
//!     RUNTIME.block_on(read_csv_impl(self, path.to_string()))
//! }
//! ```
//!
//! # Operations Provided
//!
//! ## I/O Operations
//! - **read_csv_impl**: Load CSV files into DataFusion
//!   - Parses CSV with DataFusion's native reader
//!   - Extracts schema from loaded data
//!   - Stores both schema and dataframe in LTSeqTable
//!
//! ## Query Operations
//! - **filter_impl**: Apply WHERE conditions (predicates)
//! - **select_impl**: Project columns or expressions
//! - **slice_impl**: Get row ranges (limit/offset)
//! - **count_impl**: Count total rows
//! - **distinct_impl**: Remove duplicate rows
//!
//! # Implementation Pattern
//!
//! All query operations follow this pattern:
//! 1. Validate schema and dataframe exist (return error if not)
//! 2. For expression-based ops: deserialize Python dict → PyExpr
//! 3. Transpile PyExpr → DataFusion Expr via pyexpr_to_datafusion()
//! 4. Build DataFusion logical plan (filter, project, etc.)
//! 5. Collect results via RUNTIME.block_on(df.collect())
//! 6. Build RecordBatches and return new LTSeqTable
//!
//! # Error Handling
//!
//! All operations return PyResult<T> for seamless Python exception handling:
//! - Schema/dataframe missing → PyRuntimeError
//! - Expression deserialization fails → PyValueError
//! - DataFusion execution errors → PyRuntimeError with descriptive message
//!
//! # Performance Considerations
//!
//! - **Streaming**: Uses RecordBatches for streaming I/O (not all in-memory)
//! - **Async**: CSV reads are async via DataFusion's session context
//! - **Runtime**: All async operations executed via RUNTIME.block_on() in helpers

use crate::LTSeqTable;
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
///     table: Mutable reference to LTSeqTable
///     path: Path to CSV file
pub async fn read_csv_impl(table: &mut LTSeqTable, path: String) -> PyResult<()> {
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
pub fn count_impl(table: &LTSeqTable) -> PyResult<usize> {
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
///     table: Reference to LTSeqTable
///     expr_dict: Serialized expression dict
pub fn filter_impl(table: &LTSeqTable, expr_dict: &Bound<'_, PyDict>) -> PyResult<LTSeqTable> {
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

    // Return new LTSeqTable
    Ok(LTSeqTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(filtered_df)),
        schema: Some(Arc::new(new_arrow_schema)),
        sort_exprs: table.sort_exprs.clone(),
    })
}

/// Helper function to find the first row matching a predicate
///
/// This is an optimized version of filter() that returns only the first matching row.
/// Much faster than filtering and then taking the first row, especially for large tables.
///
/// Args:
///     table: Reference to LTSeqTable
///     expr_dict: Serialized filter expression
pub fn search_first_impl(table: &LTSeqTable, expr_dict: &Bound<'_, PyDict>) -> PyResult<LTSeqTable> {
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

    // Apply filter and limit to first result
    let result_df = RUNTIME.block_on(async {
        (**df)
            .clone()
            .filter(df_expr)
            .map_err(|e| format!("Filter failed: {}", e))?
            .limit(0, Some(1))
            .map_err(|e| format!("Limit failed: {}", e))
    })
    .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Get schema from the result DataFrame
    let df_schema = result_df.schema();
    let arrow_fields: Vec<Field> =
        df_schema.fields().iter().map(|f| (**f).clone()).collect();
    let new_arrow_schema = ArrowSchema::new(arrow_fields);

    // Return new LTSeqTable
    Ok(LTSeqTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(result_df)),
        schema: Some(Arc::new(new_arrow_schema)),
        sort_exprs: table.sort_exprs.clone(),
    })
}

/// Helper function to select specific columns
///
/// Args:
///     table: Reference to LTSeqTable
///     exprs: List of serialized expression dicts
pub fn select_impl(table: &LTSeqTable, exprs: Vec<Bound<'_, PyDict>>) -> PyResult<LTSeqTable> {
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

    // Return new LTSeqTable
    Ok(LTSeqTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(selected_df)),
        schema: Some(Arc::new(new_arrow_schema)),
        sort_exprs: table.sort_exprs.clone(),
    })
}

/// Helper function to slice the table (offset and limit)
///
/// Args:
///     table: Reference to LTSeqTable
///     offset: Starting row index
///     length: Maximum number of rows to return
pub fn slice_impl(table: &LTSeqTable, offset: i64, length: Option<i64>) -> PyResult<LTSeqTable> {
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

    // Return new LTSeqTable
    Ok(LTSeqTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(sliced_df)),
        schema: Some(Arc::new(new_arrow_schema)),
        sort_exprs: table.sort_exprs.clone(),
    })
}

/// Helper function to remove duplicate rows
///
/// Args:
///     table: Reference to LTSeqTable
///     _key_exprs: Not used in Phase 1 (future: expressions to determine uniqueness)
pub fn distinct_impl(table: &LTSeqTable, _key_exprs: Vec<Bound<'_, PyDict>>) -> PyResult<LTSeqTable> {
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

    // Return new LTSeqTable
    Ok(LTSeqTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(distinct_df)),
        schema: Some(Arc::new(new_arrow_schema)),
        sort_exprs: table.sort_exprs.clone(),
    })
}

/// Union: Vertically concatenate two tables with compatible schemas
///
/// Combines all rows from both tables (equivalent to UNION ALL).
/// Schemas must match exactly in column names and order.
///
/// Args:
///     table1: First table
///     table2: Second table
pub fn union_impl(table1: &LTSeqTable, table2: &LTSeqTable) -> PyResult<LTSeqTable> {
    let df1 = table1.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Left table has no data loaded. Call read_csv() first.",
        )
    })?;

    let df2 = table2.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Right table has no data loaded. Call read_csv() first.",
        )
    })?;

    // Validate schemas match
    let schema1 = table1.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Left table schema not available",
        )
    })?;

    let schema2 = table2.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Right table schema not available",
        )
    })?;

    // Check that column names match
    let cols1: Vec<&str> = schema1.fields().iter().map(|f| f.name().as_str()).collect();
    let cols2: Vec<&str> = schema2.fields().iter().map(|f| f.name().as_str()).collect();

    if cols1 != cols2 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!("Schema mismatch: {:?} vs {:?}", cols1, cols2),
        ));
    }

    let union_df = RUNTIME.block_on(async {
        (**df1)
            .clone()
            .union((**df2).clone())
            .map_err(|e| format!("Union failed: {}", e))
    })
    .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Get schema
    let df_schema = union_df.schema();
    let arrow_fields: Vec<Field> =
        df_schema.fields().iter().map(|f| (**f).clone()).collect();
    let new_arrow_schema = ArrowSchema::new(arrow_fields);

    // Return new LTSeqTable
    Ok(LTSeqTable {
        session: Arc::clone(&table1.session),
        dataframe: Some(Arc::new(union_df)),
        schema: Some(Arc::new(new_arrow_schema)),
        sort_exprs: table1.sort_exprs.clone(),
    })
}

/// Intersect: Return rows present in both tables
///
/// Returns rows that appear in both tables based on key columns.
/// If no key expression provided, uses all columns.
///
/// Args:
///     table1: First table
///     table2: Second table
///     key_expr_dict: Optional serialized key expression (determines comparison columns)
pub fn intersect_impl(
    table1: &LTSeqTable,
    table2: &LTSeqTable,
    key_expr_dict: Option<Bound<'_, PyDict>>,
) -> PyResult<LTSeqTable> {
    use datafusion::datasource::MemTable;

    let df1 = table1.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Left table has no data loaded. Call read_csv() first.",
        )
    })?;

    let df2 = table2.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Right table has no data loaded. Call read_csv() first.",
        )
    })?;

    let schema1 = table1.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Left table schema not available",
        )
    })?;

    let schema2 = table2.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Right table schema not available",
        )
    })?;

    // Determine comparison columns
    let compare_cols: Vec<String> = if let Some(expr_dict) = key_expr_dict {
        // Extract column names from key expression
        extract_key_columns(&expr_dict, schema1)?
    } else {
        // Use all columns from left table
        schema1.fields().iter().map(|f| f.name().to_string()).collect()
    };

    if compare_cols.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "No columns specified for intersect comparison",
        ));
    }

    // Register both DataFrames as temp tables and execute semi-join SQL
    let result_df = RUNTIME.block_on(async {
        // Collect batches from both tables
        let batches1 = (**df1).clone().collect().await.map_err(|e| {
            format!("Failed to collect left table: {}", e)
        })?;
        let batches2 = (**df2).clone().collect().await.map_err(|e| {
            format!("Failed to collect right table: {}", e)
        })?;

        // Register temp tables
        let t1_name = "__intersect_t1__";
        let t2_name = "__intersect_t2__";

        let temp1 = MemTable::try_new(Arc::clone(schema1), vec![batches1])
            .map_err(|e| format!("Failed to create temp table 1: {}", e))?;
        let temp2 = MemTable::try_new(Arc::clone(schema2), vec![batches2])
            .map_err(|e| format!("Failed to create temp table 2: {}", e))?;

        table1.session.register_table(t1_name, Arc::new(temp1))
            .map_err(|e| format!("Failed to register temp table 1: {}", e))?;
        table1.session.register_table(t2_name, Arc::new(temp2))
            .map_err(|e| format!("Failed to register temp table 2: {}", e))?;

        // Build column list for SELECT (all columns from t1)
        let select_cols: Vec<String> = schema1.fields().iter()
            .map(|f| format!("t1.\"{}\"", f.name()))
            .collect();

        // Build WHERE EXISTS condition
        let where_conditions: Vec<String> = compare_cols.iter()
            .map(|c| format!("t1.\"{}\" = t2.\"{}\"", c, c))
            .collect();

        // Semi-join query: SELECT t1.* FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t1.key = t2.key)
        let sql = format!(
            "SELECT DISTINCT {} FROM \"{}\" t1 WHERE EXISTS (SELECT 1 FROM \"{}\" t2 WHERE {})",
            select_cols.join(", "),
            t1_name,
            t2_name,
            where_conditions.join(" AND ")
        );

        let result = table1.session.sql(&sql).await
            .map_err(|e| format!("Intersect query failed: {}", e))?;

        // Deregister temp tables
        let _ = table1.session.deregister_table(t1_name);
        let _ = table1.session.deregister_table(t2_name);

        Ok::<_, String>(result)
    })
    .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Get schema from result
    let df_schema = result_df.schema();
    let arrow_fields: Vec<Field> = df_schema.fields().iter().map(|f| (**f).clone()).collect();
    let new_arrow_schema = ArrowSchema::new(arrow_fields);

    Ok(LTSeqTable {
        session: Arc::clone(&table1.session),
        dataframe: Some(Arc::new(result_df)),
        schema: Some(Arc::new(new_arrow_schema)),
        sort_exprs: table1.sort_exprs.clone(),
    })
}

/// Diff: Return rows in first table but not in second
///
/// Returns rows from the left table that don't appear in the right table.
/// If no key expression provided, uses all columns.
///
/// Args:
///     table1: First table
///     table2: Second table
///     key_expr_dict: Optional serialized key expression (determines comparison columns)
pub fn diff_impl(
    table1: &LTSeqTable,
    table2: &LTSeqTable,
    key_expr_dict: Option<Bound<'_, PyDict>>,
) -> PyResult<LTSeqTable> {
    use datafusion::datasource::MemTable;

    let df1 = table1.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Left table has no data loaded. Call read_csv() first.",
        )
    })?;

    let df2 = table2.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Right table has no data loaded. Call read_csv() first.",
        )
    })?;

    let schema1 = table1.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Left table schema not available",
        )
    })?;

    let schema2 = table2.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Right table schema not available",
        )
    })?;

    // Determine comparison columns
    let compare_cols: Vec<String> = if let Some(expr_dict) = key_expr_dict {
        // Extract column names from key expression
        extract_key_columns(&expr_dict, schema1)?
    } else {
        // Use all columns from left table
        schema1.fields().iter().map(|f| f.name().to_string()).collect()
    };

    if compare_cols.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "No columns specified for diff comparison",
        ));
    }

    // Register both DataFrames as temp tables and execute anti-join SQL
    let result_df = RUNTIME.block_on(async {
        // Collect batches from both tables
        let batches1 = (**df1).clone().collect().await.map_err(|e| {
            format!("Failed to collect left table: {}", e)
        })?;
        let batches2 = (**df2).clone().collect().await.map_err(|e| {
            format!("Failed to collect right table: {}", e)
        })?;

        // Register temp tables
        let t1_name = "__diff_t1__";
        let t2_name = "__diff_t2__";

        let temp1 = MemTable::try_new(Arc::clone(schema1), vec![batches1])
            .map_err(|e| format!("Failed to create temp table 1: {}", e))?;
        let temp2 = MemTable::try_new(Arc::clone(schema2), vec![batches2])
            .map_err(|e| format!("Failed to create temp table 2: {}", e))?;

        table1.session.register_table(t1_name, Arc::new(temp1))
            .map_err(|e| format!("Failed to register temp table 1: {}", e))?;
        table1.session.register_table(t2_name, Arc::new(temp2))
            .map_err(|e| format!("Failed to register temp table 2: {}", e))?;

        // Build column list for SELECT (all columns from t1)
        let select_cols: Vec<String> = schema1.fields().iter()
            .map(|f| format!("t1.\"{}\"", f.name()))
            .collect();

        // Build WHERE NOT EXISTS condition
        let where_conditions: Vec<String> = compare_cols.iter()
            .map(|c| format!("t1.\"{}\" = t2.\"{}\"", c, c))
            .collect();

        // Anti-join query: SELECT t1.* FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t1.key = t2.key)
        let sql = format!(
            "SELECT {} FROM \"{}\" t1 WHERE NOT EXISTS (SELECT 1 FROM \"{}\" t2 WHERE {})",
            select_cols.join(", "),
            t1_name,
            t2_name,
            where_conditions.join(" AND ")
        );

        let result = table1.session.sql(&sql).await
            .map_err(|e| format!("Diff query failed: {}", e))?;

        // Deregister temp tables
        let _ = table1.session.deregister_table(t1_name);
        let _ = table1.session.deregister_table(t2_name);

        Ok::<_, String>(result)
    })
    .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Get schema from result
    let df_schema = result_df.schema();
    let arrow_fields: Vec<Field> = df_schema.fields().iter().map(|f| (**f).clone()).collect();
    let new_arrow_schema = ArrowSchema::new(arrow_fields);

    Ok(LTSeqTable {
        session: Arc::clone(&table1.session),
        dataframe: Some(Arc::new(result_df)),
        schema: Some(Arc::new(new_arrow_schema)),
        sort_exprs: table1.sort_exprs.clone(),
    })
}

/// Is Subset: Check if first table is a subset of second table
///
/// Returns true if all rows in the left table also appear in the right table.
/// If no key expression provided, uses all columns.
///
/// Args:
///     table1: First table (potential subset)
///     table2: Second table (potential superset)
///     key_expr_dict: Optional serialized key expression (determines comparison columns)
pub fn is_subset_impl(
    table1: &LTSeqTable,
    table2: &LTSeqTable,
    key_expr_dict: Option<Bound<'_, PyDict>>,
) -> PyResult<bool> {
    use datafusion::datasource::MemTable;

    let df1 = table1.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Left table has no data loaded. Call read_csv() first.",
        )
    })?;

    let df2 = table2.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Right table has no data loaded. Call read_csv() first.",
        )
    })?;

    let schema1 = table1.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Left table schema not available",
        )
    })?;

    let schema2 = table2.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Right table schema not available",
        )
    })?;

    // Determine comparison columns
    let compare_cols: Vec<String> = if let Some(expr_dict) = key_expr_dict {
        // Extract column names from key expression
        extract_key_columns(&expr_dict, schema1)?
    } else {
        // Use all columns from left table
        schema1.fields().iter().map(|f| f.name().to_string()).collect()
    };

    if compare_cols.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "No columns specified for subset comparison",
        ));
    }

    // t1 is a subset of t2 if diff(t1, t2) is empty
    // i.e., all rows in t1 also exist in t2
    let is_subset = RUNTIME.block_on(async {
        // Collect batches from both tables
        let batches1 = (**df1).clone().collect().await.map_err(|e| {
            format!("Failed to collect left table: {}", e)
        })?;
        let batches2 = (**df2).clone().collect().await.map_err(|e| {
            format!("Failed to collect right table: {}", e)
        })?;

        // Register temp tables
        let t1_name = "__subset_t1__";
        let t2_name = "__subset_t2__";

        let temp1 = MemTable::try_new(Arc::clone(schema1), vec![batches1])
            .map_err(|e| format!("Failed to create temp table 1: {}", e))?;
        let temp2 = MemTable::try_new(Arc::clone(schema2), vec![batches2])
            .map_err(|e| format!("Failed to create temp table 2: {}", e))?;

        table1.session.register_table(t1_name, Arc::new(temp1))
            .map_err(|e| format!("Failed to register temp table 1: {}", e))?;
        table1.session.register_table(t2_name, Arc::new(temp2))
            .map_err(|e| format!("Failed to register temp table 2: {}", e))?;

        // Build WHERE NOT EXISTS condition to count rows NOT in t2
        let where_conditions: Vec<String> = compare_cols.iter()
            .map(|c| format!("t1.\"{}\" = t2.\"{}\"", c, c))
            .collect();

        // Count rows in t1 that don't exist in t2
        // If count is 0, then t1 is a subset of t2
        let sql = format!(
            "SELECT COUNT(*) as cnt FROM \"{}\" t1 WHERE NOT EXISTS (SELECT 1 FROM \"{}\" t2 WHERE {})",
            t1_name,
            t2_name,
            where_conditions.join(" AND ")
        );

        let result = table1.session.sql(&sql).await
            .map_err(|e| format!("Subset query failed: {}", e))?;

        let batches = result.collect().await
            .map_err(|e| format!("Subset collect failed: {}", e))?;

        // Deregister temp tables
        let _ = table1.session.deregister_table(t1_name);
        let _ = table1.session.deregister_table(t2_name);

        // Extract count from result
        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok::<_, String>(true); // Empty table is subset of anything
        }

        use datafusion::arrow::array::Int64Array;
        let cnt_array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| "Failed to extract count".to_string())?;

        let count = cnt_array.value(0);
        Ok(count == 0)
    })
    .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    Ok(is_subset)
}

/// Helper function to extract column names from a key expression dict
///
/// Currently handles single column keys: lambda r: r.id
/// For multiple columns, leave on=None (uses all columns)
fn extract_key_columns(
    expr_dict: &Bound<'_, PyDict>,
    schema: &ArrowSchema,
) -> PyResult<Vec<String>> {
    use crate::types::dict_to_py_expr;
    use crate::types::PyExpr;

    let py_expr = dict_to_py_expr(expr_dict)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    let mut columns = Vec::new();

    // Extract column name(s) from the expression
    match &py_expr {
        PyExpr::Column(name) => columns.push(name.clone()),
        _ => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "on= argument must be a single column reference (e.g., lambda r: r.id). \
                 For multiple columns, leave on=None to use all columns."
            ));
        }
    }

    // Validate columns exist in schema
    for col in &columns {
        if schema.field_with_name(col).is_err() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Column '{}' not found in schema. Available columns: {:?}",
                col,
                schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
            )));
        }
    }

    Ok(columns)
}

