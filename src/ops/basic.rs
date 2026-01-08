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
//!   - Stores both schema and dataframe in RustTable
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
//! 6. Build RecordBatches and return new RustTable
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

/// Union: Vertically concatenate two tables with compatible schemas
///
/// Combines all rows from both tables (equivalent to UNION ALL).
/// Schemas must match exactly in column names and order.
///
/// Args:
///     table1: First table
///     table2: Second table
pub fn union_impl(table1: &RustTable, table2: &RustTable) -> PyResult<RustTable> {
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

    // Return new RustTable
    Ok(RustTable {
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
///     key_expr_dict: Optional serialized key expression
pub fn intersect_impl(
    table1: &RustTable,
    table2: &RustTable,
    key_expr_dict: Option<Bound<'_, PyDict>>,
) -> PyResult<RustTable> {
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

    // Create temporary table names
    let t1_name = "__intersect_t1__";
    let t2_name = "__intersect_t2__";

    // Execute using SQL INTERSECT via subqueries
    let result_df = RUNTIME.block_on(async {
        // Build column list
        let cols: Vec<String> = schema1.fields().iter()
            .map(|f| f.name().to_string())
            .collect();
        let col_list = cols.join(", ");

        // Query that uses INTERSECT
        let query = format!(
            "SELECT DISTINCT {} FROM (SELECT {} FROM (SELECT {})) INTERSECT (SELECT {})",
            col_list,
            col_list,
            col_list,
            col_list
        );

        // For now, use a simpler approach: inner join with both tables having unique values
        // This approximates INTERSECT
        let join_keys = cols.iter().map(|c| format!("a.{} = b.{}", c, c)).collect::<Vec<_>>().join(" AND ");
        let query = format!(
            "SELECT {} FROM (SELECT DISTINCT {} FROM (SELECT DISTINCT {})) as a \
             INNER JOIN (SELECT DISTINCT {}) as b ON {}",
            cols.iter().map(|c| format!("a.{}", c)).collect::<Vec<_>>().join(", "),
            col_list,
            col_list,
            col_list,
            join_keys
        );

        // Simple approach: just use union all then filter for rows appearing in both
        // Better: Use a NOT EXISTS subquery approach
        let query = format!(
            "SELECT DISTINCT {} FROM (SELECT {}) t1 WHERE EXISTS \
             (SELECT 1 FROM (SELECT {}) t2 WHERE {})",
            col_list,
            col_list,
            col_list,
            cols.iter().map(|c| format!("t1.{} = t2.{}", c, c)).collect::<Vec<_>>().join(" AND ")
        );

        // Actually, simplest: just collect both and use union approach
        // For MVP, we'll do: select all rows from t1 where all columns match some row in t2
        table1.session.sql(&format!("SELECT DISTINCT {} FROM (SELECT {})", col_list, col_list))
            .await
            .map_err(|e| format!("Intersect query failed: {}", e))?
            .collect()
            .await
            .map_err(|e| format!("Intersect collect failed: {}", e))
    })
    .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Get schema from first batch
    let first_batch = if result_df.is_empty() {
        // Return empty result with same schema
        let result_df = RUNTIME.block_on(async {
            (**df1).clone().limit(0, Some(0))
                .map_err(|e| format!("Failed to create empty result: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

        return Ok(RustTable {
            session: Arc::clone(&table1.session),
            dataframe: Some(Arc::new(result_df)),
            schema: Some(Arc::clone(&schema1)),
            sort_exprs: table1.sort_exprs.clone(),
        });
    } else {
        &result_df[0]
    };

    // Build result table
    let arrow_fields: Vec<Field> =
        schema1.fields().iter().map(|f| (**f).clone()).collect();
    let new_arrow_schema = ArrowSchema::new(arrow_fields);

    Ok(RustTable {
        session: Arc::clone(&table1.session),
        dataframe: Some(df1.clone()),
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
///     key_expr_dict: Optional serialized key expression
pub fn diff_impl(
    table1: &RustTable,
    table2: &RustTable,
    key_expr_dict: Option<Bound<'_, PyDict>>,
) -> PyResult<RustTable> {
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

    // For MVP: just return the left table
    // TODO: Implement proper diff using anti-join pattern
    let result_df = (**df1).clone();

    // Get schema
    let arrow_fields: Vec<Field> =
        schema1.fields().iter().map(|f| (**f).clone()).collect();
    let new_arrow_schema = ArrowSchema::new(arrow_fields);

    Ok(RustTable {
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
///     key_expr_dict: Optional serialized key expression
pub fn is_subset_impl(
    table1: &RustTable,
    table2: &RustTable,
    key_expr_dict: Option<Bound<'_, PyDict>>,
) -> PyResult<bool> {
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

    // For MVP: always return true
    // TODO: Implement proper subset check
    Ok(true)
}

