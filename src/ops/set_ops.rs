//! Set operations: distinct, union, intersect, diff, is_subset
//!
//! This module contains helper functions for set-theoretic table operations.
//! The actual methods are implemented in the #[pymethods] impl block in lib.rs
//! due to PyO3 constraints (only one #[pymethods] impl block per struct is allowed).
//!
//! # Operations Provided
//!
//! ## Deduplication
//! - **distinct_impl**: Remove duplicate rows based on key columns
//!
//! ## Set Algebra
//! - **union_impl**: Vertically concatenate two tables (UNION ALL)
//! - **intersect_impl**: Return rows present in both tables (INTERSECT)
//! - **diff_impl**: Return rows in first table but not in second (EXCEPT)
//! - **is_subset_impl**: Check if first table is a subset of second
//!
//! # Implementation Pattern
//!
//! All operations return PyResult<T> for seamless Python exception handling.
//! Set operations use SQL-based implementations via DataFusion for correctness
//! and performance.

use crate::engine::RUNTIME;
use crate::LTSeqTable;
use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

/// Helper function to remove duplicate rows
///
/// Args:
///     table: Reference to LTSeqTable
///     key_exprs: Column expressions to determine uniqueness (empty = use all columns)
pub fn distinct_impl(
    table: &LTSeqTable,
    key_exprs: Vec<Bound<'_, PyDict>>,
) -> PyResult<LTSeqTable> {
    // If no dataframe, return empty result (for unit tests)
    if table.dataframe.is_none() {
        return Ok(LTSeqTable {
            session: Arc::clone(&table.session),
            dataframe: None,
            schema: table.schema.as_ref().map(|s| Arc::clone(s)),
            sort_exprs: table.sort_exprs.clone(),
        });
    }

    // Get schema
    let schema = table.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Schema not available. Call read_csv() first.",
        )
    })?;

    // Get DataFrame
    let df = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No data loaded. Call read_csv() first.")
    })?;

    // If no key columns specified, use simple distinct on all columns
    if key_exprs.is_empty() {
        let distinct_df = RUNTIME
            .block_on(async {
                (**df)
                    .clone()
                    .distinct()
                    .map_err(|e| format!("Distinct execution failed: {}", e))
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

        return Ok(LTSeqTable {
            session: Arc::clone(&table.session),
            dataframe: Some(Arc::new(distinct_df)),
            schema: table.schema.as_ref().map(|s| Arc::clone(s)),
            sort_exprs: table.sort_exprs.clone(),
        });
    }

    // Convert key expressions to column names
    let mut key_cols = Vec::new();
    for expr_dict in key_exprs.iter() {
        let py_expr = crate::types::dict_to_py_expr(expr_dict)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        // Get column name from expression
        let col_name = match &py_expr {
            crate::types::PyExpr::Column(name) => name.clone(),
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "distinct() key expressions must be simple column references",
                ))
            }
        };

        // Verify column exists
        if !schema.fields().iter().any(|f| f.name() == &col_name) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Column '{}' not found in schema",
                col_name
            )));
        }

        key_cols.push(col_name);
    }

    // Build SQL query using ROW_NUMBER() window function
    let all_cols: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| format!("\"{}\"", f.name()))
        .collect();
    let all_cols_str = all_cols.join(", ");

    let partition_by = key_cols
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");

    // Use the key columns for ordering (first occurrence based on natural order)
    let order_by = if !key_cols.is_empty() {
        partition_by.clone()
    } else {
        "1".to_string()
    };

    let sql = format!(
        "SELECT {} FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {} ORDER BY {}) as __rn FROM __distinct_source) WHERE __rn = 1",
        all_cols_str, partition_by, order_by
    );

    // Register DataFrame as a temporary table and execute SQL
    let session = &table.session;
    let schema_clone = Arc::clone(schema);
    let distinct_df = RUNTIME
        .block_on(async {
            use datafusion::datasource::MemTable;

            // Collect DataFrame to batches
            let batches = (**df)
                .clone()
                .collect()
                .await
                .map_err(|e| format!("Failed to collect data for distinct: {}", e))?;

            // Create a MemTable from the batches
            let temp_table = MemTable::try_new(schema_clone, vec![batches])
                .map_err(|e| format!("Failed to create temp table: {}", e))?;

            // Register the source DataFrame
            session
                .register_table("__distinct_source", Arc::new(temp_table))
                .map_err(|e| format!("Failed to register source table: {}", e))?;

            // Execute SQL
            let result = session
                .sql(&sql)
                .await
                .map_err(|e| format!("Distinct SQL failed: {}", e))?;

            // Deregister temp table
            let _ = session.deregister_table("__distinct_source");

            Ok(result)
        })
        .map_err(|e: String| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Return new LTSeqTable with distinct data
    Ok(LTSeqTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(distinct_df)),
        schema: table.schema.as_ref().map(|s| Arc::clone(s)),
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
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Left table schema not available")
    })?;

    let schema2 = table2.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Right table schema not available")
    })?;

    // Check that column names match
    let cols1: Vec<&str> = schema1.fields().iter().map(|f| f.name().as_str()).collect();
    let cols2: Vec<&str> = schema2.fields().iter().map(|f| f.name().as_str()).collect();

    if cols1 != cols2 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Schema mismatch: {:?} vs {:?}",
            cols1, cols2
        )));
    }

    let union_df = RUNTIME
        .block_on(async {
            (**df1)
                .clone()
                .union((**df2).clone())
                .map_err(|e| format!("Union failed: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Get schema
    let df_schema = union_df.schema();
    let arrow_fields: Vec<Field> = df_schema.fields().iter().map(|f| (**f).clone()).collect();
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
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Left table schema not available")
    })?;

    let schema2 = table2.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Right table schema not available")
    })?;

    // Determine comparison columns
    let compare_cols: Vec<String> = if let Some(expr_dict) = key_expr_dict {
        // Extract column names from key expression
        extract_key_columns(&expr_dict, schema1)?
    } else {
        // Use all columns from left table
        schema1
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect()
    };

    if compare_cols.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "No columns specified for intersect comparison",
        ));
    }

    // Register both DataFrames as temp tables and execute semi-join SQL
    let result_df = RUNTIME
        .block_on(async {
            // Collect batches from both tables
            let batches1 = (**df1)
                .clone()
                .collect()
                .await
                .map_err(|e| format!("Failed to collect left table: {}", e))?;
            let batches2 = (**df2)
                .clone()
                .collect()
                .await
                .map_err(|e| format!("Failed to collect right table: {}", e))?;

            // Register temp tables
            let t1_name = "__intersect_t1__";
            let t2_name = "__intersect_t2__";

            let temp1 = MemTable::try_new(Arc::clone(schema1), vec![batches1])
                .map_err(|e| format!("Failed to create temp table 1: {}", e))?;
            let temp2 = MemTable::try_new(Arc::clone(schema2), vec![batches2])
                .map_err(|e| format!("Failed to create temp table 2: {}", e))?;

            table1
                .session
                .register_table(t1_name, Arc::new(temp1))
                .map_err(|e| format!("Failed to register temp table 1: {}", e))?;
            table1
                .session
                .register_table(t2_name, Arc::new(temp2))
                .map_err(|e| format!("Failed to register temp table 2: {}", e))?;

            // Build column list for SELECT (all columns from t1)
            let select_cols: Vec<String> = schema1
                .fields()
                .iter()
                .map(|f| format!("t1.\"{}\"", f.name()))
                .collect();

            // Build WHERE EXISTS condition
            let where_conditions: Vec<String> = compare_cols
                .iter()
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

            let result = table1
                .session
                .sql(&sql)
                .await
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
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Left table schema not available")
    })?;

    let schema2 = table2.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Right table schema not available")
    })?;

    // Determine comparison columns
    let compare_cols: Vec<String> = if let Some(expr_dict) = key_expr_dict {
        // Extract column names from key expression
        extract_key_columns(&expr_dict, schema1)?
    } else {
        // Use all columns from left table
        schema1
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect()
    };

    if compare_cols.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "No columns specified for diff comparison",
        ));
    }

    // Register both DataFrames as temp tables and execute anti-join SQL
    let result_df = RUNTIME
        .block_on(async {
            // Collect batches from both tables
            let batches1 = (**df1)
                .clone()
                .collect()
                .await
                .map_err(|e| format!("Failed to collect left table: {}", e))?;
            let batches2 = (**df2)
                .clone()
                .collect()
                .await
                .map_err(|e| format!("Failed to collect right table: {}", e))?;

            // Register temp tables
            let t1_name = "__diff_t1__";
            let t2_name = "__diff_t2__";

            let temp1 = MemTable::try_new(Arc::clone(schema1), vec![batches1])
                .map_err(|e| format!("Failed to create temp table 1: {}", e))?;
            let temp2 = MemTable::try_new(Arc::clone(schema2), vec![batches2])
                .map_err(|e| format!("Failed to create temp table 2: {}", e))?;

            table1
                .session
                .register_table(t1_name, Arc::new(temp1))
                .map_err(|e| format!("Failed to register temp table 1: {}", e))?;
            table1
                .session
                .register_table(t2_name, Arc::new(temp2))
                .map_err(|e| format!("Failed to register temp table 2: {}", e))?;

            // Build column list for SELECT (all columns from t1)
            let select_cols: Vec<String> = schema1
                .fields()
                .iter()
                .map(|f| format!("t1.\"{}\"", f.name()))
                .collect();

            // Build WHERE NOT EXISTS condition
            let where_conditions: Vec<String> = compare_cols
                .iter()
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

            let result = table1
                .session
                .sql(&sql)
                .await
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
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Left table schema not available")
    })?;

    let schema2 = table2.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Right table schema not available")
    })?;

    // Determine comparison columns
    let compare_cols: Vec<String> = if let Some(expr_dict) = key_expr_dict {
        // Extract column names from key expression
        extract_key_columns(&expr_dict, schema1)?
    } else {
        // Use all columns from left table
        schema1
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect()
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
                 For multiple columns, leave on=None to use all columns.",
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
