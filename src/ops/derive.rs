//! Derived column operations
//!
//! This module contains helper functions for deriving new columns from existing data,
//! including window functions like shifts, rolling windows, and cumulative sums.
//!
//! # Architecture
//!
//! Due to PyO3 constraints (only one #[pymethods] impl block per struct), all actual
//! operation implementations are extracted as helper functions here. The #[pymethods]
//! impl block in lib.rs delegates to these helpers via simple stubs.
//!
//! # Derivation Types
//!
//! The module handles two types of derivations:
//!
//! 1. **Standard Expressions** (derive_impl):
//!    - Non-window expressions evaluated directly via DataFusion
//!    - Examples: arithmetic, comparisons, string operations
//!    - Performance: Single table scan
//!
//! 2. **Window Functions** (derive_with_window_functions_impl):
//!    - SQL-based window operations: shift, lag, lead, rolling operations
//!    - Examples: rolling sums, cumulative windows, ordered shifts
//!    - Performance: Requires sorting for windowing
//!
//! # Implementation Pattern
//!
//! Each derivation:
//! 1. Validates schema and dataframe exist
//! 2. Deserializes Python expression dicts → PyExpr
//! 3. Transpiles PyExpr → DataFusion Expr (or SQL for window functions)
//! 4. Executes via DataFusion query engine
//! 5. Collects results into new RecordBatches
//! 6. Returns new RustTable with appended columns
//!
//! # Window Function SQL Translation
//!
//! Window functions are converted to SQL like:
//! ```sql
//! SELECT *,
//!   LAG(column, 1) OVER (ORDER BY sort_key ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS shifted
//! FROM table
//! ```

use crate::RustTable;
use crate::types::{dict_to_py_expr, PyExpr};
use crate::transpiler::{pyexpr_to_datafusion, pyexpr_to_sql};
use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema, DataType};
use datafusion::prelude::*;
use datafusion::datasource::MemTable;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

use crate::engine::RUNTIME;

/// Helper function to derive new columns from expressions
///
/// This performs non-window function derivations using DataFusion expressions.
/// For window functions, it delegates to derive_with_window_functions_impl.
///
/// Args:
///     table: Reference to RustTable
///     derived_cols: Dictionary mapping column names to expression dicts
pub fn derive_impl(table: &RustTable, derived_cols: &Bound<'_, PyDict>) -> PyResult<RustTable> {
    // 1. Get schema and DataFrame
    let schema = table.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Schema not available. Call read_csv() first.",
        )
    })?;

    let df = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "No data loaded. Call read_csv() first.",
        )
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
        return derive_with_window_functions_impl(table, derived_cols);
    }

    // 4. Standard (non-window) derivation using DataFusion expressions
    // Build select expressions: keep all existing columns + add derived ones
    let mut select_exprs = Vec::new();

    // Add all existing columns
    for field in schema.fields() {
        select_exprs.push(col(field.name()));
    }

    // Process each derived column
    for (col_name, expr_item) in derived_cols.iter() {
        let col_name_str = col_name.extract::<String>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Column name must be string")
        })?;

        let expr_dict = expr_item.cast::<PyDict>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Expression must be dict")
        })?;

        // Deserialize
        let py_expr = dict_to_py_expr(&expr_dict)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        // Transpile
        let df_expr = pyexpr_to_datafusion(py_expr, schema)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;

        // Add to select with alias
        select_exprs.push(df_expr.alias(&col_name_str));
    }

    // Apply select to add derived columns
    let derived_df = RUNTIME
        .block_on(async {
            (**df)
                .clone()
                .select(select_exprs)
                .map_err(|e| format!("Derive execution failed: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Get new schema from derived DataFrame (type inference!)
    let df_schema = derived_df.schema();
    let arrow_fields: Vec<Field> = df_schema.fields().iter().map(|f| (**f).clone()).collect();
    let new_arrow_schema = ArrowSchema::new(arrow_fields);

    // Return new RustTable with updated schema
    Ok(RustTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(derived_df)),
        schema: Some(Arc::new(new_arrow_schema)),
        sort_exprs: table.sort_exprs.clone(),
    })
}

/// Helper function to derive columns with window functions using SQL
///
/// This handles complex window functions like shifts, rolling windows, and differences
/// by translating them to SQL OVER clauses and executing via DataFusion's SQL engine.
///
/// Args:
///     table: Reference to RustTable
///     derived_cols: Dictionary mapping column names to expression dicts (may contain window functions)
pub fn derive_with_window_functions_impl(
    table: &RustTable,
    derived_cols: &Bound<'_, PyDict>,
) -> PyResult<RustTable> {
    let schema = table.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Schema not available")
    })?;

    let df = table
        .dataframe
        .as_ref()
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No data loaded"))?;

    RUNTIME.block_on(async {
        // Get the data from the current DataFrame as record batches
        let current_batches = (**df).clone().collect().await.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to collect current DataFrame: {}",
                e
            ))
        })?;

        // Register a temporary table with the current data
        let temp_table_name = "__ltseq_temp";
        let arrow_schema = Arc::new((**schema).clone());
        let temp_table =
            MemTable::try_new(arrow_schema, vec![current_batches]).map_err(|e| {
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
                    "Failed to register temporary table: {}",
                    e
                ))
            })?;

        // Build the SELECT clause with all columns + derived ones
        let mut select_parts = Vec::new();

        // Add all existing columns
        for field in schema.fields() {
            select_parts.push(format!("\"{}\"", field.name()));
        }

        // Add derived columns
        for (col_name, expr_item) in derived_cols.iter() {
            let col_name_str = col_name.extract::<String>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("Column name must be string")
            })?;

            let expr_dict = expr_item.cast::<PyDict>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("Expression must be dict")
            })?;

            let py_expr = dict_to_py_expr(&expr_dict)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

            // Convert expression to SQL
            let expr_sql = pyexpr_to_sql(&py_expr, schema)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;

            // Check if this is a window function and handle rolling aggregations and diff
            let is_rolling_agg = expr_sql.contains("__ROLLING_");
            let is_diff = expr_sql.contains("__DIFF_");

            let full_expr =
                if is_rolling_agg {
                    // Handle rolling aggregations: replace marker with actual window frame
                    // Pattern: __ROLLING_FUNC__(col)__size
                    // Replace with: FUNC(col) OVER (ORDER BY ... ROWS BETWEEN size-1 PRECEDING AND CURRENT ROW)
                    let window_size_start = expr_sql.rfind("__").unwrap_or(0) + 2;
                    let window_size_str = expr_sql[window_size_start..]
                        .trim_end_matches(|c: char| !c.is_ascii_digit());
                    let window_size = window_size_str.parse::<i32>().unwrap_or(1);

                    // Extract the FUNC(col) part
                    let start = expr_sql.find("__ROLLING_").unwrap_or(0) + 10; // len("__ROLLING_")
                    let end = expr_sql.find("__(").unwrap_or(start);
                    let func_part = &expr_sql[start..end];

                    let col_start = expr_sql.find("__(").unwrap_or(0) + 3; // len("__(")
                    let col_end = expr_sql.rfind(")__").unwrap_or(expr_sql.len());
                    let col_part = &expr_sql[col_start..col_end];

                    let order_by = if !table.sort_exprs.is_empty() {
                        table
                            .sort_exprs
                            .iter()
                            .map(|col| format!("\"{}\"", col))
                            .collect::<Vec<_>>()
                            .join(", ")
                    } else {
                        "".to_string()
                    };

                    if !order_by.is_empty() {
                        format!(
                          "{}({}) OVER (ORDER BY {} ROWS BETWEEN {} PRECEDING AND CURRENT ROW)",
                          func_part, col_part, order_by, window_size - 1
                      )
                    } else {
                        format!(
                            "{}({}) OVER (ROWS BETWEEN {} PRECEDING AND CURRENT ROW)",
                            func_part,
                            col_part,
                            window_size - 1
                        )
                    }
                } else if is_diff {
                    // Handle diff: replace marker with actual calculation
                    // Pattern: __DIFF_periods__(col)__periods
                    // Replace with: (col - LAG(col, periods)) OVER (ORDER BY ...)
                    let diff_start = expr_sql.find("__DIFF_").unwrap_or(0);
                    let periods_start = diff_start + 7; // len("__DIFF_")
                    let periods_end =
                        expr_sql[periods_start..].find("__").unwrap_or(0) + periods_start;
                    let periods_str = &expr_sql[periods_start..periods_end];

                    let col_start = expr_sql.find("__(").unwrap_or(0) + 3; // len("__(")
                    let col_end = expr_sql.rfind(")__").unwrap_or(expr_sql.len());
                    let col_part = &expr_sql[col_start..col_end];

                    let order_by = if !table.sort_exprs.is_empty() {
                        table
                            .sort_exprs
                            .iter()
                            .map(|col| format!("\"{}\"", col))
                            .collect::<Vec<_>>()
                            .join(", ")
                    } else {
                        "".to_string()
                    };

                    // We need to apply OVER clause to LAG
                    let diff_expr = if !order_by.is_empty() {
                        format!(
                            "({} - LAG({}, {}) OVER (ORDER BY {}))",
                            col_part, col_part, periods_str, order_by
                        )
                    } else {
                        format!(
                            "({} - LAG({}, {}) OVER ())",
                            col_part, col_part, periods_str
                        )
                    };

                    diff_expr
                } else if expr_sql.contains("LAG(") || expr_sql.contains("LEAD(") {
                    // Handle shift() window functions - LAG/LEAD may be nested in expressions
                    // Use helper function to apply OVER to ALL instances
                    let order_by = if !table.sort_exprs.is_empty() {
                        table
                            .sort_exprs
                            .iter()
                            .map(|col| format!("\"{}\"", col))
                            .collect::<Vec<_>>()
                            .join(", ")
                    } else {
                        "".to_string()
                    };
                    crate::apply_over_to_window_functions(&expr_sql, &order_by)
                } else {
                    expr_sql
                };

            select_parts.push(format!("{} AS \"{}\"", full_expr, col_name_str));
        }

        // Build and execute the SQL query
        let sql = format!(
            "SELECT {} FROM \"{}\"",
            select_parts.join(", "),
            temp_table_name
        );

        // Execute SQL using sql() which returns a DataFrame directly
        let new_df = table.session.sql(&sql).await.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Window function query failed: {}",
                e
            ))
        })?;

        // Get schema from the new DataFrame
        let df_schema = new_df.schema();
        let arrow_fields: Vec<Field> =
            df_schema.fields().iter().map(|f| (**f).clone()).collect();
        let new_arrow_schema = ArrowSchema::new(arrow_fields);

        // Return new RustTable
        Ok(RustTable {
            session: Arc::clone(&table.session),
            dataframe: Some(Arc::new(new_df)),
            schema: Some(Arc::new(new_arrow_schema)),
            sort_exprs: table.sort_exprs.clone(),
        })
    })
}

/// Helper function to add cumulative sum columns
///
/// Computes cumulative sum for each specified expression and adds the result as a new column.
/// The new column name is `{original_name}_cumsum`.
///
/// Uses window functions: `SUM(col) OVER (ORDER BY ... ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`
///
/// Args:
///     table: Reference to RustTable
///     cum_exprs: Vector of serialized expression dicts (usually just Column references)
pub fn cum_sum_impl(table: &RustTable, cum_exprs: Vec<Bound<'_, PyDict>>) -> PyResult<RustTable> {
    // If no dataframe, return empty result (for unit tests)
    if table.dataframe.is_none() {
        return Ok(RustTable {
            session: Arc::clone(&table.session),
            dataframe: None,
            schema: table.schema.as_ref().map(|s| Arc::clone(s)),
            sort_exprs: table.sort_exprs.clone(),
        });
    }

    let schema = table.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Schema not available. Call read_csv() first.",
        )
    })?;

    let df = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "No data loaded. Call read_csv() first.",
        )
    })?;

    RUNTIME.block_on(async {
        // Get the data from the current DataFrame as record batches
        let current_batches = (**df).clone().collect().await.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to collect current DataFrame: {}",
                e
            ))
        })?;

        // Check if the table is empty - if so, just return with new schema
        let is_empty = current_batches.iter().all(|batch| batch.num_rows() == 0);
        if is_empty {
            // Build schema with cumulative sum columns added
            let mut new_fields = schema.fields().to_vec();
            for (idx, expr_item) in cum_exprs.iter().enumerate() {
                let expr_dict = expr_item.cast::<PyDict>().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("Expression must be dict")
                })?;

                let py_expr = dict_to_py_expr(&expr_dict).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
                })?;

                let col_name = if let PyExpr::Column(name) = &py_expr {
                    name.clone()
                } else {
                    format!("cum_sum_{}", idx)
                };

                // Add cumsum column with float64 type
                let cumsum_col_name = format!("{}_cumsum", col_name);
                new_fields.push(Arc::new(Field::new(
                    cumsum_col_name,
                    DataType::Float64,
                    true,
                )));
            }

            let new_arrow_schema = ArrowSchema::new(new_fields);

            // Create empty MemTable with new schema
            let empty_table = MemTable::try_new(
                Arc::new(new_arrow_schema.clone()),
                vec![current_batches], // empty batches with new schema
            )
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to create empty memory table: {}",
                    e
                ))
            })?;

            // Register and query to get proper empty DataFrame
            let temp_table_name = "__ltseq_temp_empty";
            table
                .session
                .register_table(temp_table_name, Arc::new(empty_table))
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to register empty table: {}",
                        e
                    ))
                })?;

            // Build SELECT for all columns
            let mut select_parts = Vec::new();
            for field in schema.fields() {
                select_parts.push(format!("\"{}\"", field.name()));
            }
            for (idx, expr_item) in cum_exprs.iter().enumerate() {
                let expr_dict = expr_item.cast::<PyDict>().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("Expression must be dict")
                })?;

                let py_expr = dict_to_py_expr(&expr_dict).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
                })?;

                let col_name = if let PyExpr::Column(name) = &py_expr {
                    name.clone()
                } else {
                    format!("cum_sum_{}", idx)
                };

                let cumsum_col_name = format!("{}_cumsum", col_name);
                select_parts.push(format!("CAST(0.0 AS FLOAT) AS \"{}\"", cumsum_col_name));
            }

            let sql = format!(
                "SELECT {} FROM \"{}\"",
                select_parts.join(", "),
                temp_table_name
            );

            let new_df = table.session.sql(&sql).await.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "cum_sum query failed for empty table: {}",
                    e
                ))
            })?;

            let df_schema = new_df.schema();
            let arrow_fields: Vec<Field> =
                df_schema.fields().iter().map(|f| (**f).clone()).collect();
            let new_arrow_schema = ArrowSchema::new(arrow_fields);

            return Ok(RustTable {
                session: Arc::clone(&table.session),
                dataframe: Some(Arc::new(new_df)),
                schema: Some(Arc::new(new_arrow_schema)),
                sort_exprs: table.sort_exprs.clone(),
            });
        }

        // Register a temporary table with the current data
        let temp_table_name = "__ltseq_temp";
        let arrow_schema = Arc::new((**schema).clone());
        let temp_table =
            MemTable::try_new(arrow_schema, vec![current_batches]).map_err(|e| {
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
                    "Failed to register temporary table: {}",
                    e
                ))
            })?;

        // Build the SELECT clause with all columns + cumulative sum columns
        let mut select_parts = Vec::new();

        // Add all existing columns
        for field in schema.fields() {
            select_parts.push(format!("\"{}\"", field.name()));
        }

        // Add cumulative sum columns for each input column
        for (idx, expr_item) in cum_exprs.iter().enumerate() {
            let expr_dict = expr_item.cast::<PyDict>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("Expression must be dict")
            })?;

            let py_expr = dict_to_py_expr(&expr_dict)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

            // Convert expression to SQL
            let expr_sql = pyexpr_to_sql(&py_expr, schema)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;

            // Extract column name from expression (if it's a simple Column)
            let col_name = if let PyExpr::Column(name) = &py_expr {
                name.clone()
            } else {
                format!("cum_sum_{}", idx)
            };

            // Validate that the column type is numeric (only for direct Column expressions)
            if let PyExpr::Column(col_name_ref) = &py_expr {
                if let Some(field) = schema.field_with_name(col_name_ref).ok() {
                    let field_type = field.data_type();
                    if !crate::transpiler::is_numeric_type(field_type) {
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                            "cum_sum() requires numeric columns. Column '{}' has type {:?}",
                            col_name_ref, field_type
                        )));
                    }
                }
            }

            // Build cumulative sum: SUM(col) OVER (ORDER BY ... ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            let order_by = if !table.sort_exprs.is_empty() {
                let order_cols = table
                    .sort_exprs
                    .iter()
                    .map(|col| format!("\"{}\"", col))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(" ORDER BY {}", order_cols)
            } else {
                "".to_string()
            };

            let cumsum_expr = if !order_by.is_empty() {
                format!(
                    "SUM({}) OVER ({}ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
                    expr_sql, order_by
                )
            } else {
                format!(
                    "SUM({}) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
                    expr_sql
                )
            };

            let cumsum_col_name = format!("{}_cumsum", col_name);
            select_parts.push(format!("{} AS \"{}\"", cumsum_expr, cumsum_col_name));
        }

        // Build and execute the SQL query
        let sql = format!(
            "SELECT {} FROM \"{}\"",
            select_parts.join(", "),
            temp_table_name
        );

        // Execute SQL using sql() which returns a DataFrame directly
        let new_df = table.session.sql(&sql).await.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "cum_sum query failed: {}",
                e
            ))
        })?;

        // Get schema from the new DataFrame
        let df_schema = new_df.schema();
        let arrow_fields: Vec<Field> =
            df_schema.fields().iter().map(|f| (**f).clone()).collect();
        let new_arrow_schema = ArrowSchema::new(arrow_fields);

        // Return new RustTable
        Ok(RustTable {
            session: Arc::clone(&table.session),
            dataframe: Some(Arc::new(new_df)),
            schema: Some(Arc::new(new_arrow_schema)),
            sort_exprs: table.sort_exprs.clone(),
        })
    })
}
