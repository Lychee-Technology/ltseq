//! Window function operations for derived columns
//!
//! This module handles window operations including:
//! - Shift operations (LAG/LEAD)
//! - Rolling aggregations (SUM, AVG, MIN, MAX, COUNT over windows)
//! - Cumulative sums with window frames
//! - Difference calculations
//!
//! As of Phase 2 optimization, window functions are converted directly to
//! DataFusion's native `Expr::WindowFunction` via the `window_native` transpiler,
//! eliminating the previous collect→MemTable→SQL string round-trip.

use crate::engine::RUNTIME;
use crate::transpiler::pyexpr_to_sql;
use crate::transpiler::pyexpr_to_window_expr;
use crate::types::{dict_to_py_expr, PyExpr};
use crate::LTSeqTable;
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::common::Column;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

/// Apply OVER clause to all nested LAG/LEAD functions in an expression
///
/// This recursively finds all LAG(...) and LEAD(...) calls and adds the proper OVER clause.
/// Used by window function operations to ensure proper SQL generation.
///
/// # Arguments
/// * `expr` - SQL expression string potentially containing LAG/LEAD calls
/// * `order_by` - ORDER BY clause for the OVER clause (empty string for no ordering)
///
/// # Returns
/// The expression with OVER clauses added to all LAG/LEAD functions
pub(crate) fn apply_over_to_window_functions(expr: &str, order_by: &str) -> String {
    let mut result = String::new();
    let mut i = 0;
    let bytes = expr.as_bytes();

    while i < bytes.len() {
        // Look for LAG(
        if i + 4 <= bytes.len() && &expr[i..i + 4] == "LAG(" {
            result.push_str("LAG(");
            i += 4;
            let mut depth = 1;
            while i < bytes.len() && depth > 0 {
                let ch = bytes[i] as char;
                result.push(ch);
                if ch == '(' {
                    depth += 1;
                } else if ch == ')' {
                    depth -= 1;
                }
                i += 1;
            }
            // Add OVER clause
            if !order_by.is_empty() {
                result.push_str(&format!(" OVER (ORDER BY {})", order_by));
            } else {
                result.push_str(" OVER ()");
            }
        }
        // Look for LEAD(
        else if i + 5 <= bytes.len() && &expr[i..i + 5] == "LEAD(" {
            result.push_str("LEAD(");
            i += 5;
            let mut depth = 1;
            while i < bytes.len() && depth > 0 {
                let ch = bytes[i] as char;
                result.push(ch);
                if ch == '(' {
                    depth += 1;
                } else if ch == ')' {
                    depth -= 1;
                }
                i += 1;
            }
            // Add OVER clause
            if !order_by.is_empty() {
                result.push_str(&format!(" OVER (ORDER BY {})", order_by));
            } else {
                result.push_str(" OVER ()");
            }
        } else {
            result.push(bytes[i] as char);
            i += 1;
        }
    }
    result
}

/// Helper function to apply window function SQL transformations
///
/// Converts special window function markers (e.g., __ROLLING_SUM__(col)__size)
/// into proper SQL window expressions with OVER clauses.
fn apply_window_frame(expr_sql: &str, order_by: &str, is_rolling: bool, is_diff: bool) -> String {
    if is_rolling {
        // Handle rolling aggregations: replace marker with actual window frame
        // Pattern: __ROLLING_FUNC__(col)__size
        let window_size_start = expr_sql.rfind("__").unwrap_or(0) + 2;
        let window_size_str =
            expr_sql[window_size_start..].trim_end_matches(|c: char| !c.is_ascii_digit());
        let window_size = window_size_str.parse::<i32>().unwrap_or(1);

        let start = expr_sql.find("__ROLLING_").unwrap_or(0) + 10;
        let end = expr_sql.find("__(").unwrap_or(start);
        let func_part = &expr_sql[start..end];

        let col_start = expr_sql.find("__(").unwrap_or(0) + 3;
        let col_end = expr_sql.rfind(")__").unwrap_or(expr_sql.len());
        let col_part = &expr_sql[col_start..col_end];

        if !order_by.is_empty() {
            format!(
                "{}({}) OVER (ORDER BY {} ROWS BETWEEN {} PRECEDING AND CURRENT ROW)",
                func_part,
                col_part,
                order_by,
                window_size - 1
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
        let diff_start = expr_sql.find("__DIFF_").unwrap_or(0);
        let periods_start = diff_start + 7;
        let periods_end = expr_sql[periods_start..].find("__").unwrap_or(0) + periods_start;
        let periods_str = &expr_sql[periods_start..periods_end];

        let col_start = expr_sql.find("__(").unwrap_or(0) + 3;
        let col_end = expr_sql.rfind(")__").unwrap_or(expr_sql.len());
        let col_part = &expr_sql[col_start..col_end];

        if !order_by.is_empty() {
            format!(
                "({} - LAG({}, {}) OVER (ORDER BY {}))",
                col_part, col_part, periods_str, order_by
            )
        } else {
            format!(
                "({} - LAG({}, {}) OVER ())",
                col_part, col_part, periods_str
            )
        }
    } else if expr_sql.contains("LAG(") || expr_sql.contains("LEAD(") {
        // Handle shift() window functions - apply OVER to LAG/LEAD
        apply_over_to_window_functions(expr_sql, order_by)
    } else {
        expr_sql.to_string()
    }
}

/// Build SELECT parts for derived window function columns
async fn build_derived_select_parts(
    schema: &ArrowSchema,
    parsed_cols: &[(String, PyExpr)],
    table: &LTSeqTable,
) -> PyResult<Vec<String>> {
    let mut select_parts = Vec::new();

    // Add all existing columns
    for field in schema.fields() {
        select_parts.push(format!("\"{}\"", field.name()));
    }

    // Add derived columns
    for (col_name_str, py_expr) in parsed_cols.iter() {
        // Convert expression to SQL
        let expr_sql = pyexpr_to_sql(py_expr, schema)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;

        // Check what type of window function this is
        let is_rolling_agg = expr_sql.contains("__ROLLING_");
        let is_diff = expr_sql.contains("__DIFF_");

        // Build ORDER BY clause from sort expressions
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

        // Apply window frame transformation
        let full_expr = apply_window_frame(&expr_sql, &order_by, is_rolling_agg, is_diff);
        select_parts.push(format!("{} AS \"{}\"", full_expr, col_name_str));
    }

    Ok(select_parts)
}

/// Derive columns with window functions using native DataFusion expressions
///
/// This converts PyExpr window function calls directly to DataFusion `Expr::WindowFunction`
/// and uses `df.select()` to add derived columns — no materialization required.
/// Falls back to SQL path only if native conversion fails.
pub fn derive_with_window_functions_impl(
    table: &LTSeqTable,
    derived_cols: &Bound<'_, PyDict>,
) -> PyResult<LTSeqTable> {
    let schema = table
        .schema
        .as_ref()
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Schema not available"))?;

    let df = table
        .dataframe
        .as_ref()
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No data loaded"))?;

    // Deserialize all expressions once
    let parsed_cols = parse_derived_cols(derived_cols)?;

    derive_with_window_functions_from_parsed(table, schema, df, &parsed_cols)
}

/// Internal entry point when expressions are already parsed (avoids double deserialization).
pub(crate) fn derive_with_window_functions_from_parsed(
    table: &LTSeqTable,
    schema: &ArrowSchema,
    df: &Arc<DataFrame>,
    parsed_cols: &[(String, PyExpr)],
) -> PyResult<LTSeqTable> {
    // NOTE: Arrow shift fast path disabled.
    // The native DataFusion window path (below) stays lazy and benefits from column
    // pruning and predicate pushdown, making it ~150x faster for Parquet sources
    // with many columns (e.g., 105-column ClickBench hits table: 0.28s native vs
    // 42s arrow shift due to eager collect of all columns).

    // Try native path first
    match try_native_window_derive(table, schema, df, parsed_cols) {
        Ok(result) => Ok(result),
        Err(_native_err) => {
            derive_with_window_functions_sql_fallback(table, schema, df, parsed_cols)
        }
    }
}

/// Deserialize derived column expressions from a Python dict into Vec<(String, PyExpr)>.
pub(crate) fn parse_derived_cols(derived_cols: &Bound<'_, PyDict>) -> PyResult<Vec<(String, PyExpr)>> {
    let mut parsed = Vec::with_capacity(derived_cols.len());
    for (col_name, expr_item) in derived_cols.iter() {
        let col_name_str = col_name.extract::<String>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Column name must be string")
        })?;
        let expr_dict = expr_item.cast::<PyDict>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Expression must be dict")
        })?;
        let py_expr = dict_to_py_expr(&expr_dict)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
        parsed.push((col_name_str, py_expr));
    }
    Ok(parsed)
}


/// Native window derive: convert PyExpr window calls to DataFusion Expr and use df.select()
fn try_native_window_derive(
    table: &LTSeqTable,
    schema: &ArrowSchema,
    df: &Arc<DataFrame>,
    parsed_cols: &[(String, PyExpr)],
) -> PyResult<LTSeqTable> {
    // Build the list of select expressions: all existing columns + derived window columns
    let mut all_exprs: Vec<Expr> = Vec::new();

    // Add all existing columns
    for field in schema.fields() {
        all_exprs.push(Expr::Column(Column::new_unqualified(field.name())));
    }

    // Convert each derived column's PyExpr to a native DataFusion window Expr
    for (col_name_str, py_expr) in parsed_cols.iter() {
        // Convert to native DataFusion window expression
        let window_expr = pyexpr_to_window_expr(py_expr.clone(), schema, &table.sort_exprs)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;

        all_exprs.push(window_expr.alias(col_name_str));
    }

    // Apply via df.select() — stays lazy, no materialization!
    let result_df = (**df)
        .clone()
        .select(all_exprs)
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Native window derive failed: {}",
                e
            ))
        })?;

    Ok(LTSeqTable::from_df(
        Arc::clone(&table.session),
        result_df,
        table.sort_exprs.clone(),
        table.source_parquet_path.clone(),
    ))
}

/// SQL fallback for window functions (the old path, kept for edge cases)
fn derive_with_window_functions_sql_fallback(
    table: &LTSeqTable,
    schema: &ArrowSchema,
    df: &Arc<DataFrame>,
    parsed_cols: &[(String, PyExpr)],
) -> PyResult<LTSeqTable> {
    RUNTIME.block_on(async {
        // Get the data from the current DataFrame as record batches
        let current_batches = (**df).clone().collect().await.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to collect current DataFrame: {}",
                e
            ))
        })?;

        // Get schema from the actual batches
        let batch_schema = if let Some(first_batch) = current_batches.first() {
            first_batch.schema()
        } else {
            Arc::new(schema.clone())
        };

        // Use unique temp table name to avoid conflicts
        let temp_table_name = format!("__ltseq_temp_{}", std::process::id());
        let temp_table = MemTable::try_new(Arc::clone(&batch_schema), vec![current_batches])
            .map_err(|e| {
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
                    "Failed to register temporary table: {}",
                    e
                ))
            })?;

        let select_parts = build_derived_select_parts(&batch_schema, parsed_cols, table).await?;

        let sql = format!(
            "SELECT {} FROM \"{}\"",
            select_parts.join(", "),
            temp_table_name
        );

        let new_df = table.session.sql(&sql).await.map_err(|e| {
            let _ = table.session.deregister_table(&temp_table_name);
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Window function query failed: {}",
                e
            ))
        })?;

        let _ = table.session.deregister_table(&temp_table_name);

        Ok(LTSeqTable::from_df(
            Arc::clone(&table.session),
            new_df,
            table.sort_exprs.clone(),
            table.source_parquet_path.clone(),
        ))
    })
}

/// Process cumulative sum expressions and build SELECT parts
async fn build_cumsum_select_parts(
    schema: &ArrowSchema,
    cum_exprs: &[Bound<'_, PyDict>],
    table: &LTSeqTable,
) -> PyResult<Vec<String>> {
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

        // Extract column name from expression
        let col_name = if let PyExpr::Column(name) = &py_expr {
            name.clone()
        } else {
            format!("cum_sum_{}", idx)
        };

        // Validate numeric column
        if let PyExpr::Column(col_name_ref) = &py_expr {
            if let Ok(field) = schema.field_with_name(col_name_ref) {
                let field_type = field.data_type();
                if !crate::transpiler::is_numeric_type(field_type) {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                        "cum_sum() requires numeric columns. Column '{}' has type {:?}",
                        col_name_ref, field_type
                    )));
                }
            }
        }

        // Build ORDER BY clause from sort expressions
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

        // Build cumulative sum expression
        let cumsum_expr = if !order_by.is_empty() {
            format!(
                "SUM({}) OVER ({} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
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

    Ok(select_parts)
}

/// Add cumulative sum columns with proper window functions
///
/// Uses native DataFusion expressions when possible, SQL fallback for edge cases.
pub fn cum_sum_impl(table: &LTSeqTable, cum_exprs: Vec<Bound<'_, PyDict>>) -> PyResult<LTSeqTable> {
    // If no dataframe, return empty result (for unit tests)
    if table.dataframe.is_none() {
        return Ok(LTSeqTable::empty(
            Arc::clone(&table.session),
            table.schema.as_ref().map(Arc::clone),
            table.sort_exprs.clone(),
            table.source_parquet_path.clone(),
        ));
    }

    let (df, schema) = table.require_df_and_schema()?;

    // Try native path first
    match try_native_cum_sum(table, schema, df, &cum_exprs) {
        Ok(result) => Ok(result),
        Err(_native_err) => {
            // Fall back to SQL path
            cum_sum_sql_fallback(table, schema, df, cum_exprs)
        }
    }
}

/// Native cum_sum implementation using DataFusion Expr
fn try_native_cum_sum(
    table: &LTSeqTable,
    schema: &ArrowSchema,
    df: &Arc<DataFrame>,
    cum_exprs: &[Bound<'_, PyDict>],
) -> PyResult<LTSeqTable> {
    use datafusion::logical_expr::expr::Sort;
    use datafusion::logical_expr::expr::WindowFunction as WindowFunctionExpr;
    use datafusion::logical_expr::expr::WindowFunctionParams;
    use datafusion::logical_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition};
    use datafusion::scalar::ScalarValue;

    let mut all_exprs: Vec<Expr> = Vec::new();

    // Add all existing columns
    for field in schema.fields() {
        all_exprs.push(Expr::Column(Column::new_unqualified(field.name())));
    }

    // Build sort expressions
    let order_by: Vec<Sort> = table
        .sort_exprs
        .iter()
        .map(|col_name| Sort {
            expr: Expr::Column(Column::new_unqualified(col_name)),
            asc: true,
            nulls_first: true,
        })
        .collect();

    // Add cumulative sum columns for each input column
    for (idx, expr_item) in cum_exprs.iter().enumerate() {
        let expr_dict = expr_item.cast::<PyDict>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Expression must be dict")
        })?;

        let py_expr = dict_to_py_expr(&expr_dict)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        // Extract column name
        let col_name = if let PyExpr::Column(name) = &py_expr {
            name.clone()
        } else {
            format!("cum_sum_{}", idx)
        };

        // Validate numeric column
        if let PyExpr::Column(col_name_ref) = &py_expr {
            if let Ok(field) = schema.field_with_name(col_name_ref) {
                let field_type = field.data_type();
                if !crate::transpiler::is_numeric_type(field_type) {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                        "cum_sum() requires numeric columns. Column '{}' has type {:?}",
                        col_name_ref, field_type
                    )));
                }
            }
        }

        // Build native cum_sum expression: SUM(col) OVER (ORDER BY ... ROWS UNBOUNDED PRECEDING TO CURRENT ROW)
        let col_expr = crate::transpiler::pyexpr_to_datafusion(py_expr, schema)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;

        let sum_agg = datafusion::functions_aggregate::expr_fn::sum(col_expr);
        let frame = WindowFrame::new_bounds(
            WindowFrameUnits::Rows,
            WindowFrameBound::Preceding(ScalarValue::Null), // UNBOUNDED PRECEDING
            WindowFrameBound::CurrentRow,
        );

        // Extract the AggregateUDF from the sum() expression and construct a WindowFunction directly.
        // The builder API (ExprFunctionExt) does NOT convert aggregates to window functions.
        let cumsum_expr = match sum_agg {
            Expr::AggregateFunction(agg_func) => {
                Expr::WindowFunction(Box::new(WindowFunctionExpr {
                    fun: WindowFunctionDefinition::AggregateUDF(agg_func.func),
                    params: WindowFunctionParams {
                        args: agg_func.params.args,
                        partition_by: vec![],
                        order_by: order_by.clone(),
                        window_frame: frame,
                        filter: None,
                        null_treatment: None,
                        distinct: false,
                    },
                }))
            }
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "Expected AggregateFunction from sum()",
                ));
            }
        };

        let cumsum_col_name = format!("{}_cumsum", col_name);
        all_exprs.push(cumsum_expr.alias(&cumsum_col_name));
    }

    // Apply via df.select() — stays lazy
    let result_df = (**df)
        .clone()
        .select(all_exprs)
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Native cum_sum derive failed: {}",
                e
            ))
        })?;

    Ok(LTSeqTable::from_df(
        Arc::clone(&table.session),
        result_df,
        table.sort_exprs.clone(),
        table.source_parquet_path.clone(),
    ))
}

/// SQL fallback for cum_sum (old path)
fn cum_sum_sql_fallback(
    table: &LTSeqTable,
    schema: &ArrowSchema,
    df: &Arc<DataFrame>,
    cum_exprs: Vec<Bound<'_, PyDict>>,
) -> PyResult<LTSeqTable> {
    RUNTIME.block_on(async {
        let current_batches = (**df).clone().collect().await.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to collect current DataFrame: {}",
                e
            ))
        })?;

        let is_empty = current_batches.iter().all(|batch| batch.num_rows() == 0);
        if is_empty {
            return handle_empty_cum_sum(table, schema, &cum_exprs).await;
        }

        let batch_schema = if let Some(first_batch) = current_batches.first() {
            first_batch.schema()
        } else {
            Arc::new(schema.clone())
        };

        let temp_table_name = format!("__ltseq_cumsum_{}", std::process::id());
        let temp_table = MemTable::try_new(Arc::clone(&batch_schema), vec![current_batches])
            .map_err(|e| {
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
                    "Failed to register temporary table: {}",
                    e
                ))
            })?;

        let select_parts = build_cumsum_select_parts(&batch_schema, &cum_exprs, table).await?;

        let sql = format!(
            "SELECT {} FROM \"{}\"",
            select_parts.join(", "),
            temp_table_name
        );

        let new_df = table.session.sql(&sql).await.map_err(|e| {
            let _ = table.session.deregister_table(&temp_table_name);
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "cum_sum query failed: {}",
                e
            ))
        })?;

        let _ = table.session.deregister_table(&temp_table_name);

        Ok(LTSeqTable::from_df(
            Arc::clone(&table.session),
            new_df,
            table.sort_exprs.clone(),
            table.source_parquet_path.clone(),
        ))
    })
}

/// Helper function to handle empty tables for cumulative sum
async fn handle_empty_cum_sum(
    table: &LTSeqTable,
    schema: &ArrowSchema,
    cum_exprs: &[Bound<'_, PyDict>],
) -> PyResult<LTSeqTable> {
    // Build schema with cumulative sum columns added
    let mut new_fields = schema.fields().to_vec();
    for (idx, expr_item) in cum_exprs.iter().enumerate() {
        let expr_dict = expr_item.cast::<PyDict>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Expression must be dict")
        })?;

        let py_expr = dict_to_py_expr(&expr_dict)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

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

    // Return table with expanded schema but no data
    Ok(LTSeqTable {
        session: Arc::clone(&table.session),
        dataframe: table.dataframe.as_ref().map(|d| Arc::clone(d)),
        schema: Some(Arc::new(new_arrow_schema)),
        sort_exprs: table.sort_exprs.clone(),
        source_parquet_path: table.source_parquet_path.clone(),
    })
}
