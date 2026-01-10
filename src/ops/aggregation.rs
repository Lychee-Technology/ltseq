//! Aggregation operations for LTSeqTable
//!
//! Provides SQL GROUP BY aggregation and raw SQL WHERE filtering.

use crate::engine::RUNTIME;
use crate::transpiler::pyexpr_to_sql;
use crate::types::{dict_to_py_expr, PyExpr};
use crate::LTSeqTable;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::datasource::MemTable;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

/// Result type for aggregate function extraction
enum AggResult {
    /// Simple aggregate: (func_name, col_name, optional_arg)
    Simple(String, String, Option<String>),
    /// Conditional aggregate: (func_name, predicate_sql, optional_col_name)
    Conditional(String, String, Option<String>),
}

/// Extract aggregate function info from a PyExpr
fn extract_agg_function(py_expr: &PyExpr, schema: &ArrowSchema) -> Option<AggResult> {
    let PyExpr::Call { func, on, args, .. } = py_expr else {
        return None;
    };

    match func.as_str() {
        // Simple aggregations
        "sum" | "count" | "min" | "max" | "avg" | "median" | "variance" | "var" | "stddev"
        | "std" | "mode" => extract_simple_agg(func, on, args),
        "top_k" | "percentile" => extract_agg_with_arg(func, on, args),
        // Conditional aggregations
        "count_if" => extract_count_if(args, schema),
        "sum_if" | "avg_if" | "min_if" | "max_if" => extract_conditional_agg(func, args, schema),
        _ => None,
    }
}

/// Extract simple aggregate function (sum, count, min, max, avg, etc.)
fn extract_simple_agg(func: &str, on: &PyExpr, _args: &[PyExpr]) -> Option<AggResult> {
    if let PyExpr::Column(col_name) = on {
        return Some(AggResult::Simple(func.to_string(), col_name.clone(), None));
    }
    if func == "count" {
        return Some(AggResult::Simple(func.to_string(), "*".to_string(), None));
    }
    None
}

/// Extract aggregate with argument (top_k, percentile)
fn extract_agg_with_arg(func: &str, on: &PyExpr, args: &[PyExpr]) -> Option<AggResult> {
    if let PyExpr::Column(col_name) = on {
        let arg = args.first().and_then(|arg| {
            if let PyExpr::Literal { value, .. } = arg {
                Some(value.clone())
            } else {
                None
            }
        });
        return Some(AggResult::Simple(func.to_string(), col_name.clone(), arg));
    }
    None
}

/// Extract count_if aggregate
fn extract_count_if(args: &[PyExpr], schema: &ArrowSchema) -> Option<AggResult> {
    let predicate = args.first()?;
    let pred_sql = pyexpr_to_sql(predicate, schema).ok()?;
    Some(AggResult::Conditional(
        "count_if".to_string(),
        pred_sql,
        None,
    ))
}

/// Extract conditional aggregate (sum_if, avg_if, min_if, max_if)
fn extract_conditional_agg(func: &str, args: &[PyExpr], schema: &ArrowSchema) -> Option<AggResult> {
    if args.len() < 2 {
        return None;
    }
    let pred_sql = pyexpr_to_sql(&args[0], schema).ok()?;
    let col_sql = pyexpr_to_sql(&args[1], schema).ok()?;
    Some(AggResult::Conditional(
        func.to_string(),
        pred_sql,
        Some(col_sql),
    ))
}

/// Generate SQL expression for an aggregate result
fn agg_result_to_sql(agg_result: AggResult) -> Result<String, String> {
    match agg_result {
        AggResult::Simple(agg_func, col_name, arg_opt) => {
            simple_agg_to_sql(&agg_func, &col_name, arg_opt)
        }
        AggResult::Conditional(agg_func, pred_sql, col_opt) => {
            conditional_agg_to_sql(&agg_func, &pred_sql, col_opt)
        }
    }
}

/// Generate SQL for simple aggregates
fn simple_agg_to_sql(func: &str, col: &str, arg: Option<String>) -> Result<String, String> {
    Ok(match func {
        "sum" => format!("SUM({})", col),
        "count" => {
            if col == "*" {
                "COUNT(*)".to_string()
            } else {
                format!("COUNT({})", col)
            }
        }
        "min" => format!("MIN({})", col),
        "max" => format!("MAX({})", col),
        "avg" => format!("AVG({})", col),
        "median" => format!("MEDIAN({})", col),
        "variance" | "var" => format!("VAR_SAMP({})", col),
        "stddev" | "std" => format!("STDDEV_SAMP({})", col),
        "percentile" => {
            let p = arg
                .as_ref()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.5);
            format!("APPROX_PERCENTILE_CONT({}, {})", col, p)
        }
        "mode" => format!("FIRST_VALUE({} ORDER BY {} ASC)", col, col),
        "top_k" => {
            let k = arg
                .as_ref()
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(10);
            format!(
                "ARRAY_TO_STRING(ARRAY_SLICE(ARRAY_AGG(CAST({} AS DOUBLE) ORDER BY CAST({} AS DOUBLE) DESC), 1, {}), ';')",
                col, col, k
            )
        }
        _ => return Err(format!("Unknown aggregate function: {}", func)),
    })
}

/// Generate SQL for conditional aggregates
fn conditional_agg_to_sql(func: &str, pred: &str, col: Option<String>) -> Result<String, String> {
    let col_sql = col.unwrap_or_else(|| "1".to_string());
    Ok(match func {
        "count_if" => format!("SUM(CASE WHEN {} THEN 1 ELSE 0 END)", pred),
        "sum_if" => format!("SUM(CASE WHEN {} THEN {} ELSE 0 END)", pred, col_sql),
        "avg_if" => format!("AVG(CASE WHEN {} THEN {} ELSE NULL END)", pred, col_sql),
        "min_if" => format!("MIN(CASE WHEN {} THEN {} ELSE NULL END)", pred, col_sql),
        "max_if" => format!("MAX(CASE WHEN {} THEN {} ELSE NULL END)", pred, col_sql),
        _ => return Err(format!("Unknown conditional aggregate: {}", func)),
    })
}

/// Execute SQL query and return result batches
fn execute_sql_query(
    table: &LTSeqTable,
    sql_query: &str,
) -> Result<Vec<datafusion::arrow::array::RecordBatch>, String> {
    RUNTIME.block_on(async {
        let result_df = table
            .session
            .sql(sql_query)
            .await
            .map_err(|e| format!("Failed to execute query: {}", e))?;

        result_df
            .collect()
            .await
            .map_err(|e| format!("Failed to collect results: {}", e))
    })
}

/// Create a result LTSeqTable from record batches
fn create_result_table(
    table: &LTSeqTable,
    batches: Vec<datafusion::arrow::array::RecordBatch>,
) -> PyResult<LTSeqTable> {
    if batches.is_empty() {
        return Ok(LTSeqTable {
            session: Arc::clone(&table.session),
            dataframe: None,
            schema: None,
            sort_exprs: Vec::new(),
        });
    }

    let result_schema = batches[0].schema();
    let mem_table = MemTable::try_new(Arc::clone(&result_schema), vec![batches]).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to create table: {}", e))
    })?;

    let result_df = table.session.read_table(Arc::new(mem_table)).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to read table: {}", e))
    })?;

    Ok(LTSeqTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(result_df)),
        schema: Some(Arc::clone(&result_schema)),
        sort_exprs: Vec::new(),
    })
}

/// Aggregate rows into a summary table with one row per group
pub fn agg_impl(
    table: &LTSeqTable,
    group_expr: Option<Bound<'_, PyDict>>,
    agg_dict: &Bound<'_, PyDict>,
) -> PyResult<LTSeqTable> {
    let df = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No data loaded. Call read_csv() first.")
    })?;

    let schema = table
        .schema
        .as_ref()
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No schema available."))?;

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
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Failed to parse: {}", e))
        })?;

        let agg_result = extract_agg_function(&py_expr, schema).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Invalid aggregate expression - must be g.column.agg_func() or g.count()",
            )
        })?;

        let sql_expr = agg_result_to_sql(agg_result)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;

        agg_select_parts.push(format!("{} as {}", sql_expr, key_str));
    }

    // Build the GROUP BY clause
    let group_cols = parse_group_expr(group_expr, schema)?;

    // Register temp table
    let temp_table_name = "__ltseq_agg_temp";
    let current_batches = collect_dataframe(df)?;
    let arrow_schema = Arc::new((**schema).clone());

    let temp_table = MemTable::try_new(arrow_schema, vec![current_batches]).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to create table: {}", e))
    })?;

    table
        .session
        .register_table(temp_table_name, Arc::new(temp_table))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Register failed: {}", e))
        })?;

    // Build and execute SQL query
    let sql_query = build_agg_sql(&group_cols, &agg_select_parts, temp_table_name);
    let result_batches = execute_sql_query(table, &sql_query)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Cleanup temp table
    let _ = table.session.deregister_table(temp_table_name);

    create_result_table(table, result_batches)
}

/// Parse group expression into column names
fn parse_group_expr(
    group_expr: Option<Bound<'_, PyDict>>,
    schema: &Arc<ArrowSchema>,
) -> PyResult<Option<Vec<String>>> {
    let Some(group_expr_dict) = group_expr else {
        return Ok(None);
    };

    let py_expr = dict_to_py_expr(&group_expr_dict).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Failed to parse group: {}", e))
    })?;

    match py_expr {
        PyExpr::Column(col_name) => Ok(Some(vec![col_name])),
        _ => {
            let sql_expr =
                crate::transpiler::pyexpr_to_datafusion(py_expr, schema).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "Transpile failed: {}",
                        e
                    ))
                })?;
            Ok(Some(vec![format!("{}", sql_expr)]))
        }
    }
}

/// Collect dataframe into record batches
fn collect_dataframe(
    df: &Arc<datafusion::dataframe::DataFrame>,
) -> PyResult<Vec<datafusion::arrow::array::RecordBatch>> {
    RUNTIME
        .block_on(async {
            (**df)
                .clone()
                .collect()
                .await
                .map_err(|e| format!("Failed to collect: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))
}

/// Build SQL query for aggregation
fn build_agg_sql(
    group_cols: &Option<Vec<String>>,
    agg_parts: &[String],
    table_name: &str,
) -> String {
    if let Some(cols) = group_cols {
        let group_str = cols.join(", ");
        let agg_str = agg_parts.join(", ");
        format!(
            "SELECT {}, {} FROM {} GROUP BY {}",
            group_str, agg_str, table_name, group_str
        )
    } else {
        let agg_str = agg_parts.join(", ");
        format!("SELECT {} FROM {}", agg_str, table_name)
    }
}

/// Filter rows using a raw SQL WHERE clause
pub fn filter_where_impl(table: &LTSeqTable, where_clause: &str) -> PyResult<LTSeqTable> {
    let df = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No data loaded. Call read_csv() first.")
    })?;

    let schema = table
        .schema
        .as_ref()
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No schema available."))?;

    let current_batches = collect_dataframe(df)?;

    let batch_schema = current_batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new((**schema).clone()));

    let temp_table_name = format!("__ltseq_filter_temp_{}", std::process::id());
    let temp_table =
        MemTable::try_new(Arc::clone(&batch_schema), vec![current_batches]).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Create table: {}", e))
        })?;

    let _ = table.session.deregister_table(&temp_table_name);
    table
        .session
        .register_table(&temp_table_name, Arc::new(temp_table))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Register: {}", e))
        })?;

    let column_list: Vec<String> = batch_schema
        .fields()
        .iter()
        .map(|f| format!("\"{}\"", f.name()))
        .collect();

    let sql_query = format!(
        "SELECT {} FROM \"{}\" WHERE {}",
        column_list.join(", "),
        temp_table_name,
        where_clause
    );

    let result_batches = execute_sql_query(table, &sql_query)
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

    create_result_table(table, result_batches)
}
