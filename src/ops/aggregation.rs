//! Aggregation operations for LTSeqTable
//!
//! Provides SQL GROUP BY aggregation and raw SQL WHERE filtering.
//! Uses DataFusion's native df.aggregate() API for performance (no materialization).

use crate::engine::RUNTIME;
use crate::error::LtseqError;
use crate::transpiler::pyexpr_to_sql;
use crate::types::{dict_to_py_expr, PyExpr};
use crate::LTSeqTable;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::Column;
use datafusion::datasource::MemTable;
use datafusion::functions_aggregate::expr_fn as agg_fn;
use datafusion::logical_expr::{case, Expr};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

/// Convert a PyExpr aggregate call to a native DataFusion Expr.
/// Returns Ok(Some(expr)) for supported aggregates, Ok(None) for unsupported ones
/// that need the SQL fallback path (top_k, mode).
fn pyexpr_to_agg_expr(
    py_expr: &PyExpr,
    schema: &ArrowSchema,
) -> Result<Option<Expr>, String> {
    let PyExpr::Call { func, on, args, .. } = py_expr else {
        return Err("Expected aggregate function call".to_string());
    };

    match func.as_str() {
        // Simple aggregations on a column
        "sum" | "count" | "min" | "max" | "avg" | "median" | "variance" | "var" | "stddev"
        | "std" => {
            let col_expr = match on.as_ref() {
                PyExpr::Column(col_name) => {
                    Expr::Column(Column::new_unqualified(col_name))
                }
                _ if func == "count" => {
                    // count() without a specific column → count(*)
                    lit(1i64)
                }
                _ => return Err(format!("Aggregate '{}' requires a column reference", func)),
            };

            let agg_expr = match func.as_str() {
                "sum" => agg_fn::sum(col_expr),
                "count" => agg_fn::count(col_expr),
                "min" => agg_fn::min(col_expr),
                "max" => agg_fn::max(col_expr),
                "avg" => agg_fn::avg(col_expr),
                "median" => agg_fn::median(col_expr),
                "variance" | "var" => agg_fn::var_sample(col_expr),
                "stddev" | "std" => agg_fn::stddev(col_expr),
                _ => unreachable!(),
            };
            Ok(Some(agg_expr))
        }
        "percentile" => {
            let col_expr = match on.as_ref() {
                PyExpr::Column(col_name) => {
                    Expr::Column(Column::new_unqualified(col_name))
                }
                _ => return Err("percentile requires a column reference".to_string()),
            };
            let p = args.first().and_then(|arg| {
                if let PyExpr::Literal { value, .. } = arg {
                    value.parse::<f64>().ok()
                } else {
                    None
                }
            }).unwrap_or(0.5);
            let sort = datafusion::logical_expr::SortExpr::new(col_expr, true, false);
            Ok(Some(agg_fn::approx_percentile_cont(sort, lit(p), None)))
        }
        // Conditional aggregations
        "count_if" => {
            let predicate = args.first().ok_or("count_if requires a predicate argument")?;
            let pred_expr = crate::transpiler::pyexpr_to_datafusion(predicate.clone(), schema)?;
            // count_if(cond) → SUM(CASE WHEN cond THEN 1 ELSE 0 END)
            let case_expr = case(pred_expr)
                .when(lit(true), lit(1i64))
                .otherwise(lit(0i64))
                .map_err(|e| format!("Failed to create CASE: {}", e))?;
            Ok(Some(agg_fn::sum(case_expr)))
        }
        "sum_if" | "avg_if" | "min_if" | "max_if" => {
            if args.len() < 2 {
                return Err(format!("{} requires predicate and column arguments", func));
            }
            let pred_expr = crate::transpiler::pyexpr_to_datafusion(args[0].clone(), schema)?;
            let col_expr = crate::transpiler::pyexpr_to_datafusion(args[1].clone(), schema)?;

            let (true_val, false_val) = match func.as_str() {
                "sum_if" => (col_expr.clone(), lit(0i64)),
                _ => (col_expr.clone(), lit(ScalarValue::Null)),
            };

            let case_expr = case(pred_expr)
                .when(lit(true), true_val)
                .otherwise(false_val)
                .map_err(|e| format!("Failed to create CASE: {}", e))?;

            let agg_expr = match func.as_str() {
                "sum_if" => agg_fn::sum(case_expr),
                "avg_if" => agg_fn::avg(case_expr),
                "min_if" => agg_fn::min(case_expr),
                "max_if" => agg_fn::max(case_expr),
                _ => unreachable!(),
            };
            Ok(Some(agg_expr))
        }
        // Complex aggregates that need SQL path
        "top_k" | "mode" => Ok(None),
        _ => Err(format!("Unknown aggregate function: {}", func)),
    }
}

/// Parse group expression into DataFusion Expr(s)
fn parse_group_exprs(
    group_expr: Option<Bound<'_, PyDict>>,
    schema: &ArrowSchema,
) -> PyResult<Vec<Expr>> {
    let Some(group_expr_dict) = group_expr else {
        return Ok(Vec::new());
    };

    let py_expr = dict_to_py_expr(&group_expr_dict)
        .map_err(|e| LtseqError::Validation(format!("Failed to parse group: {}", e)))?;

    let df_expr = crate::transpiler::pyexpr_to_datafusion(py_expr, schema)
        .map_err(|e| LtseqError::Validation(format!("Transpile failed: {}", e)))?;

    Ok(vec![df_expr])
}

/// Aggregate rows into a summary table with one row per group.
/// Uses native DataFusion df.aggregate() API — no materialization needed.
pub fn agg_impl(
    table: &LTSeqTable,
    group_expr: Option<Bound<'_, PyDict>>,
    agg_dict: &Bound<'_, PyDict>,
) -> PyResult<LTSeqTable> {
    let (df, schema) = table.require_df_and_schema()?;

    // Try the native DataFusion path first: build aggregate Exprs directly
    let mut agg_exprs: Vec<Expr> = Vec::new();
    let mut needs_sql_fallback = false;

    for (key, value) in agg_dict.iter() {
        let key_str = key
            .extract::<String>()
            .map_err(|_| LtseqError::TypeMismatch("Agg key must be string".into()))?;

        let val_dict = value
            .cast::<PyDict>()
            .map_err(|_| LtseqError::TypeMismatch("Agg value must be dict".into()))?;

        let py_expr = dict_to_py_expr(&val_dict)
            .map_err(|e| LtseqError::Validation(format!("Failed to parse: {}", e)))?;

        match pyexpr_to_agg_expr(&py_expr, schema) {
            Ok(Some(expr)) => {
                agg_exprs.push(expr.alias(&key_str));
            }
            Ok(None) => {
                // top_k or mode — need SQL fallback
                needs_sql_fallback = true;
                break;
            }
            Err(e) => {
                return Err(LtseqError::Validation(e).into());
            }
        }
    }

    if needs_sql_fallback {
        return agg_impl_sql_fallback(table, group_expr, agg_dict);
    }

    // Parse group expressions
    let group_exprs = parse_group_exprs(group_expr, schema)?;

    // Execute native aggregate — stays lazy until collect!
    let result_df = RUNTIME.block_on(async {
        let cloned_df = (**df).clone();
        let agg_df = cloned_df
            .aggregate(group_exprs, agg_exprs)
            .map_err(|e| format!("Aggregate failed: {}", e))?;
        // Collect to get result schema
        let batches = agg_df
            .collect()
            .await
            .map_err(|e| format!("Collect failed: {}", e))?;
        Ok::<_, String>(batches)
    })
    .map_err(|e| LtseqError::Runtime(e))?;

    create_result_table(table, result_df)
}

/// SQL-based fallback for complex aggregates (top_k, mode)
fn agg_impl_sql_fallback(
    table: &LTSeqTable,
    group_expr: Option<Bound<'_, PyDict>>,
    agg_dict: &Bound<'_, PyDict>,
) -> PyResult<LTSeqTable> {
    let (df, schema) = table.require_df_and_schema()?;

    let mut agg_select_parts: Vec<String> = Vec::new();

    for (key, value) in agg_dict.iter() {
        let key_str = key
            .extract::<String>()
            .map_err(|_| LtseqError::TypeMismatch("Agg key must be string".into()))?;

        let val_dict = value
            .cast::<PyDict>()
            .map_err(|_| LtseqError::TypeMismatch("Agg value must be dict".into()))?;

        let py_expr = dict_to_py_expr(&val_dict)
            .map_err(|e| LtseqError::Validation(format!("Failed to parse: {}", e)))?;

        let agg_result = extract_agg_function(&py_expr, schema).ok_or_else(|| {
            LtseqError::Validation(
                "Invalid aggregate expression - must be g.column.agg_func() or g.count()"
                    .into(),
            )
        })?;

        let sql_expr =
            agg_result_to_sql(agg_result).map_err(|e| LtseqError::Validation(e))?;

        agg_select_parts.push(format!("{} as {}", sql_expr, key_str));
    }

    let group_cols = parse_group_expr_sql(group_expr, schema)?;

    // Register temp table (must materialize for SQL path)
    let temp_table_name = "__ltseq_agg_temp";
    let current_batches = collect_dataframe(df)?;
    let arrow_schema = Arc::new((**schema).clone());

    let temp_table = MemTable::try_new(arrow_schema, vec![current_batches])
        .map_err(|e| LtseqError::Runtime(format!("Failed to create table: {}", e)))?;

    table
        .session
        .register_table(temp_table_name, Arc::new(temp_table))
        .map_err(|e| LtseqError::Runtime(format!("Register failed: {}", e)))?;

    let sql_query = build_agg_sql(&group_cols, &agg_select_parts, temp_table_name);
    let result_batches =
        execute_sql_query(table, &sql_query).map_err(|e| LtseqError::Runtime(e))?;

    let _ = table.session.deregister_table(temp_table_name);

    create_result_table(table, result_batches)
}

// ============================================================================
// SQL-path helpers (kept for fallback and filter_where_impl)
// ============================================================================

/// Result type for aggregate function extraction (SQL path)
enum AggResult {
    Simple(String, String, Option<String>),
    Conditional(String, String, Option<String>),
}

fn extract_agg_function(py_expr: &PyExpr, schema: &ArrowSchema) -> Option<AggResult> {
    let PyExpr::Call { func, on, args, .. } = py_expr else {
        return None;
    };

    match func.as_str() {
        "sum" | "count" | "min" | "max" | "avg" | "median" | "variance" | "var" | "stddev"
        | "std" | "mode" => extract_simple_agg(func, on, args),
        "top_k" | "percentile" => extract_agg_with_arg(func, on, args),
        "count_if" => extract_count_if(args, schema),
        "sum_if" | "avg_if" | "min_if" | "max_if" => extract_conditional_agg(func, args, schema),
        _ => None,
    }
}

fn extract_simple_agg(func: &str, on: &PyExpr, _args: &[PyExpr]) -> Option<AggResult> {
    if let PyExpr::Column(col_name) = on {
        return Some(AggResult::Simple(func.to_string(), col_name.clone(), None));
    }
    if func == "count" {
        return Some(AggResult::Simple(func.to_string(), "*".to_string(), None));
    }
    None
}

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

fn extract_count_if(args: &[PyExpr], schema: &ArrowSchema) -> Option<AggResult> {
    let predicate = args.first()?;
    let pred_sql = pyexpr_to_sql(predicate, schema).ok()?;
    Some(AggResult::Conditional(
        "count_if".to_string(),
        pred_sql,
        None,
    ))
}

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
    LTSeqTable::from_batches(
        Arc::clone(&table.session),
        batches,
        Vec::new(),
        None,
    )
}

/// Parse group expression into column names (SQL path)
fn parse_group_expr_sql(
    group_expr: Option<Bound<'_, PyDict>>,
    schema: &Arc<ArrowSchema>,
) -> PyResult<Option<Vec<String>>> {
    let Some(group_expr_dict) = group_expr else {
        return Ok(None);
    };

    let py_expr = dict_to_py_expr(&group_expr_dict)
        .map_err(|e| LtseqError::Validation(format!("Failed to parse group: {}", e)))?;

    match py_expr {
        PyExpr::Column(col_name) => Ok(Some(vec![col_name])),
        _ => {
            let sql_expr =
                crate::transpiler::pyexpr_to_datafusion(py_expr, schema).map_err(|e| {
                    LtseqError::Validation(format!("Transpile failed: {}", e))
                })?;
            Ok(Some(vec![format!("{}", sql_expr)]))
        }
    }
}

/// Collect dataframe into record batches
fn collect_dataframe(
    df: &Arc<datafusion::dataframe::DataFrame>,
) -> PyResult<Vec<datafusion::arrow::array::RecordBatch>> {
    Ok(RUNTIME
        .block_on(async {
            (**df)
                .clone()
                .collect()
                .await
                .map_err(|e| format!("Failed to collect: {}", e))
        })
        .map_err(|e| LtseqError::Runtime(e))?)
}

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
    let df = table
        .dataframe
        .as_ref()
        .ok_or(LtseqError::NoData)?;

    let schema = table
        .schema
        .as_ref()
        .ok_or(LtseqError::NoSchema)?;

    let current_batches = collect_dataframe(df)?;

    let batch_schema = current_batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new((**schema).clone()));

    let temp_table_name = format!("__ltseq_filter_temp_{}", std::process::id());
    let temp_table =
        MemTable::try_new(Arc::clone(&batch_schema), vec![current_batches]).map_err(|e| {
            LtseqError::Runtime(format!("Create table: {}", e))
        })?;

    let _ = table.session.deregister_table(&temp_table_name);
    table
        .session
        .register_table(&temp_table_name, Arc::new(temp_table))
        .map_err(|e| LtseqError::Runtime(format!("Register: {}", e)))?;

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

    let result_batches =
        execute_sql_query(table, &sql_query).map_err(|e| LtseqError::Runtime(e))?;

    let _ = table.session.deregister_table(&temp_table_name);

    if result_batches.is_empty() {
        return Ok(LTSeqTable::empty(
            Arc::clone(&table.session),
            Some(Arc::clone(schema)),
            Vec::new(),
            None,
        ));
    }

    create_result_table(table, result_batches)
}
