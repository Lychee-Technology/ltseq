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
        // Statistical aggregates — native DataFusion path
        "corr" => {
            // corr(col_a, col_b) — both come from args when on is empty
            let (col_a, col_b) = if matches!(on.as_ref(), PyExpr::Column(name) if name.is_empty()) {
                if args.len() < 2 {
                    return Err("corr requires two column arguments".to_string());
                }
                (
                    crate::transpiler::pyexpr_to_datafusion(args[0].clone(), schema)?,
                    crate::transpiler::pyexpr_to_datafusion(args[1].clone(), schema)?,
                )
            } else {
                if args.is_empty() {
                    return Err("corr requires a second column argument".to_string());
                }
                (
                    crate::transpiler::pyexpr_to_datafusion(*on.clone(), schema)?,
                    crate::transpiler::pyexpr_to_datafusion(args[0].clone(), schema)?,
                )
            };
            Ok(Some(agg_fn::corr(col_a, col_b)))
        }
        "covar" => {
            let (col_a, col_b) = if matches!(on.as_ref(), PyExpr::Column(name) if name.is_empty()) {
                if args.len() < 2 {
                    return Err("covar requires two column arguments".to_string());
                }
                (
                    crate::transpiler::pyexpr_to_datafusion(args[0].clone(), schema)?,
                    crate::transpiler::pyexpr_to_datafusion(args[1].clone(), schema)?,
                )
            } else {
                if args.is_empty() {
                    return Err("covar requires a second column argument".to_string());
                }
                (
                    crate::transpiler::pyexpr_to_datafusion(*on.clone(), schema)?,
                    crate::transpiler::pyexpr_to_datafusion(args[0].clone(), schema)?,
                )
            };
            Ok(Some(agg_fn::covar_samp(col_a, col_b)))
        }
        "concat_agg" => {
            // concat_agg(col, delimiter) — uses native string_agg UDAF
            let (col_expr, delim_expr) = if matches!(on.as_ref(), PyExpr::Column(name) if name.is_empty()) {
                if args.is_empty() {
                    return Err("concat_agg requires a column argument".to_string());
                }
                let col = crate::transpiler::pyexpr_to_datafusion(args[0].clone(), schema)?;
                let delim = if args.len() > 1 {
                    crate::transpiler::pyexpr_to_datafusion(args[1].clone(), schema)?
                } else {
                    lit(",")
                };
                (col, delim)
            } else {
                let col = crate::transpiler::pyexpr_to_datafusion(*on.clone(), schema)?;
                let delim = if !args.is_empty() {
                    crate::transpiler::pyexpr_to_datafusion(args[0].clone(), schema)?
                } else {
                    lit(",")
                };
                (col, delim)
            };
            use datafusion::functions_aggregate::string_agg::string_agg;
            Ok(Some(string_agg(col_expr, delim_expr)))
        }
        // Complex aggregates that need SQL path
        // skew → SQL fallback using moment formula
        "top_k" | "mode" | "skew" => Ok(None),
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

        let py_expr = dict_to_py_expr(val_dict)
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
    .map_err(LtseqError::Runtime)?;

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

        let py_expr = dict_to_py_expr(val_dict)
            .map_err(|e| LtseqError::Validation(format!("Failed to parse: {}", e)))?;

        let agg_result = extract_agg_function(&py_expr, schema).ok_or_else(|| {
            LtseqError::Validation(
                "Invalid aggregate expression - must be g.column.agg_func() or g.count()"
                    .into(),
            )
        })?;

        let sql_expr =
            agg_result_to_sql(agg_result).map_err(LtseqError::Validation)?;

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
        execute_sql_query(table, &sql_query).map_err(LtseqError::Runtime)?;

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
        | "std" | "mode" | "skew" => extract_simple_agg(func, on, args),
        "top_k" | "percentile" => extract_agg_with_arg(func, on, args),
        "concat_agg" => extract_concat_agg(on, args),
        "count_if" => extract_count_if(args, schema),
        "sum_if" | "avg_if" | "min_if" | "max_if" => extract_conditional_agg(func, args, schema),
        _ => None,
    }
}

fn extract_concat_agg(on: &PyExpr, args: &[PyExpr]) -> Option<AggResult> {
    let col_name = if let PyExpr::Column(name) = on {
        if !name.is_empty() {
            name.clone()
        } else if let Some(PyExpr::Column(arg_name)) = args.first() {
            arg_name.clone()
        } else {
            return None;
        }
    } else if let Some(PyExpr::Column(name)) = args.first() {
        name.clone()
    } else {
        return None;
    };

    // Extract optional delimiter from args
    let delim = if let PyExpr::Column(name) = on {
        if name.is_empty() {
            // standalone: args[0]=col, args[1]=delim
            args.get(1).and_then(|a| {
                if let PyExpr::Literal { value, .. } = a { Some(value.clone()) } else { None }
            }).unwrap_or_else(|| ",".to_string())
        } else {
            // method: on=col, args[0]=delim
            args.first().and_then(|a| {
                if let PyExpr::Literal { value, .. } = a { Some(value.clone()) } else { None }
            }).unwrap_or_else(|| ",".to_string())
        }
    } else {
        ",".to_string()
    };

    Some(AggResult::Simple("concat_agg".to_string(), col_name, Some(delim)))
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
        "skew" => format!(
            "(AVG({col}*{col}*{col}) - 3*AVG({col}*{col})*AVG({col}) + 2*POWER(AVG({col}),3)) \
             / NULLIF(POWER(STDDEV_POP({col}),3), 0)"
        ),
        "concat_agg" => {
            let delim = arg.as_deref().unwrap_or(",");
            let delim_escaped = delim.replace('\'', "''");
            format!("STRING_AGG({}, '{}')", col, delim_escaped)
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
        .map_err(LtseqError::Runtime)?)
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
///
/// Uses DataFusion's SQL parser to convert the WHERE clause into a native
/// expression, then applies it via DataFrame::filter() to preserve laziness.
pub fn filter_where_impl(table: &LTSeqTable, where_clause: &str) -> PyResult<LTSeqTable> {
    let (df, schema) = table.require_df_and_schema()?;

    // Parse the WHERE clause into a native expression by building a SQL query
    // against a temp table with the same schema, then extract the filter expression
    // and strip any table qualifiers so it works with the original DataFrame.
    let filter_expr = RUNTIME
        .block_on(async {
            let temp_name = "__ltseq_filter_parse_tmp";
            let _ = table.session.deregister_table(temp_name);

            // Register an empty table with the same schema for parsing
            let empty_batch = datafusion::arrow::record_batch::RecordBatch::new_empty(Arc::clone(schema));
            let mem_table = datafusion::datasource::MemTable::try_new(
                Arc::clone(schema),
                vec![vec![empty_batch]],
            ).map_err(|e| format!("Failed to create parse table: {}", e))?;

            table
                .session
                .register_table(temp_name, Arc::new(mem_table))
                .map_err(|e| format!("Failed to register parse table: {}", e))?;

            // Parse the full SELECT to get the filter expression in context
            let parsed_df = table
                .session
                .sql(&format!("SELECT * FROM \"{}\" WHERE {}", temp_name, where_clause))
                .await
                .map_err(|e| format!("Failed to parse WHERE clause: {}", e))?;

            // Walk the logical plan to find the Filter node
            fn extract_filter_predicate(
                plan: &datafusion::logical_expr::LogicalPlan,
            ) -> Option<datafusion::logical_expr::Expr> {
                match plan {
                    datafusion::logical_expr::LogicalPlan::Filter(filter) => {
                        Some(filter.predicate.clone())
                    }
                    datafusion::logical_expr::LogicalPlan::Projection(proj) => {
                        extract_filter_predicate(&proj.input)
                    }
                    _ => None,
                }
            }

            let predicate = extract_filter_predicate(parsed_df.logical_plan())
                .ok_or_else(|| format!("No filter expression found in: {}", where_clause))?;

            // Clean up temp table
            let _ = table.session.deregister_table(temp_name);

            // Strip table qualifiers from the expression so it works with the original DataFrame.
            // Columns parsed from SQL will be qualified with the temp table name,
            // but we need unqualified columns for the native filter.
            fn strip_table_qualifiers(expr: datafusion::logical_expr::Expr) -> datafusion::logical_expr::Expr {
                use datafusion::logical_expr::Expr;
                match expr {
                    Expr::Column(col) => {
                        // Remove table qualifier
                        Expr::Column(datafusion::common::Column::new_unqualified(col.name))
                    }
                    Expr::Alias(alias) => {
                        Expr::Alias(datafusion::logical_expr::expr::Alias {
                            expr: Box::new(strip_table_qualifiers(*alias.expr)),
                            ..alias
                        })
                    }
                    Expr::BinaryExpr(binary) => {
                        Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
                            left: Box::new(strip_table_qualifiers(*binary.left)),
                            right: Box::new(strip_table_qualifiers(*binary.right)),
                            op: binary.op,
                        })
                    }
                    Expr::Like(like) => {
                        Expr::Like(datafusion::logical_expr::expr::Like {
                            negated: like.negated,
                            expr: Box::new(strip_table_qualifiers(*like.expr)),
                            pattern: Box::new(strip_table_qualifiers(*like.pattern)),
                            escape_char: like.escape_char,
                            case_insensitive: like.case_insensitive,
                        })
                    }
                    Expr::InList(in_list) => {
                        Expr::InList(datafusion::logical_expr::expr::InList {
                            expr: Box::new(strip_table_qualifiers(*in_list.expr)),
                            list: in_list.list.into_iter().map(strip_table_qualifiers).collect(),
                            negated: in_list.negated,
                        })
                    }
                    Expr::Between(between) => {
                        Expr::Between(datafusion::logical_expr::expr::Between {
                            expr: Box::new(strip_table_qualifiers(*between.expr)),
                            negated: between.negated,
                            low: Box::new(strip_table_qualifiers(*between.low)),
                            high: Box::new(strip_table_qualifiers(*between.high)),
                        })
                    }
                    Expr::Case(case) => {
                        Expr::Case(datafusion::logical_expr::expr::Case {
                            expr: case.expr.map(|e| Box::new(strip_table_qualifiers(*e))),
                            when_then_expr: case.when_then_expr.into_iter().map(|(w, t)| {
                                (Box::new(strip_table_qualifiers(*w)), Box::new(strip_table_qualifiers(*t)))
                            }).collect(),
                            else_expr: case.else_expr.map(|e| Box::new(strip_table_qualifiers(*e))),
                        })
                    }
                    Expr::ScalarFunction(func) => {
                        Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction {
                            func: func.func,
                            args: func.args.into_iter().map(strip_table_qualifiers).collect(),
                        })
                    }
                    Expr::AggregateFunction(agg) => {
                        Expr::AggregateFunction(datafusion::logical_expr::expr::AggregateFunction {
                            func: agg.func,
                            params: datafusion::logical_expr::expr::AggregateFunctionParams {
                                args: agg.params.args.into_iter().map(strip_table_qualifiers).collect(),
                                filter: agg.params.filter.map(|e| Box::new(strip_table_qualifiers(*e))),
                                order_by: agg.params.order_by,
                                distinct: agg.params.distinct,
                                null_treatment: agg.params.null_treatment,
                            },
                        })
                    }
                    Expr::WindowFunction(win) => {
                        Expr::WindowFunction(Box::new(datafusion::logical_expr::expr::WindowFunction {
                            fun: win.fun,
                            params: datafusion::logical_expr::expr::WindowFunctionParams {
                                args: win.params.args.into_iter().map(strip_table_qualifiers).collect(),
                                partition_by: win.params.partition_by.into_iter().map(strip_table_qualifiers).collect(),
                                order_by: win.params.order_by.into_iter().map(|s| datafusion::logical_expr::expr::Sort {
                                    expr: strip_table_qualifiers(s.expr),
                                    asc: s.asc,
                                    nulls_first: s.nulls_first,
                                }).collect(),
                                window_frame: win.params.window_frame,
                                filter: win.params.filter.map(|e| Box::new(strip_table_qualifiers(*e))),
                                null_treatment: win.params.null_treatment,
                                distinct: win.params.distinct,
                            },
                        }))
                    }
                    Expr::Cast(cast) => {
                        Expr::Cast(datafusion::logical_expr::expr::Cast {
                            expr: Box::new(strip_table_qualifiers(*cast.expr)),
                            data_type: cast.data_type,
                        })
                    }
                    Expr::TryCast(try_cast) => {
                        Expr::TryCast(datafusion::logical_expr::expr::TryCast {
                            expr: Box::new(strip_table_qualifiers(*try_cast.expr)),
                            data_type: try_cast.data_type,
                        })
                    }
                    Expr::Not(not) => {
                        Expr::Not(Box::new(strip_table_qualifiers(*not)))
                    }
                    Expr::IsNotNull(is_not_null) => {
                        Expr::IsNotNull(Box::new(strip_table_qualifiers(*is_not_null)))
                    }
                    Expr::IsNull(is_null) => {
                        Expr::IsNull(Box::new(strip_table_qualifiers(*is_null)))
                    }
                    Expr::IsTrue(is_true) => {
                        Expr::IsTrue(Box::new(strip_table_qualifiers(*is_true)))
                    }
                    Expr::IsFalse(is_false) => {
                        Expr::IsFalse(Box::new(strip_table_qualifiers(*is_false)))
                    }
                    Expr::IsUnknown(is_unknown) => {
                        Expr::IsUnknown(Box::new(strip_table_qualifiers(*is_unknown)))
                    }
                    Expr::IsNotTrue(is_not_true) => {
                        Expr::IsNotTrue(Box::new(strip_table_qualifiers(*is_not_true)))
                    }
                    Expr::IsNotFalse(is_not_false) => {
                        Expr::IsNotFalse(Box::new(strip_table_qualifiers(*is_not_false)))
                    }
                    Expr::IsNotUnknown(is_not_unknown) => {
                        Expr::IsNotUnknown(Box::new(strip_table_qualifiers(*is_not_unknown)))
                    }
                    Expr::Negative(neg) => {
                        Expr::Negative(Box::new(strip_table_qualifiers(*neg)))
                    }
                    // GetIndexedField removed in DataFusion 53
                    // Literals and other leaf expressions pass through unchanged
                    other => other,
                }
            }

            Ok::<_, String>(strip_table_qualifiers(predicate))
        })
        .map_err(LtseqError::Runtime)?;

    // Apply the filter natively — stays lazy
    let filtered_df = (**df)
        .clone()
        .filter(filter_expr)
        .map_err(|e| LtseqError::Runtime(format!("Failed to apply filter: {}", e)))?;

    Ok(LTSeqTable::from_df_with_schema(
        Arc::clone(&table.session),
        filtered_df,
        Arc::clone(schema),
        table.sort_exprs.clone(),
        table.source_parquet_path.clone(),
    ))
}
