//! Native DataFusion window expression builder
//!
//! Converts PyExpr window function calls directly into DataFusion `Expr::WindowFunction`
//! via the native API, eliminating the need for SQL string generation, materialization,
//! and re-parsing.
//!
//! Handles:
//! - `shift(n)` / `shift(n, default=val)` → `lag(col, n)` or `lead(col, -n)`
//! - `diff(n)` → `col - lag(col, n)`
//! - `rolling(n).mean/sum/min/max/count/std()` → aggregate OVER (ROWS BETWEEN ...)
//! - `cum_sum` → `sum(col) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`
//! - `PyExpr::Window` (row_number, rank, dense_rank, ntile) → corresponding window UDFs
//!
//! All window functions support an optional `partition_by` kwarg:
//! - `shift(n, partition_by="col")` — single column name as string
//! - `shift(n, partition_by=r.col)` — single column as expression
//!
//! The `sort_exprs` from the table are used for ORDER BY in OVER clauses.
//! When `partition_by` is specified, any sort expression that matches a partition
//! column is filtered out of ORDER BY to avoid redundant sorting.

use crate::types::PyExpr;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::expr::WindowFunction as WindowFunctionExpr;
use datafusion::logical_expr::expr::WindowFunctionParams;
use datafusion::logical_expr::{Expr, ExprFunctionExt, WindowFunctionDefinition};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;

// Window function constructors
use datafusion::functions_window::expr_fn::{dense_rank, lag, lead, ntile, rank, row_number};

// Aggregate function constructors (for rolling windows and cum_sum)
use datafusion::functions_aggregate::expr_fn::{avg, count, max, min, stddev, sum};

// Window frame types
use datafusion::logical_expr::WindowFrame;
use datafusion::logical_expr::WindowFrameBound;
use datafusion::logical_expr::WindowFrameUnits;

use std::collections::HashMap;

use super::pyexpr_to_datafusion;

/// Peek at partition_by column names from a PyExpr's kwargs without full conversion.
///
/// Returns column name strings for the common cases (string literal or Column).
/// Used to filter redundant ORDER BY keys that duplicate PARTITION BY columns —
/// within a partition, all rows share the same partition key values, so ordering
/// by a partition key is redundant and wastes sort effort.
fn peek_partition_by_cols(py_expr: &PyExpr) -> Vec<String> {
    let kwargs = match py_expr {
        PyExpr::Call { kwargs, .. } => kwargs,
        _ => return vec![],
    };
    match kwargs.get("partition_by") {
        Some(PyExpr::Literal { value, dtype }) if dtype == "String" || dtype == "Utf8" => {
            vec![value.clone()]
        }
        Some(PyExpr::Column(name)) => vec![name.clone()],
        _ => vec![],
    }
}

/// Extract `partition_by` from kwargs and convert to `Vec<Expr>`.
///
/// Handles these forms from the Python side:
/// - `partition_by="col"` → `PyExpr::Literal { value: "col", dtype: "String" }` → `col("col")`
/// - `partition_by=r.col` → `PyExpr::Column("col")` → `col("col")`
///
/// Returns an empty Vec if no `partition_by` kwarg is present.
fn extract_partition_by(
    kwargs: &HashMap<String, PyExpr>,
    schema: &ArrowSchema,
) -> Result<Vec<Expr>, String> {
    let pb = match kwargs.get("partition_by") {
        Some(pb) => pb,
        None => return Ok(vec![]),
    };
    match pb {
        // partition_by="col_name" — string literal used as column name
        PyExpr::Literal {
            value,
            dtype,
        } if dtype == "String" || dtype == "Utf8" => {
            Ok(vec![col(value)])
        }
        // partition_by=r.col — column expression
        PyExpr::Column(name) => {
            Ok(vec![col(name)])
        }
        // Any other expression — try converting through the standard path
        other => {
            let expr = pyexpr_to_datafusion(other.clone(), schema)?;
            Ok(vec![expr])
        }
    }
}

/// Convert an aggregate `Expr` (e.g., `sum(col)`) into a window function `Expr`.
///
/// The DataFusion builder API (ExprFunctionExt) does NOT convert aggregates to window
/// functions — calling `.window_frame()` or `.partition_by()` on `Expr::AggregateFunction`
/// returns an empty builder that produces errors on `.build()`. Instead, we manually
/// extract the aggregate UDF and args, then construct `Expr::WindowFunction` directly.
fn aggregate_to_window(
    agg_expr: Expr,
    partition_by: Vec<Expr>,
    order_by: Vec<Sort>,
    frame: WindowFrame,
) -> Result<Expr, String> {
    match agg_expr {
        Expr::AggregateFunction(agg_func) => {
            Ok(Expr::WindowFunction(Box::new(WindowFunctionExpr {
                fun: WindowFunctionDefinition::AggregateUDF(agg_func.func),
                params: WindowFunctionParams {
                    args: agg_func.params.args,
                    partition_by,
                    order_by,
                    window_frame: frame,
                    filter: None,
                    null_treatment: None,
                    distinct: false,
                },
            })))
        }
        _ => Err(format!(
            "Expected AggregateFunction expr, got: {:?}",
            agg_expr
        )),
    }
}

/// Apply partition_by and order_by to a window expression builder, then build.
///
/// This is the common pattern used by convert_shift and convert_diff (true window
/// functions like lag/lead). For aggregate-as-window functions (cum_sum, rolling),
/// use `aggregate_to_window()` instead.
/// When order_by is empty (unsorted table), we skip the order_by clause but still build.
fn finalize_window_expr(
    window_expr: Expr,
    partition_by_exprs: Vec<Expr>,
    order_by: &[Sort],
    context: &str,
) -> Result<Expr, String> {
    // Start by applying partition_by (if any). We need at least one builder method
    // call to get an ExprFuncBuilder, so we always call order_by or partition_by.
    if order_by.is_empty() && partition_by_exprs.is_empty() {
        // No ORDER BY and no PARTITION BY — return the raw expression.
        // DataFusion 52 rejects empty ORDER BY in .build(), so we return as-is.
        // The implicit conversion from ExprFuncBuilder to Expr handles this.
        Ok(window_expr)
    } else if order_by.is_empty() {
        // PARTITION BY only (no ORDER BY)
        window_expr
            .partition_by(partition_by_exprs)
            .build()
            .map_err(|e| format!("Failed to build {} window expr: {}", context, e))
    } else {
        let mut builder = window_expr.order_by(order_by.to_vec());
        if !partition_by_exprs.is_empty() {
            builder = builder.partition_by(partition_by_exprs);
        }
        builder
            .build()
            .map_err(|e| format!("Failed to build {} window expr: {}", context, e))
    }
}

pub fn pyexpr_to_window_expr(
    py_expr: PyExpr,
    schema: &ArrowSchema,
    sort_exprs: &[String],
) -> Result<Expr, String> {
    // Filter out sort expressions that duplicate partition_by columns.
    // Within a partition, all rows share the same partition key values,
    // so ordering by a partition key is redundant and wastes sort effort.
    let partition_cols = peek_partition_by_cols(&py_expr);

    let order_by: Vec<Sort> = sort_exprs
        .iter()
        .filter(|col_name| !partition_cols.contains(col_name))
        .map(|col_name| Sort {
            expr: Expr::Column(datafusion::common::Column::new_unqualified(col_name)),
            asc: true,
            nulls_first: true,
        })
        .collect();

    pyexpr_to_window_inner(py_expr, schema, &order_by)
}

/// Internal recursive conversion
fn pyexpr_to_window_inner(
    py_expr: PyExpr,
    schema: &ArrowSchema,
    order_by: &[Sort],
) -> Result<Expr, String> {
    match &py_expr {
        PyExpr::Call { func, .. } => match func.as_str() {
            "shift" => convert_shift(&py_expr, schema, order_by),
            "diff" => convert_diff(&py_expr, schema, order_by),
            "cum_sum" => convert_cum_sum(&py_expr, schema, order_by),
            // Aggregation functions applied to rolling()
            "mean" | "sum" | "min" | "max" | "count" | "std" => {
                // Check if this is rolling().agg()
                if let PyExpr::Call { on, .. } = &py_expr {
                    if let PyExpr::Call {
                        func: inner_func, ..
                    } = on.as_ref()
                    {
                        if inner_func == "rolling" {
                            return convert_rolling_agg(&py_expr, schema, order_by);
                        }
                    }
                }
                // Not a rolling aggregation - fall back to non-window conversion
                Err(format!(
                    "Aggregation function '{}' not applied to rolling() - not a window function",
                    func
                ))
            }
            // Non-window functions that might contain window sub-expressions
            _ => convert_expr_with_window_children(py_expr, schema, order_by),
        },
        PyExpr::Window { .. } => convert_window_ranking(&py_expr, schema, order_by),
        // BinOp, UnaryOp might contain window functions in their children
        PyExpr::BinOp { .. } | PyExpr::UnaryOp { .. } => {
            convert_expr_with_window_children(py_expr, schema, order_by)
        }
        // Simple expressions (Column, Literal) - use the standard converter
        _ => pyexpr_to_datafusion(py_expr, schema),
    }
}

/// Convert shift(n) / shift(n, default=val) to lag/lead
///
/// Supports `partition_by` kwarg: `shift(n, partition_by="col")` or `shift(n, partition_by=r.col)`
fn convert_shift(
    py_expr: &PyExpr,
    schema: &ArrowSchema,
    order_by: &[Sort],
) -> Result<Expr, String> {
    if let PyExpr::Call {
        on, args, kwargs, ..
    } = py_expr
    {
        let col_expr = pyexpr_to_datafusion(*on.clone(), schema)?;

        // Get offset (default 1)
        let offset: i64 = if args.is_empty() {
            1
        } else if let PyExpr::Literal { value, .. } = &args[0] {
            value
                .parse::<i64>()
                .map_err(|_| "shift() offset must be an integer".to_string())?
        } else {
            return Err("shift() offset must be a literal integer".to_string());
        };

        // Get optional default value
        let default_value = if let Some(default_expr) = kwargs.get("default") {
            match default_expr {
                PyExpr::Literal { value, dtype } => {
                    let sv = literal_to_scalar_value(value, dtype)?;
                    Some(sv)
                }
                _ => None,
            }
        } else {
            None
        };

        // Extract partition_by from kwargs
        let partition_by_exprs = extract_partition_by(kwargs, schema)?;

        // Build the window function expression
        let window_expr = if offset >= 0 {
            lag(col_expr, Some(offset), default_value)
        } else {
            lead(col_expr, Some(-offset), default_value)
        };

        finalize_window_expr(window_expr, partition_by_exprs, order_by, "shift")
    } else {
        Err("Expected Call expression for shift".to_string())
    }
}

/// Convert diff(n) to col - lag(col, n) with ORDER BY
///
/// Supports `partition_by` kwarg: `diff(n, partition_by="col")`
fn convert_diff(
    py_expr: &PyExpr,
    schema: &ArrowSchema,
    order_by: &[Sort],
) -> Result<Expr, String> {
    if let PyExpr::Call {
        on, args, kwargs, ..
    } = py_expr
    {
        let col_expr = pyexpr_to_datafusion(*on.clone(), schema)?;

        // Get periods (default 1)
        let periods: i64 = if args.is_empty() {
            1
        } else if let PyExpr::Literal { value, .. } = &args[0] {
            value
                .parse::<i64>()
                .map_err(|_| "diff() periods must be an integer".to_string())?
        } else {
            return Err("diff() periods must be a literal integer".to_string());
        };

        // Extract partition_by from kwargs
        let partition_by_exprs = extract_partition_by(kwargs, schema)?;

        // Build: col - lag(col, periods) with ORDER BY and optional PARTITION BY
        let lag_expr = lag(col_expr.clone(), Some(periods), None);
        let lag_with_order =
            finalize_window_expr(lag_expr, partition_by_exprs, order_by, "diff")?;

        Ok(col_expr - lag_with_order)
    } else {
        Err("Expected Call expression for diff".to_string())
    }
}

/// Convert cum_sum to sum(col) OVER (ORDER BY ... ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
///
/// Supports `partition_by` kwarg: `cum_sum(partition_by="col")`
fn convert_cum_sum(
    py_expr: &PyExpr,
    schema: &ArrowSchema,
    order_by: &[Sort],
) -> Result<Expr, String> {
    if let PyExpr::Call { on, kwargs, .. } = py_expr {
        let col_expr = pyexpr_to_datafusion(*on.clone(), schema)?;

        // Extract partition_by from kwargs
        let partition_by_exprs = extract_partition_by(kwargs, schema)?;

        // Build: sum(col) as aggregate, then convert to window function
        let sum_expr = sum(col_expr);
        let frame = WindowFrame::new_bounds(
            WindowFrameUnits::Rows,
            WindowFrameBound::Preceding(ScalarValue::Null), // UNBOUNDED PRECEDING
            WindowFrameBound::CurrentRow,
        );

        aggregate_to_window(sum_expr, partition_by_exprs, order_by.to_vec(), frame)
    } else {
        Err("Expected Call expression for cum_sum".to_string())
    }
}

/// Convert rolling(n).agg() to aggregate OVER (ORDER BY ... ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW)
///
/// Supports `partition_by` kwarg on the rolling() call:
/// `r.col.rolling(3, partition_by="group").mean()`
fn convert_rolling_agg(
    py_expr: &PyExpr,
    schema: &ArrowSchema,
    order_by: &[Sort],
) -> Result<Expr, String> {
    if let PyExpr::Call { func, on, .. } = py_expr {
        // `on` is the rolling() call
        if let PyExpr::Call {
            args: inner_args,
            on: inner_on,
            kwargs: inner_kwargs,
            ..
        } = on.as_ref()
        {
            // Get window size from rolling() args
            let window_size: i64 = if inner_args.is_empty() {
                return Err("rolling() requires a window size".to_string());
            } else if let PyExpr::Literal { value, .. } = &inner_args[0] {
                value
                    .parse::<i64>()
                    .map_err(|_| "rolling() window size must be an integer".to_string())?
            } else {
                return Err("rolling() window size must be a literal integer".to_string());
            };

            // Get the column being aggregated
            let col_expr = pyexpr_to_datafusion(*inner_on.clone(), schema)?;

            // Extract partition_by from the rolling() call's kwargs
            let partition_by_exprs = extract_partition_by(inner_kwargs, schema)?;

            // Build the appropriate aggregate function
            let agg_expr = match func.as_str() {
                "mean" => avg(col_expr),
                "sum" => sum(col_expr),
                "min" => min(col_expr),
                "max" => max(col_expr),
                "count" => count(col_expr),
                "std" => stddev(col_expr),
                _ => return Err(format!("Unknown rolling aggregation function: {}", func)),
            };

            // Build window frame: ROWS BETWEEN (window_size-1) PRECEDING AND CURRENT ROW
            let frame = WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(ScalarValue::UInt64(Some(
                    (window_size - 1).max(0) as u64,
                ))),
                WindowFrameBound::CurrentRow,
            );

            aggregate_to_window(agg_expr, partition_by_exprs, order_by.to_vec(), frame)
        } else {
            Err("Expected rolling() call as inner expression".to_string())
        }
    } else {
        Err("Expected Call expression for rolling aggregation".to_string())
    }
}

/// Convert PyExpr::Window (row_number, rank, dense_rank, ntile) to native window expressions
fn convert_window_ranking(
    py_expr: &PyExpr,
    schema: &ArrowSchema,
    order_by: &[Sort],
) -> Result<Expr, String> {
    if let PyExpr::Window {
        expr,
        partition_by,
        order_by: window_order_by,
        descending,
    } = py_expr
    {
        // Get the inner function call
        let (func_name, args) = match expr.as_ref() {
            PyExpr::Call { func, args, .. } => (func.as_str(), args),
            _ => return Err("Window expression must wrap a function call".to_string()),
        };

        // Create the window function
        let window_expr = match func_name {
            "row_number" => row_number(),
            "rank" => rank(),
            "dense_rank" => dense_rank(),
            "ntile" => {
                if args.is_empty() {
                    return Err("ntile() requires a bucket count argument".to_string());
                }
                if let PyExpr::Literal { value, .. } = &args[0] {
                    let n = value
                        .parse::<i64>()
                        .map_err(|_| "ntile() bucket count must be an integer".to_string())?;
                    ntile(lit(n))
                } else {
                    return Err("ntile() bucket count must be a literal integer".to_string());
                }
            }
            _ => return Err(format!("Unsupported window function: {}", func_name)),
        };

        // Build PARTITION BY
        let partition_by_exprs = if let Some(pb) = partition_by {
            vec![pyexpr_to_datafusion(*pb.clone(), schema)?]
        } else {
            vec![]
        };

        // Build ORDER BY - prefer the window's own order_by, fall back to table sort_exprs
        let window_order: Vec<Sort> = if let Some(ob) = window_order_by {
            let ob_expr = pyexpr_to_datafusion(*ob.clone(), schema)?;
            vec![Sort {
                expr: ob_expr,
                asc: !descending,
                nulls_first: !descending, // ascending → nulls first, descending → nulls last
            }]
        } else if !order_by.is_empty() {
            order_by.to_vec()
        } else {
            vec![]
        };

        // Build the final expression with partition/order
        let mut builder = window_expr.order_by(window_order);
        if !partition_by_exprs.is_empty() {
            builder = builder.partition_by(partition_by_exprs);
        }

        builder
            .build()
            .map_err(|e| format!("Failed to build window ranking expr: {}", e))
    } else {
        Err("Expected Window expression".to_string())
    }
}

/// Handle expressions that contain window functions in their children (e.g., BinOp, if_else wrapping shift)
fn convert_expr_with_window_children(
    py_expr: PyExpr,
    schema: &ArrowSchema,
    order_by: &[Sort],
) -> Result<Expr, String> {
    use crate::transpiler::contains_window_function;

    match py_expr {
        PyExpr::BinOp { op, left, right } => {
            let left_has_window = contains_window_function(&left);
            let right_has_window = contains_window_function(&right);

            let left_expr = if left_has_window {
                pyexpr_to_window_inner(*left, schema, order_by)?
            } else {
                pyexpr_to_datafusion(*left, schema)?
            };

            let right_expr = if right_has_window {
                pyexpr_to_window_inner(*right, schema, order_by)?
            } else {
                pyexpr_to_datafusion(*right, schema)?
            };

            let operator = match op.as_str() {
                "Add" => datafusion::logical_expr::Operator::Plus,
                "Sub" => datafusion::logical_expr::Operator::Minus,
                "Mul" => datafusion::logical_expr::Operator::Multiply,
                "Div" => datafusion::logical_expr::Operator::Divide,
                "Mod" => datafusion::logical_expr::Operator::Modulo,
                "Eq" => datafusion::logical_expr::Operator::Eq,
                "Ne" => datafusion::logical_expr::Operator::NotEq,
                "Lt" => datafusion::logical_expr::Operator::Lt,
                "Le" => datafusion::logical_expr::Operator::LtEq,
                "Gt" => datafusion::logical_expr::Operator::Gt,
                "Ge" => datafusion::logical_expr::Operator::GtEq,
                "And" => datafusion::logical_expr::Operator::And,
                "Or" => datafusion::logical_expr::Operator::Or,
                _ => return Err(format!("Unknown binary operator: {}", op)),
            };

            Ok(Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
                Box::new(left_expr),
                operator,
                Box::new(right_expr),
            )))
        }
        PyExpr::UnaryOp { op, operand } => {
            let operand_expr = if contains_window_function(&operand) {
                pyexpr_to_window_inner(*operand, schema, order_by)?
            } else {
                pyexpr_to_datafusion(*operand, schema)?
            };
            match op.as_str() {
                "Not" => Ok(operand_expr.not()),
                _ => Err(format!("Unknown unary operator: {}", op)),
            }
        }
        PyExpr::Call {
            func,
            on,
            args,
            kwargs,
        } => {
            // For non-window function calls that might contain window children
            // (e.g., fill_null(shift(...)), if_else(cond, shift(...), val))
            // We need to recursively convert window sub-expressions

            match func.as_str() {
                "fill_null" => {
                    let on_expr = if contains_window_function(&on) {
                        pyexpr_to_window_inner(*on, schema, order_by)?
                    } else {
                        pyexpr_to_datafusion(*on, schema)?
                    };
                    if args.is_empty() {
                        return Err("fill_null requires a default value".to_string());
                    }
                    let default_expr = if contains_window_function(&args[0]) {
                        pyexpr_to_window_inner(args[0].clone(), schema, order_by)?
                    } else {
                        pyexpr_to_datafusion(args[0].clone(), schema)?
                    };
                    Ok(coalesce(vec![on_expr, default_expr]))
                }
                "is_null" => {
                    let on_expr = if contains_window_function(&on) {
                        pyexpr_to_window_inner(*on, schema, order_by)?
                    } else {
                        pyexpr_to_datafusion(*on, schema)?
                    };
                    Ok(on_expr.is_null())
                }
                "is_not_null" => {
                    let on_expr = if contains_window_function(&on) {
                        pyexpr_to_window_inner(*on, schema, order_by)?
                    } else {
                        pyexpr_to_datafusion(*on, schema)?
                    };
                    Ok(on_expr.is_not_null())
                }
                "if_else" => {
                    if args.len() != 3 {
                        return Err("if_else requires 3 arguments".to_string());
                    }
                    let cond_expr = if contains_window_function(&args[0]) {
                        pyexpr_to_window_inner(args[0].clone(), schema, order_by)?
                    } else {
                        pyexpr_to_datafusion(args[0].clone(), schema)?
                    };
                    let true_expr = if contains_window_function(&args[1]) {
                        pyexpr_to_window_inner(args[1].clone(), schema, order_by)?
                    } else {
                        pyexpr_to_datafusion(args[1].clone(), schema)?
                    };
                    let false_expr = if contains_window_function(&args[2]) {
                        pyexpr_to_window_inner(args[2].clone(), schema, order_by)?
                    } else {
                        pyexpr_to_datafusion(args[2].clone(), schema)?
                    };

                    use datafusion::logical_expr::case;
                    case(cond_expr)
                        .when(lit(true), true_expr)
                        .otherwise(false_expr)
                        .map_err(|e| format!("Failed to create CASE expression: {}", e))
                }
                "abs" => {
                    // abs(x) where x might contain window functions
                    if args.is_empty() {
                        return Err("abs() requires an argument".to_string());
                    }
                    let arg_expr = if contains_window_function(&args[0]) {
                        pyexpr_to_window_inner(args[0].clone(), schema, order_by)?
                    } else {
                        pyexpr_to_datafusion(args[0].clone(), schema)?
                    };
                    Ok(datafusion::functions::math::expr_fn::abs(arg_expr))
                }
                "ceil" => {
                    if args.is_empty() {
                        return Err("ceil() requires an argument".to_string());
                    }
                    let arg_expr = if contains_window_function(&args[0]) {
                        pyexpr_to_window_inner(args[0].clone(), schema, order_by)?
                    } else {
                        pyexpr_to_datafusion(args[0].clone(), schema)?
                    };
                    Ok(datafusion::functions::math::expr_fn::ceil(arg_expr))
                }
                "floor" => {
                    if args.is_empty() {
                        return Err("floor() requires an argument".to_string());
                    }
                    let arg_expr = if contains_window_function(&args[0]) {
                        pyexpr_to_window_inner(args[0].clone(), schema, order_by)?
                    } else {
                        pyexpr_to_datafusion(args[0].clone(), schema)?
                    };
                    Ok(datafusion::functions::math::expr_fn::floor(arg_expr))
                }
                "round" => {
                    if args.is_empty() {
                        return Err("round() requires an argument".to_string());
                    }
                    let arg_expr = if contains_window_function(&args[0]) {
                        pyexpr_to_window_inner(args[0].clone(), schema, order_by)?
                    } else {
                        pyexpr_to_datafusion(args[0].clone(), schema)?
                    };
                    // round takes (value, decimal_places)
                    let decimals = if args.len() > 1 {
                        if let PyExpr::Literal { value, .. } = &args[1] {
                            value.parse::<i64>().unwrap_or(0)
                        } else {
                            0
                        }
                    } else {
                        0
                    };
                    Ok(datafusion::functions::math::expr_fn::round(vec![
                        arg_expr,
                        lit(decimals),
                    ]))
                }
                _ => {
                    // For other function calls, try to handle as non-window
                    // and reconstruct
                    let reconstructed = PyExpr::Call {
                        func,
                        on,
                        args,
                        kwargs,
                    };
                    pyexpr_to_datafusion(reconstructed, schema)
                }
            }
        }
        // For non-window expressions, use the standard converter
        other => pyexpr_to_datafusion(other, schema),
    }
}

/// Convert literal value string + dtype to ScalarValue
fn literal_to_scalar_value(value: &str, dtype: &str) -> Result<ScalarValue, String> {
    match dtype {
        "Int64" => {
            let v = value
                .parse::<i64>()
                .map_err(|_| format!("Failed to parse '{}' as Int64", value))?;
            Ok(ScalarValue::Int64(Some(v)))
        }
        "Int32" => {
            let v = value
                .parse::<i32>()
                .map_err(|_| format!("Failed to parse '{}' as Int32", value))?;
            Ok(ScalarValue::Int32(Some(v)))
        }
        "Float64" => {
            let v = value
                .parse::<f64>()
                .map_err(|_| format!("Failed to parse '{}' as Float64", value))?;
            Ok(ScalarValue::Float64(Some(v)))
        }
        "Float32" => {
            let v = value
                .parse::<f32>()
                .map_err(|_| format!("Failed to parse '{}' as Float32", value))?;
            Ok(ScalarValue::Float32(Some(v)))
        }
        "String" | "Utf8" => Ok(ScalarValue::Utf8(Some(value.to_string()))),
        "Boolean" | "Bool" => {
            let b = match value.to_lowercase().as_str() {
                "true" => true,
                "false" => false,
                _ => return Err(format!("Failed to parse '{}' as Boolean", value)),
            };
            Ok(ScalarValue::Boolean(Some(b)))
        }
        "Null" => Ok(ScalarValue::Null),
        _ => Err(format!("Unknown dtype for ScalarValue: {}", dtype)),
    }
}
