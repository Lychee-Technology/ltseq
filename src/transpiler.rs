//! Expression transpilation: Convert Python expressions to DataFusion expressions
//!
//! This module handles the conversion of serialized Python expressions (PyExpr)
//! into DataFusion's native expression format, including detection of window functions
//! and SQL transpilation for complex operations.

use crate::types::PyExpr;
use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;

/// Parse a column reference into a DataFusion expression
fn parse_column_expr(name: &str, schema: &ArrowSchema) -> Result<Expr, String> {
    if !schema.fields().iter().any(|f| f.name() == name) {
        return Err(format!("Column '{}' not found in schema", name));
    }
    // Use Column::new_unqualified to preserve case-sensitive column names
    // (col() function lowercases column names, which breaks uppercase column names like 'IsOfficial')
    use datafusion::common::Column;
    Ok(Expr::Column(Column::new_unqualified(name)))
}

/// Parse a literal value based on its dtype into a DataFusion expression
fn parse_literal_expr(value: &str, dtype: &str) -> Result<Expr, String> {
    match dtype {
        "Int64" => {
            let int_val = value
                .parse::<i64>()
                .map_err(|_| format!("Failed to parse '{}' as Int64", value))?;
            Ok(lit(int_val))
        }
        "Int32" => {
            let int_val = value
                .parse::<i32>()
                .map_err(|_| format!("Failed to parse '{}' as Int32", value))?;
            Ok(lit(int_val))
        }
        "Float64" => {
            let float_val = value
                .parse::<f64>()
                .map_err(|_| format!("Failed to parse '{}' as Float64", value))?;
            Ok(lit(float_val))
        }
        "Float32" => {
            let float_val = value
                .parse::<f32>()
                .map_err(|_| format!("Failed to parse '{}' as Float32", value))?;
            Ok(lit(float_val))
        }
        "String" | "Utf8" => Ok(lit(value)),
        "Boolean" | "Bool" => {
            let bool_val = value
                .parse::<bool>()
                .map_err(|_| format!("Failed to parse '{}' as Boolean", value))?;
            Ok(lit(bool_val))
        }
        "Null" => Ok(lit(ScalarValue::Null)),
        _ => Err(format!("Unknown dtype: {}", dtype)),
    }
}

/// Parse a binary operation into a DataFusion expression
fn parse_binop_expr(
    op: &str,
    left: PyExpr,
    right: PyExpr,
    schema: &ArrowSchema,
) -> Result<Expr, String> {
    let left_expr = pyexpr_to_datafusion(left, schema)?;
    let right_expr = pyexpr_to_datafusion(right, schema)?;

    let operator = match op {
        "Add" => Operator::Plus,
        "Sub" => Operator::Minus,
        "Mul" => Operator::Multiply,
        "Div" => Operator::Divide,
        "Mod" => Operator::Modulo,
        "Eq" => Operator::Eq,
        "Ne" => Operator::NotEq,
        "Lt" => Operator::Lt,
        "Le" => Operator::LtEq,
        "Gt" => Operator::Gt,
        "Ge" => Operator::GtEq,
        "And" => Operator::And,
        "Or" => Operator::Or,
        _ => return Err(format!("Unknown binary operator: {}", op)),
    };

    Ok(Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left_expr),
        operator,
        Box::new(right_expr),
    )))
}

/// Parse a unary operation into a DataFusion expression
fn parse_unaryop_expr(op: &str, operand: PyExpr, schema: &ArrowSchema) -> Result<Expr, String> {
    let operand_expr = pyexpr_to_datafusion(operand, schema)?;
    match op {
        "Not" => Ok(operand_expr.not()),
        _ => Err(format!("Unknown unary operator: {}", op)),
    }
}

/// Parse a function call into a DataFusion expression
fn parse_call_expr(func: &str, on: PyExpr, schema: &ArrowSchema) -> Result<Expr, String> {
    match func {
        "is_null" => {
            let on_expr = pyexpr_to_datafusion(on, schema)?;
            Ok(on_expr.is_null())
        }
        "is_not_null" => {
            let on_expr = pyexpr_to_datafusion(on, schema)?;
            Ok(on_expr.is_not_null())
        }
        "abs" | "ceil" | "floor" | "round" => Err(format!(
            "Math function '{}' requires window context - should be handled in derive()",
            func
        )),
        "shift" | "rolling" | "diff" | "cum_sum" | "mean" | "sum" | "min" | "max" | "count"
        | "std" => Err(format!(
            "Window function '{}' requires DataFrame context - should be handled in derive()",
            func
        )),
        _ => Err(format!("Method '{}' not yet supported", func)),
    }
}

/// Convert PyExpr to DataFusion Expr
pub fn pyexpr_to_datafusion(py_expr: PyExpr, schema: &ArrowSchema) -> Result<Expr, String> {
    match py_expr {
        PyExpr::Column(name) => parse_column_expr(&name, schema),
        PyExpr::Literal { value, dtype } => parse_literal_expr(&value, &dtype),
        PyExpr::BinOp { op, left, right } => parse_binop_expr(&op, *left, *right, schema),
        PyExpr::UnaryOp { op, operand } => parse_unaryop_expr(&op, *operand, schema),
        PyExpr::Call { func, on, .. } => parse_call_expr(&func, *on, schema),
    }
}

/// Helper function to detect if a PyExpr contains a window function call
pub fn contains_window_function(py_expr: &PyExpr) -> bool {
    match py_expr {
        PyExpr::Call { func, on, .. } => {
            // Direct window functions
            if matches!(func.as_str(), "shift" | "rolling" | "diff" | "cum_sum") {
                return true;
            }
            // Aggregation functions applied to rolling windows
            if matches!(
                func.as_str(),
                "mean" | "sum" | "min" | "max" | "count" | "std"
            ) {
                // Check if this is applied to a rolling() call
                if let PyExpr::Call {
                    func: inner_func, ..
                } = &**on
                {
                    if matches!(inner_func.as_str(), "rolling") {
                        return true;
                    }
                }
            }
            // Check recursively in the `on` field
            contains_window_function(on)
        }
        PyExpr::BinOp { left, right, .. } => {
            contains_window_function(left) || contains_window_function(right)
        }
        PyExpr::UnaryOp { operand, .. } => contains_window_function(operand),
        _ => false,
    }
}

/// Check if a DataType is numeric (can be summed)
pub fn is_numeric_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Null // Allow Null type for empty tables
    )
}

/// Handle SQL for shift() window function
fn sql_shift(on: &PyExpr, args: &[PyExpr], schema: &ArrowSchema) -> Result<String, String> {
    let on_sql = pyexpr_to_sql(on, schema)?;
    let offset = if args.is_empty() {
        1
    } else if let PyExpr::Literal { value, .. } = &args[0] {
        value.parse::<i32>().unwrap_or(1)
    } else {
        return Err("shift() offset must be a literal integer".to_string());
    };

    if offset >= 0 {
        Ok(format!("LAG({}, {})", on_sql, offset))
    } else {
        Ok(format!("LEAD({}, {})", on_sql, -offset))
    }
}

/// Handle SQL for diff() window function
fn sql_diff(on: &PyExpr, args: &[PyExpr], schema: &ArrowSchema) -> Result<String, String> {
    let on_sql = pyexpr_to_sql(on, schema)?;
    let periods = if args.is_empty() {
        1
    } else if let PyExpr::Literal { value, .. } = &args[0] {
        value.parse::<i32>().unwrap_or(1)
    } else {
        return Err("diff() periods must be a literal integer".to_string());
    };
    Ok(format!("{} - LAG({}, {})", on_sql, on_sql, periods))
}

/// Handle SQL for abs() math function
fn sql_abs(args: &[PyExpr], schema: &ArrowSchema) -> Result<String, String> {
    if args.is_empty() {
        return Err("abs() requires an argument".to_string());
    }
    let arg_sql = pyexpr_to_sql(&args[0], schema)?;
    Ok(format!("ABS({})", arg_sql))
}

/// Handle SQL for ceil() math function
fn sql_ceil(args: &[PyExpr], schema: &ArrowSchema) -> Result<String, String> {
    if args.is_empty() {
        return Err("ceil() requires an argument".to_string());
    }
    let arg_sql = pyexpr_to_sql(&args[0], schema)?;
    Ok(format!("CEIL({})", arg_sql))
}

/// Handle SQL for floor() math function
fn sql_floor(args: &[PyExpr], schema: &ArrowSchema) -> Result<String, String> {
    if args.is_empty() {
        return Err("floor() requires an argument".to_string());
    }
    let arg_sql = pyexpr_to_sql(&args[0], schema)?;
    Ok(format!("FLOOR({})", arg_sql))
}

/// Handle SQL for round() math function
fn sql_round(args: &[PyExpr], schema: &ArrowSchema) -> Result<String, String> {
    if args.is_empty() {
        return Err("round() requires an argument".to_string());
    }
    let arg_sql = pyexpr_to_sql(&args[0], schema)?;
    let decimals = if args.len() > 1 {
        if let PyExpr::Literal { value, .. } = &args[1] {
            value.parse::<i32>().unwrap_or(0)
        } else {
            0
        }
    } else {
        0
    };
    Ok(format!("ROUND({}, {})", arg_sql, decimals))
}

/// Handle SQL for function calls (shift, diff, abs, ceil, floor, round)
fn sql_call(
    func: &str,
    on: &PyExpr,
    args: &[PyExpr],
    schema: &ArrowSchema,
) -> Result<String, String> {
    match func {
        "shift" => sql_shift(on, args, schema),
        "rolling" => {
            Err("rolling() should not reach pyexpr_to_sql - it's handled separately".to_string())
        }
        "diff" => sql_diff(on, args, schema),
        "cum_sum" => {
            let on_sql = pyexpr_to_sql(on, schema)?;
            Ok(format!("SUM({}) OVER ()", on_sql))
        }
        // Aggregation functions that can be applied to rolling windows
        "mean" | "sum" | "min" | "max" | "count" | "std" => {
            // Check if this is applied to a rolling() call
            if let PyExpr::Call {
                func: inner_func,
                args: inner_args,
                on: inner_on,
                ..
            } = on
            {
                if inner_func == "rolling" {
                    // Get the window size from rolling() args
                    let window_size = if inner_args.is_empty() {
                        return Err("rolling() requires a window size".to_string());
                    } else if let PyExpr::Literal { value, .. } = &inner_args[0] {
                        value.parse::<i32>().unwrap_or(1)
                    } else {
                        return Err("rolling() window size must be a literal integer".to_string());
                    };

                    // Get the column being aggregated
                    let col_sql = pyexpr_to_sql(inner_on.as_ref(), schema)?;

                    // Build the aggregation function name
                    let agg_func = match func {
                        "mean" => "AVG",
                        "sum" => "SUM",
                        "min" => "MIN",
                        "max" => "MAX",
                        "count" => "COUNT",
                        "std" => "STDDEV", // Standard deviation
                        _ => unreachable!(),
                    };

                    // Mark this as a rolling aggregation with window size and func
                    // The window frame will be added in derive_with_window_functions
                    Ok(format!(
                        "__ROLLING_{}__({})__{}",
                        agg_func, col_sql, window_size
                    ))
                } else {
                    Err(format!(
                        "Aggregation function {} must be called on rolling()",
                        func
                    ))
                }
            } else {
                Err(format!(
                    "Aggregation function {} must be called on rolling()",
                    func
                ))
            }
        }
        "abs" => sql_abs(args, schema),
        "ceil" => sql_ceil(args, schema),
        "floor" => sql_floor(args, schema),
        "round" => sql_round(args, schema),
        _ => Err(format!("Unsupported function in window context: {}", func)),
    }
}

/// Convert PyExpr to SQL string representation for window functions
pub fn pyexpr_to_sql(py_expr: &PyExpr, schema: &ArrowSchema) -> Result<String, String> {
    match py_expr {
        PyExpr::Column(name) => {
            if !schema.fields().iter().any(|f| f.name() == name) {
                return Err(format!("Column '{}' not found in schema", name));
            }
            Ok(format!("\"{}\"", name))
        }

        PyExpr::Literal { value, dtype } => match dtype.as_str() {
            "String" | "Utf8" => Ok(format!("'{}'", value)),
            _ => Ok(value.clone()),
        },

        PyExpr::BinOp { op, left, right } => {
            let left_sql = pyexpr_to_sql(left, schema)?;
            let right_sql = pyexpr_to_sql(right, schema)?;
            let op_str = match op.as_str() {
                "Add" => "+",
                "Sub" => "-",
                "Mul" => "*",
                "Div" => "/",
                "Mod" => "%",
                "Eq" => "=",
                "Ne" => "!=",
                "Lt" => "<",
                "Le" => "<=",
                "Gt" => ">",
                "Ge" => ">=",
                "And" => "AND",
                "Or" => "OR",
                _ => return Err(format!("Unknown binary operator: {}", op)),
            };
            Ok(format!("({} {} {})", left_sql, op_str, right_sql))
        }

        PyExpr::UnaryOp { op, operand } => {
            let operand_sql = pyexpr_to_sql(operand, schema)?;
            match op.as_str() {
                "Not" => Ok(format!("(NOT {})", operand_sql)),
                _ => Err(format!("Unknown unary operator: {}", op)),
            }
        }

        PyExpr::Call { func, args, on, .. } => sql_call(func, on, args, schema),
    }
}
