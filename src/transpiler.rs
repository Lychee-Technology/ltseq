//! Expression transpilation: Convert Python expressions to DataFusion expressions
//!
//! This module handles the conversion of serialized Python expressions (PyExpr)
//! into DataFusion's native expression format, including detection of window functions
//! and SQL transpilation for complex operations.
//!
//! ## Expression Optimization
//!
//! This module includes compile-time optimizations:
//! - **Constant Folding**: Arithmetic operations on literals are evaluated at compile time
//!   (e.g., `1 + 2 + r.col` → `3 + r.col`)
//! - **Boolean Simplification**: Trivial boolean expressions are simplified
//!   (e.g., `x & True` → `x`, `x | False` → `x`)

use crate::types::PyExpr;
use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;

// String functions
use datafusion::functions::string::expr_fn::{
    btrim, contains, ends_with, lower, starts_with, upper,
};
// Unicode functions (for length, substr)
use datafusion::functions::unicode::expr_fn::{character_length, substring};
// Datetime functions
use datafusion::functions::datetime::expr_fn::date_part;
// Regex functions
use datafusion::functions::regex::expr_fn::regexp_like;

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
            // Python uses "True"/"False" (capitalized), Rust expects "true"/"false"
            let bool_val = match value.to_lowercase().as_str() {
                "true" => true,
                "false" => false,
                _ => return Err(format!("Failed to parse '{}' as Boolean", value)),
            };
            Ok(lit(bool_val))
        }
        "Null" => Ok(lit(ScalarValue::Null)),
        _ => Err(format!("Unknown dtype: {}", dtype)),
    }
}

// ============================================================================
// Constant Folding Optimization
// ============================================================================

/// Extract numeric value from a literal PyExpr (for constant folding)
fn get_literal_f64(expr: &PyExpr) -> Option<f64> {
    match expr {
        PyExpr::Literal { value, dtype } => match dtype.as_str() {
            "Int64" | "Int32" => value.parse::<i64>().ok().map(|v| v as f64),
            "Float64" | "Float32" => value.parse::<f64>().ok(),
            _ => None,
        },
        _ => None,
    }
}

/// Extract boolean value from a literal PyExpr
fn get_literal_bool(expr: &PyExpr) -> Option<bool> {
    match expr {
        PyExpr::Literal { value, dtype } => match dtype.as_str() {
            "Boolean" | "Bool" => match value.to_lowercase().as_str() {
                "true" => Some(true),
                "false" => Some(false),
                _ => None,
            },
            _ => None,
        },
        _ => None,
    }
}

/// Create a literal PyExpr from a float value
fn make_literal_f64(value: f64) -> PyExpr {
    // If it's a whole number, prefer Int64 representation
    if value.fract() == 0.0 && value.abs() < i64::MAX as f64 {
        PyExpr::Literal {
            value: (value as i64).to_string(),
            dtype: "Int64".to_string(),
        }
    } else {
        PyExpr::Literal {
            value: value.to_string(),
            dtype: "Float64".to_string(),
        }
    }
}

/// Create a literal PyExpr from a boolean value
fn make_literal_bool(value: bool) -> PyExpr {
    PyExpr::Literal {
        value: value.to_string(),
        dtype: "Boolean".to_string(),
    }
}

/// Try to fold a binary operation on two literals into a single literal
fn try_fold_binop(op: &str, left: &PyExpr, right: &PyExpr) -> Option<PyExpr> {
    // Try arithmetic folding
    if let (Some(l), Some(r)) = (get_literal_f64(left), get_literal_f64(right)) {
        let result = match op {
            "Add" => Some(l + r),
            "Sub" => Some(l - r),
            "Mul" => Some(l * r),
            "Div" if r != 0.0 => Some(l / r),
            "Mod" if r != 0.0 => Some(l % r),
            _ => None,
        };
        if let Some(v) = result {
            return Some(make_literal_f64(v));
        }

        // Try comparison folding
        let cmp_result = match op {
            "Eq" => Some(l == r),
            "Ne" => Some(l != r),
            "Lt" => Some(l < r),
            "Le" => Some(l <= r),
            "Gt" => Some(l > r),
            "Ge" => Some(l >= r),
            _ => None,
        };
        if let Some(b) = cmp_result {
            return Some(make_literal_bool(b));
        }
    }

    // Try boolean folding
    if let (Some(l), Some(r)) = (get_literal_bool(left), get_literal_bool(right)) {
        let result = match op {
            "And" => Some(l && r),
            "Or" => Some(l || r),
            _ => None,
        };
        if let Some(b) = result {
            return Some(make_literal_bool(b));
        }
    }

    None
}

/// Boolean simplification: x & True → x, x | False → x, etc.
fn try_simplify_boolean(op: &str, left: &PyExpr, right: &PyExpr) -> Option<PyExpr> {
    // Check for identity operations with True/False literals
    let left_bool = get_literal_bool(left);
    let right_bool = get_literal_bool(right);

    match op {
        "And" => {
            // x & True → x
            if right_bool == Some(true) {
                return Some(left.clone());
            }
            // True & x → x
            if left_bool == Some(true) {
                return Some(right.clone());
            }
            // x & False → False
            if right_bool == Some(false) || left_bool == Some(false) {
                return Some(make_literal_bool(false));
            }
        }
        "Or" => {
            // x | False → x
            if right_bool == Some(false) {
                return Some(left.clone());
            }
            // False | x → x
            if left_bool == Some(false) {
                return Some(right.clone());
            }
            // x | True → True
            if right_bool == Some(true) || left_bool == Some(true) {
                return Some(make_literal_bool(true));
            }
        }
        _ => {}
    }

    None
}

/// Recursively optimize a PyExpr tree with constant folding
pub fn optimize_expr(expr: PyExpr) -> PyExpr {
    match expr {
        PyExpr::BinOp { op, left, right } => {
            // First, recursively optimize children
            let opt_left = optimize_expr(*left);
            let opt_right = optimize_expr(*right);

            // Try constant folding (both operands are literals)
            if let Some(folded) = try_fold_binop(&op, &opt_left, &opt_right) {
                return folded;
            }

            // Try boolean simplification (one operand is True/False literal)
            if let Some(simplified) = try_simplify_boolean(&op, &opt_left, &opt_right) {
                return simplified;
            }

            // No optimization possible, return optimized children
            PyExpr::BinOp {
                op,
                left: Box::new(opt_left),
                right: Box::new(opt_right),
            }
        }
        PyExpr::UnaryOp { op, operand } => {
            let opt_operand = optimize_expr(*operand);

            // Try to fold unary operations
            if op == "Not" {
                if let Some(b) = get_literal_bool(&opt_operand) {
                    return make_literal_bool(!b);
                }
            }

            PyExpr::UnaryOp {
                op,
                operand: Box::new(opt_operand),
            }
        }
        PyExpr::Call {
            func,
            on,
            args,
            kwargs,
        } => {
            // Optimize the 'on' expression and all arguments
            let opt_on = optimize_expr(*on);
            let opt_args: Vec<PyExpr> = args.into_iter().map(optimize_expr).collect();

            PyExpr::Call {
                func,
                on: Box::new(opt_on),
                args: opt_args,
                kwargs,
            }
        }
        // Column and Literal expressions are already optimal
        _ => expr,
    }
}

/// Parse a binary operation into a DataFusion expression
fn parse_binop_expr(
    op: &str,
    left: PyExpr,
    right: PyExpr,
    schema: &ArrowSchema,
) -> Result<Expr, String> {
    let left_expr = pyexpr_to_datafusion_inner(left, schema)?;
    let right_expr = pyexpr_to_datafusion_inner(right, schema)?;

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
    let operand_expr = pyexpr_to_datafusion_inner(operand, schema)?;
    match op {
        "Not" => Ok(operand_expr.not()),
        _ => Err(format!("Unknown unary operator: {}", op)),
    }
}

/// Parse a function call into a DataFusion expression
fn parse_call_expr(
    func: &str,
    on: PyExpr,
    args: Vec<PyExpr>,
    schema: &ArrowSchema,
) -> Result<Expr, String> {
    match func {
        "if_else" => {
            // if_else(condition, true_value, false_value)
            if args.len() != 3 {
                return Err(
                    "if_else requires 3 arguments: condition, true_value, false_value".to_string(),
                );
            }
            let cond_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;
            let true_expr = pyexpr_to_datafusion_inner(args[1].clone(), schema)?;
            let false_expr = pyexpr_to_datafusion_inner(args[2].clone(), schema)?;

            // DataFusion's CASE WHEN equivalent using when().otherwise()
            use datafusion::logical_expr::case;
            Ok(case(cond_expr)
                .when(lit(true), true_expr)
                .otherwise(false_expr)
                .map_err(|e| format!("Failed to create CASE expression: {}", e))?)
        }
        "fill_null" => {
            // fill_null(default_value) - COALESCE equivalent
            if args.is_empty() {
                return Err("fill_null requires a default value argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            let default_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;
            Ok(coalesce(vec![on_expr, default_expr]))
        }
        "is_null" => {
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            Ok(on_expr.is_null())
        }
        "is_not_null" => {
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
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
        // ========== String Operations ==========
        "str_contains" => {
            validate_string_column(&on, schema, "str_contains")?;
            if args.is_empty() {
                return Err("str_contains requires a pattern argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            let pattern_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;
            Ok(contains(on_expr, pattern_expr))
        }
        "str_starts_with" => {
            validate_string_column(&on, schema, "str_starts_with")?;
            if args.is_empty() {
                return Err("str_starts_with requires a prefix argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            let prefix_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;
            Ok(starts_with(on_expr, prefix_expr))
        }
        "str_ends_with" => {
            validate_string_column(&on, schema, "str_ends_with")?;
            if args.is_empty() {
                return Err("str_ends_with requires a suffix argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            let suffix_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;
            Ok(ends_with(on_expr, suffix_expr))
        }
        "str_lower" => {
            validate_string_column(&on, schema, "str_lower")?;
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            Ok(lower(on_expr))
        }
        "str_upper" => {
            validate_string_column(&on, schema, "str_upper")?;
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            Ok(upper(on_expr))
        }
        "str_strip" => {
            validate_string_column(&on, schema, "str_strip")?;
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            Ok(btrim(vec![on_expr])) // btrim with single arg = strip whitespace from both sides
        }
        "str_len" => {
            validate_string_column(&on, schema, "str_len")?;
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            Ok(character_length(on_expr))
        }
        "str_slice" => {
            validate_string_column(&on, schema, "str_slice")?;
            if args.len() < 2 {
                return Err("str_slice requires start and length arguments".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            let start_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;
            let length_expr = pyexpr_to_datafusion_inner(args[1].clone(), schema)?;
            // DataFusion's substring uses 1-based indexing, Python uses 0-based
            // substring(string, position, length) - position is 1-based
            Ok(substring(on_expr, start_expr + lit(1), length_expr))
        }
        "str_regex_match" => {
            validate_string_column(&on, schema, "str_regex_match")?;
            if args.is_empty() {
                return Err("str_regex_match requires a pattern argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            let pattern_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;
            Ok(regexp_like(on_expr, pattern_expr, None))
        }
        // ========== Temporal Operations ==========
        "dt_year" => {
            validate_temporal_column(&on, schema, "dt_year")?;
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            Ok(date_part(lit("year"), on_expr))
        }
        "dt_month" => {
            validate_temporal_column(&on, schema, "dt_month")?;
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            Ok(date_part(lit("month"), on_expr))
        }
        "dt_day" => {
            validate_temporal_column(&on, schema, "dt_day")?;
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            Ok(date_part(lit("day"), on_expr))
        }
        "dt_hour" => {
            validate_temporal_column(&on, schema, "dt_hour")?;
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            Ok(date_part(lit("hour"), on_expr))
        }
        "dt_minute" => {
            validate_temporal_column(&on, schema, "dt_minute")?;
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            Ok(date_part(lit("minute"), on_expr))
        }
        "dt_second" => {
            validate_temporal_column(&on, schema, "dt_second")?;
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            Ok(date_part(lit("second"), on_expr))
        }
        "dt_add" => {
            validate_temporal_column(&on, schema, "dt_add")?;
            if args.len() < 3 {
                return Err("dt_add requires days, months, and years arguments".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;

            // Extract literal values for interval construction
            let days = match &args[0] {
                PyExpr::Literal { value, .. } => value.parse::<i32>().unwrap_or(0),
                _ => return Err("dt_add days must be a literal integer".to_string()),
            };
            let months = match &args[1] {
                PyExpr::Literal { value, .. } => value.parse::<i32>().unwrap_or(0),
                _ => return Err("dt_add months must be a literal integer".to_string()),
            };
            let years = match &args[2] {
                PyExpr::Literal { value, .. } => value.parse::<i32>().unwrap_or(0),
                _ => return Err("dt_add years must be a literal integer".to_string()),
            };

            // Create IntervalMonthDayNano: months include years*12, days, nanos=0
            let total_months = years * 12 + months;
            let interval = ScalarValue::new_interval_mdn(total_months, days, 0);

            // date + interval
            Ok(on_expr + lit(interval))
        }
        "dt_diff" => {
            validate_temporal_column(&on, schema, "dt_diff")?;
            if args.is_empty() {
                return Err("dt_diff requires another date argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            let other_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;

            // Compute difference: subtract dates and extract days
            // For Date32/Date64, subtraction returns an interval
            // We use date_part to extract days from the result
            // Note: This gives the difference in days as a float
            let diff_expr = on_expr - other_expr;
            Ok(date_part(lit("day"), diff_expr))
        }
        _ => Err(format!("Method '{}' not yet supported", func)),
    }
}

/// Convert PyExpr to DataFusion Expr
///
/// This function first applies expression optimization (constant folding,
/// boolean simplification) before converting to DataFusion expressions.
pub fn pyexpr_to_datafusion(py_expr: PyExpr, schema: &ArrowSchema) -> Result<Expr, String> {
    // Apply optimization pass first
    let optimized = optimize_expr(py_expr);

    pyexpr_to_datafusion_inner(optimized, schema)
}

/// Internal conversion without optimization (used after optimization pass)
fn pyexpr_to_datafusion_inner(py_expr: PyExpr, schema: &ArrowSchema) -> Result<Expr, String> {
    match py_expr {
        PyExpr::Column(name) => parse_column_expr(&name, schema),
        PyExpr::Literal { value, dtype } => parse_literal_expr(&value, &dtype),
        PyExpr::BinOp { op, left, right } => parse_binop_expr(&op, *left, *right, schema),
        PyExpr::UnaryOp { op, operand } => parse_unaryop_expr(&op, *operand, schema),
        PyExpr::Call { func, on, args, .. } => parse_call_expr(&func, *on, args, schema),
    }
}

/// Helper function to detect if a PyExpr contains a window function call
pub fn contains_window_function(py_expr: &PyExpr) -> bool {
    match py_expr {
        PyExpr::Call { func, on, args, .. } => {
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
            if contains_window_function(on) {
                return true;
            }
            // Check recursively in the `args` field (for standalone functions like abs(x))
            for arg in args {
                if contains_window_function(arg) {
                    return true;
                }
            }
            false
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

/// Check if a DataType is a string type
fn is_string_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Utf8 | DataType::LargeUtf8)
}

/// Check if a DataType is a temporal type (date/datetime)
fn is_temporal_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
            | DataType::Time32(_)
            | DataType::Time64(_)
    )
}

/// Get the column type from schema, returning None if column not found
fn get_column_type(col_name: &str, schema: &ArrowSchema) -> Option<DataType> {
    schema
        .fields()
        .iter()
        .find(|f| f.name() == col_name)
        .map(|f| f.data_type().clone())
}

/// Validate that a column is a string type
fn validate_string_column(
    on: &PyExpr,
    schema: &ArrowSchema,
    func_name: &str,
) -> Result<(), String> {
    if let PyExpr::Column(col_name) = on {
        if let Some(dtype) = get_column_type(col_name, schema) {
            if !is_string_type(&dtype) {
                return Err(format!(
                    "String function '{}' requires a string column, but '{}' has type {:?}",
                    func_name, col_name, dtype
                ));
            }
        }
    }
    Ok(())
}

/// Validate that a column is a temporal type
fn validate_temporal_column(
    on: &PyExpr,
    schema: &ArrowSchema,
    func_name: &str,
) -> Result<(), String> {
    if let PyExpr::Column(col_name) = on {
        if let Some(dtype) = get_column_type(col_name, schema) {
            if !is_temporal_type(&dtype) {
                return Err(format!(
                    "Temporal function '{}' requires a date/datetime column, but '{}' has type {:?}. \
                     Consider using to_date() or to_timestamp() to convert the column first.",
                    func_name, col_name, dtype
                ));
            }
        }
    }
    Ok(())
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
        // Conditional expressions
        "if_else" => {
            if args.len() != 3 {
                return Err(
                    "if_else requires 3 arguments: condition, true_value, false_value".to_string(),
                );
            }
            let cond_sql = pyexpr_to_sql(&args[0], schema)?;
            let true_sql = pyexpr_to_sql(&args[1], schema)?;
            let false_sql = pyexpr_to_sql(&args[2], schema)?;
            Ok(format!(
                "CASE WHEN {} THEN {} ELSE {} END",
                cond_sql, true_sql, false_sql
            ))
        }
        // Null handling
        "fill_null" => {
            if args.is_empty() {
                return Err("fill_null requires a default value argument".to_string());
            }
            let on_sql = pyexpr_to_sql(on, schema)?;
            let default_sql = pyexpr_to_sql(&args[0], schema)?;
            Ok(format!("COALESCE({}, {})", on_sql, default_sql))
        }
        "is_null" => {
            let on_sql = pyexpr_to_sql(on, schema)?;
            Ok(format!("{} IS NULL", on_sql))
        }
        "is_not_null" => {
            let on_sql = pyexpr_to_sql(on, schema)?;
            Ok(format!("{} IS NOT NULL", on_sql))
        }
        // String operations
        "str_contains" => {
            let on_sql = pyexpr_to_sql(on, schema)?;
            if args.is_empty() {
                return Err("str_contains requires a pattern argument".to_string());
            }
            let pattern = pyexpr_to_sql(&args[0], schema)?;
            Ok(format!("POSITION({} IN {}) > 0", pattern, on_sql))
        }
        "str_starts_with" => {
            let on_sql = pyexpr_to_sql(on, schema)?;
            if args.is_empty() {
                return Err("str_starts_with requires a prefix argument".to_string());
            }
            let prefix = pyexpr_to_sql(&args[0], schema)?;
            Ok(format!("LEFT({}, LENGTH({})) = {}", on_sql, prefix, prefix))
        }
        "str_ends_with" => {
            let on_sql = pyexpr_to_sql(on, schema)?;
            if args.is_empty() {
                return Err("str_ends_with requires a suffix argument".to_string());
            }
            let suffix = pyexpr_to_sql(&args[0], schema)?;
            Ok(format!(
                "RIGHT({}, LENGTH({})) = {}",
                on_sql, suffix, suffix
            ))
        }
        "str_lower" => {
            let on_sql = pyexpr_to_sql(on, schema)?;
            Ok(format!("LOWER({})", on_sql))
        }
        "str_upper" => {
            let on_sql = pyexpr_to_sql(on, schema)?;
            Ok(format!("UPPER({})", on_sql))
        }
        "str_strip" => {
            let on_sql = pyexpr_to_sql(on, schema)?;
            Ok(format!("TRIM({})", on_sql))
        }
        "str_len" => {
            let on_sql = pyexpr_to_sql(on, schema)?;
            Ok(format!("LENGTH({})", on_sql))
        }
        "str_slice" => {
            let on_sql = pyexpr_to_sql(on, schema)?;
            if args.len() < 2 {
                return Err("str_slice requires start and length arguments".to_string());
            }
            let start = pyexpr_to_sql(&args[0], schema)?;
            let length = pyexpr_to_sql(&args[1], schema)?;
            // SQL uses 1-based indexing, Python uses 0-based
            Ok(format!("SUBSTRING({}, {} + 1, {})", on_sql, start, length))
        }
        "str_regex_match" => {
            let on_sql = pyexpr_to_sql(on, schema)?;
            if args.is_empty() {
                return Err("str_regex_match requires a pattern argument".to_string());
            }
            let pattern = pyexpr_to_sql(&args[0], schema)?;
            Ok(format!("REGEXP_LIKE({}, {})", on_sql, pattern))
        }
        // Temporal operations
        "dt_year" => {
            let on_sql = pyexpr_to_sql(on, schema)?;
            Ok(format!("EXTRACT(YEAR FROM {})", on_sql))
        }
        "dt_month" => {
            let on_sql = pyexpr_to_sql(on, schema)?;
            Ok(format!("EXTRACT(MONTH FROM {})", on_sql))
        }
        "dt_day" => {
            let on_sql = pyexpr_to_sql(on, schema)?;
            Ok(format!("EXTRACT(DAY FROM {})", on_sql))
        }
        "dt_hour" => {
            let on_sql = pyexpr_to_sql(on, schema)?;
            Ok(format!("EXTRACT(HOUR FROM {})", on_sql))
        }
        "dt_minute" => {
            let on_sql = pyexpr_to_sql(on, schema)?;
            Ok(format!("EXTRACT(MINUTE FROM {})", on_sql))
        }
        "dt_second" => {
            let on_sql = pyexpr_to_sql(on, schema)?;
            Ok(format!("EXTRACT(SECOND FROM {})", on_sql))
        }
        "dt_add" => {
            let on_sql = pyexpr_to_sql(on, schema)?;
            if args.len() < 3 {
                return Err("dt_add requires days, months, and years arguments".to_string());
            }
            let days = pyexpr_to_sql(&args[0], schema)?;
            let months = pyexpr_to_sql(&args[1], schema)?;
            let years = pyexpr_to_sql(&args[2], schema)?;
            // Build interval string dynamically
            Ok(format!(
                "({} + INTERVAL '{}' DAY + INTERVAL '{}' MONTH + INTERVAL '{}' YEAR)",
                on_sql, days, months, years
            ))
        }
        "dt_diff" => {
            let on_sql = pyexpr_to_sql(on, schema)?;
            if args.is_empty() {
                return Err("dt_diff requires another date argument".to_string());
            }
            let other = pyexpr_to_sql(&args[0], schema)?;
            Ok(format!("DATEDIFF('day', {}, {})", other, on_sql))
        }
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
