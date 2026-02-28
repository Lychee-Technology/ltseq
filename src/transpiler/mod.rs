//! Expression transpilation: Convert Python expressions to DataFusion expressions
//!
//! This module handles the conversion of serialized Python expressions (PyExpr)
//! into DataFusion's native expression format, including detection of window functions
//! and SQL transpilation for complex operations.
//!
//! ## Module Structure
//!
//! - `optimization`: Constant folding and boolean simplification
//! - `sql_gen`: SQL string generation for window functions (fallback path)
//! - `window_native`: Native DataFusion window expression builder (primary path)
//!
//! ## Expression Optimization
//!
//! This module includes compile-time optimizations:
//! - **Constant Folding**: Arithmetic operations on literals are evaluated at compile time
//!   (e.g., `1 + 2 + r.col` → `3 + r.col`)
//! - **Boolean Simplification**: Trivial boolean expressions are simplified
//!   (e.g., `x & True` → `x`, `x | False` → `x`)

mod optimization;
mod sql_gen;
pub mod window_native;

pub use optimization::optimize_expr;
pub use sql_gen::pyexpr_to_sql;
pub use window_native::pyexpr_to_window_expr;

use crate::types::PyExpr;
use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;

// String functions
use datafusion::functions::string::expr_fn::{
    btrim, concat, contains, ends_with, lower, replace, split_part, starts_with, upper,
};
// Unicode functions (for length, substr, padding)
use datafusion::functions::unicode::expr_fn::{character_length, lpad, rpad, substring};
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
        // New string operations
        "str_replace" => {
            validate_string_column(&on, schema, "str_replace")?;
            if args.len() < 2 {
                return Err("str_replace requires 'old' and 'new' arguments".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            let old_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;
            let new_expr = pyexpr_to_datafusion_inner(args[1].clone(), schema)?;
            Ok(replace(on_expr, old_expr, new_expr))
        }
        "str_concat" => {
            // Concatenate the source column with all arguments
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            let mut all_args = vec![on_expr];
            for arg in args {
                all_args.push(pyexpr_to_datafusion_inner(arg, schema)?);
            }
            Ok(concat(all_args))
        }
        "str_pad_left" => {
            validate_string_column(&on, schema, "str_pad_left")?;
            if args.is_empty() {
                return Err("str_pad_left requires a width argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            let width_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;
            // Default padding character is space
            let char_expr = if args.len() > 1 {
                pyexpr_to_datafusion_inner(args[1].clone(), schema)?
            } else {
                lit(" ")
            };
            // lpad(string, length, fill_string)
            Ok(lpad(vec![on_expr, width_expr, char_expr]))
        }
        "str_pad_right" => {
            validate_string_column(&on, schema, "str_pad_right")?;
            if args.is_empty() {
                return Err("str_pad_right requires a width argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            let width_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;
            // Default padding character is space
            let char_expr = if args.len() > 1 {
                pyexpr_to_datafusion_inner(args[1].clone(), schema)?
            } else {
                lit(" ")
            };
            // rpad(string, length, fill_string)
            Ok(rpad(vec![on_expr, width_expr, char_expr]))
        }
        "str_split" => {
            validate_string_column(&on, schema, "str_split")?;
            if args.len() < 2 {
                return Err("str_split requires delimiter and index arguments".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            let delimiter_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;
            let index_expr = pyexpr_to_datafusion_inner(args[1].clone(), schema)?;
            // split_part(string, delimiter, index) - 1-based index
            Ok(split_part(on_expr, delimiter_expr, index_expr))
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
        PyExpr::Window { .. } => {
            // Window expressions should be handled via SQL path in derive_with_window_functions
            Err("Window expressions must be handled via SQL transpilation".to_string())
        }
    }
}

/// Helper function to detect if a PyExpr contains a window function call
pub fn contains_window_function(py_expr: &PyExpr) -> bool {
    match py_expr {
        // Window expressions always require window function handling
        PyExpr::Window { .. } => true,

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
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
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
