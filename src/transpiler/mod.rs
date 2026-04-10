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
pub(crate) mod window_native;

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
    ascii, btrim, chr, concat, concat_ws, contains, ends_with, lower, ltrim, replace, rtrim,
    split_part, starts_with, upper,
};
// Unicode functions (for length, substr, padding, left/right)
use datafusion::functions::unicode::expr_fn::{
    character_length, left, lpad, rpad, right, strpos, substring,
};
// Datetime functions
use datafusion::functions::datetime::expr_fn::{current_date, date_part, now, to_unixtime};
// Regex functions
use datafusion::functions::regex::expr_fn::regexp_like;
// Math functions (for gcd, lcm, factorial)
use datafusion::functions::math::expr_fn::{factorial, gcd, lcm};

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

/// Check if the "on" field is an empty column (standalone function call with on=None)
fn is_on_empty(on: &PyExpr) -> bool {
    matches!(on, PyExpr::Column(name) if name.is_empty())
}

/// Resolve the actual input expression: if "on" is empty, use args[0]; otherwise use "on"
fn resolve_on_or_args(
    on: &PyExpr,
    args: &[PyExpr],
    schema: &ArrowSchema,
    func_name: &str,
) -> Result<Expr, String> {
    if is_on_empty(on) {
        if args.is_empty() {
            return Err(format!("{} requires an argument", func_name));
        }
        pyexpr_to_datafusion_inner(args[0].clone(), schema)
    } else {
        pyexpr_to_datafusion_inner(on.clone(), schema)
    }
}

// ========== Category-based call expression handlers ==========

/// Handle conditional expressions (if_else)
fn parse_call_conditional(
    func: &str,
    args: &[PyExpr],
    schema: &ArrowSchema,
) -> Result<Expr, String> {
    match func {
        "if_else" => {
            if args.len() != 3 {
                return Err(
                    "if_else requires 3 arguments: condition, true_value, false_value".to_string(),
                );
            }
            let cond_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;
            let true_expr = pyexpr_to_datafusion_inner(args[1].clone(), schema)?;
            let false_expr = pyexpr_to_datafusion_inner(args[2].clone(), schema)?;

            use datafusion::logical_expr::case;
            Ok(case(cond_expr)
                .when(lit(true), true_expr)
                .otherwise(false_expr)
                .map_err(|e| format!("Failed to create CASE expression: {}", e))?)
        }
        _ => Err(format!("Not a conditional function: {}", func)),
    }
}

/// Handle null-related operations (fill_null, is_null, is_not_null, coalesce)
fn parse_call_null_ops(
    func: &str,
    on: PyExpr,
    args: Vec<PyExpr>,
    schema: &ArrowSchema,
) -> Result<Expr, String> {
    match func {
        "fill_null" => {
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
        "coalesce" => {
            if args.is_empty() {
                return Err("coalesce requires at least one argument".to_string());
            }
            let coalesce_args: Vec<Expr> = args
                .into_iter()
                .map(|a| pyexpr_to_datafusion_inner(a, schema))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(coalesce(coalesce_args))
        }
        _ => Err(format!("Not a null operation: {}", func)),
    }
}

/// Handle math operations (abs, ceil, floor, round, sqrt, power, sign, log, etc.)
fn parse_call_math(
    func: &str,
    on: &PyExpr,
    args: &[PyExpr],
    schema: &ArrowSchema,
) -> Result<Expr, String> {
    match func {
        "abs" => {
            use datafusion::functions::math::expr_fn::abs;
            let input = resolve_on_or_args(on, args, schema, "abs")?;
            Ok(abs(input))
        }
        "ceil" => {
            use datafusion::functions::math::expr_fn::ceil;
            let input = resolve_on_or_args(on, args, schema, "ceil")?;
            Ok(ceil(input))
        }
        "floor" => {
            use datafusion::functions::math::expr_fn::floor;
            let input = resolve_on_or_args(on, args, schema, "floor")?;
            Ok(floor(input))
        }
        "round" => {
            use datafusion::functions::math::expr_fn::round;
            let input = resolve_on_or_args(on, args, schema, "round")?;
            let decimals_expr = if is_on_empty(on) {
                // Standalone: round(expr, decimals) — decimals is args[1] if present
                if args.len() > 1 {
                    pyexpr_to_datafusion_inner(args[1].clone(), schema)?
                } else {
                    lit(0i64)
                }
            } else {
                // Method: expr.round(decimals) — decimals is args[0] if present
                if args.is_empty() {
                    lit(0i64)
                } else {
                    pyexpr_to_datafusion_inner(args[0].clone(), schema)?
                }
            };
            Ok(round(vec![input, decimals_expr]))
        }
        "math_sqrt" => {
            use datafusion::functions::math::expr_fn::sqrt;
            let input = resolve_on_or_args(on, args, schema, "sqrt")?;
            Ok(sqrt(input))
        }
        "math_power" => {
            use datafusion::functions::math::expr_fn::power;
            // base is first arg (or on), exponent is second arg
            let base = resolve_on_or_args(on, args, schema, "power")?;
            let exp_expr = if is_on_empty(on) {
                if args.len() < 2 {
                    return Err("power() requires two arguments: base and exponent".to_string());
                }
                pyexpr_to_datafusion_inner(args[1].clone(), schema)?
            } else {
                if args.is_empty() {
                    return Err("power() requires an exponent argument".to_string());
                }
                pyexpr_to_datafusion_inner(args[0].clone(), schema)?
            };
            Ok(power(base, exp_expr))
        }
        "math_sign" => {
            use datafusion::functions::math::expr_fn::signum;
            let input = resolve_on_or_args(on, args, schema, "sign")?;
            Ok(signum(input))
        }
        "math_ln" => {
            use datafusion::functions::math::expr_fn::ln;
            let input = resolve_on_or_args(on, args, schema, "ln")?;
            Ok(ln(input))
        }
        "math_log" => {
            // log(x) → ln(x), log(x, 10) → log10(x), log(x, 2) → log2(x), else log(base, x)
            let input = resolve_on_or_args(on, args, schema, "log")?;
            // Look for optional base argument
            let base_arg = if is_on_empty(on) {
                args.get(1)
            } else {
                args.first()
            };
            match base_arg {
                None => {
                    use datafusion::functions::math::expr_fn::ln;
                    Ok(ln(input))
                }
                Some(PyExpr::Literal { value, .. }) => {
                    let base_val = value
                        .parse::<f64>()
                        .map_err(|_| format!("log() base must be a number, got '{}'", value))?;
                    if (base_val - 10.0_f64).abs() < 1e-9 {
                        use datafusion::functions::math::expr_fn::log10;
                        Ok(log10(input))
                    } else if (base_val - 2.0_f64).abs() < 1e-9 {
                        use datafusion::functions::math::expr_fn::log2;
                        Ok(log2(input))
                    } else {
                        use datafusion::functions::math::expr_fn::log;
                        Ok(log(lit(base_val), input))
                    }
                }
                Some(other) => {
                    let base_expr = pyexpr_to_datafusion_inner(other.clone(), schema)?;
                    use datafusion::functions::math::expr_fn::log;
                    Ok(log(base_expr, input))
                }
            }
        }
        "math_exp" => {
            use datafusion::functions::math::expr_fn::exp;
            let input = resolve_on_or_args(on, args, schema, "exp")?;
            Ok(exp(input))
        }
        "math_sin" => {
            use datafusion::functions::math::expr_fn::sin;
            let input = resolve_on_or_args(on, args, schema, "sin")?;
            Ok(sin(input))
        }
        "math_cos" => {
            use datafusion::functions::math::expr_fn::cos;
            let input = resolve_on_or_args(on, args, schema, "cos")?;
            Ok(cos(input))
        }
        "math_tan" => {
            use datafusion::functions::math::expr_fn::tan;
            let input = resolve_on_or_args(on, args, schema, "tan")?;
            Ok(tan(input))
        }
        "math_asin" => {
            use datafusion::functions::math::expr_fn::asin;
            let input = resolve_on_or_args(on, args, schema, "asin")?;
            Ok(asin(input))
        }
        "math_acos" => {
            use datafusion::functions::math::expr_fn::acos;
            let input = resolve_on_or_args(on, args, schema, "acos")?;
            Ok(acos(input))
        }
        "math_atan" => {
            use datafusion::functions::math::expr_fn::atan;
            let input = resolve_on_or_args(on, args, schema, "atan")?;
            Ok(atan(input))
        }
        "math_atan2" => {
            use datafusion::functions::math::expr_fn::atan2;
            // atan2(y, x) — y is first arg, x is second
            if is_on_empty(on) {
                if args.len() < 2 {
                    return Err("atan2() requires two arguments: y and x".to_string());
                }
                let y_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;
                let x_expr = pyexpr_to_datafusion_inner(args[1].clone(), schema)?;
                Ok(atan2(y_expr, x_expr))
            } else {
                let y_expr = pyexpr_to_datafusion_inner(on.clone(), schema)?;
                if args.is_empty() {
                    return Err("atan2() requires x argument".to_string());
                }
                let x_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;
                Ok(atan2(y_expr, x_expr))
            }
        }
        "math_rand" => {
            use datafusion::functions::math::expr_fn::random;
            Ok(random())
        }
        "math_gcd" => {
            // gcd(a, b) — both args required
            if is_on_empty(on) {
                if args.len() < 2 {
                    return Err("gcd() requires two arguments".to_string());
                }
                let a_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;
                let b_expr = pyexpr_to_datafusion_inner(args[1].clone(), schema)?;
                Ok(gcd(a_expr, b_expr))
            } else {
                if args.is_empty() {
                    return Err("gcd() requires a second argument".to_string());
                }
                let a_expr = pyexpr_to_datafusion_inner(on.clone(), schema)?;
                let b_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;
                Ok(gcd(a_expr, b_expr))
            }
        }
        "math_lcm" => {
            if is_on_empty(on) {
                if args.len() < 2 {
                    return Err("lcm() requires two arguments".to_string());
                }
                let a_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;
                let b_expr = pyexpr_to_datafusion_inner(args[1].clone(), schema)?;
                Ok(lcm(a_expr, b_expr))
            } else {
                if args.is_empty() {
                    return Err("lcm() requires a second argument".to_string());
                }
                let a_expr = pyexpr_to_datafusion_inner(on.clone(), schema)?;
                let b_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;
                Ok(lcm(a_expr, b_expr))
            }
        }
        "math_factorial" => {
            let input = resolve_on_or_args(on, args, schema, "factorial")?;
            Ok(factorial(input))
        }
        _ => Err(format!("Not a math function: {}", func)),
    }
}

/// Handle type operations (cast, is_in)
fn parse_call_type_ops(
    func: &str,
    on: PyExpr,
    args: Vec<PyExpr>,
    schema: &ArrowSchema,
) -> Result<Expr, String> {
    match func {
        "cast" => {
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            if args.is_empty() {
                return Err("cast requires a target type argument".to_string());
            }
            let target_type = match &args[0] {
                PyExpr::Literal { value, .. } => value.clone(),
                _ => return Err("cast target type must be a string literal".to_string()),
            };
            let arrow_type = match target_type.to_lowercase().as_str() {
                "int32" | "i32" => DataType::Int32,
                "int64" | "i64" => DataType::Int64,
                "float32" | "f32" => DataType::Float32,
                "float64" | "f64" => DataType::Float64,
                "utf8" | "string" | "str" => DataType::Utf8,
                "bool" | "boolean" => DataType::Boolean,
                "date32" | "date" => DataType::Date32,
                _ => return Err(format!("Unsupported cast target type: {}", target_type)),
            };
            Ok(Expr::Cast(datafusion::logical_expr::Cast::new(
                Box::new(on_expr),
                arrow_type,
            )))
        }
        "is_in" => {
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            if args.is_empty() {
                return Err("is_in requires at least one value".to_string());
            }
            let list_exprs: Vec<Expr> = args
                .into_iter()
                .map(|a| pyexpr_to_datafusion_inner(a, schema))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(on_expr.in_list(list_exprs, false))
        }
        _ => Err(format!("Not a type operation: {}", func)),
    }
}

/// Handle string operations (str_contains, str_lower, str_upper, etc.)
fn parse_call_string(
    func: &str,
    on: PyExpr,
    args: Vec<PyExpr>,
    schema: &ArrowSchema,
) -> Result<Expr, String> {
    match func {
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
            Ok(btrim(vec![on_expr]))
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
            let char_expr = if args.len() > 1 {
                pyexpr_to_datafusion_inner(args[1].clone(), schema)?
            } else {
                lit(" ")
            };
            Ok(lpad(vec![on_expr, width_expr, char_expr]))
        }
        "str_pad_right" => {
            validate_string_column(&on, schema, "str_pad_right")?;
            if args.is_empty() {
                return Err("str_pad_right requires a width argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            let width_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;
            let char_expr = if args.len() > 1 {
                pyexpr_to_datafusion_inner(args[1].clone(), schema)?
            } else {
                lit(" ")
            };
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
            Ok(split_part(on_expr, delimiter_expr, index_expr))
        }
        "str_like" => {
            validate_string_column(&on, schema, "str_like")?;
            if args.is_empty() {
                return Err("str_like requires a pattern argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            let pattern_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;
            use datafusion::logical_expr::Like;
            Ok(Expr::Like(Like {
                negated: false,
                expr: Box::new(on_expr),
                pattern: Box::new(pattern_expr),
                escape_char: None,
                case_insensitive: false,
            }))
        }
        "str_isalpha" => {
            validate_string_column(&on, schema, "str_isalpha")?;
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            Ok(regexp_like(on_expr, lit("^[a-zA-Z]+$"), None))
        }
        "str_isdigit" => {
            validate_string_column(&on, schema, "str_isdigit")?;
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            Ok(regexp_like(on_expr, lit("^[0-9]+$"), None))
        }
        "str_islower" => {
            validate_string_column(&on, schema, "str_islower")?;
            let on_expr = pyexpr_to_datafusion_inner(on.clone(), schema)?;
            let on_expr2 = pyexpr_to_datafusion_inner(on, schema)?;
            Ok(on_expr.eq(lower(on_expr2)))
        }
        "str_isupper" => {
            validate_string_column(&on, schema, "str_isupper")?;
            let on_expr = pyexpr_to_datafusion_inner(on.clone(), schema)?;
            let on_expr2 = pyexpr_to_datafusion_inner(on, schema)?;
            Ok(on_expr.eq(upper(on_expr2)))
        }
        "str_pos" => {
            // strpos(str, substr) → 1-based position, 0 if not found
            validate_string_column(&on, schema, "str_pos")?;
            if args.is_empty() {
                return Err("str_pos requires a substring argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            let sub_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;
            Ok(strpos(on_expr, sub_expr))
        }
        "str_left" => {
            validate_string_column(&on, schema, "str_left")?;
            if args.is_empty() {
                return Err("str_left requires a length argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            let n_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;
            Ok(left(on_expr, n_expr))
        }
        "str_right" => {
            validate_string_column(&on, schema, "str_right")?;
            if args.is_empty() {
                return Err("str_right requires a length argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            let n_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;
            Ok(right(on_expr, n_expr))
        }
        "str_ltrim" => {
            validate_string_column(&on, schema, "str_ltrim")?;
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            Ok(ltrim(vec![on_expr]))
        }
        "str_rtrim" => {
            validate_string_column(&on, schema, "str_rtrim")?;
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            Ok(rtrim(vec![on_expr]))
        }
        "str_asc" => {
            // ascii(str) → Unicode code point of the first character
            validate_string_column(&on, schema, "str_asc")?;
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            Ok(ascii(on_expr))
        }
        "str_char" => {
            // chr(n) → single-character string from code point; standalone: args[0] is the input
            let n_expr = if is_on_empty(&on) {
                if args.is_empty() {
                    return Err("str_char requires a code point argument".to_string());
                }
                pyexpr_to_datafusion_inner(args[0].clone(), schema)?
            } else {
                pyexpr_to_datafusion_inner(on, schema)?
            };
            Ok(chr(n_expr))
        }
        "str_concat_ws" => {
            // concat_ws(delimiter, s1, s2, ...) — first arg is always the delimiter literal
            if args.len() < 2 {
                return Err(
                    "str_concat_ws requires a delimiter and at least one string".to_string(),
                );
            }
            let delim_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;
            let str_exprs: Vec<Expr> = args
                .into_iter()
                .skip(1)
                .map(|a| pyexpr_to_datafusion_inner(a, schema))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(concat_ws(delim_expr, str_exprs))
        }
        _ => Err(format!("Not a string function: {}", func)),
    }
}

/// Handle temporal operations (dt_year, dt_month, dt_day, dt_add, dt_diff, etc.)
fn parse_call_temporal(
    func: &str,
    on: PyExpr,
    args: &[PyExpr],
    schema: &ArrowSchema,
) -> Result<Expr, String> {
    /// Helper for simple date_part extractions
    fn date_part_extract(
        part: &str,
        func_name: &str,
        on: PyExpr,
        schema: &ArrowSchema,
    ) -> Result<Expr, String> {
        validate_temporal_column(&on, schema, func_name)?;
        let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
        Ok(date_part(lit(part), on_expr))
    }

    match func {
        "dt_year" => date_part_extract("year", "dt_year", on, schema),
        "dt_month" => date_part_extract("month", "dt_month", on, schema),
        "dt_day" => date_part_extract("day", "dt_day", on, schema),
        "dt_hour" => date_part_extract("hour", "dt_hour", on, schema),
        "dt_minute" => date_part_extract("minute", "dt_minute", on, schema),
        "dt_second" => date_part_extract("second", "dt_second", on, schema),
        "dt_add" => {
            validate_temporal_column(&on, schema, "dt_add")?;
            // Args: (days, months, years[, hours, minutes, seconds, weeks])
            // Legacy form accepts 3 args; extended form accepts 7 args.
            if args.len() < 3 {
                return Err(
                    "dt_add requires at least 3 arguments: days, months, years".to_string(),
                );
            }
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;

            let parse_lit_i64 = |arg: &PyExpr, name: &str| -> Result<i64, String> {
                match arg {
                    PyExpr::Literal { value, .. } => value
                        .parse::<i64>()
                        .map_err(|_| format!("dt_add {name} must be a literal integer")),
                    _ => Err(format!("dt_add {name} must be a literal integer")),
                }
            };

            let days = parse_lit_i64(&args[0], "days")?;
            let months = parse_lit_i64(&args[1], "months")?;
            let years = parse_lit_i64(&args[2], "years")?;
            let hours = if args.len() > 3 { parse_lit_i64(&args[3], "hours")? } else { 0 };
            let minutes = if args.len() > 4 { parse_lit_i64(&args[4], "minutes")? } else { 0 };
            let seconds = if args.len() > 5 { parse_lit_i64(&args[5], "seconds")? } else { 0 };
            let weeks = if args.len() > 6 { parse_lit_i64(&args[6], "weeks")? } else { 0 };

            let total_months = (years * 12 + months) as i32;
            let total_days = (days + weeks * 7) as i32;
            let total_nanos = (hours * 3_600_000_000_000)
                + (minutes * 60_000_000_000)
                + (seconds * 1_000_000_000);

            let interval = ScalarValue::new_interval_mdn(total_months, total_days, total_nanos);
            Ok(on_expr + lit(interval))
        }
        "dt_diff" => {
            // Args: (other_date[, unit_string])
            // unit: "day" (default), "month", "year", "hour", "minute", "second"
            validate_temporal_column(&on, schema, "dt_diff")?;
            if args.is_empty() {
                return Err("dt_diff requires another date argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            let other_expr = pyexpr_to_datafusion_inner(args[0].clone(), schema)?;

            let unit = if args.len() > 1 {
                match &args[1] {
                    PyExpr::Literal { value, .. } => value.to_lowercase(),
                    _ => "day".to_string(),
                }
            } else {
                "day".to_string()
            };

            // Use to_unixtime (→ Float64 seconds) for sub-month units.
            // date_part() no longer accepts Int64/Duration in DataFusion 53.
            match unit.as_str() {
                "day" | "days" => {
                    let sec_diff = to_unixtime(vec![on_expr]) - to_unixtime(vec![other_expr]);
                    Ok(sec_diff / lit(86400_f64))
                }
                "month" | "months" => {
                    // (year(on) - year(other)) * 12 + (month(on) - month(other))
                    let on_year = date_part(lit("year"), on_expr.clone());
                    let other_year = date_part(lit("year"), other_expr.clone());
                    let on_month = date_part(lit("month"), on_expr);
                    let other_month = date_part(lit("month"), other_expr);
                    Ok((on_year - other_year) * lit(12_f64) + (on_month - other_month))
                }
                "year" | "years" => {
                    let on_year = date_part(lit("year"), on_expr);
                    let other_year = date_part(lit("year"), other_expr);
                    Ok(on_year - other_year)
                }
                "hour" | "hours" => {
                    let sec_diff = to_unixtime(vec![on_expr]) - to_unixtime(vec![other_expr]);
                    Ok(sec_diff / lit(3600_f64))
                }
                "minute" | "minutes" => {
                    let sec_diff = to_unixtime(vec![on_expr]) - to_unixtime(vec![other_expr]);
                    Ok(sec_diff / lit(60_f64))
                }
                "second" | "seconds" => {
                    Ok(to_unixtime(vec![on_expr]) - to_unixtime(vec![other_expr]))
                }
                _ => Err(format!(
                    "dt_diff unsupported unit '{}'; use day/month/year/hour/minute/second",
                    unit
                )),
            }
        }
        "dt_age" => {
            // Number of complete years between the column date and today
            // Approximation: year(today()) - year(col) - (if month/day of col > today's → 1 else 0)
            validate_temporal_column(&on, schema, "dt_age")?;
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            let today_expr = current_date();

            // year difference, then correct for whether the birthday has passed this year
            let year_diff = date_part(lit("year"), today_expr.clone())
                - date_part(lit("year"), on_expr.clone());

            // month-day comparison: cast to day-of-year for simplicity
            // If doy(today) < doy(birth) → subtract 1
            let doy_today = date_part(lit("doy"), today_expr);
            let doy_birth = date_part(lit("doy"), on_expr);

            use datafusion::logical_expr::case;
            let correction = case(doy_today.lt(doy_birth))
                .when(lit(true), lit(1_f64))
                .otherwise(lit(0_f64))
                .map_err(|e| format!("dt_age case expression failed: {}", e))?;

            Ok(year_diff - correction)
        }
        "dt_millisecond" => {
            // date_part("millisecond", col) gives total milliseconds within the second
            // (returns the millisecond sub-second component 0–999)
            validate_temporal_column(&on, schema, "dt_millisecond")?;
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            Ok(date_part(lit("millisecond"), on_expr) % lit(1000_f64))
        }
        "dt_weekday" => {
            // DataFusion dow: 0=Sunday, 1=Monday, …, 6=Saturday
            // Target: Monday=0 … Sunday=6 → (dow + 6) % 7
            validate_temporal_column(&on, schema, "dt_weekday")?;
            let on_expr = pyexpr_to_datafusion_inner(on, schema)?;
            Ok((date_part(lit("dow"), on_expr) + lit(6_f64)) % lit(7_f64))
        }
        "dt_now" => {
            // now() returns current timestamp; no column required
            Ok(now())
        }
        "dt_today" => {
            Ok(current_date())
        }
        _ => Err(format!("Not a temporal function: {}", func)),
    }
}

/// Parse a function call into a DataFusion expression.
///
/// Dispatches to category-specific handlers: conditional, null, math,
/// type, string, and temporal operations.
fn parse_call_expr(
    func: &str,
    on: PyExpr,
    args: Vec<PyExpr>,
    schema: &ArrowSchema,
) -> Result<Expr, String> {
    match func {
        // Conditional
        "if_else" => parse_call_conditional(func, &args, schema),
        // Null handling
        "fill_null" | "is_null" | "is_not_null" | "coalesce" => {
            parse_call_null_ops(func, on, args, schema)
        }
        // Math (built-in method-style: abs, ceil, floor, round)
        "abs" | "ceil" | "floor" | "round" => parse_call_math(func, &on, &args, schema),
        // Extended math functions (math_* prefix from global functions)
        f if f.starts_with("math_") => parse_call_math(func, &on, &args, schema),
        // Standalone math functions without prefix
        "gcd" => parse_call_math("math_gcd", &on, &args, schema),
        "lcm" => parse_call_math("math_lcm", &on, &args, schema),
        "factorial" => parse_call_math("math_factorial", &on, &args, schema),
        // Type / membership
        "cast" | "is_in" => parse_call_type_ops(func, on, args, schema),
        // Window functions (must be handled elsewhere)
        "shift" | "rolling" | "diff" | "cum_sum" | "mean" | "sum" | "min" | "max" | "count"
        | "std" => Err(format!(
            "Window function '{}' requires DataFrame context - should be handled in derive()",
            func
        )),
        // String operations
        f if f.starts_with("str_") => parse_call_string(func, on, args, schema),
        // Temporal operations
        f if f.starts_with("dt_") => parse_call_temporal(func, on, &args, schema),
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
        PyExpr::Alias { expr, alias } => {
            let inner = pyexpr_to_datafusion_inner(*expr, schema)?;
            Ok(inner.alias(alias))
        }
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
        PyExpr::Alias { expr, .. } => contains_window_function(expr),
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
