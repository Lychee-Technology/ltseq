//! Expression transpilation: Convert Python expressions to DataFusion expressions
//!
//! This module handles the conversion of serialized Python expressions (PyExpr)
//! into DataFusion's native expression format, including detection of window functions
//! and SQL transpilation for complex operations.
//!
//! ## Module Structure
//!
//! - `resolve`: expression types, from DataFusion's own coercion
//! - `literal_policy`, `literals`, `exact`: how a literal is read next to the
//!   value it meets, and exact comparison and value placement
//! - `window_native`: Native DataFusion window expression builder (primary path)
//!
//! Constant expressions (`1 + 2 + r.col`, `x & True`) are left to DataFusion's
//! simplifier, which folds them with DataFusion's own types and checks
//! (decision D-f on #225).

mod exact;
pub(crate) mod floor_div;
mod literal_policy;
mod literals;
mod resolve;
pub(crate) mod window_native;

pub(crate) use resolve::{binary_input_types, Resolver};
pub use window_native::pyexpr_to_window_expr;

use crate::types::{arg, Arg, PyExpr};
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
use datafusion::functions::datetime::expr_fn::{current_date, date_part, now};
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

/// A serialized binary operator. Floor division has no DataFusion
/// `Operator` (`/` truncates toward zero), so it is its own variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BinaryOp {
    Native(Operator),
    FloorDiv,
}

/// Map a serialized operator name (shared by the row and group dialects)
/// to a [`BinaryOp`].
pub(crate) fn parse_binary_op(op: &str) -> Result<BinaryOp, String> {
    let operator = match op {
        "FloorDiv" => return Ok(BinaryOp::FloorDiv),
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
    Ok(BinaryOp::Native(operator))
}

/// `left <op> right` for a serialized operator name. The row, window and
/// group transpilers all build binary expressions here, so an operator is
/// either executable in every dialect or rejected by name in every one, and
/// a literal operand of a comparison or arithmetic operator is read next to
/// the other operand the same way in each (`literals::binary_operands`).
pub(crate) fn binary_expr(
    op: &str,
    left: Expr,
    right: Expr,
    rx: &Resolver<'_>,
) -> Result<Expr, String> {
    use literal_policy::Position;
    let op = parse_binary_op(op)?;
    let position = match op {
        BinaryOp::FloorDiv
        | BinaryOp::Native(
            Operator::Plus
            | Operator::Minus
            | Operator::Multiply
            | Operator::Divide
            | Operator::Modulo,
        ) => Some(Position::Arithmetic),
        BinaryOp::Native(
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq,
        ) => Some(Position::Comparison),
        BinaryOp::Native(_) => None,
    };
    let (left, right) = match (position, op) {
        (Some(Position::Comparison), BinaryOp::Native(operator)) => {
            return literals::comparison(operator, left, right, rx)
        }
        (Some(_), _) => literals::arithmetic_operands(left, right, rx)?,
        (None, _) => (left, right),
    };
    Ok(match op {
        BinaryOp::Native(operator) => {
            Expr::BinaryExpr(BinaryExpr::new(Box::new(left), operator, Box::new(right)))
        }
        BinaryOp::FloorDiv => floor_div::floor_div(left, right),
    })
}

/// Parse a binary operation into a DataFusion expression
fn parse_binop_expr(
    op: &str,
    left: PyExpr,
    right: PyExpr,
    rx: &Resolver<'_>,
) -> Result<Expr, String> {
    let left_expr = pyexpr_to_datafusion_inner(left, rx)?;
    let right_expr = pyexpr_to_datafusion_inner(right, rx)?;
    binary_expr(op, left_expr, right_expr, rx)
}

/// Parse a unary operation into a DataFusion expression
fn parse_unaryop_expr(op: &str, operand: PyExpr, rx: &Resolver<'_>) -> Result<Expr, String> {
    let operand_expr = pyexpr_to_datafusion_inner(operand, rx)?;
    match op {
        "Not" => Ok(operand_expr.not()),
        _ => Err(format!("Unknown unary operator: {}", op)),
    }
}

/// The receiver of a method-style call. Errors when a function that needs a
/// receiver arrives as a standalone call (`on=None`).
pub(crate) fn require_on<T>(on: Option<T>, func: &str) -> Result<T, String> {
    on.ok_or_else(|| format!("{func} must be called as a method on an expression"))
}

/// Resolve the input of a function usable both ways: the receiver for a
/// method call, or `args[0]` for a standalone call.
fn resolve_on_or_args(
    on: Option<&PyExpr>,
    args: &[PyExpr],
    rx: &Resolver<'_>,
    func_name: &str,
) -> Result<Expr, String> {
    match on {
        Some(on) => pyexpr_to_datafusion_inner(on.clone(), rx),
        None => {
            let input = args
                .first()
                .ok_or_else(|| format!("{} requires an argument", func_name))?;
            pyexpr_to_datafusion_inner(input.clone(), rx)
        }
    }
}

// ========== Category-based call expression handlers ==========

/// Handle conditional expressions (if_else)
fn parse_call_conditional(
    func: &str,
    args: &[PyExpr],
    rx: &Resolver<'_>,
) -> Result<Expr, String> {
    match func {
        "if_else" => {
            if args.len() != 3 {
                return Err(
                    "if_else requires 3 arguments: condition, true_value, false_value".to_string(),
                );
            }
            let cond_expr = pyexpr_to_datafusion_inner(args[0].clone(), rx)?;
            let true_expr = pyexpr_to_datafusion_inner(args[1].clone(), rx)?;
            let false_expr = pyexpr_to_datafusion_inner(args[2].clone(), rx)?;
            literals::if_else(cond_expr, true_expr, false_expr, rx)
        }
        _ => Err(format!("Not a conditional function: {}", func)),
    }
}

/// Handle null-related operations (fill_null, is_null, is_not_null, coalesce)
fn parse_call_null_ops(
    func: &str,
    on: Option<PyExpr>,
    args: Vec<PyExpr>,
    rx: &Resolver<'_>,
) -> Result<Expr, String> {
    match func {
        "fill_null" => {
            if args.is_empty() {
                return Err("fill_null requires a default value argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(require_on(on, func)?, rx)?;
            let default_expr = pyexpr_to_datafusion_inner(args[0].clone(), rx)?;
            literals::coalesce_values(vec![on_expr, default_expr], rx)
        }
        "is_null" => {
            let on_expr = pyexpr_to_datafusion_inner(require_on(on, func)?, rx)?;
            Ok(on_expr.is_null())
        }
        "is_not_null" => {
            let on_expr = pyexpr_to_datafusion_inner(require_on(on, func)?, rx)?;
            Ok(on_expr.is_not_null())
        }
        "coalesce" => {
            if args.is_empty() {
                return Err("coalesce requires at least one argument".to_string());
            }
            let coalesce_args: Vec<Expr> = args
                .into_iter()
                .map(|a| pyexpr_to_datafusion_inner(a, rx))
                .collect::<Result<Vec<_>, _>>()?;
            literals::coalesce_values(coalesce_args, rx)
        }
        _ => Err(format!("Not a null operation: {}", func)),
    }
}

/// The `decimals` argument of `round`: 0 when absent, an integer literal, or
/// an expression lowered by `lower` (the row and window planners each pass
/// their own lowering).
pub(crate) fn round_decimals(
    decimals: Arg<'_>,
    lower: impl FnOnce(&PyExpr) -> Result<Expr, String>,
) -> Result<Expr, String> {
    match decimals {
        Arg::Absent => Ok(lit(0i64)),
        Arg::Literal(value) => Ok(lit(value.require_i64("round() decimals")?)),
        Arg::Expr(expr) => lower(expr),
    }
}

/// Handle math operations (abs, ceil, floor, round, sqrt, power, sign, log, etc.)
fn parse_call_math(
    func: &str,
    on: Option<&PyExpr>,
    args: &[PyExpr],
    rx: &Resolver<'_>,
) -> Result<Expr, String> {
    match func {
        "abs" => {
            use datafusion::functions::math::expr_fn::abs;
            let input = resolve_on_or_args(on, args, rx, "abs")?;
            Ok(abs(input))
        }
        "ceil" => {
            use datafusion::functions::math::expr_fn::ceil;
            let input = resolve_on_or_args(on, args, rx, "ceil")?;
            Ok(ceil(input))
        }
        "floor" => {
            use datafusion::functions::math::expr_fn::floor;
            let input = resolve_on_or_args(on, args, rx, "floor")?;
            Ok(floor(input))
        }
        "round" => {
            use datafusion::functions::math::expr_fn::round;
            let input = resolve_on_or_args(on, args, rx, "round")?;
            // Standalone round(expr, decimals) has decimals in args[1];
            // the method form expr.round(decimals) in args[0].
            let decimals = round_decimals(arg(args, usize::from(on.is_none())), |e| {
                pyexpr_to_datafusion_inner(e.clone(), rx)
            })?;
            Ok(round(vec![input, decimals]))
        }
        "math_sqrt" => {
            use datafusion::functions::math::expr_fn::sqrt;
            let input = resolve_on_or_args(on, args, rx, "sqrt")?;
            Ok(sqrt(input))
        }
        "math_power" => {
            use datafusion::functions::math::expr_fn::power;
            // base is first arg (or on), exponent is second arg
            let base = resolve_on_or_args(on, args, rx, "power")?;
            let exp_expr = if on.is_none() {
                if args.len() < 2 {
                    return Err("power() requires two arguments: base and exponent".to_string());
                }
                pyexpr_to_datafusion_inner(args[1].clone(), rx)?
            } else {
                if args.is_empty() {
                    return Err("power() requires an exponent argument".to_string());
                }
                pyexpr_to_datafusion_inner(args[0].clone(), rx)?
            };
            Ok(power(base, exp_expr))
        }
        "math_sign" => {
            use datafusion::functions::math::expr_fn::signum;
            let input = resolve_on_or_args(on, args, rx, "sign")?;
            Ok(signum(input))
        }
        "math_ln" => {
            use datafusion::functions::math::expr_fn::ln;
            let input = resolve_on_or_args(on, args, rx, "ln")?;
            Ok(ln(input))
        }
        "math_log" => {
            // log(x) → ln(x), log(x, 10) → log10(x), log(x, 2) → log2(x), else log(base, x)
            let input = resolve_on_or_args(on, args, rx, "log")?;
            // The optional base: args[1] standalone, args[0] as a method.
            match arg(args, usize::from(on.is_none())) {
                Arg::Absent => {
                    use datafusion::functions::math::expr_fn::ln;
                    Ok(ln(input))
                }
                Arg::Literal(value) => {
                    let base_val = value.require_f64("log() base")?;
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
                Arg::Expr(other) => {
                    let base_expr = pyexpr_to_datafusion_inner(other.clone(), rx)?;
                    use datafusion::functions::math::expr_fn::log;
                    Ok(log(base_expr, input))
                }
            }
        }
        "math_exp" => {
            use datafusion::functions::math::expr_fn::exp;
            let input = resolve_on_or_args(on, args, rx, "exp")?;
            Ok(exp(input))
        }
        "math_sin" => {
            use datafusion::functions::math::expr_fn::sin;
            let input = resolve_on_or_args(on, args, rx, "sin")?;
            Ok(sin(input))
        }
        "math_cos" => {
            use datafusion::functions::math::expr_fn::cos;
            let input = resolve_on_or_args(on, args, rx, "cos")?;
            Ok(cos(input))
        }
        "math_tan" => {
            use datafusion::functions::math::expr_fn::tan;
            let input = resolve_on_or_args(on, args, rx, "tan")?;
            Ok(tan(input))
        }
        "math_asin" => {
            use datafusion::functions::math::expr_fn::asin;
            let input = resolve_on_or_args(on, args, rx, "asin")?;
            Ok(asin(input))
        }
        "math_acos" => {
            use datafusion::functions::math::expr_fn::acos;
            let input = resolve_on_or_args(on, args, rx, "acos")?;
            Ok(acos(input))
        }
        "math_atan" => {
            use datafusion::functions::math::expr_fn::atan;
            let input = resolve_on_or_args(on, args, rx, "atan")?;
            Ok(atan(input))
        }
        "math_atan2" => {
            use datafusion::functions::math::expr_fn::atan2;
            // atan2(y, x) — y is first arg, x is second
            if let Some(on) = on {
                let y_expr = pyexpr_to_datafusion_inner(on.clone(), rx)?;
                if args.is_empty() {
                    return Err("atan2() requires x argument".to_string());
                }
                let x_expr = pyexpr_to_datafusion_inner(args[0].clone(), rx)?;
                Ok(atan2(y_expr, x_expr))
            } else {
                if args.len() < 2 {
                    return Err("atan2() requires two arguments: y and x".to_string());
                }
                let y_expr = pyexpr_to_datafusion_inner(args[0].clone(), rx)?;
                let x_expr = pyexpr_to_datafusion_inner(args[1].clone(), rx)?;
                Ok(atan2(y_expr, x_expr))
            }
        }
        "math_rand" => {
            use datafusion::functions::math::expr_fn::random;
            Ok(random())
        }
        "math_gcd" => {
            // gcd(a, b) — both args required
            if let Some(on) = on {
                if args.is_empty() {
                    return Err("gcd() requires a second argument".to_string());
                }
                let a_expr = pyexpr_to_datafusion_inner(on.clone(), rx)?;
                let b_expr = pyexpr_to_datafusion_inner(args[0].clone(), rx)?;
                Ok(gcd(a_expr, b_expr))
            } else {
                if args.len() < 2 {
                    return Err("gcd() requires two arguments".to_string());
                }
                let a_expr = pyexpr_to_datafusion_inner(args[0].clone(), rx)?;
                let b_expr = pyexpr_to_datafusion_inner(args[1].clone(), rx)?;
                Ok(gcd(a_expr, b_expr))
            }
        }
        "math_lcm" => {
            if let Some(on) = on {
                if args.is_empty() {
                    return Err("lcm() requires a second argument".to_string());
                }
                let a_expr = pyexpr_to_datafusion_inner(on.clone(), rx)?;
                let b_expr = pyexpr_to_datafusion_inner(args[0].clone(), rx)?;
                Ok(lcm(a_expr, b_expr))
            } else {
                if args.len() < 2 {
                    return Err("lcm() requires two arguments".to_string());
                }
                let a_expr = pyexpr_to_datafusion_inner(args[0].clone(), rx)?;
                let b_expr = pyexpr_to_datafusion_inner(args[1].clone(), rx)?;
                Ok(lcm(a_expr, b_expr))
            }
        }
        "math_factorial" => {
            let input = resolve_on_or_args(on, args, rx, "factorial")?;
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
    rx: &Resolver<'_>,
) -> Result<Expr, String> {
    match func {
        "cast" => {
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            let target_type = match arg(&args, 0) {
                Arg::Absent => return Err("cast requires a target type argument".to_string()),
                Arg::Literal(value) => value.require_str("cast() target type")?.to_string(),
                Arg::Expr(_) => return Err("cast target type must be a string literal".to_string()),
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
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            if args.is_empty() {
                return Err("is_in requires at least one value".to_string());
            }
            let list_exprs: Vec<Expr> = args
                .into_iter()
                .map(|a| pyexpr_to_datafusion_inner(a, rx))
                .collect::<Result<Vec<_>, _>>()?;
            literals::in_list(on_expr, list_exprs, rx)
        }
        _ => Err(format!("Not a type operation: {}", func)),
    }
}

/// Handle string functions that accept a standalone call: `str_concat_ws`
/// never has a receiver, and `str_char` works either way.
fn parse_call_string_standalone(
    func: &str,
    on: Option<PyExpr>,
    args: Vec<PyExpr>,
    rx: &Resolver<'_>,
) -> Result<Expr, String> {
    match func {
        "str_char" => {
            // chr(n) → single-character string from code point; standalone: args[0] is the input
            let n_expr = match on {
                Some(on) => pyexpr_to_datafusion_inner(on, rx)?,
                None => {
                    let input = args
                        .first()
                        .ok_or("str_char requires a code point argument")?;
                    pyexpr_to_datafusion_inner(input.clone(), rx)?
                }
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
            let delim_expr = pyexpr_to_datafusion_inner(args[0].clone(), rx)?;
            let str_exprs: Vec<Expr> = args
                .into_iter()
                .skip(1)
                .map(|a| pyexpr_to_datafusion_inner(a, rx))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(concat_ws(delim_expr, str_exprs))
        }
        _ => Err(format!("Not a standalone string function: {}", func)),
    }
}

/// Handle string operations (str_contains, str_lower, str_upper, etc.)
fn parse_call_string(
    func: &str,
    on: PyExpr,
    args: Vec<PyExpr>,
    rx: &Resolver<'_>,
) -> Result<Expr, String> {
    match func {
        "str_contains" => {
            validate_string_column(&on, rx.arrow(), "str_contains")?;
            if args.is_empty() {
                return Err("str_contains requires a pattern argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            let pattern_expr = pyexpr_to_datafusion_inner(args[0].clone(), rx)?;
            Ok(contains(on_expr, pattern_expr))
        }
        "str_starts_with" => {
            validate_string_column(&on, rx.arrow(), "str_starts_with")?;
            if args.is_empty() {
                return Err("str_starts_with requires a prefix argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            let prefix_expr = pyexpr_to_datafusion_inner(args[0].clone(), rx)?;
            Ok(starts_with(on_expr, prefix_expr))
        }
        "str_ends_with" => {
            validate_string_column(&on, rx.arrow(), "str_ends_with")?;
            if args.is_empty() {
                return Err("str_ends_with requires a suffix argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            let suffix_expr = pyexpr_to_datafusion_inner(args[0].clone(), rx)?;
            Ok(ends_with(on_expr, suffix_expr))
        }
        "str_lower" => {
            validate_string_column(&on, rx.arrow(), "str_lower")?;
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            Ok(lower(on_expr))
        }
        "str_upper" => {
            validate_string_column(&on, rx.arrow(), "str_upper")?;
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            Ok(upper(on_expr))
        }
        "str_strip" => {
            validate_string_column(&on, rx.arrow(), "str_strip")?;
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            Ok(btrim(vec![on_expr]))
        }
        "str_len" => {
            validate_string_column(&on, rx.arrow(), "str_len")?;
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            Ok(character_length(on_expr))
        }
        "str_slice" => {
            validate_string_column(&on, rx.arrow(), "str_slice")?;
            if args.len() < 2 {
                return Err("str_slice requires start and length arguments".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            let start_expr = pyexpr_to_datafusion_inner(args[0].clone(), rx)?;
            let length_expr = pyexpr_to_datafusion_inner(args[1].clone(), rx)?;
            Ok(substring(on_expr, start_expr + lit(1), length_expr))
        }
        "str_regex_match" => {
            validate_string_column(&on, rx.arrow(), "str_regex_match")?;
            if args.is_empty() {
                return Err("str_regex_match requires a pattern argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            let pattern_expr = pyexpr_to_datafusion_inner(args[0].clone(), rx)?;
            Ok(regexp_like(on_expr, pattern_expr, None))
        }
        "str_replace" => {
            validate_string_column(&on, rx.arrow(), "str_replace")?;
            if args.len() < 2 {
                return Err("str_replace requires 'old' and 'new' arguments".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            let old_expr = pyexpr_to_datafusion_inner(args[0].clone(), rx)?;
            let new_expr = pyexpr_to_datafusion_inner(args[1].clone(), rx)?;
            Ok(replace(on_expr, old_expr, new_expr))
        }
        "str_concat" => {
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            let mut all_args = vec![on_expr];
            for arg in args {
                all_args.push(pyexpr_to_datafusion_inner(arg, rx)?);
            }
            Ok(concat(all_args))
        }
        "str_pad_left" => {
            validate_string_column(&on, rx.arrow(), "str_pad_left")?;
            if args.is_empty() {
                return Err("str_pad_left requires a width argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            let width_expr = pyexpr_to_datafusion_inner(args[0].clone(), rx)?;
            let char_expr = if args.len() > 1 {
                pyexpr_to_datafusion_inner(args[1].clone(), rx)?
            } else {
                lit(" ")
            };
            Ok(lpad(vec![on_expr, width_expr, char_expr]))
        }
        "str_pad_right" => {
            validate_string_column(&on, rx.arrow(), "str_pad_right")?;
            if args.is_empty() {
                return Err("str_pad_right requires a width argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            let width_expr = pyexpr_to_datafusion_inner(args[0].clone(), rx)?;
            let char_expr = if args.len() > 1 {
                pyexpr_to_datafusion_inner(args[1].clone(), rx)?
            } else {
                lit(" ")
            };
            Ok(rpad(vec![on_expr, width_expr, char_expr]))
        }
        "str_split" => {
            validate_string_column(&on, rx.arrow(), "str_split")?;
            if args.len() < 2 {
                return Err("str_split requires delimiter and index arguments".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            let delimiter_expr = pyexpr_to_datafusion_inner(args[0].clone(), rx)?;
            let index_expr = pyexpr_to_datafusion_inner(args[1].clone(), rx)?;
            Ok(split_part(on_expr, delimiter_expr, index_expr))
        }
        "str_like" => {
            validate_string_column(&on, rx.arrow(), "str_like")?;
            if args.is_empty() {
                return Err("str_like requires a pattern argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            let pattern_expr = pyexpr_to_datafusion_inner(args[0].clone(), rx)?;
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
            validate_string_column(&on, rx.arrow(), "str_isalpha")?;
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            Ok(regexp_like(on_expr, lit("^[a-zA-Z]+$"), None))
        }
        "str_isdigit" => {
            validate_string_column(&on, rx.arrow(), "str_isdigit")?;
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            Ok(regexp_like(on_expr, lit("^[0-9]+$"), None))
        }
        "str_islower" => {
            validate_string_column(&on, rx.arrow(), "str_islower")?;
            let on_expr = pyexpr_to_datafusion_inner(on.clone(), rx)?;
            let on_expr2 = pyexpr_to_datafusion_inner(on, rx)?;
            Ok(on_expr.eq(lower(on_expr2)))
        }
        "str_isupper" => {
            validate_string_column(&on, rx.arrow(), "str_isupper")?;
            let on_expr = pyexpr_to_datafusion_inner(on.clone(), rx)?;
            let on_expr2 = pyexpr_to_datafusion_inner(on, rx)?;
            Ok(on_expr.eq(upper(on_expr2)))
        }
        "str_pos" => {
            // strpos(str, substr) → 1-based position, 0 if not found
            validate_string_column(&on, rx.arrow(), "str_pos")?;
            if args.is_empty() {
                return Err("str_pos requires a substring argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            let sub_expr = pyexpr_to_datafusion_inner(args[0].clone(), rx)?;
            Ok(strpos(on_expr, sub_expr))
        }
        "str_left" => {
            validate_string_column(&on, rx.arrow(), "str_left")?;
            if args.is_empty() {
                return Err("str_left requires a length argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            let n_expr = pyexpr_to_datafusion_inner(args[0].clone(), rx)?;
            Ok(left(on_expr, n_expr))
        }
        "str_right" => {
            validate_string_column(&on, rx.arrow(), "str_right")?;
            if args.is_empty() {
                return Err("str_right requires a length argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            let n_expr = pyexpr_to_datafusion_inner(args[0].clone(), rx)?;
            Ok(right(on_expr, n_expr))
        }
        "str_ltrim" => {
            validate_string_column(&on, rx.arrow(), "str_ltrim")?;
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            Ok(ltrim(vec![on_expr]))
        }
        "str_rtrim" => {
            validate_string_column(&on, rx.arrow(), "str_rtrim")?;
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            Ok(rtrim(vec![on_expr]))
        }
        "str_asc" => {
            // ascii(str) → Unicode code point of the first character
            validate_string_column(&on, rx.arrow(), "str_asc")?;
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            Ok(ascii(on_expr))
        }
        _ => Err(format!("Not a string function: {}", func)),
    }
}

/// Handle temporal operations (dt_year, dt_month, dt_day, dt_add, dt_diff, etc.)
fn parse_call_temporal(
    func: &str,
    on: PyExpr,
    args: &[PyExpr],
    rx: &Resolver<'_>,
) -> Result<Expr, String> {
    /// Helper for simple date_part extractions
    fn date_part_extract(
        part: &str,
        func_name: &str,
        on: PyExpr,
        rx: &Resolver<'_>,
    ) -> Result<Expr, String> {
        validate_temporal_column(&on, rx.arrow(), func_name)?;
        let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
        Ok(date_part(lit(part), on_expr))
    }

    match func {
        "dt_year" => date_part_extract("year", "dt_year", on, rx),
        "dt_month" => date_part_extract("month", "dt_month", on, rx),
        "dt_day" => date_part_extract("day", "dt_day", on, rx),
        "dt_hour" => date_part_extract("hour", "dt_hour", on, rx),
        "dt_minute" => date_part_extract("minute", "dt_minute", on, rx),
        "dt_second" => date_part_extract("second", "dt_second", on, rx),
        "dt_add" => {
            validate_temporal_column(&on, rx.arrow(), "dt_add")?;
            // Args: (days, months, years[, hours, minutes, seconds, weeks])
            // Legacy form accepts 3 args; extended form accepts 7 args.
            if args.len() < 3 {
                return Err(
                    "dt_add requires at least 3 arguments: days, months, years".to_string(),
                );
            }
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;

            // An absent field is 0; a present one must be a literal integer.
            let field = |i: usize, name: &str| -> Result<i64, String> {
                match arg(args, i) {
                    Arg::Absent => Ok(0),
                    Arg::Literal(value) => value.require_i64(&format!("dt_add {name}")),
                    Arg::Expr(_) => Err(format!("dt_add {name} must be a literal integer")),
                }
            };

            let days = field(0, "days")?;
            let months = field(1, "months")?;
            let years = field(2, "years")?;
            let hours = field(3, "hours")?;
            let minutes = field(4, "minutes")?;
            let seconds = field(5, "seconds")?;
            let weeks = field(6, "weeks")?;

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
            validate_temporal_column(&on, rx.arrow(), "dt_diff")?;
            if args.is_empty() {
                return Err("dt_diff requires another date argument".to_string());
            }
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            let other_expr = pyexpr_to_datafusion_inner(args[0].clone(), rx)?;
            let other_expr = literals::dt_diff_other(&on_expr, other_expr, rx)?;

            let unit = match arg(args, 1) {
                Arg::Absent => "day".to_string(),
                Arg::Literal(value) => value.require_str("dt_diff unit")?.to_lowercase(),
                Arg::Expr(_) => return Err("dt_diff unit must be a literal string".to_string()),
            };

            match unit.as_str() {
                "day" | "days" => dt_elapsed(on_expr, other_expr, rx, 86_400),
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
                "hour" | "hours" => dt_elapsed(on_expr, other_expr, rx, 3_600),
                "minute" | "minutes" => dt_elapsed(on_expr, other_expr, rx, 60),
                "second" | "seconds" => dt_elapsed(on_expr, other_expr, rx, 1),
                _ => Err(format!(
                    "dt_diff unsupported unit '{}'; use day/month/year/hour/minute/second",
                    unit
                )),
            }
        }
        "dt_age" => {
            // Number of complete years between the column date and today
            // Approximation: year(today()) - year(col) - (if month/day of col > today's → 1 else 0)
            validate_temporal_column(&on, rx.arrow(), "dt_age")?;
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
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
            validate_temporal_column(&on, rx.arrow(), "dt_millisecond")?;
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            Ok(date_part(lit("millisecond"), on_expr) % lit(1000_f64))
        }
        "dt_weekday" => {
            // DataFusion dow: 0=Sunday, 1=Monday, …, 6=Saturday
            // Target: Monday=0 … Sunday=6 → (dow + 6) % 7
            validate_temporal_column(&on, rx.arrow(), "dt_weekday")?;
            let on_expr = pyexpr_to_datafusion_inner(on, rx)?;
            Ok((date_part(lit("dow"), on_expr) + lit(6_f64)) % lit(7_f64))
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
    on: Option<PyExpr>,
    args: Vec<PyExpr>,
    rx: &Resolver<'_>,
) -> Result<Expr, String> {
    match func {
        // Conditional
        "if_else" => parse_call_conditional(func, &args, rx),
        // Null handling
        "fill_null" | "is_null" | "is_not_null" | "coalesce" => {
            parse_call_null_ops(func, on, args, rx)
        }
        // Math (built-in method-style: abs, ceil, floor, round)
        "abs" | "ceil" | "floor" | "round" => parse_call_math(func, on.as_ref(), &args, rx),
        // Extended math functions (math_* prefix from global functions)
        f if f.starts_with("math_") => parse_call_math(func, on.as_ref(), &args, rx),
        // Standalone math functions without prefix
        "gcd" => parse_call_math("math_gcd", on.as_ref(), &args, rx),
        "lcm" => parse_call_math("math_lcm", on.as_ref(), &args, rx),
        "factorial" => parse_call_math("math_factorial", on.as_ref(), &args, rx),
        // Type / membership
        "cast" | "is_in" => parse_call_type_ops(func, require_on(on, func)?, args, rx),
        // Window functions (must be handled elsewhere)
        "shift" | "rolling" | "diff" | "cum_sum" | "cum_max" | "cum_min" | "mean" | "sum"
        | "min" | "max" | "count" | "std" => Err(format!(
            "Window function '{}' requires DataFrame context - should be handled in derive()",
            func
        )),
        // String operations
        "str_char" | "str_concat_ws" => parse_call_string_standalone(func, on, args, rx),
        f if f.starts_with("str_") => parse_call_string(func, require_on(on, func)?, args, rx),
        // Temporal operations
        "dt_now" => Ok(now()),
        "dt_today" => Ok(current_date()),
        f if f.starts_with("dt_") => {
            parse_call_temporal(func, require_on(on, func)?, &args, rx)
        }
        _ => Err(format!("Method '{}' not yet supported", func)),
    }
}

/// Convert PyExpr to DataFusion Expr, resolved against `schema`: coerced as
/// DataFusion's analyzer will coerce it, so the type DataFusion reports for
/// it (and stores for a projection of it) is the type it executes as.
pub fn pyexpr_to_datafusion(py_expr: PyExpr, schema: &ArrowSchema) -> Result<Expr, String> {
    let rx = Resolver::new(schema)?;
    rx.resolve(lower_expr(py_expr, &rx)?)
}

/// [`pyexpr_to_datafusion`] for an expression whose name becomes an output
/// column name (an unaliased `select` item, a group key). Coercion inserts
/// casts, which change the name DataFusion derives from an expression, so
/// the resolved expression keeps the name of the expression as written, as
/// DataFusion's analyzer does when it coerces a projection.
pub fn pyexpr_to_named_datafusion(py_expr: PyExpr, schema: &ArrowSchema) -> Result<Expr, String> {
    let rx = Resolver::new(schema)?;
    let lowered = lower_expr(py_expr, &rx)?;
    let name = lowered.schema_name().to_string();
    let resolved = rx.resolve(lowered)?;
    Ok(if resolved.schema_name().to_string() == name {
        resolved
    } else {
        resolved.alias(name)
    })
}

/// Lower `py_expr` without resolving it: the row-level part of an
/// expression that the window and group dialects lower, resolve as a whole,
/// and ask `rx` about.
pub(crate) fn lower_expr(py_expr: PyExpr, rx: &Resolver<'_>) -> Result<Expr, String> {
    pyexpr_to_datafusion_inner(py_expr, rx)
}

/// Lower one PyExpr node, recursively (see `lower_expr`). Each node is
/// lowered through `Resolver::lowering`, so questions about it while its
/// parent is lowered do not coerce it again.
fn pyexpr_to_datafusion_inner(py_expr: PyExpr, rx: &Resolver<'_>) -> Result<Expr, String> {
    rx.lowering(|| lower_node(py_expr, rx))
}

fn lower_node(py_expr: PyExpr, rx: &Resolver<'_>) -> Result<Expr, String> {
    match py_expr {
        PyExpr::Column(name) => parse_column_expr(&name, rx.arrow()),
        PyExpr::Literal(value) => Ok(lit(value.to_scalar_value())),
        PyExpr::BinOp { op, left, right } => parse_binop_expr(&op, *left, *right, rx),
        PyExpr::UnaryOp { op, operand } => parse_unaryop_expr(&op, *operand, rx),
        PyExpr::Call { func, on, args, .. } => {
            parse_call_expr(&func, on.map(|on| *on), args, rx)
        }
        PyExpr::Alias { expr, alias } => {
            let inner = pyexpr_to_datafusion_inner(*expr, rx)?;
            Ok(inner.alias(alias))
        }
        PyExpr::Window { .. } => {
            // Window expressions are planned by derive_with_window_functions
            // (window_native), not by the row-level transpiler.
            Err("Window expressions must go through derive() / the window planner".to_string())
        }
    }
}

/// Helper function to detect if a PyExpr contains a window function call
/// Is this Call node itself a window-function invocation?
///
/// The single source of truth for "what counts as a window call" — shared by
/// `contains_window_function` and the staged nested-window rewriter in
/// window.rs (issue #101). Do not duplicate this match.
pub(crate) fn is_window_call(func: &str, on: Option<&PyExpr>) -> bool {
    // Direct window functions
    if matches!(func, "shift" | "rolling" | "diff" | "cum_sum" | "cum_max" | "cum_min") {
        return true;
    }
    // Aggregation functions applied to rolling windows
    if matches!(func, "mean" | "sum" | "min" | "max" | "count" | "std") {
        if let Some(PyExpr::Call { func: inner_func, .. }) = on {
            if inner_func == "rolling" {
                return true;
            }
        }
    }
    false
}

pub fn contains_window_function(py_expr: &PyExpr) -> bool {
    match py_expr {
        // Window expressions always require window function handling
        PyExpr::Window { .. } => true,

        PyExpr::Call { func, on, args, .. } => {
            if is_window_call(func, on.as_deref()) {
                return true;
            }
            // Check recursively in the `on` field
            if on.as_deref().is_some_and(contains_window_function) {
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

/// Elapsed time `on - other` in a fixed-length unit of `unit_seconds`, as Float64.
///
/// The operand types are the types they execute as (`Resolver::data_type`).
/// A date and a timestamp become two naive timestamps at the timestamp's
/// unit; otherwise the operands are coerced with DataFusion's own rule (a
/// mixed Date32/Date64 pair becomes two Date64). Then the subtraction is
/// typed: two dates give an Int64 day count, two timestamps give a Duration
/// in the coerced time unit. The tick length is read from that type, so a
/// date pair and a timestamp pair both report the unit asked for. The
/// integer factor between tick and unit keeps whole-day date differences
/// exact.
fn dt_elapsed(
    on_expr: Expr,
    other_expr: Expr,
    rx: &Resolver<'_>,
    unit_seconds: i64,
) -> Result<Expr, String> {
    use datafusion::arrow::datatypes::TimeUnit;

    let type_of = |expr: &Expr| rx.data_type(expr).map_err(|e| format!("dt_diff: {e}"));
    // `Expr::cast_to` compares against the type DataFusion reports before
    // coercion and can skip a needed cast (a CASE reports its first branch),
    // so casts are explicit, decided from the resolved types.
    let cast = |expr: Expr, from: &DataType, to: &DataType| {
        if from == to {
            expr
        } else {
            Expr::Cast(datafusion::logical_expr::Cast::new(Box::new(expr), to.clone()))
        }
    };
    let on_type = type_of(&on_expr)?;
    let other_type = type_of(&other_expr)?;
    // A naive and a zoned timestamp have no single meaning: DataFusion reads the naive
    // side as UTC when the units match and as wall-clock time in the other zone when
    // they differ, so the elapsed time would depend on the column units. Refuse the
    // pair. Two zoned timestamps are cast to UTC at the finer unit, so any mix of zones
    // and units subtracts as instants (DataFusion cannot coerce differing zones and units).
    let finer = |a: TimeUnit, b: TimeUnit| {
        let rank = |u: TimeUnit| match u {
            TimeUnit::Second => 0,
            TimeUnit::Millisecond => 1,
            TimeUnit::Microsecond => 2,
            TimeUnit::Nanosecond => 3,
        };
        if rank(a) >= rank(b) {
            a
        } else {
            b
        }
    };
    let zoned_pair_unit = match (&on_type, &other_type) {
        (DataType::Timestamp(lu, ltz), DataType::Timestamp(ru, rtz)) => match (ltz, rtz) {
            (Some(_), Some(_)) => Some(finer(*lu, *ru)),
            (None, None) => None,
            _ => {
                return Err(format!(
                    "dt_diff cannot subtract {other_type:?} from {on_type:?}: one timestamp is \
                     timezone-aware and the other is naive; both must be aware or both naive"
                ))
            }
        },
        _ => None,
    };
    // A date against a timestamp is read as DataFusion reads the pair: both
    // naive, the date at UTC midnight and a zoned timestamp at its UTC
    // instant (decision P10 on #145). The pair is cast at the timestamp's own
    // unit rather than DataFusion's nanoseconds, which cannot hold dates
    // outside 1677-2262.
    let is_date = |t: &DataType| matches!(t, DataType::Date32 | DataType::Date64);
    let (on_expr, other_expr, on_type, other_type) = match (&on_type, &other_type) {
        (date, DataType::Timestamp(unit, _)) | (DataType::Timestamp(unit, _), date)
            if is_date(date) =>
        {
            let naive = DataType::Timestamp(*unit, None);
            (
                cast(on_expr, &on_type, &naive),
                cast(other_expr, &other_type, &naive),
                naive.clone(),
                naive,
            )
        }
        _ => (on_expr, other_expr, on_type, other_type),
    };
    let (on_expr, other_expr, on_type, other_type) = match zoned_pair_unit {
        Some(unit) => {
            let utc = DataType::Timestamp(unit, Some("UTC".into()));
            (
                cast(on_expr, &on_type, &utc),
                cast(other_expr, &other_type, &utc),
                utc.clone(),
                utc,
            )
        }
        None => (on_expr, other_expr, on_type, other_type),
    };
    // DataFusion types a subtraction before coercing its operands, and for a mixed
    // Date32/Date64 pair the two disagree: the logical type is Duration(ms) while the
    // kernel that runs after coercion returns an Int64 day count. Coerce the operands
    // first so the type read below is the type the kernel produces.
    let is_temporal =
        |t: &DataType| matches!(t, DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _));
    let (on_coerced, other_coerced) =
        binary_input_types(&on_type, &Operator::Minus, &other_type)
            .ok()
            .filter(|(l, r)| is_temporal(l) && is_temporal(r))
            .ok_or_else(|| {
                format!(
                    "dt_diff cannot subtract {other_type:?} from {on_type:?}; \
                     both sides must be dates or timestamps"
                )
            })?;
    let on_expr = cast(on_expr, &on_type, &on_coerced);
    let other_expr = cast(other_expr, &other_type, &other_coerced);
    let diff_expr = on_expr - other_expr;
    let diff_type = type_of(&diff_expr)?;
    let tick_nanos: i64 = match diff_type {
        DataType::Int64 => 86_400 * 1_000_000_000,
        DataType::Duration(TimeUnit::Second) => 1_000_000_000,
        DataType::Duration(TimeUnit::Millisecond) => 1_000_000,
        DataType::Duration(TimeUnit::Microsecond) => 1_000,
        DataType::Duration(TimeUnit::Nanosecond) => 1,
        other => {
            return Err(format!(
                "dt_diff cannot measure a difference of type {other:?}; \
                 both sides must be dates or timestamps"
            ))
        }
    };
    let unit_nanos = unit_seconds * 1_000_000_000;
    let ticks = Expr::Cast(datafusion::logical_expr::Cast::new(
        Box::new(diff_expr),
        DataType::Float64,
    ));
    // Both lengths are whole seconds or whole days, so one divides the other.
    Ok(if tick_nanos >= unit_nanos {
        ticks * lit((tick_nanos / unit_nanos) as f64)
    } else {
        ticks / lit((unit_nanos / tick_nanos) as f64)
    })
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

// Table-driven unit tests for the pure serialization→Expr mapping layer
// (issue #150). End-to-end DSL coverage lives in py-ltseq/tests/; the
// Python-side deserializer (dict_to_py_expr) needs an interpreter and is
// covered there too.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::LiteralValue;
    use datafusion::arrow::datatypes::Field;
    use datafusion::common::Column;

    fn test_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("s", DataType::Utf8, false),
            Field::new("d", DataType::Date32, false),
        ])
    }

    fn col_expr(name: &str) -> PyExpr {
        PyExpr::Column(name.to_string())
    }

    fn int_lit(value: i64) -> PyExpr {
        PyExpr::Literal(LiteralValue::Int64(value))
    }

    fn call_expr(func: &str, on: PyExpr, args: Vec<PyExpr>) -> PyExpr {
        PyExpr::Call {
            func: func.to_string(),
            args,
            kwargs: Default::default(),
            on: Some(Box::new(on)),
        }
    }

    fn standalone_call(func: &str, args: Vec<PyExpr>) -> PyExpr {
        PyExpr::Call {
            func: func.to_string(),
            args,
            kwargs: Default::default(),
            on: None,
        }
    }

    // ---- parse_binary_op: every row of the mapping table ----

    #[test]
    fn operator_mapping_table() {
        let table = [
            ("Add", Operator::Plus),
            ("Sub", Operator::Minus),
            ("Mul", Operator::Multiply),
            ("Div", Operator::Divide),
            ("Mod", Operator::Modulo),
            ("Eq", Operator::Eq),
            ("Ne", Operator::NotEq),
            ("Lt", Operator::Lt),
            ("Le", Operator::LtEq),
            ("Gt", Operator::Gt),
            ("Ge", Operator::GtEq),
            ("And", Operator::And),
            ("Or", Operator::Or),
        ];
        for (name, expected) in table {
            assert_eq!(parse_binary_op(name), Ok(BinaryOp::Native(expected)), "op {name}");
        }
        assert_eq!(parse_binary_op("FloorDiv"), Ok(BinaryOp::FloorDiv));
    }

    #[test]
    fn operator_unknown_is_error() {
        for bad in ["Pow", "BitXor", "floordiv", ""] {
            let err = parse_binary_op(bad).unwrap_err();
            assert!(err.contains("Unknown binary operator"), "op {bad}: {err}");
        }
    }

    // ---- literals: each LiteralValue transpiles to its own scalar ----

    #[test]
    fn literal_transpiles_to_its_scalar() {
        use datafusion::arrow::datatypes::TimeUnit;
        let schema = test_schema();
        let table = [
            (LiteralValue::Null, ScalarValue::Null),
            (LiteralValue::Boolean(true), ScalarValue::Boolean(Some(true))),
            (LiteralValue::Int64(42), ScalarValue::Int64(Some(42))),
            (LiteralValue::Float64(2.5), ScalarValue::Float64(Some(2.5))),
            (
                LiteralValue::String("hello".to_string()),
                ScalarValue::Utf8(Some("hello".to_string())),
            ),
            (
                LiteralValue::Decimal128 {
                    value: 150,
                    precision: 3,
                    scale: 2,
                },
                ScalarValue::Decimal128(Some(150), 3, 2),
            ),
            (LiteralValue::Date32(19723), ScalarValue::Date32(Some(19723))),
            (
                LiteralValue::Timestamp {
                    value: 1_000_000_500,
                    unit: TimeUnit::Nanosecond,
                    tz: None,
                },
                ScalarValue::TimestampNanosecond(Some(1_000_000_500), None),
            ),
            (
                LiteralValue::Timestamp {
                    value: 7,
                    unit: TimeUnit::Second,
                    tz: Some("Asia/Tokyo".into()),
                },
                ScalarValue::TimestampSecond(Some(7), Some("Asia/Tokyo".into())),
            ),
        ];
        for (value, expected) in table {
            assert_eq!(
                pyexpr_to_datafusion(PyExpr::Literal(value.clone()), &schema),
                Ok(lit(expected)),
                "{value:?}"
            );
        }
    }

    // ---- pyexpr_to_datafusion: structure and error classification ----

    #[test]
    fn column_resolves_case_sensitive_unqualified() {
        let schema = test_schema();
        let expr = pyexpr_to_datafusion(col_expr("a"), &schema).unwrap();
        assert_eq!(expr, Expr::Column(Column::new_unqualified("a")));
    }

    #[test]
    fn column_missing_is_error() {
        let schema = test_schema();
        let err = pyexpr_to_datafusion(col_expr("nope"), &schema).unwrap_err();
        assert!(err.contains("Column 'nope' not found in schema"), "{err}");
    }

    #[test]
    fn binop_builds_binary_expr() {
        let schema = test_schema();
        let expr = pyexpr_to_datafusion(
            PyExpr::BinOp {
                op: "Gt".to_string(),
                left: Box::new(col_expr("a")),
                right: Box::new(int_lit(5)),
            },
            &schema,
        )
        .unwrap();
        let expected = Expr::Column(Column::new_unqualified("a")).gt(lit(5_i64));
        assert_eq!(expr, expected);
    }

    #[test]
    fn binop_floor_div_builds_udf_call() {
        let schema = test_schema();
        let expr = pyexpr_to_datafusion(
            PyExpr::BinOp {
                op: "FloorDiv".to_string(),
                left: Box::new(col_expr("a")),
                right: Box::new(int_lit(2)),
            },
            &schema,
        )
        .unwrap();
        let expected = floor_div::floor_div(
            Expr::Column(Column::new_unqualified("a")),
            lit(2_i64),
        );
        assert_eq!(expr, expected);
    }

    /// Lowering coerces each node once, however deep the expression: the
    /// number of nodes coerced grows linearly with the depth, where coercing
    /// every operand subtree again grew it quadratically (review F4 on #225).
    #[test]
    fn lowering_coerces_each_node_once() {
        let schema = test_schema();
        let binop = |op: &str, left: PyExpr, right: PyExpr| PyExpr::BinOp {
            op: op.to_string(),
            left: Box::new(left),
            right: Box::new(right),
        };
        // `((a + 1) + 1) + ...` and `if_else(a > 1, 1, if_else(a > 2, 2, ...))`
        let sum = |depth: i64| (0..depth).fold(col_expr("a"), |e, _| binop("Add", e, int_lit(1)));
        let cases = |depth: i64| {
            (0..depth).fold(col_expr("b"), |e, i| {
                let cond = binop("Gt", col_expr("a"), int_lit(i));
                standalone_call("if_else", vec![cond, int_lit(i), e])
            })
        };
        for build in [&sum as &dyn Fn(i64) -> PyExpr, &cases] {
            let coerced = |depth: i64| {
                let rx = Resolver::new(&schema).unwrap();
                rx.resolve(lower_expr(build(depth), &rx).unwrap()).unwrap();
                rx.coerced_nodes()
            };
            let (shallow, deep) = (coerced(100), coerced(200));
            assert!(
                deep <= 2 * shallow + 10,
                "{shallow} nodes coerced at depth 100, {deep} at depth 200"
            );
        }
    }

    #[test]
    fn binop_unknown_operator_is_error() {
        let schema = test_schema();
        let err = pyexpr_to_datafusion(
            PyExpr::BinOp {
                op: "Pow".to_string(),
                left: Box::new(col_expr("a")),
                right: Box::new(col_expr("b")),
            },
            &schema,
        )
        .unwrap_err();
        assert!(err.contains("Unknown binary operator: Pow"), "{err}");
    }

    #[test]
    fn unaryop_not_negates_operand() {
        let schema = test_schema();
        let ok = pyexpr_to_datafusion(
            PyExpr::UnaryOp {
                op: "Not".to_string(),
                operand: Box::new(col_expr("a")),
            },
            &schema,
        )
        .unwrap();
        assert_eq!(ok, Expr::Column(Column::new_unqualified("a")).not());
    }

    #[test]
    fn unaryop_unknown_is_error() {
        let schema = test_schema();
        let err = pyexpr_to_datafusion(
            PyExpr::UnaryOp {
                op: "Neg".to_string(),
                operand: Box::new(col_expr("a")),
            },
            &schema,
        )
        .unwrap_err();
        assert!(err.contains("Unknown unary operator: Neg"), "{err}");
    }

    #[test]
    fn alias_wraps_inner_expr() {
        let schema = test_schema();
        let expr = pyexpr_to_datafusion(
            PyExpr::Alias {
                expr: Box::new(col_expr("a")),
                alias: "renamed".to_string(),
            },
            &schema,
        )
        .unwrap();
        assert_eq!(
            expr,
            Expr::Column(Column::new_unqualified("a")).alias("renamed")
        );
    }

    #[test]
    fn window_variant_rejected_in_row_context() {
        let schema = test_schema();
        let err = pyexpr_to_datafusion(
            PyExpr::Window {
                expr: Box::new(col_expr("a")),
                partition_by: None,
                order_by: None,
                descending: false,
            },
            &schema,
        )
        .unwrap_err();
        assert!(err.contains("window planner"), "{err}");
    }

    #[test]
    fn window_function_call_rejected_in_row_context() {
        let schema = test_schema();
        for func in ["shift", "rolling", "diff", "cum_sum", "cum_max", "cum_min"] {
            let err = pyexpr_to_datafusion(call_expr(func, col_expr("a"), vec![]), &schema)
                .unwrap_err();
            assert!(
                err.contains("requires DataFrame context"),
                "func {func}: {err}"
            );
        }
    }

    #[test]
    fn unknown_method_is_error() {
        let schema = test_schema();
        let err =
            pyexpr_to_datafusion(call_expr("made_up", col_expr("a"), vec![]), &schema).unwrap_err();
        assert!(err.contains("Method 'made_up' not yet supported"), "{err}");
    }

    #[test]
    fn string_function_on_non_string_column_is_error() {
        let schema = test_schema();
        let err = pyexpr_to_datafusion(call_expr("str_lower", col_expr("a"), vec![]), &schema)
            .unwrap_err();
        assert!(err.contains("requires a string column"), "{err}");
    }

    #[test]
    fn temporal_function_on_non_temporal_column_is_error() {
        let schema = test_schema();
        let err = pyexpr_to_datafusion(call_expr("dt_year", col_expr("a"), vec![]), &schema)
            .unwrap_err();
        assert!(err.contains("requires a date/datetime column"), "{err}");
    }

    // ---- standalone calls: no receiver, inputs in args ----

    #[test]
    fn standalone_call_takes_its_input_from_args() {
        use datafusion::functions::math::expr_fn::abs;
        let schema = test_schema();
        let a = || Expr::Column(Column::new_unqualified("a"));
        assert_eq!(
            pyexpr_to_datafusion(standalone_call("abs", vec![col_expr("a")]), &schema),
            Ok(abs(a()))
        );
        assert_eq!(
            pyexpr_to_datafusion(call_expr("abs", col_expr("a"), vec![]), &schema),
            Ok(abs(a()))
        );
        assert_eq!(
            pyexpr_to_datafusion(
                standalone_call("coalesce", vec![col_expr("a"), int_lit(0)]),
                &schema
            ),
            Ok(coalesce(vec![a(), lit(0_i64)]))
        );
    }

    #[test]
    fn receiver_functions_reject_standalone_calls() {
        // These used to fail with "Column '' not found in schema".
        let schema = test_schema();
        for func in [
            "fill_null",
            "is_null",
            "cast",
            "is_in",
            "str_lower",
            "dt_year",
        ] {
            let err = pyexpr_to_datafusion(standalone_call(func, vec![col_expr("s")]), &schema)
                .unwrap_err();
            assert!(err.contains("must be called as a method"), "{func}: {err}");
        }
    }

    #[test]
    fn standalone_aggregate_is_not_a_rolling_window() {
        let rolling = call_expr("rolling", col_expr("a"), vec![int_lit(3)]);
        assert!(is_window_call("mean", Some(&rolling)));
        assert!(!is_window_call("mean", None));
    }
}
