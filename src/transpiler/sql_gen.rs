//! SQL string generation from PyExpr
//!
//! This module converts PyExpr trees to SQL string representations,
//! primarily used for window function operations that need to be
//! executed via DataFusion's SQL interface.

use crate::types::PyExpr;
use datafusion::arrow::datatypes::Schema as ArrowSchema;

/// Handle SQL for shift() window function
///
/// Supports optional default value via kwargs:
/// - shift(1) -> LAG(col, 1)
/// - shift(1, default=0) -> LAG(col, 1, 0)
/// - shift(-1, default=0) -> LEAD(col, 1, 0)
fn sql_shift(
    on: &PyExpr,
    args: &[PyExpr],
    kwargs: &std::collections::HashMap<String, PyExpr>,
    schema: &ArrowSchema,
) -> Result<String, String> {
    let on_sql = pyexpr_to_sql(on, schema)?;
    let offset = if args.is_empty() {
        1
    } else if let PyExpr::Literal { value, .. } = &args[0] {
        value.parse::<i32>().unwrap_or(1)
    } else {
        return Err("shift() offset must be a literal integer".to_string());
    };

    // Check for default value in kwargs
    let default_sql = if let Some(default_expr) = kwargs.get("default") {
        Some(pyexpr_to_sql(default_expr, schema)?)
    } else {
        None
    };

    if offset >= 0 {
        match default_sql {
            Some(def) => Ok(format!("LAG({}, {}, {})", on_sql, offset, def)),
            None => Ok(format!("LAG({}, {})", on_sql, offset)),
        }
    } else {
        match default_sql {
            Some(def) => Ok(format!("LEAD({}, {}, {})", on_sql, -offset, def)),
            None => Ok(format!("LEAD({}, {})", on_sql, -offset)),
        }
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

/// Handle SQL for rolling aggregation functions
fn sql_rolling_agg(func: &str, on: &PyExpr, schema: &ArrowSchema) -> Result<String, String> {
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
                _ => return Err(format!("Unknown aggregation function: {}", func)),
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

// ========== Category-based SQL call handlers ==========

/// Handle SQL for conditional and null operations
fn sql_call_conditional_null(
    func: &str,
    on: &PyExpr,
    args: &[PyExpr],
    schema: &ArrowSchema,
) -> Result<String, String> {
    match func {
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
        _ => Err(format!("Not a conditional/null function: {}", func)),
    }
}

/// Handle SQL for string operations
fn sql_call_string(
    func: &str,
    on: &PyExpr,
    args: &[PyExpr],
    schema: &ArrowSchema,
) -> Result<String, String> {
    let on_sql = pyexpr_to_sql(on, schema)?;
    match func {
        "str_contains" => {
            if args.is_empty() {
                return Err("str_contains requires a pattern argument".to_string());
            }
            let pattern = pyexpr_to_sql(&args[0], schema)?;
            Ok(format!("POSITION({} IN {}) > 0", pattern, on_sql))
        }
        "str_starts_with" => {
            if args.is_empty() {
                return Err("str_starts_with requires a prefix argument".to_string());
            }
            let prefix = pyexpr_to_sql(&args[0], schema)?;
            Ok(format!("LEFT({}, LENGTH({})) = {}", on_sql, prefix, prefix))
        }
        "str_ends_with" => {
            if args.is_empty() {
                return Err("str_ends_with requires a suffix argument".to_string());
            }
            let suffix = pyexpr_to_sql(&args[0], schema)?;
            Ok(format!(
                "RIGHT({}, LENGTH({})) = {}",
                on_sql, suffix, suffix
            ))
        }
        "str_lower" => Ok(format!("LOWER({})", on_sql)),
        "str_upper" => Ok(format!("UPPER({})", on_sql)),
        "str_strip" => Ok(format!("TRIM({})", on_sql)),
        "str_len" => Ok(format!("LENGTH({})", on_sql)),
        "str_slice" => {
            if args.len() < 2 {
                return Err("str_slice requires start and length arguments".to_string());
            }
            let start = pyexpr_to_sql(&args[0], schema)?;
            let length = pyexpr_to_sql(&args[1], schema)?;
            Ok(format!("SUBSTRING({}, {} + 1, {})", on_sql, start, length))
        }
        "str_regex_match" => {
            if args.is_empty() {
                return Err("str_regex_match requires a pattern argument".to_string());
            }
            let pattern = pyexpr_to_sql(&args[0], schema)?;
            Ok(format!("REGEXP_LIKE({}, {})", on_sql, pattern))
        }
        "str_replace" => {
            if args.len() < 2 {
                return Err("str_replace requires 'old' and 'new' arguments".to_string());
            }
            let old_sql = pyexpr_to_sql(&args[0], schema)?;
            let new_sql = pyexpr_to_sql(&args[1], schema)?;
            Ok(format!("REPLACE({}, {}, {})", on_sql, old_sql, new_sql))
        }
        "str_concat" => {
            let mut all_parts = vec![on_sql];
            for arg in args {
                all_parts.push(pyexpr_to_sql(arg, schema)?);
            }
            Ok(format!("CONCAT({})", all_parts.join(", ")))
        }
        "str_pad_left" => {
            if args.is_empty() {
                return Err("str_pad_left requires a width argument".to_string());
            }
            let width_sql = pyexpr_to_sql(&args[0], schema)?;
            let char_sql = if args.len() > 1 {
                pyexpr_to_sql(&args[1], schema)?
            } else {
                "' '".to_string()
            };
            Ok(format!("LPAD({}, {}, {})", on_sql, width_sql, char_sql))
        }
        "str_pad_right" => {
            if args.is_empty() {
                return Err("str_pad_right requires a width argument".to_string());
            }
            let width_sql = pyexpr_to_sql(&args[0], schema)?;
            let char_sql = if args.len() > 1 {
                pyexpr_to_sql(&args[1], schema)?
            } else {
                "' '".to_string()
            };
            Ok(format!("RPAD({}, {}, {})", on_sql, width_sql, char_sql))
        }
        "str_split" => {
            if args.len() < 2 {
                return Err("str_split requires delimiter and index arguments".to_string());
            }
            let delimiter_sql = pyexpr_to_sql(&args[0], schema)?;
            let index_sql = pyexpr_to_sql(&args[1], schema)?;
            Ok(format!(
                "SPLIT_PART({}, {}, {})",
                on_sql, delimiter_sql, index_sql
            ))
        }
        _ => Err(format!("Not a string function: {}", func)),
    }
}

/// Handle SQL for temporal operations
fn sql_call_temporal(
    func: &str,
    on: &PyExpr,
    args: &[PyExpr],
    schema: &ArrowSchema,
) -> Result<String, String> {
    let on_sql = pyexpr_to_sql(on, schema)?;
    match func {
        "dt_year" => Ok(format!("EXTRACT(YEAR FROM {})", on_sql)),
        "dt_month" => Ok(format!("EXTRACT(MONTH FROM {})", on_sql)),
        "dt_day" => Ok(format!("EXTRACT(DAY FROM {})", on_sql)),
        "dt_hour" => Ok(format!("EXTRACT(HOUR FROM {})", on_sql)),
        "dt_minute" => Ok(format!("EXTRACT(MINUTE FROM {})", on_sql)),
        "dt_second" => Ok(format!("EXTRACT(SECOND FROM {})", on_sql)),
        "dt_add" => {
            if args.len() < 3 {
                return Err("dt_add requires days, months, and years arguments".to_string());
            }
            let days = pyexpr_to_sql(&args[0], schema)?;
            let months = pyexpr_to_sql(&args[1], schema)?;
            let years = pyexpr_to_sql(&args[2], schema)?;
            Ok(format!(
                "({} + INTERVAL '{}' DAY + INTERVAL '{}' MONTH + INTERVAL '{}' YEAR)",
                on_sql, days, months, years
            ))
        }
        "dt_diff" => {
            if args.is_empty() {
                return Err("dt_diff requires another date argument".to_string());
            }
            let other = pyexpr_to_sql(&args[0], schema)?;
            Ok(format!("DATEDIFF('day', {}, {})", other, on_sql))
        }
        _ => Err(format!("Not a temporal function: {}", func)),
    }
}

/// Handle SQL for function calls.
///
/// Dispatches to category-specific handlers: window, math, conditional/null,
/// string, and temporal operations.
fn sql_call(
    func: &str,
    on: &PyExpr,
    args: &[PyExpr],
    kwargs: &std::collections::HashMap<String, PyExpr>,
    schema: &ArrowSchema,
) -> Result<String, String> {
    match func {
        // Window functions
        "shift" => sql_shift(on, args, kwargs, schema),
        "rolling" => {
            Err("rolling() should not reach pyexpr_to_sql - it's handled separately".to_string())
        }
        "diff" => sql_diff(on, args, schema),
        "cum_sum" => {
            let on_sql = pyexpr_to_sql(on, schema)?;
            Ok(format!("SUM({}) OVER ()", on_sql))
        }
        "mean" | "sum" | "min" | "max" | "count" | "std" => sql_rolling_agg(func, on, schema),
        // Math functions
        "abs" => sql_abs(args, schema),
        "ceil" => sql_ceil(args, schema),
        "floor" => sql_floor(args, schema),
        "round" => sql_round(args, schema),
        // Conditional and null handling
        "if_else" | "fill_null" | "is_null" | "is_not_null" => {
            sql_call_conditional_null(func, on, args, schema)
        }
        // String operations
        f if f.starts_with("str_") => sql_call_string(func, on, args, schema),
        // Temporal operations
        f if f.starts_with("dt_") => sql_call_temporal(func, on, args, schema),
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

        PyExpr::Call {
            func,
            args,
            kwargs,
            on,
        } => sql_call(func, on, args, kwargs, schema),

        PyExpr::Window {
            expr,
            partition_by,
            order_by,
            descending,
        } => sql_window(
            expr,
            partition_by.as_deref(),
            order_by.as_deref(),
            *descending,
            schema,
        ),

        PyExpr::Alias { expr, alias } => {
            let inner_sql = pyexpr_to_sql(expr, schema)?;
            Ok(format!("{} AS \"{}\"", inner_sql, alias))
        }
    }
}

/// Handle SQL for window expressions with OVER clause
fn sql_window(
    expr: &PyExpr,
    partition_by: Option<&PyExpr>,
    order_by: Option<&PyExpr>,
    descending: bool,
    schema: &ArrowSchema,
) -> Result<String, String> {
    // Get the inner function call
    let (func_name, args) = match expr {
        PyExpr::Call { func, args, .. } => (func.as_str(), args),
        _ => return Err("Window expression must wrap a function call".to_string()),
    };

    // Generate the function part
    let func_sql = match func_name {
        "row_number" => "ROW_NUMBER()".to_string(),
        "rank" => "RANK()".to_string(),
        "dense_rank" => "DENSE_RANK()".to_string(),
        "ntile" => {
            if args.is_empty() {
                return Err("ntile() requires a bucket count argument".to_string());
            }
            if let PyExpr::Literal { value, .. } = &args[0] {
                format!("NTILE({})", value)
            } else {
                return Err("ntile() bucket count must be a literal integer".to_string());
            }
        }
        _ => return Err(format!("Unsupported window function: {}", func_name)),
    };

    // Build OVER clause parts
    let mut over_parts = Vec::new();

    // PARTITION BY clause
    if let Some(pb) = partition_by {
        let pb_sql = pyexpr_to_sql(pb, schema)?;
        over_parts.push(format!("PARTITION BY {}", pb_sql));
    }

    // ORDER BY clause
    if let Some(ob) = order_by {
        let ob_sql = pyexpr_to_sql(ob, schema)?;
        let direction = if descending { " DESC" } else { "" };
        over_parts.push(format!("ORDER BY {}{}", ob_sql, direction));
    }

    // Combine into final SQL
    let over_clause = if over_parts.is_empty() {
        "OVER ()".to_string()
    } else {
        format!("OVER ({})", over_parts.join(" "))
    };

    Ok(format!("{} {}", func_sql, over_clause))
}
