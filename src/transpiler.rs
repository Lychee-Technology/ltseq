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

/// Convert PyExpr to DataFusion Expr
pub fn pyexpr_to_datafusion(py_expr: PyExpr, schema: &ArrowSchema) -> Result<Expr, String> {
    match py_expr {
        PyExpr::Column(name) => {
            // Validate column exists in schema
            if !schema.fields().iter().any(|f| f.name() == &name) {
                return Err(format!("Column '{}' not found in schema", name));
            }
            Ok(col(name))
        }

        PyExpr::Literal { value, dtype } => {
            // Parse dtype and convert value string to appropriate literal type
            match dtype.as_str() {
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
                "Null" => {
                    // NULL literal - represents None in Python
                    // When comparing with NULL using == or !=, DataFusion handles it properly
                    // We use a null value which works with IS NULL / IS NOT NULL comparisons
                    Ok(lit(ScalarValue::Null))
                }
                _ => Err(format!("Unknown dtype: {}", dtype)),
            }
        }

        PyExpr::BinOp { op, left, right } => {
            let left_expr = pyexpr_to_datafusion(*left, schema)?;
            let right_expr = pyexpr_to_datafusion(*right, schema)?;

            // Map string ops to DataFusion Operator
            let operator = match op.as_str() {
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

        PyExpr::UnaryOp { op, operand } => {
            let operand_expr = pyexpr_to_datafusion(*operand, schema)?;

            match op.as_str() {
                "Not" => Ok(operand_expr.not()),
                _ => Err(format!("Unknown unary operator: {}", op)),
            }
        }

        PyExpr::Call {
            func,
            args: _,
            kwargs: _,
            on,
        } => {
            // Phase 6: Window functions are now recognized but require special handling
            // They will be transpiled at the DataFrame level in derive()
            match func.as_str() {
                "is_null" => {
                    let on_expr = pyexpr_to_datafusion(*on, schema)?;
                    Ok(on_expr.is_null())
                }
                "is_not_null" => {
                    let on_expr = pyexpr_to_datafusion(*on, schema)?;
                    Ok(on_expr.is_not_null())
                }
                // Math functions that will be handled in SQL
                "abs" | "ceil" | "floor" | "round" => {
                    // These will be handled in the SQL transpilation path
                    Err(format!("Math function '{}' requires window context - should be handled in derive()", func))
                }
                // Window functions - these will be handled specially in derive()
                "shift" | "rolling" | "diff" | "cum_sum" | "mean" | "sum" | "min" | "max"
                | "count" => {
                    // For now, return an error indicating the window function needs DataFrame-level handling
                    Err(format!("Window function '{}' requires DataFrame context - should be handled in derive()", func))
                }
                _ => Err(format!("Method '{}' not yet supported", func)),
            }
        }
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
            if matches!(func.as_str(), "mean" | "sum" | "min" | "max" | "count") {
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
            kwargs: _,
            on,
        } => {
            match func.as_str() {
                "shift" => {
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

                "rolling" => {
                    // rolling() should not be called directly - it's handled through the agg method
                    Err(
                        "rolling() should not reach pyexpr_to_sql - it's handled separately"
                            .to_string(),
                    )
                }

                "diff" => {
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

                "cum_sum" => {
                    let on_sql = pyexpr_to_sql(on, schema)?;
                    Ok(format!("SUM({}) OVER ()", on_sql))
                }

                "abs" => {
                    let arg_sql = if args.is_empty() {
                        return Err("abs() requires an argument".to_string());
                    } else {
                        pyexpr_to_sql(&args[0], schema)?
                    };
                    Ok(format!("ABS({})", arg_sql))
                }

                "ceil" => {
                    let arg_sql = if args.is_empty() {
                        return Err("ceil() requires an argument".to_string());
                    } else {
                        pyexpr_to_sql(&args[0], schema)?
                    };
                    Ok(format!("CEIL({})", arg_sql))
                }

                "floor" => {
                    let arg_sql = if args.is_empty() {
                        return Err("floor() requires an argument".to_string());
                    } else {
                        pyexpr_to_sql(&args[0], schema)?
                    };
                    Ok(format!("FLOOR({})", arg_sql))
                }

                "round" => {
                    let arg_sql = if args.is_empty() {
                        return Err("round() requires an argument".to_string());
                    } else {
                        pyexpr_to_sql(&args[0], schema)?
                    };
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

                _ => Err(format!("Unsupported function in window context: {}", func)),
            }
        }
    }
}
