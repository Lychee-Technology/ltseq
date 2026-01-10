//! Expression optimization: Constant folding and boolean simplification
//!
//! This module provides compile-time optimizations for PyExpr trees:
//! - **Constant Folding**: Arithmetic operations on literals are evaluated at compile time
//!   (e.g., `1 + 2 + r.col` → `3 + r.col`)
//! - **Boolean Simplification**: Trivial boolean expressions are simplified
//!   (e.g., `x & True` → `x`, `x | False` → `x`)

use crate::types::PyExpr;

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
