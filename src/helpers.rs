//! Helper and utility functions for Rust backend

use datafusion::arrow::datatypes::DataType;

/// Check if a DataType is numeric (Int, Float, etc.)
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
    )
}

/// Apply OVER clause to all nested LAG/LEAD functions in an expression
/// This recursively finds all LAG(...) and LEAD(...) calls and adds the proper OVER clause
pub fn apply_over_to_window_functions(expr: &str, order_by: &str) -> String {
    let mut result = String::new();
    let mut i = 0;
    let bytes = expr.as_bytes();

    while i < bytes.len() {
        // Look for LAG(
        if i + 4 <= bytes.len() && &expr[i..i + 4] == "LAG(" {
            result.push_str("LAG(");
            i += 4;
            let mut depth = 1;
            while i < bytes.len() && depth > 0 {
                let ch = bytes[i] as char;
                result.push(ch);
                if ch == '(' {
                    depth += 1;
                } else if ch == ')' {
                    depth -= 1;
                }
                i += 1;
            }
            // Add OVER clause
            if !order_by.is_empty() {
                result.push_str(&format!(" OVER (ORDER BY {})", order_by));
            } else {
                result.push_str(" OVER ()");
            }
        }
        // Look for LEAD(
        else if i + 5 <= bytes.len() && &expr[i..i + 5] == "LEAD(" {
            result.push_str("LEAD(");
            i += 5;
            let mut depth = 1;
            while i < bytes.len() && depth > 0 {
                let ch = bytes[i] as char;
                result.push(ch);
                if ch == '(' {
                    depth += 1;
                } else if ch == ')' {
                    depth -= 1;
                }
                i += 1;
            }
            // Add OVER clause
            if !order_by.is_empty() {
                result.push_str(&format!(" OVER (ORDER BY {})", order_by));
            } else {
                result.push_str(" OVER ()");
            }
        } else {
            result.push(bytes[i] as char);
            i += 1;
        }
    }
    result
}
