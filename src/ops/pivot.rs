//! Pivot operation for LTSeqTable
//!
//! Transforms table from long to wide format using native DataFusion
//! conditional aggregation (no SQL string construction).

use crate::engine::RUNTIME;
use crate::error::LtseqError;
use crate::LTSeqTable;
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
use datafusion::logical_expr::expr::Case;
use datafusion::logical_expr::{case, col, lit, Expr};
use pyo3::prelude::*;
use std::collections::HashSet;
use std::sync::Arc;

/// Pivot table from long to wide format using native conditional aggregation
///
/// Transforms data where unique values in a column become new columns,
/// and row groups are aggregated based on specified columns.
///
/// Example:
///   Input: year, region, amount
///   pivot(index="year", columns="region", values="amount", agg_fn="sum")
///   Output: year, West, East, Central
pub fn pivot_impl(
    table: &LTSeqTable,
    index_cols: Vec<String>,
    pivot_col: String,
    value_col: String,
    agg_fn: String,
) -> PyResult<LTSeqTable> {
    // Validate inputs
    if table.dataframe.is_none() {
        return Err(LtseqError::Validation("Cannot pivot empty table".into()).into());
    }

    // SAFETY: dataframe.is_none() is checked above
    let df = table.dataframe.as_ref().expect("dataframe checked above");
    let schema = table
        .schema
        .as_ref()
        .expect("schema present when dataframe is");

    // Validate that all required columns exist
    let col_names: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();

    for col in &index_cols {
        if !col_names.contains(col) {
            return Err(LtseqError::Validation(format!(
                "Index column '{}' not found in table",
                col
            ))
            .into());
        }
    }

    if !col_names.contains(&pivot_col) {
        return Err(LtseqError::Validation(format!(
            "Pivot column '{}' not found in table",
            pivot_col
        ))
        .into());
    }

    if !col_names.contains(&value_col) {
        return Err(LtseqError::Validation(format!(
            "Value column '{}' not found in table",
            value_col
        ))
        .into());
    }

    // Validate aggregation function
    let agg_fn_upper = agg_fn.to_uppercase();
    match agg_fn_upper.as_str() {
        "SUM" | "MEAN" | "COUNT" | "MIN" | "MAX" => {}
        _ => {
            return Err(LtseqError::Validation(format!(
                "Invalid aggregation function '{}'. Must be one of: sum, mean, count, min, max",
                agg_fn
            ))
            .into());
        }
    }

    // Discover distinct pivot values via lazy select + distinct + collect.
    // This only materializes the pivot column, not the entire table.
    let pivot_values = RUNTIME
        .block_on(async {
            let distinct_df = (**df)
                .clone()
                .select(vec![col(&pivot_col)])
                .map_err(|e| LtseqError::Runtime(format!("Failed to select pivot column: {}", e)))?
                .distinct()
                .map_err(|e| LtseqError::Runtime(format!("Failed to get distinct values: {}", e)))?;

            let batches = distinct_df
                .collect()
                .await
                .map_err(|e| LtseqError::Runtime(format!("Failed to collect distinct pivot values: {}", e)))?;

            let mut values_set = HashSet::new();
            for batch in &batches {
                let col_arr = batch.column(0);
                extract_pivot_values(col_arr, &mut values_set)
                    .map_err(|e| LtseqError::Runtime(format!("Failed to extract pivot values: {}", e)))?;
            }

            let mut values: Vec<String> = values_set.into_iter().collect();
            values.sort();
            Ok::<_, LtseqError>(values)
        })
        .map_err(|e| LtseqError::Runtime(format!("Failed to discover pivot values: {}", e)))?;

    // Build group-by expressions for index columns
    let group_exprs: Vec<Expr> = index_cols.iter().map(|c| col(c)).collect();

    // Build aggregate expressions with conditional logic:
    // For each pivot value, create: AGG(CASE WHEN pivot_col = value THEN value_col ELSE NULL END)
    let agg_exprs: Vec<Expr> = pivot_values
        .iter()
        .map(|val| {
            let case_expr = build_case_expr(&pivot_col, val, &value_col);
            let agg_expr = build_agg_expr(&agg_fn_upper, case_expr);
            agg_expr.alias(val)
        })
        .collect();

    // Execute native aggregate — stays lazy until collect
    let result_df = RUNTIME
        .block_on(async {
            let agg_df = (**df)
                .clone()
                .aggregate(group_exprs, agg_exprs)
                .map_err(|e| format!("Aggregate failed: {}", e))?;

            let batches = agg_df
                .collect()
                .await
                .map_err(|e| format!("Collect failed: {}", e))?;
            Ok::<_, String>(batches)
        })
        .map_err(LtseqError::Runtime)?;

    // Create result schema from the result batches' schema
    let result_schema = if !result_df.is_empty() {
        result_df[0].schema()
    } else {
        // Build expected schema for empty results
        let mut fields: Vec<ArrowField> = index_cols
            .iter()
            .filter_map(|c| schema.field_with_name(c).ok())
            .map(|f| f.clone())
            .collect();
        for val in &pivot_values {
            fields.push(ArrowField::new(val, DataType::Float64, true));
        }
        Arc::new(ArrowSchema::new(fields))
    };

    LTSeqTable::from_batches_with_schema(
        Arc::clone(&table.session),
        result_df,
        result_schema,
        Vec::new(),
        None,
    )
}

/// Build CASE WHEN pivot_col = value THEN value_col ELSE NULL END
fn build_case_expr(pivot_col: &str, value: &str, value_col: &str) -> Expr {
    // Try to parse value as numeric for proper literal typing
    if let Ok(int_val) = value.parse::<i64>() {
        case(col(pivot_col))
            .when(lit(int_val), col(value_col))
            .otherwise(lit(datafusion::scalar::ScalarValue::Null))
            .unwrap_or_else(|_| {
                // Fallback if case builder fails
                Expr::Case(Case {
                    expr: Some(Box::new(col(pivot_col))),
                    when_then_expr: vec![(
                        Box::new(lit(int_val)),
                        Box::new(col(value_col)),
                    )],
                    else_expr: Some(Box::new(lit(datafusion::scalar::ScalarValue::Null))),
                })
            })
    } else if let Ok(float_val) = value.parse::<f64>() {
        case(col(pivot_col))
            .when(lit(float_val), col(value_col))
            .otherwise(lit(datafusion::scalar::ScalarValue::Null))
            .unwrap_or_else(|_| {
                Expr::Case(Case {
                    expr: Some(Box::new(col(pivot_col))),
                    when_then_expr: vec![(
                        Box::new(lit(float_val)),
                        Box::new(col(value_col)),
                    )],
                    else_expr: Some(Box::new(lit(datafusion::scalar::ScalarValue::Null))),
                })
            })
    } else {
        // String value
        case(col(pivot_col))
            .when(lit(value), col(value_col))
            .otherwise(lit(datafusion::scalar::ScalarValue::Null))
            .unwrap_or_else(|_| {
                Expr::Case(Case {
                    expr: Some(Box::new(col(pivot_col))),
                    when_then_expr: vec![(
                        Box::new(lit(value)),
                        Box::new(col(value_col)),
                    )],
                    else_expr: Some(Box::new(lit(datafusion::scalar::ScalarValue::Null))),
                })
            })
    }
}

/// Build aggregate expression from function name and input expression
fn build_agg_expr(agg_fn: &str, expr: Expr) -> Expr {
    use datafusion::functions_aggregate::expr_fn::{avg, count, max, min, sum};

    match agg_fn {
        "SUM" => sum(expr),
        "MEAN" | "AVG" => avg(expr),
        "COUNT" => count(expr),
        "MIN" => min(expr),
        "MAX" => max(expr),
        _ => {
            // Fallback - should not happen due to validation above
            sum(expr)
        }
    }
}

/// Extract pivot values from a column into a HashSet
fn extract_pivot_values(
    col_arr: &std::sync::Arc<dyn datafusion::arrow::array::Array>,
    pivot_values_set: &mut HashSet<String>,
) -> PyResult<()> {
    match col_arr.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => {
            if let Some(string_col) = col_arr.as_any().downcast_ref::<datafusion::arrow::array::StringArray>() {
                for i in 0..string_col.len() {
                    if !string_col.is_null(i) {
                        pivot_values_set.insert(string_col.value(i).to_string());
                    }
                }
            }
        }
        DataType::Int32 => {
            if let Some(int_col) = col_arr.as_any().downcast_ref::<datafusion::arrow::array::Int32Array>() {
                for i in 0..int_col.len() {
                    if !int_col.is_null(i) {
                        pivot_values_set.insert(int_col.value(i).to_string());
                    }
                }
            }
        }
        DataType::Int64 => {
            if let Some(int_col) = col_arr.as_any().downcast_ref::<datafusion::arrow::array::Int64Array>() {
                for i in 0..int_col.len() {
                    if !int_col.is_null(i) {
                        pivot_values_set.insert(int_col.value(i).to_string());
                    }
                }
            }
        }
        DataType::Float32 => {
            if let Some(float_col) = col_arr.as_any().downcast_ref::<datafusion::arrow::array::Float32Array>() {
                for i in 0..float_col.len() {
                    if !float_col.is_null(i) {
                        pivot_values_set.insert(float_col.value(i).to_string());
                    }
                }
            }
        }
        DataType::Float64 => {
            if let Some(float_col) = col_arr.as_any().downcast_ref::<datafusion::arrow::array::Float64Array>() {
                for i in 0..float_col.len() {
                    if !float_col.is_null(i) {
                        pivot_values_set.insert(float_col.value(i).to_string());
                    }
                }
            }
        }
        _ => {
            return Err(LtseqError::Validation(
                "Pivot column must be string, int, or float type".into(),
            )
            .into());
        }
    }
    Ok(())
}
