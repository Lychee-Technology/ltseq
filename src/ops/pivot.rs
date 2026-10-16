//! Pivot operation for LTSeqTable
//!
//! Transforms table from long to wide format using native DataFusion
//! conditional aggregation (no SQL string construction).
//!
//! Runs with the GIL released (`lib.rs::pivot` wraps `pivot_impl` in
//! `gil::detached`): plain Rust arguments, `LtseqError` results.

use crate::engine::RUNTIME;
use crate::error::LtseqError;
use crate::LTSeqTable;
use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
use datafusion::logical_expr::expr::Case;
use datafusion::logical_expr::{col, lit, Expr};
use datafusion::scalar::ScalarValue;
use std::collections::BTreeMap;
use std::sync::Arc;

/// The output columns of a pivot, keyed by name, each with the distinct pivot
/// values whose rows it aggregates. The values keep the pivot column's own
/// type (its value type when dictionary-encoded), and the map's order is the
/// output column order: the names sorted as strings.
type PivotColumns = BTreeMap<String, Vec<ScalarValue>>;

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
) -> Result<LTSeqTable, LtseqError> {
    // Validate inputs
    if table.dataframe.is_none() {
        return Err(LtseqError::Validation("Cannot pivot empty table".into()));
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
            )));
        }
    }

    if !col_names.contains(&pivot_col) {
        return Err(LtseqError::Validation(format!(
            "Pivot column '{}' not found in table",
            pivot_col
        )));
    }

    if !col_names.contains(&value_col) {
        return Err(LtseqError::Validation(format!(
            "Value column '{}' not found in table",
            value_col
        )));
    }

    // Validate aggregation function
    let agg_fn_upper = agg_fn.to_uppercase();
    match agg_fn_upper.as_str() {
        "SUM" | "MEAN" | "COUNT" | "MIN" | "MAX" => {}
        _ => {
            return Err(LtseqError::Validation(format!(
                "Invalid aggregation function '{}'. Must be one of: sum, mean, count, min, max",
                agg_fn
            )));
        }
    }

    // Discover distinct pivot values via lazy select + distinct + collect.
    // This only materializes the pivot column, not the entire table.
    let pivot_columns = RUNTIME
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

            let mut columns = PivotColumns::new();
            for batch in &batches {
                collect_pivot_values(batch.column(0), &mut columns).map_err(|e| {
                    LtseqError::Runtime(format!("Failed to extract pivot values: {}", e))
                })?;
            }
            Ok::<_, LtseqError>(columns)
        })
        .map_err(|e| LtseqError::Runtime(format!("Failed to discover pivot values: {}", e)))?;

    // Build group-by expressions for index columns
    let group_exprs: Vec<Expr> = index_cols.iter().map(col).collect();

    // One conditional aggregate per output column:
    // AGG(CASE WHEN pivot_col IN (values) THEN value_col END)
    let agg_exprs: Vec<Expr> = pivot_columns
        .iter()
        .map(|(name, values)| {
            let case_expr = build_case_expr(&pivot_col, values, &value_col);
            build_agg_expr(&agg_fn_upper, case_expr).alias(name)
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
            .cloned()
            .collect();
        for name in pivot_columns.keys() {
            fields.push(ArrowField::new(name, DataType::Float64, true));
        }
        Arc::new(ArrowSchema::new(fields))
    };

    LTSeqTable::from_batches(
        Arc::clone(&table.session),
        result_df,
        result_schema,
        Vec::new(),
    )
}

/// Build `CASE WHEN pivot_col IN (v1, v2, ...) THEN value_col END` for the
/// values of one output column.
///
/// Each value is a literal of the pivot column's own type, so the comparison
/// needs no coercion between kinds: a string column is never cast to a number
/// because a value looks numeric, and a `Float32` value is compared as the
/// `Float32` it is, not as the `f64` its text parses to (#205).
///
/// The list compares as DataFusion's `=` does: the simplifier turns a list
/// of one value into `=`, and `FloatListZeros` splits the zeros out of a
/// float list, whose items DataFusion otherwise compares bit for bit (#245).
/// So `-0.0` equals `0.0`, as the discovery's grouping on a plain float
/// column has it (a simple `CASE pivot_col WHEN v` compares them bit for bit
/// and misses the `-0.0` rows), and a NaN equals only a NaN of the same bits.
/// The `NaN` column therefore lists every NaN bit pattern the discovery
/// finds ([`pivot_column_name`]), however many: a list stays one node, where
/// an OR of equalities nests one level per value and DataFusion plans it
/// recursively, so 512 patterns overflowed the stack.
fn build_case_expr(pivot_col: &str, values: &[ScalarValue], value_col: &str) -> Expr {
    let list = values.iter().cloned().map(lit).collect();
    let matches = col(pivot_col).in_list(list, false);
    Expr::Case(Case::new(
        None,
        vec![(Box::new(matches), Box::new(col(value_col)))],
        None,
    ))
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

/// Add the non-null values of `arr`, the discovered distinct pivot values, to
/// `columns`, each under the name of the output column it becomes.
fn collect_pivot_values(arr: &ArrayRef, columns: &mut PivotColumns) -> Result<(), LtseqError> {
    match arr.data_type() {
        // A dictionary-encoded column pivots on its value type. Casting
        // decodes only the entries the keys reference, so an unused dictionary
        // value (a pandas category with no rows) does not become a column.
        DataType::Dictionary(_, value_type) => {
            let decoded = cast(arr, value_type).map_err(|e| {
                LtseqError::with_context("Failed to decode dictionary pivot column", e)
            })?;
            return collect_pivot_values(&decoded, columns);
        }
        // Arrow has three physical string layouts: a Parquet scan yields
        // Utf8View, `from_arrow` keeps whatever the caller passed.
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64 => {}
        _ => {
            return Err(LtseqError::Validation(
                "Pivot column must be string, int, or float type".into(),
            ));
        }
    }
    for i in 0..arr.len() {
        if arr.is_null(i) {
            continue;
        }
        let value = ScalarValue::try_from_array(arr, i)
            .map_err(|e| LtseqError::with_context("Failed to read pivot value", e))?;
        columns
            .entry(pivot_column_name(&value))
            .or_default()
            .push(value);
    }
    Ok(())
}

/// The name of the output column a pivot value becomes: a string as it is, a
/// number as Rust displays it (`5`, `2` for `2.0`, `0.1` for the `Float32`
/// nearest 0.1, `NaN`, `inf`).
///
/// Values with one name share a column, so every row lands in exactly one.
/// Every NaN is named `NaN`, and the discovery keeps NaNs with different bits
/// apart, so the `NaN` column matches each of them. Both zeros are named `0`:
/// the discovery folds `-0.0` into `0.0` on a plain float column but keeps
/// them apart on a dictionary-encoded one, and one name gives both layouts
/// the same single column.
fn pivot_column_name(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => s.clone(),
        ScalarValue::Int32(Some(v)) => v.to_string(),
        ScalarValue::Int64(Some(v)) => v.to_string(),
        ScalarValue::Float32(Some(v)) if *v == 0.0 => "0".into(),
        ScalarValue::Float32(Some(v)) => v.to_string(),
        ScalarValue::Float64(Some(v)) if *v == 0.0 => "0".into(),
        ScalarValue::Float64(Some(v)) => v.to_string(),
        // collect_pivot_values admits no other type and skips nulls.
        other => other.to_string(),
    }
}
