//! SQL-based derive operations for LTSeqTable
//!
//! Provides derive operations using raw SQL window expressions.

use crate::engine::RUNTIME;
use crate::error::LtseqError;
use crate::ops::helpers::register_temp_table;
use crate::LTSeqTable;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use pyo3::prelude::*;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// GPU segmented aggregation support
// ---------------------------------------------------------------------------

/// A parsed GPU-eligible aggregation from a SQL window expression.
#[cfg(feature = "gpu")]
#[derive(Debug)]
struct ParsedSegAgg {
    /// The aggregation function (count, sum, min, max, avg).
    func: crate::gpu::ordered_ops::SegAggFunc,
    /// Source column name (empty for COUNT(*)).
    source_column: String,
}

/// Try to parse a SQL window expression into a GPU-eligible segmented aggregation.
///
/// Recognises the fixed patterns emitted by `group_expr_to_sql()` in Python:
///   - `COUNT(*) OVER (PARTITION BY __group_id__)`
///   - `SUM("col") OVER (PARTITION BY __group_id__)`
///   - `MIN("col") OVER (PARTITION BY __group_id__)`
///   - `MAX("col") OVER (PARTITION BY __group_id__)`
///   - `AVG("col") OVER (PARTITION BY __group_id__)`
///
/// Returns `None` if the expression doesn't match any of these patterns.
#[cfg(feature = "gpu")]
fn try_parse_seg_agg(sql_expr: &str) -> Option<ParsedSegAgg> {
    use crate::gpu::ordered_ops::SegAggFunc;

    let trimmed = sql_expr.trim();
    // Normalise to uppercase for matching, preserving original for column extraction
    let upper = trimmed.to_uppercase();

    // Check that the expression ends with the expected OVER clause
    let over_clause = "OVER (PARTITION BY __GROUP_ID__)";
    if !upper.ends_with(over_clause) {
        return None;
    }

    // Extract the function part before OVER
    let func_part = trimmed[..trimmed.len() - over_clause.len()].trim();
    let func_upper = func_part.to_uppercase();

    // COUNT(*) — no column needed
    if func_upper == "COUNT(*)" {
        return Some(ParsedSegAgg {
            func: SegAggFunc::Count,
            source_column: String::new(),
        });
    }

    // Try to match FUNC("col") pattern
    // The function name is everything before the opening paren
    let open_paren = func_part.find('(')?;
    let func_name = func_part[..open_paren].trim();
    let func_name_upper = func_name.to_uppercase();

    let func = SegAggFunc::from_str(&func_name_upper)?;

    // Count should only match COUNT(*), not COUNT("col")
    if func == SegAggFunc::Count {
        return None;
    }

    // Extract column name from between parens: SUM("col") → col
    let close_paren = func_part.rfind(')')?;
    let inner = func_part[open_paren + 1..close_paren].trim();

    // Column name is quoted with double quotes: "col_name"
    let col_name = if inner.starts_with('"') && inner.ends_with('"') && inner.len() >= 2 {
        inner[1..inner.len() - 1].to_string()
    } else {
        // Unquoted column name (e.g. col_name)
        inner.to_string()
    };

    Some(ParsedSegAgg {
        func,
        source_column: col_name,
    })
}

/// Attempt GPU-accelerated segmented aggregation on the collected batches.
///
/// Returns `Some(result_batches)` if GPU execution succeeds, `None` if the
/// expressions are not GPU-eligible or the GPU path fails (caller falls back
/// to SQL).
#[cfg(feature = "gpu")]
fn try_gpu_segmented_derive(
    current_batches: &[datafusion::arrow::record_batch::RecordBatch],
    batch_schema: &Arc<ArrowSchema>,
    derive_exprs: &std::collections::HashMap<String, String>,
) -> Option<Vec<datafusion::arrow::record_batch::RecordBatch>> {
    use crate::gpu::ordered_ops::{gpu_segmented_aggregate, SegAggRequest};
    use datafusion::arrow::array::Array;
    use datafusion::arrow::compute;
    use datafusion::arrow::record_batch::RecordBatch;

    // 1. Check GPU availability + row count threshold
    if !crate::gpu::is_gpu_available() {
        return None;
    }

    // Runtime kill-switch
    if std::env::var("LTSEQ_DISABLE_GPU").is_ok() {
        return None;
    }

    // 2. Parse ALL derive expressions — bail if any is not GPU-eligible
    let parsed: Vec<(String, ParsedSegAgg)> = {
        let mut v = Vec::with_capacity(derive_exprs.len());
        for (col_name, sql_expr) in derive_exprs {
            match try_parse_seg_agg(sql_expr) {
                Some(p) => v.push((col_name.clone(), p)),
                None => return None, // not GPU-eligible
            }
        }
        v
    };

    if parsed.is_empty() {
        return None;
    }

    // 3. Concat all batches into a single RecordBatch
    let concat_batch = if current_batches.len() == 1 {
        current_batches[0].clone()
    } else if current_batches.is_empty() {
        return None;
    } else {
        compute::concat_batches(&current_batches[0].schema(), current_batches).ok()?
    };

    let total_rows = concat_batch.num_rows();
    if total_rows < crate::gpu::GPU_MIN_ROWS_THRESHOLD {
        return None;
    }

    // 4. Find __group_id__ column and convert to UInt32Array
    let group_id_idx = concat_batch
        .schema()
        .index_of("__group_id__")
        .ok()?;
    let group_id_col = concat_batch.column(group_id_idx);
    let group_id_u32 = cast_to_uint32(group_id_col)?;

    // Compute num_groups = max(group_id)
    let num_groups = compute::max(&group_id_u32)? as usize;

    // 5. Build SegAggRequest list
    let requests: Vec<SegAggRequest> = {
        let mut reqs = Vec::with_capacity(parsed.len());
        for (col_name, p) in &parsed {
            let (column_index, data_type) = if p.source_column.is_empty() {
                // COUNT(*) — column_index/data_type ignored
                (0, datafusion::arrow::datatypes::DataType::Int64)
            } else {
                let idx = concat_batch
                    .schema()
                    .index_of(&p.source_column)
                    .ok()?;
                let dt = concat_batch.schema().field(idx).data_type().clone();
                // Only i64 and f64 are supported on GPU
                match dt {
                    datafusion::arrow::datatypes::DataType::Int64
                    | datafusion::arrow::datatypes::DataType::Float64 => {}
                    _ => return None, // unsupported type
                }
                (idx, dt)
            };

            reqs.push(SegAggRequest {
                func: p.func,
                column_index,
                data_type,
                output_name: col_name.clone(),
            });
        }
        reqs
    };

    // 6. Run GPU segmented aggregation
    let gpu_results = match gpu_segmented_aggregate(&group_id_u32, &concat_batch, num_groups, &requests) {
        Ok(r) => r,
        Err(e) => {
            if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
                eprintln!(
                    "[GPU] derive_window_sql: segmented aggregate failed, falling back to SQL: {}",
                    e
                );
            }
            return None;
        }
    };

    if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
        eprintln!(
            "[GPU] derive_window_sql: GPU segmented aggregate succeeded ({} rows, {} aggs)",
            total_rows,
            gpu_results.len()
        );
    }

    // 7. Build result RecordBatch: original columns (minus __group_id__, __rn__)
    //    + GPU-computed aggregate columns
    let mut result_columns: Vec<datafusion::arrow::array::ArrayRef> = Vec::new();
    let mut result_fields: Vec<datafusion::arrow::datatypes::Field> = Vec::new();

    for (i, field) in batch_schema.fields().iter().enumerate() {
        let col_name = field.name();
        if col_name != "__group_id__" && col_name != "__rn__" {
            result_columns.push(concat_batch.column(i).clone());
            result_fields.push((**field).clone());
        }
    }

    // Append GPU results in the order they were requested
    for (output_name, arr) in &gpu_results {
        let dt = arr.data_type().clone();
        result_fields.push(datafusion::arrow::datatypes::Field::new(
            output_name,
            dt,
            true,
        ));
        result_columns.push(arr.clone());
    }

    let result_schema = Arc::new(ArrowSchema::new(result_fields));
    let result_batch = RecordBatch::try_new(result_schema, result_columns).ok()?;

    Some(vec![result_batch])
}

/// Cast an Arrow array to UInt32Array.
///
/// The `__group_id__` column may be stored as Int64 (from the CPU `group_id_impl`
/// path). This function handles the common cases.
#[cfg(feature = "gpu")]
fn cast_to_uint32(
    array: &datafusion::arrow::array::ArrayRef,
) -> Option<UInt32Array> {
    use datafusion::arrow::array::{Array, Int64Array, Int32Array, UInt32Array};
    use datafusion::arrow::datatypes::DataType;

    match array.data_type() {
        DataType::UInt32 => {
            array.as_any().downcast_ref::<UInt32Array>().cloned()
        }
        DataType::Int64 => {
            let i64_arr = array.as_any().downcast_ref::<Int64Array>()?;
            let values: Vec<u32> = i64_arr.iter().map(|v| v.map(|x| x as u32).unwrap_or(0)).collect();
            Some(UInt32Array::from(values))
        }
        DataType::Int32 => {
            let i32_arr = array.as_any().downcast_ref::<Int32Array>()?;
            let values: Vec<u32> = i32_arr.iter().map(|v| v.map(|x| x as u32).unwrap_or(0)).collect();
            Some(UInt32Array::from(values))
        }
        _ => None,
    }
}

#[cfg(feature = "gpu")]
use datafusion::arrow::array::UInt32Array;

/// Derive columns using raw SQL window expressions
///
/// Executes a SELECT query with the provided SQL expressions,
/// typically containing window functions like FIRST_VALUE, LAST_VALUE, COUNT, etc.
///
/// # Arguments
///
/// * `table` - The source table (must already have __group_id__ and __rn__ columns)
/// * `derive_exprs` - HashMap mapping column name -> SQL expression string
///
/// # Returns
///
/// A new LTSeqTable with the derived columns added (and __group_id__, __rn__ removed)
pub fn derive_window_sql_impl(
    table: &LTSeqTable,
    derive_exprs: std::collections::HashMap<String, String>,
) -> PyResult<LTSeqTable> {
    let (df, schema) = table.require_df_and_schema()?;

    let current_batches = RUNTIME
        .block_on(async {
            (**df)
                .clone()
                .collect()
                .await
                .map_err(|e| format!("Failed to collect data: {}", e))
        })
        .map_err(|e| LtseqError::Runtime(e))?;

    let batch_schema = if let Some(first_batch) = current_batches.first() {
        first_batch.schema()
    } else {
        Arc::new((**schema).clone())
    };

    // ── GPU fast path ────────────────────────────────────────────────────
    // If all derive expressions are simple aggregations over __group_id__
    // (count, sum, min, max, avg) and the dataset is large enough, run them
    // on the GPU via segmented aggregation — skipping DataFusion SQL entirely.
    #[cfg(feature = "gpu")]
    {
        if let Some(gpu_batches) =
            try_gpu_segmented_derive(&current_batches, &batch_schema, &derive_exprs)
        {
            return LTSeqTable::from_batches(
                Arc::clone(&table.session),
                gpu_batches,
                Vec::new(),
                table.source_parquet_path.clone(),
            );
        }
    }

    // ── CPU / DataFusion SQL path (fallback) ─────────────────────────────

    let guard = register_temp_table(&table.session, &batch_schema, current_batches, "derive")?;

    // Build SELECT clause: all existing columns + derived expressions
    let mut select_parts: Vec<String> = Vec::new();

    // Add all existing columns except __group_id__ and __rn__
    for field in batch_schema.fields() {
        let col_name = field.name();
        if col_name != "__group_id__" && col_name != "__rn__" {
            select_parts.push(format!("\"{}\"", col_name));
        }
    }

    // Add derived columns
    for (col_name, sql_expr) in &derive_exprs {
        select_parts.push(format!("{} AS \"{}\"", sql_expr, col_name));
    }

    let sql_query = format!(
        "SELECT {} FROM \"{}\"",
        select_parts.join(", "),
        guard.name()
    );

    let result_batches = RUNTIME
        .block_on(async {
            let result_df = table.session.sql(&sql_query).await.map_err(|e| {
                format!(
                    "Failed to execute derive query: {} -- SQL: {}",
                    e, sql_query
                )
            })?;

            result_df
                .collect()
                .await
                .map_err(|e| format!("Failed to collect derive results: {}", e))
        })
        .map_err(|e| LtseqError::Runtime(e))?;

    drop(guard);

    if result_batches.is_empty() {
        // Build schema for empty result
        let mut result_fields: Vec<datafusion::arrow::datatypes::Field> = Vec::new();
        for field in batch_schema.fields() {
            let col_name = field.name();
            if col_name != "__group_id__" && col_name != "__rn__" {
                result_fields.push((**field).clone());
            }
        }
        // Add derived columns as nullable Float64
        for col_name in derive_exprs.keys() {
            result_fields.push(datafusion::arrow::datatypes::Field::new(
                col_name,
                datafusion::arrow::datatypes::DataType::Float64,
                true,
            ));
        }
        let empty_schema = Arc::new(ArrowSchema::new(result_fields));
        return Ok(LTSeqTable::empty(
            Arc::clone(&table.session),
            Some(empty_schema),
            Vec::new(),
            table.source_parquet_path.clone(),
        ));
    }

    LTSeqTable::from_batches(
        Arc::clone(&table.session),
        result_batches,
        Vec::new(),
        table.source_parquet_path.clone(),
    )
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
#[cfg(feature = "gpu")]
mod tests {
    use super::*;

    #[test]
    fn test_parse_count_star() {
        let parsed = try_parse_seg_agg("COUNT(*) OVER (PARTITION BY __group_id__)");
        assert!(parsed.is_some());
        let p = parsed.unwrap();
        assert_eq!(p.func, crate::gpu::ordered_ops::SegAggFunc::Count);
        assert!(p.source_column.is_empty());
    }

    #[test]
    fn test_parse_sum_quoted() {
        let parsed = try_parse_seg_agg("SUM(\"price\") OVER (PARTITION BY __group_id__)");
        assert!(parsed.is_some());
        let p = parsed.unwrap();
        assert_eq!(p.func, crate::gpu::ordered_ops::SegAggFunc::Sum);
        assert_eq!(p.source_column, "price");
    }

    #[test]
    fn test_parse_min_quoted() {
        let parsed = try_parse_seg_agg("MIN(\"value\") OVER (PARTITION BY __group_id__)");
        assert!(parsed.is_some());
        let p = parsed.unwrap();
        assert_eq!(p.func, crate::gpu::ordered_ops::SegAggFunc::Min);
        assert_eq!(p.source_column, "value");
    }

    #[test]
    fn test_parse_max_quoted() {
        let parsed = try_parse_seg_agg("MAX(\"value\") OVER (PARTITION BY __group_id__)");
        assert!(parsed.is_some());
        let p = parsed.unwrap();
        assert_eq!(p.func, crate::gpu::ordered_ops::SegAggFunc::Max);
        assert_eq!(p.source_column, "value");
    }

    #[test]
    fn test_parse_avg_quoted() {
        let parsed = try_parse_seg_agg("AVG(\"temp\") OVER (PARTITION BY __group_id__)");
        assert!(parsed.is_some());
        let p = parsed.unwrap();
        assert_eq!(p.func, crate::gpu::ordered_ops::SegAggFunc::Avg);
        assert_eq!(p.source_column, "temp");
    }

    #[test]
    fn test_parse_lowercase() {
        // group_expr_to_sql always emits uppercase, but test robustness
        let parsed = try_parse_seg_agg("sum(\"price\") OVER (PARTITION BY __group_id__)");
        assert!(parsed.is_some());
        let p = parsed.unwrap();
        assert_eq!(p.func, crate::gpu::ordered_ops::SegAggFunc::Sum);
        assert_eq!(p.source_column, "price");
    }

    #[test]
    fn test_parse_first_value_not_eligible() {
        // FIRST_VALUE is not a segmented aggregate
        let parsed = try_parse_seg_agg(
            "FIRST_VALUE(\"col\") OVER (PARTITION BY __group_id__ ORDER BY __rn__)",
        );
        assert!(parsed.is_none());
    }

    #[test]
    fn test_parse_last_value_not_eligible() {
        let parsed = try_parse_seg_agg(
            "LAST_VALUE(\"col\") OVER (PARTITION BY __group_id__ ORDER BY __rn__ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
        );
        assert!(parsed.is_none());
    }

    #[test]
    fn test_parse_binop_not_eligible() {
        // Compound expressions like (COUNT(*) + 1) are not eligible
        let parsed = try_parse_seg_agg("(COUNT(*) OVER (PARTITION BY __group_id__) + 1)");
        assert!(parsed.is_none());
    }

    #[test]
    fn test_parse_wrong_partition() {
        // Different partition key
        let parsed = try_parse_seg_agg("SUM(\"price\") OVER (PARTITION BY other_col)");
        assert!(parsed.is_none());
    }

    #[test]
    fn test_parse_plain_sql_not_eligible() {
        let parsed = try_parse_seg_agg("\"col\" + 1");
        assert!(parsed.is_none());
    }

    #[test]
    fn test_parse_count_col_not_eligible() {
        // COUNT("col") is different from COUNT(*) — not supported yet
        let parsed = try_parse_seg_agg("COUNT(\"col\") OVER (PARTITION BY __group_id__)");
        assert!(parsed.is_none());
    }
}
