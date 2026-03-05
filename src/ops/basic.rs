//! Basic table operations: search_first
//!
//! This module contains helper functions for basic table operations.
//! The actual methods are implemented in the #[pymethods] impl block in lib.rs
//! due to PyO3 constraints (only one #[pymethods] impl block per struct is allowed).
//!
//! # Architecture
//!
//! Due to PyO3 constraints, all actual operation implementations are extracted as helper
//! functions here. The #[pymethods] impl block in lib.rs simply delegates to these
//! helpers via one-line stubs.
//!
//! # Operations Provided
//!
//! - **search_first_impl**: Find the first row matching a predicate (optimized filter + limit 1)
//!   With binary search fast path when data is sorted on the predicate column.

use crate::error::LtseqError;
use crate::transpiler::pyexpr_to_datafusion;
use crate::types::dict_to_py_expr;
use crate::LTSeqTable;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

use crate::engine::RUNTIME;

/// Helper function to find the first row matching a predicate
///
/// This is an optimized version of filter() that returns only the first matching row.
/// Much faster than filtering and then taking the first row, especially for large tables.
///
/// Optimization levels:
/// 1. **Binary search** (O(log N)): When data is sorted on the predicate column
///    and the predicate is `Column {cmp} Literal`. Uses GPU when available,
///    falls back to CPU binary search.
/// 2. **Filter + Limit 1** (O(N)): Default DataFusion path for unsorted data
///    or complex predicates.
///
/// Args:
///     table: Reference to LTSeqTable
///     expr_dict: Serialized filter expression
pub fn search_first_impl(
    table: &LTSeqTable,
    expr_dict: &Bound<'_, PyDict>,
) -> PyResult<LTSeqTable> {
    let (df, schema) = table.require_df_and_schema()?;

    // Deserialize the expression
    let py_expr = dict_to_py_expr(expr_dict)?;

    // ── Binary search fast path ──────────────────────────────────────
    // When data is sorted on the predicate column and the predicate is
    // a simple Column {cmp} Literal, use O(log N) binary search.
    #[cfg(feature = "gpu")]
    {
        use crate::gpu::ordered_ops::{
            cpu_binary_search_first, try_match_gpu_search_mode,
        };
        use datafusion::arrow::record_batch::RecordBatch;

        if !table.sort_exprs.is_empty() {
            if let Some(search_mode) =
                try_match_gpu_search_mode(&py_expr, schema, &table.sort_exprs)
            {
                // Collect data into RecordBatch for binary search
                let batch_result: Result<Option<RecordBatch>, String> = RUNTIME.block_on(async {
                    use datafusion::arrow::compute::concat_batches;
                    use futures_util::StreamExt;

                    let plan = (**df)
                        .clone()
                        .create_physical_plan()
                        .await
                        .map_err(|e| format!("Failed to create physical plan: {}", e))?;

                    let ctx = datafusion::execution::TaskContext::default();
                    let mut stream = plan
                        .execute(0, Arc::new(ctx))
                        .map_err(|e| format!("Failed to execute plan: {}", e))?;

                    let mut batches = Vec::new();
                    while let Some(batch) = stream.next().await {
                        batches.push(
                            batch.map_err(|e| format!("Batch error: {}", e))?,
                        );
                    }

                    if batches.is_empty() {
                        return Ok(None);
                    }

                    let schema_ref = batches[0].schema();
                    let combined = concat_batches(&schema_ref, &batches)
                        .map_err(|e| format!("Concat error: {}", e))?;
                    Ok(Some(combined))
                });

                match batch_result {
                    Ok(Some(batch)) if batch.num_rows() > 0 => {
                        // Try GPU first, then CPU binary search
                        let search_result = if crate::gpu::is_gpu_available()
                            && batch.num_rows() >= crate::gpu::GPU_MIN_ROWS_THRESHOLD
                        {
                            crate::gpu::ordered_ops::gpu_search_first(
                                &batch,
                                &search_mode,
                            )
                            .unwrap_or_else(|_| {
                                cpu_binary_search_first(&batch, &search_mode)
                                    .ok()
                                    .flatten()
                            })
                        } else {
                            cpu_binary_search_first(&batch, &search_mode)
                                .map_err(|e| LtseqError::Runtime(e))?
                        };

                        match search_result {
                            Some(idx) => {
                                // Extract single row at idx
                                let columns: Vec<
                                    datafusion::arrow::array::ArrayRef,
                                > = batch
                                    .columns()
                                    .iter()
                                    .map(|col| col.slice(idx, 1))
                                    .collect();
                                let result_batch =
                                    datafusion::arrow::record_batch::RecordBatch::try_new(
                                        batch.schema(),
                                        columns,
                                    )
                                    .map_err(|e| {
                                        LtseqError::Runtime(format!(
                                            "Failed to create result batch: {}",
                                            e
                                        ))
                                    })?;

                                return LTSeqTable::from_batches(
                                    Arc::clone(&table.session),
                                    vec![result_batch],
                                    table.sort_exprs.clone(),
                                    table.source_parquet_path.clone(),
                                );
                            }
                            None => {
                                // No match found — return empty table
                                return LTSeqTable::from_batches(
                                    Arc::clone(&table.session),
                                    vec![],
                                    table.sort_exprs.clone(),
                                    table.source_parquet_path.clone(),
                                );
                            }
                        }
                    }
                    _ => {
                        // Fall through to DataFusion filter+limit
                    }
                }
            }
        }
    }

    // ── Default path: DataFusion filter + limit 1 ────────────────────

    // Transpile to DataFusion Expr
    let df_expr = pyexpr_to_datafusion(py_expr, schema)
        .map_err(|e| LtseqError::Validation(e))?;

    // Apply filter and limit to first result
    let result_df = RUNTIME
        .block_on(async {
            (**df)
                .clone()
                .filter(df_expr)
                .map_err(|e| format!("Filter failed: {}", e))?
                .limit(0, Some(1))
                .map_err(|e| format!("Limit failed: {}", e))
        })
        .map_err(|e| LtseqError::Runtime(e))?;

    // Return new LTSeqTable with recomputed schema
    Ok(LTSeqTable::from_df(
        Arc::clone(&table.session),
        result_df,
        table.sort_exprs.clone(),
        table.source_parquet_path.clone(),
    ))
}
