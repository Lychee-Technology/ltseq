//! Native Arrow shift bypass
//!
//! Computes shift(n) operations directly on Arrow arrays instead of going through
//! DataFusion's window function executor. This is significantly faster because:
//!
//! - No hash partitioning overhead (data is already physically sorted)
//! - No per-batch window function evaluation
//! - Simple array slice + null padding (O(n) with minimal allocation)
//!
//! # When this path is used
//!
//! The Arrow shift fast path is triggered when ALL derived columns in a `derive()`
//! call are pure shift operations (no rolling, diff, cum_sum, or mixed expressions).
//! The table must have sort_exprs that match the window's required ORDER BY.
//!
//! # Optimizations
//!
//! - Only the source columns and partition columns involved in shift operations are
//!   concatenated — not all columns. Original batches are preserved and extended.
//! - Partition boundaries are cached: when multiple shifts share the same
//!   `partition_by` column, the boundary scan runs only once.
//! - Boundary detection uses Arrow's vectorized `neq` kernel (SIMD-accelerated)
//!   instead of per-element type-dispatched comparison.

use datafusion::arrow::array::{new_null_array, Array, ArrayRef, BooleanArray, RecordBatch};
use datafusion::arrow::compute::{concat, concat_batches, kernels::cmp::neq};
use datafusion::arrow::datatypes::SchemaRef;
use std::collections::HashMap;
use std::sync::Arc;

/// Description of a single shift operation to perform
#[derive(Debug)]
pub struct ShiftOp {
    /// Name of the new derived column
    pub output_name: String,
    /// Name of the source column to shift
    pub source_column: String,
    /// Shift offset: positive = LAG (look back), negative = LEAD (look forward)
    pub offset: i64,
    /// Optional partition_by column name
    pub partition_by: Option<String>,
}

/// Compute multiple shift operations across a Vec of RecordBatches.
///
/// Only concatenates the columns actually needed for shift computation (source
/// columns and partition columns), NOT all columns. The shifted arrays are then
/// sliced back to match original batch row counts and appended to each batch.
///
/// Returns `Vec<RecordBatch>` preserving the original batch structure (useful for
/// multi-partition MemTable creation downstream).
pub fn compute_arrow_shifts_on_batches(
    batches: &[RecordBatch],
    shifts: &[ShiftOp],
) -> Result<Vec<RecordBatch>, String> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let schema = batches[0].schema();
    let batch_row_counts: Vec<usize> = batches.iter().map(|b| b.num_rows()).collect();
    let total_rows: usize = batch_row_counts.iter().sum();

    if total_rows == 0 {
        // Build empty schema with shift columns added
        let new_schema = build_extended_schema(&schema, shifts)?;
        return Ok(vec![RecordBatch::new_empty(Arc::new(new_schema))]);
    }

    // --- Collect unique columns that need concatenation ---
    // Only source columns and partition columns, not all columns.
    let mut needed_cols: HashMap<String, ArrayRef> = HashMap::new();

    for shift_op in shifts {
        // Concatenate source column if not already done
        if !needed_cols.contains_key(&shift_op.source_column) {
            let combined = concat_column(batches, &schema, &shift_op.source_column)?;
            needed_cols.insert(shift_op.source_column.clone(), combined);
        }
        // Concatenate partition column if not already done
        if let Some(ref pk) = shift_op.partition_by {
            if !needed_cols.contains_key(pk) {
                let combined = concat_column(batches, &schema, pk)?;
                needed_cols.insert(pk.clone(), combined);
            }
        }
    }

    // --- Cache partition boundaries ---
    // Multiple shifts with the same partition_by share one boundary scan.
    let mut boundary_cache: HashMap<String, Vec<usize>> = HashMap::new();

    for shift_op in shifts {
        if let Some(ref pk) = shift_op.partition_by {
            if !boundary_cache.contains_key(pk) {
                let partition_array = needed_cols
                    .get(pk)
                    .ok_or_else(|| format!("Partition column '{}' not found", pk))?;
                let boundaries = find_partition_boundaries(partition_array)?;
                boundary_cache.insert(pk.clone(), boundaries);
            }
        }
    }

    // --- Compute shifted arrays ---
    let mut shifted_arrays: Vec<(String, ArrayRef, datafusion::arrow::datatypes::DataType)> =
        Vec::with_capacity(shifts.len());

    for shift_op in shifts {
        let source_array = needed_cols
            .get(&shift_op.source_column)
            .ok_or_else(|| format!("Source column '{}' not found", shift_op.source_column))?;

        let shifted = if let Some(ref pk) = shift_op.partition_by {
            let boundaries = boundary_cache
                .get(pk)
                .ok_or_else(|| format!("Boundaries for '{}' not cached", pk))?;
            shift_partitioned(source_array, shift_op.offset, boundaries)?
        } else {
            shift_global(source_array, shift_op.offset, total_rows)?
        };

        shifted_arrays.push((
            shift_op.output_name.clone(),
            shifted,
            source_array.data_type().clone(),
        ));
    }

    // --- Slice shifted arrays back into per-batch segments and extend each batch ---
    let new_schema = Arc::new(build_extended_schema(&schema, shifts)?);
    let mut result_batches = Vec::with_capacity(batches.len());
    let mut offset = 0usize;

    for (batch_idx, batch) in batches.iter().enumerate() {
        let num_rows = batch_row_counts[batch_idx];

        // Start with existing columns (zero-copy Arc clone)
        let mut columns: Vec<ArrayRef> = (0..batch.num_columns())
            .map(|i| Arc::clone(batch.column(i)))
            .collect();

        // Append sliced shifted columns
        for (_name, shifted_array, _dt) in &shifted_arrays {
            let sliced = shifted_array.slice(offset, num_rows);
            columns.push(sliced);
        }

        let result_batch = RecordBatch::try_new(Arc::clone(&new_schema), columns)
            .map_err(|e| format!("Failed to create result batch {}: {}", batch_idx, e))?;
        result_batches.push(result_batch);

        offset += num_rows;
    }

    Ok(result_batches)
}

/// Compute multiple shift operations on a single combined RecordBatch.
///
/// Returns a new RecordBatch with all original columns plus the new shifted columns.
/// Used when data is already combined into a single batch.
pub fn compute_arrow_shifts(
    batch: &RecordBatch,
    shifts: &[ShiftOp],
) -> Result<RecordBatch, String> {
    // Delegate to batched version and unwrap the single result
    let results = compute_arrow_shifts_on_batches(&[batch.clone()], shifts)?;
    results
        .into_iter()
        .next()
        .ok_or_else(|| "Expected at least one result batch".to_string())
}

/// Build an extended schema: original fields + one nullable field per shift op.
fn build_extended_schema(
    schema: &SchemaRef,
    shifts: &[ShiftOp],
) -> Result<datafusion::arrow::datatypes::Schema, String> {
    let mut fields: Vec<Arc<datafusion::arrow::datatypes::Field>> =
        schema.fields().iter().map(Arc::clone).collect();

    for shift_op in shifts {
        let col_idx = schema.index_of(&shift_op.source_column).map_err(|e| {
            format!(
                "Column '{}' not found in schema: {}",
                shift_op.source_column, e
            )
        })?;
        let source_field = schema.field(col_idx);
        let field = datafusion::arrow::datatypes::Field::new(
            &shift_op.output_name,
            source_field.data_type().clone(),
            true, // shifted columns are always nullable (null padding)
        );
        fields.push(Arc::new(field));
    }

    Ok(datafusion::arrow::datatypes::Schema::new(fields))
}

/// Concatenate a single column across multiple batches.
fn concat_column(
    batches: &[RecordBatch],
    schema: &SchemaRef,
    col_name: &str,
) -> Result<ArrayRef, String> {
    let col_idx = schema
        .index_of(col_name)
        .map_err(|e| format!("Column '{}' not found: {}", col_name, e))?;

    let arrays: Vec<&dyn Array> = batches.iter().map(|b| b.column(col_idx).as_ref()).collect();

    if arrays.len() == 1 {
        return Ok(Arc::clone(batches[0].column(col_idx)));
    }

    concat(&arrays).map_err(|e| format!("Failed to concat column '{}': {}", col_name, e))
}

/// Shift an array globally (no partitioning).
///
/// For offset > 0 (LAG): [NULL * offset, data[0..len-offset]]
/// For offset < 0 (LEAD): [data[abs_offset..len], NULL * abs_offset]
fn shift_global(array: &ArrayRef, offset: i64, num_rows: usize) -> Result<ArrayRef, String> {
    let abs_offset = offset.unsigned_abs() as usize;

    if abs_offset >= num_rows {
        // Shift is larger than the array — all nulls
        return Ok(new_null_array(array.data_type(), num_rows));
    }

    if offset > 0 {
        // LAG: [NULL * offset, data[0..len-offset]]
        let null_prefix = new_null_array(array.data_type(), abs_offset);
        let sliced = array.slice(0, num_rows - abs_offset);
        concat(&[null_prefix.as_ref(), &*sliced]).map_err(|e| format!("concat failed: {}", e))
    } else {
        // LEAD: [data[abs_offset..len], NULL * abs_offset]
        let sliced = array.slice(abs_offset, num_rows - abs_offset);
        let null_suffix = new_null_array(array.data_type(), abs_offset);
        concat(&[&*sliced, null_suffix.as_ref()]).map_err(|e| format!("concat failed: {}", e))
    }
}

/// Shift an array within partitions.
///
/// `boundaries` contains the start index of each partition plus a final sentinel
/// equal to the total number of rows. E.g., for 3 partitions of sizes 4, 3, 5:
/// boundaries = [0, 4, 7, 12]
fn shift_partitioned(
    array: &ArrayRef,
    offset: i64,
    boundaries: &[usize],
) -> Result<ArrayRef, String> {
    if boundaries.len() < 2 {
        return Ok(new_null_array(array.data_type(), 0));
    }

    let total_rows = *boundaries.last().unwrap();
    let abs_offset = offset.unsigned_abs() as usize;

    // Build the shifted array partition by partition
    let mut chunks: Vec<ArrayRef> = Vec::with_capacity(boundaries.len() - 1);

    for window in boundaries.windows(2) {
        let start = window[0];
        let end = window[1];
        let partition_len = end - start;

        if abs_offset >= partition_len {
            // Entire partition is nulls
            chunks.push(new_null_array(array.data_type(), partition_len));
            continue;
        }

        let partition_slice = array.slice(start, partition_len);
        let shifted = shift_global(&partition_slice, offset, partition_len)?;
        chunks.push(shifted);
    }

    if chunks.is_empty() {
        return Ok(new_null_array(array.data_type(), total_rows));
    }

    // Concatenate all partition chunks into a single array
    let chunk_refs: Vec<&dyn Array> = chunks.iter().map(|a| a.as_ref()).collect();
    concat(&chunk_refs).map_err(|e| format!("concat partitions failed: {}", e))
}

/// Find partition boundaries in a sorted column using vectorized comparison.
///
/// Uses Arrow's `neq` kernel (SIMD-accelerated) to compare adjacent elements
/// in a single pass, replacing the previous per-element type-dispatched loop.
///
/// Returns indices where the value changes, including 0 and len.
/// Example: for values [A, A, B, B, B, C] → [0, 2, 5, 6]
fn find_partition_boundaries(col: &ArrayRef) -> Result<Vec<usize>, String> {
    let len = col.len();
    if len == 0 {
        return Ok(vec![0]);
    }
    if len == 1 {
        return Ok(vec![0, 1]);
    }

    // Vectorized comparison: compare col[0..n-1] with col[1..n]
    // Uses Arrow's neq kernel which supports all comparable types and leverages SIMD.
    let left = col.slice(0, len - 1);
    let right = col.slice(1, len - 1);

    let changes: BooleanArray =
        neq(&left, &right).map_err(|e| format!("neq kernel failed: {}", e))?;

    // Collect boundary indices where adjacent values differ
    let true_count = changes.true_count();
    let mut boundaries = Vec::with_capacity(true_count + 2);
    boundaries.push(0);

    for i in 0..changes.len() {
        // neq result is non-nullable (NULL != NULL is false in Arrow neq,
        // but both-null adjacent values won't occur in sorted partition columns)
        if changes.value(i) {
            boundaries.push(i + 1);
        }
    }

    boundaries.push(len);
    Ok(boundaries)
}

/// Try to extract pure shift operations from a derive() dict.
///
/// Returns `Some(Vec<ShiftOp>)` if ALL derived columns are simple shift calls
/// (e.g., `r.col.shift(n)` or `r.col.shift(n, partition_by="pk")`).
/// Returns `None` if any derived column is not a pure shift.
///
/// Also validates that the shift's partition_by column appears first in sort_exprs
/// (required for the Arrow path to produce correct results).
pub fn extract_pure_shifts(
    derived_cols: &[(String, crate::PyExpr)],
    sort_exprs: &[String],
) -> Option<Vec<ShiftOp>> {
    let mut shifts = Vec::with_capacity(derived_cols.len());

    for (col_name, py_expr) in derived_cols {
        match py_expr {
            crate::PyExpr::Call {
                func,
                on,
                args,
                kwargs,
            } if func == "shift" => {
                // Extract source column name
                let source_column = match &**on {
                    crate::PyExpr::Column(name) => name.clone(),
                    _ => return None, // shift on a non-column expression — can't use fast path
                };

                // Extract offset (default 1)
                let offset: i64 = if args.is_empty() {
                    1
                } else if let crate::PyExpr::Literal { value, .. } = &args[0] {
                    match value.parse::<i64>() {
                        Ok(v) => v,
                        Err(_) => return None,
                    }
                } else {
                    return None;
                };

                // Extract partition_by
                let partition_by = match kwargs.get("partition_by") {
                    Some(crate::PyExpr::Literal { value, dtype })
                        if dtype == "String" || dtype == "Utf8" =>
                    {
                        Some(value.clone())
                    }
                    Some(crate::PyExpr::Column(name)) => Some(name.clone()),
                    None => None,
                    _ => return None, // complex partition_by — can't use fast path
                };

                // Validate: if partition_by is used, it must be the first sort key
                if let Some(ref pk) = partition_by {
                    if sort_exprs.is_empty() || sort_exprs[0] != *pk {
                        return None;
                    }
                }

                // Reject if default value is specified (not supported in fast path)
                if kwargs.contains_key("default") {
                    return None;
                }

                shifts.push(ShiftOp {
                    output_name: col_name.clone(),
                    source_column,
                    offset,
                    partition_by,
                });
            }
            _ => return None, // Not a shift call — can't use fast path
        }
    }

    if shifts.is_empty() {
        None
    } else {
        Some(shifts)
    }
}

/// Combine multiple RecordBatches into one.
/// Uses Arrow's concat_batches for efficient concatenation.
pub fn combine_batches(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<RecordBatch, String> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }
    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }

    concat_batches(schema, batches).map_err(|e| format!("Failed to concat batches: {}", e))
}
