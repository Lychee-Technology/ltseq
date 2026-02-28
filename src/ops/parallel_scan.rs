//! Direct Parquet sequence engine — bypasses DataFusion for sequence operations.
//!
//! Two strategies for pre-sorted Parquet:
//!
//! 1. **Sequential streaming** (R2 group_ordered): Read Parquet sequentially with
//!    a single file handle, compute boundary flags with `streaming_fuse_eval`,
//!    and build group metadata in a single pass. Bypasses DataFusion completely.
//!
//! 2. **Parallel partitioned** (R3 pattern matching): Read row groups in parallel,
//!    split by partition key, run pattern matching per-partition in parallel.

use crate::ops::linear_scan::{
    build_metadata_table, extract_referenced_columns, streaming_fuse_eval, StreamState,
};
use crate::ops::pattern_match::{eval_predicate, extract_starts_with_prefix};
use crate::types::PyExpr;
use crate::LTSeqTable;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray,
    StringViewArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::compute::concat_batches;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use pyo3::prelude::*;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::sync::Arc;

// ============================================================================
// Strategy 1: Sequential Streaming for R2 (group_ordered)
// ============================================================================

/// Direct Parquet streaming group_ordered — bypasses DataFusion completely.
///
/// Reads the Parquet file sequentially with a single file handle, computes
/// boundary flags with `streaming_fuse_eval`, and builds group metadata
/// in a single pass.
///
/// Falls back with PARALLEL_FALLBACK if the expression can't be fuse-evaluated.
pub fn direct_streaming_group_ordered(
    table: &LTSeqTable,
    predicate: &PyExpr,
    parquet_path: &str,
) -> PyResult<LTSeqTable> {
    // Step 1: Extract columns needed by the predicate
    let mut needed_cols: HashSet<String> = HashSet::new();
    extract_referenced_columns(predicate, &mut needed_cols);

    // Step 2: Open Parquet and build projection
    let file = File::open(parquet_path).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to open Parquet: {}", e))
    })?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to read Parquet metadata: {}",
            e
        ))
    })?;

    let parquet_schema = builder.schema().clone();
    let total_rows_estimate = builder.metadata().file_metadata().num_rows() as usize;
    let parquet_metadata = builder.metadata().clone();

    // Build projection mask — only read columns referenced in predicate
    let proj_indices: Vec<usize> = parquet_schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| needed_cols.contains(f.name()))
        .map(|(i, _)| i)
        .collect();

    let projection_mask = ProjectionMask::roots(
        parquet_metadata.file_metadata().schema_descr(),
        proj_indices,
    );

    // Step 3: Sequential streaming read with boundary detection
    let reader = builder
        .with_projection(projection_mask)
        .with_batch_size(65536)
        .build()
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to build Parquet reader: {}",
                e
            ))
        })?;

    let mut state = StreamState::new();
    let mut group_ids: Vec<i64> = Vec::with_capacity(total_rows_estimate);
    let mut name_to_idx: HashMap<String, usize> = HashMap::new();
    let mut idx_built = false;

    for batch_result in reader {
        let batch = batch_result.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to read Parquet batch: {}",
                e
            ))
        })?;

        let n = batch.num_rows();
        if n == 0 {
            continue;
        }

        // Build name_to_idx from first batch
        if !idx_built {
            for (i, field) in batch.schema().fields().iter().enumerate() {
                name_to_idx.insert(field.name().clone(), i);
            }
            idx_built = true;
        }

        // Compute boundaries within this batch
        let mut result = vec![false; n];
        if !streaming_fuse_eval(predicate, &batch, &name_to_idx, &state, &mut result) {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "PARALLEL_FALLBACK",
            ));
        }

        // Accumulate group IDs from boundaries
        for i in 0..n {
            if result[i] {
                state.current_gid += 1;
            }
            group_ids.push(state.current_gid);
        }

        // Save last row's column values for cross-batch boundary detection
        state.save_last_row(&batch, &name_to_idx);
    }

    let total_rows = group_ids.len();
    if total_rows == 0 {
        return Ok(LTSeqTable::empty(
            Arc::clone(&table.session),
            table.schema.as_ref().map(Arc::clone),
            Vec::new(),
            table.source_parquet_path.clone(),
        ));
    }

    // Step 4: Compute group_count and rn (two passes over group_ids)
    let total_groups = state.current_gid as usize;
    let mut group_counts: Vec<i64> = vec![0; total_groups + 1];
    for &gid in &group_ids {
        group_counts[gid as usize] += 1;
    }

    let mut rn_values: Vec<i64> = Vec::with_capacity(total_rows);
    let mut count_values: Vec<i64> = Vec::with_capacity(total_rows);
    let mut rn_counters: Vec<i64> = vec![0; total_groups + 1];

    for &gid in &group_ids {
        rn_counters[gid as usize] += 1;
        rn_values.push(rn_counters[gid as usize]);
        count_values.push(group_counts[gid as usize]);
    }

    build_metadata_table(group_ids, count_values, rn_values, table)
}

/// Fast path: count the number of groups without building metadata arrays.
///
/// This is used when the consumer only needs `group_ordered().first().count()`,
/// which is equivalent to counting the number of session boundaries.
/// Avoids allocating 3 x N arrays (group_ids, rn, count) for 100M+ rows.
pub fn direct_streaming_group_count(
    _table: &LTSeqTable,
    predicate: &PyExpr,
    parquet_path: &str,
) -> PyResult<usize> {
    // Step 1: Extract columns needed by the predicate
    let mut needed_cols: HashSet<String> = HashSet::new();
    extract_referenced_columns(predicate, &mut needed_cols);

    // Step 2: Open Parquet and build projection
    let file = File::open(parquet_path).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to open Parquet: {}", e))
    })?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to read Parquet metadata: {}",
            e
        ))
    })?;

    let parquet_schema = builder.schema().clone();
    let parquet_metadata = builder.metadata().clone();

    // Build projection mask — only read columns referenced in predicate
    let proj_indices: Vec<usize> = parquet_schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| needed_cols.contains(f.name()))
        .map(|(i, _)| i)
        .collect();

    let projection_mask = ProjectionMask::roots(
        parquet_metadata.file_metadata().schema_descr(),
        proj_indices,
    );

    // Step 3: Sequential streaming read — count boundaries only
    let reader = builder
        .with_projection(projection_mask)
        .with_batch_size(65536)
        .build()
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to build Parquet reader: {}",
                e
            ))
        })?;

    let mut state = StreamState::new();
    let mut name_to_idx: HashMap<String, usize> = HashMap::new();
    let mut idx_built = false;

    for batch_result in reader {
        let batch = batch_result.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to read Parquet batch: {}",
                e
            ))
        })?;

        let n = batch.num_rows();
        if n == 0 {
            continue;
        }

        // Build name_to_idx from first batch
        if !idx_built {
            for (i, field) in batch.schema().fields().iter().enumerate() {
                name_to_idx.insert(field.name().clone(), i);
            }
            idx_built = true;
        }

        // Compute boundaries within this batch
        let mut result = vec![false; n];
        if !streaming_fuse_eval(predicate, &batch, &name_to_idx, &state, &mut result) {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "PARALLEL_FALLBACK",
            ));
        }

        // Just count boundaries — no group_ids array needed
        for i in 0..n {
            if result[i] {
                state.current_gid += 1;
            }
        }

        // Save last row's column values for cross-batch boundary detection
        state.save_last_row(&batch, &name_to_idx);
    }

    Ok(state.current_gid as usize)
}

// ============================================================================
// Strategy 2: Parallel Partitioned Pattern Matching for R3
// ============================================================================

/// Boundary info extracted from a single row group for cross-RG matching.
/// Contains only the tail of the last partition and head of the first partition.
struct RgBoundaryInfo {
    /// Key of the first partition in this RG
    first_key: i64,
    /// First few rows of the first partition (up to num_steps-1 rows)
    first_head: RecordBatch,
    /// Key of the last partition in this RG
    last_key: i64,
    /// Last few rows of the last partition (up to num_steps-1 rows)
    last_tail: RecordBatch,
}

/// Result from fused read+match of a single row group.
struct RgMatchResult {
    /// Number of intra-RG pattern matches found
    count: usize,
    /// Boundary info for cross-RG matching (None if RG was empty)
    boundary: Option<RgBoundaryInfo>,
}

/// Parallel pattern match count for pre-sorted Parquet files.
///
/// Strategy: fused read+match per row group. Each rayon task reads one RG,
/// pattern-matches it, extracts minimal boundary data, then drops the RG data.
/// This eliminates the 1.4s deallocation overhead from storing all RG data.
pub fn parallel_pattern_match_count(
    _table: &LTSeqTable,
    step_predicates: &[PyExpr],
    partition_col: &str,
    parquet_path: &str,
) -> PyResult<usize> {
    let num_steps = step_predicates.len();

    // Step 1: Extract columns needed by all predicates
    let mut needed_cols: HashSet<String> = HashSet::new();
    for expr in step_predicates {
        extract_referenced_columns(expr, &mut needed_cols);
    }
    needed_cols.insert(partition_col.to_string());

    // Step 2: Open file and get metadata
    let file = File::open(parquet_path).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to open Parquet: {}", e))
    })?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to read Parquet metadata: {}",
            e
        ))
    })?;

    let parquet_schema = builder.schema().clone();
    let num_row_groups = builder.metadata().num_row_groups();
    let parquet_metadata = builder.metadata().clone();

    // Build projection
    let mut all_needed = needed_cols;
    all_needed.insert(partition_col.to_string());

    let proj_indices: Vec<usize> = parquet_schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| all_needed.contains(f.name()))
        .map(|(i, _)| i)
        .collect();

    let projected_schema = Arc::new(parquet_schema.project(&proj_indices).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to project schema: {}",
            e
        ))
    })?);

    let projection_mask = ProjectionMask::roots(
        parquet_metadata.file_metadata().schema_descr(),
        proj_indices,
    );

    let part_col_idx = projected_schema
        .fields()
        .iter()
        .position(|f| f.name() == partition_col)
        .ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Partition column '{}' not found",
                partition_col
            ))
        })?;

    // Build name_to_idx from projected schema
    let name_to_idx: HashMap<String, usize> = projected_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().clone(), i))
        .collect();

    // Step 3+4: Fused read + match per RG in parallel.
    // Each task reads one RG, pattern-matches it, extracts boundary data, drops RG data.
    // This eliminates the 1.4s dealloc overhead from holding all RG data in memory.
    let path_str = parquet_path.to_string();

    let rg_results: Vec<RgMatchResult> = (0..num_row_groups)
        .into_par_iter()
        .map(|rg_idx| {
            read_match_and_extract_boundary(
                &path_str,
                rg_idx,
                &projection_mask,
                part_col_idx,
                step_predicates,
                num_steps,
                &name_to_idx,
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Parallel fused read+match failed: {}",
                e
            ))
        })?;

    // Step 5: Sum intra-RG counts
    let intra_rg_count: usize = rg_results.iter().map(|r| r.count).sum();

    // Step 6: Handle cross-RG boundary patterns using the small boundary data.
    let boundary_count = count_cross_rg_boundary_patterns_from_info(
        &rg_results,
        step_predicates,
        num_steps,
        &name_to_idx,
    );

    let total_count = intra_rg_count + boundary_count;

    Ok(total_count)
}

// ============================================================================
// Helper: Partition data structures and utilities
// ============================================================================

/// Fused read + match + extract boundary for a single row group.
///
/// Reads the RG, splits by partition key, pattern-matches within the RG,
/// extracts minimal boundary data (first/last few rows), then drops the full
/// RG data. This means each RG's ~20MB of data is freed immediately after
/// processing, eliminating the 1.4s dealloc overhead from holding all 814 RGs
/// (~2.7GB) in memory simultaneously.
fn read_match_and_extract_boundary(
    parquet_path: &str,
    row_group_idx: usize,
    projection_mask: &ProjectionMask,
    partition_col_idx: usize,
    step_predicates: &[PyExpr],
    num_steps: usize,
    name_to_idx: &HashMap<String, usize>,
) -> Result<RgMatchResult, String> {
    let file = File::open(parquet_path)
        .map_err(|e| format!("Failed to open Parquet for RG {}: {}", row_group_idx, e))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("Failed to build reader for RG {}: {}", row_group_idx, e))?;

    let reader = builder
        .with_row_groups(vec![row_group_idx])
        .with_projection(projection_mask.clone())
        .build()
        .map_err(|e| format!("Failed to build reader for RG {}: {}", row_group_idx, e))?;

    let mut slices: Vec<(i64, RecordBatch)> = Vec::new();

    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| format!("Failed to read batch from RG {}: {}", row_group_idx, e))?;

        if batch.num_rows() == 0 {
            continue;
        }

        let part_col = batch.column(partition_col_idx);
        let part_values = coerce_partition_to_i64(part_col).ok_or_else(|| {
            format!(
                "Partition column has unsupported type {:?}",
                part_col.data_type()
            )
        })?;

        // Split batch at partition key changes
        let mut start = 0;
        let mut current_key = part_values.value(0);

        for i in 1..batch.num_rows() {
            let key = part_values.value(i);
            if key != current_key {
                let slice = batch.slice(start, i - start);
                slices.push((current_key, slice));
                start = i;
                current_key = key;
            }
        }
        let slice = batch.slice(start, batch.num_rows() - start);
        slices.push((current_key, slice));
    }

    if slices.is_empty() {
        return Ok(RgMatchResult {
            count: 0,
            boundary: None,
        });
    }

    // Pattern match within this RG
    let count = count_patterns_in_rg_slices(&slices, step_predicates, num_steps, name_to_idx);

    // Extract boundary info for cross-RG matching
    let (first_key, first_batch) = &slices[0];
    let first_head_rows = first_batch.num_rows().min(num_steps - 1);
    let first_head = first_batch.slice(0, first_head_rows);

    let (last_key, last_batch) = slices.last().unwrap();
    let last_rows = last_batch.num_rows();
    let last_tail_start = last_rows.saturating_sub(num_steps - 1);
    let last_tail = last_batch.slice(last_tail_start, last_rows - last_tail_start);

    let boundary = RgBoundaryInfo {
        first_key: *first_key,
        first_head,
        last_key: *last_key,
        last_tail,
    };

    // `slices` is dropped here — RG data freed immediately
    Ok(RgMatchResult {
        count,
        boundary: Some(boundary),
    })
}

/// Coerce a partition column to Int64Array.
fn coerce_partition_to_i64(col: &ArrayRef) -> Option<Int64Array> {
    use datafusion::arrow::datatypes::DataType;

    match col.data_type() {
        DataType::Int64 => col.as_any().downcast_ref::<Int64Array>().cloned(),
        DataType::UInt64 => {
            let arr = col.as_any().downcast_ref::<UInt64Array>()?;
            let values: Vec<i64> = arr.values().iter().map(|v| *v as i64).collect();
            Some(Int64Array::from(values))
        }
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>()?;
            let values: Vec<i64> = arr.values().iter().map(|v| *v as i64).collect();
            Some(Int64Array::from(values))
        }
        DataType::UInt32 => {
            let arr = col.as_any().downcast_ref::<UInt32Array>()?;
            let values: Vec<i64> = arr.values().iter().map(|v| *v as i64).collect();
            Some(Int64Array::from(values))
        }
        _ => None,
    }
}

/// Count cross-RG boundary patterns using pre-extracted boundary info.
///
/// Each RgMatchResult has a small RgBoundaryInfo with the last few rows of
/// the last partition and the first few rows of the first partition.
/// We check adjacent RGs: if RG[i].last_key == RG[i+1].first_key, we
/// concatenate the tail+head and check for cross-boundary matches.
fn count_cross_rg_boundary_patterns_from_info(
    rg_results: &[RgMatchResult],
    step_predicates: &[PyExpr],
    num_steps: usize,
    name_to_idx: &HashMap<String, usize>,
) -> usize {
    if rg_results.len() < 2 || num_steps < 2 {
        return 0;
    }

    let mut boundary_count: usize = 0;

    for i in 0..rg_results.len() - 1 {
        let curr_boundary = match &rg_results[i].boundary {
            Some(b) => b,
            None => continue,
        };
        let next_boundary = match &rg_results[i + 1].boundary {
            Some(b) => b,
            None => continue,
        };

        if curr_boundary.last_key != next_boundary.first_key {
            continue; // Different users — no cross-boundary match possible
        }

        let tail_slice = &curr_boundary.last_tail;
        let head_slice = &next_boundary.first_head;

        // Concatenate tail + head into a small batch
        let schema = tail_slice.schema();
        let combined = match concat_batches(&schema, &[tail_slice.clone(), head_slice.clone()]) {
            Ok(b) => b,
            Err(_) => {
                let small = vec![tail_slice.clone(), head_slice.clone()];
                match unify_and_concat_batches(&small) {
                    Ok(b) => b,
                    Err(_) => continue,
                }
            }
        };

        let n = combined.num_rows();
        if n < num_steps {
            continue;
        }

        // Pattern match on this small boundary batch
        // Only count matches that actually cross the boundary (start in tail, end in head)
        let tail_len = tail_slice.num_rows();

        let step1_mask = match eval_predicate(&step_predicates[0], &combined, name_to_idx) {
            Ok(m) => m,
            Err(_) => continue,
        };

        let step_prefixes: Vec<Option<String>> = step_predicates[1..]
            .iter()
            .map(|expr| extract_starts_with_prefix(expr))
            .collect();

        let url_idx = name_to_idx.get("url");
        let all_simple = step_prefixes.iter().all(|p| p.is_some()) && url_idx.is_some();

        let max_start = n.saturating_sub(num_steps - 1);

        if all_simple {
            let url_col = combined.column(*url_idx.unwrap());
            let prefixes: Vec<&str> = step_prefixes
                .iter()
                .map(|p| p.as_ref().unwrap().as_str())
                .collect();

            if let Some(str_arr) = url_col.as_any().downcast_ref::<StringViewArray>() {
                for i in 0..max_start {
                    if i >= tail_len {
                        break;
                    }
                    let last_row = i + num_steps - 1;
                    if last_row < tail_len {
                        continue;
                    }
                    if !step1_mask.is_valid(i) || !step1_mask.value(i) {
                        continue;
                    }
                    let mut all_match = true;
                    for (step_offset, prefix) in prefixes.iter().enumerate() {
                        let row = i + step_offset + 1;
                        if str_arr.is_null(row) || !str_arr.value(row).starts_with(prefix) {
                            all_match = false;
                            break;
                        }
                    }
                    if all_match {
                        boundary_count += 1;
                    }
                }
            } else if let Some(str_arr) = url_col.as_any().downcast_ref::<StringArray>() {
                for i in 0..max_start {
                    if i >= tail_len {
                        break;
                    }
                    let last_row = i + num_steps - 1;
                    if last_row < tail_len {
                        continue;
                    }
                    if !step1_mask.is_valid(i) || !step1_mask.value(i) {
                        continue;
                    }
                    let mut all_match = true;
                    for (step_offset, prefix) in prefixes.iter().enumerate() {
                        let row = i + step_offset + 1;
                        if str_arr.is_null(row) || !str_arr.value(row).starts_with(prefix) {
                            all_match = false;
                            break;
                        }
                    }
                    if all_match {
                        boundary_count += 1;
                    }
                }
            }
        } else {
            // General fallback for cross-boundary
            let step_masks: Vec<BooleanArray> = step_predicates
                .iter()
                .filter_map(|expr| eval_predicate(expr, &combined, name_to_idx).ok())
                .collect();

            if step_masks.len() != step_predicates.len() {
                continue;
            }

            for i in 0..max_start {
                if i >= tail_len {
                    break;
                }
                let last_row = i + num_steps - 1;
                if last_row < tail_len {
                    continue;
                }
                if !step_masks[0].is_valid(i) || !step_masks[0].value(i) {
                    continue;
                }
                let mut all_match = true;
                for (step_offset, mask) in step_masks[1..].iter().enumerate() {
                    let row = i + step_offset + 1;
                    if !mask.is_valid(row) || !mask.value(row) {
                        all_match = false;
                        break;
                    }
                }
                if all_match {
                    boundary_count += 1;
                }
            }
        }
    }

    boundary_count
}

/// Unify schemas across batches and concatenate.
///
/// Different Parquet row groups can produce different Arrow types for string columns
/// (e.g., Utf8View vs Utf8). This function casts all batches to a common schema
/// before concatenating.
fn unify_and_concat_batches(batches: &[RecordBatch]) -> Result<RecordBatch, String> {
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{Field, Schema};

    if batches.is_empty() {
        return Err("No batches".to_string());
    }
    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }

    // Build unified schema: for each field, pick the "widest" compatible type.
    // Utf8View → Utf8 (Utf8 is universally supported by concat_batches)
    let base_schema = batches[0].schema();
    let unified_fields: Vec<Field> = base_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let mut dt = f.data_type().clone();
            // Check if any batch has a different type for this column
            for b in batches.iter().skip(1) {
                let b_schema = b.schema();
                let other_dt = b_schema.field(i).data_type();
                if other_dt != &dt {
                    // If either is a view type, downgrade to non-view
                    dt = unify_data_type(&dt, other_dt);
                }
            }
            Field::new(f.name(), dt, f.is_nullable())
        })
        .collect();

    let unified_schema = Arc::new(Schema::new(unified_fields));

    // Cast each batch to the unified schema
    let cast_batches: Result<Vec<RecordBatch>, String> = batches
        .iter()
        .map(|batch| {
            let columns: Result<Vec<ArrayRef>, String> = batch
                .columns()
                .iter()
                .enumerate()
                .map(|(i, col)| {
                    let target_type = unified_schema.field(i).data_type();
                    if col.data_type() == target_type {
                        Ok(Arc::clone(col))
                    } else {
                        cast(col.as_ref(), target_type).map_err(|e| {
                            format!(
                                "Failed to cast column {} from {:?} to {:?}: {}",
                                unified_schema.field(i).name(),
                                col.data_type(),
                                target_type,
                                e
                            )
                        })
                    }
                })
                .collect();

            let cols = columns?;
            RecordBatch::try_new(unified_schema.clone(), cols)
                .map_err(|e| format!("Failed to create unified batch: {}", e))
        })
        .collect();

    let cast_batches = cast_batches?;
    concat_batches(&unified_schema, &cast_batches).map_err(|e| format!("concat failed: {}", e))
}

/// Pick a common data type when two columns have different types.
fn unify_data_type(
    a: &datafusion::arrow::datatypes::DataType,
    b: &datafusion::arrow::datatypes::DataType,
) -> datafusion::arrow::datatypes::DataType {
    use datafusion::arrow::datatypes::DataType;

    match (a, b) {
        // View types → non-view
        (DataType::Utf8View, DataType::Utf8) | (DataType::Utf8, DataType::Utf8View) => {
            DataType::Utf8
        }
        (DataType::Utf8View, DataType::LargeUtf8) | (DataType::LargeUtf8, DataType::Utf8View) => {
            DataType::LargeUtf8
        }
        (DataType::BinaryView, DataType::Binary) | (DataType::Binary, DataType::BinaryView) => {
            DataType::Binary
        }
        (DataType::BinaryView, DataType::LargeBinary)
        | (DataType::LargeBinary, DataType::BinaryView) => DataType::LargeBinary,
        // Same view types
        (DataType::Utf8View, DataType::Utf8View) => DataType::Utf8View,
        (DataType::BinaryView, DataType::BinaryView) => DataType::BinaryView,
        // Default: keep the first type (may fail on concat, but let it surface)
        _ => a.clone(),
    }
}

/// Count pattern matches within a single row group's worth of slices.
///
/// Concatenates all partition slices in the RG into one batch, then pattern-matches
/// across the entire batch — but skips any match where consecutive rows belong to
/// different partitions. This avoids creating per-user PartitionSlice objects.
fn count_patterns_in_rg_slices(
    rg_slices: &[(i64, RecordBatch)],
    step_predicates: &[PyExpr],
    num_steps: usize,
    name_to_idx: &HashMap<String, usize>,
) -> usize {
    if rg_slices.is_empty() {
        return 0;
    }

    // Build a combined batch from all slices in this RG
    let batches: Vec<&RecordBatch> = rg_slices.iter().map(|(_, b)| b).collect();
    let schema = batches[0].schema();

    let combined = {
        let batch_refs: Vec<RecordBatch> = batches.iter().map(|b| (*b).clone()).collect();
        match concat_batches(&schema, &batch_refs) {
            Ok(b) => b,
            Err(_) => match unify_and_concat_batches(&batch_refs) {
                Ok(b) => b,
                Err(_) => return 0,
            },
        }
    };

    let n = combined.num_rows();
    if n < num_steps {
        return 0;
    }

    // Build partition boundary mask: partition_boundaries[i] = true means row i
    // starts a new partition (different key from row i-1).
    // We compute this from the original slices' key + row count info.
    let mut partition_boundaries = vec![false; n];
    partition_boundaries[0] = true; // first row always starts a partition
    let mut offset = 0;
    for (idx, (key, batch)) in rg_slices.iter().enumerate() {
        if idx > 0 && *key != rg_slices[idx - 1].0 {
            partition_boundaries[offset] = true;
        }
        offset += batch.num_rows();
    }

    // Pattern match: skip matches that span partition boundaries
    let step1_mask = match eval_predicate(&step_predicates[0], &combined, name_to_idx) {
        Ok(m) => m,
        Err(_) => return 0,
    };

    let max_start = n.saturating_sub(num_steps - 1);
    let mut count: usize = 0;

    // Try fast path: all remaining predicates are starts_with on same column
    let step_prefixes: Vec<Option<String>> = step_predicates[1..]
        .iter()
        .map(|expr| extract_starts_with_prefix(expr))
        .collect();

    let url_idx = name_to_idx.get("url");
    let all_simple = step_prefixes.iter().all(|p| p.is_some()) && url_idx.is_some();

    if all_simple {
        let url_col = combined.column(*url_idx.unwrap());
        let prefixes: Vec<&str> = step_prefixes
            .iter()
            .map(|p| p.as_ref().unwrap().as_str())
            .collect();

        if let Some(str_arr) = url_col.as_any().downcast_ref::<StringViewArray>() {
            for i in 0..max_start {
                if !step1_mask.is_valid(i) || !step1_mask.value(i) {
                    continue;
                }
                // Check that all rows i..i+num_steps are in the same partition
                let mut crosses_boundary = false;
                for offset in 1..num_steps {
                    if partition_boundaries[i + offset] {
                        crosses_boundary = true;
                        break;
                    }
                }
                if crosses_boundary {
                    continue;
                }
                let mut all_match = true;
                for (step_offset, prefix) in prefixes.iter().enumerate() {
                    let row = i + step_offset + 1;
                    if str_arr.is_null(row) || !str_arr.value(row).starts_with(prefix) {
                        all_match = false;
                        break;
                    }
                }
                if all_match {
                    count += 1;
                }
            }
        } else if let Some(str_arr) = url_col.as_any().downcast_ref::<StringArray>() {
            for i in 0..max_start {
                if !step1_mask.is_valid(i) || !step1_mask.value(i) {
                    continue;
                }
                let mut crosses_boundary = false;
                for offset in 1..num_steps {
                    if partition_boundaries[i + offset] {
                        crosses_boundary = true;
                        break;
                    }
                }
                if crosses_boundary {
                    continue;
                }
                let mut all_match = true;
                for (step_offset, prefix) in prefixes.iter().enumerate() {
                    let row = i + step_offset + 1;
                    if str_arr.is_null(row) || !str_arr.value(row).starts_with(prefix) {
                        all_match = false;
                        break;
                    }
                }
                if all_match {
                    count += 1;
                }
            }
        }
    } else {
        // General fallback
        let step_masks: Vec<BooleanArray> = step_predicates
            .iter()
            .filter_map(|expr| eval_predicate(expr, &combined, name_to_idx).ok())
            .collect();

        if step_masks.len() != step_predicates.len() {
            return 0;
        }

        for i in 0..max_start {
            if !step_masks[0].is_valid(i) || !step_masks[0].value(i) {
                continue;
            }
            let mut crosses_boundary = false;
            for offset in 1..num_steps {
                if partition_boundaries[i + offset] {
                    crosses_boundary = true;
                    break;
                }
            }
            if crosses_boundary {
                continue;
            }
            let mut all_match = true;
            for (step_offset, mask) in step_masks[1..].iter().enumerate() {
                let row = i + step_offset + 1;
                if !mask.is_valid(row) || !mask.value(row) {
                    all_match = false;
                    break;
                }
            }
            if all_match {
                count += 1;
            }
        }
    }

    count
}
