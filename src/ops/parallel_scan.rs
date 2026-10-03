//! Direct Parquet sequence engine — bypasses DataFusion for sequence operations.
//!
//! Two strategies for pre-sorted Parquet:
//!
//! 1. **Parallel chunk streaming** (R2 group_ordered count): Divide row groups
//!    into N contiguous chunks (N = CPU threads).  Each thread opens the file
//!    once and reads its chunk sequentially, carrying the previous row across
//!    batch boundaries.  N seam checks between chunks.  Only N file opens.
//!
//! 2. **Parallel partitioned** (R3 pattern matching): Read row groups in parallel,
//!    split by partition key, run pattern matching per-partition in parallel.

use crate::error::LtseqError;
use crate::ops::linear_scan::{extract_referenced_columns, fused_boundaries};
use crate::ops::pattern_match::{
    count_prefix_matches_on, eval_predicate, same_string_column_starts_with_plan,
    StartsWithFastPathPlan,
};
use crate::types::PyExpr;
use crate::LTSeqTable;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, UInt32Array,
    UInt64Array,
};
use datafusion::arrow::compute::concat_batches;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::sync::Arc;

// ============================================================================
// Strategy 1: Parallel Chunk Streaming for R2 (group_ordered count)
//
// Divides row groups into N contiguous chunks (N = rayon thread count).
// Each thread opens the file ONCE and reads its chunk sequentially, carrying
// the previous row across batch boundaries.  This yields only N file opens
// (vs. num_row_groups in a naïve per-RG approach) and keeps I/O sequential
// within each thread — better for both OS page-cache and SSD.
//
// The only cross-chunk boundaries that need a seam check are the N-1 joints
// between consecutive chunks.
// ============================================================================

/// Result from processing a contiguous chunk of row groups (one parallel worker).
struct RgChunkResult {
    /// Boundaries in this chunk, excluding the chunk's first row: a worker
    /// cannot see the row before it, so the seam pass decides that one.
    count: usize,
    /// First and last row of this chunk as one-row batches, for the seam
    /// pass. `None` only when the entire chunk was empty.
    first_row: Option<RecordBatch>,
    last_row: Option<RecordBatch>,
}

/// Process a contiguous range of row groups for session boundary counting.
///
/// Opens the Parquet file once and streams row groups `start_rg..end_rg`
/// sequentially, carrying the previous batch's last row into the next
/// batch, so boundaries inside the chunk need no seam handling here.
///
/// Returns `Err("PARALLEL_FALLBACK: …")` if the predicate has no fused form
/// for these columns (see `linear_scan::fused_boundaries`).
fn process_chunk_session_count(
    parquet_path: &str,
    start_rg: usize,
    end_rg: usize,
    projection_mask: &ProjectionMask,
    predicate: &PyExpr,
    name_to_idx: &HashMap<String, usize>,
) -> Result<RgChunkResult, String> {
    let file = File::open(parquet_path)
        .map_err(|e| format!("PARALLEL_FALLBACK: open failed: {}", e))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("PARALLEL_FALLBACK: builder failed: {}", e))?;
    let row_groups: Vec<usize> = (start_rg..end_rg).collect();
    let reader = builder
        .with_row_groups(row_groups)
        .with_projection(projection_mask.clone())
        .with_batch_size(65536)
        .build()
        .map_err(|e| format!("PARALLEL_FALLBACK: build failed: {}", e))?;

    let mut count = 0usize;
    let mut first_row: Option<RecordBatch> = None;
    let mut last_row: Option<RecordBatch> = None;

    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| format!("PARALLEL_FALLBACK: read failed: {}", e))?;
        let n = batch.num_rows();
        if n == 0 {
            continue;
        }

        let flags = fused_boundaries(predicate, &batch, name_to_idx, last_row.as_ref())
            .ok_or("PARALLEL_FALLBACK: predicate has no fused form")?;

        // The chunk's first row has no previous row here: leave it to the
        // seam pass, which compares it with the previous chunk's last row.
        let skip = if first_row.is_none() {
            first_row = Some(batch.slice(0, 1));
            1
        } else {
            0
        };
        count += flags.iter().skip(skip).filter(|&&is_boundary| is_boundary).count();

        last_row = Some(batch.slice(n - 1, 1));
    }

    Ok(RgChunkResult {
        count,
        first_row,
        last_row,
    })
}

/// Parallel session boundary count for pre-sorted Parquet files.
///
/// Divides the Parquet row groups into N contiguous chunks (N = rayon thread
/// count) and processes each chunk in parallel.  Each worker opens the file
/// once and streams its chunk sequentially, carrying the previous row
/// across batch boundaries within the chunk.
///
/// Algorithm:
/// 1. Read Parquet metadata; partition row groups into N chunks.
/// 2. `rayon::into_par_iter` over chunks → `process_chunk_session_count`,
///    which counts every boundary except the chunk's first row.
/// 3. Sequential seam pass: each non-empty chunk's first row against the
///    last row of the nearest preceding non-empty chunk.
/// 4. total = Σ(chunk internal counts) + seam boundaries.
///
/// Returns `Err("PARALLEL_FALLBACK: …")` when the predicate is unsupported;
/// the caller degrades to the general linear-scan path.
pub fn parallel_streaming_group_count(
    _table: &LTSeqTable,
    predicate: &PyExpr,
    parquet_path: &str,
) -> Result<usize, LtseqError> {
    // 1. Extract referenced columns for projection pruning.
    let mut needed_cols: HashSet<String> = HashSet::new();
    extract_referenced_columns(predicate, &mut needed_cols);

    // 2. Open Parquet and read metadata (sequential, metadata-only).
    let file = File::open(parquet_path)
        .map_err(|e| LtseqError::Runtime(format!("PARALLEL_FALLBACK: {}", e)))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| LtseqError::Runtime(format!("PARALLEL_FALLBACK: {}", e)))?;

    let parquet_schema = builder.schema().clone();
    let num_row_groups = builder.metadata().num_row_groups();
    let parquet_metadata = builder.metadata().clone();

    if num_row_groups == 0 {
        return Ok(0);
    }

    // 3. Build column projection mask.
    let proj_indices: Vec<usize> = parquet_schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| needed_cols.contains(f.name()))
        .map(|(i, _)| i)
        .collect();

    if proj_indices.is_empty() {
        return Err(LtseqError::Runtime(
            "PARALLEL_FALLBACK: no predicate columns found in schema".into(),
        ));
    }

    let projection_mask = ProjectionMask::roots(
        parquet_metadata.file_metadata().schema_descr(),
        proj_indices.clone(),
    );

    let projected_schema = parquet_schema
        .project(&proj_indices)
        .map_err(|e| LtseqError::Runtime(format!("PARALLEL_FALLBACK: project schema: {}", e)))?;
    let name_to_idx: HashMap<String, usize> = projected_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().clone(), i))
        .collect();

    // 4. Partition row groups into N chunks (N = rayon thread count).
    //    Each chunk is processed by one worker with a single file open.
    //
    // Safety: PyExpr, ProjectionMask, HashMap<String, usize> are all
    // Send+Sync (pure Rust data, no Py<T> or interior mutability).
    let num_threads = rayon::current_num_threads().max(1).min(num_row_groups);
    let chunk_size = num_row_groups.div_ceil(num_threads);
    let path_str = parquet_path.to_string();

    let chunk_results: Vec<RgChunkResult> = (0..num_threads)
        .into_par_iter()
        .filter(|&t| t * chunk_size < num_row_groups) // skip idle threads
        .map(|t| {
            let start_rg = t * chunk_size;
            let end_rg = (start_rg + chunk_size).min(num_row_groups);
            process_chunk_session_count(
                &path_str,
                start_rg,
                end_rg,
                &projection_mask,
                predicate,
                &name_to_idx,
            )
        })
        .collect::<Result<Vec<_>, String>>()
        .map_err(LtseqError::Runtime)?;

    // 5. Sum internal boundary counts.
    let total_internal: usize = chunk_results.iter().map(|r| r.count).sum();

    // 6. Sequential seam pass over the non-empty chunks. Each chunk's first
    //    row is compared with the last row of the nearest preceding non-empty
    //    chunk; the first non-empty chunk has none, so its first row starts
    //    the sequence and counts as a boundary.
    let mut seam_count = 0usize;
    let mut prev_last_row: Option<&RecordBatch> = None;
    for chunk in &chunk_results {
        if let Some(first_row) = &chunk.first_row {
            let flags = fused_boundaries(predicate, first_row, &name_to_idx, prev_last_row)
                .ok_or_else(|| {
                    LtseqError::Runtime("PARALLEL_FALLBACK: predicate has no fused form".into())
                })?;
            seam_count += usize::from(flags[0]);
        }
        if let Some(last_row) = &chunk.last_row {
            prev_last_row = Some(last_row);
        }
    }

    Ok(total_internal + seam_count)
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
) -> Result<usize, LtseqError> {
    let num_steps = step_predicates.len();
    let fast_path_plan = same_string_column_starts_with_plan(step_predicates);

    // Step 1: Extract columns needed by all predicates
    let mut needed_cols: HashSet<String> = HashSet::new();
    for expr in step_predicates {
        extract_referenced_columns(expr, &mut needed_cols);
    }
    needed_cols.insert(partition_col.to_string());

    // Step 2: Open file and get metadata
    let file = File::open(parquet_path)
        .map_err(|e| LtseqError::Runtime(format!("Failed to open Parquet: {}", e)))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| LtseqError::Runtime(format!("Failed to read Parquet metadata: {}", e)))?;

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

    let projected_schema = Arc::new(
        parquet_schema
            .project(&proj_indices)
            .map_err(|e| LtseqError::Runtime(format!("Failed to project schema: {}", e)))?,
    );

    let projection_mask = ProjectionMask::roots(
        parquet_metadata.file_metadata().schema_descr(),
        proj_indices,
    );

    let part_col_idx = projected_schema
        .fields()
        .iter()
        .position(|f| f.name() == partition_col)
        .ok_or_else(|| LtseqError::ColumnNotFound(partition_col.to_string()))?;

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
                fast_path_plan.as_ref(),
                num_steps,
                &name_to_idx,
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| LtseqError::Runtime(format!("Parallel fused read+match failed: {}", e)))?;

    // Step 5: Sum intra-RG counts
    let intra_rg_count: usize = rg_results.iter().map(|r| r.count).sum();

    // Step 6: Handle cross-RG boundary patterns using the small boundary data.
    let boundary_count = count_cross_rg_boundary_patterns_from_info(
        &rg_results,
        step_predicates,
        num_steps,
        &name_to_idx,
    )
    .map_err(LtseqError::Runtime)?;

    let total_count = intra_rg_count + boundary_count;

    Ok(total_count)
}

// ============================================================================
// Helper: Partition data structures and utilities
// ============================================================================

/// Fused read + match + extract boundary for a single row group.
///
/// Reads the RG, concatenates projected batches once, pattern-matches within the RG,
/// extracts minimal boundary data (first/last few rows), then drops the full
/// RG data. This means each RG's ~20MB of data is freed immediately after
/// processing, eliminating the 1.4s dealloc overhead from holding all 814 RGs
/// (~2.7GB) in memory simultaneously.
#[expect(
    clippy::too_many_arguments,
    reason = "packing the args into a struct is a behavior-neutral refactor out of scope for #150"
)]
fn read_match_and_extract_boundary(
    parquet_path: &str,
    row_group_idx: usize,
    projection_mask: &ProjectionMask,
    partition_col_idx: usize,
    step_predicates: &[PyExpr],
    fast_path_plan: Option<&StartsWithFastPathPlan>,
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

    let mut rg_batches: Vec<RecordBatch> = Vec::new();

    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| format!("Failed to read batch from RG {}: {}", row_group_idx, e))?;

        if batch.num_rows() == 0 {
            continue;
        }

        rg_batches.push(batch);
    }

    if rg_batches.is_empty() {
        return Ok(RgMatchResult {
            count: 0,
            boundary: None,
        });
    }

    let combined = match concat_batches(&rg_batches[0].schema(), &rg_batches) {
        Ok(batch) => batch,
        Err(_) => unify_and_concat_batches(&rg_batches)
            .map_err(|e| format!("Failed to concatenate RG {}: {}", row_group_idx, e))?,
    };

    let part_col = combined.column(partition_col_idx);
    let part_values = coerce_partition_to_i64(part_col).ok_or_else(|| {
        format!(
            "Partition column has unsupported type {:?}",
            part_col.data_type()
        )
    })?;

    let n = combined.num_rows();
    let mut partition_boundaries = vec![false; n];
    partition_boundaries[0] = true;
    for (i, boundary) in partition_boundaries.iter_mut().enumerate().skip(1) {
        if part_values.value(i) != part_values.value(i - 1) {
            *boundary = true;
        }
    }

    // Pattern match within this RG
    let count = count_patterns_in_rg_batch(
        &combined,
        &partition_boundaries,
        step_predicates,
        fast_path_plan,
        num_steps,
        name_to_idx,
    )?;

    // Extract boundary info for cross-RG matching
    let first_key = part_values.value(0);
    let first_partition_end = partition_boundaries
        .iter()
        .enumerate()
        .skip(1)
        .find_map(|(idx, is_boundary)| is_boundary.then_some(idx))
        .unwrap_or(n);
    let first_head_rows = first_partition_end.min(num_steps.saturating_sub(1));
    let first_head = combined.slice(0, first_head_rows);

    let last_partition_start = partition_boundaries
        .iter()
        .enumerate()
        .rev()
        .find_map(|(idx, is_boundary)| is_boundary.then_some(idx))
        .unwrap_or(0);
    let last_key = part_values.value(last_partition_start);
    let last_tail_start = last_partition_start.max(n.saturating_sub(num_steps.saturating_sub(1)));
    let last_tail = combined.slice(last_tail_start, n - last_tail_start);

    let boundary = RgBoundaryInfo {
        first_key,
        first_head,
        last_key,
        last_tail,
    };

    // `combined` is dropped here — RG data freed immediately
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
/// Walks the row groups in order, carrying a "pending tail" — the last
/// `num_steps - 1` rows of the stream's trailing partition. The tail can
/// accumulate across several short row groups, so matches spanning three or
/// more row groups (and empty row groups in between) are counted correctly.
///
/// At each non-empty RG whose first partition continues the pending one, the
/// tail is concatenated with the RG's head and only matches that actually
/// cross the seam (start in the tail, end in the head) are counted — matches
/// fully inside either side were already counted intra-RG or at an earlier
/// seam.
fn count_cross_rg_boundary_patterns_from_info(
    rg_results: &[RgMatchResult],
    step_predicates: &[PyExpr],
    num_steps: usize,
    name_to_idx: &HashMap<String, usize>,
) -> Result<usize, String> {
    if num_steps < 2 {
        return Ok(0);
    }

    let keep = num_steps - 1;
    let mut boundary_count: usize = 0;
    let mut pending: Option<(i64, RecordBatch)> = None;

    for result in rg_results {
        let Some(b) = &result.boundary else {
            // Empty row group: contributes no rows, the pending tail carries over.
            continue;
        };

        if let Some((pending_key, pending_batch)) = &pending {
            if *pending_key == b.first_key
                && pending_batch.num_rows() > 0
                && b.first_head.num_rows() > 0
            {
                let combined = concat_boundary_pair(pending_batch, &b.first_head)?;
                if combined.num_rows() >= num_steps {
                    boundary_count += count_seam_matches(
                        &combined,
                        pending_batch.num_rows(),
                        step_predicates,
                        name_to_idx,
                    )?;
                }
            }
        }

        pending = Some(match pending.take() {
            Some((pending_key, pending_batch))
                if pending_key == b.first_key && b.first_key == b.last_key =>
            {
                // The whole row group belongs to the pending partition:
                // extend the tail and keep only the last `keep` rows.
                let merged = concat_boundary_pair(&pending_batch, &b.last_tail)?;
                let rows = merged.num_rows();
                let start = rows.saturating_sub(keep);
                (b.last_key, merged.slice(start, rows - start))
            }
            _ => (b.last_key, b.last_tail.clone()),
        });
    }

    Ok(boundary_count)
}

/// Concatenate two boundary slices, unifying schemas if the row groups
/// produced different Arrow string encodings.
fn concat_boundary_pair(a: &RecordBatch, b: &RecordBatch) -> Result<RecordBatch, String> {
    let schema = a.schema();
    match concat_batches(&schema, &[a.clone(), b.clone()]) {
        Ok(batch) => Ok(batch),
        Err(_) => unify_and_concat_batches(&[a.clone(), b.clone()])
            .map_err(|e| format!("PARALLEL_FALLBACK: boundary concat failed: {}", e)),
    }
}

/// Count matches in a seam batch that start in the pending tail
/// (`i < pending_len`) and end in the head (`i + num_steps - 1 >= pending_len`).
///
/// The batch is tiny (at most `2 * (num_steps - 1)` rows), so predicates are
/// evaluated uniformly via `eval_predicate`; failures propagate as
/// `PARALLEL_FALLBACK` instead of being treated as "no match".
fn count_seam_matches(
    combined: &RecordBatch,
    pending_len: usize,
    step_predicates: &[PyExpr],
    name_to_idx: &HashMap<String, usize>,
) -> Result<usize, String> {
    let num_steps = step_predicates.len();
    let n = combined.num_rows();
    let max_start = n.saturating_sub(num_steps - 1);

    let step_masks: Vec<BooleanArray> = step_predicates
        .iter()
        .map(|expr| {
            eval_predicate(expr, combined, name_to_idx).map_err(|e| {
                format!("PARALLEL_FALLBACK: predicate evaluation failed: {}", e)
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut count = 0;
    for i in 0..max_start.min(pending_len) {
        if i + num_steps - 1 < pending_len {
            continue; // fully inside the tail — counted before this seam
        }
        let mut all_match = true;
        for (step_offset, mask) in step_masks.iter().enumerate() {
            let row = i + step_offset;
            if !mask.is_valid(row) || !mask.value(row) {
                all_match = false;
                break;
            }
        }
        if all_match {
            count += 1;
        }
    }
    Ok(count)
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

/// Count pattern matches within a single row group batch.
///
/// Evaluation failures propagate as `Err("PARALLEL_FALLBACK: …")` so the
/// caller degrades to the general path — a failure must never be reported
/// as a zero count.
fn count_patterns_in_rg_batch(
    combined: &RecordBatch,
    partition_boundaries: &[bool],
    step_predicates: &[PyExpr],
    fast_path_plan: Option<&StartsWithFastPathPlan>,
    num_steps: usize,
    name_to_idx: &HashMap<String, usize>,
) -> Result<usize, String> {
    let n = combined.num_rows();
    if n < num_steps {
        return Ok(0);
    }

    // Pattern match: skip matches that span partition boundaries
    let max_start = n.saturating_sub(num_steps - 1);
    let same_partition =
        |i: usize| (1..num_steps).all(|offset| !partition_boundaries[i + offset]);

    // Fast path: all predicates are starts_with on the same string column.
    // If the column's array type is unsupported, fall through to the general
    // path (which handles it or errors explicitly) instead of returning 0.
    if let Some(plan) = fast_path_plan {
        if let Some(col_idx) = name_to_idx.get(plan.column.as_str()) {
            let prefixes: Vec<&str> = plan.prefixes.iter().map(|p| p.as_str()).collect();
            if let Some(count) = count_prefix_matches_on(
                combined.column(*col_idx),
                None,
                &prefixes,
                max_start,
                same_partition,
            ) {
                return Ok(count);
            }
        }
    }

    // General path: vectorized predicate evaluation.
    let step_masks: Vec<BooleanArray> = step_predicates
        .iter()
        .map(|expr| {
            eval_predicate(expr, combined, name_to_idx).map_err(|e| {
                format!("PARALLEL_FALLBACK: predicate evaluation failed: {}", e)
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut count: usize = 0;
    for i in 0..max_start {
        if !step_masks[0].is_valid(i) || !step_masks[0].value(i) {
            continue;
        }
        if !same_partition(i) {
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

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{create_session_context, RUNTIME};
    use crate::metadata::SortSpec;
    use crate::ops::grouping::group_ordered_count_impl;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::file_format::options::ParquetReadOptions;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    fn col(name: &str) -> PyExpr {
        PyExpr::Column(name.to_string())
    }

    fn binop(op: &str, left: PyExpr, right: PyExpr) -> PyExpr {
        PyExpr::BinOp {
            op: op.to_string(),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    fn shift1(name: &str) -> PyExpr {
        PyExpr::Call {
            func: "shift".to_string(),
            args: vec![PyExpr::Literal {
                value: "1".to_string(),
                dtype: "Int64".to_string(),
            }],
            kwargs: HashMap::new(),
            on: Some(Box::new(col(name))),
        }
    }

    /// The R2 shape: `(u != u.shift(1)) | ((t - t.shift(1)) > 4)`.
    fn sessionization() -> PyExpr {
        let gap = binop(
            "Gt",
            binop("Sub", col("t"), shift1("t")),
            PyExpr::Literal {
                value: "4".to_string(),
                dtype: "Int64".to_string(),
            },
        );
        binop("Or", binop("Ne", col("u"), shift1("u")), gap)
    }

    /// Sorted by `i`, with NULLs in both predicate columns. With row groups
    /// of 1–4 rows, seams fall both on boundaries and inside groups, and
    /// NULLs sit on either side of some of them. Parquet reads a NULL slot
    /// back as 0, so `u = [.., ∅, 0, ..]` catches a seam check that compares
    /// the previous row's value without looking at its NULL bit.
    fn events() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int64, false),
            Field::new("u", DataType::Int64, true),
            Field::new("t", DataType::Int64, true),
        ]));
        let u = vec![
            Some(1),
            Some(1),
            Some(1),
            None,
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            None,
            None,
            Some(3),
            Some(3),
        ];
        let t = vec![
            Some(0),
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            None,
            Some(6),
            Some(20),
            Some(21),
            Some(22),
            Some(23),
            Some(24),
        ];
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from_iter_values(0..12)),
                Arc::new(Int64Array::from(u)),
                Arc::new(Int64Array::from(t)),
            ],
        )
        .expect("valid events batch")
    }

    fn write_parquet(batch: &RecordBatch, rows_per_group: usize, tag: &str) -> String {
        let path = std::env::temp_dir().join(format!(
            "ltseq_issue157_{}_{}_{}.parquet",
            tag,
            rows_per_group,
            std::process::id()
        ));
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(rows_per_group))
            .build();
        let file = File::create(&path).expect("create parquet file");
        let mut writer =
            ArrowWriter::try_new(file, batch.schema(), Some(props)).expect("parquet writer");
        writer.write(batch).expect("write batch");
        writer.close().expect("close parquet file");
        path.to_str().expect("utf-8 temp path").to_string()
    }

    /// A table as `read_parquet(path).assume_sorted("i")` builds it.
    fn sorted_parquet_table(path: &str) -> LTSeqTable {
        let session = create_session_context();
        let df = RUNTIME
            .block_on(session.read_parquet(path, ParquetReadOptions::default()))
            .expect("read parquet");
        let schema = Arc::new(df.schema().as_arrow().clone());
        LTSeqTable::from_df_with_schema(
            session,
            df,
            schema,
            vec![SortSpec::new("i".to_string(), false)],
            Some(path.to_string()),
        )
    }

    /// The same rows as one in-memory batch, which takes the general
    /// linear-scan path.
    fn general_path_count(expr: &PyExpr) -> usize {
        let batch = events();
        let table = LTSeqTable::from_batches(
            create_session_context(),
            vec![batch.clone()],
            batch.schema(),
            vec![SortSpec::new("i".to_string(), false)],
        )
        .expect("in-memory table");
        group_ordered_count_impl(&table, expr).expect("general path count")
    }

    /// The parallel count stitches chunks at their first rows, so its answer
    /// must not depend on where the chunks start. Row groups of 1–4 rows on
    /// 1–4 workers cover the usual layouts; 12 workers on one-row groups put
    /// a seam before every row, including the rows that do not start a group.
    #[test]
    fn parallel_count_matches_general_path_at_every_seam() {
        let expr = sessionization();
        let expected = general_path_count(&expr);
        // Hand count for `events()`: rows 0 and 3–10 start a group; rows 1,
        // 2 and 11 do not.
        assert_eq!(expected, 9);

        for rows_per_group in 1..=4 {
            let path = write_parquet(&events(), rows_per_group, "seams");
            let table = sorted_parquet_table(&path);
            for threads in [1, 2, 3, 4, 12] {
                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build()
                    .expect("rayon pool");
                let count = pool
                    .install(|| parallel_streaming_group_count(&table, &expr, &path))
                    .expect("parallel count");
                assert_eq!(
                    count, expected,
                    "rows_per_group={rows_per_group}, threads={threads}"
                );
            }
            let _ = std::fs::remove_file(&path);
        }
    }

    /// A predicate with no fused form makes the parallel count fall back, and
    /// `group_ordered_count_impl` answers through the general path instead.
    #[test]
    fn unfused_predicate_falls_back_to_general_path() {
        // `u.shift(1) != u`: swapped operands are outside the fused shapes.
        let expr = binop("Ne", shift1("u"), col("u"));
        let path = write_parquet(&events(), 2, "fallback");
        let table = sorted_parquet_table(&path);

        let err = parallel_streaming_group_count(&table, &expr, &path)
            .expect_err("no fused form");
        assert!(err.to_string().contains("PARALLEL_FALLBACK"), "{err}");
        let count = group_ordered_count_impl(&table, &expr).expect("general path count");
        let _ = std::fs::remove_file(&path);
        assert_eq!(count, general_path_count(&expr));
    }
}
