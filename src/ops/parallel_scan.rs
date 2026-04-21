//! Direct Parquet sequence engine — bypasses DataFusion for sequence operations.
//!
//! Three strategies for pre-sorted Parquet:
//!
//! 1. **Sequential streaming** (R2 group_ordered): Single file handle, sequential
//!    scan, `streaming_fuse_eval` per batch.  Used as fallback.
//!
//! 1b. **Parallel chunk streaming** (R2 group_ordered count): Divide row groups
//!    into N contiguous chunks (N = CPU threads).  Each thread opens the file
//!    once, reads its chunk sequentially, and `StreamState` carries naturally
//!    across RG boundaries.  N seam checks between chunks.  Only N file opens.
//!
//! 2. **Parallel partitioned** (R3 pattern matching): Read row groups in parallel,
//!    split by partition key, run pattern matching per-partition in parallel.

use crate::error::LtseqError;
use crate::ops::linear_scan::{
    build_metadata_table, extract_referenced_columns, streaming_fuse_eval, StreamState,
};
use crate::ops::pattern_match::eval_predicate;
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

struct StartsWithFastPathPlan {
    column: String,
    prefixes: Vec<String>,
}

fn same_string_column_starts_with_plan(
    step_predicates: &[PyExpr],
) -> Option<StartsWithFastPathPlan> {
    if step_predicates.is_empty() {
        return None;
    }

    let mut column_name: Option<String> = None;
    let mut prefixes = Vec::with_capacity(step_predicates.len());

    for expr in step_predicates {
        let PyExpr::Call { func, args, on, kwargs } = expr else {
            return None;
        };
        if !kwargs.is_empty() || args.len() != 1 {
            return None;
        }
        if func != "starts_with" && func != "str_starts_with" {
            return None;
        }
        let PyExpr::Column(name) = on.as_ref() else {
            return None;
        };
        let PyExpr::Literal { value, dtype } = &args[0] else {
            return None;
        };
        if dtype != "Utf8" && dtype != "str" && dtype != "String" {
            return None;
        }
        let prefix = value.clone();

        match &column_name {
            Some(existing) if existing != name => return None,
            Some(_) => {}
            None => column_name = Some(name.clone()),
        }
        prefixes.push(prefix);
    }

    Some(StartsWithFastPathPlan {
        column: column_name?,
        prefixes,
    })
}

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
    let file = File::open(parquet_path)
        .map_err(|e| LtseqError::Runtime(format!("Failed to open Parquet: {}", e)))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| LtseqError::Runtime(format!("Failed to read Parquet metadata: {}", e)))?;

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
        .map_err(|e| LtseqError::Runtime(format!("Failed to build Parquet reader: {}", e)))?;

    let mut state = StreamState::new();
    let mut group_ids: Vec<i64> = Vec::with_capacity(total_rows_estimate);
    let mut name_to_idx: HashMap<String, usize> = HashMap::new();
    let mut idx_built = false;

    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| LtseqError::Runtime(format!("Failed to read Parquet batch: {}", e)))?;

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
            return Err(LtseqError::Runtime("PARALLEL_FALLBACK".into()).into());
        }

        // Accumulate group IDs from boundaries
        for &is_boundary in result.iter().take(n) {
            if is_boundary {
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
    let file = File::open(parquet_path)
        .map_err(|e| LtseqError::Runtime(format!("Failed to open Parquet: {}", e)))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| LtseqError::Runtime(format!("Failed to read Parquet metadata: {}", e)))?;

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
        .map_err(|e| LtseqError::Runtime(format!("Failed to build Parquet reader: {}", e)))?;

    let mut state = StreamState::new();
    let mut name_to_idx: HashMap<String, usize> = HashMap::new();
    let mut idx_built = false;

    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| LtseqError::Runtime(format!("Failed to read Parquet batch: {}", e)))?;

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
            return Err(LtseqError::Runtime("PARALLEL_FALLBACK".into()).into());
        }

        // Just count boundaries — no group_ids array needed
        for &is_boundary in result.iter().take(n) {
            if is_boundary {
                state.current_gid += 1;
            }
        }

        // Save last row's column values for cross-batch boundary detection
        state.save_last_row(&batch, &name_to_idx);
    }

    Ok(state.current_gid as usize)
}

// ============================================================================
// Strategy 1b: Parallel Chunk Streaming for R2 (group_ordered count)
//
// Divides row groups into N contiguous chunks (N = rayon thread count).
// Each thread opens the file ONCE and reads its chunk sequentially, carrying
// StreamState across row group boundaries naturally.  This yields only N file
// opens (vs. num_row_groups in a naïve per-RG approach) and keeps I/O
// sequential within each thread — better for both OS page-cache and SSD.
//
// The only cross-chunk boundaries that need a seam check are the N-1 joints
// between consecutive chunks.
// ============================================================================

/// Result from processing a contiguous chunk of row groups (one parallel worker).
struct RgChunkResult {
    /// Internal boundary count for this chunk.
    /// First chunk: includes the row-0 boundary (first session start).
    /// Non-first chunks: excludes the chunk's row 0 (seam pass handles it).
    count: usize,
    /// StreamState after the last row of this chunk.
    /// Used as context when the seam pass evaluates the next chunk's first row.
    /// `None` only when the entire chunk was empty.
    last_row_state: Option<StreamState>,
    /// First row of this chunk as a 1-row RecordBatch.
    /// Used by the previous chunk's seam check.
    /// `None` only when the entire chunk was empty.
    first_row_batch: Option<RecordBatch>,
}

/// Process a contiguous range of row groups for session boundary counting.
///
/// Opens the Parquet file once and streams row groups `start_rg..end_rg`
/// sequentially.  `StreamState` is carried naturally across RG boundaries
/// within the chunk — no per-RG seam handling is needed here.
///
/// For non-first chunks (`is_first_chunk = false`), the first row of the
/// chunk is skipped: the caller's seam pass will check whether it is a real
/// boundary by comparing the previous chunk's last-row state.
///
/// Returns `Err("PARALLEL_FALLBACK: …")` if the predicate cannot be handled
/// by `streaming_fuse_eval` (e.g. columns not i64-coercible).
fn process_chunk_session_count(
    parquet_path: &str,
    start_rg: usize,
    end_rg: usize,
    projection_mask: &ProjectionMask,
    predicate: &PyExpr,
    name_to_idx: &HashMap<String, usize>,
    is_first_chunk: bool,
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

    let mut state = StreamState::new();
    let mut count = 0usize;
    let mut first_row_batch: Option<RecordBatch> = None;
    let mut is_first_batch = true;

    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| format!("PARALLEL_FALLBACK: read failed: {}", e))?;
        let n = batch.num_rows();
        if n == 0 {
            continue;
        }

        // Capture the first row of this chunk for the caller's seam check.
        if first_row_batch.is_none() {
            first_row_batch = Some(batch.slice(0, 1));
        }

        let mut result = vec![false; n];
        if !streaming_fuse_eval(predicate, &batch, name_to_idx, &state, &mut result) {
            return Err("PARALLEL_FALLBACK: streaming_fuse_eval failed".into());
        }

        // For the first batch of a non-first chunk, skip row 0.
        // `streaming_fuse_eval` always marks it `true` (empty prev_values),
        // but the seam pass decides whether it is a real boundary.
        let start = if is_first_batch && !is_first_chunk { 1 } else { 0 };
        for &is_boundary in result.iter().take(n).skip(start) {
            if is_boundary {
                count += 1;
            }
        }

        state.save_last_row(&batch, name_to_idx);
        is_first_batch = false;
    }

    // If the chunk had rows but `save_last_row` left prev_values empty
    // (columns not i64-coercible), the seam check would be incorrect.
    if first_row_batch.is_some() && state.prev_values.is_empty() {
        return Err("PARALLEL_FALLBACK: column type not i64-coercible".into());
    }

    let last_row_state = if first_row_batch.is_some() {
        Some(state)
    } else {
        None
    };

    Ok(RgChunkResult {
        count,
        last_row_state,
        first_row_batch,
    })
}

/// Parallel session boundary count for pre-sorted Parquet files.
///
/// Divides the Parquet row groups into N contiguous chunks (N = rayon thread
/// count) and processes each chunk in parallel.  Each worker opens the file
/// once and streams its chunk sequentially; `StreamState` carries naturally
/// across RG boundaries within the chunk.
///
/// Algorithm:
/// 1. Read Parquet metadata; partition row groups into N chunks.
/// 2. `rayon::into_par_iter` over chunks → `process_chunk_session_count`.
///    - First chunk: row-0 counted as a session start.
///    - Non-first chunks: row 0 of first batch skipped (seam pass handles it).
/// 3. Sequential seam pass: N-1 checks between adjacent chunks.
/// 4. total = Σ(chunk internal counts) + seam boundaries.
///
/// Returns `Err("PARALLEL_FALLBACK: …")` when the predicate is unsupported;
/// the caller degrades to the sequential single-pass path.
pub fn parallel_streaming_group_count(
    _table: &LTSeqTable,
    predicate: &PyExpr,
    parquet_path: &str,
) -> PyResult<usize> {
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
        )
        .into());
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
                start_rg == 0,
            )
        })
        .collect::<Result<Vec<_>, String>>()
        .map_err(LtseqError::Runtime)?;

    // 5. Sum internal boundary counts.
    let total_internal: usize = chunk_results.iter().map(|r| r.count).sum();

    // 6. Sequential seam pass: N-1 checks between adjacent chunks.
    let mut seam_count = 0usize;
    for i in 0..chunk_results.len().saturating_sub(1) {
        let Some(ref last_state) = chunk_results[i].last_row_state else {
            continue; // chunk_i was entirely empty
        };
        let Some(ref first_batch) = chunk_results[i + 1].first_row_batch else {
            continue; // chunk_{i+1} was entirely empty
        };

        let mut result = vec![false; 1];
        if streaming_fuse_eval(predicate, first_batch, &name_to_idx, last_state, &mut result)
            && result[0]
        {
            seam_count += 1;
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
) -> PyResult<usize> {
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
        fast_path_plan.as_ref(),
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
/// Reads the RG, concatenates projected batches once, pattern-matches within the RG,
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
    for i in 1..n {
        if part_values.value(i) != part_values.value(i - 1) {
            partition_boundaries[i] = true;
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
    );

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
/// Each RgMatchResult has a small RgBoundaryInfo with the last few rows of
/// the last partition and the first few rows of the first partition.
/// We check adjacent RGs: if RG[i].last_key == RG[i+1].first_key, we
/// concatenate the tail+head and check for cross-boundary matches.
fn count_cross_rg_boundary_patterns_from_info(
    rg_results: &[RgMatchResult],
    step_predicates: &[PyExpr],
    fast_path_plan: Option<&StartsWithFastPathPlan>,
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

        let fast_path = fast_path_plan.and_then(|plan| {
            name_to_idx
                .get(plan.column.as_str())
                .copied()
                .map(|col_idx| (col_idx, plan))
        });

        let max_start = n.saturating_sub(num_steps - 1);

        if let Some((fast_col_idx, plan)) = fast_path {
            let url_col = combined.column(fast_col_idx);
            let prefixes = &plan.prefixes;
            let step1_prefix = prefixes[0].as_str();
            let remaining_prefixes: Vec<&str> = prefixes[1..].iter().map(|p| p.as_str()).collect();

            if let Some(str_arr) = url_col.as_any().downcast_ref::<StringViewArray>() {
                if prefixes.len() == 3 {
                    let step2_prefix = prefixes[1].as_str();
                    let step3_prefix = prefixes[2].as_str();
                    for i in 0..max_start {
                        if i >= tail_len {
                            break;
                        }
                        let row1 = i + 1;
                        let row2 = i + 2;
                        if row2 < tail_len {
                            continue;
                        }
                        if str_arr.is_null(i)
                            || str_arr.is_null(row1)
                            || str_arr.is_null(row2)
                            || !str_arr.value(i).starts_with(step1_prefix)
                            || !str_arr.value(row1).starts_with(step2_prefix)
                            || !str_arr.value(row2).starts_with(step3_prefix)
                        {
                            continue;
                        }
                        boundary_count += 1;
                    }
                } else {
                    for i in 0..max_start {
                        if i >= tail_len {
                            break;
                        }
                        let last_row = i + num_steps - 1;
                        if last_row < tail_len {
                            continue;
                        }
                        if str_arr.is_null(i) || !str_arr.value(i).starts_with(step1_prefix) {
                            continue;
                        }
                        let mut all_match = true;
                        for (step_offset, prefix) in remaining_prefixes.iter().enumerate() {
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
            } else if let Some(str_arr) = url_col.as_any().downcast_ref::<StringArray>() {
                if prefixes.len() == 3 {
                    let step2_prefix = prefixes[1].as_str();
                    let step3_prefix = prefixes[2].as_str();
                    for i in 0..max_start {
                        if i >= tail_len {
                            break;
                        }
                        let row1 = i + 1;
                        let row2 = i + 2;
                        if row2 < tail_len {
                            continue;
                        }
                        if str_arr.is_null(i)
                            || str_arr.is_null(row1)
                            || str_arr.is_null(row2)
                            || !str_arr.value(i).starts_with(step1_prefix)
                            || !str_arr.value(row1).starts_with(step2_prefix)
                            || !str_arr.value(row2).starts_with(step3_prefix)
                        {
                            continue;
                        }
                        boundary_count += 1;
                    }
                } else {
                    for i in 0..max_start {
                        if i >= tail_len {
                            break;
                        }
                        let last_row = i + num_steps - 1;
                        if last_row < tail_len {
                            continue;
                        }
                        if str_arr.is_null(i) || !str_arr.value(i).starts_with(step1_prefix) {
                            continue;
                        }
                        let mut all_match = true;
                        for (step_offset, prefix) in remaining_prefixes.iter().enumerate() {
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

/// Count pattern matches within a single row group batch.
fn count_patterns_in_rg_batch(
    combined: &RecordBatch,
    partition_boundaries: &[bool],
    step_predicates: &[PyExpr],
    fast_path_plan: Option<&StartsWithFastPathPlan>,
    num_steps: usize,
    name_to_idx: &HashMap<String, usize>,
) -> usize {
    let n = combined.num_rows();
    if n < num_steps {
        return 0;
    }

    // Pattern match: skip matches that span partition boundaries
    let max_start = n.saturating_sub(num_steps - 1);
    let mut count: usize = 0;

    // Try fast path: all remaining predicates are starts_with on same column
    let fast_path = fast_path_plan.and_then(|plan| {
        name_to_idx
            .get(plan.column.as_str())
            .copied()
            .map(|col_idx| (col_idx, plan))
    });

    if let Some((fast_col_idx, plan)) = fast_path {
        let url_col = combined.column(fast_col_idx);
        let prefixes = &plan.prefixes;
        let step1_prefix = prefixes[0].as_str();
        let remaining_prefixes: Vec<&str> = prefixes[1..].iter().map(|p| p.as_str()).collect();

        if let Some(str_arr) = url_col.as_any().downcast_ref::<StringViewArray>() {
            if prefixes.len() == 3 {
                let step2_prefix = prefixes[1].as_str();
                let step3_prefix = prefixes[2].as_str();
                for i in 0..max_start {
                    let row1 = i + 1;
                    let row2 = i + 2;
                    if partition_boundaries[row1] || partition_boundaries[row2] {
                        continue;
                    }
                    if str_arr.is_null(i)
                        || str_arr.is_null(row1)
                        || str_arr.is_null(row2)
                        || !str_arr.value(i).starts_with(step1_prefix)
                        || !str_arr.value(row1).starts_with(step2_prefix)
                        || !str_arr.value(row2).starts_with(step3_prefix)
                    {
                        continue;
                    }
                    count += 1;
                }
            } else {
                for i in 0..max_start {
                    if str_arr.is_null(i) || !str_arr.value(i).starts_with(step1_prefix) {
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
                    for (step_offset, prefix) in remaining_prefixes.iter().enumerate() {
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
        } else if let Some(str_arr) = url_col.as_any().downcast_ref::<StringArray>() {
            if prefixes.len() == 3 {
                let step2_prefix = prefixes[1].as_str();
                let step3_prefix = prefixes[2].as_str();
                for i in 0..max_start {
                    let row1 = i + 1;
                    let row2 = i + 2;
                    if partition_boundaries[row1] || partition_boundaries[row2] {
                        continue;
                    }
                    if str_arr.is_null(i)
                        || str_arr.is_null(row1)
                        || str_arr.is_null(row2)
                        || !str_arr.value(i).starts_with(step1_prefix)
                        || !str_arr.value(row1).starts_with(step2_prefix)
                        || !str_arr.value(row2).starts_with(step3_prefix)
                    {
                        continue;
                    }
                    count += 1;
                }
            } else {
                for i in 0..max_start {
                    if str_arr.is_null(i) || !str_arr.value(i).starts_with(step1_prefix) {
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
                    for (step_offset, prefix) in remaining_prefixes.iter().enumerate() {
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
