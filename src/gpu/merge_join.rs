//! GPU-accelerated merge join for pre-sorted tables.
//!
//! # Algorithm
//!
//! For equi-join on pre-sorted key columns (both ascending):
//!
//! 1. **Binary search kernel**: For each left row `i`, find `lower_bound` and
//!    `upper_bound` in the right key column via binary search.
//!    Output: `counts[i] = upper_bound - lower_bound` (number of matches).
//!
//! 2. **Prefix sum**: Exclusive scan on `counts` → `offsets[i]` = where to write
//!    the output for left row `i`. Total output rows = `offsets[N] + counts[N-1]`.
//!
//! 3. **Scatter kernel**: For each left row `i`, write `counts[i]` output pairs:
//!    `(left_idx[offset + j] = i, right_idx[offset + j] = lower_bound + j)`
//!
//! 4. **CPU gather**: Use Arrow `take()` with the index arrays to build the result.
//!
//! ## Supported Types
//!
//! i32, i64, f32, f64 key columns (must be sorted ascending, no nulls in keys).
//!
//! ## Join Types
//!
//! - Inner: only matched pairs
//! - Left: all left rows, unmatched get NULL right columns
//!
//! Right and Full outer joins fall back to CPU/SQL.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, UInt32Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};

use cudarc::driver::safe::{CudaModule, CudaStream, LaunchConfig};
use cudarc::driver::PushKernelArg;
use cudarc::nvrtc::compile_ptx;
use std::sync::OnceLock;

use super::sort_aware::{SortOrderEffect, SortOrderPropagation};
use super::{get_context, get_stream, CUDA_BLOCK_SIZE, grid_size};

// ============================================================================
// CUDA Kernel Source
// ============================================================================

/// CUDA kernels for merge join operations:
/// - Binary search (lower_bound, upper_bound) per left key
/// - Exclusive prefix sum on match counts
/// - Scatter kernel to write (left_idx, right_idx) pairs
const MERGE_JOIN_KERNEL_SRC: &str = r#"
// ── Binary search: find [lower_bound, upper_bound) for each left key ───
//
// For each left[i], find the range [lb, ub) in right[] where right[j] == left[i].
// lower_bound: first position where right[j] >= left[i]
// upper_bound: first position where right[j] > left[i]
// count = ub - lb

extern "C" __global__ void merge_join_search_i64(
    const long long* __restrict__ left_keys,
    const long long* __restrict__ right_keys,
    unsigned int* __restrict__ lower_bounds,
    unsigned int* __restrict__ upper_bounds,
    unsigned int* __restrict__ counts,
    const unsigned int left_n,
    const unsigned int right_n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= left_n) return;

    long long key = left_keys[idx];

    // lower_bound: first j where right[j] >= key
    unsigned int lo = 0, hi = right_n;
    while (lo < hi) {
        unsigned int mid = lo + (hi - lo) / 2;
        if (right_keys[mid] < key) lo = mid + 1;
        else hi = mid;
    }
    unsigned int lb = lo;

    // upper_bound: first j where right[j] > key
    lo = lb; hi = right_n;
    while (lo < hi) {
        unsigned int mid = lo + (hi - lo) / 2;
        if (right_keys[mid] <= key) lo = mid + 1;
        else hi = mid;
    }
    unsigned int ub = lo;

    lower_bounds[idx] = lb;
    upper_bounds[idx] = ub;
    counts[idx] = ub - lb;
}

extern "C" __global__ void merge_join_search_i32(
    const int* __restrict__ left_keys,
    const int* __restrict__ right_keys,
    unsigned int* __restrict__ lower_bounds,
    unsigned int* __restrict__ upper_bounds,
    unsigned int* __restrict__ counts,
    const unsigned int left_n,
    const unsigned int right_n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= left_n) return;

    int key = left_keys[idx];

    unsigned int lo = 0, hi = right_n;
    while (lo < hi) {
        unsigned int mid = lo + (hi - lo) / 2;
        if (right_keys[mid] < key) lo = mid + 1;
        else hi = mid;
    }
    unsigned int lb = lo;

    lo = lb; hi = right_n;
    while (lo < hi) {
        unsigned int mid = lo + (hi - lo) / 2;
        if (right_keys[mid] <= key) lo = mid + 1;
        else hi = mid;
    }
    unsigned int ub = lo;

    lower_bounds[idx] = lb;
    upper_bounds[idx] = ub;
    counts[idx] = ub - lb;
}

extern "C" __global__ void merge_join_search_f64(
    const double* __restrict__ left_keys,
    const double* __restrict__ right_keys,
    unsigned int* __restrict__ lower_bounds,
    unsigned int* __restrict__ upper_bounds,
    unsigned int* __restrict__ counts,
    const unsigned int left_n,
    const unsigned int right_n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= left_n) return;

    double key = left_keys[idx];

    unsigned int lo = 0, hi = right_n;
    while (lo < hi) {
        unsigned int mid = lo + (hi - lo) / 2;
        if (right_keys[mid] < key) lo = mid + 1;
        else hi = mid;
    }
    unsigned int lb = lo;

    lo = lb; hi = right_n;
    while (lo < hi) {
        unsigned int mid = lo + (hi - lo) / 2;
        if (right_keys[mid] <= key) lo = mid + 1;
        else hi = mid;
    }
    unsigned int ub = lo;

    lower_bounds[idx] = lb;
    upper_bounds[idx] = ub;
    counts[idx] = ub - lb;
}

extern "C" __global__ void merge_join_search_f32(
    const float* __restrict__ left_keys,
    const float* __restrict__ right_keys,
    unsigned int* __restrict__ lower_bounds,
    unsigned int* __restrict__ upper_bounds,
    unsigned int* __restrict__ counts,
    const unsigned int left_n,
    const unsigned int right_n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= left_n) return;

    float key = left_keys[idx];

    unsigned int lo = 0, hi = right_n;
    while (lo < hi) {
        unsigned int mid = lo + (hi - lo) / 2;
        if (right_keys[mid] < key) lo = mid + 1;
        else hi = mid;
    }
    unsigned int lb = lo;

    lo = lb; hi = right_n;
    while (lo < hi) {
        unsigned int mid = lo + (hi - lo) / 2;
        if (right_keys[mid] <= key) lo = mid + 1;
        else hi = mid;
    }
    unsigned int ub = lo;

    lower_bounds[idx] = lb;
    upper_bounds[idx] = ub;
    counts[idx] = ub - lb;
}

// ── Scatter kernel: write (left_idx, right_idx) pairs to output ─────────
//
// For each left row i with count[i] matches starting at lower_bounds[i]:
//   for j in 0..count[i]:
//     out_left[offsets[i] + j]  = i
//     out_right[offsets[i] + j] = lower_bounds[i] + j

extern "C" __global__ void merge_join_scatter(
    const unsigned int* __restrict__ lower_bounds,
    const unsigned int* __restrict__ counts,
    const unsigned int* __restrict__ offsets,
    unsigned int* __restrict__ out_left,
    unsigned int* __restrict__ out_right,
    const unsigned int left_n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= left_n) return;

    unsigned int c = counts[idx];
    if (c == 0) return;

    unsigned int off = offsets[idx];
    unsigned int lb = lower_bounds[idx];

    for (unsigned int j = 0; j < c; j++) {
        out_left[off + j] = idx;
        out_right[off + j] = lb + j;
    }
}
"#;

// ============================================================================
// GPU Resources for merge join
// ============================================================================

/// Cached compiled module for merge join kernels.
static MERGE_JOIN_MODULE: OnceLock<Option<Arc<CudaModule>>> = OnceLock::new();

/// Get or compile the merge join CUDA module.
fn get_merge_join_module() -> Option<Arc<CudaModule>> {
    MERGE_JOIN_MODULE
        .get_or_init(|| {
            let ctx = get_context()?;
            let ptx = compile_ptx(MERGE_JOIN_KERNEL_SRC).ok()?;
            let module = ctx.load_module(ptx).ok()?;
            Some(module)
        })
        .clone()
}

/// Get a kernel function from the merge join module.
fn get_merge_join_function(name: &str) -> Option<cudarc::driver::safe::CudaFunction> {
    get_merge_join_module()?.load_function(name).ok()
}

// ============================================================================
// Merge Join Types
// ============================================================================

/// GPU merge join type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GpuJoinType {
    /// Inner join: only matched pairs
    Inner,
    /// Left join: all left rows, unmatched get NULLs for right columns
    Left,
}

// ============================================================================
// GPU Merge Join — Core Algorithm
// ============================================================================

/// Run GPU merge join on two pre-sorted key columns.
///
/// Returns `(left_indices, right_indices)` as `UInt32Array` pairs.
/// For inner join, both arrays are non-nullable.
/// For left join, `right_indices` may contain null entries for unmatched left rows.
///
/// # Arguments
/// * `left_keys` - Left key column (sorted ascending)
/// * `right_keys` - Right key column (sorted ascending)
/// * `join_type` - Inner or Left join
///
/// # Returns
/// `(left_indices, right_indices)` — index arrays for `arrow::compute::take()`
pub fn gpu_merge_join(
    left_keys: &ArrayRef,
    right_keys: &ArrayRef,
    join_type: GpuJoinType,
) -> std::result::Result<(UInt32Array, UInt32Array), String> {
    let left_n = left_keys.len();
    let right_n = right_keys.len();

    if left_n == 0 {
        return Ok((
            UInt32Array::from(Vec::<u32>::new()),
            UInt32Array::from(Vec::<u32>::new()),
        ));
    }

    if right_n == 0 {
        return match join_type {
            GpuJoinType::Inner => Ok((
                UInt32Array::from(Vec::<u32>::new()),
                UInt32Array::from(Vec::<u32>::new()),
            )),
            GpuJoinType::Left => {
                // All left rows, right indices are all null
                let left_idx: Vec<u32> = (0..left_n as u32).collect();
                let right_idx: Vec<Option<u32>> = vec![None; left_n];
                Ok((
                    UInt32Array::from(left_idx),
                    UInt32Array::from(right_idx),
                ))
            }
        };
    }

    let stream = get_stream().ok_or("CUDA stream unavailable")?;
    let data_type = left_keys.data_type();

    // Step 1: Upload keys and run binary search kernel
    let (lower_bounds, upper_bounds, counts) =
        run_binary_search(&stream, left_keys, right_keys, data_type, left_n, right_n)?;

    // Step 2: Compute exclusive prefix sum on CPU (counts → offsets)
    // For typical workloads this is fast enough; GPU prefix sum would be overkill
    // since we already have the counts on host from step 1.
    let mut offsets: Vec<u32> = Vec::with_capacity(left_n);
    let mut running: u32 = 0;
    for &c in &counts {
        offsets.push(running);
        running += c;
    }
    let total_inner_rows = running as usize;

    // Step 3: Scatter — write (left_idx, right_idx) pairs
    let (mut left_indices, mut right_indices) = if total_inner_rows > 0 {
        run_scatter(&stream, &lower_bounds, &counts, &offsets, left_n, total_inner_rows)?
    } else {
        (Vec::new(), Vec::new())
    };

    // Step 4: For left join, append unmatched left rows
    match join_type {
        GpuJoinType::Inner => {
            Ok((
                UInt32Array::from(left_indices),
                UInt32Array::from(right_indices),
            ))
        }
        GpuJoinType::Left => {
            // For left join, we need nullable right indices
            let mut left_result: Vec<u32> = left_indices;
            let mut right_result: Vec<Option<u32>> = right_indices.into_iter().map(Some).collect();

            // Add unmatched left rows (count == 0)
            for (i, &c) in counts.iter().enumerate() {
                if c == 0 {
                    left_result.push(i as u32);
                    right_result.push(None);
                }
            }

            Ok((
                UInt32Array::from(left_result),
                UInt32Array::from(right_result),
            ))
        }
    }
}

/// Run binary search kernel on GPU.
/// Returns (lower_bounds, upper_bounds, counts) as host Vec<u32>.
fn run_binary_search(
    stream: &Arc<CudaStream>,
    left_keys: &ArrayRef,
    right_keys: &ArrayRef,
    data_type: &DataType,
    left_n: usize,
    right_n: usize,
) -> std::result::Result<(Vec<u32>, Vec<u32>, Vec<u32>), String> {
    let kernel_name = match data_type {
        DataType::Int64 => "merge_join_search_i64",
        DataType::Int32 => "merge_join_search_i32",
        DataType::Float64 => "merge_join_search_f64",
        DataType::Float32 => "merge_join_search_f32",
        _ => return Err(format!("Unsupported key type for GPU merge join: {:?}", data_type)),
    };

    let func = get_merge_join_function(kernel_name)
        .ok_or_else(|| format!("CUDA kernel '{}' not found", kernel_name))?;

    let launch_cfg = LaunchConfig {
        grid_dim: (grid_size(left_n as u32), 1, 1),
        block_dim: (CUDA_BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    let mut d_lower = stream.alloc_zeros::<u32>(left_n).map_err(cuda_err)?;
    let mut d_upper = stream.alloc_zeros::<u32>(left_n).map_err(cuda_err)?;
    let mut d_counts = stream.alloc_zeros::<u32>(left_n).map_err(cuda_err)?;
    let left_n_u32 = left_n as u32;
    let right_n_u32 = right_n as u32;

    match data_type {
        DataType::Int64 => {
            let left_arr = left_keys.as_any().downcast_ref::<Int64Array>()
                .ok_or("Left key is not Int64Array")?;
            let right_arr = right_keys.as_any().downcast_ref::<Int64Array>()
                .ok_or("Right key is not Int64Array")?;
            let left_vals: &[i64] = left_arr.values();
            let right_vals: &[i64] = right_arr.values();
            let d_left = stream.clone_htod(left_vals).map_err(cuda_err)?;
            let d_right = stream.clone_htod(right_vals).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_left)
                    .arg(&d_right)
                    .arg(&mut d_lower)
                    .arg(&mut d_upper)
                    .arg(&mut d_counts)
                    .arg(&left_n_u32)
                    .arg(&right_n_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
        }
        DataType::Int32 => {
            let left_arr = left_keys.as_any().downcast_ref::<Int32Array>()
                .ok_or("Left key is not Int32Array")?;
            let right_arr = right_keys.as_any().downcast_ref::<Int32Array>()
                .ok_or("Right key is not Int32Array")?;
            let left_vals: &[i32] = left_arr.values();
            let right_vals: &[i32] = right_arr.values();
            let d_left = stream.clone_htod(left_vals).map_err(cuda_err)?;
            let d_right = stream.clone_htod(right_vals).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_left)
                    .arg(&d_right)
                    .arg(&mut d_lower)
                    .arg(&mut d_upper)
                    .arg(&mut d_counts)
                    .arg(&left_n_u32)
                    .arg(&right_n_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
        }
        DataType::Float64 => {
            let left_arr = left_keys.as_any().downcast_ref::<Float64Array>()
                .ok_or("Left key is not Float64Array")?;
            let right_arr = right_keys.as_any().downcast_ref::<Float64Array>()
                .ok_or("Right key is not Float64Array")?;
            let left_vals: &[f64] = left_arr.values();
            let right_vals: &[f64] = right_arr.values();
            let d_left = stream.clone_htod(left_vals).map_err(cuda_err)?;
            let d_right = stream.clone_htod(right_vals).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_left)
                    .arg(&d_right)
                    .arg(&mut d_lower)
                    .arg(&mut d_upper)
                    .arg(&mut d_counts)
                    .arg(&left_n_u32)
                    .arg(&right_n_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
        }
        DataType::Float32 => {
            let left_arr = left_keys.as_any().downcast_ref::<Float32Array>()
                .ok_or("Left key is not Float32Array")?;
            let right_arr = right_keys.as_any().downcast_ref::<Float32Array>()
                .ok_or("Right key is not Float32Array")?;
            let left_vals: &[f32] = left_arr.values();
            let right_vals: &[f32] = right_arr.values();
            let d_left = stream.clone_htod(left_vals).map_err(cuda_err)?;
            let d_right = stream.clone_htod(right_vals).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_left)
                    .arg(&d_right)
                    .arg(&mut d_lower)
                    .arg(&mut d_upper)
                    .arg(&mut d_counts)
                    .arg(&left_n_u32)
                    .arg(&right_n_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
        }
        _ => unreachable!(),
    }

    let lower_bounds = stream.clone_dtoh(&d_lower).map_err(cuda_err)?;
    let upper_bounds = stream.clone_dtoh(&d_upper).map_err(cuda_err)?;
    let counts = stream.clone_dtoh(&d_counts).map_err(cuda_err)?;

    Ok((lower_bounds, upper_bounds, counts))
}

/// Run scatter kernel on GPU to write (left_idx, right_idx) pairs.
/// Returns (left_indices, right_indices) as host Vec<u32>.
fn run_scatter(
    stream: &Arc<CudaStream>,
    lower_bounds: &[u32],
    counts: &[u32],
    offsets: &[u32],
    left_n: usize,
    total_output: usize,
) -> std::result::Result<(Vec<u32>, Vec<u32>), String> {
    let func = get_merge_join_function("merge_join_scatter")
        .ok_or("CUDA kernel 'merge_join_scatter' not found")?;

    let d_lower = stream.clone_htod(lower_bounds).map_err(cuda_err)?;
    let d_counts = stream.clone_htod(counts).map_err(cuda_err)?;
    let d_offsets = stream.clone_htod(offsets).map_err(cuda_err)?;
    let mut d_out_left = stream.alloc_zeros::<u32>(total_output).map_err(cuda_err)?;
    let mut d_out_right = stream.alloc_zeros::<u32>(total_output).map_err(cuda_err)?;
    let left_n_u32 = left_n as u32;

    let launch_cfg = LaunchConfig {
        grid_dim: (grid_size(left_n as u32), 1, 1),
        block_dim: (CUDA_BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    unsafe {
        stream
            .launch_builder(&func)
            .arg(&d_lower)
            .arg(&d_counts)
            .arg(&d_offsets)
            .arg(&mut d_out_left)
            .arg(&mut d_out_right)
            .arg(&left_n_u32)
            .launch(launch_cfg)
            .map_err(cuda_err)?;
    }

    let out_left = stream.clone_dtoh(&d_out_left).map_err(cuda_err)?;
    let out_right = stream.clone_dtoh(&d_out_right).map_err(cuda_err)?;

    Ok((out_left, out_right))
}

/// Convert a CUDA driver error into a string.
fn cuda_err(e: cudarc::driver::safe::DriverError) -> String {
    format!("CUDA error: {:?}", e)
}

// ============================================================================
// GpuMergeJoinExec — DataFusion ExecutionPlan
// ============================================================================

/// GPU-accelerated merge join execution plan for pre-sorted tables.
///
/// Both inputs must be sorted by their respective join key columns (ascending).
/// Currently supports single-column equi-joins on numeric types.
#[derive(Debug)]
pub struct GpuMergeJoinExec {
    /// Left input plan
    left: Arc<dyn ExecutionPlan>,
    /// Right input plan
    right: Arc<dyn ExecutionPlan>,
    /// Left key column name
    left_key: String,
    /// Right key column name
    right_key: String,
    /// Join type (Inner or Left)
    join_type: GpuJoinType,
    /// Alias prefix for right columns
    alias: String,
    /// Output schema
    output_schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cached plan properties
    cache: PlanProperties,
}

impl GpuMergeJoinExec {
    /// Create a new GpuMergeJoinExec.
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        left_key: String,
        right_key: String,
        join_type: GpuJoinType,
        alias: String,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        // Build output schema: left columns + aliased right columns
        let mut fields: Vec<Field> = left_schema.fields().iter().map(|f| (**f).clone()).collect();
        let nullable = matches!(join_type, GpuJoinType::Left);
        for field in right_schema.fields() {
            fields.push(Field::new(
                format!("{}_{}", alias, field.name()),
                field.data_type().clone(),
                nullable || field.is_nullable(),
            ));
        }
        let output_schema = Arc::new(ArrowSchema::new(fields));

        let cache = Self::compute_properties(&left, &output_schema);

        Ok(Self {
            left,
            right,
            left_key,
            right_key,
            join_type,
            alias,
            output_schema,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }

    fn compute_properties(
        left: &Arc<dyn ExecutionPlan>,
        _output_schema: &SchemaRef,
    ) -> PlanProperties {
        PlanProperties::new(
            left.equivalence_properties().clone(),
            left.output_partitioning().clone(),
            EmissionType::Final,
            left.boundedness(),
        )
    }
}

impl DisplayAs for GpuMergeJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "GpuMergeJoinExec: left_key={}, right_key={}, type={:?} [CUDA]",
                    self.left_key, self.right_key, self.join_type
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "left_key={} right_key={} type={:?} [CUDA]",
                    self.left_key, self.right_key, self.join_type
                )
            }
        }
    }
}

impl ExecutionPlan for GpuMergeJoinExec {
    fn name(&self) -> &'static str {
        "GpuMergeJoinExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true, false]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let right = children.pop().unwrap();
        let left = children.pop().unwrap();
        Ok(Arc::new(GpuMergeJoinExec::try_new(
            left,
            right,
            self.left_key.clone(),
            self.right_key.clone(),
            self.join_type,
            self.alias.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let left_stream = self.left.execute(partition, Arc::clone(&context))?;
        let right_stream = self.right.execute(partition, context)?;
        let schema = Arc::clone(&self.output_schema);
        let left_key = self.left_key.clone();
        let right_key = self.right_key.clone();
        let join_type = self.join_type;
        let alias = self.alias.clone();
        let left_schema = self.left.schema();
        let right_schema = self.right.schema();
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        let output_stream = futures_util::stream::once(async move {
            let timer = metrics.elapsed_compute().timer();

            // Collect both sides
            let left_batches: Vec<RecordBatch> =
                futures_util::StreamExt::collect::<Vec<_>>(left_stream)
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?;

            let right_batches: Vec<RecordBatch> =
                futures_util::StreamExt::collect::<Vec<_>>(right_stream)
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?;

            if left_batches.is_empty() || (right_batches.is_empty() && join_type == GpuJoinType::Inner) {
                timer.done();
                return Ok(RecordBatch::new_empty(schema));
            }

            // Coalesce into single batches
            let left_batch = datafusion::arrow::compute::concat_batches(&left_schema, left_batches.iter())?;
            let right_batch = if right_batches.is_empty() {
                RecordBatch::new_empty(right_schema.clone())
            } else {
                datafusion::arrow::compute::concat_batches(&right_schema, right_batches.iter())?
            };

            // Extract key columns
            let left_key_idx = left_batch.schema().index_of(&left_key)
                .map_err(|_| datafusion::common::DataFusionError::Execution(
                    format!("Left key column '{}' not found", left_key)
                ))?;
            let right_key_idx = right_batch.schema().index_of(&right_key)
                .map_err(|_| datafusion::common::DataFusionError::Execution(
                    format!("Right key column '{}' not found", right_key)
                ))?;

            let left_key_col = Arc::clone(left_batch.column(left_key_idx));
            let right_key_col = Arc::clone(right_batch.column(right_key_idx));

            // Run GPU merge join
            let (left_idx, right_idx) = gpu_merge_join(&left_key_col, &right_key_col, join_type)
                .map_err(|e| datafusion::common::DataFusionError::Execution(e))?;

            // Gather result columns using take()
            let mut result_columns: Vec<ArrayRef> = Vec::new();

            // Left columns
            for col_idx in 0..left_batch.num_columns() {
                let col = left_batch.column(col_idx);
                let taken = datafusion::arrow::compute::take(col, &left_idx, None)?;
                result_columns.push(taken);
            }

            // Right columns
            for col_idx in 0..right_batch.num_columns() {
                let col = right_batch.column(col_idx);
                let taken = datafusion::arrow::compute::take(col, &right_idx, None)?;
                result_columns.push(taken);
            }

            let result = RecordBatch::try_new(schema.clone(), result_columns)?;
            timer.done();
            metrics.record_output(result.num_rows());
            Ok(result)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.output_schema),
            output_stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl SortOrderPropagation for GpuMergeJoinExec {
    /// Merge join on sorted data produces output sorted by the join key.
    fn sort_order_effect(&self) -> SortOrderEffect {
        SortOrderEffect::Produce(vec![self.left_key.clone()])
    }
}

// ============================================================================
// Helper: Check eligibility for GPU merge join
// ============================================================================

/// Check if a data type is supported for GPU merge join keys.
pub fn is_merge_join_key_type_supported(dt: &DataType) -> bool {
    matches!(dt, DataType::Int64 | DataType::Int32 | DataType::Float64 | DataType::Float32)
}

/// Check if GPU merge join is eligible for the given tables and key columns.
///
/// Returns `true` if:
/// - GPU is available
/// - Combined row count exceeds threshold
/// - Key column types are supported
/// - Single-column join (composite keys not supported yet)
pub fn is_gpu_merge_join_eligible(
    left_n: usize,
    right_n: usize,
    left_key_type: &DataType,
    right_key_type: &DataType,
) -> bool {
    if !super::is_gpu_available() {
        return false;
    }

    // Must meet minimum row threshold (use larger of the two tables)
    let max_n = left_n.max(right_n);
    if max_n < super::GPU_MIN_ROWS_THRESHOLD {
        return false;
    }

    // Both key types must be supported and matching
    if !is_merge_join_key_type_supported(left_key_type) {
        return false;
    }
    if !is_merge_join_key_type_supported(right_key_type) {
        return false;
    }

    // Key types must be compatible (same type)
    left_key_type == right_key_type
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_join_key_type_support() {
        assert!(is_merge_join_key_type_supported(&DataType::Int64));
        assert!(is_merge_join_key_type_supported(&DataType::Int32));
        assert!(is_merge_join_key_type_supported(&DataType::Float64));
        assert!(is_merge_join_key_type_supported(&DataType::Float32));
        assert!(!is_merge_join_key_type_supported(&DataType::Utf8));
        assert!(!is_merge_join_key_type_supported(&DataType::Boolean));
    }

    #[test]
    fn test_merge_join_eligibility() {
        // GPU unavailable in test environment, so should return false
        let eligible = is_gpu_merge_join_eligible(
            200_000,
            200_000,
            &DataType::Int64,
            &DataType::Int64,
        );
        // In test environment without GPU, this will be false
        // If GPU were available, it would be true
        // We just verify the function doesn't panic
        let _ = eligible;
    }

    #[test]
    fn test_merge_join_eligibility_type_mismatch() {
        // Even with GPU, mismatched types should be ineligible
        let eligible = is_gpu_merge_join_eligible(
            200_000,
            200_000,
            &DataType::Int64,
            &DataType::Float64,
        );
        assert!(!eligible);
    }

    #[test]
    fn test_merge_join_eligibility_below_threshold() {
        // Below minimum row threshold
        let eligible = is_gpu_merge_join_eligible(
            100,
            100,
            &DataType::Int64,
            &DataType::Int64,
        );
        assert!(!eligible);
    }

    #[test]
    fn test_gpu_merge_join_empty_left() {
        // Empty left should return empty result for both join types
        let left: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
        let right: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2, 3]));

        let (l, r) = gpu_merge_join(&left, &right, GpuJoinType::Inner).unwrap();
        assert_eq!(l.len(), 0);
        assert_eq!(r.len(), 0);

        let (l, r) = gpu_merge_join(&left, &right, GpuJoinType::Left).unwrap();
        assert_eq!(l.len(), 0);
        assert_eq!(r.len(), 0);
    }

    #[test]
    fn test_gpu_merge_join_empty_right_inner() {
        let left: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2, 3]));
        let right: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));

        let (l, r) = gpu_merge_join(&left, &right, GpuJoinType::Inner).unwrap();
        assert_eq!(l.len(), 0);
        assert_eq!(r.len(), 0);
    }

    #[test]
    fn test_gpu_merge_join_empty_right_left_join() {
        let left: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2, 3]));
        let right: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));

        let (l, r) = gpu_merge_join(&left, &right, GpuJoinType::Left).unwrap();
        assert_eq!(l.len(), 3);
        assert_eq!(r.len(), 3);
        // All right indices should be null
        assert!(r.is_null(0));
        assert!(r.is_null(1));
        assert!(r.is_null(2));
    }
}
