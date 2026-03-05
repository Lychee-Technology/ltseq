//! GPU-accelerated window operations for LTSeq.
//!
//! # GpuWindowShiftExec
//!
//! Replaces DataFusion's `BoundedWindowAggExec` / `WindowAggExec` for simple
//! shift (lag/lead) and diff operations on numeric columns.
//!
//! ## Algorithm
//!
//! Shift (lag/lead) is a simple GPU memory copy with offset:
//! - **lag(col, n)**: `out[i] = (i >= n) ? data[i - n] : default_or_null`
//! - **lead(col, n)**: `out[i] = (i + n < N) ? data[i + n] : default_or_null`
//!
//! Diff is adjacent subtraction with configurable period:
//! - **diff(col, n)**: `out[i] = (i >= n) ? data[i] - data[i - n] : null`
//!
//! These are embarrassingly parallel — each thread handles one output element.
//! The data is already sorted (window functions require ORDER BY) so we have
//! coalesced memory access patterns.
//!
//! ## Supported operations
//!
//! - `shift(n)` / `lag(col, n)` / `lead(col, n)` — offset copy
//! - `diff(n)` — `col - lag(col, n)` expressed as BinaryExpr(col, Minus, WindowFunction(lag))
//!
//! ## CUDA Kernels
//!
//! - `shift_copy_{i64,f64,i32,f32}`: Copy with offset, producing null mask
//! - `adjacent_diff_{i64,f64,i32,f32}`: Subtract element at offset, producing null mask
//!
//! ## Performance Target
//!
//! 100M rows < 2ms (single coalesced read + write per thread)

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array,
    BooleanArray, BooleanBufferBuilder, NullArray,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};

use cudarc::driver::safe::LaunchConfig;
use cudarc::driver::PushKernelArg;

use super::{get_stream, CUDA_BLOCK_SIZE, grid_size};
use super::sort_aware::{SortOrderEffect, SortOrderPropagation};

// ============================================================================
// CUDA Kernel Source
// ============================================================================

/// CUDA kernels for window shift (lag/lead) and diff operations.
///
/// Kernel naming: `shift_copy_{type}` for offset copy,
///                `window_diff_{type}` for adjacent subtraction.
///
/// Each shift kernel:
/// - `data`: input column
/// - `out`: output column (same type)
/// - `valid`: output null mask (u8: 0=null, 1=valid)
/// - `n`: number of elements
/// - `offset`: shift offset (positive = lag, negative = lead)
///
/// Each diff kernel:
/// - `data`: input column
/// - `out`: output column (same type)
/// - `valid`: output null mask
/// - `n`: number of elements
/// - `period`: diff period (positive integer)
///
/// Rolling aggregate kernels (`rolling_{sum,min,max}_{i64,f64}`, `rolling_avg_f64`,
/// `rolling_count`):
/// - Each thread computes the aggregate over a sliding window of `window_size`
///   elements ending at the current row: `[max(0, i - window_size + 1), i]`
/// - For rows where `i < window_size - 1`, the window is smaller (partial window)
/// - `rolling_count` always produces a valid (non-null) result
/// - Others produce valid results for all rows (partial windows are supported)
const WINDOW_OPS_KERNEL_SRC: &str = r#"
// ── Shift (lag/lead) kernels ─────────────────────────────────────────
// out[i] = data[i - offset] if in bounds, else null
// offset > 0 = lag (look backward), offset < 0 = lead (look forward)

extern "C" __global__ void shift_copy_i64(
    const long long* __restrict__ data,
    long long* __restrict__ out,
    unsigned char* __restrict__ valid,
    const unsigned int n,
    const int offset
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    int src = (int)idx - offset;
    if (src >= 0 && src < (int)n) {
        out[idx] = data[src];
        valid[idx] = 1;
    } else {
        out[idx] = 0;
        valid[idx] = 0;
    }
}

extern "C" __global__ void shift_copy_f64(
    const double* __restrict__ data,
    double* __restrict__ out,
    unsigned char* __restrict__ valid,
    const unsigned int n,
    const int offset
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    int src = (int)idx - offset;
    if (src >= 0 && src < (int)n) {
        out[idx] = data[src];
        valid[idx] = 1;
    } else {
        out[idx] = 0.0;
        valid[idx] = 0;
    }
}

extern "C" __global__ void shift_copy_i32(
    const int* __restrict__ data,
    int* __restrict__ out,
    unsigned char* __restrict__ valid,
    const unsigned int n,
    const int offset
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    int src = (int)idx - offset;
    if (src >= 0 && src < (int)n) {
        out[idx] = data[src];
        valid[idx] = 1;
    } else {
        out[idx] = 0;
        valid[idx] = 0;
    }
}

extern "C" __global__ void shift_copy_f32(
    const float* __restrict__ data,
    float* __restrict__ out,
    unsigned char* __restrict__ valid,
    const unsigned int n,
    const int offset
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    int src = (int)idx - offset;
    if (src >= 0 && src < (int)n) {
        out[idx] = data[src];
        valid[idx] = 1;
    } else {
        out[idx] = 0.0f;
        valid[idx] = 0;
    }
}

// ── Diff kernels ─────────────────────────────────────────────────────
// out[i] = data[i] - data[i - period] if i >= period, else null

extern "C" __global__ void window_diff_i64(
    const long long* __restrict__ data,
    long long* __restrict__ out,
    unsigned char* __restrict__ valid,
    const unsigned int n,
    const int period
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    if ((int)idx >= period && period > 0) {
        out[idx] = data[idx] - data[idx - period];
        valid[idx] = 1;
    } else {
        out[idx] = 0;
        valid[idx] = 0;
    }
}

extern "C" __global__ void window_diff_f64(
    const double* __restrict__ data,
    double* __restrict__ out,
    unsigned char* __restrict__ valid,
    const unsigned int n,
    const int period
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    if ((int)idx >= period && period > 0) {
        out[idx] = data[idx] - data[idx - period];
        valid[idx] = 1;
    } else {
        out[idx] = 0.0;
        valid[idx] = 0;
    }
}

extern "C" __global__ void window_diff_i32(
    const int* __restrict__ data,
    int* __restrict__ out,
    unsigned char* __restrict__ valid,
    const unsigned int n,
    const int period
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    if ((int)idx >= period && period > 0) {
        out[idx] = data[idx] - data[idx - period];
        valid[idx] = 1;
    } else {
        out[idx] = 0;
        valid[idx] = 0;
    }
}

extern "C" __global__ void window_diff_f32(
    const float* __restrict__ data,
    float* __restrict__ out,
    unsigned char* __restrict__ valid,
    const unsigned int n,
    const int period
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    if ((int)idx >= period && period > 0) {
        out[idx] = data[idx] - data[idx - period];
        valid[idx] = 1;
    } else {
        out[idx] = 0.0f;
        valid[idx] = 0;
    }
}

// ── Rolling aggregate kernels ────────────────────────────────────────
// Each thread computes an aggregate over [max(0, i-window_size+1), i].
// For partial windows (i < window_size-1), the aggregate is over fewer elements.

// --- Rolling SUM ---
extern "C" __global__ void rolling_sum_i64(
    const long long* __restrict__ data,
    long long* __restrict__ out,
    unsigned char* __restrict__ valid,
    const unsigned int n,
    const int window_size
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    int start = (int)idx - window_size + 1;
    if (start < 0) start = 0;
    long long acc = 0;
    for (int j = start; j <= (int)idx; j++) {
        acc += data[j];
    }
    out[idx] = acc;
    valid[idx] = 1;
}

extern "C" __global__ void rolling_sum_f64(
    const double* __restrict__ data,
    double* __restrict__ out,
    unsigned char* __restrict__ valid,
    const unsigned int n,
    const int window_size
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    int start = (int)idx - window_size + 1;
    if (start < 0) start = 0;
    double acc = 0.0;
    for (int j = start; j <= (int)idx; j++) {
        acc += data[j];
    }
    out[idx] = acc;
    valid[idx] = 1;
}

// --- Rolling AVG (always f64 output) ---
extern "C" __global__ void rolling_avg_f64(
    const double* __restrict__ data,
    double* __restrict__ out,
    unsigned char* __restrict__ valid,
    const unsigned int n,
    const int window_size
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    int start = (int)idx - window_size + 1;
    if (start < 0) start = 0;
    double acc = 0.0;
    int count = (int)idx - start + 1;
    for (int j = start; j <= (int)idx; j++) {
        acc += data[j];
    }
    out[idx] = acc / (double)count;
    valid[idx] = 1;
}

// --- Rolling MIN ---
extern "C" __global__ void rolling_min_i64(
    const long long* __restrict__ data,
    long long* __restrict__ out,
    unsigned char* __restrict__ valid,
    const unsigned int n,
    const int window_size
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    int start = (int)idx - window_size + 1;
    if (start < 0) start = 0;
    long long val = data[start];
    for (int j = start + 1; j <= (int)idx; j++) {
        if (data[j] < val) val = data[j];
    }
    out[idx] = val;
    valid[idx] = 1;
}

extern "C" __global__ void rolling_min_f64(
    const double* __restrict__ data,
    double* __restrict__ out,
    unsigned char* __restrict__ valid,
    const unsigned int n,
    const int window_size
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    int start = (int)idx - window_size + 1;
    if (start < 0) start = 0;
    double val = data[start];
    for (int j = start + 1; j <= (int)idx; j++) {
        if (data[j] < val) val = data[j];
    }
    out[idx] = val;
    valid[idx] = 1;
}

// --- Rolling MAX ---
extern "C" __global__ void rolling_max_i64(
    const long long* __restrict__ data,
    long long* __restrict__ out,
    unsigned char* __restrict__ valid,
    const unsigned int n,
    const int window_size
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    int start = (int)idx - window_size + 1;
    if (start < 0) start = 0;
    long long val = data[start];
    for (int j = start + 1; j <= (int)idx; j++) {
        if (data[j] > val) val = data[j];
    }
    out[idx] = val;
    valid[idx] = 1;
}

extern "C" __global__ void rolling_max_f64(
    const double* __restrict__ data,
    double* __restrict__ out,
    unsigned char* __restrict__ valid,
    const unsigned int n,
    const int window_size
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    int start = (int)idx - window_size + 1;
    if (start < 0) start = 0;
    double val = data[start];
    for (int j = start + 1; j <= (int)idx; j++) {
        if (data[j] > val) val = data[j];
    }
    out[idx] = val;
    valid[idx] = 1;
}

// --- Rolling COUNT (always u32 output, stored in int array) ---
// Counts elements in the window; for non-null-aware version, just window size
extern "C" __global__ void rolling_count(
    const unsigned int n,
    int* __restrict__ out,
    unsigned char* __restrict__ valid,
    const int window_size
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    int start = (int)idx - window_size + 1;
    if (start < 0) start = 0;
    out[idx] = (int)idx - start + 1;
    valid[idx] = 1;
}

// ── Prefix Sum (cumulative sum) kernels ──────────────────────────────
// Blelloch-style work-efficient prefix sum within a single thread block
// using shared memory. For arrays larger than block_size, we use a
// multi-pass approach with block-level sums.
//
// Phase 1 (prefix_sum_block_*): Each block computes an inclusive prefix
// sum of its chunk using shared memory up-sweep / down-sweep.
//
// Phase 2 (prefix_sum_propagate_*): Add the cumulative block sum from
// block (b-1) to all elements of block b (for b >= 1).

// -- Block-level inclusive prefix sum for i64 --
// Each block processes up to blockDim.x elements.
// block_sums[blockIdx.x] receives the total sum for this block.
extern "C" __global__ void prefix_sum_block_i64(
    const long long* __restrict__ data,
    long long* __restrict__ out,
    long long* __restrict__ block_sums,
    const unsigned int n
) {
    extern __shared__ long long sdata_i64[];
    unsigned int tid = threadIdx.x;
    unsigned int gid = blockIdx.x * blockDim.x + tid;

    // Load into shared memory
    sdata_i64[tid] = (gid < n) ? data[gid] : 0;
    __syncthreads();

    // Up-sweep (reduce) phase
    unsigned int limit = blockDim.x;
    for (unsigned int stride = 1; stride < limit; stride <<= 1) {
        unsigned int idx = (tid + 1) * (stride << 1) - 1;
        if (idx < limit) {
            sdata_i64[idx] += sdata_i64[idx - stride];
        }
        __syncthreads();
    }

    // Save block total and clear last element for down-sweep
    if (tid == 0) {
        block_sums[blockIdx.x] = sdata_i64[limit - 1];
        sdata_i64[limit - 1] = 0;
    }
    __syncthreads();

    // Down-sweep phase
    for (unsigned int stride = limit >> 1; stride >= 1; stride >>= 1) {
        unsigned int idx = (tid + 1) * (stride << 1) - 1;
        if (idx < limit) {
            long long tmp = sdata_i64[idx - stride];
            sdata_i64[idx - stride] = sdata_i64[idx];
            sdata_i64[idx] += tmp;
        }
        __syncthreads();
    }

    // Write exclusive prefix sum + original value = inclusive prefix sum
    if (gid < n) {
        out[gid] = sdata_i64[tid] + data[gid];
    }
}

// -- Propagate block sums for i64 --
// After block-level prefix sums, propagate cumulative block sums.
// block_prefix[b] = sum of all blocks 0..b-1 (exclusive prefix sum of block_sums).
extern "C" __global__ void prefix_sum_propagate_i64(
    long long* __restrict__ out,
    const long long* __restrict__ block_prefix,
    const unsigned int n
) {
    unsigned int gid = blockIdx.x * blockDim.x + threadIdx.x;
    if (gid < n && blockIdx.x > 0) {
        out[gid] += block_prefix[blockIdx.x];
    }
}

// -- Block-level inclusive prefix sum for f64 --
extern "C" __global__ void prefix_sum_block_f64(
    const double* __restrict__ data,
    double* __restrict__ out,
    double* __restrict__ block_sums,
    const unsigned int n
) {
    extern __shared__ double sdata_f64[];
    unsigned int tid = threadIdx.x;
    unsigned int gid = blockIdx.x * blockDim.x + tid;

    sdata_f64[tid] = (gid < n) ? data[gid] : 0.0;
    __syncthreads();

    unsigned int limit = blockDim.x;
    for (unsigned int stride = 1; stride < limit; stride <<= 1) {
        unsigned int idx = (tid + 1) * (stride << 1) - 1;
        if (idx < limit) {
            sdata_f64[idx] += sdata_f64[idx - stride];
        }
        __syncthreads();
    }

    if (tid == 0) {
        block_sums[blockIdx.x] = sdata_f64[limit - 1];
        sdata_f64[limit - 1] = 0.0;
    }
    __syncthreads();

    for (unsigned int stride = limit >> 1; stride >= 1; stride >>= 1) {
        unsigned int idx = (tid + 1) * (stride << 1) - 1;
        if (idx < limit) {
            double tmp = sdata_f64[idx - stride];
            sdata_f64[idx - stride] = sdata_f64[idx];
            sdata_f64[idx] += tmp;
        }
        __syncthreads();
    }

    if (gid < n) {
        out[gid] = sdata_f64[tid] + data[gid];
    }
}

// -- Propagate block sums for f64 --
extern "C" __global__ void prefix_sum_propagate_f64(
    double* __restrict__ out,
    const double* __restrict__ block_prefix,
    const unsigned int n
) {
    unsigned int gid = blockIdx.x * blockDim.x + threadIdx.x;
    if (gid < n && blockIdx.x > 0) {
        out[gid] += block_prefix[blockIdx.x];
    }
}
"#;

// ============================================================================
// GPU Resource Management for Window Kernels
// ============================================================================

use cudarc::driver::safe::CudaModule;
use cudarc::nvrtc::compile_ptx;
use std::sync::OnceLock;

/// Cached compiled CUDA module for window operations.
static WINDOW_MODULE: OnceLock<Option<Arc<CudaModule>>> = OnceLock::new();

/// Get the compiled window ops CUDA module.
fn get_window_module() -> Option<Arc<CudaModule>> {
    WINDOW_MODULE
        .get_or_init(|| {
            let ctx = super::get_context()?;
            let ptx = compile_ptx(WINDOW_OPS_KERNEL_SRC).ok()?;
            let module = ctx.load_module(ptx).ok()?;
            Some(module)
        })
        .clone()
}

// ============================================================================
// Window Operation Types
// ============================================================================

/// Aggregation function for rolling window operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollingAggFunc {
    Sum,
    Avg,
    Min,
    Max,
    Count,
}

impl fmt::Display for RollingAggFunc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RollingAggFunc::Sum => write!(f, "sum"),
            RollingAggFunc::Avg => write!(f, "avg"),
            RollingAggFunc::Min => write!(f, "min"),
            RollingAggFunc::Max => write!(f, "max"),
            RollingAggFunc::Count => write!(f, "count"),
        }
    }
}

/// Describes a single window shift/diff/rolling operation to execute on GPU.
#[derive(Debug, Clone)]
pub enum GpuWindowOp {
    /// Shift (lag/lead) operation: copy with offset.
    /// Positive offset = lag (look backward), negative = lead (look forward).
    Shift {
        /// Column index in the input schema
        column_index: usize,
        /// Column name (for display)
        column_name: String,
        /// Shift offset (positive = lag, negative = lead)
        offset: i32,
        /// Output column name
        output_name: String,
        /// Data type of the column
        data_type: DataType,
    },
    /// Diff operation: subtract element at offset.
    Diff {
        /// Column index in the input schema
        column_index: usize,
        /// Column name (for display)
        column_name: String,
        /// Diff period (positive)
        period: i32,
        /// Output column name
        output_name: String,
        /// Data type of the column
        data_type: DataType,
    },
    /// Rolling aggregate operation: compute an aggregate over a sliding window.
    /// Window is `ROWS BETWEEN (window_size-1) PRECEDING AND CURRENT ROW`.
    Rolling {
        /// Column index in the input schema
        column_index: usize,
        /// Column name (for display)
        column_name: String,
        /// Window size (number of rows in the window)
        window_size: usize,
        /// Aggregation function to apply
        agg_func: RollingAggFunc,
        /// Output column name
        output_name: String,
        /// Data type of the input column
        data_type: DataType,
    },
    /// Cumulative sum (prefix sum) operation.
    /// Frame is `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.
    /// Uses Blelloch work-efficient parallel prefix sum on GPU.
    CumulativeSum {
        /// Column index in the input schema
        column_index: usize,
        /// Column name (for display)
        column_name: String,
        /// Output column name
        output_name: String,
        /// Data type of the input column
        data_type: DataType,
    },
}

impl GpuWindowOp {
    /// Get the output column name.
    pub fn output_name(&self) -> &str {
        match self {
            GpuWindowOp::Shift { output_name, .. } => output_name,
            GpuWindowOp::Diff { output_name, .. } => output_name,
            GpuWindowOp::Rolling { output_name, .. } => output_name,
            GpuWindowOp::CumulativeSum { output_name, .. } => output_name,
        }
    }

    /// Get the data type of the output.
    pub fn data_type(&self) -> DataType {
        match self {
            GpuWindowOp::Shift { data_type, .. } => data_type.clone(),
            GpuWindowOp::Diff { data_type, .. } => data_type.clone(),
            GpuWindowOp::Rolling { agg_func, data_type, .. } => {
                match agg_func {
                    // Avg always produces Float64
                    RollingAggFunc::Avg => DataType::Float64,
                    // Count always produces Int64
                    RollingAggFunc::Count => DataType::Int64,
                    // Sum/Min/Max preserve the input type
                    _ => data_type.clone(),
                }
            }
            // Cumulative sum preserves the input type (with i32→i64 promotion)
            GpuWindowOp::CumulativeSum { data_type, .. } => {
                match data_type {
                    DataType::Int32 => DataType::Int64, // promote to avoid overflow
                    _ => data_type.clone(),
                }
            }
        }
    }
}

/// Configuration for `GpuWindowShiftExec`.
#[derive(Debug, Clone)]
pub struct GpuWindowShiftConfig {
    /// Window operations to execute (in order of output columns).
    pub ops: Vec<GpuWindowOp>,
}

// ============================================================================
// GpuWindowShiftExec
// ============================================================================

/// GPU execution node for window shift (lag/lead) and diff operations.
///
/// Replaces DataFusion's `BoundedWindowAggExec` / `WindowAggExec` when all
/// window expressions are simple lag/lead/diff on numeric columns without
/// partition_by (whole-table windows).
///
/// The input data is assumed to already be sorted (enforced by the Python layer
/// which requires `.sort()` before window operations).
///
/// ## Execution
///
/// 1. Collect all input batches into a single contiguous RecordBatch.
/// 2. For each window operation, transfer the source column to GPU.
/// 3. Launch the appropriate CUDA kernel (shift_copy or window_diff).
/// 4. Transfer results back and build the output RecordBatch:
///    - All original columns (pass-through)
///    - Plus new window result columns appended
#[derive(Debug)]
pub struct GpuWindowShiftExec {
    /// Window operations to execute
    config: GpuWindowShiftConfig,
    /// Child execution plan (provides sorted input data)
    input: Arc<dyn ExecutionPlan>,
    /// Output schema: input schema + window result columns
    output_schema: SchemaRef,
    /// Cached plan properties
    properties: PlanProperties,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl GpuWindowShiftExec {
    /// Create a new `GpuWindowShiftExec`.
    ///
    /// The output schema is the input schema plus one new column per window operation.
    pub fn try_new(
        config: GpuWindowShiftConfig,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let input_schema = input.schema();

        // Build output schema: input columns + window result columns
        let mut fields: Vec<Arc<Field>> = input_schema.fields().iter().cloned().collect();
        for op in &config.ops {
            // Window columns are nullable (nulls for out-of-bounds shifts)
            let field = Arc::new(Field::new(
                op.output_name(),
                op.data_type(),
                true, // always nullable
            ));
            fields.push(field);
        }
        let output_schema = Arc::new(Schema::new(fields));

        let properties = PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            EmissionType::Final,
        );

        Ok(Self {
            config,
            input,
            output_schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl SortOrderPropagation for GpuWindowShiftExec {
    fn sort_order_effect(&self) -> SortOrderEffect {
        // Window operations preserve the input sort order —
        // they only append new columns, they don't reorder rows.
        SortOrderEffect::Preserve
    }
}

impl DisplayAs for GpuWindowShiftExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let ops: Vec<String> = self.config.ops.iter().map(|op| {
                    match op {
                        GpuWindowOp::Shift { column_name, offset, output_name, .. } => {
                            format!("{}=shift({}, {})", output_name, column_name, offset)
                        }
                        GpuWindowOp::Diff { column_name, period, output_name, .. } => {
                            format!("{}=diff({}, {})", output_name, column_name, period)
                        }
                        GpuWindowOp::Rolling { column_name, window_size, agg_func, output_name, .. } => {
                            format!("{}=rolling_{}({}, {})", output_name, agg_func, column_name, window_size)
                        }
                        GpuWindowOp::CumulativeSum { column_name, output_name, .. } => {
                            format!("{}=cum_sum({})", output_name, column_name)
                        }
                    }
                }).collect();
                write!(f, "GpuWindowShiftExec: [{}]", ops.join(", "))
            }
        }
    }
}

impl ExecutionPlan for GpuWindowShiftExec {
    fn name(&self) -> &str {
        "GpuWindowShiftExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "GpuWindowShiftExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(GpuWindowShiftExec::try_new(
            self.config.clone(),
            Arc::clone(&children[0]),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, Arc::clone(&context))?;
        let config = self.config.clone();
        let output_schema = Arc::clone(&self.output_schema);
        let input_schema = self.input.schema();
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        let stream = futures::stream::once(async move {
            let _timer = metrics.elapsed_compute().timer();

            // Step 1: Collect all batches
            use futures::StreamExt;
            let batches: Vec<RecordBatch> = input_stream
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()?;

            if batches.is_empty() {
                // Return empty batch with output schema
                return Ok(RecordBatch::new_empty(output_schema));
            }

            // Step 2: Concatenate into single batch
            let combined = datafusion::arrow::compute::concat_batches(&input_schema, &batches)?;
            let num_rows = combined.num_rows();

            if num_rows == 0 {
                return Ok(RecordBatch::new_empty(output_schema));
            }

            // Step 3: Execute each window operation
            let mut result_columns: Vec<ArrayRef> = Vec::new();

            // First, include all original columns
            for i in 0..combined.num_columns() {
                result_columns.push(Arc::clone(combined.column(i)));
            }

            // Then compute window operation results
            for op in &config.ops {
                let result = execute_window_op(&combined, op, num_rows)?;
                result_columns.push(result);
            }

            // Step 4: Build output batch
            let output = RecordBatch::try_new(output_schema, result_columns)?;
            metrics.record_output(output.num_rows());
            Ok(output)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.output_schema),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

// ============================================================================
// GPU Kernel Execution
// ============================================================================

/// Execute a single window operation (shift, diff, or rolling) on the GPU.
fn execute_window_op(
    batch: &RecordBatch,
    op: &GpuWindowOp,
    num_rows: usize,
) -> Result<ArrayRef> {
    // Try GPU execution; fall back to CPU if GPU is unavailable
    match op {
        GpuWindowOp::Shift {
            column_index,
            offset,
            data_type,
            ..
        } => {
            let column = batch.column(*column_index);
            execute_shift_gpu(column, *offset, data_type, num_rows)
                .or_else(|_| execute_shift_cpu(column, *offset, data_type, num_rows))
        }
        GpuWindowOp::Diff {
            column_index,
            period,
            data_type,
            ..
        } => {
            let column = batch.column(*column_index);
            execute_diff_gpu(column, *period, data_type, num_rows)
                .or_else(|_| execute_diff_cpu(column, *period, data_type, num_rows))
        }
        GpuWindowOp::Rolling {
            column_index,
            window_size,
            agg_func,
            data_type,
            ..
        } => {
            let column = batch.column(*column_index);
            execute_rolling_gpu(column, *window_size, *agg_func, data_type, num_rows)
                .or_else(|_| execute_rolling_cpu(column, *window_size, *agg_func, data_type, num_rows))
        }
        GpuWindowOp::CumulativeSum {
            column_index,
            data_type,
            ..
        } => {
            let column = batch.column(*column_index);
            execute_cumsum_gpu(column, data_type, num_rows)
                .or_else(|_| execute_cumsum_cpu(column, data_type, num_rows))
        }
    }
}

/// Execute shift on GPU using CUDA kernel.
fn execute_shift_gpu(
    column: &ArrayRef,
    offset: i32,
    data_type: &DataType,
    num_rows: usize,
) -> Result<ArrayRef> {
    let module = get_window_module().ok_or_else(|| {
        DataFusionError::Internal("GPU not available for window shift".to_string())
    })?;
    let stream = get_stream().ok_or_else(|| {
        DataFusionError::Internal("CUDA stream not available".to_string())
    })?;

    let n = num_rows as u32;
    let grid = grid_size(n);
    let launch_cfg = LaunchConfig {
        grid_dim: (grid, 1, 1),
        block_dim: (CUDA_BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    match data_type {
        DataType::Int64 => {
            let arr = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Int64Array".to_string())
            })?;
            gpu_shift_typed::<i64>(arr.values(), &module, &stream, launch_cfg, n, offset, "shift_copy_i64")
                .map(|(values, valid_mask)| {
                    let null_buffer = build_null_buffer(&valid_mask);
                    Arc::new(Int64Array::new(values.into(), Some(null_buffer))) as ArrayRef
                })
        }
        DataType::Float64 => {
            let arr = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Float64Array".to_string())
            })?;
            gpu_shift_typed::<f64>(arr.values(), &module, &stream, launch_cfg, n, offset, "shift_copy_f64")
                .map(|(values, valid_mask)| {
                    let null_buffer = build_null_buffer(&valid_mask);
                    Arc::new(Float64Array::new(values.into(), Some(null_buffer))) as ArrayRef
                })
        }
        DataType::Int32 => {
            let arr = column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Int32Array".to_string())
            })?;
            gpu_shift_typed::<i32>(arr.values(), &module, &stream, launch_cfg, n, offset, "shift_copy_i32")
                .map(|(values, valid_mask)| {
                    let null_buffer = build_null_buffer(&valid_mask);
                    Arc::new(Int32Array::new(values.into(), Some(null_buffer))) as ArrayRef
                })
        }
        DataType::Float32 => {
            let arr = column.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Float32Array".to_string())
            })?;
            gpu_shift_typed::<f32>(arr.values(), &module, &stream, launch_cfg, n, offset, "shift_copy_f32")
                .map(|(values, valid_mask)| {
                    let null_buffer = build_null_buffer(&valid_mask);
                    Arc::new(Float32Array::new(values.into(), Some(null_buffer))) as ArrayRef
                })
        }
        _ => Err(DataFusionError::Internal(format!(
            "Unsupported data type for GPU shift: {:?}",
            data_type
        ))),
    }
}

/// Generic GPU shift kernel execution for a typed column.
fn gpu_shift_typed<T: cudarc::driver::DeviceRepr + Default + Clone>(
    input_values: &[T],
    module: &Arc<CudaModule>,
    stream: &Arc<cudarc::driver::safe::CudaStream>,
    launch_cfg: LaunchConfig,
    n: u32,
    offset: i32,
    kernel_name: &str,
) -> Result<(Vec<T>, Vec<u8>)> {
    let func = module.load_function(kernel_name).map_err(|e| {
        DataFusionError::Internal(format!("Failed to load kernel {}: {}", kernel_name, e))
    })?;

    // Allocate device memory
    let d_data = stream.memcpy_htod(input_values).map_err(|e| {
        DataFusionError::Internal(format!("H2D copy failed: {}", e))
    })?;
    let d_out = stream.alloc_zeros::<T>(n as usize).map_err(|e| {
        DataFusionError::Internal(format!("GPU alloc failed: {}", e))
    })?;
    let d_valid = stream.alloc_zeros::<u8>(n as usize).map_err(|e| {
        DataFusionError::Internal(format!("GPU alloc failed: {}", e))
    })?;

    // Launch kernel
    unsafe {
        let mut builder = stream.launch_builder(&func);
        builder.arg(&d_data);
        builder.arg(&d_out);
        builder.arg(&d_valid);
        builder.arg(&n);
        builder.arg(&offset);
        builder.launch(launch_cfg).map_err(|e| {
            DataFusionError::Internal(format!("Kernel launch failed: {}", e))
        })?;
    }

    // Copy results back
    let out_values = stream.memcpy_dtoh(&d_out).map_err(|e| {
        DataFusionError::Internal(format!("D2H copy failed: {}", e))
    })?;
    let valid_mask = stream.memcpy_dtoh(&d_valid).map_err(|e| {
        DataFusionError::Internal(format!("D2H copy failed: {}", e))
    })?;

    Ok((out_values, valid_mask))
}

/// Execute diff on GPU using CUDA kernel.
fn execute_diff_gpu(
    column: &ArrayRef,
    period: i32,
    data_type: &DataType,
    num_rows: usize,
) -> Result<ArrayRef> {
    let module = get_window_module().ok_or_else(|| {
        DataFusionError::Internal("GPU not available for window diff".to_string())
    })?;
    let stream = get_stream().ok_or_else(|| {
        DataFusionError::Internal("CUDA stream not available".to_string())
    })?;

    let n = num_rows as u32;
    let grid = grid_size(n);
    let launch_cfg = LaunchConfig {
        grid_dim: (grid, 1, 1),
        block_dim: (CUDA_BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    match data_type {
        DataType::Int64 => {
            let arr = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Int64Array".to_string())
            })?;
            gpu_diff_typed::<i64>(arr.values(), &module, &stream, launch_cfg, n, period, "window_diff_i64")
                .map(|(values, valid_mask)| {
                    let null_buffer = build_null_buffer(&valid_mask);
                    Arc::new(Int64Array::new(values.into(), Some(null_buffer))) as ArrayRef
                })
        }
        DataType::Float64 => {
            let arr = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Float64Array".to_string())
            })?;
            gpu_diff_typed::<f64>(arr.values(), &module, &stream, launch_cfg, n, period, "window_diff_f64")
                .map(|(values, valid_mask)| {
                    let null_buffer = build_null_buffer(&valid_mask);
                    Arc::new(Float64Array::new(values.into(), Some(null_buffer))) as ArrayRef
                })
        }
        DataType::Int32 => {
            let arr = column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Int32Array".to_string())
            })?;
            gpu_diff_typed::<i32>(arr.values(), &module, &stream, launch_cfg, n, period, "window_diff_i32")
                .map(|(values, valid_mask)| {
                    let null_buffer = build_null_buffer(&valid_mask);
                    Arc::new(Int32Array::new(values.into(), Some(null_buffer))) as ArrayRef
                })
        }
        DataType::Float32 => {
            let arr = column.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Float32Array".to_string())
            })?;
            gpu_diff_typed::<f32>(arr.values(), &module, &stream, launch_cfg, n, period, "window_diff_f32")
                .map(|(values, valid_mask)| {
                    let null_buffer = build_null_buffer(&valid_mask);
                    Arc::new(Float32Array::new(values.into(), Some(null_buffer))) as ArrayRef
                })
        }
        _ => Err(DataFusionError::Internal(format!(
            "Unsupported data type for GPU diff: {:?}",
            data_type
        ))),
    }
}

/// Generic GPU diff kernel execution for a typed column.
fn gpu_diff_typed<T: cudarc::driver::DeviceRepr + Default + Clone>(
    input_values: &[T],
    module: &Arc<CudaModule>,
    stream: &Arc<cudarc::driver::safe::CudaStream>,
    launch_cfg: LaunchConfig,
    n: u32,
    period: i32,
    kernel_name: &str,
) -> Result<(Vec<T>, Vec<u8>)> {
    let func = module.load_function(kernel_name).map_err(|e| {
        DataFusionError::Internal(format!("Failed to load kernel {}: {}", kernel_name, e))
    })?;

    // Allocate device memory
    let d_data = stream.memcpy_htod(input_values).map_err(|e| {
        DataFusionError::Internal(format!("H2D copy failed: {}", e))
    })?;
    let d_out = stream.alloc_zeros::<T>(n as usize).map_err(|e| {
        DataFusionError::Internal(format!("GPU alloc failed: {}", e))
    })?;
    let d_valid = stream.alloc_zeros::<u8>(n as usize).map_err(|e| {
        DataFusionError::Internal(format!("GPU alloc failed: {}", e))
    })?;

    // Launch kernel
    unsafe {
        let mut builder = stream.launch_builder(&func);
        builder.arg(&d_data);
        builder.arg(&d_out);
        builder.arg(&d_valid);
        builder.arg(&n);
        builder.arg(&period);
        builder.launch(launch_cfg).map_err(|e| {
            DataFusionError::Internal(format!("Kernel launch failed: {}", e))
        })?;
    }

    // Copy results back
    let out_values = stream.memcpy_dtoh(&d_out).map_err(|e| {
        DataFusionError::Internal(format!("D2H copy failed: {}", e))
    })?;
    let valid_mask = stream.memcpy_dtoh(&d_valid).map_err(|e| {
        DataFusionError::Internal(format!("D2H copy failed: {}", e))
    })?;

    Ok((out_values, valid_mask))
}

// ============================================================================
// GPU Rolling Aggregate Execution
// ============================================================================

/// Execute a rolling aggregate on GPU using CUDA kernel.
fn execute_rolling_gpu(
    column: &ArrayRef,
    window_size: usize,
    agg_func: RollingAggFunc,
    data_type: &DataType,
    num_rows: usize,
) -> Result<ArrayRef> {
    let module = get_window_module().ok_or_else(|| {
        DataFusionError::Internal("GPU not available for rolling window".to_string())
    })?;
    let stream = get_stream().ok_or_else(|| {
        DataFusionError::Internal("CUDA stream not available".to_string())
    })?;

    let n = num_rows as u32;
    let grid = grid_size(n);
    let launch_cfg = LaunchConfig {
        grid_dim: (grid, 1, 1),
        block_dim: (CUDA_BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };
    let ws = window_size as i32;

    match agg_func {
        RollingAggFunc::Count => {
            // Count doesn't need input data — just produces window sizes
            execute_rolling_count_gpu(&module, &stream, launch_cfg, n, ws)
        }
        RollingAggFunc::Avg => {
            // Avg always works on f64
            let f64_values = column_to_f64(column, data_type)?;
            gpu_rolling_typed::<f64>(
                &f64_values, &module, &stream, launch_cfg, n, ws,
                "rolling_avg_f64",
            ).map(|(values, valid_mask)| {
                let null_buffer = build_null_buffer(&valid_mask);
                Arc::new(Float64Array::new(values.into(), Some(null_buffer))) as ArrayRef
            })
        }
        RollingAggFunc::Sum => {
            match data_type {
                DataType::Int64 => {
                    let arr = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                        DataFusionError::Internal("Expected Int64Array".to_string())
                    })?;
                    gpu_rolling_typed::<i64>(
                        arr.values(), &module, &stream, launch_cfg, n, ws,
                        "rolling_sum_i64",
                    ).map(|(values, valid_mask)| {
                        let null_buffer = build_null_buffer(&valid_mask);
                        Arc::new(Int64Array::new(values.into(), Some(null_buffer))) as ArrayRef
                    })
                }
                DataType::Float64 => {
                    let arr = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                        DataFusionError::Internal("Expected Float64Array".to_string())
                    })?;
                    gpu_rolling_typed::<f64>(
                        arr.values(), &module, &stream, launch_cfg, n, ws,
                        "rolling_sum_f64",
                    ).map(|(values, valid_mask)| {
                        let null_buffer = build_null_buffer(&valid_mask);
                        Arc::new(Float64Array::new(values.into(), Some(null_buffer))) as ArrayRef
                    })
                }
                DataType::Int32 => {
                    // Promote i32 sum to i64 to avoid overflow
                    let i64_values: Vec<i64> = column
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or_else(|| DataFusionError::Internal("Expected Int32Array".to_string()))?
                        .values()
                        .iter()
                        .map(|&v| v as i64)
                        .collect();
                    gpu_rolling_typed::<i64>(
                        &i64_values, &module, &stream, launch_cfg, n, ws,
                        "rolling_sum_i64",
                    ).map(|(values, valid_mask)| {
                        let null_buffer = build_null_buffer(&valid_mask);
                        // Sum of i32 promoted to i64
                        Arc::new(Int64Array::new(values.into(), Some(null_buffer))) as ArrayRef
                    })
                }
                DataType::Float32 => {
                    // Promote f32 sum to f64 for precision
                    let f64_values: Vec<f64> = column
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .ok_or_else(|| DataFusionError::Internal("Expected Float32Array".to_string()))?
                        .values()
                        .iter()
                        .map(|&v| v as f64)
                        .collect();
                    gpu_rolling_typed::<f64>(
                        &f64_values, &module, &stream, launch_cfg, n, ws,
                        "rolling_sum_f64",
                    ).map(|(values, valid_mask)| {
                        let null_buffer = build_null_buffer(&valid_mask);
                        Arc::new(Float64Array::new(values.into(), Some(null_buffer))) as ArrayRef
                    })
                }
                _ => Err(DataFusionError::Internal(format!(
                    "Unsupported data type for GPU rolling sum: {:?}", data_type
                ))),
            }
        }
        RollingAggFunc::Min => {
            match data_type {
                DataType::Int64 => {
                    let arr = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                        DataFusionError::Internal("Expected Int64Array".to_string())
                    })?;
                    gpu_rolling_typed::<i64>(
                        arr.values(), &module, &stream, launch_cfg, n, ws,
                        "rolling_min_i64",
                    ).map(|(values, valid_mask)| {
                        let null_buffer = build_null_buffer(&valid_mask);
                        Arc::new(Int64Array::new(values.into(), Some(null_buffer))) as ArrayRef
                    })
                }
                DataType::Float64 => {
                    let arr = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                        DataFusionError::Internal("Expected Float64Array".to_string())
                    })?;
                    gpu_rolling_typed::<f64>(
                        arr.values(), &module, &stream, launch_cfg, n, ws,
                        "rolling_min_f64",
                    ).map(|(values, valid_mask)| {
                        let null_buffer = build_null_buffer(&valid_mask);
                        Arc::new(Float64Array::new(values.into(), Some(null_buffer))) as ArrayRef
                    })
                }
                DataType::Int32 => {
                    let arr = column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                        DataFusionError::Internal("Expected Int32Array".to_string())
                    })?;
                    let i64_values: Vec<i64> = arr.values().iter().map(|&v| v as i64).collect();
                    gpu_rolling_typed::<i64>(
                        &i64_values, &module, &stream, launch_cfg, n, ws,
                        "rolling_min_i64",
                    ).map(|(values, valid_mask)| {
                        let null_buffer = build_null_buffer(&valid_mask);
                        // Min of i32 values fits in i32, but we keep i64 for consistency
                        let i32_values: Vec<i32> = values.iter().map(|&v| v as i32).collect();
                        Arc::new(Int32Array::new(i32_values.into(), Some(null_buffer))) as ArrayRef
                    })
                }
                DataType::Float32 => {
                    let arr = column.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                        DataFusionError::Internal("Expected Float32Array".to_string())
                    })?;
                    let f64_values: Vec<f64> = arr.values().iter().map(|&v| v as f64).collect();
                    gpu_rolling_typed::<f64>(
                        &f64_values, &module, &stream, launch_cfg, n, ws,
                        "rolling_min_f64",
                    ).map(|(values, valid_mask)| {
                        let null_buffer = build_null_buffer(&valid_mask);
                        let f32_values: Vec<f32> = values.iter().map(|&v| v as f32).collect();
                        Arc::new(Float32Array::new(f32_values.into(), Some(null_buffer))) as ArrayRef
                    })
                }
                _ => Err(DataFusionError::Internal(format!(
                    "Unsupported data type for GPU rolling min: {:?}", data_type
                ))),
            }
        }
        RollingAggFunc::Max => {
            match data_type {
                DataType::Int64 => {
                    let arr = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                        DataFusionError::Internal("Expected Int64Array".to_string())
                    })?;
                    gpu_rolling_typed::<i64>(
                        arr.values(), &module, &stream, launch_cfg, n, ws,
                        "rolling_max_i64",
                    ).map(|(values, valid_mask)| {
                        let null_buffer = build_null_buffer(&valid_mask);
                        Arc::new(Int64Array::new(values.into(), Some(null_buffer))) as ArrayRef
                    })
                }
                DataType::Float64 => {
                    let arr = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                        DataFusionError::Internal("Expected Float64Array".to_string())
                    })?;
                    gpu_rolling_typed::<f64>(
                        arr.values(), &module, &stream, launch_cfg, n, ws,
                        "rolling_max_f64",
                    ).map(|(values, valid_mask)| {
                        let null_buffer = build_null_buffer(&valid_mask);
                        Arc::new(Float64Array::new(values.into(), Some(null_buffer))) as ArrayRef
                    })
                }
                DataType::Int32 => {
                    let arr = column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                        DataFusionError::Internal("Expected Int32Array".to_string())
                    })?;
                    let i64_values: Vec<i64> = arr.values().iter().map(|&v| v as i64).collect();
                    gpu_rolling_typed::<i64>(
                        &i64_values, &module, &stream, launch_cfg, n, ws,
                        "rolling_max_i64",
                    ).map(|(values, valid_mask)| {
                        let null_buffer = build_null_buffer(&valid_mask);
                        let i32_values: Vec<i32> = values.iter().map(|&v| v as i32).collect();
                        Arc::new(Int32Array::new(i32_values.into(), Some(null_buffer))) as ArrayRef
                    })
                }
                DataType::Float32 => {
                    let arr = column.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                        DataFusionError::Internal("Expected Float32Array".to_string())
                    })?;
                    let f64_values: Vec<f64> = arr.values().iter().map(|&v| v as f64).collect();
                    gpu_rolling_typed::<f64>(
                        &f64_values, &module, &stream, launch_cfg, n, ws,
                        "rolling_max_f64",
                    ).map(|(values, valid_mask)| {
                        let null_buffer = build_null_buffer(&valid_mask);
                        let f32_values: Vec<f32> = values.iter().map(|&v| v as f32).collect();
                        Arc::new(Float32Array::new(f32_values.into(), Some(null_buffer))) as ArrayRef
                    })
                }
                _ => Err(DataFusionError::Internal(format!(
                    "Unsupported data type for GPU rolling max: {:?}", data_type
                ))),
            }
        }
    }
}

/// Generic GPU rolling aggregate kernel execution for a typed column.
fn gpu_rolling_typed<T: cudarc::driver::DeviceRepr + Default + Clone>(
    input_values: &[T],
    module: &Arc<CudaModule>,
    stream: &Arc<cudarc::driver::safe::CudaStream>,
    launch_cfg: LaunchConfig,
    n: u32,
    window_size: i32,
    kernel_name: &str,
) -> Result<(Vec<T>, Vec<u8>)> {
    let func = module.load_function(kernel_name).map_err(|e| {
        DataFusionError::Internal(format!("Failed to load kernel {}: {}", kernel_name, e))
    })?;

    // Allocate device memory
    let d_data = stream.memcpy_htod(input_values).map_err(|e| {
        DataFusionError::Internal(format!("H2D copy failed: {}", e))
    })?;
    let d_out = stream.alloc_zeros::<T>(n as usize).map_err(|e| {
        DataFusionError::Internal(format!("GPU alloc failed: {}", e))
    })?;
    let d_valid = stream.alloc_zeros::<u8>(n as usize).map_err(|e| {
        DataFusionError::Internal(format!("GPU alloc failed: {}", e))
    })?;

    // Launch kernel: (data, out, valid, n, window_size)
    unsafe {
        let mut builder = stream.launch_builder(&func);
        builder.arg(&d_data);
        builder.arg(&d_out);
        builder.arg(&d_valid);
        builder.arg(&n);
        builder.arg(&window_size);
        builder.launch(launch_cfg).map_err(|e| {
            DataFusionError::Internal(format!("Kernel launch failed: {}", e))
        })?;
    }

    // Copy results back
    let out_values = stream.memcpy_dtoh(&d_out).map_err(|e| {
        DataFusionError::Internal(format!("D2H copy failed: {}", e))
    })?;
    let valid_mask = stream.memcpy_dtoh(&d_valid).map_err(|e| {
        DataFusionError::Internal(format!("D2H copy failed: {}", e))
    })?;

    Ok((out_values, valid_mask))
}

/// Execute rolling count on GPU — no input data column needed.
fn execute_rolling_count_gpu(
    module: &Arc<CudaModule>,
    stream: &Arc<cudarc::driver::safe::CudaStream>,
    launch_cfg: LaunchConfig,
    n: u32,
    window_size: i32,
) -> Result<ArrayRef> {
    let func = module.load_function("rolling_count").map_err(|e| {
        DataFusionError::Internal(format!("Failed to load rolling_count kernel: {}", e))
    })?;

    let d_out = stream.alloc_zeros::<i32>(n as usize).map_err(|e| {
        DataFusionError::Internal(format!("GPU alloc failed: {}", e))
    })?;
    let d_valid = stream.alloc_zeros::<u8>(n as usize).map_err(|e| {
        DataFusionError::Internal(format!("GPU alloc failed: {}", e))
    })?;

    // Kernel signature: rolling_count(n, out, valid, window_size)
    unsafe {
        let mut builder = stream.launch_builder(&func);
        builder.arg(&n);
        builder.arg(&d_out);
        builder.arg(&d_valid);
        builder.arg(&window_size);
        builder.launch(launch_cfg).map_err(|e| {
            DataFusionError::Internal(format!("Kernel launch failed: {}", e))
        })?;
    }

    let out_values = stream.memcpy_dtoh(&d_out).map_err(|e| {
        DataFusionError::Internal(format!("D2H copy failed: {}", e))
    })?;
    let valid_mask = stream.memcpy_dtoh(&d_valid).map_err(|e| {
        DataFusionError::Internal(format!("D2H copy failed: {}", e))
    })?;

    // Convert i32 → i64 for the Arrow Int64 output
    let i64_values: Vec<i64> = out_values.iter().map(|&v| v as i64).collect();
    let null_buffer = build_null_buffer(&valid_mask);
    Ok(Arc::new(Int64Array::new(i64_values.into(), Some(null_buffer))) as ArrayRef)
}

/// Convert any numeric column to f64 values for avg computation.
fn column_to_f64(column: &ArrayRef, data_type: &DataType) -> Result<Vec<f64>> {
    match data_type {
        DataType::Float64 => {
            let arr = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Float64Array".to_string())
            })?;
            Ok(arr.values().to_vec())
        }
        DataType::Float32 => {
            let arr = column.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Float32Array".to_string())
            })?;
            Ok(arr.values().iter().map(|&v| v as f64).collect())
        }
        DataType::Int64 => {
            let arr = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Int64Array".to_string())
            })?;
            Ok(arr.values().iter().map(|&v| v as f64).collect())
        }
        DataType::Int32 => {
            let arr = column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Int32Array".to_string())
            })?;
            Ok(arr.values().iter().map(|&v| v as f64).collect())
        }
        _ => Err(DataFusionError::Internal(format!(
            "Cannot convert {:?} to f64 for rolling avg", data_type
        ))),
    }
}

// ============================================================================
// CPU Fallback Implementations
// ============================================================================

/// CPU fallback for shift operation.
fn execute_shift_cpu(
    column: &ArrayRef,
    offset: i32,
    data_type: &DataType,
    num_rows: usize,
) -> Result<ArrayRef> {
    match data_type {
        DataType::Int64 => {
            let arr = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Int64Array".to_string())
            })?;
            Ok(cpu_shift_typed(arr.values(), offset, num_rows, |vals, nulls| {
                Arc::new(Int64Array::new(vals.into(), Some(nulls))) as ArrayRef
            }))
        }
        DataType::Float64 => {
            let arr = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Float64Array".to_string())
            })?;
            Ok(cpu_shift_typed(arr.values(), offset, num_rows, |vals, nulls| {
                Arc::new(Float64Array::new(vals.into(), Some(nulls))) as ArrayRef
            }))
        }
        DataType::Int32 => {
            let arr = column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Int32Array".to_string())
            })?;
            Ok(cpu_shift_typed(arr.values(), offset, num_rows, |vals, nulls| {
                Arc::new(Int32Array::new(vals.into(), Some(nulls))) as ArrayRef
            }))
        }
        DataType::Float32 => {
            let arr = column.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Float32Array".to_string())
            })?;
            Ok(cpu_shift_typed(arr.values(), offset, num_rows, |vals, nulls| {
                Arc::new(Float32Array::new(vals.into(), Some(nulls))) as ArrayRef
            }))
        }
        _ => Err(DataFusionError::Internal(format!(
            "Unsupported type for CPU shift: {:?}", data_type
        ))),
    }
}

/// Generic CPU shift for typed arrays.
fn cpu_shift_typed<T: Default + Copy, F>(
    input: &[T],
    offset: i32,
    num_rows: usize,
    build_array: F,
) -> ArrayRef
where
    F: FnOnce(Vec<T>, NullBuffer) -> ArrayRef,
{
    let mut out = vec![T::default(); num_rows];
    let mut valid = vec![false; num_rows];

    for i in 0..num_rows {
        let src = i as i64 - offset as i64;
        if src >= 0 && (src as usize) < num_rows {
            out[i] = input[src as usize];
            valid[i] = true;
        }
    }

    let null_buffer = NullBuffer::from(valid);
    build_array(out, null_buffer)
}

/// CPU fallback for diff operation.
fn execute_diff_cpu(
    column: &ArrayRef,
    period: i32,
    data_type: &DataType,
    num_rows: usize,
) -> Result<ArrayRef> {
    match data_type {
        DataType::Int64 => {
            let arr = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Int64Array".to_string())
            })?;
            Ok(cpu_diff_typed_i64(arr.values(), period, num_rows))
        }
        DataType::Float64 => {
            let arr = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Float64Array".to_string())
            })?;
            Ok(cpu_diff_typed_f64(arr.values(), period, num_rows))
        }
        DataType::Int32 => {
            let arr = column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Int32Array".to_string())
            })?;
            Ok(cpu_diff_typed_i32(arr.values(), period, num_rows))
        }
        DataType::Float32 => {
            let arr = column.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Float32Array".to_string())
            })?;
            Ok(cpu_diff_typed_f32(arr.values(), period, num_rows))
        }
        _ => Err(DataFusionError::Internal(format!(
            "Unsupported type for CPU diff: {:?}", data_type
        ))),
    }
}

fn cpu_diff_typed_i64(input: &[i64], period: i32, num_rows: usize) -> ArrayRef {
    let mut out = vec![0i64; num_rows];
    let mut valid = vec![false; num_rows];
    for i in 0..num_rows {
        if (i as i32) >= period && period > 0 {
            out[i] = input[i] - input[i - period as usize];
            valid[i] = true;
        }
    }
    let null_buffer = NullBuffer::from(valid);
    Arc::new(Int64Array::new(out.into(), Some(null_buffer)))
}

fn cpu_diff_typed_f64(input: &[f64], period: i32, num_rows: usize) -> ArrayRef {
    let mut out = vec![0.0f64; num_rows];
    let mut valid = vec![false; num_rows];
    for i in 0..num_rows {
        if (i as i32) >= period && period > 0 {
            out[i] = input[i] - input[i - period as usize];
            valid[i] = true;
        }
    }
    let null_buffer = NullBuffer::from(valid);
    Arc::new(Float64Array::new(out.into(), Some(null_buffer)))
}

fn cpu_diff_typed_i32(input: &[i32], period: i32, num_rows: usize) -> ArrayRef {
    let mut out = vec![0i32; num_rows];
    let mut valid = vec![false; num_rows];
    for i in 0..num_rows {
        if (i as i32) >= period && period > 0 {
            out[i] = input[i] - input[i - period as usize];
            valid[i] = true;
        }
    }
    let null_buffer = NullBuffer::from(valid);
    Arc::new(Int32Array::new(out.into(), Some(null_buffer)))
}

fn cpu_diff_typed_f32(input: &[f32], period: i32, num_rows: usize) -> ArrayRef {
    let mut out = vec![0.0f32; num_rows];
    let mut valid = vec![false; num_rows];
    for i in 0..num_rows {
        if (i as i32) >= period && period > 0 {
            out[i] = input[i] - input[i - period as usize];
            valid[i] = true;
        }
    }
    let null_buffer = NullBuffer::from(valid);
    Arc::new(Float32Array::new(out.into(), Some(null_buffer)))
}

/// CPU fallback for rolling aggregate operations.
///
/// Implements rolling sum/avg/min/max/count over a sliding window of `window_size`
/// rows. The window is `ROWS BETWEEN (window_size-1) PRECEDING AND CURRENT ROW`.
///
/// Semantics:
/// - All rows get a valid output (no nulls) because partial windows are allowed.
///   For row i, the window covers rows `max(0, i - window_size + 1)..=i`.
/// - Count always returns Int64, Avg always returns Float64.
/// - Sum/Min/Max preserve the input type (with i32→i64 promotion for sum).
fn execute_rolling_cpu(
    column: &ArrayRef,
    window_size: usize,
    agg_func: RollingAggFunc,
    data_type: &DataType,
    num_rows: usize,
) -> Result<ArrayRef> {
    match agg_func {
        RollingAggFunc::Count => {
            // Count: for row i, count = min(i + 1, window_size)
            let mut out = vec![0i64; num_rows];
            for i in 0..num_rows {
                out[i] = std::cmp::min(i + 1, window_size) as i64;
            }
            Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
        }
        RollingAggFunc::Avg => {
            // Avg always returns Float64
            let f64_values = column_to_f64(column, data_type)?;
            let out = cpu_rolling_avg(&f64_values, window_size, num_rows);
            Ok(Arc::new(Float64Array::from(out)) as ArrayRef)
        }
        RollingAggFunc::Sum => {
            // Sum preserves type (i32→i64 promotion)
            match data_type {
                DataType::Int64 => {
                    let arr = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                        DataFusionError::Internal("Expected Int64Array".to_string())
                    })?;
                    let out = cpu_rolling_sum_i64(arr.values(), window_size, num_rows);
                    Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
                }
                DataType::Int32 => {
                    // i32→i64 promotion for sum
                    let arr = column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                        DataFusionError::Internal("Expected Int32Array".to_string())
                    })?;
                    let vals: Vec<i64> = arr.values().iter().map(|&v| v as i64).collect();
                    let out = cpu_rolling_sum_i64(&vals, window_size, num_rows);
                    Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
                }
                DataType::Float64 => {
                    let arr = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                        DataFusionError::Internal("Expected Float64Array".to_string())
                    })?;
                    let out = cpu_rolling_sum_f64(arr.values(), window_size, num_rows);
                    Ok(Arc::new(Float64Array::from(out)) as ArrayRef)
                }
                DataType::Float32 => {
                    let arr = column.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                        DataFusionError::Internal("Expected Float32Array".to_string())
                    })?;
                    let vals: Vec<f64> = arr.values().iter().map(|&v| v as f64).collect();
                    let out = cpu_rolling_sum_f64(&vals, window_size, num_rows);
                    // Return as Float64 (Float32 sum promotes to Float64 for precision)
                    Ok(Arc::new(Float64Array::from(out)) as ArrayRef)
                }
                _ => Err(DataFusionError::Internal(format!(
                    "Unsupported type for CPU rolling sum: {:?}", data_type
                ))),
            }
        }
        RollingAggFunc::Min => {
            match data_type {
                DataType::Int64 => {
                    let arr = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                        DataFusionError::Internal("Expected Int64Array".to_string())
                    })?;
                    let out = cpu_rolling_minmax(arr.values(), window_size, num_rows, true);
                    Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
                }
                DataType::Int32 => {
                    let arr = column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                        DataFusionError::Internal("Expected Int32Array".to_string())
                    })?;
                    let out = cpu_rolling_minmax(arr.values(), window_size, num_rows, true);
                    Ok(Arc::new(Int32Array::from(out)) as ArrayRef)
                }
                DataType::Float64 => {
                    let arr = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                        DataFusionError::Internal("Expected Float64Array".to_string())
                    })?;
                    let out = cpu_rolling_minmax_f64(arr.values(), window_size, num_rows, true);
                    Ok(Arc::new(Float64Array::from(out)) as ArrayRef)
                }
                DataType::Float32 => {
                    let arr = column.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                        DataFusionError::Internal("Expected Float32Array".to_string())
                    })?;
                    let out = cpu_rolling_minmax_f32(arr.values(), window_size, num_rows, true);
                    Ok(Arc::new(Float32Array::from(out)) as ArrayRef)
                }
                _ => Err(DataFusionError::Internal(format!(
                    "Unsupported type for CPU rolling min: {:?}", data_type
                ))),
            }
        }
        RollingAggFunc::Max => {
            match data_type {
                DataType::Int64 => {
                    let arr = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                        DataFusionError::Internal("Expected Int64Array".to_string())
                    })?;
                    let out = cpu_rolling_minmax(arr.values(), window_size, num_rows, false);
                    Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
                }
                DataType::Int32 => {
                    let arr = column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                        DataFusionError::Internal("Expected Int32Array".to_string())
                    })?;
                    let out = cpu_rolling_minmax(arr.values(), window_size, num_rows, false);
                    Ok(Arc::new(Int32Array::from(out)) as ArrayRef)
                }
                DataType::Float64 => {
                    let arr = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                        DataFusionError::Internal("Expected Float64Array".to_string())
                    })?;
                    let out = cpu_rolling_minmax_f64(arr.values(), window_size, num_rows, false);
                    Ok(Arc::new(Float64Array::from(out)) as ArrayRef)
                }
                DataType::Float32 => {
                    let arr = column.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                        DataFusionError::Internal("Expected Float32Array".to_string())
                    })?;
                    let out = cpu_rolling_minmax_f32(arr.values(), window_size, num_rows, false);
                    Ok(Arc::new(Float32Array::from(out)) as ArrayRef)
                }
                _ => Err(DataFusionError::Internal(format!(
                    "Unsupported type for CPU rolling max: {:?}", data_type
                ))),
            }
        }
    }
}

/// CPU rolling sum for i64 values.
fn cpu_rolling_sum_i64(input: &[i64], window_size: usize, num_rows: usize) -> Vec<i64> {
    let mut out = vec![0i64; num_rows];
    let mut sum: i64 = 0;
    for i in 0..num_rows {
        sum += input[i];
        if i >= window_size {
            sum -= input[i - window_size];
        }
        out[i] = sum;
    }
    out
}

/// CPU rolling sum for f64 values.
fn cpu_rolling_sum_f64(input: &[f64], window_size: usize, num_rows: usize) -> Vec<f64> {
    let mut out = vec![0.0f64; num_rows];
    let mut sum: f64 = 0.0;
    for i in 0..num_rows {
        sum += input[i];
        if i >= window_size {
            sum -= input[i - window_size];
        }
        out[i] = sum;
    }
    out
}

/// CPU rolling average for f64 values.
fn cpu_rolling_avg(input: &[f64], window_size: usize, num_rows: usize) -> Vec<f64> {
    let mut out = vec![0.0f64; num_rows];
    let mut sum: f64 = 0.0;
    for i in 0..num_rows {
        sum += input[i];
        if i >= window_size {
            sum -= input[i - window_size];
        }
        let count = std::cmp::min(i + 1, window_size) as f64;
        out[i] = sum / count;
    }
    out
}

/// CPU rolling min/max for i64/i32 (types implementing Ord).
fn cpu_rolling_minmax<T: Copy + Ord>(
    input: &[T],
    window_size: usize,
    num_rows: usize,
    is_min: bool,
) -> Vec<T> {
    let mut out = Vec::with_capacity(num_rows);
    for i in 0..num_rows {
        let start = if i >= window_size { i - window_size + 1 } else { 0 };
        let window = &input[start..=i];
        let val = if is_min {
            *window.iter().min().unwrap()
        } else {
            *window.iter().max().unwrap()
        };
        out.push(val);
    }
    out
}

/// CPU rolling min/max for f64 (uses partial_cmp for floats).
fn cpu_rolling_minmax_f64(
    input: &[f64],
    window_size: usize,
    num_rows: usize,
    is_min: bool,
) -> Vec<f64> {
    let mut out = Vec::with_capacity(num_rows);
    for i in 0..num_rows {
        let start = if i >= window_size { i - window_size + 1 } else { 0 };
        let window = &input[start..=i];
        let val = if is_min {
            window.iter().copied().reduce(f64::min).unwrap()
        } else {
            window.iter().copied().reduce(f64::max).unwrap()
        };
        out.push(val);
    }
    out
}

/// CPU rolling min/max for f32 (uses partial_cmp for floats).
fn cpu_rolling_minmax_f32(
    input: &[f32],
    window_size: usize,
    num_rows: usize,
    is_min: bool,
) -> Vec<f32> {
    let mut out = Vec::with_capacity(num_rows);
    for i in 0..num_rows {
        let start = if i >= window_size { i - window_size + 1 } else { 0 };
        let window = &input[start..=i];
        let val = if is_min {
            window.iter().copied().reduce(f32::min).unwrap()
        } else {
            window.iter().copied().reduce(f32::max).unwrap()
        };
        out.push(val);
    }
    out
}

    out
}

// ============================================================================
// GPU Cumulative Sum (Prefix Sum) Execution
// ============================================================================

/// Execute cumulative sum on GPU using Blelloch prefix sum kernels.
///
/// Algorithm (multi-block):
/// 1. Each thread block computes a local inclusive prefix sum in shared memory.
/// 2. Block totals are extracted into a `block_sums` array.
/// 3. A sequential prefix sum is computed over `block_sums` on CPU (cheap, few blocks).
/// 4. Each block's elements are offset by the cumulative sum of all prior blocks.
///
/// For single-block arrays (≤ CUDA_BLOCK_SIZE elements), steps 2-4 are skipped.
fn execute_cumsum_gpu(
    column: &ArrayRef,
    data_type: &DataType,
    num_rows: usize,
) -> Result<ArrayRef> {
    let module = get_window_module().ok_or_else(|| {
        DataFusionError::Internal("GPU not available for cumulative sum".to_string())
    })?;
    let stream = get_stream().ok_or_else(|| {
        DataFusionError::Internal("CUDA stream not available".to_string())
    })?;

    match data_type {
        DataType::Int64 => {
            let arr = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Int64Array".to_string())
            })?;
            let values = gpu_prefix_sum_typed::<i64>(
                arr.values(), &module, &stream, num_rows,
                "prefix_sum_block_i64", "prefix_sum_propagate_i64",
            )?;
            Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
        }
        DataType::Float64 => {
            let arr = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Float64Array".to_string())
            })?;
            let values = gpu_prefix_sum_typed::<f64>(
                arr.values(), &module, &stream, num_rows,
                "prefix_sum_block_f64", "prefix_sum_propagate_f64",
            )?;
            Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
        }
        DataType::Int32 => {
            // Promote i32 → i64 for cumulative sum to avoid overflow
            let arr = column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Int32Array".to_string())
            })?;
            let i64_values: Vec<i64> = arr.values().iter().map(|&v| v as i64).collect();
            let values = gpu_prefix_sum_typed::<i64>(
                &i64_values, &module, &stream, num_rows,
                "prefix_sum_block_i64", "prefix_sum_propagate_i64",
            )?;
            Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
        }
        DataType::Float32 => {
            // Promote f32 → f64 for precision
            let arr = column.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Float32Array".to_string())
            })?;
            let f64_values: Vec<f64> = arr.values().iter().map(|&v| v as f64).collect();
            let values = gpu_prefix_sum_typed::<f64>(
                &f64_values, &module, &stream, num_rows,
                "prefix_sum_block_f64", "prefix_sum_propagate_f64",
            )?;
            Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
        }
        _ => Err(DataFusionError::Internal(format!(
            "Unsupported data type for GPU cumulative sum: {:?}", data_type
        ))),
    }
}

/// Generic GPU prefix sum for a typed column.
///
/// Implements a multi-block Blelloch-style inclusive prefix sum:
/// 1. Launch `prefix_sum_block_*` with shared memory for each block.
/// 2. Read back block_sums, compute exclusive prefix sum on CPU.
/// 3. Launch `prefix_sum_propagate_*` to add block offsets.
fn gpu_prefix_sum_typed<T>(
    input_values: &[T],
    module: &Arc<CudaModule>,
    stream: &Arc<cudarc::driver::safe::CudaStream>,
    num_rows: usize,
    block_kernel_name: &str,
    propagate_kernel_name: &str,
) -> Result<Vec<T>>
where
    T: cudarc::driver::DeviceRepr + Default + Clone + Copy + std::ops::AddAssign,
{
    let n = num_rows as u32;
    let block_size = CUDA_BLOCK_SIZE;
    let num_blocks = grid_size(n);

    let shared_mem = block_size as u32 * std::mem::size_of::<T>() as u32;
    let launch_cfg = LaunchConfig {
        grid_dim: (num_blocks, 1, 1),
        block_dim: (block_size, 1, 1),
        shared_mem_bytes: shared_mem,
    };

    let block_func = module.load_function(block_kernel_name).map_err(|e| {
        DataFusionError::Internal(format!("Failed to load kernel {}: {}", block_kernel_name, e))
    })?;

    // Allocate device memory
    let d_data = stream.memcpy_htod(input_values).map_err(|e| {
        DataFusionError::Internal(format!("H2D copy failed: {}", e))
    })?;
    let d_out = stream.alloc_zeros::<T>(num_rows).map_err(|e| {
        DataFusionError::Internal(format!("GPU alloc failed: {}", e))
    })?;
    let d_block_sums = stream.alloc_zeros::<T>(num_blocks as usize).map_err(|e| {
        DataFusionError::Internal(format!("GPU alloc failed: {}", e))
    })?;

    // Phase 1: Block-level prefix sums
    unsafe {
        let mut builder = stream.launch_builder(&block_func);
        builder.arg(&d_data);
        builder.arg(&d_out);
        builder.arg(&d_block_sums);
        builder.arg(&n);
        builder.launch(launch_cfg).map_err(|e| {
            DataFusionError::Internal(format!("Block prefix sum kernel launch failed: {}", e))
        })?;
    }

    // For multi-block: compute exclusive prefix sum of block_sums on CPU, then propagate
    if num_blocks > 1 {
        let block_sums: Vec<T> = stream.memcpy_dtoh(&d_block_sums).map_err(|e| {
            DataFusionError::Internal(format!("D2H copy of block_sums failed: {}", e))
        })?;

        // Compute exclusive prefix sum of block_sums on CPU
        let mut block_prefix = vec![T::default(); num_blocks as usize];
        for i in 1..num_blocks as usize {
            block_prefix[i] = block_prefix[i - 1];
            block_prefix[i] += block_sums[i - 1];
        }

        // Upload block_prefix to GPU
        let d_block_prefix = stream.memcpy_htod(&block_prefix).map_err(|e| {
            DataFusionError::Internal(format!("H2D copy of block_prefix failed: {}", e))
        })?;

        // Phase 2: Propagate block sums
        let propagate_func = module.load_function(propagate_kernel_name).map_err(|e| {
            DataFusionError::Internal(format!("Failed to load kernel {}: {}", propagate_kernel_name, e))
        })?;

        let propagate_cfg = LaunchConfig {
            grid_dim: (num_blocks, 1, 1),
            block_dim: (block_size, 1, 1),
            shared_mem_bytes: 0,
        };

        unsafe {
            let mut builder = stream.launch_builder(&propagate_func);
            builder.arg(&d_out);
            builder.arg(&d_block_prefix);
            builder.arg(&n);
            builder.launch(propagate_cfg).map_err(|e| {
                DataFusionError::Internal(format!("Propagate kernel launch failed: {}", e))
            })?;
        }
    }

    // Copy results back
    let out_values = stream.memcpy_dtoh(&d_out).map_err(|e| {
        DataFusionError::Internal(format!("D2H copy failed: {}", e))
    })?;

    Ok(out_values)
}

// ============================================================================
// CPU Cumulative Sum Fallback
// ============================================================================

/// CPU fallback for cumulative sum (prefix sum).
///
/// Simple sequential scan: `out[0] = in[0]; out[i] = out[i-1] + in[i]`.
/// All outputs are valid (no nulls).
fn execute_cumsum_cpu(
    column: &ArrayRef,
    data_type: &DataType,
    num_rows: usize,
) -> Result<ArrayRef> {
    match data_type {
        DataType::Int64 => {
            let arr = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Int64Array".to_string())
            })?;
            let out = cpu_prefix_sum_i64(arr.values(), num_rows);
            Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
        }
        DataType::Float64 => {
            let arr = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Float64Array".to_string())
            })?;
            let out = cpu_prefix_sum_f64(arr.values(), num_rows);
            Ok(Arc::new(Float64Array::from(out)) as ArrayRef)
        }
        DataType::Int32 => {
            // Promote i32→i64
            let arr = column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Int32Array".to_string())
            })?;
            let vals: Vec<i64> = arr.values().iter().map(|&v| v as i64).collect();
            let out = cpu_prefix_sum_i64(&vals, num_rows);
            Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
        }
        DataType::Float32 => {
            // Promote f32→f64
            let arr = column.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Float32Array".to_string())
            })?;
            let vals: Vec<f64> = arr.values().iter().map(|&v| v as f64).collect();
            let out = cpu_prefix_sum_f64(&vals, num_rows);
            Ok(Arc::new(Float64Array::from(out)) as ArrayRef)
        }
        _ => Err(DataFusionError::Internal(format!(
            "Unsupported type for CPU cumulative sum: {:?}", data_type
        ))),
    }
}

/// CPU prefix sum for i64 values.
fn cpu_prefix_sum_i64(input: &[i64], num_rows: usize) -> Vec<i64> {
    let mut out = Vec::with_capacity(num_rows);
    let mut acc: i64 = 0;
    for i in 0..num_rows {
        acc += input[i];
        out.push(acc);
    }
    out
}

/// CPU prefix sum for f64 values.
fn cpu_prefix_sum_f64(input: &[f64], num_rows: usize) -> Vec<f64> {
    let mut out = Vec::with_capacity(num_rows);
    let mut acc: f64 = 0.0;
    for i in 0..num_rows {
        acc += input[i];
        out.push(acc);
    }
    out
}

// ============================================================================
// Utility Functions
// ============================================================================

/// Build a `NullBuffer` from a u8 validity mask (0 = null, 1 = valid).
fn build_null_buffer(valid_mask: &[u8]) -> NullBuffer {
    let bools: Vec<bool> = valid_mask.iter().map(|&v| v != 0).collect();
    NullBuffer::from(bools)
}

/// Check if a data type is supported for GPU window operations.
pub fn is_window_gpu_supported_type(dt: &DataType) -> bool {
    matches!(dt, DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64)
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, Float64Array};

    #[test]
    fn test_cpu_shift_lag_positive() {
        // lag(col, 2): out[i] = data[i-2], first 2 rows are null
        let input = vec![10i64, 20, 30, 40, 50];
        let result = cpu_shift_typed(&input, 2, 5, |vals, nulls| {
            Arc::new(Int64Array::new(vals.into(), Some(nulls))) as ArrayRef
        });
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(arr.is_null(0));
        assert!(arr.is_null(1));
        assert_eq!(arr.value(2), 10);
        assert_eq!(arr.value(3), 20);
        assert_eq!(arr.value(4), 30);
    }

    #[test]
    fn test_cpu_shift_lead_negative() {
        // lead(col, 1) = shift(col, -1): out[i] = data[i+1], last row is null
        let input = vec![10i64, 20, 30, 40, 50];
        let result = cpu_shift_typed(&input, -1, 5, |vals, nulls| {
            Arc::new(Int64Array::new(vals.into(), Some(nulls))) as ArrayRef
        });
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 20);
        assert_eq!(arr.value(1), 30);
        assert_eq!(arr.value(2), 40);
        assert_eq!(arr.value(3), 50);
        assert!(arr.is_null(4));
    }

    #[test]
    fn test_cpu_shift_zero() {
        // shift(0): identity
        let input = vec![10i64, 20, 30];
        let result = cpu_shift_typed(&input, 0, 3, |vals, nulls| {
            Arc::new(Int64Array::new(vals.into(), Some(nulls))) as ArrayRef
        });
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 10);
        assert_eq!(arr.value(1), 20);
        assert_eq!(arr.value(2), 30);
    }

    #[test]
    fn test_cpu_diff_period_1() {
        let input = vec![10i64, 30, 60, 100];
        let result = cpu_diff_typed_i64(&input, 1, 4);
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(arr.is_null(0));
        assert_eq!(arr.value(1), 20);  // 30 - 10
        assert_eq!(arr.value(2), 30);  // 60 - 30
        assert_eq!(arr.value(3), 40);  // 100 - 60
    }

    #[test]
    fn test_cpu_diff_period_2() {
        let input = vec![10i64, 20, 35, 55, 80];
        let result = cpu_diff_typed_i64(&input, 2, 5);
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(arr.is_null(0));
        assert!(arr.is_null(1));
        assert_eq!(arr.value(2), 25);  // 35 - 10
        assert_eq!(arr.value(3), 35);  // 55 - 20
        assert_eq!(arr.value(4), 45);  // 80 - 35
    }

    #[test]
    fn test_cpu_diff_f64() {
        let input = vec![1.0f64, 2.5, 4.5, 8.0];
        let result = cpu_diff_typed_f64(&input, 1, 4);
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(arr.is_null(0));
        assert!((arr.value(1) - 1.5).abs() < 1e-10);
        assert!((arr.value(2) - 2.0).abs() < 1e-10);
        assert!((arr.value(3) - 3.5).abs() < 1e-10);
    }

    #[test]
    fn test_build_null_buffer() {
        let mask = vec![1u8, 0, 1, 1, 0];
        let nb = build_null_buffer(&mask);
        assert!(nb.is_valid(0));
        assert!(!nb.is_valid(1));
        assert!(nb.is_valid(2));
        assert!(nb.is_valid(3));
        assert!(!nb.is_valid(4));
    }

    #[test]
    fn test_window_op_output_name() {
        let op = GpuWindowOp::Shift {
            column_index: 0,
            column_name: "price".to_string(),
            offset: 1,
            output_name: "prev_price".to_string(),
            data_type: DataType::Float64,
        };
        assert_eq!(op.output_name(), "prev_price");

        let op2 = GpuWindowOp::Diff {
            column_index: 0,
            column_name: "price".to_string(),
            period: 1,
            output_name: "price_change".to_string(),
            data_type: DataType::Float64,
        };
        assert_eq!(op2.output_name(), "price_change");
    }

    #[test]
    fn test_is_window_gpu_supported_type() {
        assert!(is_window_gpu_supported_type(&DataType::Int64));
        assert!(is_window_gpu_supported_type(&DataType::Float64));
        assert!(is_window_gpu_supported_type(&DataType::Int32));
        assert!(is_window_gpu_supported_type(&DataType::Float32));
        assert!(!is_window_gpu_supported_type(&DataType::Utf8));
        assert!(!is_window_gpu_supported_type(&DataType::Boolean));
    }

    #[test]
    fn test_shift_large_offset() {
        // Offset larger than array: all null
        let input = vec![10i64, 20, 30];
        let result = cpu_shift_typed(&input, 10, 3, |vals, nulls| {
            Arc::new(Int64Array::new(vals.into(), Some(nulls))) as ArrayRef
        });
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(arr.is_null(0));
        assert!(arr.is_null(1));
        assert!(arr.is_null(2));
    }

    // ========================================================================
    // Rolling CPU tests
    // ========================================================================

    #[test]
    fn test_cpu_rolling_sum_i64() {
        // Window size 3: [10], [10,20], [10,20,30], [20,30,40], [30,40,50]
        let input = vec![10i64, 20, 30, 40, 50];
        let out = cpu_rolling_sum_i64(&input, 3, 5);
        assert_eq!(out, vec![10, 30, 60, 90, 120]);
    }

    #[test]
    fn test_cpu_rolling_sum_f64() {
        let input = vec![1.0f64, 2.0, 3.0, 4.0, 5.0];
        let out = cpu_rolling_sum_f64(&input, 2, 5);
        // Window 2: [1.0], [1.0,2.0], [2.0,3.0], [3.0,4.0], [4.0,5.0]
        assert!((out[0] - 1.0).abs() < 1e-10);
        assert!((out[1] - 3.0).abs() < 1e-10);
        assert!((out[2] - 5.0).abs() < 1e-10);
        assert!((out[3] - 7.0).abs() < 1e-10);
        assert!((out[4] - 9.0).abs() < 1e-10);
    }

    #[test]
    fn test_cpu_rolling_avg() {
        let input = vec![10.0f64, 20.0, 30.0, 40.0, 50.0];
        let out = cpu_rolling_avg(&input, 3, 5);
        // Window 3 avg: [10/1=10], [30/2=15], [60/3=20], [90/3=30], [120/3=40]
        assert!((out[0] - 10.0).abs() < 1e-10);
        assert!((out[1] - 15.0).abs() < 1e-10);
        assert!((out[2] - 20.0).abs() < 1e-10);
        assert!((out[3] - 30.0).abs() < 1e-10);
        assert!((out[4] - 40.0).abs() < 1e-10);
    }

    #[test]
    fn test_cpu_rolling_min_i64() {
        let input = vec![30i64, 10, 50, 20, 40];
        let out = cpu_rolling_minmax(&input, 3, 5, true);
        // Window 3 min: [30], [10,30], [10,30,50], [10,20,50], [20,40,50]
        assert_eq!(out, vec![30, 10, 10, 10, 20]);
    }

    #[test]
    fn test_cpu_rolling_max_i64() {
        let input = vec![30i64, 10, 50, 20, 40];
        let out = cpu_rolling_minmax(&input, 3, 5, false);
        // Window 3 max: [30], [10,30], [10,30,50], [10,20,50], [20,40,50]
        assert_eq!(out, vec![30, 30, 50, 50, 50]);
    }

    #[test]
    fn test_cpu_rolling_min_f64() {
        let input = vec![3.0f64, 1.0, 5.0, 2.0, 4.0];
        let out = cpu_rolling_minmax_f64(&input, 2, 5, true);
        // Window 2 min: [3], [1,3], [1,5], [2,5], [2,4]
        assert!((out[0] - 3.0).abs() < 1e-10);
        assert!((out[1] - 1.0).abs() < 1e-10);
        assert!((out[2] - 1.0).abs() < 1e-10);
        assert!((out[3] - 2.0).abs() < 1e-10);
        assert!((out[4] - 2.0).abs() < 1e-10);
    }

    #[test]
    fn test_cpu_rolling_max_f64() {
        let input = vec![3.0f64, 1.0, 5.0, 2.0, 4.0];
        let out = cpu_rolling_minmax_f64(&input, 2, 5, false);
        // Window 2 max: [3], [1,3], [1,5], [2,5], [2,4]
        assert!((out[0] - 3.0).abs() < 1e-10);
        assert!((out[1] - 3.0).abs() < 1e-10);
        assert!((out[2] - 5.0).abs() < 1e-10);
        assert!((out[3] - 5.0).abs() < 1e-10);
        assert!((out[4] - 4.0).abs() < 1e-10);
    }

    #[test]
    fn test_cpu_rolling_count() {
        // Count via execute_rolling_cpu
        let input = Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])) as ArrayRef;
        let result = execute_rolling_cpu(&input, 3, RollingAggFunc::Count, &DataType::Int64, 5).unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 1);
        assert_eq!(arr.value(1), 2);
        assert_eq!(arr.value(2), 3);
        assert_eq!(arr.value(3), 3);
        assert_eq!(arr.value(4), 3);
    }

    #[test]
    fn test_cpu_rolling_sum_via_execute() {
        // Sum via execute_rolling_cpu
        let input = Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])) as ArrayRef;
        let result = execute_rolling_cpu(&input, 3, RollingAggFunc::Sum, &DataType::Int64, 5).unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 10);
        assert_eq!(arr.value(1), 30);
        assert_eq!(arr.value(2), 60);
        assert_eq!(arr.value(3), 90);
        assert_eq!(arr.value(4), 120);
    }

    #[test]
    fn test_cpu_rolling_avg_via_execute() {
        // Avg via execute_rolling_cpu always returns Float64
        let input = Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef;
        let result = execute_rolling_cpu(&input, 2, RollingAggFunc::Avg, &DataType::Int64, 3).unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((arr.value(0) - 10.0).abs() < 1e-10);   // [10] / 1
        assert!((arr.value(1) - 15.0).abs() < 1e-10);   // [10,20] / 2
        assert!((arr.value(2) - 25.0).abs() < 1e-10);   // [20,30] / 2
    }

    #[test]
    fn test_cpu_rolling_window_size_1() {
        // Window size 1: each output equals the input
        let input = vec![10i64, 20, 30];
        let out = cpu_rolling_sum_i64(&input, 1, 3);
        assert_eq!(out, vec![10, 20, 30]);
    }

    #[test]
    fn test_cpu_rolling_window_larger_than_data() {
        // Window size larger than data: includes all preceding rows
        let input = vec![10i64, 20, 30];
        let out = cpu_rolling_sum_i64(&input, 10, 3);
        assert_eq!(out, vec![10, 30, 60]);
    }

    #[test]
    fn test_rolling_op_output_name() {
        let op = GpuWindowOp::Rolling {
            column_index: 0,
            column_name: "price".to_string(),
            window_size: 3,
            agg_func: RollingAggFunc::Avg,
            output_name: "avg_price".to_string(),
            data_type: DataType::Float64,
        };
        assert_eq!(op.output_name(), "avg_price");
    }

    #[test]
    fn test_rolling_agg_func_display() {
        assert_eq!(format!("{}", RollingAggFunc::Sum), "sum");
        assert_eq!(format!("{}", RollingAggFunc::Avg), "avg");
        assert_eq!(format!("{}", RollingAggFunc::Min), "min");
        assert_eq!(format!("{}", RollingAggFunc::Max), "max");
        assert_eq!(format!("{}", RollingAggFunc::Count), "count");
    }

    // ========================================================================
    // Cumulative Sum (Prefix Sum) CPU tests
    // ========================================================================

    #[test]
    fn test_cpu_prefix_sum_i64() {
        let input = vec![10i64, 20, 30, 40, 50];
        let out = cpu_prefix_sum_i64(&input, 5);
        assert_eq!(out, vec![10, 30, 60, 100, 150]);
    }

    #[test]
    fn test_cpu_prefix_sum_f64() {
        let input = vec![1.0f64, 2.5, 3.5, 4.0];
        let out = cpu_prefix_sum_f64(&input, 4);
        assert!((out[0] - 1.0).abs() < 1e-10);
        assert!((out[1] - 3.5).abs() < 1e-10);
        assert!((out[2] - 7.0).abs() < 1e-10);
        assert!((out[3] - 11.0).abs() < 1e-10);
    }

    #[test]
    fn test_cpu_prefix_sum_single_element() {
        let input = vec![42i64];
        let out = cpu_prefix_sum_i64(&input, 1);
        assert_eq!(out, vec![42]);
    }

    #[test]
    fn test_cpu_prefix_sum_zeros() {
        let input = vec![0i64, 0, 0, 0];
        let out = cpu_prefix_sum_i64(&input, 4);
        assert_eq!(out, vec![0, 0, 0, 0]);
    }

    #[test]
    fn test_cpu_prefix_sum_negative() {
        let input = vec![10i64, -3, 5, -2, 1];
        let out = cpu_prefix_sum_i64(&input, 5);
        assert_eq!(out, vec![10, 7, 12, 10, 11]);
    }

    #[test]
    fn test_cumsum_via_execute_cpu_i64() {
        let input = Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])) as ArrayRef;
        let result = execute_cumsum_cpu(&input, &DataType::Int64, 5).unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 10);
        assert_eq!(arr.value(1), 30);
        assert_eq!(arr.value(2), 60);
        assert_eq!(arr.value(3), 100);
        assert_eq!(arr.value(4), 150);
        // No nulls in cumulative sum
        assert_eq!(arr.null_count(), 0);
    }

    #[test]
    fn test_cumsum_via_execute_cpu_f64() {
        let input = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef;
        let result = execute_cumsum_cpu(&input, &DataType::Float64, 3).unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((arr.value(0) - 1.0).abs() < 1e-10);
        assert!((arr.value(1) - 3.0).abs() < 1e-10);
        assert!((arr.value(2) - 6.0).abs() < 1e-10);
    }

    #[test]
    fn test_cumsum_via_execute_cpu_i32_promotes_to_i64() {
        let input = Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef;
        let result = execute_cumsum_cpu(&input, &DataType::Int32, 3).unwrap();
        // Should produce Int64 (promoted)
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 10);
        assert_eq!(arr.value(1), 30);
        assert_eq!(arr.value(2), 60);
    }

    #[test]
    fn test_cumsum_op_output_name() {
        let op = GpuWindowOp::CumulativeSum {
            column_index: 0,
            column_name: "volume".to_string(),
            output_name: "volume_cumsum".to_string(),
            data_type: DataType::Int64,
        };
        assert_eq!(op.output_name(), "volume_cumsum");
    }

    #[test]
    fn test_cumsum_op_data_type() {
        // Int64 → Int64
        let op = GpuWindowOp::CumulativeSum {
            column_index: 0,
            column_name: "v".to_string(),
            output_name: "cs".to_string(),
            data_type: DataType::Int64,
        };
        assert_eq!(op.data_type(), DataType::Int64);

        // Int32 → Int64 (promoted)
        let op2 = GpuWindowOp::CumulativeSum {
            column_index: 0,
            column_name: "v".to_string(),
            output_name: "cs".to_string(),
            data_type: DataType::Int32,
        };
        assert_eq!(op2.data_type(), DataType::Int64);

        // Float64 → Float64
        let op3 = GpuWindowOp::CumulativeSum {
            column_index: 0,
            column_name: "v".to_string(),
            output_name: "cs".to_string(),
            data_type: DataType::Float64,
        };
        assert_eq!(op3.data_type(), DataType::Float64);
    }
}
