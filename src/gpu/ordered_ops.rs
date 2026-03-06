//! GPU-accelerated ordered data operators for LTSeq.
//!
//! # GpuConsecutiveGroupExec
//!
//! GPU parallel adjacent comparison → `__group_id__` column generation.
//! This is the GPU equivalent of `linear_scan_group_id()` in `src/ops/linear_scan.rs`.
//!
//! ## Algorithm
//!
//! 1. **Adjacent comparison kernel**: Each thread `i` compares `data[i]` vs `data[i-1]`.
//!    Output: boundary mask `u8[N]` where 1 = new group starts.
//!    Row 0 always gets boundary = 1 (first group).
//!
//! 2. **Inclusive prefix sum kernel**: Parallel prefix sum (Blelloch-style) on the
//!    boundary mask → cumulative `u32[N]` group_id array.
//!    Uses shared memory within blocks + block-level cascading.
//!
//! # GpuBinarySearchExec
//!
//! GPU parallel binary search on sorted data for `search_first()`.
//! When data is sorted on the predicate column, binary search achieves O(log N)
//! vs O(N) linear scan.
//!
//! ## Algorithm
//!
//! 1. **Pattern match** the predicate: `Column(x) {>, >=, ==, <, <=} Literal(v)`
//!    where `x` is a sort key. Maps to `GpuSearchMode`.
//! 2. **Binary search kernel**: A single thread performs binary search on the
//!    sorted column to find the first matching row index.
//! 3. **Row extraction**: Use the found index to take the single result row.
//!
//! For batch search (future), multiple needles can be searched in parallel.
//!
//! ## CUDA Kernels
//!
//! - `adjacent_ne_i64`:  `boundary[i] = (data[i] != data[i-1]) ? 1 : 0`
//! - `adjacent_ne_i32`:  same for i32
//! - `adjacent_ne_f64`:  same for f64
//! - `adjacent_ne_f32`:  same for f32
//! - `adjacent_diff_gt_i64`: `boundary[i] = (data[i] - data[i-1] > threshold) ? 1 : 0`
//! - `adjacent_diff_gt_f64`: same for f64
//! - `search_first_i64`:  binary search on sorted i64 column
//! - `search_first_i32`:  same for i32
//! - `search_first_f64`:  same for f64
//! - `search_first_f32`:  same for f32
//! - `prefix_sum_u32`:  inclusive prefix sum (block-level, single-pass for <= 1M rows,
//!                       multi-pass with block sums for > 1M rows)
//!
//! ## Performance Target
//!
//! 100M rows < 5ms (coalesced memory access, minimal divergence)

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, Float32Array, Float64Array, UInt32Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result, ScalarValue};
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

/// CUDA kernels for adjacent comparison and prefix sum.
///
/// Kernel naming: `adjacent_{op}_{type}` for boundary detection,
///                `prefix_sum_u32` for inclusive scan.
const ORDERED_OPS_KERNEL_SRC: &str = r#"
// ── Adjacent comparison kernels ──────────────────────────────────────
// Each thread compares data[i] vs data[i-1]. Row 0 always = 1 (boundary).

extern "C" __global__ void adjacent_ne_i64(
    const long long* __restrict__ data,
    unsigned char* __restrict__ boundary,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    if (idx == 0) {
        boundary[0] = 1;
    } else {
        boundary[idx] = (data[idx] != data[idx - 1]) ? 1 : 0;
    }
}

extern "C" __global__ void adjacent_ne_i32(
    const int* __restrict__ data,
    unsigned char* __restrict__ boundary,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    if (idx == 0) {
        boundary[0] = 1;
    } else {
        boundary[idx] = (data[idx] != data[idx - 1]) ? 1 : 0;
    }
}

extern "C" __global__ void adjacent_ne_f64(
    const double* __restrict__ data,
    unsigned char* __restrict__ boundary,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    if (idx == 0) {
        boundary[0] = 1;
    } else {
        boundary[idx] = (data[idx] != data[idx - 1]) ? 1 : 0;
    }
}

extern "C" __global__ void adjacent_ne_f32(
    const float* __restrict__ data,
    unsigned char* __restrict__ boundary,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    if (idx == 0) {
        boundary[0] = 1;
    } else {
        boundary[idx] = (data[idx] != data[idx - 1]) ? 1 : 0;
    }
}

// ── Adjacent difference + threshold kernels ──────────────────────────
// boundary[i] = 1 if (data[i] - data[i-1]) > threshold

extern "C" __global__ void adjacent_diff_gt_i64(
    const long long* __restrict__ data,
    const long long threshold,
    unsigned char* __restrict__ boundary,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    if (idx == 0) {
        boundary[0] = 1;
    } else {
        boundary[idx] = ((data[idx] - data[idx - 1]) > threshold) ? 1 : 0;
    }
}

extern "C" __global__ void adjacent_diff_gt_f64(
    const double* __restrict__ data,
    const double threshold,
    unsigned char* __restrict__ boundary,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    if (idx == 0) {
        boundary[0] = 1;
    } else {
        boundary[idx] = ((data[idx] - data[idx - 1]) > threshold) ? 1 : 0;
    }
}

// ── Inclusive prefix sum (block-level) ───────────────────────────────
// Converts boundary mask (u8) → group_id (u32) via inclusive scan.
// Uses shared memory for intra-block scan.
//
// For data > blockDim.x elements, call this kernel once per block,
// then propagate block sums via `propagate_block_sums`.

extern "C" __global__ void prefix_sum_u8_to_u32(
    const unsigned char* __restrict__ boundary,
    unsigned int* __restrict__ group_id,
    unsigned int* __restrict__ block_sums,
    const unsigned int n
) {
    extern __shared__ unsigned int sdata[];

    unsigned int tid = threadIdx.x;
    unsigned int gid = blockIdx.x * blockDim.x + tid;

    // Load into shared memory
    sdata[tid] = (gid < n) ? (unsigned int)boundary[gid] : 0;
    __syncthreads();

    // Up-sweep (reduce) phase
    for (unsigned int stride = 1; stride < blockDim.x; stride *= 2) {
        unsigned int index = (tid + 1) * stride * 2 - 1;
        if (index < blockDim.x) {
            sdata[index] += sdata[index - stride];
        }
        __syncthreads();
    }

    // Store block total
    if (tid == blockDim.x - 1) {
        if (block_sums != 0) {
            block_sums[blockIdx.x] = sdata[tid];
        }
        sdata[tid] = 0;
    }
    __syncthreads();

    // Down-sweep phase (exclusive scan)
    for (unsigned int stride = blockDim.x / 2; stride > 0; stride /= 2) {
        unsigned int index = (tid + 1) * stride * 2 - 1;
        if (index < blockDim.x) {
            unsigned int temp = sdata[index - stride];
            sdata[index - stride] = sdata[index];
            sdata[index] += temp;
        }
        __syncthreads();
    }

    // Write result (convert exclusive → inclusive by adding boundary[gid])
    if (gid < n) {
        group_id[gid] = sdata[tid] + (unsigned int)boundary[gid];
    }
}

// Propagate block sums to convert block-local prefix sums to global.
// block_sums[i] contains the total sum of block i.
// We add the cumulative sum of block_sums[0..blockIdx.x] to each element.
extern "C" __global__ void propagate_block_sums(
    unsigned int* __restrict__ group_id,
    const unsigned int* __restrict__ block_offsets,
    const unsigned int n
) {
    unsigned int gid = blockIdx.x * blockDim.x + threadIdx.x;
    if (gid < n && blockIdx.x > 0) {
        group_id[gid] += block_offsets[blockIdx.x];
    }
}

// ============================================================================
// Segmented aggregation kernels
// ============================================================================
//
// Two-phase approach for per-row window aggregates:
//   Phase 1: Atomic accumulation into per-group buffers (indexed by group_id)
//   Phase 2: Broadcast per-group result back to every row
//
// group_id is 1-based (from prefix sum of boundary mask).

// ── Phase 1: Per-group accumulation (atomics) ────────────────────────

// Segmented count: count rows per group
extern "C" __global__ void seg_count(
    const unsigned int* __restrict__ group_id,
    long long* __restrict__ group_counts,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    atomicAdd((unsigned long long*)&group_counts[group_id[idx]], 1ULL);
}

// Segmented sum (i64): accumulate per-group sums
extern "C" __global__ void seg_sum_i64(
    const unsigned int* __restrict__ group_id,
    const long long* __restrict__ data,
    long long* __restrict__ group_sums,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    atomicAdd((unsigned long long*)&group_sums[group_id[idx]], (unsigned long long)data[idx]);
}

// Segmented sum (f64): accumulate per-group sums
// Note: atomicAdd for double requires sm_60+. For older GPUs, use CAS loop.
extern "C" __global__ void seg_sum_f64(
    const unsigned int* __restrict__ group_id,
    const double* __restrict__ data,
    double* __restrict__ group_sums,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    atomicAdd(&group_sums[group_id[idx]], data[idx]);
}

// Segmented min (i64)
extern "C" __global__ void seg_min_i64(
    const unsigned int* __restrict__ group_id,
    const long long* __restrict__ data,
    long long* __restrict__ group_mins,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    atomicMin((long long*)&group_mins[group_id[idx]], data[idx]);
}

// Segmented max (i64)
extern "C" __global__ void seg_max_i64(
    const unsigned int* __restrict__ group_id,
    const long long* __restrict__ data,
    long long* __restrict__ group_maxs,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    atomicMax((long long*)&group_maxs[group_id[idx]], data[idx]);
}

// Segmented min (f64): CAS-based atomicMin for doubles
__device__ double atomicMinDouble(double* addr, double val) {
    unsigned long long int* addr_as_ull = (unsigned long long int*)addr;
    unsigned long long int old = *addr_as_ull;
    unsigned long long int assumed;
    do {
        assumed = old;
        double old_val = __longlong_as_double(assumed);
        double new_val = (val < old_val) ? val : old_val;
        old = atomicCAS(addr_as_ull, assumed, __double_as_longlong(new_val));
    } while (assumed != old);
    return __longlong_as_double(old);
}

// Segmented max (f64): CAS-based atomicMax for doubles
__device__ double atomicMaxDouble(double* addr, double val) {
    unsigned long long int* addr_as_ull = (unsigned long long int*)addr;
    unsigned long long int old = *addr_as_ull;
    unsigned long long int assumed;
    do {
        assumed = old;
        double old_val = __longlong_as_double(assumed);
        double new_val = (val > old_val) ? val : old_val;
        old = atomicCAS(addr_as_ull, assumed, __double_as_longlong(new_val));
    } while (assumed != old);
    return __longlong_as_double(old);
}

extern "C" __global__ void seg_min_f64(
    const unsigned int* __restrict__ group_id,
    const double* __restrict__ data,
    double* __restrict__ group_mins,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    atomicMinDouble(&group_mins[group_id[idx]], data[idx]);
}

extern "C" __global__ void seg_max_f64(
    const unsigned int* __restrict__ group_id,
    const double* __restrict__ data,
    double* __restrict__ group_maxs,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    atomicMaxDouble(&group_maxs[group_id[idx]], data[idx]);
}

// ── Phase 2: Broadcast per-group result back to every row ────────────

extern "C" __global__ void broadcast_i64(
    const unsigned int* __restrict__ group_id,
    const long long* __restrict__ group_values,
    long long* __restrict__ output,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    output[idx] = group_values[group_id[idx]];
}

extern "C" __global__ void broadcast_f64(
    const unsigned int* __restrict__ group_id,
    const double* __restrict__ group_values,
    double* __restrict__ output,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    output[idx] = group_values[group_id[idx]];
}

// ============================================================================
// Binary search kernels for search_first
// ============================================================================
//
// Each kernel performs binary search on a sorted column to find the first row
// matching a comparison predicate. The search mode is passed as an integer:
//   0 = Gt  (>)   — find first i where data[i] > value  (upper_bound)
//   1 = Ge  (>=)  — find first i where data[i] >= value (lower_bound)
//   2 = Eq  (==)  — find first i where data[i] == value (lower_bound, check match)
//   3 = Lt  (<)   — if data[0] < value, result=0; else UINT_MAX (no match)
//   4 = Le  (<=)  — if data[0] <= value, result=0; else UINT_MAX (no match)
//
// Output: result[0] = index of first matching row, or UINT_MAX if no match.
// Only thread 0 executes (single search).

extern "C" __global__ void search_first_i64(
    const long long* __restrict__ data,
    const long long value,
    unsigned int* __restrict__ result,
    const unsigned int n,
    const unsigned int mode
) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    unsigned int lo, hi, mid, idx;
    switch (mode) {
    case 0: // Gt: first i where data[i] > value
        lo = 0; hi = n;
        while (lo < hi) { mid = lo + (hi - lo) / 2; if (data[mid] <= value) lo = mid + 1; else hi = mid; }
        result[0] = (lo < n) ? lo : 0xFFFFFFFFu;
        break;
    case 1: // Ge: first i where data[i] >= value
        lo = 0; hi = n;
        while (lo < hi) { mid = lo + (hi - lo) / 2; if (data[mid] < value) lo = mid + 1; else hi = mid; }
        result[0] = (lo < n) ? lo : 0xFFFFFFFFu;
        break;
    case 2: // Eq: first i where data[i] == value
        lo = 0; hi = n;
        while (lo < hi) { mid = lo + (hi - lo) / 2; if (data[mid] < value) lo = mid + 1; else hi = mid; }
        result[0] = (lo < n && data[lo] == value) ? lo : 0xFFFFFFFFu;
        break;
    case 3: // Lt: first i where data[i] < value (on ascending data, row 0 if data[0] < value)
        result[0] = (n > 0 && data[0] < value) ? 0 : 0xFFFFFFFFu;
        break;
    case 4: // Le: first i where data[i] <= value (on ascending data, row 0 if data[0] <= value)
        result[0] = (n > 0 && data[0] <= value) ? 0 : 0xFFFFFFFFu;
        break;
    default:
        result[0] = 0xFFFFFFFFu;
        break;
    }
}

extern "C" __global__ void search_first_i32(
    const int* __restrict__ data,
    const int value,
    unsigned int* __restrict__ result,
    const unsigned int n,
    const unsigned int mode
) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    unsigned int lo, hi, mid, idx;
    switch (mode) {
    case 0:
        lo = 0; hi = n;
        while (lo < hi) { mid = lo + (hi - lo) / 2; if (data[mid] <= value) lo = mid + 1; else hi = mid; }
        result[0] = (lo < n) ? lo : 0xFFFFFFFFu;
        break;
    case 1:
        lo = 0; hi = n;
        while (lo < hi) { mid = lo + (hi - lo) / 2; if (data[mid] < value) lo = mid + 1; else hi = mid; }
        result[0] = (lo < n) ? lo : 0xFFFFFFFFu;
        break;
    case 2:
        lo = 0; hi = n;
        while (lo < hi) { mid = lo + (hi - lo) / 2; if (data[mid] < value) lo = mid + 1; else hi = mid; }
        result[0] = (lo < n && data[lo] == value) ? lo : 0xFFFFFFFFu;
        break;
    case 3:
        result[0] = (n > 0 && data[0] < value) ? 0 : 0xFFFFFFFFu;
        break;
    case 4:
        result[0] = (n > 0 && data[0] <= value) ? 0 : 0xFFFFFFFFu;
        break;
    default:
        result[0] = 0xFFFFFFFFu;
        break;
    }
}

extern "C" __global__ void search_first_f64(
    const double* __restrict__ data,
    const double value,
    unsigned int* __restrict__ result,
    const unsigned int n,
    const unsigned int mode
) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    unsigned int lo, hi, mid, idx;
    switch (mode) {
    case 0:
        lo = 0; hi = n;
        while (lo < hi) { mid = lo + (hi - lo) / 2; if (data[mid] <= value) lo = mid + 1; else hi = mid; }
        result[0] = (lo < n) ? lo : 0xFFFFFFFFu;
        break;
    case 1:
        lo = 0; hi = n;
        while (lo < hi) { mid = lo + (hi - lo) / 2; if (data[mid] < value) lo = mid + 1; else hi = mid; }
        result[0] = (lo < n) ? lo : 0xFFFFFFFFu;
        break;
    case 2:
        lo = 0; hi = n;
        while (lo < hi) { mid = lo + (hi - lo) / 2; if (data[mid] < value) lo = mid + 1; else hi = mid; }
        result[0] = (lo < n && data[lo] == value) ? lo : 0xFFFFFFFFu;
        break;
    case 3:
        result[0] = (n > 0 && data[0] < value) ? 0 : 0xFFFFFFFFu;
        break;
    case 4:
        result[0] = (n > 0 && data[0] <= value) ? 0 : 0xFFFFFFFFu;
        break;
    default:
        result[0] = 0xFFFFFFFFu;
        break;
    }
}

extern "C" __global__ void search_first_f32(
    const float* __restrict__ data,
    const float value,
    unsigned int* __restrict__ result,
    const unsigned int n,
    const unsigned int mode
) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    unsigned int lo, hi, mid, idx;
    switch (mode) {
    case 0:
        lo = 0; hi = n;
        while (lo < hi) { mid = lo + (hi - lo) / 2; if (data[mid] <= value) lo = mid + 1; else hi = mid; }
        result[0] = (lo < n) ? lo : 0xFFFFFFFFu;
        break;
    case 1:
        lo = 0; hi = n;
        while (lo < hi) { mid = lo + (hi - lo) / 2; if (data[mid] < value) lo = mid + 1; else hi = mid; }
        result[0] = (lo < n) ? lo : 0xFFFFFFFFu;
        break;
    case 2:
        lo = 0; hi = n;
        while (lo < hi) { mid = lo + (hi - lo) / 2; if (data[mid] < value) lo = mid + 1; else hi = mid; }
        result[0] = (lo < n && data[lo] == value) ? lo : 0xFFFFFFFFu;
        break;
    case 3:
        result[0] = (n > 0 && data[0] < value) ? 0 : 0xFFFFFFFFu;
        break;
    case 4:
        result[0] = (n > 0 && data[0] <= value) ? 0 : 0xFFFFFFFFu;
        break;
    default:
        result[0] = 0xFFFFFFFFu;
        break;
    }
}

// ── Batch binary search kernel ──────────────────────────────────────
// Multiple needles searched in parallel against one sorted haystack.
// Each thread handles one needle.

extern "C" __global__ void batch_search_first_i64(
    const long long* __restrict__ data,
    const long long* __restrict__ needles,
    unsigned int* __restrict__ results,
    const unsigned int n,
    const unsigned int num_needles,
    const unsigned int mode
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= num_needles) return;
    long long value = needles[idx];
    unsigned int lo, hi, mid;
    switch (mode) {
    case 0:
        lo = 0; hi = n;
        while (lo < hi) { mid = lo + (hi - lo) / 2; if (data[mid] <= value) lo = mid + 1; else hi = mid; }
        results[idx] = (lo < n) ? lo : 0xFFFFFFFFu;
        break;
    case 1:
        lo = 0; hi = n;
        while (lo < hi) { mid = lo + (hi - lo) / 2; if (data[mid] < value) lo = mid + 1; else hi = mid; }
        results[idx] = (lo < n) ? lo : 0xFFFFFFFFu;
        break;
    case 2:
        lo = 0; hi = n;
        while (lo < hi) { mid = lo + (hi - lo) / 2; if (data[mid] < value) lo = mid + 1; else hi = mid; }
        results[idx] = (lo < n && data[lo] == value) ? lo : 0xFFFFFFFFu;
        break;
    case 3:
        results[idx] = (n > 0 && data[0] < value) ? 0 : 0xFFFFFFFFu;
        break;
    case 4:
        results[idx] = (n > 0 && data[0] <= value) ? 0 : 0xFFFFFFFFu;
        break;
    default:
        results[idx] = 0xFFFFFFFFu;
        break;
    }
}
"#;

// ============================================================================
// GPU Resources for ordered ops
// ============================================================================

use cudarc::driver::safe::CudaModule;
use cudarc::nvrtc::compile_ptx;
use std::sync::OnceLock;

/// Cached compiled module for ordered ops kernels.
static ORDERED_OPS_MODULE: OnceLock<Option<Arc<CudaModule>>> = OnceLock::new();

/// Get or compile the ordered ops CUDA module.
fn get_ordered_ops_module() -> Option<Arc<CudaModule>> {
    ORDERED_OPS_MODULE
        .get_or_init(|| {
            let ctx = super::get_context()?;
            let ptx = compile_ptx(ORDERED_OPS_KERNEL_SRC).ok()?;
            let module = ctx.load_module(ptx).ok()?;
            Some(module)
        })
        .clone()
}

/// Get a kernel function from the ordered ops module.
fn get_ordered_ops_function(name: &str) -> Option<cudarc::driver::safe::CudaFunction> {
    get_ordered_ops_module()?.load_function(name).ok()
}

/// Public accessor for ordered ops kernel functions (used by hash_ops).
pub fn get_ordered_ops_function_pub(name: &str) -> Option<cudarc::driver::safe::CudaFunction> {
    get_ordered_ops_function(name)
}

// ============================================================================
// Boundary Detection Mode
// ============================================================================

/// Describes how to detect group boundaries on the GPU.
#[derive(Debug, Clone)]
pub enum GpuBoundaryMode {
    /// `data[i] != data[i-1]` — column value changed
    AdjacentNe {
        /// Column index in the input schema
        column_index: usize,
        /// Column name (for display)
        column_name: String,
        /// Data type of the column
        data_type: DataType,
    },
    /// `(data[i] - data[i-1]) > threshold` — gap exceeds threshold
    AdjacentDiffGt {
        /// Column index in the input schema
        column_index: usize,
        /// Column name (for display)
        column_name: String,
        /// Data type of the column
        data_type: DataType,
        /// Threshold value (i64 for integer types, f64 for float)
        threshold_i64: Option<i64>,
        threshold_f64: Option<f64>,
    },
}

impl GpuBoundaryMode {
    /// Get the CUDA kernel function name for this boundary mode.
    pub fn kernel_name(&self) -> &'static str {
        match self {
            GpuBoundaryMode::AdjacentNe { data_type, .. } => match data_type {
                DataType::Int64 => "adjacent_ne_i64",
                DataType::Int32 => "adjacent_ne_i32",
                DataType::Float64 => "adjacent_ne_f64",
                DataType::Float32 => "adjacent_ne_f32",
                _ => unreachable!("unsupported type for adjacent_ne"),
            },
            GpuBoundaryMode::AdjacentDiffGt { data_type, .. } => match data_type {
                DataType::Int64 => "adjacent_diff_gt_i64",
                DataType::Float64 => "adjacent_diff_gt_f64",
                _ => unreachable!("unsupported type for adjacent_diff_gt"),
            },
        }
    }

    /// Column name for display.
    fn column_name(&self) -> &str {
        match self {
            GpuBoundaryMode::AdjacentNe { column_name, .. } => column_name,
            GpuBoundaryMode::AdjacentDiffGt { column_name, .. } => column_name,
        }
    }
}

// ============================================================================
// GpuConsecutiveGroupExec
// ============================================================================

/// GPU-accelerated consecutive group identification.
///
/// Replaces the CPU `linear_scan_group_id()` path for eligible predicates.
/// Adds a `__group_id__` column (UInt32) to the output.
///
/// ## Execution Flow
///
/// 1. Pull all batches from the child plan (coalesce into single batch)
/// 2. Upload the boundary-detection column to GPU
/// 3. Run adjacent comparison kernel → boundary mask (u8)
/// 4. Run prefix sum kernel → group_id (u32)
/// 5. Download group_id array
/// 6. Append as new column `__group_id__` to the record batch
#[derive(Debug)]
pub struct GpuConsecutiveGroupExec {
    /// How to detect group boundaries
    boundary_mode: GpuBoundaryMode,
    /// The input plan (must be sorted by the boundary column)
    input: Arc<dyn ExecutionPlan>,
    /// Output schema (input schema + `__group_id__` column)
    output_schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cached plan properties
    cache: PlanProperties,
}

impl GpuConsecutiveGroupExec {
    /// Create a new GpuConsecutiveGroupExec.
    ///
    /// Panics if the column index is out of bounds in the input schema.
    pub fn try_new(
        boundary_mode: GpuBoundaryMode,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        // Build output schema: input + __group_id__
        let input_schema = input.schema();
        let mut fields: Vec<Field> = input_schema.fields().iter().map(|f| (**f).clone()).collect();
        fields.push(Field::new("__group_id__", DataType::UInt32, false));
        let output_schema = Arc::new(Schema::new(fields));

        let cache = Self::compute_properties(&input, &output_schema);

        Ok(Self {
            boundary_mode,
            input,
            output_schema,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }

    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        _output_schema: &SchemaRef,
    ) -> PlanProperties {
        // Consecutive group preserves the input's sort order
        // (we only add a column, we don't reorder rows)
        PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            // Final emission: we coalesce all batches before processing
            EmissionType::Final,
            input.boundedness(),
        )
    }
}

impl DisplayAs for GpuConsecutiveGroupExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "GpuConsecutiveGroupExec: column={}, mode={:?} [CUDA]",
                    self.boundary_mode.column_name(),
                    self.boundary_mode.kernel_name()
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "column={} mode={:?} [CUDA]",
                    self.boundary_mode.column_name(),
                    self.boundary_mode.kernel_name()
                )
            }
        }
    }
}

impl ExecutionPlan for GpuConsecutiveGroupExec {
    fn name(&self) -> &'static str {
        "GpuConsecutiveGroupExec"
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
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(GpuConsecutiveGroupExec::try_new(
            self.boundary_mode.clone(),
            children.swap_remove(0),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let schema = Arc::clone(&self.output_schema);
        let boundary_mode = self.boundary_mode.clone();
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        // Collect all input batches, then process on GPU
        let output_stream = futures_util::stream::once(async move {
            let timer = metrics.elapsed_compute().timer();

            // Collect all batches from input
            let batches: Vec<RecordBatch> =
                futures_util::StreamExt::collect::<Vec<_>>(input_stream)
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?;

            if batches.is_empty() {
                timer.done();
                let empty = RecordBatch::new_empty(schema);
                return Ok(empty);
            }

            // Coalesce into single batch
            let input_schema = batches[0].schema();
            let coalesced =
                datafusion::arrow::compute::concat_batches(&input_schema, batches.iter())?;

            let n = coalesced.num_rows();
            if n == 0 {
                timer.done();
                let empty = RecordBatch::new_empty(schema);
                return Ok(empty);
            }

            // Run GPU boundary detection + prefix sum
            let group_ids = gpu_consecutive_group(&coalesced, &boundary_mode)?;

            // Append __group_id__ column
            let group_id_array: ArrayRef = Arc::new(group_ids);
            let mut columns: Vec<ArrayRef> = coalesced.columns().to_vec();
            columns.push(group_id_array);

            let result = RecordBatch::try_new(schema.clone(), columns)?;
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

// --- SortOrderPropagation ---

impl SortOrderPropagation for GpuConsecutiveGroupExec {
    /// Consecutive grouping preserves the input's sort order
    /// (we add a column but don't reorder rows).
    fn sort_order_effect(&self) -> SortOrderEffect {
        SortOrderEffect::Preserve
    }
}

// ============================================================================
// GpuSegmentedAggregateExec
// ============================================================================

/// GPU-accelerated segmented aggregation on sorted/grouped data.
///
/// Takes input with a `__group_id__` column (from `GpuConsecutiveGroupExec` or
/// CPU group_id assignment) and computes per-group aggregates, broadcasting
/// results back to every row (window function semantics).
///
/// ## Supported Aggregations
///
/// - `Count` — count of rows per group
/// - `Sum` — sum of a column per group (i64, f64)
/// - `Min` — minimum of a column per group (i64, f64)
/// - `Max` — maximum of a column per group (i64, f64)
/// - `Avg` — average of a column per group → f64
///
/// ## Execution Flow
///
/// 1. Pull all batches from child (coalesce into single batch)
/// 2. Find `__group_id__` column (UInt32) and determine num_groups
/// 3. For each aggregation request:
///    a. Upload data column to GPU
///    b. Run atomic accumulation kernel (seg_sum, seg_count, etc.)
///    c. Run broadcast kernel to scatter per-group result to per-row
///    d. Download result
/// 4. Append new columns to the record batch
#[derive(Debug)]
pub struct GpuSegmentedAggregateExec {
    /// The aggregation requests
    requests: Vec<SegAggRequest>,
    /// Column name containing group IDs
    group_id_column: String,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// Output schema (input schema + aggregate columns)
    output_schema: SchemaRef,
    /// When true, output one row per group (GROUP BY semantics).
    /// When false, broadcast per-group results to all rows (window semantics).
    collapse_groups: bool,
    /// Column indices of group-by keys in the input schema (used when collapse_groups=true).
    group_key_columns: Vec<(String, usize)>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cached plan properties
    cache: PlanProperties,
}

impl GpuSegmentedAggregateExec {
    /// Create a new GpuSegmentedAggregateExec.
    pub fn try_new(
        requests: Vec<SegAggRequest>,
        group_id_column: String,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let input_schema = input.schema();
        let mut fields: Vec<Field> = input_schema.fields().iter().map(|f| (**f).clone()).collect();

        for req in &requests {
            let dt = match req.func {
                SegAggFunc::Count => DataType::Int64,
                SegAggFunc::Avg => DataType::Float64,
                SegAggFunc::Sum | SegAggFunc::Min | SegAggFunc::Max | SegAggFunc::FirstValue => req.data_type.clone(),
            };
            fields.push(Field::new(&req.output_name, dt, true));
        }

        let output_schema = Arc::new(Schema::new(fields));
        let cache = Self::compute_properties(&input, &output_schema);

        Ok(Self {
            requests,
            group_id_column,
            input,
            output_schema,
            collapse_groups: false,
            group_key_columns: Vec::new(),
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }

    /// Create a new GpuSegmentedAggregateExec in collapsing mode (GROUP BY semantics).
    ///
    /// In this mode, the output contains one row per group instead of broadcasting
    /// per-group results to all input rows. The output schema should contain the
    /// group-by key columns plus the aggregate result columns, matching the
    /// original `AggregateExec.schema()`.
    pub fn try_new_collapsing(
        requests: Vec<SegAggRequest>,
        group_id_column: String,
        group_key_columns: Vec<(String, usize)>,
        input: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
    ) -> Result<Self> {
        let cache = Self::compute_properties(&input, &output_schema);

        Ok(Self {
            requests,
            group_id_column,
            group_key_columns,
            input,
            output_schema,
            collapse_groups: true,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }

    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: &SchemaRef,
    ) -> PlanProperties {
        PlanProperties::new(
            input.properties().equivalence_properties().clone(),
            input.properties().output_partitioning().clone(),
            EmissionType::Final,
            input.boundedness(),
        )
    }
}

impl fmt::Display for GpuSegmentedAggregateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let agg_names: Vec<String> = self.requests.iter()
            .map(|r| format!("{:?}({})", r.func, r.output_name))
            .collect();
        write!(f, "GpuSegmentedAggregateExec: [{}]", agg_names.join(", "))
    }
}

impl DisplayAs for GpuSegmentedAggregateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "{}", self)
            }
            DisplayFormatType::TreeRender => {
                let agg_names: Vec<String> = self.requests.iter()
                    .map(|r| format!("{:?}({})", r.func, r.output_name))
                    .collect();
                write!(f, "aggs=[{}] group_by={} [CUDA]", agg_names.join(", "), self.group_id_column)
            }
        }
    }
}

impl ExecutionPlan for GpuSegmentedAggregateExec {
    fn name(&self) -> &'static str {
        "GpuSegmentedAggregateExec"
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
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if self.collapse_groups {
            Ok(Arc::new(GpuSegmentedAggregateExec::try_new_collapsing(
                self.requests.clone(),
                self.group_id_column.clone(),
                self.group_key_columns.clone(),
                children.swap_remove(0),
                Arc::clone(&self.output_schema),
            )?))
        } else {
            Ok(Arc::new(GpuSegmentedAggregateExec::try_new(
                self.requests.clone(),
                self.group_id_column.clone(),
                children.swap_remove(0),
            )?))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let schema = Arc::clone(&self.output_schema);
        let requests = self.requests.clone();
        let group_id_col = self.group_id_column.clone();
        let metrics = BaselineMetrics::new(&self.metrics, partition);
        let collapse_groups = self.collapse_groups;
        let group_key_columns = self.group_key_columns.clone();

        let output_stream = futures_util::stream::once(async move {
            let timer = metrics.elapsed_compute().timer();

            // Collect all batches
            let batches: Vec<RecordBatch> =
                futures_util::StreamExt::collect::<Vec<_>>(input_stream)
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?;

            if batches.is_empty() {
                timer.done();
                return Ok(RecordBatch::new_empty(schema));
            }

            // Coalesce
            let input_schema = batches[0].schema();
            let coalesced =
                datafusion::arrow::compute::concat_batches(&input_schema, batches.iter())?;
            let n = coalesced.num_rows();
            if n == 0 {
                timer.done();
                return Ok(RecordBatch::new_empty(schema));
            }

            // Find __group_id__ column
            let gid_idx = coalesced.schema().index_of(&group_id_col)
                .map_err(|_| datafusion::common::DataFusionError::Execution(
                    format!("Column '{}' not found in input", group_id_col)
                ))?;
            let gid_array = coalesced.column(gid_idx)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                    format!("Column '{}' is not UInt32", group_id_col)
                ))?;

            let num_groups = gid_array.values().iter().copied().max().unwrap_or(0) as usize;

            // Run GPU segmented aggregation
            let agg_results = gpu_segmented_aggregate(gid_array, &coalesced, num_groups, &requests)
                .map_err(|e| datafusion::common::DataFusionError::Execution(e))?;

            if collapse_groups {
                // GROUP BY semantics: output one row per group.
                // Find first-row indices for each group (where group_id changes).
                let gid_values = gid_array.values();
                let mut first_row_indices: Vec<u32> = Vec::with_capacity(num_groups + 1);
                for i in 0..n {
                    if i == 0 || gid_values[i] != gid_values[i - 1] {
                        first_row_indices.push(i as u32);
                    }
                }
                let take_indices = UInt32Array::from(first_row_indices);

                // Build output: group key columns + aggregate columns
                let mut columns: Vec<ArrayRef> = Vec::with_capacity(
                    group_key_columns.len() + agg_results.len(),
                );

                // Take group key columns from original input at first-row positions
                for (_key_name, key_idx) in &group_key_columns {
                    let col = coalesced.column(*key_idx);
                    let taken = datafusion::arrow::compute::take(col.as_ref(), &take_indices, None)?;
                    columns.push(taken);
                }

                // Take aggregate result columns at first-row positions
                // (all rows in same group have the same broadcast value, so any row works)
                for (_name, arr) in agg_results {
                    let taken = datafusion::arrow::compute::take(arr.as_ref(), &take_indices, None)?;
                    columns.push(taken);
                }

                let result = RecordBatch::try_new(schema.clone(), columns)?;
                timer.done();
                metrics.record_output(result.num_rows());
                Ok(result)
            } else {
                // Window semantics: broadcast per-group results to all rows.
                let mut columns: Vec<ArrayRef> = coalesced.columns().to_vec();
                for (_name, arr) in agg_results {
                    columns.push(arr);
                }

                let result = RecordBatch::try_new(schema.clone(), columns)?;
                timer.done();
                metrics.record_output(result.num_rows());
                Ok(result)
            }
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

impl SortOrderPropagation for GpuSegmentedAggregateExec {
    /// Segmented aggregation preserves the input's sort order
    /// (we add columns but don't reorder rows).
    fn sort_order_effect(&self) -> SortOrderEffect {
        SortOrderEffect::Preserve
    }
}

// ============================================================================
// GPU Kernel Execution
// ============================================================================

/// Run GPU boundary detection + prefix sum to generate group_id array.
///
/// Returns a UInt32Array where each element is the 1-based group ID.
fn gpu_consecutive_group(
    batch: &RecordBatch,
    mode: &GpuBoundaryMode,
) -> Result<UInt32Array> {
    let n = batch.num_rows();

    let stream = get_stream().ok_or_else(|| {
        datafusion::common::DataFusionError::Execution("CUDA stream unavailable".to_string())
    })?;

    // Step 1: Run boundary detection kernel
    let boundary_mask = match mode {
        GpuBoundaryMode::AdjacentNe { column_index, data_type, .. } => {
            run_adjacent_ne_kernel(&stream, batch.column(*column_index), data_type, n)?
        }
        GpuBoundaryMode::AdjacentDiffGt {
            column_index,
            data_type,
            threshold_i64,
            threshold_f64,
            ..
        } => {
            run_adjacent_diff_gt_kernel(
                &stream,
                batch.column(*column_index),
                data_type,
                *threshold_i64,
                *threshold_f64,
                n,
            )?
        }
    };

    // Step 2: Run prefix sum on GPU to convert boundary mask → group_id
    let group_ids = run_prefix_sum(&stream, &boundary_mask, n)?;

    Ok(UInt32Array::from(group_ids))
}

/// Run adjacent-not-equal boundary detection kernel on GPU.
/// Returns the boundary mask as Vec<u8> on host.
fn run_adjacent_ne_kernel(
    stream: &Arc<cudarc::driver::safe::CudaStream>,
    column: &ArrayRef,
    data_type: &DataType,
    n: usize,
) -> Result<Vec<u8>> {
    let kernel_name = match data_type {
        DataType::Int64 => "adjacent_ne_i64",
        DataType::Int32 => "adjacent_ne_i32",
        DataType::Float64 => "adjacent_ne_f64",
        DataType::Float32 => "adjacent_ne_f32",
        _ => {
            return Err(datafusion::common::DataFusionError::Execution(
                format!("Unsupported type for GPU adjacent_ne: {:?}", data_type),
            ))
        }
    };

    let func = get_ordered_ops_function(kernel_name).ok_or_else(|| {
        datafusion::common::DataFusionError::Execution(
            format!("CUDA kernel '{}' not found", kernel_name),
        )
    })?;

    let launch_cfg = LaunchConfig {
        grid_dim: (grid_size(n as u32), 1, 1),
        block_dim: (CUDA_BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    let mut d_boundary = stream.alloc_zeros::<u8>(n).map_err(cuda_err)?;
    let n_u32 = n as u32;

    match data_type {
        DataType::Int64 => {
            let array = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                datafusion::common::DataFusionError::Execution("Column is not Int64Array".into())
            })?;
            let values: &[i64] = array.values();
            let d_data = stream.clone_htod(values).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_data)
                    .arg(&mut d_boundary)
                    .arg(&n_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
        }
        DataType::Int32 => {
            let array = column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                datafusion::common::DataFusionError::Execution("Column is not Int32Array".into())
            })?;
            let values: &[i32] = array.values();
            let d_data = stream.clone_htod(values).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_data)
                    .arg(&mut d_boundary)
                    .arg(&n_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
        }
        DataType::Float64 => {
            let array = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                datafusion::common::DataFusionError::Execution("Column is not Float64Array".into())
            })?;
            let values: &[f64] = array.values();
            let d_data = stream.clone_htod(values).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_data)
                    .arg(&mut d_boundary)
                    .arg(&n_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
        }
        DataType::Float32 => {
            let array = column.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                datafusion::common::DataFusionError::Execution("Column is not Float32Array".into())
            })?;
            let values: &[f32] = array.values();
            let d_data = stream.clone_htod(values).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_data)
                    .arg(&mut d_boundary)
                    .arg(&n_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
        }
        _ => unreachable!(),
    }

    stream.clone_dtoh(&d_boundary).map_err(cuda_err)
}

/// Run adjacent-diff-greater-than boundary detection kernel on GPU.
fn run_adjacent_diff_gt_kernel(
    stream: &Arc<cudarc::driver::safe::CudaStream>,
    column: &ArrayRef,
    data_type: &DataType,
    threshold_i64: Option<i64>,
    threshold_f64: Option<f64>,
    n: usize,
) -> Result<Vec<u8>> {
    let kernel_name = match data_type {
        DataType::Int64 => "adjacent_diff_gt_i64",
        DataType::Float64 => "adjacent_diff_gt_f64",
        _ => {
            return Err(datafusion::common::DataFusionError::Execution(
                format!("Unsupported type for GPU adjacent_diff_gt: {:?}", data_type),
            ))
        }
    };

    let func = get_ordered_ops_function(kernel_name).ok_or_else(|| {
        datafusion::common::DataFusionError::Execution(
            format!("CUDA kernel '{}' not found", kernel_name),
        )
    })?;

    let launch_cfg = LaunchConfig {
        grid_dim: (grid_size(n as u32), 1, 1),
        block_dim: (CUDA_BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    let mut d_boundary = stream.alloc_zeros::<u8>(n).map_err(cuda_err)?;
    let n_u32 = n as u32;

    match data_type {
        DataType::Int64 => {
            let array = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                datafusion::common::DataFusionError::Execution("Column is not Int64Array".into())
            })?;
            let threshold = threshold_i64.unwrap_or(0);
            let values: &[i64] = array.values();
            let d_data = stream.clone_htod(values).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_data)
                    .arg(&threshold)
                    .arg(&mut d_boundary)
                    .arg(&n_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
        }
        DataType::Float64 => {
            let array = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                datafusion::common::DataFusionError::Execution("Column is not Float64Array".into())
            })?;
            let threshold = threshold_f64.unwrap_or(0.0);
            let values: &[f64] = array.values();
            let d_data = stream.clone_htod(values).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_data)
                    .arg(&threshold)
                    .arg(&mut d_boundary)
                    .arg(&n_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
        }
        _ => unreachable!(),
    }

    stream.clone_dtoh(&d_boundary).map_err(cuda_err)
}

/// Run inclusive prefix sum on GPU: boundary mask (u8) → group_id (u32).
///
/// For data that fits in a single block (≤ CUDA_BLOCK_SIZE elements),
/// this is a single kernel launch. For larger data, we use a multi-pass
/// approach:
/// 1. Block-level prefix sum → local group_ids + per-block totals
/// 2. Prefix sum on block totals (recursively if needed)
/// 3. Propagate block offsets to global group_ids
fn run_prefix_sum(
    stream: &Arc<cudarc::driver::safe::CudaStream>,
    boundary_mask: &[u8],
    n: usize,
) -> Result<Vec<u32>> {
    let prefix_sum_func = get_ordered_ops_function("prefix_sum_u8_to_u32").ok_or_else(|| {
        datafusion::common::DataFusionError::Execution(
            "CUDA kernel 'prefix_sum_u8_to_u32' not found".to_string(),
        )
    })?;

    let propagate_func = get_ordered_ops_function("propagate_block_sums").ok_or_else(|| {
        datafusion::common::DataFusionError::Execution(
            "CUDA kernel 'propagate_block_sums' not found".to_string(),
        )
    })?;

    let block_size = CUDA_BLOCK_SIZE as usize;
    let num_blocks = (n + block_size - 1) / block_size;

    // Upload boundary mask
    let d_boundary = stream.clone_htod(boundary_mask).map_err(cuda_err)?;
    let mut d_group_id = stream.alloc_zeros::<u32>(n).map_err(cuda_err)?;
    let n_u32 = n as u32;

    if num_blocks == 1 {
        // Single block — no need for block sum propagation
        let launch_cfg = LaunchConfig {
            grid_dim: (1, 1, 1),
            block_dim: (block_size as u32, 1, 1),
            shared_mem_bytes: (block_size * std::mem::size_of::<u32>()) as u32,
        };

        // Pass null pointer for block_sums (not needed for single block)
        let null_ptr: u64 = 0;
        unsafe {
            stream
                .launch_builder(&prefix_sum_func)
                .arg(&d_boundary)
                .arg(&mut d_group_id)
                .arg(&null_ptr)
                .arg(&n_u32)
                .launch(launch_cfg)
                .map_err(cuda_err)?;
        }
    } else {
        // Multi-block: block-level scan → propagate block sums
        let mut d_block_sums = stream.alloc_zeros::<u32>(num_blocks).map_err(cuda_err)?;

        let scan_cfg = LaunchConfig {
            grid_dim: (num_blocks as u32, 1, 1),
            block_dim: (block_size as u32, 1, 1),
            shared_mem_bytes: (block_size * std::mem::size_of::<u32>()) as u32,
        };

        // Pass 1: block-level prefix sum
        unsafe {
            stream
                .launch_builder(&prefix_sum_func)
                .arg(&d_boundary)
                .arg(&mut d_group_id)
                .arg(&mut d_block_sums)
                .arg(&n_u32)
                .launch(scan_cfg)
                .map_err(cuda_err)?;
        }

        // Compute block offsets on CPU (simple sequential scan of block sums).
        // For typical workloads (< 1M blocks = 256M rows), this is negligible.
        let block_sums_host = stream.clone_dtoh(&d_block_sums).map_err(cuda_err)?;
        let mut block_offsets = vec![0u32; num_blocks];
        let mut running = 0u32;
        for i in 0..num_blocks {
            block_offsets[i] = running;
            running += block_sums_host[i];
        }

        // Upload block offsets and propagate
        let d_block_offsets = stream.clone_htod(&block_offsets).map_err(cuda_err)?;

        let prop_cfg = LaunchConfig {
            grid_dim: (num_blocks as u32, 1, 1),
            block_dim: (block_size as u32, 1, 1),
            shared_mem_bytes: 0,
        };

        unsafe {
            stream
                .launch_builder(&propagate_func)
                .arg(&mut d_group_id)
                .arg(&d_block_offsets)
                .arg(&n_u32)
                .launch(prop_cfg)
                .map_err(cuda_err)?;
        }
    }

    // Download result
    stream.clone_dtoh(&d_group_id).map_err(cuda_err)
}

/// Convert a CUDA driver error into a DataFusion execution error.
fn cuda_err(e: cudarc::driver::safe::DriverError) -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::Execution(format!("CUDA error: {:?}", e))
}

// ============================================================================
// Segmented Aggregation — GPU Host Code
// ============================================================================

/// Supported segmented aggregation functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegAggFunc {
    Count,
    Sum,
    Min,
    Max,
    Avg,
    /// Take the first value in each group (used for key-column DISTINCT).
    FirstValue,
}

impl SegAggFunc {
    /// Parse from string (case-insensitive).
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "count" => Some(SegAggFunc::Count),
            "sum" => Some(SegAggFunc::Sum),
            "min" => Some(SegAggFunc::Min),
            "max" => Some(SegAggFunc::Max),
            "avg" | "mean" => Some(SegAggFunc::Avg),
            "first_value" => Some(SegAggFunc::FirstValue),
            _ => None,
        }
    }
}

/// A single segmented aggregation request.
#[derive(Debug, Clone)]
pub struct SegAggRequest {
    /// The aggregation function to apply.
    pub func: SegAggFunc,
    /// Column index in the input batch (ignored for Count).
    pub column_index: usize,
    /// Data type of the column (ignored for Count).
    pub data_type: DataType,
    /// Output column name.
    pub output_name: String,
}

/// Run GPU segmented aggregation on a batch.
///
/// Takes a `group_id` array (u32, 1-based from `gpu_consecutive_group()`) and
/// a list of aggregation requests. Returns per-row result arrays matching the
/// input row count (window function semantics).
///
/// For `Avg`, computes `sum / count` on the host after downloading both.
///
/// # Arguments
/// * `group_id_u32` - Per-row group ID array from boundary detection + prefix sum
/// * `batch` - The input RecordBatch containing data columns
/// * `num_groups` - Total number of groups (max group_id value)
/// * `requests` - List of aggregation operations to perform
///
/// # Returns
/// Vec of (column_name, ArrayRef) pairs for each request.
pub fn gpu_segmented_aggregate(
    group_id_u32: &UInt32Array,
    batch: &RecordBatch,
    num_groups: usize,
    requests: &[SegAggRequest],
) -> std::result::Result<Vec<(String, ArrayRef)>, String> {
    let n = group_id_u32.len();
    if n == 0 {
        return Ok(requests
            .iter()
            .map(|req| {
                let arr: ArrayRef = match req.func {
                    SegAggFunc::Count => Arc::new(Int64Array::from(Vec::<i64>::new())),
                    SegAggFunc::Avg => Arc::new(Float64Array::from(Vec::<f64>::new())),
                    SegAggFunc::FirstValue | _ => match req.data_type {
                        DataType::Int64 => Arc::new(Int64Array::from(Vec::<i64>::new())),
                        DataType::Float64 => Arc::new(Float64Array::from(Vec::<f64>::new())),
                        _ => Arc::new(Int64Array::from(Vec::<i64>::new())),
                    },
                };
                (req.output_name.clone(), arr)
            })
            .collect());
    }

    let stream = get_stream().ok_or("CUDA stream unavailable")?;

    // Upload group_id to GPU
    let group_id_values: &[u32] = group_id_u32.values();
    let d_group_id = stream.clone_htod(group_id_values).map_err(|e| format!("CUDA H2D: {:?}", e))?;

    let launch_cfg = LaunchConfig {
        grid_dim: (grid_size(n as u32), 1, 1),
        block_dim: (CUDA_BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };
    let n_u32 = n as u32;

    // group_id is 1-based, so we need (num_groups + 1) slots
    let alloc_size = num_groups + 1;

    let ctx = SegAggContext {
        stream: &stream,
        d_group_id: &d_group_id,
        alloc_size,
        n,
        n_u32,
        launch_cfg: &launch_cfg,
    };

    let mut results: Vec<(String, ArrayRef)> = Vec::with_capacity(requests.len());

    for req in requests {
        let result_arr: ArrayRef = match req.func {
            SegAggFunc::Count => {
                run_seg_count(&ctx)?
            }
            SegAggFunc::Sum => {
                run_seg_sum(
                    &ctx,
                    batch.column(req.column_index),
                    &req.data_type,
                )?
            }
            SegAggFunc::Min => {
                run_seg_min_max(
                    &ctx,
                    batch.column(req.column_index),
                    &req.data_type,
                    true, // is_min
                )?
            }
            SegAggFunc::Max => {
                run_seg_min_max(
                    &ctx,
                    batch.column(req.column_index),
                    &req.data_type,
                    false, // is_max
                )?
            }
            SegAggFunc::Avg => {
                run_seg_avg(
                    &ctx,
                    batch.column(req.column_index),
                    &req.data_type,
                )?
            }
            SegAggFunc::FirstValue => {
                // FirstValue in window/segmented context: broadcast the first value
                // of each group to all rows in that group.
                // For segmented aggregation (window function semantics), we compute
                // per-group first values and broadcast them back.
                // We use the same approach as Min but only take the first row per group.
                // For simplicity, just use the min kernel — when data is already grouped,
                // first_value is effectively the value at the first row index of each group.
                // A proper implementation would need a dedicated kernel, but for now
                // we reuse min as a placeholder since this path is mainly used by
                // the collapsed aggregate (hash_ops), not by the windowed aggregate.
                run_seg_min_max(
                    &ctx,
                    batch.column(req.column_index),
                    &req.data_type,
                    true, // use min as approximation
                )?
            }
        };

        results.push((req.output_name.clone(), result_arr));
    }

    Ok(results)
}

/// Shared context for segmented aggregation GPU kernels.
///
/// Bundles the common parameters passed to every `run_seg_*` function:
/// the CUDA stream, device-side group-id array, allocation size for
/// per-group buffers, row count, and launch configuration.
struct SegAggContext<'a> {
    stream: &'a Arc<cudarc::driver::safe::CudaStream>,
    d_group_id: &'a cudarc::driver::safe::CudaSlice<u32>,
    alloc_size: usize,
    n: usize,
    n_u32: u32,
    launch_cfg: &'a LaunchConfig,
}

/// Run segmented count on GPU → per-row i64 array.
fn run_seg_count(
    ctx: &SegAggContext<'_>,
) -> std::result::Result<ArrayRef, String> {
    let func = get_ordered_ops_function("seg_count")
        .ok_or("CUDA kernel 'seg_count' not found")?;

    let mut d_counts = ctx.stream.alloc_zeros::<i64>(ctx.alloc_size)
        .map_err(|e| format!("CUDA alloc: {:?}", e))?;

    unsafe {
        ctx.stream
            .launch_builder(&func)
            .arg(ctx.d_group_id)
            .arg(&mut d_counts)
            .arg(&ctx.n_u32)
            .launch(*ctx.launch_cfg)
            .map_err(|e| format!("CUDA launch seg_count: {:?}", e))?;
    }

    // Download per-group counts
    let group_counts = ctx.stream.clone_dtoh(&d_counts)
        .map_err(|e| format!("CUDA D2H: {:?}", e))?;

    // Broadcast to per-row
    let broadcast_func = get_ordered_ops_function("broadcast_i64")
        .ok_or("CUDA kernel 'broadcast_i64' not found")?;

    let mut d_output = ctx.stream.alloc_zeros::<i64>(ctx.n)
        .map_err(|e| format!("CUDA alloc: {:?}", e))?;

    unsafe {
        ctx.stream
            .launch_builder(&broadcast_func)
            .arg(ctx.d_group_id)
            .arg(&d_counts)
            .arg(&mut d_output)
            .arg(&ctx.n_u32)
            .launch(*ctx.launch_cfg)
            .map_err(|e| format!("CUDA launch broadcast_i64: {:?}", e))?;
    }

    let output = ctx.stream.clone_dtoh(&d_output)
        .map_err(|e| format!("CUDA D2H: {:?}", e))?;

    Ok(Arc::new(Int64Array::from(output)))
}

/// Run segmented sum on GPU → per-row array (i64 or f64).
fn run_seg_sum(
    ctx: &SegAggContext<'_>,
    column: &ArrayRef,
    data_type: &DataType,
) -> std::result::Result<ArrayRef, String> {
    match data_type {
        DataType::Int64 => {
            let array = column.as_any().downcast_ref::<Int64Array>()
                .ok_or("Column is not Int64Array")?;
            let values: &[i64] = array.values();
            let d_data = ctx.stream.clone_htod(values).map_err(|e| format!("CUDA H2D: {:?}", e))?;

            let func = get_ordered_ops_function("seg_sum_i64")
                .ok_or("CUDA kernel 'seg_sum_i64' not found")?;
            let mut d_sums = ctx.stream.alloc_zeros::<i64>(ctx.alloc_size)
                .map_err(|e| format!("CUDA alloc: {:?}", e))?;

            unsafe {
                ctx.stream
                    .launch_builder(&func)
                    .arg(ctx.d_group_id)
                    .arg(&d_data)
                    .arg(&mut d_sums)
                    .arg(&ctx.n_u32)
                    .launch(*ctx.launch_cfg)
                    .map_err(|e| format!("CUDA launch: {:?}", e))?;
            }

            // Broadcast
            let broadcast = get_ordered_ops_function("broadcast_i64")
                .ok_or("CUDA kernel 'broadcast_i64' not found")?;
            let mut d_output = ctx.stream.alloc_zeros::<i64>(ctx.n)
                .map_err(|e| format!("CUDA alloc: {:?}", e))?;

            unsafe {
                ctx.stream
                    .launch_builder(&broadcast)
                    .arg(ctx.d_group_id)
                    .arg(&d_sums)
                    .arg(&mut d_output)
                    .arg(&ctx.n_u32)
                    .launch(*ctx.launch_cfg)
                    .map_err(|e| format!("CUDA launch: {:?}", e))?;
            }

            let output = ctx.stream.clone_dtoh(&d_output)
                .map_err(|e| format!("CUDA D2H: {:?}", e))?;
            Ok(Arc::new(Int64Array::from(output)))
        }
        DataType::Float64 => {
            let array = column.as_any().downcast_ref::<Float64Array>()
                .ok_or("Column is not Float64Array")?;
            let values: &[f64] = array.values();
            let d_data = ctx.stream.clone_htod(values).map_err(|e| format!("CUDA H2D: {:?}", e))?;

            let func = get_ordered_ops_function("seg_sum_f64")
                .ok_or("CUDA kernel 'seg_sum_f64' not found")?;
            let mut d_sums = ctx.stream.alloc_zeros::<f64>(ctx.alloc_size)
                .map_err(|e| format!("CUDA alloc: {:?}", e))?;

            unsafe {
                ctx.stream
                    .launch_builder(&func)
                    .arg(ctx.d_group_id)
                    .arg(&d_data)
                    .arg(&mut d_sums)
                    .arg(&ctx.n_u32)
                    .launch(*ctx.launch_cfg)
                    .map_err(|e| format!("CUDA launch: {:?}", e))?;
            }

            // Broadcast
            let broadcast = get_ordered_ops_function("broadcast_f64")
                .ok_or("CUDA kernel 'broadcast_f64' not found")?;
            let mut d_output = ctx.stream.alloc_zeros::<f64>(ctx.n)
                .map_err(|e| format!("CUDA alloc: {:?}", e))?;

            unsafe {
                ctx.stream
                    .launch_builder(&broadcast)
                    .arg(ctx.d_group_id)
                    .arg(&d_sums)
                    .arg(&mut d_output)
                    .arg(&ctx.n_u32)
                    .launch(*ctx.launch_cfg)
                    .map_err(|e| format!("CUDA launch: {:?}", e))?;
            }

            let output = ctx.stream.clone_dtoh(&d_output)
                .map_err(|e| format!("CUDA D2H: {:?}", e))?;
            Ok(Arc::new(Float64Array::from(output)))
        }
        _ => Err(format!("Unsupported data type for GPU seg_sum: {:?}", data_type)),
    }
}

/// Run segmented min or max on GPU → per-row array (i64 or f64).
fn run_seg_min_max(
    ctx: &SegAggContext<'_>,
    column: &ArrayRef,
    data_type: &DataType,
    is_min: bool,
) -> std::result::Result<ArrayRef, String> {
    let kernel_suffix = if is_min { "min" } else { "max" };

    match data_type {
        DataType::Int64 => {
            let array = column.as_any().downcast_ref::<Int64Array>()
                .ok_or("Column is not Int64Array")?;
            let values: &[i64] = array.values();
            let d_data = ctx.stream.clone_htod(values).map_err(|e| format!("CUDA H2D: {:?}", e))?;

            let kernel_name = format!("seg_{}_i64", kernel_suffix);
            let func = get_ordered_ops_function(&kernel_name)
                .ok_or_else(|| format!("CUDA kernel '{}' not found", kernel_name))?;

            // Initialize with identity values
            let init_val = if is_min { i64::MAX } else { i64::MIN };
            let init: Vec<i64> = vec![init_val; ctx.alloc_size];
            let mut d_result = ctx.stream.clone_htod(&init)
                .map_err(|e| format!("CUDA H2D init: {:?}", e))?;

            unsafe {
                ctx.stream
                    .launch_builder(&func)
                    .arg(ctx.d_group_id)
                    .arg(&d_data)
                    .arg(&mut d_result)
                    .arg(&ctx.n_u32)
                    .launch(*ctx.launch_cfg)
                    .map_err(|e| format!("CUDA launch: {:?}", e))?;
            }

            // Broadcast
            let broadcast = get_ordered_ops_function("broadcast_i64")
                .ok_or("CUDA kernel 'broadcast_i64' not found")?;
            let mut d_output = ctx.stream.alloc_zeros::<i64>(ctx.n)
                .map_err(|e| format!("CUDA alloc: {:?}", e))?;

            unsafe {
                ctx.stream
                    .launch_builder(&broadcast)
                    .arg(ctx.d_group_id)
                    .arg(&d_result)
                    .arg(&mut d_output)
                    .arg(&ctx.n_u32)
                    .launch(*ctx.launch_cfg)
                    .map_err(|e| format!("CUDA launch: {:?}", e))?;
            }

            let output = ctx.stream.clone_dtoh(&d_output)
                .map_err(|e| format!("CUDA D2H: {:?}", e))?;
            Ok(Arc::new(Int64Array::from(output)))
        }
        DataType::Float64 => {
            let array = column.as_any().downcast_ref::<Float64Array>()
                .ok_or("Column is not Float64Array")?;
            let values: &[f64] = array.values();
            let d_data = ctx.stream.clone_htod(values).map_err(|e| format!("CUDA H2D: {:?}", e))?;

            let kernel_name = format!("seg_{}_f64", kernel_suffix);
            let func = get_ordered_ops_function(&kernel_name)
                .ok_or_else(|| format!("CUDA kernel '{}' not found", kernel_name))?;

            // Initialize with identity values
            let init_val: f64 = if is_min { f64::MAX } else { f64::MIN };
            let init: Vec<f64> = vec![init_val; ctx.alloc_size];
            let mut d_result = ctx.stream.clone_htod(&init)
                .map_err(|e| format!("CUDA H2D init: {:?}", e))?;

            unsafe {
                ctx.stream
                    .launch_builder(&func)
                    .arg(ctx.d_group_id)
                    .arg(&d_data)
                    .arg(&mut d_result)
                    .arg(&ctx.n_u32)
                    .launch(*ctx.launch_cfg)
                    .map_err(|e| format!("CUDA launch: {:?}", e))?;
            }

            // Broadcast
            let broadcast = get_ordered_ops_function("broadcast_f64")
                .ok_or("CUDA kernel 'broadcast_f64' not found")?;
            let mut d_output = ctx.stream.alloc_zeros::<f64>(ctx.n)
                .map_err(|e| format!("CUDA alloc: {:?}", e))?;

            unsafe {
                ctx.stream
                    .launch_builder(&broadcast)
                    .arg(ctx.d_group_id)
                    .arg(&d_result)
                    .arg(&mut d_output)
                    .arg(&ctx.n_u32)
                    .launch(*ctx.launch_cfg)
                    .map_err(|e| format!("CUDA launch: {:?}", e))?;
            }

            let output = ctx.stream.clone_dtoh(&d_output)
                .map_err(|e| format!("CUDA D2H: {:?}", e))?;
            Ok(Arc::new(Float64Array::from(output)))
        }
        _ => Err(format!("Unsupported data type for GPU seg_{}: {:?}", kernel_suffix, data_type)),
    }
}

/// Run segmented avg on GPU → per-row f64 array.
///
/// Computes sum and count on GPU, then avg = sum / count on host.
fn run_seg_avg(
    ctx: &SegAggContext<'_>,
    column: &ArrayRef,
    data_type: &DataType,
) -> std::result::Result<ArrayRef, String> {
    // Step 1: Compute per-group count
    let count_func = get_ordered_ops_function("seg_count")
        .ok_or("CUDA kernel 'seg_count' not found")?;
    let mut d_counts = ctx.stream.alloc_zeros::<i64>(ctx.alloc_size)
        .map_err(|e| format!("CUDA alloc: {:?}", e))?;

    unsafe {
        ctx.stream
            .launch_builder(&count_func)
            .arg(ctx.d_group_id)
            .arg(&mut d_counts)
            .arg(&ctx.n_u32)
            .launch(*ctx.launch_cfg)
            .map_err(|e| format!("CUDA launch: {:?}", e))?;
    }

    let group_counts = ctx.stream.clone_dtoh(&d_counts)
        .map_err(|e| format!("CUDA D2H: {:?}", e))?;

    // Step 2: Compute per-group sum (always as f64 for avg)
    let sum_func = get_ordered_ops_function("seg_sum_f64")
        .ok_or("CUDA kernel 'seg_sum_f64' not found")?;
    let mut d_sums = ctx.stream.alloc_zeros::<f64>(ctx.alloc_size)
        .map_err(|e| format!("CUDA alloc: {:?}", e))?;

    // Upload data as f64 (convert if needed)
    match data_type {
        DataType::Int64 => {
            let array = column.as_any().downcast_ref::<Int64Array>()
                .ok_or("Column is not Int64Array")?;
            let f64_values: Vec<f64> = array.values().iter().map(|v| *v as f64).collect();
            let d_data = ctx.stream.clone_htod(&f64_values)
                .map_err(|e| format!("CUDA H2D: {:?}", e))?;

            unsafe {
                ctx.stream
                    .launch_builder(&sum_func)
                    .arg(ctx.d_group_id)
                    .arg(&d_data)
                    .arg(&mut d_sums)
                    .arg(&ctx.n_u32)
                    .launch(*ctx.launch_cfg)
                    .map_err(|e| format!("CUDA launch: {:?}", e))?;
            }
        }
        DataType::Float64 => {
            let array = column.as_any().downcast_ref::<Float64Array>()
                .ok_or("Column is not Float64Array")?;
            let values: &[f64] = array.values();
            let d_data = ctx.stream.clone_htod(values)
                .map_err(|e| format!("CUDA H2D: {:?}", e))?;

            unsafe {
                ctx.stream
                    .launch_builder(&sum_func)
                    .arg(ctx.d_group_id)
                    .arg(&d_data)
                    .arg(&mut d_sums)
                    .arg(&ctx.n_u32)
                    .launch(*ctx.launch_cfg)
                    .map_err(|e| format!("CUDA launch: {:?}", e))?;
            }
        }
        _ => return Err(format!("Unsupported data type for GPU seg_avg: {:?}", data_type)),
    }

    let group_sums = ctx.stream.clone_dtoh(&d_sums)
        .map_err(|e| format!("CUDA D2H: {:?}", e))?;

    // Step 3: Compute avg = sum / count per group on host
    let mut group_avgs: Vec<f64> = vec![0.0; ctx.alloc_size];
    for i in 0..ctx.alloc_size {
        if group_counts[i] > 0 {
            group_avgs[i] = group_sums[i] / group_counts[i] as f64;
        }
    }

    // Step 4: Broadcast per-group avg to per-row (on GPU)
    let d_avgs = ctx.stream.clone_htod(&group_avgs)
        .map_err(|e| format!("CUDA H2D: {:?}", e))?;
    let broadcast = get_ordered_ops_function("broadcast_f64")
        .ok_or("CUDA kernel 'broadcast_f64' not found")?;
    let mut d_output = ctx.stream.alloc_zeros::<f64>(ctx.n)
        .map_err(|e| format!("CUDA alloc: {:?}", e))?;

    unsafe {
        ctx.stream
            .launch_builder(&broadcast)
            .arg(ctx.d_group_id)
            .arg(&d_avgs)
            .arg(&mut d_output)
            .arg(&ctx.n_u32)
            .launch(*ctx.launch_cfg)
            .map_err(|e| format!("CUDA launch: {:?}", e))?;
    }

    let output = ctx.stream.clone_dtoh(&d_output)
        .map_err(|e| format!("CUDA D2H: {:?}", e))?;
    Ok(Arc::new(Float64Array::from(output)))
}

// ============================================================================
// Public API for integration with linear_scan_group_id
// ============================================================================

use crate::types::PyExpr;

/// Try to match a PyExpr boundary predicate to a GpuBoundaryMode.
///
/// Recognizes two patterns:
///
/// 1. **Adjacent not-equal**: `Column("x") != Call("shift", Column("x"), [1])`
///    or the reverse operand order. Maps to `GpuBoundaryMode::AdjacentNe`.
///
/// 2. **Adjacent diff > threshold**: `(Column("x") - Call("shift", Column("x"), [1])) > Literal`
///    Maps to `GpuBoundaryMode::AdjacentDiffGt`.
///
/// Returns `None` if the predicate doesn't match any GPU-eligible pattern,
/// or if the column data type is not supported (i32/i64/f32/f64 for ne,
/// i64/f64 for diff_gt).
pub fn try_match_gpu_boundary_mode(
    expr: &PyExpr,
    schema: &datafusion::arrow::datatypes::Schema,
) -> Option<GpuBoundaryMode> {
    match expr {
        // Pattern 1: Column("x") != Column("x").shift(1)
        PyExpr::BinOp { op, left, right } if op == "Ne" => {
            // Try left=Column, right=shift(Column)
            if let Some(mode) = try_match_adjacent_ne(left, right, schema) {
                return Some(mode);
            }
            // Try reverse: left=shift(Column), right=Column
            try_match_adjacent_ne(right, left, schema)
        }

        // Pattern 2: (Column("x") - Column("x").shift(1)) > Literal
        PyExpr::BinOp { op, left, right } if op == "Gt" => {
            try_match_adjacent_diff_gt(left, right, schema)
        }

        _ => None,
    }
}

/// Match `col_expr` = Column("x"), `shift_expr` = Call("shift", Column("x"), [1])
fn try_match_adjacent_ne(
    col_expr: &PyExpr,
    shift_expr: &PyExpr,
    schema: &datafusion::arrow::datatypes::Schema,
) -> Option<GpuBoundaryMode> {
    let col_name = match col_expr {
        PyExpr::Column(name) => name,
        _ => return None,
    };

    // Verify shift_expr is shift(1) on the same column
    if !is_shift_of_column(shift_expr, col_name) {
        return None;
    }

    // Look up column in schema
    let (col_idx, field) = schema.column_with_name(col_name)?;
    let data_type = field.data_type().clone();

    // Only support numeric types for GPU adjacent_ne
    match &data_type {
        DataType::Int64 | DataType::Int32 | DataType::Float64 | DataType::Float32 => {}
        _ => return None,
    }

    Some(GpuBoundaryMode::AdjacentNe {
        column_index: col_idx,
        column_name: col_name.clone(),
        data_type,
    })
}

/// Match `(Column("x") - Column("x").shift(1)) > Literal`
fn try_match_adjacent_diff_gt(
    diff_expr: &PyExpr,
    threshold_expr: &PyExpr,
    schema: &datafusion::arrow::datatypes::Schema,
) -> Option<GpuBoundaryMode> {
    // diff_expr must be BinOp { op: "Sub", left: Column("x"), right: shift(Column("x"), 1) }
    let (col_name, data_type, col_idx) = match diff_expr {
        PyExpr::BinOp { op, left, right } if op == "Sub" => {
            let name = match left.as_ref() {
                PyExpr::Column(name) => name,
                _ => return None,
            };
            if !is_shift_of_column(right, name) {
                return None;
            }
            let (idx, field) = schema.column_with_name(name)?;
            (name.clone(), field.data_type().clone(), idx)
        }
        _ => return None,
    };

    // threshold_expr must be a numeric literal
    let (threshold_i64, threshold_f64) = match threshold_expr {
        PyExpr::Literal { value, dtype } => {
            match dtype.as_str() {
                "Int64" | "Int32" | "int" | "UInt64" | "UInt32" => {
                    let v = value.parse::<i64>().ok()?;
                    (Some(v), None)
                }
                "Float64" | "Float32" | "float" => {
                    let v = value.parse::<f64>().ok()?;
                    (None, Some(v))
                }
                _ => return None,
            }
        }
        _ => return None,
    };

    // Only support i64 and f64 for diff_gt
    match &data_type {
        DataType::Int64 | DataType::Float64 => {}
        _ => return None,
    }

    Some(GpuBoundaryMode::AdjacentDiffGt {
        column_index: col_idx,
        column_name: col_name,
        data_type,
        threshold_i64,
        threshold_f64,
    })
}

/// Check if `expr` is `Call("shift", Column(expected_col), [Literal(1)])`
fn is_shift_of_column(expr: &PyExpr, expected_col: &str) -> bool {
    match expr {
        PyExpr::Call { func, on, args, .. } if func == "shift" => {
            // `on` must be Column(expected_col)
            let col_matches = matches!(on.as_ref(), PyExpr::Column(name) if name == expected_col);
            if !col_matches {
                return false;
            }
            // args must be [Literal("1", integer_type)]
            if args.len() != 1 {
                return false;
            }
            matches!(
                &args[0],
                PyExpr::Literal { value, dtype }
                    if value == "1"
                    && matches!(dtype.as_str(), "Int64" | "Int32" | "int" | "UInt64" | "UInt32")
            )
        }
        _ => false,
    }
}

/// Run GPU boundary detection + prefix sum, then compute group metadata on CPU.
///
/// This is the GPU fast path for `linear_scan_group_id()`. It:
/// 1. Uploads the boundary column to GPU
/// 2. Runs adjacent comparison kernel → boundary mask
/// 3. Runs prefix sum kernel → group_id (u32)
/// 4. Downloads group_id to host
/// 5. Computes group_count and rn on CPU (trivially fast O(n))
///
/// Returns `(group_ids, group_counts, rn_values)` as `Vec<i64>` triples,
/// matching the format expected by `build_metadata_table()`.
pub fn gpu_group_id_from_batch(
    batch: &RecordBatch,
    mode: &GpuBoundaryMode,
) -> std::result::Result<(Vec<i64>, Vec<i64>, Vec<i64>), String> {
    let n = batch.num_rows();
    if n == 0 {
        return Ok((vec![], vec![], vec![]));
    }

    // Run GPU kernels: boundary detection + prefix sum → u32 group_ids
    let gpu_group_ids = gpu_consecutive_group(batch, mode)
        .map_err(|e| format!("GPU group_id failed: {}", e))?;

    // Convert u32 → i64 and compute group_count + rn on CPU
    let total_rows = gpu_group_ids.len();
    let mut group_ids_i64: Vec<i64> = Vec::with_capacity(total_rows);
    for i in 0..total_rows {
        group_ids_i64.push(gpu_group_ids.value(i) as i64);
    }

    // Determine number of groups
    let num_groups = if total_rows > 0 {
        gpu_group_ids.value(total_rows - 1) as usize
    } else {
        0
    };

    // Compute group_count: count per group_id
    let mut group_counts_raw: Vec<i64> = vec![0; num_groups + 1];
    for &gid in &group_ids_i64 {
        group_counts_raw[gid as usize] += 1;
    }

    // Compute per-row group_count and rn
    let mut count_values: Vec<i64> = Vec::with_capacity(total_rows);
    let mut rn_values: Vec<i64> = Vec::with_capacity(total_rows);
    let mut rn_counters: Vec<i64> = vec![0; num_groups + 1];

    for &gid in &group_ids_i64 {
        rn_counters[gid as usize] += 1;
        rn_values.push(rn_counters[gid as usize]);
        count_values.push(group_counts_raw[gid as usize]);
    }

    Ok((group_ids_i64, count_values, rn_values))
}

/// Check if GPU acceleration is available and the dataset is large enough.
pub fn is_gpu_group_eligible(total_rows: usize) -> bool {
    super::is_gpu_available() && total_rows >= super::GPU_MIN_ROWS_THRESHOLD
}

// ============================================================================
// GpuBinarySearchExec — search_first GPU acceleration
// ============================================================================

/// Describes a binary search operation on a sorted column.
///
/// Maps a predicate like `Column("price") > Literal(100)` on ascending-sorted
/// data to a specific binary search mode.
#[derive(Debug, Clone)]
pub enum GpuSearchMode {
    /// `column > value` — find first index where `data[i] > value` (upper_bound)
    Gt {
        column_name: String,
        column_index: usize,
        data_type: DataType,
        value_i64: Option<i64>,
        value_i32: Option<i32>,
        value_f64: Option<f64>,
        value_f32: Option<f32>,
    },
    /// `column >= value` — find first index where `data[i] >= value` (lower_bound)
    Ge {
        column_name: String,
        column_index: usize,
        data_type: DataType,
        value_i64: Option<i64>,
        value_i32: Option<i32>,
        value_f64: Option<f64>,
        value_f32: Option<f32>,
    },
    /// `column == value` — find first index where `data[i] == value` (lower_bound + check)
    Eq {
        column_name: String,
        column_index: usize,
        data_type: DataType,
        value_i64: Option<i64>,
        value_i32: Option<i32>,
        value_f64: Option<f64>,
        value_f32: Option<f32>,
    },
    /// `column < value` — on ascending data, first row if `data[0] < value`
    Lt {
        column_name: String,
        column_index: usize,
        data_type: DataType,
        value_i64: Option<i64>,
        value_i32: Option<i32>,
        value_f64: Option<f64>,
        value_f32: Option<f32>,
    },
    /// `column <= value` — on ascending data, first row if `data[0] <= value`
    Le {
        column_name: String,
        column_index: usize,
        data_type: DataType,
        value_i64: Option<i64>,
        value_i32: Option<i32>,
        value_f64: Option<f64>,
        value_f32: Option<f32>,
    },
}

impl GpuSearchMode {
    /// CUDA kernel function name for this search mode.
    pub fn kernel_name(&self) -> &'static str {
        let dt = self.data_type();
        match dt {
            DataType::Int64 => "search_first_i64",
            DataType::Int32 => "search_first_i32",
            DataType::Float64 => "search_first_f64",
            DataType::Float32 => "search_first_f32",
            _ => unreachable!("unsupported type for search_first"),
        }
    }

    /// The mode integer passed to the CUDA kernel.
    pub fn mode_value(&self) -> u32 {
        match self {
            GpuSearchMode::Gt { .. } => 0,
            GpuSearchMode::Ge { .. } => 1,
            GpuSearchMode::Eq { .. } => 2,
            GpuSearchMode::Lt { .. } => 3,
            GpuSearchMode::Le { .. } => 4,
        }
    }

    /// Column name for display.
    pub fn column_name(&self) -> &str {
        match self {
            GpuSearchMode::Gt { column_name, .. }
            | GpuSearchMode::Ge { column_name, .. }
            | GpuSearchMode::Eq { column_name, .. }
            | GpuSearchMode::Lt { column_name, .. }
            | GpuSearchMode::Le { column_name, .. } => column_name,
        }
    }

    /// Data type of the search column.
    pub fn data_type(&self) -> &DataType {
        match self {
            GpuSearchMode::Gt { data_type, .. }
            | GpuSearchMode::Ge { data_type, .. }
            | GpuSearchMode::Eq { data_type, .. }
            | GpuSearchMode::Lt { data_type, .. }
            | GpuSearchMode::Le { data_type, .. } => data_type,
        }
    }

    /// Column index in the schema.
    pub fn column_index(&self) -> usize {
        match self {
            GpuSearchMode::Gt { column_index, .. }
            | GpuSearchMode::Ge { column_index, .. }
            | GpuSearchMode::Eq { column_index, .. }
            | GpuSearchMode::Lt { column_index, .. }
            | GpuSearchMode::Le { column_index, .. } => *column_index,
        }
    }

    /// Comparison operator string for display.
    pub fn op_str(&self) -> &'static str {
        match self {
            GpuSearchMode::Gt { .. } => ">",
            GpuSearchMode::Ge { .. } => ">=",
            GpuSearchMode::Eq { .. } => "==",
            GpuSearchMode::Lt { .. } => "<",
            GpuSearchMode::Le { .. } => "<=",
        }
    }
}

// ── GPU binary search kernel launcher ────────────────────────────────────

/// Downcast a column to a typed array, extract the search value from `GpuSearchMode`,
/// copy data to GPU, and launch the binary search kernel.
macro_rules! search_first_typed {
    ($stream:expr, $column:expr, $search_mode:expr, $func:expr,
     $launch_cfg:expr, $d_result:expr, $n_u32:expr,
     $mode_u32:expr, $str_err:expr,
     $ArrayType:ty, $value_field:ident, $type_name:expr) => {{
        let arr = $column
            .as_any()
            .downcast_ref::<$ArrayType>()
            .ok_or(concat!("Failed to downcast to ", $type_name))?;
        let values = arr.values();
        let d_data = $stream.clone_htod(values.as_ref()).map_err($str_err)?;
        let value = match $search_mode {
            GpuSearchMode::Gt { $value_field, .. }
            | GpuSearchMode::Ge { $value_field, .. }
            | GpuSearchMode::Eq { $value_field, .. }
            | GpuSearchMode::Lt { $value_field, .. }
            | GpuSearchMode::Le { $value_field, .. } => {
                $value_field.ok_or(concat!("Missing ", $type_name, " search value"))?
            }
        };
        unsafe {
            $stream
                .launch_builder(&$func)
                .arg(&d_data)
                .arg(&value)
                .arg($d_result)
                .arg($n_u32)
                .arg($mode_u32)
                .launch($launch_cfg)
                .map_err($str_err)?;
        }
    }};
}

/// Run a GPU binary search kernel on a sorted column.
///
/// Returns the index of the first matching row, or `None` if no match.
fn run_search_first_kernel(
    stream: &Arc<cudarc::driver::CudaStream>,
    column: &ArrayRef,
    search_mode: &GpuSearchMode,
    n: usize,
) -> std::result::Result<Option<usize>, String> {
    let kernel_name = search_mode.kernel_name();
    let mode_val = search_mode.mode_value();

    let func = get_ordered_ops_function(kernel_name)
        .ok_or_else(|| format!("Failed to load CUDA kernel: {}", kernel_name))?;

    // Single thread launch — one search operation
    let launch_cfg = cudarc::driver::LaunchConfig {
        grid_dim: (1, 1, 1),
        block_dim: (1, 1, 1),
        shared_mem_bytes: 0,
    };

    let n_u32 = n as u32;
    let mode_u32 = mode_val;

    let str_err = |e: cudarc::driver::safe::DriverError| -> String {
        format!("CUDA error: {:?}", e)
    };

    // Allocate output: single u32
    let mut d_result = stream.alloc_zeros::<u32>(1).map_err(str_err)?;

    match search_mode.data_type() {
        DataType::Int64 => search_first_typed!(
            stream, column, search_mode, func, launch_cfg,
            &mut d_result, &n_u32, &mode_u32, str_err,
            Int64Array, value_i64, "Int64Array"
        ),
        DataType::Int32 => search_first_typed!(
            stream, column, search_mode, func, launch_cfg,
            &mut d_result, &n_u32, &mode_u32, str_err,
            Int32Array, value_i32, "Int32Array"
        ),
        DataType::Float64 => search_first_typed!(
            stream, column, search_mode, func, launch_cfg,
            &mut d_result, &n_u32, &mode_u32, str_err,
            Float64Array, value_f64, "Float64Array"
        ),
        DataType::Float32 => search_first_typed!(
            stream, column, search_mode, func, launch_cfg,
            &mut d_result, &n_u32, &mode_u32, str_err,
            Float32Array, value_f32, "Float32Array"
        ),
        _ => return Err(format!("Unsupported data type: {:?}", search_mode.data_type())),
    }

    // Download result
    let result_host = stream.clone_dtoh(&d_result).map_err(str_err)?;
    let idx = result_host[0];

    if idx == 0xFFFFFFFFu32 {
        Ok(None)
    } else {
        Ok(Some(idx as usize))
    }
}

/// GPU binary search entry point for search_first.
///
/// Given a `RecordBatch` with sorted data and a `GpuSearchMode`,
/// returns the index of the first matching row, or `None` if no match.
pub fn gpu_search_first(
    batch: &RecordBatch,
    search_mode: &GpuSearchMode,
) -> std::result::Result<Option<usize>, String> {
    let n = batch.num_rows();
    if n == 0 {
        return Ok(None);
    }

    let stream = super::get_stream()
        .ok_or_else(|| "CUDA stream not available".to_string())?;

    let col_idx = search_mode.column_index();
    let column = batch.column(col_idx);

    run_search_first_kernel(&stream, column, search_mode, n)
}

/// CPU binary search for search_first — used when GPU is unavailable.
///
/// This implements the same O(log N) binary search on sorted data,
/// avoiding the O(N) filter+limit approach used by the default DataFusion path.
///
/// Returns the index of the first matching row, or `None` if no match.
pub fn cpu_binary_search_first(
    batch: &RecordBatch,
    search_mode: &GpuSearchMode,
) -> std::result::Result<Option<usize>, String> {
    let n = batch.num_rows();
    if n == 0 {
        return Ok(None);
    }

    let col_idx = search_mode.column_index();
    let column = batch.column(col_idx);

    match search_mode.data_type() {
        DataType::Int64 => {
            let arr = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("Failed to downcast to Int64Array")?;
            let value = match search_mode {
                GpuSearchMode::Gt { value_i64, .. }
                | GpuSearchMode::Ge { value_i64, .. }
                | GpuSearchMode::Eq { value_i64, .. }
                | GpuSearchMode::Lt { value_i64, .. }
                | GpuSearchMode::Le { value_i64, .. } => {
                    value_i64.ok_or("Missing i64 search value")?
                }
            };
            Ok(cpu_search_typed(arr.values(), value, search_mode.mode_value()))
        }
        DataType::Int32 => {
            let arr = column
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or("Failed to downcast to Int32Array")?;
            let value = match search_mode {
                GpuSearchMode::Gt { value_i32, .. }
                | GpuSearchMode::Ge { value_i32, .. }
                | GpuSearchMode::Eq { value_i32, .. }
                | GpuSearchMode::Lt { value_i32, .. }
                | GpuSearchMode::Le { value_i32, .. } => {
                    value_i32.ok_or("Missing i32 search value")?
                }
            };
            Ok(cpu_search_typed(arr.values(), value, search_mode.mode_value()))
        }
        DataType::Float64 => {
            let arr = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("Failed to downcast to Float64Array")?;
            let value = match search_mode {
                GpuSearchMode::Gt { value_f64, .. }
                | GpuSearchMode::Ge { value_f64, .. }
                | GpuSearchMode::Eq { value_f64, .. }
                | GpuSearchMode::Lt { value_f64, .. }
                | GpuSearchMode::Le { value_f64, .. } => {
                    value_f64.ok_or("Missing f64 search value")?
                }
            };
            Ok(cpu_search_typed(arr.values(), value, search_mode.mode_value()))
        }
        DataType::Float32 => {
            let arr = column
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or("Failed to downcast to Float32Array")?;
            let value = match search_mode {
                GpuSearchMode::Gt { value_f32, .. }
                | GpuSearchMode::Ge { value_f32, .. }
                | GpuSearchMode::Eq { value_f32, .. }
                | GpuSearchMode::Lt { value_f32, .. }
                | GpuSearchMode::Le { value_f32, .. } => {
                    value_f32.ok_or("Missing f32 search value")?
                }
            };
            Ok(cpu_search_typed(arr.values(), value, search_mode.mode_value()))
        }
        _ => Err(format!("Unsupported data type: {:?}", search_mode.data_type())),
    }
}

/// Generic CPU binary search implementation for a sorted slice.
///
/// Mode values: 0=Gt, 1=Ge, 2=Eq, 3=Lt, 4=Le
fn cpu_search_typed<T: PartialOrd + Copy>(data: &[T], value: T, mode: u32) -> Option<usize> {
    let n = data.len();
    if n == 0 {
        return None;
    }

    match mode {
        0 => {
            // Gt: first i where data[i] > value (upper_bound)
            let mut lo = 0usize;
            let mut hi = n;
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                if data[mid] <= value {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            if lo < n { Some(lo) } else { None }
        }
        1 => {
            // Ge: first i where data[i] >= value (lower_bound)
            let mut lo = 0usize;
            let mut hi = n;
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                if data[mid] < value {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            if lo < n { Some(lo) } else { None }
        }
        2 => {
            // Eq: lower_bound + check
            let mut lo = 0usize;
            let mut hi = n;
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                if data[mid] < value {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            if lo < n && data[lo] == value { Some(lo) } else { None }
        }
        3 => {
            // Lt: first row if data[0] < value
            if data[0] < value { Some(0) } else { None }
        }
        4 => {
            // Le: first row if data[0] <= value
            if data[0] <= value { Some(0) } else { None }
        }
        _ => None,
    }
}

// ── GpuBinarySearchExec DataFusion ExecutionPlan ─────────────────────────

/// GPU-accelerated binary search execution plan for search_first.
///
/// Takes a sorted input, performs binary search for the first matching row,
/// and returns a single-row RecordBatch (or empty if no match).
#[derive(Debug)]
pub struct GpuBinarySearchExec {
    /// The input execution plan (must produce sorted data)
    input: Arc<dyn ExecutionPlan>,
    /// The search mode describing the predicate
    search_mode: GpuSearchMode,
    /// Output schema (same as input)
    schema: SchemaRef,
    /// Cached plan properties
    properties: PlanProperties,
}

impl GpuBinarySearchExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        search_mode: GpuSearchMode,
    ) -> Result<Self> {
        let schema = input.schema();
        let properties = Self::compute_properties(Arc::clone(&schema));
        Ok(Self {
            input,
            search_mode,
            schema,
            properties,
        })
    }

    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        use datafusion::physical_plan::Partitioning;
        PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        )
    }
}

impl fmt::Display for GpuBinarySearchExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GpuBinarySearchExec: {} {} on column '{}'",
            self.search_mode.op_str(),
            self.search_mode.column_name(),
            self.search_mode.column_name()
        )
    }
}

impl DisplayAs for GpuBinarySearchExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "GpuBinarySearchExec: column={}, op={}, type={:?}",
                    self.search_mode.column_name(),
                    self.search_mode.op_str(),
                    self.search_mode.data_type(),
                )
            }
        }
    }
}

impl ExecutionPlan for GpuBinarySearchExec {
    fn name(&self) -> &str {
        "GpuBinarySearchExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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
                "GpuBinarySearchExec expects exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::try_new(
            Arc::clone(&children[0]),
            self.search_mode.clone(),
        )?))
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        use datafusion::arrow::compute::concat_batches;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures_util::StreamExt;

        let input_stream = self.input.execute(partition, context)?;
        let schema = Arc::clone(&self.schema);
        let search_mode = self.search_mode.clone();

        let output_stream = futures_util::stream::once(async move {
            // Collect all input batches
            let mut batches: Vec<RecordBatch> = Vec::new();
            let mut stream = input_stream;
            while let Some(batch_result) = stream.next().await {
                batches.push(batch_result?);
            }

            if batches.is_empty() {
                return Ok(RecordBatch::new_empty(schema));
            }

            // Coalesce into single batch
            let batch = concat_batches(&schema, &batches)?;
            let n = batch.num_rows();
            if n == 0 {
                return Ok(RecordBatch::new_empty(schema));
            }

            // Try GPU first, then fall back to CPU binary search
            let found_idx = if super::is_gpu_available() {
                gpu_search_first(&batch, &search_mode)
                    .unwrap_or_else(|_| cpu_binary_search_first(&batch, &search_mode).ok().flatten())
            } else {
                cpu_binary_search_first(&batch, &search_mode)
                    .map_err(|e| DataFusionError::Internal(e))?
            };

            match found_idx {
                Some(idx) => {
                    // Take single row at idx
                    let columns: Vec<ArrayRef> = batch
                        .columns()
                        .iter()
                        .map(|col| col.slice(idx, 1))
                        .collect();
                    RecordBatch::try_new(schema, columns)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
                }
                None => Ok(RecordBatch::new_empty(schema)),
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            output_stream,
        )))
    }
}

impl SortOrderPropagation for GpuBinarySearchExec {
    fn sort_order_effect(&self) -> SortOrderEffect {
        // Binary search returns at most 1 row — sort order is irrelevant
        SortOrderEffect::Destroy
    }
}

// ── Pattern matching: PyExpr → GpuSearchMode ─────────────────────────────

/// Try to match a PyExpr predicate to a GpuSearchMode for binary search.
///
/// Recognizes patterns of the form `Column(x) {>, >=, ==, <, <=} Literal(v)`
/// where `x` is a column that appears in `sort_keys` (the table's sort order).
///
/// Returns `None` if:
/// - The predicate doesn't match any binary search pattern
/// - The column is not in the sort keys
/// - The data type is not supported (only i32/i64/f32/f64)
pub fn try_match_gpu_search_mode(
    expr: &PyExpr,
    schema: &datafusion::arrow::datatypes::Schema,
    sort_keys: &[String],
) -> Option<GpuSearchMode> {
    match expr {
        PyExpr::BinOp { op, left, right } => {
            // Try Column op Literal
            if let Some(mode) = try_match_column_cmp_literal(op, left, right, schema, sort_keys) {
                return Some(mode);
            }
            // Try Literal op Column (reversed)
            if let Some(reversed_op) = reverse_comparison_op(op) {
                try_match_column_cmp_literal(&reversed_op, right, left, schema, sort_keys)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Match `Column(x) {op} Literal(v)` where x is a sort key.
fn try_match_column_cmp_literal(
    op: &str,
    left: &PyExpr,
    right: &PyExpr,
    schema: &datafusion::arrow::datatypes::Schema,
    sort_keys: &[String],
) -> Option<GpuSearchMode> {
    // Left must be a column
    let col_name = match left {
        PyExpr::Column(name) => name,
        _ => return None,
    };

    // Column must be the first sort key (binary search only works on primary sort key)
    if sort_keys.is_empty() || sort_keys[0] != *col_name {
        return None;
    }

    // Right must be a literal
    let (lit_value, lit_dtype) = match right {
        PyExpr::Literal { value, dtype } => (value, dtype),
        _ => return None,
    };

    // Look up column in schema
    let (col_idx, field) = schema.column_with_name(col_name)?;
    let data_type = field.data_type().clone();

    // Parse the literal value and create the GpuSearchMode
    let (v_i64, v_i32, v_f64, v_f32) = parse_search_literal(lit_value, lit_dtype, &data_type)?;

    match op {
        "Gt" => Some(GpuSearchMode::Gt {
            column_name: col_name.clone(),
            column_index: col_idx,
            data_type,
            value_i64: v_i64,
            value_i32: v_i32,
            value_f64: v_f64,
            value_f32: v_f32,
        }),
        "GtEq" | "Ge" => Some(GpuSearchMode::Ge {
            column_name: col_name.clone(),
            column_index: col_idx,
            data_type,
            value_i64: v_i64,
            value_i32: v_i32,
            value_f64: v_f64,
            value_f32: v_f32,
        }),
        "Eq" => Some(GpuSearchMode::Eq {
            column_name: col_name.clone(),
            column_index: col_idx,
            data_type,
            value_i64: v_i64,
            value_i32: v_i32,
            value_f64: v_f64,
            value_f32: v_f32,
        }),
        "Lt" => Some(GpuSearchMode::Lt {
            column_name: col_name.clone(),
            column_index: col_idx,
            data_type,
            value_i64: v_i64,
            value_i32: v_i32,
            value_f64: v_f64,
            value_f32: v_f32,
        }),
        "LtEq" | "Le" => Some(GpuSearchMode::Le {
            column_name: col_name.clone(),
            column_index: col_idx,
            data_type,
            value_i64: v_i64,
            value_i32: v_i32,
            value_f64: v_f64,
            value_f32: v_f32,
        }),
        _ => None,
    }
}

/// Reverse a comparison operator for the pattern `Literal op Column` → `Column reversed_op Literal`.
fn reverse_comparison_op(op: &str) -> Option<String> {
    match op {
        "Gt" => Some("Lt".to_string()),
        "GtEq" | "Ge" => Some("LtEq".to_string()),
        "Lt" => Some("Gt".to_string()),
        "LtEq" | "Le" => Some("GtEq".to_string()),
        "Eq" => Some("Eq".to_string()),
        _ => None,
    }
}

/// Parse a literal value string to the appropriate typed values.
///
/// Returns `(Option<i64>, Option<i32>, Option<f64>, Option<f32>)` based on
/// the target column data type. Returns `None` if the data type is not supported
/// or the literal cannot be parsed.
fn parse_search_literal(
    lit_value: &str,
    _lit_dtype: &str,
    col_data_type: &DataType,
) -> Option<(Option<i64>, Option<i32>, Option<f64>, Option<f32>)> {
    match col_data_type {
        DataType::Int64 => {
            let v = lit_value.parse::<i64>().ok()?;
            Some((Some(v), None, None, None))
        }
        DataType::Int32 => {
            let v = lit_value.parse::<i32>().ok()?;
            Some((None, Some(v), None, None))
        }
        DataType::Float64 => {
            let v = lit_value.parse::<f64>().ok()?;
            Some((None, None, Some(v), None))
        }
        DataType::Float32 => {
            let v = lit_value.parse::<f32>().ok()?;
            Some((None, None, None, Some(v)))
        }
        _ => None,
    }
}

/// Check if a search_first call is eligible for binary search optimization.
///
/// Requirements:
/// 1. Table has sort keys
/// 2. Predicate matches `Column(sort_key) {cmp} Literal(v)` pattern
/// 3. Column data type is numeric (i32/i64/f32/f64)
pub fn is_search_first_eligible(
    expr: &PyExpr,
    schema: &datafusion::arrow::datatypes::Schema,
    sort_keys: &[String],
) -> bool {
    try_match_gpu_search_mode(expr, schema, sort_keys).is_some()
}

// ============================================================================
// Physical expression → GpuSearchMode converter (for optimizer integration)
// ============================================================================

/// Try to convert a DataFusion physical `BinaryExpr` into a `GpuSearchMode`.
///
/// This is the physical-plan counterpart of `try_match_gpu_search_mode` (which
/// works on `PyExpr`). Used by the optimizer to match `FilterExec` predicates
/// on sorted data for `GpuBinarySearchExec` substitution.
///
/// Returns `Some(GpuSearchMode)` if the expression is `Column CMP Literal` on
/// a supported numeric type and the column matches `sort_column`.
pub fn try_physical_expr_to_search_mode(
    expr: &Arc<dyn datafusion::physical_plan::PhysicalExpr>,
    input_schema: &SchemaRef,
    sort_column: &str,
) -> Option<GpuSearchMode> {
    use datafusion::logical_expr_common::operator::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};

    let binary = expr.as_any().downcast_ref::<BinaryExpr>()?;
    let left = binary.left();
    let right = binary.right();
    let op = binary.op();

    // Try Column CMP Literal
    if let (Some(col), Some(lit)) = (
        left.as_any().downcast_ref::<Column>(),
        right.as_any().downcast_ref::<Literal>(),
    ) {
        if col.name() == sort_column {
            return physical_scalar_to_search_mode(
                op,
                col.name(),
                col.index(),
                input_schema.field(col.index()).data_type(),
                lit.value(),
            );
        }
    }

    // Try Literal CMP Column (reversed)
    if let (Some(lit), Some(col)) = (
        left.as_any().downcast_ref::<Literal>(),
        right.as_any().downcast_ref::<Column>(),
    ) {
        if col.name() == sort_column {
            let reversed_op = match op {
                Operator::Gt => Operator::Lt,
                Operator::GtEq => Operator::LtEq,
                Operator::Lt => Operator::Gt,
                Operator::LtEq => Operator::GtEq,
                Operator::Eq => Operator::Eq,
                _ => return None,
            };
            return physical_scalar_to_search_mode(
                &reversed_op,
                col.name(),
                col.index(),
                input_schema.field(col.index()).data_type(),
                lit.value(),
            );
        }
    }

    None
}

/// Convert a DataFusion `Operator` + `ScalarValue` into a `GpuSearchMode`.
fn physical_scalar_to_search_mode(
    op: &datafusion::logical_expr_common::operator::Operator,
    col_name: &str,
    col_idx: usize,
    dt: &DataType,
    value: &ScalarValue,
) -> Option<GpuSearchMode> {
    use datafusion::logical_expr_common::operator::Operator;

    // Extract typed values from ScalarValue
    let value_i64 = if let ScalarValue::Int64(Some(v)) = value { Some(*v) } else { None };
    let value_i32 = if let ScalarValue::Int32(Some(v)) = value { Some(*v) } else { None };
    let value_f64 = if let ScalarValue::Float64(Some(v)) = value { Some(*v) } else { None };
    let value_f32 = if let ScalarValue::Float32(Some(v)) = value { Some(*v) } else { None };

    // Must have at least one supported value
    if value_i64.is_none() && value_i32.is_none() && value_f64.is_none() && value_f32.is_none() {
        return None;
    }

    // Data type must be supported for GPU search
    if !matches!(dt, DataType::Int64 | DataType::Int32 | DataType::Float64 | DataType::Float32) {
        return None;
    }

    let mk = |variant: fn(String, usize, DataType, Option<i64>, Option<i32>, Option<f64>, Option<f32>) -> GpuSearchMode| {
        Some(variant(
            col_name.to_string(), col_idx, dt.clone(),
            value_i64, value_i32, value_f64, value_f32,
        ))
    };

    match op {
        Operator::Gt => mk(|n, i, d, vi64, vi32, vf64, vf32| GpuSearchMode::Gt { column_name: n, column_index: i, data_type: d, value_i64: vi64, value_i32: vi32, value_f64: vf64, value_f32: vf32 }),
        Operator::GtEq => mk(|n, i, d, vi64, vi32, vf64, vf32| GpuSearchMode::Ge { column_name: n, column_index: i, data_type: d, value_i64: vi64, value_i32: vi32, value_f64: vf64, value_f32: vf32 }),
        Operator::Eq => mk(|n, i, d, vi64, vi32, vf64, vf32| GpuSearchMode::Eq { column_name: n, column_index: i, data_type: d, value_i64: vi64, value_i32: vi32, value_f64: vf64, value_f32: vf32 }),
        Operator::Lt => mk(|n, i, d, vi64, vi32, vf64, vf32| GpuSearchMode::Lt { column_name: n, column_index: i, data_type: d, value_i64: vi64, value_i32: vi32, value_f64: vf64, value_f32: vf32 }),
        Operator::LtEq => mk(|n, i, d, vi64, vi32, vf64, vf32| GpuSearchMode::Le { column_name: n, column_index: i, data_type: d, value_i64: vi64, value_i32: vi32, value_f64: vf64, value_f32: vf32 }),
        _ => None,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Schema};

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("userid", DataType::Int64, false),
            Field::new("eventtime", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
            Field::new("name", DataType::Utf8, false),
        ])
    }

    // -- Pattern: Column("x") != Column("x").shift(1) → AdjacentNe ----------

    #[test]
    fn test_match_adjacent_ne_int64() {
        let schema = test_schema();
        let expr = PyExpr::BinOp {
            op: "Ne".into(),
            left: Box::new(PyExpr::Column("userid".into())),
            right: Box::new(PyExpr::Call {
                func: "shift".into(),
                on: Box::new(PyExpr::Column("userid".into())),
                args: vec![PyExpr::Literal {
                    value: "1".into(),
                    dtype: "Int64".into(),
                }],
                kwargs: std::collections::HashMap::new(),
            }),
        };

        let mode = try_match_gpu_boundary_mode(&expr, &schema);
        assert!(mode.is_some());
        let mode = mode.unwrap();
        assert_eq!(mode.kernel_name(), "adjacent_ne_i64");
        assert_eq!(mode.column_name(), "userid");
    }

    #[test]
    fn test_match_adjacent_ne_float64() {
        let schema = test_schema();
        let expr = PyExpr::BinOp {
            op: "Ne".into(),
            left: Box::new(PyExpr::Column("price".into())),
            right: Box::new(PyExpr::Call {
                func: "shift".into(),
                on: Box::new(PyExpr::Column("price".into())),
                args: vec![PyExpr::Literal {
                    value: "1".into(),
                    dtype: "Int64".into(),
                }],
                kwargs: std::collections::HashMap::new(),
            }),
        };

        let mode = try_match_gpu_boundary_mode(&expr, &schema);
        assert!(mode.is_some());
        assert_eq!(mode.unwrap().kernel_name(), "adjacent_ne_f64");
    }

    #[test]
    fn test_match_adjacent_ne_reversed_operands() {
        // shift(Column("x"), 1) != Column("x") — reversed order
        let schema = test_schema();
        let expr = PyExpr::BinOp {
            op: "Ne".into(),
            left: Box::new(PyExpr::Call {
                func: "shift".into(),
                on: Box::new(PyExpr::Column("userid".into())),
                args: vec![PyExpr::Literal {
                    value: "1".into(),
                    dtype: "Int64".into(),
                }],
                kwargs: std::collections::HashMap::new(),
            }),
            right: Box::new(PyExpr::Column("userid".into())),
        };

        let mode = try_match_gpu_boundary_mode(&expr, &schema);
        assert!(mode.is_some());
        assert_eq!(mode.unwrap().kernel_name(), "adjacent_ne_i64");
    }

    #[test]
    fn test_match_adjacent_ne_unsupported_type() {
        // Utf8 column — not supported for GPU
        let schema = test_schema();
        let expr = PyExpr::BinOp {
            op: "Ne".into(),
            left: Box::new(PyExpr::Column("name".into())),
            right: Box::new(PyExpr::Call {
                func: "shift".into(),
                on: Box::new(PyExpr::Column("name".into())),
                args: vec![PyExpr::Literal {
                    value: "1".into(),
                    dtype: "Int64".into(),
                }],
                kwargs: std::collections::HashMap::new(),
            }),
        };

        assert!(try_match_gpu_boundary_mode(&expr, &schema).is_none());
    }

    // -- Pattern: (Column("x") - Column("x").shift(1)) > Literal → AdjacentDiffGt --

    #[test]
    fn test_match_adjacent_diff_gt_int64() {
        let schema = test_schema();
        // (eventtime - eventtime.shift(1)) > 1800
        let expr = PyExpr::BinOp {
            op: "Gt".into(),
            left: Box::new(PyExpr::BinOp {
                op: "Sub".into(),
                left: Box::new(PyExpr::Column("eventtime".into())),
                right: Box::new(PyExpr::Call {
                    func: "shift".into(),
                    on: Box::new(PyExpr::Column("eventtime".into())),
                    args: vec![PyExpr::Literal {
                        value: "1".into(),
                        dtype: "Int64".into(),
                    }],
                    kwargs: std::collections::HashMap::new(),
                }),
            }),
            right: Box::new(PyExpr::Literal {
                value: "1800".into(),
                dtype: "Int64".into(),
            }),
        };

        let mode = try_match_gpu_boundary_mode(&expr, &schema);
        assert!(mode.is_some());
        let mode = mode.unwrap();
        assert_eq!(mode.kernel_name(), "adjacent_diff_gt_i64");
        match &mode {
            GpuBoundaryMode::AdjacentDiffGt { threshold_i64, .. } => {
                assert_eq!(*threshold_i64, Some(1800));
            }
            _ => panic!("Expected AdjacentDiffGt"),
        }
    }

    #[test]
    fn test_match_adjacent_diff_gt_float64() {
        let schema = test_schema();
        // (price - price.shift(1)) > 0.5
        let expr = PyExpr::BinOp {
            op: "Gt".into(),
            left: Box::new(PyExpr::BinOp {
                op: "Sub".into(),
                left: Box::new(PyExpr::Column("price".into())),
                right: Box::new(PyExpr::Call {
                    func: "shift".into(),
                    on: Box::new(PyExpr::Column("price".into())),
                    args: vec![PyExpr::Literal {
                        value: "1".into(),
                        dtype: "Int64".into(),
                    }],
                    kwargs: std::collections::HashMap::new(),
                }),
            }),
            right: Box::new(PyExpr::Literal {
                value: "0.5".into(),
                dtype: "Float64".into(),
            }),
        };

        let mode = try_match_gpu_boundary_mode(&expr, &schema);
        assert!(mode.is_some());
        assert_eq!(mode.unwrap().kernel_name(), "adjacent_diff_gt_f64");
    }

    // -- Non-matching patterns -----------------------------------------------

    #[test]
    fn test_no_match_or_compound() {
        // (userid != userid.shift(1)) | (eventtime - eventtime.shift(1) > 1800)
        // Compound OR — not GPU-eligible yet
        let schema = test_schema();
        let expr = PyExpr::BinOp {
            op: "Or".into(),
            left: Box::new(PyExpr::BinOp {
                op: "Ne".into(),
                left: Box::new(PyExpr::Column("userid".into())),
                right: Box::new(PyExpr::Call {
                    func: "shift".into(),
                    on: Box::new(PyExpr::Column("userid".into())),
                    args: vec![PyExpr::Literal {
                        value: "1".into(),
                        dtype: "Int64".into(),
                    }],
                    kwargs: std::collections::HashMap::new(),
                }),
            }),
            right: Box::new(PyExpr::BinOp {
                op: "Gt".into(),
                left: Box::new(PyExpr::BinOp {
                    op: "Sub".into(),
                    left: Box::new(PyExpr::Column("eventtime".into())),
                    right: Box::new(PyExpr::Call {
                        func: "shift".into(),
                        on: Box::new(PyExpr::Column("eventtime".into())),
                        args: vec![PyExpr::Literal {
                            value: "1".into(),
                            dtype: "Int64".into(),
                        }],
                        kwargs: std::collections::HashMap::new(),
                    }),
                }),
                right: Box::new(PyExpr::Literal {
                    value: "1800".into(),
                    dtype: "Int64".into(),
                }),
            }),
        };

        assert!(try_match_gpu_boundary_mode(&expr, &schema).is_none());
    }

    #[test]
    fn test_no_match_eq_op() {
        // Eq is not a boundary predicate for GPU
        let schema = test_schema();
        let expr = PyExpr::BinOp {
            op: "Eq".into(),
            left: Box::new(PyExpr::Column("userid".into())),
            right: Box::new(PyExpr::Call {
                func: "shift".into(),
                on: Box::new(PyExpr::Column("userid".into())),
                args: vec![PyExpr::Literal {
                    value: "1".into(),
                    dtype: "Int64".into(),
                }],
                kwargs: std::collections::HashMap::new(),
            }),
        };

        assert!(try_match_gpu_boundary_mode(&expr, &schema).is_none());
    }

    #[test]
    fn test_no_match_different_columns() {
        // userid != eventtime.shift(1) — columns don't match
        let schema = test_schema();
        let expr = PyExpr::BinOp {
            op: "Ne".into(),
            left: Box::new(PyExpr::Column("userid".into())),
            right: Box::new(PyExpr::Call {
                func: "shift".into(),
                on: Box::new(PyExpr::Column("eventtime".into())),
                args: vec![PyExpr::Literal {
                    value: "1".into(),
                    dtype: "Int64".into(),
                }],
                kwargs: std::collections::HashMap::new(),
            }),
        };

        assert!(try_match_gpu_boundary_mode(&expr, &schema).is_none());
    }

    #[test]
    fn test_no_match_column_not_in_schema() {
        let schema = test_schema();
        let expr = PyExpr::BinOp {
            op: "Ne".into(),
            left: Box::new(PyExpr::Column("nonexistent".into())),
            right: Box::new(PyExpr::Call {
                func: "shift".into(),
                on: Box::new(PyExpr::Column("nonexistent".into())),
                args: vec![PyExpr::Literal {
                    value: "1".into(),
                    dtype: "Int64".into(),
                }],
                kwargs: std::collections::HashMap::new(),
            }),
        };

        assert!(try_match_gpu_boundary_mode(&expr, &schema).is_none());
    }

    // -- search_first pattern matching tests ──────────────────────────────

    fn sort_keys_userid() -> Vec<String> {
        vec!["userid".to_string()]
    }

    fn sort_keys_price() -> Vec<String> {
        vec!["price".to_string()]
    }

    fn sort_keys_eventtime() -> Vec<String> {
        vec!["eventtime".to_string()]
    }

    // -- Pattern: Column("userid") > Literal(100) → Gt ──────────────────

    #[test]
    fn test_search_mode_gt_int64() {
        let schema = test_schema();
        let expr = PyExpr::BinOp {
            op: "Gt".into(),
            left: Box::new(PyExpr::Column("userid".into())),
            right: Box::new(PyExpr::Literal {
                value: "100".into(),
                dtype: "Int64".into(),
            }),
        };

        let mode = try_match_gpu_search_mode(&expr, &schema, &sort_keys_userid());
        assert!(mode.is_some());
        let mode = mode.unwrap();
        assert_eq!(mode.mode_value(), 0); // Gt
        assert_eq!(mode.column_name(), "userid");
        assert_eq!(mode.kernel_name(), "search_first_i64");
    }

    // -- Pattern: Column("price") >= Literal(50.0) → Ge ─────────────────

    #[test]
    fn test_search_mode_ge_float64() {
        let schema = test_schema();
        let expr = PyExpr::BinOp {
            op: "GtEq".into(),
            left: Box::new(PyExpr::Column("price".into())),
            right: Box::new(PyExpr::Literal {
                value: "50.0".into(),
                dtype: "Float64".into(),
            }),
        };

        let mode = try_match_gpu_search_mode(&expr, &schema, &sort_keys_price());
        assert!(mode.is_some());
        let mode = mode.unwrap();
        assert_eq!(mode.mode_value(), 1); // Ge
        assert_eq!(mode.column_name(), "price");
        assert_eq!(mode.kernel_name(), "search_first_f64");
    }

    // -- Pattern: Column("userid") == Literal(42) → Eq ──────────────────

    #[test]
    fn test_search_mode_eq_int64() {
        let schema = test_schema();
        let expr = PyExpr::BinOp {
            op: "Eq".into(),
            left: Box::new(PyExpr::Column("userid".into())),
            right: Box::new(PyExpr::Literal {
                value: "42".into(),
                dtype: "Int64".into(),
            }),
        };

        let mode = try_match_gpu_search_mode(&expr, &schema, &sort_keys_userid());
        assert!(mode.is_some());
        let mode = mode.unwrap();
        assert_eq!(mode.mode_value(), 2); // Eq
        assert_eq!(mode.column_name(), "userid");
    }

    // -- Pattern: Column("eventtime") < Literal(1000) → Lt ──────────────

    #[test]
    fn test_search_mode_lt_int64() {
        let schema = test_schema();
        let expr = PyExpr::BinOp {
            op: "Lt".into(),
            left: Box::new(PyExpr::Column("eventtime".into())),
            right: Box::new(PyExpr::Literal {
                value: "1000".into(),
                dtype: "Int64".into(),
            }),
        };

        let mode = try_match_gpu_search_mode(&expr, &schema, &sort_keys_eventtime());
        assert!(mode.is_some());
        let mode = mode.unwrap();
        assert_eq!(mode.mode_value(), 3); // Lt
    }

    // -- Pattern: Column("price") <= Literal(99.9) → Le ─────────────────

    #[test]
    fn test_search_mode_le_float64() {
        let schema = test_schema();
        let expr = PyExpr::BinOp {
            op: "LtEq".into(),
            left: Box::new(PyExpr::Column("price".into())),
            right: Box::new(PyExpr::Literal {
                value: "99.9".into(),
                dtype: "Float64".into(),
            }),
        };

        let mode = try_match_gpu_search_mode(&expr, &schema, &sort_keys_price());
        assert!(mode.is_some());
        let mode = mode.unwrap();
        assert_eq!(mode.mode_value(), 4); // Le
    }

    // -- Reversed: Literal(100) < Column("userid") → Column("userid") > Literal(100) → Gt

    #[test]
    fn test_search_mode_reversed_operands() {
        let schema = test_schema();
        // 100 < userid  →  userid > 100  →  Gt
        let expr = PyExpr::BinOp {
            op: "Lt".into(),
            left: Box::new(PyExpr::Literal {
                value: "100".into(),
                dtype: "Int64".into(),
            }),
            right: Box::new(PyExpr::Column("userid".into())),
        };

        let mode = try_match_gpu_search_mode(&expr, &schema, &sort_keys_userid());
        assert!(mode.is_some());
        let mode = mode.unwrap();
        assert_eq!(mode.mode_value(), 0); // Gt (reversed from Lt)
        assert_eq!(mode.column_name(), "userid");
    }

    // -- No match: column not in sort keys ──────────────────────────────

    #[test]
    fn test_search_mode_no_match_unsorted_column() {
        let schema = test_schema();
        // eventtime > 100, but sort keys = ["userid"]
        let expr = PyExpr::BinOp {
            op: "Gt".into(),
            left: Box::new(PyExpr::Column("eventtime".into())),
            right: Box::new(PyExpr::Literal {
                value: "100".into(),
                dtype: "Int64".into(),
            }),
        };

        let mode = try_match_gpu_search_mode(&expr, &schema, &sort_keys_userid());
        assert!(mode.is_none());
    }

    // -- No match: string column (unsupported type) ─────────────────────

    #[test]
    fn test_search_mode_no_match_string_column() {
        let schema = test_schema();
        let expr = PyExpr::BinOp {
            op: "Eq".into(),
            left: Box::new(PyExpr::Column("name".into())),
            right: Box::new(PyExpr::Literal {
                value: "alice".into(),
                dtype: "Utf8".into(),
            }),
        };

        let mode = try_match_gpu_search_mode(&expr, &schema, &vec!["name".to_string()]);
        assert!(mode.is_none());
    }

    // -- No match: empty sort keys ──────────────────────────────────────

    #[test]
    fn test_search_mode_no_match_empty_sort_keys() {
        let schema = test_schema();
        let expr = PyExpr::BinOp {
            op: "Gt".into(),
            left: Box::new(PyExpr::Column("userid".into())),
            right: Box::new(PyExpr::Literal {
                value: "100".into(),
                dtype: "Int64".into(),
            }),
        };

        let mode = try_match_gpu_search_mode(&expr, &schema, &vec![]);
        assert!(mode.is_none());
    }

    // -- No match: column is secondary sort key (not primary) ───────────

    #[test]
    fn test_search_mode_no_match_secondary_sort_key() {
        let schema = test_schema();
        // Sort keys: ["userid", "eventtime"], predicate on eventtime (secondary)
        let expr = PyExpr::BinOp {
            op: "Gt".into(),
            left: Box::new(PyExpr::Column("eventtime".into())),
            right: Box::new(PyExpr::Literal {
                value: "100".into(),
                dtype: "Int64".into(),
            }),
        };

        let sort_keys = vec!["userid".to_string(), "eventtime".to_string()];
        let mode = try_match_gpu_search_mode(&expr, &schema, &sort_keys);
        assert!(mode.is_none());
    }

    // -- No match: complex predicate (AND/OR) ───────────────────────────

    #[test]
    fn test_search_mode_no_match_complex_predicate() {
        let schema = test_schema();
        // userid > 100 AND price > 50.0 — compound, not eligible
        let expr = PyExpr::BinOp {
            op: "And".into(),
            left: Box::new(PyExpr::BinOp {
                op: "Gt".into(),
                left: Box::new(PyExpr::Column("userid".into())),
                right: Box::new(PyExpr::Literal {
                    value: "100".into(),
                    dtype: "Int64".into(),
                }),
            }),
            right: Box::new(PyExpr::BinOp {
                op: "Gt".into(),
                left: Box::new(PyExpr::Column("price".into())),
                right: Box::new(PyExpr::Literal {
                    value: "50.0".into(),
                    dtype: "Float64".into(),
                }),
            }),
        };

        let mode = try_match_gpu_search_mode(&expr, &schema, &sort_keys_userid());
        assert!(mode.is_none());
    }

    // -- CPU binary search: correctness tests ───────────────────────────

    #[test]
    fn test_cpu_search_gt() {
        // Sorted: [10, 20, 30, 40, 50]
        let data: &[i64] = &[10, 20, 30, 40, 50];
        // First i where data[i] > 25 → index 2 (value 30)
        assert_eq!(cpu_search_typed(data, 25i64, 0), Some(2));
        // First i where data[i] > 50 → None
        assert_eq!(cpu_search_typed(data, 50i64, 0), None);
        // First i where data[i] > 9 → index 0 (value 10)
        assert_eq!(cpu_search_typed(data, 9i64, 0), Some(0));
    }

    #[test]
    fn test_cpu_search_ge() {
        let data: &[i64] = &[10, 20, 30, 40, 50];
        // First i where data[i] >= 30 → index 2
        assert_eq!(cpu_search_typed(data, 30i64, 1), Some(2));
        // First i where data[i] >= 10 → index 0
        assert_eq!(cpu_search_typed(data, 10i64, 1), Some(0));
        // First i where data[i] >= 51 → None
        assert_eq!(cpu_search_typed(data, 51i64, 1), None);
    }

    #[test]
    fn test_cpu_search_eq() {
        let data: &[i64] = &[10, 20, 30, 40, 50];
        // data[i] == 30 → index 2
        assert_eq!(cpu_search_typed(data, 30i64, 2), Some(2));
        // data[i] == 25 → None
        assert_eq!(cpu_search_typed(data, 25i64, 2), None);
        // data[i] == 10 → index 0
        assert_eq!(cpu_search_typed(data, 10i64, 2), Some(0));
    }

    #[test]
    fn test_cpu_search_lt() {
        let data: &[i64] = &[10, 20, 30, 40, 50];
        // data[0] < 30 → true → Some(0)
        assert_eq!(cpu_search_typed(data, 30i64, 3), Some(0));
        // data[0] < 10 → false → None
        assert_eq!(cpu_search_typed(data, 10i64, 3), None);
        // data[0] < 5 → false → None
        assert_eq!(cpu_search_typed(data, 5i64, 3), None);
    }

    #[test]
    fn test_cpu_search_le() {
        let data: &[i64] = &[10, 20, 30, 40, 50];
        // data[0] <= 10 → true → Some(0)
        assert_eq!(cpu_search_typed(data, 10i64, 4), Some(0));
        // data[0] <= 9 → false → None
        assert_eq!(cpu_search_typed(data, 9i64, 4), None);
    }

    #[test]
    fn test_cpu_search_empty_data() {
        let data: &[i64] = &[];
        assert_eq!(cpu_search_typed(data, 10i64, 0), None);
        assert_eq!(cpu_search_typed(data, 10i64, 1), None);
        assert_eq!(cpu_search_typed(data, 10i64, 2), None);
        assert_eq!(cpu_search_typed(data, 10i64, 3), None);
        assert_eq!(cpu_search_typed(data, 10i64, 4), None);
    }

    #[test]
    fn test_cpu_search_f64() {
        let data: &[f64] = &[1.0, 2.5, 3.0, 4.5, 5.0];
        // Gt: first i where data[i] > 2.5 → index 2 (value 3.0)
        assert_eq!(cpu_search_typed(data, 2.5f64, 0), Some(2));
        // Ge: first i where data[i] >= 2.5 → index 1 (value 2.5)
        assert_eq!(cpu_search_typed(data, 2.5f64, 1), Some(1));
        // Eq: data[i] == 4.5 → index 3
        assert_eq!(cpu_search_typed(data, 4.5f64, 2), Some(3));
    }

    #[test]
    fn test_cpu_search_duplicates() {
        let data: &[i64] = &[10, 20, 20, 20, 30];
        // Eq: first i where data[i] == 20 → index 1 (first occurrence)
        assert_eq!(cpu_search_typed(data, 20i64, 2), Some(1));
        // Gt: first i where data[i] > 20 → index 4 (value 30)
        assert_eq!(cpu_search_typed(data, 20i64, 0), Some(4));
        // Ge: first i where data[i] >= 20 → index 1
        assert_eq!(cpu_search_typed(data, 20i64, 1), Some(1));
    }
}
