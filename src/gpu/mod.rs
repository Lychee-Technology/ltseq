//! GPU acceleration for LTSeq via CUDA.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │  GpuQueryPlanner  (QueryPlanner trait)   │  ← Phase 2
//! │  Detects GPU-compatible logical plans    │
//! │  Propagates sort metadata from CPU plan  │
//! │  (Phase 4: routes to GpuExecNode)        │
//! └──────────────────┬──────────────────────┘
//!                    │ physical plan + sort metadata
//! ┌──────────────────▼──────────────────────┐
//! │  HostToGpuRule  (PhysicalOptimizerRule)  │  ← Phase 1 (sort-aware)
//! │  Reads output_ordering() from inputs     │
//! │  Chooses ordered vs unordered GPU ops:   │
//! │    FilterExec → GpuFilterExec            │
//! │    (Phase 1.2+: AggExec → GpuSegmented/  │
//! │     GpuMergeJoin / GpuBinarySearch)      │
//! └──────────────────┬──────────────────────┘
//!                    │
//! ┌──────────────────▼──────────────────────┐
//! │  GPU Exec Nodes (SortOrderPropagation)   │
//! │  GpuFilterExec     → Preserve            │
//! │  GpuExecNode       → Preserve/Produce    │
//! │  (Phase 1.2+: GpuConsecutiveGroupExec,   │
//! │   GpuSegmentedAggregateExec, etc.)       │
//! └─────────────────────────────────────────┘
//! ```
//!
//! `GpuExecNode` (Phase 2/3) is a leaf node that wraps a CPU fallback plan
//! and optionally stores Substrait bytes for the future C++ libcudf engine.
//! `GpuQueryPlanner` is registered via `SessionStateBuilder::with_query_planner`.
//!
//! `sort_aware` module provides the `SortOrderPropagation` trait and utilities
//! for extracting sort metadata from DataFusion's `EquivalenceProperties`.

pub mod exec_node;
pub mod ffi;
pub mod filter_exec;
pub mod merge_join;
pub mod optimizer;
pub mod ordered_ops;
pub mod planner;
pub mod raw_exec;
pub mod sort_aware;
pub mod substrait;

use cudarc::driver::safe::{CudaContext, CudaFunction, CudaModule, CudaStream};
use cudarc::nvrtc::compile_ptx;
use std::sync::{Arc, OnceLock};

/// Minimum number of rows before offloading to GPU.
/// Below this threshold, CPU execution is faster due to H2D/D2H transfer overhead.
pub const GPU_MIN_ROWS_THRESHOLD: usize = 100_000;

/// Cached GPU resources: context, stream, and compiled filter module.
struct GpuResources {
    ctx: Arc<CudaContext>,
    stream: Arc<CudaStream>,
    filter_module: Arc<CudaModule>,
}

/// Global GPU resources singleton.
static GPU_RESOURCES: OnceLock<Option<GpuResources>> = OnceLock::new();

/// Initialize GPU resources: context, default stream, and compile filter kernels.
fn init_gpu_resources() -> Option<GpuResources> {
    let ctx = CudaContext::new(0).ok()?;
    let stream = ctx.default_stream();
    let ptx = compile_ptx(FILTER_KERNEL_SRC).ok()?;
    let filter_module = ctx.load_module(ptx).ok()?;

    // Also initialise the C++ libcuDF engine (idempotent; uses the same GPU).
    // 512 MiB pool covers typical workloads; grow on demand.
    ffi::cudf_engine_startup(512 * 1024 * 1024, 256 * 1024 * 1024);

    Some(GpuResources {
        ctx,
        stream,
        filter_module,
    })
}

/// Check if GPU acceleration is available.
pub fn is_gpu_available() -> bool {
    GPU_RESOURCES
        .get_or_init(|| init_gpu_resources())
        .is_some()
}

/// Get the CUDA context, or `None` if GPU is unavailable.
pub fn get_context() -> Option<Arc<CudaContext>> {
    GPU_RESOURCES
        .get_or_init(|| init_gpu_resources())
        .as_ref()
        .map(|r| Arc::clone(&r.ctx))
}

/// Get the default CUDA stream, or `None` if GPU is unavailable.
pub fn get_stream() -> Option<Arc<CudaStream>> {
    GPU_RESOURCES
        .get_or_init(|| init_gpu_resources())
        .as_ref()
        .map(|r| Arc::clone(&r.stream))
}

/// Get a compiled filter kernel function by name.
/// Returns `None` if GPU is unavailable or the function doesn't exist.
pub fn get_filter_function(name: &str) -> Option<CudaFunction> {
    GPU_RESOURCES
        .get_or_init(|| init_gpu_resources())
        .as_ref()
        .and_then(|r| r.filter_module.load_function(name).ok())
}

/// CUDA kernel source for filter operations.
/// Compiled at runtime via NVRTC (NVIDIA Runtime Compilation).
///
/// Kernel naming convention: `filter_{op}_{type}`
/// - op: gt, lt, gte, lte, eq, neq
/// - type: i32, i64, f32, f64
///
/// Each kernel takes:
/// - `data`: pointer to the column data
/// - `threshold`: scalar comparison value
/// - `mask`: output boolean mask (u8: 0 or 1)
/// - `n`: number of elements
const FILTER_KERNEL_SRC: &str = r#"
extern "C" __global__ void filter_gt_i64(
    const long long* __restrict__ data,
    const long long threshold,
    unsigned char* __restrict__ mask,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        mask[idx] = (data[idx] > threshold) ? 1 : 0;
    }
}

extern "C" __global__ void filter_lt_i64(
    const long long* __restrict__ data,
    const long long threshold,
    unsigned char* __restrict__ mask,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        mask[idx] = (data[idx] < threshold) ? 1 : 0;
    }
}

extern "C" __global__ void filter_gte_i64(
    const long long* __restrict__ data,
    const long long threshold,
    unsigned char* __restrict__ mask,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        mask[idx] = (data[idx] >= threshold) ? 1 : 0;
    }
}

extern "C" __global__ void filter_lte_i64(
    const long long* __restrict__ data,
    const long long threshold,
    unsigned char* __restrict__ mask,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        mask[idx] = (data[idx] <= threshold) ? 1 : 0;
    }
}

extern "C" __global__ void filter_eq_i64(
    const long long* __restrict__ data,
    const long long threshold,
    unsigned char* __restrict__ mask,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        mask[idx] = (data[idx] == threshold) ? 1 : 0;
    }
}

extern "C" __global__ void filter_neq_i64(
    const long long* __restrict__ data,
    const long long threshold,
    unsigned char* __restrict__ mask,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        mask[idx] = (data[idx] != threshold) ? 1 : 0;
    }
}

extern "C" __global__ void filter_gt_f64(
    const double* __restrict__ data,
    const double threshold,
    unsigned char* __restrict__ mask,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        mask[idx] = (data[idx] > threshold) ? 1 : 0;
    }
}

extern "C" __global__ void filter_lt_f64(
    const double* __restrict__ data,
    const double threshold,
    unsigned char* __restrict__ mask,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        mask[idx] = (data[idx] < threshold) ? 1 : 0;
    }
}

extern "C" __global__ void filter_gte_f64(
    const double* __restrict__ data,
    const double threshold,
    unsigned char* __restrict__ mask,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        mask[idx] = (data[idx] >= threshold) ? 1 : 0;
    }
}

extern "C" __global__ void filter_lte_f64(
    const double* __restrict__ data,
    const double threshold,
    unsigned char* __restrict__ mask,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        mask[idx] = (data[idx] <= threshold) ? 1 : 0;
    }
}

extern "C" __global__ void filter_eq_f64(
    const double* __restrict__ data,
    const double threshold,
    unsigned char* __restrict__ mask,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        mask[idx] = (data[idx] == threshold) ? 1 : 0;
    }
}

extern "C" __global__ void filter_neq_f64(
    const double* __restrict__ data,
    const double threshold,
    unsigned char* __restrict__ mask,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        mask[idx] = (data[idx] != threshold) ? 1 : 0;
    }
}

extern "C" __global__ void filter_gt_i32(
    const int* __restrict__ data,
    const int threshold,
    unsigned char* __restrict__ mask,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        mask[idx] = (data[idx] > threshold) ? 1 : 0;
    }
}

extern "C" __global__ void filter_lt_i32(
    const int* __restrict__ data,
    const int threshold,
    unsigned char* __restrict__ mask,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        mask[idx] = (data[idx] < threshold) ? 1 : 0;
    }
}

extern "C" __global__ void filter_gte_i32(
    const int* __restrict__ data,
    const int threshold,
    unsigned char* __restrict__ mask,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        mask[idx] = (data[idx] >= threshold) ? 1 : 0;
    }
}

extern "C" __global__ void filter_lte_i32(
    const int* __restrict__ data,
    const int threshold,
    unsigned char* __restrict__ mask,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        mask[idx] = (data[idx] <= threshold) ? 1 : 0;
    }
}

extern "C" __global__ void filter_eq_i32(
    const int* __restrict__ data,
    const int threshold,
    unsigned char* __restrict__ mask,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        mask[idx] = (data[idx] == threshold) ? 1 : 0;
    }
}

extern "C" __global__ void filter_neq_i32(
    const int* __restrict__ data,
    const int threshold,
    unsigned char* __restrict__ mask,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        mask[idx] = (data[idx] != threshold) ? 1 : 0;
    }
}

extern "C" __global__ void filter_gt_f32(
    const float* __restrict__ data,
    const float threshold,
    unsigned char* __restrict__ mask,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        mask[idx] = (data[idx] > threshold) ? 1 : 0;
    }
}

extern "C" __global__ void filter_lt_f32(
    const float* __restrict__ data,
    const float threshold,
    unsigned char* __restrict__ mask,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        mask[idx] = (data[idx] < threshold) ? 1 : 0;
    }
}

extern "C" __global__ void filter_gte_f32(
    const float* __restrict__ data,
    const float threshold,
    unsigned char* __restrict__ mask,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        mask[idx] = (data[idx] >= threshold) ? 1 : 0;
    }
}

extern "C" __global__ void filter_lte_f32(
    const float* __restrict__ data,
    const float threshold,
    unsigned char* __restrict__ mask,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        mask[idx] = (data[idx] <= threshold) ? 1 : 0;
    }
}

extern "C" __global__ void filter_eq_f32(
    const float* __restrict__ data,
    const float threshold,
    unsigned char* __restrict__ mask,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        mask[idx] = (data[idx] == threshold) ? 1 : 0;
    }
}

extern "C" __global__ void filter_neq_f32(
    const float* __restrict__ data,
    const float threshold,
    unsigned char* __restrict__ mask,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        mask[idx] = (data[idx] != threshold) ? 1 : 0;
    }
}
"#;

/// CUDA block size for kernel launches.
pub const CUDA_BLOCK_SIZE: u32 = 256;

/// Calculate grid size for a given number of elements.
pub fn grid_size(n: u32) -> u32 {
    (n + CUDA_BLOCK_SIZE - 1) / CUDA_BLOCK_SIZE
}
