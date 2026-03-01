//! GPU acceleration module for LTSeq using wgpu.
//!
//! This module provides cross-platform GPU acceleration for specific operators:
//! - Filter (element-wise predicate evaluation)
//! - Derive/Projection (element-wise arithmetic)
//! - Aggregation (hash-based GROUP BY)
//! - AsOf Join (parallel binary search)
//!
//! All GPU paths have automatic CPU fallback when:
//! - No GPU adapter is available
//! - `LTSEQ_DISABLE_GPU=1` environment variable is set
//! - Data is below the size threshold
//! - Column types are not GPU-compatible

pub mod aggregate_exec;
pub mod buffer_pool;
pub mod buffers;
pub mod codegen;
pub mod context;
pub mod filter_exec;
pub mod fused_exec;
pub mod fused_filter;
pub mod optimizer;
pub mod projection_exec;

use context::GPU_CONTEXT;

/// Check whether GPU acceleration is available.
///
/// Returns `true` if:
/// - The `gpu` feature is enabled (compile-time)
/// - `LTSEQ_DISABLE_GPU` environment variable is NOT set
/// - A wgpu adapter was successfully initialized
pub fn is_gpu_available() -> bool {
    GPU_CONTEXT.is_some()
}

/// Enumerate all available GPU adapters without touching the singleton.
///
/// Returns adapter metadata (index, name, backend, device_type) for every adapter
/// that wgpu can see across all backends. Safe to call before GPU init.
pub fn enumerate_gpus() -> Vec<context::GpuAdapterInfo> {
    context::enumerate_gpus()
}

/// Return metadata about the currently selected GPU adapter.
///
/// Returns `Some((info, is_uma))` if the GPU context is initialized, `None` otherwise.
pub fn selected_gpu_info() -> Option<(context::GpuAdapterInfo, bool)> {
    context::selected_gpu_info()
}
