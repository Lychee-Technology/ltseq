//! DataFusion engine configuration and session management
//!
//! This module provides:
//! - Global Tokio runtime for async operations
//! - Configured SessionContext factory for optimal performance

use datafusion::execution::config::SessionConfig;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use std::sync::{Arc, LazyLock};
use tokio::runtime::Runtime;

/// Default batch size for DataFusion operations.
/// 16384 rows reduces per-batch overhead for large scans while
/// maintaining reasonable memory usage per batch.
pub(crate) const DEFAULT_BATCH_SIZE: usize = 16384;

// Global Tokio runtime for async operations
// Configured with optimal thread count based on available CPUs
pub static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    let num_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus)
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
});

/// Number of CPU cores available (cached for performance)
pub static NUM_CPUS: LazyLock<usize> = LazyLock::new(|| {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
});

/// Create a new SessionContext with optimized configuration.
///
/// Configuration choices:
/// - `batch_size`: 16384 rows (reduces per-batch overhead for large scans)
/// - `information_schema`: Disabled (not needed, saves memory)
///
/// **Partitioning strategy:**
/// - When GPU is available: `target_partitions=1` with repartition disabled.
///   All GPU operators run on a single device, so multi-partition planning only
///   adds `Partial → RepartitionExec → FinalPartitioned` overhead that the GPU
///   must immediately undo via `CoalescePartitionsExec`. Single-partition mode
///   produces `AggregateMode::Single` plans that the GPU optimizer can fully
///   intercept, and preserves sort order for merge-join and ordered operators.
/// - When GPU is not available: `target_partitions=NUM_CPUS` with repartition
///   enabled for CPU-parallel execution.
///
/// When the `gpu` feature is enabled and a CUDA GPU is available, the
/// `HostToGpuRule` physical optimizer rule is appended to the pipeline.
pub fn create_session_context() -> Arc<SessionContext> {
    // Detect GPU availability to choose partitioning strategy.
    // GPU operations are inherently single-device, so multi-partition planning
    // adds overhead without benefit and blocks several GPU optimizations.
    let use_gpu_partitioning = is_gpu_session_enabled();

    let (partitions, repartition) = if use_gpu_partitioning {
        (1, false)
    } else {
        (*NUM_CPUS, true)
    };

    let mut config = SessionConfig::new()
        .with_target_partitions(partitions)
        .with_batch_size(DEFAULT_BATCH_SIZE)
        .with_information_schema(false)
        .with_repartition_joins(repartition)
        .with_repartition_aggregations(repartition)
        .with_coalesce_batches(true);

    // Enable Parquet filter pushdown for predicate skipping
    config.options_mut().execution.parquet.pushdown_filters = true;
    config.options_mut().execution.parquet.reorder_filters = true;

    let state = build_session_state(config);
    Arc::new(SessionContext::new_with_state(state))
}

/// Create a SessionContext optimized for sequential/streaming operations.
///
/// Uses `target_partitions=1` to avoid the "parallelize then serialize" pattern
/// that hurts sequential operations on pre-sorted data:
///   - No SortPreservingMergeExec (single partition preserves order natively)
///   - No CoalescePartitionsExec overhead
///   - `execute_stream()` returns batches directly from the single partition
///
/// All other settings match `create_session_context()` (pushdown filters, etc.)
pub fn create_sequential_session() -> Arc<SessionContext> {
    let mut config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(DEFAULT_BATCH_SIZE)
        .with_information_schema(false)
        .with_repartition_joins(false)
        .with_repartition_aggregations(false)
        .with_coalesce_batches(true);

    // Enable Parquet filter pushdown for predicate skipping
    config.options_mut().execution.parquet.pushdown_filters = true;
    config.options_mut().execution.parquet.reorder_filters = true;

    let state = build_session_state(config);
    Arc::new(SessionContext::new_with_state(state))
}

/// Check whether this session should use GPU-optimized partitioning.
///
/// Returns `true` when all of the following are met:
/// - The `gpu` feature is compiled in
/// - A CUDA device is available
/// - `LTSEQ_DISABLE_GPU=1` is NOT set
///
/// When `true`, `create_session_context()` uses `target_partitions=1`
/// so DataFusion produces single-partition plans that the GPU optimizer
/// can fully intercept.
fn is_gpu_session_enabled() -> bool {
    #[cfg(feature = "gpu")]
    {
        let gpu_disabled = std::env::var("LTSEQ_DISABLE_GPU").is_ok();
        if !gpu_disabled && crate::gpu::is_gpu_available() {
            return true;
        }
    }
    false
}

/// Build a `SessionState` from a `SessionConfig`, optionally appending
/// the GPU physical optimizer rule when the `gpu` feature is enabled
/// and a CUDA device is available.
fn build_session_state(config: SessionConfig) -> datafusion::execution::SessionState {
    #[allow(unused_mut)]
    let mut builder = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features();

    #[cfg(feature = "gpu")]
    {
        // Allow disabling GPU at runtime via LTSEQ_DISABLE_GPU=1 for benchmarking
        let gpu_disabled = std::env::var("LTSEQ_DISABLE_GPU").is_ok();
        if !gpu_disabled && crate::gpu::is_gpu_available() {
            // GpuQueryPlanner: intercepts logical planning to detect GPU-compatible
            // plans and log them.  In Phase 2/3 it falls back to DefaultPhysicalPlanner,
            // preserving HostToGpuRule's filter optimisation.
            builder = builder
                .with_query_planner(Arc::new(crate::gpu::planner::GpuQueryPlanner))
                // Append HostToGpuRule at the end of the optimizer pipeline.
                // Running last means all standard rewrites (predicate pushdown,
                // coalesce batches, etc.) have already been applied.
                .with_physical_optimizer_rule(Arc::new(crate::gpu::optimizer::HostToGpuRule));
        }
    }

    builder.build()
}
