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
/// - `target_partitions`: Set to number of CPUs for parallel execution
/// - `batch_size`: 16384 rows (reduces per-batch overhead for large scans)
/// - `information_schema`: Disabled (not needed, saves memory)
/// - `repartition_joins`: Enabled for parallel join execution
/// - `repartition_aggregations`: Enabled for parallel aggregations
///
/// When the `gpu` feature is enabled and a CUDA GPU is available, the
/// `HostToGpuRule` physical optimizer rule is appended to the pipeline.
pub fn create_session_context() -> Arc<SessionContext> {
    let mut config = SessionConfig::new()
        .with_target_partitions(*NUM_CPUS)
        .with_batch_size(DEFAULT_BATCH_SIZE)
        .with_information_schema(false)
        .with_repartition_joins(true)
        .with_repartition_aggregations(true)
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
            // Append HostToGpuRule at the end of the optimizer pipeline.
            // Running last means all standard rewrites (predicate pushdown,
            // coalesce batches, etc.) have already been applied.
            builder = builder
                .with_physical_optimizer_rule(Arc::new(crate::gpu::optimizer::HostToGpuRule));
        }
    }

    builder.build()
}
