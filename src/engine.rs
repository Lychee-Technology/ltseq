//! DataFusion engine configuration and session management
//!
//! This module provides:
//! - Global Tokio runtime for async operations
//! - Configured SessionContext factory for optimal performance

use datafusion::execution::config::SessionConfig;
use datafusion::prelude::SessionContext;
use std::sync::{Arc, LazyLock};
use tokio::runtime::Runtime;

/// Default batch size for DataFusion operations.
/// 8192 is DataFusion's default and is cache-optimized.
pub(crate) const DEFAULT_BATCH_SIZE: usize = 8192;

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
/// - `batch_size`: 8192 rows (DataFusion default, cache-optimized)
/// - `information_schema`: Disabled (not needed, saves memory)
/// - `repartition_joins`: Enabled for parallel join execution
/// - `repartition_aggregations`: Enabled for parallel aggregations
pub fn create_session_context() -> Arc<SessionContext> {
    let config = SessionConfig::new()
        .with_target_partitions(*NUM_CPUS)
        .with_batch_size(DEFAULT_BATCH_SIZE)
        .with_information_schema(false)
        .with_repartition_joins(true)
        .with_repartition_aggregations(true)
        .with_coalesce_batches(true);

    Arc::new(SessionContext::new_with_config(config))
}
