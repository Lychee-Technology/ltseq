//! DataFusion engine configuration and session management
//!
//! This module provides:
//! - Global Tokio runtime for async operations
//! - Configured SessionContext factory for optimal performance
//! - Test-only overrides of the session layout (`LayoutOverrides`)

use datafusion::execution::config::SessionConfig;
use datafusion::prelude::SessionContext;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use tokio::runtime::Runtime;

/// Default batch size for DataFusion operations.
/// 16384 rows reduces per-batch overhead for large scans while
/// maintaining reasonable memory usage per batch.
pub(crate) const DEFAULT_BATCH_SIZE: usize = 16384;

// Global Tokio runtime for async operations, one worker per available CPU
pub static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(*NUM_CPUS)
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

// The process-wide overrides; zero means "not overridden".
static TARGET_PARTITIONS_OVERRIDE: AtomicUsize = AtomicUsize::new(0);
static BATCH_SIZE_OVERRIDE: AtomicUsize = AtomicUsize::new(0);

/// Test-only overrides of a session's parallelism and batch size (#274).
///
/// The default parallelism is the CPU count, so a result that depends on
/// the partition count passes on a small machine and fails on a large one.
/// Tests and measurements (#251) pin both values to run the same check at
/// every layout. The overrides are process-wide and read when a session is
/// created: a table keeps the layout in effect at its reader or constructor
/// call through every later transform and execution. Python sets them
/// through `ltseq._testing`; they are not public API.
///
/// Each session carries the overrides it was created with as a config
/// extension, so evaluators that bypass DataFusion (`parallel_scan`) follow
/// the same layout as the session they run for.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct LayoutOverrides {
    pub(crate) target_partitions: Option<NonZeroUsize>,
    pub(crate) batch_size: Option<NonZeroUsize>,
}

impl LayoutOverrides {
    /// The overrides a session created now would use.
    fn current() -> Self {
        LayoutOverrides {
            target_partitions: NonZeroUsize::new(
                TARGET_PARTITIONS_OVERRIDE.load(Ordering::Relaxed),
            ),
            batch_size: NonZeroUsize::new(BATCH_SIZE_OVERRIDE.load(Ordering::Relaxed)),
        }
    }

    /// Make these the process-wide overrides and return the previous ones.
    fn install(self) -> Self {
        let swap = |slot: &AtomicUsize, value: Option<NonZeroUsize>| {
            NonZeroUsize::new(slot.swap(value.map_or(0, NonZeroUsize::get), Ordering::Relaxed))
        };
        LayoutOverrides {
            target_partitions: swap(&TARGET_PARTITIONS_OVERRIDE, self.target_partitions),
            batch_size: swap(&BATCH_SIZE_OVERRIDE, self.batch_size),
        }
    }

    /// The overrides `session` was created with; none for a session built
    /// outside `create_session_context`.
    pub(crate) fn of(session: &SessionContext) -> Self {
        session
            .copied_config()
            .get_extension::<LayoutOverrides>()
            .map_or_else(LayoutOverrides::default, |overrides| *overrides)
    }

    fn target_partitions(self) -> usize {
        self.target_partitions.map_or(*NUM_CPUS, NonZeroUsize::get)
    }

    fn batch_size(self) -> usize {
        self.batch_size
            .map_or(DEFAULT_BATCH_SIZE, NonZeroUsize::get)
    }
}

/// Create a new SessionContext with optimized configuration.
///
/// Configuration choices:
/// - `target_partitions`: Set to number of CPUs for parallel execution
/// - `batch_size`: 16384 rows (reduces per-batch overhead for large scans)
/// - `information_schema`: Disabled (not needed, saves memory)
/// - `repartition_joins`: Enabled for parallel join execution
/// - `repartition_aggregations`: Enabled for parallel aggregations
///
/// `LayoutOverrides` replaces the first two when a test has set them.
pub fn create_session_context() -> Arc<SessionContext> {
    session_context_with(LayoutOverrides::current())
}

/// A session with the given overrides; Rust tests use it to pin a layout
/// without touching the process-wide values other test threads read.
pub(crate) fn session_context_with(overrides: LayoutOverrides) -> Arc<SessionContext> {
    let mut config = SessionConfig::new()
        .with_target_partitions(overrides.target_partitions())
        .with_batch_size(overrides.batch_size())
        .with_information_schema(false)
        .with_repartition_joins(true)
        .with_repartition_aggregations(true)
        .with_coalesce_batches(true)
        .with_extension(Arc::new(overrides));

    // Enable Parquet filter pushdown for predicate skipping
    config.options_mut().execution.parquet.pushdown_filters = true;
    config.options_mut().execution.parquet.reorder_filters = true;

    Arc::new(SessionContext::new_with_config(config))
}

/// Set the layout overrides (`None` clears one) and return the previous
/// pair. Tests restore the previous pair when they finish.
#[pyfunction]
#[pyo3(signature = (target_partitions=None, batch_size=None))]
pub(crate) fn _set_layout_overrides(
    target_partitions: Option<usize>,
    batch_size: Option<usize>,
) -> PyResult<(Option<usize>, Option<usize>)> {
    let positive = |name: &str, value: Option<usize>| match value {
        Some(0) => Err(PyValueError::new_err(format!("{name} must be at least 1"))),
        value => Ok(value.and_then(NonZeroUsize::new)),
    };
    let previous = LayoutOverrides {
        target_partitions: positive("target_partitions", target_partitions)?,
        batch_size: positive("batch_size", batch_size)?,
    }
    .install();
    Ok((
        previous.target_partitions.map(NonZeroUsize::get),
        previous.batch_size.map(NonZeroUsize::get),
    ))
}

/// The layout a session created now would use (`target_partitions`,
/// `batch_size`) and the defaults the overrides replace
/// (`default_target_partitions`, `default_batch_size`).
#[pyfunction]
pub(crate) fn _engine_config() -> HashMap<&'static str, usize> {
    let current = LayoutOverrides::current();
    let defaults = LayoutOverrides::default();
    HashMap::from([
        ("target_partitions", current.target_partitions()),
        ("batch_size", current.batch_size()),
        ("default_target_partitions", defaults.target_partitions()),
        ("default_batch_size", defaults.batch_size()),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    // These tests build sessions from explicit overrides instead of
    // installing them: the process-wide values are shared by every test
    // thread, and installing them here would change other tests' sessions.

    #[test]
    fn overrides_set_the_session_layout_and_travel_with_it() {
        let overrides = LayoutOverrides {
            target_partitions: NonZeroUsize::new(3),
            batch_size: NonZeroUsize::new(7),
        };
        let session = session_context_with(overrides);
        let options = session.copied_config();
        assert_eq!(options.target_partitions(), 3);
        assert_eq!(options.batch_size(), 7);
        assert_eq!(LayoutOverrides::of(&session), overrides);
    }

    #[test]
    fn without_overrides_the_session_keeps_the_defaults() {
        let session = session_context_with(LayoutOverrides::default());
        let options = session.copied_config();
        assert_eq!(options.target_partitions(), *NUM_CPUS);
        assert_eq!(options.batch_size(), DEFAULT_BATCH_SIZE);
        assert_eq!(LayoutOverrides::of(&session), LayoutOverrides::default());
    }

    #[test]
    fn a_session_built_elsewhere_has_no_overrides() {
        assert_eq!(
            LayoutOverrides::of(&SessionContext::new()),
            LayoutOverrides::default()
        );
    }
}
