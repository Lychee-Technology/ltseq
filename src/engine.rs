//! DataFusion engine configuration and session management
//!
//! This module provides:
//! - Global Tokio runtime for async operations
//! - Configured SessionContext factory for optimal performance
//! - ltseq's optimizer rules (`FloatListZeros`, #245; `KeepFileOrder`, #325;
//!   `KeepSortsAboveOffsets`, #339; `DropFloatStatistics`, #291)
//! - Test-only overrides of the session layout (`LayoutOverrides`)

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::SessionContext;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use tokio::runtime::Runtime;

use crate::float_statistics::DropFloatStatistics;
use crate::ops::lookup::OneRowPerKeyPlanner;
use crate::source::LocalFiles;
use crate::transpiler::FloatListZeros;

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

    /// The `(target_partitions, batch_size)` a session with these overrides
    /// is configured with, as `_engine_config` reports it, if any part of
    /// the layout is pinned; a part left unpinned is the engine default.
    /// `None` if nothing is pinned. An evaluator that bypasses DataFusion
    /// runs at this layout, so a pinned test gets the same layout on both
    /// paths, and keeps its own tuning when there is none.
    pub(crate) fn pinned(self) -> Option<(usize, usize)> {
        (self != LayoutOverrides::default()).then(|| (self.target_partitions(), self.batch_size()))
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
/// - `enable_aggregate_dynamic_filter_pushdown`: Disabled, so a scan keeps
///   the values a float `min` or `max` needs (#253)
/// - `FloatListZeros`: Last optimizer rule, so a float IN list compares
///   -0.0 equal to 0.0 however DataFusion built it (#245)
/// - `KeepFileOrder`: Last physical optimizer rule, so a scan with a
///   declared order reads its files in order however DataFusion planned
///   it (#325)
/// - `DropFloatStatistics`: First physical optimizer rule, so no Parquet
///   scan skips or replaces float values by statistics that leave NaN out
///   or by bloom filters that tell the zeros apart (#291)
/// - `KeepSortsAboveOffsets`: Next, so a sort required above a slice is not
///   planned beneath its offset (#339)
/// - `LocalFiles`: The store of local files, so an execution that looks up
///   a file a reader listed fails if the file has disappeared (#262), and
///   reads its Parquet footer afresh (#359)
/// - `QueryPlanner`: DataFusion's planner plus those of LTSeq's own plan
///   nodes, such as the check under a lookup's join (#351)
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
    // DataFusion 55 tells an ungrouped `min` or `max` by its name and pushes
    // `x < min so far` or `x > max so far` into the scan, which compares
    // floats in DataFusion's order. That drops values the contract's order
    // needs (`transpiler::min_max`): a NaN with the sign bit set is below
    // `2.0` there, so `max` lost it, and `min` lost `0.0` once it had seen
    // `-0.0`. The filter is chosen by name alone, so it is off for every type.
    config
        .options_mut()
        .optimizer
        .enable_aggregate_dynamic_filter_pushdown = false;

    let session = new_session(config);
    // Appended to DataFusion's rules, so it runs after the ones that build
    // IN lists in every pass.
    session.add_optimizer_rule(Arc::new(FloatListZeros));
    // `DropFloatStatistics` goes before DataFusion's physical rules, so the
    // ones that read a scan's statistics see what the scan reads, and
    // `KeepSortsAboveOffsets` too, so `EnsureRequirements` sees its limits;
    // `KeepFileOrder` after them, so it runs after `LimitPushdown`.
    let state = session.state();
    let rules = [
        Arc::new(DropFloatStatistics) as _,
        Arc::new(KeepSortsAboveOffsets) as _,
    ]
    .into_iter()
    .chain(state.physical_optimizers().iter().cloned())
    .chain(std::iter::once(Arc::new(KeepFileOrder) as _))
    .collect();
    let state = SessionStateBuilder::new_from_existing(state)
        .with_physical_optimizer_rules(rules)
        .with_query_planner(Arc::new(QueryPlanner))
        .build();
    let session = SessionContext::new_with_state(state);
    let footers = session
        .runtime_env()
        .cache_manager
        .get_file_metadata_cache();
    session.register_object_store(
        ObjectStoreUrl::local_filesystem().as_ref(),
        Arc::new(LocalFiles::new(footers)),
    );
    Arc::new(session)
}

/// DataFusion's planner, plus the planners of the plan nodes LTSeq adds.
#[derive(Debug)]
struct QueryPlanner;

#[async_trait]
impl datafusion::execution::context::QueryPlanner for QueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> = vec![
            Arc::new(OneRowPerKeyPlanner),
            // A measurement build's filter barrier (`crate::m0`).
            #[cfg(feature = "m0")]
            Arc::new(crate::m0::pushdown::BarrierPlanner),
        ];
        DefaultPhysicalPlanner::with_extension_planners(planners)
            .create_physical_plan(logical_plan, session)
            .await
    }
}

/// Puts a limit with the same fetch and no offset over every limit that
/// skips rows and has a fetch, so that an order required above the offset
/// is sorted above it (#339).
///
/// DataFusion 55's `EnsureRequirements` pushes an order requirement down
/// through a `GlobalLimitExec` with a fetch, and gives the operator below it
/// the limit's fetch without its offset. `sort("a").tail(5).sort("a", "b")`
/// then kept the first 5 rows by `(a, b)` beneath the limit, which skipped
/// all of them; above a slice of a table with no declared order, any sort
/// did the same. A window's sort and the kernels' own sorts after a slice
/// lost rows the same way. `EnsureRequirements` does not push a
/// requirement that carries a fetch through an operator that drops rows, so
/// under the outer limit the requirement stops at the offset limit and is
/// sorted above it. The outer limit passes on every row the inner one
/// returns, and `LimitPushdown` folds it into what is above.
///
/// Remove it with the DataFusion upgrade that stops pushing a requirement
/// through such a limit, once #339's tests pass without it.
#[derive(Debug)]
struct KeepSortsAboveOffsets;

impl PhysicalOptimizerRule for KeepSortsAboveOffsets {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let fetch = node
                .downcast_ref::<GlobalLimitExec>()
                .filter(|limit| limit.skip() > 0)
                .and_then(GlobalLimitExec::fetch);
            Ok(match fetch {
                Some(fetch) => {
                    Transformed::yes(Arc::new(GlobalLimitExec::new(node, 0, Some(fetch))) as _)
                }
                None => Transformed::no(node),
            })
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "ltseq_keep_sorts_above_offsets"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Makes every file scan that declares an output ordering read its files,
/// and the row groups in them, in order (#325).
///
/// DataFusion builds such a scan with `preserve_order` set, but in 55
/// `LimitPushdown` clears the flag when it pushes the fetch of a
/// `SortPreservingMergeExec` into the scan: any limit over a declared-sorted
/// Parquet source that two or more partitions read, which is a directory of
/// several files or a file of 1 MiB or more split into byte ranges. Without
/// the flag the Parquet opener prunes by the limit, skipping a row group the
/// filter matches in part for a later one it matches whole, so `head`,
/// `slice` and `search_first` returned matching rows that were not the
/// first; and the partitions take files from one shared queue instead of
/// each reading its own in order. Setting the flag again turns off only
/// these two shortcuts, and only for scans with a declared order.
///
/// apache/datafusion#26434 makes a scan with an output ordering always read
/// in order; remove this rule with the upgrade that includes it (56).
#[derive(Debug)]
struct KeepFileOrder;

impl PhysicalOptimizerRule for KeepFileOrder {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let lost_order = node
                .downcast_ref::<DataSourceExec>()
                .and_then(|scan| scan.data_source().downcast_ref::<FileScanConfig>())
                .is_some_and(|scan| !scan.output_ordering.is_empty() && !scan.preserve_order);
            if !lost_order {
                return Ok(Transformed::no(node));
            }
            Ok(match node.with_preserve_order(true) {
                Some(scan) => Transformed::yes(scan),
                None => Transformed::no(node),
            })
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "ltseq_keep_file_order"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(not(feature = "m0"))]
fn new_session(config: SessionConfig) -> SessionContext {
    SessionContext::new_with_config(config)
}

/// A measurement build adds the #251 prototypes in effect (`crate::m0`).
#[cfg(feature = "m0")]
fn new_session(config: SessionConfig) -> SessionContext {
    crate::m0::session(config)
}

/// Set the layout overrides (`None` clears one) and return the previous
/// pair. Tests restore the previous pair when they finish.
#[pyfunction]
#[pyo3(signature = (target_partitions=None, batch_size=None))]
pub(crate) fn _set_layout_overrides(
    target_partitions: Option<isize>,
    batch_size: Option<isize>,
) -> PyResult<(Option<usize>, Option<usize>)> {
    // Signed, so that a negative value gets the same `ValueError` as zero
    // instead of the `OverflowError` of PyO3's conversion to `usize`.
    let positive = |name: &str, value: Option<isize>| match value {
        None => Ok(None),
        Some(n) => usize::try_from(n)
            .ok()
            .and_then(NonZeroUsize::new)
            .map(Some)
            .ok_or_else(|| PyValueError::new_err(format!("{name} must be at least 1, got {n}"))),
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
        assert_eq!(overrides.pinned(), Some((3, 7)));
    }

    #[test]
    fn a_part_left_unpinned_is_the_engine_default() {
        let overrides = LayoutOverrides {
            target_partitions: NonZeroUsize::new(2),
            batch_size: None,
        };
        assert_eq!(overrides.pinned(), Some((2, DEFAULT_BATCH_SIZE)));
        assert_eq!(LayoutOverrides::default().pinned(), None);
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
