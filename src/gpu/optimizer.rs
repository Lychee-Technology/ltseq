//! Physical optimizer rule that replaces CPU execution nodes with GPU equivalents.
//!
//! `HostToGpuRule` walks the physical plan tree via `transform_up` and replaces
//! eligible nodes with GPU-accelerated versions:
//!
//! - `FilterExec` → `GpuFilterExec` when the predicate is a simple
//!   `Column CMP Literal` on a supported numeric type (i32, i64, f32, f64).
//!
//! The rule is **sort-order-aware**: it reads `EquivalenceProperties::output_ordering()`
//! from input nodes to determine if data is sorted. This enables Phase 1.2+
//! operators to choose between ordered and unordered algorithms:
//!
//! - Sorted group-by key → `GpuConsecutiveGroupExec` + `GpuSegmentedAggregateExec`
//! - Sorted join keys → `GpuMergeJoinExec`
//! - Sorted search column → `GpuBinarySearchExec`
//!
//! The rule also checks row count statistics: if the estimated row count is
//! below `GPU_MIN_ROWS_THRESHOLD`, the operation stays on CPU to avoid
//! H2D/D2H transfer overhead.

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::ExecutionPlan;

use super::filter_exec::{GpuFilterExec, GpuFilterPredicate};
use super::sort_aware::extract_sort_columns;
use super::GPU_MIN_ROWS_THRESHOLD;

/// Physical optimizer rule that offloads eligible operations to GPU.
///
/// The rule is appended to DataFusion's default optimizer pipeline, so it runs
/// after all standard optimizations (predicate pushdown, coalesce batches, etc.)
/// have already been applied.
///
/// Sort-awareness: the rule reads `output_ordering()` from plan nodes to
/// choose between ordered and unordered GPU operator variants.
#[derive(Debug)]
pub struct HostToGpuRule;

impl PhysicalOptimizerRule for HostToGpuRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| try_replace_node(node))
            .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "HostToGpuRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Attempt to replace a single plan node with a GPU-accelerated equivalent.
///
/// Currently handles:
/// - `FilterExec` → `GpuFilterExec` (simple Column CMP Literal)
///
/// Phase 1.2+ will add:
/// - `AggregateExec` → `GpuSegmentedAggregateExec` (when sorted by group key)
/// - `SortMergeJoinExec` → `GpuMergeJoinExec` (when both inputs sorted)
fn try_replace_node(plan: Arc<dyn ExecutionPlan>) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    // Try filter replacement first
    if let Some(result) = try_replace_filter(&plan)? {
        return Ok(result);
    }

    // Phase 1.2+: try_replace_aggregate, try_replace_join, etc.
    // These will use `extract_sort_columns()` to check if inputs are sorted
    // and choose the optimal GPU operator variant.

    Ok(Transformed::no(plan))
}

/// Attempt to replace a `FilterExec` with `GpuFilterExec`.
fn try_replace_filter(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Option<Transformed<Arc<dyn ExecutionPlan>>>> {
    let Some(filter_exec) = plan.as_any().downcast_ref::<FilterExec>() else {
        return Ok(None);
    };

    let input = filter_exec.input();
    let schema = input.schema();
    let predicate = filter_exec.predicate();

    let Some(gpu_pred) = GpuFilterPredicate::try_from_physical_expr(predicate, &schema) else {
        return Ok(None);
    };

    // Check row count statistics — skip GPU for small datasets
    if let Ok(stats) = input.partition_statistics(None) {
        if let Some(num_rows) = stats.num_rows.get_value() {
            if *num_rows < GPU_MIN_ROWS_THRESHOLD {
                return Ok(None);
            }
        }
    }

    // Log sort metadata for debugging
    if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
        let sort_cols = extract_sort_columns(input);
        if sort_cols.is_empty() {
            eprintln!("[GPU] HostToGpuRule: FilterExec input is unsorted");
        } else {
            eprintln!(
                "[GPU] HostToGpuRule: FilterExec input sorted by [{}]",
                sort_cols.join(", ")
            );
        }
    }

    let gpu_filter = GpuFilterExec::try_new(gpu_pred, Arc::clone(predicate), Arc::clone(input))?;
    Ok(Some(Transformed::yes(Arc::new(gpu_filter))))
}
