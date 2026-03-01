//! Physical optimizer rule that replaces CPU `FilterExec` with `GpuFilterExec`.
//!
//! `HostToGpuRule` walks the physical plan tree via `transform_up` and replaces
//! `FilterExec` nodes with `GpuFilterExec` when the predicate is a simple
//! `Column CMP Literal` on a supported numeric type (i32, i64, f32, f64).
//!
//! The rule also checks row count statistics: if the estimated row count is
//! below `GPU_MIN_ROWS_THRESHOLD`, the filter stays on CPU to avoid
//! H2D/D2H transfer overhead.

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::ExecutionPlan;

use super::filter_exec::{GpuFilterExec, GpuFilterPredicate};
use super::GPU_MIN_ROWS_THRESHOLD;

/// Physical optimizer rule that offloads eligible filter operations to GPU.
///
/// The rule is appended to DataFusion's default optimizer pipeline, so it runs
/// after all standard optimizations (predicate pushdown, coalesce batches, etc.)
/// have already been applied.
#[derive(Debug)]
pub struct HostToGpuRule;

impl PhysicalOptimizerRule for HostToGpuRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| try_replace_filter(node))
            .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "HostToGpuRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Attempt to replace a single plan node if it is a GPU-eligible FilterExec.
fn try_replace_filter(plan: Arc<dyn ExecutionPlan>) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    // Check if this node is a FilterExec
    let Some(filter_exec) = plan.as_any().downcast_ref::<FilterExec>() else {
        return Ok(Transformed::no(plan));
    };

    // Check if we can handle this predicate on GPU
    let input = filter_exec.input();
    let schema = input.schema();
    let predicate = filter_exec.predicate();

    let Some(gpu_pred) = GpuFilterPredicate::try_from_physical_expr(predicate, &schema) else {
        // Predicate is not a simple Column CMP Literal, or column type unsupported
        return Ok(Transformed::no(plan));
    };

    // Check row count statistics — skip GPU for small datasets
    if let Ok(stats) = input.partition_statistics(None) {
        if let Some(num_rows) = stats.num_rows.get_value() {
            if *num_rows < GPU_MIN_ROWS_THRESHOLD {
                return Ok(Transformed::no(plan));
            }
        }
        // If statistics are inexact or absent, we optimistically offload to GPU.
        // The GPU kernel handles any batch size efficiently, and the threshold
        // is mainly to avoid H2D/D2H overhead on trivially small tables.
    }

    // Build GpuFilterExec replacement
    let gpu_filter = GpuFilterExec::try_new(gpu_pred, Arc::clone(predicate), Arc::clone(input))?;

    Ok(Transformed::yes(Arc::new(gpu_filter)))
}
