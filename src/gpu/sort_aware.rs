//! Sort-order-aware traits and utilities for GPU execution nodes.
//!
//! # Overview
//!
//! LTSeq's core value proposition is "ordered data 5x acceleration".
//! GPU execution nodes need to know whether their input data is sorted
//! so they can choose optimal algorithms:
//!
//! - Sorted data → merge join, segmented aggregate, binary search
//! - Unsorted data → hash join, hash aggregate, linear scan
//!
//! # Design
//!
//! Rather than maintaining a parallel metadata channel, we leverage
//! DataFusion's existing `EquivalenceProperties::output_ordering()` on
//! physical plan nodes.  The `SortOrderEffect` enum and helper functions
//! let GPU execution nodes declare and query sort-order information.
//!
//! # Sort Order Flow
//!
//! ```text
//! LTSeqTable.sort_exprs
//!     │
//!     ▼  (via assume_sorted → MemTable::with_sort_order / ParquetReadOptions::file_sort_order)
//! DataFusion EquivalenceProperties
//!     │
//!     ▼  (propagated through physical plan tree)
//! HostToGpuRule reads output_ordering() → chooses ordered/unordered GPU op
//!     │
//!     ▼
//! GpuExecNode / GPU exec nodes apply SortOrderEffect
//! ```

use std::sync::Arc;

use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

/// Describes how a GPU execution node affects the sort order of its output.
///
/// Each GPU exec node declares its effect so the optimizer can propagate
/// sort metadata correctly through the plan tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortOrderEffect {
    /// The node preserves the input's sort order.
    /// Examples: GpuFilterExec (rows removed but order unchanged),
    ///           GpuProjectExec (columns changed but row order unchanged)
    Preserve,

    /// The node destroys the input's sort order.
    /// Examples: GpuHashAggregateExec, GpuHashJoinExec
    Destroy,

    /// The node produces a new sort order (specified by column names).
    /// Examples: GpuSortExec (produces the requested sort),
    ///           GpuMergeJoinExec (output sorted by join key)
    Produce(Vec<String>),
}

/// Trait for GPU execution plan nodes that are sort-order-aware.
///
/// Implementors declare how they affect sort order, enabling the optimizer
/// to make correct decisions about downstream operators.
///
/// This is separate from DataFusion's `ExecutionPlan::maintains_input_order()`
/// because it provides richer semantics (Preserve/Destroy/Produce) needed
/// for the GPU optimizer's algorithm selection logic.
pub trait SortOrderPropagation {
    /// Declare this node's effect on sort order.
    fn sort_order_effect(&self) -> SortOrderEffect;
}

/// Extract sort column names from a physical plan's output ordering.
///
/// This is the bridge between DataFusion's `EquivalenceProperties::output_ordering()`
/// and our GPU optimizer's sort-aware logic.  It extracts simple column references
/// from the physical sort expressions.
///
/// Returns an empty vec if the plan has no output ordering or if the sort
/// expressions reference complex expressions (not simple columns).
pub fn extract_sort_columns(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    let eq_props = plan.equivalence_properties();
    let Some(ordering) = eq_props.output_ordering() else {
        return Vec::new();
    };

    ordering
        .iter()
        .filter_map(|sort_expr: &PhysicalSortExpr| {
            // Try to extract the column name from a simple Column reference.
            // Complex expressions (e.g., col_a + col_b) are skipped.
            use datafusion::physical_expr::expressions::Column;
            sort_expr
                .expr
                .as_any()
                .downcast_ref::<Column>()
                .map(|col: &Column| col.name().to_string())
        })
        .collect()
}

/// Check if a physical plan has a specific column in its output ordering.
///
/// Useful for the optimizer to determine if data is sorted by a join key
/// or group-by key, which enables merge join or segmented aggregation.
pub fn is_sorted_by(plan: &Arc<dyn ExecutionPlan>, column_name: &str) -> bool {
    let sort_cols = extract_sort_columns(plan);
    sort_cols.iter().any(|c| c == column_name)
}

/// Check if a physical plan is sorted by all of the given columns (in order).
///
/// The plan's sort order must start with exactly these columns (prefix match).
/// Additional trailing sort columns are allowed.
pub fn is_sorted_by_prefix(plan: &Arc<dyn ExecutionPlan>, column_names: &[&str]) -> bool {
    let sort_cols = extract_sort_columns(plan);
    if sort_cols.len() < column_names.len() {
        return false;
    }
    column_names
        .iter()
        .zip(sort_cols.iter())
        .all(|(expected, actual)| *expected == actual)
}

/// Build DataFusion `PhysicalSortExpr` from column names and a schema.
///
/// Used by GPU exec nodes that produce a new sort order (SortOrderEffect::Produce)
/// to construct the `EquivalenceProperties` for their output.
pub fn sort_exprs_from_column_names(
    column_names: &[String],
    schema: &arrow::datatypes::SchemaRef,
) -> Vec<PhysicalSortExpr> {
    use datafusion::physical_expr::expressions::Column;

    column_names
        .iter()
        .filter_map(|name| {
            schema.index_of(name).ok().map(|idx| PhysicalSortExpr {
                expr: Arc::new(Column::new(name, idx)),
                options: arrow::compute::SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sort_order_effect_eq() {
        assert_eq!(SortOrderEffect::Preserve, SortOrderEffect::Preserve);
        assert_eq!(SortOrderEffect::Destroy, SortOrderEffect::Destroy);
        assert_eq!(
            SortOrderEffect::Produce(vec!["a".into()]),
            SortOrderEffect::Produce(vec!["a".into()])
        );
        assert_ne!(SortOrderEffect::Preserve, SortOrderEffect::Destroy);
    }
}
