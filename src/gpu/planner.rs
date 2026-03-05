//! GPU query planner for DataFusion.
//!
//! `GpuQueryPlanner` intercepts DataFusion's logical planning step and checks
//! whether the entire plan is GPU-compatible via `is_fully_gpu_compatible()`.
//!
//! # Phase 2 behaviour (current, C++ engine absent)
//! Falls back to `DefaultPhysicalPlanner` for all plans.
//! `HostToGpuRule` (physical optimizer rule) still handles simple filter offloads.
//!
//! # Phase 3 behaviour (current, Substrait enabled)
//! For GPU-compatible plans:
//!   1. Builds the CPU physical plan via `DefaultPhysicalPlanner`.
//!   2. Applies `HostToGpuRule` to the CPU plan (before wrapping as a leaf node).
//!   3. Serialises the logical plan to Substrait protobuf bytes.
//!   4. Returns a `GpuExecNode` that stores both the CPU fallback and the bytes.
//!   Execution still runs on the CPU fallback; the bytes are ready for Phase 4.
//!
//! # Phase 4 upgrade path
//! When `cudf_engine_available()` is true, replace the `GpuExecNode::new(…)`
//! call with one that skips the CPU fallback and sends the Substrait bytes to
//! the C++ libcudf engine via FFI.
//!
//! Setting `LTSEQ_GPU_DEBUG=1` logs plan-compatibility decisions to stderr.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::DefaultTableSource;
use datafusion::datasource::listing::ListingTable;
use datafusion::error::Result;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, TableScan};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};

/// GPU-aware query planner.
///
/// Registered in `SessionState` via `SessionStateBuilder::with_query_planner`.
#[derive(Debug)]
pub struct GpuQueryPlanner;

#[async_trait]
impl QueryPlanner for GpuQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let compatible = is_fully_gpu_compatible(logical_plan);

        if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
            eprintln!("[GPU] plan GPU-compatible: {compatible}");
        }

        // Phase 3: for GPU-compatible plans, build the CPU fallback, apply
        // HostToGpuRule, serialise to Substrait, and wrap in GpuExecNode.
        // Execution still delegates to the CPU plan.
        //
        // Phase 4 upgrade: check `cudf_engine_available()` here; if true,
        // construct a GpuExecNode that sends substrait_bytes to the C++ engine.
        if compatible {
            let cpu_plan = DefaultPhysicalPlanner::default()
                .create_physical_plan(logical_plan, session_state)
                .await?;

            // Apply HostToGpuRule now: GpuExecNode.children() is empty so the
            // physical optimizer pipeline cannot reach inside cpu_fallback after
            // wrapping.
            let cpu_plan = crate::gpu::optimizer::HostToGpuRule
                .optimize(cpu_plan, session_state.config_options())?;

            // Serialise the logical plan to Substrait bytes; gracefully degrade
            // on unsupported operators (serialisation error → None → CPU only).
            let substrait_bytes =
                crate::gpu::substrait::to_substrait_bytes(logical_plan, session_state).ok();
            // note: to_substrait_bytes is synchronous (no .await needed)

            if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
                match &substrait_bytes {
                    Some(b) => {
                        eprintln!("[GPU] substrait plan serialised: {} bytes", b.len());
                        // Dump to /tmp/ltseq_plan.bin for offline inspection
                        let _ = std::fs::write("/tmp/ltseq_plan.bin", b);
                    }
                    None => eprintln!("[GPU] substrait serialisation failed — CPU fallback only"),
                }
            }

            // Extract (table_name, file_path) pairs so GpuExecNode can load
            // Parquet files into GPU HBM when the engine is available.
            let parquet_paths = extract_parquet_paths(logical_plan);

            return Ok(Arc::new(crate::gpu::exec_node::GpuExecNode::new(
                cpu_plan,
                substrait_bytes,
                parquet_paths,
            )));
        }

        DefaultPhysicalPlanner::default()
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

/// Extract `(substrait_table_name, file_path)` pairs from all `TableScan` nodes
/// in the logical plan.  Used to pre-load Parquet files into GPU HBM so that
/// `GpuExecNode::execute()` can pass real handles to `cudf_execute_substrait()`.
///
/// For anonymous tables created by `ctx.read_parquet()`, DataFusion assigns the
/// placeholder name `"?table?"`.  We preserve this so the C++ engine can match
/// the name against what appears in the Substrait ReadRel.
fn extract_parquet_paths(plan: &LogicalPlan) -> Vec<(String, String)> {
    let mut paths = Vec::new();
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(TableScan { table_name, source, .. }) = node {
            let table_name_str = table_name.table().to_string();
            // Downcast: DefaultTableSource → ListingTable → table_paths
            if let Some(default_src) = source.as_any().downcast_ref::<DefaultTableSource>() {
                if let Some(listing) = default_src
                    .table_provider
                    .as_any()
                    .downcast_ref::<ListingTable>()
                {
                    for url in listing.table_paths() {
                        let file_path = url.as_str()
                            .trim_start_matches("file://")
                            .to_string();
                        paths.push((table_name_str.clone(), file_path));
                    }
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    paths
}

/// Return `true` if every node in the logical plan can execute on the GPU.
///
/// Currently conservative: plans containing `Window`, `Subquery`, or `Unnest`
/// nodes are considered incompatible and fall back to CPU execution.
pub fn is_fully_gpu_compatible(plan: &LogicalPlan) -> bool {
    use datafusion::logical_expr::LogicalPlan::{Subquery, Unnest, Window};

    // `exists()` returns true if any node matches the predicate.
    // We return false (incompatible) when an unsupported node is found.
    !plan
        .exists(|node| Ok(matches!(node, Window(_) | Subquery(_) | Unnest(_))))
        .unwrap_or(true) // treat traversal errors as incompatible
}

#[cfg(test)]
mod tests {
    use super::is_fully_gpu_compatible;
    use datafusion::execution::context::SessionContext;

    /// A simple scan+filter plan should be GPU-compatible.
    #[tokio::test]
    async fn test_simple_plan_compatible() {
        let ctx = SessionContext::new();
        ctx.sql("CREATE TABLE t (x INT) AS VALUES (1), (2)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let plan = ctx
            .sql("SELECT x FROM t WHERE x > 1")
            .await
            .unwrap()
            .into_unoptimized_plan();
        assert!(is_fully_gpu_compatible(&plan));
    }

    /// A plan with a window function is NOT GPU-compatible.
    #[tokio::test]
    async fn test_window_plan_incompatible() {
        let ctx = SessionContext::new();
        ctx.sql("CREATE TABLE t (x INT) AS VALUES (1), (2)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let plan = ctx
            .sql("SELECT x, ROW_NUMBER() OVER () AS rn FROM t")
            .await
            .unwrap()
            .into_unoptimized_plan();
        assert!(!is_fully_gpu_compatible(&plan));
    }
}
