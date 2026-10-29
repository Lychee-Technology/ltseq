//! m4b: a filter kept above a sort, aggregate or distinct, or above its
//! input (D10a; M16).
//!
//! D10a makes a sort, grouping, distinct or aggregate demand its keys and
//! arguments on every input row (contract §20.2), so a filter written after
//! one may not move to where a fallible key or argument would go
//! unevaluated on the rows it removes. DataFusion pushes such filters down,
//! through a sort or distinct always and through an aggregate when they
//! read only group keys, then into the scan, where Parquet statistics can
//! skip row groups. This prototype prices losing that, with a no-op
//! [`Barrier`] that `PushDownFilter` does not cross, as it crosses no
//! extension node it knows nothing about. Two placements bound the cost
//! ([`Restrict`]): above each sort, aggregate and distinct a filter sits
//! over (through projections), the filter never crosses the operator;
//! between the operator and its input, it crosses the operator but stays
//! above the projection that computes the keys. The second is what an
//! implementation that knows a key is fallible pays, short of proving from
//! statistics that the key cannot fail in a row group. Above a sort, the
//! first placement filters the sort's merged stream in one partition
//! (`session` turns off round-robin repartitioning, which would return the
//! rows unsorted); filtering each sorted partition before the merge would
//! cost less, and is not measured. DataFusion 55 also
//! pushes filters in the physical plan (through an aggregate's group keys
//! into the scan), so the barrier is planned as [`BarrierExec`], which
//! passes batches through and, like any operator that does not say
//! otherwise, no filters.
//!
//! It does not ask whether a key is fallible: the barrier goes in under
//! every such filter, whatever the keys compute, so the timings price an
//! implementation that treats every checked operation as fallible. None of
//! the m4b keys can fail. Each adds an `int` literal (`int64`, §17.2) to
//! an `int32` column, which no value of the column overflows, so a rule
//! that reads only the operand types would keep the pushdown in all four
//! pipelines.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchemaRef, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{
    Expr, Extension, Filter, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    ReplaceChildrenOptions,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

use super::Restrict;

/// A node that passes its input through unchanged and stops filter pushdown.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub(crate) struct Barrier {
    input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for Barrier {
    fn name(&self) -> &str {
        "M0Barrier"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "M0Barrier")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Barrier {
            input: inputs.swap_remove(0),
        })
    }

    // Projection pruning still crosses the barrier: it needs from its input
    // exactly the columns asked of it.
    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![output_columns.to_vec()])
    }
}

/// `plan` under a barrier, unless it is one already: the rule runs in
/// every optimizer pass.
fn barrier(plan: LogicalPlan) -> Transformed<LogicalPlan> {
    if let LogicalPlan::Extension(Extension { node }) = &plan {
        if node.as_any().is::<Barrier>() {
            return Transformed::no(plan);
        }
    }
    Transformed::yes(LogicalPlan::Extension(Extension {
        node: Arc::new(Barrier { input: plan }),
    }))
}

/// Puts a barrier above the sort, aggregate or distinct under `plan`, or
/// above its input, looking through projections and aliases.
fn guard(plan: LogicalPlan, at: Restrict) -> Result<Transformed<LogicalPlan>> {
    match plan {
        LogicalPlan::Sort(_) | LogicalPlan::Aggregate(_) | LogicalPlan::Distinct(_) => match at {
            Restrict::Keys => plan.map_children(|input| Ok(barrier(input))),
            _ => Ok(barrier(plan)),
        },
        LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_) => {
            plan.map_children(|child| guard(child, at))
        }
        other => Ok(Transformed::no(other)),
    }
}

/// Runs first in each optimizer pass, before `PushDownFilter`.
#[derive(Debug)]
pub(crate) struct RestrictPushdown(pub(crate) Restrict);

impl OptimizerRule for RestrictPushdown {
    fn name(&self) -> &str {
        "ltseq_m0_restrict_pushdown"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_down(|node| {
            let LogicalPlan::Filter(filter) = node else {
                return Ok(Transformed::no(node));
            };
            let input = guard(Arc::unwrap_or_clone(filter.input), self.0)?;
            let transformed = input.transformed;
            let node =
                LogicalPlan::Filter(Filter::try_new(filter.predicate, Arc::new(input.data))?);
            Ok(if transformed {
                Transformed::yes(node)
            } else {
                Transformed::no(node)
            })
        })
    }
}

/// A [`Barrier`] in the physical plan: its input's batches, unchanged.
#[derive(Debug)]
struct BarrierExec {
    input: Arc<dyn ExecutionPlan>,
}

impl DisplayAs for BarrierExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "M0BarrierExec")
    }
}

impl ExecutionPlan for BarrierExec {
    fn name(&self) -> &str {
        "M0BarrierExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(BarrierExec {
            input: children.swap_remove(0),
        }))
    }

    #[allow(deprecated)]
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}

/// Plans a [`Barrier`] as a [`BarrierExec`]; the engine's query planner
/// holds it (`engine::QueryPlanner`).
pub(crate) struct BarrierPlanner;

#[async_trait]
impl ExtensionPlanner for BarrierPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session: &dyn datafusion::catalog::Session,
        _planning_ctx: &datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(node.as_any().downcast_ref::<Barrier>().map(|_| {
            Arc::new(BarrierExec {
                input: Arc::clone(&physical_inputs[0]),
            }) as Arc<dyn ExecutionPlan>
        }))
    }
}
