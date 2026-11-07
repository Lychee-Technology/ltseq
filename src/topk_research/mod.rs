//! Measurement hook for the exact Top-K aggregate pruning research (#386).
//!
//! Compiled only with the `topk_research` cargo feature, so no shipped build
//! contains it. It runs the plan a terminal would run (`ordered_df`), the
//! same way `to_arrow` does, and hands back DataFusion's per-operator
//! metrics, so `benchmarks/topk_agg/` can attribute time to the scan, the
//! partial aggregate, the hash repartition, the final aggregate and the
//! Top-K sort instead of inferring a stage's cost from wall-clock
//! differences.
//!
//! The hook changes no plan and adds no operator: it measures today's
//! engine. `prune` holds the spike the measurements price: an exact
//! slot-bound pruner for a grouped COUNT Top-K, off unless
//! `_topk_set_prune` turns it on.

pub(crate) mod prune;

use std::time::Instant;

use datafusion::physical_plan::{collect, displayable, ExecutionPlan};
use pyo3::prelude::*;

use crate::engine::RUNTIME;
use crate::error::LtseqError;
use crate::gil::detached;
use crate::LTSeqTable;

/// One operator of an executed plan: its depth in the tree, its name, its
/// one-line description, and its metrics summed over partitions by name
/// (times in nanoseconds; `elapsed_compute` is CPU time summed over
/// partitions, not wall time).
type OperatorMetrics = (usize, String, String, Vec<(String, usize)>);

/// What `_topk_analyze` returns: planning and execution wall time in
/// nanoseconds, the number of result rows, the plan annotated with its
/// metrics, and one record per operator in pre-order.
type Analysis = (u64, u64, usize, String, Vec<OperatorMetrics>);

fn walk(plan: &dyn ExecutionPlan, depth: usize, out: &mut Vec<OperatorMetrics>) {
    let metrics = plan
        .metrics()
        .map(|set| {
            set.aggregate_by_name()
                .sorted_for_display()
                .timestamps_removed()
                .iter()
                .map(|m| (m.value().name().to_string(), m.value().as_usize()))
                .collect()
        })
        .unwrap_or_default();
    out.push((
        depth,
        plan.name().to_string(),
        displayable(plan).one_line().to_string().trim_end().to_string(),
        metrics,
    ));
    for child in plan.children() {
        walk(child.as_ref(), depth + 1, out);
    }
}

/// Execute `table` once and return its plan's metrics (see [`Analysis`]).
#[pyfunction]
pub(crate) fn _topk_analyze(py: Python<'_>, table: PyRef<'_, LTSeqTable>) -> PyResult<Analysis> {
    let df = table.ordered_df()?;
    detached(py, move || {
        let started = Instant::now();
        let task_ctx = df.task_ctx();
        let plan = RUNTIME
            .block_on(df.create_physical_plan())
            .map_err(|e| LtseqError::with_context("Failed to create physical plan", e))?;
        let planned = Instant::now();
        let batches = RUNTIME
            .block_on(collect(plan.clone(), task_ctx.into()))
            .map_err(|e| LtseqError::with_context("Failed to execute plan", e))?;
        let executed = Instant::now();
        let rows = batches.iter().map(|b| b.num_rows()).sum();
        let text = datafusion::physical_plan::display::DisplayableExecutionPlan::with_metrics(
            plan.as_ref(),
        )
        .indent(true)
        .to_string();
        let mut operators = Vec::new();
        walk(plan.as_ref(), 0, &mut operators);
        Ok((
            (planned - started).as_nanos() as u64,
            (executed - planned).as_nanos() as u64,
            rows,
            text,
            operators,
        ))
    })
}

/// Turn the slot-bound pruning spike on with `slots` slots per final
/// partition, or off with 0; `prune=False` keeps its waves but merges every
/// slot (the control). Returns the previous `(slots, prune)`. Applies to
/// every plan optimized afterwards (see `prune`).
#[pyfunction]
#[pyo3(signature = (slots, prune = true))]
pub(crate) fn _topk_set_prune(slots: usize, prune: bool) -> (usize, bool) {
    prune::set(slots, prune)
}
