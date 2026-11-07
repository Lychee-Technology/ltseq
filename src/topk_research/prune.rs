//! Spike: exact slot-bound pruning of a grouped COUNT Top-K (#386).
//!
//! Off unless `_topk_set_prune` sets a slot count, and compiled only with
//! the `topk_research` feature. When on, [`SlotPruneTopKCount`] replaces
//! the final aggregate of
//!
//! ```text
//! SortExec: TopK(fetch=K), expr=[c DESC]
//!   [ProjectionExec passing c through]*
//!     AggregateExec: mode=FinalPartitioned, gby=[keys], aggr=[count(..) as c]
//!       RepartitionExec: partitioning=Hash(..)
//! ```
//!
//! with a [`SlotPrunedCountExec`]; every other plan is left alone.
//!
//! The pruner runs in each final partition on its own. The hash
//! repartition below gives each key's partial rows to exactly one
//! partition, so the partition's exact top-K is all the Top-K sort above
//! needs. The pruner buffers the partition's partial rows, hashes each
//! row's key into one of `slots` slots, and sums the partial counts per
//! slot. COUNT's partial counts are non-negative and add up to the final
//! count, and equal keys hash alike (DataFusion's own hash tables rely on
//! it), so `count(g) <= bound(slot(g))` for every group. Slots are ranked
//! by bound, descending, and merged in waves of doubling size (ranks
//! `[2^w - 1, 2^(w+1) - 1)`) by DataFusion's own `AggregateExec` in mode
//! `Final`, so the group table is the baseline's. Before each wave a slot
//! whose bound is strictly below the threshold is skipped: none of its
//! groups can reach it, and the threshold only rises. The groups of every
//! merged slot are emitted, ties with the threshold included.
//!
//! The threshold is the largest K-th count any partition has finalized so
//! far, shared through an atomic. A partition's K-th count is at most the
//! global K-th count, so a group below it is in no top-K; every group at
//! or above the global K-th count is merged and emitted by its partition,
//! and the Top-K sort and merge above pick an exact top-K from them.
//!
//! What the spike leaves out: the buffered input is not registered with
//! the memory pool and never spills; the hash is computed again rather
//! than taken from the repartition; and the shared threshold lives in the
//! plan, so it is right for one execution of the plan (LTSeq plans every
//! terminal afresh; re-executing a plan over changed files would not be).

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, UInt32Array};
use datafusion::arrow::compute::take_record_batch;
use datafusion::arrow::datatypes::{DataType, Int64Type, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::config::ConfigOptions;
use datafusion::common::hash_utils::{create_hashes, RandomState};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{internal_err, Result};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    collect, ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    InputDistributionRequirements, Partitioning, PlanProperties, ReplaceChildrenOptions,
};
use futures_util::{stream, StreamExt, TryStreamExt};

/// Slots per final partition; zero means the spike is off.
static SLOTS: AtomicUsize = AtomicUsize::new(0);

/// Whether the spike prunes. Off, it buffers, slots and merges in waves
/// exactly as when on but merges every slot: the control that tells the
/// gain of pruning from the gain of merging in slot waves.
static PRUNE: AtomicBool = AtomicBool::new(true);

/// Seed of the slot hash; any seed but the repartition's (0) will do.
const SLOT_SEED: u64 = 386;

/// Set the slots per partition (0 turns the spike off) and whether it
/// prunes; returns the previous values. Read when a plan is optimized, so
/// they apply to the next terminal of any table.
pub(crate) fn set(slots: usize, prune: bool) -> (usize, bool) {
    (
        SLOTS.swap(slots, Ordering::Relaxed),
        PRUNE.swap(prune, Ordering::Relaxed),
    )
}

/// The physical rule that plans [`SlotPrunedCountExec`]; registered last
/// but one in a `topk_research` build's sessions (`engine`).
#[derive(Debug)]
pub(crate) struct SlotPruneTopKCount;

impl PhysicalOptimizerRule for SlotPruneTopKCount {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let slots = SLOTS.load(Ordering::Relaxed);
        if slots == 0 {
            return Ok(plan);
        }
        plan.transform_down(|node| {
            Ok(match rewrite_sort(&node, slots) {
                Some(new) => Transformed::new(new, true, TreeNodeRecursion::Jump),
                None => Transformed::no(node),
            })
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "ltseq_topk_slot_prune"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn rewrite_sort(node: &Arc<dyn ExecutionPlan>, slots: usize) -> Option<Arc<dyn ExecutionPlan>> {
    let sort = node.downcast_ref::<SortExec>()?;
    let k = sort.fetch().filter(|&k| k > 0)?;
    let mut orders = sort.expr().iter();
    let order = orders.next()?;
    if orders.next().is_some() || !order.options.descending {
        return None;
    }
    let mut name = order.expr.downcast_ref::<Column>()?.name().to_string();
    let child = rewrite_below(sort.input(), &mut name, k, slots)?;
    with_child(node, child)
}

/// `plan` over `child`, its properties recomputed.
fn with_child(
    plan: &Arc<dyn ExecutionPlan>,
    child: Arc<dyn ExecutionPlan>,
) -> Option<Arc<dyn ExecutionPlan>> {
    Arc::clone(plan)
        .replace_children(
            vec![child],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
        .ok()
}

/// Rewrite the final aggregate under pass-through projections, following
/// the ordering column's name down; `None` if the shape is not eligible.
fn rewrite_below(
    plan: &Arc<dyn ExecutionPlan>,
    name: &mut String,
    k: usize,
    slots: usize,
) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        let expr = projection.expr().iter().find(|e| e.alias == *name)?;
        *name = expr.expr.downcast_ref::<Column>()?.name().to_string();
        let child = rewrite_below(projection.input(), name, k, slots)?;
        return with_child(plan, child);
    }
    let aggregate = plan.downcast_ref::<AggregateExec>()?;
    let shape = Shape::of(aggregate, name)?;
    Some(Arc::new(SlotPrunedCountExec {
        aggregate: Arc::clone(plan),
        shape,
        k,
        slots,
        prune: PRUNE.load(Ordering::Relaxed),
        threshold: Arc::new(AtomicI64::new(i64::MIN)),
        metrics: ExecutionPlanMetricsSet::new(),
    }))
}

/// What the pruner needs of an eligible final aggregate.
#[derive(Debug, Clone)]
struct Shape {
    group_by: Arc<PhysicalGroupBy>,
    aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    filter_expr: Vec<Option<Arc<dyn PhysicalExpr>>>,
    /// The schema the aggregate's expressions were planned against.
    aggregate_input_schema: SchemaRef,
    /// The schema of the partial rows the final aggregate reads.
    partial_schema: SchemaRef,
    /// Partial-row columns of the group keys, and of the partial count.
    key_columns: Vec<usize>,
    state_column: usize,
    /// Output column of the count.
    count_column: usize,
}

impl Shape {
    /// The eligible shape: a final aggregate over a hash repartition, one
    /// grouping set, and a single non-distinct, unfiltered `count` named
    /// `order_by` whose partial state is one Int64 column.
    fn of(aggregate: &AggregateExec, order_by: &str) -> Option<Shape> {
        if *aggregate.mode() != AggregateMode::FinalPartitioned
            || aggregate.limit_options().is_some()
            || !aggregate.group_expr().is_single()
            || aggregate.group_expr().expr().is_empty()
            || aggregate.filter_expr().iter().any(Option::is_some)
        {
            return None;
        }
        let [count] = aggregate.aggr_expr() else {
            return None;
        };
        if count.fun().name() != "count" || count.is_distinct() || count.name() != order_by {
            return None;
        }
        let repartition = aggregate.input().downcast_ref::<RepartitionExec>()?;
        if !matches!(repartition.partitioning(), Partitioning::Hash(..)) {
            return None;
        }
        let partial_schema = aggregate.input().schema();
        let state = count.state_fields().ok()?;
        let [state] = state.as_slice() else {
            return None;
        };
        if state.data_type() != &DataType::Int64 {
            return None;
        }
        let state_column = partial_schema.index_of(state.name()).ok()?;
        let key_columns = aggregate
            .group_expr()
            .expr()
            .iter()
            .map(|(_, alias)| partial_schema.index_of(alias).ok())
            .collect::<Option<Vec<_>>>()?;
        let count_column = aggregate.schema().index_of(count.name()).ok()?;
        Some(Shape {
            group_by: Arc::new(aggregate.group_expr().clone()),
            aggr_expr: aggregate.aggr_expr().to_vec(),
            filter_expr: aggregate.filter_expr().to_vec(),
            aggregate_input_schema: aggregate.input_schema(),
            partial_schema,
            key_columns,
            state_column,
            count_column,
        })
    }
}

/// The final aggregate of an eligible grouped COUNT Top-K, with slot-bound
/// pruning (see the module docstring). Same input, partitioning and
/// output schema as the `AggregateExec` it replaces.
#[derive(Debug)]
pub(crate) struct SlotPrunedCountExec {
    /// The replaced `AggregateExec`, kept for its properties and input.
    aggregate: Arc<dyn ExecutionPlan>,
    shape: Shape,
    k: usize,
    slots: usize,
    prune: bool,
    /// The largest K-th count a partition has finalized; `i64::MIN` until
    /// one has.
    threshold: Arc<AtomicI64>,
    metrics: ExecutionPlanMetricsSet,
}

impl DisplayAs for SlotPrunedCountExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SlotPrunedCountExec: k={}, slots={}, prune={}, replaces ",
            self.k, self.slots, self.prune
        )?;
        self.aggregate.fmt_as(t, f)
    }
}

impl ExecutionPlan for SlotPrunedCountExec {
    fn name(&self) -> &str {
        "SlotPrunedCountExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.aggregate.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.aggregate.children()
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        self.aggregate.input_distribution_requirements()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let aggregate = Arc::clone(&self.aggregate).replace_children(children, options)?;
        Ok(Arc::new(SlotPrunedCountExec {
            aggregate,
            shape: self.shape.clone(),
            k: self.k,
            slots: self.slots,
            prune: self.prune,
            threshold: Arc::new(AtomicI64::new(i64::MIN)),
            metrics: ExecutionPlanMetricsSet::new(),
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
        let [input] = self.aggregate.children()[..] else {
            return internal_err!("SlotPrunedCountExec needs one input");
        };
        let input = input.execute(partition, Arc::clone(&context))?;
        let metrics = PartitionMetrics::new(&self.metrics, partition);
        let pruner = Pruner {
            shape: self.shape.clone(),
            k: self.k,
            slots: self.slots,
            prune: self.prune,
            shared: Arc::clone(&self.threshold),
        };
        let batches = stream::once(async move { pruner.run(input, context, metrics).await })
            .map_ok(|batches| stream::iter(batches.into_iter().map(Ok)))
            .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema(), batches)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct PartitionMetrics {
    /// Everything after the input is drained: slotting, gathers and the
    /// merges (comparable to the replaced aggregate's `elapsed_compute`,
    /// which also leaves out waiting for input).
    elapsed_compute: Time,
    /// Hashing rows into slots, summing bounds, and bucketing rows.
    slot_time: Time,
    output_rows: Count,
    rows_in: Count,
    rows_merged: Count,
    slots_merged: Count,
    waves: Count,
}

impl PartitionMetrics {
    fn new(set: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        PartitionMetrics {
            elapsed_compute: MetricBuilder::new(set).elapsed_compute(partition),
            slot_time: MetricBuilder::new(set).subset_time("slot_time", partition),
            output_rows: MetricBuilder::new(set).output_rows(partition),
            rows_in: MetricBuilder::new(set).counter("rows_in", partition),
            rows_merged: MetricBuilder::new(set).counter("rows_merged", partition),
            slots_merged: MetricBuilder::new(set).counter("slots_merged", partition),
            waves: MetricBuilder::new(set).counter("waves", partition),
        }
    }
}

struct Pruner {
    shape: Shape,
    k: usize,
    slots: usize,
    prune: bool,
    shared: Arc<AtomicI64>,
}

impl Pruner {
    async fn run(
        self,
        mut input: SendableRecordBatchStream,
        context: Arc<TaskContext>,
        metrics: PartitionMetrics,
    ) -> Result<Vec<RecordBatch>> {
        // The bounds are complete only once every partial row is in.
        let mut batches = Vec::new();
        while let Some(batch) = input.next().await {
            let batch = batch?;
            if batch.num_rows() > 0 {
                batches.push(batch);
            }
        }
        let compute = metrics.elapsed_compute.clone();
        let started = compute.timer();

        let slot_timer = metrics.slot_time.timer();
        let (bound, row_slots) = self.bounds(&batches)?;
        metrics
            .rows_in
            .add(row_slots.iter().map(Vec::len).sum::<usize>());
        // Occupied slots by bound, descending; a slot's wave is
        // floor(log2(rank + 1)).
        let mut occupied = vec![false; self.slots];
        row_slots.iter().flatten().for_each(|&s| occupied[s as usize] = true);
        let mut order: Vec<usize> = (0..self.slots).filter(|&s| occupied[s]).collect();
        order.sort_unstable_by_key(|&s| Reverse(bound[s]));
        let mut wave_of = vec![0usize; self.slots];
        for (rank, &slot) in order.iter().enumerate() {
            wave_of[slot] = (usize::BITS - 1 - (rank + 1).leading_zeros()) as usize;
        }
        let waves = order.last().map_or(0, |&s| wave_of[s] + 1);
        // rows[wave][batch]: the rows of a wave's slots in each batch.
        let mut rows = vec![vec![Vec::<u32>::new(); batches.len()]; waves];
        for (b, slots) in row_slots.iter().enumerate() {
            for (row, &slot) in slots.iter().enumerate() {
                rows[wave_of[slot as usize]][b].push(row as u32);
            }
        }
        drop(slot_timer);

        let mut heap = BinaryHeap::<Reverse<i64>>::new();
        let mut out = Vec::new();
        for (wave, wave_rows) in rows.into_iter().enumerate() {
            let local = (heap.len() == self.k).then(|| heap.peek().map(|r| r.0)).flatten();
            if let Some(local) = local {
                self.shared.fetch_max(local, Ordering::Relaxed);
            }
            let threshold = Some(self.shared.load(Ordering::Relaxed))
                .filter(|&t| self.prune && t != i64::MIN);
            let first_rank = (1usize << wave) - 1;
            if threshold.is_some_and(|t| bound[order[first_rank]] < t) {
                break;
            }
            let last_rank = ((1usize << (wave + 1)) - 1).min(order.len());
            let keep = |slot: u32| threshold.is_none_or(|t| bound[slot as usize] >= t);
            metrics.slots_merged.add(
                order[first_rank..last_rank]
                    .iter()
                    .filter(|&&s| keep(s as u32))
                    .count(),
            );
            let mut gathered = Vec::new();
            for (b, indices) in wave_rows.into_iter().enumerate() {
                let indices: Vec<u32> = match threshold {
                    None => indices,
                    Some(_) => indices
                        .into_iter()
                        .filter(|&row| keep(row_slots[b][row as usize]))
                        .collect(),
                };
                if !indices.is_empty() {
                    metrics.rows_merged.add(indices.len());
                    gathered.push(take_record_batch(&batches[b], &UInt32Array::from(indices))?);
                }
            }
            if gathered.is_empty() {
                continue;
            }
            metrics.waves.add(1);
            let merged = self.merge(gathered, &context).await?;
            for batch in &merged {
                let counts = batch.column(self.shape.count_column).as_primitive::<Int64Type>();
                for count in counts.iter().flatten() {
                    if heap.len() < self.k {
                        heap.push(Reverse(count));
                    } else if heap.peek().is_some_and(|min| count > min.0) {
                        heap.pop();
                        heap.push(Reverse(count));
                    }
                }
                metrics.output_rows.add(batch.num_rows());
            }
            out.extend(merged);
        }
        drop(started);
        Ok(out)
    }

    /// Each slot's bound (the sum of its partial counts) and each row's slot.
    fn bounds(&self, batches: &[RecordBatch]) -> Result<(Vec<i64>, Vec<Vec<u32>>)> {
        let random = RandomState::with_seed(SLOT_SEED);
        let mut bound = vec![0i64; self.slots];
        let mut row_slots = Vec::with_capacity(batches.len());
        let mut hashes = Vec::new();
        for batch in batches {
            let keys: Vec<ArrayRef> = self
                .shape
                .key_columns
                .iter()
                .map(|&c| Arc::clone(batch.column(c)))
                .collect();
            hashes.clear();
            hashes.resize(batch.num_rows(), 0u64);
            create_hashes(&keys, &random, &mut hashes)?;
            let counts = batch.column(self.shape.state_column).as_primitive::<Int64Type>();
            let slots: Vec<u32> = hashes
                .iter()
                .map(|h| ((h >> 32) % self.slots as u64) as u32)
                .collect();
            for (row, &slot) in slots.iter().enumerate() {
                if counts.is_valid(row) {
                    bound[slot as usize] += counts.value(row);
                }
            }
            row_slots.push(slots);
        }
        Ok((bound, row_slots))
    }

    /// Merge one wave's partial rows with DataFusion's final aggregate.
    async fn merge(
        &self,
        batches: Vec<RecordBatch>,
        context: &Arc<TaskContext>,
    ) -> Result<Vec<RecordBatch>> {
        let source = MemorySourceConfig::try_new_exec(
            &[batches],
            Arc::clone(&self.shape.partial_schema),
            None,
        )?;
        let aggregate = AggregateExec::try_new(
            AggregateMode::Final,
            Arc::clone(&self.shape.group_by),
            self.shape.aggr_expr.clone(),
            self.shape.filter_expr.clone(),
            source,
            Arc::clone(&self.shape.aggregate_input_schema),
        )?;
        collect(Arc::new(aggregate), Arc::clone(context)).await
    }
}
