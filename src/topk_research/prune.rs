//! Spike: exact slot-bound pruning of a grouped COUNT Top-K (#386).
//!
//! Off unless `_topk_set_prune` sets a slot count, and compiled only with
//! the `topk_research` feature. When on, [`SlotPruneTopKCount`] replaces
//! the final aggregate of
//!
//! ```text
//! SortExec: TopK(fetch=K), expr=[c DESC]
//!   [ProjectionExec of columns and literals only]*
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
//! row's key into one of S slots, and records per slot the sum of its
//! partial counts (`bound`) and its largest single partial count (`maxp`).
//! S is fixed, or sized from the buffered row count (`rows_per_slot`).
//! COUNT's partial counts are non-negative and add up to the final count,
//! and equal keys hash alike (DataFusion's own hash tables rely on it), so
//! for every group g, `maxp(s) <= max count in s` and
//! `count(g) <= bound(slot(g))`.
//!
//! Thresholds, each at most the global K-th count:
//!
//! - `T0`, before any merge: the K-th largest `maxp` over occupied slots.
//!   Slots hold disjoint groups, so K slots with `maxp >= T0` name K
//!   distinct groups of this partition whose counts reach `T0`.
//! - `T1`, after the first merge: the K-th count of the groups of the K
//!   slots with the largest `maxp` (at least K groups, all final).
//! - the shared one: the largest value any partition has published.
//!
//! A partition's K-th count is at most the global K-th count, so a slot
//! whose bound is strictly below a threshold holds no group of any top-K;
//! equality never prunes. Every group of a merged slot is emitted, and the
//! Top-K sort and merge above pick an exact top-K from them.
//!
//! The steps, at most three merges by DataFusion's own `AggregateExec` in
//! mode `Final` (so the group table is the baseline's):
//!
//! 1. merge the K slots with the largest `maxp` (when there are K
//!    occupied slots), giving `T1`;
//! 2. take the other slots whose bound reaches the threshold; if they and
//!    the first merge's slots hold more than half of the partition's rows,
//!    bail out: discard the first merge and merge every buffered batch as
//!    it is, with no gather;
//! 3. otherwise gather the kept rows once, and merge them.
//!
//! Before all of that, once a partition has buffered [`PROBE_ROWS`] rows,
//! a probe computes `T0` and the bounds of those rows alone; if more than
//! [`EARLY_BAIL_SHARE`] of them lie in slots that reach it, the partition
//! stops buffering and streams the buffered batches and the rest of its
//! input into one final aggregate. Bailing out is always exact, so the
//! probe is a guess about speed only.
//!
//! The control (`prune = false`) probes alike, and after buffering does
//! step 1 and then always bails out, so it pays everything the pruner pays
//! when it cannot prune.
//!
//! What the spike leaves out: the buffered input is not registered with
//! the memory pool and never spills; the hash is computed again rather
//! than taken from the repartition; and the shared threshold lives in the
//! plan, so it is right for one execution of the plan (LTSeq plans every
//! terminal afresh; re-executing a plan over changed files would not be).

use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use datafusion::arrow::array::{Array, ArrayRef, AsArray, UInt32Array};
use datafusion::arrow::compute::{concat_batches, take_record_batch};
use datafusion::arrow::datatypes::{DataType, Int64Type, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::config::ConfigOptions;
use datafusion::common::hash_utils::{create_hashes, RandomState};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{internal_err, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::expressions::{Column, Literal};
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

/// Fixed slots per final partition; with [`ROWS_PER_SLOT`] zero too, the
/// spike is off.
static SLOTS: AtomicUsize = AtomicUsize::new(0);

/// When non-zero, each partition sizes its slot count from its input:
/// one slot per this many buffered partial rows ([`SLOTS`] is ignored).
static ROWS_PER_SLOT: AtomicUsize = AtomicUsize::new(0);

/// Whether the spike prunes. Off, it buffers, slots and runs the first
/// merge exactly as when on, then merges everything: the control that
/// tells the gain of pruning from the cost of the mechanics.
static PRUNE: AtomicBool = AtomicBool::new(true);

/// Seed of the slot hash; any seed but the repartition's (0) will do.
const SLOT_SEED: u64 = 386;

/// Bail out (merge every buffered batch as it is) when the rows to merge
/// exceed this share of the partition's input.
const BAIL_SHARE: f64 = 0.5;

/// Rows a partition buffers before its probe decides whether to keep
/// buffering.
const PROBE_ROWS: usize = 1 << 17;

/// The probe bails out when more than this share of its rows lies in slots
/// it could not prune.
const EARLY_BAIL_SHARE: f64 = 0.9;

/// Largest slot count a partition sizes for itself.
const MAX_SLOTS: usize = 1 << 22;

/// How a partition chooses its slot count.
#[derive(Debug, Clone, Copy)]
enum Sizing {
    Fixed(usize),
    PerRows(usize),
}

impl Sizing {
    fn slots(self, rows_in: usize) -> usize {
        match self {
            Sizing::Fixed(slots) => slots,
            Sizing::PerRows(per) => rows_in.div_ceil(per).clamp(1, MAX_SLOTS),
        }
    }
}

impl fmt::Display for Sizing {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Sizing::Fixed(slots) => write!(f, "slots={slots}"),
            Sizing::PerRows(per) => write!(f, "rows_per_slot={per}"),
        }
    }
}

/// Set the fixed slots per partition, whether the spike prunes, and the
/// rows per slot that size the slot count from the data instead (0: use
/// `slots`); `slots` and `rows_per_slot` both 0 turn the spike off.
/// Returns the previous values. Read when a plan is optimized, so they
/// apply to the next terminal of any table.
pub(crate) fn set(slots: usize, prune: bool, rows_per_slot: usize) -> (usize, bool, usize) {
    (
        SLOTS.swap(slots, Ordering::Relaxed),
        PRUNE.swap(prune, Ordering::Relaxed),
        ROWS_PER_SLOT.swap(rows_per_slot, Ordering::Relaxed),
    )
}

fn sizing() -> Option<Sizing> {
    match (
        ROWS_PER_SLOT.load(Ordering::Relaxed),
        SLOTS.load(Ordering::Relaxed),
    ) {
        (0, 0) => None,
        (0, slots) => Some(Sizing::Fixed(slots)),
        (per, _) => Some(Sizing::PerRows(per)),
    }
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
        let Some(sizing) = sizing() else {
            return Ok(plan);
        };
        plan.transform_down(|node| {
            Ok(match rewrite_sort(&node, sizing) {
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

fn rewrite_sort(node: &Arc<dyn ExecutionPlan>, sizing: Sizing) -> Option<Arc<dyn ExecutionPlan>> {
    let sort = node.downcast_ref::<SortExec>()?;
    let k = sort.fetch().filter(|&k| k > 0)?;
    let mut orders = sort.expr().iter();
    let order = orders.next()?;
    if orders.next().is_some() || !order.options.descending {
        return None;
    }
    let mut name = order.expr.downcast_ref::<Column>()?.name().to_string();
    let child = rewrite_below(sort.input(), &mut name, k, sizing)?;
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

/// Whether evaluating `expr` can never fail: a column reference or a
/// literal. Anything else (arithmetic, casts, calls) is fallible in general,
/// for example `1 // (k - 20000)` or a checked `cnt * 2`.
fn infallible(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.downcast_ref::<Column>().is_some() || expr.downcast_ref::<Literal>().is_some()
}

/// Rewrite the final aggregate under pass-through projections, following
/// the ordering column's name down; `None` if the shape is not eligible.
///
/// A projection between the Top-K sort and the aggregate evaluates every
/// one of its expressions on every group the aggregate emits, so an error
/// it raises on a group the pruner would discard is observable today.
/// Pruning below it is exact only when none of its expressions can fail,
/// so every expression must be a column or a literal, not just the one
/// that carries the count.
fn rewrite_below(
    plan: &Arc<dyn ExecutionPlan>,
    name: &mut String,
    k: usize,
    sizing: Sizing,
) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        if !projection.expr().iter().all(|e| infallible(&e.expr)) {
            return None;
        }
        let expr = projection.expr().iter().find(|e| e.alias == *name)?;
        *name = expr.expr.downcast_ref::<Column>()?.name().to_string();
        let child = rewrite_below(projection.input(), name, k, sizing)?;
        return with_child(plan, child);
    }
    let aggregate = plan.downcast_ref::<AggregateExec>()?;
    let shape = Shape::of(aggregate, name)?;
    Some(Arc::new(SlotPrunedCountExec {
        aggregate: Arc::clone(plan),
        shape,
        k,
        sizing,
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
    sizing: Sizing,
    prune: bool,
    /// The largest threshold a partition has published; `i64::MIN` until
    /// one has.
    threshold: Arc<AtomicI64>,
    metrics: ExecutionPlanMetricsSet,
}

impl DisplayAs for SlotPrunedCountExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SlotPrunedCountExec: k={}, {}, prune={}, replaces ",
            self.k, self.sizing, self.prune
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
            sizing: self.sizing,
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
            sizing: self.sizing,
            prune: self.prune,
            shared: Arc::clone(&self.threshold),
        };
        let batches = stream::once(pruner.run_timed(input, context, metrics))
            .map_ok(|batches| stream::iter(batches.into_iter().map(Ok)))
            .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            batches,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct PartitionMetrics {
    /// The operator's own work: the time spent inside its polls (buffering,
    /// the probe, slotting, gathers and the merges) less the time spent
    /// inside its input's polls. Time the operator is suspended waiting for
    /// input lies between polls and is not counted, as in the replaced
    /// aggregate's `elapsed_compute`; like it, this is wall time on a worker
    /// thread, so it also counts time that thread waited for a CPU.
    elapsed_compute: Time,
    /// Time spent inside the input's polls, left out of `elapsed_compute`.
    input_time: Time,
    /// Hashing rows into slots and computing the bounds.
    slot_time: Time,
    /// Copying the rows of the first merge and of the kept slots.
    gather_time: Time,
    output_rows: Count,
    rows_in: Count,
    /// Rows of the merges whose groups are emitted: every input row when
    /// the partition bails out, otherwise the first merge's and the kept
    /// slots' rows.
    rows_merged: Count,
    /// Rows of the first merge (merged again when the partition bails out).
    rows_first: Count,
    slots: Count,
    slots_merged: Count,
    /// Merges run, the discarded first one included.
    waves: Count,
    /// Partitions that bailed out, and those whose probe did.
    bailed: Count,
    bailed_early: Count,
}

impl PartitionMetrics {
    fn new(set: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let counter = |name: &'static str| MetricBuilder::new(set).counter(name, partition);
        PartitionMetrics {
            elapsed_compute: MetricBuilder::new(set).elapsed_compute(partition),
            input_time: MetricBuilder::new(set).subset_time("input_time", partition),
            slot_time: MetricBuilder::new(set).subset_time("slot_time", partition),
            gather_time: MetricBuilder::new(set).subset_time("gather_time", partition),
            output_rows: MetricBuilder::new(set).output_rows(partition),
            rows_in: counter("rows_in"),
            rows_merged: counter("rows_merged"),
            rows_first: counter("rows_first"),
            slots: counter("slots"),
            slots_merged: counter("slots_merged"),
            waves: counter("waves"),
            bailed: counter("bailed"),
            bailed_early: counter("bailed_early"),
        }
    }
}

/// A partition's slots: each buffered row's slot, and per slot its bound,
/// its largest partial count and its row count.
struct Slots {
    of_row: Vec<Vec<u32>>,
    bound: Vec<i64>,
    maxp: Vec<i64>,
    rows: Vec<usize>,
}

impl Slots {
    fn occupied(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.rows.len()).filter(|&s| self.rows[s] > 0)
    }
}

/// The K-th largest of `values`, if there are K.
fn kth_largest(mut values: Vec<i64>, k: usize) -> Option<i64> {
    if k == 0 || values.len() < k {
        return None;
    }
    let (_, kth, _) = values.select_nth_unstable_by(k - 1, |a, b| b.cmp(a));
    Some(*kth)
}

struct Pruner {
    shape: Shape,
    k: usize,
    sizing: Sizing,
    prune: bool,
    shared: Arc<AtomicI64>,
}

impl Pruner {
    /// [`Pruner::run`], with its own work measured into `elapsed_compute`.
    ///
    /// Every merge runs a one-partition plan inside this future, so all of
    /// the operator's work happens inside its polls. Each poll is timed, and
    /// the time spent inside the input's polls during it is subtracted. A
    /// poll that returns `Pending` ends the timing, so the intervals spent
    /// suspended on the input (or on anything else) are never counted, on
    /// the early-bail path's streaming merge as on the buffering one.
    fn run_timed(
        self,
        input: SendableRecordBatchStream,
        context: Arc<TaskContext>,
        metrics: PartitionMetrics,
    ) -> impl Future<Output = Result<Vec<RecordBatch>>> {
        let (compute, input_time) = (metrics.elapsed_compute.clone(), metrics.input_time.clone());
        let input = {
            let input_time = input_time.clone();
            let schema = input.schema();
            let mut input = input;
            let timed = stream::poll_fn(move |cx| {
                let _timer = input_time.timer();
                input.poll_next_unpin(cx)
            });
            Box::pin(RecordBatchStreamAdapter::new(schema, timed)) as SendableRecordBatchStream
        };
        let mut run = Box::pin(self.run(input, context, metrics));
        std::future::poll_fn(move |cx| {
            let started = Instant::now();
            let waited = input_time.value();
            let polled = run.as_mut().poll(cx);
            let inside = started.elapsed();
            let waited = Duration::from_nanos((input_time.value() - waited) as u64);
            compute.add_duration(inside.saturating_sub(waited));
            polled
        })
    }

    async fn run(
        self,
        mut input: SendableRecordBatchStream,
        context: Arc<TaskContext>,
        metrics: PartitionMetrics,
    ) -> Result<Vec<RecordBatch>> {
        // The bounds are complete only once every partial row is in; a probe
        // of the first rows can give up on pruning before then.
        let mut batches = Vec::new();
        let mut buffered = 0;
        let mut probed = false;
        while let Some(batch) = input.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            buffered += batch.num_rows();
            batches.push(batch);
            if !probed && buffered >= PROBE_ROWS {
                probed = true;
                if self.hopeless(&batches, buffered, &metrics)? {
                    return self.stream_all(batches, input, &context, &metrics).await;
                }
            }
        }
        let rows_in = buffered;
        let batch_size = context.session_config().batch_size();
        metrics.rows_in.add(rows_in);
        if rows_in == 0 {
            return Ok(Vec::new());
        }

        let slot_timer = metrics.slot_time.timer();
        let slots = self.slots(&batches, self.sizing.slots(rows_in))?;
        metrics.slots.add(slots.rows.len());
        let maxps: Vec<i64> = slots.occupied().map(|s| slots.maxp[s]).collect();
        let t0 = kth_largest(maxps, self.k);
        drop(slot_timer);

        // First merge: the K slots with the largest single partial count.
        let mut first = vec![false; slots.rows.len()];
        let mut first_out = Vec::new();
        let mut first_rows = 0;
        let mut local = None;
        if let Some(t0) = t0 {
            if self.prune {
                self.shared.fetch_max(t0, Ordering::Relaxed);
            }
            let mut ranked: Vec<usize> = slots.occupied().collect();
            ranked.select_nth_unstable_by_key(self.k - 1, |&s| std::cmp::Reverse(slots.maxp[s]));
            for &s in &ranked[..self.k] {
                first[s] = true;
                first_rows += slots.rows[s];
            }
            let gathered = self.gather(&batches, &slots, &metrics, batch_size, |s| first[s])?;
            metrics.waves.add(1);
            metrics.rows_first.add(first_rows);
            first_out = self.merge(gathered, &context).await?;
            local = kth_largest(self.counts(&first_out), self.k);
            if let (true, Some(t1)) = (self.prune, local) {
                self.shared.fetch_max(t1, Ordering::Relaxed);
            }
        }

        // The slots left to merge, and whether to bail out.
        let threshold = self.prune.then(|| {
            [local, t0, Some(self.shared.load(Ordering::Relaxed))]
                .into_iter()
                .flatten()
                .filter(|&t| t != i64::MIN)
                .max()
        });
        let threshold = threshold.flatten();
        let keep = |s: usize| !first[s] && threshold.is_none_or(|t| slots.bound[s] >= t);
        let kept_rows: usize = slots
            .occupied()
            .filter(|&s| keep(s))
            .map(|s| slots.rows[s])
            .sum();
        if !self.prune || (first_rows + kept_rows) as f64 > BAIL_SHARE * rows_in as f64 {
            metrics.bailed.add(1);
            metrics.waves.add(1);
            metrics.rows_merged.add(rows_in);
            metrics.slots_merged.add(slots.occupied().count());
            drop(first_out);
            drop(slots);
            let out = self.merge(batches, &context).await?;
            metrics
                .output_rows
                .add(out.iter().map(RecordBatch::num_rows).sum());
            return Ok(out);
        }
        metrics.rows_merged.add(first_rows + kept_rows);
        metrics
            .slots_merged
            .add(slots.occupied().filter(|&s| first[s] || keep(s)).count());
        let mut out = first_out;
        if kept_rows > 0 {
            let gathered = self.gather(&batches, &slots, &metrics, batch_size, keep)?;
            drop(batches);
            metrics.waves.add(1);
            out.extend(self.merge(gathered, &context).await?);
        }
        metrics
            .output_rows
            .add(out.iter().map(RecordBatch::num_rows).sum());
        Ok(out)
    }

    /// Whether the probe says pruning will not pay: more than
    /// [`EARLY_BAIL_SHARE`] of the first rows lie in slots whose bound
    /// reaches their own `T0`. A guess about speed, not a correctness rule:
    /// the bounds of the whole input only grow, and so may the threshold.
    fn hopeless(
        &self,
        batches: &[RecordBatch],
        rows: usize,
        metrics: &PartitionMetrics,
    ) -> Result<bool> {
        let _timer = metrics.slot_time.timer();
        let slots = self.slots(batches, self.sizing.slots(rows))?;
        let maxps: Vec<i64> = slots.occupied().map(|s| slots.maxp[s]).collect();
        let Some(t0) = kth_largest(maxps, self.k) else {
            return Ok(false);
        };
        let kept: usize = slots
            .occupied()
            .filter(|&s| slots.bound[s] >= t0)
            .map(|s| slots.rows[s])
            .sum();
        Ok(kept as f64 > EARLY_BAIL_SHARE * rows as f64)
    }

    /// Bail out early: merge the buffered batches and the rest of the input
    /// in one final aggregate, streaming.
    async fn stream_all(
        &self,
        batches: Vec<RecordBatch>,
        input: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
        metrics: &PartitionMetrics,
    ) -> Result<Vec<RecordBatch>> {
        let buffered: usize = batches.iter().map(RecordBatch::num_rows).sum();
        metrics.rows_in.add(buffered);
        metrics.rows_merged.add(buffered);
        metrics.bailed.add(1);
        metrics.bailed_early.add(1);
        metrics.waves.add(1);
        let rest = {
            let (rows_in, rows_merged) = (metrics.rows_in.clone(), metrics.rows_merged.clone());
            let mut input = input;
            stream::poll_fn(move |cx| {
                let next = input.poll_next_unpin(cx);
                if let std::task::Poll::Ready(Some(Ok(batch))) = &next {
                    rows_in.add(batch.num_rows());
                    rows_merged.add(batch.num_rows());
                }
                next
            })
        };
        let all = stream::iter(batches.into_iter().map(Ok)).chain(rest);
        let out = self
            .merge_stream(
                Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&self.shape.partial_schema),
                    all,
                )),
                context,
            )
            .await?;
        metrics
            .output_rows
            .add(out.iter().map(RecordBatch::num_rows).sum());
        Ok(out)
    }

    /// Hash every row into one of `count` slots.
    fn slots(&self, batches: &[RecordBatch], count: usize) -> Result<Slots> {
        let random = RandomState::with_seed(SLOT_SEED);
        let mut slots = Slots {
            of_row: Vec::with_capacity(batches.len()),
            bound: vec![0; count],
            maxp: vec![0; count],
            rows: vec![0; count],
        };
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
            let counts = batch
                .column(self.shape.state_column)
                .as_primitive::<Int64Type>();
            let of_row: Vec<u32> = hashes
                .iter()
                .map(|h| ((h >> 32) % count as u64) as u32)
                .collect();
            for (row, &slot) in of_row.iter().enumerate() {
                let slot = slot as usize;
                slots.rows[slot] += 1;
                if counts.is_valid(row) {
                    let c = counts.value(row);
                    slots.bound[slot] += c;
                    slots.maxp[slot] = slots.maxp[slot].max(c);
                }
            }
            slots.of_row.push(of_row);
        }
        Ok(slots)
    }

    /// The rows of the slots `pick` selects, in batches of the session's
    /// batch size.
    fn gather(
        &self,
        batches: &[RecordBatch],
        slots: &Slots,
        metrics: &PartitionMetrics,
        batch_size: usize,
        pick: impl Fn(usize) -> bool,
    ) -> Result<Vec<RecordBatch>> {
        let _timer = metrics.gather_time.timer();
        let mut pieces = Vec::new();
        for (batch, of_row) in batches.iter().zip(&slots.of_row) {
            let indices: Vec<u32> = (0..of_row.len() as u32)
                .filter(|&row| pick(of_row[row as usize] as usize))
                .collect();
            if indices.len() == of_row.len() {
                pieces.push(batch.clone());
            } else if !indices.is_empty() {
                pieces.push(take_record_batch(batch, &UInt32Array::from(indices))?);
            }
        }
        if pieces.len() <= 1 {
            return Ok(pieces);
        }
        let all = concat_batches(&self.shape.partial_schema, &pieces)?;
        drop(pieces);
        let size = batch_size.max(1);
        Ok((0..all.num_rows())
            .step_by(size)
            .map(|offset| all.slice(offset, size.min(all.num_rows() - offset)))
            .collect())
    }

    /// The counts of merged groups.
    fn counts(&self, merged: &[RecordBatch]) -> Vec<i64> {
        merged
            .iter()
            .flat_map(|b| {
                let counts = b
                    .column(self.shape.count_column)
                    .as_primitive::<Int64Type>();
                counts.iter().flatten().collect::<Vec<_>>()
            })
            .collect()
    }

    /// Merge partial rows with DataFusion's final aggregate, handing it the
    /// batches so it can drop each once it is read.
    async fn merge(
        &self,
        batches: Vec<RecordBatch>,
        context: &Arc<TaskContext>,
    ) -> Result<Vec<RecordBatch>> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        let stream = RecordBatchStreamAdapter::new(
            Arc::clone(&self.shape.partial_schema),
            stream::iter(batches.into_iter().map(Ok)),
        );
        self.merge_stream(Box::pin(stream), context).await
    }

    /// Merge a stream of partial rows with DataFusion's final aggregate.
    async fn merge_stream(
        &self,
        input: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<Vec<RecordBatch>> {
        let source = Arc::new(Buffered::new(input));
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

/// A one-partition source that hands out a stream once (buffered batches,
/// which the consumer drops as it reads them, or the rest of an input).
struct Buffered {
    input: Mutex<Option<SendableRecordBatchStream>>,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for Buffered {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("Buffered")
    }
}

impl Buffered {
    fn new(input: SendableRecordBatchStream) -> Self {
        let schema = input.schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Buffered {
            input: Mutex::new(Some(input)),
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for Buffered {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Buffered")
    }
}

impl ExecutionPlan for Buffered {
    fn name(&self) -> &str {
        "Buffered"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
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
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("Buffered has no children");
        }
        Ok(self)
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
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        match (partition, self.input.lock().ok().and_then(|mut b| b.take())) {
            (0, Some(input)) => Ok(input),
            _ => internal_err!("Buffered runs once, as partition 0"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::task::{Poll, Waker};
    use std::time::Duration;

    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::functions_aggregate::count::count_udaf;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_plan::expressions::{col, lit};

    /// The shape of `group by k, count(1) as cnt` over a hash repartition,
    /// planned the way DataFusion plans it.
    fn count_shape() -> Shape {
        let raw = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let source = MemorySourceConfig::try_new_exec(&[vec![]], Arc::clone(&raw), None).unwrap();
        let group_by = PhysicalGroupBy::new_single(vec![(col("k", &raw).unwrap(), "k".into())]);
        let count = Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![lit(1i64)])
                .schema(Arc::clone(&raw))
                .alias("cnt")
                .build()
                .unwrap(),
        );
        let partial = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Partial,
                Arc::new(group_by.clone()),
                vec![Arc::clone(&count)],
                vec![None],
                source,
                Arc::clone(&raw),
            )
            .unwrap(),
        );
        let hash = vec![col("k", &partial.schema()).unwrap()];
        let repartition =
            Arc::new(RepartitionExec::try_new(partial, Partitioning::Hash(hash, 1)).unwrap());
        let last = AggregateExec::try_new(
            AggregateMode::FinalPartitioned,
            Arc::new(group_by.as_final()),
            vec![count],
            vec![None],
            repartition,
            raw,
        )
        .unwrap();
        Shape::of(&last, "cnt").expect("eligible")
    }

    /// Partial rows that arrive `delay` apart: each batch is preceded by a
    /// `Pending`, woken by a thread once the delay has passed.
    fn delayed(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        delay: Duration,
    ) -> SendableRecordBatchStream {
        let mut batches = VecDeque::from(batches);
        let waker: Arc<Mutex<Option<Waker>>> = Arc::new(Mutex::new(None));
        let ready = Arc::new(AtomicBool::new(false));
        let mut sleeping = false;
        let pending = stream::poll_fn(move |cx| {
            if batches.is_empty() {
                return Poll::Ready(None);
            }
            *waker.lock().unwrap() = Some(cx.waker().clone());
            if !sleeping {
                sleeping = true;
                let (waker, ready) = (Arc::clone(&waker), Arc::clone(&ready));
                std::thread::spawn(move || {
                    std::thread::sleep(delay);
                    ready.store(true, Ordering::SeqCst);
                    if let Some(w) = waker.lock().unwrap().take() {
                        w.wake();
                    }
                });
                return Poll::Pending;
            }
            if !ready.swap(false, Ordering::SeqCst) {
                return Poll::Pending;
            }
            sleeping = false;
            Poll::Ready(batches.pop_front().map(Ok))
        });
        Box::pin(RecordBatchStreamAdapter::new(schema, pending))
    }

    #[test]
    fn early_bail_compute_leaves_out_suspended_input_waits() {
        let shape = count_shape();
        let schema = Arc::clone(&shape.partial_schema);
        // Four batches of distinct keys, each counted once: the probe after
        // the second (131,072 rows) finds every slot reaching T0 and streams
        // the other two into one merge, waiting for each.
        let rows = PROBE_ROWS / 2;
        let batches: Vec<RecordBatch> = (0..4)
            .map(|b| {
                let keys =
                    Int64Array::from_iter_values((0..rows as i64).map(|i| b * rows as i64 + i));
                let ones = Int64Array::from_iter_values(std::iter::repeat_n(1, rows));
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(keys), Arc::new(ones)])
                    .unwrap()
            })
            .collect();
        let delay = Duration::from_millis(300);
        let input = delayed(schema, batches, delay);

        let set = ExecutionPlanMetricsSet::new();
        let metrics = PartitionMetrics::new(&set, 0);
        let (compute, slot_time, input_time) = (
            metrics.elapsed_compute.clone(),
            metrics.slot_time.clone(),
            metrics.input_time.clone(),
        );
        let (bailed_early, rows_in) = (metrics.bailed_early.clone(), metrics.rows_in.clone());
        let pruner = Pruner {
            shape,
            k: 1,
            sizing: Sizing::PerRows(128),
            prune: true,
            shared: Arc::new(AtomicI64::new(i64::MIN)),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let started = Instant::now();
        let out = runtime
            .block_on(pruner.run_timed(input, Arc::new(TaskContext::default()), metrics))
            .unwrap();
        let wall = started.elapsed();

        assert_eq!(
            out.iter().map(RecordBatch::num_rows).sum::<usize>(),
            4 * rows
        );
        assert_eq!(bailed_early.value(), 1);
        assert_eq!(rows_in.value(), 4 * rows);
        let compute = Duration::from_nanos(compute.value() as u64);
        // The four waits, two of them during the streaming merge, are about
        // 1.2 s of the wall time and none of the compute time. Measuring
        // from the probe on, as before, counted the two merge-time waits.
        assert!(
            compute + delay * 4 * 4 / 5 <= wall,
            "compute {compute:?} counts input waits (wall {wall:?})"
        );
        assert!(compute > Duration::ZERO);
        // The probe's slotting is the operator's work too.
        assert!(slot_time.value() <= compute.as_nanos() as usize);
        assert!(input_time.value() < wall.as_nanos() as usize);
    }
}
