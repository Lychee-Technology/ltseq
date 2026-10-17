//! m3: file-then-row order restored after a parallel scan (M1, #148).
//!
//! DataFusion splits a Parquet scan into `target_partitions` contiguous byte
//! ranges and interleaves their batches in arrival order. Restoring file
//! order while memory stays bounded (contract §21.1) needs a merge that
//! holds only a bounded number of batches ahead of the consumer. Over the
//! contiguous ranges such a merge serializes the scan: while range 0 drains,
//! every later range stops after `buffer` batches. `Order::Concat` measures
//! that. `Order::Interleaved` splits the scan into one unit per row group
//! (`SplitScanByRowGroup`), runs `window` consecutive units at a time and
//! emits them in unit order (`OrderedMergeExec`), so all workers stay busy
//! and at most `window * buffer` batches wait.
//!
//! `SlowRegion` delays the batches of the first `1 / target_partitions` of
//! the scanned data, to price the time to first row when one part of the
//! input is slow.

use std::collections::VecDeque;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::config::ConfigOptions;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{internal_err, DataFusionError, Result};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, ReplaceChildrenOptions,
};
use futures_util::StreamExt;
use tokio::sync::mpsc;

use super::Order;

/// Replace each Parquet scan's file groups with one group per row group, in
/// file order (the order DataFusion listed the files in, then row groups).
#[derive(Debug)]
pub(crate) struct SplitScanByRowGroup;

impl PhysicalOptimizerRule for SplitScanByRowGroup {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(scan) = node.downcast_ref::<DataSourceExec>() else {
                return Ok(Transformed::no(node));
            };
            let Some((config, _)) = scan.downcast_to_file_source::<ParquetSource>() else {
                return Ok(Transformed::no(node));
            };
            let units = row_group_units(config.file_groups.iter().flat_map(|g| g.iter()))?;
            let config = FileScanConfigBuilder::from(config.clone())
                .with_file_groups(units)
                .with_preserve_order(true)
                .build();
            Ok(Transformed::yes(
                DataSourceExec::from_data_source(config) as Arc<dyn ExecutionPlan>
            ))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "ltseq_m0_split_scan_by_row_group"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// One file group per row group of `files`, each a one-byte range at the row
/// group's first page: DataFusion's Parquet opener scans a row group exactly
/// when that offset lies in the range (`prune_by_range`).
fn row_group_units<'a>(files: impl Iterator<Item = &'a PartitionedFile>) -> Result<Vec<FileGroup>> {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let mut seen = std::collections::HashSet::new();
    let mut units = Vec::new();
    for file in files {
        // DataFusion may have split one file across several groups already.
        let path = format!("/{}", file.object_meta.location);
        if !seen.insert(path.clone()) {
            continue;
        }
        let reader = std::fs::File::open(&path)
            .map_err(|e| DataFusionError::External(Box::new(e)))
            .and_then(|f| {
                SerializedFileReader::new(f).map_err(|e| DataFusionError::External(Box::new(e)))
            })?;
        for rg in reader.metadata().row_groups() {
            let col = rg.column(0);
            let offset = col
                .dictionary_page_offset()
                .unwrap_or_else(|| col.data_page_offset());
            units.push(FileGroup::new(vec![file
                .clone()
                .with_range(offset, offset + 1)]));
        }
    }
    Ok(units)
}

/// Put an [`OrderedMergeExec`] on top of a multi-partition plan whose
/// partitions each preserve their input order down to a scan; leave any
/// other plan as it is.
#[derive(Debug)]
pub(crate) struct OrderedOutput {
    pub(crate) order: Order,
    pub(crate) window: Option<NonZeroUsize>,
    pub(crate) buffer: NonZeroUsize,
}

impl PhysicalOptimizerRule for OrderedOutput {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = match plan.downcast_ref::<CoalescePartitionsExec>() {
            Some(coalesce) => Arc::clone(coalesce.input()),
            None => Arc::clone(&plan),
        };
        let partitions = input.output_partitioning().partition_count();
        if partitions <= 1 || !preserves_scan_order(&input) {
            return Ok(plan);
        }
        let window = match self.order {
            // Every contiguous range starts at once, as today's scan does.
            Order::Concat => partitions,
            Order::Interleaved => self
                .window
                .map_or(config.execution.target_partitions, NonZeroUsize::get),
            Order::Off => return Ok(plan),
        };
        Ok(Arc::new(OrderedMergeExec::new(
            input,
            window,
            self.buffer.get(),
        )))
    }

    fn name(&self) -> &str {
        "ltseq_m0_ordered_output"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Whether every node from `plan` down to its leaf keeps its partitions and
/// the order of rows within each, so that concatenating the partitions in
/// index order gives the scan's order.
fn preserves_scan_order(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let mut node = Arc::clone(plan);
    loop {
        let children = node.children();
        match children.as_slice() {
            [] => return node.is::<DataSourceExec>(),
            [child] => {
                let keeps_order = node
                    .maintains_input_order()
                    .first()
                    .copied()
                    .unwrap_or(false);
                let same_partitions = child.output_partitioning().partition_count()
                    == node.output_partitioning().partition_count();
                if !keeps_order || !same_partitions {
                    return false;
                }
                node = Arc::clone(child);
            }
            _ => return false,
        }
    }
}

/// Emits its input's partitions one after another, in index order, running
/// up to `window` of them at a time; each may produce up to `buffer`
/// batches before the consumer reaches it.
#[derive(Debug)]
pub(crate) struct OrderedMergeExec {
    input: Arc<dyn ExecutionPlan>,
    window: usize,
    buffer: usize,
    properties: Arc<PlanProperties>,
}

impl OrderedMergeExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>, window: usize, buffer: usize) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        OrderedMergeExec {
            input,
            window: window.max(1),
            buffer: buffer.max(1),
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for OrderedMergeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "OrderedMergeExec: window={}, buffer={}",
            self.window, self.buffer
        )
    }
}

impl ExecutionPlan for OrderedMergeExec {
    fn name(&self) -> &str {
        "OrderedMergeExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
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
        Ok(Arc::new(OrderedMergeExec::new(
            children.swap_remove(0),
            self.window,
            self.buffer,
        )))
    }

    #[allow(deprecated)]
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(OrderedMergeExec::new(
            children.swap_remove(0),
            self.window,
            self.buffer,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("OrderedMergeExec has one partition, got {partition}");
        }
        let state = MergeState {
            input: Arc::clone(&self.input),
            context,
            partitions: self.input.output_partitioning().partition_count(),
            window: self.window,
            buffer: self.buffer,
            next: 0,
            running: VecDeque::new(),
        };
        let stream = futures_util::stream::unfold(state, |mut state| async move {
            state.fill();
            loop {
                let (_, receiver, _) = state.running.front_mut()?;
                match receiver.recv().await {
                    Some(item) => return Some((item, state)),
                    None => {
                        // The front partition is drained; the next in line
                        // becomes the front and a new one starts.
                        state.running.pop_front();
                        state.fill();
                    }
                }
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

type Running = (usize, mpsc::Receiver<Result<RecordBatch>>, SpawnedTask<()>);

struct MergeState {
    input: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    partitions: usize,
    window: usize,
    buffer: usize,
    next: usize,
    /// Partitions in flight, front first; dropping one aborts its task.
    running: VecDeque<Running>,
}

impl MergeState {
    fn fill(&mut self) {
        while self.running.len() < self.window && self.next < self.partitions {
            let index = self.next;
            self.next += 1;
            let (sender, receiver) = mpsc::channel(self.buffer);
            let input = Arc::clone(&self.input);
            let context = Arc::clone(&self.context);
            let task = SpawnedTask::spawn(async move {
                let mut stream = match input.execute(index, context) {
                    Ok(stream) => stream,
                    Err(e) => {
                        let _ = sender.send(Err(e)).await;
                        return;
                    }
                };
                while let Some(item) = stream.next().await {
                    if sender.send(item).await.is_err() {
                        return;
                    }
                }
            });
            self.running.push_back((index, receiver, task));
        }
    }
}

/// Delay every batch the first `1 / target_partitions` of each Parquet
/// scan's partitions produce by `delay_ms`.
#[derive(Debug)]
pub(crate) struct SlowRegion {
    pub(crate) delay_ms: u64,
}

impl PhysicalOptimizerRule for SlowRegion {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let target = config.execution.target_partitions.max(1);
        plan.transform_up(|node| {
            if !node.is::<DataSourceExec>() {
                return Ok(Transformed::no(node));
            }
            let partitions = node.output_partitioning().partition_count();
            let slow = partitions.div_ceil(target).max(1);
            Ok(Transformed::yes(Arc::new(SlowExec {
                input: node,
                slow_partitions: slow,
                delay: Duration::from_millis(self.delay_ms),
            }) as Arc<dyn ExecutionPlan>))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "ltseq_m0_slow_region"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[derive(Debug)]
struct SlowExec {
    input: Arc<dyn ExecutionPlan>,
    slow_partitions: usize,
    delay: Duration,
}

impl DisplayAs for SlowExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SlowExec: partitions 0..{} delayed {:?} per batch",
            self.slow_partitions, self.delay
        )
    }
}

impl ExecutionPlan for SlowExec {
    fn name(&self) -> &str {
        "SlowExec"
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
        Ok(Arc::new(SlowExec {
            input: children.swap_remove(0),
            slow_partitions: self.slow_partitions,
            delay: self.delay,
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
        let stream = self.input.execute(partition, context)?;
        if partition >= self.slow_partitions {
            return Ok(stream);
        }
        let schema: SchemaRef = stream.schema();
        let delay = self.delay;
        let delayed = stream.then(move |item| async move {
            tokio::time::sleep(delay).await;
            item
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, delayed)))
    }
}
