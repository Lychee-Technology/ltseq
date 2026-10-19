//! m5: `assume_sorted` validated on every row pair, with no pruning below
//! it (contract §9.5, D7; M3).
//!
//! v0.4 trusts the declaration. The contract checks each adjacent pair when
//! its later row is read, across batch, file and partition boundaries, and
//! applies no statistics pruning or pushdown below the declaration, so that
//! no pair goes unchecked. The session turns pruning and pushdown off and
//! keeps each partition of a scan to its own range (`super::session`); this
//! rule wraps each scan that declares an order in [`ValidateSortedExec`],
//! which compares adjacent rows' keys in the row format and has the last
//! partition to finish check the pairs between partitions, which a Parquet
//! scan reads as contiguous ranges in file order.
//!
//! The keys must be among the scan's columns, which they are in the m5
//! pipelines; an implementation would add them. A `count()` that DataFusion
//! answers from metadata never reaches the scan, so m5 times a stream.

use std::fmt;
use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{exec_err, Result};
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    ReplaceChildrenOptions,
};
use futures_util::StreamExt;

/// The error the contract names, as the message prefix a test can match.
const VIOLATION: &str = "OrderViolationError";

/// Wraps each scan that declares an output order in [`ValidateSortedExec`].
#[derive(Debug)]
pub(crate) struct ValidateSorted;

impl PhysicalOptimizerRule for ValidateSorted {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(scan) = node.downcast_ref::<DataSourceExec>() else {
                return Ok(Transformed::no(node));
            };
            // The declared order, not `properties().output_ordering()`: that
            // one is normalized, and drops a key the scan's predicate makes
            // constant (`userid = 17...` leaves `eventtime, watchid`), while
            // the predicate prunes nothing here and every key still varies.
            let Some((config, _)) = scan.downcast_to_file_source::<ParquetSource>() else {
                return Ok(Transformed::no(node));
            };
            let Some(ordering) = config.output_ordering.first() else {
                return Ok(Transformed::no(node));
            };
            let schema = node.schema();
            let mut keys = Vec::new();
            for sort in ordering.iter() {
                let Some(column) = sort.expr.downcast_ref::<Column>() else {
                    return exec_err!("m0 validates orders on columns only, not {}", sort.expr);
                };
                // The ordering names table columns; the scan may project.
                let Ok(index) = schema.index_of(column.name()) else {
                    return exec_err!(
                        "m0 validates only keys the scan reads, not {}",
                        column.name()
                    );
                };
                keys.push((
                    index,
                    SortField::new_with_options(
                        schema.field(index).data_type().clone(),
                        sort.options,
                    ),
                ));
            }
            let partitions = node.properties().partitioning.partition_count();
            Ok(Transformed::yes(Arc::new(ValidateSortedExec {
                input: node,
                keys: Arc::new(keys),
                ends: Arc::new(Mutex::new(Ends::new(partitions))),
            }) as Arc<dyn ExecutionPlan>))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "ltseq_m0_validate_sorted"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// The first and last key of each partition that has finished (`None`
/// inside: the partition had no rows).
#[derive(Debug)]
struct Ends {
    partitions: Vec<Option<Option<(OwnedRow, OwnedRow)>>>,
}

impl Ends {
    fn new(partitions: usize) -> Self {
        Ends {
            partitions: vec![None; partitions],
        }
    }

    /// Record a finished partition; once all have, check each pair of
    /// neighbouring non-empty partitions.
    fn finish(&mut self, partition: usize, ends: Option<(OwnedRow, OwnedRow)>) -> Result<()> {
        self.partitions[partition] = Some(ends);
        if self.partitions.iter().any(Option::is_none) {
            return Ok(());
        }
        let mut previous: Option<(usize, &OwnedRow)> = None;
        for (index, ends) in self.partitions.iter().enumerate() {
            let Some(Some((first, last))) = ends else {
                continue;
            };
            if let Some((before, last_before)) = previous {
                if first.row() < last_before.row() {
                    return exec_err!(
                        "{VIOLATION}: the first row of partition {index} sorts before the last row of partition {before}"
                    );
                }
            }
            previous = Some((index, last));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct ValidateSortedExec {
    input: Arc<dyn ExecutionPlan>,
    keys: Arc<Vec<(usize, SortField)>>,
    ends: Arc<Mutex<Ends>>,
}

impl DisplayAs for ValidateSortedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let keys: Vec<String> = self.keys.iter().map(|(i, _)| i.to_string()).collect();
        write!(f, "ValidateSortedExec: keys=[{}]", keys.join(", "))
    }
}

/// Checks the pairs of one partition's stream.
struct Checker {
    converter: RowConverter,
    keys: Arc<Vec<(usize, SortField)>>,
    first: Option<OwnedRow>,
    last: Option<OwnedRow>,
    rows_seen: usize,
}

impl Checker {
    fn check(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let columns: Vec<_> = self
            .keys
            .iter()
            .map(|(i, _)| Arc::clone(batch.column(*i)))
            .collect();
        let rows = self.converter.convert_columns(&columns)?;
        if let Some(last) = &self.last {
            if rows.row(0) < last.row() {
                return exec_err!(
                    "{VIOLATION}: row {} sorts before the row read before it",
                    self.rows_seen
                );
            }
        }
        for i in 1..rows.num_rows() {
            if rows.row(i) < rows.row(i - 1) {
                return exec_err!(
                    "{VIOLATION}: row {} sorts before the row read before it",
                    self.rows_seen + i
                );
            }
        }
        if self.first.is_none() {
            self.first = Some(rows.row(0).owned());
        }
        self.last = Some(rows.row(rows.num_rows() - 1).owned());
        self.rows_seen += rows.num_rows();
        Ok(())
    }
}

impl ExecutionPlan for ValidateSortedExec {
    fn name(&self) -> &str {
        "ValidateSortedExec"
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
        let input = children.swap_remove(0);
        let partitions = input.properties().partitioning.partition_count();
        Ok(Arc::new(ValidateSortedExec {
            input,
            keys: Arc::clone(&self.keys),
            ends: Arc::new(Mutex::new(Ends::new(partitions))),
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
        let schema: SchemaRef = stream.schema();
        let converter = RowConverter::new(self.keys.iter().map(|(_, f)| f.clone()).collect())?;
        let checker = Checker {
            converter,
            keys: Arc::clone(&self.keys),
            first: None,
            last: None,
            rows_seen: 0,
        };
        let ends = Arc::clone(&self.ends);
        // Each batch is checked as it passes; the end of the stream records
        // the partition's first and last keys.
        let checked =
            futures_util::stream::unfold((stream, Some(checker)), move |(mut stream, checker)| {
                let ends = Arc::clone(&ends);
                async move {
                    let mut checker = checker?;
                    match stream.next().await {
                        Some(Ok(batch)) => match checker.check(&batch) {
                            Ok(()) => Some((Ok(batch), (stream, Some(checker)))),
                            Err(e) => Some((Err(e), (stream, None))),
                        },
                        Some(Err(e)) => Some((Err(e), (stream, None))),
                        None => {
                            let pair = checker.first.take().zip(checker.last.take());
                            let finished = ends
                                .lock()
                                .expect("partition ends lock")
                                .finish(partition, pair);
                            match finished {
                                Ok(()) => None,
                                Err(e) => Some((Err(e), (stream, None))),
                            }
                        }
                    }
                }
            });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, checked)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::DataType;

    fn checker() -> Checker {
        let field = SortField::new_with_options(DataType::Int64, SortOptions::default());
        Checker {
            converter: RowConverter::new(vec![field.clone()]).unwrap(),
            keys: Arc::new(vec![(0, field)]),
            first: None,
            last: None,
            rows_seen: 0,
        }
    }

    fn batch(values: Vec<i64>) -> RecordBatch {
        RecordBatch::try_from_iter([("x", Arc::new(Int64Array::from(values)) as _)]).unwrap()
    }

    #[test]
    fn violations_inside_and_across_batches_raise() {
        let mut c = checker();
        c.check(&batch(vec![1, 2, 2])).unwrap();
        c.check(&batch(vec![3, 4])).unwrap();
        let err = c.check(&batch(vec![3])).unwrap_err();
        assert!(err.to_string().contains("row 5"), "{err}");
        let mut c = checker();
        assert!(c
            .check(&batch(vec![1, 3, 2]))
            .unwrap_err()
            .to_string()
            .contains(VIOLATION));
    }

    #[test]
    fn partitions_are_checked_once_all_finish() {
        let mut c = checker();
        c.check(&batch(vec![5, 7])).unwrap();
        let late = c.first.clone().zip(c.last.clone());
        let mut c = checker();
        c.check(&batch(vec![1, 6])).unwrap();
        let early = c.first.clone().zip(c.last.clone());
        let mut ends = Ends::new(3);
        ends.finish(2, late.clone()).unwrap();
        ends.finish(1, None).unwrap();
        // Partition 0 ends at 6, partition 2 starts at 5.
        assert!(ends.finish(0, early.clone()).is_err());
        let mut ends = Ends::new(2);
        ends.finish(1, early).unwrap();
        assert!(ends.finish(0, late).is_err());
    }

    /// `Ends::finish` pairs partition `i` with partition `i + 1`, which holds
    /// only while each partition reads its own range of the file.
    #[test]
    fn validated_scans_keep_each_partition_to_its_own_range() {
        use super::super::{session_with, Prototypes, Sorted};
        use crate::engine::RUNTIME;
        use datafusion::execution::config::SessionConfig;
        use datafusion::prelude::{col, lit, ParquetReadOptions};
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;

        let path = std::env::temp_dir().join(format!(
            "ltseq_m0_validate_ranges_{}.parquet",
            std::process::id()
        ));
        let rows = batch((0..1024).collect());
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(64))
            .build();
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, rows.schema(), Some(props)).unwrap();
        writer.write(&rows).unwrap();
        writer.close().unwrap();
        let path = path.to_str().unwrap().to_string();

        // Whether an idle partition of the plan's one scan may read a
        // sibling's ranges, as DataFusion decides when it executes the scan.
        let shares_work = |validate: Sorted, limit: Option<usize>| -> bool {
            let mut config = SessionConfig::new().with_target_partitions(4);
            config.options_mut().optimizer.repartition_file_min_size = 0;
            let p = Prototypes {
                validate,
                ..Prototypes::default()
            };
            let ctx = session_with(p, config);
            let plan = RUNTIME
                .block_on(async {
                    let order = vec![vec![col("x").sort(true, false)]];
                    let options = ParquetReadOptions::default().file_sort_order(order);
                    // A filter, as in m5, so that the scan is split.
                    let df = ctx.read_parquet(path.as_str(), options).await?;
                    let df = df.filter(col("x").gt_eq(lit(10)))?;
                    let df = match limit {
                        Some(n) => df.limit(0, Some(n))?,
                        None => df,
                    };
                    df.create_physical_plan().await
                })
                .unwrap();
            let options = ctx.state().config().options().clone();
            let mut checked = false;
            let mut scans = Vec::new();
            plan.apply(|node| {
                checked |= node.is::<ValidateSortedExec>();
                if let Some(scan) = node.downcast_ref::<DataSourceExec>() {
                    assert!(node.properties().partitioning.partition_count() > 1);
                    scans.push(scan.data_source().create_sibling_state(&options).is_some());
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap();
            assert_eq!(checked, validate == Sorted::Validate, "{plan:?}");
            assert_eq!(scans.len(), 1, "{plan:?}");
            scans[0]
        };

        let cases = [Sorted::Off, Sorted::PruneOff, Sorted::Validate]
            .into_iter()
            .flat_map(|validate| [(validate, None), (validate, Some(5))]);
        let shared: Vec<_> = cases
            .map(|(validate, limit)| (validate, limit, shares_work(validate, limit)))
            .collect();
        let _ = std::fs::remove_file(&path);
        for (validate, limit, shares) in shared {
            // Today a declared order keeps the scan's partitions to their
            // ranges, and a limit above the scan lifts that.
            let today_with_limit = validate == Sorted::Off && limit.is_some();
            assert_eq!(shares, today_with_limit, "{validate:?}, limit {limit:?}");
        }
    }
}
