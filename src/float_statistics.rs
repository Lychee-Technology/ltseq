//! Parquet scans that read no statistics of a float column's values (#291,
//! #355).
//!
//! Parquet writers leave NaN out of a float column's min and max, as the
//! format asks, both in the footer's column chunk statistics and in the page
//! index. DataFusion 55 reads them as bounds on every value of the column:
//!
//! - Pruning skips a file, a row group or a page whose min and max a
//!   predicate cannot match, though its NaN rows match it: `x > 2.0` over
//!   `[1.0, NaN, 2.0, NaN]` read no rows (apache/datafusion#15812). Every
//!   predicate on the scan prunes this way: DataFusion's physical filter
//!   pushdown gives the Parquet source each filter above it for statistics
//!   pruning, whatever the table provider answered for pushdown, and the
//!   dynamic filter of a top-k or a join reaches it too, so
//!   `sort("x", desc=True).head(2)` over a directory skipped a file whose
//!   max was below the two values it had kept, and lost the NaN in it.
//! - The file opener reads a column whose file min equals its max, with no
//!   NULL, as that one value: each NaN of `[20.0, NaN, 20.0]` read as `20.0`
//!   (#355, apache/datafusion#26490).
//!
//! A bloom filter skips a row group that holds no value with the bits of
//! the one compared, while `=` takes `-0.0` to equal `0.0`: `x == r.z` on a
//! derived `z = -0.0` reaches the scan as `x = -0.0`, and skipped a row
//! group whose zero is `0.0`. Lowering compares a zero literal as a range
//! (`literals::float_equality`), which no bloom filter answers, but a value
//! it sees only as a column folds into a zero later. And §18 makes every
//! NaN equal to every other, which `=` does not yet (#290).
//!
//! [`DropFloatStatistics`] takes all of this out of what a Parquet scan reads
//! about a float column: its min and max, from the file statistics the table
//! listed and from the row-group statistics and page index of each footer,
//! and its bloom filters. Nothing then prunes by a float column, and the
//! opener reads its values from the file; every other column prunes as
//! before. Null counts stay, since NaN is not NULL.
//!
//! Remove the rule with the DataFusion upgrade whose scans stop reading
//! float statistics as bounds on NaN (apache/datafusion#15812 and
//! apache/datafusion#26490), once #291's and #355's tests pass without it,
//! and check bloom filters against how `=` then compares floats.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::config::ConfigOptions;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, Statistics};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, ParquetFileReaderFactory, ParquetSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::basic::{LogicalType, Type as PhysicalType};
use datafusion::parquet::errors::Result as ParquetResult;
use datafusion::parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData};
use datafusion::parquet::file::page_index::column_index::ColumnIndexMetaData;
use datafusion::parquet::file::statistics::Statistics as ChunkStatistics;
use datafusion::parquet::schema::types::ColumnDescriptor;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use futures_util::future::{BoxFuture, FutureExt};

/// Takes the min and max of every float column out of each Parquet scan's
/// file statistics, and has the scan read each footer without that column's
/// min, max and bloom filters ([`without_float_statistics`]).
///
/// It runs before DataFusion's physical rules, so the rules that read a
/// scan's statistics, and the orderings a scan of several files per
/// partition proves from them, see the statistics the scan reads.
#[derive(Debug)]
pub(crate) struct DropFloatStatistics;

impl PhysicalOptimizerRule for DropFloatStatistics {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some((config, source)) = node
                .downcast_ref::<DataSourceExec>()
                .and_then(|scan| scan.downcast_to_file_source::<ParquetSource>())
            else {
                return Ok(Transformed::no(node));
            };
            let floats = float_columns(config.file_source.table_schema().table_schema());
            if floats.is_empty() {
                return Ok(Transformed::no(node));
            }
            let file_groups = config
                .file_groups
                .iter()
                .map(|group| group_without_bounds(group, &floats))
                .collect();
            let config = FileScanConfigBuilder::from(config.clone())
                .with_source(Arc::new(reading_no_float_statistics(source)))
                .with_file_groups(file_groups)
                .with_statistics(without_bounds(config.statistics(), &floats))
                .build();
            Ok(Transformed::yes(
                DataSourceExec::from_data_source(config) as Arc<dyn ExecutionPlan>
            ))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "ltseq_drop_float_statistics"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// The indices of `schema`'s float columns, which index a scan's statistics.
fn float_columns(schema: &Schema) -> Vec<usize> {
    (schema.fields().iter().enumerate())
        .filter(|(_, field)| is_float(field.data_type()))
        .map(|(i, _)| i)
        .collect()
}

fn is_float(data_type: &DataType) -> bool {
    match data_type {
        DataType::Dictionary(_, values) => is_float(values),
        other => other.is_floating(),
    }
}

/// `statistics` with no min or max for the columns `floats` names.
fn without_bounds(mut statistics: Statistics, floats: &[usize]) -> Statistics {
    for &i in floats {
        if let Some(column) = statistics.column_statistics.get_mut(i) {
            column.min_value = Precision::Absent;
            column.max_value = Precision::Absent;
        }
    }
    statistics
}

/// `group` with no float min or max in its statistics or its files'.
fn group_without_bounds(group: &FileGroup, floats: &[usize]) -> FileGroup {
    let files = group
        .iter()
        .map(|file| {
            let mut file = file.clone();
            file.statistics = (file.statistics.as_deref())
                .map(|statistics| Arc::new(without_bounds(statistics.clone(), floats)));
            file
        })
        .collect();
    let without = FileGroup::new(files);
    match group.file_statistics(None) {
        Some(statistics) => {
            without.with_statistics(Arc::new(without_bounds(statistics.clone(), floats)))
        }
        None => without,
    }
}

/// `source` reading each footer without float statistics.
///
/// DataFusion's Parquet format gives every scan it plans a reader factory,
/// which is wrapped. A scan planned without one would open files with a
/// default factory made from an object store this rule cannot reach, so it
/// prunes by no row-group statistics, page index or bloom filter instead.
fn reading_no_float_statistics(source: &ParquetSource) -> ParquetSource {
    match source.parquet_file_reader_factory() {
        Some(factory) => source
            .clone()
            .with_parquet_file_reader_factory(Arc::new(NoFloatStatistics(Arc::clone(factory)))),
        None => {
            let mut options = source.table_parquet_options().clone();
            options.global.pruning = false;
            options.global.enable_page_index = false;
            options.global.bloom_filter_on_read = false;
            source.clone().with_table_parquet_options(options)
        }
    }
}

/// Opens each file with the reader of the factory it wraps, which then
/// returns the file's footer without float statistics.
#[derive(Debug)]
struct NoFloatStatistics(Arc<dyn ParquetFileReaderFactory>);

impl ParquetFileReaderFactory for NoFloatStatistics {
    fn create_reader(
        &self,
        partition_index: usize,
        partitioned_file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        let reader = self.0.create_reader(
            partition_index,
            partitioned_file,
            metadata_size_hint,
            metrics,
        )?;
        Ok(Box::new(NoFloatStatisticsReader(reader)))
    }
}

struct NoFloatStatisticsReader(Box<dyn AsyncFileReader + Send>);

impl AsyncFileReader for NoFloatStatisticsReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        self.0.get_bytes(range)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        self.0.get_byte_ranges(ranges)
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        (self.0.get_metadata(options))
            .map(|metadata| metadata.and_then(without_float_statistics))
            .boxed()
    }
}

/// `metadata` without what it says about its float columns' values: each
/// such column chunk keeps its statistics but their min and max, and has no
/// column index, the page index's min and max, whether loaded already or
/// left for the scan to load, and no bloom filter. Other columns are as they
/// were.
fn without_float_statistics(metadata: Arc<ParquetMetaData>) -> ParquetResult<Arc<ParquetMetaData>> {
    let floats: Vec<bool> = (metadata.file_metadata().schema_descr().columns().iter())
        .map(|column| is_float_leaf(column))
        .collect();
    if !floats.contains(&true) {
        return Ok(metadata);
    }
    let mut builder = Arc::unwrap_or_clone(metadata).into_builder();
    let row_groups = (builder.take_row_groups().into_iter())
        .map(|row_group| {
            let mut row_group = row_group.into_builder();
            let columns = (row_group.take_columns().into_iter().zip(&floats))
                .map(|(column, &float)| {
                    if float {
                        chunk_without_statistics(column)
                    } else {
                        Ok(column)
                    }
                })
                .collect::<ParquetResult<_>>()?;
            row_group.set_column_metadata(columns).build()
        })
        .collect::<ParquetResult<_>>()?;
    let column_index = builder.take_column_index().map(|row_groups| {
        (row_groups.into_iter())
            .map(|indexes| {
                (indexes.into_iter().zip(&floats))
                    .map(|(index, &float)| {
                        if float {
                            ColumnIndexMetaData::NONE
                        } else {
                            index
                        }
                    })
                    .collect()
            })
            .collect()
    });
    Ok(Arc::new(
        builder
            .set_row_groups(row_groups)
            .set_column_index(column_index)
            .build(),
    ))
}

/// A float leaf: `float32` and `float64`, and `float16`, which Parquet
/// stores as two-byte fixed-length binary.
fn is_float_leaf(column: &ColumnDescriptor) -> bool {
    matches!(
        column.physical_type(),
        PhysicalType::FLOAT | PhysicalType::DOUBLE
    ) || matches!(column.logical_type_ref(), Some(LogicalType::Float16))
}

fn chunk_without_statistics(column: ColumnChunkMetaData) -> ParquetResult<ColumnChunkMetaData> {
    let statistics = column.statistics().and_then(without_min_max);
    let chunk = (column.into_builder().clear_statistics())
        .set_column_index_offset(None)
        .set_column_index_length(None)
        .set_bloom_filter_offset(None)
        .set_bloom_filter_length(None);
    match statistics {
        Some(statistics) => chunk.set_statistics(statistics),
        None => chunk,
    }
    .build()
}

/// A float chunk's `statistics` without their min and max, or none for any
/// other kind of statistics, which a float leaf does not have.
fn without_min_max(statistics: &ChunkStatistics) -> Option<ChunkStatistics> {
    let distinct = statistics.distinct_count_opt();
    let nulls = statistics.null_count_opt();
    Some(match statistics {
        ChunkStatistics::Float(_) => ChunkStatistics::float(None, None, distinct, nulls, false),
        ChunkStatistics::Double(_) => ChunkStatistics::double(None, None, distinct, nulls, false),
        ChunkStatistics::FixedLenByteArray(_) => {
            ChunkStatistics::fixed_len_byte_array(None, None, distinct, nulls, false)
        }
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{session_context_with, LayoutOverrides, RUNTIME};
    use datafusion::arrow::array::{AsArray, Float32Array, Float64Array, Int64Array, RecordBatch};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::Int64Type;
    use datafusion::datasource::physical_plan::FileScanConfig;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
    use datafusion::parquet::file::properties::WriterProperties;
    use datafusion::physical_plan::metrics::MetricValue;
    use datafusion::physical_plan::{collect, ExecutionPlanProperties};
    use datafusion::prelude::{col, lit, Expr, ParquetReadOptions, SessionConfig, SessionContext};
    use std::num::NonZeroUsize;

    const NAN: f64 = f64::NAN;

    /// Twelve rows: `i` counts them, and `x` is `1.0` to `10.0` with a NaN
    /// in the second and fourth group of three rows and a NULL in the third.
    /// `f` and `h` are `x` as `float32` and `float16`.
    fn rows() -> RecordBatch {
        let x = Float64Array::from(vec![
            Some(1.0),
            Some(2.0),
            Some(3.0),
            Some(4.0),
            Some(NAN),
            Some(5.0),
            Some(6.0),
            None,
            Some(8.0),
            Some(9.0),
            Some(10.0),
            Some(NAN),
        ]);
        let f = Float32Array::from_iter(x.iter().map(|v| v.map(|v| v as f32)));
        let h = cast(&f, &DataType::Float16).expect("float16");
        RecordBatch::try_from_iter([
            ("i", Arc::new(Int64Array::from_iter_values(0..12)) as _),
            ("x", Arc::new(x) as _),
            ("f", Arc::new(f) as _),
            ("h", h),
        ])
        .expect("rows")
    }

    /// `batch` as a Parquet file of row groups of `rows_per_group` rows, with
    /// statistics, a page index and a bloom filter for every column.
    fn parquet(batch: &RecordBatch, rows_per_group: usize) -> Vec<u8> {
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(rows_per_group))
            .set_bloom_filter_enabled(true)
            .build();
        let mut file = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut file, batch.schema(), Some(props)).expect("writer");
        writer.write(batch).expect("write");
        writer.close().expect("close");
        file
    }

    fn footer(file: Vec<u8>) -> ParquetMetaData {
        ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required)
            .parse_and_finish(&Bytes::from(file))
            .expect("footer")
    }

    #[test]
    fn a_footer_loses_its_float_statistics_and_nothing_else() {
        let before = footer(parquet(&rows(), 3));
        let after = without_float_statistics(Arc::new(before.clone())).expect("rewrite");
        let leaves = before.file_metadata().schema_descr();
        let floats: Vec<bool> = (leaves.columns().iter())
            .map(|leaf| is_float_leaf(leaf))
            .collect();
        assert_eq!(floats, [false, true, true, true]);
        let (index_before, index_after) = (
            before.column_index().unwrap(),
            after.column_index().unwrap(),
        );
        assert_eq!(after.offset_index(), before.offset_index());
        assert_eq!(after.num_row_groups(), 4);
        for (g, (group_before, group_after)) in before
            .row_groups()
            .iter()
            .zip(after.row_groups())
            .enumerate()
        {
            assert_eq!(group_after.num_rows(), group_before.num_rows());
            for (c, (chunk_before, chunk_after)) in group_before
                .columns()
                .iter()
                .zip(group_after.columns())
                .enumerate()
            {
                if !floats[c] {
                    assert_eq!(chunk_after, chunk_before);
                    assert_eq!(index_after[g][c], index_before[g][c]);
                    continue;
                }
                let (stats_before, stats_after) = (
                    chunk_before.statistics().unwrap(),
                    chunk_after.statistics().unwrap(),
                );
                assert!(stats_before.min_bytes_opt().is_some());
                assert_eq!(stats_after.min_bytes_opt(), None);
                assert_eq!(stats_after.max_bytes_opt(), None);
                assert_eq!(stats_after.null_count_opt(), stats_before.null_count_opt());
                assert!(chunk_before.column_index_offset().is_some());
                assert_eq!(chunk_after.column_index_offset(), None);
                assert_eq!(chunk_after.column_index_length(), None);
                assert_eq!(
                    chunk_after.offset_index_offset(),
                    chunk_before.offset_index_offset()
                );
                assert!(chunk_before.bloom_filter_offset().is_some());
                assert_eq!(chunk_after.bloom_filter_offset(), None);
                assert_eq!(chunk_after.bloom_filter_length(), None);
                assert!(!matches!(index_before[g][c], ColumnIndexMetaData::NONE));
                assert!(matches!(index_after[g][c], ColumnIndexMetaData::NONE));
            }
        }
    }

    #[test]
    fn a_footer_without_floats_is_kept() {
        let rows = rows().project(&[0]).expect("i");
        let before = Arc::new(footer(parquet(&rows, 3)));
        let after = without_float_statistics(Arc::clone(&before)).expect("rewrite");
        assert!(Arc::ptr_eq(&after, &before));
    }

    /// A session with one partition, as ltseq builds it or as DataFusion
    /// does with ltseq's scan options.
    fn session(ltseq: bool) -> Arc<SessionContext> {
        if ltseq {
            return session_context_with(LayoutOverrides {
                target_partitions: NonZeroUsize::new(1),
                batch_size: None,
            });
        }
        let mut config = SessionConfig::new().with_target_partitions(1);
        config.options_mut().execution.parquet.pushdown_filters = true;
        Arc::new(SessionContext::new_with_config(config))
    }

    /// The `i` of the rows of the Parquet file at `path` that `filter`
    /// keeps, and the scan's plan after it ran.
    fn scan(
        session: &SessionContext,
        path: &str,
        filter: Expr,
    ) -> (Vec<i64>, Arc<dyn ExecutionPlan>) {
        RUNTIME.block_on(async {
            let df = (session
                .read_parquet(path, ParquetReadOptions::default())
                .await)
                .and_then(|df| df.filter(filter))
                .and_then(|df| df.select_columns(&["i"]))
                .expect("query");
            let plan = df.create_physical_plan().await.expect("plan");
            let batches = collect(Arc::clone(&plan), session.task_ctx())
                .await
                .expect("run");
            let mut kept: Vec<i64> = (batches.iter())
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_primitive::<Int64Type>()
                        .values()
                        .to_vec()
                })
                .collect();
            kept.sort_unstable();
            (kept, plan)
        })
    }

    fn the_scan(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let mut found = None;
        plan.apply(|node| {
            if node.downcast_ref::<DataSourceExec>().is_some() {
                found = Some(Arc::clone(node));
            }
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        })
        .expect("walk");
        found.expect("a scan")
    }

    /// The row groups the scan of `plan` pruned by their statistics.
    fn pruned_row_groups(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let metrics = the_scan(plan).metrics().expect("metrics");
        match metrics.sum_by_name("row_groups_pruned_statistics") {
            Some(MetricValue::PruningMetrics {
                pruning_metrics, ..
            }) => pruning_metrics.pruned(),
            other => panic!("no row-group pruning metric: {other:?}"),
        }
    }

    #[test]
    fn a_float_filter_keeps_nan_rows_and_other_columns_still_prune() {
        let dir = tempfile::tempdir().expect("directory");
        let path = dir.path().join("rows.parquet");
        std::fs::write(&path, parquet(&rows(), 3)).expect("write");
        let path = path.to_str().expect("utf-8");

        // Rows 4 and 11 are NaN, so `x > 5.5` holds for them; the first two
        // row groups' max is at most 5.0.
        let (kept, plan) = scan(&session(true), path, col("x").gt(lit(5.5)));
        assert_eq!(kept, [4, 6, 8, 9, 10, 11]);
        assert_eq!(pruned_row_groups(&plan), 0);
        let (kept, plan) = scan(&session(false), path, col("x").gt(lit(5.5)));
        assert_eq!(
            kept,
            [6, 8, 9, 10, 11],
            "DataFusion prunes the NaN's row group"
        );
        assert_eq!(pruned_row_groups(&plan), 2);

        // A filter on `i` prunes the same row groups with and without the rule.
        for ltseq in [true, false] {
            let (kept, plan) = scan(&session(ltseq), path, col("i").gt(lit(7_i64)));
            assert_eq!(kept, [8, 9, 10, 11]);
            assert_eq!(pruned_row_groups(&plan), 2, "ltseq: {ltseq}");
        }
    }

    #[test]
    fn a_scan_lists_no_float_bounds_and_keeps_the_rest() {
        let dir = tempfile::tempdir().expect("directory");
        let path = dir.path().join("rows.parquet");
        std::fs::write(&path, parquet(&rows(), 3)).expect("write");
        let session = session(true);
        let plan = RUNTIME.block_on(async {
            (session
                .read_parquet(path.to_str().unwrap(), ParquetReadOptions::default())
                .await)
                .expect("read")
                .create_physical_plan()
                .await
                .expect("plan")
        });
        let scan = the_scan(&plan);
        let (config, _) = (scan.downcast_ref::<DataSourceExec>().unwrap())
            .downcast_to_file_source::<ParquetSource>()
            .unwrap();
        let file = &config.file_groups[0].files()[0];
        for (name, statistics) in [
            ("scan", config.statistics()),
            (
                "group",
                config.file_groups[0].file_statistics(None).unwrap().clone(),
            ),
            ("file", file.statistics.as_deref().unwrap().clone()),
        ] {
            let [i, x, f, h] = &statistics.column_statistics[..] else {
                panic!("four columns: {statistics:?}");
            };
            assert_eq!(
                i.min_value,
                Precision::Exact(0_i64.into()),
                "{name}: {statistics:?}"
            );
            assert_eq!(i.max_value, Precision::Exact(11_i64.into()), "{name}");
            for float in [x, f, h] {
                assert_eq!(float.min_value, Precision::Absent, "{name}");
                assert_eq!(float.max_value, Precision::Absent, "{name}");
                assert_eq!(float.null_count, Precision::Exact(1), "{name}");
            }
        }
        assert_eq!(scan.output_partitioning().partition_count(), 1);
    }

    #[test]
    fn a_scan_without_a_reader_factory_prunes_by_no_statistics() {
        let source = ParquetSource::new(rows().schema());
        assert!(source.parquet_file_reader_factory().is_none());
        let config: FileScanConfig =
            FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(source))
                .build();
        let plan = (DropFloatStatistics.optimize(
            DataSourceExec::from_data_source(config),
            &ConfigOptions::default(),
        ))
        .expect("rule");
        let (_, source) = (plan.downcast_ref::<DataSourceExec>().unwrap())
            .downcast_to_file_source::<ParquetSource>()
            .unwrap();
        let options = &source.table_parquet_options().global;
        assert!(!options.pruning);
        assert!(!options.enable_page_index);
        assert!(!options.bloom_filter_on_read);
    }
}
