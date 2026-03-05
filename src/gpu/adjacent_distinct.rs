//! GPU-accelerated adjacent distinct execution plan.
//!
//! `GpuAdjacentDistinctExec` performs O(N) deduplication on already-sorted data
//! by comparing adjacent rows on key columns. When data is pre-sorted by the
//! distinct key columns, this avoids the hash table overhead of the standard
//! `GpuHashAggregateExec` DISTINCT path.
//!
//! ## Algorithm
//!
//! 1. Collect all input batches into a single `RecordBatch`.
//! 2. For each row, compare key columns with the previous row (CPU comparison).
//! 3. Mark rows where any key column differs from the previous row as "keep".
//! 4. Gather kept rows to produce the output batch (CPU gather via `arrow::compute::filter`).
//!
//! The first row is always kept. The comparison is done on CPU because adjacent
//! comparison is inherently serial and very fast in a single pass.
//!
//! ## Sort Order
//!
//! Since the input is required to be sorted and we only remove rows (never
//! reorder), the output preserves the input's sort order (`SortOrderEffect::Preserve`).

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

use super::sort_aware::{SortOrderEffect, SortOrderPropagation};

// ============================================================================
// Configuration
// ============================================================================

/// Describes a key column for adjacent distinct comparison.
#[derive(Debug, Clone)]
pub struct AdjacentDistinctKey {
    /// Column index in the input schema
    pub column_index: usize,
    /// Column name (for display)
    pub column_name: String,
}

/// Configuration for a GPU adjacent distinct operation.
#[derive(Debug, Clone)]
pub struct GpuAdjacentDistinctConfig {
    /// Key columns to compare for distinctness
    pub keys: Vec<AdjacentDistinctKey>,
    /// Whether this is a key-column distinct (true) or all-column distinct (false).
    /// For key-column distinct, non-key columns use FirstValue semantics (take
    /// the value from the first row in each group of duplicates).
    pub is_key_distinct: bool,
}

// ============================================================================
// GpuAdjacentDistinctExec
// ============================================================================

/// GPU-accelerated adjacent distinct execution plan for pre-sorted data.
///
/// Performs O(N) deduplication by comparing adjacent rows on key columns.
/// Requires input to be sorted by the key columns.
///
/// ## Output
///
/// Produces unique rows based on key columns. The output schema is identical
/// to the input schema. For key-column distinct, non-key columns retain the
/// value from the first occurrence of each key combination.
#[derive(Debug)]
pub struct GpuAdjacentDistinctExec {
    /// Configuration
    config: GpuAdjacentDistinctConfig,
    /// The input plan (must be sorted by key columns)
    input: Arc<dyn ExecutionPlan>,
    /// Output schema (same as input)
    output_schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cached plan properties
    cache: PlanProperties,
}

impl GpuAdjacentDistinctExec {
    /// Create a new GpuAdjacentDistinctExec.
    ///
    /// # Arguments
    /// * `config` - Adjacent distinct configuration with key columns
    /// * `input` - Input plan (must be sorted by the key columns)
    /// * `output_schema` - Output schema (same as input)
    pub fn try_new(
        config: GpuAdjacentDistinctConfig,
        input: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
    ) -> Result<Self> {
        let cache = Self::compute_properties(&input, &output_schema);
        // Coalesce multiple partitions into one — adjacent distinct requires
        // a single sorted stream to compare adjacent rows correctly.
        let coalesced_input =
            if input.properties().output_partitioning().partition_count() > 1 {
                Arc::new(
                    datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(
                        input,
                    ),
                ) as Arc<dyn ExecutionPlan>
            } else {
                input
            };
        Ok(Self {
            config,
            input: coalesced_input,
            output_schema,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }

    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: &SchemaRef,
    ) -> PlanProperties {
        use datafusion::physical_expr::EquivalenceProperties;
        use datafusion::physical_plan::Partitioning;

        // Adjacent distinct preserves the input's sort order (we only remove rows).
        // Copy the input's equivalence properties with the output schema.
        let input_eq = input.equivalence_properties();
        let mut eq_props = EquivalenceProperties::new(Arc::clone(schema));
        if let Some(ordering) = input_eq.output_ordering() {
            let _ = eq_props.reorder(ordering.iter().cloned());
        }

        PlanProperties::new(
            eq_props,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        )
    }
}

impl DisplayAs for GpuAdjacentDistinctExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let keys: Vec<&str> = self
            .config
            .keys
            .iter()
            .map(|k| k.column_name.as_str())
            .collect();
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "GpuAdjacentDistinctExec: keys=[{}] [CPU adjacent-dedup]",
                    keys.join(", ")
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "keys=[{}] [CPU adjacent-dedup]",
                    keys.join(", ")
                )
            }
        }
    }
}

impl ExecutionPlan for GpuAdjacentDistinctExec {
    fn name(&self) -> &'static str {
        "GpuAdjacentDistinctExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true] // Adjacent distinct preserves order
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(GpuAdjacentDistinctExec::try_new(
            self.config.clone(),
            children.swap_remove(0),
            Arc::clone(&self.output_schema),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let schema = Arc::clone(&self.output_schema);
        let config = self.config.clone();
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        let output_stream = futures_util::stream::once(async move {
            let timer = metrics.elapsed_compute().timer();

            // Step 1: Collect all input batches
            let batches: Vec<RecordBatch> =
                futures_util::StreamExt::collect::<Vec<_>>(input_stream)
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?;

            if batches.is_empty() {
                timer.done();
                return Ok(RecordBatch::new_empty(schema));
            }

            let input_schema = batches[0].schema();
            let coalesced =
                datafusion::arrow::compute::concat_batches(&input_schema, batches.iter())?;
            let n = coalesced.num_rows();
            if n == 0 {
                timer.done();
                return Ok(RecordBatch::new_empty(schema));
            }

            // Step 2: Adjacent comparison — mark rows to keep
            let result = adjacent_distinct(&coalesced, &config)?;

            timer.done();
            metrics.record_output(result.num_rows());

            if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
                eprintln!(
                    "[GPU] adjacent_distinct: input={} rows, output={} rows, keys=[{}]",
                    n,
                    result.num_rows(),
                    config
                        .keys
                        .iter()
                        .map(|k| k.column_name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }

            Ok(result)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.output_schema),
            output_stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<datafusion::physical_plan::Statistics> {
        // Conservative: we don't know how many duplicates there are
        self.input.partition_statistics(None)
    }
}

impl SortOrderPropagation for GpuAdjacentDistinctExec {
    fn sort_order_effect(&self) -> SortOrderEffect {
        SortOrderEffect::Preserve
    }
}

// ============================================================================
// Adjacent Distinct Algorithm
// ============================================================================

/// Perform adjacent distinct on a record batch.
///
/// Compares each row with the previous row on all key columns. A row is "kept"
/// if any key column value differs from the previous row. The first row is
/// always kept.
///
/// Uses `arrow::compute::filter` for the final gather step, which handles
/// all Arrow data types (strings, timestamps, etc.) without type-specific code.
fn adjacent_distinct(
    batch: &RecordBatch,
    config: &GpuAdjacentDistinctConfig,
) -> Result<RecordBatch> {
    let n = batch.num_rows();

    // Build boolean mask: true = keep this row
    let mut keep = vec![false; n];
    keep[0] = true; // Always keep the first row

    // Get key columns
    let key_columns: Vec<&dyn Array> = config
        .keys
        .iter()
        .map(|k| batch.column(k.column_index).as_ref())
        .collect();

    // Compare adjacent rows — a row is "new" if ANY key column differs
    for i in 1..n {
        let mut differs = false;
        for col in &key_columns {
            // Use Arrow's comparison: compare row i with row i-1
            // Two values are "same" if neither is null and they're equal,
            // or if both are null.
            let prev_null = col.is_null(i - 1);
            let curr_null = col.is_null(i);

            if prev_null != curr_null {
                // One is null, the other isn't → different
                differs = true;
                break;
            }
            if prev_null && curr_null {
                // Both null → same, continue to next column
                continue;
            }
            // Both non-null: use the underlying data comparison
            // Arrow's Array doesn't have a direct row-level equality method,
            // so we use the generic `ScalarValue` comparison.
            use datafusion::common::ScalarValue;
            let prev_val = ScalarValue::try_from_array(*col, i - 1)
                .map_err(|e| DataFusionError::Execution(format!("ScalarValue extraction: {}", e)))?;
            let curr_val = ScalarValue::try_from_array(*col, i)
                .map_err(|e| DataFusionError::Execution(format!("ScalarValue extraction: {}", e)))?;
            if prev_val != curr_val {
                differs = true;
                break;
            }
        }
        keep[i] = differs;
    }

    // Apply filter mask using arrow::compute::filter
    let mask = BooleanArray::from(keep);
    let filtered_columns: Vec<_> = batch
        .columns()
        .iter()
        .map(|col| {
            datafusion::arrow::compute::filter(col.as_ref(), &mask)
                .map_err(|e| DataFusionError::Execution(format!("Arrow filter: {}", e)))
        })
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(batch.schema(), filtered_columns)
        .map_err(|e| DataFusionError::Execution(format!("Failed to build output batch: {}", e)))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Helper to build a RecordBatch from column data.
    fn make_batch(
        columns: Vec<(&str, Arc<dyn Array>)>,
    ) -> RecordBatch {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<Arc<dyn Array>> = columns.into_iter().map(|(_, arr)| arr).collect();
        RecordBatch::try_new(schema, arrays).unwrap()
    }

    fn key(index: usize, name: &str) -> AdjacentDistinctKey {
        AdjacentDistinctKey {
            column_index: index,
            column_name: name.to_string(),
        }
    }

    #[test]
    fn test_adjacent_distinct_single_key_ints() {
        // Sorted input: [1, 1, 2, 2, 3] → expect [1, 2, 3]
        let batch = make_batch(vec![
            ("id", Arc::new(Int32Array::from(vec![1, 1, 2, 2, 3])) as Arc<dyn Array>),
            ("val", Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0])) as Arc<dyn Array>),
        ]);
        let config = GpuAdjacentDistinctConfig {
            keys: vec![key(0, "id")],
            is_key_distinct: true,
        };
        let result = adjacent_distinct(&batch, &config).unwrap();
        assert_eq!(result.num_rows(), 3);
        let ids: Vec<i32> = result.column(0).as_any().downcast_ref::<Int32Array>().unwrap().values().to_vec();
        assert_eq!(ids, vec![1, 2, 3]);
        // FirstValue semantics: takes the first row's value for each group
        let vals: Vec<f64> = result.column(1).as_any().downcast_ref::<Float64Array>().unwrap().values().to_vec();
        assert_eq!(vals, vec![10.0, 30.0, 50.0]);
    }

    #[test]
    fn test_adjacent_distinct_all_unique() {
        // All rows are unique → no dedup
        let batch = make_batch(vec![
            ("id", Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as Arc<dyn Array>),
        ]);
        let config = GpuAdjacentDistinctConfig {
            keys: vec![key(0, "id")],
            is_key_distinct: false,
        };
        let result = adjacent_distinct(&batch, &config).unwrap();
        assert_eq!(result.num_rows(), 4);
    }

    #[test]
    fn test_adjacent_distinct_all_same() {
        // All rows identical → collapse to 1
        let batch = make_batch(vec![
            ("id", Arc::new(Int32Array::from(vec![5, 5, 5, 5, 5])) as Arc<dyn Array>),
            ("val", Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])) as Arc<dyn Array>),
        ]);
        let config = GpuAdjacentDistinctConfig {
            keys: vec![key(0, "id")],
            is_key_distinct: true,
        };
        let result = adjacent_distinct(&batch, &config).unwrap();
        assert_eq!(result.num_rows(), 1);
        let ids: Vec<i32> = result.column(0).as_any().downcast_ref::<Int32Array>().unwrap().values().to_vec();
        assert_eq!(ids, vec![5]);
        let vals: Vec<i32> = result.column(1).as_any().downcast_ref::<Int32Array>().unwrap().values().to_vec();
        assert_eq!(vals, vec![10]); // FirstValue: first row
    }

    #[test]
    fn test_adjacent_distinct_single_row() {
        let batch = make_batch(vec![
            ("id", Arc::new(Int32Array::from(vec![42])) as Arc<dyn Array>),
        ]);
        let config = GpuAdjacentDistinctConfig {
            keys: vec![key(0, "id")],
            is_key_distinct: false,
        };
        let result = adjacent_distinct(&batch, &config).unwrap();
        assert_eq!(result.num_rows(), 1);
    }

    #[test]
    fn test_adjacent_distinct_multi_key() {
        // Two key columns: (region, category)
        // Sorted by region then category
        let batch = make_batch(vec![
            ("region", Arc::new(StringArray::from(vec!["A", "A", "A", "B", "B"])) as Arc<dyn Array>),
            ("category", Arc::new(StringArray::from(vec!["x", "x", "y", "x", "y"])) as Arc<dyn Array>),
            ("value", Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])) as Arc<dyn Array>),
        ]);
        let config = GpuAdjacentDistinctConfig {
            keys: vec![key(0, "region"), key(1, "category")],
            is_key_distinct: true,
        };
        let result = adjacent_distinct(&batch, &config).unwrap();
        assert_eq!(result.num_rows(), 4); // (A,x), (A,y), (B,x), (B,y)
        let regions: Vec<&str> = result.column(0).as_any().downcast_ref::<StringArray>().unwrap().iter().map(|v| v.unwrap()).collect();
        assert_eq!(regions, vec!["A", "A", "B", "B"]);
        let categories: Vec<&str> = result.column(1).as_any().downcast_ref::<StringArray>().unwrap().iter().map(|v| v.unwrap()).collect();
        assert_eq!(categories, vec!["x", "y", "x", "y"]);
        let values: Vec<i32> = result.column(2).as_any().downcast_ref::<Int32Array>().unwrap().values().to_vec();
        assert_eq!(values, vec![10, 30, 40, 50]); // FirstValue from each group
    }

    #[test]
    fn test_adjacent_distinct_with_nulls() {
        // Nulls in key column: [NULL, NULL, 1, 1, NULL] → [NULL, 1, NULL]
        let batch = make_batch(vec![
            ("id", Arc::new(Int32Array::from(vec![None, None, Some(1), Some(1), None])) as Arc<dyn Array>),
            ("val", Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])) as Arc<dyn Array>),
        ]);
        let config = GpuAdjacentDistinctConfig {
            keys: vec![key(0, "id")],
            is_key_distinct: true,
        };
        let result = adjacent_distinct(&batch, &config).unwrap();
        assert_eq!(result.num_rows(), 3);
        let ids = result.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(ids.is_null(0));   // NULL group
        assert_eq!(ids.value(1), 1); // 1 group
        assert!(ids.is_null(2));   // NULL group again (not adjacent to first NULL)
    }

    #[test]
    fn test_adjacent_distinct_string_keys() {
        let batch = make_batch(vec![
            ("name", Arc::new(StringArray::from(vec!["alice", "alice", "bob", "carol", "carol"])) as Arc<dyn Array>),
            ("score", Arc::new(Int32Array::from(vec![90, 85, 70, 60, 95])) as Arc<dyn Array>),
        ]);
        let config = GpuAdjacentDistinctConfig {
            keys: vec![key(0, "name")],
            is_key_distinct: true,
        };
        let result = adjacent_distinct(&batch, &config).unwrap();
        assert_eq!(result.num_rows(), 3);
        let names: Vec<&str> = result.column(0).as_any().downcast_ref::<StringArray>().unwrap().iter().map(|v| v.unwrap()).collect();
        assert_eq!(names, vec!["alice", "bob", "carol"]);
        let scores: Vec<i32> = result.column(1).as_any().downcast_ref::<Int32Array>().unwrap().values().to_vec();
        assert_eq!(scores, vec![90, 70, 60]); // FirstValue
    }

    #[test]
    fn test_adjacent_distinct_all_columns() {
        // All-column distinct (is_key_distinct=false): every column is a key
        let batch = make_batch(vec![
            ("a", Arc::new(Int32Array::from(vec![1, 1, 2, 2])) as Arc<dyn Array>),
            ("b", Arc::new(Int32Array::from(vec![10, 10, 20, 30])) as Arc<dyn Array>),
        ]);
        let config = GpuAdjacentDistinctConfig {
            keys: vec![key(0, "a"), key(1, "b")],
            is_key_distinct: false,
        };
        let result = adjacent_distinct(&batch, &config).unwrap();
        assert_eq!(result.num_rows(), 3); // (1,10), (2,20), (2,30)
    }

    #[test]
    fn test_gpu_adjacent_distinct_exec_creation() {
        // Test that GpuAdjacentDistinctExec can be created and has correct properties
        use datafusion::physical_plan::memory::MemoryExec;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Float64, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2, 3])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0])),
            ],
        )
        .unwrap();

        let input = Arc::new(MemoryExec::try_new(&[vec![batch]], Arc::clone(&schema), None).unwrap());
        let config = GpuAdjacentDistinctConfig {
            keys: vec![key(0, "id")],
            is_key_distinct: true,
        };

        let exec = GpuAdjacentDistinctExec::try_new(config, input, schema).unwrap();
        assert_eq!(exec.name(), "GpuAdjacentDistinctExec");
        assert_eq!(exec.schema().fields().len(), 2);
        assert_eq!(exec.children().len(), 1);
    }

    #[tokio::test]
    async fn test_gpu_adjacent_distinct_exec_execution() {
        // End-to-end test: create exec node, execute it, check results
        use datafusion::physical_plan::memory::MemoryExec;
        use datafusion::execution::TaskContext;
        use futures_util::StreamExt;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Float64, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2, 2, 3])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0])),
            ],
        )
        .unwrap();

        let input = Arc::new(
            MemoryExec::try_new(&[vec![batch]], Arc::clone(&schema), None).unwrap(),
        );
        let config = GpuAdjacentDistinctConfig {
            keys: vec![key(0, "id")],
            is_key_distinct: true,
        };
        let exec = Arc::new(
            GpuAdjacentDistinctExec::try_new(config, input, Arc::clone(&schema)).unwrap(),
        );

        let ctx = Arc::new(TaskContext::default());
        let mut stream = exec.execute(0, ctx).unwrap();

        let mut results = Vec::new();
        while let Some(batch) = stream.next().await {
            results.push(batch.unwrap());
        }

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(result.num_rows(), 3);

        let ids: Vec<i32> = result.column(0).as_any().downcast_ref::<Int32Array>().unwrap().values().to_vec();
        assert_eq!(ids, vec![1, 2, 3]);
        let vals: Vec<f64> = result.column(1).as_any().downcast_ref::<Float64Array>().unwrap().values().to_vec();
        assert_eq!(vals, vec![10.0, 30.0, 50.0]);
    }

    #[test]
    fn test_sort_order_propagation() {
        let exec = GpuAdjacentDistinctExec {
            config: GpuAdjacentDistinctConfig {
                keys: vec![],
                is_key_distinct: false,
            },
            input: Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                Arc::new(Schema::empty()),
            )),
            output_schema: Arc::new(Schema::empty()),
            metrics: ExecutionPlanMetricsSet::new(),
            cache: PlanProperties::new(
                datafusion::physical_expr::EquivalenceProperties::new(Arc::new(Schema::empty())),
                datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                datafusion::physical_plan::execution_plan::Boundedness::Bounded,
            ),
        };
        assert_eq!(exec.sort_order_effect(), SortOrderEffect::Preserve);
    }
}
