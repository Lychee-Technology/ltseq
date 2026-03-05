//! GPU-accelerated sort execution plan.
//!
//! `GpuSortExec` replaces DataFusion's `SortExec` for sorting numeric columns.
//! It uses GPU-accelerated key encoding followed by CPU stable sort:
//!
//! 1. For each sort key column, encode values to u64 on GPU (preserving order).
//! 2. Compose a composite sort key (for multi-column sorts) using bit shifting.
//! 3. Download encoded keys to CPU and perform stable sort to get permutation.
//! 4. Apply permutation on GPU to reorder all output columns.
//!
//! For descending sort, the encoded u64 key bits are flipped (bitwise NOT)
//! so that normal ascending sort produces descending order.
//!
//! ## Supported Types
//!
//! Sort key columns: i32, i64, f32, f64 (same as hash_ops).
//! Non-key columns (pass-through): i32, i64, f32, f64 (reordered by permutation).
//!
//! ## Fetch / LIMIT
//!
//! When `SortExec` has a `fetch` value (TopK), the result is truncated after
//! sorting. The full sort is still performed on GPU — TopK optimization with
//! partial sort can be added in Phase 2.5.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

use cudarc::driver::safe::{CudaStream, LaunchConfig};
use cudarc::driver::PushKernelArg;

use super::hash_ops::get_hash_ops_function_pub;
use super::sort_aware::{SortOrderEffect, SortOrderPropagation};
use super::{get_stream, CUDA_BLOCK_SIZE, grid_size};

/// Convert a CUDA driver error into a DataFusion execution error.
fn cuda_err(e: cudarc::driver::safe::DriverError) -> DataFusionError {
    DataFusionError::Execution(format!("CUDA error: {:?}", e))
}

// ============================================================================
// Configuration
// ============================================================================

/// Describes a single sort key for GPU sort.
#[derive(Debug, Clone)]
pub struct GpuSortKey {
    /// Column index in the input schema
    pub column_index: usize,
    /// Column name
    pub column_name: String,
    /// Data type (Int32, Int64, Float32, Float64)
    pub data_type: DataType,
    /// Sort direction: true = descending, false = ascending
    pub descending: bool,
    /// Nulls first: true = nulls sort before non-nulls
    pub nulls_first: bool,
}

/// Configuration for a GPU sort operation.
#[derive(Debug, Clone)]
pub struct GpuSortConfig {
    /// Sort key columns (in order of priority)
    pub sort_keys: Vec<GpuSortKey>,
    /// Optional fetch/limit — only return the first N rows
    pub fetch: Option<usize>,
}

// ============================================================================
// GpuSortExec
// ============================================================================

/// GPU-accelerated sort execution plan.
///
/// Replaces DataFusion's `SortExec` for sorting by numeric columns.
/// Uses GPU key encoding + CPU stable sort + GPU permutation.
///
/// ## Output
///
/// Produces all input rows in sorted order. The output schema is identical
/// to the input schema.
#[derive(Debug)]
pub struct GpuSortExec {
    /// Configuration
    config: GpuSortConfig,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// Output schema (same as input)
    output_schema: SchemaRef,
    /// The original sort expressions from SortExec (for EquivalenceProperties)
    sort_exprs: Vec<PhysicalSortExpr>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cached plan properties
    cache: PlanProperties,
}

impl GpuSortExec {
    /// Create a new GpuSortExec.
    pub fn try_new(
        config: GpuSortConfig,
        input: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
        sort_exprs: Vec<PhysicalSortExpr>,
    ) -> Result<Self> {
        let cache = Self::compute_properties(&output_schema, &sort_exprs);
        // Wrap input in CoalescePartitionsExec if it has multiple partitions.
        // GPU sort must process all data in a single partition to produce
        // a globally sorted output.
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
            sort_exprs,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }

    fn compute_properties(
        schema: &SchemaRef,
        sort_exprs: &[PhysicalSortExpr],
    ) -> PlanProperties {
        use datafusion::physical_expr::EquivalenceProperties;
        use datafusion::physical_plan::Partitioning;

        // Sort produces a single partition with the requested ordering
        let mut eq_props = EquivalenceProperties::new(Arc::clone(schema));
        if !sort_exprs.is_empty() {
            // Ignore error — if reorder fails, we just have no ordering metadata
            let _ = eq_props.reorder(sort_exprs.iter().cloned());
        }

        PlanProperties::new(
            eq_props,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        )
    }
}

impl DisplayAs for GpuSortExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let keys: Vec<String> = self
            .config
            .sort_keys
            .iter()
            .map(|k| {
                let dir = if k.descending { "DESC" } else { "ASC" };
                format!("{}:{}", k.column_name, dir)
            })
            .collect();
        let fetch_str = match self.config.fetch {
            Some(n) => format!(", fetch={}", n),
            None => String::new(),
        };
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "GpuSortExec: sort_keys=[{}]{} [CUDA]",
                    keys.join(", "),
                    fetch_str
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "sort_keys=[{}]{} [CUDA]",
                    keys.join(", "),
                    fetch_str
                )
            }
        }
    }
}

impl ExecutionPlan for GpuSortExec {
    fn name(&self) -> &'static str {
        "GpuSortExec"
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
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(GpuSortExec::try_new(
            self.config.clone(),
            children.swap_remove(0),
            Arc::clone(&self.output_schema),
            self.sort_exprs.clone(),
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

            // Step 2: Execute GPU sort
            let result = gpu_sort(&coalesced, &config, &schema)?;

            timer.done();
            metrics.record_output(result.num_rows());
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
        // Sort preserves row count
        self.input.partition_statistics(None)
    }
}

impl SortOrderPropagation for GpuSortExec {
    fn sort_order_effect(&self) -> SortOrderEffect {
        SortOrderEffect::Produce(
            self.config
                .sort_keys
                .iter()
                .map(|k| k.column_name.clone())
                .collect(),
        )
    }
}

// ============================================================================
// GPU Sort Execution
// ============================================================================

/// Execute GPU sort on a coalesced RecordBatch.
///
/// Returns the batch with rows reordered according to the sort keys.
fn gpu_sort(
    batch: &RecordBatch,
    config: &GpuSortConfig,
    output_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let n = batch.num_rows();
    let stream = get_stream().ok_or_else(|| {
        DataFusionError::Execution("CUDA stream unavailable".to_string())
    })?;

    assert!(!config.sort_keys.is_empty(), "Must have at least one sort key");

    if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
        eprintln!(
            "[GPU] gpu_sort: n={}, keys={}, fetch={:?}",
            n,
            config.sort_keys.len(),
            config.fetch
        );
    }

    // Step 1: Encode each sort key column to u64 on GPU
    let mut encoded_columns: Vec<Vec<u64>> = Vec::new();
    for sort_key in &config.sort_keys {
        let column = batch.column(sort_key.column_index);
        let encoded = encode_sort_key_to_u64(&stream, column, &sort_key.data_type, n)?;

        // For descending sort, flip all bits so ascending sort gives descending order
        let final_encoded = if sort_key.descending {
            encoded.into_iter().map(|v| !v).collect()
        } else {
            encoded
        };

        encoded_columns.push(final_encoded);
    }

    // Step 2: Create permutation via CPU stable sort
    let mut indices: Vec<u32> = (0..n as u32).collect();

    if encoded_columns.len() == 1 {
        // Single-key sort: direct comparison
        let keys = &encoded_columns[0];
        indices.sort_by_key(|&i| keys[i as usize]);
    } else {
        // Multi-key sort: compare columns in priority order (lexicographic)
        indices.sort_by(|&a, &b| {
            for col in &encoded_columns {
                let cmp = col[a as usize].cmp(&col[b as usize]);
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
        eprintln!("[GPU] gpu_sort: permutation computed (CPU stable sort), n={}", n);
    }

    // Step 3: Apply fetch/limit — truncate indices
    let effective_n = match config.fetch {
        Some(fetch) => fetch.min(n),
        None => n,
    };
    let indices = &indices[..effective_n];

    // Step 4: Apply permutation to all columns using GPU
    let mut output_columns: Vec<ArrayRef> = Vec::new();
    for col_idx in 0..batch.num_columns() {
        let column = batch.column(col_idx);
        let data_type = column.data_type();

        let sorted_col = if is_gpu_permute_supported(data_type) {
            apply_permutation_column_gpu(&stream, column, data_type, indices, effective_n)?
        } else {
            // CPU fallback for unsupported types: use Arrow take()
            let indices_array = datafusion::arrow::array::UInt32Array::from(indices.to_vec());
            datafusion::arrow::compute::take(column.as_ref(), &indices_array, None)
                .map_err(|e| DataFusionError::Execution(format!("Arrow take failed: {}", e)))?
        };
        output_columns.push(sorted_col);
    }

    RecordBatch::try_new(Arc::clone(output_schema), output_columns)
        .map_err(|e| DataFusionError::Execution(format!("Failed to build sorted batch: {}", e)))
}

/// Encode a column's values to u64 for sort ordering using GPU kernels.
///
/// Downloads encoded values back to CPU for the stable sort step.
fn encode_sort_key_to_u64(
    stream: &Arc<CudaStream>,
    column: &ArrayRef,
    data_type: &DataType,
    n: usize,
) -> Result<Vec<u64>> {
    let n_u32 = n as u32;
    let launch_cfg = LaunchConfig {
        grid_dim: (grid_size(n_u32), 1, 1),
        block_dim: (CUDA_BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };
    let mut d_encoded = stream.alloc_zeros::<u64>(n).map_err(cuda_err)?;

    match data_type {
        DataType::Int64 => {
            let func = get_hash_ops_function_pub("encode_i64_keys").ok_or_else(|| {
                DataFusionError::Execution("CUDA kernel 'encode_i64_keys' not found".into())
            })?;
            let array = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Execution("Column is not Int64Array".into())
            })?;
            let d_src = stream.clone_htod(array.values().as_ref()).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_src)
                    .arg(&mut d_encoded)
                    .arg(&n_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
        }
        DataType::Int32 => {
            let func = get_hash_ops_function_pub("encode_i32_keys").ok_or_else(|| {
                DataFusionError::Execution("CUDA kernel 'encode_i32_keys' not found".into())
            })?;
            let array = column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                DataFusionError::Execution("Column is not Int32Array".into())
            })?;
            let d_src = stream.clone_htod(array.values().as_ref()).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_src)
                    .arg(&mut d_encoded)
                    .arg(&n_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
        }
        DataType::Float64 => {
            let func = get_hash_ops_function_pub("encode_f64_keys").ok_or_else(|| {
                DataFusionError::Execution("CUDA kernel 'encode_f64_keys' not found".into())
            })?;
            let array = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                DataFusionError::Execution("Column is not Float64Array".into())
            })?;
            let d_src = stream.clone_htod(array.values().as_ref()).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_src)
                    .arg(&mut d_encoded)
                    .arg(&n_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
        }
        DataType::Float32 => {
            let func = get_hash_ops_function_pub("encode_f32_keys").ok_or_else(|| {
                DataFusionError::Execution("CUDA kernel 'encode_f32_keys' not found".into())
            })?;
            let array = column.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                DataFusionError::Execution("Column is not Float32Array".into())
            })?;
            let d_src = stream.clone_htod(array.values().as_ref()).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_src)
                    .arg(&mut d_encoded)
                    .arg(&n_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
        }
        _ => {
            return Err(DataFusionError::Execution(format!(
                "Unsupported data type for GPU sort key: {:?}",
                data_type
            )));
        }
    }

    stream.clone_dtoh(&d_encoded).map_err(cuda_err)
}

/// Check if a data type supports GPU permutation (apply_perm_* kernels).
fn is_gpu_permute_supported(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
    )
}

/// Check if a data type is supported as a sort key column.
pub fn is_sort_key_type_supported(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
    )
}

/// Apply a permutation to reorder a column using GPU kernels.
///
/// Reuses the apply_perm_* kernels from hash_ops.
fn apply_permutation_column_gpu(
    stream: &Arc<CudaStream>,
    column: &ArrayRef,
    data_type: &DataType,
    permutation: &[u32],
    n: usize,
) -> Result<ArrayRef> {
    let n_u32 = n as u32;
    let launch_cfg = LaunchConfig {
        grid_dim: (grid_size(n_u32), 1, 1),
        block_dim: (CUDA_BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };
    let d_perm = stream.clone_htod(permutation).map_err(cuda_err)?;

    match data_type {
        DataType::Int64 => {
            let func = get_hash_ops_function_pub("apply_perm_i64").ok_or_else(|| {
                DataFusionError::Execution("CUDA kernel 'apply_perm_i64' not found".into())
            })?;
            let array = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Execution("Column is not Int64Array".into())
            })?;
            let d_src = stream.clone_htod(array.values().as_ref()).map_err(cuda_err)?;
            let mut d_dst = stream.alloc_zeros::<i64>(n).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_src)
                    .arg(&d_perm)
                    .arg(&mut d_dst)
                    .arg(&n_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
            let result = stream.clone_dtoh(&d_dst).map_err(cuda_err)?;
            Ok(Arc::new(Int64Array::from(result)))
        }
        DataType::Int32 => {
            let func = get_hash_ops_function_pub("apply_perm_i32").ok_or_else(|| {
                DataFusionError::Execution("CUDA kernel 'apply_perm_i32' not found".into())
            })?;
            let array = column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                DataFusionError::Execution("Column is not Int32Array".into())
            })?;
            let d_src = stream.clone_htod(array.values().as_ref()).map_err(cuda_err)?;
            let mut d_dst = stream.alloc_zeros::<i32>(n).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_src)
                    .arg(&d_perm)
                    .arg(&mut d_dst)
                    .arg(&n_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
            let result = stream.clone_dtoh(&d_dst).map_err(cuda_err)?;
            Ok(Arc::new(Int32Array::from(result)))
        }
        DataType::Float64 => {
            let func = get_hash_ops_function_pub("apply_perm_f64").ok_or_else(|| {
                DataFusionError::Execution("CUDA kernel 'apply_perm_f64' not found".into())
            })?;
            let array = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                DataFusionError::Execution("Column is not Float64Array".into())
            })?;
            let d_src = stream.clone_htod(array.values().as_ref()).map_err(cuda_err)?;
            let mut d_dst = stream.alloc_zeros::<f64>(n).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_src)
                    .arg(&d_perm)
                    .arg(&mut d_dst)
                    .arg(&n_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
            let result = stream.clone_dtoh(&d_dst).map_err(cuda_err)?;
            Ok(Arc::new(Float64Array::from(result)))
        }
        DataType::Float32 => {
            let func = get_hash_ops_function_pub("apply_perm_f32").ok_or_else(|| {
                DataFusionError::Execution("CUDA kernel 'apply_perm_f32' not found".into())
            })?;
            let array = column.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                DataFusionError::Execution("Column is not Float32Array".into())
            })?;
            let d_src = stream.clone_htod(array.values().as_ref()).map_err(cuda_err)?;
            let mut d_dst = stream.alloc_zeros::<f32>(n).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_src)
                    .arg(&d_perm)
                    .arg(&mut d_dst)
                    .arg(&n_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
            let result = stream.clone_dtoh(&d_dst).map_err(cuda_err)?;
            Ok(Arc::new(Float32Array::from(result)))
        }
        _ => Err(DataFusionError::Execution(format!(
            "Unsupported type for GPU permutation: {:?}",
            data_type
        ))),
    }
}
