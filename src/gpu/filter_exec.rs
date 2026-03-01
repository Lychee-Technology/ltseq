//! GPU-accelerated filter execution plan.
//!
//! `GpuFilterExec` replaces DataFusion's `FilterExec` for simple column-vs-literal
//! predicates on numeric types. It transfers only the filter column to GPU,
//! runs a CUDA kernel to produce a boolean mask, then applies the mask on CPU
//! using Arrow's `filter_record_batch`.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::logical_expr::Operator;
use datafusion::common::ScalarValue;

use cudarc::driver::safe::LaunchConfig;
use cudarc::driver::PushKernelArg;

use super::{get_filter_function, get_stream, CUDA_BLOCK_SIZE, grid_size};

/// Describes a simple filter predicate that can be executed on the GPU.
/// Only supports: Column {op} Literal (or Literal {op} Column, which is normalized).
#[derive(Debug, Clone)]
pub struct GpuFilterPredicate {
    /// Index of the column in the schema
    pub column_index: usize,
    /// Column name (for display)
    pub column_name: String,
    /// Comparison operator
    pub op: Operator,
    /// The literal value to compare against
    pub literal: ScalarValue,
    /// Data type of the column
    pub data_type: DataType,
}

impl GpuFilterPredicate {
    /// Try to extract a GPU-compatible predicate from a DataFusion PhysicalExpr.
    /// Returns `None` if the expression is not a simple Column CMP Literal.
    pub fn try_from_physical_expr(
        expr: &Arc<dyn PhysicalExpr>,
        schema: &SchemaRef,
    ) -> Option<Self> {
        let binary = expr.as_any().downcast_ref::<BinaryExpr>()?;
        let op = *binary.op();

        // Only support comparison operators
        if !matches!(
            op,
            Operator::Gt
                | Operator::Lt
                | Operator::GtEq
                | Operator::LtEq
                | Operator::Eq
                | Operator::NotEq
        ) {
            return None;
        }

        // Try Column op Literal
        if let (Some(col), Some(lit)) = (
            binary.left().as_any().downcast_ref::<Column>(),
            binary.right().as_any().downcast_ref::<Literal>(),
        ) {
            let data_type = schema.field(col.index()).data_type().clone();
            if is_gpu_supported_type(&data_type) {
                return Some(GpuFilterPredicate {
                    column_index: col.index(),
                    column_name: col.name().to_string(),
                    op,
                    literal: lit.value().clone(),
                    data_type,
                });
            }
        }

        // Try Literal op Column (normalize by flipping the operator)
        if let (Some(lit), Some(col)) = (
            binary.left().as_any().downcast_ref::<Literal>(),
            binary.right().as_any().downcast_ref::<Column>(),
        ) {
            let flipped_op = flip_operator(op)?;
            let data_type = schema.field(col.index()).data_type().clone();
            if is_gpu_supported_type(&data_type) {
                return Some(GpuFilterPredicate {
                    column_index: col.index(),
                    column_name: col.name().to_string(),
                    op: flipped_op,
                    literal: lit.value().clone(),
                    data_type,
                });
            }
        }

        None
    }

    /// Get the CUDA kernel function name for this predicate.
    fn kernel_name(&self) -> String {
        let op_str = match self.op {
            Operator::Gt => "gt",
            Operator::Lt => "lt",
            Operator::GtEq => "gte",
            Operator::LtEq => "lte",
            Operator::Eq => "eq",
            Operator::NotEq => "neq",
            _ => unreachable!("Only comparison operators are supported"),
        };
        let type_str = match self.data_type {
            DataType::Int32 => "i32",
            DataType::Int64 => "i64",
            DataType::Float32 => "f32",
            DataType::Float64 => "f64",
            _ => unreachable!("Only numeric types are supported"),
        };
        format!("filter_{}_{}", op_str, type_str)
    }
}

/// Check if a data type is supported for GPU filter operations.
fn is_gpu_supported_type(dt: &DataType) -> bool {
    matches!(dt, DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64)
}

/// Flip a comparison operator for Literal op Column → Column op' Literal normalization.
fn flip_operator(op: Operator) -> Option<Operator> {
    match op {
        Operator::Gt => Some(Operator::Lt),
        Operator::Lt => Some(Operator::Gt),
        Operator::GtEq => Some(Operator::LtEq),
        Operator::LtEq => Some(Operator::GtEq),
        Operator::Eq => Some(Operator::Eq),
        Operator::NotEq => Some(Operator::NotEq),
        _ => None,
    }
}

/// GPU-accelerated filter execution plan.
///
/// Replaces DataFusion's `FilterExec` for simple `Column CMP Literal` predicates
/// on numeric columns. The execution flow per batch:
///
/// 1. Pull a `RecordBatch` from the child plan
/// 2. Extract the filter column's data buffer
/// 3. Copy the column data to GPU via `cudarc`
/// 4. Launch a CUDA kernel that produces a `u8` mask (1=keep, 0=drop)
/// 5. Copy the mask back to CPU
/// 6. Convert to Arrow `BooleanArray` and apply `filter_record_batch`
#[derive(Debug)]
pub struct GpuFilterExec {
    /// The GPU-compatible predicate
    predicate: GpuFilterPredicate,
    /// The original PhysicalExpr (kept for display/debugging)
    original_predicate: Arc<dyn PhysicalExpr>,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cached plan properties
    cache: PlanProperties,
}

impl GpuFilterExec {
    /// Create a new GpuFilterExec.
    pub fn try_new(
        predicate: GpuFilterPredicate,
        original_predicate: Arc<dyn PhysicalExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let cache = Self::compute_properties(&input);
        Ok(Self {
            predicate,
            original_predicate,
            input,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }

    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        // GPU filter preserves ordering and partitioning of input,
        // same as CPU FilterExec
        PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            EmissionType::Incremental,
            input.boundedness(),
        )
    }
}

impl DisplayAs for GpuFilterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "GpuFilterExec: {} {} {:?} [CUDA]",
                    self.predicate.column_name, self.predicate.op, self.predicate.literal
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "predicate={} {} {:?} [CUDA]",
                    self.predicate.column_name, self.predicate.op, self.predicate.literal
                )
            }
        }
    }
}

impl ExecutionPlan for GpuFilterExec {
    fn name(&self) -> &'static str {
        "GpuFilterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(GpuFilterExec::try_new(
            self.predicate.clone(),
            Arc::clone(&self.original_predicate),
            children.swap_remove(0),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let schema = input_stream.schema();
        let predicate = self.predicate.clone();
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        let output_stream = futures_util::stream::unfold(
            (input_stream, predicate, metrics),
            |(mut stream, pred, metrics)| async move {
                loop {
                    let timer = metrics.elapsed_compute().timer();
                    match futures_util::StreamExt::next(&mut stream).await {
                        Some(Ok(batch)) => {
                            if batch.num_rows() == 0 {
                                timer.done();
                                continue;
                            }
                            let result = gpu_filter_batch(&batch, &pred);
                            timer.done();
                            match result {
                                Ok(filtered) => {
                                    metrics.record_output(filtered.num_rows());
                                    return Some((Ok(filtered), (stream, pred, metrics)));
                                }
                                Err(e) => {
                                    return Some((Err(e), (stream, pred, metrics)));
                                }
                            }
                        }
                        Some(Err(e)) => {
                            timer.done();
                            return Some((Err(e), (stream, pred, metrics)));
                        }
                        None => {
                            timer.done();
                            return None;
                        }
                    }
                }
            },
        );

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            output_stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<datafusion::physical_plan::Statistics> {
        // Use input statistics with estimated selectivity (conservative: 50%)
        let mut stats = self.input.partition_statistics(None)?.to_inexact();
        stats.num_rows = stats.num_rows.with_estimated_selectivity(0.5);
        stats.total_byte_size = stats.total_byte_size.with_estimated_selectivity(0.5);
        Ok(stats)
    }
}

/// Execute a GPU filter on a single RecordBatch.
///
/// 1. Extract filter column data
/// 2. Upload to GPU
/// 3. Run CUDA kernel
/// 4. Download mask
/// 5. Apply mask using Arrow
fn gpu_filter_batch(batch: &RecordBatch, pred: &GpuFilterPredicate) -> Result<RecordBatch> {
    let n = batch.num_rows();
    let column = batch.column(pred.column_index);
    let kernel_name = pred.kernel_name();

    let stream = get_stream().ok_or_else(|| {
        datafusion::common::DataFusionError::Execution("CUDA stream unavailable".to_string())
    })?;

    let func = get_filter_function(&kernel_name).ok_or_else(|| {
        datafusion::common::DataFusionError::Execution(
            format!("CUDA kernel '{}' not found", kernel_name),
        )
    })?;

    let launch_cfg = LaunchConfig {
        grid_dim: (grid_size(n as u32), 1, 1),
        block_dim: (CUDA_BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    // Run the appropriate kernel based on data type
    let mask_host: Vec<u8> = match pred.data_type {
        DataType::Int64 => {
            let array = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "Column is not Int64Array".to_string(),
                    )
                })?;
            let values: &[i64] = array.values();
            let threshold = match &pred.literal {
                ScalarValue::Int64(Some(v)) => *v,
                ScalarValue::Int32(Some(v)) => *v as i64,
                _ => {
                    return Err(datafusion::common::DataFusionError::Execution(
                        format!("Cannot convert {:?} to i64", pred.literal),
                    ))
                }
            };
            run_kernel_i64(&stream, &func, values, threshold, n, launch_cfg)?
        }
        DataType::Int32 => {
            let array = column
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "Column is not Int32Array".to_string(),
                    )
                })?;
            let values: &[i32] = array.values();
            let threshold = match &pred.literal {
                ScalarValue::Int32(Some(v)) => *v,
                ScalarValue::Int64(Some(v)) => *v as i32,
                _ => {
                    return Err(datafusion::common::DataFusionError::Execution(
                        format!("Cannot convert {:?} to i32", pred.literal),
                    ))
                }
            };
            run_kernel_i32(&stream, &func, values, threshold, n, launch_cfg)?
        }
        DataType::Float64 => {
            let array = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "Column is not Float64Array".to_string(),
                    )
                })?;
            let values: &[f64] = array.values();
            let threshold = match &pred.literal {
                ScalarValue::Float64(Some(v)) => *v,
                ScalarValue::Float32(Some(v)) => *v as f64,
                _ => {
                    return Err(datafusion::common::DataFusionError::Execution(
                        format!("Cannot convert {:?} to f64", pred.literal),
                    ))
                }
            };
            run_kernel_f64(&stream, &func, values, threshold, n, launch_cfg)?
        }
        DataType::Float32 => {
            let array = column
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "Column is not Float32Array".to_string(),
                    )
                })?;
            let values: &[f32] = array.values();
            let threshold = match &pred.literal {
                ScalarValue::Float32(Some(v)) => *v,
                _ => {
                    return Err(datafusion::common::DataFusionError::Execution(
                        format!("Cannot convert {:?} to f32", pred.literal),
                    ))
                }
            };
            run_kernel_f32(&stream, &func, values, threshold, n, launch_cfg)?
        }
        _ => {
            return Err(datafusion::common::DataFusionError::Execution(
                format!("Unsupported GPU filter type: {:?}", pred.data_type),
            ))
        }
    };

    // Convert u8 mask to Arrow BooleanArray
    let bool_mask = BooleanArray::from(
        mask_host.iter().map(|&v| v != 0).collect::<Vec<bool>>(),
    );

    // Apply the mask to the entire batch
    let filtered = filter_record_batch(batch, &bool_mask)?;
    Ok(filtered)
}

/// Upload i64 data to GPU, run kernel, download mask.
fn run_kernel_i64(
    stream: &Arc<cudarc::driver::safe::CudaStream>,
    func: &cudarc::driver::safe::CudaFunction,
    values: &[i64],
    threshold: i64,
    n: usize,
    launch_cfg: LaunchConfig,
) -> Result<Vec<u8>> {
    let d_data = stream.clone_htod(values).map_err(cuda_err)?;
    let mut d_mask = stream.alloc_zeros::<u8>(n).map_err(cuda_err)?;
    let n_u32 = n as u32;

    unsafe {
        stream
            .launch_builder(func)
            .arg(&d_data)
            .arg(&threshold)
            .arg(&mut d_mask)
            .arg(&n_u32)
            .launch(launch_cfg)
            .map_err(cuda_err)?;
    }

    let mask = stream.clone_dtoh(&d_mask).map_err(cuda_err)?;
    Ok(mask)
}

/// Upload i32 data to GPU, run kernel, download mask.
fn run_kernel_i32(
    stream: &Arc<cudarc::driver::safe::CudaStream>,
    func: &cudarc::driver::safe::CudaFunction,
    values: &[i32],
    threshold: i32,
    n: usize,
    launch_cfg: LaunchConfig,
) -> Result<Vec<u8>> {
    let d_data = stream.clone_htod(values).map_err(cuda_err)?;
    let mut d_mask = stream.alloc_zeros::<u8>(n).map_err(cuda_err)?;
    let n_u32 = n as u32;

    unsafe {
        stream
            .launch_builder(func)
            .arg(&d_data)
            .arg(&threshold)
            .arg(&mut d_mask)
            .arg(&n_u32)
            .launch(launch_cfg)
            .map_err(cuda_err)?;
    }

    let mask = stream.clone_dtoh(&d_mask).map_err(cuda_err)?;
    Ok(mask)
}

/// Upload f64 data to GPU, run kernel, download mask.
fn run_kernel_f64(
    stream: &Arc<cudarc::driver::safe::CudaStream>,
    func: &cudarc::driver::safe::CudaFunction,
    values: &[f64],
    threshold: f64,
    n: usize,
    launch_cfg: LaunchConfig,
) -> Result<Vec<u8>> {
    let d_data = stream.clone_htod(values).map_err(cuda_err)?;
    let mut d_mask = stream.alloc_zeros::<u8>(n).map_err(cuda_err)?;
    let n_u32 = n as u32;

    unsafe {
        stream
            .launch_builder(func)
            .arg(&d_data)
            .arg(&threshold)
            .arg(&mut d_mask)
            .arg(&n_u32)
            .launch(launch_cfg)
            .map_err(cuda_err)?;
    }

    let mask = stream.clone_dtoh(&d_mask).map_err(cuda_err)?;
    Ok(mask)
}

/// Upload f32 data to GPU, run kernel, download mask.
fn run_kernel_f32(
    stream: &Arc<cudarc::driver::safe::CudaStream>,
    func: &cudarc::driver::safe::CudaFunction,
    values: &[f32],
    threshold: f32,
    n: usize,
    launch_cfg: LaunchConfig,
) -> Result<Vec<u8>> {
    let d_data = stream.clone_htod(values).map_err(cuda_err)?;
    let mut d_mask = stream.alloc_zeros::<u8>(n).map_err(cuda_err)?;
    let n_u32 = n as u32;

    unsafe {
        stream
            .launch_builder(func)
            .arg(&d_data)
            .arg(&threshold)
            .arg(&mut d_mask)
            .arg(&n_u32)
            .launch(launch_cfg)
            .map_err(cuda_err)?;
    }

    let mask = stream.clone_dtoh(&d_mask).map_err(cuda_err)?;
    Ok(mask)
}

/// Convert a CUDA driver error into a DataFusion execution error.
fn cuda_err(e: cudarc::driver::safe::DriverError) -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::Execution(format!("CUDA error: {:?}", e))
}
