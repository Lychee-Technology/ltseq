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
    Date32Array, Date64Array,
    TimestampSecondArray, TimestampMillisecondArray,
    TimestampMicrosecondArray, TimestampNanosecondArray,
};
use datafusion::arrow::compute::{filter_record_batch, cast};
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal, NotExpr};
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

/// A compound GPU filter predicate that can represent AND/OR/NOT trees
/// of simple leaf predicates.
///
/// The tree is evaluated bottom-up: each leaf produces a u8 mask on GPU,
/// then AND/OR/NOT combine masks using lightweight CUDA kernels.
#[derive(Debug, Clone)]
pub enum GpuCompoundPredicate {
    /// A single Column CMP Literal comparison
    Leaf(GpuFilterPredicate),
    /// Logical AND of two sub-predicates
    And(Box<GpuCompoundPredicate>, Box<GpuCompoundPredicate>),
    /// Logical OR of two sub-predicates
    Or(Box<GpuCompoundPredicate>, Box<GpuCompoundPredicate>),
    /// Logical NOT of a sub-predicate
    Not(Box<GpuCompoundPredicate>),
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
            // Dictionary columns: store the dictionary type, we'll cast at execution time
            if is_gpu_supported_dictionary_type(&data_type) {
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
            // Dictionary columns: store the dictionary type, we'll cast at execution time
            if is_gpu_supported_dictionary_type(&data_type) {
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
        let type_str = match &self.data_type {
            DataType::Int32 | DataType::Date32 => "i32",
            DataType::Int64 | DataType::Date64 | DataType::Timestamp(_, _) => "i64",
            DataType::Float32 => "f32",
            DataType::Float64 => "f64",
            DataType::Dictionary(_, value_type) => match value_type.as_ref() {
                DataType::Int32 | DataType::Date32 => "i32",
                DataType::Int64 | DataType::Date64 | DataType::Timestamp(_, _) => "i64",
                DataType::Float32 => "f32",
                DataType::Float64 => "f64",
                _ => unreachable!("Unsupported dictionary value type"),
            },
            _ => unreachable!("Only numeric/date/timestamp types are supported"),
        };
        format!("filter_{}_{}", op_str, type_str)
    }
}

impl GpuCompoundPredicate {
    /// Try to build a compound predicate tree from an arbitrary DataFusion PhysicalExpr.
    ///
    /// Supports:
    /// - Simple `Column CMP Literal` → `Leaf`
    /// - `expr AND expr` → `And(left, right)`
    /// - `expr OR expr` → `Or(left, right)`
    /// - `NOT expr` → `Not(inner)`
    /// - `IS NOT TRUE expr` → `Not(inner)` (DataFusion sometimes wraps NOT this way)
    ///
    /// Returns `None` if any sub-expression can't be handled on GPU.
    pub fn try_from_physical_expr(
        expr: &Arc<dyn PhysicalExpr>,
        schema: &SchemaRef,
    ) -> Option<Self> {
        // First try: simple leaf predicate
        if let Some(leaf) = GpuFilterPredicate::try_from_physical_expr(expr, schema) {
            return Some(GpuCompoundPredicate::Leaf(leaf));
        }

        // Try NOT expression
        if let Some(not_expr) = expr.as_any().downcast_ref::<NotExpr>() {
            let inner = GpuCompoundPredicate::try_from_physical_expr(not_expr.arg(), schema)?;
            return Some(GpuCompoundPredicate::Not(Box::new(inner)));
        }

        // Try AND / OR
        if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>() {
            match binary.op() {
                Operator::And => {
                    let left = GpuCompoundPredicate::try_from_physical_expr(binary.left(), schema)?;
                    let right = GpuCompoundPredicate::try_from_physical_expr(binary.right(), schema)?;
                    return Some(GpuCompoundPredicate::And(Box::new(left), Box::new(right)));
                }
                Operator::Or => {
                    let left = GpuCompoundPredicate::try_from_physical_expr(binary.left(), schema)?;
                    let right = GpuCompoundPredicate::try_from_physical_expr(binary.right(), schema)?;
                    return Some(GpuCompoundPredicate::Or(Box::new(left), Box::new(right)));
                }
                _ => {}
            }
        }

        None
    }

    /// Returns true if this is a simple leaf predicate (no AND/OR/NOT).
    pub fn is_leaf(&self) -> bool {
        matches!(self, GpuCompoundPredicate::Leaf(_))
    }

    /// Get a human-readable summary for display.
    pub fn display_summary(&self) -> String {
        match self {
            GpuCompoundPredicate::Leaf(pred) => {
                format!("{} {} {:?}", pred.column_name, pred.op, pred.literal)
            }
            GpuCompoundPredicate::And(left, right) => {
                format!("({} AND {})", left.display_summary(), right.display_summary())
            }
            GpuCompoundPredicate::Or(left, right) => {
                format!("({} OR {})", left.display_summary(), right.display_summary())
            }
            GpuCompoundPredicate::Not(inner) => {
                format!("NOT {}", inner.display_summary())
            }
        }
    }
}

/// Check if a data type is supported for GPU filter operations.
fn is_gpu_supported_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
    )
}

/// Check if a data type is a dictionary type whose value type is GPU-supported.
fn is_gpu_supported_dictionary_type(dt: &DataType) -> bool {
    if let DataType::Dictionary(_, value_type) = dt {
        is_gpu_supported_type(value_type)
    } else {
        false
    }
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
/// Replaces DataFusion's `FilterExec` for predicates on numeric columns.
/// Supports both simple `Column CMP Literal` and compound `AND`/`OR`/`NOT`
/// combinations. The execution flow per batch:
///
/// 1. Pull a `RecordBatch` from the child plan
/// 2. Evaluate the predicate tree recursively on GPU:
///    - Leaf: extract column, upload to GPU, run comparison kernel → u8 mask
///    - AND: evaluate both children, combine masks with `mask_and` kernel
///    - OR: evaluate both children, combine masks with `mask_or` kernel
///    - NOT: evaluate child, invert mask with `mask_not` kernel
/// 3. Download final mask to CPU
/// 4. Convert to Arrow `BooleanArray` and apply `filter_record_batch`
#[derive(Debug)]
pub struct GpuFilterExec {
    /// The GPU-compatible compound predicate tree
    compound_predicate: GpuCompoundPredicate,
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
    /// Create a new GpuFilterExec from a simple (leaf) predicate (backward compatible).
    pub fn try_new(
        predicate: GpuFilterPredicate,
        original_predicate: Arc<dyn PhysicalExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        Self::try_new_compound(
            GpuCompoundPredicate::Leaf(predicate),
            original_predicate,
            input,
        )
    }

    /// Create a new GpuFilterExec from a compound predicate tree.
    pub fn try_new_compound(
        compound_predicate: GpuCompoundPredicate,
        original_predicate: Arc<dyn PhysicalExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let cache = Self::compute_properties(&input);
        Ok(Self {
            compound_predicate,
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
        let summary = self.compound_predicate.display_summary();
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "GpuFilterExec: {} [CUDA]", summary)
            }
            DisplayFormatType::TreeRender => {
                write!(f, "predicate={} [CUDA]", summary)
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
        Ok(Arc::new(GpuFilterExec::try_new_compound(
            self.compound_predicate.clone(),
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
        let compound_pred = self.compound_predicate.clone();
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        let output_stream = futures_util::stream::unfold(
            (input_stream, compound_pred, metrics),
            |(mut stream, pred, metrics)| async move {
                loop {
                    let timer = metrics.elapsed_compute().timer();
                    match futures_util::StreamExt::next(&mut stream).await {
                        Some(Ok(batch)) => {
                            if batch.num_rows() == 0 {
                                timer.done();
                                continue;
                            }
                            let result = gpu_filter_batch_compound(&batch, &pred);
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

/// Execute a GPU filter on a single RecordBatch using a compound predicate tree.
///
/// Evaluates the predicate tree recursively:
/// - Leaf: runs a single comparison kernel → device mask
/// - AND: evaluates both children, combines with `mask_and` kernel
/// - OR: evaluates both children, combines with `mask_or` kernel
/// - NOT: evaluates child, inverts with `mask_not` kernel
///
/// The final mask is downloaded to CPU and applied via Arrow's `filter_record_batch`.
fn gpu_filter_batch_compound(batch: &RecordBatch, pred: &GpuCompoundPredicate) -> Result<RecordBatch> {
    let n = batch.num_rows();

    let stream = get_stream().ok_or_else(|| {
        datafusion::common::DataFusionError::Execution("CUDA stream unavailable".to_string())
    })?;

    // Evaluate predicate tree on GPU, producing a device-side u8 mask
    let d_mask = evaluate_predicate_on_gpu(batch, pred, &stream, n)?;

    // Download mask to host
    let mask_host = stream.clone_dtoh(&d_mask).map_err(cuda_err)?;

    // Convert u8 mask to Arrow BooleanArray
    let bool_mask = BooleanArray::from(
        mask_host.iter().map(|&v| v != 0).collect::<Vec<bool>>(),
    );

    // Apply the mask to the entire batch
    let filtered = filter_record_batch(batch, &bool_mask)?;
    Ok(filtered)
}

/// Recursively evaluate a compound predicate on GPU, returning a device-side u8 mask.
fn evaluate_predicate_on_gpu(
    batch: &RecordBatch,
    pred: &GpuCompoundPredicate,
    stream: &Arc<cudarc::driver::safe::CudaStream>,
    n: usize,
) -> Result<cudarc::driver::safe::CudaSlice<u8>> {
    let launch_cfg = LaunchConfig {
        grid_dim: (grid_size(n as u32), 1, 1),
        block_dim: (CUDA_BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    match pred {
        GpuCompoundPredicate::Leaf(leaf_pred) => {
            // Single comparison: upload column data, run comparison kernel
            gpu_filter_leaf(batch, leaf_pred, stream, n, launch_cfg)
        }
        GpuCompoundPredicate::And(left, right) => {
            let d_left = evaluate_predicate_on_gpu(batch, left, stream, n)?;
            let d_right = evaluate_predicate_on_gpu(batch, right, stream, n)?;
            let mut d_out = stream.alloc_zeros::<u8>(n).map_err(cuda_err)?;

            let func = get_filter_function("mask_and").ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "CUDA kernel 'mask_and' not found".to_string(),
                )
            })?;

            let n_u32 = n as u32;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_left)
                    .arg(&d_right)
                    .arg(&mut d_out)
                    .arg(&n_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
            Ok(d_out)
        }
        GpuCompoundPredicate::Or(left, right) => {
            let d_left = evaluate_predicate_on_gpu(batch, left, stream, n)?;
            let d_right = evaluate_predicate_on_gpu(batch, right, stream, n)?;
            let mut d_out = stream.alloc_zeros::<u8>(n).map_err(cuda_err)?;

            let func = get_filter_function("mask_or").ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "CUDA kernel 'mask_or' not found".to_string(),
                )
            })?;

            let n_u32 = n as u32;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_left)
                    .arg(&d_right)
                    .arg(&mut d_out)
                    .arg(&n_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
            Ok(d_out)
        }
        GpuCompoundPredicate::Not(inner) => {
            let d_inner = evaluate_predicate_on_gpu(batch, inner, stream, n)?;
            let mut d_out = stream.alloc_zeros::<u8>(n).map_err(cuda_err)?;

            let func = get_filter_function("mask_not").ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "CUDA kernel 'mask_not' not found".to_string(),
                )
            })?;

            let n_u32 = n as u32;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_inner)
                    .arg(&mut d_out)
                    .arg(&n_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
            Ok(d_out)
        }
    }
}

/// Evaluate a single leaf predicate on GPU, returning a device-side u8 mask.
fn gpu_filter_leaf(
    batch: &RecordBatch,
    pred: &GpuFilterPredicate,
    stream: &Arc<cudarc::driver::safe::CudaStream>,
    n: usize,
    launch_cfg: LaunchConfig,
) -> Result<cudarc::driver::safe::CudaSlice<u8>> {
    let column = batch.column(pred.column_index);
    let kernel_name = pred.kernel_name();

    let func = get_filter_function(&kernel_name).ok_or_else(|| {
        datafusion::common::DataFusionError::Execution(
            format!("CUDA kernel '{}' not found", kernel_name),
        )
    })?;

    match pred.data_type {
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
            run_kernel_i64_device(stream, &func, values, threshold, n, launch_cfg)
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
            run_kernel_i32_device(stream, &func, values, threshold, n, launch_cfg)
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
            run_kernel_f64_device(stream, &func, values, threshold, n, launch_cfg)
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
            run_kernel_f32_device(stream, &func, values, threshold, n, launch_cfg)
        }
        DataType::Date32 => {
            let array = column
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "Column is not Date32Array".to_string(),
                    )
                })?;
            let values: &[i32] = array.values();
            let threshold = match &pred.literal {
                ScalarValue::Date32(Some(v)) => *v,
                ScalarValue::Int32(Some(v)) => *v,
                ScalarValue::Int64(Some(v)) => *v as i32,
                _ => {
                    return Err(datafusion::common::DataFusionError::Execution(
                        format!("Cannot convert {:?} to Date32 (i32)", pred.literal),
                    ))
                }
            };
            run_kernel_i32_device(stream, &func, values, threshold, n, launch_cfg)
        }
        DataType::Date64 => {
            let array = column
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "Column is not Date64Array".to_string(),
                    )
                })?;
            let values: &[i64] = array.values();
            let threshold = match &pred.literal {
                ScalarValue::Date64(Some(v)) => *v,
                ScalarValue::Int64(Some(v)) => *v,
                _ => {
                    return Err(datafusion::common::DataFusionError::Execution(
                        format!("Cannot convert {:?} to Date64 (i64)", pred.literal),
                    ))
                }
            };
            run_kernel_i64_device(stream, &func, values, threshold, n, launch_cfg)
        }
        DataType::Timestamp(unit, _) => {
            // All timestamp variants store underlying data as i64.
            // Extract the i64 slice by matching on the time unit for the correct array type.
            let values: &[i64] = match unit {
                TimeUnit::Second => {
                    column.as_any().downcast_ref::<TimestampSecondArray>()
                        .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                            "Column is not TimestampSecondArray".to_string(),
                        ))?.values()
                }
                TimeUnit::Millisecond => {
                    column.as_any().downcast_ref::<TimestampMillisecondArray>()
                        .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                            "Column is not TimestampMillisecondArray".to_string(),
                        ))?.values()
                }
                TimeUnit::Microsecond => {
                    column.as_any().downcast_ref::<TimestampMicrosecondArray>()
                        .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                            "Column is not TimestampMicrosecondArray".to_string(),
                        ))?.values()
                }
                TimeUnit::Nanosecond => {
                    column.as_any().downcast_ref::<TimestampNanosecondArray>()
                        .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                            "Column is not TimestampNanosecondArray".to_string(),
                        ))?.values()
                }
            };
            let threshold = match &pred.literal {
                ScalarValue::TimestampSecond(Some(v), _) => *v,
                ScalarValue::TimestampMillisecond(Some(v), _) => *v,
                ScalarValue::TimestampMicrosecond(Some(v), _) => *v,
                ScalarValue::TimestampNanosecond(Some(v), _) => *v,
                ScalarValue::Int64(Some(v)) => *v,
                _ => {
                    return Err(datafusion::common::DataFusionError::Execution(
                        format!("Cannot convert {:?} to Timestamp (i64)", pred.literal),
                    ))
                }
            };
            run_kernel_i64_device(stream, &func, values, threshold, n, launch_cfg)
        }
        _ => Err(datafusion::common::DataFusionError::Execution(
            format!("Unsupported GPU filter type: {:?}", pred.data_type),
        )),
    }
}

/// Upload i64 data to GPU, run comparison kernel, return device-side mask.
///
/// The mask stays on the GPU as `CudaSlice<u8>` so compound predicate
/// combinators (`mask_and`, `mask_or`, `mask_not`) can work without
/// an intermediate device-to-host transfer.
fn run_kernel_i64_device(
    stream: &Arc<cudarc::driver::safe::CudaStream>,
    func: &cudarc::driver::safe::CudaFunction,
    values: &[i64],
    threshold: i64,
    n: usize,
    launch_cfg: LaunchConfig,
) -> Result<cudarc::driver::safe::CudaSlice<u8>> {
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

    Ok(d_mask)
}

/// Upload i32 data to GPU, run comparison kernel, return device-side mask.
fn run_kernel_i32_device(
    stream: &Arc<cudarc::driver::safe::CudaStream>,
    func: &cudarc::driver::safe::CudaFunction,
    values: &[i32],
    threshold: i32,
    n: usize,
    launch_cfg: LaunchConfig,
) -> Result<cudarc::driver::safe::CudaSlice<u8>> {
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

    Ok(d_mask)
}

/// Upload f64 data to GPU, run comparison kernel, return device-side mask.
fn run_kernel_f64_device(
    stream: &Arc<cudarc::driver::safe::CudaStream>,
    func: &cudarc::driver::safe::CudaFunction,
    values: &[f64],
    threshold: f64,
    n: usize,
    launch_cfg: LaunchConfig,
) -> Result<cudarc::driver::safe::CudaSlice<u8>> {
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

    Ok(d_mask)
}

/// Upload f32 data to GPU, run comparison kernel, return device-side mask.
fn run_kernel_f32_device(
    stream: &Arc<cudarc::driver::safe::CudaStream>,
    func: &cudarc::driver::safe::CudaFunction,
    values: &[f32],
    threshold: f32,
    n: usize,
    launch_cfg: LaunchConfig,
) -> Result<cudarc::driver::safe::CudaSlice<u8>> {
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

    Ok(d_mask)
}

/// Convert a CUDA driver error into a DataFusion execution error.
fn cuda_err(e: cudarc::driver::safe::DriverError) -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::Execution(format!("CUDA error: {:?}", e))
}

// --- SortOrderPropagation ---

impl super::sort_aware::SortOrderPropagation for GpuFilterExec {
    /// Filtering preserves the input's sort order (rows are removed, not reordered).
    fn sort_order_effect(&self) -> super::sort_aware::SortOrderEffect {
        super::sort_aware::SortOrderEffect::Preserve
    }
}
