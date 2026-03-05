//! GPU-accelerated window operations for LTSeq.
//!
//! # GpuWindowShiftExec
//!
//! Replaces DataFusion's `BoundedWindowAggExec` / `WindowAggExec` for simple
//! shift (lag/lead) and diff operations on numeric columns.
//!
//! ## Algorithm
//!
//! Shift (lag/lead) is a simple GPU memory copy with offset:
//! - **lag(col, n)**: `out[i] = (i >= n) ? data[i - n] : default_or_null`
//! - **lead(col, n)**: `out[i] = (i + n < N) ? data[i + n] : default_or_null`
//!
//! Diff is adjacent subtraction with configurable period:
//! - **diff(col, n)**: `out[i] = (i >= n) ? data[i] - data[i - n] : null`
//!
//! These are embarrassingly parallel — each thread handles one output element.
//! The data is already sorted (window functions require ORDER BY) so we have
//! coalesced memory access patterns.
//!
//! ## Supported operations
//!
//! - `shift(n)` / `lag(col, n)` / `lead(col, n)` — offset copy
//! - `diff(n)` — `col - lag(col, n)` expressed as BinaryExpr(col, Minus, WindowFunction(lag))
//!
//! ## CUDA Kernels
//!
//! - `shift_copy_{i64,f64,i32,f32}`: Copy with offset, producing null mask
//! - `adjacent_diff_{i64,f64,i32,f32}`: Subtract element at offset, producing null mask
//!
//! ## Performance Target
//!
//! 100M rows < 2ms (single coalesced read + write per thread)

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array,
    BooleanArray, BooleanBufferBuilder, NullArray,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};

use cudarc::driver::safe::LaunchConfig;
use cudarc::driver::PushKernelArg;

use super::{get_stream, CUDA_BLOCK_SIZE, grid_size};
use super::sort_aware::{SortOrderEffect, SortOrderPropagation};

// ============================================================================
// CUDA Kernel Source
// ============================================================================

/// CUDA kernels for window shift (lag/lead) and diff operations.
///
/// Kernel naming: `shift_copy_{type}` for offset copy,
///                `window_diff_{type}` for adjacent subtraction.
///
/// Each shift kernel:
/// - `data`: input column
/// - `out`: output column (same type)
/// - `valid`: output null mask (u8: 0=null, 1=valid)
/// - `n`: number of elements
/// - `offset`: shift offset (positive = lag, negative = lead)
///
/// Each diff kernel:
/// - `data`: input column
/// - `out`: output column (same type)
/// - `valid`: output null mask
/// - `n`: number of elements
/// - `period`: diff period (positive integer)
const WINDOW_OPS_KERNEL_SRC: &str = r#"
// ── Shift (lag/lead) kernels ─────────────────────────────────────────
// out[i] = data[i - offset] if in bounds, else null
// offset > 0 = lag (look backward), offset < 0 = lead (look forward)

extern "C" __global__ void shift_copy_i64(
    const long long* __restrict__ data,
    long long* __restrict__ out,
    unsigned char* __restrict__ valid,
    const unsigned int n,
    const int offset
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    int src = (int)idx - offset;
    if (src >= 0 && src < (int)n) {
        out[idx] = data[src];
        valid[idx] = 1;
    } else {
        out[idx] = 0;
        valid[idx] = 0;
    }
}

extern "C" __global__ void shift_copy_f64(
    const double* __restrict__ data,
    double* __restrict__ out,
    unsigned char* __restrict__ valid,
    const unsigned int n,
    const int offset
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    int src = (int)idx - offset;
    if (src >= 0 && src < (int)n) {
        out[idx] = data[src];
        valid[idx] = 1;
    } else {
        out[idx] = 0.0;
        valid[idx] = 0;
    }
}

extern "C" __global__ void shift_copy_i32(
    const int* __restrict__ data,
    int* __restrict__ out,
    unsigned char* __restrict__ valid,
    const unsigned int n,
    const int offset
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    int src = (int)idx - offset;
    if (src >= 0 && src < (int)n) {
        out[idx] = data[src];
        valid[idx] = 1;
    } else {
        out[idx] = 0;
        valid[idx] = 0;
    }
}

extern "C" __global__ void shift_copy_f32(
    const float* __restrict__ data,
    float* __restrict__ out,
    unsigned char* __restrict__ valid,
    const unsigned int n,
    const int offset
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    int src = (int)idx - offset;
    if (src >= 0 && src < (int)n) {
        out[idx] = data[src];
        valid[idx] = 1;
    } else {
        out[idx] = 0.0f;
        valid[idx] = 0;
    }
}

// ── Diff kernels ─────────────────────────────────────────────────────
// out[i] = data[i] - data[i - period] if i >= period, else null

extern "C" __global__ void window_diff_i64(
    const long long* __restrict__ data,
    long long* __restrict__ out,
    unsigned char* __restrict__ valid,
    const unsigned int n,
    const int period
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    if ((int)idx >= period && period > 0) {
        out[idx] = data[idx] - data[idx - period];
        valid[idx] = 1;
    } else {
        out[idx] = 0;
        valid[idx] = 0;
    }
}

extern "C" __global__ void window_diff_f64(
    const double* __restrict__ data,
    double* __restrict__ out,
    unsigned char* __restrict__ valid,
    const unsigned int n,
    const int period
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    if ((int)idx >= period && period > 0) {
        out[idx] = data[idx] - data[idx - period];
        valid[idx] = 1;
    } else {
        out[idx] = 0.0;
        valid[idx] = 0;
    }
}

extern "C" __global__ void window_diff_i32(
    const int* __restrict__ data,
    int* __restrict__ out,
    unsigned char* __restrict__ valid,
    const unsigned int n,
    const int period
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    if ((int)idx >= period && period > 0) {
        out[idx] = data[idx] - data[idx - period];
        valid[idx] = 1;
    } else {
        out[idx] = 0;
        valid[idx] = 0;
    }
}

extern "C" __global__ void window_diff_f32(
    const float* __restrict__ data,
    float* __restrict__ out,
    unsigned char* __restrict__ valid,
    const unsigned int n,
    const int period
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    if ((int)idx >= period && period > 0) {
        out[idx] = data[idx] - data[idx - period];
        valid[idx] = 1;
    } else {
        out[idx] = 0.0f;
        valid[idx] = 0;
    }
}
"#;

// ============================================================================
// GPU Resource Management for Window Kernels
// ============================================================================

use cudarc::driver::safe::CudaModule;
use cudarc::nvrtc::compile_ptx;
use std::sync::OnceLock;

/// Cached compiled CUDA module for window operations.
static WINDOW_MODULE: OnceLock<Option<Arc<CudaModule>>> = OnceLock::new();

/// Get the compiled window ops CUDA module.
fn get_window_module() -> Option<Arc<CudaModule>> {
    WINDOW_MODULE
        .get_or_init(|| {
            let ctx = super::get_context()?;
            let ptx = compile_ptx(WINDOW_OPS_KERNEL_SRC).ok()?;
            let module = ctx.load_module(ptx).ok()?;
            Some(module)
        })
        .clone()
}

// ============================================================================
// Window Operation Types
// ============================================================================

/// Describes a single window shift/diff operation to execute on GPU.
#[derive(Debug, Clone)]
pub enum GpuWindowOp {
    /// Shift (lag/lead) operation: copy with offset.
    /// Positive offset = lag (look backward), negative = lead (look forward).
    Shift {
        /// Column index in the input schema
        column_index: usize,
        /// Column name (for display)
        column_name: String,
        /// Shift offset (positive = lag, negative = lead)
        offset: i32,
        /// Output column name
        output_name: String,
        /// Data type of the column
        data_type: DataType,
    },
    /// Diff operation: subtract element at offset.
    Diff {
        /// Column index in the input schema
        column_index: usize,
        /// Column name (for display)
        column_name: String,
        /// Diff period (positive)
        period: i32,
        /// Output column name
        output_name: String,
        /// Data type of the column
        data_type: DataType,
    },
}

impl GpuWindowOp {
    /// Get the output column name.
    pub fn output_name(&self) -> &str {
        match self {
            GpuWindowOp::Shift { output_name, .. } => output_name,
            GpuWindowOp::Diff { output_name, .. } => output_name,
        }
    }

    /// Get the data type of the output.
    pub fn data_type(&self) -> &DataType {
        match self {
            GpuWindowOp::Shift { data_type, .. } => data_type,
            GpuWindowOp::Diff { data_type, .. } => data_type,
        }
    }
}

/// Configuration for `GpuWindowShiftExec`.
#[derive(Debug, Clone)]
pub struct GpuWindowShiftConfig {
    /// Window operations to execute (in order of output columns).
    pub ops: Vec<GpuWindowOp>,
}

// ============================================================================
// GpuWindowShiftExec
// ============================================================================

/// GPU execution node for window shift (lag/lead) and diff operations.
///
/// Replaces DataFusion's `BoundedWindowAggExec` / `WindowAggExec` when all
/// window expressions are simple lag/lead/diff on numeric columns without
/// partition_by (whole-table windows).
///
/// The input data is assumed to already be sorted (enforced by the Python layer
/// which requires `.sort()` before window operations).
///
/// ## Execution
///
/// 1. Collect all input batches into a single contiguous RecordBatch.
/// 2. For each window operation, transfer the source column to GPU.
/// 3. Launch the appropriate CUDA kernel (shift_copy or window_diff).
/// 4. Transfer results back and build the output RecordBatch:
///    - All original columns (pass-through)
///    - Plus new window result columns appended
#[derive(Debug)]
pub struct GpuWindowShiftExec {
    /// Window operations to execute
    config: GpuWindowShiftConfig,
    /// Child execution plan (provides sorted input data)
    input: Arc<dyn ExecutionPlan>,
    /// Output schema: input schema + window result columns
    output_schema: SchemaRef,
    /// Cached plan properties
    properties: PlanProperties,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl GpuWindowShiftExec {
    /// Create a new `GpuWindowShiftExec`.
    ///
    /// The output schema is the input schema plus one new column per window operation.
    pub fn try_new(
        config: GpuWindowShiftConfig,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let input_schema = input.schema();

        // Build output schema: input columns + window result columns
        let mut fields: Vec<Arc<Field>> = input_schema.fields().iter().cloned().collect();
        for op in &config.ops {
            // Window columns are nullable (nulls for out-of-bounds shifts)
            let field = Arc::new(Field::new(
                op.output_name(),
                op.data_type().clone(),
                true, // always nullable
            ));
            fields.push(field);
        }
        let output_schema = Arc::new(Schema::new(fields));

        let properties = PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            EmissionType::Final,
        );

        Ok(Self {
            config,
            input,
            output_schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl SortOrderPropagation for GpuWindowShiftExec {
    fn sort_order_effect(&self) -> SortOrderEffect {
        // Window operations preserve the input sort order —
        // they only append new columns, they don't reorder rows.
        SortOrderEffect::Preserve
    }
}

impl DisplayAs for GpuWindowShiftExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let ops: Vec<String> = self.config.ops.iter().map(|op| {
                    match op {
                        GpuWindowOp::Shift { column_name, offset, output_name, .. } => {
                            format!("{}=shift({}, {})", output_name, column_name, offset)
                        }
                        GpuWindowOp::Diff { column_name, period, output_name, .. } => {
                            format!("{}=diff({}, {})", output_name, column_name, period)
                        }
                    }
                }).collect();
                write!(f, "GpuWindowShiftExec: [{}]", ops.join(", "))
            }
        }
    }
}

impl ExecutionPlan for GpuWindowShiftExec {
    fn name(&self) -> &str {
        "GpuWindowShiftExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "GpuWindowShiftExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(GpuWindowShiftExec::try_new(
            self.config.clone(),
            Arc::clone(&children[0]),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, Arc::clone(&context))?;
        let config = self.config.clone();
        let output_schema = Arc::clone(&self.output_schema);
        let input_schema = self.input.schema();
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        let stream = futures::stream::once(async move {
            let _timer = metrics.elapsed_compute().timer();

            // Step 1: Collect all batches
            use futures::StreamExt;
            let batches: Vec<RecordBatch> = input_stream
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()?;

            if batches.is_empty() {
                // Return empty batch with output schema
                return Ok(RecordBatch::new_empty(output_schema));
            }

            // Step 2: Concatenate into single batch
            let combined = datafusion::arrow::compute::concat_batches(&input_schema, &batches)?;
            let num_rows = combined.num_rows();

            if num_rows == 0 {
                return Ok(RecordBatch::new_empty(output_schema));
            }

            // Step 3: Execute each window operation
            let mut result_columns: Vec<ArrayRef> = Vec::new();

            // First, include all original columns
            for i in 0..combined.num_columns() {
                result_columns.push(Arc::clone(combined.column(i)));
            }

            // Then compute window operation results
            for op in &config.ops {
                let result = execute_window_op(&combined, op, num_rows)?;
                result_columns.push(result);
            }

            // Step 4: Build output batch
            let output = RecordBatch::try_new(output_schema, result_columns)?;
            metrics.record_output(output.num_rows());
            Ok(output)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.output_schema),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

// ============================================================================
// GPU Kernel Execution
// ============================================================================

/// Execute a single window operation (shift or diff) on the GPU.
fn execute_window_op(
    batch: &RecordBatch,
    op: &GpuWindowOp,
    num_rows: usize,
) -> Result<ArrayRef> {
    // Try GPU execution; fall back to CPU if GPU is unavailable
    match op {
        GpuWindowOp::Shift {
            column_index,
            offset,
            data_type,
            ..
        } => {
            let column = batch.column(*column_index);
            execute_shift_gpu(column, *offset, data_type, num_rows)
                .or_else(|_| execute_shift_cpu(column, *offset, data_type, num_rows))
        }
        GpuWindowOp::Diff {
            column_index,
            period,
            data_type,
            ..
        } => {
            let column = batch.column(*column_index);
            execute_diff_gpu(column, *period, data_type, num_rows)
                .or_else(|_| execute_diff_cpu(column, *period, data_type, num_rows))
        }
    }
}

/// Execute shift on GPU using CUDA kernel.
fn execute_shift_gpu(
    column: &ArrayRef,
    offset: i32,
    data_type: &DataType,
    num_rows: usize,
) -> Result<ArrayRef> {
    let module = get_window_module().ok_or_else(|| {
        DataFusionError::Internal("GPU not available for window shift".to_string())
    })?;
    let stream = get_stream().ok_or_else(|| {
        DataFusionError::Internal("CUDA stream not available".to_string())
    })?;

    let n = num_rows as u32;
    let grid = grid_size(n);
    let launch_cfg = LaunchConfig {
        grid_dim: (grid, 1, 1),
        block_dim: (CUDA_BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    match data_type {
        DataType::Int64 => {
            let arr = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Int64Array".to_string())
            })?;
            gpu_shift_typed::<i64>(arr.values(), &module, &stream, launch_cfg, n, offset, "shift_copy_i64")
                .map(|(values, valid_mask)| {
                    let null_buffer = build_null_buffer(&valid_mask);
                    Arc::new(Int64Array::new(values.into(), Some(null_buffer))) as ArrayRef
                })
        }
        DataType::Float64 => {
            let arr = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Float64Array".to_string())
            })?;
            gpu_shift_typed::<f64>(arr.values(), &module, &stream, launch_cfg, n, offset, "shift_copy_f64")
                .map(|(values, valid_mask)| {
                    let null_buffer = build_null_buffer(&valid_mask);
                    Arc::new(Float64Array::new(values.into(), Some(null_buffer))) as ArrayRef
                })
        }
        DataType::Int32 => {
            let arr = column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Int32Array".to_string())
            })?;
            gpu_shift_typed::<i32>(arr.values(), &module, &stream, launch_cfg, n, offset, "shift_copy_i32")
                .map(|(values, valid_mask)| {
                    let null_buffer = build_null_buffer(&valid_mask);
                    Arc::new(Int32Array::new(values.into(), Some(null_buffer))) as ArrayRef
                })
        }
        DataType::Float32 => {
            let arr = column.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Float32Array".to_string())
            })?;
            gpu_shift_typed::<f32>(arr.values(), &module, &stream, launch_cfg, n, offset, "shift_copy_f32")
                .map(|(values, valid_mask)| {
                    let null_buffer = build_null_buffer(&valid_mask);
                    Arc::new(Float32Array::new(values.into(), Some(null_buffer))) as ArrayRef
                })
        }
        _ => Err(DataFusionError::Internal(format!(
            "Unsupported data type for GPU shift: {:?}",
            data_type
        ))),
    }
}

/// Generic GPU shift kernel execution for a typed column.
fn gpu_shift_typed<T: cudarc::driver::DeviceRepr + Default + Clone>(
    input_values: &[T],
    module: &Arc<CudaModule>,
    stream: &Arc<cudarc::driver::safe::CudaStream>,
    launch_cfg: LaunchConfig,
    n: u32,
    offset: i32,
    kernel_name: &str,
) -> Result<(Vec<T>, Vec<u8>)> {
    let func = module.load_function(kernel_name).map_err(|e| {
        DataFusionError::Internal(format!("Failed to load kernel {}: {}", kernel_name, e))
    })?;

    // Allocate device memory
    let d_data = stream.memcpy_htod(input_values).map_err(|e| {
        DataFusionError::Internal(format!("H2D copy failed: {}", e))
    })?;
    let d_out = stream.alloc_zeros::<T>(n as usize).map_err(|e| {
        DataFusionError::Internal(format!("GPU alloc failed: {}", e))
    })?;
    let d_valid = stream.alloc_zeros::<u8>(n as usize).map_err(|e| {
        DataFusionError::Internal(format!("GPU alloc failed: {}", e))
    })?;

    // Launch kernel
    unsafe {
        let mut builder = stream.launch_builder(&func);
        builder.arg(&d_data);
        builder.arg(&d_out);
        builder.arg(&d_valid);
        builder.arg(&n);
        builder.arg(&offset);
        builder.launch(launch_cfg).map_err(|e| {
            DataFusionError::Internal(format!("Kernel launch failed: {}", e))
        })?;
    }

    // Copy results back
    let out_values = stream.memcpy_dtoh(&d_out).map_err(|e| {
        DataFusionError::Internal(format!("D2H copy failed: {}", e))
    })?;
    let valid_mask = stream.memcpy_dtoh(&d_valid).map_err(|e| {
        DataFusionError::Internal(format!("D2H copy failed: {}", e))
    })?;

    Ok((out_values, valid_mask))
}

/// Execute diff on GPU using CUDA kernel.
fn execute_diff_gpu(
    column: &ArrayRef,
    period: i32,
    data_type: &DataType,
    num_rows: usize,
) -> Result<ArrayRef> {
    let module = get_window_module().ok_or_else(|| {
        DataFusionError::Internal("GPU not available for window diff".to_string())
    })?;
    let stream = get_stream().ok_or_else(|| {
        DataFusionError::Internal("CUDA stream not available".to_string())
    })?;

    let n = num_rows as u32;
    let grid = grid_size(n);
    let launch_cfg = LaunchConfig {
        grid_dim: (grid, 1, 1),
        block_dim: (CUDA_BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    match data_type {
        DataType::Int64 => {
            let arr = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Int64Array".to_string())
            })?;
            gpu_diff_typed::<i64>(arr.values(), &module, &stream, launch_cfg, n, period, "window_diff_i64")
                .map(|(values, valid_mask)| {
                    let null_buffer = build_null_buffer(&valid_mask);
                    Arc::new(Int64Array::new(values.into(), Some(null_buffer))) as ArrayRef
                })
        }
        DataType::Float64 => {
            let arr = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Float64Array".to_string())
            })?;
            gpu_diff_typed::<f64>(arr.values(), &module, &stream, launch_cfg, n, period, "window_diff_f64")
                .map(|(values, valid_mask)| {
                    let null_buffer = build_null_buffer(&valid_mask);
                    Arc::new(Float64Array::new(values.into(), Some(null_buffer))) as ArrayRef
                })
        }
        DataType::Int32 => {
            let arr = column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Int32Array".to_string())
            })?;
            gpu_diff_typed::<i32>(arr.values(), &module, &stream, launch_cfg, n, period, "window_diff_i32")
                .map(|(values, valid_mask)| {
                    let null_buffer = build_null_buffer(&valid_mask);
                    Arc::new(Int32Array::new(values.into(), Some(null_buffer))) as ArrayRef
                })
        }
        DataType::Float32 => {
            let arr = column.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Float32Array".to_string())
            })?;
            gpu_diff_typed::<f32>(arr.values(), &module, &stream, launch_cfg, n, period, "window_diff_f32")
                .map(|(values, valid_mask)| {
                    let null_buffer = build_null_buffer(&valid_mask);
                    Arc::new(Float32Array::new(values.into(), Some(null_buffer))) as ArrayRef
                })
        }
        _ => Err(DataFusionError::Internal(format!(
            "Unsupported data type for GPU diff: {:?}",
            data_type
        ))),
    }
}

/// Generic GPU diff kernel execution for a typed column.
fn gpu_diff_typed<T: cudarc::driver::DeviceRepr + Default + Clone>(
    input_values: &[T],
    module: &Arc<CudaModule>,
    stream: &Arc<cudarc::driver::safe::CudaStream>,
    launch_cfg: LaunchConfig,
    n: u32,
    period: i32,
    kernel_name: &str,
) -> Result<(Vec<T>, Vec<u8>)> {
    let func = module.load_function(kernel_name).map_err(|e| {
        DataFusionError::Internal(format!("Failed to load kernel {}: {}", kernel_name, e))
    })?;

    // Allocate device memory
    let d_data = stream.memcpy_htod(input_values).map_err(|e| {
        DataFusionError::Internal(format!("H2D copy failed: {}", e))
    })?;
    let d_out = stream.alloc_zeros::<T>(n as usize).map_err(|e| {
        DataFusionError::Internal(format!("GPU alloc failed: {}", e))
    })?;
    let d_valid = stream.alloc_zeros::<u8>(n as usize).map_err(|e| {
        DataFusionError::Internal(format!("GPU alloc failed: {}", e))
    })?;

    // Launch kernel
    unsafe {
        let mut builder = stream.launch_builder(&func);
        builder.arg(&d_data);
        builder.arg(&d_out);
        builder.arg(&d_valid);
        builder.arg(&n);
        builder.arg(&period);
        builder.launch(launch_cfg).map_err(|e| {
            DataFusionError::Internal(format!("Kernel launch failed: {}", e))
        })?;
    }

    // Copy results back
    let out_values = stream.memcpy_dtoh(&d_out).map_err(|e| {
        DataFusionError::Internal(format!("D2H copy failed: {}", e))
    })?;
    let valid_mask = stream.memcpy_dtoh(&d_valid).map_err(|e| {
        DataFusionError::Internal(format!("D2H copy failed: {}", e))
    })?;

    Ok((out_values, valid_mask))
}

// ============================================================================
// CPU Fallback Implementations
// ============================================================================

/// CPU fallback for shift operation.
fn execute_shift_cpu(
    column: &ArrayRef,
    offset: i32,
    data_type: &DataType,
    num_rows: usize,
) -> Result<ArrayRef> {
    match data_type {
        DataType::Int64 => {
            let arr = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Int64Array".to_string())
            })?;
            Ok(cpu_shift_typed(arr.values(), offset, num_rows, |vals, nulls| {
                Arc::new(Int64Array::new(vals.into(), Some(nulls))) as ArrayRef
            }))
        }
        DataType::Float64 => {
            let arr = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Float64Array".to_string())
            })?;
            Ok(cpu_shift_typed(arr.values(), offset, num_rows, |vals, nulls| {
                Arc::new(Float64Array::new(vals.into(), Some(nulls))) as ArrayRef
            }))
        }
        DataType::Int32 => {
            let arr = column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Int32Array".to_string())
            })?;
            Ok(cpu_shift_typed(arr.values(), offset, num_rows, |vals, nulls| {
                Arc::new(Int32Array::new(vals.into(), Some(nulls))) as ArrayRef
            }))
        }
        DataType::Float32 => {
            let arr = column.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Float32Array".to_string())
            })?;
            Ok(cpu_shift_typed(arr.values(), offset, num_rows, |vals, nulls| {
                Arc::new(Float32Array::new(vals.into(), Some(nulls))) as ArrayRef
            }))
        }
        _ => Err(DataFusionError::Internal(format!(
            "Unsupported type for CPU shift: {:?}", data_type
        ))),
    }
}

/// Generic CPU shift for typed arrays.
fn cpu_shift_typed<T: Default + Copy, F>(
    input: &[T],
    offset: i32,
    num_rows: usize,
    build_array: F,
) -> ArrayRef
where
    F: FnOnce(Vec<T>, NullBuffer) -> ArrayRef,
{
    let mut out = vec![T::default(); num_rows];
    let mut valid = vec![false; num_rows];

    for i in 0..num_rows {
        let src = i as i64 - offset as i64;
        if src >= 0 && (src as usize) < num_rows {
            out[i] = input[src as usize];
            valid[i] = true;
        }
    }

    let null_buffer = NullBuffer::from(valid);
    build_array(out, null_buffer)
}

/// CPU fallback for diff operation.
fn execute_diff_cpu(
    column: &ArrayRef,
    period: i32,
    data_type: &DataType,
    num_rows: usize,
) -> Result<ArrayRef> {
    match data_type {
        DataType::Int64 => {
            let arr = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Int64Array".to_string())
            })?;
            Ok(cpu_diff_typed_i64(arr.values(), period, num_rows))
        }
        DataType::Float64 => {
            let arr = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Float64Array".to_string())
            })?;
            Ok(cpu_diff_typed_f64(arr.values(), period, num_rows))
        }
        DataType::Int32 => {
            let arr = column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Int32Array".to_string())
            })?;
            Ok(cpu_diff_typed_i32(arr.values(), period, num_rows))
        }
        DataType::Float32 => {
            let arr = column.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                DataFusionError::Internal("Expected Float32Array".to_string())
            })?;
            Ok(cpu_diff_typed_f32(arr.values(), period, num_rows))
        }
        _ => Err(DataFusionError::Internal(format!(
            "Unsupported type for CPU diff: {:?}", data_type
        ))),
    }
}

fn cpu_diff_typed_i64(input: &[i64], period: i32, num_rows: usize) -> ArrayRef {
    let mut out = vec![0i64; num_rows];
    let mut valid = vec![false; num_rows];
    for i in 0..num_rows {
        if (i as i32) >= period && period > 0 {
            out[i] = input[i] - input[i - period as usize];
            valid[i] = true;
        }
    }
    let null_buffer = NullBuffer::from(valid);
    Arc::new(Int64Array::new(out.into(), Some(null_buffer)))
}

fn cpu_diff_typed_f64(input: &[f64], period: i32, num_rows: usize) -> ArrayRef {
    let mut out = vec![0.0f64; num_rows];
    let mut valid = vec![false; num_rows];
    for i in 0..num_rows {
        if (i as i32) >= period && period > 0 {
            out[i] = input[i] - input[i - period as usize];
            valid[i] = true;
        }
    }
    let null_buffer = NullBuffer::from(valid);
    Arc::new(Float64Array::new(out.into(), Some(null_buffer)))
}

fn cpu_diff_typed_i32(input: &[i32], period: i32, num_rows: usize) -> ArrayRef {
    let mut out = vec![0i32; num_rows];
    let mut valid = vec![false; num_rows];
    for i in 0..num_rows {
        if (i as i32) >= period && period > 0 {
            out[i] = input[i] - input[i - period as usize];
            valid[i] = true;
        }
    }
    let null_buffer = NullBuffer::from(valid);
    Arc::new(Int32Array::new(out.into(), Some(null_buffer)))
}

fn cpu_diff_typed_f32(input: &[f32], period: i32, num_rows: usize) -> ArrayRef {
    let mut out = vec![0.0f32; num_rows];
    let mut valid = vec![false; num_rows];
    for i in 0..num_rows {
        if (i as i32) >= period && period > 0 {
            out[i] = input[i] - input[i - period as usize];
            valid[i] = true;
        }
    }
    let null_buffer = NullBuffer::from(valid);
    Arc::new(Float32Array::new(out.into(), Some(null_buffer)))
}

// ============================================================================
// Utility Functions
// ============================================================================

/// Build a `NullBuffer` from a u8 validity mask (0 = null, 1 = valid).
fn build_null_buffer(valid_mask: &[u8]) -> NullBuffer {
    let bools: Vec<bool> = valid_mask.iter().map(|&v| v != 0).collect();
    NullBuffer::from(bools)
}

/// Check if a data type is supported for GPU window operations.
pub fn is_window_gpu_supported_type(dt: &DataType) -> bool {
    matches!(dt, DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64)
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, Float64Array};

    #[test]
    fn test_cpu_shift_lag_positive() {
        // lag(col, 2): out[i] = data[i-2], first 2 rows are null
        let input = vec![10i64, 20, 30, 40, 50];
        let result = cpu_shift_typed(&input, 2, 5, |vals, nulls| {
            Arc::new(Int64Array::new(vals.into(), Some(nulls))) as ArrayRef
        });
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(arr.is_null(0));
        assert!(arr.is_null(1));
        assert_eq!(arr.value(2), 10);
        assert_eq!(arr.value(3), 20);
        assert_eq!(arr.value(4), 30);
    }

    #[test]
    fn test_cpu_shift_lead_negative() {
        // lead(col, 1) = shift(col, -1): out[i] = data[i+1], last row is null
        let input = vec![10i64, 20, 30, 40, 50];
        let result = cpu_shift_typed(&input, -1, 5, |vals, nulls| {
            Arc::new(Int64Array::new(vals.into(), Some(nulls))) as ArrayRef
        });
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 20);
        assert_eq!(arr.value(1), 30);
        assert_eq!(arr.value(2), 40);
        assert_eq!(arr.value(3), 50);
        assert!(arr.is_null(4));
    }

    #[test]
    fn test_cpu_shift_zero() {
        // shift(0): identity
        let input = vec![10i64, 20, 30];
        let result = cpu_shift_typed(&input, 0, 3, |vals, nulls| {
            Arc::new(Int64Array::new(vals.into(), Some(nulls))) as ArrayRef
        });
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 10);
        assert_eq!(arr.value(1), 20);
        assert_eq!(arr.value(2), 30);
    }

    #[test]
    fn test_cpu_diff_period_1() {
        let input = vec![10i64, 30, 60, 100];
        let result = cpu_diff_typed_i64(&input, 1, 4);
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(arr.is_null(0));
        assert_eq!(arr.value(1), 20);  // 30 - 10
        assert_eq!(arr.value(2), 30);  // 60 - 30
        assert_eq!(arr.value(3), 40);  // 100 - 60
    }

    #[test]
    fn test_cpu_diff_period_2() {
        let input = vec![10i64, 20, 35, 55, 80];
        let result = cpu_diff_typed_i64(&input, 2, 5);
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(arr.is_null(0));
        assert!(arr.is_null(1));
        assert_eq!(arr.value(2), 25);  // 35 - 10
        assert_eq!(arr.value(3), 35);  // 55 - 20
        assert_eq!(arr.value(4), 45);  // 80 - 35
    }

    #[test]
    fn test_cpu_diff_f64() {
        let input = vec![1.0f64, 2.5, 4.5, 8.0];
        let result = cpu_diff_typed_f64(&input, 1, 4);
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(arr.is_null(0));
        assert!((arr.value(1) - 1.5).abs() < 1e-10);
        assert!((arr.value(2) - 2.0).abs() < 1e-10);
        assert!((arr.value(3) - 3.5).abs() < 1e-10);
    }

    #[test]
    fn test_build_null_buffer() {
        let mask = vec![1u8, 0, 1, 1, 0];
        let nb = build_null_buffer(&mask);
        assert!(nb.is_valid(0));
        assert!(!nb.is_valid(1));
        assert!(nb.is_valid(2));
        assert!(nb.is_valid(3));
        assert!(!nb.is_valid(4));
    }

    #[test]
    fn test_window_op_output_name() {
        let op = GpuWindowOp::Shift {
            column_index: 0,
            column_name: "price".to_string(),
            offset: 1,
            output_name: "prev_price".to_string(),
            data_type: DataType::Float64,
        };
        assert_eq!(op.output_name(), "prev_price");

        let op2 = GpuWindowOp::Diff {
            column_index: 0,
            column_name: "price".to_string(),
            period: 1,
            output_name: "price_change".to_string(),
            data_type: DataType::Float64,
        };
        assert_eq!(op2.output_name(), "price_change");
    }

    #[test]
    fn test_is_window_gpu_supported_type() {
        assert!(is_window_gpu_supported_type(&DataType::Int64));
        assert!(is_window_gpu_supported_type(&DataType::Float64));
        assert!(is_window_gpu_supported_type(&DataType::Int32));
        assert!(is_window_gpu_supported_type(&DataType::Float32));
        assert!(!is_window_gpu_supported_type(&DataType::Utf8));
        assert!(!is_window_gpu_supported_type(&DataType::Boolean));
    }

    #[test]
    fn test_shift_large_offset() {
        // Offset larger than array: all null
        let input = vec![10i64, 20, 30];
        let result = cpu_shift_typed(&input, 10, 3, |vals, nulls| {
            Arc::new(Int64Array::new(vals.into(), Some(nulls))) as ArrayRef
        });
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(arr.is_null(0));
        assert!(arr.is_null(1));
        assert!(arr.is_null(2));
    }
}
