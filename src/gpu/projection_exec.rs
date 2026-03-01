//! `WgpuProjectionExec` — GPU-accelerated projection execution plan.
//!
//! Replaces DataFusion's `ProjectionExec` when derived expressions contain
//! GPU-compatible arithmetic (Add, Sub, Mul, Div, Mod on i32/i64/f32).
//! Column-only pass-through expressions remain CPU-evaluated.
//!
//! The execution flow:
//! 1. Child plan produces `RecordBatch` stream
//! 2. Per batch: upload input columns → dispatch generated WGSL shader → read back results
//! 3. Combine GPU-computed arrays with CPU pass-through columns into output batch
//!
//! Falls back to CPU evaluation (via the original projection expressions) when:
//! - Batch row count is below threshold
//! - GPU execution fails for any reason

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{ArrayRef, Float32Array, Int32Array, Int64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::projection::ProjectionExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};

use futures_util::stream::StreamExt;

use super::buffers;
use super::codegen::{generate_wgsl, shader_hash, GpuDerivedColumn, GpuExprNode, GpuType};
use super::context::GPU_CONTEXT;

// ── Minimum row count to use GPU (same as filter) ──────────────────────
const GPU_ROW_THRESHOLD: usize = 4_096;

// ── Pipeline cache ─────────────────────────────────────────────────────

/// Cached compiled pipeline for a specific expression.
struct CachedProjectionPipeline {
    compute_pipeline: wgpu::ComputePipeline,
    bind_group_layout: wgpu::BindGroupLayout,
}

/// Global pipeline cache keyed by shader hash.
///
/// Unlike the filter shader (which is a single static shader), projection
/// shaders are generated per-expression. We cache them by expression hash
/// to avoid recompiling identical shaders across batches.
static PIPELINE_CACHE: std::sync::LazyLock<Mutex<HashMap<u64, Arc<CachedProjectionPipeline>>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

/// Get or create a cached pipeline for the given expression.
fn get_or_create_pipeline(
    expr: &GpuExprNode,
    col_indices: &[usize],
) -> Result<Arc<CachedProjectionPipeline>> {
    let hash = shader_hash(expr);

    // Fast path: check cache
    {
        let cache = PIPELINE_CACHE
            .lock()
            .map_err(|e| DataFusionError::Internal(format!("Pipeline cache lock poisoned: {e}")))?;
        if let Some(pipeline) = cache.get(&hash) {
            return Ok(Arc::clone(pipeline));
        }
    }

    // Slow path: generate shader and compile pipeline
    let ctx = GPU_CONTEXT.as_ref().ok_or_else(|| {
        DataFusionError::Internal("GPU context not available for projection".to_string())
    })?;

    let (wgsl_source, _col_indices) = generate_wgsl(expr);

    let shader_module = ctx
        .device
        .create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("projection_shader"),
            source: wgpu::ShaderSource::Wgsl(wgsl_source.into()),
        });

    // Build bind group layout: params + N input columns + 1 output
    let num_input_cols = col_indices.len();
    let mut layout_entries = Vec::with_capacity(num_input_cols + 2);

    // @binding(0): params uniform
    layout_entries.push(wgpu::BindGroupLayoutEntry {
        binding: 0,
        visibility: wgpu::ShaderStages::COMPUTE,
        ty: wgpu::BindingType::Buffer {
            ty: wgpu::BufferBindingType::Uniform,
            has_dynamic_offset: false,
            min_binding_size: None,
        },
        count: None,
    });

    // @binding(1..N): input column storage buffers
    for i in 0..num_input_cols {
        layout_entries.push(wgpu::BindGroupLayoutEntry {
            binding: (i + 1) as u32,
            visibility: wgpu::ShaderStages::COMPUTE,
            ty: wgpu::BindingType::Buffer {
                ty: wgpu::BufferBindingType::Storage { read_only: true },
                has_dynamic_offset: false,
                min_binding_size: None,
            },
            count: None,
        });
    }

    // @binding(N+1): output storage buffer
    layout_entries.push(wgpu::BindGroupLayoutEntry {
        binding: (num_input_cols + 1) as u32,
        visibility: wgpu::ShaderStages::COMPUTE,
        ty: wgpu::BindingType::Buffer {
            ty: wgpu::BufferBindingType::Storage { read_only: false },
            has_dynamic_offset: false,
            min_binding_size: None,
        },
        count: None,
    });

    let bind_group_layout = ctx
        .device
        .create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("projection_bgl"),
            entries: &layout_entries,
        });

    let pipeline_layout = ctx
        .device
        .create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("projection_pipeline_layout"),
            bind_group_layouts: &[&bind_group_layout],
            push_constant_ranges: &[],
        });

    let compute_pipeline = ctx
        .device
        .create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some("projection_pipeline"),
            layout: Some(&pipeline_layout),
            module: &shader_module,
            entry_point: Some("main"),
            compilation_options: Default::default(),
            cache: None,
        });

    let cached = Arc::new(CachedProjectionPipeline {
        compute_pipeline,
        bind_group_layout,
    });

    // Insert into cache
    {
        let mut cache = PIPELINE_CACHE
            .lock()
            .map_err(|e| DataFusionError::Internal(format!("Pipeline cache lock poisoned: {e}")))?;
        cache.insert(hash, Arc::clone(&cached));
    }

    Ok(cached)
}

// ── Params uniform (just num_rows for projection) ──────────────────────

#[repr(C)]
#[derive(Copy, Clone, bytemuck::Pod, bytemuck::Zeroable)]
struct ProjectionParams {
    num_rows: u32,
    _pad1: u32,
    _pad2: u32,
    _pad3: u32,
}

// ── WgpuProjectionExec ─────────────────────────────────────────────────

/// Describes how to produce one column of the output.
#[derive(Debug, Clone)]
pub enum OutputColumnPlan {
    /// Pass through from input (evaluated on CPU via original PhysicalExpr).
    CpuPassthrough {
        /// The original projection expression.
        expr: Arc<dyn PhysicalExpr>,
        /// Output column name.
        name: String,
    },
    /// Compute on GPU using generated WGSL shader.
    GpuComputed(GpuDerivedColumn),
}

/// GPU-accelerated projection execution plan.
///
/// For each output column, either:
/// - Passes through from input using CPU evaluation (column refs, unsupported types)
/// - Computes on GPU using a generated WGSL shader (arithmetic expressions)
#[derive(Debug)]
pub struct WgpuProjectionExec {
    /// The child plan producing input batches.
    child: Arc<dyn ExecutionPlan>,
    /// Plan for each output column.
    output_plan: Vec<OutputColumnPlan>,
    /// The original projection expressions (for CPU fallback).
    original_exprs: Vec<ProjectionExpr>,
    /// Output schema.
    output_schema: SchemaRef,
    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
    /// Cached plan properties.
    properties: PlanProperties,
}

impl WgpuProjectionExec {
    pub fn new(
        child: Arc<dyn ExecutionPlan>,
        output_plan: Vec<OutputColumnPlan>,
        original_exprs: Vec<ProjectionExpr>,
        output_schema: SchemaRef,
    ) -> Self {
        let properties = Self::compute_properties(&child, &output_schema);
        Self {
            child,
            output_plan,
            original_exprs,
            output_schema,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
        }
    }

    /// Accessor for the output plan (for `GpuFusionRule`).
    pub fn output_plan(&self) -> &Vec<OutputColumnPlan> {
        &self.output_plan
    }

    /// Accessor for the original projection expressions (for `GpuFusionRule`).
    pub fn original_exprs(&self) -> &Vec<ProjectionExpr> {
        &self.original_exprs
    }

    /// Accessor for the output schema (for `GpuFusionRule`).
    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        output_schema: &SchemaRef,
    ) -> PlanProperties {
        // Use the OUTPUT schema for equivalence properties, not the input's.
        // The input schema may have fewer columns than the output (derive adds columns).
        // DataFusion's schema_check validates that properties().schema() == plan output schema.
        let eq_properties =
            datafusion::physical_expr::EquivalenceProperties::new(output_schema.clone());
        PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(),
            EmissionType::Incremental,
            input.boundedness(),
        )
    }
}

impl DisplayAs for WgpuProjectionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let gpu_cols: Vec<&str> = self
            .output_plan
            .iter()
            .filter_map(|p| match p {
                OutputColumnPlan::GpuComputed(gc) => Some(gc.output_name.as_str()),
                _ => None,
            })
            .collect();
        write!(f, "WgpuProjectionExec: gpu_cols=[{}]", gpu_cols.join(", "))
    }
}

impl ExecutionPlan for WgpuProjectionExec {
    fn name(&self) -> &str {
        "WgpuProjectionExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "WgpuProjectionExec expects exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(WgpuProjectionExec::new(
            children[0].clone(),
            self.output_plan.clone(),
            self.original_exprs.clone(),
            self.output_schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let child_stream = self.child.execute(partition, context)?;
        let output_schema = self.output_schema.clone();
        let output_plan = self.output_plan.clone();
        let original_exprs = self.original_exprs.clone();
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        let stream = child_stream.map(move |batch_result| {
            let batch = batch_result?;
            let num_rows = batch.num_rows();

            if num_rows == 0 {
                return Ok(RecordBatch::new_empty(output_schema.clone()));
            }

            let timer = metrics.elapsed_compute().timer();

            let result = if num_rows < GPU_ROW_THRESHOLD {
                // Below threshold → CPU-only
                cpu_project_batch(&batch, &original_exprs, &output_schema)?
            } else {
                // Try GPU for computed columns, CPU for pass-through
                match gpu_project_batch(&batch, &output_plan, &output_schema) {
                    Ok(result) => result,
                    Err(e) => {
                        eprintln!("[ltseq-gpu] GPU projection failed, falling back to CPU: {e}");
                        cpu_project_batch(&batch, &original_exprs, &output_schema)?
                    }
                }
            };

            timer.done();
            metrics.record_output(result.num_rows());
            Ok(result)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<datafusion::physical_plan::Statistics> {
        // Projection preserves row count
        self.child.partition_statistics(None)
    }
}

// ── CPU fallback ───────────────────────────────────────────────────────

/// Evaluate all projection expressions on CPU.
fn cpu_project_batch(
    batch: &RecordBatch,
    exprs: &[ProjectionExpr],
    output_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let arrays: Vec<ArrayRef> = exprs
        .iter()
        .map(|pe| pe.expr.evaluate(batch)?.into_array(batch.num_rows()))
        .collect::<Result<Vec<_>>>()?;

    Ok(RecordBatch::try_new(output_schema.clone(), arrays)?)
}

// ── GPU execution ──────────────────────────────────────────────────────

/// Execute the projection with GPU acceleration for computed columns.
fn gpu_project_batch(
    batch: &RecordBatch,
    output_plan: &[OutputColumnPlan],
    output_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();

    let arrays: Vec<ArrayRef> = output_plan
        .iter()
        .map(|plan| match plan {
            OutputColumnPlan::CpuPassthrough { expr, .. } => {
                expr.evaluate(batch)?.into_array(num_rows)
            }
            OutputColumnPlan::GpuComputed(gpu_col) => {
                gpu_compute_column(batch, &gpu_col.expr, num_rows)
            }
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(RecordBatch::try_new(output_schema.clone(), arrays)?)
}

/// Execute a single GPU-computed column expression.
fn gpu_compute_column(
    batch: &RecordBatch,
    expr: &GpuExprNode,
    num_rows: usize,
) -> Result<ArrayRef> {
    let ctx = GPU_CONTEXT.as_ref().ok_or_else(|| {
        DataFusionError::Internal("GPU context not available for projection".to_string())
    })?;

    let col_indices = expr.column_indices();
    let output_type = expr.output_type();

    // Get or create cached pipeline
    let pipeline = get_or_create_pipeline(expr, &col_indices)?;

    // Create params buffer
    let params = ProjectionParams {
        num_rows: num_rows as u32,
        _pad1: 0,
        _pad2: 0,
        _pad3: 0,
    };
    let params_buffer = ctx
        .device
        .create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("projection_params"),
            contents: bytemuck::bytes_of(&params),
            usage: wgpu::BufferUsages::UNIFORM | wgpu::BufferUsages::COPY_DST,
        });

    // Upload input columns (pooled)
    let mut input_buffers = Vec::with_capacity(col_indices.len());
    for &col_idx in &col_indices {
        let column = batch.column(col_idx);
        let buffer = buffers::upload_column_pooled(ctx, column.as_ref()).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Column {} has unsupported type for GPU projection",
                col_idx
            ))
        })?;
        input_buffers.push(buffer);
    }

    // Create output buffer + staging buffer (pooled)
    let output_elements = if output_type.is_wide() {
        num_rows * 2 // i64: two i32 words per element
    } else {
        num_rows
    };
    let output_size = (output_elements * 4) as u64; // 4 bytes per i32

    let output_buffer = buffers::acquire_output_buffer(ctx, output_size);
    let staging_buffer = buffers::acquire_staging_bytes(ctx, output_size);

    // Create bind group
    let mut bind_entries = Vec::with_capacity(col_indices.len() + 2);
    bind_entries.push(wgpu::BindGroupEntry {
        binding: 0,
        resource: params_buffer.as_entire_binding(),
    });
    for (i, buf) in input_buffers.iter().enumerate() {
        bind_entries.push(wgpu::BindGroupEntry {
            binding: (i + 1) as u32,
            resource: buf.as_entire_binding(),
        });
    }
    bind_entries.push(wgpu::BindGroupEntry {
        binding: (col_indices.len() + 1) as u32,
        resource: output_buffer.as_entire_binding(),
    });

    let bind_group = ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: Some("projection_bind_group"),
        layout: &pipeline.bind_group_layout,
        entries: &bind_entries,
    });

    // Encode and dispatch
    let mut encoder = ctx
        .device
        .create_command_encoder(&wgpu::CommandEncoderDescriptor {
            label: Some("projection_encoder"),
        });

    {
        let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
            label: Some("projection_pass"),
            timestamp_writes: None,
        });
        pass.set_pipeline(&pipeline.compute_pipeline);
        pass.set_bind_group(0, &bind_group, &[]);
        let num_workgroups = (num_rows as u32 + 255) / 256;
        pass.dispatch_workgroups(num_workgroups, 1, 1);
    }

    // Copy output → staging for readback
    encoder.copy_buffer_to_buffer(&output_buffer, 0, &staging_buffer, 0, output_size);
    ctx.queue.submit(std::iter::once(encoder.finish()));

    // Read back results
    let result = read_output_array(ctx, &staging_buffer, output_size, output_type, num_rows);

    // Return pooled buffers
    for buf in input_buffers {
        ctx.buffer_pool.release(buf);
    }
    ctx.buffer_pool.release(output_buffer);
    ctx.buffer_pool.release(staging_buffer);

    result
}

/// Public wrapper for `gpu_compute_column` — called by `FusedGpuPipelineExec`.
pub fn gpu_compute_column_pub(
    batch: &RecordBatch,
    expr: &GpuExprNode,
    num_rows: usize,
) -> Result<ArrayRef> {
    gpu_compute_column(batch, expr, num_rows)
}

/// Read back the GPU output buffer into an Arrow array.
fn read_output_array(
    ctx: &super::context::WgpuContext,
    staging_buffer: &wgpu::Buffer,
    byte_size: u64,
    output_type: GpuType,
    num_rows: usize,
) -> Result<ArrayRef> {
    let buffer_slice = staging_buffer.slice(..);
    let (sender, receiver) = std::sync::mpsc::channel();
    buffer_slice.map_async(wgpu::MapMode::Read, move |result| {
        sender.send(result).unwrap();
    });
    ctx.device.poll(wgpu::Maintain::Wait);
    receiver
        .recv()
        .unwrap()
        .map_err(|e| DataFusionError::Internal(format!("Failed to map staging buffer: {e:?}")))?;

    let data = buffer_slice.get_mapped_range();
    let bytes = &data[..byte_size as usize];

    let result: ArrayRef = match output_type {
        GpuType::Int32 => {
            let values: &[i32] = bytemuck::cast_slice(bytes);
            Arc::new(Int32Array::from(values[..num_rows].to_vec()))
        }
        GpuType::Float32 => {
            let values: &[f32] = bytemuck::cast_slice(bytes);
            Arc::new(Float32Array::from(values[..num_rows].to_vec()))
        }
        GpuType::Int64 => {
            // Two i32 words per element (lo, hi) → reconstitute i64
            let i32_values: &[i32] = bytemuck::cast_slice(bytes);
            let mut i64_values = Vec::with_capacity(num_rows);
            for i in 0..num_rows {
                let lo = i32_values[i * 2] as u32;
                let hi = i32_values[i * 2 + 1] as u32;
                let val = (lo as i64) | ((hi as i64) << 32);
                i64_values.push(val);
            }
            Arc::new(Int64Array::from(i64_values))
        }
    };

    drop(data);
    staging_buffer.unmap();

    Ok(result)
}

use wgpu::util::DeviceExt;
