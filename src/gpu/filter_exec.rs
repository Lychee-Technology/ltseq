//! `WgpuFilterExec` — GPU-accelerated filter execution plan.
//!
//! Replaces DataFusion's `FilterExec` for simple column-vs-literal predicates
//! on GPU-compatible types (i32, i64, f32). Falls back to CPU evaluation when:
//! - Batch row count is below threshold
//! - GPU execution fails for any reason
//!
//! The execution flow:
//! 1. Child plan produces `RecordBatch` stream
//! 2. Per batch: upload column data → dispatch WGSL shader → read back bitmask
//! 3. Apply bitmask to filter the batch (same as `filter_record_batch`)

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, LazyLock, Mutex};

use datafusion::arrow::array::{Array, BooleanArray, Float32Array, Int32Array, Int64Array};
use datafusion::arrow::buffer::BooleanBuffer;
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};

use futures_util::future::ready;
use futures_util::stream::StreamExt;
use wgpu::util::DeviceExt;

use super::buffers::{self, FilterParams, OP_EQ, OP_GT, OP_GTE, OP_LT, OP_LTE, OP_NEQ};
use super::context::GPU_CONTEXT;
use super::fused_filter;

// ── Minimum row count to use GPU (below this, CPU is faster) ───────────
const GPU_ROW_THRESHOLD: usize = 4_096;

// ── WGSL shader source (embedded at compile time) ──────────────────────
const FILTER_SHADER: &str = include_str!("shaders/filter.wgsl");

// ── Cached GPU pipeline (shader, layout, pipeline — compiled once) ─────
/// Cached wgpu compute pipeline for the filter shader.
///
/// Creating a shader module + pipeline is expensive (involves WGSL → SPIR-V
/// compilation). We cache these objects globally since the filter shader is
/// the same for all filter operations — only the data buffers and uniform
/// params change per dispatch.
struct CachedFilterPipeline {
    compute_pipeline: wgpu::ComputePipeline,
    bind_group_layout: wgpu::BindGroupLayout,
}

/// Global cached filter pipeline — initialized lazily on first GPU dispatch.
static FILTER_PIPELINE: LazyLock<Option<CachedFilterPipeline>> = LazyLock::new(|| {
    let ctx = GPU_CONTEXT.as_ref()?;

    let shader_module = ctx
        .device
        .create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("filter_shader"),
            source: wgpu::ShaderSource::Wgsl(FILTER_SHADER.into()),
        });

    let bind_group_layout = ctx
        .device
        .create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("filter_bgl"),
            entries: &[
                // @binding(0): params uniform
                wgpu::BindGroupLayoutEntry {
                    binding: 0,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Uniform,
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
                // @binding(1): col_data storage (read-only)
                wgpu::BindGroupLayoutEntry {
                    binding: 1,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: true },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
                // @binding(2): out_mask storage (read-write)
                wgpu::BindGroupLayoutEntry {
                    binding: 2,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: false },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
            ],
        });

    let pipeline_layout = ctx
        .device
        .create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("filter_pipeline_layout"),
            bind_group_layouts: &[&bind_group_layout],
            push_constant_ranges: &[],
        });

    let compute_pipeline = ctx
        .device
        .create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some("filter_pipeline"),
            layout: Some(&pipeline_layout),
            module: &shader_module,
            entry_point: Some("main"),
            compilation_options: Default::default(),
            cache: None,
        });

    Some(CachedFilterPipeline {
        compute_pipeline,
        bind_group_layout,
    })
});

// ── Fused filter pipeline cache ─────────────────────────────────────────

/// A compiled pipeline for a specific predicate-tree *structure* (keyed by
/// structural hash).  Literals are passed at runtime via the uniform buffer.
struct CachedFusedPipeline {
    compute_pipeline: wgpu::ComputePipeline,
    bind_group_layout: wgpu::BindGroupLayout,
    /// The ordered list of (column_index, col_type_tag) that determine which
    /// input batches columns to upload, in binding order (1…N).
    unique_cols: Vec<(usize, u8)>,
}

/// Global cache: structural_hash → compiled pipeline.
static FUSED_FILTER_CACHE: LazyLock<Mutex<HashMap<u64, Arc<CachedFusedPipeline>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Look up or compile a fused pipeline for `tree`.
fn get_or_create_fused_pipeline(tree: &GpuFilterTree) -> Result<Arc<CachedFusedPipeline>> {
    let hash = fused_filter::structural_hash(tree);

    // Fast path: already compiled
    {
        let cache = FUSED_FILTER_CACHE.lock().unwrap();
        if let Some(entry) = cache.get(&hash) {
            return Ok(entry.clone());
        }
    }

    // Slow path: generate + compile shader
    let ctx = GPU_CONTEXT
        .as_ref()
        .ok_or_else(|| DataFusionError::Internal("GPU context not available".to_string()))?;

    let (wgsl_src, unique_cols) = fused_filter::generate_fused_filter_wgsl(tree);

    let shader_module = ctx
        .device
        .create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("fused_filter_shader"),
            source: wgpu::ShaderSource::Wgsl(wgsl_src.into()),
        });

    // Build bind group layout:
    //   binding 0 — uniform (FusedFilterParams)
    //   binding 1…N — storage read (one per unique column)
    //   binding N+1 — storage read_write (output mask)
    let n_cols = unique_cols.len();
    let mut bgl_entries: Vec<wgpu::BindGroupLayoutEntry> = Vec::with_capacity(n_cols + 2);

    bgl_entries.push(wgpu::BindGroupLayoutEntry {
        binding: 0,
        visibility: wgpu::ShaderStages::COMPUTE,
        ty: wgpu::BindingType::Buffer {
            ty: wgpu::BufferBindingType::Uniform,
            has_dynamic_offset: false,
            min_binding_size: None,
        },
        count: None,
    });
    for i in 0..n_cols {
        bgl_entries.push(wgpu::BindGroupLayoutEntry {
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
    bgl_entries.push(wgpu::BindGroupLayoutEntry {
        binding: (n_cols + 1) as u32,
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
            label: Some("fused_filter_bgl"),
            entries: &bgl_entries,
        });

    let pipeline_layout = ctx
        .device
        .create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("fused_filter_pipeline_layout"),
            bind_group_layouts: &[&bind_group_layout],
            push_constant_ranges: &[],
        });

    let compute_pipeline = ctx
        .device
        .create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some("fused_filter_pipeline"),
            layout: Some(&pipeline_layout),
            module: &shader_module,
            entry_point: Some("main"),
            compilation_options: Default::default(),
            cache: None,
        });

    let entry = Arc::new(CachedFusedPipeline {
        compute_pipeline,
        bind_group_layout,
        unique_cols,
    });

    FUSED_FILTER_CACHE
        .lock()
        .unwrap()
        .insert(hash, entry.clone());

    Ok(entry)
}

/// Execute the fused filter: one GPU dispatch for the entire predicate tree.
fn dispatch_fused_filter(batch: &RecordBatch, tree: &GpuFilterTree) -> Result<BooleanArray> {
    let ctx = GPU_CONTEXT
        .as_ref()
        .ok_or_else(|| DataFusionError::Internal("GPU context not available".to_string()))?;

    let pipeline = get_or_create_fused_pipeline(tree)?;
    let num_rows = batch.num_rows();

    // Build and upload the uniform params buffer
    let leaves = fused_filter::collect_leaves(tree);
    let params_bytes = fused_filter::build_fused_params(num_rows as u32, &leaves);
    let params_buf = ctx
        .device
        .create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("fused_filter_params"),
            contents: &params_bytes,
            usage: wgpu::BufferUsages::UNIFORM | wgpu::BufferUsages::COPY_DST,
        });

    // Upload each unique column using pooled buffers
    let mut col_bufs: Vec<wgpu::Buffer> = Vec::with_capacity(pipeline.unique_cols.len());
    for &(col_idx, col_type_tag) in &pipeline.unique_cols {
        let col = batch.column(col_idx);
        let buf = match col_type_tag {
            0 => {
                // i32
                let arr = col.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                    DataFusionError::Internal(format!("Expected Int32Array at column {col_idx}"))
                })?;
                buffers::upload_i32_column_pooled(ctx, arr)
            }
            1 => {
                // i64
                let arr = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    DataFusionError::Internal(format!("Expected Int64Array at column {col_idx}"))
                })?;
                buffers::upload_i64_column_pooled(ctx, arr)
            }
            2 => {
                // f32
                let arr = col.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                    DataFusionError::Internal(format!("Expected Float32Array at column {col_idx}"))
                })?;
                buffers::upload_f32_column_pooled(ctx, arr)
            }
            _ => {
                return Err(DataFusionError::Internal(
                    "Unknown column type tag in fused filter".to_string(),
                ))
            }
        };
        col_bufs.push(buf);
    }

    // Output mask + staging — both pooled
    let mask_buf = buffers::acquire_output_mask_buffer(ctx, num_rows);
    let staging_buf = buffers::acquire_staging_buffer(ctx, num_rows);

    // Build bind group
    let mut bg_entries: Vec<wgpu::BindGroupEntry> =
        Vec::with_capacity(pipeline.unique_cols.len() + 2);
    bg_entries.push(wgpu::BindGroupEntry {
        binding: 0,
        resource: params_buf.as_entire_binding(),
    });
    for (i, col_buf) in col_bufs.iter().enumerate() {
        bg_entries.push(wgpu::BindGroupEntry {
            binding: (i + 1) as u32,
            resource: col_buf.as_entire_binding(),
        });
    }
    bg_entries.push(wgpu::BindGroupEntry {
        binding: (pipeline.unique_cols.len() + 1) as u32,
        resource: mask_buf.as_entire_binding(),
    });

    let bind_group = ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: Some("fused_filter_bg"),
        layout: &pipeline.bind_group_layout,
        entries: &bg_entries,
    });

    // Encode + dispatch
    let mut encoder = ctx
        .device
        .create_command_encoder(&wgpu::CommandEncoderDescriptor {
            label: Some("fused_filter_encoder"),
        });
    {
        let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
            label: Some("fused_filter_pass"),
            timestamp_writes: None,
        });
        pass.set_pipeline(&pipeline.compute_pipeline);
        pass.set_bind_group(0, &bind_group, &[]);
        let num_workgroups = (num_rows as u32 + 255) / 256;
        pass.dispatch_workgroups(num_workgroups, 1, 1);
    }

    let mask_byte_size = ((num_rows + 31) / 32 * 4) as u64;
    encoder.copy_buffer_to_buffer(&mask_buf, 0, &staging_buf, 0, mask_byte_size);
    ctx.queue.submit(std::iter::once(encoder.finish()));

    let result = buffers::bitmask_to_boolean_array(ctx, &staging_buf, num_rows);

    // Return pooled buffers
    for buf in col_bufs {
        ctx.buffer_pool.release(buf);
    }
    ctx.buffer_pool.release(mask_buf);
    ctx.buffer_pool.release(staging_buf);

    Ok(result)
}

/// Comparison operator (matches WGSL op codes).
#[derive(Debug, Clone, Copy)]
pub enum CompareOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl CompareOp {
    pub fn to_wgsl_op(self) -> u32 {
        match self {
            CompareOp::Eq => OP_EQ,
            CompareOp::NotEq => OP_NEQ,
            CompareOp::Lt => OP_LT,
            CompareOp::LtEq => OP_LTE,
            CompareOp::Gt => OP_GT,
            CompareOp::GtEq => OP_GTE,
        }
    }

    /// Flip the operator for reversed operand order (Literal op Column → Column flipped_op Literal).
    pub fn flip(self) -> Self {
        match self {
            CompareOp::Eq => CompareOp::Eq,
            CompareOp::NotEq => CompareOp::NotEq,
            CompareOp::Lt => CompareOp::Gt,
            CompareOp::LtEq => CompareOp::GtEq,
            CompareOp::Gt => CompareOp::Lt,
            CompareOp::GtEq => CompareOp::LtEq,
        }
    }
}

/// Literal value for GPU comparison.
#[derive(Debug, Clone)]
pub enum GpuLiteral {
    Int32(i32),
    Int64(i64),
    Float32(f32),
}

/// A fully-resolved GPU filter operation: column index, comparison op, literal value.
#[derive(Debug, Clone)]
pub struct GpuFilterOp {
    pub column_index: usize,
    pub op: CompareOp,
    pub literal: GpuLiteral,
}

/// A tree of GPU filter operations, supporting compound predicates (AND/OR/NOT).
///
/// Each leaf is a simple `GpuFilterOp`. Interior nodes combine sub-expressions
/// with AND, OR, or NOT. Each leaf produces an independent GPU bitmask dispatch;
/// interior nodes combine bitmasks on CPU (bitwise AND/OR/NOT on u32 words).
#[derive(Debug, Clone)]
pub enum GpuFilterTree {
    /// A single column-vs-literal comparison.
    Leaf(GpuFilterOp),
    /// Logical AND of two sub-trees. Both are GPU-evaluated and masks ANDed.
    And(Box<GpuFilterTree>, Box<GpuFilterTree>),
    /// Logical OR of two sub-trees. Both are GPU-evaluated and masks ORed.
    Or(Box<GpuFilterTree>, Box<GpuFilterTree>),
    /// Logical NOT of a sub-tree. GPU-evaluated, mask inverted.
    Not(Box<GpuFilterTree>),
}

// ── WgpuFilterExec ─────────────────────────────────────────────────────

/// GPU-accelerated filter execution plan.
///
/// Wraps a child `ExecutionPlan` and applies a compound predicate tree
/// using wgpu compute shaders. Each leaf predicate (column-vs-literal
/// comparison) dispatches an independent GPU shader; interior AND/OR/NOT
/// nodes combine bitmasks on CPU. Falls back to CPU evaluation via the
/// original `predicate` when GPU execution is not beneficial or fails.
#[derive(Debug)]
pub struct WgpuFilterExec {
    /// The child plan producing input batches.
    child: Arc<dyn ExecutionPlan>,
    /// The GPU filter tree (compound predicate with AND/OR/NOT).
    gpu_filter: GpuFilterTree,
    /// The original DataFusion predicate (for CPU fallback).
    predicate: Arc<dyn PhysicalExpr>,
    /// Execution metrics (elapsed compute, output rows).
    metrics: ExecutionPlanMetricsSet,
    /// Cached plan properties (schema, partitioning, ordering).
    properties: PlanProperties,
}

impl WgpuFilterExec {
    pub fn new(
        child: Arc<dyn ExecutionPlan>,
        gpu_filter: GpuFilterTree,
        predicate: Arc<dyn PhysicalExpr>,
    ) -> Self {
        let properties = Self::compute_properties(&child);
        Self {
            child,
            gpu_filter,
            predicate,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
        }
    }

    /// Accessor for the GPU filter tree (for `GpuFusionRule`).
    pub fn gpu_filter(&self) -> &GpuFilterTree {
        &self.gpu_filter
    }

    /// Accessor for the CPU fallback predicate (for `GpuFusionRule`).
    pub fn predicate(&self) -> &Arc<dyn PhysicalExpr> {
        &self.predicate
    }

    /// Accessor for the child plan (for `GpuFusionRule`).
    pub fn child(&self) -> &Arc<dyn ExecutionPlan> {
        &self.child
    }

    /// Compute plan properties — filter preserves ordering, partitioning,
    /// emission type, and boundedness from its input.
    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            EmissionType::Incremental,
            input.boundedness(),
        )
    }
}

impl DisplayAs for WgpuFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "WgpuFilterExec: {}",
            format_filter_tree(&self.gpu_filter)
        )
    }
}

/// Format a GpuFilterTree for display.
fn format_filter_tree(tree: &GpuFilterTree) -> String {
    match tree {
        GpuFilterTree::Leaf(op) => {
            format!("col[{}] {:?} {:?}", op.column_index, op.op, op.literal)
        }
        GpuFilterTree::And(left, right) => {
            format!(
                "({} AND {})",
                format_filter_tree(left),
                format_filter_tree(right)
            )
        }
        GpuFilterTree::Or(left, right) => {
            format!(
                "({} OR {})",
                format_filter_tree(left),
                format_filter_tree(right)
            )
        }
        GpuFilterTree::Not(inner) => {
            format!("NOT {}", format_filter_tree(inner))
        }
    }
}

impl ExecutionPlan for WgpuFilterExec {
    fn name(&self) -> &str {
        "WgpuFilterExec"
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
        // Filter preserves the ordering of its input (same as CPU FilterExec)
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "WgpuFilterExec expects exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(WgpuFilterExec::new(
            children[0].clone(),
            self.gpu_filter.clone(),
            self.predicate.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let child_stream = self.child.execute(partition, context)?;
        let schema = child_stream.schema();
        let gpu_filter = self.gpu_filter.clone();
        let predicate = self.predicate.clone();
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        let stream = child_stream.filter_map(move |batch_result| {
            let result = (|| {
                let batch = batch_result?;
                let num_rows = batch.num_rows();

                // Skip empty input batches
                if num_rows == 0 {
                    return Ok(None);
                }

                let timer = metrics.elapsed_compute().timer();

                // Below threshold → CPU fallback
                let filtered = if num_rows < GPU_ROW_THRESHOLD {
                    cpu_filter_batch(&batch, &predicate)?
                } else {
                    // Try GPU, fall back to CPU on error
                    match gpu_filter_batch_tree(&batch, &gpu_filter) {
                        Ok(filtered) => filtered,
                        Err(e) => {
                            eprintln!("[ltseq-gpu] GPU filter failed, falling back to CPU: {e}");
                            cpu_filter_batch(&batch, &predicate)?
                        }
                    }
                };

                timer.done();
                metrics.record_output(filtered.num_rows());

                // Skip empty output batches (matches FilterExec behavior)
                if filtered.num_rows() == 0 {
                    Ok(None)
                } else {
                    Ok(Some(filtered))
                }
            })();

            ready(match result {
                Ok(Some(batch)) => Some(Ok(batch)),
                Ok(None) => None, // skip empty batches
                Err(e) => Some(Err(e)),
            })
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<datafusion::physical_plan::Statistics> {
        // Estimate 50% selectivity (conservative default for unknown predicates)
        let mut stats = self.child.partition_statistics(None)?.to_inexact();
        stats.num_rows = stats.num_rows.with_estimated_selectivity(0.5);
        stats.total_byte_size = stats.total_byte_size.with_estimated_selectivity(0.5);
        Ok(stats)
    }
}

// ── CPU fallback ───────────────────────────────────────────────────────

/// Evaluate the predicate on CPU and filter the batch.
fn cpu_filter_batch(batch: &RecordBatch, predicate: &Arc<dyn PhysicalExpr>) -> Result<RecordBatch> {
    let eval_result = predicate.evaluate(batch)?;
    let bool_array = eval_result
        .into_array(batch.num_rows())
        .map_err(|e| DataFusionError::Execution(format!("Failed to convert to array: {e}")))?;
    let bool_array = bool_array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("Predicate did not return BooleanArray".to_string())
        })?;
    Ok(filter_record_batch(batch, bool_array)?)
}

// ── GPU filter execution (compound tree) ───────────────────────────────

/// Evaluate a compound GPU filter tree on a batch, producing a filtered RecordBatch.
///
/// Recursively evaluates the tree:
/// - Leaf nodes dispatch a GPU shader and produce a bitmask
/// - AND nodes combine sub-tree bitmasks with bitwise AND
/// - OR nodes combine sub-tree bitmasks with bitwise OR
/// - NOT nodes invert the sub-tree bitmask
fn gpu_filter_batch_tree(batch: &RecordBatch, tree: &GpuFilterTree) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let bool_mask = eval_filter_tree(batch, tree, num_rows)?;
    Ok(filter_record_batch(batch, &bool_mask)?)
}

/// Recursively evaluate a GpuFilterTree, returning a BooleanArray bitmask.
///
/// For compound (non-leaf) trees, we use the fused path: a single GPU dispatch
/// evaluates the entire predicate tree in one pass, avoiding N-1 intermediate
/// GPU round-trips.  Single leaves fall through to the original leaf path.
fn eval_filter_tree(
    batch: &RecordBatch,
    tree: &GpuFilterTree,
    _num_rows: usize,
) -> Result<BooleanArray> {
    match tree {
        // Single leaf: use the existing per-leaf path (no overhead from fused codegen)
        GpuFilterTree::Leaf(gpu_op) => gpu_filter_to_mask(batch, gpu_op),
        // Compound tree: fused single-dispatch path
        _ => dispatch_fused_filter(batch, tree),
    }
}

/// Run a single GPU filter leaf, returning a BooleanArray bitmask.
fn gpu_filter_to_mask(batch: &RecordBatch, gpu_op: &GpuFilterOp) -> Result<BooleanArray> {
    let ctx = GPU_CONTEXT
        .as_ref()
        .ok_or_else(|| DataFusionError::Internal("GPU context not available".to_string()))?;

    let pipeline = FILTER_PIPELINE.as_ref().ok_or_else(|| {
        DataFusionError::Internal("GPU filter pipeline not available".to_string())
    })?;

    let num_rows = batch.num_rows();
    let column = batch.column(gpu_op.column_index);

    // Build FilterParams based on column type
    let params = match &gpu_op.literal {
        GpuLiteral::Int32(v) => FilterParams::for_i32(gpu_op.op.to_wgsl_op(), *v, num_rows as u32),
        GpuLiteral::Int64(v) => FilterParams::for_i64(gpu_op.op.to_wgsl_op(), *v, num_rows as u32),
        GpuLiteral::Float32(v) => {
            FilterParams::for_f32(gpu_op.op.to_wgsl_op(), *v, num_rows as u32)
        }
    };

    // Upload column data to GPU (pooled)
    let col_buffer = upload_column_for_op(ctx, column.as_ref(), &gpu_op.literal)?;

    // Create per-dispatch buffers (params non-pooled; mask + staging pooled)
    let params_buffer = buffers::create_params_buffer(ctx, &params);
    let mask_buffer = buffers::acquire_output_mask_buffer(ctx, num_rows);
    let staging_buffer = buffers::acquire_staging_buffer(ctx, num_rows);

    // Create bind group for this dispatch (uses cached layout)
    let bind_group = ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: Some("filter_bind_group"),
        layout: &pipeline.bind_group_layout,
        entries: &[
            wgpu::BindGroupEntry {
                binding: 0,
                resource: params_buffer.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 1,
                resource: col_buffer.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 2,
                resource: mask_buffer.as_entire_binding(),
            },
        ],
    });

    // Encode and dispatch (uses cached pipeline)
    let mut encoder = ctx
        .device
        .create_command_encoder(&wgpu::CommandEncoderDescriptor {
            label: Some("filter_encoder"),
        });

    {
        let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
            label: Some("filter_pass"),
            timestamp_writes: None,
        });
        pass.set_pipeline(&pipeline.compute_pipeline);
        pass.set_bind_group(0, &bind_group, &[]);
        // Workgroup size is 256 (matching WGSL @workgroup_size(256))
        let num_workgroups = (num_rows as u32 + 255) / 256;
        pass.dispatch_workgroups(num_workgroups, 1, 1);
    }

    // Copy mask buffer → staging buffer for CPU readback
    let mask_byte_size = ((num_rows + 31) / 32 * 4) as u64;
    encoder.copy_buffer_to_buffer(&mask_buffer, 0, &staging_buffer, 0, mask_byte_size);

    ctx.queue.submit(std::iter::once(encoder.finish()));

    // Read back bitmask
    let result = buffers::bitmask_to_boolean_array(ctx, &staging_buffer, num_rows);

    // Return pooled buffers
    ctx.buffer_pool.release(col_buffer);
    ctx.buffer_pool.release(mask_buffer);
    ctx.buffer_pool.release(staging_buffer);

    Ok(result)
}

// ── Bitmask combination utilities ──────────────────────────────────────

/// Combine two BooleanArrays with bitwise AND.
#[allow(dead_code)]
fn combine_masks_and(left: &BooleanArray, right: &BooleanArray, num_rows: usize) -> BooleanArray {
    let left_buf = left.values();
    let right_buf = right.values();
    let left_bytes = left_buf.inner().as_slice();
    let right_bytes = right_buf.inner().as_slice();

    let num_bytes = (num_rows + 7) / 8;
    let mut result_bytes = vec![0u8; num_bytes];
    for i in 0..num_bytes {
        result_bytes[i] = left_bytes[i] & right_bytes[i];
    }

    let bool_buf = BooleanBuffer::new(result_bytes.into(), 0, num_rows);
    BooleanArray::new(bool_buf, None)
}

/// Combine two BooleanArrays with bitwise OR.
#[allow(dead_code)]
fn combine_masks_or(left: &BooleanArray, right: &BooleanArray, num_rows: usize) -> BooleanArray {
    let left_buf = left.values();
    let right_buf = right.values();
    let left_bytes = left_buf.inner().as_slice();
    let right_bytes = right_buf.inner().as_slice();

    let num_bytes = (num_rows + 7) / 8;
    let mut result_bytes = vec![0u8; num_bytes];
    for i in 0..num_bytes {
        result_bytes[i] = left_bytes[i] | right_bytes[i];
    }

    let bool_buf = BooleanBuffer::new(result_bytes.into(), 0, num_rows);
    BooleanArray::new(bool_buf, None)
}

/// Invert a BooleanArray (bitwise NOT), respecting the valid row count.
#[allow(dead_code)]
fn invert_mask(mask: &BooleanArray, num_rows: usize) -> BooleanArray {
    let buf = mask.values();
    let bytes = buf.inner().as_slice();

    let num_bytes = (num_rows + 7) / 8;
    let mut result_bytes = vec![0u8; num_bytes];
    for i in 0..num_bytes {
        result_bytes[i] = !bytes[i];
    }

    // Mask off trailing bits beyond num_rows in the last byte
    let trailing = num_rows % 8;
    if trailing > 0 {
        result_bytes[num_bytes - 1] &= (1u8 << trailing) - 1;
    }

    let bool_buf = BooleanBuffer::new(result_bytes.into(), 0, num_rows);
    BooleanArray::new(bool_buf, None)
}

// ── Public helpers for FusedGpuPipelineExec ────────────────────────────────

/// Public wrapper for `gpu_filter_to_mask` — evaluates a single leaf predicate.
///
/// Called by `FusedGpuPipelineExec` to run the leaf filter path directly.
pub fn gpu_filter_leaf_pub(batch: &RecordBatch, op: &GpuFilterOp) -> Result<BooleanArray> {
    gpu_filter_to_mask(batch, op)
}

/// Public wrapper for `dispatch_fused_filter` — evaluates a compound predicate tree.
///
/// Called by `FusedGpuPipelineExec` to avoid duplicating the fused dispatch logic.
pub fn dispatch_fused_filter_pub(
    batch: &RecordBatch,
    tree: &GpuFilterTree,
) -> Result<BooleanArray> {
    dispatch_fused_filter(batch, tree)
}

/// Upload the correct column type based on the GpuLiteral variant (pooled).
fn upload_column_for_op(
    ctx: &super::context::WgpuContext,
    column: &dyn Array,
    literal: &GpuLiteral,
) -> Result<wgpu::Buffer> {
    match literal {
        GpuLiteral::Int32(_) => {
            let arr = column
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("Expected Int32Array for GPU filter".to_string())
                })?;
            Ok(buffers::upload_i32_column_pooled(ctx, arr))
        }
        GpuLiteral::Int64(_) => {
            let arr = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("Expected Int64Array for GPU filter".to_string())
                })?;
            Ok(buffers::upload_i64_column_pooled(ctx, arr))
        }
        GpuLiteral::Float32(_) => {
            let arr = column
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("Expected Float32Array for GPU filter".to_string())
                })?;
            Ok(buffers::upload_f32_column_pooled(ctx, arr))
        }
    }
}
