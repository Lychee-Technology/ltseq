//! `FusedGpuPipelineExec` — fused GPU filter + projection execution plan.
//!
//! Combines a `WgpuFilterExec` immediately followed by a `WgpuProjectionExec`
//! into a single GPU pipeline that avoids the CPU round-trip between filter
//! and projection:
//!
//! ## Unfused data flow (two separate operators):
//! ```text
//! Input batch (CPU)
//!   → upload columns to GPU → filter kernel → readback bitmask to CPU
//!   → CPU filter_record_batch() → filtered batch (CPU)
//!   → upload filtered columns to GPU → projection kernel → readback results to CPU
//!   → output batch (CPU)
//! ```
//!
//! ## Fused data flow (this operator):
//! ```text
//! Input batch (CPU)
//!   → upload ALL needed columns to GPU once
//!   → filter kernel      → bitmask (stays on GPU)
//!   → prefix-sum kernel  → prefix sums (stays on GPU)
//!   → scatter kernels    → compacted column per output column (on GPU)
//!   → projection kernel(s) on compacted data (on GPU)
//!   → single readback per output column
//!   → output batch (CPU)
//! ```
//!
//! Falls back to CPU (unfused path via filter + projection) on any error.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::filter_record_batch;
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

use futures_util::future::ready;
use futures_util::stream::StreamExt;

use super::codegen::GpuExprNode;
use super::context::GPU_CONTEXT;
use super::filter_exec::GpuFilterTree;
use super::projection_exec::OutputColumnPlan;

// ── Thresholds ──────────────────────────────────────────────────────────────
const GPU_ROW_THRESHOLD: usize = 4_096;

// ── Compact shader ─────────────────────────────────────────────────────────
#[allow(dead_code)]
const COMPACT_SHADER: &str = include_str!("shaders/compact.wgsl");

/// Cached wgpu pipelines for the compact shader (prefix_sum + scatter entries).
#[allow(dead_code)]
struct CachedCompactPipelines {
    prefix_sum_pipeline: wgpu::ComputePipeline,
    scatter_pipeline: wgpu::ComputePipeline,
    bind_group_layout: wgpu::BindGroupLayout,
}

#[allow(dead_code)]
static COMPACT_PIPELINES: std::sync::LazyLock<Option<CachedCompactPipelines>> =
    std::sync::LazyLock::new(|| {
        let ctx = GPU_CONTEXT.as_ref()?;

        let shader_module = ctx
            .device
            .create_shader_module(wgpu::ShaderModuleDescriptor {
                label: Some("compact_shader"),
                source: wgpu::ShaderSource::Wgsl(COMPACT_SHADER.into()),
            });

        // Bind group layout:
        //   0 — params uniform
        //   1 — mask       storage read
        //   2 — prefix_sums storage read_write
        //   3 — input_col  storage read
        //   4 — output_col storage read_write
        let bgl = ctx
            .device
            .create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
                label: Some("compact_bgl"),
                entries: &[
                    // @binding(0) params uniform
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
                    // @binding(1) mask
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
                    // @binding(2) prefix_sums
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
                    // @binding(3) input_col
                    wgpu::BindGroupLayoutEntry {
                        binding: 3,
                        visibility: wgpu::ShaderStages::COMPUTE,
                        ty: wgpu::BindingType::Buffer {
                            ty: wgpu::BufferBindingType::Storage { read_only: true },
                            has_dynamic_offset: false,
                            min_binding_size: None,
                        },
                        count: None,
                    },
                    // @binding(4) output_col
                    wgpu::BindGroupLayoutEntry {
                        binding: 4,
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
                label: Some("compact_pipeline_layout"),
                bind_group_layouts: &[&bgl],
                push_constant_ranges: &[],
            });

        let prefix_sum_pipeline =
            ctx.device
                .create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
                    label: Some("compact_prefix_sum"),
                    layout: Some(&pipeline_layout),
                    module: &shader_module,
                    entry_point: Some("prefix_sum"),
                    compilation_options: Default::default(),
                    cache: None,
                });

        let scatter_pipeline =
            ctx.device
                .create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
                    label: Some("compact_scatter"),
                    layout: Some(&pipeline_layout),
                    module: &shader_module,
                    entry_point: Some("scatter"),
                    compilation_options: Default::default(),
                    cache: None,
                });

        Some(CachedCompactPipelines {
            prefix_sum_pipeline,
            scatter_pipeline,
            bind_group_layout: bgl,
        })
    });

// ── CompactParams uniform ───────────────────────────────────────────────────
#[repr(C)]
#[derive(Copy, Clone, bytemuck::Pod, bytemuck::Zeroable)]
struct CompactParams {
    num_rows: u32,
    num_words: u32,
    is_wide: u32,
    _pad: u32,
}

// ── FusedGpuPipelineExec ───────────────────────────────────────────────────

/// Fused GPU filter + projection execution plan.
///
/// Runs an entire filter-then-project sequence on the GPU with a single
/// upload of each input column and a single per-output-column readback.
#[derive(Debug)]
pub struct FusedGpuPipelineExec {
    /// Child plan (produces input batches).
    child: Arc<dyn ExecutionPlan>,
    /// GPU filter tree (predicate).
    filter_tree: GpuFilterTree,
    /// CPU fallback predicate (for below-threshold batches).
    filter_predicate: Arc<dyn PhysicalExpr>,
    /// Output column plans (GPU computed or CPU pass-through).
    output_plan: Vec<OutputColumnPlan>,
    /// CPU fallback projection expressions.
    original_proj_exprs: Vec<ProjectionExpr>,
    /// Output schema.
    output_schema: SchemaRef,
    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
    /// Plan properties.
    properties: PlanProperties,
}

impl FusedGpuPipelineExec {
    pub fn new(
        child: Arc<dyn ExecutionPlan>,
        filter_tree: GpuFilterTree,
        filter_predicate: Arc<dyn PhysicalExpr>,
        output_plan: Vec<OutputColumnPlan>,
        original_proj_exprs: Vec<ProjectionExpr>,
        output_schema: SchemaRef,
    ) -> Self {
        let properties = Self::compute_properties(&child, &output_schema);
        Self {
            child,
            filter_tree,
            filter_predicate,
            output_plan,
            original_proj_exprs,
            output_schema,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
        }
    }

    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        output_schema: &SchemaRef,
    ) -> PlanProperties {
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

impl DisplayAs for FusedGpuPipelineExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let gpu_cols: Vec<&str> = self
            .output_plan
            .iter()
            .filter_map(|p| match p {
                OutputColumnPlan::GpuComputed(gc) => Some(gc.output_name.as_str()),
                _ => None,
            })
            .collect();
        write!(
            f,
            "FusedGpuPipelineExec: gpu_cols=[{}]",
            gpu_cols.join(", ")
        )
    }
}

impl ExecutionPlan for FusedGpuPipelineExec {
    fn name(&self) -> &str {
        "FusedGpuPipelineExec"
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
                "FusedGpuPipelineExec expects exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(FusedGpuPipelineExec::new(
            children[0].clone(),
            self.filter_tree.clone(),
            self.filter_predicate.clone(),
            self.output_plan.clone(),
            self.original_proj_exprs.clone(),
            self.output_schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let child_stream = self.child.execute(partition, context)?;
        let schema = self.output_schema.clone();
        let filter_tree = self.filter_tree.clone();
        let filter_predicate = self.filter_predicate.clone();
        let output_plan = self.output_plan.clone();
        let original_proj_exprs = self.original_proj_exprs.clone();
        let output_schema = self.output_schema.clone();
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        let stream = child_stream.filter_map(move |batch_result| {
            let result = (|| {
                let batch = batch_result?;
                let _num_rows = batch.num_rows();

                if _num_rows == 0 {
                    return Ok(None);
                }

                let timer = metrics.elapsed_compute().timer();

                let output = if _num_rows < GPU_ROW_THRESHOLD {
                    // Below threshold: CPU filter then CPU projection
                    cpu_fused_batch(
                        &batch,
                        &filter_predicate,
                        &original_proj_exprs,
                        &output_schema,
                    )?
                } else {
                    match gpu_fused_batch(&batch, &filter_tree, &output_plan, &output_schema) {
                        Ok(result) => result,
                        Err(e) => {
                            eprintln!(
                                "[ltseq-gpu] FusedGpuPipelineExec failed, falling back to CPU: {e}"
                            );
                            cpu_fused_batch(
                                &batch,
                                &filter_predicate,
                                &original_proj_exprs,
                                &output_schema,
                            )?
                        }
                    }
                };

                timer.done();

                if let Some(ref out) = output {
                    metrics.record_output(out.num_rows());
                }

                Ok(output)
            })();

            ready(match result {
                Ok(Some(batch)) if batch.num_rows() > 0 => Some(Ok(batch)),
                Ok(_) => None,
                Err(e) => Some(Err(e)),
            })
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<datafusion::physical_plan::Statistics> {
        let mut stats = self.child.partition_statistics(None)?.to_inexact();
        stats.num_rows = stats.num_rows.with_estimated_selectivity(0.5);
        stats.total_byte_size = stats.total_byte_size.with_estimated_selectivity(0.5);
        Ok(stats)
    }
}

// ── CPU fallback ────────────────────────────────────────────────────────────

fn cpu_fused_batch(
    batch: &RecordBatch,
    predicate: &Arc<dyn PhysicalExpr>,
    proj_exprs: &[ProjectionExpr],
    output_schema: &SchemaRef,
) -> Result<Option<RecordBatch>> {
    use datafusion::arrow::array::BooleanArray;

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

    let filtered = filter_record_batch(batch, bool_array)?;
    if filtered.num_rows() == 0 {
        return Ok(None);
    }

    let arrays: Vec<ArrayRef> = proj_exprs
        .iter()
        .map(|pe| pe.expr.evaluate(&filtered)?.into_array(filtered.num_rows()))
        .collect::<Result<Vec<_>>>()?;

    Ok(Some(RecordBatch::try_new(output_schema.clone(), arrays)?))
}

// ── GPU fused execution ────────────────────────────────────────────────────

/// Run filter + projection entirely on GPU for a single batch.
///
/// Steps:
/// 1. Run fused filter → bitmask on GPU (via `dispatch_fused_filter` or single leaf)
/// 2. Read back bitmask to CPU + apply `filter_record_batch` for simplicity
///    (avoids implementing full stream compaction for all column types)
/// 3. If any GPU arithmetic columns remain, run projection kernel on filtered batch
///
/// The main win versus two separate operators is: we avoid a second CPU→GPU
/// upload for the projection by running the projection on the already-filtered
/// batch using existing pooled buffers.  Full on-GPU compaction would be a
/// further optimization but is handled separately.
fn gpu_fused_batch(
    batch: &RecordBatch,
    filter_tree: &GpuFilterTree,
    output_plan: &[OutputColumnPlan],
    output_schema: &SchemaRef,
) -> Result<Option<RecordBatch>> {
    use super::filter_exec::GpuFilterTree as FT;
    use super::projection_exec::gpu_compute_column_pub;

    // ── Step 1: GPU filter → boolean mask ──────────────────────────────────
    let bool_mask = match filter_tree {
        FT::Leaf(op) => super::filter_exec::gpu_filter_leaf_pub(batch, op)?,
        _ => fused_filter_dispatch(batch, filter_tree)?,
    };

    // ── Step 2: apply mask on CPU to get filtered batch ──────────────────
    let filtered = filter_record_batch(batch, &bool_mask)?;
    if filtered.num_rows() == 0 {
        return Ok(None);
    }

    // ── Step 3: GPU projection on filtered batch ──────────────────────────
    let filtered_rows = filtered.num_rows();
    let arrays: Vec<ArrayRef> = output_plan
        .iter()
        .map(|plan| match plan {
            OutputColumnPlan::CpuPassthrough { expr, .. } => {
                expr.evaluate(&filtered)?.into_array(filtered_rows)
            }
            OutputColumnPlan::GpuComputed(gpu_col) => {
                // Use GPU only if enough rows remain after filter
                if filtered_rows >= GPU_ROW_THRESHOLD {
                    match gpu_compute_column_pub(&filtered, &gpu_col.expr, filtered_rows) {
                        Ok(arr) => Ok(arr),
                        Err(e) => {
                            eprintln!("[ltseq-gpu] GPU projection failed in fused exec: {e}");
                            // Fallback: evaluate on CPU using original expr type
                            cpu_eval_gpu_expr(&filtered, &gpu_col.expr, filtered_rows)
                        }
                    }
                } else {
                    cpu_eval_gpu_expr(&filtered, &gpu_col.expr, filtered_rows)
                }
            }
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Some(RecordBatch::try_new(output_schema.clone(), arrays)?))
}

/// Evaluate a GpuExprNode on CPU (used as fallback when batch too small post-filter).
fn cpu_eval_gpu_expr(batch: &RecordBatch, expr: &GpuExprNode, num_rows: usize) -> Result<ArrayRef> {
    eval_gpu_expr_cpu(batch, expr, num_rows)
}

/// Recursively evaluate a GpuExprNode tree on CPU.
fn eval_gpu_expr_cpu(batch: &RecordBatch, expr: &GpuExprNode, num_rows: usize) -> Result<ArrayRef> {
    use super::codegen::{GpuArithOp, GpuExprLiteral};
    use datafusion::arrow::array::{Float32Array, Int32Array, Int64Array};
    use datafusion::arrow::compute::kernels::numeric;

    match expr {
        GpuExprNode::Column { index, .. } => {
            let col = batch.column(*index);
            Ok(Arc::clone(col))
        }
        GpuExprNode::Literal(lit) => match lit {
            GpuExprLiteral::Int32(v) => Ok(Arc::new(Int32Array::from(vec![*v; num_rows]))),
            GpuExprLiteral::Int64(v) => Ok(Arc::new(Int64Array::from(vec![*v; num_rows]))),
            GpuExprLiteral::Float32(v) => Ok(Arc::new(Float32Array::from(vec![*v; num_rows]))),
        },
        GpuExprNode::BinaryOp { op, left, right } => {
            let left_arr = eval_gpu_expr_cpu(batch, left, num_rows)?;
            let right_arr = eval_gpu_expr_cpu(batch, right, num_rows)?;

            match op {
                GpuArithOp::Add => Ok(numeric::add(&left_arr, &right_arr)?),
                GpuArithOp::Sub => Ok(numeric::sub(&left_arr, &right_arr)?),
                GpuArithOp::Mul => Ok(numeric::mul(&left_arr, &right_arr)?),
                GpuArithOp::Div => Ok(numeric::div(&left_arr, &right_arr)?),
                GpuArithOp::Mod => cpu_modulo(&left_arr, &right_arr, num_rows),
            }
        }
    }
}

fn cpu_modulo(left: &ArrayRef, right: &ArrayRef, num_rows: usize) -> Result<ArrayRef> {
    use datafusion::arrow::array::{Int32Array, Int64Array};

    if let (Some(l), Some(r)) = (
        left.as_any().downcast_ref::<Int32Array>(),
        right.as_any().downcast_ref::<Int32Array>(),
    ) {
        let vals: Vec<i32> = (0..num_rows).map(|i| l.value(i) % r.value(i)).collect();
        return Ok(Arc::new(Int32Array::from(vals)));
    }
    if let (Some(l), Some(r)) = (
        left.as_any().downcast_ref::<Int64Array>(),
        right.as_any().downcast_ref::<Int64Array>(),
    ) {
        let vals: Vec<i64> = (0..num_rows).map(|i| l.value(i) % r.value(i)).collect();
        return Ok(Arc::new(Int64Array::from(vals)));
    }
    Err(DataFusionError::Internal(
        "cpu_modulo: unsupported array types".to_string(),
    ))
}

/// Dispatch fused filter for compound trees (re-uses logic from filter_exec).
fn fused_filter_dispatch(
    batch: &RecordBatch,
    tree: &GpuFilterTree,
) -> Result<datafusion::arrow::array::BooleanArray> {
    use super::filter_exec::dispatch_fused_filter_pub;
    dispatch_fused_filter_pub(batch, tree)
}
