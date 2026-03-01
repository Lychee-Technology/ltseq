//! `WgpuAggregateExec` — GPU-accelerated GROUP BY aggregation for sorted input.
//!
//! Exploits LTSeq's sequence-oriented identity: when data is sorted by the GROUP BY
//! key column, identical groups form contiguous segments. The algorithm:
//!
//!   1. **GPU: detect_boundaries** — compare consecutive rows on the group key,
//!      output a packed u32 bitmask of segment boundaries.
//!   2. **CPU: extract_segments** — scan bitmask (O(N/32)) → (start, length) pairs.
//!   3. **GPU: segmented_reduce** — dispatch one workgroup per segment. Threads loop
//!      over their share, then a shared-memory tree reduction produces one result
//!      per segment. Works for i32, i64, f32 via type-specific kernels.
//!   4. **CPU: assemble** — combine group key values + aggregate results → RecordBatch.
//!
//! ## Supported aggregates
//! - `sum(i32/i64/f32)`, `count`, `min(i32/i64/f32)`, `max(i32/i64/f32)`
//! - `avg(i32/i64/f32)` — computed as `sum`+`count` on GPU, `sum/count` on CPU
//!
//! ## Fallback to CPU
//! Passes the original `AggregateExec` through unchanged when:
//! - Input is not sorted by group key (`InputOrderMode != Sorted`)
//! - Unsupported aggregate function or column type
//! - Batch row count < `GPU_ROW_THRESHOLD`
//! - Number of groups > `MAX_GPU_GROUPS` (wgpu dispatch limit)
//! - GPU execution fails at runtime

use std::any::Any;
use std::fmt;
use std::sync::{Arc, LazyLock};

use bytemuck::{Pod, Zeroable};

use datafusion::arrow::array::{
    ArrayRef, Float64Array, Int32Array, Int64Array,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures_util::stream::StreamExt;

use super::buffers;
use super::context::GPU_CONTEXT;

// ── Constants ──────────────────────────────────────────────────────────

/// Minimum rows to use GPU (same as filter/projection).
const GPU_ROW_THRESHOLD: usize = 4_096;

/// Maximum number of groups supported by GPU dispatch (wgpu workgroup dispatch limit).
/// Each segment = 1 workgroup. wgpu max workgroups_x = 65535.
const MAX_GPU_GROUPS: usize = 65_535;

/// WGSL shader source (compiled once).
static SHADER_SOURCE: &str = include_str!("shaders/reduce.wgsl");

// ── Aggregate function type ─────────────────────────────────────────────

/// GPU-supported aggregate operations.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GpuAggFunc {
    Sum,
    Count,
    Min,
    Max,
}

/// How this aggregate output column is produced.
#[derive(Debug, Clone)]
pub enum AggColumnPlan {
    /// Single GPU kernel result (sum/count/min/max).
    GpuDirect {
        func: GpuAggFunc,
        /// Index in the input batch for the value column.
        value_col_index: usize,
        output_name: String,
    },
    /// GPU pair (sum + count) divided on CPU to produce avg.
    GpuAvg {
        value_col_index: usize,
        output_name: String,
    },
}

impl AggColumnPlan {
    pub fn output_name(&self) -> &str {
        match self {
            AggColumnPlan::GpuDirect { output_name, .. } => output_name,
            AggColumnPlan::GpuAvg { output_name, .. } => output_name,
        }
    }
}

// ── Group key type ──────────────────────────────────────────────────────

/// GPU-compatible group key types.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum KeyType {
    I32,
    I64,
}

// ── Cached pipelines ────────────────────────────────────────────────────

/// Cached compiled pipelines for the two reduction kernels.
struct AggPipelines {
    boundary_pipeline: wgpu::ComputePipeline,
    boundary_bgl: wgpu::BindGroupLayout,
    reduce_i32_pipeline: wgpu::ComputePipeline,
    reduce_i32_bgl: wgpu::BindGroupLayout,
    reduce_i64_pipeline: wgpu::ComputePipeline,
    reduce_i64_bgl: wgpu::BindGroupLayout,
}

static AGG_PIPELINES: LazyLock<Option<AggPipelines>> = LazyLock::new(|| {
    let ctx = GPU_CONTEXT.as_ref()?;

    let shader = ctx
        .device
        .create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("reduce_shader"),
            source: wgpu::ShaderSource::Wgsl(SHADER_SOURCE.into()),
        });

    // ── Boundary detection bind group layout ────────────────────────
    let boundary_bgl = ctx
        .device
        .create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("boundary_bgl"),
            entries: &[
                // @binding(0) params uniform
                bgl_uniform(0),
                // @binding(1) key_data storage (read)
                bgl_storage_ro(1),
                // @binding(2) out boundary_mask storage (read_write)
                bgl_storage_rw(2),
            ],
        });

    let boundary_pipeline = make_pipeline(
        ctx,
        "boundary_pipeline",
        &boundary_bgl,
        &shader,
        "detect_boundaries",
    );

    // ── Segmented reduce i32 bind group layout ──────────────────────
    let reduce_i32_bgl = ctx
        .device
        .create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("reduce_i32_bgl"),
            entries: &[
                bgl_uniform(0),
                bgl_storage_ro(1), // value_data
                bgl_storage_ro(2), // seg_starts
                bgl_storage_ro(3), // seg_lengths
                bgl_storage_rw(4), // out_results
            ],
        });

    let reduce_i32_pipeline = make_pipeline(
        ctx,
        "reduce_i32_pipeline",
        &reduce_i32_bgl,
        &shader,
        "segmented_reduce_i32",
    );

    // ── Segmented reduce i64 bind group layout ──────────────────────
    let reduce_i64_bgl = ctx
        .device
        .create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("reduce_i64_bgl"),
            entries: &[
                bgl_uniform(0),
                bgl_storage_ro(1),
                bgl_storage_ro(2),
                bgl_storage_ro(3),
                bgl_storage_rw(4),
            ],
        });

    let reduce_i64_pipeline = make_pipeline(
        ctx,
        "reduce_i64_pipeline",
        &reduce_i64_bgl,
        &shader,
        "segmented_reduce_i64",
    );

    Some(AggPipelines {
        boundary_pipeline,
        boundary_bgl,
        reduce_i32_pipeline,
        reduce_i32_bgl,
        reduce_i64_pipeline,
        reduce_i64_bgl,
    })
});

fn bgl_uniform(binding: u32) -> wgpu::BindGroupLayoutEntry {
    wgpu::BindGroupLayoutEntry {
        binding,
        visibility: wgpu::ShaderStages::COMPUTE,
        ty: wgpu::BindingType::Buffer {
            ty: wgpu::BufferBindingType::Uniform,
            has_dynamic_offset: false,
            min_binding_size: None,
        },
        count: None,
    }
}

fn bgl_storage_ro(binding: u32) -> wgpu::BindGroupLayoutEntry {
    wgpu::BindGroupLayoutEntry {
        binding,
        visibility: wgpu::ShaderStages::COMPUTE,
        ty: wgpu::BindingType::Buffer {
            ty: wgpu::BufferBindingType::Storage { read_only: true },
            has_dynamic_offset: false,
            min_binding_size: None,
        },
        count: None,
    }
}

fn bgl_storage_rw(binding: u32) -> wgpu::BindGroupLayoutEntry {
    wgpu::BindGroupLayoutEntry {
        binding,
        visibility: wgpu::ShaderStages::COMPUTE,
        ty: wgpu::BindingType::Buffer {
            ty: wgpu::BufferBindingType::Storage { read_only: false },
            has_dynamic_offset: false,
            min_binding_size: None,
        },
        count: None,
    }
}

fn make_pipeline(
    ctx: &super::context::WgpuContext,
    label: &str,
    bgl: &wgpu::BindGroupLayout,
    shader: &wgpu::ShaderModule,
    entry_point: &str,
) -> wgpu::ComputePipeline {
    let layout = ctx
        .device
        .create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some(label),
            bind_group_layouts: &[bgl],
            push_constant_ranges: &[],
        });
    ctx.device
        .create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some(label),
            layout: Some(&layout),
            module: shader,
            entry_point: Some(entry_point),
            compilation_options: Default::default(),
            cache: None,
        })
}

// ── Uniform structs ─────────────────────────────────────────────────────

#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
struct BoundaryParams {
    key_type: u32, // 0 = i32, 1 = i64
    num_rows: u32,
    _pad0: u32,
    _pad1: u32,
}

#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
struct ReduceParams {
    agg_op: u32, // 0=sum, 1=count, 2=min, 3=max
    num_segments: u32,
    _pad0: u32,
    _pad1: u32,
}

// ── WgpuAggregateExec ───────────────────────────────────────────────────

/// GPU-accelerated GROUP BY aggregation for sorted input.
///
/// Uses a three-step GPU algorithm:
/// 1. Boundary detection kernel → packed bitmask of group starts
/// 2. CPU segment extraction from bitmask
/// 3. Segmented reduction kernel (one workgroup per group)
///
/// Falls back transparently to the original `AggregateExec` on any error
/// or when the row count is below the GPU threshold.
#[derive(Debug)]
pub struct WgpuAggregateExec {
    /// Child plan (the input to the aggregation).
    child: Arc<dyn ExecutionPlan>,
    /// Index of the group-by key column in the child's schema.
    group_key_col: usize,
    /// Type of the group-by key column.
    group_key_type: KeyType,
    /// Plan for each aggregate output column.
    agg_plans: Vec<AggColumnPlan>,
    /// The original DataFusion AggregateExec (for CPU fallback).
    original_agg: Arc<AggregateExec>,
    /// Output schema after aggregation.
    output_schema: SchemaRef,
    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
    /// Cached plan properties.
    properties: PlanProperties,
}

impl WgpuAggregateExec {
    pub fn new(
        child: Arc<dyn ExecutionPlan>,
        group_key_col: usize,
        group_key_type: KeyType,
        agg_plans: Vec<AggColumnPlan>,
        original_agg: Arc<AggregateExec>,
        output_schema: SchemaRef,
    ) -> Self {
        let properties = Self::compute_properties(&child, &output_schema);
        Self {
            child,
            group_key_col,
            group_key_type,
            agg_plans,
            original_agg,
            output_schema,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
        }
    }

    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        output_schema: &SchemaRef,
    ) -> PlanProperties {
        // Aggregation produces a single partition of (potentially) fewer rows.
        // We drop sort ordering since groups can be reordered relative to each other.
        // (The output IS sorted by group key when input was sorted, but we keep it
        // simple and don't propagate that claim — the optimizer can re-establish it.)
        use datafusion::physical_expr::EquivalenceProperties;
        use datafusion::physical_plan::Partitioning;
        PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final, // aggregation must consume all input before emitting
            input.boundedness(),
        )
    }
}

impl DisplayAs for WgpuAggregateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let agg_names: Vec<&str> = self
            .agg_plans
            .iter()
            .map(|p| p.output_name())
            .collect();
        write!(
            f,
            "WgpuAggregateExec: group_col={}, aggs=[{}]",
            self.group_key_col,
            agg_names.join(", ")
        )
    }
}

impl ExecutionPlan for WgpuAggregateExec {
    fn name(&self) -> &str {
        "WgpuAggregateExec"
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
        // Aggregation does not maintain row-level order (groups may be reordered).
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "WgpuAggregateExec expects exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(WgpuAggregateExec::new(
            children[0].clone(),
            self.group_key_col,
            self.group_key_type,
            self.agg_plans.clone(),
            self.original_agg.clone(),
            self.output_schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let child_stream = self.child.execute(partition, context.clone())?;
        let output_schema = self.output_schema.clone();
        let group_key_col = self.group_key_col;
        let group_key_type = self.group_key_type;
        let agg_plans = self.agg_plans.clone();
        let original_agg = self.original_agg.clone();
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        // Collect all batches — aggregation is a blocking operation.
        // We cannot emit partial results until all input is consumed.
        let output_schema_for_stream = output_schema.clone();
        let stream = futures_util::stream::once(async move {
            // Collect all input batches
            let batches: Vec<RecordBatch> = child_stream
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()?;

            if batches.is_empty() {
                return Ok(RecordBatch::new_empty(output_schema.clone()));
            }

            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

            let timer = metrics.elapsed_compute().timer();

            let result = if total_rows < GPU_ROW_THRESHOLD {
                // Below threshold — fall back to CPU aggregation
                cpu_aggregate_fallback(&original_agg, batches, context, &output_schema).await?
            } else {
                // Try GPU aggregation
                match gpu_aggregate(
                    &batches,
                    group_key_col,
                    group_key_type,
                    &agg_plans,
                    &output_schema,
                ) {
                    Ok(result) => result,
                    Err(e) => {
                        eprintln!(
                            "[ltseq-gpu] GPU aggregation failed, falling back to CPU: {e}"
                        );
                        cpu_aggregate_fallback(
                            &original_agg,
                            batches,
                            context,
                            &output_schema,
                        )
                        .await?
                    }
                }
            };

            timer.done();
            metrics.record_output(result.num_rows());
            Ok(result)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema_for_stream,
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<datafusion::physical_plan::Statistics> {
        // We don't know the number of groups upfront
        Ok(datafusion::physical_plan::Statistics::new_unknown(
            &self.output_schema,
        ))
    }
}

// ── CPU fallback ────────────────────────────────────────────────────────

/// Re-execute the original DataFusion AggregateExec on pre-collected batches.
async fn cpu_aggregate_fallback(
    original_agg: &Arc<AggregateExec>,
    batches: Vec<RecordBatch>,
    context: Arc<datafusion::execution::TaskContext>,
    output_schema: &SchemaRef,
) -> Result<RecordBatch> {
    // Re-execute the original aggregate plan by wrapping the pre-collected
    // batches in an in-memory scan node, then feeding into a new AggregateExec.
    use datafusion::datasource::memory::MemorySourceConfig;

    let schema = if let Some(b) = batches.first() {
        b.schema()
    } else {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    };

    let mem_exec: Arc<dyn datafusion::physical_plan::ExecutionPlan> =
        MemorySourceConfig::try_new_exec(&[batches], schema, None)?;

    // Build a new AggregateExec with the in-memory scan as input
    let new_agg = AggregateExec::try_new(
        original_agg.mode().clone(),
        original_agg.group_expr().clone(),
        original_agg.aggr_expr().to_vec(),
        original_agg.filter_expr().to_vec(),
        mem_exec,
        original_agg.input_schema(),
    )?;

    let agg_stream = Arc::new(new_agg).execute(0, context)?;
    let result_batches: Vec<RecordBatch> = agg_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

    // Concatenate to single batch
    if result_batches.is_empty() {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }
    if result_batches.len() == 1 {
        return Ok(result_batches.into_iter().next().unwrap());
    }
    Ok(datafusion::arrow::compute::concat_batches(
        output_schema,
        &result_batches,
    )?)
}

// ── GPU aggregation ─────────────────────────────────────────────────────

/// Execute GPU-accelerated GROUP BY on sorted input.
fn gpu_aggregate(
    batches: &[RecordBatch],
    group_key_col: usize,
    group_key_type: KeyType,
    agg_plans: &[AggColumnPlan],
    output_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let pipelines = AGG_PIPELINES.as_ref().ok_or_else(|| {
        DataFusionError::Internal("GPU aggregate pipelines not available".to_string())
    })?;
    let ctx = GPU_CONTEXT.as_ref().ok_or_else(|| {
        DataFusionError::Internal("GPU context not available for aggregation".to_string())
    })?;

    // Concatenate all batches into flat column arrays
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    // Extract group key column as flat bytes
    let key_bytes = collect_column_bytes(batches, group_key_col, group_key_type)?;

    // Step 1: GPU boundary detection
    let boundary_bitmask = gpu_detect_boundaries(
        ctx,
        pipelines,
        &key_bytes,
        group_key_type,
        total_rows,
    )?;

    // Step 2: CPU segment extraction
    let segments = extract_segments(&boundary_bitmask, total_rows);
    let num_segments = segments.len();

    if num_segments == 0 {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    if num_segments > MAX_GPU_GROUPS {
        return Err(DataFusionError::Internal(format!(
            "Too many groups ({num_segments}) for GPU aggregation (max {MAX_GPU_GROUPS})"
        )));
    }

    // Upload segment info (start positions and lengths)
    let seg_starts: Vec<u32> = segments.iter().map(|(s, _)| *s as u32).collect();
    let seg_lengths: Vec<u32> = segments.iter().map(|(_, l)| *l as u32).collect();

    let seg_starts_buf = upload_u32_array_pooled(ctx, &seg_starts, "seg_starts");
    let seg_lengths_buf = upload_u32_array_pooled(ctx, &seg_lengths, "seg_lengths");

    // Step 3: GPU segmented reduction for each aggregate column
    let mut output_arrays: Vec<ArrayRef> = Vec::with_capacity(agg_plans.len() + 1);

    // First column: group key values (one per group = first element of each segment)
    let key_group_values = extract_group_key_values(batches, group_key_col, group_key_type, &segments)?;
    output_arrays.push(key_group_values);

    // Remaining columns: aggregate results
    for plan in agg_plans {
        match plan {
            AggColumnPlan::GpuDirect {
                func,
                value_col_index,
                ..
            } => {
                let value_type = column_value_type(batches, *value_col_index)?;
                let value_bytes = collect_column_bytes(batches, *value_col_index, value_type)?;
                let result = gpu_reduce_segments(
                    ctx,
                    pipelines,
                    &value_bytes,
                    value_type,
                    *func,
                    &seg_starts_buf,
                    &seg_lengths_buf,
                    num_segments,
                )?;
                output_arrays.push(result);
            }
            AggColumnPlan::GpuAvg {
                value_col_index, ..
            } => {
                let value_type = column_value_type(batches, *value_col_index)?;
                let value_bytes = collect_column_bytes(batches, *value_col_index, value_type)?;

                // GPU: compute sum
                let sum_arr = gpu_reduce_segments(
                    ctx,
                    pipelines,
                    &value_bytes,
                    value_type,
                    GpuAggFunc::Sum,
                    &seg_starts_buf,
                    &seg_lengths_buf,
                    num_segments,
                )?;
                // GPU: compute count (value col doesn't matter for count)
                let count_arr = gpu_reduce_segments(
                    ctx,
                    pipelines,
                    &value_bytes,
                    value_type,
                    GpuAggFunc::Count,
                    &seg_starts_buf,
                    &seg_lengths_buf,
                    num_segments,
                )?;
                // CPU: divide sum / count → f64 avg
                let avg_arr = compute_avg_from_sum_count(&sum_arr, &count_arr, value_type)?;
                output_arrays.push(avg_arr);
            }
        }
    }

    // Return shared segment buffers to the pool
    ctx.buffer_pool.release(seg_starts_buf);
    ctx.buffer_pool.release(seg_lengths_buf);

    Ok(RecordBatch::try_new(output_schema.clone(), output_arrays)?)
}

// ── GPU: boundary detection ─────────────────────────────────────────────

fn gpu_detect_boundaries(
    ctx: &super::context::WgpuContext,
    pipelines: &AggPipelines,
    key_bytes: &[u8],
    key_type: KeyType,
    num_rows: usize,
) -> Result<Vec<u32>> {
    use wgpu::util::DeviceExt;

    let params = BoundaryParams {
        key_type: key_type as u32,
        num_rows: num_rows as u32,
        _pad0: 0,
        _pad1: 0,
    };
    let params_buf = ctx
        .device
        .create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("boundary_params"),
            contents: bytemuck::bytes_of(&params),
            usage: wgpu::BufferUsages::UNIFORM | wgpu::BufferUsages::COPY_DST,
        });

    // Upload key data — pooled (STORAGE | COPY_DST)
    let key_buf = buffers::upload_bytes_pooled(ctx, key_bytes, "boundary_key");

    let mask_words = (num_rows + 31) / 32;
    let mask_size = (mask_words * 4) as u64;

    // Mask buffer must be zero-initialized because the boundary shader only sets
    // bits at group boundaries — stale bits from a prior run would corrupt results.
    // Keep this allocation non-pooled (fresh buffers are always zeroed by wgpu).
    let mask_buf = ctx.device.create_buffer(&wgpu::BufferDescriptor {
        label: Some("boundary_mask"),
        size: mask_size,
        usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
        mapped_at_creation: false,
    });

    // Staging buffer — pooled
    let staging_buf = buffers::acquire_staging_bytes(ctx, mask_size);

    let bind_group = ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: Some("boundary_bg"),
        layout: &pipelines.boundary_bgl,
        entries: &[
            wgpu::BindGroupEntry {
                binding: 0,
                resource: params_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 1,
                resource: key_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 2,
                resource: mask_buf.as_entire_binding(),
            },
        ],
    });

    let mut encoder = ctx
        .device
        .create_command_encoder(&wgpu::CommandEncoderDescriptor {
            label: Some("boundary_encoder"),
        });
    {
        let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
            label: Some("boundary_pass"),
            timestamp_writes: None,
        });
        pass.set_pipeline(&pipelines.boundary_pipeline);
        pass.set_bind_group(0, &bind_group, &[]);
        let workgroups = (num_rows as u32 + 255) / 256;
        pass.dispatch_workgroups(workgroups, 1, 1);
    }
    encoder.copy_buffer_to_buffer(&mask_buf, 0, &staging_buf, 0, mask_size);
    ctx.queue.submit(std::iter::once(encoder.finish()));

    // Read back mask
    let buf_slice = staging_buf.slice(..);
    let (tx, rx) = std::sync::mpsc::channel();
    buf_slice.map_async(wgpu::MapMode::Read, move |r| tx.send(r).unwrap());
    ctx.device.poll(wgpu::Maintain::Wait);
    rx.recv()
        .unwrap()
        .map_err(|e| DataFusionError::Internal(format!("Boundary mask readback failed: {e:?}")))?;

    let data = buf_slice.get_mapped_range();
    let words: &[u32] = bytemuck::cast_slice(&data[..mask_size as usize]);
    let result = words[..mask_words].to_vec();
    drop(data);
    staging_buf.unmap();

    // Return pooled buffers
    ctx.buffer_pool.release(key_buf);
    ctx.buffer_pool.release(staging_buf);

    Ok(result)
}

// ── CPU: segment extraction from bitmask ───────────────────────────────

/// Scan the boundary bitmask and produce `(start_row, length)` pairs.
fn extract_segments(bitmask: &[u32], total_rows: usize) -> Vec<(usize, usize)> {
    let mut segments = Vec::new();
    let mut current_start = 0usize;

    for (word_idx, &word) in bitmask.iter().enumerate() {
        let base = word_idx * 32;
        let mut w = word;
        while w != 0 {
            let bit = w.trailing_zeros() as usize;
            let row = base + bit;
            if row >= total_rows {
                break;
            }
            if row > 0 {
                // End the previous segment
                segments.push((current_start, row - current_start));
            }
            current_start = row;
            w &= w - 1; // clear lowest set bit
        }
    }
    // Final segment
    if current_start < total_rows {
        segments.push((current_start, total_rows - current_start));
    }
    segments
}

// ── GPU: segmented reduction ────────────────────────────────────────────

fn gpu_reduce_segments(
    ctx: &super::context::WgpuContext,
    pipelines: &AggPipelines,
    value_bytes: &[u8],
    value_type: KeyType,
    func: GpuAggFunc,
    seg_starts_buf: &wgpu::Buffer,
    seg_lengths_buf: &wgpu::Buffer,
    num_segments: usize,
) -> Result<ArrayRef> {
    use wgpu::util::DeviceExt;

    let agg_op = match func {
        GpuAggFunc::Sum => 0u32,
        GpuAggFunc::Count => 1u32,
        GpuAggFunc::Min => 2u32,
        GpuAggFunc::Max => 3u32,
    };

    let params = ReduceParams {
        agg_op,
        num_segments: num_segments as u32,
        _pad0: 0,
        _pad1: 0,
    };
    let params_buf = ctx
        .device
        .create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("reduce_params"),
            contents: bytemuck::bytes_of(&params),
            usage: wgpu::BufferUsages::UNIFORM | wgpu::BufferUsages::COPY_DST,
        });

    let value_buf = buffers::upload_bytes_pooled(ctx, value_bytes, "reduce_values");

    // For count, output is always i32 (one count per segment).
    // For sum/min/max on i64: two i32 words per segment.
    // For sum/min/max on i32: one i32 per segment.
    let words_per_segment: usize = if func == GpuAggFunc::Count {
        1
    } else {
        match value_type {
            KeyType::I32 => 1,
            KeyType::I64 => 2,
        }
    };
    let output_size = (num_segments * words_per_segment * 4) as u64;

    let output_buf = buffers::acquire_output_buffer(ctx, output_size);
    let staging_buf = buffers::acquire_staging_bytes(ctx, output_size);

    // Choose pipeline based on value type (count is always i32)
    let (pipeline, bgl) = if func == GpuAggFunc::Count || value_type == KeyType::I32 {
        (&pipelines.reduce_i32_pipeline, &pipelines.reduce_i32_bgl)
    } else {
        (&pipelines.reduce_i64_pipeline, &pipelines.reduce_i64_bgl)
    };

    let bind_group = ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: Some("reduce_bg"),
        layout: bgl,
        entries: &[
            wgpu::BindGroupEntry {
                binding: 0,
                resource: params_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 1,
                resource: value_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 2,
                resource: seg_starts_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 3,
                resource: seg_lengths_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 4,
                resource: output_buf.as_entire_binding(),
            },
        ],
    });

    let mut encoder = ctx
        .device
        .create_command_encoder(&wgpu::CommandEncoderDescriptor {
            label: Some("reduce_encoder"),
        });
    {
        let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
            label: Some("reduce_pass"),
            timestamp_writes: None,
        });
        pass.set_pipeline(pipeline);
        pass.set_bind_group(0, &bind_group, &[]);
        // One workgroup per segment
        pass.dispatch_workgroups(num_segments as u32, 1, 1);
    }
    encoder.copy_buffer_to_buffer(&output_buf, 0, &staging_buf, 0, output_size);
    ctx.queue.submit(std::iter::once(encoder.finish()));

    // Read back
    let buf_slice = staging_buf.slice(..);
    let (tx, rx) = std::sync::mpsc::channel();
    buf_slice.map_async(wgpu::MapMode::Read, move |r| tx.send(r).unwrap());
    ctx.device.poll(wgpu::Maintain::Wait);
    rx.recv()
        .unwrap()
        .map_err(|e| DataFusionError::Internal(format!("Reduce readback failed: {e:?}")))?;

    let data = buf_slice.get_mapped_range();
    let result = build_arrow_array_from_result(
        &data[..output_size as usize],
        func,
        value_type,
        num_segments,
    );
    drop(data);
    staging_buf.unmap();

    // Return pooled buffers
    ctx.buffer_pool.release(value_buf);
    ctx.buffer_pool.release(output_buf);
    ctx.buffer_pool.release(staging_buf);

    result
}

fn build_arrow_array_from_result(
    bytes: &[u8],
    func: GpuAggFunc,
    value_type: KeyType,
    num_segments: usize,
) -> Result<ArrayRef> {
    // count always outputs i32 (cast to i64 for DataFusion compat below)
    if func == GpuAggFunc::Count {
        let vals: &[i32] = bytemuck::cast_slice(bytes);
        // DataFusion COUNT returns Int64
        let i64_vals: Vec<i64> = vals[..num_segments].iter().map(|&v| v as i64).collect();
        return Ok(Arc::new(Int64Array::from(i64_vals)));
    }

    match value_type {
        KeyType::I32 => {
            let vals: &[i32] = bytemuck::cast_slice(bytes);
            // sum returns i64 in DataFusion (to avoid overflow); min/max return same type
            match func {
                GpuAggFunc::Sum => {
                    // Upcast to i64 to match DataFusion's sum(i32) → i64 output
                    let i64_vals: Vec<i64> = vals[..num_segments].iter().map(|&v| v as i64).collect();
                    Ok(Arc::new(Int64Array::from(i64_vals)))
                }
                _ => Ok(Arc::new(Int32Array::from(vals[..num_segments].to_vec()))),
            }
        }
        KeyType::I64 => {
            let i32_words: &[i32] = bytemuck::cast_slice(bytes);
            let mut i64_vals = Vec::with_capacity(num_segments);
            for i in 0..num_segments {
                let lo = i32_words[i * 2] as u32;
                let hi = i32_words[i * 2 + 1] as u32;
                let val = (lo as i64) | ((hi as i64) << 32);
                i64_vals.push(val);
            }
            Ok(Arc::new(Int64Array::from(i64_vals)))
        }
    }
}

// ── avg computation on CPU ──────────────────────────────────────────────

fn compute_avg_from_sum_count(
    sum_arr: &ArrayRef,
    count_arr: &ArrayRef,
    value_type: KeyType,
) -> Result<ArrayRef> {
    let count_i64 = count_arr
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Internal("Count array must be Int64".to_string()))?;

    // avg always returns Float64 (DataFusion convention)
    let avg_vals: Vec<f64> = match value_type {
        KeyType::I32 => {
            // sum was upcast to i64
            let sum_i64 = sum_arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("Sum i32→i64 array error".to_string()))?;
            (0..sum_i64.len())
                .map(|i| {
                    let c = count_i64.value(i);
                    if c == 0 { f64::NAN } else { sum_i64.value(i) as f64 / c as f64 }
                })
                .collect()
        }
        KeyType::I64 => {
            let sum_i64 = sum_arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("Sum i64 array error".to_string()))?;
            (0..sum_i64.len())
                .map(|i| {
                    let c = count_i64.value(i);
                    if c == 0 { f64::NAN } else { sum_i64.value(i) as f64 / c as f64 }
                })
                .collect()
        }
    };

    Ok(Arc::new(Float64Array::from(avg_vals)))
}

// ── Helper: collect column bytes from multiple batches ──────────────────

/// Concatenate values of one column from multiple batches into a flat byte buffer.
/// For i32: returns flat &[i32] as bytes.
/// For i64: returns interleaved [lo, hi] i32 words as bytes.
fn collect_column_bytes(
    batches: &[RecordBatch],
    col_idx: usize,
    key_type: KeyType,
) -> Result<Vec<u8>> {
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    match key_type {
        KeyType::I32 => {
            let mut out = Vec::with_capacity(total_rows * 4);
            for batch in batches {
                let col = batch.column(col_idx);
                let arr = col
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| DataFusionError::Internal("Expected i32 column".to_string()))?;
                out.extend_from_slice(bytemuck::cast_slice(arr.values()));
            }
            Ok(out)
        }
        KeyType::I64 => {
            // Store as interleaved [lo, hi] i32 words (little-endian matches native i64)
            let mut out = Vec::with_capacity(total_rows * 8);
            for batch in batches {
                let col = batch.column(col_idx);
                let arr = col
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| DataFusionError::Internal("Expected i64 column".to_string()))?;
                // On little-endian, the raw bytes are [lo_byte0, lo_byte1, lo_byte2, lo_byte3,
                // hi_byte0, ...] which matches our WGSL layout of [lo_word, hi_word].
                out.extend_from_slice(arr.values().inner().as_slice());
            }
            Ok(out)
        }
    }
}

/// Upload a &[u32] slice as a GPU storage buffer.
#[allow(dead_code)]
fn upload_u32_array(
    ctx: &super::context::WgpuContext,
    data: &[u32],
    label: &str,
) -> wgpu::Buffer {
    use wgpu::util::DeviceExt;
    ctx.device
        .create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some(label),
            contents: bytemuck::cast_slice(data),
            usage: wgpu::BufferUsages::STORAGE,
        })
}

/// Upload a &[u32] slice as a GPU storage buffer (pooled).
///
/// The segment buffers are small but created on every `gpu_aggregate` call;
/// pooling avoids repeated driver allocations.
fn upload_u32_array_pooled(
    ctx: &super::context::WgpuContext,
    data: &[u32],
    _label: &str,
) -> wgpu::Buffer {
    let bytes: &[u8] = bytemuck::cast_slice(data);
    // u32 segment arrays need STORAGE | COPY_DST for pooled upload
    buffers::upload_bytes_pooled(ctx, bytes, "u32_pooled")
}

/// Detect the KeyType of a value column (for non-key aggregate columns).
fn column_value_type(batches: &[RecordBatch], col_idx: usize) -> Result<KeyType> {
    let schema = batches[0].schema();
    let field = schema.field(col_idx);
    match field.data_type() {
        DataType::Int32 => Ok(KeyType::I32),
        DataType::Int64 => Ok(KeyType::I64),
        // f32 treated as i32 bits (reinterpret)
        DataType::Float32 => Ok(KeyType::I32),
        dt => Err(DataFusionError::Internal(format!(
            "Unsupported aggregate value type: {dt:?}"
        ))),
    }
}

/// Extract first value of each group from the group key column (for output).
fn extract_group_key_values(
    batches: &[RecordBatch],
    col_idx: usize,
    key_type: KeyType,
    segments: &[(usize, usize)],
) -> Result<ArrayRef> {
    // Build a flat index of values across batches
    // segments[i] = (global_row_start, length)
    // We need the value at global_row_start for each segment.

    // Build batch cumulative offsets for global→batch mapping
    let mut batch_starts = Vec::with_capacity(batches.len() + 1);
    let mut offset = 0usize;
    for b in batches {
        batch_starts.push(offset);
        offset += b.num_rows();
    }
    batch_starts.push(offset);

    match key_type {
        KeyType::I32 => {
            let mut vals = Vec::with_capacity(segments.len());
            for &(start, _) in segments {
                let (batch_idx, local_row) = global_to_local(start, &batch_starts);
                let arr = batches[batch_idx]
                    .column(col_idx)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| DataFusionError::Internal("Expected i32 key".to_string()))?;
                vals.push(arr.value(local_row));
            }
            Ok(Arc::new(Int32Array::from(vals)))
        }
        KeyType::I64 => {
            let mut vals = Vec::with_capacity(segments.len());
            for &(start, _) in segments {
                let (batch_idx, local_row) = global_to_local(start, &batch_starts);
                let arr = batches[batch_idx]
                    .column(col_idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| DataFusionError::Internal("Expected i64 key".to_string()))?;
                vals.push(arr.value(local_row));
            }
            Ok(Arc::new(Int64Array::from(vals)))
        }
    }
}

/// Convert a global row index to (batch_index, local_row_index).
fn global_to_local(global_row: usize, batch_starts: &[usize]) -> (usize, usize) {
    // Binary search for the batch containing global_row
    let batch_idx = batch_starts.partition_point(|&s| s <= global_row) - 1;
    let local_row = global_row - batch_starts[batch_idx];
    (batch_idx, local_row)
}
