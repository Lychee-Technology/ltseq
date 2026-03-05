//! GPU-accelerated hash aggregate execution plan.
//!
//! `GpuHashAggregateExec` replaces DataFusion's `AggregateExec` for GROUP BY
//! queries on numeric columns. It uses a sort-then-segment approach:
//!
//! 1. GPU radix sort the group-by key column (producing sorted keys + permutation)
//! 2. Apply permutation to reorder value columns
//! 3. GPU boundary detection on sorted key → group boundaries
//! 4. GPU prefix sum → group_id assignment
//! 5. GPU segmented aggregation (reuses Phase 1.3 kernels from `ordered_ops`)
//! 6. Collapse to one row per group (AggregateExec semantics)
//!
//! If the input is already sorted by the group key (detected via
//! `EquivalenceProperties`), steps 1-2 are skipped entirely.
//!
//! ## CUDA Kernels
//!
//! - `radix_sort_*` — LSB radix sort with histogram + scatter (4-bit digits)
//! - `apply_permutation_*` — reorder columns by sorted indices
//! - Reuses: `adjacent_ne_*`, `prefix_sum_u8_to_u32`, `seg_count/sum/min/max`
//!   from `ordered_ops`

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, UInt32Array,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};

use cudarc::driver::safe::{CudaModule, CudaStream, LaunchConfig};
use cudarc::driver::PushKernelArg;
use cudarc::nvrtc::compile_ptx;
use std::sync::OnceLock;

use super::ordered_ops::SegAggFunc;
use super::sort_aware::{SortOrderEffect, SortOrderPropagation};
use super::{get_stream, CUDA_BLOCK_SIZE, grid_size};

// ============================================================================
// CUDA Kernel Source — Radix Sort + Permutation
// ============================================================================

/// CUDA kernels for radix sort and permutation.
///
/// Uses a 4-bit radix (16 buckets) LSB sort. For N elements with B-bit keys:
/// - Number of passes = ceil(B / 4)
/// - Each pass: histogram → prefix sum (on CPU) → scatter
///
/// Key encoding for correct ordering:
/// - i64: XOR with 0x8000000000000000 to flip sign bit (signed → unsigned order)
/// - f64: if negative, flip all bits; if positive, flip sign bit
const HASH_OPS_KERNEL_SRC: &str = r#"
// ── Radix sort histogram kernel ──────────────────────────────────────
// Counts occurrences of each 4-bit digit at a given bit offset.
// Each block uses shared memory histogram, then atomicAdd to global.

extern "C" __global__ void radix_histogram(
    const unsigned long long* __restrict__ keys,
    unsigned int* __restrict__ histogram,
    const unsigned int n,
    const unsigned int bit_offset
) {
    __shared__ unsigned int local_hist[16];
    if (threadIdx.x < 16) local_hist[threadIdx.x] = 0;
    __syncthreads();

    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        unsigned int digit = (keys[idx] >> bit_offset) & 0xF;
        atomicAdd(&local_hist[digit], 1);
    }
    __syncthreads();

    if (threadIdx.x < 16) {
        atomicAdd(&histogram[threadIdx.x], local_hist[threadIdx.x]);
    }
}

// ── Radix sort scatter kernel ────────────────────────────────────────
// Scatters keys and indices to their sorted positions using prefix-summed histogram.
// Uses per-block local offsets to handle concurrent writes.

extern "C" __global__ void radix_scatter(
    const unsigned long long* __restrict__ keys_in,
    const unsigned int* __restrict__ indices_in,
    unsigned long long* __restrict__ keys_out,
    unsigned int* __restrict__ indices_out,
    unsigned int* __restrict__ counters,
    const unsigned int n,
    const unsigned int bit_offset
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        unsigned int digit = (keys_in[idx] >> bit_offset) & 0xF;
        unsigned int pos = atomicAdd(&counters[digit], 1);
        keys_out[pos] = keys_in[idx];
        indices_out[pos] = indices_in[idx];
    }
}

// ── Initialize index array ──────────────────────────────────────────
extern "C" __global__ void init_indices(
    unsigned int* __restrict__ indices,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        indices[idx] = idx;
    }
}

// ── Apply permutation kernels ───────────────────────────────────────
// Reorder arrays according to a permutation (index) array.

extern "C" __global__ void apply_perm_i64(
    const long long* __restrict__ src,
    const unsigned int* __restrict__ perm,
    long long* __restrict__ dst,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        dst[idx] = src[perm[idx]];
    }
}

extern "C" __global__ void apply_perm_f64(
    const double* __restrict__ src,
    const unsigned int* __restrict__ perm,
    double* __restrict__ dst,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        dst[idx] = src[perm[idx]];
    }
}

extern "C" __global__ void apply_perm_i32(
    const int* __restrict__ src,
    const unsigned int* __restrict__ perm,
    int* __restrict__ dst,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        dst[idx] = src[perm[idx]];
    }
}

extern "C" __global__ void apply_perm_f32(
    const float* __restrict__ src,
    const unsigned int* __restrict__ perm,
    float* __restrict__ dst,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        dst[idx] = src[perm[idx]];
    }
}

// ── Encode keys for radix sort ──────────────────────────────────────
// Convert signed/float keys to unsigned for correct lexicographic sort.

extern "C" __global__ void encode_i64_keys(
    const long long* __restrict__ src,
    unsigned long long* __restrict__ dst,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        // Flip sign bit: signed order → unsigned order
        dst[idx] = (unsigned long long)(src[idx]) ^ 0x8000000000000000ULL;
    }
}

extern "C" __global__ void encode_i32_keys(
    const int* __restrict__ src,
    unsigned long long* __restrict__ dst,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        // Widen to u64 with sign-flip for correct order
        unsigned int u = (unsigned int)(src[idx]) ^ 0x80000000U;
        dst[idx] = (unsigned long long)u;
    }
}

extern "C" __global__ void encode_f64_keys(
    const double* __restrict__ src,
    unsigned long long* __restrict__ dst,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        unsigned long long bits = __double_as_longlong(src[idx]);
        // If sign bit set (negative): flip all bits
        // If sign bit clear (positive/zero): flip only sign bit
        unsigned long long mask = (bits >> 63) ? 0xFFFFFFFFFFFFFFFFULL : 0x8000000000000000ULL;
        dst[idx] = bits ^ mask;
    }
}

extern "C" __global__ void encode_f32_keys(
    const float* __restrict__ src,
    unsigned long long* __restrict__ dst,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        unsigned int bits = __float_as_uint(src[idx]);
        unsigned int mask = (bits >> 31) ? 0xFFFFFFFFU : 0x80000000U;
        unsigned int encoded = bits ^ mask;
        dst[idx] = (unsigned long long)encoded;
    }
}

// ── Extract group keys from sorted+collapsed data ────────────────────
// Extracts the first element of each group (where boundary[i] == 1).
// `out_indices[j] = i` where i is the j-th group's first row.
// j is determined by `group_id[i] - 1` (group_id is 1-based).

extern "C" __global__ void extract_group_first_indices(
    const unsigned int* __restrict__ group_id,
    const unsigned char* __restrict__ boundary,
    unsigned int* __restrict__ out_indices,
    const unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        if (boundary[idx] == 1) {
            // group_id is 1-based, output index is 0-based
            out_indices[group_id[idx] - 1] = idx;
        }
    }
}

// ── Gather: extract values at given indices ─────────────────────────

extern "C" __global__ void gather_i64(
    const long long* __restrict__ src,
    const unsigned int* __restrict__ indices,
    long long* __restrict__ dst,
    const unsigned int num_groups
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < num_groups) {
        dst[idx] = src[indices[idx]];
    }
}

extern "C" __global__ void gather_f64(
    const double* __restrict__ src,
    const unsigned int* __restrict__ indices,
    double* __restrict__ dst,
    const unsigned int num_groups
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < num_groups) {
        dst[idx] = src[indices[idx]];
    }
}

extern "C" __global__ void gather_i32(
    const int* __restrict__ src,
    const unsigned int* __restrict__ indices,
    int* __restrict__ dst,
    const unsigned int num_groups
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < num_groups) {
        dst[idx] = src[indices[idx]];
    }
}

extern "C" __global__ void gather_f32(
    const float* __restrict__ src,
    const unsigned int* __restrict__ indices,
    float* __restrict__ dst,
    const unsigned int num_groups
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < num_groups) {
        dst[idx] = src[indices[idx]];
    }
}
"#;

// ============================================================================
// GPU Resources for hash ops
// ============================================================================

/// Cached compiled module for hash ops kernels.
static HASH_OPS_MODULE: OnceLock<Option<Arc<CudaModule>>> = OnceLock::new();

/// Get or compile the hash ops CUDA module.
fn get_hash_ops_module() -> Option<Arc<CudaModule>> {
    HASH_OPS_MODULE
        .get_or_init(|| {
            let ctx = super::get_context()?;
            let ptx = compile_ptx(HASH_OPS_KERNEL_SRC).ok()?;
            let module = ctx.load_module(ptx).ok()?;
            Some(module)
        })
        .clone()
}

/// Get a kernel function from the hash ops module.
fn get_hash_ops_function(name: &str) -> Option<cudarc::driver::safe::CudaFunction> {
    get_hash_ops_module()?.load_function(name).ok()
}

/// Public accessor for hash ops kernel functions (used by sort_exec).
pub fn get_hash_ops_function_pub(name: &str) -> Option<cudarc::driver::safe::CudaFunction> {
    get_hash_ops_function(name)
}

/// Convert a CUDA driver error into a DataFusion execution error.
fn cuda_err(e: cudarc::driver::safe::DriverError) -> DataFusionError {
    DataFusionError::Execution(format!("CUDA error: {:?}", e))
}

// ============================================================================
// Configuration extracted from AggregateExec
// ============================================================================

/// Describes a single group-by key column for GPU hash aggregate.
#[derive(Debug, Clone)]
pub struct GpuGroupKey {
    /// Column index in the input schema
    pub column_index: usize,
    /// Column name
    pub column_name: String,
    /// Data type (Int32, Int64, Float32, Float64)
    pub data_type: DataType,
}

/// Describes a single aggregate function for GPU hash aggregate.
#[derive(Debug, Clone)]
pub struct GpuAggRequest {
    /// The aggregation function
    pub func: SegAggFunc,
    /// Column index for the aggregate input (ignored for Count)
    pub column_index: usize,
    /// Data type of the input column
    pub data_type: DataType,
    /// Output column name (from the AggregateExec schema)
    pub output_name: String,
}

/// Configuration for a GPU hash aggregate operation.
#[derive(Debug, Clone)]
pub struct GpuHashAggConfig {
    /// Group-by key columns (single key for Phase 2.1)
    pub group_keys: Vec<GpuGroupKey>,
    /// Aggregate requests
    pub agg_requests: Vec<GpuAggRequest>,
    /// Whether the input is already sorted by the group key
    pub input_sorted: bool,
}

// ============================================================================
// GpuHashAggregateExec
// ============================================================================

/// GPU-accelerated hash aggregate execution plan.
///
/// Replaces DataFusion's `AggregateExec` for GROUP BY queries with:
/// - Single numeric group-by key (i32, i64, f32, f64)
/// - Simple aggregate functions (count, sum, min, max, avg)
/// - No DISTINCT, no FILTER, no ORDER BY on aggregates
///
/// ## Output
///
/// Produces one row per group, matching `AggregateExec` output semantics:
/// `[group_key_columns..., aggregate_result_columns...]`
#[derive(Debug)]
pub struct GpuHashAggregateExec {
    /// Configuration
    config: GpuHashAggConfig,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// Output schema: group keys + aggregate results
    output_schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cached plan properties
    cache: PlanProperties,
}

impl GpuHashAggregateExec {
    /// Create a new GpuHashAggregateExec.
    ///
    /// `output_schema` should match the original `AggregateExec` output schema
    /// to ensure the plan substitution is transparent.
    pub fn try_new(
        config: GpuHashAggConfig,
        input: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
    ) -> Result<Self> {
        let cache = Self::compute_properties(&output_schema);
        // Wrap input in CoalescePartitionsExec if it has multiple partitions.
        // GPU aggregate must process all data in a single partition to produce
        // correct results.
        let coalesced_input = if input.properties().output_partitioning().partition_count() > 1 {
            Arc::new(
                datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(input),
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

    fn compute_properties(schema: &SchemaRef) -> PlanProperties {
        use datafusion::physical_plan::Partitioning;
        // Hash aggregate produces a single partition with final emission
        // and destroys any input ordering
        PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(Arc::clone(schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        )
    }
}

impl DisplayAs for GpuHashAggregateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let keys: Vec<&str> = self
            .config
            .group_keys
            .iter()
            .map(|k| k.column_name.as_str())
            .collect();
        let aggs: Vec<String> = self
            .config
            .agg_requests
            .iter()
            .map(|r| format!("{:?}({})", r.func, r.output_name))
            .collect();
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "GpuHashAggregateExec: keys=[{}], aggs=[{}], sorted={} [CUDA]",
                    keys.join(", "),
                    aggs.join(", "),
                    self.config.input_sorted
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "keys=[{}] aggs=[{}] sorted={} [CUDA]",
                    keys.join(", "),
                    aggs.join(", "),
                    self.config.input_sorted
                )
            }
        }
    }
}

impl ExecutionPlan for GpuHashAggregateExec {
    fn name(&self) -> &'static str {
        "GpuHashAggregateExec"
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
        Ok(Arc::new(GpuHashAggregateExec::try_new(
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

            // Step 2: Execute GPU hash aggregate
            let result = gpu_hash_aggregate(&coalesced, &config, &schema)?;

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
        // Aggregate reduces rows — use inexact estimate
        let stats = self.input.partition_statistics(None)?.to_inexact();
        Ok(stats)
    }
}

impl SortOrderPropagation for GpuHashAggregateExec {
    fn sort_order_effect(&self) -> SortOrderEffect {
        SortOrderEffect::Destroy
    }
}

// ============================================================================
// GPU Hash Aggregate Execution
// ============================================================================

/// Execute GPU hash aggregate on a coalesced RecordBatch.
///
/// Returns a RecordBatch with one row per group:
/// `[group_key_col_0, ..., agg_result_0, agg_result_1, ...]`
///
/// Supports multiple group keys via composite key encoding:
/// - Each key column is encoded to u64 on GPU (preserving sort order).
/// - CPU stable sort by composite key (lexicographic comparison).
/// - Boundary detection: row i is a boundary if ANY key column differs from row i-1.
/// - When `agg_requests` is empty (DISTINCT case), only group key columns are output.
fn gpu_hash_aggregate(
    batch: &RecordBatch,
    config: &GpuHashAggConfig,
    output_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let n = batch.num_rows();
    let stream = get_stream().ok_or_else(|| {
        DataFusionError::Execution("CUDA stream unavailable".to_string())
    })?;

    assert!(!config.group_keys.is_empty(), "Must have at least one group key");

    // Step 1: Sort by group key(s) (or skip if already sorted)
    let permutation = if config.input_sorted {
        None
    } else if config.group_keys.len() == 1 {
        // Single key: encode + sort (existing fast path)
        let group_key = &config.group_keys[0];
        let key_column = batch.column(group_key.column_index);
        let (_sorted_col, perm) = gpu_radix_sort_column(&stream, key_column, &group_key.data_type, n)?;
        perm
    } else {
        // Multi-key: encode each key to u64, CPU lexicographic sort
        let mut encoded_columns: Vec<Vec<u64>> = Vec::new();
        for gk in &config.group_keys {
            let column = batch.column(gk.column_index);
            let n_u32 = n as u32;
            let launch_cfg = LaunchConfig {
                grid_dim: (grid_size(n_u32), 1, 1),
                block_dim: (CUDA_BLOCK_SIZE, 1, 1),
                shared_mem_bytes: 0,
            };
            let d_encoded = encode_keys_to_u64(&stream, column, &gk.data_type, n, &launch_cfg)?;
            let encoded = stream.clone_dtoh(&d_encoded).map_err(cuda_err)?;
            encoded_columns.push(encoded);
        }

        let mut indices: Vec<u32> = (0..n as u32).collect();
        indices.sort_by(|&a, &b| {
            for col in &encoded_columns {
                let cmp = col[a as usize].cmp(&col[b as usize]);
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
            eprintln!("[GPU] hash_agg: multi-key sort (CPU, {} keys), n={}", config.group_keys.len(), n);
        }
        Some(indices)
    };

    // Step 2: Boundary detection on sorted key(s) → group boundaries
    // For multi-key: row i is a boundary if ANY key column at position i
    // differs from position i-1. We compute per-column boundaries and OR them.
    let boundary_mask = if config.group_keys.len() == 1 {
        // Single key: fast path using GPU boundary detection
        let group_key = &config.group_keys[0];
        let sorted_col = if let Some(ref perm) = permutation {
            apply_permutation_column(&stream, batch.column(group_key.column_index), &group_key.data_type, perm, n)?
        } else {
            Arc::clone(batch.column(group_key.column_index))
        };
        run_boundary_detection(&stream, &sorted_col, &group_key.data_type, n)?
    } else {
        // Multi-key: compute boundary per column on GPU, then OR on CPU
        let mut combined_mask = vec![0u8; n];
        if n > 0 {
            combined_mask[0] = 1; // First row is always a boundary
        }
        for gk in &config.group_keys {
            let sorted_col = if let Some(ref perm) = permutation {
                apply_permutation_column(&stream, batch.column(gk.column_index), &gk.data_type, perm, n)?
            } else {
                Arc::clone(batch.column(gk.column_index))
            };
            let col_mask = run_boundary_detection(&stream, &sorted_col, &gk.data_type, n)?;
            for i in 0..n {
                combined_mask[i] |= col_mask[i];
            }
        }
        combined_mask
    };

    // Step 3: Prefix sum → group_id (1-based)
    let group_ids = run_prefix_sum_for_hash(&stream, &boundary_mask, n)?;
    let num_groups = group_ids.iter().copied().max().unwrap_or(0) as usize;

    if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
        eprintln!(
            "[GPU] hash_agg: n={}, num_groups={}, keys={}, aggs={}, sorted={}",
            n, num_groups, config.group_keys.len(), config.agg_requests.len(), config.input_sorted
        );
    }

    if num_groups == 0 {
        return Ok(RecordBatch::new_empty(Arc::clone(output_schema)));
    }

    let group_id_u32 = UInt32Array::from(group_ids.clone());

    // Step 4: Extract group-first indices (for key column extraction)
    let group_first_indices = extract_group_first_indices_gpu(&stream, &group_ids, &boundary_mask, num_groups, n)?;

    // Step 5: Gather group key values (one per group)
    let mut output_columns: Vec<ArrayRef> = Vec::new();

    for gk in &config.group_keys {
        let col = if let Some(ref perm) = permutation {
            apply_permutation_column(&stream, batch.column(gk.column_index), &gk.data_type, perm, n)?
        } else {
            Arc::clone(batch.column(gk.column_index))
        };
        let gathered = gather_column(&stream, &col, &gk.data_type, &group_first_indices, num_groups)?;
        output_columns.push(gathered);
    }

    // Step 6: Run segmented aggregation on sorted data (skip for DISTINCT / empty agg_requests)
    for agg_req in &config.agg_requests {
        if agg_req.func == SegAggFunc::FirstValue {
            // FirstValue: gather the value at the first row of each group.
            // This reuses the group_first_indices already computed in Step 4.
            let col = if let Some(ref perm) = permutation {
                apply_permutation_column(&stream, batch.column(agg_req.column_index), &agg_req.data_type, perm, n)?
            } else {
                Arc::clone(batch.column(agg_req.column_index))
            };
            let gathered = gather_column(&stream, &col, &agg_req.data_type, &group_first_indices, num_groups)?;
            output_columns.push(gathered);
        } else {
            let result = run_collapsed_aggregate(
                &stream,
                batch,
                agg_req,
                &permutation,
                &group_id_u32,
                num_groups,
                n,
            )?;
            output_columns.push(result);
        }
    }

    RecordBatch::try_new(Arc::clone(output_schema), output_columns)
        .map_err(|e| DataFusionError::Execution(format!("Failed to build output batch: {}", e)))
}

// ============================================================================
// GPU Radix Sort
// ============================================================================

/// Perform GPU radix sort on a single column.
///
/// Returns (sorted_column, permutation_indices).
/// The permutation array maps sorted position → original position.
fn gpu_radix_sort_column(
    stream: &Arc<CudaStream>,
    column: &ArrayRef,
    data_type: &DataType,
    n: usize,
) -> Result<(ArrayRef, Option<Vec<u32>>)> {
    let n_u32 = n as u32;
    let launch_cfg = LaunchConfig {
        grid_dim: (grid_size(n_u32), 1, 1),
        block_dim: (CUDA_BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    // Step 1: Encode keys as u64 on GPU for correct sort ordering
    let d_encoded = encode_keys_to_u64(stream, column, data_type, n, &launch_cfg)?;

    // Step 2: Download encoded keys to CPU and sort there.
    // GPU radix sort with atomicAdd scatter is not stable, which breaks
    // subsequent passes. CPU sort is correct and fast for moderate sizes.
    let encoded = stream.clone_dtoh(&d_encoded).map_err(cuda_err)?;

    // Create index array and sort by encoded key
    let mut indices: Vec<u32> = (0..n as u32).collect();
    indices.sort_by_key(|&i| encoded[i as usize]);

    if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
        eprintln!("[GPU] radix sort (CPU fallback): n={}", n);
    }

    // Apply permutation to reconstruct sorted column in original data type
    let sorted_column = apply_permutation_column(stream, column, data_type, &indices, n)?;

    Ok((sorted_column, Some(indices)))
}

/// Encode column values as u64 for radix sort.
fn encode_keys_to_u64(
    stream: &Arc<CudaStream>,
    column: &ArrayRef,
    data_type: &DataType,
    n: usize,
    launch_cfg: &LaunchConfig,
) -> Result<cudarc::driver::safe::CudaSlice<u64>> {
    let n_u32 = n as u32;
    let mut d_encoded = stream.alloc_zeros::<u64>(n).map_err(cuda_err)?;

    match data_type {
        DataType::Int64 => {
            let func = get_hash_ops_function("encode_i64_keys").ok_or_else(|| {
                DataFusionError::Execution("CUDA kernel 'encode_i64_keys' not found".to_string())
            })?;
            let array = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Execution("Column is not Int64Array".to_string())
            })?;
            let d_src = stream.clone_htod(array.values().as_ref()).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_src)
                    .arg(&mut d_encoded)
                    .arg(&n_u32)
                    .launch(*launch_cfg)
                    .map_err(cuda_err)?;
            }
        }
        DataType::Int32 => {
            let func = get_hash_ops_function("encode_i32_keys").ok_or_else(|| {
                DataFusionError::Execution("CUDA kernel 'encode_i32_keys' not found".to_string())
            })?;
            let array = column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                DataFusionError::Execution("Column is not Int32Array".to_string())
            })?;
            let d_src = stream.clone_htod(array.values().as_ref()).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_src)
                    .arg(&mut d_encoded)
                    .arg(&n_u32)
                    .launch(*launch_cfg)
                    .map_err(cuda_err)?;
            }
        }
        DataType::Float64 => {
            let func = get_hash_ops_function("encode_f64_keys").ok_or_else(|| {
                DataFusionError::Execution("CUDA kernel 'encode_f64_keys' not found".to_string())
            })?;
            let array = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                DataFusionError::Execution("Column is not Float64Array".to_string())
            })?;
            let d_src = stream.clone_htod(array.values().as_ref()).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_src)
                    .arg(&mut d_encoded)
                    .arg(&n_u32)
                    .launch(*launch_cfg)
                    .map_err(cuda_err)?;
            }
        }
        DataType::Float32 => {
            let func = get_hash_ops_function("encode_f32_keys").ok_or_else(|| {
                DataFusionError::Execution("CUDA kernel 'encode_f32_keys' not found".to_string())
            })?;
            let array = column.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                DataFusionError::Execution("Column is not Float32Array".to_string())
            })?;
            let d_src = stream.clone_htod(array.values().as_ref()).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_src)
                    .arg(&mut d_encoded)
                    .arg(&n_u32)
                    .launch(*launch_cfg)
                    .map_err(cuda_err)?;
            }
        }
        _ => {
            return Err(DataFusionError::Execution(format!(
                "Unsupported data type for GPU radix sort: {:?}",
                data_type
            )))
        }
    }

    Ok(d_encoded)
}

/// Apply a permutation to reorder a column.
fn apply_permutation_column(
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
            let func = get_hash_ops_function("apply_perm_i64").ok_or_else(|| {
                DataFusionError::Execution("CUDA kernel 'apply_perm_i64' not found".to_string())
            })?;
            let array = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Execution("Column is not Int64Array".to_string())
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
            let func = get_hash_ops_function("apply_perm_i32").ok_or_else(|| {
                DataFusionError::Execution("CUDA kernel 'apply_perm_i32' not found".to_string())
            })?;
            let array = column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                DataFusionError::Execution("Column is not Int32Array".to_string())
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
            let func = get_hash_ops_function("apply_perm_f64").ok_or_else(|| {
                DataFusionError::Execution("CUDA kernel 'apply_perm_f64' not found".to_string())
            })?;
            let array = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                DataFusionError::Execution("Column is not Float64Array".to_string())
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
            let func = get_hash_ops_function("apply_perm_f32").ok_or_else(|| {
                DataFusionError::Execution("CUDA kernel 'apply_perm_f32' not found".to_string())
            })?;
            let array = column.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                DataFusionError::Execution("Column is not Float32Array".to_string())
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

// ============================================================================
// Boundary Detection + Prefix Sum (reuses ordered_ops kernels)
// ============================================================================

/// Run boundary detection on a sorted column using ordered_ops kernels.
///
/// Returns boundary mask as Vec<u8> (1 = new group, 0 = same group).
fn run_boundary_detection(
    stream: &Arc<CudaStream>,
    sorted_column: &ArrayRef,
    data_type: &DataType,
    n: usize,
) -> Result<Vec<u8>> {
    let kernel_name = match data_type {
        DataType::Int64 => "adjacent_ne_i64",
        DataType::Int32 => "adjacent_ne_i32",
        DataType::Float64 => "adjacent_ne_f64",
        DataType::Float32 => "adjacent_ne_f32",
        _ => {
            return Err(DataFusionError::Execution(format!(
                "Unsupported type for boundary detection: {:?}",
                data_type
            )))
        }
    };

    // Use ordered_ops module for boundary detection kernels
    let func = super::ordered_ops::get_ordered_ops_function_pub(kernel_name).ok_or_else(|| {
        DataFusionError::Execution(format!("CUDA kernel '{}' not found", kernel_name))
    })?;

    let n_u32 = n as u32;
    let launch_cfg = LaunchConfig {
        grid_dim: (grid_size(n_u32), 1, 1),
        block_dim: (CUDA_BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };
    let mut d_boundary = stream.alloc_zeros::<u8>(n).map_err(cuda_err)?;

    match data_type {
        DataType::Int64 => {
            let array = sorted_column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Execution("Column is not Int64Array".to_string())
            })?;
            let d_data = stream.clone_htod(array.values().as_ref()).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_data)
                    .arg(&mut d_boundary)
                    .arg(&n_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
        }
        DataType::Int32 => {
            let array = sorted_column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                DataFusionError::Execution("Column is not Int32Array".to_string())
            })?;
            let d_data = stream.clone_htod(array.values().as_ref()).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_data)
                    .arg(&mut d_boundary)
                    .arg(&n_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
        }
        DataType::Float64 => {
            let array = sorted_column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                DataFusionError::Execution("Column is not Float64Array".to_string())
            })?;
            let d_data = stream.clone_htod(array.values().as_ref()).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_data)
                    .arg(&mut d_boundary)
                    .arg(&n_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
        }
        DataType::Float32 => {
            let array = sorted_column.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                DataFusionError::Execution("Column is not Float32Array".to_string())
            })?;
            let d_data = stream.clone_htod(array.values().as_ref()).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_data)
                    .arg(&mut d_boundary)
                    .arg(&n_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
        }
        _ => unreachable!(),
    }

    stream.clone_dtoh(&d_boundary).map_err(cuda_err)
}

/// Run prefix sum to convert boundary mask → group_id (1-based u32).
///
/// Reuses the prefix sum infrastructure from ordered_ops.
fn run_prefix_sum_for_hash(
    stream: &Arc<CudaStream>,
    boundary_mask: &[u8],
    n: usize,
) -> Result<Vec<u32>> {
    let prefix_sum_func =
        super::ordered_ops::get_ordered_ops_function_pub("prefix_sum_u8_to_u32").ok_or_else(|| {
            DataFusionError::Execution("CUDA kernel 'prefix_sum_u8_to_u32' not found".to_string())
        })?;

    let propagate_func =
        super::ordered_ops::get_ordered_ops_function_pub("propagate_block_sums").ok_or_else(|| {
            DataFusionError::Execution(
                "CUDA kernel 'propagate_block_sums' not found".to_string(),
            )
        })?;

    let block_size = CUDA_BLOCK_SIZE as usize;
    let num_blocks = (n + block_size - 1) / block_size;

    let d_boundary = stream.clone_htod(boundary_mask).map_err(cuda_err)?;
    let mut d_group_id = stream.alloc_zeros::<u32>(n).map_err(cuda_err)?;
    let n_u32 = n as u32;

    if num_blocks == 1 {
        let launch_cfg = LaunchConfig {
            grid_dim: (1, 1, 1),
            block_dim: (block_size as u32, 1, 1),
            shared_mem_bytes: (block_size * std::mem::size_of::<u32>()) as u32,
        };
        let null_ptr: u64 = 0;
        unsafe {
            stream
                .launch_builder(&prefix_sum_func)
                .arg(&d_boundary)
                .arg(&mut d_group_id)
                .arg(&null_ptr)
                .arg(&n_u32)
                .launch(launch_cfg)
                .map_err(cuda_err)?;
        }
    } else {
        let mut d_block_sums = stream.alloc_zeros::<u32>(num_blocks).map_err(cuda_err)?;
        let scan_cfg = LaunchConfig {
            grid_dim: (num_blocks as u32, 1, 1),
            block_dim: (block_size as u32, 1, 1),
            shared_mem_bytes: (block_size * std::mem::size_of::<u32>()) as u32,
        };

        unsafe {
            stream
                .launch_builder(&prefix_sum_func)
                .arg(&d_boundary)
                .arg(&mut d_group_id)
                .arg(&mut d_block_sums)
                .arg(&n_u32)
                .launch(scan_cfg)
                .map_err(cuda_err)?;
        }

        let block_sums_host = stream.clone_dtoh(&d_block_sums).map_err(cuda_err)?;
        let mut block_offsets = vec![0u32; num_blocks];
        let mut running = 0u32;
        for i in 0..num_blocks {
            block_offsets[i] = running;
            running += block_sums_host[i];
        }

        let d_block_offsets = stream.clone_htod(&block_offsets).map_err(cuda_err)?;
        let prop_cfg = LaunchConfig {
            grid_dim: (num_blocks as u32, 1, 1),
            block_dim: (block_size as u32, 1, 1),
            shared_mem_bytes: 0,
        };

        unsafe {
            stream
                .launch_builder(&propagate_func)
                .arg(&mut d_group_id)
                .arg(&d_block_offsets)
                .arg(&n_u32)
                .launch(prop_cfg)
                .map_err(cuda_err)?;
        }
    }

    stream.clone_dtoh(&d_group_id).map_err(cuda_err)
}

// ============================================================================
// Group Collapse — Extract One Row Per Group
// ============================================================================

/// Extract the first row index of each group using GPU kernel.
fn extract_group_first_indices_gpu(
    stream: &Arc<CudaStream>,
    group_ids: &[u32],
    boundary_mask: &[u8],
    num_groups: usize,
    n: usize,
) -> Result<Vec<u32>> {
    let func = get_hash_ops_function("extract_group_first_indices").ok_or_else(|| {
        DataFusionError::Execution(
            "CUDA kernel 'extract_group_first_indices' not found".to_string(),
        )
    })?;

    let n_u32 = n as u32;
    let launch_cfg = LaunchConfig {
        grid_dim: (grid_size(n_u32), 1, 1),
        block_dim: (CUDA_BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    let d_group_id = stream.clone_htod(group_ids).map_err(cuda_err)?;
    let d_boundary = stream.clone_htod(boundary_mask).map_err(cuda_err)?;
    let mut d_out_indices = stream.alloc_zeros::<u32>(num_groups).map_err(cuda_err)?;

    unsafe {
        stream
            .launch_builder(&func)
            .arg(&d_group_id)
            .arg(&d_boundary)
            .arg(&mut d_out_indices)
            .arg(&n_u32)
            .launch(launch_cfg)
            .map_err(cuda_err)?;
    }

    stream.clone_dtoh(&d_out_indices).map_err(cuda_err)
}

/// Gather values from a column at specific indices (one per group).
fn gather_column(
    stream: &Arc<CudaStream>,
    column: &ArrayRef,
    data_type: &DataType,
    indices: &[u32],
    num_groups: usize,
) -> Result<ArrayRef> {
    let ng_u32 = num_groups as u32;
    let launch_cfg = LaunchConfig {
        grid_dim: (grid_size(ng_u32), 1, 1),
        block_dim: (CUDA_BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };
    let d_indices = stream.clone_htod(indices).map_err(cuda_err)?;

    match data_type {
        DataType::Int64 => {
            let func = get_hash_ops_function("gather_i64").ok_or_else(|| {
                DataFusionError::Execution("CUDA kernel 'gather_i64' not found".to_string())
            })?;
            let array = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Execution("Column is not Int64Array".to_string())
            })?;
            let d_src = stream.clone_htod(array.values().as_ref()).map_err(cuda_err)?;
            let mut d_dst = stream.alloc_zeros::<i64>(num_groups).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_src)
                    .arg(&d_indices)
                    .arg(&mut d_dst)
                    .arg(&ng_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
            let result = stream.clone_dtoh(&d_dst).map_err(cuda_err)?;
            Ok(Arc::new(Int64Array::from(result)))
        }
        DataType::Int32 => {
            let func = get_hash_ops_function("gather_i32").ok_or_else(|| {
                DataFusionError::Execution("CUDA kernel 'gather_i32' not found".to_string())
            })?;
            let array = column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                DataFusionError::Execution("Column is not Int32Array".to_string())
            })?;
            let d_src = stream.clone_htod(array.values().as_ref()).map_err(cuda_err)?;
            let mut d_dst = stream.alloc_zeros::<i32>(num_groups).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_src)
                    .arg(&d_indices)
                    .arg(&mut d_dst)
                    .arg(&ng_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
            let result = stream.clone_dtoh(&d_dst).map_err(cuda_err)?;
            Ok(Arc::new(Int32Array::from(result)))
        }
        DataType::Float64 => {
            let func = get_hash_ops_function("gather_f64").ok_or_else(|| {
                DataFusionError::Execution("CUDA kernel 'gather_f64' not found".to_string())
            })?;
            let array = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                DataFusionError::Execution("Column is not Float64Array".to_string())
            })?;
            let d_src = stream.clone_htod(array.values().as_ref()).map_err(cuda_err)?;
            let mut d_dst = stream.alloc_zeros::<f64>(num_groups).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_src)
                    .arg(&d_indices)
                    .arg(&mut d_dst)
                    .arg(&ng_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
            let result = stream.clone_dtoh(&d_dst).map_err(cuda_err)?;
            Ok(Arc::new(Float64Array::from(result)))
        }
        DataType::Float32 => {
            let func = get_hash_ops_function("gather_f32").ok_or_else(|| {
                DataFusionError::Execution("CUDA kernel 'gather_f32' not found".to_string())
            })?;
            let array = column.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                DataFusionError::Execution("Column is not Float32Array".to_string())
            })?;
            let d_src = stream.clone_htod(array.values().as_ref()).map_err(cuda_err)?;
            let mut d_dst = stream.alloc_zeros::<f32>(num_groups).map_err(cuda_err)?;
            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(&d_src)
                    .arg(&d_indices)
                    .arg(&mut d_dst)
                    .arg(&ng_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
            let result = stream.clone_dtoh(&d_dst).map_err(cuda_err)?;
            Ok(Arc::new(Float32Array::from(result)))
        }
        _ => Err(DataFusionError::Execution(format!(
            "Unsupported type for GPU gather: {:?}",
            data_type
        ))),
    }
}

// ============================================================================
// Collapsed Aggregation — Per-Group Results (not broadcast)
// ============================================================================

/// Run a single aggregation and produce collapsed output (one value per group).
///
/// Unlike `gpu_segmented_aggregate()` in ordered_ops which broadcasts results
/// back to every row, this function extracts per-group accumulator values
/// directly from the GPU — producing exactly `num_groups` output values.
fn run_collapsed_aggregate(
    stream: &Arc<CudaStream>,
    batch: &RecordBatch,
    agg_req: &GpuAggRequest,
    permutation: &Option<Vec<u32>>,
    group_id_u32: &UInt32Array,
    num_groups: usize,
    n: usize,
) -> Result<ArrayRef> {
    let n_u32 = n as u32;
    let launch_cfg = LaunchConfig {
        grid_dim: (grid_size(n_u32), 1, 1),
        block_dim: (CUDA_BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    // group_id is 1-based, so we need (num_groups + 1) slots
    let alloc_size = num_groups + 1;

    // Upload group_id to GPU
    let group_id_values: &[u32] = group_id_u32.values();
    let d_group_id = stream.clone_htod(group_id_values).map_err(cuda_err)?;

    match agg_req.func {
        SegAggFunc::Count => {
            run_collapsed_count(stream, &d_group_id, alloc_size, num_groups, n_u32, &launch_cfg)
        }
        SegAggFunc::Sum => {
            let sorted_col = get_sorted_value_column(stream, batch, agg_req, permutation, n)?;
            run_collapsed_sum(stream, &d_group_id, &sorted_col, &agg_req.data_type, alloc_size, num_groups, n_u32, &launch_cfg)
        }
        SegAggFunc::Min => {
            let sorted_col = get_sorted_value_column(stream, batch, agg_req, permutation, n)?;
            run_collapsed_min_max(stream, &d_group_id, &sorted_col, &agg_req.data_type, alloc_size, num_groups, n_u32, &launch_cfg, true)
        }
        SegAggFunc::Max => {
            let sorted_col = get_sorted_value_column(stream, batch, agg_req, permutation, n)?;
            run_collapsed_min_max(stream, &d_group_id, &sorted_col, &agg_req.data_type, alloc_size, num_groups, n_u32, &launch_cfg, false)
        }
        SegAggFunc::Avg => {
            let sorted_col = get_sorted_value_column(stream, batch, agg_req, permutation, n)?;
            run_collapsed_avg(stream, &d_group_id, &sorted_col, &agg_req.data_type, alloc_size, num_groups, n_u32, &launch_cfg)
        }
        SegAggFunc::FirstValue => {
            // FirstValue in the collapsed path should be handled in gpu_hash_aggregate
            // via gather_column + group_first_indices. This branch is a fallback that
            // should not normally be reached.
            Err(DataFusionError::Execution(
                "FirstValue should be handled via gather_column in gpu_hash_aggregate".to_string(),
            ))
        }
    }
}

/// Get the value column in sorted order (apply permutation if needed).
fn get_sorted_value_column(
    stream: &Arc<CudaStream>,
    batch: &RecordBatch,
    agg_req: &GpuAggRequest,
    permutation: &Option<Vec<u32>>,
    n: usize,
) -> Result<ArrayRef> {
    let col = batch.column(agg_req.column_index);
    if let Some(perm) = permutation {
        apply_permutation_column(stream, col, &agg_req.data_type, perm, n)
    } else {
        Ok(Arc::clone(col))
    }
}

/// Collapsed count: per-group row count → num_groups output values.
fn run_collapsed_count(
    stream: &Arc<CudaStream>,
    d_group_id: &cudarc::driver::safe::CudaSlice<u32>,
    alloc_size: usize,
    num_groups: usize,
    n_u32: u32,
    launch_cfg: &LaunchConfig,
) -> Result<ArrayRef> {
    let func = super::ordered_ops::get_ordered_ops_function_pub("seg_count").ok_or_else(|| {
        DataFusionError::Execution("CUDA kernel 'seg_count' not found".to_string())
    })?;

    let mut d_counts = stream.alloc_zeros::<i64>(alloc_size).map_err(cuda_err)?;
    unsafe {
        stream
            .launch_builder(&func)
            .arg(d_group_id)
            .arg(&mut d_counts)
            .arg(&n_u32)
            .launch(*launch_cfg)
            .map_err(cuda_err)?;
    }

    // Download per-group counts, skip slot 0 (group_id is 1-based)
    let all_counts = stream.clone_dtoh(&d_counts).map_err(cuda_err)?;
    let counts: Vec<i64> = all_counts[1..=num_groups].to_vec();
    Ok(Arc::new(Int64Array::from(counts)))
}

/// Collapsed sum: per-group sum → num_groups output values.
fn run_collapsed_sum(
    stream: &Arc<CudaStream>,
    d_group_id: &cudarc::driver::safe::CudaSlice<u32>,
    sorted_col: &ArrayRef,
    data_type: &DataType,
    alloc_size: usize,
    num_groups: usize,
    n_u32: u32,
    launch_cfg: &LaunchConfig,
) -> Result<ArrayRef> {
    // Cast i32→i64, f32→f64 for the aggregation kernels
    match data_type {
        DataType::Int64 | DataType::Int32 => {
            let values_i64: Vec<i64> = match data_type {
                DataType::Int64 => {
                    let arr = sorted_col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                        DataFusionError::Execution("Not Int64Array".to_string())
                    })?;
                    arr.values().to_vec()
                }
                DataType::Int32 => {
                    let arr = sorted_col.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                        DataFusionError::Execution("Not Int32Array".to_string())
                    })?;
                    arr.values().iter().map(|v| *v as i64).collect()
                }
                _ => unreachable!(),
            };

            let func = super::ordered_ops::get_ordered_ops_function_pub("seg_sum_i64")
                .ok_or_else(|| DataFusionError::Execution("CUDA kernel 'seg_sum_i64' not found".to_string()))?;

            let d_data = stream.clone_htod(&values_i64).map_err(cuda_err)?;
            let mut d_sums = stream.alloc_zeros::<i64>(alloc_size).map_err(cuda_err)?;

            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(d_group_id)
                    .arg(&d_data)
                    .arg(&mut d_sums)
                    .arg(&n_u32)
                    .launch(*launch_cfg)
                    .map_err(cuda_err)?;
            }

            let all_sums = stream.clone_dtoh(&d_sums).map_err(cuda_err)?;
            let sums: Vec<i64> = all_sums[1..=num_groups].to_vec();
            Ok(Arc::new(Int64Array::from(sums)))
        }
        DataType::Float64 | DataType::Float32 => {
            let values_f64: Vec<f64> = match data_type {
                DataType::Float64 => {
                    let arr = sorted_col.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                        DataFusionError::Execution("Not Float64Array".to_string())
                    })?;
                    arr.values().to_vec()
                }
                DataType::Float32 => {
                    let arr = sorted_col.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                        DataFusionError::Execution("Not Float32Array".to_string())
                    })?;
                    arr.values().iter().map(|v| *v as f64).collect()
                }
                _ => unreachable!(),
            };

            let func = super::ordered_ops::get_ordered_ops_function_pub("seg_sum_f64")
                .ok_or_else(|| DataFusionError::Execution("CUDA kernel 'seg_sum_f64' not found".to_string()))?;

            let d_data = stream.clone_htod(&values_f64).map_err(cuda_err)?;
            let mut d_sums = stream.alloc_zeros::<f64>(alloc_size).map_err(cuda_err)?;

            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(d_group_id)
                    .arg(&d_data)
                    .arg(&mut d_sums)
                    .arg(&n_u32)
                    .launch(*launch_cfg)
                    .map_err(cuda_err)?;
            }

            let all_sums = stream.clone_dtoh(&d_sums).map_err(cuda_err)?;
            let sums: Vec<f64> = all_sums[1..=num_groups].to_vec();
            Ok(Arc::new(Float64Array::from(sums)))
        }
        _ => Err(DataFusionError::Execution(format!(
            "Unsupported data type for GPU sum: {:?}",
            data_type
        ))),
    }
}

/// Collapsed min/max: per-group min or max → num_groups output values.
fn run_collapsed_min_max(
    stream: &Arc<CudaStream>,
    d_group_id: &cudarc::driver::safe::CudaSlice<u32>,
    sorted_col: &ArrayRef,
    data_type: &DataType,
    alloc_size: usize,
    num_groups: usize,
    n_u32: u32,
    launch_cfg: &LaunchConfig,
    is_min: bool,
) -> Result<ArrayRef> {
    let kernel_suffix = if is_min { "min" } else { "max" };

    match data_type {
        DataType::Int64 | DataType::Int32 => {
            let values_i64: Vec<i64> = match data_type {
                DataType::Int64 => {
                    let arr = sorted_col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                        DataFusionError::Execution("Not Int64Array".to_string())
                    })?;
                    arr.values().to_vec()
                }
                DataType::Int32 => {
                    let arr = sorted_col.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                        DataFusionError::Execution("Not Int32Array".to_string())
                    })?;
                    arr.values().iter().map(|v| *v as i64).collect()
                }
                _ => unreachable!(),
            };

            let kernel_name = format!("seg_{}_i64", kernel_suffix);
            let func = super::ordered_ops::get_ordered_ops_function_pub(&kernel_name)
                .ok_or_else(|| DataFusionError::Execution(format!("CUDA kernel '{}' not found", kernel_name)))?;

            let init_val = if is_min { i64::MAX } else { i64::MIN };
            let init: Vec<i64> = vec![init_val; alloc_size];
            let mut d_result = stream.clone_htod(&init).map_err(cuda_err)?;
            let d_data = stream.clone_htod(&values_i64).map_err(cuda_err)?;

            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(d_group_id)
                    .arg(&d_data)
                    .arg(&mut d_result)
                    .arg(&n_u32)
                    .launch(*launch_cfg)
                    .map_err(cuda_err)?;
            }

            let all_results = stream.clone_dtoh(&d_result).map_err(cuda_err)?;
            let results: Vec<i64> = all_results[1..=num_groups].to_vec();

            // If original type was i32, cast back
            if matches!(data_type, DataType::Int32) {
                let i32_results: Vec<i32> = results.iter().map(|v| *v as i32).collect();
                Ok(Arc::new(Int32Array::from(i32_results)))
            } else {
                Ok(Arc::new(Int64Array::from(results)))
            }
        }
        DataType::Float64 | DataType::Float32 => {
            let values_f64: Vec<f64> = match data_type {
                DataType::Float64 => {
                    let arr = sorted_col.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                        DataFusionError::Execution("Not Float64Array".to_string())
                    })?;
                    arr.values().to_vec()
                }
                DataType::Float32 => {
                    let arr = sorted_col.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                        DataFusionError::Execution("Not Float32Array".to_string())
                    })?;
                    arr.values().iter().map(|v| *v as f64).collect()
                }
                _ => unreachable!(),
            };

            let kernel_name = format!("seg_{}_f64", kernel_suffix);
            let func = super::ordered_ops::get_ordered_ops_function_pub(&kernel_name)
                .ok_or_else(|| DataFusionError::Execution(format!("CUDA kernel '{}' not found", kernel_name)))?;

            let init_val: f64 = if is_min { f64::MAX } else { f64::MIN };
            let init: Vec<f64> = vec![init_val; alloc_size];
            let mut d_result = stream.clone_htod(&init).map_err(cuda_err)?;
            let d_data = stream.clone_htod(&values_f64).map_err(cuda_err)?;

            unsafe {
                stream
                    .launch_builder(&func)
                    .arg(d_group_id)
                    .arg(&d_data)
                    .arg(&mut d_result)
                    .arg(&n_u32)
                    .launch(*launch_cfg)
                    .map_err(cuda_err)?;
            }

            let all_results = stream.clone_dtoh(&d_result).map_err(cuda_err)?;
            let results: Vec<f64> = all_results[1..=num_groups].to_vec();

            // If original type was f32, cast back
            if matches!(data_type, DataType::Float32) {
                let f32_results: Vec<f32> = results.iter().map(|v| *v as f32).collect();
                Ok(Arc::new(Float32Array::from(f32_results)))
            } else {
                Ok(Arc::new(Float64Array::from(results)))
            }
        }
        _ => Err(DataFusionError::Execution(format!(
            "Unsupported data type for GPU {}: {:?}",
            kernel_suffix, data_type
        ))),
    }
}

/// Collapsed avg: per-group average → num_groups f64 output values.
fn run_collapsed_avg(
    stream: &Arc<CudaStream>,
    d_group_id: &cudarc::driver::safe::CudaSlice<u32>,
    sorted_col: &ArrayRef,
    data_type: &DataType,
    alloc_size: usize,
    num_groups: usize,
    n_u32: u32,
    launch_cfg: &LaunchConfig,
) -> Result<ArrayRef> {
    // Step 1: count
    let count_func = super::ordered_ops::get_ordered_ops_function_pub("seg_count")
        .ok_or_else(|| DataFusionError::Execution("CUDA kernel 'seg_count' not found".to_string()))?;
    let mut d_counts = stream.alloc_zeros::<i64>(alloc_size).map_err(cuda_err)?;
    unsafe {
        stream
            .launch_builder(&count_func)
            .arg(d_group_id)
            .arg(&mut d_counts)
            .arg(&n_u32)
            .launch(*launch_cfg)
            .map_err(cuda_err)?;
    }
    let all_counts = stream.clone_dtoh(&d_counts).map_err(cuda_err)?;

    // Step 2: sum as f64
    let sum_func = super::ordered_ops::get_ordered_ops_function_pub("seg_sum_f64")
        .ok_or_else(|| DataFusionError::Execution("CUDA kernel 'seg_sum_f64' not found".to_string()))?;
    let mut d_sums = stream.alloc_zeros::<f64>(alloc_size).map_err(cuda_err)?;

    // Convert to f64 if needed
    let f64_values: Vec<f64> = match data_type {
        DataType::Int64 => {
            let arr = sorted_col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Execution("Not Int64Array".to_string())
            })?;
            arr.values().iter().map(|v| *v as f64).collect()
        }
        DataType::Int32 => {
            let arr = sorted_col.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                DataFusionError::Execution("Not Int32Array".to_string())
            })?;
            arr.values().iter().map(|v| *v as f64).collect()
        }
        DataType::Float64 => {
            let arr = sorted_col.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                DataFusionError::Execution("Not Float64Array".to_string())
            })?;
            arr.values().to_vec()
        }
        DataType::Float32 => {
            let arr = sorted_col.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                DataFusionError::Execution("Not Float32Array".to_string())
            })?;
            arr.values().iter().map(|v| *v as f64).collect()
        }
        _ => return Err(DataFusionError::Execution(format!(
            "Unsupported data type for GPU avg: {:?}", data_type
        ))),
    };

    let d_data = stream.clone_htod(&f64_values).map_err(cuda_err)?;
    unsafe {
        stream
            .launch_builder(&sum_func)
            .arg(d_group_id)
            .arg(&d_data)
            .arg(&mut d_sums)
            .arg(&n_u32)
            .launch(*launch_cfg)
            .map_err(cuda_err)?;
    }
    let all_sums = stream.clone_dtoh(&d_sums).map_err(cuda_err)?;

    // Step 3: avg = sum / count (per group, on CPU)
    let mut avgs = Vec::with_capacity(num_groups);
    for i in 1..=num_groups {
        let count = all_counts[i];
        let sum = all_sums[i];
        avgs.push(if count > 0 { sum / count as f64 } else { 0.0 });
    }

    Ok(Arc::new(Float64Array::from(avgs)))
}
