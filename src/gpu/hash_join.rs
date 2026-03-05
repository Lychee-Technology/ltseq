//! GPU-accelerated hash join for unordered tables.
//!
//! # Algorithm
//!
//! Uses a GPU hash table with open addressing (linear probing) for equi-joins:
//!
//! 1. **Build phase**: Upload the build-side (right table) keys to GPU.
//!    Each thread inserts its key+index into a hash table using linear probing.
//!    Table capacity = 2 * build_n for ~50% load factor.
//!
//! 2. **Count phase**: Upload the probe-side (left table) keys to GPU.
//!    Each probe thread counts how many matches it has in the hash table.
//!
//! 3. **Prefix sum**: Exclusive scan on match counts → output offsets.
//!
//! 4. **Scatter phase**: Each probe thread writes its (probe_idx, build_idx) pairs
//!    to the output arrays at the computed offsets.
//!
//! 5. **CPU gather**: Use Arrow `take()` with the index arrays to build the result.
//!
//! ## Supported Types
//!
//! i32, i64, f32, f64 key columns (single-column equi-join only).
//!
//! ## Join Types
//!
//! - Inner: only matched pairs
//! - Left: all left rows, unmatched get NULL right columns

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, UInt32Array,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};

use cudarc::driver::safe::{CudaModule, CudaStream, LaunchConfig};
use cudarc::driver::PushKernelArg;
use cudarc::nvrtc::compile_ptx;
use std::sync::OnceLock;

use super::merge_join::GpuJoinType;
use super::sort_aware::{SortOrderEffect, SortOrderPropagation};
use super::{get_stream, CUDA_BLOCK_SIZE, grid_size};

// ============================================================================
// CUDA Kernel Source
// ============================================================================

/// CUDA kernels for hash join operations:
///
/// Hash table uses open addressing with linear probing.
/// Sentinel value for empty slots: 0xFFFFFFFF (u32 max).
///
/// Build phase: insert (key, row_index) into hash table.
/// Probe phase (count): count matches per probe row.
/// Probe phase (write): write (probe_idx, build_idx) pairs at prefix-sum offsets.
const HASH_JOIN_KERNEL_SRC: &str = r#"
// Sentinel for empty hash table slots
#define EMPTY_SLOT 0xFFFFFFFFu

// ── Hash function (Murmur3-like finalizer for 64-bit keys) ──────────────
// Converts key to u32 hash value in [0, capacity).
__device__ unsigned int hash_key_i64(long long key, unsigned int capacity) {
    unsigned long long k = (unsigned long long)key;
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33;
    return (unsigned int)(k % (unsigned long long)capacity);
}

__device__ unsigned int hash_key_i32(int key, unsigned int capacity) {
    unsigned int k = (unsigned int)key;
    k ^= k >> 16;
    k *= 0x45d9f3b;
    k ^= k >> 16;
    k *= 0x45d9f3b;
    k ^= k >> 16;
    return k % capacity;
}

__device__ unsigned int hash_key_f64(double key, unsigned int capacity) {
    // Treat as i64 for hashing
    long long k;
    memcpy(&k, &key, sizeof(long long));
    return hash_key_i64(k, capacity);
}

__device__ unsigned int hash_key_f32(float key, unsigned int capacity) {
    int k;
    memcpy(&k, &key, sizeof(int));
    return hash_key_i32(k, capacity);
}

// ── Build phase: insert keys into hash table ────────────────────────────
// Hash table layout:
//   ht_keys[capacity]   — keys (as i64/i32/f64/f32)
//   ht_indices[capacity] — original row indices (u32), EMPTY_SLOT if empty
//
// For duplicate keys: each insertion occupies its own slot (no aggregation).
// Linear probing continues until an empty slot is found.

extern "C" __global__ void hash_build_i64(
    const long long* __restrict__ keys,
    unsigned int* __restrict__ ht_indices,
    long long* __restrict__ ht_keys,
    const unsigned int n,
    const unsigned int capacity
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;

    long long key = keys[idx];
    unsigned int slot = hash_key_i64(key, capacity);

    // Linear probing
    while (true) {
        unsigned int old = atomicCAS(&ht_indices[slot], EMPTY_SLOT, idx);
        if (old == EMPTY_SLOT) {
            // Successfully inserted
            ht_keys[slot] = key;
            return;
        }
        slot = (slot + 1) % capacity;
    }
}

extern "C" __global__ void hash_build_i32(
    const int* __restrict__ keys,
    unsigned int* __restrict__ ht_indices,
    int* __restrict__ ht_keys,
    const unsigned int n,
    const unsigned int capacity
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;

    int key = keys[idx];
    unsigned int slot = hash_key_i32(key, capacity);

    while (true) {
        unsigned int old = atomicCAS(&ht_indices[slot], EMPTY_SLOT, idx);
        if (old == EMPTY_SLOT) {
            ht_keys[slot] = key;
            return;
        }
        slot = (slot + 1) % capacity;
    }
}

extern "C" __global__ void hash_build_f64(
    const double* __restrict__ keys,
    unsigned int* __restrict__ ht_indices,
    double* __restrict__ ht_keys,
    const unsigned int n,
    const unsigned int capacity
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;

    double key = keys[idx];
    unsigned int slot = hash_key_f64(key, capacity);

    while (true) {
        unsigned int old = atomicCAS(&ht_indices[slot], EMPTY_SLOT, idx);
        if (old == EMPTY_SLOT) {
            ht_keys[slot] = key;
            return;
        }
        slot = (slot + 1) % capacity;
    }
}

extern "C" __global__ void hash_build_f32(
    const float* __restrict__ keys,
    unsigned int* __restrict__ ht_indices,
    float* __restrict__ ht_keys,
    const unsigned int n,
    const unsigned int capacity
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;

    float key = keys[idx];
    unsigned int slot = hash_key_f32(key, capacity);

    while (true) {
        unsigned int old = atomicCAS(&ht_indices[slot], EMPTY_SLOT, idx);
        if (old == EMPTY_SLOT) {
            ht_keys[slot] = key;
            return;
        }
        slot = (slot + 1) % capacity;
    }
}

// ── Probe phase (count): count matches per probe row ────────────────────
// For each probe key, walk the hash table and count all matching build keys.

extern "C" __global__ void hash_probe_count_i64(
    const long long* __restrict__ probe_keys,
    const unsigned int* __restrict__ ht_indices,
    const long long* __restrict__ ht_keys,
    unsigned int* __restrict__ counts,
    const unsigned int probe_n,
    const unsigned int capacity
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= probe_n) return;

    long long key = probe_keys[idx];
    unsigned int slot = hash_key_i64(key, capacity);
    unsigned int cnt = 0;

    while (true) {
        unsigned int stored_idx = ht_indices[slot];
        if (stored_idx == EMPTY_SLOT) break;
        if (ht_keys[slot] == key) cnt++;
        slot = (slot + 1) % capacity;
    }

    counts[idx] = cnt;
}

extern "C" __global__ void hash_probe_count_i32(
    const int* __restrict__ probe_keys,
    const unsigned int* __restrict__ ht_indices,
    const int* __restrict__ ht_keys,
    unsigned int* __restrict__ counts,
    const unsigned int probe_n,
    const unsigned int capacity
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= probe_n) return;

    int key = probe_keys[idx];
    unsigned int slot = hash_key_i32(key, capacity);
    unsigned int cnt = 0;

    while (true) {
        unsigned int stored_idx = ht_indices[slot];
        if (stored_idx == EMPTY_SLOT) break;
        if (ht_keys[slot] == key) cnt++;
        slot = (slot + 1) % capacity;
    }

    counts[idx] = cnt;
}

extern "C" __global__ void hash_probe_count_f64(
    const double* __restrict__ probe_keys,
    const unsigned int* __restrict__ ht_indices,
    const double* __restrict__ ht_keys,
    unsigned int* __restrict__ counts,
    const unsigned int probe_n,
    const unsigned int capacity
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= probe_n) return;

    double key = probe_keys[idx];
    unsigned int slot = hash_key_f64(key, capacity);
    unsigned int cnt = 0;

    while (true) {
        unsigned int stored_idx = ht_indices[slot];
        if (stored_idx == EMPTY_SLOT) break;
        if (ht_keys[slot] == key) cnt++;
        slot = (slot + 1) % capacity;
    }

    counts[idx] = cnt;
}

extern "C" __global__ void hash_probe_count_f32(
    const float* __restrict__ probe_keys,
    const unsigned int* __restrict__ ht_indices,
    const float* __restrict__ ht_keys,
    unsigned int* __restrict__ counts,
    const unsigned int probe_n,
    const unsigned int capacity
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= probe_n) return;

    float key = probe_keys[idx];
    unsigned int slot = hash_key_f32(key, capacity);
    unsigned int cnt = 0;

    while (true) {
        unsigned int stored_idx = ht_indices[slot];
        if (stored_idx == EMPTY_SLOT) break;
        if (ht_keys[slot] == key) cnt++;
        slot = (slot + 1) % capacity;
    }

    counts[idx] = cnt;
}

// ── Probe phase (scatter): write (probe_idx, build_idx) pairs ───────────
// offsets[i] = exclusive prefix sum of counts[i] (where to start writing).

extern "C" __global__ void hash_probe_scatter_i64(
    const long long* __restrict__ probe_keys,
    const unsigned int* __restrict__ ht_indices,
    const long long* __restrict__ ht_keys,
    const unsigned int* __restrict__ offsets,
    unsigned int* __restrict__ out_probe_idx,
    unsigned int* __restrict__ out_build_idx,
    const unsigned int probe_n,
    const unsigned int capacity
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= probe_n) return;

    long long key = probe_keys[idx];
    unsigned int slot = hash_key_i64(key, capacity);
    unsigned int write_pos = offsets[idx];

    while (true) {
        unsigned int stored_idx = ht_indices[slot];
        if (stored_idx == EMPTY_SLOT) break;
        if (ht_keys[slot] == key) {
            out_probe_idx[write_pos] = idx;
            out_build_idx[write_pos] = stored_idx;
            write_pos++;
        }
        slot = (slot + 1) % capacity;
    }
}

extern "C" __global__ void hash_probe_scatter_i32(
    const int* __restrict__ probe_keys,
    const unsigned int* __restrict__ ht_indices,
    const int* __restrict__ ht_keys,
    const unsigned int* __restrict__ offsets,
    unsigned int* __restrict__ out_probe_idx,
    unsigned int* __restrict__ out_build_idx,
    const unsigned int probe_n,
    const unsigned int capacity
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= probe_n) return;

    int key = probe_keys[idx];
    unsigned int slot = hash_key_i32(key, capacity);
    unsigned int write_pos = offsets[idx];

    while (true) {
        unsigned int stored_idx = ht_indices[slot];
        if (stored_idx == EMPTY_SLOT) break;
        if (ht_keys[slot] == key) {
            out_probe_idx[write_pos] = idx;
            out_build_idx[write_pos] = stored_idx;
            write_pos++;
        }
        slot = (slot + 1) % capacity;
    }
}

extern "C" __global__ void hash_probe_scatter_f64(
    const double* __restrict__ probe_keys,
    const unsigned int* __restrict__ ht_indices,
    const double* __restrict__ ht_keys,
    const unsigned int* __restrict__ offsets,
    unsigned int* __restrict__ out_probe_idx,
    unsigned int* __restrict__ out_build_idx,
    const unsigned int probe_n,
    const unsigned int capacity
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= probe_n) return;

    double key = probe_keys[idx];
    unsigned int slot = hash_key_f64(key, capacity);
    unsigned int write_pos = offsets[idx];

    while (true) {
        unsigned int stored_idx = ht_indices[slot];
        if (stored_idx == EMPTY_SLOT) break;
        if (ht_keys[slot] == key) {
            out_probe_idx[write_pos] = idx;
            out_build_idx[write_pos] = stored_idx;
            write_pos++;
        }
        slot = (slot + 1) % capacity;
    }
}

extern "C" __global__ void hash_probe_scatter_f32(
    const float* __restrict__ probe_keys,
    const unsigned int* __restrict__ ht_indices,
    const float* __restrict__ ht_keys,
    const unsigned int* __restrict__ offsets,
    unsigned int* __restrict__ out_probe_idx,
    unsigned int* __restrict__ out_build_idx,
    const unsigned int probe_n,
    const unsigned int capacity
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= probe_n) return;

    float key = probe_keys[idx];
    unsigned int slot = hash_key_f32(key, capacity);
    unsigned int write_pos = offsets[idx];

    while (true) {
        unsigned int stored_idx = ht_indices[slot];
        if (stored_idx == EMPTY_SLOT) break;
        if (ht_keys[slot] == key) {
            out_probe_idx[write_pos] = idx;
            out_build_idx[write_pos] = stored_idx;
            write_pos++;
        }
        slot = (slot + 1) % capacity;
    }
}

// ── Prefix sum (exclusive, u32) for output offset computation ───────────
// Simple serial prefix sum (run on a single thread). Fine for probe_n < 100M.
// For larger inputs, use a parallel scan.
extern "C" __global__ void exclusive_prefix_sum_u32(
    const unsigned int* __restrict__ input,
    unsigned int* __restrict__ output,
    const unsigned int n
) {
    // Single-thread serial scan
    if (blockIdx.x != 0 || threadIdx.x != 0) return;
    unsigned int running = 0;
    for (unsigned int i = 0; i < n; i++) {
        output[i] = running;
        running += input[i];
    }
}
"#;

// ============================================================================
// CUDA Module Cache
// ============================================================================

static HASH_JOIN_MODULE: OnceLock<Option<Arc<CudaModule>>> = OnceLock::new();

fn get_hash_join_module() -> Option<Arc<CudaModule>> {
    HASH_JOIN_MODULE
        .get_or_init(|| {
            let ctx = super::get_context()?;
            let ptx = compile_ptx(HASH_JOIN_KERNEL_SRC).ok()?;
            let module = ctx.load_module(ptx).ok()?;
            Some(module)
        })
        .clone()
}

fn get_hash_join_function(name: &str) -> Option<cudarc::driver::safe::CudaFunction> {
    get_hash_join_module()?.load_function(name).ok()
}

fn cuda_err(e: cudarc::driver::safe::DriverError) -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::Execution(format!("CUDA error: {:?}", e))
}

// ============================================================================
// GPU Hash Join — Core Algorithm
// ============================================================================

/// Configuration for GPU hash join.
#[derive(Debug, Clone)]
pub struct GpuHashJoinConfig {
    /// Left (probe) key column name
    pub left_key: String,
    /// Left key column index in left schema
    pub left_key_idx: usize,
    /// Right (build) key column name
    pub right_key: String,
    /// Right key column index in right schema
    pub right_key_idx: usize,
    /// Key data type (must be same for both sides)
    pub key_type: DataType,
    /// Join type
    pub join_type: GpuJoinType,
    /// Whether inputs were swapped (Right join → Left join with swapped sides).
    /// When true, the GPU runs probe=left, build=right as Left join, but
    /// output columns are reordered to [right_cols, left_cols] to match
    /// the original HashJoinExec schema.
    pub swapped: bool,
}

/// Hash table capacity: 2x the build side for ~50% load factor.
fn hash_table_capacity(build_n: usize) -> usize {
    let cap = (build_n * 2).max(64);
    // Round up to next power of 2 for better hash distribution
    cap.next_power_of_two()
}

/// Run the complete GPU hash join pipeline.
///
/// Returns (probe_indices, build_indices) as UInt32Arrays.
/// For left join, unmatched probe rows have null in build_indices.
fn gpu_hash_join(
    stream: &Arc<CudaStream>,
    probe_keys: &ArrayRef,
    build_keys: &ArrayRef,
    key_type: &DataType,
    join_type: GpuJoinType,
) -> Result<(UInt32Array, UInt32Array)> {
    let probe_n = probe_keys.len();
    let build_n = build_keys.len();

    // Handle empty edge cases
    if probe_n == 0 {
        return Ok((
            UInt32Array::from(Vec::<u32>::new()),
            UInt32Array::from(Vec::<u32>::new()),
        ));
    }

    if build_n == 0 {
        return match join_type {
            GpuJoinType::Inner => Ok((
                UInt32Array::from(Vec::<u32>::new()),
                UInt32Array::from(Vec::<u32>::new()),
            )),
            GpuJoinType::Left => {
                // All probe rows with null build indices
                let probe_idx: Vec<u32> = (0..probe_n as u32).collect();
                let build_idx: Vec<Option<u32>> = vec![None; probe_n];
                Ok((
                    UInt32Array::from(probe_idx),
                    UInt32Array::from(build_idx),
                ))
            }
        };
    }

    let capacity = hash_table_capacity(build_n);

    if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
        eprintln!(
            "[GPU] hash_join: probe_n={}, build_n={}, capacity={}, type={:?}, join={:?}",
            probe_n, build_n, capacity, key_type, join_type
        );
    }

    // Step 1: Build hash table on GPU
    build_hash_table(stream, build_keys, key_type, capacity)?;

    // Step 2: Count matches per probe row
    let counts = probe_count(stream, probe_keys, key_type, probe_n, capacity)?;

    // Step 3: Exclusive prefix sum → offsets
    let offsets = cpu_exclusive_prefix_sum(&counts);
    let total_matched: usize = offsets.last().copied().unwrap_or(0) as usize
        + counts.last().copied().unwrap_or(0) as usize;

    // Step 4: Scatter matched pairs
    let (matched_probe_idx, matched_build_idx) = if total_matched > 0 {
        probe_scatter(stream, probe_keys, key_type, &offsets, probe_n, capacity, total_matched)?
    } else {
        (Vec::new(), Vec::new())
    };

    // Step 5: Assemble output based on join type
    match join_type {
        GpuJoinType::Inner => {
            Ok((
                UInt32Array::from(matched_probe_idx),
                UInt32Array::from(matched_build_idx),
            ))
        }
        GpuJoinType::Left => {
            // For left join, we need all probe rows. Matched rows come first,
            // then unmatched rows with NULL build indices.
            let mut probe_out: Vec<u32> = Vec::with_capacity(total_matched + probe_n);
            let mut build_out: Vec<Option<u32>> = Vec::with_capacity(total_matched + probe_n);

            // Add matched pairs
            for i in 0..total_matched {
                probe_out.push(matched_probe_idx[i]);
                build_out.push(Some(matched_build_idx[i]));
            }

            // Add unmatched probe rows
            for i in 0..probe_n {
                if counts[i] == 0 {
                    probe_out.push(i as u32);
                    build_out.push(None);
                }
            }

            Ok((
                UInt32Array::from(probe_out),
                UInt32Array::from(build_out),
            ))
        }
    }
}

// ============================================================================
// GPU Operations — Build, Count, Scatter
// ============================================================================

/// Thread-local (task-level) storage for the hash table device buffers.
/// We keep them alive across build → probe steps within one join execution.
use std::cell::RefCell;

thread_local! {
    static HT_INDICES: RefCell<Option<cudarc::driver::safe::CudaSlice<u32>>> = const { RefCell::new(None) };
    static HT_KEYS_I64: RefCell<Option<cudarc::driver::safe::CudaSlice<i64>>> = const { RefCell::new(None) };
    static HT_KEYS_I32: RefCell<Option<cudarc::driver::safe::CudaSlice<i32>>> = const { RefCell::new(None) };
    static HT_KEYS_F64: RefCell<Option<cudarc::driver::safe::CudaSlice<f64>>> = const { RefCell::new(None) };
    static HT_KEYS_F32: RefCell<Option<cudarc::driver::safe::CudaSlice<f32>>> = const { RefCell::new(None) };
}

/// Build the hash table on GPU. Stores results in thread-local storage
/// for subsequent probe operations.
fn build_hash_table(
    stream: &Arc<CudaStream>,
    build_keys: &ArrayRef,
    key_type: &DataType,
    capacity: usize,
) -> Result<()> {
    // Initialize hash table indices to EMPTY_SLOT (0xFFFFFFFF)
    let init_vals = vec![0xFFFFFFFFu32; capacity];
    let d_ht_indices = stream.clone_htod(&init_vals).map_err(cuda_err)?;

    let build_n = build_keys.len();
    let build_n_u32 = build_n as u32;
    let capacity_u32 = capacity as u32;

    let launch_cfg = LaunchConfig {
        grid_dim: (grid_size(build_n_u32), 1, 1),
        block_dim: (CUDA_BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    match key_type {
        DataType::Int64 => {
            let arr = build_keys.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                    "Build key is not Int64Array".into(),
                ))?;
            let d_keys = stream.clone_htod(arr.values().as_ref()).map_err(cuda_err)?;
            let mut d_ht_keys = stream.alloc_zeros::<i64>(capacity).map_err(cuda_err)?;
            let func = get_hash_join_function("hash_build_i64")
                .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                    "CUDA kernel 'hash_build_i64' not found".into(),
                ))?;
            let mut d_ht_idx = d_ht_indices;
            unsafe {
                stream.launch_builder(&func)
                    .arg(&d_keys)
                    .arg(&mut d_ht_idx)
                    .arg(&mut d_ht_keys)
                    .arg(&build_n_u32)
                    .arg(&capacity_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
            HT_INDICES.with(|cell| *cell.borrow_mut() = Some(d_ht_idx));
            HT_KEYS_I64.with(|cell| *cell.borrow_mut() = Some(d_ht_keys));
        }
        DataType::Int32 => {
            let arr = build_keys.as_any().downcast_ref::<Int32Array>()
                .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                    "Build key is not Int32Array".into(),
                ))?;
            let d_keys = stream.clone_htod(arr.values().as_ref()).map_err(cuda_err)?;
            let mut d_ht_keys = stream.alloc_zeros::<i32>(capacity).map_err(cuda_err)?;
            let func = get_hash_join_function("hash_build_i32")
                .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                    "CUDA kernel 'hash_build_i32' not found".into(),
                ))?;
            let mut d_ht_idx = d_ht_indices;
            unsafe {
                stream.launch_builder(&func)
                    .arg(&d_keys)
                    .arg(&mut d_ht_idx)
                    .arg(&mut d_ht_keys)
                    .arg(&build_n_u32)
                    .arg(&capacity_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
            HT_INDICES.with(|cell| *cell.borrow_mut() = Some(d_ht_idx));
            HT_KEYS_I32.with(|cell| *cell.borrow_mut() = Some(d_ht_keys));
        }
        DataType::Float64 => {
            let arr = build_keys.as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                    "Build key is not Float64Array".into(),
                ))?;
            let d_keys = stream.clone_htod(arr.values().as_ref()).map_err(cuda_err)?;
            let mut d_ht_keys = stream.alloc_zeros::<f64>(capacity).map_err(cuda_err)?;
            let func = get_hash_join_function("hash_build_f64")
                .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                    "CUDA kernel 'hash_build_f64' not found".into(),
                ))?;
            let mut d_ht_idx = d_ht_indices;
            unsafe {
                stream.launch_builder(&func)
                    .arg(&d_keys)
                    .arg(&mut d_ht_idx)
                    .arg(&mut d_ht_keys)
                    .arg(&build_n_u32)
                    .arg(&capacity_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
            HT_INDICES.with(|cell| *cell.borrow_mut() = Some(d_ht_idx));
            HT_KEYS_F64.with(|cell| *cell.borrow_mut() = Some(d_ht_keys));
        }
        DataType::Float32 => {
            let arr = build_keys.as_any().downcast_ref::<Float32Array>()
                .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                    "Build key is not Float32Array".into(),
                ))?;
            let d_keys = stream.clone_htod(arr.values().as_ref()).map_err(cuda_err)?;
            let mut d_ht_keys = stream.alloc_zeros::<f32>(capacity).map_err(cuda_err)?;
            let func = get_hash_join_function("hash_build_f32")
                .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                    "CUDA kernel 'hash_build_f32' not found".into(),
                ))?;
            let mut d_ht_idx = d_ht_indices;
            unsafe {
                stream.launch_builder(&func)
                    .arg(&d_keys)
                    .arg(&mut d_ht_idx)
                    .arg(&mut d_ht_keys)
                    .arg(&build_n_u32)
                    .arg(&capacity_u32)
                    .launch(launch_cfg)
                    .map_err(cuda_err)?;
            }
            HT_INDICES.with(|cell| *cell.borrow_mut() = Some(d_ht_idx));
            HT_KEYS_F32.with(|cell| *cell.borrow_mut() = Some(d_ht_keys));
        }
        _ => {
            return Err(datafusion::common::DataFusionError::Execution(
                format!("Unsupported key type for GPU hash join: {:?}", key_type),
            ));
        }
    }

    Ok(())
}

/// Probe the hash table and count matches per probe row.
/// Returns counts as a host Vec<u32>.
fn probe_count(
    stream: &Arc<CudaStream>,
    probe_keys: &ArrayRef,
    key_type: &DataType,
    probe_n: usize,
    capacity: usize,
) -> Result<Vec<u32>> {
    let mut d_counts = stream.alloc_zeros::<u32>(probe_n).map_err(cuda_err)?;
    let probe_n_u32 = probe_n as u32;
    let capacity_u32 = capacity as u32;

    let launch_cfg = LaunchConfig {
        grid_dim: (grid_size(probe_n_u32), 1, 1),
        block_dim: (CUDA_BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    match key_type {
        DataType::Int64 => {
            let arr = probe_keys.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                    "Probe key is not Int64Array".into(),
                ))?;
            let d_probe = stream.clone_htod(arr.values().as_ref()).map_err(cuda_err)?;
            let func = get_hash_join_function("hash_probe_count_i64")
                .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                    "CUDA kernel 'hash_probe_count_i64' not found".into(),
                ))?;
            HT_INDICES.with(|cell| -> Result<()> {
                let borrow = cell.borrow();
                let d_ht_idx = borrow.as_ref().ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution("Hash table not built".into())
                })?;
                HT_KEYS_I64.with(|kcell| -> Result<()> {
                    let kborrow = kcell.borrow();
                    let d_ht_keys = kborrow.as_ref().ok_or_else(|| {
                        datafusion::common::DataFusionError::Execution("Hash table keys not found".into())
                    })?;
                    unsafe {
                        stream.launch_builder(&func)
                            .arg(&d_probe)
                            .arg(d_ht_idx)
                            .arg(d_ht_keys)
                            .arg(&mut d_counts)
                            .arg(&probe_n_u32)
                            .arg(&capacity_u32)
                            .launch(launch_cfg)
                            .map_err(cuda_err)?;
                    }
                    Ok(())
                })
            })?;
        }
        DataType::Int32 => {
            let arr = probe_keys.as_any().downcast_ref::<Int32Array>()
                .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                    "Probe key is not Int32Array".into(),
                ))?;
            let d_probe = stream.clone_htod(arr.values().as_ref()).map_err(cuda_err)?;
            let func = get_hash_join_function("hash_probe_count_i32")
                .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                    "CUDA kernel 'hash_probe_count_i32' not found".into(),
                ))?;
            HT_INDICES.with(|cell| -> Result<()> {
                let borrow = cell.borrow();
                let d_ht_idx = borrow.as_ref().ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution("Hash table not built".into())
                })?;
                HT_KEYS_I32.with(|kcell| -> Result<()> {
                    let kborrow = kcell.borrow();
                    let d_ht_keys = kborrow.as_ref().ok_or_else(|| {
                        datafusion::common::DataFusionError::Execution("Hash table keys not found".into())
                    })?;
                    unsafe {
                        stream.launch_builder(&func)
                            .arg(&d_probe)
                            .arg(d_ht_idx)
                            .arg(d_ht_keys)
                            .arg(&mut d_counts)
                            .arg(&probe_n_u32)
                            .arg(&capacity_u32)
                            .launch(launch_cfg)
                            .map_err(cuda_err)?;
                    }
                    Ok(())
                })
            })?;
        }
        DataType::Float64 => {
            let arr = probe_keys.as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                    "Probe key is not Float64Array".into(),
                ))?;
            let d_probe = stream.clone_htod(arr.values().as_ref()).map_err(cuda_err)?;
            let func = get_hash_join_function("hash_probe_count_f64")
                .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                    "CUDA kernel 'hash_probe_count_f64' not found".into(),
                ))?;
            HT_INDICES.with(|cell| -> Result<()> {
                let borrow = cell.borrow();
                let d_ht_idx = borrow.as_ref().ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution("Hash table not built".into())
                })?;
                HT_KEYS_F64.with(|kcell| -> Result<()> {
                    let kborrow = kcell.borrow();
                    let d_ht_keys = kborrow.as_ref().ok_or_else(|| {
                        datafusion::common::DataFusionError::Execution("Hash table keys not found".into())
                    })?;
                    unsafe {
                        stream.launch_builder(&func)
                            .arg(&d_probe)
                            .arg(d_ht_idx)
                            .arg(d_ht_keys)
                            .arg(&mut d_counts)
                            .arg(&probe_n_u32)
                            .arg(&capacity_u32)
                            .launch(launch_cfg)
                            .map_err(cuda_err)?;
                    }
                    Ok(())
                })
            })?;
        }
        DataType::Float32 => {
            let arr = probe_keys.as_any().downcast_ref::<Float32Array>()
                .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                    "Probe key is not Float32Array".into(),
                ))?;
            let d_probe = stream.clone_htod(arr.values().as_ref()).map_err(cuda_err)?;
            let func = get_hash_join_function("hash_probe_count_f32")
                .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                    "CUDA kernel 'hash_probe_count_f32' not found".into(),
                ))?;
            HT_INDICES.with(|cell| -> Result<()> {
                let borrow = cell.borrow();
                let d_ht_idx = borrow.as_ref().ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution("Hash table not built".into())
                })?;
                HT_KEYS_F32.with(|kcell| -> Result<()> {
                    let kborrow = kcell.borrow();
                    let d_ht_keys = kborrow.as_ref().ok_or_else(|| {
                        datafusion::common::DataFusionError::Execution("Hash table keys not found".into())
                    })?;
                    unsafe {
                        stream.launch_builder(&func)
                            .arg(&d_probe)
                            .arg(d_ht_idx)
                            .arg(d_ht_keys)
                            .arg(&mut d_counts)
                            .arg(&probe_n_u32)
                            .arg(&capacity_u32)
                            .launch(launch_cfg)
                            .map_err(cuda_err)?;
                    }
                    Ok(())
                })
            })?;
        }
        _ => {
            return Err(datafusion::common::DataFusionError::Execution(
                format!("Unsupported key type for GPU hash join probe: {:?}", key_type),
            ));
        }
    }

    let counts = stream.clone_dtoh(&d_counts).map_err(cuda_err)?;
    Ok(counts)
}

/// Compute exclusive prefix sum on CPU.
fn cpu_exclusive_prefix_sum(counts: &[u32]) -> Vec<u32> {
    let mut offsets = Vec::with_capacity(counts.len());
    let mut running = 0u32;
    for &c in counts {
        offsets.push(running);
        running += c;
    }
    offsets
}

/// Probe the hash table and scatter matched (probe_idx, build_idx) pairs.
fn probe_scatter(
    stream: &Arc<CudaStream>,
    probe_keys: &ArrayRef,
    key_type: &DataType,
    offsets: &[u32],
    probe_n: usize,
    capacity: usize,
    total_output: usize,
) -> Result<(Vec<u32>, Vec<u32>)> {
    let d_offsets = stream.clone_htod(offsets).map_err(cuda_err)?;
    let mut d_out_probe = stream.alloc_zeros::<u32>(total_output).map_err(cuda_err)?;
    let mut d_out_build = stream.alloc_zeros::<u32>(total_output).map_err(cuda_err)?;
    let probe_n_u32 = probe_n as u32;
    let capacity_u32 = capacity as u32;

    let launch_cfg = LaunchConfig {
        grid_dim: (grid_size(probe_n_u32), 1, 1),
        block_dim: (CUDA_BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    match key_type {
        DataType::Int64 => {
            let arr = probe_keys.as_any().downcast_ref::<Int64Array>().unwrap();
            let d_probe = stream.clone_htod(arr.values().as_ref()).map_err(cuda_err)?;
            let func = get_hash_join_function("hash_probe_scatter_i64")
                .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                    "CUDA kernel 'hash_probe_scatter_i64' not found".into(),
                ))?;
            HT_INDICES.with(|cell| -> Result<()> {
                let borrow = cell.borrow();
                let d_ht_idx = borrow.as_ref().unwrap();
                HT_KEYS_I64.with(|kcell| -> Result<()> {
                    let kborrow = kcell.borrow();
                    let d_ht_keys = kborrow.as_ref().unwrap();
                    unsafe {
                        stream.launch_builder(&func)
                            .arg(&d_probe)
                            .arg(d_ht_idx)
                            .arg(d_ht_keys)
                            .arg(&d_offsets)
                            .arg(&mut d_out_probe)
                            .arg(&mut d_out_build)
                            .arg(&probe_n_u32)
                            .arg(&capacity_u32)
                            .launch(launch_cfg)
                            .map_err(cuda_err)?;
                    }
                    Ok(())
                })
            })?;
        }
        DataType::Int32 => {
            let arr = probe_keys.as_any().downcast_ref::<Int32Array>().unwrap();
            let d_probe = stream.clone_htod(arr.values().as_ref()).map_err(cuda_err)?;
            let func = get_hash_join_function("hash_probe_scatter_i32")
                .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                    "CUDA kernel 'hash_probe_scatter_i32' not found".into(),
                ))?;
            HT_INDICES.with(|cell| -> Result<()> {
                let borrow = cell.borrow();
                let d_ht_idx = borrow.as_ref().unwrap();
                HT_KEYS_I32.with(|kcell| -> Result<()> {
                    let kborrow = kcell.borrow();
                    let d_ht_keys = kborrow.as_ref().unwrap();
                    unsafe {
                        stream.launch_builder(&func)
                            .arg(&d_probe)
                            .arg(d_ht_idx)
                            .arg(d_ht_keys)
                            .arg(&d_offsets)
                            .arg(&mut d_out_probe)
                            .arg(&mut d_out_build)
                            .arg(&probe_n_u32)
                            .arg(&capacity_u32)
                            .launch(launch_cfg)
                            .map_err(cuda_err)?;
                    }
                    Ok(())
                })
            })?;
        }
        DataType::Float64 => {
            let arr = probe_keys.as_any().downcast_ref::<Float64Array>().unwrap();
            let d_probe = stream.clone_htod(arr.values().as_ref()).map_err(cuda_err)?;
            let func = get_hash_join_function("hash_probe_scatter_f64")
                .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                    "CUDA kernel 'hash_probe_scatter_f64' not found".into(),
                ))?;
            HT_INDICES.with(|cell| -> Result<()> {
                let borrow = cell.borrow();
                let d_ht_idx = borrow.as_ref().unwrap();
                HT_KEYS_F64.with(|kcell| -> Result<()> {
                    let kborrow = kcell.borrow();
                    let d_ht_keys = kborrow.as_ref().unwrap();
                    unsafe {
                        stream.launch_builder(&func)
                            .arg(&d_probe)
                            .arg(d_ht_idx)
                            .arg(d_ht_keys)
                            .arg(&d_offsets)
                            .arg(&mut d_out_probe)
                            .arg(&mut d_out_build)
                            .arg(&probe_n_u32)
                            .arg(&capacity_u32)
                            .launch(launch_cfg)
                            .map_err(cuda_err)?;
                    }
                    Ok(())
                })
            })?;
        }
        DataType::Float32 => {
            let arr = probe_keys.as_any().downcast_ref::<Float32Array>().unwrap();
            let d_probe = stream.clone_htod(arr.values().as_ref()).map_err(cuda_err)?;
            let func = get_hash_join_function("hash_probe_scatter_f32")
                .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                    "CUDA kernel 'hash_probe_scatter_f32' not found".into(),
                ))?;
            HT_INDICES.with(|cell| -> Result<()> {
                let borrow = cell.borrow();
                let d_ht_idx = borrow.as_ref().unwrap();
                HT_KEYS_F32.with(|kcell| -> Result<()> {
                    let kborrow = kcell.borrow();
                    let d_ht_keys = kborrow.as_ref().unwrap();
                    unsafe {
                        stream.launch_builder(&func)
                            .arg(&d_probe)
                            .arg(d_ht_idx)
                            .arg(d_ht_keys)
                            .arg(&d_offsets)
                            .arg(&mut d_out_probe)
                            .arg(&mut d_out_build)
                            .arg(&probe_n_u32)
                            .arg(&capacity_u32)
                            .launch(launch_cfg)
                            .map_err(cuda_err)?;
                    }
                    Ok(())
                })
            })?;
        }
        _ => unreachable!(),
    }

    let out_probe = stream.clone_dtoh(&d_out_probe).map_err(cuda_err)?;
    let out_build = stream.clone_dtoh(&d_out_build).map_err(cuda_err)?;

    // Clean up thread-local hash table references
    HT_INDICES.with(|cell| *cell.borrow_mut() = None);
    HT_KEYS_I64.with(|cell| *cell.borrow_mut() = None);
    HT_KEYS_I32.with(|cell| *cell.borrow_mut() = None);
    HT_KEYS_F64.with(|cell| *cell.borrow_mut() = None);
    HT_KEYS_F32.with(|cell| *cell.borrow_mut() = None);

    Ok((out_probe, out_build))
}

// ============================================================================
// GpuHashJoinExec — DataFusion ExecutionPlan
// ============================================================================

/// GPU-accelerated hash join execution plan.
///
/// Intercepts DataFusion's `HashJoinExec` for single-column equi-joins on
/// numeric types. Uses a GPU hash table (open addressing, linear probing).
#[derive(Debug)]
pub struct GpuHashJoinExec {
    /// Left (probe) input plan
    left: Arc<dyn ExecutionPlan>,
    /// Right (build) input plan
    right: Arc<dyn ExecutionPlan>,
    /// Join configuration
    config: GpuHashJoinConfig,
    /// Output schema (matches original HashJoinExec schema)
    output_schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cached plan properties
    cache: PlanProperties,
}

impl GpuHashJoinExec {
    /// Create a new GpuHashJoinExec.
    ///
    /// Left is the probe side, right is the build side.
    /// For multi-partition inputs, wraps in CoalescePartitionsExec.
    pub fn try_new(
        config: GpuHashJoinConfig,
        mut left: Arc<dyn ExecutionPlan>,
        mut right: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
    ) -> Result<Self> {
        // Coalesce partitions if needed
        if left.output_partitioning().partition_count() > 1 {
            left = Arc::new(
                datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(left),
            );
        }
        if right.output_partitioning().partition_count() > 1 {
            right = Arc::new(
                datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(right),
            );
        }

        let cache = Self::compute_properties(&left, &output_schema);

        Ok(Self {
            left,
            right,
            config,
            output_schema,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }

    fn compute_properties(
        left: &Arc<dyn ExecutionPlan>,
        _output_schema: &SchemaRef,
    ) -> PlanProperties {
        PlanProperties::new(
            left.equivalence_properties().clone(),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            left.boundedness(),
        )
    }
}

impl DisplayAs for GpuHashJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "GpuHashJoinExec: left_key={}, right_key={}, type={:?}, key_type={:?} [CUDA]",
                    self.config.left_key, self.config.right_key,
                    self.config.join_type, self.config.key_type
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "left_key={} right_key={} type={:?} [CUDA]",
                    self.config.left_key, self.config.right_key,
                    self.config.join_type
                )
            }
        }
    }
}

impl ExecutionPlan for GpuHashJoinExec {
    fn name(&self) -> &'static str {
        "GpuHashJoinExec"
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
        vec![&self.left, &self.right]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false, false]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let right = children.pop().unwrap();
        let left = children.pop().unwrap();
        Ok(Arc::new(GpuHashJoinExec::try_new(
            self.config.clone(),
            left,
            right,
            Arc::clone(&self.output_schema),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let left_stream = self.left.execute(partition, Arc::clone(&context))?;
        let right_stream = self.right.execute(partition, context)?;
        let schema = Arc::clone(&self.output_schema);
        let config = self.config.clone();
        let left_schema = self.left.schema();
        let right_schema = self.right.schema();
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        let output_stream = futures_util::stream::once(async move {
            let timer = metrics.elapsed_compute().timer();

            // Collect both sides
            let left_batches: Vec<RecordBatch> =
                futures_util::StreamExt::collect::<Vec<_>>(left_stream)
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?;

            let right_batches: Vec<RecordBatch> =
                futures_util::StreamExt::collect::<Vec<_>>(right_stream)
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?;

            if left_batches.is_empty()
                || (right_batches.is_empty() && config.join_type == GpuJoinType::Inner)
            {
                timer.done();
                return Ok(RecordBatch::new_empty(schema));
            }

            // Coalesce into single batches
            let left_batch = datafusion::arrow::compute::concat_batches(
                &left_schema,
                left_batches.iter(),
            )?;
            let right_batch = if right_batches.is_empty() {
                RecordBatch::new_empty(right_schema.clone())
            } else {
                datafusion::arrow::compute::concat_batches(
                    &right_schema,
                    right_batches.iter(),
                )?
            };

            // Extract key columns
            let left_key_col = Arc::clone(left_batch.column(config.left_key_idx));
            let right_key_col = Arc::clone(right_batch.column(config.right_key_idx));

            // Get CUDA stream
            let stream = get_stream().ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "GPU stream not available".into(),
                )
            })?;

            // Run GPU hash join
            let (probe_idx, build_idx) = gpu_hash_join(
                &stream,
                &left_key_col,
                &right_key_col,
                &config.key_type,
                config.join_type,
            )?;

            // Gather result columns using take()
            let mut result_columns: Vec<ArrayRef> = Vec::new();

            if config.swapped {
                // Right join implemented as Left join with swapped inputs.
                // Output order: original_left (=build) columns, then original_right (=probe) columns.
                // In the swapped execution: left_batch=original_right (probe), right_batch=original_left (build).
                // So: build columns (right_batch via build_idx) first, then probe columns (left_batch via probe_idx).
                for col_idx in 0..right_batch.num_columns() {
                    let col = right_batch.column(col_idx);
                    let taken = datafusion::arrow::compute::take(col, &build_idx, None)?;
                    result_columns.push(taken);
                }
                for col_idx in 0..left_batch.num_columns() {
                    let col = left_batch.column(col_idx);
                    let taken = datafusion::arrow::compute::take(col, &probe_idx, None)?;
                    result_columns.push(taken);
                }
            } else {
                // Normal order: left (probe) columns, then right (build) columns
                for col_idx in 0..left_batch.num_columns() {
                    let col = left_batch.column(col_idx);
                    let taken = datafusion::arrow::compute::take(col, &probe_idx, None)?;
                    result_columns.push(taken);
                }
                for col_idx in 0..right_batch.num_columns() {
                    let col = right_batch.column(col_idx);
                    let taken = datafusion::arrow::compute::take(col, &build_idx, None)?;
                    result_columns.push(taken);
                }
            }

            let result = RecordBatch::try_new(schema.clone(), result_columns)?;
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
}

impl SortOrderPropagation for GpuHashJoinExec {
    /// Hash join destroys sort order.
    fn sort_order_effect(&self) -> SortOrderEffect {
        SortOrderEffect::Destroy
    }
}

// ============================================================================
// Helper: Check eligibility for GPU hash join
// ============================================================================

/// Check if a data type is supported for GPU hash join keys.
pub fn is_hash_join_key_type_supported(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int64 | DataType::Int32 | DataType::Float64 | DataType::Float32
    )
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_join_key_type_support() {
        assert!(is_hash_join_key_type_supported(&DataType::Int64));
        assert!(is_hash_join_key_type_supported(&DataType::Int32));
        assert!(is_hash_join_key_type_supported(&DataType::Float64));
        assert!(is_hash_join_key_type_supported(&DataType::Float32));
        assert!(!is_hash_join_key_type_supported(&DataType::Utf8));
        assert!(!is_hash_join_key_type_supported(&DataType::Boolean));
    }

    #[test]
    fn test_hash_table_capacity() {
        assert_eq!(hash_table_capacity(10), 64);  // min 64
        assert_eq!(hash_table_capacity(100), 256); // 200 → next power of 2 = 256
        assert_eq!(hash_table_capacity(1000), 2048); // 2000 → 2048
    }

    #[test]
    fn test_cpu_exclusive_prefix_sum() {
        let counts = vec![3u32, 0, 2, 1, 0];
        let offsets = cpu_exclusive_prefix_sum(&counts);
        assert_eq!(offsets, vec![0, 3, 3, 5, 6]);
    }

    #[test]
    fn test_cpu_exclusive_prefix_sum_empty() {
        let counts: Vec<u32> = vec![];
        let offsets = cpu_exclusive_prefix_sum(&counts);
        assert!(offsets.is_empty());
    }
}
