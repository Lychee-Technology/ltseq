//! Arrow ↔ GPU buffer conversion for the filter shader.
//!
//! Handles:
//! - `FilterParams` Pod struct (matches WGSL `Params` uniform layout)
//! - Uploading Arrow column data (i32, i64, f32) to wgpu storage buffers
//! - Downloading GPU bitmask output to Arrow `BooleanArray`
//!
//! # Pooled helpers
//! Functions ending in `_pooled` acquire from (and callers must release to)
//! `ctx.buffer_pool`.  The non-pooled variants are kept for callers that already
//! manage buffer lifetimes themselves (e.g. fused pipeline nodes that hold
//! buffers across multiple operations).

use bytemuck::{Pod, Zeroable};
use datafusion::arrow::array::{Array, BooleanArray, Float32Array, Int32Array, Int64Array};
use datafusion::arrow::buffer::BooleanBuffer;
use wgpu::util::DeviceExt;

use super::context::WgpuContext;

// ── Column type tags (must match WGSL Params.col_type) ─────────────────────
pub const COL_TYPE_I32: u32 = 0;
pub const COL_TYPE_I64: u32 = 1;
pub const COL_TYPE_F32: u32 = 2;

// ── Compare op tags (must match WGSL) ──────────────────────────────────────
pub const OP_EQ: u32 = 0;
pub const OP_NEQ: u32 = 1;
pub const OP_LT: u32 = 2;
pub const OP_LTE: u32 = 3;
pub const OP_GT: u32 = 4;
pub const OP_GTE: u32 = 5;

// ── FilterParams (32 bytes, matches WGSL `Params` struct layout) ───────────
//
// WGSL layout (std140-like):
//   offset 0:  op          (u32)
//   offset 4:  col_type    (u32)
//   offset 8:  num_rows    (u32)
//   offset 12: _pad        (u32)
//   offset 16: literal_lo  (i32)
//   offset 20: literal_hi  (i32)
//   offset 24: _pad2       (i32)
//   offset 28: _pad3       (i32)
//   total: 32 bytes
#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable)]
pub struct FilterParams {
    pub op: u32,
    pub col_type: u32,
    pub num_rows: u32,
    pub _pad: u32,
    pub literal_lo: i32,
    pub literal_hi: i32,
    pub _pad2: i32,
    pub _pad3: i32,
}

impl FilterParams {
    /// Create params for an i32 column filter.
    pub fn for_i32(op: u32, literal: i32, num_rows: u32) -> Self {
        Self {
            op,
            col_type: COL_TYPE_I32,
            num_rows,
            _pad: 0,
            literal_lo: literal,
            literal_hi: 0,
            _pad2: 0,
            _pad3: 0,
        }
    }

    /// Create params for an i64 column filter.
    /// The i64 literal is split into lo/hi 32-bit words (little-endian).
    pub fn for_i64(op: u32, literal: i64, num_rows: u32) -> Self {
        let bytes = literal.to_le_bytes();
        let lo = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let hi = i32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        Self {
            op,
            col_type: COL_TYPE_I64,
            num_rows,
            _pad: 0,
            literal_lo: lo,
            literal_hi: hi,
            _pad2: 0,
            _pad3: 0,
        }
    }

    /// Create params for an f32 column filter.
    /// The f32 literal is bitcast to i32 (shader does the reverse bitcast).
    pub fn for_f32(op: u32, literal: f32, num_rows: u32) -> Self {
        Self {
            op,
            col_type: COL_TYPE_F32,
            num_rows,
            _pad: 0,
            literal_lo: literal.to_bits() as i32,
            literal_hi: 0,
            _pad2: 0,
            _pad3: 0,
        }
    }
}

// ── GPU buffer creation helpers ────────────────────────────────────────────

/// Create the uniform buffer for `FilterParams`.
pub fn create_params_buffer(ctx: &WgpuContext, params: &FilterParams) -> wgpu::Buffer {
    ctx.device
        .create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("filter_params"),
            contents: bytemuck::bytes_of(params),
            usage: wgpu::BufferUsages::UNIFORM | wgpu::BufferUsages::COPY_DST,
        })
}

/// Upload an Arrow `Int32Array` as a GPU storage buffer.
pub fn upload_i32_column(ctx: &WgpuContext, array: &Int32Array) -> wgpu::Buffer {
    // Int32Array values are contiguous i32 — safe to view as bytes
    let values = array.values();
    let byte_slice = bytemuck::cast_slice::<i32, u8>(values.as_ref());
    ctx.device
        .create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("col_i32"),
            contents: byte_slice,
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_DST,
        })
}

/// Upload an Arrow `Int64Array` as a GPU storage buffer.
///
/// The i64 values are reinterpreted as pairs of i32 words. On little-endian
/// platforms (x86, ARM), the memory layout is already [lo, hi] per element,
/// which matches the WGSL shader's expected layout.
pub fn upload_i64_column(ctx: &WgpuContext, array: &Int64Array) -> wgpu::Buffer {
    let values = array.values();
    let byte_slice = bytemuck::cast_slice::<i64, u8>(values.as_ref());
    ctx.device
        .create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("col_i64"),
            contents: byte_slice,
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_DST,
        })
}

/// Upload an Arrow `Float32Array` as a GPU storage buffer.
///
/// The f32 values are uploaded as-is; the WGSL shader reads them via
/// `bitcast<f32>(col_data[idx])` since the storage type is `array<i32>`.
pub fn upload_f32_column(ctx: &WgpuContext, array: &Float32Array) -> wgpu::Buffer {
    let values = array.values();
    let byte_slice = bytemuck::cast_slice::<f32, u8>(values.as_ref());
    ctx.device
        .create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("col_f32"),
            contents: byte_slice,
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_DST,
        })
}

/// Create the output bitmask buffer (zero-initialized).
///
/// The bitmask has ceil(num_rows / 32) u32 words, where bit N in word N/32
/// indicates whether row N passes the filter.
#[allow(dead_code)]
pub fn create_output_mask_buffer(ctx: &WgpuContext, num_rows: usize) -> wgpu::Buffer {
    let num_words = (num_rows + 31) / 32;
    let size = (num_words * 4) as u64; // 4 bytes per u32
    ctx.device.create_buffer(&wgpu::BufferDescriptor {
        label: Some("filter_mask"),
        size,
        usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
        mapped_at_creation: false,
    })
}

/// Create a staging buffer for reading back the bitmask from GPU.
#[allow(dead_code)]
pub fn create_staging_buffer(ctx: &WgpuContext, num_rows: usize) -> wgpu::Buffer {
    let num_words = (num_rows + 31) / 32;
    let size = (num_words * 4) as u64;
    ctx.device.create_buffer(&wgpu::BufferDescriptor {
        label: Some("filter_staging"),
        size,
        usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
        mapped_at_creation: false,
    })
}

// ── Pooled buffer helpers ──────────────────────────────────────────────────

/// Usage flags for staging (readback) buffers.
pub const STAGING_USAGE: wgpu::BufferUsages =
    wgpu::BufferUsages::MAP_READ.union(wgpu::BufferUsages::COPY_DST);

/// Usage flags for output mask buffers.
pub const MASK_USAGE: wgpu::BufferUsages = wgpu::BufferUsages::STORAGE
    .union(wgpu::BufferUsages::COPY_SRC)
    .union(wgpu::BufferUsages::COPY_DST);

/// Acquire an output mask buffer from the pool (or allocate fresh).
///
/// The buffer is **always zero-initialised** before being returned, so callers
/// can safely use it as a bitmask accumulator (the filter shader only ORs bits
/// in; it never clears them).  Without this, a reused buffer that still has
/// bits set from a previous dispatch would produce spuriously passing rows.
///
/// Caller is responsible for returning it via `ctx.buffer_pool.release()`.
pub fn acquire_output_mask_buffer(ctx: &WgpuContext, num_rows: usize) -> wgpu::Buffer {
    let num_words = (num_rows + 31) / 32;
    let size = (num_words * 4) as u64;
    let buf = ctx.buffer_pool.acquire(&ctx.device, size, MASK_USAGE);
    // Zero-initialise: overwrite the used portion with zeroes so stale bits
    // from a previous dispatch don't contaminate this one.
    let zeros = vec![0u8; size as usize];
    ctx.queue.write_buffer(&buf, 0, &zeros);
    buf
}

/// Acquire a staging buffer from the pool (or allocate fresh).
///
/// Caller is responsible for returning it via `ctx.buffer_pool.release()`.
pub fn acquire_staging_buffer(ctx: &WgpuContext, num_rows: usize) -> wgpu::Buffer {
    let num_words = (num_rows + 31) / 32;
    let size = (num_words * 4) as u64;
    ctx.buffer_pool.acquire(&ctx.device, size, STAGING_USAGE)
}

/// Upload raw bytes to a GPU storage buffer.
///
/// On UMA (unified memory architecture) systems, uses `mapped_at_creation`
/// to write directly into the buffer — no driver-side copy.
/// On discrete GPU systems, acquires a pooled buffer and uses `queue.write_buffer`.
/// Callers must release pooled buffers via `ctx.buffer_pool.release()`.
/// UMA buffers should **not** be released to the pool (they were freshly allocated).
///
/// **Note**: callers that need to release via the pool should always call `release()`;
/// UMA buffers returned here have STORAGE | COPY_DST usage and will simply be
/// dropped by the pool's bucket-eviction logic if over capacity.
pub fn upload_bytes_pooled(ctx: &WgpuContext, data: &[u8], _label: &'static str) -> wgpu::Buffer {
    let usage = wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_DST;

    if ctx.is_uma {
        // UMA: allocate a fresh buffer with mapped_at_creation — no PCIe copy.
        // The buffer contents are written into directly from CPU, then unmapped.
        ctx.device
            .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some("uma_upload"),
                contents: data,
                usage,
            })
    } else {
        // Discrete GPU: acquire pooled buffer, upload via write_buffer (async DMA).
        let buf = ctx
            .buffer_pool
            .acquire(&ctx.device, data.len() as u64, usage);
        ctx.queue.write_buffer(&buf, 0, data);
        buf
    }
}

/// Upload an `Int32Array` column using a pooled buffer.
pub fn upload_i32_column_pooled(ctx: &WgpuContext, array: &Int32Array) -> wgpu::Buffer {
    let byte_slice = bytemuck::cast_slice::<i32, u8>(array.values().as_ref());
    upload_bytes_pooled(ctx, byte_slice, "col_i32_pooled")
}

/// Upload an `Int64Array` column using a pooled buffer.
pub fn upload_i64_column_pooled(ctx: &WgpuContext, array: &Int64Array) -> wgpu::Buffer {
    let byte_slice = bytemuck::cast_slice::<i64, u8>(array.values().as_ref());
    upload_bytes_pooled(ctx, byte_slice, "col_i64_pooled")
}

/// Upload a `Float32Array` column using a pooled buffer.
pub fn upload_f32_column_pooled(ctx: &WgpuContext, array: &Float32Array) -> wgpu::Buffer {
    let byte_slice = bytemuck::cast_slice::<f32, u8>(array.values().as_ref());
    upload_bytes_pooled(ctx, byte_slice, "col_f32_pooled")
}

/// Convert a GPU bitmask (packed u32 words, LSB-first) to an Arrow `BooleanArray`.
///
/// The GPU shader sets bit N in word N/32 if row N passes. This function
/// reads back the staging buffer and converts it to a `BooleanArray`.
pub fn bitmask_to_boolean_array(
    ctx: &WgpuContext,
    staging_buffer: &wgpu::Buffer,
    num_rows: usize,
) -> BooleanArray {
    let num_words = (num_rows + 31) / 32;
    let byte_size = num_words * 4;

    // Map the staging buffer and read it
    let buffer_slice = staging_buffer.slice(..);
    let (sender, receiver) = std::sync::mpsc::channel();
    buffer_slice.map_async(wgpu::MapMode::Read, move |result| {
        sender.send(result).unwrap();
    });
    ctx.device.poll(wgpu::Maintain::Wait);
    receiver
        .recv()
        .unwrap()
        .expect("Failed to map staging buffer");

    let data = buffer_slice.get_mapped_range();
    let mask_words: &[u32] = bytemuck::cast_slice(&data[..byte_size]);

    // Convert packed u32 bitmask to Arrow BooleanBuffer.
    // The GPU bitmask is LSB-first (bit 0 of word 0 = row 0), which matches
    // Arrow's BooleanBuffer bit layout (LSB-first within each byte).
    // We can directly copy the bytes.
    let mask_bytes: &[u8] = bytemuck::cast_slice(mask_words);
    let bool_buf = BooleanBuffer::new(mask_bytes.to_vec().into(), 0, num_rows);

    drop(data);
    staging_buffer.unmap();

    BooleanArray::new(bool_buf, None)
}

/// Acquire a generic output storage buffer from the pool (or allocate fresh).
///
/// Unlike `acquire_output_mask_buffer`, this takes an explicit byte count
/// and is suitable for projection/aggregate output buffers of arbitrary size.
/// Caller is responsible for returning it via `ctx.buffer_pool.release()`.
pub fn acquire_output_buffer(ctx: &WgpuContext, byte_size: u64) -> wgpu::Buffer {
    ctx.buffer_pool.acquire(&ctx.device, byte_size, MASK_USAGE)
}

/// Acquire a generic staging (readback) buffer from the pool (or allocate fresh).
///
/// Takes an explicit byte count — suitable for projection/aggregate staging
/// buffers of arbitrary size.
/// Caller is responsible for returning it via `ctx.buffer_pool.release()`.
pub fn acquire_staging_bytes(ctx: &WgpuContext, byte_size: u64) -> wgpu::Buffer {
    ctx.buffer_pool
        .acquire(&ctx.device, byte_size, STAGING_USAGE)
}

/// Upload a column from a `RecordBatch` to a GPU storage buffer (pooled).
///
/// Returns `Some(buffer)` if the column type is GPU-compatible (i32, i64, f32),
/// `None` otherwise.  Caller must release via `ctx.buffer_pool.release()`.
pub fn upload_column_pooled(ctx: &WgpuContext, array: &dyn Array) -> Option<wgpu::Buffer> {
    if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        Some(upload_i32_column_pooled(ctx, arr))
    } else if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        Some(upload_i64_column_pooled(ctx, arr))
    } else if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
        Some(upload_f32_column_pooled(ctx, arr))
    } else {
        None
    }
}

/// Upload a column from a `RecordBatch` to a GPU storage buffer.
///
/// Returns `Some(buffer)` if the column type is GPU-compatible (i32, i64, f32),
/// `None` otherwise.
#[allow(dead_code)]
pub fn upload_column(ctx: &WgpuContext, array: &dyn Array) -> Option<wgpu::Buffer> {
    if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        Some(upload_i32_column(ctx, arr))
    } else if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        Some(upload_i64_column(ctx, arr))
    } else if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
        Some(upload_f32_column(ctx, arr))
    } else {
        None
    }
}
